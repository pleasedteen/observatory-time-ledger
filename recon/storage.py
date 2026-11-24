"""Content-addressed data-lake storage with atomic, snapshot-isolated versions.

On-disk layout under a root directory::

    root/
      blobs/<sha256-prefix>/<sha256>      immutable ingested file copies
      versions/
        v<id>/                              one complete, immutable version
          current.json                      manifest of groups + lineage
          groups/<group_id>.json           per-group reconciliation result
          quarantine/<qid>.json             rejected records with reasons
      current -> versions/v<id>             atomic symlink (or file pointer)
      state.db                              sqlite ingestion/run bookkeeping

Publication consistency model
-----------------------------
Every accepted ingestion advances a monotonic ``input_epoch`` in SQLite.
A rerun captures the epoch it computed from; publication only commits when
that epoch is still the newest one (fencing), so a slow run that finishes
after a newer input batch took effect can never publish stale results as
the current version. The same transaction also makes identical-input
reruns converge to one published version (idempotent no-op).

Publishing is a crash-safe two-phase protocol driven by a ``publications``
ledger:

  1. a fenced SQLite transaction allocates the next version id and commits a
     row in state ``staged``; the per-group ``group_versions`` lineage rows
     are then persisted durably against that same version id, all before any
     reader-visible change happens;
  2. the complete version directory is built in a temp staging dir and
     ``current`` is atomically swapped to it, after which the ledger row is
     marked ``current`` and the previous one demoted to ``abandoned``.

A crash at any point leaves the previous ``current`` pointer readable and
whole. Leftover ``staged`` rows that do not match the current pointer are
reconciled as abandoned on the next open/rerun; incremental reruns reuse
group results only through lineage rows attached to the current
publication, so a superseded or interrupted run can never feed the next
incremental rerun a wrong-version result.

Concurrent writers run on SQLite WAL with a busy timeout: a long in-memory
solve holds no lock, so ingestion and a second rerun can complete while an
older run is still computing.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def canonical_json(obj) -> bytes:
    """Deterministic JSON used for hashing (sorted keys, no whitespace)."""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")


class PublicationSuperseded(Exception):
    """Raised when a run's inputs are no longer the newest accepted state."""

    def __init__(self, reason: str, run_id: str,
                 current_version_id: int | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.run_id = run_id
        self.current_version_id = current_version_id


class Lake:
    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        self.blobs = os.path.join(self.root, "blobs")
        self.versions = os.path.join(self.root, "versions")
        self.current_link = os.path.join(self.root, "current")
        self.db_path = os.path.join(self.root, "state.db")
        os.makedirs(self.blobs, exist_ok=True)
        os.makedirs(self.versions, exist_ok=True)
        self._init_db()
        self.recover_publications()
        self.cleanup_staging_dirs()

    # ------------------------------------------------------------------ #
    # sqlite bookkeeping
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        # WAL allows concurrent readers while a writer commits; the busy
        # timeout makes short lock waits (e.g. an ingest landing during a
        # solve) block briefly instead of raising "database is locked".
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingests (
                    source       TEXT NOT NULL,
                    partition    TEXT NOT NULL,
                    blob_sha     TEXT NOT NULL,
                    source_path  TEXT NOT NULL,
                    byte_size    INTEGER NOT NULL,
                    ingested_at  TEXT NOT NULL,
                    run_id       TEXT NOT NULL,
                    PRIMARY KEY (source, partition)
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id       TEXT PRIMARY KEY,
                    started_at   TEXT NOT NULL,
                    finished_at  TEXT,
                    status       TEXT NOT NULL,
                    note         TEXT
                );
                CREATE TABLE IF NOT EXISTS group_versions (
                    group_id     TEXT NOT NULL,
                    version_id   INTEGER NOT NULL,
                    input_hash   TEXT NOT NULL,
                    result_hash  TEXT NOT NULL,
                    run_id       TEXT NOT NULL,
                    created_at   TEXT NOT NULL,
                    PRIMARY KEY (group_id, version_id)
                );
                CREATE TABLE IF NOT EXISTS quarantine (
                    qid          TEXT PRIMARY KEY,
                    payload      TEXT NOT NULL,
                    created_at   TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS meta (
                    key          TEXT PRIMARY KEY,
                    value        INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS publications (
                    version_id      INTEGER PRIMARY KEY,
                    run_id          TEXT NOT NULL,
                    input_epoch     INTEGER NOT NULL,
                    input_fingerprint TEXT NOT NULL,
                    state           TEXT NOT NULL,
                    created_at      TEXT NOT NULL,
                    committed_at    TEXT
                );
                """
            )
            # monotonic counter of the accepted input state
            conn.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('input_epoch', 0)")
        self._migrate_legacy_versions()

    def _migrate_legacy_versions(self) -> None:
        """Backfill the publications ledger for lakes created before fencing."""
        existing = sorted(
            int(d[1:]) for d in os.listdir(self.versions)
            if d.startswith("v") and d[1:].isdigit())
        if not existing:
            return
        with self._connect() as conn:
            known = {r["version_id"] for r in conn.execute(
                "SELECT version_id FROM publications")}
            current_id = self._current_version_id_from_link()
            for vid in existing:
                if vid in known:
                    continue
                state = "current" if vid == current_id else "abandoned"
                conn.execute(
                    "INSERT INTO publications(version_id, run_id, input_epoch,"
                    " input_fingerprint, state, created_at, committed_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (vid, "legacy-migration", 0, "legacy", state,
                     utc_now(), utc_now() if state == "current" else None))

    # ------------------------------------------------------------------ #
    # durable quarantine (append-only, content-deduplicated)
    # ------------------------------------------------------------------ #
    def add_quarantine(self, qid: str, payload: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO quarantine(qid, payload, created_at)"
                " VALUES (?,?,?)",
                (qid, json.dumps(payload, ensure_ascii=False), utc_now()),
            )

    def all_quarantine(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload FROM quarantine ORDER BY created_at, qid")
            return [json.loads(r["payload"]) for r in rows]

    # ------------------------------------------------------------------ #
    # immutable blobs
    # ------------------------------------------------------------------ #
    def store_blob(self, data: bytes) -> tuple[str, str]:
        """Store bytes content-addressed; return (sha, absolute_blob_path)."""
        sha = sha256_bytes(data)
        rel = os.path.join(sha[:2], sha)
        path = os.path.join(self.blobs, rel)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            # write to a temp file in the same dir, then atomic rename
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path))
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)
        return sha, path

    def store_file_blob(self, src_path: str) -> tuple[str, str, int]:
        with open(src_path, "rb") as f:
            data = f.read()
        sha, path = self.store_blob(data)
        return sha, path, len(data)

    def blob_path(self, sha: str) -> str:
        return os.path.join(self.blobs, sha[:2], sha)

    def read_blob(self, sha: str) -> bytes:
        with open(self.blob_path(sha), "rb") as f:
            return f.read()

    # ------------------------------------------------------------------ #
    # ingestion records (whole-partition replacement per source)
    # ------------------------------------------------------------------ #
    def record_ingest(self, source: str, partition: str, blob_sha: str,
                      source_path: str, byte_size: int, run_id: str) -> int:
        """Record an accepted partition replacement and bump input_epoch.

        Returns the new epoch. The bump happens in the same transaction as
        the ingest row, so a rerun can fence itself against any input that
        became effective while it was solving.
        """
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO ingests(source, partition, blob_sha, source_path,
                                       byte_size, ingested_at, run_id)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(source, partition) DO UPDATE SET
                       blob_sha=excluded.blob_sha,
                       source_path=excluded.source_path,
                       byte_size=excluded.byte_size,
                       ingested_at=excluded.ingested_at,
                       run_id=excluded.run_id""",
                (source, partition, blob_sha, source_path, byte_size,
                 utc_now(), run_id),
            )
            cur = conn.execute(
                "UPDATE meta SET value = value + 1 WHERE key='input_epoch'")
            row = conn.execute(
                "SELECT value FROM meta WHERE key='input_epoch'").fetchone()
            return int(row["value"])

    def get_ingest(self, source: str, partition: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM ingests WHERE source=? AND partition=?",
                (source, partition),
            ).fetchone()
            return dict(row) if row else None

    def all_ingests(self) -> list[dict]:
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM ingests ORDER BY source, partition")]

    def ingests_for_partitions(self, source: str, partitions: set) -> list[dict]:
        if not partitions:
            return []
        q = ("SELECT * FROM ingests WHERE source=? AND partition IN ("
             + ",".join("?" * len(partitions)) + ")")
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                q, [source, *sorted(partitions)])]

    # ------------------------------------------------------------------ #
    # input epoch / fingerprint fencing
    # ------------------------------------------------------------------ #
    def current_input_epoch(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key='input_epoch'").fetchone()
            return int(row["value"]) if row else 0

    def input_fingerprint(self) -> str:
        """Stable digest of the whole accepted input state + quarantine."""
        with self._connect() as conn:
            ingests = [
                (r["source"], r["partition"], r["blob_sha"])
                for r in conn.execute(
                    "SELECT source, partition, blob_sha FROM ingests "
                    "ORDER BY source, partition")]
            qids = [r["qid"] for r in conn.execute(
                "SELECT qid FROM quarantine ORDER BY qid")]
        return sha256_bytes(canonical_json(
            {"ingests": ingests, "quarantine": qids}))

    # ------------------------------------------------------------------ #
    # runs
    # ------------------------------------------------------------------ #
    def start_run(self, run_id: str, note: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO runs(run_id, started_at, status, note)"
                " VALUES (?,?,?,?)",
                (run_id, utc_now(), "running", note),
            )

    def finish_run(self, run_id: str, status: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET finished_at=?, status=? WHERE run_id=?",
                (utc_now(), status, run_id),
            )

    def group_version(self, group_id: str):
        """Lineage of the group as attached to the CURRENT publication only.

        Rows belonging to staged/abandoned (i.e. never or no longer current)
        versions are invisible here, so an interrupted or superseded run can
        never supply a wrong-version result to the next incremental rerun.
        """
        with self._connect() as conn:
            row = conn.execute(
                """SELECT gv.* FROM group_versions gv
                   JOIN publications p ON p.version_id = gv.version_id
                   WHERE gv.group_id=? AND p.state='current'
                   ORDER BY gv.version_id DESC LIMIT 1""",
                (group_id,)).fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # crash-safe, fenced two-phase publication
    # ------------------------------------------------------------------ #
    def reserve_publication(self, run_id: str, input_epoch: int,
                            input_fingerprint: str) -> dict:
        """Phase 1: fence check + durable version reservation.

        The reservation (version id, fenced input epoch/fingerprint) is
        COMMITTED here in state ``staged``. It therefore survives a hard
        process stop during the (potentially slow) version-tree build, and
        recovery can reconcile it deterministically.

        Returns ``{"status": "reserved", "version_id": id}``,
        ``{"status": "unchanged", "version_id": current_id}`` when this
        exact input state is already published, or raises
        :class:`PublicationSuperseded` when a newer input epoch is active.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            newest_epoch = int(conn.execute(
                "SELECT value FROM meta WHERE key='input_epoch'").fetchone()[
                    "value"])
            if newest_epoch != input_epoch:
                cur = conn.execute(
                    "SELECT version_id FROM publications WHERE state='current'"
                    " ORDER BY version_id DESC LIMIT 1").fetchone()
                conn.commit()
                raise PublicationSuperseded(
                    f"run {run_id} computed from input_epoch {input_epoch} but "
                    f"the accepted input epoch is now {newest_epoch}; "
                    "stale publication rejected",
                    run_id, cur["version_id"] if cur else None)

            same = conn.execute(
                "SELECT version_id FROM publications "
                "WHERE state='current' AND input_fingerprint=? "
                "ORDER BY version_id DESC LIMIT 1",
                (input_fingerprint,)).fetchone()
            if same:
                conn.commit()
                return {"status": "unchanged",
                        "version_id": same["version_id"]}

            row = conn.execute(
                "SELECT COALESCE(MAX(version_id), 0) + 1 AS next_id "
                "FROM publications").fetchone()
            version_id = int(row["next_id"])
            conn.execute(
                "INSERT INTO publications(version_id, run_id, input_epoch,"
                " input_fingerprint, state, created_at, committed_at)"
                " VALUES (?,?,?,?, 'staged', ?, NULL)",
                (version_id, run_id, input_epoch, input_fingerprint,
                 utc_now()))
            conn.commit()
            return {"status": "reserved", "version_id": version_id}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_group_version(self, group_id: str, version_id: int,
                             input_hash: str, result_hash: str,
                             run_id: str) -> None:
        """Persist one group lineage row against a staged publication.

        Each row is its own durable transaction so the lineage of a
        reserved version survives a crash before the tree build finishes.
        """
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO group_versions
                   (group_id, version_id, input_hash, result_hash,
                    run_id, created_at) VALUES (?,?,?,?,?,?)""",
                (group_id, version_id, input_hash, result_hash,
                 run_id, utc_now()))

    def write_version_tree(self, version_id: int, manifest: dict,
                           group_docs: dict, quarantine_docs: dict) -> str:
        """Build the complete immutable version directory, then swap current.

        The directory is fully written and atomically renamed before the
        reader-visible ``current`` pointer moves; readers never observe a
        half-published tree. A crash before the swap leaves the durable
        ``staged`` reservation (and its lineage) behind for recovery.
        """
        vdir = os.path.join(self.versions, f"v{version_id}")
        if os.path.exists(vdir):
            raise FileExistsError(f"version already exists: {vdir}")
        parent = self.versions
        fd, stage = tempfile.mkstemp(prefix=".stage-", dir=parent)
        os.close(fd)
        os.unlink(stage)
        os.makedirs(os.path.join(stage, "groups"))
        os.makedirs(os.path.join(stage, "quarantine"))
        try:
            for gid, doc in group_docs.items():
                _atomic_write_json(os.path.join(stage, "groups", f"{gid}.json"),
                                   doc)
            for qid, doc in quarantine_docs.items():
                _atomic_write_json(
                    os.path.join(stage, "quarantine", f"{qid}.json"), doc)
            _atomic_write_json(os.path.join(stage, "current.json"), manifest)
            os.replace(stage, vdir)
        except Exception:
            _rmtree(stage)
            raise
        # fault point: the version tree is complete and durable but the
        # reader-visible pointer has not moved yet
        from .pipeline import maybe_fault
        maybe_fault("before_swap")
        self._swap_current(vdir)
        return vdir

    def commit_publication(self, version_id: int) -> None:
        """Phase 2: mark the reserved publication current and demote the old."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE publications SET state='abandoned' WHERE state='current'")
            conn.execute(
                "UPDATE publications SET state='current', committed_at=? "
                "WHERE version_id=?", (utc_now(), version_id))

    def abandon_publication(self, version_id: int) -> None:
        """Mark a reserved version abandoned after the tree build failed."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE publications SET state='abandoned' "
                "WHERE version_id=? AND state='staged'", (version_id,))

    def abort_publication(self, version_id: int | None = None) -> None:
        """Roll back/finalize a reservation after a publication failure."""
        if version_id is None:
            return
        self.abandon_publication(version_id)

    def recover_publications(self) -> None:
        """Reconcile the ledger with the on-disk ``current`` pointer.

        A crash between the pointer swap and the ledger commit leaves a
        ``staged`` row whose directory is actually current: promote it.
        Other staged rows are abandoned reservations; their directories
        (if any) are never referenced by ``current`` and are safe to drop.
        """
        current_id = self._current_version_id_from_link()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT version_id, state FROM publications").fetchall()
            for r in rows:
                vid, state = r["version_id"], r["state"]
                if state == "staged" and vid == current_id:
                    conn.execute(
                        "UPDATE publications SET state='current', "
                        "committed_at=COALESCE(committed_at, ?) "
                        "WHERE version_id=?", (utc_now(), vid))
                    conn.execute(
                        "UPDATE publications SET state='abandoned' "
                        "WHERE state='current' AND version_id<>?", (vid,))
                elif state == "staged":
                    conn.execute(
                        "UPDATE publications SET state='abandoned' "
                        "WHERE version_id=?", (vid,))
            # exactly one current row; if none matches the pointer, keep
            # the newest current we have and leave the pointer untouched
            cur_rows = conn.execute(
                "SELECT COUNT(*) AS c FROM publications WHERE state='current'"
            ).fetchone()
            if cur_rows["c"] > 1:
                keep = conn.execute(
                    "SELECT MAX(version_id) AS m FROM publications "
                    "WHERE state='current'").fetchone()["m"]
                conn.execute(
                    "UPDATE publications SET state='abandoned' "
                    "WHERE state='current' AND version_id<>?", (keep,))

    def cleanup_staging_dirs(self) -> None:
        """Remove temp stage trees left behind by a hard process stop."""
        for name in os.listdir(self.versions):
            if name.startswith(".stage-"):
                _rmtree(os.path.join(self.versions, name))

    def _current_version_id_from_link(self) -> int | None:
        vdir = self.current_version_dir()
        if not vdir:
            return None
        name = os.path.basename(vdir)
        if name.startswith("v") and name[1:].isdigit():
            return int(name[1:])
        return None

    def current_publication(self) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM publications WHERE state='current' "
                "ORDER BY version_id DESC LIMIT 1").fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    def _swap_current(self, vdir: str) -> None:
        """Atomically point ``current`` at the given version directory."""
        target = os.path.abspath(vdir)
        tmp_link = os.path.join(self.root, ".current.tmp")
        if os.path.islink(tmp_link) or os.path.exists(tmp_link):
            os.unlink(tmp_link)
        try:
            os.symlink(target, tmp_link)
            os.replace(tmp_link, self.current_link)
        except OSError:
            # Filesystem without symlink support: fall back to an atomic
            # pointer file containing the absolute version path.
            _atomic_write_text(self.current_link + ".tmp", target)
            os.replace(self.current_link + ".tmp", self.current_link)

    def current_version_dir(self) -> str | None:
        link = self.current_link
        if os.path.islink(link):
            return os.readlink(link)
        if os.path.exists(link):
            with open(link, "r", encoding="utf-8") as f:
                p = f.read().strip()
                return p if os.path.isdir(p) else None
        return None

    def current_manifest(self) -> dict | None:
        vdir = self.current_version_dir()
        if not vdir:
            return None
        with open(os.path.join(vdir, "current.json"), "r",
                  encoding="utf-8") as f:
            return json.load(f)

    def read_group_doc(self, group_id: str) -> dict | None:
        vdir = self.current_version_dir()
        if not vdir:
            return None
        p = os.path.join(vdir, "groups", f"{group_id}.json")
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    def read_all_group_docs(self) -> list[dict]:
        vdir = self.current_version_dir()
        if not vdir:
            return []
        gdir = os.path.join(vdir, "groups")
        out = []
        if os.path.isdir(gdir):
            for name in sorted(os.listdir(gdir)):
                if name.endswith(".json"):
                    with open(os.path.join(gdir, name), "r",
                              encoding="utf-8") as f:
                        out.append(json.load(f))
        return out

    def read_all_quarantine(self) -> list[dict]:
        vdir = self.current_version_dir()
        if not vdir:
            return []
        qdir = os.path.join(vdir, "quarantine")
        out = []
        if os.path.isdir(qdir):
            for name in sorted(os.listdir(qdir)):
                if name.endswith(".json"):
                    with open(os.path.join(qdir, name), "r",
                              encoding="utf-8") as f:
                        out.append(json.load(f))
        return out


def _atomic_write_json(path: str, obj) -> None:
    _atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2,
                                        sort_keys=True))


def _atomic_write_text(path: str, text: str) -> None:
    d = os.path.dirname(path)
    fd, tmp = tempfile.mkstemp(dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _rmtree(path: str) -> None:
    import shutil
    shutil.rmtree(path, ignore_errors=True)
