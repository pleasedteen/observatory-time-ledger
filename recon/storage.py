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

Design properties required by the task:
  * A query reads exactly one complete version (the symlink target), never a
    half-published one. Publishing writes the whole version directory first
    and only then atomically swaps the ``current`` pointer.
  * A crash mid-publish leaves the previous (or no) current readable; the
    new complete version can be published again on rerun.
  * Re-ingesting byte-identical input is idempotent: blobs are content
    addressed, and identical inputs produce identical group results and
    lineage (hashes), so reruns converge to the same state.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
import tempfile
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

try:  # POSIX advisory locks close the lease/check/deletion race.
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None


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


class Lake:
    def __init__(self, root: str, keep_versions: int | None = None,
                 *, retention_keep_versions: int | None = None,
                 auto_retention: bool = False) -> None:
        self.root = os.path.abspath(root)
        if keep_versions is None:
            keep_versions = retention_keep_versions
        if keep_versions is None and os.environ.get("RECON_KEEP_VERSIONS"):
            try:
                keep_versions = int(os.environ["RECON_KEEP_VERSIONS"])
            except ValueError:
                keep_versions = None
        self.default_keep_versions = keep_versions
        self.auto_retention = auto_retention
        self.blobs = os.path.join(self.root, "blobs")
        self.versions = os.path.join(self.root, "versions")
        self.current_link = os.path.join(self.root, "current")
        self.db_path = os.path.join(self.root, "state.db")
        self.locks_dir = os.path.join(self.root, "locks")
        os.makedirs(self.blobs, exist_ok=True)
        os.makedirs(self.versions, exist_ok=True)
        os.makedirs(self.locks_dir, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------ #
    # sqlite bookkeeping
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
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
                CREATE TABLE IF NOT EXISTS ingest_history (
                    history_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    source       TEXT NOT NULL,
                    partition    TEXT NOT NULL,
                    blob_sha     TEXT NOT NULL,
                    source_path  TEXT NOT NULL,
                    byte_size    INTEGER NOT NULL,
                    run_id       TEXT NOT NULL,
                    ingested_at  TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_ingest_history_sha
                    ON ingest_history(blob_sha);
                CREATE TABLE IF NOT EXISTS blob_registry (
                    blob_sha       TEXT PRIMARY KEY,
                    attribution    TEXT NOT NULL,
                    source         TEXT,
                    partition      TEXT,
                    source_path    TEXT,
                    first_seen_at  TEXT NOT NULL,
                    attributed_at  TEXT
                );
                CREATE TABLE IF NOT EXISTS version_blobs (
                    version_id  INTEGER NOT NULL,
                    blob_sha    TEXT NOT NULL,
                    PRIMARY KEY (version_id, blob_sha)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    lease_id     TEXT PRIMARY KEY,
                    kind         TEXT NOT NULL,
                    target_type  TEXT NOT NULL,
                    target_id    TEXT NOT NULL,
                    pid          INTEGER,
                    created_at   TEXT NOT NULL,
                    expires_at   REAL,
                    metadata     TEXT
                );
                CREATE TABLE IF NOT EXISTS gc_runs (
                    gc_id          TEXT PRIMARY KEY,
                    keep_versions  INTEGER NOT NULL,
                    status         TEXT NOT NULL,
                    report         TEXT,
                    started_at     TEXT NOT NULL,
                    finished_at    TEXT
                );
                CREATE TABLE IF NOT EXISTS gc_items (
                    item_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                    gc_id       TEXT NOT NULL,
                    seq         INTEGER NOT NULL,
                    kind        TEXT NOT NULL,
                    target      TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    last_error  TEXT,
                    updated_at  TEXT NOT NULL,
                    UNIQUE(gc_id, seq)
                );
                """
            )
            # Upgrade lakes created before retention existed.  The current
            # ingest row gives one confirmed attribution; older unregistered
            # blobs remain "unknown ownership" and retention never deletes
            # them.
            conn.execute(
                """
                INSERT OR IGNORE INTO blob_registry(
                    blob_sha, attribution, source, partition, source_path,
                    first_seen_at, attributed_at)
                SELECT blob_sha, 'ingest', source, partition, source_path,
                       ingested_at, ingested_at
                  FROM ingests
                """
            )
            conn.execute(
                """
                INSERT INTO ingest_history(
                    source, partition, blob_sha, source_path, byte_size,
                    run_id, ingested_at)
                SELECT i.source, i.partition, i.blob_sha, i.source_path,
                       i.byte_size, i.run_id, i.ingested_at
                  FROM ingests i
                 WHERE NOT EXISTS (
                       SELECT 1 FROM ingest_history h
                        WHERE h.source=i.source
                          AND h.partition=i.partition
                          AND h.blob_sha=i.blob_sha)
                """
            )

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
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO blob_registry(
                       blob_sha, attribution, first_seen_at)
                   VALUES (?, 'unknown', ?)""",
                (sha, utc_now()),
            )
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
                      source_path: str, byte_size: int, run_id: str) -> None:
        now = utc_now()
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
                 now, run_id),
            )
            # Append every accepted replacement.  Unlike ingests, this history
            # keeps attribution after a partition is replaced, allowing
            # retention to distinguish known obsolete originals from files
            # whose origin cannot be established.
            conn.execute(
                """INSERT INTO ingest_history(
                       source, partition, blob_sha, source_path, byte_size,
                       run_id, ingested_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (source, partition, blob_sha, source_path, byte_size,
                 run_id, now),
            )
            conn.execute(
                """INSERT INTO blob_registry(
                       blob_sha, attribution, source, partition, source_path,
                       first_seen_at, attributed_at)
                   VALUES (?, 'ingest', ?, ?, ?, ?, ?)
                   ON CONFLICT(blob_sha) DO UPDATE SET
                       attribution='ingest',
                       source=excluded.source,
                       partition=excluded.partition,
                       source_path=excluded.source_path,
                       attributed_at=excluded.attributed_at""",
                (blob_sha, source, partition, source_path, now, now),
            )

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
    # blob attribution / discovery for retention
    # ------------------------------------------------------------------ #
    def known_blob_shas(self) -> set[str]:
        """SHAs with confirmed ingestion attribution (possibly obsolete)."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT blob_sha FROM ingest_history
                UNION
                SELECT blob_sha FROM blob_registry
                 WHERE attribution='ingest'
                """
            ).fetchall()
        return {r["blob_sha"] for r in rows}

    def current_blob_shas(self) -> set[str]:
        return {r["blob_sha"] for r in self.all_ingests()}

    def scan_blob_files(self) -> dict[str, str]:
        """Return ``{sha: path}`` for well-formed content-addressed files."""
        out: dict[str, str] = {}
        if not os.path.isdir(self.blobs):
            return out
        for prefix in sorted(os.listdir(self.blobs)):
            pdir = os.path.join(self.blobs, prefix)
            if not os.path.isdir(pdir) or prefix.startswith("."):
                continue
            for name in sorted(os.listdir(pdir)):
                path = os.path.join(pdir, name)
                if (len(prefix) == 2 and len(name) == 64
                        and re.fullmatch(r"[0-9a-f]{64}", name)
                        and prefix == name[:2]
                        and os.path.isfile(path)
                        and not os.path.islink(path)):
                    out[name] = path
        return out

    # ------------------------------------------------------------------ #
    # leases protecting paused reads and in-progress writes
    # ------------------------------------------------------------------ #
    def acquire_lease(self, kind: str = "read",
                      target_type: str = "version",
                      target_id: object = None,
                      ttl_seconds: float | None = None,
                      lease_id: str | None = None) -> "Lease":
        self.reap_leases()
        lid = lease_id or f"{kind}-{uuid.uuid4().hex}"
        target = "*" if target_id is None else str(target_id)
        expires = time.time() + ttl_seconds if ttl_seconds else None
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO leases(lease_id, kind, target_type, target_id,
                                      pid, created_at, expires_at, metadata)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (lid, kind, target_type, target, os.getpid(), utc_now(),
                 expires, None),
            )
        lease = Lease(self, lid, kind, target_type, target)
        try:
            lease.acquire_file_lock()
        except Exception:
            self.release_lease(lid)
            raise
        if target_type == "version":
            # If retention held the exclusive lock first, our shared lock was
            # acquired only after deletion finished.  Fail loudly rather than
            # returning a handle to a missing snapshot.
            vid = self.parse_version_id(target)
            if vid is not None and not os.path.exists(
                    os.path.join(self.version_dir(vid), "current.json")):
                lease.release()
                raise FileNotFoundError(f"version was reclaimed: v{vid}")
        return lease

    def read_lease(self, version: object = None,
                   ttl_seconds: float | None = None) -> "Lease":
        vid = self.resolve_version_id(version) if version is not None \
            else self.current_version_id()
        if vid is None:
            raise FileNotFoundError("no published version to read-lock")
        return self.acquire_lease("read", "version", vid, ttl_seconds)

    # Descriptive aliases for snapshot/read coordination callers.
    begin_read = read_lease
    acquire_read_lock = read_lease
    read_lock = read_lease
    snapshot_lease = read_lease
    pin_version = read_lease

    def unpin_version(self, lease_or_id: "Lease | str") -> None:
        if isinstance(lease_or_id, Lease):
            lease_or_id.release()
        else:
            self.release_lease(str(lease_or_id))

    unpin = unpin_version
    release_pin = unpin_version

    def write_lease(self, target_type: str = "lake",
                    target_id: object = "*",
                    ttl_seconds: float | None = None) -> "Lease":
        return self.acquire_lease("write", target_type, target_id, ttl_seconds)

    begin_write = write_lease
    acquire_write_lock = write_lease
    write_lock = write_lease

    def target_lock_path(self, target_type: str, target_id: object) -> str:
        raw = f"{target_type}-{target_id}"
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._") or "target"
        if len(safe) > 120:
            safe = safe[:80] + "-" + sha256_bytes(raw.encode())[:32]
        return os.path.join(self.locks_dir, safe + ".lock")

    @contextmanager
    def target_lock(self, target_type: str, target_id: object,
                    exclusive: bool = True, blocking: bool = False):
        """POSIX flock coordination for one version/blob/partition target."""
        path = self.target_lock_path(target_type, target_id)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        if fcntl is None:  # pragma: no cover
            try:
                yield True
            finally:
                os.close(fd)
            return

        mode = (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        if not blocking:
            mode |= fcntl.LOCK_NB
        try:
            fcntl.flock(fd, mode)
        except BlockingIOError:
            os.close(fd)
            if blocking:
                raise
            yield False
            return

        try:
            yield True
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(fd)

    @contextmanager
    def gc_lock(self, blocking: bool = False):
        with self.target_lock("gc", "run", True, blocking) as acquired:
            yield acquired

    def release_lease(self, lease_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM leases WHERE lease_id=?", (lease_id,))

    def reap_leases(self) -> list[dict]:
        """Delete expired leases and leases owned by dead processes."""
        removed = []
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM leases").fetchall()
            for row in rows:
                d = dict(row)
                if not _lease_alive(d):
                    conn.execute("DELETE FROM leases WHERE lease_id=?",
                                 (d["lease_id"],))
                    removed.append(d)
        return removed

    def active_leases(self, kind: str | None = None) -> list[dict]:
        self.reap_leases()
        q = "SELECT * FROM leases"
        args: list[object] = []
        if kind:
            q += " WHERE kind=?"
            args.append(kind)
        q += " ORDER BY created_at, lease_id"
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(q, args)]

    # ------------------------------------------------------------------ #
    # GC journal
    # ------------------------------------------------------------------ #
    def start_gc_run(self, gc_id: str, keep_versions: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO gc_runs(
                       gc_id, keep_versions, status, started_at)
                   VALUES (?,?, 'running', ?)""",
                (gc_id, int(keep_versions), utc_now()),
            )

    def finish_gc_run(self, gc_id: str, status: str,
                      report: dict | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE gc_runs
                      SET status=?, report=?, finished_at=?
                    WHERE gc_id=?""",
                (status, json.dumps(report or {}, ensure_ascii=False,
                                    sort_keys=True),
                 utc_now(), gc_id),
            )

    def incomplete_gc_runs(self) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM gc_runs WHERE status IN ('running','interrupted')"
                " ORDER BY started_at, gc_id"
            ).fetchall()
            return [dict(r) for r in rows]

    def add_gc_item(self, gc_id: str, seq: int, kind: str,
                    target: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO gc_items(
                       gc_id, seq, kind, target, status, updated_at)
                   VALUES (?,?,?,?, 'pending', ?)""",
                (gc_id, seq, kind, target, utc_now()),
            )

    def gc_items(self, gc_id: str,
                 statuses: tuple[str, ...] | None = None) -> list[dict]:
        q = "SELECT * FROM gc_items WHERE gc_id=?"
        args: list[object] = [gc_id]
        if statuses:
            q += " AND status IN (" + ",".join("?" * len(statuses)) + ")"
            args.extend(statuses)
        q += " ORDER BY seq, item_id"
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(q, args)]

    def set_gc_item_status(self, gc_id: str, seq: int, status: str,
                           error: str | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """UPDATE gc_items
                      SET status=?, last_error=?, updated_at=?
                    WHERE gc_id=? AND seq=?""",
                (status, error, utc_now(), gc_id, seq),
            )

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

    def record_group_version(self, group_id: str, version_id: int,
                             input_hash: str, result_hash: str,
                             run_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO group_versions
                   (group_id, version_id, input_hash, result_hash,
                    run_id, created_at) VALUES (?,?,?,?,?,?)""",
                (group_id, version_id, input_hash, result_hash,
                 run_id, utc_now()),
            )

    def group_version(self, group_id: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM group_versions WHERE group_id=? "
                "ORDER BY version_id DESC LIMIT 1", (group_id,)).fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # version discovery and old-snapshot reads
    # ------------------------------------------------------------------ #
    @staticmethod
    def parse_version_id(value: object) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        s = str(value)
        m = re.fullmatch(r"v?(\d+)", s)
        return int(m.group(1)) if m else None

    def version_dir(self, version_id: object) -> str:
        vid = self.parse_version_id(version_id)
        if vid is None:
            # Also accept an absolute/relative version directory.
            s = os.fspath(version_id)
            if os.path.isdir(s) and os.path.basename(s).startswith("v"):
                return os.path.abspath(s)
            candidate = os.path.join(self.versions, os.path.basename(s.rstrip(os.sep)))
            if os.path.isdir(candidate):
                return candidate
            raise ValueError(f"not a version id/directory: {version_id!r}")
        return os.path.join(self.versions, f"v{vid}")

    def resolve_version_id(self, version: object = None) -> int | None:
        if version is None:
            return self.current_version_id()
        vid = self.parse_version_id(version)
        if vid is not None:
            return vid
        return self.version_id_from_dir(self.version_dir(version))

    @staticmethod
    def version_id_from_dir(path: str) -> int | None:
        m = re.fullmatch(r"v(\d+)", os.path.basename(os.path.abspath(path)))
        return int(m.group(1)) if m else None

    def list_version_ids(self, complete_only: bool = True) -> list[int]:
        if not os.path.isdir(self.versions):
            return []
        ids = []
        for name in os.listdir(self.versions):
            m = re.fullmatch(r"v(\d+)", name)
            if not m or not os.path.isdir(os.path.join(self.versions, name)):
                continue
            vid = int(m.group(1))
            if complete_only and not os.path.exists(
                    os.path.join(self.versions, name, "current.json")):
                continue
            ids.append(vid)
        return sorted(ids)

    def current_version_id(self) -> int | None:
        vdir = self.current_version_dir()
        return self.version_id_from_dir(vdir) if vdir else None

    def record_version_blobs(self, version_id: int,
                             blob_shas: set[str] | list[str]) -> None:
        shas = sorted(set(blob_shas))
        with self._connect() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO version_blobs(version_id, blob_sha)"
                " VALUES (?,?)",
                [(int(version_id), sha) for sha in shas],
            )

    def version_blob_shas(self, version_id: int) -> set[str]:
        vid = int(version_id)
        refs: set[str] = set()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT blob_sha FROM version_blobs WHERE version_id=?",
                (vid,),
            ).fetchall()
        refs.update(r["blob_sha"] for r in rows)

        vdir = self.version_dir(vid)
        manifest_path = os.path.join(vdir, "current.json")
        manifest = None
        if os.path.exists(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            for key in ("input_blobs", "blob_shas", "blobs"):
                value = manifest.get(key)
                if isinstance(value, list):
                    refs.update(x for x in value
                                if isinstance(x, str) and len(x) == 64)
        # Defensive extraction from lineage/manifests.  Only fields that name
        # blob references are accepted; business input/result hashes are not
        # mistaken for content-addressed originals.
        if os.path.isdir(vdir):
            for root, _dirs, files in os.walk(vdir):
                for name in files:
                    if not name.endswith(".json"):
                        continue
                    try:
                        with open(os.path.join(root, name), "r",
                                  encoding="utf-8") as f:
                            doc = json.load(f)
                    except (OSError, json.JSONDecodeError):
                        continue
                    refs.update(_extract_blob_refs(doc))
        return refs

    def all_on_disk_version_blob_shas(self) -> set[str]:
        """Conservative refs from every surviving ``v*`` directory."""
        refs: set[str] = set()
        for vid in self.list_version_ids(complete_only=False):
            vdir = self.version_dir(vid)
            for root, _dirs, files in os.walk(vdir):
                for name in files:
                    if not name.endswith(".json"):
                        continue
                    try:
                        with open(os.path.join(root, name), "r",
                                  encoding="utf-8") as f:
                            doc = json.load(f)
                    except (OSError, json.JSONDecodeError):
                        continue
                    refs.update(_extract_blob_refs(doc))
        return refs

    def open_snapshot(self, version: object = None,
                      ttl_seconds: float | None = None):
        # Imported here to keep storage free of query-layer dependencies.
        from .query import Snapshot
        vid = self.resolve_version_id(version)
        if vid is None:
            raise FileNotFoundError("no published snapshot")
        lease = self.read_lease(vid, ttl_seconds=ttl_seconds)
        return Snapshot(self, version_id=vid, lease=lease)

    snapshot_read = open_snapshot
    read_snapshot = open_snapshot
    begin_snapshot = open_snapshot
    snapshot_session = open_snapshot
    acquire_snapshot = open_snapshot
    snapshot_context = open_snapshot
    snapshot = open_snapshot

    def query_service(self, version: object = None):
        from .query import QueryService
        return QueryService(self, version=version)

    query = query_service

    # ------------------------------------------------------------------ #
    # atomic version publishing
    # ------------------------------------------------------------------ #
    def next_version_id(self) -> int:
        existing = [d for d in os.listdir(self.versions)
                    if re.fullmatch(r"v\d+", d)]
        if not existing:
            return 1
        return max(int(d[1:]) for d in existing) + 1

    def publish_version(self, version_id: int, manifest: dict,
                        group_docs: dict, quarantine_docs: dict) -> str:
        """Write a complete version directory then atomically swap current.

        Returns the version directory path. The swap is the only mutating
        step visible to readers, guaranteeing snapshot isolation.
        """
        vdir = os.path.join(self.versions, f"v{version_id}")
        if os.path.exists(vdir):
            raise FileExistsError(f"version already exists: {vdir}")
        gdir = os.path.join(vdir, "groups")
        qdir = os.path.join(vdir, "quarantine")
        # build in a temp staging dir next to versions, then atomic rename
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

        self._swap_current(vdir)
        return vdir

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
        return self.manifest_for_dir(vdir)

    @staticmethod
    def manifest_for_dir(vdir: str) -> dict | None:
        path = os.path.join(vdir, "current.json")
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def read_group_doc(self, group_id: str,
                       version_dir: str | None = None) -> dict | None:
        vdir = version_dir or self.current_version_dir()
        if not vdir:
            return None
        p = os.path.join(vdir, "groups", f"{group_id}.json")
        if not os.path.exists(p):
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)

    def read_all_group_docs(self,
                            version_dir: str | None = None) -> list[dict]:
        vdir = version_dir or self.current_version_dir()
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

    def read_all_quarantine(self,
                            version_dir: str | None = None) -> list[dict]:
        vdir = version_dir or self.current_version_dir()
        if not vdir:
            return []
        qdir = os.path.join(vdir, "quarantine")
        out = []
        if os.path.isdir(qdir):
            for name in sorted(os.listdir(qdir)):
                if name.endswith(".json"):
                    with open(os.path.join(qdir, name),
                              encoding="utf-8") as f:
                        out.append(json.load(f))
        return out

    # ------------------------------------------------------------------ #
    # retention convenience wrappers
    # ------------------------------------------------------------------ #
    def retention_plan(self, keep_versions: int | None = 3, **kwargs):
        from .retention import plan_retention
        return plan_retention(self, keep_versions=keep_versions, **kwargs)

    def retain_versions(self, keep_versions: int | None = None, **kwargs):
        from .retention import run_retention
        if keep_versions is None:
            keep_versions = self.default_keep_versions
        return run_retention(self, keep_versions=keep_versions, **kwargs)

    def resume_gc(self, gc_id: str | None = None,
                  keep_versions: int | None = None, **kwargs):
        from .retention import resume_retention
        return resume_retention(self, gc_id, keep_versions, **kwargs)

    resume_retention = resume_gc

    # Common synonyms used by operations scripts and tests.
    apply_retention = retain_versions
    apply_retention_policy = retain_versions
    enforce_retention = retain_versions
    retain_latest = retain_versions
    collect_garbage = retain_versions
    garbage_collect = retain_versions
    cleanup_old_versions = retain_versions
    cleanup = retain_versions
    prune = retain_versions
    gc = retain_versions
    vacuum = retain_versions


class Lease:
    """Advisory SQLite-backed lease; usable as a context manager."""

    def __init__(self, lake: Lake, lease_id: str, kind: str,
                 target_type: str, target_id: str) -> None:
        self.lake = lake
        self.lease_id = lease_id
        self.kind = kind
        self.target_type = target_type
        self.target_id = target_id
        self._released = False
        self._lock_fd = None

    def acquire_file_lock(self) -> None:
        if fcntl is None:
            return
        path = self.lake.target_lock_path(self.target_type, self.target_id)
        self._lock_fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        # Readers share version locks; writers and GC are exclusive.  A reader
        # waits for an in-progress deletion, then Lake.acquire_lease verifies
        # that the version still exists.
        mode = fcntl.LOCK_SH if self.kind == "read" else fcntl.LOCK_EX
        fcntl.flock(self._lock_fd, mode)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        if self._lock_fd is not None and fcntl is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
        try:
            self.lake.release_lease(self.lease_id)
        except sqlite3.Error:
            pass

    def close(self) -> None:
        self.release()

    def heartbeat(self, ttl_seconds: float | None = None) -> None:
        expires = time.time() + ttl_seconds if ttl_seconds else None
        with self.lake._connect() as conn:
            conn.execute(
                "UPDATE leases SET expires_at=? WHERE lease_id=?",
                (expires, self.lease_id),
            )

    def __enter__(self) -> "Lease":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()

    def __del__(self) -> None:
        self.release()


ReadLease = Lease
WriteLease = Lease
SnapshotLease = Lease


def retain_versions(lake_or_root, keep_versions: int | None = 3, **kwargs):
    """Module-level retention wrapper for ``Lake.retain_versions``."""
    lake = lake_or_root if isinstance(lake_or_root, Lake) else Lake(lake_or_root)
    return lake.retain_versions(keep_versions, **kwargs)


def collect_garbage(lake_or_root, keep_versions: int | None = 3, **kwargs):
    return retain_versions(lake_or_root, keep_versions, **kwargs)


def plan_retention(lake_or_root, keep_versions: int | None = 3, **kwargs):
    lake = lake_or_root if isinstance(lake_or_root, Lake) else Lake(lake_or_root)
    return lake.retention_plan(keep_versions, **kwargs)


_BLOB_REF_KEYS = {
    "blob_sha", "blob_shas", "blob_ref", "blob_refs", "input_blob",
    "input_blobs", "blobs", "sha256", "sha",
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _extract_blob_refs(obj, refs: set[str] | None = None) -> set[str]:
    if refs is None:
        refs = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_l = str(key).lower()
            if isinstance(value, str) and (
                    key_l in _BLOB_REF_KEYS or key_l.endswith("_blob_sha")):
                if _SHA256_RE.fullmatch(value):
                    refs.add(value)
            elif isinstance(value, (list, tuple, set)) and (
                    key_l in _BLOB_REF_KEYS or "blob" in key_l):
                for item in value:
                    if isinstance(item, str) and _SHA256_RE.fullmatch(item):
                        refs.add(item)
                    else:
                        _extract_blob_refs(item, refs)
            else:
                _extract_blob_refs(value, refs)
    elif isinstance(obj, (list, tuple, set)):
        for item in obj:
            _extract_blob_refs(item, refs)
    return refs


def _lease_alive(row: dict) -> bool:
    expires = row.get("expires_at")
    if expires and float(expires) < time.time():
        return False
    pid = row.get("pid")
    if pid is None or int(pid) == os.getpid():
        # A same-process lease is considered alive while its object/context
        # exists.  Normal exception paths release it in ``finally``; a hard
        # in-process leak is conservatively protected.
        return True
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


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
