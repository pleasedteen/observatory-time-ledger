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

    # ------------------------------------------------------------------ #
    # sqlite bookkeeping
    # ------------------------------------------------------------------ #
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
    # atomic version publishing
    # ------------------------------------------------------------------ #
    def next_version_id(self) -> int:
        existing = [d for d in os.listdir(self.versions)
                    if d.startswith("v") and d[1:].isdigit()]
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
        with open(os.path.join(vdir, "current.json"), "r", encoding="utf-8") as f:
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
                    with open(os.path.join(qdir, name),
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
