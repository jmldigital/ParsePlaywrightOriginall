import hashlib
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime


class Conflict(Exception):
    pass


class QueueFull(Exception):
    pass


def iso(value):
    return (
        datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z")
        if value is not None
        else None
    )


class Store:
    def __init__(self, path, retention_hours=50, max_active_jobs=100):
        self.path = path
        self._db = None
        self._lock = threading.RLock()
        self.retention = retention_hours * 3600
        self.max_active_jobs = max_active_jobs

    @contextmanager
    def connection(self):
        # A persistent connection avoids a WAL checkpoint on every last-close.
        # All access is serialized; methods run in asyncio's thread executor.
        with self._lock:
            if self._db is None:
                self._db = sqlite3.connect(
                    self.path, timeout=10, check_same_thread=False
                )
                self._db.row_factory = sqlite3.Row
                self._db.execute("PRAGMA foreign_keys=ON")
            with self._db:
                yield self._db

    def close(self):
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY, batch_id TEXT UNIQUE NOT NULL,
                    payload_hash TEXT NOT NULL, max_days INTEGER NOT NULL,
                    status TEXT NOT NULL, total INTEGER NOT NULL,
                    created REAL NOT NULL, last_dispatch REAL NOT NULL DEFAULT 0,
                    completed REAL, expires REAL
                );
                CREATE TABLE IF NOT EXISTS items (
                    job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                    ordinal INTEGER NOT NULL, client_id TEXT NOT NULL,
                    brand TEXT NOT NULL, oem TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', result TEXT,
                    parsed REAL, PRIMARY KEY(job_id, ordinal)
                );
                CREATE INDEX IF NOT EXISTS item_status ON items(job_id,status,ordinal);
                CREATE INDEX IF NOT EXISTS job_expiry ON jobs(expires);
            """)
            # Only one service process may hold the database lock. A crash leaves
            # running items here; completed results must never be discarded.
            db.execute("UPDATE items SET status='pending' WHERE status='running'")
            db.execute("UPDATE jobs SET status='queued' WHERE status='running'")

    def create(self, request):
        payload = request.model_dump(mode="json")
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
        now = time.time()
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM jobs WHERE expires <= ?", (now,))
            existing = db.execute(
                "SELECT * FROM jobs WHERE batch_id=?", (request.batch_id,)
            ).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise Conflict()
                return self.accepted(existing)
            active = db.execute(
                "SELECT count(*) FROM jobs WHERE status IN ('queued','running')"
            ).fetchone()[0]
            if active >= self.max_active_jobs:
                raise QueueFull()
            job_id = str(uuid.uuid4())
            db.execute(
                "INSERT INTO jobs(job_id,batch_id,payload_hash,max_days,status,total,created) VALUES(?,?,?,?,?,?,?)",
                (
                    job_id,
                    request.batch_id,
                    digest,
                    request.max_delivery_days,
                    "queued",
                    len(request.parts),
                    now,
                ),
            )
            db.executemany(
                "INSERT INTO items(job_id,ordinal,client_id,brand,oem) VALUES(?,?,?,?,?)",
                [
                    (job_id, i, p.id, p.brand, p.oem)
                    for i, p in enumerate(request.parts)
                ],
            )
            return dict(
                job_id=job_id,
                batch_id=request.batch_id,
                status="queued",
                total=len(request.parts),
            )

    @staticmethod
    def accepted(row):
        return {key: row[key] for key in ("job_id", "batch_id", "status", "total")}

    def claim(self):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            job = db.execute("""
                SELECT j.job_id, j.max_days FROM jobs j
                WHERE j.status IN ('queued','running')
                  AND EXISTS (SELECT 1 FROM items p WHERE p.job_id=j.job_id AND p.status='pending')
                  AND (SELECT count(*) FROM items r WHERE r.job_id=j.job_id AND r.status='running') < 2
                ORDER BY j.last_dispatch, j.created LIMIT 1
            """).fetchone()
            if job is None:
                return None
            row = dict(
                db.execute(
                    "SELECT * FROM items WHERE job_id=? AND status='pending' ORDER BY ordinal LIMIT 1",
                    (job["job_id"],),
                ).fetchone()
            )
            row["max_days"] = job["max_days"]
            db.execute(
                "UPDATE items SET status='running' WHERE job_id=? AND ordinal=?",
                (row["job_id"], row["ordinal"]),
            )
            db.execute(
                "UPDATE jobs SET status='running',last_dispatch=? WHERE job_id=?",
                (time.time(), row["job_id"]),
            )
            return dict(row)

    def finish(self, item, result):
        now = time.time()
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE items SET status=?,result=?,parsed=? WHERE job_id=? AND ordinal=?",
                (
                    result.status,
                    result.model_dump_json(),
                    now,
                    item["job_id"],
                    item["ordinal"],
                ),
            )
            remaining = db.execute(
                "SELECT count(*) FROM items WHERE job_id=? AND status IN ('pending','running')",
                (item["job_id"],),
            ).fetchone()[0]
            if remaining == 0:
                db.execute(
                    "UPDATE jobs SET status='completed',completed=?,expires=? WHERE job_id=?",
                    (now, now + self.retention, item["job_id"]),
                )

    def get(self, job_id, results=False):
        with self.connection() as db:
            # One read transaction keeps the job status and item snapshot consistent.
            db.execute("BEGIN")
            job = db.execute(
                "SELECT * FROM jobs WHERE job_id=? AND (expires IS NULL OR expires>?)",
                (job_id, time.time()),
            ).fetchone()
            if job is None:
                return None
            rows = db.execute(
                "SELECT * FROM items WHERE job_id=? ORDER BY ordinal", (job_id,)
            ).fetchall()
            if results:
                return dict(
                    job_id=job_id,
                    batch_id=job["batch_id"],
                    status=job["status"],
                    max_delivery_days=job["max_days"],
                    expires_at=iso(job["expires"]),
                    parts=[
                        dict(
                            id=r["client_id"],
                            brand=r["brand"],
                            oem=r["oem"],
                            currency="RUB",
                            parsed_at=iso(r["parsed"]),
                            **json.loads(r["result"]),
                        )
                        for r in rows
                        if r["result"]
                    ],
                )
            counts = {
                s: sum(r["status"] == s for r in rows)
                for s in ("found", "not_found", "error")
            }
            return dict(
                **self.accepted(job),
                processed=sum(counts.values()),
                found=counts["found"],
                not_found=counts["not_found"],
                errors=counts["error"],
            )

    def cleanup(self):
        with self.connection() as db:
            db.execute("DELETE FROM jobs WHERE expires<=?", (time.time(),))
