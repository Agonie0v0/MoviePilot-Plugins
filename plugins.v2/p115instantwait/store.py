"""Durable queue. This module deliberately has no MoviePilot dependency."""
import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


TERMINAL = ("completed", "cancelled", "failed")


class QueueStore:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, source TEXT NOT NULL, state TEXT NOT NULL,
                ready INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                next_at REAL NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
                history_id INTEGER, message TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL
            )""")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS active_source ON jobs(source)
                WHERE state NOT IN ('completed', 'cancelled', 'failed')""")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def decode(row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def enqueue(self, source, payload):
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE source=? AND state NOT IN "
                             "('completed','cancelled','failed')", (source,)).fetchone()
            if row:
                return self.decode(row), False
            key = hashlib.sha256(f"{source}:{time.time_ns()}".encode()).hexdigest()[:24]
            db.execute("INSERT INTO jobs(id,source,state,next_at,created,updated,payload) "
                       "VALUES(?,?,'queued',?,?,?,?)",
                       (key, source, now, now, now, json.dumps(payload, ensure_ascii=False)))
            return self.decode(db.execute("SELECT * FROM jobs WHERE id=?", (key,)).fetchone()), True

    def get(self, key):
        with self.connect() as db:
            return self.decode(db.execute("SELECT * FROM jobs WHERE id=?", (key,)).fetchone())

    def active_for(self, source):
        with self.connect() as db:
            return self.decode(db.execute("SELECT * FROM jobs WHERE source=? AND state NOT IN "
                                          "('completed','cancelled','failed')", (source,)).fetchone())

    def all(self, active=False, limit=200):
        with self.connect() as db:
            where = "WHERE state NOT IN ('completed','cancelled','failed')" if active else ""
            return [self.decode(row) for row in db.execute(
                f"SELECT * FROM jobs {where} ORDER BY created DESC LIMIT ?", (limit,))]

    def update(self, key, **changes):
        allowed = {"state", "ready", "attempts", "next_at", "history_id", "message", "payload"}
        if not changes or set(changes) - allowed:
            raise ValueError("Invalid queue update")
        if "payload" in changes:
            changes["payload"] = json.dumps(changes["payload"], ensure_ascii=False)
        changes["updated"] = time.time()
        with self.connect() as db:
            db.execute("UPDATE jobs SET " + ",".join(f"{k}=?" for k in changes) + " WHERE id=?",
                       (*changes.values(), key))

    def claim(self, now=None):
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE ready=1 AND state IN ('queued','waiting') "
                             "AND next_at<=? ORDER BY next_at,created LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE jobs SET state='running', attempts=attempts+1, updated=? WHERE id=?",
                       (now, row["id"]))
            return self.decode(db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())

    def recover(self):
        with self.connect() as db:
            db.execute("UPDATE jobs SET state='waiting',next_at=?,ready=1 WHERE state='running'",
                       (time.time(),))

    def command(self, key, action):
        """Do not race an in-flight upload or completion callback."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE id=?", (key,)).fetchone()
            if not row:
                raise KeyError(key)
            if row["state"] == "running":
                raise ValueError("任务正在执行，请在本轮完成后操作")
            if action == "resume" and row["state"] == "paused":
                state = "waiting"
                payload = json.loads(row["payload"])
                payload["wait_since"] = time.time()
                db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), key))
            elif action == "pause" and row["state"] in ("queued", "waiting"):
                state = "paused"
            elif action == "cancel" and row["state"] in ("queued", "waiting", "paused"):
                state = "cancelled"
            else:
                raise ValueError("当前状态不支持此操作")
            db.execute("UPDATE jobs SET state=?,message=?,next_at=?,updated=? WHERE id=?",
                       (state, {"waiting": "等待秒传", "paused": "已暂停", "cancelled": "已取消"}[state],
                        time.time(), time.time(), key))
        return self.get(key)
