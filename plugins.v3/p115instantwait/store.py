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
            db.execute("CREATE TABLE IF NOT EXISTS target_owners (path TEXT PRIMARY KEY, job_id TEXT NOT NULL)")
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

    def reserve_target(self, key, path):
        """Fence both workers and restarts against concurrent same-target uploads."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM target_owners WHERE job_id IN (SELECT id FROM jobs "
                       "WHERE state IN ('completed','cancelled','failed'))")
            db.execute("DELETE FROM target_owners WHERE job_id NOT IN (SELECT id FROM jobs)")
            db.execute("INSERT OR IGNORE INTO target_owners(path,job_id) VALUES(?,?)", (str(path), key))
            return db.execute("SELECT job_id FROM target_owners WHERE path=?", (str(path),)).fetchone()[0] == key

    def clear_history(self, *, mode="selected", keys=None, states=None, days=30, now=None):
        """Call while the engine is stopped. Leave active batch recovery peers intact."""
        if mode not in ("selected", "filtered"):
            raise ValueError("请选择指定记录或按条件清理")
        if mode == "selected":
            if not isinstance(keys, list) or not keys or len(keys) > 200 or any(not isinstance(k, str) or not k for k in keys):
                raise ValueError("请选择 1～200 条已结束记录")
            keys = set(keys)
        else:
            if not isinstance(states, list) or not states or any(s not in TERMINAL for s in states):
                raise ValueError("请选择成功、失败或已取消的记录状态")
            try:
                number = float(days)
            except (TypeError, ValueError):
                raise ValueError("保留天数必须是 0～36500 的整数") from None
            if not number.is_integer() or not 0 <= number <= 36500:
                raise ValueError("保留天数必须是 0～36500 的整数")
            cutoff = (time.time() if now is None else now) - int(number) * 86400
        result = dict(kind="history", status="completed", deleted=0, skipped=0, items=[])
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = [self.decode(row) for row in db.execute("SELECT * FROM jobs")]
            batches = {row["payload"].get("task", {}).get("transfer_batch_id") for row in rows if row["state"] not in TERMINAL}
            batches.discard(None)
            batches.discard("")
            found = set()
            for row in rows:
                if mode == "selected":
                    if row["id"] not in keys:
                        continue
                    found.add(row["id"])
                elif row["state"] not in states or row["updated"] > cutoff:
                    continue
                batch = row["payload"].get("task", {}).get("transfer_batch_id")
                if row["state"] not in TERMINAL or (row["state"] == "completed" and batch in batches):
                    result["skipped"] += 1
                    if len(result["items"]) < 50:
                        result["items"].append(f"{row['id']}：任务未结束，或同批次仍需此记录恢复")
                    continue
                db.execute("DELETE FROM jobs WHERE id=? AND state IN ('completed','cancelled','failed')", (row["id"],))
                result["deleted"] += 1
            if mode == "selected":
                result["skipped"] += len(keys - found)
        return result

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

    def claim(self, now=None, manual=False):
        now = time.time() if now is None else now
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            states = "('upload_queued')" if manual else "('queued','waiting')"
            row = db.execute(f"SELECT * FROM jobs WHERE ready=1 AND state IN {states} "
                             "AND next_at<=? ORDER BY next_at,created LIMIT 1", (now,)).fetchone()
            if row is None:
                return None
            payload = json.loads(row["payload"])
            payload.setdefault("auto_attempts", row["attempts"])
            if not manual:
                payload["auto_attempts"] = payload.get("auto_attempts", row["attempts"]) + 1
            db.execute("UPDATE jobs SET state=?, attempts=attempts+1, payload=?, updated=? WHERE id=?",
                       ("uploading" if manual else "running", json.dumps(payload), now, row["id"]))
            return self.decode(db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())

    def recover(self):
        with self.connect() as db:
            db.execute("UPDATE jobs SET state='waiting',next_at=?,ready=1 WHERE state='running'",
                       (time.time(),))
            # Never resume sending file contents without a new manual action.
            db.execute("UPDATE jobs SET state='paused',ready=1,message=? WHERE state='uploading'",
                       ("强制上传被中断，请点强制上传继续；将先核对远端及已保存的上传进度",))

    def command(self, key, action):
        """Do not race an in-flight upload or completion callback."""
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE id=?", (key,)).fetchone()
            if not row:
                raise KeyError(key)
            if row["state"] in ("running", "uploading"):
                raise ValueError("任务正在执行，请在本轮完成后操作")
            if action == "resume" and row["state"] == "paused":
                state = "waiting"
                payload = json.loads(row["payload"])
                payload["wait_since"] = time.time()
                payload["auto_attempts"] = 0
                db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), key))
            elif action == "upload" and row["state"] in ("queued", "waiting", "paused"):
                state = "upload_queued"
                payload = json.loads(row["payload"])
                payload["upload_origin"] = "manual"
                db.execute("UPDATE jobs SET payload=? WHERE id=?", (json.dumps(payload), key))
            elif action == "pause" and row["state"] in ("queued", "waiting", "upload_queued"):
                state = "paused"
            elif action == "cancel" and row["state"] in ("queued", "waiting", "paused", "upload_queued"):
                state = "cancelled"
            else:
                raise ValueError("当前状态不支持此操作")
            db.execute("UPDATE jobs SET state=?,message=?,next_at=?,updated=? WHERE id=?",
                       (state, {"waiting": "等待秒传", "paused": "已暂停", "cancelled": "已取消",
                                "upload_queued": "已安排手动处理：先秒传，未命中则普通上传"}[state],
                        time.time(), time.time(), key))
        return self.get(key)
