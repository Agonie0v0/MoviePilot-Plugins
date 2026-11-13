"""Full-history counts must not depend on the bounded preview."""
import ast
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from tests.support import PLUGIN, load
from tests.test_ui_actions import walk
from tests.test_records import modules

store, records, ui = load("store"), load("records"), load("ui")


class DashboardHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.db"
        self.db = store.QueueStore(self.path)

    def job(self, name, state="completed", **flags):
        payload = dict(task={"fileitem": {"path": f"/{name}.mkv"}}, final_path=f"/library/{name}.mkv", **flags)
        row, _ = self.db.enqueue(name, payload)
        self.db.update(row["id"], state=state)
        return row["id"]

    def test_all_counts_and_wait_schedule_include_records_outside_preview(self):
        self.job("old-wait", "waiting")
        self.job("old-paused", "paused")
        self.job("old-failed", "failed")
        self.job("old-running", "running")
        self.job("old-uploading", "uploading")
        self.job("old-upload-queued", "upload_queued")
        self.job("old-cancelled", "cancelled")
        self.job("unknown", upload_requested=True)
        self.job("uploaded", instant_confirmed=True, upload_confirmed=True)
        self.job("reconciled", upload_session={"completing": True})
        for i in range(251):
            self.job(f"instant-{i}", instant_confirmed=True)
        with patch.object(self.db, "decode", wraps=self.db.decode) as decode:
            rows, summary = self.db.dashboard()
            self.assertEqual(decode.call_count, 57)
        self.assertEqual(len(rows), 57)
        self.assertEqual({r["source"] for r in rows}, {f"instant-{i}" for i in range(201, 251)} |
                         {"old-wait", "old-paused", "old-failed", "old-running", "old-uploading", "old-upload-queued", "old-cancelled"})
        self.assertEqual(summary["total"], 261)
        self.assertEqual(summary["states"]["completed"], 254)
        self.assertEqual(summary["methods"], dict(instant=251, upload=2, unknown=1))
        self.assertIsNotNone(summary["next_at"])
        page = ui.task_page([records.task_record(row) for row in rows], True, summary=summary)
        nodes = list(walk(page))
        metrics = [n for n in nodes if "p115-metric-value" in n.get("props", {}).get("class", "")]
        self.assertEqual([n["text"] for n in metrics], ["1", "3", "2", "254"])
        self.assertEqual(len([n for n in nodes if n.get("props", {}).get("class") == "p115-task"]), 57)
        self.assertTrue(any("/batch/" in n.get("events", {}).get("click", {}).get("api", "") for n in nodes))
        self.assertEqual(store.QueueStore(self.path).dashboard()[1], summary)
        tree = ast.parse((PLUGIN / "__init__.py").read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
        cls.bases = []
        cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in ("get_page", "list_tasks")]
        namespace = dict(task_record=records.task_record, task_page=Mock(return_value="page"))
        exec(compile(ast.Module(body=[cls], type_ignores=[]), "entry", "exec"), namespace)
        plugin = namespace[cls.name]()
        plugin.queue, plugin.get_state, plugin.get_data = lambda: self.db, lambda: False, lambda key: None
        plugin._error, plugin._engine = "", None
        self.assertEqual(len(plugin.list_tasks()), 50)
        self.assertEqual(plugin.get_page(), "page")
        args, kwargs = namespace["task_page"].call_args
        self.assertEqual(len(args[0]), 57)
        self.assertEqual(kwargs["summary"]["total"], 261)

    def test_only_completed_tasks_are_limited_in_both_generations(self):
        for generation in ("v2", "v3"):
            with self.subTest(generation=generation):
                record_module, store_module, ui_module = modules(generation)
                database = store_module.QueueStore(Path(self.temp.name) / f"{generation}.db")
                states = list(ui_module.LABELS)
                payload = json.dumps(dict(task={"fileitem": {"path": "/film.mkv"}}, final_path="/library/film.mkv"))
                with database.connect() as connection:
                    connection.executemany(
                        "INSERT INTO jobs(id,source,state,next_at,created,updated,payload) VALUES(?,?,?,0,?,?,?)",
                        [(f"{state}-{index:03}", f"/{state}-{index:03}.mkv", state, index, 1000-index, payload)
                         for state in states for index in range(65)])
                rows, summary = database.dashboard()
                expected = {f"{state}-{index:03}" for state in states for index in range(65)
                            if state != "completed" or index < 50}
                self.assertEqual({row["id"] for row in rows}, expected)
                self.assertEqual(len(rows), len(expected))
                self.assertEqual(summary["total"], len(states) * 65)
                page = ui_module.task_page([record_module.task_record(row) for row in rows], True, summary=summary)
                nodes = list(walk(page))
                self.assertEqual(sum(node.get("props", {}).get("class") == "p115-task" for node in nodes), len(expected))
                filters = {node["props"]["value"]: node["props"]["aria-label"] for node in nodes
                           if node["component"] == "input"}
                self.assertEqual(filters["waiting"], "等待 130 条任务")
                self.assertEqual(filters["attention"], "待处理 130 条任务")
                self.assertEqual(filters["transfer"], f"执行/上传 {260 if generation == 'v3' else 195} 条任务")

    def test_updates_cleanup_and_empty_database_have_no_stale_counts(self):
        self.assertEqual(self.db.dashboard(), ([], dict(states={}, methods={}, next_at=None, total=0)))
        key = self.job("film", "waiting", instant_confirmed=True)
        self.db.update(key, state="completed")
        self.assertEqual(self.db.dashboard()[1]["methods"], {"instant": 1})
        payload = self.db.get(key)["payload"]
        payload["upload_confirmed"] = True
        self.db.update(key, payload=payload)
        self.db.update(key, state="completed")
        self.assertEqual(self.db.dashboard()[1]["methods"], {"upload": 1})
        self.db.clear_history(keys=[key])
        self.assertEqual(self.db.dashboard()[1]["total"], 0)

    def test_legacy_database_migrates_once_without_changing_records(self):
        # Recreate the old schema to exercise upgrade, not just new installs.
        with self.db.connect() as db:
            db.execute("DROP INDEX jobs_summary")
            db.execute("ALTER TABLE jobs DROP COLUMN transfer_method")
            for i, flags in enumerate(({}, {"instant_confirmed": True}, {"upload_session": {"completing": True}})):
                db.execute("INSERT INTO jobs(id,source,state,next_at,created,updated,payload) VALUES(?,?,'completed',0,1,2,?)",
                           (str(i), str(i), json.dumps(flags)))
        upgraded = store.QueueStore(self.path)
        rows, summary = upgraded.dashboard()
        self.assertEqual(summary["methods"], dict(unknown=1, instant=1, upload=1))
        self.assertTrue(all(r["created"] == 1 and r["updated"] == 2 for r in rows))
        self.assertEqual(store.QueueStore(self.path).dashboard()[1], summary)
        with upgraded.connect() as db:
            plans = " ".join(r[3] for r in db.execute("EXPLAIN QUERY PLAN SELECT state,transfer_method,COUNT(*),MIN(next_at) FROM jobs GROUP BY state,transfer_method"))
            self.assertIn("COVERING INDEX jobs_summary", plans)
            plans = " ".join(r[3] for r in db.execute("EXPLAIN QUERY PLAN SELECT * FROM jobs ORDER BY created DESC LIMIT 50"))
            self.assertIn("jobs_created", plans)


if __name__ == "__main__":
    unittest.main()
