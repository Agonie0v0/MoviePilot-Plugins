"""The visible controls must match durable task permissions, without mutations."""
import copy
import tempfile
import time
import unittest
from pathlib import Path

from tests.support import load

ui = load("ui")
store = load("store")


def walk(nodes):
    for node in nodes:
        yield node
        yield from walk(node.get("content", []))


class TaskActionTests(unittest.TestCase):
    def test_update_notice_disables_actions_without_showing_plugin_error(self):
        from tests.test_records import modules
        task = dict(id="task", history_id=1, source="/film.mkv", target="/library/film.mkv",
                    state="paused", attempts=1, next_at=0, message="等待处理")
        for generation in ("v2", "v3"):
            with self.subTest(generation=generation):
                _, _, page_ui = modules(generation)
                nodes = list(walk(page_ui.task_page([task], False, notice="等待任务结束后自动更新。")))
                self.assertTrue(any(n.get("props", {}).get("role") == "status" for n in nodes))
                self.assertFalse(any(n.get("props", {}).get("type") == "error" for n in nodes))
                self.assertFalse(any(n.get("events", {}).get("click", {}).get("method") == "POST" for n in nodes))
                self.assertFalse(any(n.get("text") == "插件已关闭" for n in nodes))

    def test_controls_match_queue_permissions_and_keep_reading_side_effect_free(self):
        expected = {"queued": {"pause", "upload", "cancel"}, "waiting": {"pause", "upload", "cancel"},
                    "paused": {"resume", "upload", "cancel"}, "upload_queued": {"pause", "cancel"},
                    "running": set(), "uploading": set(), "completed": set(), "failed": set(), "cancelled": set()}
        for state, actions in expected.items():
            with self.subTest(state=state), tempfile.TemporaryDirectory() as temp:
                db = store.QueueStore(Path(temp) / "queue.db")
                row, _ = db.enqueue("local:film", {"task": {"fileitem": {"path": "/film.mkv"}}, "final_path": "/library/film.mkv"})
                db.update(row["id"], state=state)
                before = copy.deepcopy(db.get(row["id"]))
                task = load("records").task_record(before)
                page = ui.task_page([task], True, max_retries=3)
                emitted = {n["events"]["click"]["api"].rsplit("/", 1)[1] for n in walk(page)
                           if n.get("events", {}).get("click", {}).get("method") == "POST"}
                self.assertEqual(emitted, actions)
                self.assertEqual(db.get(row["id"]), before)
                for action in actions:
                    db.update(row["id"], state=state)
                    self.assertIn(db.command(row["id"], action)["state"], ui.ACTIVE | {"cancelled"})
                disabled_page = ui.task_page([task], False)
                self.assertFalse(any(n.get("events", {}).get("click", {}).get("method") == "POST" for n in walk(disabled_page)))

    def test_upload_and_cancel_require_opening_a_confirmation(self):
        task = dict(id="task-1", history_id=10, source="/film.mkv", target="/library/film.mkv",
                    state="paused", attempts=4, next_at=time.time(), message="达到重试上限")
        row = ui.task_row(task, True, time.time(), 3)
        confirmations = [n for n in walk([row]) if "p115-confirm" in n.get("props", {}).get("class", "").split()]
        self.assertEqual(len(confirmations), 2)
        for confirmation in confirmations:
            self.assertFalse(confirmation.get("props", {}).get("open", False))
            self.assertNotIn("events", confirmation["content"][0])
            self.assertTrue(any(n.get("events", {}).get("click", {}).get("method") == "POST"
                                for n in walk(confirmation["content"][1:])))

    def test_filters_and_bulk_snapshots_match_actual_states(self):
        tasks = [dict(id=state, history_id=1, source=f"/{state}.mkv", target="/library/film.mkv",
                      state=state, attempts=4, next_at=0) for state in ui.LABELS]
        page = ui.task_page(tasks, True, max_retries=3)
        nodes = list(walk(page))
        rows = [n for n in nodes if n.get("props", {}).get("class") == "p115-task"]
        self.assertEqual(len(rows), len(tasks))
        self.assertEqual(sum("attention" in n["props"]["data-category"].split() for n in rows), 2)
        self.assertEqual(sum("ended" in n["props"]["data-category"].split() for n in rows), 3)
        events = {n["events"]["click"]["api"].rsplit("/", 1)[1]: n["events"]["click"]["params"]["keys"]
                  for n in nodes if "/batch/" in n.get("events", {}).get("click", {}).get("api", "")}
        self.assertEqual(events["resume"], ["paused"])
        self.assertEqual(set(events["upload"]), {"queued", "waiting", "paused"})
        radios = [n["props"] for n in nodes if n["component"] == "input"]
        self.assertEqual(sum(r["checked"] for r in radios), 1)
        self.assertEqual({r["value"] for r in radios}, {"all", "waiting", "attention", "transfer", "ended"})

    def test_cooldown_banner_density_toggle_and_confirm_layout(self):
        import json
        from tests.test_records import modules
        now = time.time()
        tasks = [
            dict(id=f"p{i}", history_id=i, source=f"/downloads/Series.S01/Ep0{i}.mkv",
                 target=f"/library/Ep0{i}.mkv", state="paused", attempts=2, next_at=now + 600,
                 size=2 * 1024 ** 3, message="达到上限")
            for i in (1, 2)
        ] + [
            dict(id="w1", history_id=3, source="/downloads/Series.S01/Ep03.mkv",
                 target="/library/Ep03.mkv", state="waiting", attempts=1, next_at=now + 600,
                 size=1024 ** 3, message="等待重试")
        ]
        for generation in ("v2", "v3"):
            with self.subTest(generation=generation):
                _, _, page_ui = modules(generation)
                self.assertNotIn("-92px", page_ui.STYLES)
                self.assertIn("p115-reveal", page_ui.STYLES)
                self.assertIn("p115-shimmer", page_ui.STYLES)
                page = page_ui.task_page(tasks, True, max_retries=3, cooldown_until=now + 600)
                nodes = list(walk(page))
                dumped = json.dumps(page, ensure_ascii=False)
                self.assertTrue(any(n.get("props", {}).get("class") == "p115-cooldown-banner" for n in nodes))
                self.assertIn("风控冷却至", dumped)
                self.assertTrue(any(n.get("props", {}).get("class") == "p115-density-toggle" for n in nodes))
                self.assertTrue(any(n.get("props", {}).get("class") == "p115-quick-bulk" for n in nodes))
                self.assertEqual(len([n for n in nodes if n["component"] == "input"]), 5)
                disabled_nodes = list(walk(page_ui.task_page(tasks, False, cooldown_until=now + 600)))
                self.assertFalse(any(n.get("props", {}).get("class") == "p115-cooldown-banner" for n in disabled_nodes))
                label, value, _ = page_ui.schedule_summary({"state": "waiting", "next_at": now + 900}, True, now)
                self.assertEqual(label, "下次重试 · MP 时间")
                self.assertIn("分钟后", value)
