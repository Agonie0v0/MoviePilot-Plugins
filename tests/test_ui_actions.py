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
        confirmations = [n for n in walk([row]) if n.get("props", {}).get("class") == "p115-confirm"]
        self.assertEqual(len(confirmations), 2)
        for confirmation in confirmations:
            self.assertFalse(confirmation.get("props", {}).get("open", False))
            self.assertNotIn("events", confirmation["content"][0])
            self.assertTrue(any(n.get("events", {}).get("click", {}).get("method") == "POST"
                                for n in walk(confirmation["content"][1:])))
