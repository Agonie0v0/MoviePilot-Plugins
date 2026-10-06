"""Maintenance must preserve media, in-flight jobs and durable recovery state."""
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path


class MaintenanceV2Tests(unittest.TestCase):
    generation = "v2"

    def setUp(self):
        name = "maintenance_test_" + self.generation
        if name not in sys.modules:
            package = types.ModuleType(name)
            package.__path__ = [str(Path(__file__).resolve().parents[1] / f"plugins.{self.generation}" / "p115instantwait")]
            sys.modules[name] = package
        self.store_module = importlib.import_module(name + ".store")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = self.store_module.QueueStore(Path(self.temp.name) / "queue.db")

    def job(self, state, name="A", batch=None):
        row, _ = self.store.enqueue(name, {"final_path": f"/library/{name}.mkv",
            "task": {"transfer_batch_id": batch, "fileitem": {"path": f"/{name}.mkv"}}})
        self.store.update(row["id"], state=state)
        return self.store.get(row["id"])

    def test_selected_history_only_removes_terminal_jobs_and_preserves_active_after_restart(self):
        done = self.job("completed", "done")
        states = ("queued", "waiting", "running", "paused", "upload_queued", "uploading", "finalizing")
        active = [self.job(state, state) for state in states]
        result = self.store.clear_history(keys=[done["id"], done["id"], "missing"] + [r["id"] for r in active])
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["skipped"], len(active) + 1)
        self.assertIsNone(self.store.get(done["id"]))
        for row in active:
            self.assertEqual(self.store.get(row["id"])["state"], row["state"])
        reopened = self.store_module.QueueStore(self.store.path)
        self.assertEqual(len(reopened.all(active=True)), len(active))

    def test_filtered_history_uses_end_time_and_selected_states(self):
        old = self.job("completed", "old")
        recent = self.job("completed", "recent")
        failed = self.job("failed", "failed")
        with self.store.connect() as db:
            db.execute("UPDATE jobs SET updated=? WHERE id IN (?,?)", (100, old["id"], failed["id"]))
            db.execute("UPDATE jobs SET updated=? WHERE id=?", (200000, recent["id"]))
        result = self.store.clear_history(mode="filtered", states=["completed"], days=1, now=200000)
        self.assertEqual(result["deleted"], 1)
        self.assertIsNone(self.store.get(old["id"]))
        self.assertIsNotNone(self.store.get(recent["id"]))
        self.assertIsNotNone(self.store.get(failed["id"]))

    def test_completed_batch_peer_is_retained_until_unfinished_peer_ends(self):
        done = self.job("completed", "done", "batch")
        peer = self.job("paused", "peer", "batch")
        self.assertEqual(self.store.clear_history(keys=[done["id"]])["skipped"], 1)
        self.store.update(peer["id"], state="cancelled")
        self.assertEqual(self.store.clear_history(keys=[done["id"]])["deleted"], 1)

    def test_invalid_filters_do_not_delete_any_history(self):
        row = self.job("completed")
        bad = [dict(mode="all"), dict(keys=[]), dict(keys="id"),
               dict(mode="filtered", states=["paused"], days=0),
               dict(mode="filtered", states=[], days=0)]
        bad += [dict(mode="filtered", states=["completed"], days=days) for days in (-1, .5, "nan", "inf", None)]
        for kwargs in bad:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.store.clear_history(**kwargs)
            self.assertIsNotNone(self.store.get(row["id"]))

    def test_filtered_cleanup_includes_history_beyond_display_limit(self):
        for index in range(205):
            self.job("cancelled", str(index))
        self.assertEqual(len(self.store.all()), 200)
        self.assertEqual(self.store.clear_history(mode="filtered", states=["cancelled"], days=0)["deleted"], 205)
        self.assertEqual(self.store.all(), [])
class MaintenanceV3Tests(MaintenanceV2Tests):
    generation = "v3"
