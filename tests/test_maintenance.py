"""Maintenance must preserve media, in-flight jobs and durable recovery state."""
import importlib
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock

import httpx


class NoSleep:
    def wait(self, delay):
        return False

    def is_set(self):
        return False


class MaintenanceV2Tests(unittest.TestCase):
    generation = "v2"

    def setUp(self):
        name = "maintenance_test_" + self.generation
        if name not in sys.modules:
            package = types.ModuleType(name)
            package.__path__ = [str(Path(__file__).resolve().parents[1] / f"plugins.{self.generation}" / "p115instantwait")]
            sys.modules[name] = package
        self.maintenance = importlib.import_module(name + ".maintenance")
        self.remote = importlib.import_module(name + ".remote")
        self.store_module = importlib.import_module(name + ".store")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = self.store_module.QueueStore(Path(self.temp.name) / "queue.db")
        self.root = "/library/.mp115-staging"
        self.calls = []

    def job(self, state, name="A", batch=None):
        row, _ = self.store.enqueue(name, {"final_path": f"/library/{name}.mkv",
            "task": {"transfer_batch_id": batch, "fileitem": {"path": f"/{name}.mkv"}}})
        self.store.update(row["id"], state=state)
        return self.store.get(row["id"])

    def api(self, replies):
        replies = iter(replies)
        def handler(request):
            self.calls.append(request)
            status, body = next(replies)
            return httpx.Response(status, json=body)
        api = self.remote.OpenAPI(SimpleNamespace(access_token="test", base_url="https://example.invalid"),
            lambda **kw: SimpleNamespace(**kw), NoSleep(), httpx.Client(transport=httpx.MockTransport(handler)))
        self.addCleanup(api.close)
        return api

    @staticmethod
    def response(data):
        return (200, {"state": True, "code": 0, "data": data})

    def test_selected_history_only_removes_terminal_jobs_and_keeps_root_after_restart(self):
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
        self.assertEqual(reopened.staging_roots(), [self.root])

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
        self.assertEqual(self.store.staging_roots(), [self.root])

    def test_delete_requires_explicit_empty_listing_and_only_deletes_folder_id(self):
        api = self.api([self.response({"file_id": "10", "file_category": "0"}),
                        self.response([]), self.response({})])
        self.assertEqual(api.remove_empty_staging_dir(self.root), "deleted")
        self.assertEqual(self.calls[-1].url.path, "/open/ufile/delete")
        self.assertEqual(self.calls[-1].content, b"file_ids=10")
        self.assertEqual(self.calls[1].url.params["show_dir"], "1")
        self.assertEqual(self.calls[1].url.params["limit"], "1")

    def test_cleanup_keeps_nonempty_and_malformed_or_failed_listing(self):
        for data in ([{"fc": "0"}], [{"fc": "1"}], None, {}, False, ""):
            self.calls.clear()
            api = self.api([self.response({"file_id": "10", "file_category": "0"}), self.response(data)])
            if isinstance(data, list):
                self.assertEqual(api.remove_empty_staging_dir(self.root), "retained")
            else:
                with self.assertRaises(self.remote.RetryLater):
                    api.remove_empty_staging_dir(self.root)
            self.assertEqual(len(self.calls), 2)
        api = self.api([self.response({"file_id": "10", "file_category": "0"}), (429, {})])
        with self.assertRaises(self.remote.RetryLater):
            api.remove_empty_staging_dir(self.root)

    def test_missing_directory_is_not_created_and_same_named_file_is_not_deleted(self):
        api = self.api([(200, {"state": False, "code": 430004})])
        self.assertEqual(api.remove_empty_staging_dir(self.root), "missing")
        api = self.api([self.response({"file_id": "10", "file_category": "1"})])
        with self.assertRaises(self.remote.RetryLater):
            api.remove_empty_staging_dir(self.root)
        self.assertTrue(all(c.url.path == "/open/folder/get_info" for c in self.calls))

    def test_roots_and_children_cannot_escape_staging_or_reach_backups(self):
        for path in ("/library", "/", "relative/.mp115-staging", "/../.mp115-staging", "/library/.mp115-backups"):
            with self.assertRaises(ValueError):
                self.maintenance.parse_roots(path)
        api = self.api([self.response({"file_id": "10", "file_category": "0"}), self.response([
            {"fc": "0", "fn": "a" * 24}, {"fc": "0", "fn": "../.mp115-backups"},
            {"fc": "0", "fn": "manual"}, {"fc": "1", "fn": "b" * 24}])])
        self.assertEqual(api.staging_children(self.root), [self.root + "/" + "a" * 24])

    def test_scan_snapshots_all_pages_before_any_deletion(self):
        rows = [{"fc": "0", "fn": f"{i:024x}"} for i in range(1001)]
        api = self.api([self.response({"file_id": "10", "file_category": "0"}),
                        self.response(rows[:1000]), self.response(rows[1000:])])
        self.assertEqual(len(api.staging_children(self.root)), 1001)
        self.assertEqual(self.calls[-1].url.params["offset"], "1000")
        self.assertFalse(any(c.url.path.endswith("/delete") for c in self.calls))

    def test_scan_skips_active_dirs_and_cleans_orphans_even_after_records_deleted(self):
        active = self.job("uploading", "active")
        done = self.job("completed", "done")
        self.store.clear_history(keys=[done["id"]])
        api = Mock()
        api.staging_children.return_value = [self.root + "/" + r["id"] for r in (active, done)]
        api.remove_empty_staging_dir.return_value = "deleted"
        stop = threading.Event()
        manager = self.maintenance.StagingMaintenance(self.store, lambda: api, stop, Mock())
        reports = []
        manager.scan([], reports.append)
        api.remove_empty_staging_dir.assert_called_once_with(self.root + "/" + done["id"])
        self.assertEqual(reports[-1]["deleted"], 1)
        self.assertEqual(reports[-1]["retained"], 2)
        self.assertEqual(reports[-1]["status"], "completed")

    def test_scan_can_be_stopped_without_deleting_remaining_directories(self):
        row = self.job("completed")
        stop = threading.Event()
        api = Mock()
        def children(root):
            stop.set()
            return [root + "/" + row["id"]]
        api.staging_children.side_effect = children
        reports = []
        self.maintenance.StagingMaintenance(self.store, lambda: api, stop, Mock()).scan([], reports.append)
        api.remove_empty_staging_dir.assert_not_called()
        self.assertEqual(reports[-1]["status"], "stopped")

    def test_scan_reports_failed_root_and_continues_other_roots(self):
        api = Mock()
        api.staging_children.side_effect = [RuntimeError("network"), []]
        api.remove_empty_staging_dir.return_value = "missing"
        reports = []
        manager = self.maintenance.StagingMaintenance(self.store, lambda: api, threading.Event(), Mock())
        manager.scan(["/a/.mp115-staging", "/b/.mp115-staging"], reports.append)
        self.assertEqual(reports[-1]["failed"], 1)
        self.assertEqual(reports[-1]["missing"], 1)
        self.assertEqual(api.staging_children.call_count, 2)

    def test_completion_cleanup_failure_does_not_modify_success(self):
        row = self.job("completed")
        api = Mock()
        api.remove_empty_staging_dir.side_effect = RuntimeError("network")
        manager = self.maintenance.StagingMaintenance(self.store, lambda: api, threading.Event(), Mock())
        manager.cleanup_completed(row, api)
        self.assertEqual(self.store.get(row["id"])["state"], "completed")


class MaintenanceV3Tests(MaintenanceV2Tests):
    generation = "v3"
