"""Direct uploads must never manufacture temporary cloud paths or overwrite peers."""
import importlib
import sys
import tempfile
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock


class DirectV2Tests(unittest.TestCase):
    generation = "v2"

    def setUp(self):
        name = "direct_test_" + self.generation
        package = types.ModuleType(name)
        package.__path__ = [str(Path(__file__).resolve().parents[1] / f"plugins.{self.generation}/p115instantwait")]
        sys.modules[name] = package
        self.remote = importlib.import_module(name + ".remote")
        self.store_class = importlib.import_module(name + ".store").QueueStore
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "source.mkv"
        self.path.write_bytes(b"media")
        self.store = self.store_class(Path(self.temp.name) / "queue.db")
        self.data = dict(layout="direct", final_path="/library/Film.mkv",
                         hashes=self.remote.hash_file(self.path, threading.Event()))
        self.row, _ = self.store.enqueue(str(self.path), self.data)
        self.visible = None
        self.api = Mock()
        self.api.raw_path.side_effect = lambda _: self.visible
        self.api.raw_id.return_value = {"sha1": self.data["hashes"]["sha1"]}
        self.api.get_folder.return_value = types.SimpleNamespace(fileid="10", path="/library")
        self.api.verify.return_value = types.SimpleNamespace(fileid="123", path=self.data["final_path"])

    def prepare(self, force=False):
        return self.remote.prepare_direct(self.api, self.store, self.row, self.path, self.data, force)

    def hit(self, *args):
        self.visible = {"file_id": "123"}
        return "123"

    def test_instant_writes_final_name_and_folder_without_move_or_delete(self):
        self.api.instant.side_effect = self.hit
        self.prepare()
        self.assertEqual(self.api.get_folder.call_count, 1)
        self.assertEqual(str(self.api.get_folder.call_args.args[0]), "/library")
        self.assertEqual(self.api.instant.call_args.args[2], "Film.mkv")
        self.api.move_id.assert_not_called()
        self.api.delete.assert_not_called()
        self.assertEqual(self.store.get(self.row["id"])["payload"]["remote_id"], "123")

    def test_definite_miss_can_retry_without_other_cloud_mutations(self):
        self.api.instant.side_effect = self.remote.NotInstant("miss")
        for _ in range(2):
            with self.assertRaises(self.remote.NotInstant):
                self.prepare()
        self.assertEqual(self.api.instant.call_count, 2)
        self.assertNotIn("write_intent", self.store.get(self.row["id"])["payload"])
        self.api.upload.assert_not_called()

    def test_existing_target_is_not_adopted_even_with_same_hash(self):
        self.visible = {"file_id": "123"}
        with self.assertRaises(self.remote.PauseTask):
            self.prepare(True)
        self.api.instant.assert_not_called()
        self.api.upload.assert_not_called()
        self.api.get_folder.assert_not_called()

    def test_lost_init_response_recovers_hash_without_reupload(self):
        def lost(*args):
            self.hit()
            raise self.remote.RetryLater("lost response")
        self.api.instant.side_effect = lost
        with self.assertRaises(self.remote.RetryLater):
            self.prepare()
        self.prepare()
        self.assertEqual(self.api.instant.call_count, 1)

    def test_uncertain_init_with_missing_target_never_repeats(self):
        self.api.instant.side_effect = self.remote.RetryLater("lost response")
        with self.assertRaises(self.remote.RetryLater):
            self.prepare()
        with self.assertRaises(self.remote.PauseTask):
            self.prepare(True)
        self.assertEqual(self.api.instant.call_count, 1)

    def test_unowned_recovery_requires_hash_or_exact_saved_upload_identity(self):
        self.data["write_intent"] = True
        self.visible = {"file_id": "123"}
        for info in ({}, {"sha1": "different"}, {"pick_code": "other"}):
            self.api.raw_id.return_value = info
            with self.subTest(info=info), self.assertRaises(self.remote.PauseTask):
                self.prepare(True)
        self.data["upload_session"] = {"pick_code": "owned"}
        self.api.raw_id.return_value = {"pick_code": "owned"}
        self.prepare(True)
        self.api.upload.assert_not_called()
        self.api.instant.assert_not_called()

    def test_confirmed_invisible_target_is_never_uploaded_twice(self):
        self.data.update(write_intent=True, instant_confirmed=True)
        with self.assertRaises(self.remote.RetryLater):
            self.prepare(True)
        self.api.instant.assert_not_called()
        self.api.upload.assert_not_called()

    def test_saved_id_cannot_follow_a_replaced_target(self):
        self.data["remote_id"] = "123"
        self.visible = {"file_id": "999"}
        with self.assertRaises(self.remote.PauseTask):
            self.prepare()
        self.api.verify.assert_not_called()

    def test_saved_upload_cannot_be_retargeted_to_recreated_directory(self):
        self.data.update(write_intent=True, target_folder_id="old", upload_session={"upload_id": "same"})
        with self.assertRaises(self.remote.PauseTask):
            self.prepare(True)
        self.api.upload.assert_not_called()
        self.api.instant.assert_not_called()

    def test_native_deletion_progress_survives_restart_and_is_not_repeated(self):
        self.data["native_deletions"] = [{"item": dict(type="file", fileid="42", path="/library/Film.mkv")}]
        self.store.update(self.row["id"], payload=self.data)
        self.row = self.store.get(self.row["id"])
        self.remote.apply_native_deletions(self.api, self.store, self.row, lambda **kw: types.SimpleNamespace(**kw))
        self.assertTrue(self.store.get(self.row["id"])["payload"]["native_deletions"][0]["deleted"])
        reopened = self.store_class(self.store.path)
        self.remote.apply_native_deletions(self.api, reopened, reopened.get(self.row["id"]),
                                          lambda **kw: types.SimpleNamespace(**kw))
        self.assertEqual(self.api.delete.call_count, 1)

    def test_legacy_directory_cannot_be_used_as_a_new_upload_target(self):
        for part in (".mp115-staging", ".mp115-backups"):
            self.data["final_path"] = f"/library/{part}/Film.mkv"
            with self.subTest(part=part), self.assertRaises(self.remote.PauseTask):
                self.prepare(True)
        self.api.get_folder.assert_not_called()
        self.api.instant.assert_not_called()

    def test_manual_upload_uses_final_folder_and_recovers_saved_session(self):
        self.data.update(write_intent=True, upload_session={"upload_id": "same", "pick_code": "pick"})
        def upload(*args):
            self.visible = {"file_id": "123"}
            self.data["upload_confirmed"] = True
            args[-1]()
        self.api.upload.side_effect = upload
        self.prepare(True)
        self.assertEqual(self.api.upload.call_args.args[1].path, "/library")
        self.assertIsNone(self.api.upload.call_args.args[3])
        self.api.instant.assert_not_called()

    def test_same_target_reservation_survives_restart_and_is_atomic(self):
        other, _ = self.store.enqueue("other", self.data)
        barrier = threading.Barrier(2)
        def reserve(row):
            barrier.wait()
            return self.store.reserve_target(row["id"], self.data["final_path"])
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(reserve, [self.row, other]))
        self.assertEqual(sorted(results), [False, True])
        owner = self.row if results[0] else other
        loser = other if results[0] else self.row
        reopened = self.store_class(self.store.path)
        self.assertTrue(reopened.reserve_target(owner["id"], self.data["final_path"]))
        self.assertFalse(reopened.reserve_target(loser["id"], self.data["final_path"]))
        self.store.update(owner["id"], state="cancelled")
        self.assertTrue(reopened.reserve_target(loser["id"], self.data["final_path"]))


class DirectV3Tests(DirectV2Tests):
    generation = "v3"
