import os
import tempfile
import threading
import time
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from tests.support import remote, store


class Fake115:
    def __init__(self, harness):
        self.h = harness
        self.files = {}
        self.inits = 0
        self.moves = []
        self.hit = False
        self.entered, self.release = threading.Event(), threading.Event()
        self.block = False
        self.fail_move = False

    def close(self):
        pass

    def get_folder(self, path):
        path = str(path).replace("\\", "/")
        return self.h.FileItem(storage="u115", type="dir", path=path, fileid="10", name=PurePosixPath(path).name)

    def get_item(self, path):
        path = str(path).replace("\\", "/")
        return next((f.model_copy() for f in self.files.values() if f.path == path), None)

    get_item_strict = get_item

    def raw_path(self, path):
        item = self.get_item(path)
        return {"file_id": item.fileid} if item else None

    def instant(self, path, folder, name, hashes):
        self.inits += 1
        if self.block:
            self.entered.set()
            if not self.release.wait(10):
                raise remote.RetryLater("test timeout")
        if not self.hit:
            raise remote.NotInstant("未命中秒传")
        item = self.h.FileItem(storage="u115", type="file", fileid="123",
            path=str(PurePosixPath(folder.path) / name), name=name, basename=PurePosixPath(name).stem,
            extension=PurePosixPath(name).suffix.lstrip("."), size=hashes["fingerprint"]["size"])
        self.files[item.fileid] = item
        return item.fileid

    def verify(self, file_id, path, hashes):
        item = self.files.get(str(file_id))
        if not item:
            raise remote.RetryLater("文件尚不可见")
        if item.size != hashes["fingerprint"]["size"]:
            raise remote.PauseTask("大小错误")
        return item.model_copy(update={"path": str(path)})

    def move_id(self, file_id, folder, name):
        if self.fail_move and ".mp115-backups" not in folder.path:
            raise remote.RetryLater("移动响应丢失")
        self.moves.append((str(file_id), folder.path))
        self.files[str(file_id)] = self.files[str(file_id)].model_copy(update={
            "path": str(PurePosixPath(folder.path) / name), "name": name})


@unittest.skipUnless(os.environ.get("MP115_REFERENCE"), "Set MP115_REFERENCE to test against actual MP V2 source")
class MoviePilotTests(unittest.TestCase):
    def setUp(self):
        from tests.mp_harness import Harness
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.h = Harness(self.root)
        self.engine = self.new_engine()
        self.api = Fake115(self.h)
        self.api_patch = patch.object(self.h.bridge, "OpenAPI", lambda *args: self.api)
        self.api_patch.start()

    def new_engine(self):
        engine = self.h.bridge.InstantWaitEngine(self.root / "plugin", {
            "extensions": "mkv,mp4", "retry_delays": [60, 180, 600], "max_wait_hours": 24})
        with patch.object(threading.Thread, "start", lambda _: None):
            engine.install()
        engine.worker = None
        return engine

    def tearDown(self):
        self.api_patch.stop()
        self.engine.stop()
        self.h.close()
        self.temp.cleanup()

    def enroll(self, name="A.mkv", mode="copy", target="u115"):
        path = self.root / name
        path.write_bytes(b"test video content")
        task = self.h.task(path, target=target, mode=mode)
        if target == "local":
            task.target_path = self.root / "local_library"
            task.target_directory.library_path = str(task.target_path)
        self.h.chain.jobview.add_task(task)
        self.h.chain._TransferChain__register_scrape_batch_task(task)
        self.h.chain._TransferChain__handle_transfer(task,
            callback=self.h.chain._TransferChain__default_callback)
        return task

    def attempt(self, key):
        self.engine.store.update(key, next_at=0)
        self.engine.execute(self.engine.store.claim())

    def test_wait_is_failed_history_without_failure_events_or_success_side_effects(self):
        task = self.enroll()
        row = self.engine.store.active_for(self.h.bridge.source_key(task.fileitem))
        history = self.h.Oper().get(row["history_id"])
        self.assertFalse(history.status)
        self.assertIn("等待秒传", history.errmsg)
        self.assertTrue(Path(task.fileitem.path).exists())
        self.assertEqual(self.h.normal_uploads, 0)
        self.assertEqual(self.h.notifications, [])
        self.assertFalse(any(kind in ("TransferFailed", "TransferComplete") for kind, _ in self.h.events))
        self.assertFalse(self.h.chain.jobview.is_done(task))
        self.assertIn(task.fileitem.path, self.h.chain._scrape_batches[task.transfer_batch_id]["pending"])

    def test_retry_success_updates_same_native_history_id(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        history_id = row["history_id"]
        self.attempt(row["id"])
        self.assertFalse(self.h.Oper().get(history_id).status)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "waiting")
        self.api.hit = True
        self.attempt(row["id"])
        finished = self.engine.store.get(row["id"])
        self.assertEqual(finished["state"], "completed", finished["message"])
        self.assertEqual(finished["history_id"], history_id)
        self.assertTrue(self.h.Oper().get(history_id).status)
        self.assertEqual(self.h.db.query(self.h.History).count(), 1)
        self.assertEqual(sum(kind == "TransferComplete" for kind, _ in self.h.events), 1)
        self.assertTrue(Path(task.fileitem.path).exists())

    def test_waiting_video_does_not_block_other_local_transfer(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit, self.api.block = True, True
        thread = threading.Thread(target=self.attempt, args=(row["id"],))
        thread.start()
        try:
            self.assertTrue(self.api.entered.wait(3))
            start = time.monotonic()
            other = self.enroll("B.mkv", target="local")
            self.assertLess(time.monotonic() - start, 2)
            self.assertTrue(self.h.Oper().get_by_src(other.fileitem.path, "local").status)
            self.assertFalse(self.h.Oper().get(row["history_id"]).status)
            self.assertEqual(self.h.local_calls, 1)
        finally:
            self.api.release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_batch_does_not_scrape_until_waiting_peer_finishes(self):
        self.enroll("A.mkv")
        self.enroll("B.mkv")
        rows = {r["payload"]["task"]["fileitem"]["name"]: r for r in self.engine.store.all()}
        self.h.chain._TransferChain__close_scrape_batch("batch")
        self.api.hit = True
        self.attempt(rows["B.mkv"]["id"])
        self.assertFalse(any(kind == "MetadataScrape" for kind, _ in self.h.events))
        # Every upload must have a distinct id in this two-file fake.
        self.api.files["124"] = self.api.files.pop("123").model_copy(update={"fileid": "124"})
        self.attempt(rows["A.mkv"]["id"])
        scrapes = [p for kind, p in self.h.events if kind == "MetadataScrape"]
        self.assertEqual(len(scrapes), 1)
        self.assertEqual(len(scrapes[0]["file_list"]), 2)

    def test_move_keeps_source_until_native_success_record_exists(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.attempt(row["id"])
        self.assertTrue(Path(task.fileitem.path).exists())
        self.api.hit = True
        self.attempt(row["id"])
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertFalse(Path(task.fileitem.path).exists())

    def test_overwrite_preserves_old_file_while_waiting_then_backs_it_up(self):
        self.enroll()
        row = self.engine.store.all()[0]
        old = self.h.FileItem(storage="u115", type="file", fileid="old", path="/library/A.mkv", name="A.mkv", size=10)
        self.api.files["old"] = old
        self.attempt(row["id"])
        self.assertEqual(self.api.files["old"].path, old.path)
        self.api.hit = True
        self.attempt(row["id"])
        self.assertIn(".mp115-backups", self.api.files["old"].path)
        self.assertEqual(self.api.files["123"].path, "/library/A.mkv")
        self.assertEqual(self.api.moves[0][0], "old")
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_failed_promotion_keeps_non_success_record_and_source(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.api.hit, self.api.fail_move = True, True
        self.attempt(row["id"])
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(Path(task.fileitem.path).exists())
        self.assertEqual(self.engine.store.get(row["id"])["state"], "waiting")
        self.api.fail_move = False
        self.attempt(row["id"])
        self.assertEqual(self.api.inits, 1)
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_instant_hit_without_visible_id_does_not_resubmit_upload(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit = True
        real = self.api.instant
        saved = {}
        def delayed(*args):
            real(*args)
            saved.update(self.api.files)
            self.api.files.clear()
            return None
        with patch.object(self.api, "instant", side_effect=delayed):
            self.attempt(row["id"])
            self.attempt(row["id"])
        self.assertEqual(self.api.inits, 1)
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.api.files.update(saved)
        self.attempt(row["id"])
        self.assertEqual(self.api.inits, 1)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")

    def test_changed_source_pauses_without_attempting_upload(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        Path(task.fileitem.path).write_bytes(b"different content")
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertEqual(self.api.inits, 0)
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)

    def test_source_changed_just_before_hashing_is_not_uploaded(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        real_hash = remote.hash_file
        def changed(path, stop):
            Path(path).write_bytes(b"replaced just before hash")
            return real_hash(path, stop)
        with patch.object(self.h.bridge, "hash_file", side_effect=changed):
            self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertEqual(self.api.inits, 0)
        self.assertTrue(Path(task.fileitem.path).exists())

    def test_source_change_before_recovered_callback_preserves_file_and_failed_history(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.api.hit = True
        real = self.engine.original_callback
        self.engine.original_callback = lambda *args: (_ for _ in ()).throw(remote.RetryLater("interrupted"))
        self.attempt(row["id"])
        self.engine.original_callback = real
        Path(task.fileitem.path).write_bytes(b"new source after upload")
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(Path(task.fileitem.path).read_bytes(), b"new source after upload")
        self.assertFalse(any(kind == "TransferComplete" for kind, _ in self.h.events))

    def test_native_never_overwrite_preserves_both_source_and_existing_target(self):
        self.enroll()
        row = self.engine.store.all()[0]
        task = self.engine.live_tasks[row["id"]]
        task.target_directory.overwrite_mode = "never"
        self.api.files["old"] = self.h.FileItem(storage="u115", type="file", fileid="old",
            path="/library/A.mkv", name="A.mkv", size=10)
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "failed")
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.api.files["old"].path, "/library/A.mkv")
        self.assertTrue(Path(task.fileitem.path).exists())
        self.assertEqual(self.api.moves, [])

    def test_native_directory_and_rename_plan_survives_restart(self):
        path = self.root / "original.mkv"
        path.write_bytes(b"video")
        task = self.h.task(path)
        task.target_directory.renaming = True
        task.library_type_folder = True
        self.h.chain.jobview.add_task(task)
        self.h.chain._TransferChain__register_scrape_batch_task(task)
        self.h.chain._TransferChain__handle_transfer(task, self.h.chain._TransferChain__default_callback)
        row = self.engine.store.all()[0]
        self.assertEqual(row["payload"]["final_path"], "/library/电影/Film/Film.mkv")
        self.engine.stop()
        self.engine = self.new_engine()
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertEqual(self.api.files["123"].path, "/library/电影/Film/Film.mkv")
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_same_named_remote_directory_pauses_without_replacing_it(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        self.api.files["old"] = self.h.FileItem(storage="u115", type="dir", fileid="old",
            path="/library/A.mkv", name="A.mkv")
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(Path(task.fileitem.path).exists())
        self.assertEqual(self.api.moves, [])

    def test_restart_recovers_result_without_reupload_or_duplicate_completion_event(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit = True
        real = self.engine.original_callback
        def interrupted(chain, task, info):
            real(chain, task, info)
            raise remote.RetryLater("模拟回调提交后中断")
        self.engine.original_callback = interrupted
        self.attempt(row["id"])
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "waiting")
        self.engine.stop()
        self.h.chain.jobview = self.h.JobManager()
        self.h.chain._scrape_batches = {}
        self.engine = self.new_engine()
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertEqual(self.api.inits, 1)
        self.assertEqual(sum(kind == "TransferComplete" for kind, _ in self.h.events), 1)
        self.assertEqual(self.h.db.query(self.h.History).count(), 1)

    def test_cancel_keeps_source_and_finishes_batch_as_failure(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        self.engine.control(row["id"], "cancel")
        self.assertTrue(Path(task.fileitem.path).exists())
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertIsNone(self.engine.store.claim())
        self.assertTrue(self.h.chain.jobview.is_done(task))

    def test_checkpoint_does_not_mark_success_if_remote_file_disappeared(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit = True
        def interrupted_before_history(chain, task, info):
            raise remote.RetryLater("模拟历史提交前中断")
        self.engine.original_callback = interrupted_before_history
        self.attempt(row["id"])
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(self.engine.store.get(row["id"])["payload"]["result"])
        self.api.files.clear()
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)


if __name__ == "__main__":
    unittest.main()
