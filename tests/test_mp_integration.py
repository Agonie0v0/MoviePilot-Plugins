import os
import tempfile
import threading
import time
import unittest
from pathlib import Path, PurePosixPath
from unittest.mock import Mock, patch

from tests.support import remote, store


class Fake115:
    def __init__(self, harness):
        self.h = harness
        self.files = {}
        self.hashes = {}
        self.folders = []
        self.inits = 0
        self.uploads = 0
        self.moves = []
        self.deletes = []
        self.hit = False
        self.entered, self.release = threading.Event(), threading.Event()
        self.block = False
        self.fail_move = False

    def close(self):
        pass

    def get_folder(self, path):
        path = str(path).replace("\\", "/")
        self.folders.append(path)
        return self.h.FileItem(storage="u115", type="dir", path=path, fileid="10", name=PurePosixPath(path).name)

    def get_item(self, path):
        path = str(path).replace("\\", "/")
        item = next((f.model_copy() for f in self.files.values() if f.path == path), None)
        if not item and any(str(PurePosixPath(f.path).parent) == path for f in self.files.values()):
            return self.h.FileItem(storage="u115", type="dir", path=path, fileid="10", name=PurePosixPath(path).name)
        return item

    get_item_strict = get_item

    def raw_path(self, path):
        item = self.get_item(path)
        return {"file_id": item.fileid} if item else None

    def raw_id(self, file_id):
        return {"sha1": self.hashes.get(str(file_id))}

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
        self.hashes[item.fileid] = hashes["sha1"]
        return item.fileid

    def verify(self, file_id, path, hashes):
        item = self.files.get(str(file_id))
        if not item:
            raise remote.RetryLater("文件尚不可见")
        if item.size != hashes["fingerprint"]["size"]:
            raise remote.PauseTask("大小错误")
        return item.model_copy(update={"path": str(path)})

    def upload(self, path, folder, hashes, init, payload, checkpoint):
        self.uploads += 1
        name = PurePosixPath(payload["final_path"]).name
        item = self.h.FileItem(storage="u115", type="file", fileid="123",
            path=str(PurePosixPath(folder.path) / name), name=name,
            size=hashes["fingerprint"]["size"])
        self.files[item.fileid] = item
        self.hashes[item.fileid] = hashes["sha1"]
        payload["upload_confirmed"] = True
        checkpoint()

    def move_id(self, file_id, folder, name):
        if self.fail_move and ".mp115-backups" not in folder.path:
            raise remote.RetryLater("移动响应丢失")
        self.moves.append((str(file_id), folder.path))
        self.files[str(file_id)] = self.files[str(file_id)].model_copy(update={
            "path": str(PurePosixPath(folder.path) / name), "name": name})

    def delete(self, item):
        current = self.get_item(item.path)
        if current and str(current.fileid) != str(item.fileid):
            raise remote.PauseTask("旧目标已变化")
        self.deletes.append(str(item.fileid))
        self.files.pop(str(item.fileid), None)
        return True

    def list(self, folder):
        return [item for item in self.files.values() if PurePosixPath(item.path).parent == PurePosixPath(folder.path)]


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

    def attach_plugin_notifier(self):
        """Load the real entry point and MP's native post_message adapter."""
        import enum
        import importlib.util
        import sys
        import types
        from typing import Optional
        from tests.mp_harness import selected
        from tests.support import PLUGIN
        ns = {"Enum": enum.Enum}
        selected("app/schemas/types.py", {"NotificationType"}, ns)
        ns.update(Optional=Optional, MessageChannel=object,
                  Notification=lambda **kw: types.SimpleNamespace(**kw),
                  settings=types.SimpleNamespace(MP_DOMAIN=lambda path: path))
        selected("app/plugins/__init__.py", {"_PluginBase"}, ns,
                 methods={"post_message"}, bases="object")
        plugins = types.ModuleType("app.plugins")
        plugins._PluginBase = ns["_PluginBase"]
        fastapi = types.ModuleType("fastapi")
        fastapi.HTTPException = RuntimeError
        spec = importlib.util.spec_from_file_location("instantwait_test.plugin_entry", PLUGIN / "__init__.py")
        spec.submodule_search_locations = None
        entry = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"app.plugins": plugins, "fastapi": fastapi}), \
                patch.object(self.h.schemas, "NotificationType", ns["NotificationType"], create=True):
            spec.loader.exec_module(entry)
        plugin = entry.P115InstantWait()
        plugin.systemmessage, plugin.chain = Mock(), Mock()
        self.engine.notify = plugin._notify
        plugin.get_data = Mock(return_value=None)
        return plugin

    def test_notifications_reach_mp_channels_on_first_wait_and_retry_exhaustion(self):
        plugin = self.attach_plugin_notifier()
        self.enroll()
        row = self.engine.store.all()[0]
        self.engine.config["max_retries"] = 2
        self.attempt(row["id"])
        first = plugin.chain.post_message.call_args.args[0]
        self.assertEqual(first.mtype.value, "整理入库")
        self.assertIn("A.mkv", first.text)
        self.assertIn(f"#{row['history_id']}", first.text)
        self.assertIn(self.h.bridge.log_time(self.engine.store.get(row["id"])["next_at"]), first.text)
        self.attempt(row["id"])
        self.assertEqual(plugin.chain.post_message.call_count, 1)
        self.attempt(row["id"])
        last = plugin.chain.post_message.call_args.args[0]
        self.assertEqual(plugin.chain.post_message.call_count, 2)
        self.assertEqual(last.mtype.value, "手动处理")
        self.assertIn("自动重试次数已用完", last.text)
        self.assertIn("3/3", last.text)
        self.assertIn(row["id"], last.text)
        self.assertIn("P115InstantWait", last.link)
        self.assertEqual(plugin.systemmessage.put.call_count, 2)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.engine.restore()
        self.assertEqual(plugin.chain.post_message.call_count, 2)

    def test_batch_upload_deduplicates_and_skips_running_missing_and_finished_tasks(self):
        tasks = [self.enroll(name=f"Batch{i}.mkv") for i in range(4)]
        rows = [self.engine.store.active_for(self.h.bridge.source_key(t.fileitem)) for t in tasks]
        self.engine.store.update(rows[1]["id"], state="running")
        self.engine.store.update(rows[2]["id"], state="completed")
        self.engine.control(rows[3]["id"], "pause")
        keys = [r["id"] for r in rows] + [rows[0]["id"], "missing"]
        result = self.engine.control_many(keys, "upload")
        self.assertEqual((result["accepted"], result["skipped"], result["failed"]), (2, 3, 0))
        self.assertEqual(len(result["items"]), 5)
        self.assertEqual(self.engine.store.get(rows[1]["id"])["state"], "running")
        self.assertEqual(self.engine.store.get(rows[2]["id"])["state"], "completed")
        self.assertEqual(self.api.uploads, 0)
        self.assertEqual(self.engine.store.claim(manual=True)["id"], rows[0]["id"])
        self.assertEqual(self.engine.store.claim(manual=True)["id"], rows[3]["id"])
        self.assertIsNone(self.engine.store.claim(manual=True))

    def test_batch_resume_only_resets_paused_tasks_and_keeps_original_history(self):
        tasks = [self.enroll(name=f"Resume{i}.mkv") for i in range(2)]
        rows = [self.engine.store.active_for(self.h.bridge.source_key(t.fileitem)) for t in tasks]
        self.engine.control(rows[0]["id"], "pause")
        result = self.engine.control_many([r["id"] for r in rows], "resume")
        self.assertEqual((result["accepted"], result["skipped"]), (1, 1))
        current = self.engine.store.get(rows[0]["id"])
        self.assertEqual(current["payload"]["auto_attempts"], 0)
        self.assertEqual(current["history_id"], rows[0]["history_id"])
        self.assertFalse(self.h.Oper().get(current["history_id"]).status)

    def test_batch_isolates_unexpected_failure_and_validates_before_any_mutation(self):
        self.enroll()
        key = self.engine.store.all()[0]["id"]
        with patch.object(self.engine, "control", wraps=self.engine.control) as control:
            for keys, action in (([], "upload"), ("not-a-list", "upload"), ([key, None], "upload"),
                                 ([key] * 201, "upload"), ([key], "invalid")):
                with self.assertRaises(ValueError):
                    self.engine.control_many(keys, action)
            control.assert_not_called()
        original_get = self.engine.store.get
        def broken_get(task_id):
            if task_id == "broken":
                raise RuntimeError("private data")
            return original_get(task_id)
        with patch.object(self.engine.store, "get", side_effect=broken_get):
            result = self.engine.control_many(["broken", key], "pause")
        self.assertEqual((result["accepted"], result["failed"]), (1, 1))
        self.assertNotIn("private data", str(result))
        self.assertEqual(self.engine.store.get(key)["state"], "paused")

    def test_batch_cancel_preserves_sources_and_failed_history(self):
        tasks = [self.enroll(name=f"Cancel{i}.mkv") for i in range(2)]
        rows = [self.engine.store.active_for(self.h.bridge.source_key(t.fileitem)) for t in tasks]
        result = self.engine.control_many([r["id"] for r in rows], "cancel")
        self.assertEqual(result["accepted"], 2)
        for task, row in zip(tasks, rows):
            self.assertTrue(Path(task.fileitem.path).exists())
            self.assertFalse(self.h.Oper().get(row["history_id"]).status)
            self.assertEqual(self.engine.store.get(row["id"])["state"], "cancelled")

    def test_install_applies_batch_before_worker_start(self):
        self.enroll()
        key = self.engine.store.all()[0]["id"]
        self.engine.stop()
        engine = self.h.bridge.InstantWaitEngine(self.root / "plugin", self.engine.config)
        observed = []
        with patch.object(threading.Thread, "start", lambda _: observed.append(engine.store.get(key)["state"])):
            engine.install(before_start=lambda: engine.control_many([key], "upload"))
        self.engine = engine
        self.assertEqual(observed, ["upload_queued", "upload_queued"])

    def test_saved_batch_is_consumed_once_and_old_single_selection_still_works(self):
        import sys
        import types
        self.enroll()
        key = self.engine.store.all()[0]["id"]
        plugin = self.attach_plugin_notifier()
        plugin.get_data_path = Mock(return_value=self.root / "plugin")
        plugin.update_config = Mock(return_value=True)
        plugin.save_data = Mock()
        fake = Mock()
        fake.control_many.side_effect = self.engine.control_many
        fake.install.side_effect = lambda before_start=None: before_start() if before_start else None
        version = types.ModuleType("version")
        version.APP_VERSION = "v2.15.6"
        with patch.dict(sys.modules, {"version": version}), \
                patch.dict(plugin.init_plugin.__globals__, {"InstantWaitEngine": Mock(return_value=fake)}):
            plugin.init_plugin({"enabled": True, "task_ids": [key, key], "action": "upload", "apply_action": True})
            saved = dict(plugin.update_config.call_args.args[0])
            self.assertFalse(saved["apply_action"])
            self.assertEqual(saved["task_ids"], [])
            self.assertEqual(saved["task_id"], "")
            self.assertEqual(self.engine.store.get(key)["state"], "upload_queued")
            plugin.init_plugin(saved)
            self.assertEqual(fake.control_many.call_count, 1)
            plugin.init_plugin({"enabled": True, "task_id": key, "action": "pause", "apply_action": True})
            self.assertEqual(self.engine.store.get(key)["state"], "paused")
            plugin.init_plugin({"enabled": True, "task_ids": [], "task_id": key, "action": "upload", "apply_action": True})
            self.assertEqual(self.engine.store.get(key)["state"], "paused")
            self.assertIn("请选择", plugin._error)

    def test_batch_not_run_if_one_shot_marker_cannot_be_saved(self):
        plugin = self.attach_plugin_notifier()
        plugin.update_config = Mock(return_value=False)
        with patch.dict(plugin.init_plugin.__globals__, {"InstantWaitEngine": Mock()}) as namespace:
            plugin.init_plugin({"enabled": True, "task_ids": ["some-task"], "action": "upload", "apply_action": True})
            namespace["InstantWaitEngine"].assert_not_called()
        self.assertIn("本次未执行", plugin._error)

    def test_history_cleanup_is_local_consumed_once_and_preserves_mp_history(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit = True
        self.attempt(row["id"])
        plugin = self.attach_plugin_notifier()
        plugin.get_data_path = Mock(return_value=self.root / "plugin")
        plugin.update_config = Mock(return_value=True)
        plugin.save_data = Mock()
        with patch.object(store.QueueStore, "clear_history", autospec=True,
                          side_effect=store.QueueStore.clear_history) as cleanup:
            plugin.init_plugin({"enabled": False, "cleanup_history": True, "history_ids": [row["id"]]})
            saved = dict(plugin.update_config.call_args.args[0])
            self.assertFalse(saved["cleanup_history"])
            self.assertEqual(saved["history_ids"], [])
            plugin.init_plugin(saved)
            self.assertEqual(cleanup.call_count, 1)
        self.assertIsNone(self.engine.store.get(row["id"]))
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(self.api.files)

    def test_no_cleanup_when_one_shot_config_cannot_be_saved(self):
        plugin = self.attach_plugin_notifier()
        plugin.update_config = Mock(return_value=False)
        with patch.object(store.QueueStore, "clear_history") as cleanup:
            plugin.init_plugin({"enabled": False, "cleanup_history": True, "history_ids": ["old"]})
        cleanup.assert_not_called()
        self.assertIn("本次未执行", plugin._error)


    def test_invalid_history_filter_does_not_disable_engine_or_delete_records(self):
        import sys
        import types
        self.enroll()
        row = self.engine.store.all()[0]
        plugin = self.attach_plugin_notifier()
        plugin.get_data_path = Mock(return_value=self.root / "plugin")
        plugin.update_config = Mock(return_value=True)
        plugin.save_data = Mock()
        fake = Mock()
        version = types.ModuleType("version")
        version.APP_VERSION = "v2.15.6"
        with patch.dict(sys.modules, {"version": version}), \
                patch.dict(plugin.init_plugin.__globals__, {"InstantWaitEngine": Mock(return_value=fake)}):
            plugin.init_plugin({"enabled": True, "cleanup_history": True, "history_mode": "filtered",
                                "history_states": ["paused"], "history_days": 0})
            fake.install.assert_called_once()
        self.assertIsNotNone(self.engine.store.get(row["id"]))
        self.assertIn("记录状态", plugin._error)
        self.assertEqual(plugin.save_data.call_args.args[1]["status"], "failed")


    def test_notification_toggle_suppresses_both_local_and_push_messages(self):
        plugin = self.attach_plugin_notifier()
        plugin._config["notify"] = False
        self.engine.config["max_retries"] = 0
        self.enroll()
        self.attempt(self.engine.store.all()[0]["id"])
        plugin.systemmessage.put.assert_not_called()
        plugin.chain.post_message.assert_not_called()

    def test_failed_notification_cannot_change_pause_or_block_next_task(self):
        plugin = self.attach_plugin_notifier()
        plugin.chain.post_message.side_effect = RuntimeError("channel unavailable secret")
        self.engine.config["max_retries"] = 0
        self.enroll()
        row = self.engine.store.all()[0]
        with patch.object(self.h.bridge.logger, "warning") as warning:
            self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertIn("自动重试次数已用完", self.engine.store.get(row["id"])["message"])
        self.assertEqual(plugin.chain.post_message.call_count, 1)
        self.assertTrue(any("通知提交失败" in c.args[0] for c in warning.call_args_list))
        self.assertFalse(any("secret" in c.args[0] for c in warning.call_args_list))
        task = self.enroll(name="B.mkv")
        second = self.engine.store.active_for(self.h.bridge.source_key(task.fileitem))
        self.api.hit = True
        self.attempt(second["id"])
        self.assertEqual(self.engine.store.get(second["id"])["state"], "completed")

    def test_local_message_failure_still_dispatches_channel_notification(self):
        plugin = self.attach_plugin_notifier()
        plugin.systemmessage.put.side_effect = RuntimeError("local message store failed")
        self.engine.config["max_retries"] = 0
        self.enroll()
        self.attempt(self.engine.store.all()[0]["id"])
        plugin.chain.post_message.assert_called_once()

    def test_timeout_auth_error_and_manual_upload_error_send_actionable_notifications(self):
        plugin = self.attach_plugin_notifier()
        self.enroll()
        row = self.engine.store.all()[0]
        row["payload"]["wait_since"] = time.time() - 90000
        self.engine.store.update(row["id"], payload=row["payload"])
        self.attempt(row["id"])
        self.assertIn("等待超过设定期限", plugin.chain.post_message.call_args.args[0].text)
        self.engine.control(row["id"], "resume")
        with patch.object(self.api, "instant", side_effect=remote.PauseTask("115 授权失效")):
            self.attempt(row["id"])
        self.assertIn("授权失效", plugin.chain.post_message.call_args.args[0].text)
        self.engine.control(row["id"], "upload")
        with patch.object(self.api, "upload", side_effect=remote.RetryLater("网络连接中断")):
            self.engine.execute(self.engine.store.claim(manual=True))
        self.assertIn("手动处理未完成", plugin.chain.post_message.call_args.args[0].text)
        self.assertEqual(plugin.chain.post_message.call_count, 3)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")

    def test_user_pause_is_quiet_and_native_failure_notification_is_not_duplicated(self):
        plugin = self.attach_plugin_notifier()
        task = self.enroll()
        row = self.engine.store.all()[0]
        self.engine.control(row["id"], "pause")
        plugin.chain.post_message.assert_not_called()
        self.engine.control(row["id"], "resume")
        self.engine.finish_failure(row, task, self.h.TransferInfo(success=False, message="目标冲突"))
        self.assertEqual(len(self.h.notifications), 1)
        self.assertEqual(self.h.notifications[0]["mtype"], "Manual")
        plugin.chain.post_message.assert_not_called()

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

    def test_retry_logs_correlate_history_and_exact_next_time(self):
        with patch.object(self.h.bridge.logger, "info") as info, patch.object(self.h.bridge.logger, "warning") as warning:
            self.enroll()
            row = self.engine.store.all()[0]
            self.attempt(row["id"])
            waiting = self.engine.store.get(row["id"])
            lines = [str(call.args[0]) for call in info.call_args_list]
            self.assertTrue(any("任务入队" in line and f"整理记录={row['history_id']}" in line for line in lines))
            start = next(line for line in lines if "开始自动尝试" in line)
            deferred = next(line for line in lines if "等待下次重试" in line)
            for line in (start, deferred):
                self.assertIn(row["id"], line)
                self.assertIn(f"整理记录={row['history_id']}", line)
                self.assertIn("已尝试=1", line)
                self.assertIn("本轮自动尝试=1/4", line)
            self.assertIn(self.h.bridge.log_time(waiting["next_at"]), deferred)
            self.assertIn("未命中秒传", deferred)
            self.assertFalse(warning.called)
            self.engine.config["max_retries"] = 1
            self.attempt(row["id"])
            paused = str(warning.call_args.args[0])
            self.assertIn("自动重试次数已用完", paused)
            self.assertIn("下次重试=无，等待手动处理", paused)

    def test_manual_success_logs_are_traceable_without_payload_secrets(self):
        self.enroll()
        row = self.engine.store.all()[0]
        row["payload"]["AccessKeySecret"] = "do-not-log-this"
        row["payload"]["task"]["fileitem"]["path"] = "/video/Fake\nLog.mkv"
        with patch.object(self.h.bridge.logger, "info") as info:
            self.engine.log_task(row, "测试单行日志", 原因="line1\r\nline2")
            self.engine.control(row["id"], "upload")
            self.engine.execute(self.engine.store.claim(manual=True))
            lines = [str(call.args[0]) for call in info.call_args_list if "【115秒传等待】" in str(call.args[0])]
        self.assertFalse(any("do-not-log-this" in line or "\n" in line or "\r" in line for line in lines))
        for event in ("用户操作", "开始手动处理", "秒传未命中，转普通上传", "普通上传已提交", "整理成功"):
            self.assertTrue(any(event in line and row["id"] in line for line in lines), event)

    def test_retry_limit_pauses_then_manual_miss_uploads_and_updates_original_history(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.engine.config["max_retries"] = 2
        for _ in range(3):
            self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertEqual(self.api.inits, 3)
        self.assertEqual(self.api.uploads, 0)
        self.assertIsNone(self.engine.store.claim(now=time.time() + 9999))
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(Path(task.fileitem.path).exists())
        self.engine.control(row["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.uploads, 1)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.h.db.query(self.h.History).count(), 1)
        self.assertFalse(Path(task.fileitem.path).exists())

    def test_limit_policy_upload_uses_separate_queue_and_original_history(self):
        plugin = self.attach_plugin_notifier()
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.engine.config.update(max_retries=1, limit_action="upload")
        for _ in range(2):
            self.attempt(row["id"])
        queued = self.engine.store.get(row["id"])
        self.assertEqual(queued["state"], "upload_queued")
        self.assertEqual(queued["payload"]["upload_origin"], "limit")
        self.assertEqual(self.api.uploads, 0)
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertIn("已安排自动上传", plugin.chain.post_message.call_args.args[0].title)
        self.assertEqual(plugin.chain.post_message.call_args.args[0].mtype.value, "整理入库")
        self.assertIsNone(self.engine.store.claim(now=time.time() + 9999))
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.uploads, 1)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.h.db.query(self.h.History).count(), 1)
        self.assertFalse(Path(task.fileitem.path).exists())

    def test_time_limit_schedules_deadline_and_does_not_count_unattempted_retry(self):
        self.engine.config.update(max_wait_hours=1, limit_action="upload")
        self.enroll()
        row = self.engine.store.all()[0]
        deadline = time.time() + 10
        row["payload"]["wait_since"] = deadline - 3600
        self.engine.store.update(row["id"], payload=row["payload"])
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["next_at"], deadline)
        with patch.object(self.h.bridge.time, "time", return_value=deadline):
            self.engine.execute(self.engine.store.claim(now=deadline))
        current = self.engine.store.get(row["id"])
        self.assertEqual(current["state"], "upload_queued")
        self.assertEqual(current["attempts"], 1)
        self.assertEqual(current["payload"]["auto_attempts"], 1)
        self.assertEqual(self.api.inits, 1)

    def test_automatic_force_upload_hit_sends_no_contents(self):
        self.engine.config.update(max_retries=0, limit_action="upload")
        self.enroll()
        row = self.engine.store.all()[0]
        self.attempt(row["id"])
        self.api.hit = True
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.uploads, 0)
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_restoring_changed_deadline_keeps_paused_tasks_paused(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.attempt(row["id"])
        self.enroll("Paused.mkv")
        paused = next(t for t in self.engine.store.all() if t["id"] != row["id"])
        self.engine.control(paused["id"], "pause")
        self.engine.config.update(max_wait_hours=0.001, limit_action="upload")
        self.engine.restore()
        restored = self.engine.store.get(row["id"])
        self.assertEqual(restored["next_at"], row["payload"]["wait_since"] + 3.6)
        self.assertEqual(self.engine.store.get(paused["id"])["state"], "paused")

    def test_transition_to_upload_finishes_mp_bookkeeping_before_claim(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.engine.config.update(max_retries=0, limit_action="upload")
        real_update = self.engine.store.update
        def fast_worker(key, **changes):
            real_update(key, **changes)
            if changes.get("state") == "upload_queued":
                self.engine.execute(self.engine.store.claim(manual=True))
        with patch.object(self.engine.store, "update", side_effect=fast_worker):
            self.attempt(row["id"])
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        jobs = self.h.chain.jobview._job_view.values()
        self.assertFalse(any(task.state == "waiting" for job in jobs for task in job.tasks))

    def test_automatic_upload_failure_pauses_without_repeating(self):
        plugin = self.attach_plugin_notifier()
        self.engine.config.update(max_retries=0, limit_action="upload")
        self.enroll()
        row = self.engine.store.all()[0]
        self.attempt(row["id"])
        with patch.object(self.api, "upload", side_effect=remote.RetryLater("网络中断")):
            self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertIn("自动强制上传未完成", plugin.chain.post_message.call_args.args[0].text)
        self.assertIsNone(self.engine.store.claim(manual=True))
        self.assertIsNone(self.engine.store.claim(now=time.time() + 9999))
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)

    def test_upload_policy_never_overrides_source_and_authorization_guards(self):
        self.engine.config.update(max_retries=0, limit_action="upload")
        self.enroll()
        row = self.engine.store.all()[0]
        with patch.object(self.api, "instant", side_effect=remote.PauseTask("授权失效")):
            self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        task = self.enroll(name="Changed.mkv")
        changed = next(t for t in self.engine.store.all() if t["id"] != row["id"])
        self.attempt(changed["id"])
        Path(task.fileitem.path).write_bytes(b"changed source")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.engine.store.get(changed["id"])["state"], "paused")
        self.assertEqual(self.api.uploads, 0)

    def test_invalid_limit_policy_refuses_to_enable(self):
        import sys
        import types
        plugin = self.attach_plugin_notifier()
        with patch.dict(sys.modules, {"version": types.SimpleNamespace(APP_VERSION="v2.15.6")}):
            plugin.init_plugin({"enabled": True, "limit_action": "unexpected"})
        self.assertIn("达到上限后的操作", plugin._error)
        self.assertIsNone(plugin._engine)

    def test_manual_hit_never_sends_file_contents(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.engine.control(row["id"], "upload")
        self.api.hit = True
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.uploads, 0)
        self.assertEqual(self.api.inits, 1)
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_zero_retry_and_continue_waiting_reset_auto_budget(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.engine.config["max_retries"] = 0
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.engine.control(row["id"], "resume")
        self.attempt(row["id"])
        current = self.engine.store.get(row["id"])
        self.assertEqual(current["state"], "paused")
        self.assertEqual(current["attempts"], 2)
        self.assertEqual(current["payload"]["auto_attempts"], 1)
        self.assertEqual(self.api.uploads, 0)

    def test_manual_recovery_reconciles_existing_file_without_uploading_again(self):
        self.enroll()
        row = self.engine.store.all()[0]
        payload = row["payload"]
        path = Path(payload["task"]["fileitem"]["path"])
        payload["hashes"] = remote.hash_file(path, threading.Event())
        payload["upload_session"] = {"upload_id": "interrupted"}
        payload["write_intent"] = True
        self.api.hashes["123"] = payload["hashes"]["sha1"]
        final = PurePosixPath(payload["final_path"])
        self.api.files["123"] = self.h.FileItem(storage="u115", type="file", fileid="123",
            path=str(final),
            name=final.name, size=path.stat().st_size)
        self.engine.store.update(row["id"], state="uploading", payload=payload)
        self.engine.stop()
        self.engine = self.new_engine()
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.engine.control(row["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.inits, 0)
        self.assertEqual(self.api.uploads, 0)
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        self.assertEqual(self.h.db.query(self.h.History).count(), 1)

    def test_manual_upload_failure_pauses_and_preserves_source(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        self.engine.control(row["id"], "upload")
        with patch.object(self.api, "upload", side_effect=remote.RetryLater("网络中断")):
            self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.assertIsNone(self.engine.store.claim(now=time.time() + 9999))
        self.assertIsNone(self.engine.store.claim(manual=True))
        self.assertFalse(self.h.Oper().get(row["history_id"]).status)
        self.assertTrue(Path(task.fileitem.path).exists())

    def test_manual_upload_bypasses_expired_wait_limit_but_not_source_guard(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        payload = row["payload"]
        payload["wait_since"] = 0
        payload["auto_attempts"] = 99
        self.engine.store.update(row["id"], payload=payload)
        self.engine.control(row["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)
        other = self.enroll("changed.mkv")
        changed = self.engine.store.active_for(self.h.bridge.source_key(other.fileitem))
        Path(other.fileitem.path).write_bytes(b"changed")
        self.engine.control(changed["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.engine.store.get(changed["id"])["state"], "paused")
        self.assertEqual(self.api.uploads, 1)

    def test_confirmed_manual_upload_not_repeated_when_metadata_is_delayed(self):
        self.enroll()
        row = self.engine.store.all()[0]
        real_upload = self.api.upload
        hidden = {}
        def delayed(*args):
            real_upload(*args)
            hidden.update(self.api.files)
            self.api.files.clear()
        self.engine.control(row["id"], "upload")
        with patch.object(self.api, "upload", side_effect=delayed):
            self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        self.engine.control(row["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertEqual(self.api.inits, 1)
        self.assertEqual(self.api.uploads, 1)
        self.api.files.update(hidden)
        self.engine.control(row["id"], "upload")
        self.engine.execute(self.engine.store.claim(manual=True))
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

    def test_manual_upload_does_not_block_auto_queue_or_local_organization(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.engine.control(row["id"], "upload")
        entered, release = threading.Event(), threading.Event()
        real = self.api.upload
        def blocked(*args):
            entered.set()
            if not release.wait(10):
                raise remote.RetryLater("test timeout")
            return real(*args)
        with patch.object(self.api, "upload", side_effect=blocked):
            worker = threading.Thread(target=lambda: self.engine.execute(self.engine.store.claim(manual=True)))
            worker.start()
            try:
                self.assertTrue(entered.wait(3))
                other = self.enroll("other.mkv")
                waiting = self.engine.store.active_for(self.h.bridge.source_key(other.fileitem))
                self.attempt(waiting["id"])
                self.assertEqual(self.engine.store.get(waiting["id"])["state"], "waiting")
                local = self.enroll("local.mkv", target="local")
                self.assertTrue(self.h.Oper().get_by_src(local.fileitem.path, "local").status)
                self.assertFalse(self.h.Oper().get(row["history_id"]).status)
            finally:
                release.set()
                worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertTrue(self.h.Oper().get(row["history_id"]).status)

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

    def test_native_always_overwrites_without_temporary_directory(self):
        task = self.enroll(mode="move")
        row = self.engine.store.all()[0]
        old = self.h.FileItem(storage="u115", type="file", fileid="old", path="/library/A.mkv", name="A.mkv", size=10)
        self.api.files["old"] = old
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertNotIn("old", self.api.files)
        self.assertEqual(self.api.deletes, ["old"])
        self.assertEqual(self.api.inits, 1)
        self.assertEqual(self.api.moves, [])
        self.assertFalse(Path(task.fileitem.path).exists())

    def test_native_size_policy_overwrites_only_smaller_target(self):
        for size, expected in ((1, "completed"), (18, "failed"), (40, "failed")):
            with self.subTest(size=size):
                task = self.enroll(name=f"Size{size}.mkv")
                task.target_directory.overwrite_mode = "size"
                row = self.engine.store.active_for(self.h.bridge.source_key(task.fileitem))
                old_id = f"old{size}"
                self.api.files[old_id] = self.h.FileItem(storage="u115", type="file", fileid=old_id,
                    path=row["payload"]["final_path"], name=f"Size{size}.mkv", size=size)
                self.api.hit = True
                self.attempt(row["id"])
                self.assertEqual(self.engine.store.get(row["id"])["state"], expected)
                self.assertEqual(old_id in self.api.deletes, expected == "completed")
                self.assertEqual(old_id in self.api.files, expected == "failed")
                self.assertTrue(Path(task.fileitem.path).exists())

    def test_native_latest_removes_other_video_versions_without_backups(self):
        task = self.enroll()
        task.target_directory.overwrite_mode = "latest"
        row = self.engine.store.all()[0]
        self.api.files["old"] = self.h.FileItem(storage="u115", type="file", fileid="old",
            path="/library/A.Old.mkv", name="A.Old.mkv", extension="mkv", size=10)
        self.api.files["nfo"] = self.h.FileItem(storage="u115", type="file", fileid="nfo",
            path="/library/A.nfo", name="A.nfo", extension="nfo", size=1)
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertEqual(self.api.deletes, ["old"])
        self.assertIn("nfo", self.api.files)
        self.assertEqual(self.api.moves, [])

    def test_native_overwrite_event_veto_keeps_old_file(self):
        task = self.enroll()
        row = self.engine.store.all()[0]
        self.api.files["old"] = self.h.FileItem(storage="u115", type="file", fileid="old",
            path="/library/A.mkv", name="A.mkv", size=1)
        events = self.h.Handler.transfer_media.__globals__["eventmanager"]
        original = events.send_event
        from types import SimpleNamespace
        def veto(kind, data):
            if kind == "TransferOverwriteCheck":
                return SimpleNamespace(event_data=SimpleNamespace(overwrite=False, source_size=None,
                    target_size=None, source="test", reason="native event veto"))
            return original(kind, data)
        with patch.object(events, "send_event", side_effect=veto):
            self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "failed")
        self.assertEqual(self.api.deletes, [])
        self.assertEqual(self.api.inits, 0)
        self.assertIn("old", self.api.files)

    def test_success_uses_only_final_directory_and_never_moves_files(self):
        self.enroll()
        row = self.engine.store.all()[0]
        self.api.hit = True
        self.attempt(row["id"])
        self.assertEqual(self.engine.store.get(row["id"])["state"], "completed")
        self.assertTrue(all(".mp115-" not in path for path in self.api.folders))
        self.assertEqual(self.api.files["123"].path, row["payload"]["final_path"])
        self.assertEqual(self.api.moves, [])

    def test_legacy_staged_task_is_paused_and_cannot_resume_upload(self):
        self.enroll()
        row = self.engine.store.all()[0]
        data = row["payload"]
        data.pop("layout")
        data["stage_path"] = "/library/.mp115-staging/old/A.mkv"
        self.engine.store.update(row["id"], payload=data, state="upload_queued")
        self.engine.stop()
        self.engine = self.new_engine()
        self.assertEqual(self.engine.store.get(row["id"])["state"], "paused")
        for action in ("resume", "upload"):
            with self.assertRaises(ValueError):
                self.engine.control(row["id"], action)
        self.assertEqual(self.api.inits, 0)
        self.assertEqual(self.api.folders, [])
        self.engine.control(row["id"], "cancel")
        self.assertEqual(self.engine.store.get(row["id"])["payload"], data)

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
