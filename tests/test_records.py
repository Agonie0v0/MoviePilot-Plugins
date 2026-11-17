"""Record semantics must survive restart and stay consistent across MP versions."""
import importlib
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch


def modules(generation):
    name = f"record_test_{generation}"
    package = types.ModuleType(name)
    package.__path__ = [str(Path(__file__).resolve().parents[1] / f"plugins.{generation}" / "p115instantwait")]
    sys.modules[name] = package
    return [importlib.import_module(f"{name}.{part}") for part in ("records", "store", "ui")]


class RecordTests(unittest.TestCase):
    def test_actual_transfer_checkpoints_determine_success_after_restart(self):
        cases = [
            ({"instant_confirmed": True}, "instant", "触发秒传成功"),
            ({"upload_requested": True, "upload_origin": "manual", "instant_confirmed": True}, "instant", "触发秒传成功"),
            ({"upload_confirmed": True, "upload_origin": "limit"}, "upload", "强制上传成功"),
            ({"upload_session": {"completing": True}}, "upload", "强制上传成功"),
            ({"upload_requested": True}, "unknown", "传输方式未记录"),
            ({"remote_id": "123"}, "unknown", "传输方式未记录"),
            ({"upload_session": None}, "unknown", "传输方式未记录"),
        ]
        for generation in ("v2", "v3"):
            records, store, ui = modules(generation)
            for flags, method, label in cases:
                with self.subTest(generation=generation, flags=flags), tempfile.TemporaryDirectory() as temp:
                    db = store.QueueStore(Path(temp) / "queue.db")
                    payload = {"task": {"fileitem": {"path": "/downloads/电影.mkv"}}, "final_path": "/library/电影.mkv", **flags}
                    with patch.object(store.time, "time", return_value=1700000000):
                        row, _ = db.enqueue("local:电影.mkv", payload)
                    with patch.object(store.time, "time", return_value=1700000123):
                        db.update(row["id"], state="completed", message="文件传输与 MP 整理完成")
                    saved = store.QueueStore(db.path).get(row["id"])
                    record = records.task_record(saved)
                    self.assertEqual(record["created"], 1700000000)
                    self.assertEqual(record["updated"], 1700000123)
                    self.assertEqual(record["transfer_method"], method)
                    self.assertIn(label, record["result_message"])
                    page = json.dumps(ui.task_page([record], False), ensure_ascii=False)
                    self.assertIn(label, page)
                    self.assertIn("入队时间：" + ui.record_time(1700000000), page)
                    self.assertIn("完成时间：" + ui.record_time(1700000123), page)
                    self.assertEqual(db.get(row["id"]), saved, "Reading the page must not alter timestamps")

    def test_failure_and_unfinished_transfer_are_not_reported_as_success(self):
        for generation in ("v2", "v3"):
            records, _, _ = modules(generation)
            for state in ("failed", "paused", "waiting", "finalizing", "uploading"):
                with self.subTest(generation=generation, state=state):
                    message = "目标文件冲突：禁止覆盖 /电影/电影.mkv"
                    result = records.latest_result({"state": state, "message": message, "transfer_method": "upload"})
                    self.assertIn(message, result)
                    self.assertNotIn("上传成功", result)
                    if state == "failed":
                        self.assertTrue(result.startswith("失败原因："))
            self.assertIn("未记录具体原因", records.latest_result({"state": "failed", "message": ""}))

    def test_legacy_and_invalid_timestamps_have_an_honest_fallback(self):
        for generation in ("v2", "v3"):
            _, _, ui = modules(generation)
            for value in (None, "bad timestamp", float("nan"), float("inf")):
                with self.subTest(generation=generation, value=value):
                    self.assertEqual(ui.record_time(value), "未记录")

    def test_size_and_multipart_upload_progress_rendering(self):
        size = 100 * 1024 * 1024
        part_size = 20 * 1024 * 1024
        for generation in ("v2", "v3"):
            records, store, ui = modules(generation)
            with self.subTest(generation=generation), tempfile.TemporaryDirectory() as temp:
                db = store.QueueStore(Path(temp) / "queue.db")
                payload = {
                    "task": {"fileitem": {"path": "/downloads/Show.S01/Ep01.mkv", "size": size}},
                    "final_path": "/library/Show/Ep01.mkv",
                    "upload_session": {"part_size": part_size, "parts": [{"number": 1, "etag": "a"}, {"number": 2, "etag": "b"}]},
                }
                row, _ = db.enqueue("local:/downloads/Show.S01/Ep01.mkv", payload)
                db.update(row["id"], state="uploading")
                record = records.task_record(db.get(row["id"]))
                self.assertEqual(record["size"], size)
                self.assertEqual(record["upload_progress"]["percent"], 40)
                self.assertEqual((record["upload_progress"]["done_parts"], record["upload_progress"]["total_parts"]), (2, 5))
                html = json.dumps(ui.task_page([record], True, summary={
                    "states": {"completed": 20}, "methods": {"instant": 19, "upload": 1}, "total": 21, "next_at": None,
                }), ensure_ascii=False)
                self.assertIn("100.0 MB", html)
                self.assertIn("Show.S01", html)
                self.assertIn("分片上传进度 · 40%", html)
                self.assertIn("40.0 MB / 100.0 MB · 2/5 片", html)
                self.assertIn("秒传率 95%", html)
                self.assertIn("p115-ratio-bar", html)

                payload["upload_session"]["completing"] = True
                db.update(row["id"], payload=payload)
                completing_record = records.task_record(db.get(row["id"]))
                self.assertEqual(completing_record["upload_progress"]["percent"], 99)
                self.assertIn("正在提交上传合并", json.dumps(ui.task_page([completing_record], True), ensure_ascii=False))

                db.update(row["id"], state="completed")
                done_record = records.task_record(db.get(row["id"]))
                self.assertIsNone(done_record["upload_progress"])
