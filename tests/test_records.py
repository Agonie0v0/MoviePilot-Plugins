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
