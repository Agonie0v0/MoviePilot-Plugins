"""Time/status semantics; visual layout is verified in the browser."""
import unittest
from tests.support import load

ui = load("ui")


class ScheduleTests(unittest.TestCase):
    def test_disabled_and_paused_tasks_do_not_advertise_a_retry_time(self):
        task = {"state": "waiting", "next_at": 2000}
        self.assertEqual(ui.schedule_summary(task, False, 1000)[1], "插件已关闭")
        task["state"] = "paused"
        self.assertEqual(ui.schedule_summary(task, True, 1000)[1], "等待手动处理")

    def test_due_task_is_scheduled_not_claimed_to_be_running(self):
        label, value, _ = ui.schedule_summary({"state": "waiting", "next_at": 999}, True, 1000)
        self.assertEqual(label, "下次尝试")
        self.assertEqual(value, "已到时间 · 待调度")

    def test_success_stays_visible_when_plugin_is_disabled(self):
        self.assertEqual(ui.schedule_summary({"state": "completed"}, False, 1000)[1], "原记录已成功")
