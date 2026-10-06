"""Exercise the real dashboard endpoint without importing the MoviePilot host."""
import ast
from pathlib import Path
import unittest
from unittest.mock import Mock

from fastapi import Body, FastAPI, HTTPException
from fastapi.testclient import TestClient


class DashboardApiTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / "plugins.v2/p115instantwait/__init__.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
        # Compile only host-independent methods, preserving the actual FastAPI
        # signature and production bodies rather than duplicating the endpoint.
        methods = [n for n in cls.body if isinstance(n, ast.FunctionDef)
                   and n.name in ("control_batch", "get_api", "get_state")]
        cls.bases, cls.body = [], methods
        namespace = {"Body": Body, "HTTPException": HTTPException, "logger": Mock()}
        exec(compile(ast.Module(body=[cls], type_ignores=[]), str(source), "exec"), namespace)
        self.plugin = namespace[cls.name]()
        self.plugin.list_tasks = self.plugin.control_task = Mock()
        self.plugin._engine = Mock()
        self.plugin._engine.stop_event.is_set.return_value = False
        self.plugin.save_data = Mock()
        self.result = dict(action="resume", accepted=1, skipped=1, failed=0, items=[])
        self.plugin._engine.control_many.return_value = self.result
        app = FastAPI()
        app.add_api_route("/batch/{action}", self.plugin.control_batch, methods=["POST"])
        self.client = TestClient(app)

    def test_snapshot_is_forwarded_and_partial_result_is_persisted(self):
        res = self.client.post("/batch/resume", json={"keys": ["paused", "now-running"]})
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json(), self.result)
        self.plugin._engine.control_many.assert_called_once_with(["paused", "now-running"], "resume")
        self.plugin.save_data.assert_called_once_with("last_batch_result", self.result)
        route = next(r for r in self.plugin.get_api() if r["path"] == "/batch/{action}")
        self.assertEqual(route["auth"], "bear")

    def test_invalid_or_disabled_requests_cannot_issue_commands(self):
        self.assertEqual(self.client.post("/batch/resume", json={"keys": "all"}).status_code, 422)
        self.assertEqual(self.client.post("/batch/cancel", json={"keys": ["task"]}).status_code, 400)
        self.plugin._engine.control_many.assert_not_called()
        self.plugin._engine.control_many.side_effect = ValueError("请选择 1～200 个任务")
        self.assertEqual(self.client.post("/batch/upload", json={"keys": []}).status_code, 400)
        self.plugin._engine.control_many.reset_mock()
        self.plugin._engine = None
        self.assertEqual(self.client.post("/batch/resume", json={"keys": ["task"]}).status_code, 409)
        self.plugin.save_data.assert_not_called()

    def test_result_save_failure_does_not_replay_commands(self):
        self.plugin.save_data.side_effect = RuntimeError("storage unavailable")
        res = self.client.post("/batch/upload", json={"keys": ["task"]})
        self.assertEqual(res.status_code, 200)
        self.assertIn("未能保存", res.json()["notice"])
        self.plugin._engine.control_many.assert_called_once()
