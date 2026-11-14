"""Reload coordination survives module replacement and delayed engine shutdown."""
import importlib.util
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]


def load_lifecycle(generation):
    path = ROOT / f"plugins.{generation}" / "p115instantwait" / "lifecycle.py"
    spec = importlib.util.spec_from_file_location("reload_lifecycle", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReloadV2Tests(unittest.TestCase):
    generation = "v2"

    def setUp(self):
        self.host = type("TransferChain", (), {})
        self.logger = Mock()
        self.engines = []
        self.plugins = []
        self.registered = {}

    def tearDown(self):
        for engine in self.engines:
            engine.finished.set()
        if self.plugins:
            runtime = self.plugins[0]._runtime
            runtime.stop(runtime.owner)
            thread = runtime.thread
            if thread:
                thread.join(3)
                self.assertFalse(thread.is_alive())

    def plugin(self):
        # Each instance uses freshly imported code, as after sys.modules is cleared.
        lifecycle = load_lifecycle(self.generation)
        host, logger, engines, registered = self.host, self.logger, self.engines, self.registered

        class Engine:
            def __init__(self, config):
                self.config = config
                self.stop_event = threading.Event()
                self.finished = threading.Event()
                self.finished.set()
                engines.append(self)

            def stop(self, timeout=35):
                self.stop_event.set()
                return self.finished.is_set()

        class Plugin(lifecycle.RuntimePlugin):
            def __init__(self):
                self._runtime = lifecycle.shared_runtime(host, "P115InstantWait", {"enabled": False}, logger)
                self._ticket = self._runtime.register(self._code_revision)
                self._initialized = False
                self.update_config = Mock(return_value=True)
                self.systemmessage = Mock()
                self.applied = []

            def init_plugin(self, config):
                self._runtime.configure(self, config)

            def _registered_plugin(self):
                return registered.get("P115InstantWait")

            def _initialize(self, config):
                self.applied.append(dict(config))
                self._config = dict(config)
                self._error = ""
                if config.get("enabled"):
                    self._engine = Engine(config)

        plugin = Plugin()
        self.plugins.append(plugin)
        return plugin

    def finish_update(self, plugin):
        thread = plugin._runtime.thread
        self.assertIsNotNone(thread)
        thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertFalse(plugin._runtime.waiting)

    def test_late_initialization_and_stop_cannot_replace_or_stop_new_engine(self):
        old, new = self.plugin(), self.plugin()
        new.init_plugin({"enabled": True, "name": "new"})
        engine = new._engine
        old.init_plugin({"enabled": True, "name": "old"})
        old.stop_service()
        self.assertIs(old._engine, engine)
        self.assertEqual(old._config["name"], "new")
        self.assertFalse(engine.stop_event.is_set())
        self.assertEqual(old.applied, [])

    def test_old_registered_instance_can_still_apply_user_settings(self):
        old, new = self.plugin(), self.plugin()
        old.init_plugin({"enabled": True})
        new.init_plugin({"enabled": True})
        old.init_plugin({"enabled": True, "name": "changed"})
        self.assertEqual(new._config["name"], "changed")
        self.assertIs(old._engine, new._engine)
        self.assertEqual(new.applied[-1]["name"], "changed")
        self.assertEqual(len(old.applied), 1)

    def test_old_code_constructed_late_cannot_replace_new_implementation(self):
        old, new = self.plugin(), self.plugin()
        new.init_plugin({"enabled": True, "name": "new"})
        late = type(old)()
        late.init_plugin({"enabled": True, "name": "old"})
        self.assertEqual(new._config["name"], "new")
        self.assertEqual(type(new._runtime).revision, new._code_revision)

    def test_uninstall_through_old_registered_proxy_stops_current_engine(self):
        old, new = self.plugin(), self.plugin()
        old.init_plugin({"enabled": True})
        new.init_plugin({"enabled": True})
        self.registered["P115InstantWait"] = old
        with patch.object(threading, "Timer") as timer:
            old.stop_service()
            timer.assert_called_once()
        self.registered.clear()
        new._runtime.stop_unregistered(new._runtime.serial, old)
        self.assertIsNone(new._engine)

    def test_deferred_old_stop_does_not_cancel_another_reload(self):
        old, new = self.plugin(), self.plugin()
        old.init_plugin({"enabled": True})
        new.init_plugin({"enabled": True})
        serial = new._runtime.serial
        newest = self.plugin()
        newest.init_plugin({"enabled": True})
        new._runtime.stop_unregistered(serial, old)
        self.assertFalse(newest._engine.stop_event.is_set())

    def test_concurrent_initialization_keeps_only_one_running_engine(self):
        plugins = [self.plugin(), self.plugin()]
        barrier = threading.Barrier(2)
        errors = []

        def initialize(plugin):
            try:
                barrier.wait(3)
                plugin.init_plugin({"enabled": True})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=initialize, args=(p,)) for p in plugins]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(sum(not e.stop_event.is_set() for e in self.engines), 1)
        self.assertIs(plugins[0]._engine, plugins[1]._engine)

    def test_delayed_stop_automatically_applies_only_latest_config(self):
        old = self.plugin()
        old.init_plugin({"enabled": True})
        engine = old._engine
        engine.finished.clear()
        old.stop_service()
        middle = self.plugin()
        middle.init_plugin({"enabled": True, "name": "middle"})
        latest = self.plugin()
        latest.init_plugin({"enabled": True, "name": "latest"})
        old.stop_service()
        self.assertTrue(latest._notice)
        self.assertEqual(latest._error, "")
        self.assertEqual(len(self.engines), 1)
        engine.finished.set()
        self.finish_update(latest)
        self.assertEqual(middle.applied, [])
        self.assertEqual(latest._config["name"], "latest")
        self.assertEqual(len(self.engines), 2)
        self.assertFalse(latest._engine.stop_event.is_set())

    def test_disable_during_update_does_not_start_another_engine(self):
        plugin = self.plugin()
        plugin.init_plugin({"enabled": True})
        engine = plugin._engine
        engine.finished.clear()
        replacement = self.plugin()
        replacement.init_plugin({"enabled": True})
        replacement.init_plugin({"enabled": False})
        engine.finished.set()
        self.finish_update(replacement)
        self.assertIsNone(replacement._engine)
        self.assertEqual(len(self.engines), 1)

    def test_finishing_waiter_does_not_stop_engine_started_by_another_request(self):
        old = self.plugin()
        old.init_plugin({"enabled": True})
        engine = old._engine
        engine.finished.clear()
        new = self.plugin()
        new.init_plugin({"enabled": True})
        waiter = new._runtime.thread
        engine.finished.set()
        new.init_plugin({"enabled": True, "name": "latest"})
        waiter.join(3)
        self.assertFalse(waiter.is_alive())
        self.assertFalse(new._engine.stop_event.is_set())
        self.assertEqual(new._config["name"], "latest")

    def test_stop_cancels_pending_restart(self):
        old = self.plugin()
        old.init_plugin({"enabled": True})
        engine = old._engine
        engine.finished.clear()
        new = self.plugin()
        new.init_plugin({"enabled": True})
        new.stop_service()
        engine.finished.set()
        self.finish_update(new)
        self.assertIsNone(new._engine)
        self.assertEqual(new.applied, [])

    def test_one_shot_operations_during_update_are_rejected_and_cleared(self):
        old = self.plugin()
        old.init_plugin({"enabled": True})
        engine = old._engine
        engine.finished.clear()
        new = self.plugin()
        new.init_plugin({"enabled": True, "apply_action": True, "task_ids": ["job"],
                         "cleanup_history": True, "history_ids": ["history"]})
        saved = new.update_config.call_args.args[0]
        self.assertFalse(saved["apply_action"])
        self.assertFalse(saved["cleanup_history"])
        new.systemmessage.put.assert_called_once()
        engine.finished.set()
        self.finish_update(new)
        self.assertFalse(new.applied[0]["apply_action"])
        self.assertFalse(new.applied[0]["cleanup_history"])

    def test_failed_one_shot_save_does_not_apply_pending_request(self):
        old = self.plugin()
        old.init_plugin({"enabled": True})
        engine = old._engine
        engine.finished.clear()
        new = self.plugin()
        new.update_config.return_value = False
        new.init_plugin({"enabled": True, "apply_action": True})
        engine.finished.set()
        self.finish_update(new)
        self.assertEqual(new.applied, [])
        self.assertIsNone(new._engine)
        self.assertIn("配置保存失败", new._error)

    def test_clone_uses_separate_runtime(self):
        plugin = self.plugin()
        lifecycle = load_lifecycle(self.generation)
        clone = lifecycle.shared_runtime(self.host, "P115InstantWaitClone", {}, self.logger)
        self.assertIsNot(clone, plugin._runtime)


class ReloadV3Tests(ReloadV2Tests):
    generation = "v3"
