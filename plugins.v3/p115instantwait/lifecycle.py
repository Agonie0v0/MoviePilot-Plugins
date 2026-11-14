"""Keep one engine across MoviePilot plugin reloads."""
import sys
import threading
from time import monotonic_ns


CODE_REVISION = monotonic_ns()


class Runtime:
    revision = CODE_REVISION

    def __init__(self, defaults, logger):
        self.lock = threading.RLock()
        self.config, self.logger = dict(defaults), logger
        self.engine = self.owner = self.implementation = self.pending = self.thread = None
        self.error = ""
        self.waiting = False
        self.serial = 0
        self.latest = (0, 0)

    def register(self, revision):
        with self.lock:
            self.serial += 1
            return revision, self.serial

    def configure(self, plugin, config):
        with self.lock:
            # A late first initialization must not replace a newer instance.
            if not plugin._initialized and plugin._ticket < self.latest:
                plugin._initialized = True
                return
            plugin._initialized = True
            self.serial += 1
            if plugin._ticket > self.latest:
                self.latest = plugin._ticket
                self.implementation = plugin
            self.owner = plugin
            self.error = ""
            self.pending = (self.implementation, dict(config or {}))
            if not self.advance():
                self.start_waiter()
                pending_config = self.pending[1]
                if pending_config.get("apply_action") or pending_config.get("cleanup_history"):
                    pending_config.update(apply_action=False, task_ids=[], task_id="",
                                          cleanup_history=False, history_ids=[])
                    if not plugin.update_config(pending_config):
                        self.pending = None
                        self.error = "操作未执行，配置保存失败，请重试"
                    message = "正在更新，本次任务操作未执行，请稍后重试"
                    self.logger.warning(f"【115秒传等待】{message}")
                    plugin.systemmessage.put(f"115秒传等待：{message}")

    def advance(self):
        if self.engine:
            if self.engine.stop(timeout=0) is False:
                if not self.waiting:
                    self.logger.info("【115秒传等待】等待当前任务结束后应用配置")
                self.waiting = True
                return False
            self.engine = None
        was_waiting, self.waiting = self.waiting, False
        pending, self.pending = self.pending, None
        if pending:
            plugin, config = pending
            plugin._initialize(config)
        if was_waiting:
            message = ("配置已应用" if pending else "插件已停止") if not self.error else "配置未生效，请查看插件提示"
            self.logger.info(f"【115秒传等待】{message}")
        return True

    def start_waiter(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self.finish, name="p115-reload", daemon=True)
            self.thread.start()

    def finish(self):
        wake = threading.Event()
        try:
            while True:
                wake.wait(.25)
                with self.lock:
                    if not self.waiting or self.advance():
                        self.thread = None
                        return
        except Exception:
            with self.lock:
                self.error = "更新未完成，请查看日志后重试"
                self.waiting = False
                self.thread = None
                self.logger.exception("【115秒传等待】更新失败")

    def stop(self, plugin):
        with self.lock:
            if self.owner is not plugin:
                if plugin._registered_plugin() is plugin:
                    timer = threading.Timer(.25, self.stop_unregistered, args=(self.serial, plugin))
                    timer.daemon = True
                    timer.start()
                return
            self.pending = None
            if not self.advance():
                self.start_waiter()

    def stop_unregistered(self, serial, plugin):
        with self.lock:
            # An uninstall can reach an old proxy; newer requests cancel this stop.
            if self.serial == serial and plugin._registered_plugin() is None:
                self.stop(self.owner)


def shared_runtime(host, plugin_id, defaults, logger):
    # The host module survives removal of the plugin and its submodules.
    runtimes = vars(sys.modules[host.__module__]).setdefault("_p115instantwait_runtimes", {})
    runtime = runtimes.setdefault((host, plugin_id), Runtime(defaults, logger))
    with runtime.lock:
        if Runtime.revision > type(runtime).revision:
            runtime.__class__ = Runtime
    return runtime


class RuntimePlugin:
    _code_revision = CODE_REVISION

    @property
    def _engine(self):
        return self._runtime.engine

    @_engine.setter
    def _engine(self, value):
        self._runtime.engine = value

    @property
    def _config(self):
        return self._runtime.config

    @_config.setter
    def _config(self, value):
        self._runtime.config = value

    @property
    def _error(self):
        return self._runtime.error

    @_error.setter
    def _error(self, value):
        self._runtime.error = value

    @property
    def _notice(self):
        return "正在等待当前任务结束，随后自动应用配置。" if self._runtime.waiting else ""

    def stop_service(self):
        self._runtime.stop(self)
