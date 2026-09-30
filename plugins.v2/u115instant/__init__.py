"""115 秒传整理插件（MoviePilot V2）。"""

import hashlib
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.interval import IntervalTrigger

from app import schemas
from app.chain.transfer import TransferChain
from app.core.event import Event, eventmanager
from app.plugins import _PluginBase
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.modules.filemanager.storages.u115 import U115Pan
from app.schemas import NotificationType
from app.schemas.types import ChainEventType, EventType


class _U115InstantProxy:
    """只替换 upload，其余 115 操作全部委托给 MP 原对象。"""

    def __init__(self, plugin: "u115instant", target: U115Pan):
        self._plugin = plugin
        self._target = target

    def __getattr__(self, name: str) -> Any:
        return getattr(self._target, name)

    def __call__(self, *args, **kwargs):
        # Pydantic 的 Callable 字段要求对象可调用；整理链不会调用这个方法。
        return self

    def upload(self, target_dir, local_path: Path, new_name: Optional[str] = None):
        return self._plugin._instant_upload(self._target, target_dir, local_path, new_name)


class u115instant(_PluginBase):
    plugin_name = "115秒传整理"
    plugin_desc = "115 未命中秒传时取消普通上传，达到上限转人工处理并支持强制上传"
    plugin_icon = "https://raw.githubusercontent.com/Agonie0v0/MoviePilot-Plugins/main/icons/u115instant.png"
    plugin_version = "1.5.0"
    plugin_author = "Agonie"
    author_url = "https://github.com/Agonie0v0"
    plugin_config_prefix = "u115instant"
    plugin_order = 1
    auth_level = 1

    _MARKER = "115秒传整理：未命中秒传，已取消普通上传"
    _DEFAULT_RISK_COOLDOWN_SECONDS = 3600
    _VIDEO_EXTENSIONS = {
        ".264",
        ".265",
        ".avi",
        ".flv",
        ".iso",
        ".m2ts",
        ".m4v",
        ".mkv",
        ".mov",
        ".mp4",
        ".mpeg",
        ".mpg",
        ".ts",
        ".webm",
        ".wmv",
    }

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._notify = True
        self._first_retry_minutes = 10
        self._second_retry_minutes = 120
        self._later_retry_minutes = 360
        self._max_retry_attempts = 5
        self._max_wait_hours = 72
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        # ponytail: 一个账号一个锁即可，插件只允许一个 115 整理链路在途。
        self._u115_lock = threading.RLock()
        self._cooldown_until = 0.0
        self._u115_target: Optional[U115Pan] = None
        self._context = threading.local()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._notify = bool(config.get("notify", True))
        self._first_retry_minutes = self._positive_int(
            config.get("first_retry_minutes", 10), 10, 1, 1440
        )
        self._second_retry_minutes = self._positive_int(
            config.get("second_retry_minutes", 120), 120, 1, 10080
        )
        self._later_retry_minutes = self._positive_int(
            config.get("later_retry_minutes", 360), 360, 1, 10080
        )
        self._max_retry_attempts = self._positive_int(
            config.get("max_retry_attempts", 5), 5, 1, 100
        )
        self._max_wait_hours = self._positive_int(
            config.get("max_wait_hours", 72), 72, 1, 720
        )
        saved = self.get_data("tasks") or {}
        self._tasks = saved if isinstance(saved, dict) else {}
        if self._enabled:
            logger.info("[115Instant] 已启用，仅拦截本地到 115 的视频移动整理")

    @staticmethod
    def _positive_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(value)
        except (TypeError, ValueError):
            return default
        return max(minimum, min(value, maximum))

    @staticmethod
    def _host_cooldown_until(original: Optional[U115Pan]) -> float:
        """读取 MP 内置 115 对象的共享风控截止时间。"""
        if original is None:
            return 0.0
        try:
            limit_lock = getattr(original, "_limit_lock", None)
            if limit_lock is not None:
                with limit_lock:
                    return float(getattr(original, "_limit_until", 0.0) or 0.0)
            return float(getattr(original, "_limit_until", 0.0) or 0.0)
        except (TypeError, ValueError, AttributeError):
            return 0.0

    def _get_u115_target(self) -> Optional[U115Pan]:
        """获取当前进程共享的 115 存储对象。"""
        if self._u115_target is not None:
            return self._u115_target
        try:
            self._u115_target = U115Pan()
        except Exception as exc:
            logger.debug(f"[115Instant] 获取 115 对象失败：{exc}")
        return self._u115_target

    def _install_request_guard(self, original: U115Pan):
        """将宿主请求方法接到插件冷却和并发锁。"""
        guard = getattr(original, "_u115instant_request_guard", None)
        if guard:
            guard["plugin"] = self
            self._u115_target = original
            return

        raw_request = original._request_api
        guard = {"plugin": self, "original": raw_request}

        def guarded_request(method, endpoint, result_key=None, *args, **kwargs):
            plugin = guard.get("plugin")
            if plugin is None or not plugin.get_state():
                return guard["original"](method, endpoint, result_key, *args, **kwargs)
            return plugin._request_115(
                original,
                method,
                endpoint,
                result_key,
                *args,
                **kwargs,
            )

        original._u115instant_request_guard = guard
        original._request_api = guarded_request
        self._u115_target = original

    def _restore_request_guard(self):
        """插件停用时恢复宿主原始请求方法。"""
        original = self._u115_target
        guard = getattr(original, "_u115instant_request_guard", None)
        if not original or not guard or guard.get("plugin") is not self:
            return
        original._request_api = guard["original"]
        delattr(original, "_u115instant_request_guard")
        self._u115_target = None

    @staticmethod
    def _raw_request_api(original: U115Pan):
        guard = getattr(original, "_u115instant_request_guard", None)
        if guard:
            return guard["original"]
        return original._request_api

    def _cooldown_until_for(self, original: Optional[U115Pan] = None) -> float:
        """合并插件冷却与宿主冷却，避免调用方绕过任一层保护。"""
        host_until = self._host_cooldown_until(original)
        with self._lock:
            self._cooldown_until = max(self._cooldown_until, host_until)
            return self._cooldown_until

    def _cooldown_remaining(self, original: Optional[U115Pan] = None) -> float:
        return max(0.0, self._cooldown_until_for(original) - time.time())

    @staticmethod
    def _cooldown_minutes(seconds: float) -> int:
        return max(1, int((max(0.0, seconds) + 59) // 60))

    def _activate_cooldown(self, original: Optional[U115Pan], reason: str) -> float:
        """记录一次插件侧冷却；宿主冷却更长时沿用宿主截止时间。"""
        now = time.time()
        try:
            duration = float(
                getattr(original, "limit_sleep_seconds", self._DEFAULT_RISK_COOLDOWN_SECONDS)
            )
        except (TypeError, ValueError):
            duration = self._DEFAULT_RISK_COOLDOWN_SECONDS
        until = max(now + max(60.0, duration), self._host_cooldown_until(original))
        with self._lock:
            previous = self._cooldown_until
            self._cooldown_until = max(previous, until)
            active_until = self._cooldown_until
        if active_until > previous + 1:
            logger.warning(
                f"[115Instant] {reason}，插件暂停 115 请求 {self._cooldown_minutes(active_until - now)} 分钟"
            )
        return active_until

    def _request_115(
        self,
        original: U115Pan,
        method: str,
        endpoint: str,
        result_key: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """插件请求入口：串行调用并在失败后进入插件侧冷却。"""
        with self._u115_lock:
            remaining = self._cooldown_remaining(original)
            if remaining > 0:
                logger.debug(
                    f"[115Instant] {method} {endpoint} 被插件冷却拦截，"
                    f"剩余 {remaining:.0f} 秒"
                )
                return None
            # 宿主的重试会在风控响应上同步等待；重试调度交给插件队列。
            kwargs["retry_limit"] = 0
            response = self._raw_request_api(original)(
                method,
                endpoint,
                result_key,
                *args,
                **kwargs,
            )
            if response is None:
                self._activate_cooldown(
                    original,
                    f"{method} {endpoint} 请求失败或触发访问限制",
                )
            return response

    def _defer_task(self, key: str, next_at: float, reason: str):
        """不增加重试次数地顺延任务，供锁竞争和风控冷却使用。"""
        with self._lock:
            task = self._tasks.get(key)
            if not task:
                return
            task.update(
                {
                    "status": "waiting",
                    "next_at": next_at,
                    "last_error": reason,
                    "updated_at": time.time(),
                }
            )
            self._save_tasks()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/115instant_retry",
                "event": EventType.PluginAction,
                "desc": "立即重试 115 秒传等待任务",
                "category": "整理",
                "data": {"action": "retry"},
            },
            {
                "cmd": "/115instant_force",
                "event": EventType.PluginAction,
                "desc": "强制上传一条 115 人工处理任务（忽略秒传）",
                "category": "整理",
                "data": {"action": "force_upload"},
            },
        ]

    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event: Event):
        if not event or not isinstance(event.event_data, dict):
            return
        action = event.event_data.get("action")
        if action == "retry":
            self.retry_pending(force=True)
        elif action == "force_upload":
            self.force_upload_pending()

    @eventmanager.register(ChainEventType.StorageOperSelection, priority=5)
    def select_storage_oper(self, event: Event):
        """给 MP 的本次整理提供代理对象，避免修改宿主 115 类。"""
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        storage = self._field(data, "storage")
        if storage == "u115":
            try:
                original = U115Pan()
                self._install_request_guard(original)
                proxy = _U115InstantProxy(self, original)
                if isinstance(data, dict):
                    data["storage_oper"] = proxy
                else:
                    setattr(data, "storage_oper", proxy)
            except Exception as exc:
                logger.error(f"[115Instant] 获取 115 操作对象失败：{exc}")

    @eventmanager.register(ChainEventType.TransferIntercept, priority=5)
    def capture_transfer_context(self, event: Event):
        """记录当前文件上下文，供 upload 判断是否需要保护。"""
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        fileitem = self._field(data, "fileitem")
        path = self._field(fileitem, "path")
        previous = getattr(self._context, "value", None)
        self._context.value = {
            "source_storage": self._field(fileitem, "storage"),
            "target_storage": self._field(data, "target_storage"),
            "transfer_type": self._field(data, "transfer_type"),
            "path": str(path) if path else "",
            "force_upload": bool(previous.get("force_upload"))
            if isinstance(previous, dict)
            else False,
            "retrying": bool(previous.get("retrying"))
            if isinstance(previous, dict)
            else False,
        }
        path_obj = Path(str(path or ""))
        if (
            self._protected(path_obj)
            and self._cooldown_remaining(self._get_u115_target()) > 0
        ):
            remaining = self._cooldown_remaining(self._get_u115_target())
            self._record_wait(
                path_obj,
                Path(str(self._field(data, "target_path") or "")),
                f"115 风控冷却中，已顺延 {self._cooldown_minutes(remaining)} 分钟",
            )
            self._set_field(data, "cancel", True)
            self._set_field(data, "source", "u115instant")
            self._set_field(
                data,
                "reason",
                f"115 风控冷却中，剩余 {self._cooldown_minutes(remaining)} 分钟，本次整理已顺延",
            )

    @eventmanager.register(ChainEventType.TransferOverwriteCheck, priority=5)
    def deny_overwrite(self, event: Event):
        """保护旧文件，避免等待秒传时覆盖模式先删除远端文件。"""
        if not self._enabled or not event or not event.event_data:
            return
        data = event.event_data
        fileitem = self._field(data, "fileitem")
        path = Path(str(self._field(fileitem, "path") or ""))
        if (
            self._field(fileitem, "storage") == "local"
            and self._field(data, "target_storage") == "u115"
            and self._field(data, "transfer_type") == "move"
            and self._is_video(path)
        ):
            self._set_field(data, "overwrite", False)
            self._set_field(data, "source", "u115instant")
            self._set_field(data, "reason", "115秒传整理不允许覆盖已有远端文件")

    @eventmanager.register(EventType.TransferFailed)
    def capture_failed_history(self, event: Event):
        """把 MP 生成的失败历史 ID 绑定到等待任务。"""
        if not self._enabled or not event or not isinstance(event.event_data, dict):
            return
        data = event.event_data
        fileitem = data.get("fileitem")
        path = self._field(fileitem, "path")
        if not path:
            return
        key = self._task_key(str(path))
        history_id = data.get("transfer_history_id")
        if not history_id:
            return
        with self._lock:
            task = self._tasks.get(key)
            if task and task.get("status") in {"waiting", "retrying", "manual", "forcing"}:
                task["history_id"] = int(history_id)
                task["updated_at"] = time.time()
                self._save_tasks()

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [
            {
                "id": "u115instant",
                "name": "115秒传整理重试",
                "trigger": IntervalTrigger(minutes=1),
                "func": self.retry_pending,
                "kwargs": {},
            }
        ]

    def stop_service(self):
        self._restore_request_guard()
        self._context.value = None

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        def field_col(model: str, label: str, icon: str, minimum: int, maximum: int, md: int = 6) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": md, "class": "py-1 px-2"},
                "content": [
                    {
                        "component": "VTextField",
                        "props": {
                            "model": model,
                            "label": label,
                            "type": "number",
                            "min": minimum,
                            "max": maximum,
                            "variant": "outlined",
                            "density": "comfortable",
                            "hide-details": "auto",
                            "prepend-inner-icon": icon,
                        },
                    }
                ],
            }

        def switch_col(model: str, label: str, icon: str, color: str) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 12, "md": 6, "class": "py-1 px-2"},
                "content": [
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": model,
                            "label": label,
                            "color": color,
                            "inset": True,
                            "density": "comfortable",
                            "hide-details": True,
                            "prepend-icon": icon,
                        },
                    }
                ],
            }

        return [
            {
                "component": "VForm",
                "props": {"class": "pa-1"},
                "content": [
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "icon": "mdi-cloud-check-outline",
                            "title": "115 秒传保护",
                            "class": "mb-3",
                        },
                        "text": "只保护本地到内置 115 的视频移动整理。未命中秒传会保留源文件并进入等待队列，不会启动普通 OSS 上传。",
                    },
                    {
                        "component": "div",
                        "props": {"class": "text-subtitle-2 font-weight-bold px-2 mt-1 mb-1"},
                        "text": "运行开关",
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True, "class": "mx-n2 mb-1"},
                        "content": [
                            switch_col("enabled", "启用 115 仅秒传整理", "mdi-power", "primary"),
                            switch_col("notify", "等待或异常时通知", "mdi-bell-outline", "info"),
                        ],
                    },
                    {
                        "component": "VDivider",
                        "props": {"class": "my-3"},
                    },
                    {
                        "component": "div",
                        "props": {"class": "text-subtitle-2 font-weight-bold px-2 mt-1 mb-1"},
                        "text": "重试策略",
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True, "class": "mx-n2"},
                        "content": [
                            field_col("first_retry_minutes", "首次重试（分钟）", "mdi-timer-sand", 1, 1440, 4),
                            field_col("second_retry_minutes", "第二次重试（分钟）", "mdi-timer-outline", 1, 10080, 4),
                            field_col("later_retry_minutes", "之后间隔（分钟）", "mdi-repeat", 1, 10080, 4),
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"dense": True, "class": "mx-n2"},
                        "content": [
                            field_col("max_retry_attempts", "最大重试次数（不含首次）", "mdi-counter", 1, 100, 6),
                            field_col("max_wait_hours", "最长等待（小时）", "mdi-clock-alert-outline", 1, 720, 6),
                        ],
                    },
                    {
                        "component": "VSheet",
                        "props": {"class": "pa-3 mt-2 mb-2 rounded-lg bg-grey-lighten-5", "border": True},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "text-caption text-medium-emphasis mb-2"},
                                "text": "默认节奏为 10 分钟、2 小时、6 小时，最多自动重试 5 次；超过上限后转人工处理。",
                            },
                            {
                                "component": "div",
                                "props": {"class": "d-flex flex-wrap ga-2"},
                                "content": [
                                    {
                                        "component": "VChip",
                                        "props": {"size": "small", "variant": "tonal", "color": "primary", "prepend-icon": "mdi-numeric-1-circle-outline"},
                                        "text": "首次：可自定义",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {"size": "small", "variant": "tonal", "color": "info", "prepend-icon": "mdi-numeric-2-circle-outline"},
                                        "text": "第二次：可自定义",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {"size": "small", "variant": "tonal", "color": "secondary", "prepend-icon": "mdi-repeat"},
                                        "text": "之后：可自定义",
                                    },
                                    {
                                        "component": "VChip",
                                        "props": {"size": "small", "variant": "tonal", "color": "error", "prepend-icon": "mdi-account-alert-outline"},
                                        "text": "超限：人工处理",
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "icon": "mdi-shield-alert-outline",
                            "title": "整理前请确认保护边界",
                            "class": "mt-3",
                        },
                        "text": "媒体库整理方式请选择“移动”，覆盖模式请选择“不覆盖”。插件只保护视频主文件，字幕、NFO 和封面仍按 MP 原生逻辑处理。",
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "first_retry_minutes": 10,
            "second_retry_minutes": 120,
            "later_retry_minutes": 360,
            "max_retry_attempts": 5,
            "max_wait_hours": 72,
        }

    def get_page(self) -> List[dict]:
        with self._lock:
            tasks = [dict(task) for task in self._tasks.values()]
        waiting = sum(task.get("status") == "waiting" for task in tasks)
        retrying = sum(task.get("status") in {"retrying", "forcing"} for task in tasks)
        needs_action = sum(task.get("status") in {"manual", "stale", "error", "verify"} for task in tasks)
        cooldown_remaining = self._cooldown_remaining(self._get_u115_target())

        def format_time(value: Any) -> str:
            try:
                return time.strftime("%m-%d %H:%M", time.localtime(float(value)))
            except (TypeError, ValueError, OverflowError):
                return "—"

        def status_meta(status: str) -> Tuple[str, str, str]:
            return {
                "waiting": ("等待重试", "info", "mdi-clock-outline"),
                "retrying": ("正在重试", "primary", "mdi-sync"),
                "forcing": ("强制上传中", "primary", "mdi-cloud-upload-outline"),
                "manual": ("人工处理", "error", "mdi-account-alert-outline"),
                "verify": ("待核验", "warning", "mdi-shield-search-outline"),
                "error": ("需处理", "error", "mdi-alert-circle-outline"),
                "stale": ("源文件变化", "warning", "mdi-file-alert-outline"),
            }.get(status, ("未知状态", "secondary", "mdi-help-circle-outline"))

        def stat_tile(label: str, value: int, color: str, icon: str) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 6, "sm": 3, "class": "py-1 px-2"},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "pa-3 rounded-lg", "color": color},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "d-flex align-center justify-space-between mb-2"},
                                "content": [
                                    {"component": "VIcon", "props": {"icon": icon, "size": "20"}},
                                    {"component": "span", "props": {"class": "text-caption text-medium-emphasis"}, "text": label},
                                ],
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-h5 font-weight-bold", "style": "line-height: 1.15;"},
                                "text": str(value),
                            },
                        ],
                    }
                ],
            }

        if not self._enabled:
            alert_type, alert_icon, alert_title = "warning", "mdi-power-off", "插件当前未启用"
            alert_text = "启用插件并保存配置后，MP 才会拦截本地到 115 的视频移动整理。"
        elif cooldown_remaining > 0:
            alert_type, alert_icon, alert_title = "warning", "mdi-shield-alert-outline", "115 风控冷却中"
            alert_text = (
                f"插件已暂停新的 115 请求，预计还需等待 "
                f"{self._cooldown_minutes(cooldown_remaining)} 分钟；队列任务会自动顺延。"
            )
        elif needs_action:
            alert_type, alert_icon, alert_title = "warning", "mdi-alert-outline", "有任务需要人工处理"
            alert_text = f"当前有 {needs_action} 个任务需要人工处理；执行 /115instant_force 后会忽略秒传检查并强制上传。"
        elif waiting or retrying:
            alert_type, alert_icon, alert_title = "info", "mdi-sync", "115 秒传保护正在工作"
            alert_text = f"当前有 {waiting} 个文件等待重试，{retrying} 个文件正在执行。普通上传已被阻止。"
        else:
            alert_type, alert_icon, alert_title = "success", "mdi-check-circle-outline", "队列为空，保护已就绪"
            alert_text = "暂无等待任务。新的未命中秒传文件会自动出现在这里。"

        rows = []
        for task in tasks[-30:]:
            status, color, icon = status_meta(str(task.get("status", "")))
            path = str(task.get("path", ""))
            reason = str(task.get("last_error", "") or "—")
            if task.get("status") == "forcing":
                next_at = "强制上传中"
            elif task.get("status") == "manual":
                next_at = "等待人工操作"
            elif task.get("status") == "retrying":
                next_at = "执行中"
            else:
                next_at = format_time(task.get("next_at"))
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "props": {"style": "max-width: 330px;"},
                            "content": [
                                {
                                    "component": "div",
                                    "props": {"class": "text-body-2 font-weight-medium text-truncate", "style": "max-width: 330px;"},
                                    "text": Path(path).name or path,
                                },
                                {
                                    "component": "div",
                                    "props": {"class": "text-caption text-medium-emphasis text-truncate", "style": "max-width: 330px;"},
                                    "text": path,
                                },
                            ],
                        },
                        {
                            "component": "td",
                            "content": [{"component": "VChip", "props": {"size": "small", "variant": "tonal", "color": color, "prepend-icon": icon}, "text": status}],
                        },
                        {"component": "td", "props": {"class": "text-body-2 text-no-wrap"}, "text": next_at},
                        {"component": "td", "props": {"class": "text-body-2 text-center"}, "text": str(task.get("attempts", 0))},
                        {"component": "td", "props": {"class": "text-caption text-medium-emphasis", "style": "max-width: 300px;"}, "text": reason},
                    ],
                }
            )

        if not rows:
            rows = [
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "props": {"colspan": 5, "class": "text-center py-8"},
                            "content": [
                                {"component": "VIcon", "props": {"icon": "mdi-inbox-outline", "size": "34", "color": "disabled"}},
                                {"component": "div", "props": {"class": "text-subtitle-2 text-medium-emphasis mt-2"}, "text": "暂无等待或人工任务"},
                                {"component": "div", "props": {"class": "text-caption text-disabled mt-1"}, "text": "未命中秒传的文件会在这里显示并按策略自动重试。"},
                            ],
                        }
                    ],
                }
            ]

        return [
            {
                "component": "VAlert",
                "props": {"type": alert_type, "variant": "tonal", "icon": alert_icon, "title": alert_title, "class": "mb-3"},
                "text": alert_text,
            },
            {
                "component": "VRow",
                "props": {"dense": True, "class": "mx-n2 mb-3"},
                "content": [
                    stat_tile("等待重试", waiting, "info", "mdi-clock-outline"),
                    stat_tile("正在执行", retrying, "primary", "mdi-sync"),
                    stat_tile("需人工处理", needs_action, "warning", "mdi-alert-outline"),
                    stat_tile("队列总数", len(tasks), "secondary", "mdi-format-list-bulleted"),
                ],
            },
            {
                "component": "VSheet",
                "props": {"class": "pa-3 mb-3 rounded-lg bg-grey-lighten-5", "border": True},
                "content": [
                    {"component": "div", "props": {"class": "text-subtitle-2 font-weight-bold mb-2"}, "text": "运行边界"},
                    {
                        "component": "VRow",
                        "props": {"dense": True},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4, "class": "py-1"},
                                "content": [{"component": "VListItem", "props": {"density": "compact", "prepend-icon": "mdi-file-video-outline", "title": "只保护视频主文件", "subtitle": "字幕和 NFO 仍走 MP 原生逻辑"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4, "class": "py-1"},
                                "content": [{"component": "VListItem", "props": {"density": "compact", "prepend-icon": "mdi-cloud-off-outline", "title": "未命中不启动普通上传", "subtitle": "源文件保留在本地"}}],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4, "class": "py-1"},
                                "content": [{"component": "VListItem", "props": {"density": "compact", "prepend-icon": "mdi-reload", "title": "可使用命令重试或强制上传", "subtitle": "/115instant_retry · /115instant_force"}}],
                            },
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "rounded-lg"},
                "content": [
                    {
                        "component": "VCardItem",
                        "props": {"class": "pb-2"},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "d-flex align-center justify-space-between w-100"},
                                "content": [
                                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1 font-weight-bold pa-0"}, "text": "等待与人工队列"},
                                    {"component": "VChip", "props": {"size": "small", "variant": "tonal", "color": "primary"}, "text": f"最近 {min(len(tasks), 30)} 条"},
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCardText",
                        "props": {"class": "pt-0 px-0"},
                        "content": [
                            {
                                "component": "VTable",
                                "props": {"density": "comfortable", "hover": True, "class": "w-100"},
                                "content": [
                                    {
                                        "component": "thead",
                                        "content": [
                                            {
                                                "component": "tr",
                                                "content": [
                                                    {"component": "th", "props": {"style": "min-width: 250px;"}, "text": "文件"},
                                                    {"component": "th", "props": {"style": "width: 120px;"}, "text": "状态"},
                                                    {"component": "th", "props": {"style": "width: 120px;"}, "text": "下次执行"},
                                                    {"component": "th", "props": {"style": "width: 70px;"}, "text": "次数"},
                                                    {"component": "th", "text": "最近原因"},
                                                ],
                                            }
                                        ],
                                    },
                                    {"component": "tbody", "content": rows},
                                ],
                            }
                        ],
                    },
                ],
            },
        ]

    @staticmethod
    def _field(value: Any, name: str, default: Any = None) -> Any:
        if isinstance(value, dict):
            return value.get(name, default)
        return getattr(value, name, default)

    @staticmethod
    def _set_field(value: Any, name: str, item: Any):
        if isinstance(value, dict):
            value[name] = item
        else:
            setattr(value, name, item)

    @staticmethod
    def _task_key(path: str) -> str:
        try:
            return str(Path(path).absolute())
        except (OSError, ValueError):
            return path

    @classmethod
    def _is_video(cls, path: Path) -> bool:
        return path.suffix.lower() in cls._VIDEO_EXTENSIONS

    @staticmethod
    def _signature(path: Path) -> Optional[Dict[str, int]]:
        try:
            stat = path.stat()
        except OSError:
            return None
        return {"size": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}

    def _save_tasks(self):
        self.save_data("tasks", self._tasks)

    def _notify_user(self, title: str, text: str):
        if self._notify:
            self.post_message(mtype=NotificationType.Manual, title=title, text=text)

    def _record_wait(self, local_path: Path, target_path: Path, reason: str):
        signature = self._signature(local_path)
        if not signature:
            return
        now = time.time()
        key = self._task_key(str(local_path))
        with self._lock:
            old = self._tasks.get(key, {})
            attempts = int(old.get("attempts", 0)) + 1
            first_at = float(old.get("first_at", now))
            manual = attempts > self._max_retry_attempts
            if attempts == 1:
                delay_minutes = self._first_retry_minutes
            elif attempts == 2:
                delay_minutes = self._second_retry_minutes
            else:
                delay_minutes = self._later_retry_minutes
            next_at = now + delay_minutes * 60
            last_error = reason
            if manual:
                retry_count = max(attempts - 1, 0)
                last_error = f"{reason}；已完成 {retry_count} 次重试，已转人工处理"
            self._tasks[key] = {
                "path": str(local_path),
                "target_path": str(target_path),
                "signature": signature,
                "status": "manual" if manual else "waiting",
                "attempts": attempts,
                "first_at": first_at,
                "next_at": None if manual else next_at,
                "history_id": old.get("history_id"),
                "updated_at": now,
                "last_error": last_error,
            }
            self._save_tasks()
        if manual:
            logger.warning(f"[115Instant] {local_path.name} 已达到最大重试次数，转人工处理")
            self._notify_user(
                "115 秒传转人工处理",
                f"{local_path.name}\n已完成 {max(attempts - 1, 0)} 次重试\n可执行 /115instant_force 强制上传",
            )
        else:
            logger.warning(f"[115Instant] {local_path.name} 未命中秒传，{delay_minutes} 分钟后重试")
        if attempts == 1 and not manual:
            self._notify_user("115 秒传等待中", f"{local_path.name}\n{reason}\n源文件已保留")

    def _record_state(self, local_path: Path, status: str, reason: str):
        key = self._task_key(str(local_path))
        with self._lock:
            task = self._tasks.get(key, {})
            task.update({"path": str(local_path), "status": status, "last_error": reason, "updated_at": time.time()})
            self._tasks[key] = task
            self._save_tasks()
        self._notify_user("115 秒传整理需人工处理", f"{local_path.name}\n{reason}")

    def _protected(self, local_path: Path) -> bool:
        context = getattr(self._context, "value", None)
        if context:
            return (
                context.get("source_storage") == "local"
                and context.get("target_storage") == "u115"
                and context.get("transfer_type") == "move"
                and self._is_video(local_path)
            )
        # 没有拦截上下文时对视频文件保持保护，避免插件失效时意外普通上传。
        return self._is_video(local_path)

    def _build_file_item(self, target_path: Path, info: Dict[str, Any]):
        category = str(info.get("file_category", "1"))
        name = str(info.get("file_name") or target_path.name)
        return schemas.FileItem(
            storage="u115",
            fileid=str(info.get("file_id")) if info.get("file_id") is not None else None,
            path=target_path.as_posix(),
            type="file" if category == "1" else "dir",
            name=name,
            basename=Path(name).stem,
            extension=Path(name).suffix[1:] if category == "1" else None,
            pickcode=info.get("pick_code"),
            size=info.get("size"),
            modify_time=info.get("utime"),
        )

    def _instant_upload(self, original: U115Pan, target_dir, local_path: Path, new_name: Optional[str]):
        local_path = Path(local_path)
        context = getattr(self._context, "value", None)
        force_upload = isinstance(context, dict) and context.get("force_upload")
        try:
            with self._u115_lock:
                if force_upload:
                    # 人工处理明确要求放弃秒传拦截，交回 MP 原生上传流程。
                    if self._cooldown_remaining(original) > 0:
                        logger.warning(
                            f"[115Instant] {local_path.name} 暂停强制上传，等待风控冷却结束"
                        )
                        return None
                    return original.upload(target_dir, local_path, new_name)
                if not self._enabled or not self._protected(local_path):
                    return original.upload(target_dir, local_path, new_name)

                if self._cooldown_remaining(original) > 0:
                    remaining = self._cooldown_remaining(original)
                    self._record_wait(
                        local_path,
                        Path(target_dir.path) / (new_name or local_path.name),
                        f"115 风控冷却中，已顺延 {self._cooldown_minutes(remaining)} 分钟",
                    )
                    return None

                target_name = new_name or local_path.name
                target_path = Path(target_dir.path) / target_name
                file_size = local_path.stat().st_size
                file_sha1 = original._calc_sha1(local_path)
                file_preid = original._calc_sha1(local_path, 128 * 1024 * 1024)
                target_cid = target_dir.fileid
                if not target_cid:
                    self._record_state(local_path, "error", "115 目标目录缺少 fileid")
                    return None

                init_data = {
                    "file_name": target_name,
                    "file_size": file_size,
                    "target": f"U_1_{target_cid}",
                    "fileid": file_sha1,
                    "preid": file_preid,
                }
                init_resp = self._request_115(
                    original,
                    "POST",
                    "/open/upload/init",
                    data=init_data,
                )
                if not init_resp or not init_resp.get("state"):
                    if self._cooldown_remaining(original) > 0:
                        self._record_wait(
                            local_path,
                            target_path,
                            f"115 请求失败，已进入风控冷却，稍后重试",
                        )
                    else:
                        self._record_state(
                            local_path,
                            "error",
                            "115 秒传预检接口失败，请检查登录或网络",
                        )
                    return None

                init_result = init_resp.get("data") or {}
                if init_result.get("code") in (700, 701) and init_result.get("sign_check"):
                    sign_check = str(init_result["sign_check"]).split("-")
                    if len(sign_check) != 2:
                        self._record_state(local_path, "error", "115 二次认证范围无效")
                        return None
                    start, end = int(sign_check[0]), int(sign_check[1])
                    with local_path.open("rb") as handle:
                        handle.seek(start)
                        sign_value = hashlib.sha1(handle.read(end - start + 1)).hexdigest().upper()
                    init_data.update(
                        {
                            "pick_code": init_result.get("pick_code"),
                            "sign_key": init_result.get("sign_key"),
                            "sign_val": sign_value,
                        }
                    )
                    init_resp = self._request_115(
                        original,
                        "POST",
                        "/open/upload/init",
                        data=init_data,
                    )
                    if not init_resp or not init_resp.get("state"):
                        if self._cooldown_remaining(original) > 0:
                            self._record_wait(
                                local_path,
                                target_path,
                                "115 二次认证请求进入风控冷却，稍后重试",
                            )
                        else:
                            self._record_state(local_path, "error", "115 二次认证失败")
                        return None
                    init_result = init_resp.get("data") or {}

                if init_result.get("status") != 2:
                    self._record_wait(local_path, target_path, self._MARKER)
                    return None

                file_id = init_result.get("file_id")
                if file_id:
                    info = self._request_115(
                        original,
                        "GET",
                        "/open/folder/get_info",
                        "data",
                        params={"file_id": int(file_id)},
                    )
                    if info:
                        logger.info(f"[115Instant] {target_name} 秒传成功")
                        return self._build_file_item(target_path, info)
                remote_item = original.get_item(target_path)
                if remote_item:
                    logger.info(f"[115Instant] {target_name} 秒传成功")
                    return remote_item
                self._record_state(local_path, "verify", "115 已返回秒传成功，但目标文件暂不可核验")
                return None
        except (OSError, ValueError, TypeError) as exc:
            self._record_state(local_path, "error", f"115 秒传预检异常：{exc}")
            return None
        finally:
            self._context.value = None

    def _resolve_history_id(self, task: Dict[str, Any]) -> Optional[int]:
        if task.get("history_id"):
            return int(task["history_id"])
        try:
            history = TransferHistoryOper().get_by_src(task["path"], "local")
            if history and not getattr(history, "status", True) and self._MARKER in (history.errmsg or ""):
                return int(history.id)
        except Exception as exc:
            logger.debug(f"[115Instant] 查询失败历史记录失败：{exc}")
        return None

    def force_upload_pending(self):
        """强制上传一条人工任务，跳过插件的秒传预检。"""
        if not self._enabled:
            return
        try:
            if not self._u115_lock.acquire(blocking=False):
                logger.info("[115Instant] 已有 115 整理正在执行，人工强制上传顺延")
                return
            try:
                pending_statuses = {"manual", "stale", "error", "verify"}
                selected = None
                with self._lock:
                    for key, task in self._tasks.items():
                        if task.get("status") in pending_statuses:
                            selected = (key, dict(task))
                            break
                if not selected:
                    self._notify_user("115 秒传暂无人工任务", "当前没有可强制上传的等待任务")
                    return

                key, task = selected
                source = Path(task["path"])
                if not self._signature(source):
                    self._record_state(source, "stale", "源文件已不存在，无法强制上传")
                    return
                history_id = self._resolve_history_id(task)
                if not history_id:
                    self._record_state(source, "error", "找不到整理历史记录，无法强制上传")
                    return

                original = self._get_u115_target()
                remaining = self._cooldown_remaining(original)
                if remaining > 0:
                    reason = (
                        f"115 风控冷却中，剩余 {self._cooldown_minutes(remaining)} 分钟；"
                        "人工强制上传已保留"
                    )
                    with self._lock:
                        current = self._tasks.get(key)
                        if current:
                            current.update({"last_error": reason, "updated_at": time.time()})
                            self._save_tasks()
                    self._notify_user("115 仍在风控冷却", f"{source.name}\n{reason}")
                    return

                with self._lock:
                    current = self._tasks.get(key)
                    if not current or current.get("status") not in pending_statuses:
                        return
                    current.update({
                        "status": "forcing",
                        "history_id": history_id,
                        "updated_at": time.time(),
                        "last_error": "人工确认强制上传，已跳过秒传预检",
                    })
                    self._save_tasks()

                self._context.value = {"force_upload": True, "path": str(source)}
                try:
                    try:
                        state, message = TransferChain().redo_transfer_history(history_id)
                    except Exception as exc:
                        state, message = False, f"强制上传异常：{exc}"
                finally:
                    self._context.value = None

                if state:
                    with self._lock:
                        self._tasks.pop(key, None)
                        self._save_tasks()
                    self._notify_user("115 强制上传完成", source.name)
                    return

                with self._lock:
                    current = self._tasks.get(key)
                    if current and current.get("status") == "forcing":
                        current.update({
                            "status": "manual",
                            "next_at": None,
                            "last_error": f"强制上传失败：{message or '未知错误'}",
                            "updated_at": time.time(),
                        })
                        self._save_tasks()
                self._notify_user("115 强制上传失败", f"{source.name}\n{message or '未知错误'}")
            finally:
                self._u115_lock.release()
        except Exception as exc:
            logger.error(f"[115Instant] 人工强制上传处理异常：{exc}")

    def retry_pending(self, force: bool = False):
        if not self._enabled:
            return
        if not self._u115_lock.acquire(blocking=False):
            logger.debug("[115Instant] 已有 115 整理正在执行，跳过本轮自动重试")
            return
        try:
            now = time.time()
            due: List[Tuple[str, Dict[str, Any]]] = []
            with self._lock:
                for key, task in self._tasks.items():
                    if task.get("status") != "waiting":
                        continue
                    if not force and float(task.get("next_at", 0)) > now:
                        continue
                    due.append((key, dict(task)))

            if not due:
                return

            key, task = due[0]
            original = self._get_u115_target()
            remaining = self._cooldown_remaining(original)
            if remaining > 0:
                self._defer_task(
                    key,
                    now + remaining,
                    f"115 风控冷却中，已顺延 {self._cooldown_minutes(remaining)} 分钟",
                )
                return

            source = Path(task["path"])
            signature = self._signature(source)
            if not signature:
                self._record_state(source, "stale", "源文件已不存在")
                return
            if signature != task.get("signature"):
                self._record_state(source, "stale", "源文件大小或修改时间已变化")
                return
            first_at = float(task.get("first_at", now))
            if now - first_at > self._max_wait_hours * 3600:
                self._record_state(source, "stale", "已超过最大等待时间，请人工执行强制上传")
                return
            history_id = self._resolve_history_id(task)
            if not history_id:
                self._defer_task(key, now + 60, "等待 MP 生成整理历史记录")
                return
            manual = False
            with self._lock:
                current = self._tasks.get(key)
                if not current or current.get("status") != "waiting":
                    return
                current["status"] = "retrying"
                current["history_id"] = history_id
                current["updated_at"] = now
                self._save_tasks()

            self._context.value = {"retrying": True, "path": str(source)}
            try:
                try:
                    state, message = TransferChain().redo_transfer_history(history_id)
                except Exception as exc:
                    state, message = False, f"重试异常：{exc}"
            finally:
                self._context.value = None

            if state:
                with self._lock:
                    self._tasks.pop(key, None)
                    self._save_tasks()
                self._notify_user("115 秒传整理完成", source.name)
                return

            with self._lock:
                current = self._tasks.get(key)
                if current and current.get("status") == "retrying":
                    attempts = int(current.get("attempts", 0)) + 1
                    manual = attempts > self._max_retry_attempts
                    current.update({
                        "status": "manual" if manual else "waiting",
                        "attempts": attempts,
                        "next_at": None if manual else time.time() + self._later_retry_minutes * 60,
                        "last_error": str(message or "再次未命中秒传"),
                        "updated_at": time.time(),
                    })
                    self._save_tasks()
            if manual:
                self._notify_user(
                    "115 秒传转人工处理",
                    f"{source.name}\n已达到最大重试次数，可执行 /115instant_force 强制上传",
                )
        finally:
            self._u115_lock.release()

