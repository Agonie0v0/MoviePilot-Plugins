"""115 秒传等待 — MoviePilot V2 plugin entry point."""
from pathlib import Path

from fastapi import HTTPException

from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType

from .bridge import InstantWaitEngine
from .store import QueueStore
from .ui import config_form, task_page


DEFAULTS = {
    "enabled": False,
    "extensions": "mkv,mp4,m4v,avi,mov,wmv,ts,m2ts,mpg,mpeg,iso,flv,webm",
    "retry_intervals": "60,180,600,1800",
    "max_wait_hours": 24,
    "max_retries": 3,
    "notify": True,
    "task_id": "",
    "action": "pause",
    "apply_action": False,
}


class P115InstantWait(_PluginBase):
    plugin_name = "115秒传等待"
    plugin_desc = "内置115整理限次等待秒传，支持手动强制上传并更新原整理记录"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot-Frontend/refs/heads/v2/src/assets/images/misc/u115.png"
    plugin_version = "0.2.2"
    plugin_author = "Agonie"
    author_url = "https://github.com/Agonie0v0/MoviePilot-Plugins"
    plugin_config_prefix = "p115instantwait_"
    plugin_order = 30
    auth_level = 1

    def __init__(self):
        super().__init__()
        self._engine = None
        self._config = dict(DEFAULTS)
        self._error = ""

    def init_plugin(self, config=None):
        self.stop_service()
        self._config = {**DEFAULTS, **(config or {})}
        self._error = ""
        if not self._config["enabled"]:
            return
        try:
            from version import APP_VERSION
            if APP_VERSION != "v2.15.6":
                raise ValueError(f"当前支持 V2.15.6，检测到 {APP_VERSION}；需先验证版本兼容性")
            hours = float(self._config["max_wait_hours"])
            if not 0 <= hours <= 8760:
                raise ValueError("等待时限必须在 0 到 8760 小时之间，0 表示不限时")
            delays = [int(v.strip()) for v in self._config["retry_intervals"].split(",")]
            if not delays or any(v < 30 or v > 86400 for v in delays):
                raise ValueError("每个重试间隔必须在 30 到 86400 秒之间")
            retries = float(self._config["max_retries"])
            if not retries.is_integer() or not 0 <= retries <= 100:
                raise ValueError("自动重试次数必须是 0 到 100 的整数；0 表示首次未成功就暂停")
            if not self._config["extensions"].strip():
                raise ValueError("请配置需要接管的文件扩展名")
            runtime = {**self._config, "max_wait_hours": hours, "max_retries": int(retries), "retry_delays": delays}
            engine = InstantWaitEngine(self.get_data_path(), runtime, self._notify)
            engine.install()
            self._engine = engine
            if self._config["apply_action"]:
                engine.control(self._config["task_id"], self._config["action"])
        except Exception as exc:
            self._error = str(exc)
            logger.error(f"【115秒传等待】{self._error}")
            self.systemmessage.put(f"115秒传等待：{self._error}")
        finally:
            if self._config["apply_action"]:
                self._config["apply_action"] = False
                self.update_config(self._config)

    def _notify(self, title, text, manual=True):
        if not self._config["notify"]:
            return False
        # The system-message bell does not dispatch to MP notification channels.
        # Keep it as a local record, and independently submit the actual push.
        try:
            self.systemmessage.put(f"{title}\n{text}")
        except Exception:
            logger.warning("【115秒传等待】站内系统消息写入失败，仍尝试提交渠道通知")
        self.post_message(title=title, text=text,
                          mtype=NotificationType.Manual if manual else NotificationType.Organize)
        return True

    def get_state(self):
        return bool(self._engine and not self._engine.stop_event.is_set())

    @staticmethod
    def get_command():
        return []

    def get_api(self):
        return [
            {"path": "/tasks", "endpoint": self.list_tasks, "methods": ["GET"],
             "auth": "bear", "summary": "查看秒传等待队列"},
            {"path": "/tasks/{task_id}/{action}", "endpoint": self.control_task,
             "methods": ["POST"], "auth": "bear", "summary": "暂停、继续等待、强制上传或取消任务"},
        ]

    def queue(self):
        return self._engine.store if self._engine else QueueStore(Path(self.get_data_path()) / "queue.db")

    def list_tasks(self):
        result = []
        for row in self.queue().all():
            task = row["payload"]["task"]
            result.append({"id": row["id"], "history_id": row["history_id"],
                "source": task["fileitem"]["path"], "target": row["payload"]["final_path"],
                "state": row["state"], "attempts": row["attempts"], "next_at": row["next_at"],
                "auto_attempts": row["payload"].get("auto_attempts", row["attempts"]),
                "message": row["message"], "backup_files": row["payload"].get("backups", [])})
        return result

    def control_task(self, task_id: str, action: str):
        if not self.get_state():
            raise HTTPException(status_code=409, detail="请先启用插件")
        if action not in ("pause", "resume", "cancel", "upload"):
            raise HTTPException(status_code=400, detail="不支持此操作")
        try:
            row = self._engine.control(task_id, action)
            return {"success": True, "state": row["state"]}
        except KeyError:
            raise HTTPException(status_code=404, detail="任务不存在") from None
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from None

    def get_form(self):
        return config_form(self.list_tasks(), self._error), dict(DEFAULTS)

    def get_page(self):
        return task_page(self.list_tasks(), self.get_state(), self._error,
                         max_retries=int(self._engine.config["max_retries"]) if self._engine else None)

    def stop_service(self):
        if self._engine:
            self._engine.stop()
            self._engine = None
