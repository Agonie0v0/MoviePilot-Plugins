"""115 秒传等待 — MoviePilot V3 plugin entry point.

不再按 MoviePilot 版本号拒绝启用：整理接口的兼容性由 ``InstantWaitEngine.install()``
在装载时逐项探测（所需方法是否存在、执行合同签名是否匹配），缺失或变化时才拒绝，
并给出具体原因。
"""
from pathlib import Path
from datetime import datetime

from fastapi import Body, HTTPException

from app.sdk.logging import logger
from app.sdk.plugin import _PluginBase
from app.schemas.types import MessageType as NotificationType
from pydantic import BaseModel

from .bridge import InstantWaitEngine
from .store import QueueStore
from .records import task_record
from .ui import config_form, task_page, record_choices, record_search_model


DEFAULTS = {
    "enabled": False,
    "extensions": "mkv,mp4,m4v,avi,mov,wmv,ts,m2ts,mpg,mpeg,iso,flv,webm",
    "retry_intervals": "60,180,600,1800",
    "max_wait_hours": 24,
    "max_retries": 3,
    "limit_action": "manual",
    "notify": True,
    "task_id": "",
    "task_ids": [],
    "action": "pause",
    "apply_action": False,
    "cleanup_history": False,
    "history_mode": "selected",
    "history_ids": [],
    "history_states": ["completed"],
    "history_days": 30,
}


class QueueTask(BaseModel):
    """V3 插件队列 API 的显式输出合同。"""
    id: str
    history_id: int | None
    source: str
    target: str
    state: str
    attempts: int
    next_at: float
    auto_attempts: int
    message: str
    backup_files: list[dict]
    created: float | None = None
    updated: float | None = None
    transfer_method: str = "unknown"
    result_message: str = ""


class ControlResponse(BaseModel):
    """任务操作仍返回裸 JSON 成功状态。"""
    success: bool
    state: str


class RecordChoice(BaseModel):
    title: str
    value: str
    props: dict[str, bool]


class RecordSearchResponse(BaseModel):
    items: list[RecordChoice]
    total: int
    page: int
    pages: int


class BatchItem(BaseModel):
    id: str
    name: str
    status: str
    message: str


class BatchResponse(BaseModel):
    action: str
    at: str
    accepted: int
    skipped: int
    failed: int
    items: list[BatchItem]
    notice: str = ""


class P115InstantWait(_PluginBase):
    plugin_name = "115秒传等待"
    plugin_desc = "内置115整理等待秒传，到达上限可自动上传或手动处理，更新原整理记录"
    plugin_icon = "https://raw.githubusercontent.com/Agonie0v0/MoviePilot-Plugins/main/icons/p115instantwait.png"
    plugin_version = "1.2.1"
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
        self._config = {**DEFAULTS, **{k: v for k, v in (config or {}).items() if not k.startswith("_records_")}}
        self._error = ""
        apply_action = self._config["apply_action"]
        cleanup_history = self._config["cleanup_history"]
        history_ids = self._config["history_ids"]
        # Accept an old single-task form, but an explicitly empty new selection
        # must not fall back to a stale task_id from a previous version.
        selected = self._config["task_ids"] if "task_ids" in (config or {}) else ([self._config["task_id"]] if self._config["task_id"] else [])
        try:
            if apply_action or cleanup_history or any(k.startswith("_records_") for k in (config or {})):
                # Consume the one-shot request before mutating any task. A
                # restart must never repeat uploads/cancels from saved config.
                self._config.update(apply_action=False, task_ids=[], task_id="",
                                    cleanup_history=False, history_ids=[])
                if not self.update_config(self._config):
                    raise ValueError("无法保存一次性操作标记，本次未执行，请重试保存")
            if cleanup_history:
                try:
                    result = self.queue().clear_history(mode=self._config["history_mode"], keys=history_ids,
                        states=self._config["history_states"], days=self._config["history_days"])
                except Exception as exc:
                    self._error = str(exc) if isinstance(exc, ValueError) else "记录清理异常，已回滚本次删除，请查看日志"
                    result = dict(kind="history", status="failed", deleted=0, skipped=0, items=[self._error])
                    logger.warning(f"【115秒传等待】{self._error}")
                self._save_maintenance("last_history_cleanup", result)
            if not self._config["enabled"]:
                if apply_action:
                    raise ValueError("请先启用插件，再执行批量操作")
                return
            hours = float(self._config["max_wait_hours"])
            if not 0 <= hours <= 8760:
                raise ValueError("等待时限必须在 0 到 8760 小时之间，0 表示不限时")
            delays = [int(v.strip()) for v in self._config["retry_intervals"].split(",")]
            if not delays or any(v < 30 or v > 86400 for v in delays):
                raise ValueError("每个重试间隔必须在 30 到 86400 秒之间")
            retries = float(self._config["max_retries"])
            if not retries.is_integer() or not 0 <= retries <= 100:
                raise ValueError("自动重试次数必须是 0 到 100 的整数；0 表示首次未成功就执行上限策略")
            if self._config["limit_action"] not in ("manual", "upload"):
                raise ValueError("达到上限后的操作必须为手动处理或强制上传")
            if not self._config["extensions"].strip():
                raise ValueError("请配置需要接管的文件扩展名")
            runtime = {**self._config, "max_wait_hours": hours, "max_retries": int(retries), "retry_delays": delays}
            engine = InstantWaitEngine(self.get_data_path(), runtime, self._notify)
            engine.install(before_start=(lambda: self._apply_actions(engine, selected, self._config["action"])) if apply_action else None)
            self._engine = engine
        except Exception as exc:
            self._error = str(exc)
            logger.error(f"【115秒传等待】{self._error}")
            self.systemmessage.put(f"115秒传等待：{self._error}")

    def _save_maintenance(self, key, result):
        result = {**result, "at": datetime.now().astimezone().isoformat(timespec="seconds")}
        try:
            self.save_data(key, result)
        except Exception:
            logger.warning("【115秒传等待】清理结果保存失败，请查看日志")
        if result["status"] != "running":
            logger.info(f"【115秒传等待】清理结果 | 类型={result['kind']} | 状态={result['status']} | 删除={result['deleted']} | 跳过={result.get('skipped', result.get('retained', 0))} | 异常={result.get('failed', 0)}")

    def _apply_actions(self, engine, selected, action):
        try:
            result = engine.control_many(selected, action)
        except ValueError as exc:
            self._error = str(exc)
            logger.warning(f"【115秒传等待】批量操作未执行：{self._error}")
            return
        try:
            self.save_data("last_batch_result", result)
            self.systemmessage.put(f"115秒传等待批量操作：已接受 {result['accepted']}，跳过 {result['skipped']}，异常 {result['failed']}。详情见配置页「批量任务操作」。")
        except Exception:
            self._error = "批量操作已处理，但结果提示保存失败，请查看任务状态与日志"
            logger.warning(f"【115秒传等待】{self._error}")

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
             "auth": "bear", "summary": "查看秒传等待队列", "response_model": list[QueueTask]},
            {"path": "/records", "endpoint": self.search_records, "methods": ["GET"],
             "auth": "bear", "summary": "全库搜索任务与历史，每页50条", "response_model": RecordSearchResponse},
            {"path": "/batch/{action}", "endpoint": self.control_batch, "methods": ["POST"],
             "auth": "bear", "summary": "批量恢复等待或安排强制上传", "response_model": BatchResponse},
            {"path": "/tasks/{task_id}/{action}", "endpoint": self.control_task,
             "methods": ["POST"], "auth": "bear", "summary": "暂停、继续等待、强制上传或取消任务",
             "response_model": ControlResponse},
        ]

    def queue(self):
        return self._engine.store if self._engine else QueueStore(Path(self.get_data_path()) / "queue.db")

    def list_tasks(self):
        return [task_record(row) for row in self.queue().all(limit=50)]

    def search_records(self, scope: str = "history", query: str = "", state: str = "", page: int = 1):
        try:
            result = self.queue().search(scope=scope, query=query, state=state, page=page)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        return {k: v for k, v in result.items() if k != "rows"} | {
            "items": record_choices([task_record(row) for row in result["rows"]])}

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
        model = {**DEFAULTS, "task_ids": [], "history_ids": []}
        for scope in ("tasks", "history"):
            model.update(record_search_model(scope, self.search_records(scope=scope)))
        return config_form([], self._error, self.get_data("last_batch_result"),
                           history_result=self.get_data("last_history_cleanup")), model

    def control_batch(self, action: str, keys: list[str] = Body(..., embed=True)):
        if not self.get_state():
            raise HTTPException(status_code=409, detail="请先启用插件")
        if action not in ("resume", "upload"):
            raise HTTPException(status_code=400, detail="不支持此批量操作")
        try:
            # IDs are the reviewed page snapshot, never an open-ended 'all'.
            # control_many validates the list and rechecks each current state.
            result = self._engine.control_many(keys, action)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        try:
            self.save_data("last_batch_result", result)
        except Exception:
            logger.warning("【115秒传等待】批量操作结果保存失败，请核对队列状态与日志")
            self._error = "批量操作已提交，但结果未能保存；请核对任务状态与日志。"
            result["notice"] = self._error
        return result

    def get_page(self):
        rows, summary = self.queue().dashboard()
        return task_page([task_record(row) for row in rows], self.get_state(), self._error, summary=summary,
                         max_retries=int(self._engine.config["max_retries"]) if self._engine else None,
                         batch_result=self.get_data("last_batch_result"))

    def stop_service(self):
        if self._engine:
            self._engine.stop()
            self._engine = None
