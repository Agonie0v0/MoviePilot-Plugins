"""V3.1 adapter: stage uploads outside MP, then replay its durable plan/settlement.

The host owns media snapshots, leases, execution steps and history writes. This
adapter never imports host ORM models or session factories. Narrow guarded hooks
are necessary because V3 currently has no public asynchronous upload gate.
"""
import inspect
import random
import threading
import time
from datetime import datetime
from pathlib import Path, PurePosixPath

from app.chain.transfer import TransferChain
from app.modules.filemanager.storages.local import LocalStorage
from app.modules.filemanager.storages.u115 import U115Pan
from app.schemas.file import FileItem
from app.schemas.transfer import TransferInfo
from app.sdk.logging import logger

from .remote import NotInstant, OpenAPI, PauseTask, PreparedStorage, RetryLater, fingerprint, hash_file
from .store import QueueStore


WAIT_MESSAGE = "等待秒传，由115秒传等待插件后台处理"


def log_time(value):
    """Render host-local, timezone-qualified timestamps for tracing."""
    return datetime.fromtimestamp(value).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def log_text(value):
    """Keep external filenames and messages on one bounded log line."""
    return " ".join(str(value).split())[:400]


def source_key(item):
    """Match one local source without discarding its storage identity."""
    return f"{item.storage}:{Path(item.path).resolve()}"


class InstantWaitEngine:
    """Maintain a plugin-owned staging queue with native V3 final settlement."""

    def __init__(self, data_path, config, notify=None):
        self.store = QueueStore(Path(data_path) / "queue.db")
        self.config, self.notify = config, notify
        self.chain = TransferChain()
        self.stop_event, self.wake, self.upload_wake = threading.Event(), threading.Event(), threading.Event()
        self.worker = self.upload_worker = None
        self.patches = []
        self.live_tasks = {}
        self.context = threading.local()
        self.native_lock = threading.Condition()
        self.native_active = 0
        self.extensions = {e.strip().lower().lstrip(".") for e in config["extensions"].split(",") if e.strip()}

    def log_task(self, row, event, level="info", **details):
        """Correlate all queue transitions with the original native history ID."""
        fields = {"任务": row["id"], "整理记录": row["history_id"] or "待生成",
                  "文件": PurePosixPath(row["payload"]["task"]["fileitem"]["path"].replace("\\", "/")).name,
                  "已尝试": row["attempts"],
                  "本轮自动尝试": f"{row['payload'].get('auto_attempts', row['attempts'])}/{self.config['max_retries'] + 1}", **details}
        getattr(logger, level)(f"【115秒传等待】{event} | " + " | ".join(f"{k}={log_text(v)}" for k, v in fields.items()))

    def notify_task(self, row, manual=True):
        """Use the host notification adapter without letting channel errors retry work."""
        if not self.notify:
            return
        auto_upload = row["state"] == "upload_queued"
        title = "已安排自动上传" if auto_upload else "需要手动处理" if manual else "整理暂未成功"
        text = f"文件：{Path(row['payload']['task']['fileitem']['path']).name}\n整理记录：#{row['history_id'] or '待生成'}\n原因：{log_text(row['message'])}\n累计尝试：{row['attempts']} 次\n任务 ID：{row['id']}"
        if row["state"] == "waiting":
            text += f"\n下次重试：{log_time(row['next_at'])}"
        text += "\n已进入独立上传队列，先试秒传，未命中上传正文。" if auto_upload else "\n请在插件中强制上传或继续等待秒传。" if manual else "\n后台继续等待，不影响其他整理。"
        try:
            submitted = self.notify("115秒传等待 · " + title, text, manual=manual)
            self.log_task(row, "通知已关闭" if submitted is False else "通知已提交 MP")
        except Exception:
            self.log_task(row, "通知提交失败", level="warning")

    def patch(self, name, replacement):
        """Reject foreign patches and remember inherited method ownership."""
        original = getattr(TransferChain, name)
        if getattr(original, "__instant_wait_owner__", None) or not original.__module__.startswith("app.chain.transfer"):
            raise RuntimeError(f"整理接口 {name} 已被其他插件接管")
        own = name in TransferChain.__dict__
        replacement.__instant_wait_owner__ = self
        self.patches.append((name, own, original, replacement))
        setattr(TransferChain, name, replacement)
        return original

    def install(self, before_start=None):
        """Check V3 contracts before installing reversible gates or starting workers."""
        gate = "_TransferChain__execute_host_transfer_plan"
        callback = "_TransferChain__default_callback"
        required = (gate, callback, "_TransferChain__handle_transfer", "_finish_scrape_batch_task", "queue_failed_transfer_notification", "redo_transfer_history", "_TransferChain__cleanup_transfer_destination")
        if not all(callable(getattr(TransferChain, n, None)) for n in required):
            raise RuntimeError("V3 整理接口发生变化，未启用接管")
        if not {"task", "checkpoint", "step_runner", "source_oper", "target_oper"} <= set(inspect.signature(getattr(TransferChain, gate)).parameters):
            raise RuntimeError("V3 整理执行合同发生变化，未启用接管")
        if not all(callable(getattr(self.chain.transfer_history_repository, n, None)) for n in ("get", "get_by_transfer_task_id")):
            raise RuntimeError("V3 整理历史查询合同不完整")
        engine = self
        original_gate = getattr(TransferChain, gate)
        original_callback = getattr(TransferChain, callback)
        original_handle = getattr(TransferChain, "_TransferChain__handle_transfer")
        original_notify = TransferChain.queue_failed_transfer_notification
        self.original_finish = TransferChain._finish_scrape_batch_task
        original_cleanup = TransferChain._TransferChain__cleanup_transfer_destination

        def wrapped_cleanup(chain, fileitem):
            proxy = getattr(engine.context, "prepared_storage", None)
            if proxy is None:
                return original_cleanup(chain, fileitem)
            if fileitem.storage != "u115":
                raise PauseTask("旧目标存储不一致，拒绝自动清理")
            current = proxy.api.get_item(fileitem.path)
            return True if current is None else proxy.delete(current)

        def wrapped_gate(chain, task, checkpoint, *, source_oper, target_oper, step_runner):
            if not engine.in_scope(task, checkpoint):
                return original_gate(chain, task, checkpoint, source_oper=source_oper, target_oper=target_oper, step_runner=step_runner)
            return engine.gate(task, checkpoint, source_oper, target_oper, step_runner, original_gate)

        def wrapped_callback(chain, task, info, /):
            row = engine.store.active_for(source_key(task.fileitem))
            owned = row and row["payload"].get("native_task_id") == task.admission_task_id
            if owned and info.success:
                try:
                    engine.verify_result(row, info)
                except Exception:
                    engine.defer(row, "paused", "完成前校验失败，已阻止成功回写；请核对远端与 V3 整理队列")
                    raise
            result = original_callback(chain, task, info)
            if owned:
                history = (chain.transfer_history_repository.get(row["history_id"]) if row["history_id"]
                           else chain.transfer_history_repository.get_by_transfer_task_id(task_id=task.admission_task_id))
                if not history:
                    raise RuntimeError("V3 未返回原整理记录")
                if row["history_id"] and history.id != row["history_id"]:
                    engine.defer(row, "paused", "V3 原整理记录 ID 发生变化，请核对")
                    raise RuntimeError("V3 整理记录关联发生变化")
                engine.store.update(row["id"], history_id=history.id)
                if info.success and result[0] and history.status:
                    engine.store.update(row["id"], state="completed", message="远端确认且 V3 原整理记录已成功")
                    engine.log_task(engine.store.get(row["id"]), "整理成功")
                    engine.original_finish(chain, task)
                    engine.live_tasks.pop(row["id"], None)
                elif row["payload"].get("prepared"):
                    engine.defer(row, "paused", info.message or "V3 完成整理失败，请人工核对")
            return result

        def wrapped_handle(chain, task, callback=None):
            with engine.native_lock:
                engine.native_active += 1
            try:
                return original_handle(chain, task, callback)
            finally:
                try:
                    row = engine.store.active_for(source_key(task.fileitem))
                    if row and row["payload"].get("native_task_id") == task.admission_task_id:
                        history = chain.transfer_history_repository.get_by_transfer_task_id(task_id=task.admission_task_id)
                        if history and not history.status and history.transfer_task_id == task.admission_task_id:
                            engine.store.update(row["id"], history_id=history.id, ready=1)
                            engine.wake.set()
                finally:
                    with engine.native_lock:
                        engine.native_active -= 1
                        engine.native_lock.notify_all()

        def wrapped_notify(chain, *, task, transferinfo, **kwargs):
            if transferinfo.message == WAIT_MESSAGE:
                return
            return original_notify(chain, task=task, transferinfo=transferinfo, **kwargs)

        def wrapped_finish(chain, task):
            if engine.store.active_for(source_key(task.fileitem)):
                return
            return engine.original_finish(chain, task)

        try:
            self.patch(gate, wrapped_gate)
            self.patch(callback, wrapped_callback)
            self.patch("_TransferChain__handle_transfer", wrapped_handle)
            self.patch("queue_failed_transfer_notification", wrapped_notify)
            self.patch("_finish_scrape_batch_task", wrapped_finish)
            self.patch("_TransferChain__cleanup_transfer_destination", wrapped_cleanup)
            self.restore()
            if before_start:
                before_start()
            self.worker = threading.Thread(target=self.run, name="p115-v3-wait", daemon=True)
            self.upload_worker = threading.Thread(target=self.run, kwargs={"manual": True}, name="p115-v3-upload", daemon=True)
            self.worker.start()
            self.upload_worker.start()
        except Exception:
            self.stop()
            raise

    def in_scope(self, task, checkpoint):
        """Only gate local video files in a single-file native u115 copy/move plan."""
        return (not task.preview and not checkpoint.skip_reason and not checkpoint.rejection_error
                and task.fileitem.storage == "local" and checkpoint.target_storage == "u115"
                and Path(task.fileitem.path).suffix.lower().lstrip(".") in self.extensions)

    def gate(self, task, checkpoint, source_oper, target_oper, runner, original):
        """Leave a native durable failure until a verified staged file is ready."""
        if task.fileitem.type != "file" or checkpoint.resolved_transfer_type not in ("copy", "move") or len(checkpoint.items) != 1:
            return TransferInfo(success=False, fileitem=task.fileitem, message="秒传等待只支持单视频文件的复制或移动整理")
        if not isinstance(source_oper, LocalStorage) or not isinstance(target_oper, U115Pan):
            return TransferInfo(success=False, fileitem=task.fileitem, message="其他插件已接管存储，无法安全接管秒传等待")
        if not task.admission_task_id or runner is None:
            return TransferInfo(success=False, fileitem=task.fileitem, message="缺少 V3 持久整理身份，未执行上传")
        cleanup = checkpoint.planning_input.options.get("cleanup_dest_fileitem")
        if cleanup and (cleanup.get("type") != "file" or cleanup.get("storage") != "u115"):
            return TransferInfo(success=False, fileitem=task.fileitem, message="不支持自动替换旧目录，请先在 V3 人工处理旧目标")
        row = self.store.active_for(source_key(task.fileitem))
        created = False
        if row is None:
            payload = {"host_generation": 3, "native_task_id": task.admission_task_id,
                   "task": {"fileitem": task.fileitem.model_dump(mode="json")},
                   "final_path": checkpoint.final_target_path, "mode": checkpoint.resolved_transfer_type,
                   "source_initial": fingerprint(task.fileitem.path), "wait_since": time.time()}
            row, created = self.store.enqueue(source_key(task.fileitem), payload)
        self.live_tasks[row["id"]] = task
        if row["payload"].get("native_task_id") != task.admission_task_id or row["payload"]["final_path"] != checkpoint.final_target_path:
            self.defer(row, "paused", "整理任务或目标发生变化，请取消旧队列后重新整理")
            return TransferInfo(success=False, fileitem=task.fileitem, message="秒传队列关联发生变化，已暂停")
        if not row["payload"].get("prepared"):
            if created:
                self.log_task(row, "任务入队", 原因=WAIT_MESSAGE)
            return TransferInfo(success=False, fileitem=task.fileitem, transfer_type=checkpoint.resolved_transfer_type,
                                target_item=FileItem(storage="u115", type="file", path=checkpoint.final_target_path),
                                message=WAIT_MESSAGE, need_notify=False)
        if row["state"] != "finalizing":
            return TransferInfo(success=False, fileitem=task.fileitem, message="已暂停，需在插件中手动恢复整理")
        api = OpenAPI(U115Pan(), FileItem, self.stop_event)
        try:
            data = row["payload"]
            staged = api.verify(data["remote_id"], data["stage_path"], data["hashes"])
            source_path = Path(data["task"]["fileitem"]["path"])
            # A durable cross-storage move may have already deleted the source
            # before its success checkpoint response was lost. Require the exact
            # committed destination before replaying those native step receipts.
            visible = api.raw_path(data["final_path"]) if not source_path.exists() else None
            if not (data.get("committed") and data["mode"] == "move" and visible
                    and str(visible.get("file_id")) == str(data["remote_id"])):
                self.check_source(row)
            proxy = PreparedStorage(api, self.store, row, data["final_path"], staged, data["hashes"])
            self.context.prepared_storage = proxy
            try:
                info = original(self.chain, task, checkpoint, source_oper=source_oper, target_oper=proxy,
                                step_runner=PreparedStepRunner(runner, api, data))
            finally:
                self.context.prepared_storage = None
            if proxy.error:
                raise proxy.error
            if info and info.success:
                api.verify(data["remote_id"], info.target_item.path, data["hashes"])
            return info
        except (PauseTask, RetryLater) as error:
            self.defer(row, "paused", str(error))
            return TransferInfo(success=False, fileitem=task.fileitem, message=str(error),
                                transfer_type=checkpoint.resolved_transfer_type)
        finally:
            api.close()

    def check_source(self, row):
        """Verify the original file before hashing, body upload, or native replay."""
        path = Path(row["payload"]["task"]["fileitem"]["path"])
        if not path.is_file() or fingerprint(path) != row["payload"]["source_initial"]:
            raise PauseTask("源文件不存在或发生变化，已暂停")
        return path

    def verify_result(self, row, info):
        """Verify the exact remote object before allowing native success/cleanup."""
        data = row["payload"]
        if not info.target_item or str(info.target_item.fileid) != str(data["remote_id"]):
            raise PauseTask("V3 整理结果与远端暂存文件不一致")
        path = Path(data["task"]["fileitem"]["path"])
        if path.exists() and fingerprint(path) != data["source_initial"]:
            raise PauseTask("源文件在完成确认前变化，已停止回写与清理")
        api = OpenAPI(U115Pan(), FileItem, self.stop_event)
        try:
            visible = api.raw_path(info.target_item.path)
            if not visible or str(visible.get("file_id")) != str(data["remote_id"]):
                raise PauseTask("远端目标已删除或替换，请人工核对")
            api.verify(data["remote_id"], info.target_item.path, data["hashes"])
        finally:
            api.close()

    def restore(self):
        """Recover plugin jobs without replaying V2 internal task snapshots."""
        self.store.recover()
        for row in self.store.all(active=True, limit=100000):
            if row["payload"].get("host_generation") != 3:
                self.store.update(row["id"], state="paused", message="V2 遗留任务已保留；请先在 V2 完成或取消，再在 V3 重新整理")
                continue
            data = row["payload"]
            history = (self.chain.transfer_history_repository.get(row["history_id"]) if row["history_id"]
                       else self.chain.transfer_history_repository.get_by_transfer_task_id(task_id=data["native_task_id"]))
            if history and history.status and row["history_id"] == history.id:
                self.store.update(row["id"], state="completed", history_id=history.id, message="已恢复 V3 成功整理记录")
            elif history and history.transfer_task_id == data["native_task_id"]:
                next_at = row["next_at"]
                if self.config["max_wait_hours"] and row["state"] in ("queued", "waiting"):
                    next_at = min(next_at, data["wait_since"] + self.config["max_wait_hours"] * 3600)
                self.store.update(row["id"], ready=1, next_at=next_at, history_id=history.id)
            else:
                self.store.update(row["id"], state="paused", message="原 V3 整理记录或持久任务不存在，请核对后重新整理")

    def run(self, manual=False):
        """Stage uploads in two independent workers; native replay only finalizes."""
        wake = self.upload_wake if manual else self.wake
        while not self.stop_event.is_set():
            try:
                if not manual:
                    self.poll_finalizing()
                row = self.store.claim(manual=manual)
                if row:
                    self.execute(row)
                    continue
            except Exception:
                logger.error("【115秒传等待】V3 调度异常，队列仍保留")
            wake.wait(5)
            wake.clear()

    def execute(self, row):
        """Only perform remote staging here; MP owns the final plan and history."""
        data, key = row["payload"], row["id"]
        force = row["state"] == "uploading"
        api = None
        try:
            if data.get("host_generation") != 3:
                raise PauseTask("V2 遗留任务需要先在 V2 完成或取消")
            if not force:
                expired = self.config["max_wait_hours"] and time.time() >= data["wait_since"] + self.config["max_wait_hours"] * 3600
                if expired or data.get("auto_attempts", row["attempts"]) > self.config["max_retries"] + 1:
                    data["auto_attempts"] -= 1
                    row["attempts"] -= 1
                    self.store.update(key, attempts=row["attempts"], payload=data)
                    self.defer(row, "waiting", "等待超过设定期限" if expired else "自动重试次数已用完", limit_reached=True)
                    return
            path = self.check_source(row)
            self.log_task(row, "开始强制上传" if force else "开始自动尝试")
            if not data.get("hashes"):
                data["hashes"] = hash_file(path, self.stop_event)
                if data["hashes"]["fingerprint"] != data["source_initial"]:
                    raise PauseTask("源文件在计算哈希前发生变化")
                self.store.update(key, payload=data)
            api = OpenAPI(U115Pan(), FileItem, self.stop_event)
            final = PurePosixPath(data["final_path"])
            stage = final.parent / ".mp115-staging" / key / final.name
            data["stage_path"] = str(stage)
            folder = api.get_folder(stage.parent)
            existing = api.raw_path(stage)
            file_id = data.get("remote_id") or (existing.get("file_id") if existing else None)
            if not file_id:
                if data.get("upload_session") and not data.get("upload_confirmed"):
                    if not force:
                        raise PauseTask("有未完成的普通上传，请强制上传继续")
                    api.upload(path, folder, data["hashes"], None, data, lambda: self.store.update(key, payload=data))
                elif not data.get("instant_confirmed") and not data.get("upload_confirmed"):
                    try:
                        file_id = api.instant(path, folder, final.name, data["hashes"])
                        data["instant_confirmed"] = True
                    except NotInstant as error:
                        if not force:
                            raise
                        self.log_task(row, "秒传未命中，转普通上传", 触发方式=data.get("upload_origin", "manual"))
                        api.upload(path, folder, data["hashes"], error.upload_data, data, lambda: self.store.update(key, payload=data))
                self.store.update(key, payload=data)
                if not file_id:
                    existing = api.raw_path(stage)
                    file_id = existing.get("file_id") if existing else None
            if not file_id:
                raise RetryLater("已提交上传结果，等待远端文件可见")
            api.verify(file_id, str(stage), data["hashes"])
            data.update(remote_id=str(file_id), prepared=True)
            self.store.update(key, state="finalizing", payload=data, next_at=0, message="远端文件已确认，等待 V3 完成原整理记录")
            self.request_finalize(self.store.get(key))
        except PauseTask as error:
            self.defer(row, "paused", str(error))
        except (NotInstant, RetryLater) as error:
            self.defer(row, "waiting", str(error))
        except Exception:
            self.defer(row, "waiting", "后台整理异常，已保留源文件及队列")
        finally:
            if api:
                api.close()

    def request_finalize(self, row):
        """Request host-owned retry, preserving its task/checkpoint/history identity."""
        try:
            accepted, message = self.chain.redo_transfer_history(row["history_id"])
        except Exception:
            accepted, message = False, "宿主暂不可用，稍后核对"
        if self.store.get(row["id"])["state"] != "finalizing":
            return
        self.store.update(row["id"], next_at=time.time() + 30, message="等待 V3 回写原整理记录" if accepted else f"等待 V3 接受完成请求：{message}")
        self.log_task(row, "请求 V3 完成整理", 已接受=accepted)

    def poll_finalizing(self):
        """Reconcile missed callbacks after restart without resending file contents."""
        for row in self.store.all(active=True, limit=100000):
            if row["state"] != "finalizing" or row["next_at"] > time.time():
                continue
            history = self.chain.transfer_history_repository.get(row["history_id"])
            if history and history.status:
                self.store.update(row["id"], state="completed", message="V3 原整理记录已成功")
                task = self.live_tasks.pop(row["id"], None)
                if task:
                    self.original_finish(self.chain, task)
            elif not history or history.transfer_task_id != row["payload"]["native_task_id"]:
                self.defer(row, "paused", "原 V3 持久任务或历史关联已变化")
            else:
                snapshot = self.chain.transfer_execution_repository.get_snapshot(task_id=row["payload"]["native_task_id"])
                if snapshot is None:
                    self.defer(row, "paused", "V3 持久整理任务已不存在，请保留暂存文件并人工核对")
                elif str(getattr(snapshot.state, "value", snapshot.state)) == "manual_review":
                    self.defer(row, "paused", "V3 外部操作需要人工复核，请先在整理队列完成复核")
                else:
                    self.request_finalize(row)

    def defer(self, row, state, reason, limit_reached=False):
        """Only count/time limits opt into force upload; all guard failures pause."""
        data = self.store.get(row["id"])["payload"]
        now = time.time()
        deadline = data.get("wait_since", row["created"]) + self.config["max_wait_hours"] * 3600 if self.config["max_wait_hours"] else None
        if row["state"] == "uploading":
            state, reason = "paused", f"强制上传未完成：{reason}；请手动核对并继续"
        elif state == "waiting":
            reached = "等待超过设定期限" if deadline and now >= deadline else "自动重试次数已用完" if data.get("auto_attempts", row["attempts"]) >= self.config["max_retries"] + 1 else None
            if reached or limit_reached:
                state = "upload_queued" if self.config.get("limit_action", "manual") == "upload" else "paused"
                reason = f"{reached or reason}；最后结果：{reason}"
                if state == "upload_queued":
                    data["upload_origin"] = "limit"
        delays = self.config["retry_delays"]
        delay = delays[min(max(0, data.get("auto_attempts", row["attempts"]) - 1), len(delays) - 1)] * random.uniform(.9, 1.1)
        next_at = min(now + delay, deadline) if deadline and state == "waiting" else now + delay
        if state == "upload_queued":
            next_at = now
        self.store.update(row["id"], state=state, next_at=next_at, message=reason, payload=data)
        saved = {**row, "state": state, "next_at": next_at, "message": reason, "payload": data}
        self.log_task(saved, "任务状态变化", level="warning" if state == "paused" else "info", 状态=state, 原因=reason,
                      下次重试=log_time(next_at) if state == "waiting" else "无")
        if state in ("paused", "upload_queued") or (state == "waiting" and data.get("auto_attempts", row["attempts"]) == 1):
            self.notify_task(saved, manual=state == "paused")
        self.upload_wake.set()

    def control(self, key, action):
        """Control staged work only; executing/native-finalizing work is fenced."""
        existing = self.store.get(key)
        if not existing:
            raise KeyError(key)
        if existing["state"] == "finalizing":
            raise ValueError("V3 正在完成整理，请等待本轮回写结束")
        if existing["payload"].get("host_generation") != 3 and action != "cancel":
            raise ValueError("V2 遗留任务仅可取消；请在 V3 重新整理")
        row = self.store.command(key, action)
        self.log_task(row, "用户操作", 操作=action)
        if action == "cancel":
            task = self.live_tasks.pop(key, None)
            if task:
                self.original_finish(self.chain, task)
        self.wake.set()
        self.upload_wake.set()
        return row

    def control_many(self, keys, action):
        """Apply at most 200 deduplicated commands, isolating per-task failures."""
        if action not in ("pause", "resume", "cancel", "upload") or not isinstance(keys, list) or not keys or len(keys) > 200 or any(not isinstance(k, str) or not k for k in keys):
            raise ValueError("请选择 1～200 个任务及有效操作")
        result = dict(action=action, at=log_time(time.time()), accepted=0, skipped=0, failed=0, items=[])
        for key in dict.fromkeys(keys):
            item = dict(id=key, name=key)
            try:
                row = self.store.get(key)
                if row:
                    item["name"] = Path(row["payload"]["task"]["fileitem"]["path"]).name
                updated = self.control(key, action)
                item.update(status="accepted", message=updated["message"])
            except (KeyError, ValueError) as error:
                item.update(status="skipped", message="任务不存在" if isinstance(error, KeyError) else str(error))
            except Exception:
                item.update(status="failed", message="处理异常，请查看日志")
            result[item["status"]] += 1
            result["items"].append(item)
        return result

    def stop(self):
        """Keep patches until both staging workers and native executions have exited."""
        self.stop_event.set()
        self.wake.set()
        self.upload_wake.set()
        deadline = time.monotonic() + 35
        for worker in (self.worker, self.upload_worker):
            if worker:
                worker.join(max(0, deadline - time.monotonic()))
                if worker.is_alive():
                    raise RuntimeError("后台上传仍在结束，暂不能重新初始化插件")
        with self.native_lock:
            while self.native_active and time.monotonic() < deadline:
                self.native_lock.wait(max(0, deadline - time.monotonic()))
            if self.native_active:
                raise RuntimeError("V3 正在完成整理，暂不能移除接管接口")
        for name, own, original, replacement in reversed(self.patches):
            if getattr(TransferChain, name) is replacement:
                if own:
                    setattr(TransferChain, name, original)
                else:
                    delattr(TransferChain, name)
        self.patches.clear()
        for task in self.live_tasks.values():
            self.original_finish(self.chain, task)
        self.live_tasks.clear()


class PreparedStepRunner:
    """Give V3 strict evidence for an upload that committed before a lost response."""

    def __init__(self, runner, api, data):
        self.runner, self.api, self.data = runner, api, data

    def run(self, *, phase, kind, payload, execute, observe):
        """Replace only cloud materialization observation; preserve native intents/CAS."""
        if kind == "materialize_target":
            original_observe = observe
            def observe():
                from app.application.transfer.execution import (
                    TransferOperationObservation, TransferOperationObservationState, TransferStepResult,
                )
                visible = self.api.raw_path(payload["target_path"])
                if visible and str(visible.get("file_id")) == str(self.data["remote_id"]):
                    item = self.api.verify(self.data["remote_id"], payload["target_path"], self.data["hashes"])
                    return TransferOperationObservation(state=TransferOperationObservationState.APPLIED,
                        evidence=TransferStepResult(payload={"item": item.model_dump(mode="json"), "message": "远端文件已核对"}))
                return original_observe()
        return self.runner.run(phase=phase, kind=kind, payload=payload, execute=execute, observe=observe)
