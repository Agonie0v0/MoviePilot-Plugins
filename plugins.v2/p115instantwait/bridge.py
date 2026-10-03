"""MoviePilot V2 bridge. Patches are narrow, reversible, and owned by one engine."""
import inspect
import random
import threading
import time
from datetime import datetime
from enum import Enum
from pathlib import Path, PurePosixPath

from app.chain.transfer import TransferChain, job_lock
from app.core.context import MediaInfo
from app.core.meta import MetaAnime, MetaBase, MetaVideo
from app.db import SessionFactory
from app.db.models.transferhistory import TransferHistory
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.modules.filemanager.storages.local import LocalStorage
from app.modules.filemanager.storages.u115 import U115Pan
from app.schemas import FileItem, TransferInfo, TransferTask
from app.schemas.types import MediaType

from .remote import (DeferredSource, NotInstant, OpenAPI, PauseTask, PreparedStorage,
                     RetryLater, fingerprint, hash_file)
from .store import QueueStore


WAIT_MESSAGE = "等待秒传，将自动重试（115 秒传等待插件）"


def log_text(value):
    # Keep externally supplied filenames/messages on one log line.
    return " ".join(str(value).split())[:400]


def log_time(value):
    return datetime.fromtimestamp(value).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def json_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (Path, datetime)):
        return str(value)
    if isinstance(value, dict):
        return {k: json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_value(v) for v in value]
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"不能持久化 {type(value).__name__}")


def dump_task(task):
    data = task.model_dump(mode="json", exclude={"meta", "mediainfo"})
    data["meta"] = {"class": type(task.meta).__name__, "data": json_value(vars(task.meta))}
    data["mediainfo"] = json_value(task.mediainfo.to_dict())
    return data


def load_task(data):
    data = dict(data)
    saved = data.pop("meta")
    classes = {"MetaVideo": MetaVideo, "MetaAnime": MetaAnime, "MetaBase": MetaBase}
    if saved["class"] not in classes:
        raise PauseTask("任务元数据类型不受支持")
    meta = classes[saved["class"]]("", isfile=True)
    meta.__dict__.update(saved["data"])
    if isinstance(getattr(meta, "type", None), str):
        meta.type = MediaType(meta.type)
    media = MediaInfo()
    media.from_dict(data.pop("mediainfo"))
    return TransferTask(meta=meta, mediainfo=media, **data)


def source_key(item):
    return f"{item.storage}:{Path(item.path).resolve()}"


def task_kwargs(task):
    keys = ("fileitem", "meta", "mediainfo", "target_directory", "target_storage", "target_path",
            "transfer_type", "episodes_info", "scrape", "library_type_folder", "library_category_folder")
    return {key: getattr(task, key, None) for key in keys}


class InstantWaitEngine:
    def __init__(self, data_path, config, notify=None):
        self.store = QueueStore(Path(data_path) / "queue.db")
        self.config, self.notify = config, notify
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.context = threading.local()
        self.patches = []
        self.live_tasks = {}
        self.chain = TransferChain()
        self.worker = None
        self.upload_worker = None
        self.upload_wake = threading.Event()
        self.extensions = {ext.strip().lower().lstrip(".") for ext in config["extensions"].split(",") if ext.strip()}

    def log_task(self, row, event, level="info", **details):
        payload = row["payload"]
        source = payload.get("task", {}).get("fileitem", {}).get("path", "")
        fields = {"任务": row["id"], "整理记录": row.get("history_id") or "待生成",
                  "文件": PurePosixPath(str(source).replace("\\", "/")).name,
                  "已尝试": row["attempts"],
                  "本轮自动尝试": f"{payload.get('auto_attempts', row['attempts'])}/{self.config.get('max_retries', 3) + 1}",
                  **details}
        getattr(logger, level)(f"【115秒传等待】{event} | " +
                               " | ".join(f"{key}={log_text(value)}" for key, value in fields.items()))

    def notify_task(self, row, manual=True):
        if not self.notify:
            return
        payload = row["payload"]
        source = payload.get("task", {}).get("fileitem", {}).get("path", "")
        name = PurePosixPath(str(source).replace("\\", "/")).name
        title = "115秒传等待 · 需要手动处理" if manual else "115秒传等待 · 整理暂未成功"
        text = "\n".join([
            f"文件：{log_text(name)}",
            f"整理记录：#{row.get('history_id') or '待生成'}",
            f"累计尝试：{row['attempts']} 次；本轮自动：{payload.get('auto_attempts', row['attempts'])}/{self.config.get('max_retries', 3) + 1}",
            f"原因：{log_text(row['message'])}",
            "后续：自动重试已停止，请在插件「查看数据」中强制上传或继续等待秒传。" if manual else
            f"下次重试：{log_time(row['next_at'])}；后台继续等待，不影响其他整理。",
            f"任务 ID：{row['id']}",
        ])
        try:
            submitted = self.notify(title, text, manual=manual)
            self.log_task(row, "通知已关闭" if submitted is False else "通知已提交 MP",
                          通知类型="手动处理" if manual else "整理入库")
        except Exception:
            # Notification failures must never re-enter execute's retry handler,
            # change a paused task back to waiting, or interrupt other tasks.
            self.log_task(row, "通知提交失败", level="warning", 说明="请检查 MP 通知渠道；任务状态保持不变")

    def patch(self, cls, name, method):
        original = getattr(cls, name)
        if getattr(original, "__instant_wait_owner__", None):
            raise RuntimeError("115 秒传等待补丁已存在")
        if original.__module__ not in ("app.chain", "app.chain.transfer", "app.db.transferhistory_oper"):
            raise RuntimeError(f"{name} 已被其他插件接管，请先停用冲突插件")
        own = name in cls.__dict__
        method.__instant_wait_owner__ = self
        self.patches.append((cls, name, own, original, method))
        setattr(cls, name, method)
        return original

    def install(self):
        required = ("_TransferChain__handle_transfer", "_TransferChain__default_callback",
                    "_TransferChain__finish_scrape_batch_task", "_TransferChain__register_scrape_batch_task",
                    "_TransferChain__close_scrape_batch", "_TransferChain__record_scrape_target", "transfer")
        if not all(callable(getattr(TransferChain, name, None)) for name in required):
            raise RuntimeError("此 MP 版本缺少所需整理接口；请使用 V2.15.6")
        transfer = TransferChain.transfer
        if "preview" not in inspect.signature(transfer).parameters:
            raise RuntimeError("此 MP 版本不支持整理预览接口")
        self.original_transfer = transfer
        self.original_finish_batch = getattr(TransferChain, "_TransferChain__finish_scrape_batch_task")
        self.original_callback = getattr(TransferChain, "_TransferChain__default_callback")
        handle = getattr(TransferChain, "_TransferChain__handle_transfer")
        engine = self

        def wrapped_handle(chain, task, callback=None):
            previous = getattr(engine.context, "task", None)
            engine.context.task = task
            engine.context.deferred = None
            engine.context.enrolled_new = False

            def receive(actual_task, info):
                key = getattr(engine.context, "deferred", None)
                if key:
                    engine.ensure_history(key, actual_task, info)
                    engine.waiting_view(actual_task)
                    return False, WAIT_MESSAGE
                return callback(actual_task, info) if callback else (info.success, info.message)

            try:
                return handle(chain, task, receive)
            finally:
                key = getattr(engine.context, "deferred", None)
                # Activate only after MP's original finally has released its batch execution.
                if key:
                    engine.log_task(engine.store.get(key), "任务入队" if engine.context.enrolled_new else "复用已有任务")
                    engine.store.update(key, ready=1)
                    engine.wake.set()
                engine.context.task = previous
                engine.context.deferred = None

        signature = inspect.signature(transfer)

        def wrapped_transfer(chain, *args, **kwargs):
            bound = signature.bind(chain, *args, **kwargs)
            values = bound.arguments
            task = getattr(engine.context, "task", None)
            if not task or values.get("preview") or not engine.in_scope(values):
                return transfer(chain, *args, **kwargs)
            if values["fileitem"].type != "file":
                return TransferInfo(success=False, fileitem=task.fileitem,
                    message="当前版本只支持文件整理，蓝光目录暂不支持秒传等待")
            # A storage-selection plugin must not silently be bypassed.
            if values.get("source_oper") is not None or values.get("target_oper") is not None:
                return TransferInfo(success=False, fileitem=task.fileitem,
                    message="其他插件已接管存储，无法安全接管秒传等待")
            planned = dict(values)
            planned.pop("self", None)
            planned["preview"] = True
            preview = transfer(chain, **planned)
            if not preview or not preview.success or not preview.target_item:
                return preview
            try:
                mode = values.get("transfer_type") or getattr(task.target_directory, "transfer_type", None)
                if mode not in ("copy", "move"):
                    raise PauseTask("仅支持复制或移动整理")
                task.transfer_type = mode
                payload = {"version": 1, "task": dump_task(task),
                    "final_path": preview.target_item.path, "preview": preview.model_dump(mode="json"),
                    "source_initial": fingerprint(task.fileitem.path), "wait_since": time.time()}
                row, created = engine.store.enqueue(source_key(task.fileitem), payload)
                engine.context.enrolled_new = created
            except (PauseTask, TypeError, OSError) as exc:
                return TransferInfo(success=False, fileitem=task.fileitem, message=str(exc))
            engine.live_tasks[row["id"]] = task
            engine.context.deferred = row["id"]
            return TransferInfo(success=False, fileitem=task.fileitem, transfer_type=mode,
                target_item=preview.target_item, message=WAIT_MESSAGE)

        finish = self.original_finish_batch

        def wrapped_finish(chain, task):
            if engine.store.active_for(source_key(task.fileitem)):
                return
            return finish(chain, task)

        add_force = TransferHistoryOper.add_force

        def wrapped_add_force(oper, **kwargs):
            key = getattr(engine.context, "history_job", None)
            if not key:
                return add_force(oper, **kwargs)
            row = engine.store.get(key)
            existing = oper.get(row["history_id"]) if row["history_id"] else None
            if existing:
                history_id = existing.id
                if existing.src != kwargs.get("src") or existing.src_storage != kwargs.get("src_storage"):
                    raise PauseTask("原整理记录关联不一致")
                kwargs["date"] = time.strftime("%Y-%m-%d %H:%M:%S")
                if kwargs.get("status"):
                    kwargs["errmsg"] = ""
                existing.update(oper._db, kwargs)
                return oper.get(history_id)
            # Do not delete arbitrary existing source histories on initial enrollment.
            previous = oper.get_by_src(kwargs["src"], kwargs.get("src_storage"))
            marker = f"[115wait:{key}]"
            if previous and marker in (previous.errmsg or ""):
                history_id = previous.id
                engine.store.update(key, history_id=history_id)
                previous.update(oper._db, kwargs)
                return oper.get(history_id)
            kwargs["date"] = time.strftime("%Y-%m-%d %H:%M:%S")
            if not kwargs.get("status"):
                kwargs["errmsg"] = f"{kwargs.get('errmsg', WAIT_MESSAGE)} {marker}"
            record = TransferHistory(**kwargs)
            # MP's default sessions expire ORM attributes on commit. Capture the
            # id before commit/close rather than reading an expired detached row.
            with SessionFactory() as db:
                db.add(record)
                db.flush()
                history_id = record.id
                db.commit()
            saved = oper.get(history_id)
            if not saved:
                raise RuntimeError("整理记录创建失败")
            engine.store.update(key, history_id=saved.id)
            return saved

        try:
            self.patch(TransferChain, "_TransferChain__handle_transfer", wrapped_handle)
            self.patch(TransferChain, "transfer", wrapped_transfer)
            self.patch(TransferChain, "_TransferChain__finish_scrape_batch_task", wrapped_finish)
            self.patch(TransferHistoryOper, "add_force", wrapped_add_force)
            self.restore()
            self.worker = threading.Thread(target=self.run, name="p115-instant-wait", daemon=True)
            self.worker.start()
            self.upload_worker = threading.Thread(target=self.run, kwargs={"manual": True},
                                                  name="p115-manual-upload", daemon=True)
            self.upload_worker.start()
        except Exception:
            self.stop()
            raise

    def in_scope(self, values):
        item = values.get("fileitem")
        directory = values.get("target_directory")
        target = values.get("target_storage") or getattr(directory, "library_storage", None)
        return bool(item and item.storage == "local" and target == "u115" and
                    (item.type == "dir" or (item.extension or Path(item.path).suffix).lower().lstrip(".") in self.extensions))

    def uninstall(self):
        for cls, name, own, original, installed in reversed(self.patches):
            if getattr(cls, name) is installed:
                if own:
                    setattr(cls, name, original)
                else:
                    delattr(cls, name)
        self.patches.clear()

    def stop(self):
        self.stop_event.set()
        self.wake.set()
        self.upload_wake.set()
        deadline = time.monotonic() + 35
        for worker in (self.worker, self.upload_worker):
            if worker and worker.ident is not None:
                worker.join(timeout=max(0, deadline - time.monotonic()))
            if worker and worker.is_alive():
                raise RuntimeError("后台请求仍在结束，暂不能重新初始化插件")
        self.uninstall()

    def waiting_view(self, task):
        # "waiting" is already a nonterminal MP JobManager state; it holds no execution lease.
        # Match MP's lexical file key; never resolve filesystem paths under its global lock.
        def identity(item):
            return item.storage, Path(str(item.path).replace("\\", "/")).as_posix().rstrip("/")
        key = identity(task.fileitem)
        with job_lock:
            for group in self.chain.jobview._job_view.values():
                for job_task in group.tasks:
                    if identity(job_task.fileitem) == key:
                        job_task.state = "waiting"

    def ensure_history(self, key, task, info=None):
        previous = getattr(self.context, "history_job", None)
        self.context.history_job = key
        try:
            row = self.store.get(key)
            oper = TransferHistoryOper()
            existing = oper.get(row["history_id"]) if row["history_id"] else None
            if existing:
                return existing
            if info is None:
                info = TransferInfo.model_validate(row["payload"]["preview"])
                info.success = False
            info.message = WAIT_MESSAGE
            return oper.add_fail(fileitem=task.fileitem, mode=task.transfer_type, meta=task.meta,
                mediainfo=task.mediainfo, transferinfo=info,
                downloader=task.downloader, download_hash=task.download_hash)
        finally:
            self.context.history_job = previous

    def history_message(self, key, message):
        row = self.store.get(key)
        oper = TransferHistoryOper()
        history = oper.get(row["history_id"]) if row["history_id"] else None
        if history and not history.status:
            history.update(oper._db, {"errmsg": message})

    def restore(self):
        self.store.recover()
        rows = self.store.all(active=True, limit=100000)
        # Restore all unfinished peers before running any completion callback.
        for row in rows:
            try:
                task = load_task(row["payload"]["task"])
                self.live_tasks[row["id"]] = task
                self.chain.jobview.add_task(task, state="waiting")
                self.chain._TransferChain__register_scrape_batch_task(task)
                self.ensure_history(row["id"], task)
                if row["state"] == "paused":
                    self.history_message(row["id"], row["message"])
                self.log_task(self.store.get(row["id"]), "恢复持久化任务", 状态=row["state"],
                              原因=row["message"], 下次重试=log_time(row["next_at"]) if row["state"] in ("queued", "waiting") else "无")
                self.store.update(row["id"], ready=1)
            except Exception:
                self.store.update(row["id"], state="paused", message="任务恢复失败，请检查 MP 版本与队列数据")
                logger.error(f"【115秒传等待】任务恢复失败 | 任务={row['id']} | 整理记录={row['history_id'] or '待生成'} | 已保留队列并暂停")
                if row["state"] != "paused":
                    self.notify_task(self.store.get(row["id"]))
        active_batches = {task.transfer_batch_id for task in self.live_tasks.values() if task.transfer_batch_id}
        # Recover already completed peers from our own durable checkpoint.
        for row in self.store.all(limit=100000):
            payload = row["payload"]
            if row["state"] != "completed" or payload["task"].get("transfer_batch_id") not in active_batches:
                continue
            task = load_task(payload["task"])
            self.chain.jobview.add_task(task, state="completed")
            if payload.get("result"):
                info = TransferInfo.model_validate(payload["result"])
                self.chain._TransferChain__record_scrape_target(task, info)
        for batch in active_batches:
            self.chain._TransferChain__close_scrape_batch(batch)

    def run(self, manual=False):
        wake = self.upload_wake if manual else self.wake
        while not self.stop_event.is_set():
            try:
                row = self.store.claim(manual=manual)
                if not row:
                    wake.wait(5)
                    wake.clear()
                    continue
                self.execute(row)
            except Exception:
                logger.error("【115秒传等待】调度异常，队列仍保留，稍后重试")
                self.stop_event.wait(5)

    def execute(self, row):
        key = row["id"]
        manual = row["state"] == "uploading"
        api = None
        try:
            payload = row["payload"]
            task = self.live_tasks.get(key) or load_task(payload["task"])
            self.live_tasks[key] = task
            self.ensure_history(key, task)
            self.log_task(self.store.get(key), "开始手动处理" if manual else "开始自动尝试")
            if payload.get("result"):
                self.log_task(row, "恢复整理结果", 说明="核对远端后继续原记录回写")
                info = TransferInfo.model_validate(payload["result"])
                api = OpenAPI(U115Pan(), FileItem, self.stop_event)
                visible = api.raw_path(info.target_item.path)
                if not visible or str(visible.get("file_id")) != str(payload["remote_id"]):
                    raise PauseTask("已上传的目标文件被删除或替换，请人工核对")
                api.verify(payload["remote_id"], info.target_item.path, payload["hashes"])
                self.complete(row, task, info)
                return
            limit = self.config["max_wait_hours"] * 3600
            if not manual and limit and time.time() - payload.get("wait_since", row["created"]) > limit:
                raise PauseTask("等待超过设定期限，可手动恢复继续等待")
            if not manual and payload.get("auto_attempts", row["attempts"]) > self.config.get("max_retries", 3) + 1:
                raise PauseTask("自动重试次数已用完，请手动处理；强制上传会在未秒传时普通上传")
            path = Path(task.fileitem.path)
            if not path.is_file():
                raise PauseTask("源文件不存在，已暂停")
            if fingerprint(path) != payload["source_initial"]:
                raise PauseTask("源文件在等待期间发生变化，已暂停")
            if not payload.get("hashes"):
                self.log_task(row, "开始计算文件哈希")
                payload["hashes"] = hash_file(path, self.stop_event)
                if payload["hashes"]["fingerprint"] != payload["source_initial"]:
                    raise PauseTask("源文件在开始计算哈希前发生变化，已暂停")
                self.store.update(key, payload=payload)
            api = OpenAPI(U115Pan(), FileItem, self.stop_event)
            final = PurePosixPath(payload["final_path"])
            stage_path = final.parent / ".mp115-staging" / key / final.name
            stage_dir = api.get_folder(stage_path.parent)
            file_id = payload.get("remote_id")
            if not file_id:
                # Reconcile an init response lost after a successful server-side commit.
                existing = api.raw_path(stage_path)
                if existing and existing.get("file_id"):
                    file_id = existing["file_id"]
                    self.log_task(row, "发现已有暂存文件", 说明="核对后继续整理，不重复上传")
                elif payload.get("upload_session") and not payload.get("upload_confirmed"):
                    if not manual:
                        raise PauseTask("有未完成的普通上传，请点强制上传继续")
                    self.log_task(row, "继续普通上传", 已完成分片=len(payload["upload_session"].get("parts", [])))
                    api.upload(path, stage_dir, payload["hashes"], None, payload,
                               lambda: self.store.update(key, payload=payload))
                    self.log_task(row, "普通上传已提交", 说明="等待远端校验与整理完成")
                    existing = api.raw_path(stage_path)
                    file_id = existing.get("file_id") if existing else None
                elif not payload.get("instant_confirmed") and not payload.get("upload_confirmed"):
                    try:
                        file_id = api.instant(path, stage_dir, final.name, payload["hashes"])
                    except NotInstant as exc:
                        if not manual:
                            raise
                        self.log_task(row, "秒传未命中，转普通上传", 触发方式="用户手动")
                        self.store.update(key, message="未命中秒传，正在普通上传")
                        self.history_message(key, "手动处理：未命中秒传，正在普通上传")
                        api.upload(path, stage_dir, payload["hashes"], exc.upload_data, payload,
                                   lambda: self.store.update(key, payload=payload))
                        self.log_task(row, "普通上传已提交", 说明="等待远端校验与整理完成")
                    else:
                        payload["instant_confirmed"] = True
                        self.log_task(row, "秒传命中", 说明="等待远端校验与整理完成")
                    # A hit without an ID must wait for visibility, not submit
                    # another init that can create a duplicate remote file.
                    self.store.update(key, payload=payload)
                    if not file_id:
                        existing = api.raw_path(stage_path)
                        file_id = existing.get("file_id") if existing else None
                if not file_id:
                    raise RetryLater("已提交上传结果，等待远端文件 ID 可见")
                payload["remote_id"] = str(file_id)
                self.store.update(key, payload=payload)
            staged = api.verify(file_id, str(stage_path), payload["hashes"])
            proxy = PreparedStorage(api, self.store, row, str(final), staged, payload["hashes"])
            args = task_kwargs(task)
            args.update(source_oper=DeferredSource(LocalStorage()), target_oper=proxy, preview=False)
            info = self.original_transfer(self.chain, **args)
            if proxy.error:
                raise proxy.error
            if not info or not info.success:
                # A definite MP conflict/rename error is a real failure, not a seconds miss.
                info = info or TransferInfo(success=False, message="MP 未返回整理结果")
                self.finish_failure(row, task, info)
                return
            if not info.target_item or str(info.target_item.fileid) != str(file_id):
                raise PauseTask("MP 整理结果与秒传远端文件不一致")
            api.verify(file_id, info.target_item.path, payload["hashes"])
            # Persist the complete MP result before the native callback (or any cleanup).
            payload = self.store.get(key)["payload"]
            payload["result"] = info.model_dump(mode="json")
            self.store.update(key, payload=payload)
            self.complete(self.store.get(key), task, info)
        except PauseTask as exc:
            self.defer(row, "paused", str(exc))
        except NotInstant as exc:
            self.defer(row, "waiting", str(exc))
        except RetryLater as exc:
            self.defer(row, "waiting", str(exc))
        except Exception:
            self.defer(row, "waiting", "后台整理异常，保留任务后重试")
            logger.error("【115秒传等待】任务执行异常，已保留源文件与队列")
        finally:
            if api:
                api.close()

    def defer(self, row, state, reason):
        previous_state = self.store.get(row["id"])["state"]
        if row["state"] == "uploading":
            state = "paused"
            reason = f"手动处理未完成：{reason}；可再次点强制上传核对并继续"
        elif state == "waiting" and row["payload"].get("auto_attempts", row["attempts"]) >= self.config.get("max_retries", 3) + 1:
            state = "paused"
            reason = f"自动重试次数已用完，请手动处理。最后结果：{reason}"
        delays = self.config["retry_delays"]
        base = delays[min(max(0, row["payload"].get("auto_attempts", row["attempts"]) - 1), len(delays) - 1)]
        delay = base * random.uniform(0.9, 1.1)
        message = f"等待秒传：{reason}" if state == "waiting" else f"秒传等待已暂停：{reason}"
        self.store.update(row["id"], state=state, next_at=time.time() + delay, message=message)
        saved = self.store.get(row["id"])
        self.log_task(saved, "等待下次重试" if state == "waiting" else "任务暂停",
                      level="info" if state == "waiting" else "warning", 原因=reason,
                      下次重试=log_time(saved["next_at"]) if state == "waiting" else "无，等待手动处理",
                      **({"间隔秒": round(delay)} if state == "waiting" else {}))
        if state == "paused" and previous_state != "paused":
            self.notify_task(saved)
        elif state == "waiting" and previous_state != "waiting" and saved["payload"].get("auto_attempts", saved["attempts"]) == 1:
            self.notify_task(saved, manual=False)
        self.history_message(row["id"], message)
        task = self.live_tasks.get(row["id"])
        if task:
            self.waiting_view(task)

    def complete(self, row, task, info):
        key = row["id"]
        # The native callback can delete torrents and their data in move mode.
        # Check before entering it, including when recovering a saved result.
        path = Path(task.fileitem.path)
        if path.exists() and fingerprint(path) != row["payload"]["source_initial"]:
            raise PauseTask("源文件在完成确认前发生变化，已停止回写与清理")
        oper = TransferHistoryOper()
        history = oper.get(row["history_id"]) if row["history_id"] else None
        # If the callback committed history and then crashed, don't replay external events.
        if history and history.status:
            self.chain.jobview.finish_task(task)
            self.chain._TransferChain__record_scrape_target(task, info)
        else:
            self.context.history_job = key
            try:
                state, _ = self.original_callback(self.chain, task, info)
                if not state:
                    raise RetryLater("MP 完成回调尚未成功")
            finally:
                self.context.history_job = None
        # Preserve the configured native copy/move semantics after history is committed.
        if task.transfer_type == "move":
            path = Path(task.fileitem.path)
            if path.exists():
                if fingerprint(path) != row["payload"]["source_initial"]:
                    raise PauseTask("整理已成功，但源文件变化，已停止清理")
                LocalStorage().delete(task.fileitem)
                if path.exists():
                    raise RetryLater("整理已成功，等待源文件清理完成")
        self.store.update(key, state="completed", message="文件传输与 MP 整理完成")
        self.original_finish_batch(self.chain, task)
        self.chain.jobview.try_remove_job(task)
        self.live_tasks.pop(key, None)
        self.log_task(self.store.get(key), "整理成功", 结果="原整理记录已更新成功")

    def finish_failure(self, row, task, info):
        self.context.history_job = row["id"]
        try:
            self.original_callback(self.chain, task, info)
        finally:
            self.context.history_job = None
        self.store.update(row["id"], state="failed", message=info.message or "整理失败")
        self.log_task(self.store.get(row["id"]), "整理失败", level="warning", 原因=info.message or "整理失败")
        self.original_finish_batch(self.chain, task)
        self.chain.jobview.try_remove_job(task)
        self.live_tasks.pop(row["id"], None)

    def control(self, key, action):
        row = self.store.command(key, action)
        self.log_task(row, "用户操作", 操作={"upload": "强制上传", "resume": "继续等待秒传",
                                           "pause": "暂停", "cancel": "取消"}[action], 结果=row["message"])
        self.history_message(key, "等待秒传，将自动重试" if action == "resume" else row["message"])
        if action == "cancel":
            task = self.live_tasks.pop(key, None)
            if task:
                self.chain.jobview.fail_task(task)
                self.original_finish_batch(self.chain, task)
                self.chain.jobview.try_remove_job(task)
        self.wake.set()
        self.upload_wake.set()
        return row
