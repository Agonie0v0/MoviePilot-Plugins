"""Run pinned V3 host methods with isolated media, network and scheduler ports.

These are contract/integration tests, not an installed MP server or live 115 test.
Set MP_V3_REFERENCE to the upstream checkout recorded in docs/v3-compatibility.md.
"""
import ast
import copy
import enum
import importlib.util
import json
import os
import re
import sys
import threading
import time
import types
import typing
from pathlib import Path, PurePosixPath
from types import SimpleNamespace as NS
from unittest.mock import Mock

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Boolean, CheckConstraint, Index, Integer, JSON, String, create_engine, false, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, declared_attr, mapped_column, sessionmaker

ROOT = Path(__file__).resolve().parents[2]
REFERENCE = Path(os.environ.get("MP_V3_REFERENCE", "__missing_reference__"))


def module(name):
    result = sys.modules.setdefault(name, types.ModuleType(name))
    result.__path__ = []
    return result


def selected(path, names, ns, methods=None, base=None):
    tree = ast.parse((REFERENCE / path).read_text(encoding="utf-8-sig"))
    nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in names]
    assert {n.name for n in nodes} == set(names), (path, names)
    for node in nodes:
        if isinstance(node, ast.ClassDef):
            if methods is not None:
                node.body = [m for m in node.body if not isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) or m.name in methods]
            if base:
                node.bases = [ast.Name(id=base, ctx=ast.Load())]
                node.keywords = []
                node.body = [m for m in node.body if isinstance(m, ast.FunctionDef)]
    tree = ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[]))
    exec(compile(tree, str(REFERENCE / path), "exec", flags=__import__("__future__").annotations.compiler_flag), ns)


def load_plugin(plugin_id):
    name = "v3_test_" + plugin_id
    # Fresh classes per test, so patches and plugin state cannot leak.
    for key in list(sys.modules):
        if key == name or key.startswith(name + "."):
            del sys.modules[key]
    spec = importlib.util.spec_from_file_location(name, ROOT / "plugins.v3" / plugin_id / "__init__.py",
                                                submodule_search_locations=[str(ROOT / "plugins.v3" / plugin_id)])
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


class SQLBase(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls):
        return cls.__name__.lower()


class PluginBase:
    def __init__(self):
        self.data, self.messages, self.saved_config = {}, [], None
        self.systemmessage = NS(put=lambda value: self.messages.append(value))
    def get_data(self, key):
        return self.data.get(key)
    def save_data(self, key, value):
        self.data[key] = value
    def update_config(self, config):
        self.saved_config = copy.deepcopy(config)
        return True
    def post_message(self, **kwargs):
        self.messages.append(kwargs)


class Harness:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.logs, self.events, self.finish_calls = [], [], []
        logger = NS(**{n: lambda *a, **kw: self.logs.append(a) for n in ("info", "debug", "warn", "warning", "error")})
        for path in ("app.sdk.logging", "app.runtime.log"):
            module(path).logger = logger
        module("app.sdk.plugin")._PluginBase = PluginBase
        module("app.sdk.config").settings = NS(TZ="Asia/Shanghai", PROXY=None)
        module("app.sdk.events").eventmanager = NS(register=lambda _: lambda fn: fn)
        module("app.sdk.events").Event = NS
        module("app.sdk.services").DownloaderHelper = Mock
        module("app.sdk.string").StringUtils = NS(str_filesize=str)
        module("app.schemas.system").ServiceInfo = NS
        module("version").APP_VERSION = "v3.1.0"
        for name in ("app.schemas.file", "app.schemas.transfer"):
            sys.modules.pop(name, None)
        ns = {**vars(typing), "__name__": "app.schemas.types", "Enum": enum.Enum, "IntEnum": enum.IntEnum,
              "auto": enum.auto}
        selected("app/schemas/types.py", {"MessageType", "EventType", "MediaType", "ChainEventType"}, ns)
        module("app.schemas.types").__dict__.update(ns)
        self.MediaType = ns["MediaType"]
        ns = {**vars(typing), "__name__": "app.schemas.file", "BaseModel": BaseModel, "ConfigDict": ConfigDict,
              "Field": Field, "Path": Path, "re": re, "WINDOWS_DRIVE_PATTERN": re.compile(r"^[A-Za-z]:")}
        selected("app/schemas/file.py", {"FileURI", "FileItem"}, ns)
        module("app.schemas.file").__dict__.update(ns)
        self.FileItem = ns["FileItem"]
        ns = {**vars(typing), "__name__": "app.schemas.transfer", "BaseModel": BaseModel, "Field": Field,
              "FileItem": self.FileItem}
        selected("app/schemas/transfer.py", {"TransferInfo"}, ns)
        module("app.schemas.transfer").__dict__.update(ns)
        self.Info = ns["TransferInfo"]
        # Execution observations/results are the real V3 dataclasses.
        spec = importlib.util.spec_from_file_location("app.application.transfer.execution", REFERENCE / "app/application/transfer/execution.py")
        module("app.schemas.exception").BusinessRejectedError = type("BusinessRejectedError", (Exception,), {})
        execution = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = execution
        spec.loader.exec_module(execution)
        self.execution = execution
        history_ns = module("v3_test_history").__dict__
        history_ns.update(vars(typing), Base=SQLBase, Mapped=Mapped, mapped_column=mapped_column, String=String,
                          Integer=Integer, Boolean=Boolean, JSON=JSON, Index=Index, false=false, select=select,
                          get_id_column=lambda: mapped_column(Integer, primary_key=True),
                          media_identity_constraint=lambda *a, **kw: CheckConstraint("1=1"))
        history_ns["__name__"] = "v3_test_history"
        selected("app/db/models/transferhistory.py", {"TransferHistory"}, history_ns,
                 methods={"get_by_transfer_task_id", "upsert_by_transfer_task_id"})
        self.History = history_ns["TransferHistory"]
        db_engine = create_engine("sqlite:///" + str(self.directory / "native-history.db"), connect_args={"check_same_thread": False})
        SQLBase.metadata.create_all(db_engine)
        self.factory = sessionmaker(db_engine, expire_on_commit=False)
        self.db_engine = db_engine
        owner = self
        class HistoryRepository:
            def get(self, history_id):
                with owner.factory() as db:
                    return db.get(owner.History, history_id)
            def get_by_transfer_task_id(self, *, task_id):
                with owner.factory() as db:
                    return owner.History.get_by_transfer_task_id(db, task_id=task_id)
        self.history = HistoryRepository()
        class Local:
            def get_item_strict(self, path):
                return owner.FileItem(storage="local", path=str(path), type="file") if Path(path).exists() else None
            def delete(self, item):
                Path(item.path).unlink(missing_ok=True)
                return True
        class U115:
            pass
        module("app.modules.filemanager.storages.local").LocalStorage = Local
        module("app.modules.filemanager.storages.u115").U115Pan = U115
        self.Local, self.U115 = Local, U115
        class Data(NS):
            cancel = False
        native_ns = {**vars(typing), "__name__": "app.modules.filemanager.transhandler", "Path": Path,
                     "FileItem": self.FileItem, "TransferInfo": self.Info, "deepcopy": copy.deepcopy,
                     "re": re, "logger": logger, "MediaType": self.MediaType,
                     "eventmanager": NS(send_event=lambda *a, **kw: None), "ChainEventType": sys.modules["app.schemas.types"].ChainEventType,
                     "TransferInterceptEventData": Data, "TransferOverwriteCheckEventData": Data,
                     "StorageQueryError": type("StorageQueryError", (Exception,), {}),
                     "get_runtime_setting": lambda key: [".mkv", ".mp4"] if key == "RMT_MEDIAEXT" else [],
                     "MetaInfoPath": lambda _: NS(season=None, episode=None, part=None), **vars(execution)}
        native_ns["__name__"] = "app.modules.filemanager.transhandler"
        selected("app/modules/filemanager/transhandler.py", {"TransHandler"}, native_ns)
        self.Handler = native_ns["TransHandler"]
        chain_ns = {**vars(typing), "__name__": "app.chain.transfer", "TransferInfo": self.Info}
        selected("app/chain/transfer/plan.py", {"TransferPlanningOwner"}, chain_ns,
                 methods={"_TransferChain__execute_host_transfer_plan"}, base="object")
        class Chain(chain_ns["TransferPlanningOwner"]):
            def __init__(self):
                self.transfer_history_repository = owner.history
                self.transfer_execution_repository = NS(get_snapshot=lambda **kw: NS(state="failed"))
            def run_module(self, name, **kwargs):
                kwargs.pop("raise_exception", None)
                checkpoint = kwargs.pop("checkpoint")
                cleanup = kwargs.pop("cleanup_media_file")
                observe = kwargs.pop("observe_cleanup_media_file")
                old = checkpoint.planning_input.options.get("cleanup_dest_fileitem")
                return owner.Handler().execute_transfer_plan(checkpoint, **kwargs,
                    cleanup_before_transfer=(lambda: cleanup(owner.FileItem(**old))) if old else None,
                    observe_cleanup_before_transfer=(lambda: observe(owner.FileItem(**old))) if old else None)
            def _TransferChain__default_callback(self, task, info, /):
                # Exercise real V3 history upsert, including its success mapping
                # removal. Media events/transactional outbox are isolated ports.
                with owner.factory() as db:
                    old = owner.History.get_by_transfer_task_id(db, task_id=task.admission_task_id)
                    revision = old.transfer_settlement_revision + 1 if old else 1
                    record = owner.History.upsert_by_transfer_task_id(db, task_id=task.admission_task_id,
                        settlement_revision=revision, retain_task_mapping=not info.success,
                        payload=dict(src=task.fileitem.path, src_storage="local", status=info.success,
                                     dest=info.target_item.path if info.target_item else None,
                                     dest_fileitem=info.target_item.model_dump(mode="json") if info.target_item else {}))
                    db.commit()
                self._finish_scrape_batch_task(task)
                owner.events.append(info.success)
                return info.success, info.message or ""
            def _TransferChain__handle_transfer(self, task, callback=None):
                info = self._TransferChain__execute_host_transfer_plan(task, task.plan_checkpoint,
                    source_oper=Local(), target_oper=U115(), step_runner=task.runner)
                return (callback or self._TransferChain__default_callback)(task, info)
            def queue_failed_transfer_notification(self, *, task, transferinfo, **kwargs):
                owner.events.append("failure-notification")
            def _finish_scrape_batch_task(self, task):
                owner.finish_calls.append(task.admission_task_id)
            def _TransferChain__cleanup_transfer_destination(self, item):
                raise AssertionError("native destructive cleanup must be intercepted")
            def _TransferChain__observe_cleanup_destination(self, item):
                return owner.api.raw_path(item.path) is None
            def redo_transfer_history(self, history_id):
                owner.requests.append(history_id)
                return True, "已登记"
        for name, value in Chain.__dict__.items():
            if callable(value):
                value.__module__ = "app.chain.transfer"
        Chain.__module__ = "app.chain.transfer"
        module("app.chain.transfer").TransferChain = Chain
        self.Chain, self.requests = Chain, []
        self.plugin = load_plugin("p115instantwait")
        self.bridge = sys.modules[self.plugin.__name__ + ".bridge"]
        self.remote = sys.modules[self.plugin.__name__ + ".remote"]
        self.api = Fake115(self)
        self.bridge.OpenAPI = lambda *a, **kw: self.api
        self.bridge.InstantWaitEngine.run = lambda *a, **kw: None
        self.engine = self.bridge.InstantWaitEngine(self.directory, {**self.plugin.DEFAULTS, "retry_delays": [30]})
        self.engine.install()

    def close(self):
        self.engine.stop()
        self.db_engine.dispose()
        SQLBase.registry.dispose()
        SQLBase.metadata.clear()
        sys.modules.pop("v3_test_history", None)

    def task(self, path, mode="copy", cleanup=None):
        item = self.FileItem(storage="local", path=str(path), type="file", name=Path(path).name,
                             size=Path(path).stat().st_size, extension="mkv")
        checkpoint = NS(preview=False, skip_reason=None, rejection_error=None, resolved_transfer_type=mode,
            target_storage="u115", final_target_path="/library/Film.mkv", need_scrape=True, need_notify=True,
            overwrite_mode="always", planning_input=NS(source_fileitem=item.model_dump(mode="json"),
                                                       options={"cleanup_dest_fileitem": cleanup} if cleanup else {}),
            items=[NS(action="transfer", target_storage="u115", target_path="/library/Film.mkv",
                      source_fileitem=item.model_dump(mode="json"))])
        return NS(fileitem=item, admission_task_id=str(time.time_ns()), preview=False, plan_checkpoint=checkpoint,
                  runner=Runner(), meta=NS(), mediainfo=NS(type=self.MediaType.MOVIE))

    def enter(self, task):
        self.Chain()._TransferChain__handle_transfer(task)
        return (self.engine.store.active_for(self.bridge.source_key(task.fileitem))
                or next(row for row in self.engine.store.all() if row["source"] == self.bridge.source_key(task.fileitem)))


class Runner:
    def __init__(self):
        self.receipts = {}
    def run(self, *, phase, kind, payload, execute, observe):
        key = (phase, kind, json.dumps(payload, sort_keys=True))
        if key not in self.receipts:
            self.receipts[key] = execute()
        return self.receipts[key]


class Fake115:
    def __init__(self, h):
        self.h, self.files, self.hit = h, {}, False
        self.hashes, self.folders, self.moves = {}, [], []
        self.deletes = []
        self.inits = self.uploads = 0
        self.block_entered, self.block_release = threading.Event(), threading.Event()
        self.block = False
    def close(self):
        pass
    def get_folder(self, path):
        self.folders.append(str(path))
        return self.h.FileItem(storage="u115", type="dir", fileid="10", path=str(path).replace("\\", "/"))
    def get_item(self, path):
        path = str(path).replace("\\", "/")
        item = next((f.model_copy() for f in self.files.values() if f.path == path), None)
        if not item and any(str(PurePosixPath(f.path).parent) == path for f in self.files.values()):
            return self.h.FileItem(storage="u115", type="dir", path=path, fileid="10", name=PurePosixPath(path).name)
        return item
    get_item_strict = get_item
    def raw_path(self, path):
        item = self.get_item(path)
        return {"file_id": item.fileid} if item else None
    def verify(self, file_id, path, hashes):
        item = self.files.get(str(file_id))
        if not item:
            raise self.h.remote.RetryLater("不可见")
        if item.size != hashes["fingerprint"]["size"]:
            raise self.h.remote.PauseTask("大小错误")
        return item.model_copy(update={"path": str(path)})
    def raw_id(self, file_id):
        return {"sha1": self.hashes.get(str(file_id))}

    def instant(self, path, folder, name, hashes):
        self.inits += 1
        if self.block:
            self.block_entered.set()
            assert self.block_release.wait(10)
        if not self.hit:
            raise self.h.remote.NotInstant("未秒传")
        self.files["123"] = self.h.FileItem(storage="u115", type="file", fileid="123",
            path=str(PurePosixPath(folder.path) / name), size=hashes["fingerprint"]["size"], name=name)
        self.hashes["123"] = hashes["sha1"]
        return "123"
    def upload(self, path, folder, hashes, init, payload, checkpoint):
        self.uploads += 1
        self.hit = True
        self.instant(path, folder, PurePosixPath(payload["final_path"]).name, hashes)
        payload["upload_confirmed"] = True
        checkpoint()
    def move_id(self, file_id, folder, name):
        self.moves.append((file_id, str(folder.path)))
        self.files[str(file_id)] = self.files[str(file_id)].model_copy(update={"path": str(PurePosixPath(folder.path) / name)})

    def delete(self, item):
        current = self.get_item(item.path)
        if current and str(current.fileid) != str(item.fileid):
            raise self.h.remote.PauseTask("旧目标已变化")
        self.deletes.append(str(item.fileid))
        self.files.pop(str(item.fileid), None)
        return True

    def list(self, folder):
        return [item for item in self.files.values() if PurePosixPath(item.path).parent == PurePosixPath(folder.path)]
