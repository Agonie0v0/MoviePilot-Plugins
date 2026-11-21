"""Offline unit tests for configurable notify_events triggers across V2 and V3."""
import ast
from contextlib import contextmanager
from enum import Enum
import importlib
import importlib.util
import json
from pathlib import Path
import sys
import threading
import time
import types
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pydantic import BaseModel
import pytest


ROOT = Path(__file__).resolve().parents[1]
ALL_NOTIFY_EVENTS = ["not_instant", "max_retries", "auto_upload", "manual_action"]


def _register_module(name, snapshot):
    if name not in snapshot:
        snapshot[name] = sys.modules.get(name)
    mod = types.ModuleType(name)
    mod.__path__ = []
    sys.modules[name] = mod
    if "." in name:
        parent_name, attr = name.rsplit(".", 1)
        parent = sys.modules.get(parent_name) or _register_module(parent_name, snapshot)
        setattr(parent, attr, mod)
    return mod


class _StubBaseModel(BaseModel):
    pass


class _StubFileItem(_StubBaseModel):
    storage: str = "local"
    type: str = "file"
    path: str = ""
    name: str = ""
    basename: str = ""
    extension: str = ""
    size: int = 0
    fileid: str | None = None
    modify_time: float = 0.0
    modify_time_ns: int = 0
    change_time_ns: int = 0


class _StubTransferInfo(_StubBaseModel):
    success: bool = False
    fileitem: _StubFileItem | None = None
    target_item: _StubFileItem | None = None
    transfer_type: str | None = None
    message: str = ""
    need_notify: bool = True


class _StubMediaType(str, Enum):
    MOVIE = "电影"
    TV = "电视剧"


class _StubNotificationType(str, Enum):
    Manual = "Manual"
    Organize = "Organize"


class _StubSystemConfigKey(str, Enum):
    TransferExcludeWords = "TransferExcludeWords"


class _StubPluginBase:
    def __init__(self):
        self.data = {}
        self.messages = []
        self.saved_config = None
        self.systemmessage = SimpleNamespace(put=lambda value: self.messages.append(value))
        self._data_path = ""

    def get_data_path(self):
        return self._data_path

    def get_data(self, key):
        return self.data.get(key)

    def save_data(self, key, value):
        self.data[key] = value

    def update_config(self, config):
        self.saved_config = dict(config)
        return True

    def post_message(self, **kwargs):
        self.messages.append(kwargs)


class _StubMetaBase:
    def __init__(self, title="", isfile=True):
        self.name = title
        self.type = _StubMediaType.MOVIE
        self.begin_season = 1


class _StubMediaInfo:
    def __init__(self):
        self.type = _StubMediaType.MOVIE

    def from_dict(self, data):
        self.__dict__.update(data)
        if isinstance(getattr(self, "type", None), str):
            self.type = _StubMediaType(self.type)

    def to_dict(self):
        return {"type": self.type.value if isinstance(self.type, Enum) else self.type}


class _StubSessionContext:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def add(self, obj):
        pass

    def flush(self):
        pass

    def commit(self):
        pass


class _StubHistoryRecord:
    def __init__(self, history_id=101):
        self.id = history_id
        self.status = False
        self.errmsg = ""
        self.transfer_task_id = None

    def update(self, db, values):
        for key, value in values.items():
            setattr(self, key, value)


class _StubTransferHistoryOper:
    def __init__(self):
        self._db = None

    def get(self, history_id):
        return _StubHistoryRecord(history_id) if history_id else None

    def get_by_src(self, src, src_storage=None):
        return None

    def add_fail(self, **kwargs):
        return _StubHistoryRecord(101)


class _StubLocalStorage:
    def delete(self, item):
        return True


class _StubU115Pan:
    _limit_until = 0


class _StubTransferChain:
    __module__ = "app.chain.transfer"

    def __init__(self):
        self.jobview = SimpleNamespace(
            _job_view={},
            add_task=lambda *a, **kw: None,
            finish_task=lambda *a, **kw: None,
            fail_task=lambda *a, **kw: None,
            try_remove_job=lambda *a, **kw: None,
            is_success=lambda *a, **kw: True,
        )
        self.transfer_history_repository = SimpleNamespace(
            get=lambda history_id: _StubHistoryRecord(history_id),
            get_by_transfer_task_id=lambda task_id: None,
        )
        self.transfer_execution_repository = SimpleNamespace(
            get_snapshot=lambda task_id: SimpleNamespace(state="pending"),
        )

    def transfer(self, *args, preview=False, **kwargs):
        return _StubTransferInfo(success=False)

    def redo_transfer_history(self, history_id):
        return True, "ok"

    def _TransferChain__handle_transfer(self, *args, **kwargs):
        return False, ""

    def _TransferChain__default_callback(self, *args, **kw):
        return False, ""

    def _TransferChain__finish_scrape_batch_task(self, *args, **kwargs):
        pass

    def _TransferChain__register_scrape_batch_task(self, *args, **kwargs):
        pass

    def _TransferChain__close_scrape_batch(self, *args, **kwargs):
        pass

    def _TransferChain__record_scrape_target(self, *args, **kwargs):
        pass

    def _TransferChain__execute_host_transfer_plan(self, task, checkpoint, *, step_runner, source_oper, target_oper):
        return _StubTransferInfo(success=False)

    def queue_failed_transfer_notification(self, *args, **kwargs):
        pass

    def _finish_scrape_batch_task(self, *args, **kwargs):
        pass

    def _TransferChain__cleanup_transfer_destination(self, *args, **kwargs):
        return True


_StubTransferChain.__module__ = "app.chain.transfer"


@contextmanager
def stubbed_generation(generation):
    """Load V2 or V3 plugin package with isolated in-memory MoviePilot stubs."""
    snapshot = {}
    logs = []

    def make_log_fn(level):
        def log_fn(msg, *args, **kwargs):
            logs.append((level, str(msg)))
        return log_fn

    stub_logger = SimpleNamespace(
        debug=make_log_fn("debug"),
        info=make_log_fn("info"),
        warn=make_log_fn("warning"),
        warning=make_log_fn("warning"),
        error=make_log_fn("error"),
        exception=make_log_fn("error"),
    )

    pkg_name = f"notify_test_{generation}"
    try:
        for mod_name in (
            "version",
            "app",
            "app.log",
            "app.plugins",
            "app.core",
            "app.core.plugin",
            "app.core.context",
            "app.core.meta",
            "app.db",
            "app.db.systemconfig_oper",
            "app.db.models",
            "app.db.models.transferhistory",
            "app.db.transferhistory_oper",
            "app.chain",
            "app.chain.transfer",
            "app.modules",
            "app.modules.filemanager",
            "app.modules.filemanager.storages",
            "app.modules.filemanager.storages.local",
            "app.modules.filemanager.storages.u115",
            "app.schemas",
            "app.schemas.types",
            "app.schemas.file",
            "app.schemas.transfer",
            "app.sdk",
            "app.sdk.logging",
            "app.sdk.plugin",
        ):
            _register_module(mod_name, snapshot)

        sys.modules["version"].APP_VERSION = "v2.15.6"
        sys.modules["app.log"].logger = stub_logger
        sys.modules["app.sdk.logging"].logger = stub_logger
        sys.modules["app.plugins"]._PluginBase = _StubPluginBase
        sys.modules["app.sdk.plugin"]._PluginBase = _StubPluginBase
        sys.modules["app.sdk.plugin"].PluginManager = lambda: SimpleNamespace(running_plugins={})
        sys.modules["app.core.plugin"].PluginManager = lambda: SimpleNamespace(running_plugins={})
        sys.modules["app.core.context"].MediaInfo = _StubMediaInfo
        sys.modules["app.core.meta"].MetaBase = _StubMetaBase
        sys.modules["app.core.meta"].MetaVideo = _StubMetaBase
        sys.modules["app.core.meta"].MetaAnime = _StubMetaBase
        sys.modules["app.db"].SessionFactory = _StubSessionContext
        sys.modules["app.db.systemconfig_oper"].SystemConfigOper = lambda: SimpleNamespace(get=lambda *a: None)
        sys.modules["app.db.models.transferhistory"].TransferHistory = SimpleNamespace
        sys.modules["app.db.transferhistory_oper"].TransferHistoryOper = _StubTransferHistoryOper
        sys.modules["app.chain.transfer"].TransferChain = _StubTransferChain
        sys.modules["app.chain.transfer"].job_lock = threading.RLock()
        sys.modules["app.modules.filemanager.storages.local"].LocalStorage = _StubLocalStorage
        sys.modules["app.modules.filemanager.storages.u115"].U115Pan = _StubU115Pan

        sys.modules["app.schemas"].FileItem = _StubFileItem
        sys.modules["app.schemas"].TransferInfo = _StubTransferInfo
        sys.modules["app.schemas"].TransferTask = SimpleNamespace
        sys.modules["app.schemas"].NotificationType = _StubNotificationType
        sys.modules["app.schemas.file"].FileItem = _StubFileItem
        sys.modules["app.schemas.transfer"].TransferInfo = _StubTransferInfo
        sys.modules["app.schemas.types"].MediaType = _StubMediaType
        sys.modules["app.schemas.types"].SystemConfigKey = _StubSystemConfigKey
        sys.modules["app.schemas.types"].MessageType = _StubNotificationType

        for key in list(sys.modules):
            if key == pkg_name or key.startswith(pkg_name + "."):
                snapshot[key] = sys.modules.pop(key)

        snapshot[pkg_name] = sys.modules.get(pkg_name)
        plugin_dir = ROOT / f"plugins.{generation}" / "p115instantwait"
        spec = importlib.util.spec_from_file_location(
            pkg_name,
            plugin_dir / "__init__.py",
            submodule_search_locations=[str(plugin_dir)],
        )
        init_mod = importlib.util.module_from_spec(spec)
        sys.modules[pkg_name] = init_mod
        spec.loader.exec_module(init_mod)
        bridge = importlib.import_module(f"{pkg_name}.bridge")
        remote = importlib.import_module(f"{pkg_name}.remote")
        store = importlib.import_module(f"{pkg_name}.store")
        ui = importlib.import_module(f"{pkg_name}.ui")
        yield SimpleNamespace(
            generation=generation,
            init=init_mod,
            bridge=bridge,
            remote=remote,
            store=store,
            ui=ui,
            logs=logs,
        )
    finally:
        for key in list(sys.modules):
            if key == pkg_name or key.startswith(pkg_name + "."):
                sys.modules.pop(key, None)
        for key, previous in snapshot.items():
            if previous is None:
                sys.modules.pop(key, None)
            else:
                sys.modules[key] = previous


def _parse_defaults(generation):
    init_path = ROOT / f"plugins.{generation}" / "p115instantwait" / "__init__.py"
    tree = ast.parse(init_path.read_text(encoding="utf-8"))
    return next(
        ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "DEFAULTS" for t in node.targets)
    )


def _walk_with_ancestors(nodes, ancestors=()):
    for node in nodes:
        yield node, ancestors
        yield from _walk_with_ancestors(node.get("content", []), (*ancestors, node))


class _Fake115Api:
    def __init__(self, remote_mod, stop_event):
        self.remote = remote_mod
        self.stop = stop_event
        self.get_folder_fn = lambda path: _StubFileItem(storage="u115", type="dir", fileid="10", path=str(path), name="library")
        self.raw_path_fn = lambda path: None
        self.raw_id_fn = lambda file_id: None
        self.instant_fn = None
        self.upload_fn = None
        self.verify_fn = lambda file_id, path, hashes: _StubFileItem(storage="u115", type="file", fileid=str(file_id), path=str(path))
        self.delete_fn = lambda item: True

    def get_folder(self, path):
        return self.get_folder_fn(path)

    def raw_path(self, path):
        return self.raw_path_fn(path)

    def raw_id(self, file_id):
        return self.raw_id_fn(file_id)

    def instant(self, path, folder, name, hashes):
        if self.instant_fn is None:
            raise self.remote.NotInstant("未命中秒传")
        return self.instant_fn(path, folder, name, hashes)

    def upload(self, path, folder, hashes, init, payload, checkpoint):
        if self.upload_fn is None:
            raise self.remote.RetryLater("上传临时错误")
        return self.upload_fn(path, folder, hashes, init, payload, checkpoint)

    def verify(self, file_id, path, hashes):
        return self.verify_fn(file_id, path, hashes)

    def delete(self, item):
        return self.delete_fn(item)

    def close(self):
        pass


class _EngineHarness:
    def __init__(self, mods, tmp_path, config_overrides=None, omit_notify_events=False):
        self.mods = mods
        self.generation = mods.generation
        self.tmp_path = tmp_path
        self.tmp_path.mkdir(parents=True, exist_ok=True)
        self.source_path = tmp_path / "Film.mkv"
        self.source_path.write_bytes(b"video-content-for-notify-tests")
        self.hashes = mods.remote.hash_file(self.source_path, threading.Event())

        config = {
            **mods.init.DEFAULTS,
            "retry_intervals": "60,180,600",
            "retry_delays": [60, 180, 600],
            "max_retries": 2,
            "max_wait_hours": 24,
            "limit_action": "manual",
            **(config_overrides or {}),
        }
        if omit_notify_events:
            config.pop("notify_events", None)

        self.notifications = []
        self.engine = mods.bridge.InstantWaitEngine(tmp_path, config, notify=self._record_notify)
        self.api = _Fake115Api(mods.remote, self.engine.stop_event)
        self.engine._open_api = lambda: self.api

    def _record_notify(self, title, text, manual=True):
        self.notifications.append({"title": title, "text": text, "manual": manual})
        return True

    def enqueue(self, *, state="queued", wait_since=None, attempts=0, auto_attempts=0,
                upload_attempts=0, upload_origin=None, extra_payload=None):
        now = time.time()
        payload = {
            "layout": "direct",
            "native_plan": True,
            "final_path": "/library/Film.mkv",
            "source_initial": self.hashes["fingerprint"],
            "hashes": self.hashes,
            "wait_since": now if wait_since is None else wait_since,
            "auto_attempts": auto_attempts,
            "upload_attempts": upload_attempts,
            "task": {
                "fileitem": {
                    "storage": "local",
                    "type": "file",
                    "path": str(self.source_path),
                    "name": self.source_path.name,
                    "size": self.source_path.stat().st_size,
                }
            },
        }
        if self.generation == "v3":
            payload["host_generation"] = 3
            payload["native_task_id"] = f"admission-{time.time_ns()}"
            payload["mode"] = "copy"
        if upload_origin is not None:
            payload["upload_origin"] = upload_origin
        if extra_payload:
            payload.update(extra_payload)

        row, _ = self.engine.store.enqueue(f"local:{self.source_path.resolve()}", payload)
        self.engine.store.update(row["id"], state=state, ready=1, attempts=attempts,
                                 next_at=0, history_id=101, payload=payload)
        if self.generation == "v2":
            self.engine.live_tasks[row["id"]] = SimpleNamespace(
                fileitem=_StubFileItem(storage="local", type="file", path=str(self.source_path),
                                       name=self.source_path.name, size=self.source_path.stat().st_size),
                transfer_type="copy",
                meta=_StubMetaBase("Film"),
                mediainfo=_StubMediaInfo(),
                downloader=None,
                download_hash=None,
                transfer_batch_id=None,
            )
        return self.engine.store.get(row["id"])

    def run_next(self, key, manual=False):
        self.engine.store.update(key, next_at=0, ready=1)
        row = self.engine.store.claim(manual=manual)
        assert row is not None and row["id"] == key
        self.engine.execute(row)
        return self.engine.store.get(key)

    def clear_logs(self):
        self.mods.logs.clear()

    def skip_notify_info_logs(self):
        return [msg for level, msg in self.mods.logs if level == "info" and "跳过通知" in msg]


@pytest.fixture
def env(tmp_path, request):
    generation = request.param
    with stubbed_generation(generation) as mods:
        yield mods, tmp_path


@pytest.mark.parametrize("generation,expected_version", [("v2", "0.4.7"), ("v3", "1.3.7")])
def test_defaults_ast_literal_eval_and_package_version(generation, expected_version):
    defaults = _parse_defaults(generation)
    assert defaults["notify"] is True
    assert defaults["notify_events"] == ALL_NOTIFY_EVENTS

    pkg_data = json.loads((ROOT / f"package.{generation}.json").read_text(encoding="utf-8"))
    assert pkg_data["P115InstantWait"]["version"] == expected_version
    assert f"v{expected_version}" in pkg_data["P115InstantWait"]["history"]

    with stubbed_generation(generation) as mods:
        assert mods.init.DEFAULTS["notify_events"] == ALL_NOTIFY_EVENTS
        assert mods.init.P115InstantWait.plugin_version == expected_version


@pytest.mark.parametrize("generation", ["v2", "v3"])
def test_initialize_accepts_subsets_deduplicates_empty_and_rejects_invalid(generation, tmp_path):
    with stubbed_generation(generation) as mods:
        captured = []

        class FakeEngine:
            def __init__(self, data_path, runtime, notify_cb):
                captured.append(runtime)
                self.stop_event = threading.Event()
                self.runtime_lock = None
                self.store = SimpleNamespace()

            def install(self, before_start=None):
                pass

            def stop(self, timeout=35):
                return True

        with patch.object(mods.init, "InstantWaitEngine", FakeEngine):
            for raw_events, expected in (
                (["manual_action", "not_instant"], ["manual_action", "not_instant"]),
                (["auto_upload", "not_instant", "auto_upload", "max_retries", "not_instant"],
                 ["auto_upload", "not_instant", "max_retries"]),
                ([], []),
                (None, []),
            ):
                plugin = mods.init.P115InstantWait()
                plugin._data_path = str(tmp_path)
                captured.clear()
                plugin._initialize({"enabled": True, "notify_events": raw_events})
                assert plugin._error == ""
                assert len(captured) == 1
                assert captured[0]["notify_events"] == expected

            # Omitted key in legacy config falls back to DEFAULTS (all 4 enabled)
            plugin = mods.init.P115InstantWait()
            plugin._data_path = str(tmp_path)
            captured.clear()
            plugin._initialize({"enabled": True})
            assert plugin._error == ""
            assert captured[0]["notify_events"] == ALL_NOTIFY_EVENTS

            for bad_value in (
                "not_instant",
                123,
                {"not_instant": True},
                ["not_instant", "unknown_event"],
                [123],
                [None],
            ):
                plugin = mods.init.P115InstantWait()
                plugin._data_path = str(tmp_path)
                captured.clear()
                plugin._initialize({"enabled": True, "notify_events": bad_value})
                assert "通知触发条件配置无效" in plugin._error
                assert captured == []


@pytest.mark.parametrize("generation", ["v2", "v3"])
def test_ast_extracted_initialize_requires_no_extra_globals(generation, tmp_path):
    init_path = ROOT / f"plugins.{generation}" / "p115instantwait" / "__init__.py"
    tree = ast.parse(init_path.read_text(encoding="utf-8"))
    defaults = _parse_defaults(generation)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "P115InstantWait")
    cls.bases = []
    cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_initialize"]

    captured = []

    class FakeEngine:
        def __init__(self, data_path, runtime, notify_cb):
            captured.append(runtime)
            self.stop_event = threading.Event()

        def install(self, before_start=None):
            pass

    namespace = {
        "DEFAULTS": defaults,
        "InstantWaitEngine": FakeEngine,
        "logger": Mock(),
    }
    exec(compile(ast.Module(body=[cls], type_ignores=[]), str(init_path), "exec"), namespace)

    snapshot = {"version": sys.modules.get("version")}
    try:
        version_mod = types.ModuleType("version")
        version_mod.APP_VERSION = "v2.15.6"
        sys.modules["version"] = version_mod

        plugin = namespace["P115InstantWait"]()
        plugin._runtime = SimpleNamespace(lock=threading.RLock())
        plugin._error = ""
        plugin._engine = None
        plugin.systemmessage = SimpleNamespace(put=lambda *a: None)
        plugin.get_data_path = lambda: str(tmp_path)
        plugin._notify = lambda *a, **kw: True

        plugin._initialize({"enabled": True, "notify_events": ["auto_upload", "auto_upload", "not_instant"]})
        assert plugin._error == ""
        assert captured[0]["notify_events"] == ["auto_upload", "not_instant"]

        plugin._initialize({"enabled": True, "notify_events": ["invalid_event"]})
        assert "通知触发条件配置无效" in plugin._error
    finally:
        if snapshot["version"] is None:
            sys.modules.pop("version", None)
        else:
            sys.modules["version"] = snapshot["version"]


def test_preview_ui_ast_defaults_and_form_construction():
    preview_script = ROOT / "scripts" / "preview_ui.py"
    snapshot = {k: sys.modules.get(k) for k in ("p115_preview", "p115_preview.ui", "p115_preview.records", "preview_ui_under_test")}
    try:
        spec = importlib.util.spec_from_file_location("preview_ui_under_test", preview_script)
        preview_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(preview_mod)

        assert preview_mod.defaults["notify_events"] == ALL_NOTIFY_EVENTS
        model = {**preview_mod.defaults, "enabled": True}
        for scope in ("tasks", "history"):
            model.update(preview_mod.ui.record_search_model(scope, preview_mod.search_records(scope)))
        form = preview_mod.ui.config_form(preview_mod.TASKS, "", preview_mod.BATCH_RESULT)
        dumped = json.dumps({"form": form, "defaults": model}, ensure_ascii=False)
        assert "notify_events" in dumped
    finally:
        for key in list(sys.modules):
            if key == "p115_preview" or key.startswith("p115_preview.") or key == "preview_ui_under_test":
                sys.modules.pop(key, None)
        for key, prev in snapshot.items():
            if prev is not None:
                sys.modules[key] = prev


@pytest.mark.parametrize("generation", ["v2", "v3"])
def test_ui_config_form_notify_events_select_and_visibility(generation):
    with stubbed_generation(generation) as mods:
        form = mods.ui.config_form([], "")
        nodes = list(_walk_with_ancestors(form))

        notify_switch = next(
            (n for n, _ in nodes if n.get("component") == "VSwitch" and n.get("props", {}).get("model") == "notify"),
            None,
        )
        assert notify_switch is not None

        select_match = next(
            ((n, anc) for n, anc in nodes
             if n.get("component") == "VSelect" and n.get("props", {}).get("model") == "notify_events"),
            None,
        )
        assert select_match is not None
        vselect, ancestors = select_match
        props = vselect["props"]
        assert props["multiple"] is True
        assert props["chips"] is True
        assert props["closable-chips"] is True
        assert [item["value"] for item in props["items"]] == ALL_NOTIFY_EVENTS
        assert any(a.get("props", {}).get("show") == "{{ notify }}" for a in ancestors)


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_only_not_instant_selected_and_resume_cycle(env):
    mods, tmp_path = env
    h = _EngineHarness(mods, tmp_path, {"notify_events": ["not_instant"], "max_retries": 2, "limit_action": "manual"})
    key = h.enqueue()["id"]

    # 1st NotInstant -> notifies once with waiting/retry content
    row = h.run_next(key)
    assert row["state"] == "waiting"
    assert len(h.notifications) == 1
    first = h.notifications[0]
    assert first["manual"] is False
    assert "整理暂未成功" in first["title"]
    assert "下次重试" in first["text"]
    assert "继续等待" in first["text"]

    # 2nd NotInstant -> does not notify again in same cycle
    row = h.run_next(key)
    assert row["state"] == "waiting"
    assert len(h.notifications) == 1

    # 3rd NotInstant reaches max_retries=2 -> paused without notifying
    row = h.run_next(key)
    assert row["state"] == "paused"
    assert len(h.notifications) == 1

    # Resume resets waiting cycle -> next NotInstant notifies again
    resumed = h.engine.store.command(key, "resume")
    assert resumed["state"] == "waiting"
    assert "not_instant_notified" not in resumed["payload"]
    row = h.run_next(key)
    assert row["state"] == "waiting"
    assert len(h.notifications) == 2

    # Transitioning to upload_queued after first NotInstant must not notify under ["not_instant"]
    h2 = _EngineHarness(mods, tmp_path / "upload_limit",
                        {"notify_events": ["not_instant"], "max_retries": 1, "limit_action": "upload"})
    key2 = h2.enqueue()["id"]
    h2.run_next(key2)
    assert len(h2.notifications) == 1
    row2 = h2.run_next(key2)
    assert row2["state"] == "upload_queued"
    assert len(h2.notifications) == 1

    # PauseTask (e.g., expired auth) must not notify under ["not_instant"]
    h3 = _EngineHarness(mods, tmp_path / "auth_pause",
                        {"notify_events": ["not_instant"], "max_retries": 2})
    h3.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.PauseTask("115 授权已失效"))
    key3 = h3.enqueue()["id"]
    row3 = h3.run_next(key3)
    assert row3["state"] == "paused"
    assert len(h3.notifications) == 0


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_retry_later_before_not_instant_notifies_only_on_real_not_instant(env):
    mods, tmp_path = env
    h = _EngineHarness(mods, tmp_path / "folder_timeout", {"notify_events": ["not_instant"], "max_retries": 3})
    key = h.enqueue()["id"]

    # Attempt 1: network timeout before instant -> RetryLater (must NOT trigger not_instant notification)
    def timeout_get_folder(path):
        raise mods.remote.RetryLater("网络超时")

    h.api.get_folder_fn = timeout_get_folder
    row = h.run_next(key)
    assert row["state"] == "waiting"
    assert len(h.notifications) == 0
    assert not row["payload"].get("not_instant_notified")

    # Attempt 2: folder succeeds, instant misses -> NotInstant (triggers 1 notification)
    h.api.get_folder_fn = lambda path: _StubFileItem(storage="u115", type="dir", fileid="10", path=str(path), name="library")
    h.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.NotInstant("未命中秒传"))
    row = h.run_next(key)
    assert row["state"] == "waiting"
    assert len(h.notifications) == 1
    assert row["payload"].get("not_instant_notified") is True

    # Also verify RetryLater raised directly from api.instant() before NotInstant
    h2 = _EngineHarness(mods, tmp_path / "instant_timeout", {"notify_events": ["not_instant"], "max_retries": 3})
    h2.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.RetryLater("秒传初始化超时"))
    key2 = h2.enqueue()["id"]
    row2 = h2.run_next(key2)
    assert row2["state"] == "waiting"
    assert len(h2.notifications) == 0
    assert not row2["payload"].get("not_instant_notified")

    # Advance past the 60s reconciliation grace window so prepare_direct re-invokes api.instant()
    payload2 = row2["payload"]
    payload2["missing_since"] = time.time() - 65
    h2.engine.store.update(key2, payload=payload2)
    h2.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.NotInstant("未命中秒传"))
    row2 = h2.run_next(key2)
    assert row2["state"] == "waiting"
    assert len(h2.notifications) == 1


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_only_max_retries_selected(env):
    mods, tmp_path = env

    # Both limit_action="manual" and limit_action="upload" notify once upon reaching max_retries
    for idx, limit_action in enumerate(("manual", "upload")):
        h = _EngineHarness(mods, tmp_path / f"retries_{idx}",
                           {"notify_events": ["max_retries"], "max_retries": 1, "limit_action": limit_action})
        key = h.enqueue()["id"]
        row = h.run_next(key)
        assert row["state"] == "waiting"
        assert len(h.notifications) == 0

        row = h.run_next(key)
        assert row["state"] == ("paused" if limit_action == "manual" else "upload_queued")
        assert len(h.notifications) == 1
        assert h.notifications[0]["manual"] is (limit_action == "manual")
        if limit_action == "upload":
            assert h.engine.upload_wake.is_set()

    # Timeout via max_wait_hours also triggers max_retries regardless of limit_action
    for idx, limit_action in enumerate(("manual", "upload")):
        h = _EngineHarness(mods, tmp_path / f"timeout_{idx}",
                           {"notify_events": ["max_retries"], "max_retries": 5, "max_wait_hours": 1,
                            "limit_action": limit_action})
        key = h.enqueue(wait_since=time.time() - 7200)["id"]
        row = h.run_next(key)
        assert row["state"] == ("paused" if limit_action == "manual" else "upload_queued")
        assert len(h.notifications) == 1

    # Non-limit PauseTask (auth expired) must NOT notify under ["max_retries"]
    h_pause = _EngineHarness(mods, tmp_path / "pause_non_limit",
                             {"notify_events": ["max_retries"], "max_retries": 3})
    h_pause.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.PauseTask("115 授权已失效"))
    key_pause = h_pause.enqueue()["id"]
    row_pause = h_pause.run_next(key_pause)
    assert row_pause["state"] == "paused"
    assert len(h_pause.notifications) == 0


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_only_auto_upload_selected(env):
    mods, tmp_path = env

    # Limit with limit_action="upload" -> upload_queued notifies once and sets upload_wake
    h = _EngineHarness(mods, tmp_path / "auto_up",
                       {"notify_events": ["auto_upload"], "max_retries": 1, "limit_action": "upload"})
    key = h.enqueue()["id"]
    assert h.run_next(key)["state"] == "waiting"
    assert len(h.notifications) == 0

    row = h.run_next(key)
    assert row["state"] == "upload_queued"
    assert len(h.notifications) == 1
    assert h.notifications[0]["manual"] is False
    assert h.engine.upload_wake.is_set()

    # Uploading temporary error triggers upload_retry=True -> must NOT re-notify
    h.engine.upload_wake.clear()
    h.api.upload_fn = lambda *a: (_ for _ in ()).throw(mods.remote.RetryLater("OSS 临时超时"))
    row = h.run_next(key, manual=True)
    assert row["state"] == "upload_queued"
    assert len(h.notifications) == 1
    assert h.engine.upload_wake.is_set()

    # Limit with limit_action="manual" -> paused must NOT notify under ["auto_upload"]
    h_manual = _EngineHarness(mods, tmp_path / "manual_limit",
                              {"notify_events": ["auto_upload"], "max_retries": 0, "limit_action": "manual"})
    key_m = h_manual.enqueue()["id"]
    row_m = h_manual.run_next(key_m)
    assert row_m["state"] == "paused"
    assert len(h_manual.notifications) == 0


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_only_manual_action_selected(env):
    mods, tmp_path = env

    # 1. Reaches max_retries with limit_action="manual" -> paused notifies once; initial NotInstant does not
    h = _EngineHarness(mods, tmp_path / "limit_pause",
                       {"notify_events": ["manual_action"], "max_retries": 1, "limit_action": "manual"})
    key = h.enqueue()["id"]
    assert h.run_next(key)["state"] == "waiting"
    assert len(h.notifications) == 0
    assert h.run_next(key)["state"] == "paused"
    assert len(h.notifications) == 1
    assert h.notifications[0]["manual"] is True

    # Already paused -> calling defer again to paused must NOT re-notify
    h.engine.defer(h.engine.store.get(key), "paused", "重复暂停")
    assert h.engine.store.get(key)["state"] == "paused"
    assert len(h.notifications) == 1

    # 2. PauseTask (auth expired) -> paused notifies once
    h_auth = _EngineHarness(mods, tmp_path / "auth_pause",
                            {"notify_events": ["manual_action"], "max_retries": 3})
    h_auth.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.PauseTask("115 授权已失效"))
    key_auth = h_auth.enqueue()["id"]
    assert h_auth.run_next(key_auth)["state"] == "paused"
    assert len(h_auth.notifications) == 1
    assert h_auth.notifications[0]["manual"] is True

    # 3. Upload exhausts retries -> paused notifies once
    h_up_fail = _EngineHarness(mods, tmp_path / "upload_exhausted",
                               {"notify_events": ["manual_action"], "max_retries": 1})
    h_up_fail.api.upload_fn = lambda *a: (_ for _ in ()).throw(mods.remote.RetryLater("OSS 失败"))
    key_up = h_up_fail.enqueue(state="upload_queued", upload_attempts=1)["id"]
    assert h_up_fail.run_next(key_up, manual=True)["state"] == "paused"
    assert len(h_up_fail.notifications) == 1
    assert h_up_fail.notifications[0]["manual"] is True

    # 4. Transition to upload_queued must NOT notify under ["manual_action"]
    h_to_up = _EngineHarness(mods, tmp_path / "to_upload_queued",
                             {"notify_events": ["manual_action"], "max_retries": 0, "limit_action": "upload"})
    key_to_up = h_to_up.enqueue()["id"]
    assert h_to_up.run_next(key_to_up)["state"] == "upload_queued"
    assert len(h_to_up.notifications) == 0

    # 5. Generation-specific pause transitions:
    #    - V2: restore() failure transitioning non-paused -> paused notifies once
    #    - V3: finalizing retry exhaustion -> paused notifies once
    if mods.generation == "v2":
        h_restore = _EngineHarness(mods, tmp_path / "restore_fail",
                                   {"notify_events": ["manual_action"], "max_retries": 2})
        bad_key = h_restore.enqueue(
            state="waiting",
            extra_payload={"task": {"meta": {"class": "UnknownMeta", "data": {}}, "mediainfo": {}, "fileitem": {"path": str(h_restore.source_path)}}},
        )["id"]
        h_restore.engine.restore()
        assert h_restore.engine.store.get(bad_key)["state"] == "paused"
        assert len(h_restore.notifications) == 1
        assert h_restore.notifications[0]["manual"] is True

        # Calling restore() again when already paused must not re-notify
        h_restore.engine.restore()
        assert len(h_restore.notifications) == 1

        # When manual_action is not enabled, restore() failure skips notification with INFO log
        h_restore_silent = _EngineHarness(mods, tmp_path / "restore_fail_silent",
                                          {"notify_events": [], "max_retries": 2})
        h_restore_silent.enqueue(
            state="waiting",
            extra_payload={"task": {"meta": {"class": "UnknownMeta", "data": {}}, "mediainfo": {}, "fileitem": {"path": str(h_restore_silent.source_path)}}},
        )
        h_restore_silent.clear_logs()
        h_restore_silent.engine.restore()
        assert len(h_restore_silent.notifications) == 0
        assert h_restore_silent.skip_notify_info_logs()
    else:
        h_fin = _EngineHarness(mods, tmp_path / "finalize_fail",
                               {"notify_events": ["manual_action"], "max_retries": 0})
        fin_row = h_fin.enqueue(state="finalizing")
        h_fin.engine.defer(fin_row, "finalizing", "回写暂不可用")
        assert h_fin.engine.store.get(fin_row["id"])["state"] == "paused"
        assert len(h_fin.notifications) == 1
        assert h_fin.notifications[0]["manual"] is True


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
@pytest.mark.parametrize("limit_action,expected_state,expected_manual", [
    ("manual", "paused", True),
    ("upload", "upload_queued", False),
])
def test_multi_event_deduplication_with_max_retries_zero(env, limit_action, expected_state, expected_manual):
    mods, tmp_path = env
    h = _EngineHarness(
        mods,
        tmp_path / limit_action,
        {"notify_events": list(ALL_NOTIFY_EVENTS), "max_retries": 0, "limit_action": limit_action},
    )
    key = h.enqueue()["id"]
    row = h.run_next(key)
    assert row["state"] == expected_state
    assert len(h.notifications) == 1
    assert h.notifications[0]["manual"] is expected_manual
    if expected_state == "upload_queued":
        assert h.engine.upload_wake.is_set()


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_empty_notify_events_silences_notifications_without_breaking_state_or_wake(env):
    mods, tmp_path = env
    h = _EngineHarness(
        mods,
        tmp_path / "silent_upload",
        {"notify_events": [], "max_retries": 1, "limit_action": "upload"},
    )
    key = h.enqueue()["id"]

    # Attempt 1: waiting, 0 notify, logs "跳过通知" at INFO level
    h.clear_logs()
    assert h.run_next(key)["state"] == "waiting"
    assert len(h.notifications) == 0
    assert h.skip_notify_info_logs()

    # Attempt 2: upload_queued, upload_wake still set, 0 notify, logs "跳过通知"
    h.clear_logs()
    assert h.run_next(key)["state"] == "upload_queued"
    assert len(h.notifications) == 0
    assert h.engine.upload_wake.is_set()
    assert h.skip_notify_info_logs()

    # PauseTask also transitions cleanly and logs "跳过通知"
    h2 = _EngineHarness(mods, tmp_path / "silent_pause", {"notify_events": [], "max_retries": 2})
    h2.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.PauseTask("115 授权已失效"))
    key2 = h2.enqueue()["id"]
    h2.clear_logs()
    assert h2.run_next(key2)["state"] == "paused"
    assert len(h2.notifications) == 0
    assert h2.skip_notify_info_logs()


@pytest.mark.parametrize("env", ["v2", "v3"], indirect=True)
def test_backward_compatibility_when_notify_events_missing_from_engine_config(env):
    mods, tmp_path = env

    # 1. not_instant fires + upload_queued (max_retries + auto_upload) fires
    h = _EngineHarness(
        mods,
        tmp_path / "legacy_upload",
        {"max_retries": 1, "limit_action": "upload"},
        omit_notify_events=True,
    )
    assert "notify_events" not in h.engine.config
    key = h.enqueue()["id"]
    assert h.run_next(key)["state"] == "waiting"
    assert len(h.notifications) == 1
    assert h.notifications[0]["manual"] is False

    assert h.run_next(key)["state"] == "upload_queued"
    assert len(h.notifications) == 2
    assert h.notifications[1]["manual"] is False
    assert h.engine.upload_wake.is_set()

    # 2. manual_action fires on PauseTask
    h2 = _EngineHarness(
        mods,
        tmp_path / "legacy_pause",
        {"max_retries": 2},
        omit_notify_events=True,
    )
    assert "notify_events" not in h2.engine.config
    h2.api.instant_fn = lambda *a: (_ for _ in ()).throw(mods.remote.PauseTask("115 授权已失效"))
    key2 = h2.enqueue()["id"]
    assert h2.run_next(key2)["state"] == "paused"
    assert len(h2.notifications) == 1
    assert h2.notifications[0]["manual"] is True
