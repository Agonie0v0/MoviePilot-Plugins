import ast
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from .support import Harness, REFERENCE, ROOT, load_plugin, module, selected


@pytest.fixture
def h(tmp_path):
    assert (REFERENCE / "version.py").is_file(), "Set MP_V3_REFERENCE to pinned MoviePilot V3 source"
    value = Harness(tmp_path)
    try:
        yield value
    finally:
        value.api.block_release.set()
        value.close()


@pytest.fixture
def task(h, tmp_path):
    path = tmp_path / "Film.mkv"
    path.write_bytes(b"test media content")
    return h.task(path)


def stage(h, task):
    row = h.enter(task)
    h.api.hit = True
    h.engine.execute(h.engine.store.claim())
    return h.engine.store.get(row["id"])


def test_wait_failure_then_same_history_success(h, task):
    row = h.enter(task)
    assert row["ready"] and row["history_id"]
    assert not h.history.get(row["history_id"]).status
    assert h.api.inits == 0 and h.finish_calls == []
    h.engine.execute(h.engine.store.claim())
    assert h.engine.store.get(row["id"])["state"] == "waiting"
    assert not h.history.get(row["history_id"]).status
    h.api.hit = True
    h.engine.store.update(row["id"], next_at=0)
    h.engine.execute(h.engine.store.claim())
    assert h.engine.store.get(row["id"])["state"] == "finalizing"
    assert h.requests == [row["history_id"]]
    assert not h.history.get(row["history_id"]).status
    h.Chain()._TransferChain__handle_transfer(task)
    final = h.engine.store.get(row["id"])
    assert final["state"] == "completed"
    history = h.history.get(row["history_id"])
    assert history.status and history.transfer_task_id is None  # Real V3 removes success mapping.
    assert history.id == row["history_id"]
    assert h.finish_calls == [task.admission_task_id]
    assert Path(task.fileitem.path).exists()
    assert h.api.get_item("/library/Film.mkv").fileid == "123"


@pytest.mark.parametrize("mode", ["copy", "move"])
def test_native_finalization_and_replay_receipts(h, task, mode):
    task.plan_checkpoint.resolved_transfer_type = mode
    row = stage(h, task)
    original = h.Chain._TransferChain__default_callback
    with patch.object(h.Chain, "_TransferChain__default_callback", side_effect=RuntimeError("lost settlement response")):
        with pytest.raises(RuntimeError):
            h.Chain()._TransferChain__handle_transfer(task)
    assert Path(task.fileitem.path).exists() == (mode == "copy")
    assert h.engine.store.get(row["id"])["state"] == "finalizing"
    h.Chain()._TransferChain__handle_transfer(task)
    assert h.engine.store.get(row["id"])["state"] == "completed"
    assert h.api.inits == 1 and h.api.uploads == 0


@pytest.mark.parametrize("action", ["manual", "upload"])
@pytest.mark.parametrize("limit", ["count", "time"])
def test_limits_and_notifications(h, task, action, limit):
    h.engine.config.update(max_retries=0, limit_action=action)
    notices = []
    h.engine.notify = lambda title, text, **kw: notices.append(title)
    row = h.enter(task)
    if limit == "time":
        data = row["payload"]
        data["wait_since"] = time.time() - 7200
        h.engine.config.update(max_wait_hours=1, max_retries=10)
        h.engine.store.update(row["id"], payload=data)
    h.engine.execute(h.engine.store.claim())
    saved = h.engine.store.get(row["id"])
    assert saved["state"] == ("paused" if action == "manual" else "upload_queued")
    assert len(notices) == 1 and not h.history.get(row["history_id"]).status
    if action == "upload":
        h.engine.execute(h.engine.store.claim(manual=True))
        assert h.api.uploads == 1
        h.Chain()._TransferChain__handle_transfer(task)
        assert h.history.get(row["history_id"]).status


def test_any_waiting_task_force_upload_and_batch_dedup(h, task):
    row = h.enter(task)
    result = h.engine.control_many([row["id"], row["id"], "missing"], "upload")
    assert (result["accepted"], result["skipped"]) == (1, 1)
    assert h.engine.store.claim() is None
    h.engine.execute(h.engine.store.claim(manual=True))
    assert h.api.uploads == 1
    h.Chain()._TransferChain__handle_transfer(task)
    assert h.engine.store.get(row["id"])["state"] == "completed"


def test_upload_does_not_block_other_native_tasks(h, task, tmp_path):
    row = h.enter(task)
    h.engine.control(row["id"], "upload")
    h.api.block = True
    worker = threading.Thread(target=h.engine.execute, args=(h.engine.store.claim(manual=True),))
    worker.start()
    assert h.api.block_entered.wait(3)
    try:
        path = tmp_path / "Other.mkv"
        path.write_bytes(b"another media")
        other = h.task(path)
        start = time.monotonic()
        second = h.enter(other)
        assert time.monotonic() - start < 1
        assert second["ready"] and second["history_id"] != row["history_id"]
        assert not h.history.get(second["history_id"]).status
    finally:
        h.api.block_release.set()
        worker.join(3)
    assert not worker.is_alive()


def test_back_up_native_overwrite_and_cleanup(h, task):
    old = h.FileItem(storage="u115", type="file", path="/library/Old.mkv", fileid="999", name="Old.mkv", size=2)
    h.api.files["999"] = old
    task.plan_checkpoint.planning_input.options["cleanup_dest_fileitem"] = old.model_dump(mode="json")
    row = stage(h, task)
    h.Chain()._TransferChain__handle_transfer(task)
    assert ".mp115-backups/" in h.api.files["999"].path
    assert h.history.get(row["history_id"]).status
    assert h.engine.store.get(row["id"])["payload"]["backups"][0]["moved"]


def test_existing_final_file_is_backed_up(h, task):
    h.api.files["999"] = h.FileItem(storage="u115", type="file", path="/library/Film.mkv", fileid="999", name="Film.mkv", size=2)
    row = stage(h, task)
    h.Chain()._TransferChain__handle_transfer(task)
    assert h.history.get(row["history_id"]).status
    assert ".mp115-backups/" in h.api.files["999"].path


def test_source_change_pauses_without_upload_or_success(h, task):
    row = h.enter(task)
    Path(task.fileitem.path).write_bytes(b"changed")
    h.engine.config["limit_action"] = "upload"
    h.engine.execute(h.engine.store.claim())
    assert h.engine.store.get(row["id"])["state"] == "paused"
    assert h.api.inits == h.api.uploads == 0
    assert not h.history.get(row["history_id"]).status


def test_directory_cleanup_is_refused_before_staging(h, task):
    task.plan_checkpoint.planning_input.options["cleanup_dest_fileitem"] = dict(storage="u115", type="dir", path="/library/old")
    h.Chain()._TransferChain__handle_transfer(task)
    assert h.engine.store.all() == [] and h.api.inits == 0


def test_finalizing_manual_control_is_fenced(h, task):
    row = stage(h, task)
    for action in ("upload", "resume", "pause", "cancel"):
        with pytest.raises(ValueError):
            h.engine.control(row["id"], action)


def test_legacy_queue_preserved_and_blocked(h, task):
    row = h.enter(task)
    data = row["payload"]
    del data["host_generation"]
    h.engine.store.update(row["id"], payload=data, state="upload_queued")
    h.engine.restore()
    assert h.engine.store.get(row["id"])["state"] == "paused"
    with pytest.raises(ValueError):
        h.engine.control(row["id"], "upload")
    h.engine.control(row["id"], "cancel")
    assert h.engine.store.get(row["id"])["payload"] == data


def test_restart_reconcile_success_same_id(h, task):
    row = stage(h, task)
    h.Chain()._TransferChain__handle_transfer(task)
    h.engine.store.update(row["id"], state="finalizing")
    h.engine.restore()
    assert h.engine.store.get(row["id"])["state"] == "completed"
    assert h.api.inits == 1


def test_native_manual_review_stops_finalize_poll(h, task):
    row = stage(h, task)
    h.engine.store.update(row["id"], next_at=0)
    h.engine.chain.transfer_execution_repository.get_snapshot = lambda **kw: NS(state="manual_review")
    h.engine.poll_finalizing()
    assert h.engine.store.get(row["id"])["state"] == "paused"


def test_uninstall_restores_inherited_native_gate(h, task):
    original = h.engine.patches[0][2]
    h.enter(task)
    h.engine.stop()
    assert h.Chain._TransferChain__execute_host_transfer_plan is original
    assert "_TransferChain__execute_host_transfer_plan" not in h.Chain.__dict__
    assert h.finish_calls == [task.admission_task_id]


def test_api_response_models_preserve_bare_json(h, task):
    row = h.enter(task)
    plugin = h.plugin.P115InstantWait()
    plugin._engine = h.engine
    app = FastAPI()
    for route in plugin.get_api():
        app.add_api_route(route["path"], route["endpoint"], methods=route["methods"], response_model=route["response_model"])
    client = TestClient(app)
    assert client.get("/tasks").json()[0]["history_id"] == row["history_id"]
    assert client.post(f"/tasks/{row['id']}/upload").json() == {"success": True, "state": "upload_queued"}


def test_qmj_import_form_history_api_and_message_type(h):
    p = load_plugin("qmjsign")
    plugin = p.qmjsign()
    form, defaults = plugin.get_form()
    assert form and defaults
    plugin.save_data("sign_history", [{"date": "2026-10-04"}])
    route = plugin.get_api()[0]
    app = FastAPI()
    app.add_api_route(route["path"], route["endpoint"], methods=route["methods"], response_model=route["response_model"])
    response = TestClient(app).post("/clear_history").json()
    assert response["code"] == 0 and "message" in response and "data" not in response
    assert plugin.get_data("sign_history") == []
    assert p.NotificationType.SiteMessage.value
    plugin.stop_service()


@pytest.mark.parametrize("confirmed", [True, False])
def test_cleanup_only_counts_confirmed_deletion_and_stop(h, tmp_path, confirmed):
    p = load_plugin("autocleanunlinkedseed")
    plugin = p.autocleanunlinkedseed()
    plugin._stop_event = threading.Event()
    plugin._notify = True
    path = tmp_path / "unlinked.mkv"
    path.write_bytes(b"video")
    torrent = NS(progress=1, content_path=str(path), hash="abc", name="Film", size=5, tags="allowed")
    downloader = NS(get_torrents=lambda: ([torrent], None), delete_torrents=Mock(return_value=confirmed))
    plugin.downloader_helper = NS(get_services=lambda: {"qb": NS(name="qb", instance=downloader)},
                                  is_downloader=lambda **kw: kw["service_type"] == "qbittorrent")
    plugin.clean_unlinked_seeds()
    downloader.delete_torrents.assert_called_once_with(delete_file=True, ids="abc")
    assert len(plugin.messages) == int(confirmed)
    plugin.stop_service()
    plugin.clean_unlinked_seeds()
    assert downloader.delete_torrents.call_count == 1
    assert plugin.get_form()[0] and plugin.get_api() == [] and plugin.get_page() == []


def test_sdk_exports_and_api_signatures_exist_in_pinned_host(h):
    # Import ports are mocked above; check every SDK import against real exports.
    for plugin_id in ("p115instantwait", "qmjsign", "autocleanunlinkedseed"):
        tree = ast.parse((ROOT / "plugins.v3" / plugin_id / "__init__.py").read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module.startswith("app.sdk"):
                path = REFERENCE / (node.module.replace(".", "/") + ".py")
                if not path.exists():
                    path = path.with_suffix("") / "__init__.py"
                source = path.read_text(encoding="utf-8")
                for name in node.names:
                    assert re_name(name.name, source), (path, name.name)


@pytest.mark.parametrize("visible", ["exact", "other", "missing"])
def test_real_native_started_step_requires_exact_cloud_evidence(h, task, visible):
    row = stage(h, task)
    data = row["payload"]
    data["hashes"] = h.engine.store.get(row["id"])["payload"]["hashes"]
    if visible == "exact":
        h.api.move_id("123", h.api.get_folder("/library"), "Film.mkv")
    elif visible == "other":
        h.api.files["999"] = h.FileItem(storage="u115", type="file", path="/library/Film.mkv", fileid="999", size=18)
    ns = {**vars(h.execution), "__name__": "v3_native_runner", "time": time}
    selected("app/chain/transfer/execution.py",
             {"_DurableTransferStepRunner", "_TransferManualReviewRequired", "_TransferRetryDeferred", "_TransferRetryExhausted"}, ns)
    runner = ns["_DurableTransferStepRunner"].__new__(ns["_DurableTransferStepRunner"])
    runner._task_id, runner._lease_token = task.admission_task_id, "test-lease"
    runner._checkpoint_fingerprint, runner._ordinal, runner._operation_ids = "frozen-plan", 0, []
    step = NS(state=h.execution.TransferStepState.STARTED, operation_id="stable-step", result=None)
    runner._command = Mock()
    runner._command.prepare.return_value = step
    runner._command.complete.side_effect = lambda **kw: NS(result=kw["result"])
    proxy = h.bridge.PreparedStepRunner(runner, h.api, data)
    execute = Mock(side_effect=AssertionError("uncertain step must not be blindly replayed"))
    observe = lambda: h.execution.TransferOperationObservation(
        state=h.execution.TransferOperationObservationState.UNKNOWN,
        evidence=h.execution.TransferStepResult(payload={"reason": "native cloud uncertainty"}))
    kwargs = dict(phase="transfer", kind="materialize_target", payload={"target_path": "/library/Film.mkv"},
                  execute=execute, observe=observe)
    if visible == "exact":
        result = proxy.run(**kwargs)
        assert result.payload["item"]["fileid"] == "123"
        runner._command.complete.assert_called_once()
    else:
        with pytest.raises(ns["_TransferManualReviewRequired"]):
            proxy.run(**kwargs)
        runner._command.manual_review.assert_called_once()
        runner._command.complete.assert_not_called()
    execute.assert_not_called()


def test_v3_market_versions_and_old_fallbacks():
    metadata = json.loads((ROOT / "package.v3.json").read_text(encoding="utf-8"))
    assert set(metadata) == {"P115InstantWait", "qmjsign", "autocleanunlinkedseed"}
    for plugin_id, item in metadata.items():
        folder = "p115instantwait" if plugin_id == "P115InstantWait" else plugin_id
        tree = ast.parse((ROOT / "plugins.v3" / folder / "__init__.py").read_text(encoding="utf-8"))
        values = [n.value.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                  and any(isinstance(t, ast.Name) and t.id == "plugin_version" for t in n.targets)]
        assert values == [item["version"]] and item["v3"] and not item["v2"]
        assert item["system_version"].startswith(">=3.1.0")
    for path in ("package.json", "package.v2.json"):
        assert all(item["v3"] is False for item in json.loads((ROOT / path).read_text(encoding="utf-8")).values())


def test_final_validation_failure_blocks_native_success(h, task):
    row = stage(h, task)
    info = h.Info(success=True, target_item=h.FileItem(storage="u115", type="file", fileid="999", path="/library/Film.mkv"))
    with pytest.raises(h.remote.PauseTask):
        h.Chain()._TransferChain__default_callback(task, info)
    assert not h.history.get(row["history_id"]).status
    assert h.engine.store.get(row["id"])["state"] == "paused"


def re_name(name, source):
    import re
    return re.search(r"\b" + re.escape(name) + r"\b", source)
