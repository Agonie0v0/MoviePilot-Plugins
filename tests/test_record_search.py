"""Whole-queue lookup, bounded pages, and selection safety in native forms."""
import ast
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from tests.support import PLUGIN, load

store, ui, records = load("store"), load("ui"), load("records")


class RecordSearchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = store.QueueStore(Path(self.temp.name) / "queue.db")
        with self.db.connect() as db:
            for state in ("completed", "paused"):
                db.executemany("INSERT INTO jobs(id,source,state,next_at,created,updated,history_id,payload) VALUES(?,?,?,0,?,?,?,?)",
                    [(f"{state}-{i}", f"local:/downloads/历史_movie_{i:03}.mkv", state, i, i, 1000+i,
                      json.dumps(dict(task={"fileitem": {"path": f"/downloads/历史_movie_{i:03}.mkv"}}, final_path="/library/movie.mkv")))
                     for i in range(251)])

    def test_old_records_are_searchable_and_can_be_operated_or_cleaned(self):
        for scope, state in (("tasks", "paused"), ("history", "completed")):
            result = self.db.search(scope=scope, query="历史_movie_000")
            self.assertEqual(result["total"], 1)
            self.assertEqual(result["rows"][0]["id"], state + "-0")
            self.assertEqual(self.db.search(scope=scope, query="#1000")["total"], 1)
            self.assertEqual(self.db.search(scope=scope, query=state + "-0")["total"], 1)
        self.assertEqual(self.db.command("paused-0", "resume")["state"], "waiting")
        self.assertEqual(self.db.clear_history(keys=["completed-0"])["deleted"], 1)
        self.assertEqual(self.db.search(query="历史_movie_000")["total"], 0)
        self.assertIsNotNone(self.db.get("paused-0"))

    def test_pagination_is_bounded_and_covers_all_records_once(self):
        seen = []
        for page in range(1, 7):
            with patch.object(self.db, "decode", wraps=self.db.decode) as decode:
                result = self.db.search(page=page)
                self.assertEqual(decode.call_count, 50 if page < 6 else 1)
            self.assertEqual((result["total"], result["pages"], result["page"]), (251, 6, page))
            seen += [r["id"] for r in result["rows"]]
        self.assertEqual(len(set(seen)), 251)
        self.assertEqual(seen[0], "completed-250")
        self.assertEqual(seen[-1], "completed-0")
        self.assertEqual(self.db.search(page=999)["page"], 6)
        for query in ("%", "' OR 1=1 --", "missing", "\\"):
            self.assertEqual(self.db.search(query=query)["total"], 0)
        self.assertEqual(self.db.search(query="_")["total"], 251)
        for kwargs in (dict(scope="all"), dict(state="paused"), dict(page=0), dict(page=True), dict(query="x"*201)):
            with self.assertRaises(ValueError):
                self.db.search(**kwargs)

    def plugin(self):
        tree = ast.parse((PLUGIN / "__init__.py").read_text(encoding="utf-8"))
        defaults = next(ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
                        and any(isinstance(t, ast.Name) and t.id == "DEFAULTS" for t in n.targets))
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef))
        cls.bases = []
        cls.body = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in ("search_records", "get_form", "init_plugin")]
        namespace = dict(HTTPException=HTTPException, task_record=records.task_record, record_choices=ui.record_choices,
                         config_form=ui.config_form, record_search_model=ui.record_search_model, DEFAULTS=defaults,
                         logger=Mock())
        exec(compile(ast.Module(body=[cls], type_ignores=[]), "entry", "exec"), namespace)
        plugin = namespace[cls.name]()
        plugin.queue, plugin.get_data = lambda: self.db, lambda key: None
        plugin._error = ""
        return plugin

    def test_authenticated_api_and_initial_form_use_full_search(self):
        plugin = self.plugin()
        app = FastAPI()
        app.add_api_route("/records", plugin.search_records, methods=["GET"])
        client = TestClient(app)
        result = client.get("/records", params=dict(scope="tasks", query="历史_movie_000")).json()
        self.assertEqual(result["items"][0]["value"], "paused-0")
        self.assertNotIn("payload", str(result))
        for params in (dict(scope="wrong"), dict(state="wrong"), dict(page=0), dict(query="a"*201)):
            self.assertEqual(client.get("/records", params=params).status_code, 400)
        self.assertEqual(client.get("/records", params=dict(page="bad")).status_code, 422)
        form, model = plugin.get_form()
        for scope in ("tasks", "history"):
            self.assertEqual(len(model[f"_records_{scope}_items"]), 50)
            self.assertEqual(model[f"_records_{scope}_total"], 251)
        plugin.stop_service = Mock()
        plugin.update_config = Mock(return_value=True)
        plugin.init_plugin({**model, "enabled": False})
        saved = plugin.update_config.call_args.args[0]
        self.assertFalse(any(k.startswith("_records_") for k in saved))
        self.assertEqual(saved["history_ids"], [])

    @unittest.skipUnless(shutil.which("node"), "Node is needed to execute native form handlers")
    def test_form_search_handlers_clear_selection_and_handle_errors(self):
        data = dict(handler=ui.record_search_handler("history", 1),
                    model=ui.record_search_model("history", dict(items=[], total=251, page=1, pages=6)))
        script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {handler,model} = JSON.parse(fs.readFileSync(0,'utf8'));
const run = new Function('model','with(model){return ('+handler+')()}');
global.window = {MoviePilotAPI:{get:async(path,{params})=>{
  assert.equal(path,'plugin/P115InstantWait/records');
  assert.equal(params.page,2);
  return {items:[{value:'old'}],total:251,page:2,pages:6};
}}};
(async()=>{
  model.history_ids=['previous'];model.cleanup_history=true;
  await run(model);
  assert.deepEqual(model.history_ids,[]);assert.equal(model.cleanup_history,false);
  assert.equal(model._records_history_page,2);assert.equal(model._records_history_busy,false);
  model._records_history_query='new';
  window.MoviePilotAPI.get=async(path,{params})=>{
    assert.equal(params.page,1);assert.equal(params.query,'new');throw new Error('offline');
  };
  model.history_ids=['old'];model.cleanup_history=true;
  await run(model);
  assert.deepEqual(model._records_history_items,[]);assert.deepEqual(model.history_ids,[]);
  assert.equal(model.cleanup_history,false);assert.ok(model._records_history_error);
  assert.equal(model._records_history_busy,false);
})().catch(e=>{console.error(e);process.exit(1)});
"""
        result = subprocess.run([shutil.which("node"), "-e", script], input=json.dumps(data), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
