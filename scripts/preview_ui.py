"""Preview the actual V2 UI schemas with isolated fixtures, never an MP account.

Run: python scripts/preview_ui.py
The first run downloads pinned Vue/Vuetify/MDI preview assets into dist/ only.
The local page can exercise refresh and task actions against in-memory fixtures.
"""
import argparse
import ast
import copy
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import importlib
import json
from pathlib import Path
import sys
import time
import types
from urllib.request import urlretrieve


ROOT = Path(__file__).resolve().parents[1]
PACKAGE = types.ModuleType("p115_preview")
PACKAGE.__path__ = [str(ROOT / "plugins.v2" / "p115instantwait")]
sys.modules[PACKAGE.__name__] = PACKAGE
ui = importlib.import_module("p115_preview.ui")
tree = ast.parse((Path(PACKAGE.__path__[0]) / "__init__.py").read_text(encoding="utf-8"))
defaults = next(ast.literal_eval(n.value) for n in tree.body
                if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "DEFAULTS" for t in n.targets))


def fixtures():
    now = time.time()
    cases = [
        ("paused", "星际穿越.Interstellar.2014.2160p.BluRay.mkv", "已达到自动尝试上限，请选择继续等待或强制上传", 4),
        ("running", "流浪地球2.2023.2160p.WEB-DL.mkv", "正在计算哈希并确认秒传", 1),
        ("waiting", "Dune.Part.Two.2024.2160p.BluRay.HEVC.TrueHD.Atmos.mkv", "本轮未命中秒传，等待下次自动尝试", 2),
        ("upload_queued", "三体.S01E12.2023.1080p.WEB-DL.mp4", "排队等待强制上传", 4),
        ("completed", "千与千寻.2001.1080p.BluRay.mkv", "文件传输与 MP 整理完成", 2),
        ("completed", "天空之城.1986.1080p.BluRay.mkv", "文件传输与 MP 整理完成", 4),
        ("cancelled", "测试视频.mp4", "已取消", 1),
    ]
    return [dict(id=f"preview-{i}", history_id=1801+i, source="/downloads/movies/" + name,
                 target="/115/电影/" + name, state=state, attempts=attempts, auto_attempts=attempts,
                 next_at=now + 180, created=now - 3600, updated=now - 60, message=message,
                 transfer_method="upload" if i == 5 else "instant" if state == "completed" else "unknown")
            for i, (state, name, message, attempts) in enumerate(cases)]


TASKS = fixtures()


class Handler(SimpleHTTPRequestHandler):
    def response(self, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/schema"):
            scenario = self.path.split("scenario=", 1)[-1] if "scenario=" in self.path else "normal"
            tasks = [] if scenario == "empty" else copy.deepcopy(TASKS)
            enabled = scenario != "disabled"
            error = "115 授权已失效，请在 MP 存储设置中重新授权后继续等待。" if scenario == "error" else ""
            if scenario == "long":
                tasks[0]["source"] = "/downloads/" + "VeryLongMovieName_" * 30 + "电影.mkv"
                tasks[0]["message"] = "目标路径冲突：" + "/115/电影/" * 30 + "，请核对目标文件。"
            self.response(dict(form=ui.config_form(tasks, error), page=ui.task_page(tasks, enabled, error, 3),
                               defaults={**defaults, "enabled": enabled}))
        elif self.path == "/plugin/P115InstantWait/tasks":
            self.response(TASKS)
        else:
            super().do_GET()

    def do_POST(self):
        parts = self.path.strip("/").split("/")
        if len(parts) == 5 and parts[:3] == ["plugin", "P115InstantWait", "tasks"]:
            task = next((t for t in TASKS if t["id"] == parts[3]), None)
            states = {"resume": "waiting", "pause": "paused", "upload": "upload_queued", "cancel": "cancelled"}
            if task and parts[4] in states:
                task.update(state=states[parts[4]], message="预览操作已提交", updated=time.time())
                self.response(dict(success=True, state=task["state"]))
                return
        self.send_error(404)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    output = ROOT / "dist" / "ui-preview"
    output.mkdir(parents=True, exist_ok=True)
    assets = {
        "vue.js": "https://cdn.jsdelivr.net/npm/vue@3.5.13/dist/vue.global.prod.js",
        "vuetify.js": "https://cdn.jsdelivr.net/npm/vuetify@3.7.6/dist/vuetify.min.js",
        "vuetify.css": "https://cdn.jsdelivr.net/npm/vuetify@3.7.6/dist/vuetify.min.css",
        "mdi.css": "https://cdn.jsdelivr.net/npm/@mdi/font@7.4.47/css/materialdesignicons.min.css",
        "materialdesignicons-webfont.woff2": "https://cdn.jsdelivr.net/npm/@mdi/font@7.4.47/fonts/materialdesignicons-webfont.woff2",
    }
    for name, url in assets.items():
        path = output / name
        if not path.exists():
            print("Downloading preview asset:", name, flush=True)
            urlretrieve(url, path)
    mdi = output / "mdi.css"
    mdi.write_text(mdi.read_text(encoding="utf-8").replace("../fonts/", ""), encoding="utf-8")
    (output / "index.html").write_text((ROOT / "scripts" / "ui-preview.html").read_text(encoding="utf-8"), encoding="utf-8")
    print(f"Fixture-only UI preview: http://127.0.0.1:{args.port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", args.port), partial(Handler, directory=str(output))).serve_forever()


if __name__ == "__main__":
    main()
