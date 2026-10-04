"""Build a local installation zip without modifying a running MoviePilot."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description="构建 115 秒传等待插件安装包")
parser.add_argument("--generation", choices=("v2", "v3"), default="v2")
args = parser.parse_args()
plugin = root / f"plugins.{args.generation}" / "p115instantwait"
metadata = json.loads((root / f"package.{args.generation}.json").read_text(encoding="utf-8"))["P115InstantWait"]
suffix = "-v3" if args.generation == "v3" else ""
output = root / "dist" / f"p115instantwait-{metadata['version']}{suffix}.zip"
output.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(plugin.glob("*.py")):
        archive.write(path, "p115instantwait/" + path.name)
    for name in ("README.md", "LICENSE"):
        archive.write(plugin / name, name)
    archive.writestr(f"package.{args.generation}.json", json.dumps({"P115InstantWait": metadata}, ensure_ascii=False, indent=2))
    # The local installer imports V2-only database/config paths. V3 installs
    # through its plugin market and must never receive that installer.
    if args.generation == "v2":
        archive.write(root / "scripts" / "install_local.py", "install_local.py")
    for name in ("p115instantwait.png", "p115instantwait.svg"):
        archive.write(root / "icons" / name, "icons/" + name)
with zipfile.ZipFile(output) as archive:
    assert archive.testzip() is None
    assert "p115instantwait/__init__.py" in archive.namelist()
print(output)
print("SHA256:", hashlib.sha256(output.read_bytes()).hexdigest())
