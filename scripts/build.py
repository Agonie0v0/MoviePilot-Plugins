"""Build a local installation zip without modifying a running MoviePilot."""
import hashlib
import json
from pathlib import Path
import zipfile


root = Path(__file__).resolve().parents[1]
plugin = root / "plugins.v2" / "p115instantwait"
metadata = json.loads((root / "package.v2.json").read_text(encoding="utf-8"))["P115InstantWait"]
output = root / "dist" / f"p115instantwait-{metadata['version']}.zip"
output.parent.mkdir(exist_ok=True)
with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
    for path in sorted(plugin.glob("*.py")):
        archive.write(path, "p115instantwait/" + path.name)
    for name in ("README.md", "LICENSE"):
        archive.write(plugin / name, name)
    archive.writestr("package.v2.json", json.dumps({"P115InstantWait": metadata}, ensure_ascii=False, indent=2))
    archive.write(root / "scripts" / "install_local.py", "install_local.py")
    for name in ("p115instantwait.png", "p115instantwait.svg"):
        archive.write(root / "icons" / name, "icons/" + name)
with zipfile.ZipFile(output) as archive:
    assert archive.testzip() is None
    assert "p115instantwait/__init__.py" in archive.namelist()
print(output)
print("SHA256:", hashlib.sha256(output.read_bytes()).hexdigest())
