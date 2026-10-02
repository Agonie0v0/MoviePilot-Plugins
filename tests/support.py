import importlib.util
import sys
import types
from pathlib import Path


PLUGIN = Path(__file__).resolve().parents[1] / "plugins.v2" / "p115instantwait"
package = types.ModuleType("instantwait_test")
package.__path__ = [str(PLUGIN)]
sys.modules[package.__name__] = package


def load(name):
    full = f"instantwait_test.{name}"
    if full in sys.modules:
        return sys.modules[full]
    spec = importlib.util.spec_from_file_location(full, PLUGIN / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    spec.loader.exec_module(module)
    return module


store = load("store")
remote = load("remote")
