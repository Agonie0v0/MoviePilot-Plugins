"""Run inside the MoviePilot Python environment, then restart MoviePilot.

Copies plugin files and adds only this plugin to UserInstalledPlugins. It does
not enable the plugin, start uploads, or edit any other plugin configuration.
"""
import argparse
from datetime import datetime
from pathlib import Path
import shutil


def main():
    parser = argparse.ArgumentParser(description="本地安装 115 秒传等待插件")
    parser.add_argument("--source", type=Path, help="p115instantwait 源码文件夹")
    args = parser.parse_args()
    from version import APP_VERSION
    from app.core.config import settings
    from app.db.systemconfig_oper import SystemConfigOper
    from app.schemas.types import SystemConfigKey
    if APP_VERSION != "v2.15.6":
        raise SystemExit(f"仅适配 v2.15.6，当前版本 {APP_VERSION}")
    source = (args.source or Path(__file__).resolve().parent / "p115instantwait").resolve()
    if not (source / "__init__.py").is_file():
        raise SystemExit("未找到插件源码，请通过 --source 指定文件夹")
    plugin_root = (settings.ROOT_PATH / "app" / "plugins").resolve()
    destination = (plugin_root / "p115instantwait").resolve()
    if destination.parent != plugin_root or source == destination:
        raise SystemExit("插件安装目标无效")
    system = SystemConfigOper()
    installed = system.get(SystemConfigKey.UserInstalledPlugins) or []
    if not isinstance(installed, list):
        raise SystemExit("已安装插件配置格式异常，未修改配置")
    if destination.exists():
        backup = plugin_root / ("_p115instantwait_backup_" + datetime.now().strftime("%Y%m%d%H%M%S%f"))
        shutil.copytree(destination, backup)
        print(f"现有插件源码备份：{backup}")
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("__init__.py", "bridge.py", "remote.py", "store.py"):
        shutil.copy2(source / name, destination / name)
    if "P115InstantWait" not in installed:
        system.set(SystemConfigKey.UserInstalledPlugins, [*installed, "P115InstantWait"])
    print(f"已安装到 {destination}，请重启 MP 后在插件配置中启用。")


if __name__ == "__main__":
    main()
