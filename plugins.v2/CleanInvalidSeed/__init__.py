import os
import time
import threading
from pathlib import Path
from typing import Any, List, Dict, Tuple, Optional

from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.plugins import _PluginBase
from app.log import logger
from app.schemas import NotificationType, ServiceInfo
from app.schemas.types import EventType
from app.core.event import eventmanager, Event
from app.helper.downloader import DownloaderHelper
from app.utils.string import StringUtils

class CleanUnlinkedSeed(_PluginBase):
    # 插件名称
    plugin_name = "Emby入库删种"
    # 插件描述
    plugin_desc = "监听系统的 Webhook 事件并在后台定时扫描，自动清理被删除了硬链接的源文件和下载任务。支持按标签精细过滤。"
    # 插件图标
    plugin_icon = "clean_a.png"
    # 插件版本
    plugin_version = "1.2"
    # 插件作者
    plugin_author = "Agonie"
    # 作者主页
    author_url = "https://github.com/Agonie0v0"
    # 插件配置项ID前缀
    plugin_config_prefix = "cleanunlinkedseed"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 1

    # 日志标签
    LOG_TAG = "[CleanUnlinkedSeed]"

    # 状态变量
    _enabled = False
    _notify = False
    _onlyonce = False
    _cron = "0 0 * * *"
    _download_dirs = ""
    _exclude_labels: List[str] = []
    _include_labels: List[str] = []

    # 并发与防抖控制
    _lock = threading.Lock()
    _last_run_time = 0.0
    _cooldown_seconds = 10  # 冷却时间：10秒

    def init_plugin(self, config: dict = None):
        self.downloader_helper = DownloaderHelper()

        if config:
            self._enabled = config.get("enabled", False)
            self._notify = config.get("notify", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = config.get("cron", "0 0 * * *")
            self._download_dirs = config.get("download_dirs", "")
            
            # 解析标签配置，按换行符分割并去除空白
            exclude_str = config.get("exclude_labels", "")
            include_str = config.get("include_labels", "")
            self._exclude_labels = [t.strip() for t in exclude_str.split("\n") if t.strip()]
            self._include_labels = [t.strip() for t in include_str.split("\n") if t.strip()]

        if self._enabled:
            logger.info(f"{self.LOG_TAG} 服务已启动，正在监听事件并已注册定时任务。")

        # 立即运行一次逻辑
        if self._onlyonce:
            logger.info(f"{self.LOG_TAG} 收到立即运行指令，将在后台启动一次断链扫描...")
            # 使用独立线程运行，防止阻塞主程序的配置保存
            threading.Thread(target=self.clean_unlinked_seeds).start()
            
            # 运行后立刻将开关复位为 False 并更新配置
            self._onlyonce = False
            self.update_config({
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": False,
                "cron": self._cron,
                "exclude_labels": "\n".join(self._exclude_labels),
                "include_labels": "\n".join(self._include_labels),
                "download_dirs": self._download_dirs,
            })

    def get_state(self) -> bool:
        return self._enabled

    @property
    def service_info(self) -> Optional[Dict[str, ServiceInfo]]:
        services = self.downloader_helper.get_services()
        if not services:
            logger.warning(f"{self.LOG_TAG} 获取下载器实例失败，请检查配置")
            return None
        return services

    def get_downloader_type(self, service_info) -> str:
        if self.downloader_helper.is_downloader(service_type="qbittorrent", service=service_info):
            return "qbittorrent"
        elif self.downloader_helper.is_downloader(service_type="transmission", service=service_info):
            return "transmission"
        return "unknown"

    # ================= 监听系统 Webhook 事件 =================
    
    @eventmanager.register(EventType.WebhookMessage)
    def handle_system_webhook(self, event: Event):
        """
        监听 MoviePilot 系统收到的 Webhook 事件
        只要 Emby 向 MP 发送了通知，这里就会被触发
        """
        if not self._enabled:
            return
            
        current_time = time.time()
        # 1. 防抖拦截：如果距离上次触发不足10秒，直接忽略，避免 Emby 瞬间轰炸
        if current_time - self.__class__._last_run_time < self._cooldown_seconds:
            return
            
        # 记录本次触发时间
        self.__class__._last_run_time = current_time
        logger.info(f"{self.LOG_TAG} 捕获到系统 Webhook 事件，开始执行断链扫描...")
        # MoviePilot 的 eventmanager 默认在线程池中异步运行，所以直接调用即可，不会卡死主程序
        self.clean_unlinked_seeds()

    # ================= 远程命令 =================
    
    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/clean_unlinked_seeds",
                "event": EventType.PluginAction,
                "desc": "立即清理断链做种",
                "category": "下载器",
                "data": {"action": "clean_unlinked_seeds"},
            }
        ]

    @eventmanager.register(EventType.PluginAction)
    def handle_commands(self, event: Event):
        if event and event.event_data and event.event_data.get("action") == "clean_unlinked_seeds":
            self.post_message(
                channel=event.event_data.get("channel"),
                title="🚀 开始手动执行清理断链做种...",
                userid=event.event_data.get("user"),
            )
            self.clean_unlinked_seeds()
            self.post_message(
                channel=event.event_data.get("channel"),
                title="✅ 断链清理命令执行完成！",
                userid=event.event_data.get("user"),
            )

    def get_api(self) -> List[Dict[str, Any]]:
        pass
        
    # ================= 注册后台定时任务 =================
    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务，返回给 MP 系统供前端展示和定时调度
        """
        if self._enabled and self._cron:
            return [
                {
                    "id": "CleanUnlinkedSeed",
                    "name": "Emby入库删种",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.clean_unlinked_seeds,
                    "kwargs": {},
                }
            ]
        return []

    def stop_service(self):
        pass

    # ================= 核心处理逻辑 =================

    def get_mp_path(self, downloader_path: str) -> str:
        """根据目录映射配置，将下载器路径转换为 MoviePilot 容器内部路径"""
        if not self._download_dirs:
            return downloader_path
        
        for path_map in self._download_dirs.split("\n"):
            if ":" not in path_map:
                continue
            parts = path_map.split(":")
            mp_path = parts[0].strip()
            dl_paths = [p.strip() for p in ":".join(parts[1:]).split(",") if p.strip()]
            
            for dl_path in dl_paths:
                if downloader_path.startswith(dl_path):
                    return downloader_path.replace(dl_path, mp_path, 1)
        return downloader_path

    def is_torrent_completed(self, torrent, downloader_type) -> bool:
        """检查种子是否已下载完成"""
        if downloader_type == "qbittorrent":
            return getattr(torrent, 'progress', 0.0) >= 1.0
        elif downloader_type == "transmission":
            return getattr(torrent, 'percentDone', 0.0) >= 1.0
        return False

    def get_torrent_hash(self, torrent, downloader_type) -> str:
        if downloader_type == "qbittorrent":
            if hasattr(torrent, 'get') and callable(torrent.get):
                return torrent.get("hash")
            return getattr(torrent, 'hash', None)
        elif downloader_type == "transmission":
            return getattr(torrent, 'hashString', None)
        return None

    def get_content_path(self, torrent, downloader_type) -> Optional[str]:
        if downloader_type == "qbittorrent":
            return getattr(torrent, 'content_path', None)
        elif downloader_type == "transmission":
            download_dir = getattr(torrent, 'downloadDir', getattr(torrent, 'download_dir', None))
            name = getattr(torrent, 'name', None)
            if download_dir and name:
                return os.path.join(download_dir, name)
        return None

    def get_torrent_tags(self, torrent, downloader_type) -> List[str]:
        """统一获取种子的标签列表"""
        tags_str = ""
        if downloader_type == "qbittorrent":
            tags_str = getattr(torrent, 'tags', '')
        elif downloader_type == "transmission":
            labels = getattr(torrent, 'labels', [])
            if labels:
                tags_str = ','.join([str(label) for label in labels])
        
        if not tags_str:
            return []
        return [t.strip() for t in tags_str.split(',') if t.strip()]

    def check_is_unlinked(self, path_str: str) -> bool:
        """检查源文件是否断链（所有视频文件 st_nlink == 1）"""
        path = Path(path_str)
        if not path.exists():
            return False

        video_exts = {".mp4", ".mkv", ".avi", ".ts", ".wmv", ".iso", ".rmvb", ".m2ts"}
        files_to_check = []

        if path.is_file():
            if path.suffix.lower() in video_exts:
                files_to_check.append(path)
        else:
            for f in path.rglob("*"):
                if f.is_file() and f.suffix.lower() in video_exts:
                    files_to_check.append(f)

        if not files_to_check:
            if path.is_file():
                files_to_check.append(path)
            else:
                files_to_check = [f for f in path.rglob("*") if f.is_file()]
                
        if not files_to_check:
            return False 

        for f in files_to_check:
            try:
                if f.stat().st_nlink > 1:
                    return False
            except Exception as e:
                logger.error(f"{self.LOG_TAG} 读取文件属性出错 {f}: {e}")
                return False

        return True

    def clean_unlinked_seeds(self):
        # 2. 并发锁拦截：尝试获取非阻塞锁，如果获取失败说明正有其他线程在扫描，直接安全退出
        if not self.__class__._lock.acquire(blocking=False):
            return

        try:
            services = self.service_info
            if not services:
                return

            total_deleted = 0
            deleted_msgs = []

            for service in services.values():
                downloader_name = service.name
                downloader_obj = service.instance
                downloader_type = self.get_downloader_type(service)
                
                if downloader_type == "unknown":
                    continue

                try:
                    torrents, error = downloader_obj.get_torrents()
                    if error or not torrents:
                        continue

                    for torrent in torrents:
                        # 1. 检查是否下载完成
                        if not self.is_torrent_completed(torrent, downloader_type):
                            continue

                        # 2. 标签过滤逻辑
                        t_tags = self.get_torrent_tags(torrent, downloader_type)
                        
                        # 规则A：不处理的优先级最高
                        if self._exclude_labels:
                            if any(tag in self._exclude_labels for tag in t_tags):
                                continue
                                
                        # 规则B：仅处理对应标签（如果设定了）
                        if self._include_labels:
                            if not any(tag in self._include_labels for tag in t_tags):
                                continue

                        # 3. 路径转换与验证
                        dl_content_path = self.get_content_path(torrent, downloader_type)
                        if not dl_content_path:
                            continue
                        mp_content_path = self.get_mp_path(dl_content_path)

                        # 4. 断链检测
                        if self.check_is_unlinked(mp_content_path):
                            t_hash = self.get_torrent_hash(torrent, downloader_type)
                            t_name = getattr(torrent, 'name', 'Unknown')
                            t_size = getattr(torrent, 'size', getattr(torrent, 'total_size', 0))
                            
                            logger.info(f"{self.LOG_TAG} 检测到断链文件，准备删除: {t_name}")
                            
                            # 5. 触发删除
                            if t_hash:
                                downloader_obj.delete_torrents(delete_file=True, ids=t_hash)
                                total_deleted += 1
                                deleted_msgs.append(f"📁 {t_name} (释放: {StringUtils.str_filesize(t_size)})")

                except Exception as e:
                    logger.error(f"{self.LOG_TAG} 处理下载器 {downloader_name} 失败: {e}")

            if total_deleted > 0:
                logger.info(f"{self.LOG_TAG} 断链检测任务结束，共清理 {total_deleted} 个任务")
                
                # 发送通知
                if self._notify:
                    msg = f"🔍 成功清理 {total_deleted} 个已经成功入库的做种任务：\n\n"
                    msg += "\n".join(deleted_msgs)
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title="🧹 【Emby入库删种】执行报告",
                        text=msg,
                    )
        finally:
            # 无论扫描过程中是否发生异常，最后必须释放锁，以免死锁导致后续永远无法触发
            self.__class__._lock.release()

    # ================= UI 前端配置表单 =================
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "title": "🧹 Emby入库删种",
                                            "text": "本插件支持【Webhook事件触发】与【定时周期扫描】双重保障。开启“立即运行一次”并保存，可以立刻手动执行全盘扫描。"
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "color": "primary"
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                            "color": "warning"
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "清理成功发通知",
                                            "color": "success"
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "cron",
                                            "label": "兜底定时周期",
                                            "placeholder": "0 0 * * *",
                                            "prepend-inner-icon": "mdi-clock-outline"
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "exclude_labels",
                                            "label": "🚫 不处理的标签 (最高优先级)",
                                            "rows": 3,
                                            "placeholder": "若种子包含以下标签，将忽略处理。\n多个标签请回车换行",
                                            "prepend-inner-icon": "mdi-tag-off"
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "include_labels",
                                            "label": "🎯 仅处理对应标签",
                                            "rows": 3,
                                            "placeholder": "若填写此项，仅处理包含以下标签的种子。\n留空则默认处理所有标签。\n多个标签请回车换行",
                                            "prepend-inner-icon": "mdi-tag-check"
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "download_dirs",
                                            "label": "📂 下载目录映射",
                                            "rows": 3,
                                            "placeholder": "MP容器内路径:下载器内路径\n例如：/downloads:/data/downloads\n不填写则默认路径相同不转换",
                                            "prepend-inner-icon": "mdi-folder-multiple"
                                        },
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "cron": "0 0 * * *",
            "exclude_labels": "",
            "include_labels": "",
            "download_dirs": "",
        }

    def get_page(self) -> List[dict]:
        pass
