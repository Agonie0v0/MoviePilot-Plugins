"""Bounded, explicit maintenance of plugin records and empty staging folders."""
import re
import threading
from datetime import datetime
from pathlib import PurePosixPath


TASK_ID = re.compile(r"[0-9a-f]{24}")


def staging_root(final_path):
    path = PurePosixPath(str(final_path).replace("\\", "/"))
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("整理目标必须是绝对路径")
    return str(path.parent / ".mp115-staging")


def parse_roots(text):
    """Extra locations must name the staging root itself, never a library tree."""
    if not isinstance(text, str):
        raise ValueError("额外暂存目录格式不正确")
    roots = []
    for line in text.splitlines():
        if not line.strip():
            continue
        path = PurePosixPath(line.strip())
        if not path.is_absolute() or ".." in path.parts or "\\" in line or path.name != ".mp115-staging":
            raise ValueError("每行填写一个以 /.mp115-staging 结尾的 115 绝对路径")
        roots.append(str(path))
    if len(roots) > 100:
        raise ValueError("每次最多补充 100 个暂存目录")
    return list(dict.fromkeys(roots))


class StagingMaintenance:
    def __init__(self, store, api_factory, stop, logger):
        self.store, self.api_factory, self.stop, self.logger = store, api_factory, stop, logger
        # Both staging workers take this lock for mkdir. File transfers remain
        # concurrent; active queue rows protect their directories throughout.
        self.lock = threading.RLock()
        self.worker = None

    def cleanup_completed(self, row, api=None):
        own_api = api is None
        try:
            if self.stop.is_set():
                return
            if own_api:
                api = self.api_factory()
            root = staging_root(row["payload"]["final_path"])
            with self.lock:
                if api.remove_empty_staging_dir(PurePosixPath(root) / row["id"]) == "retained":
                    return
                if not self.store.staging_in_use(root):
                    api.remove_empty_staging_dir(root)
        except Exception:
            self.logger.warning("【115秒传等待】空暂存目录清理未完成，不影响整理成功")
        finally:
            if own_api and api:
                try:
                    api.close()
                except Exception:
                    self.logger.warning("【115秒传等待】清理连接关闭失败，不影响整理成功")

    def start(self, roots, report):
        if self.worker and self.worker.is_alive():
            raise ValueError("历史暂存目录清理正在执行")
        self.worker = threading.Thread(target=self.scan, args=(roots, report),
                                       name="p115-staging-cleanup", daemon=True)
        self.worker.start()

    def scan(self, roots, report):
        result = dict(kind="staging", status="running", deleted=0, retained=0, missing=0,
                      failed=0, checked=0, at=datetime.now().astimezone().isoformat(timespec="seconds"), items=[])
        api = None
        try:
            report(dict(result))
            roots = self.store.staging_roots(roots)
            api = self.api_factory()
            for root in roots:
                if self.stop.is_set():
                    break
                try:
                    # Snapshot all pages before deleting any child: deletion must
                    # not shift pagination offsets and silently skip directories.
                    children = api.staging_children(root)
                    for child in children:
                        if self.stop.is_set():
                            break
                        with self.lock:
                            if self.store.staging_in_use(root, PurePosixPath(child).name):
                                result["retained"] += 1
                            else:
                                result[api.remove_empty_staging_dir(child)] += 1
                            result["checked"] += 1
                    if self.stop.is_set():
                        break
                    with self.lock:
                        if self.store.staging_in_use(root):
                            result["retained"] += 1
                        else:
                            result[api.remove_empty_staging_dir(root)] += 1
                        result["checked"] += 1
                except Exception:
                    if self.stop.is_set():
                        break
                    result["failed"] += 1
                    # Keep the saved report bounded even for large libraries.
                    if len(result["items"]) < 50:
                        result["items"].append(f"{root}：清理未完成，保留未确认目录；检查授权或稍后重试")
                report(dict(result))
            result["status"] = "stopped" if self.stop.is_set() else "completed"
        except Exception:
            result["status"] = "failed"
            result["failed"] += 1
            self.logger.warning("【115秒传等待】历史暂存目录清理中断，未确认目录保留")
        finally:
            if api:
                try:
                    api.close()
                except Exception:
                    result["failed"] += 1
            try:
                report(result)
            except Exception:
                self.logger.warning("【115秒传等待】暂存目录清理结果保存失败，请查看日志")
