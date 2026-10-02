"""115 open API adapter: initialization only; never uploads file contents to OSS."""
import hashlib
import time
from pathlib import Path, PurePosixPath


class RetryLater(Exception):
    pass


class PauseTask(Exception):
    pass


class NotInstant(RetryLater):
    pass


def fingerprint(path):
    st = Path(path).stat()
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns,
            "device": st.st_dev, "inode": st.st_ino}


def hash_file(path, stop):
    before = fingerprint(path)
    whole, prefix = hashlib.sha1(), hashlib.sha1()
    remaining = 128 * 1024 * 1024
    with open(path, "rb") as stream:
        while True:
            if stop.is_set():
                raise RetryLater("插件正在停止")
            block = stream.read(1024 * 1024)
            if not block:
                break
            whole.update(block)
            if remaining:
                prefix.update(block[:remaining])
                remaining = max(0, remaining - len(block))
    if fingerprint(path) != before:
        raise PauseTask("源文件在计算哈希时发生变化")
    return {"fingerprint": before, "sha1": whole.hexdigest().upper(),
            "preid": prefix.hexdigest().upper()}


class OpenAPI:
    def __init__(self, backend, item_factory, stop, client=None):
        import httpx
        self.backend, self.item_factory, self.stop = backend, item_factory, stop
        self.client = client or httpx.Client(timeout=30, follow_redirects=False)
        self._last_request = 0

    def close(self):
        self.client.close()

    def request(self, method, endpoint, *, data=None, params=None, missing=False):
        # An independent session avoids changing MP's global storage cooldown state.
        delay = max(0, 1.0 - (time.monotonic() - self._last_request))
        if self.stop.wait(delay):
            raise RetryLater("插件正在停止")
        token = self.backend.access_token
        if not token:
            raise PauseTask("内置 115 授权失效，请重新授权后恢复任务")
        self._last_request = time.monotonic()
        try:
            resp = self.client.request(method, self.backend.base_url + endpoint,
                data=data, params=params, headers={"Authorization": f"Bearer {token}",
                    "User-Agent": "MoviePilot-P115InstantWait/0.1",
                    "Content-Type": "application/x-www-form-urlencoded"})
            if resp.status_code in (401, 403):
                raise PauseTask("115 授权或访问权限异常，请检查授权后恢复")
            if resp.status_code == 429:
                raise RetryLater("115 请求限流，稍后重试")
            if resp.status_code >= 500:
                raise RetryLater("115 服务暂时异常")
            resp.raise_for_status()
            result = resp.json()
        except (RetryLater, PauseTask):
            raise
        except Exception:
            # Never copy response headers, tokens, or request bodies into logs/history.
            raise RetryLater("115 网络请求或响应解析失败") from None
        if not isinstance(result, dict):
            raise RetryLater("115 接口响应格式异常")
        code = result.get("code", 0 if result.get("state") else -1)
        if missing and code in (20004, 430004):
            return None
        if code != 0 or result.get("state") is False:
            raise RetryLater(f"115 接口返回业务错误（代码 {code}）")
        return result.get("data")

    @staticmethod
    def one_info(data):
        # 115 may return get_info.data as either an object or a one-item array.
        if isinstance(data, list):
            if not data:
                return None
            if len(data) != 1:
                raise RetryLater("115 文件查询返回多个结果，无法确认目标")
            data = data[0]
        if data is not None and not isinstance(data, dict):
            raise RetryLater("115 文件信息格式异常")
        return data

    def raw_path(self, path):
        return self.one_info(self.request("POST", "/open/folder/get_info",
            data={"path": str(path).replace("\\", "/")}, missing=True))

    def raw_id(self, file_id):
        info = self.one_info(self.request("GET", "/open/folder/get_info",
            params={"file_id": int(file_id)}, missing=True))
        if info and str(info.get("file_id")) != str(file_id):
            raise PauseTask("115 返回的文件 ID 不一致")
        return info

    def item(self, info, path):
        if not info or not info.get("file_id"):
            return None
        p = PurePosixPath(str(path))
        size = info.get("size_byte")
        if size is None and str(info.get("size", "")).isdigit():
            size = int(info["size"])
        return self.item_factory(storage="u115", fileid=str(info["file_id"]),
            path=p.as_posix(), name=info.get("file_name", p.name), basename=p.stem,
            type="dir" if str(info.get("file_category")) == "0" else "file",
            extension=p.suffix.lstrip("."), size=int(size) if size is not None else None,
            pickcode=info.get("pick_code"), modify_time=info.get("utime"))

    def get_item(self, path):
        return self.item(self.raw_path(path), path)

    get_item_strict = get_item

    def get_folder(self, path):
        p = PurePosixPath(str(path).replace("\\", "/"))
        if not p.is_absolute() or ".." in p.parts:
            raise PauseTask("115 目标必须是绝对路径")
        if p == PurePosixPath("/"):
            return self.item_factory(storage="u115", type="dir", path="/", fileid="0", name="/")
        existing = self.get_item(p)
        if existing:
            if existing.type != "dir":
                raise PauseTask("115 目标目录被同名文件占用")
            return existing
        parent = self.get_folder(p.parent)
        try:
            info = self.request("POST", "/open/folder/add", data={"pid": int(parent.fileid), "file_name": p.name})
        except RetryLater:
            # Folder creation may have committed even if its response was lost.
            result = self.get_item(p)
            if result and result.type == "dir":
                return result
            raise
        if not info or not info.get("file_id"):
            raise RetryLater("115 创建目录后尚未返回目录 ID")
        return self.item_factory(storage="u115", type="dir", path=p.as_posix(),
                                 fileid=str(info["file_id"]), name=p.name)

    def list(self, folder):
        result, offset = [], 0
        while True:
            rows = self.request("GET", "/open/ufile/files", params={"cid": int(folder.fileid),
                "limit": 1000, "offset": offset, "cur": True, "show_dir": 1}) or []
            if not isinstance(rows, list):
                raise RetryLater("115 文件列表格式异常")
            for row in rows:
                p = PurePosixPath(folder.path) / row["fn"]
                result.append(self.item_factory(storage="u115", path=str(p), fileid=str(row["fid"]),
                    name=row["fn"], basename=p.stem, extension=row.get("ico"), size=row.get("fs"),
                    type="dir" if str(row["fc"]) == "0" else "file", pickcode=row.get("pc")))
            if len(rows) < 1000:
                return result
            offset += len(rows)

    def move_id(self, file_id, folder, name):
        # Query first: replaying a committed move is harmless after response loss/restart.
        existing = self.raw_path(PurePosixPath(folder.path) / name)
        if existing and str(existing.get("file_id")) == str(file_id):
            return
        if existing:
            raise PauseTask("目标被另一个文件占用，已保留源文件及暂存文件")
        self.request("POST", "/open/ufile/move", data={"file_ids": int(file_id), "to_cid": int(folder.fileid)})
        self.request("POST", "/open/ufile/update", data={"file_id": int(file_id), "file_name": name})
        visible = self.raw_path(PurePosixPath(folder.path) / name)
        if not visible or str(visible.get("file_id")) != str(file_id):
            raise RetryLater("115 移动后目标信息尚未可见")

    def verify(self, file_id, path, hashes):
        info = self.raw_id(file_id)
        if not info or str(info.get("file_category")) != "1":
            raise RetryLater("秒传结果的远端文件尚未可见")
        item = self.item(info, path)
        if item.size is None:
            raise RetryLater("远端文件尚未返回精确字节大小")
        if item.size != hashes["fingerprint"]["size"]:
            raise PauseTask("远端文件大小校验不一致")
        remote_hash = info.get("sha1") or info.get("file_sha1")
        if remote_hash and str(remote_hash).upper() != hashes["sha1"]:
            raise PauseTask("远端 SHA1 校验不一致")
        return item

    def instant(self, path, folder, name, hashes):
        data = {"file_name": name, "file_size": hashes["fingerprint"]["size"],
                "target": f"U_1_{folder.fileid}", "fileid": hashes["sha1"], "preid": hashes["preid"]}
        result = self.request("POST", "/open/upload/init", data=data)
        if not isinstance(result, dict):
            raise RetryLater("115 上传初始化响应不完整")
        if result.get("code") in (700, 701) and result.get("sign_check"):
            try:
                start, end = map(int, result["sign_check"].split("-"))
            except (ValueError, TypeError):
                raise PauseTask("115 二次校验区间格式异常") from None
            if not 0 <= start <= end < hashes["fingerprint"]["size"]:
                raise PauseTask("115 二次校验区间越界")
            sha = hashlib.sha1()
            remaining = end - start + 1
            with open(path, "rb") as stream:
                stream.seek(start)
                while remaining:
                    if self.stop.is_set():
                        raise RetryLater("插件正在停止")
                    block = stream.read(min(1024 * 1024, remaining))
                    if not block:
                        raise PauseTask("源文件无法满足二次校验")
                    sha.update(block)
                    remaining -= len(block)
            data.update(pick_code=result.get("pick_code"), sign_key=result.get("sign_key"),
                        sign_val=sha.hexdigest().upper())
            result = self.request("POST", "/open/upload/init", data=data)
            if not isinstance(result, dict):
                raise RetryLater("115 二次校验响应不完整")
        if result.get("status") != 2:
            raise NotInstant("未命中秒传，等待下次重试")
        return result.get("file_id")


class PreparedStorage:
    """Use a verified staged file inside MP's normal transfer operation.

    MP's delete-before-upload calls are buffered. Old files move to a recoverable
    backup directory only after the new file has been verified.
    """
    def __init__(self, api, store, job, final_path, staged, hashes):
        self.api, self.store, self.job = api, store, job
        self.final_path = PurePosixPath(final_path)
        self.staged, self.hashes = staged, hashes
        self.deletions = []
        self.error = None

    def __getattr__(self, name):
        value = getattr(self.api, name)
        if not callable(value):
            return value
        def guarded(*args, **kwargs):
            try:
                return value(*args, **kwargs)
            except Exception as exc:
                self.error = exc
                raise
        return guarded

    def get_item(self, path):
        try:
            item = self.api.get_item(path)
        except Exception as exc:
            self.error = exc
            raise
        # Our own already committed upload must not become a same-name conflict.
        if item and str(item.fileid) == str(self.staged.fileid) and PurePosixPath(str(path).replace("\\", "/")) == self.final_path:
            return None
        return item

    get_item_strict = get_item

    def delete(self, item):
        if item.type != "file":
            self.error = PauseTask("拒绝替换整个远端目录")
            raise self.error
        if all(str(old.fileid) != str(item.fileid) for old in self.deletions):
            self.deletions.append(item)
        return True

    def upload(self, target_dir, local_path, new_name=None):
        try:
            final = PurePosixPath(target_dir.path) / (new_name or Path(local_path).name)
            if final != self.final_path:
                raise PauseTask("重试时整理目标发生变化，请人工处理")
            if fingerprint(local_path) != self.hashes["fingerprint"]:
                raise PauseTask("源文件在等待期间发生变化")
            self.api.verify(self.staged.fileid, self.staged.path, self.hashes)
            payload = self.store.get(self.job["id"])["payload"]
            backups = payload.setdefault("backups", [])
            for old in self.deletions:
                if not any(str(b["fileid"]) == str(old.fileid) for b in backups):
                    backups.append({"fileid": str(old.fileid), "path": old.path, "name": old.name})
            self.store.update(self.job["id"], payload=payload)
            # Keep old versions as recoverable backups instead of deleting them.
            if backups:
                backup_dir = self.api.get_folder(self.final_path.parent / ".mp115-backups" / self.job["id"])
                for old in backups:
                    self.api.move_id(old["fileid"], backup_dir, old["name"])
            self.api.move_id(self.staged.fileid, target_dir, final.name)
            item = self.api.verify(self.staged.fileid, str(final), self.hashes)
            payload["committed"] = True
            self.store.update(self.job["id"], payload=payload)
            return item
        except Exception as exc:
            self.error = exc
            raise


class DeferredSource:
    def __init__(self, backend):
        self.backend = backend

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def delete(self, item):
        # Completion/history is durably checkpointed before source cleanup.
        return True
