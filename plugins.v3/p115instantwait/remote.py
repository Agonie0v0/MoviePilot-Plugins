"""115 open API adapter; OSS upload is only called by the manual worker."""
import hashlib
import time
from pathlib import Path, PurePosixPath

from .maintenance import TASK_ID, parse_roots


class RetryLater(Exception):
    pass


class PauseTask(Exception):
    pass


class NotInstant(RetryLater):
    def __init__(self, message, upload_data=None):
        super().__init__(message)
        self.upload_data = upload_data


class StoppableReader:
    """Interrupt an OSS request while its streaming body is being read."""
    def __init__(self, stream, stop):
        self.stream, self.stop = stream, stop

    def read(self, size=-1):
        if self.stop.is_set():
            raise RetryLater("插件正在停止，上传进度已保留")
        return self.stream.read(size)


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

    def staging_children(self, root):
        root = parse_roots(str(root))[0]
        info = self.raw_path(root)
        if info is None:
            return []
        if str(info.get("file_category")) != "0" or not info.get("file_id") or int(info["file_id"]) <= 0:
            raise RetryLater("暂存目录信息异常")
        children, offset = [], 0
        while True:
            rows = self.request("GET", "/open/ufile/files", params={
                "cid": int(info["file_id"]), "limit": 1000, "offset": offset,
                "cur": 1, "show_dir": 1, "stdir": 1, "star": 0})
            if not isinstance(rows, list):
                raise RetryLater("暂存目录列表不完整")
            for row in rows:
                if str(row.get("fc")) == "0" and TASK_ID.fullmatch(str(row.get("fn", ""))):
                    children.append(str(PurePosixPath(root) / row["fn"]))
            if len(rows) < 1000:
                return children
            offset += len(rows)

    def remove_empty_staging_dir(self, path):
        path = PurePosixPath(str(path))
        if (not path.is_absolute() or ".." in path.parts or not (
                path.name == ".mp115-staging" or
                (path.parent.name == ".mp115-staging" and TASK_ID.fullmatch(path.name)))):
            raise ValueError("拒绝清理非暂存目录")
        info = self.raw_path(path)
        if info is None:
            return "missing"
        if str(info.get("file_category")) != "0" or not info.get("file_id") or int(info["file_id"]) <= 0:
            raise RetryLater("暂存目录信息异常，保留目录")
        folder_id = int(info["file_id"])
        rows = self.request("GET", "/open/ufile/files", params={
            "cid": folder_id, "limit": 1, "offset": 0,
            "cur": 1, "show_dir": 1, "stdir": 1, "star": 0})
        if not isinstance(rows, list):
            raise RetryLater("暂存目录列表不完整，保留目录")
        if rows:
            return "retained"
        self.request("POST", "/open/ufile/delete", data={"file_ids": folder_id})
        return "deleted"

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
            verified = self.request("POST", "/open/upload/init", data=data)
            if not isinstance(verified, dict):
                raise RetryLater("115 二次校验响应不完整")
            # The second response may omit bucket/object/callback from the first.
            result = {**result, **verified}
        if result.get("status") != 2:
            raise NotInstant("未命中秒传，等待下次重试", result)
        return result.get("file_id")

    def upload(self, path, folder, hashes, init, payload, checkpoint):
        """Manual multipart upload using MP's existing oss2 dependency.

        Persist part receipts, never credentials. Retrying uses the same upload
        ID, including an uncertain completion, instead of creating another file.
        """
        import oss2
        from oss2.models import PartInfo

        def check_source():
            if self.stop.is_set():
                raise RetryLater("插件正在停止，上传进度已保留")
            if fingerprint(path) != hashes["fingerprint"]:
                raise PauseTask("源文件在普通上传期间发生变化，已停止提交")

        check_source()
        saved = payload.get("upload_session")
        details = saved or init or {}
        if not all(details.get(k) for k in ("bucket", "object", "pick_code")):
            raise PauseTask("115 普通上传参数不完整，请检查授权后重试")
        token = self.request("GET", "/open/upload/get_token")
        if not isinstance(token, dict) or not all(token.get(k) for k in
                ("endpoint", "AccessKeyId", "AccessKeySecret", "SecurityToken")):
            raise PauseTask("115 未返回完整的普通上传凭证")
        resumed = self.request("POST", "/open/upload/resume", data={
            "file_size": hashes["fingerprint"]["size"], "target": f"U_1_{folder.fileid}",
            "fileid": hashes["sha1"], "pick_code": details["pick_code"]})
        callback = (resumed or {}).get("callback") or (init or {}).get("callback")
        if not isinstance(callback, dict) or not all(callback.get(k) for k in ("callback", "callback_var")):
            raise PauseTask("115 未返回上传完成回调参数，未发送文件内容")
        # Force TLS even when the credential endpoint is returned as http://.
        endpoint = token["endpoint"]
        if endpoint.startswith("http://"):
            endpoint = "https://" + endpoint[len("http://"):]
        auth = oss2.StsAuth(token["AccessKeyId"], token["AccessKeySecret"], token["SecurityToken"])
        bucket = oss2.Bucket(auth, endpoint, details["bucket"], connect_timeout=30)
        try:
            if not saved:
                upload_id = bucket.init_multipart_upload(details["object"],
                    params={"encoding-type": "url", "sequential": ""}).upload_id
                # Stable part sizes allow a manual retry to resume from receipts.
                part_size = oss2.determine_part_size(hashes["fingerprint"]["size"], preferred_size=10 * 1024 * 1024)
                saved = {"bucket": details["bucket"], "object": details["object"],
                         "pick_code": details["pick_code"], "upload_id": upload_id,
                         "part_size": part_size, "parts": []}
                payload["upload_session"] = saved
                checkpoint()
            size = hashes["fingerprint"]["size"]
            offset = min(len(saved["parts"]) * saved["part_size"], size)
            with open(path, "rb") as stream:
                stream.seek(offset)
                while offset < size:
                    check_source()
                    length = min(saved["part_size"], size - offset)
                    number = len(saved["parts"]) + 1
                    part = bucket.upload_part(saved["object"], saved["upload_id"], number,
                        data=oss2.SizedFileAdapter(StoppableReader(stream, self.stop), length))
                    saved["parts"].append({"number": number, "etag": part.etag})
                    offset += length
                    checkpoint()
            check_source()
            # Persist before committing: after response loss retry this exact ID.
            saved["completing"] = True
            checkpoint()
            result = bucket.complete_multipart_upload(saved["object"], saved["upload_id"],
                [PartInfo(p["number"], p["etag"]) for p in saved["parts"]], headers={
                    "X-oss-callback": oss2.utils.b64encode_as_string(callback["callback"]),
                    "x-oss-callback-var": oss2.utils.b64encode_as_string(callback["callback_var"]),
                    "x-oss-forbid-overwrite": "false"})
            if result.status != 200:
                raise RetryLater("普通上传提交未确认，上传进度已保留")
            response = result.resp.response.json()
            if not isinstance(response, dict) or not response.get("state"):
                raise RetryLater("115 上传完成回调未确认，需核对远端结果")
            payload["upload_confirmed"] = True
            checkpoint()
        except (RetryLater, PauseTask):
            raise
        except Exception:
            # SDK exception text can contain signed URLs or temporary credentials.
            raise RetryLater("普通上传未完成，上传进度已保留；请手动重试") from None


class PreparedStorage:
    """Use a verified staged file inside MP's normal transfer operation.

    Old files move to a recoverable backup directory after staging verification,
    so V3's durable deletion observer can confirm the old path is absent.
    """
    def __init__(self, api, store, job, final_path, staged, hashes):
        self.api, self.store, self.job = api, store, job
        self.final_path = PurePosixPath(final_path)
        self.staged, self.hashes = staged, hashes
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
        # V3 records deletion as a durable step: move the old version now so its
        # observer can prove it is absent, without permanently deleting data.
        if str(item.fileid) == str(self.staged.fileid):
            self.error = PauseTask("拒绝删除本任务已上传的目标文件")
            raise self.error
        payload = self.store.get(self.job["id"])["payload"]
        backups = payload.setdefault("backups", [])
        backup = next((b for b in backups if str(b["fileid"]) == str(item.fileid)), None)
        if not backup:
            backup = {"fileid": str(item.fileid), "path": item.path, "name": item.name}
            backups.append(backup)
            self.store.update(self.job["id"], payload=payload)
        folder = self.api.get_folder(self.final_path.parent / ".mp115-backups" / self.job["id"])
        if not backup.get("moved"):
            self.api.move_id(item.fileid, folder, item.name)
            backup["moved"] = True
            self.store.update(self.job["id"], payload=payload)
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
            # Keep old versions as recoverable backups instead of deleting them.
            if backups:
                backup_dir = self.api.get_folder(self.final_path.parent / ".mp115-backups" / self.job["id"])
                for old in backups:
                    if not old.get("moved"):
                        self.api.move_id(old["fileid"], backup_dir, old["name"])
                        old["moved"] = True
                        self.store.update(self.job["id"], payload=payload)
            self.api.move_id(self.staged.fileid, target_dir, final.name)
            item = self.api.verify(self.staged.fileid, str(final), self.hashes)
            payload["committed"] = True
            self.store.update(self.job["id"], payload=payload)
            return item
        except Exception as exc:
            self.error = exc
            raise
