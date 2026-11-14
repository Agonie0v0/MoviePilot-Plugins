"""115 open API adapter; OSS upload is only called by the manual worker."""
import hashlib
import threading
import time
from pathlib import Path, PurePosixPath


_REQUEST_LOCK = threading.Lock()
_COOLDOWN_UNTIL = 0.0
_PATH_CACHE = {}


def cooldown_until():
    return _COOLDOWN_UNTIL


class RetryLater(Exception):
    pass


class RateLimited(RetryLater):
    def __init__(self, message, until):
        super().__init__(message)
        self.until = until


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


def check_source(path, expected, stop, hashes=None):
    try:
        current = fingerprint(path)
    except FileNotFoundError:
        raise PauseTask("源文件不存在，已暂停") from None
    if current == expected:
        return current
    if current["size"] != expected["size"]:
        raise PauseTask("源文件内容已变化，已暂停")
    if hashes:
        refreshed = hash_file(path, stop)
        if refreshed["sha1"] != hashes["sha1"]:
            raise PauseTask("源文件内容已变化，已暂停")
        hashes.update(refreshed)
        current = refreshed["fingerprint"]
    return current


class OpenAPI:
    def __init__(self, backend, item_factory, stop, client=None):
        import httpx
        self.backend, self.item_factory, self.stop = backend, item_factory, stop
        self.client = client or httpx.Client(timeout=30, follow_redirects=False)
        self._path_cache, self._id_cache = {}, {}
        self._last_request = 0

    def close(self):
        self.client.close()

    def request(self, method, endpoint, *, data=None, params=None, missing=False):
        # Every task creates a fresh OpenAPI object. Serialize those clients so
        # a large queue cannot send a burst of requests when the shared 115
        # cooldown expires.
        with _REQUEST_LOCK:
            return self._request_locked(method, endpoint, data=data, params=params, missing=missing)

    def _request_locked(self, method, endpoint, *, data=None, params=None, missing=False):
        import httpx
        if method.upper() == "POST" and endpoint != "/open/folder/get_info":
            _PATH_CACHE.clear()
            self._path_cache.clear()
            self._id_cache.clear()

        def cooldown(reason, start=False):
            global _COOLDOWN_UNTIL
            lock = getattr(self.backend, "_limit_lock", None)
            if lock is None:
                raise PauseTask("115 风控保护不可用：宿主未提供共享冷却锁，请检查 MoviePilot 版本")
            with lock:
                until = getattr(self.backend, "_limit_until", 0)
                if start:
                    until = max(until, time.time() + getattr(self.backend, "limit_sleep_seconds", 3600))
                    self.backend._limit_until = until
            until = max(until, _COOLDOWN_UNTIL)
            if until > time.time():
                _COOLDOWN_UNTIL = max(_COOLDOWN_UNTIL, until)
                raise RateLimited(f"115 风控：{reason}；接口 {endpoint}，冷却至 " +
                                  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(until)), until)
            _COOLDOWN_UNTIL = max(_COOLDOWN_UNTIL, until)

        cooldown("MoviePilot 共享冷却中")
        limiter = getattr(self.backend, "_api_limiter", None)
        if limiter is not None:
            limiter.acquire()
        stats = getattr(self.backend, "_rate_stats", None)
        if stats is not None:
            stats.record()
        cooldown("MoviePilot 共享冷却中")
        token = self.backend.access_token
        if not token:
            raise PauseTask("内置 115 授权失效，请重新授权后恢复任务")
        try:
            resp = self.client.request(method, self.backend.base_url + endpoint,
                data=data, params=params, headers={"Authorization": f"Bearer {token}",
                    "User-Agent": "MoviePilot-P115InstantWait/0.1",
                    "Content-Type": "application/x-www-form-urlencoded"})
        except httpx.TimeoutException:
            raise RetryLater(f"115 请求超时（{endpoint}）") from None
        except httpx.RequestError:
            raise RetryLater(f"115 网络连接失败（{endpoint}）") from None
        if resp.status_code in (401, 403):
            raise PauseTask(f"115 授权或访问权限异常（HTTP {resp.status_code}，{endpoint}），请检查授权")
        if resp.status_code == 429:
            cooldown("请求限流（HTTP 429）", start=True)
        if resp.status_code >= 500:
            raise RetryLater(f"115 服务异常（HTTP {resp.status_code}，{endpoint}）")
        if resp.status_code >= 400:
            raise RetryLater(f"115 请求失败（HTTP {resp.status_code}，{endpoint}）")
        try:
            result = resp.json()
        except ValueError:
            raise RetryLater(f"115 响应不是有效 JSON（{endpoint}）") from None
        if not isinstance(result, dict):
            raise RetryLater(f"115 接口响应格式异常（{endpoint}）")
        code = result.get("code", 0 if result.get("state") else -1)
        if missing and code in (20004, 430004):
            return None
        if code != 0 or result.get("state") is False:
            message = result.get("message")
            if isinstance(message, str) and "已达到当前访问上限" in message:
                cooldown(f"已达到当前访问上限（代码 {code}）", start=True)
            # The remote message can contain credentials or signed URLs. Only
            # expose the known risk-control message above, plus the error code.
            code = code if isinstance(code, int) else "未知"
            raise RetryLater(f"115 接口返回业务错误（{endpoint}，代码 {code}）")
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
        key = str(path).replace("\\", "/")
        cache_key = (id(self.backend), key)
        cached = _PATH_CACHE.get(cache_key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        value = self.one_info(self.request("POST", "/open/folder/get_info",
            data={"path": key}, missing=True))
        if value and str(value.get("file_category")) == "0":
            _PATH_CACHE[cache_key] = (time.monotonic() + 300.0, value)
        self._path_cache[key] = value
        return value

    def raw_id(self, file_id):
        key = str(file_id)
        if key in self._id_cache:
            return self._id_cache[key]
        info = self.one_info(self.request("GET", "/open/folder/get_info",
            params={"file_id": int(file_id)}, missing=True))
        if info and str(info.get("file_id")) != str(file_id):
            raise PauseTask("115 返回的文件 ID 不一致")
        self._id_cache[key] = info
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

    def delete(self, item):
        """Execute an MP-approved file deletion by exact identity, without backups."""
        if item.type != "file" or not item.fileid or int(item.fileid) <= 0:
            raise PauseTask("拒绝自动删除整个远端目录")
        current = self.raw_path(item.path)
        if current is None:
            return True
        if str(current.get("file_id")) != str(item.fileid):
            raise PauseTask("待覆盖旧文件已变化，已停止删除")
        self.request("POST", "/open/ufile/delete", data={"file_ids": int(item.fileid)})
        visible = self.raw_path(item.path)
        if visible and str(visible.get("file_id")) == str(item.fileid):
            raise RetryLater("旧文件删除后仍可见，等待远端确认")
        return True

    def get_folder(self, path):
        p = PurePosixPath(str(path).replace("\\", "/"))
        if not p.is_absolute() or ".." in p.parts:
            raise PauseTask("115 目标必须是绝对路径")
        if any(part in (".mp115-staging", ".mp115-backups") for part in p.parts):
            raise PauseTask("已停用临时目录，不允许将其作为整理目标")
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
        """Persist multipart receipts for retries without storing credentials."""
        import oss2
        from oss2.models import PartInfo

        def check_upload_source():
            if self.stop.is_set():
                raise RetryLater("插件正在停止，上传进度已保留")
            check_source(path, hashes["fingerprint"], self.stop, hashes)

        check_upload_source()
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
        stage = "初始化上传"
        try:
            if not saved:
                upload_id = bucket.init_multipart_upload(details["object"],
                    params={"encoding-type": "url", "sequential": ""}).upload_id
                # Stable part sizes let retries resume from saved receipts.
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
                    stage = "上传分片"
                    length = min(saved["part_size"], size - offset)
                    number = len(saved["parts"]) + 1
                    part = bucket.upload_part(saved["object"], saved["upload_id"], number,
                        data=oss2.SizedFileAdapter(StoppableReader(stream, self.stop), length))
                    saved["parts"].append({"number": number, "etag": part.etag})
                    offset += length
                    checkpoint()
            check_upload_source()
            # Mark the commit first so retries reconcile its result before uploading.
            saved["completing"] = True
            checkpoint()
            stage = "提交上传"
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
            result_data = response.get("data")
            if isinstance(result_data, dict) and str(result_data.get("file_id", "")).isdigit():
                payload["remote_id"] = str(result_data["file_id"])
            checkpoint()
        except (RetryLater, PauseTask):
            raise
        except oss2.exceptions.OssError as error:
            code = error.code
            if code == "NoSuchUpload":
                payload["upload_pick_code"] = details["pick_code"]
                payload.pop("upload_session", None)
                payload["write_intent"] = True
                checkpoint()
                raise RetryLater("上传会话已失效，稍后核对目标并重新上传") from None
            if code == "SecurityTokenExpired":
                if saved:
                    saved.pop("completing", None)
                    checkpoint()
                raise RetryLater("上传凭证已过期，稍后刷新后继续") from None
            # Only expose the SDK error code, never its signed URL or credentials.
            code = code if code and code.isascii() and code.isalnum() else type(error).__name__
            if code in ("AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise PauseTask(f"115 上传授权不可用（{code}），请检查授权") from None
            raise RetryLater(f"{stage}失败（{code}）") from None
        except Exception as error:
            raise RetryLater(f"{stage}失败（{type(error).__name__}），已保留进度") from None


def resolve_target(api, data):
    """Resolve identity once for uploads, native replay and final cleanup."""
    existing = api.raw_path(data["final_path"])
    if not existing:
        return None
    file_id = existing.get("file_id")
    if not file_id:
        raise RetryLater("目标文件信息尚未完整，稍后核对")
    if str(file_id) != str(data.get("remote_id")):
        info = api.raw_id(file_id) or {}
        digest = info.get("sha1") or info.get("file_sha1")
        pick_code = data.get("upload_session", {}).get("pick_code") or data.get("upload_pick_code")
        same_upload = pick_code and str(info.get("pick_code", "")) == str(pick_code)
        if digest and str(digest).upper() != data["hashes"]["sha1"]:
            raise PauseTask("目标文件内容不同，请核对同名文件")
        if not digest and not same_upload:
            raise RetryLater("目标文件信息尚未完整，稍后核对")
    item = api.verify(file_id, data["final_path"], data["hashes"])
    data["remote_id"] = str(file_id)
    data.pop("missing_since", None)
    return item


def prepare_direct(api, store, row, path, data, force, progress=None):
    """Reconcile the final target, then initialize or resume an upload."""
    final = PurePosixPath(data["final_path"])
    if any(part in (".mp115-staging", ".mp115-backups") for part in final.parts):
        raise PauseTask("已停用临时目录，请选择正式整理目标")
    if not store.reserve_target(row["id"], str(final)):
        raise PauseTask("另一个未结束任务占用相同正式目标，请先处理该任务")
    checkpoint = lambda: store.update(row["id"], payload=data)
    item = resolve_target(api, data)
    if item:
        checkpoint()
        return item
    session = data.get("upload_session") or {}
    if (data.get("remote_id") or data.get("instant_confirmed") or data.get("upload_confirmed")
            or session.get("completing") or (data.get("write_intent") and not session)):
        if data.get("remote_id") and api.raw_id(data["remote_id"]):
            raise RetryLater("上传结果已存在，等待目标路径刷新")
        # Allow delayed 115 metadata to appear before starting another upload.
        since = data.setdefault("missing_since", time.time())
        checkpoint()
        if time.time() - since < 60:
            raise RetryLater("目标暂不可见，稍后核对上传结果")
        for key in ("remote_id", "instant_confirmed", "upload_confirmed", "upload_session", "upload_pick_code", "write_intent", "missing_since"):
            data.pop(key, None)
        checkpoint()
        if progress:
            progress("目标仍不存在，重新尝试上传")
    folder = api.get_folder(final.parent)
    if data.get("target_folder_id") and str(data["target_folder_id"]) != str(folder.fileid):
        data.pop("upload_session", None)
        data.pop("upload_pick_code", None)
    data["target_folder_id"] = str(folder.fileid)
    item = resolve_target(api, data)
    if item:
        checkpoint()
        return item
    if data.get("upload_session"):
        if not force:
            raise PauseTask("有未完成的普通上传，请强制上传继续")
        checkpoint()
        api.upload(path, folder, data["hashes"], None, data, checkpoint)
        if progress:
            progress("普通上传已提交")
    else:
        data["write_intent"] = True
        checkpoint()
        try:
            file_id = api.instant(path, folder, final.name, data["hashes"])
        except NotInstant as error:
            # A definite miss creates no cloud file. Persist safe retry state.
            data.pop("write_intent", None)
            checkpoint()
            if not force:
                raise
            if progress:
                progress("秒传未命中，转普通上传")
            # Check again immediately before multipart upload starts.
            item = resolve_target(api, data)
            if item:
                checkpoint()
                return item
            data["write_intent"] = True
            checkpoint()
            api.upload(path, folder, data["hashes"], error.upload_data, data, checkpoint)
            if progress:
                progress("普通上传已提交")
        except RateLimited:
            # A 429 / access-limit response definitively rejected the request.
            data.pop("write_intent", None)
            checkpoint()
            raise
        except PauseTask:
            data.pop("write_intent", None)
            checkpoint()
            raise
        except RetryLater as error:
            raise RetryLater(f"115 秒传初始化未确认（{error}）；目标可能延迟出现，本次不会重复提交") from error
        else:
            data["instant_confirmed"] = True
            if file_id:
                data["remote_id"] = str(file_id)
            checkpoint()
            if progress:
                progress("秒传命中")
    # Reconcile the final path, including callbacks that don't return a file ID.
    return prepare_direct(api, store, row, path, data, force, progress)


class DirectStorage:
    """Let MP finalize its native plan using an already verified final object."""
    def __init__(self, api, store, job, final_path, item, hashes):
        self.api, self.store, self.job = api, store, job
        self.final_path = PurePosixPath(final_path)
        self.item, self.hashes = item, hashes
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
            if item and self.item and str(item.fileid) == str(self.item.fileid) and PurePosixPath(str(path).replace("\\", "/")) == self.final_path:
                return None
            return item
        except Exception as exc:
            self.error = exc
            raise

    get_item_strict = get_item

    def delete(self, item):
        try:
            if self.item and str(item.fileid) == str(self.item.fileid):
                return True
            return self.api.delete(item)
        except Exception as exc:
            self.error = exc
            raise

    def upload(self, target_dir, local_path, new_name=None):
        try:
            final = PurePosixPath(target_dir.path) / (new_name or Path(local_path).name)
            if final != self.final_path:
                raise PauseTask("重试时整理目标发生变化，请人工处理")
            payload = self.store.get(self.job["id"])["payload"]
            check_source(local_path, self.hashes["fingerprint"], self.api.stop, self.hashes)
            item = resolve_target(self.api, payload)
            if not item:
                raise RetryLater("目标暂不可见，稍后继续整理")
            self.item = item
            payload["committed"] = True
            self.store.update(self.job["id"], payload=payload)
            return item
        except Exception as exc:
            self.error = exc
            raise


class NativePlanStorage(DirectStorage):
    """Run MP's actual policy with buffered deletes and a virtual upload result."""
    def __init__(self, api, store, job, item_factory):
        data = job["payload"]
        super().__init__(api, store, job, data["final_path"], None,
                         data.get("hashes") or {"fingerprint": data["source_initial"]})
        self.item_factory = item_factory
        self.deletions = []
        self.upload_planned = False

    def get_item(self, path):
        item = super().get_item(path)
        if item and any(str(old.fileid) == str(item.fileid) for old in self.deletions):
            return None
        return item

    get_item_strict = get_item

    def get_folder(self, path):
        path = PurePosixPath(str(path).replace("\\", "/"))
        return self.item_factory(storage="u115", type="dir", fileid="0", path=str(path), name=path.name)

    def delete(self, item):
        if item.type != "file":
            self.error = PauseTask("不支持自动替换整个远端目录")
            raise self.error
        if all(str(old.fileid) != str(item.fileid) for old in self.deletions):
            self.deletions.append(item)
        return True

    def upload(self, target_dir, local_path, new_name=None):
        final = PurePosixPath(str(target_dir.path).replace("\\", "/")) / (new_name or Path(local_path).name)
        if final != self.final_path:
            self.error = PauseTask("整理路径或源文件发生变化，已停止覆盖")
            raise self.error
        try:
            check_source(local_path, self.hashes["fingerprint"], self.api.stop, self.hashes if "sha1" in self.hashes else None)
        except (PauseTask, RetryLater) as error:
            self.error = error
            raise
        self.upload_planned = True
        return self.item_factory(storage="u115", type="file", fileid="0", path=str(final),
                                 name=final.name, size=self.hashes["fingerprint"]["size"])


def apply_native_deletions(api, store, row, item_factory):
    """Reconcile the persisted MP decision before writing the final filename."""
    data = row["payload"]
    if any(part in (".mp115-staging", ".mp115-backups") for part in PurePosixPath(data["final_path"]).parts):
        raise PauseTask("已停用临时目录，请选择正式整理目标")
    if not store.reserve_target(row["id"], str(PurePosixPath(data["final_path"]))):
        raise PauseTask("另一个未结束任务占用相同正式目标")
    for saved in data.get("native_deletions", []):
        if saved.get("deleted"):
            continue
        item = item_factory(**saved["item"])
        api.delete(item)
        saved["deleted"] = True
        store.update(row["id"], payload=data)


class PlanningStepRunner:
    """Evaluate the native plan without writing durable execution receipts."""
    def run(self, *, phase, kind, payload, execute, observe):
        return execute()


class DeferredSource:
    def __init__(self, backend):
        self.backend = backend

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def delete(self, item):
        # The bridge verifies and checkpoints the remote result before cleanup.
        return True
