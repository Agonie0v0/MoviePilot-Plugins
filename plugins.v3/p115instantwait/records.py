"""Read-only task records derived from durable transfer checkpoints."""


def transfer_method(payload):
    # A lost multipart completion response can still be reconciled remotely.
    # The user's upload request alone is not evidence of an actual upload.
    if payload.get("upload_confirmed") or (payload.get("upload_session") or {}).get("completing"):
        return "upload"
    if payload.get("instant_confirmed"):
        return "instant"
    return "unknown"


def latest_result(task):
    state = task["state"]
    message = (task.get("message") or "").strip()
    if state == "completed":
        return {
            "upload": "强制上传成功 · 文件传输与 MP 整理完成",
            "instant": "触发秒传成功 · 文件传输与 MP 整理完成",
        }.get(task.get("transfer_method"), "整理成功 · 传输方式未记录")
    if state == "failed":
        return "失败原因：" + (message if message and message != "整理失败" else "未记录具体原因，请查看 MP 日志")
    if state == "paused":
        return "暂停原因：" + (message or "未记录具体原因，请查看 MP 日志")
    return message or {
        "queued": "等待首次执行", "waiting": "等待重试", "running": "本轮正在执行",
        "upload_queued": "排队等待强制上传", "uploading": "强制上传中",
        "finalizing": "远端文件已确认，等待 V3 完成原整理记录", "cancelled": "已取消",
    }.get(state, state)


def extract_size(payload):
    """Extract source file byte size without touching local disk."""
    candidates = (
        (payload.get("hashes") or {}).get("fingerprint") or {},
        payload.get("source_initial") or {},
        ((payload.get("task") or {}).get("fileitem") or {}),
    )
    for source in candidates:
        if isinstance(source, dict):
            value = source.get("size")
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                return value
    return None


def extract_upload_progress(payload, state, size):
    """Derive durable multipart upload progress from checkpointed session state."""
    if state in ("completed", "cancelled", "failed"):
        return None
    session = payload.get("upload_session")
    if not isinstance(session, dict):
        return None
    completing = bool(session.get("completing"))
    raw_part_size = session.get("part_size")
    part_size = int(raw_part_size) if isinstance(raw_part_size, (int, float)) and not isinstance(raw_part_size, bool) and raw_part_size > 0 else 0
    parts = session.get("parts")
    done_parts = len(parts) if isinstance(parts, list) else 0
    if size and size > 0 and part_size > 0:
        total_parts = max(1, (size + part_size - 1) // part_size)
        uploaded_bytes = size if completing else min(size, done_parts * part_size)
        percent = 99 if completing else min(99, int(round(uploaded_bytes * 100 / size)))
        return {
            "done_parts": min(done_parts, total_parts),
            "total_parts": total_parts,
            "uploaded_bytes": uploaded_bytes,
            "total_bytes": size,
            "percent": percent,
            "completing": completing,
        }
    if completing:
        return {
            "done_parts": done_parts,
            "total_parts": max(1, done_parts),
            "uploaded_bytes": size or 0,
            "total_bytes": size or 0,
            "percent": 99,
            "completing": True,
        }
    if done_parts > 0:
        return {
            "done_parts": done_parts,
            "total_parts": 0,
            "uploaded_bytes": 0,
            "total_bytes": size or 0,
            "percent": 0,
            "completing": False,
        }
    return None


def task_record(row):
    payload = row["payload"]
    size = extract_size(payload)
    task = {
        "id": row["id"], "history_id": row["history_id"],
        "source": payload["task"]["fileitem"]["path"], "target": payload["final_path"],
        "state": row["state"], "attempts": row["attempts"], "next_at": row["next_at"],
        "created": row.get("created"), "updated": row.get("updated"),
        "auto_attempts": payload.get("auto_attempts", row["attempts"]),
        "message": row["message"], "backup_files": payload.get("backups", []),
        "transfer_method": transfer_method(payload),
        "size": size,
        "upload_progress": extract_upload_progress(payload, row["state"], size),
    }
    task["result_message"] = latest_result(task)
    return task
