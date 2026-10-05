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


def task_record(row):
    payload = row["payload"]
    task = {
        "id": row["id"], "history_id": row["history_id"],
        "source": payload["task"]["fileitem"]["path"], "target": payload["final_path"],
        "state": row["state"], "attempts": row["attempts"], "next_at": row["next_at"],
        "created": row.get("created"), "updated": row.get("updated"),
        "auto_attempts": payload.get("auto_attempts", row["attempts"]),
        "message": row["message"], "backup_files": payload.get("backups", []),
        "transfer_method": transfer_method(payload),
    }
    task["result_message"] = latest_result(task)
    return task
