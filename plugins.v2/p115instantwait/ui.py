"""Native MoviePilot/Vuetify views, with no upload or queue mutations."""
from datetime import datetime
from pathlib import PurePosixPath
import time

from .records import latest_result


LABELS = {"queued": "等待首次秒传", "waiting": "等待重试", "running": "正在执行",
          "paused": "已暂停 · 待处理", "completed": "整理成功", "failed": "整理失败", "cancelled": "已取消",
          "upload_queued": "强制上传排队", "uploading": "强制上传中"}
ACTIVE = {"queued", "waiting", "running", "paused", "upload_queued", "uploading"}
ACTION_HINT = "强制上传：先试秒传，未命中就上传文件。继续等待：只试秒传，重置次数和时限。取消：停止任务，保留文件。"


def node(component, text=None, content=None, **props):
    result = {"component": component}
    if props:
        result["props"] = props
    if text is not None:
        result["text"] = text
    if content is not None:
        result["content"] = content
    return result


def paragraph(text, **props):
    return node("div", text, **{"class": "text-body-2", "style": "line-height:1.65", **props})


def col(content, sm=6):
    return node("VCol", content=content, cols=12, sm=sm,
                **{"style": "box-sizing:border-box;min-width:0"})


def field(component, model, label, hint, **props):
    if component in ("VTextField", "VSelect"):
        control_id = f"p115wait-{model}"
        return node("div", content=[
            node("label", label, **{"for": control_id, "class": "d-block mb-2",
                                   "style": INK + "font-size:15px;font-weight:600"}),
            node(component, model=model, id=control_id,
                 **{"variant": "outlined", "density": "comfortable", "hide-details": "auto",
                    "color": "primary", "style": INK, **props}),
            paragraph(hint, **{"class": "text-body-2 mt-2"}),
        ])
    return node("div", content=[
        node(component, model=model, label=label,
             **{"variant": "outlined", "density": "comfortable", "hide-details": "auto",
                "color": "primary", "style": INK, **props}),
        paragraph(hint, **{"class": "text-body-2 mt-2"}),
    ])


def filename(path):
    return PurePosixPath(str(path).replace("\\", "/")).name


def config_form(tasks, error="", batch_result=None):
    choices = [{"title": f"{filename(t['source'])} · {LABELS.get(t['state'], t['state'])}"
                         f" · 整理记录 #{t['history_id'] or '待生成'}", "value": t["id"],
                "props": {"disabled": t["state"] in ("running", "uploading")}}
               for t in tasks if t["state"] in ACTIVE]
    def toggle(model, title, hint):
        control_id = f"p115wait-{model}"
        return node("div", content=[
            node("div", content=[
                node("label", title, **{"for": control_id, "style": INK + "font-size:16px;font-weight:700;cursor:pointer"}),
                paragraph(hint, **{"class": "text-body-2 mt-1"}),
            ], **{"style": "min-width:0"}),
            node("VSwitch", model=model, id=control_id, color="primary",
                 **{"inset": True, "hide-details": True, "density": "comfortable", "style": "flex:0 0 auto"}),
        ], **{"class": "d-flex align-center justify-space-between ga-3"})

    intro = node("div", content=[node("VRow", content=[
        col([toggle("enabled", "启用秒传等待", "后台等待，其他整理照常进行。")]),
        col([toggle("notify", "任务状态通知", "首次等待、达到上限或异常时推送。")]),
    ])], **{"class": "pa-4 rounded-lg mb-5", "style": "background:rgba(var(--v-theme-primary),.06);" + INK})
    strategy = node("div", content=[
        node("div", "自动重试", **{"class": "text-subtitle-1 font-weight-bold mb-4", "style": INK}),
        node("div", content=[
            node("div", content=[field("VTextField", "max_retries", "最多重试", "不含首次尝试。",
                 type="number", suffix="次", min=0, max=100, step=1)],
                 **{"style": "flex:0 1 160px;min-width:0;max-width:100%"}),
            node("div", content=[field("VTextField", "max_wait_hours", "最长等待", "0 表示不限时。",
                 type="number", suffix="小时", min=0, max=8760, step=1)],
                 **{"style": "flex:0 1 160px;min-width:0;max-width:100%"}),
            node("div", content=[field("VTextField", "retry_intervals", "每次重试间隔", "英文逗号分隔，依次使用。",
                 placeholder="60,180,600,1800", suffix="秒", **{"spellcheck": False})],
                 **{"style": "flex:0 1 320px;min-width:0;max-width:100%"}),
        ], **{"class": "d-flex flex-wrap ga-4"}),
        node("div", content=[field("VSelect", "limit_action", "达到上限后", "次数或时限任一达到，即执行所选操作。",
            items=[{"title": "需要手动操作", "value": "manual"},
                   {"title": "自动强制上传", "value": "upload"}])],
            **{"class": "mt-5", "style": "width:320px;max-width:100%"}),
        node("div", content=[node("VIcon", "mdi-cloud-upload-outline", size=20),
            node("span", "自动强制上传：先试秒传，未命中就上传文件。")],
            **{"class": "d-flex align-center ga-2 mt-3", "style": tone_style("info") + "font-size:14px;font-weight:600"}),
    ], **{"class": "mb-5"})
    def disclosure(title, children):
        return node("details", content=[
            node("summary", title, **{"style": INK + "cursor:pointer;box-sizing:border-box;min-height:48px;padding:12px 0;font-weight:600;line-height:24px"}),
            node("div", content=children, **{"class": "pt-2 pb-4"}),
        ], **{"style": "box-sizing:border-box;border-top:1px solid rgba(var(--v-theme-on-surface),.15)"})

    help_section = disclosure("规则与通知说明", [
        paragraph("整理记录：等待时显示失败，完成后原记录更新成功；本地文件保留到整理完成。"),
        paragraph("上限策略：默认需手动操作。自动强制上传使用独立队列；上传失败或授权、源文件异常仍暂停。已暂停任务不会因修改策略自动恢复。",
                  **{"class": "text-body-2 mt-3"}),
        paragraph("重试：次数不含首次，填 3 即最多尝试 4 次，填 0 只试首次。时限填 0 仍受次数限制。间隔每项 30～86400 秒，用完后沿用最后一项，从每轮结束后计时，实际有 ±10% 浮动。",
                  **{"class": "text-body-2 mt-3"}),
        paragraph("通知：请在 MP 通知渠道中开启「整理入库」和「手动处理」。首次等待及自动转上传走整理入库，暂停走手动处理；中间重试不重复推送。成功及最终整理失败沿用 MP 原生通知。",
                  **{"class": "text-body-2 mt-3"}),
        paragraph("兼容：MP V2.15.6，本地到内置 115 的复制/移动；字幕、NFO、图片沿用 MP，蓝光原盘目录暂不支持。",
                  **{"class": "text-body-2 mt-3"}),
        paragraph("覆盖备份：旧文件放在目标目录 .mp115-backups，确认无误后可自行清理。",
                  **{"class": "text-body-2 mt-3"}),
    ])
    scope = disclosure("视频格式 · 通常无需修改", [
        field("VTextarea", "extensions", "接管的视频后缀", "例如 mkv,mp4,iso，不加点号；未列出的格式走 MP 原流程。",
              rows=2, **{"auto-grow": True, "spellcheck": False}),
    ])
    operations = disclosure("批量任务操作", [
            paragraph("选择多个任务，统一执行操作；强制上传会依次排队。",
                      **{"class": "text-body-2 mb-4"}),
            node("VRow", content=[
                col([field("VAutocomplete", "task_ids", "选择任务（可多选、可搜索）",
                           "执行中的任务不可选；状态不支持的任务会跳过。" if choices else "暂无可操作任务。启用插件后照常发起整理即可。",
                           items=choices, **{"item-title": "title", "item-value": "value",
                                            "multiple": True, "chips": True, "closable-chips": True,
                                            "clearable": True, "disabled": not choices})], sm=8),
                col([field("VSelect", "action", "统一执行", "继续等待仅适用于已暂停任务。",
                           items=[{"title": "强制上传（未秒传就上传）", "value": "upload"},
                                  {"title": "暂停自动重试", "value": "pause"},
                                  {"title": "继续等待秒传", "value": "resume"},
                                  {"title": "取消等待", "value": "cancel"}],
                           **{"disabled": "{{ !task_ids || !task_ids.length }}"})], sm=4),
            ]),
            paragraph(ACTION_HINT, **{"class": "text-body-2 mt-3"}),
            node("VCheckbox", model="apply_action", label="保存时对所选任务执行一次",
                 **{"color": "primary", "class": "mt-3", "hide-details": True,
                    "disabled": "{{ !enabled || !task_ids || !task_ids.length }}"}),
            paragraph("执行后清空选择；重新打开配置可查看结果。"),
    ])
    if batch_result:
        actions = {"upload": "强制上传", "resume": "继续等待秒传", "pause": "暂停重试", "cancel": "取消等待"}
        summary = (f"上次{actions.get(batch_result['action'], '批量操作')}：已接受 {batch_result['accepted']} · "
                   f"跳过 {batch_result['skipped']} · 异常 {batch_result['failed']}")
        result_details = [paragraph(batch_result["at"] + " · 已接受表示操作已提交，不代表文件已上传完成。",
                                    **{"class": "text-body-2 mb-3"})]
        for item in batch_result["items"]:
            result_details.append(paragraph(f"{item['name']}：{item['message']}",
                **{"class": "text-body-2 mb-2", "style": "overflow-wrap:anywhere;line-height:1.65"}))
        operations["content"][1]["content"].insert(0, disclosure(summary, result_details))
    content = [intro]
    if error:
        content.append(node("VAlert", error, type="error", variant="tonal", **{"class": "mb-4"}))
    content.extend([strategy, help_section, scope, operations])
    return [node("VForm", content=content, **{"style": INK})]


TONES = {"waiting": ("warning", "mdi-clock-outline"), "queued": ("info", "mdi-clock-outline"),
         "paused": ("warning", "mdi-pause-circle-outline"), "running": ("primary", "mdi-sync"),
         "uploading": ("primary", "mdi-cloud-upload-outline"), "upload_queued": ("info", "mdi-cloud-clock-outline"),
         "completed": ("success", "mdi-check-circle-outline"), "failed": ("error", "mdi-alert-circle-outline"),
         "cancelled": ("secondary", "mdi-close-circle-outline")}
INK = "color:rgb(var(--v-theme-on-surface));opacity:1;"


def tone_style(tone, background=False):
    # Mix semantic colors with theme ink for readable text in both MP themes.
    style = INK + f"color:color-mix(in srgb,rgb(var(--v-theme-{tone})) 40%,rgb(var(--v-theme-on-surface)));"
    if background:
        style += f"background:rgba(var(--v-theme-{tone}),.14);"
    return style


def status_chip(state, label=None):
    tone, icon = TONES.get(state, ("secondary", "mdi-circle-outline"))
    return node("VChip", content=[node("VIcon", icon, size=18, **{"class": "mr-1"}),
                                  node("span", label or LABELS.get(state, state))],
                variant="flat", size="small", **{"style": tone_style(tone, True) + "font-weight:700"})


def schedule_summary(task, enabled, now):
    state = task["state"]
    if state == "completed":
        return "整理结果", "原记录已成功", "success"
    if state in ("failed", "cancelled"):
        return "后续安排", "不再重试", "error" if state == "failed" else "secondary"
    if not enabled:
        return "后续安排", "插件已关闭", "secondary"
    if state == "paused":
        return "后续安排", "等待手动处理", "warning"
    if state == "upload_queued":
        return "后续安排", "排队等待上传", "info"
    if state in ("running", "uploading"):
        return "当前进度", "强制上传中" if state == "uploading" else "本轮正在执行", "primary"
    next_at = task.get("next_at") or 0
    if next_at <= now:
        return "下次尝试", "已到时间 · 待调度", "primary"
    return "下次重试 · MP 时间", datetime.fromtimestamp(next_at).strftime("%m-%d %H:%M:%S"), "primary"


def record_time(value):
    if value is None:
        return "未记录"
    try:
        return datetime.fromtimestamp(value).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    except (TypeError, ValueError, OverflowError, OSError):
        return "未记录"


def task_button(task, action, label):
    button = node("VBtn", label, variant="tonal" if action == "upload" else "outlined",
                  **{"size": "small", "style": "min-height:40px"})
    button["events"] = {"click": {"api": f"plugin/P115InstantWait/tasks/{task['id']}/{action}", "method": "POST"}}
    return button


def task_page(tasks, enabled, error="", max_retries=None):
    now = time.time()
    active = sum(t["state"] in ACTIVE for t in tasks)
    content = [node("div", content=[
        node("div", "整理任务", **{"class": "text-h6 font-weight-bold"}),
        node("VChip", "运行中" if enabled else "插件已关闭", size="small", variant="tonal"),
    ], **{"class": "d-flex align-center justify-space-between ga-2 mb-2"}),
        paragraph(f"最近 {len(tasks)} 条 · {active} 条未结束。展开任务可操作，路径默认收起。时间按 MP 所在时区显示。" if tasks else
                  "启用插件后，在 MP 发起本地 → 内置 115 的视频整理，任务会出现在这里。",
                  **{"class": "text-body-2 mb-4"})]
    if tasks:
        counts = [("paused", "待处理", ("paused",)), ("waiting", "等待重试", ("queued", "waiting")),
                  ("running", "执行中", ("running", "uploading")), ("upload_queued", "上传排队", ("upload_queued",)),
                  ("completed", "已成功", ("completed",)), ("failed", "失败", ("failed",))]
        content.append(node("div", content=[status_chip(state, f"{label} {sum(t['state'] in states for t in tasks)}")
            for state, label, states in counts if any(t["state"] in states for t in tasks)],
            **{"class": "d-flex flex-wrap ga-2 mb-4"}))
    if error:
        content.append(node("VAlert", error, type="error", variant="tonal", **{"class": "mb-4"}))
    if tasks and not enabled:
        content.append(node("VAlert", "插件关闭期间队列不会重试。请到配置页启用插件。",
                            type="info", variant="tonal", **{"class": "mb-4"}))
    panels = []
    for task in sorted(tasks, key=lambda t: t["state"] not in ACTIVE):
        status = task["state"]
        next_label, next_value, next_tone = schedule_summary(task, enabled, now)
        auto = task.get("auto_attempts", task["attempts"])
        budget = f"本轮自动 {auto}/{max_retries + 1}" if max_retries is not None and status in ACTIVE else f"自动尝试 {auto} 次"
        title = node("VExpansionPanelTitle", content=[
            node("div", content=[
                node("div", filename(task["source"]),
                     **{"class": "text-subtitle-1 font-weight-bold", "style": INK + "overflow-wrap:anywhere;line-height:1.5"}),
                paragraph(f"整理记录 #{task['history_id'] or '待生成'}", **{"class": "text-body-2 mt-1"}),
                node("VRow", content=[
                    node("VCol", content=[paragraph("当前状态", **{"class": "text-caption mb-2"}), status_chip(status)], cols=6, sm=3),
                    node("VCol", content=[paragraph("累计尝试", **{"class": "text-caption mb-1"}),
                        node("div", f"{task['attempts']} 次", **{"style": INK + "font-size:20px;font-weight:700;line-height:1.4;font-variant-numeric:tabular-nums"}),
                        paragraph(budget, **{"class": "text-caption mt-1"})], cols=6, sm=3),
                    node("VCol", content=[paragraph(next_label, **{"class": "text-caption mb-1"}),
                        node("div", next_value, **{"style": tone_style(next_tone) + "font-size:20px;font-weight:700;line-height:1.4;font-variant-numeric:tabular-nums;overflow-wrap:anywhere"})], cols=12, sm=6),
                ], **{"class": "mt-1"}),
                node("div", content=[
                    paragraph("入队时间：" + record_time(task.get("created")), **{"class": "text-caption"}),
                    paragraph(("完成时间：" if status == "completed" else "结束时间：" if status in ("failed", "cancelled") else "更新时间：") +
                              record_time(task.get("updated")), **{"class": "text-caption"}),
                ], **{"class": "d-flex flex-wrap mt-3", "style": "gap:4px 24px;font-variant-numeric:tabular-nums"}),
            ], **{"style": "min-width:0;flex:1"}),
        ], **{"class": "ga-3 align-start py-4"})
        details = [node("div", content=[paragraph("最新结果", **{"class": "text-caption mb-1"}),
            paragraph(latest_result(task),
                      **{"style": INK + "line-height:1.65;overflow-wrap:anywhere"})],
            **{"class": "pa-3 rounded mb-3", "style": f"background:rgba(var(--v-theme-{TONES.get(status, ('secondary', ''))[0]}),.08)"})]
        if status in ("queued", "waiting", "paused", "upload_queued") and enabled:
            buttons = [task_button(task, "resume", "继续等待秒传") if status == "paused" else task_button(task, "pause", "暂停重试"),
                       task_button(task, "cancel", "取消等待")]
            if status != "upload_queued":
                buttons.insert(0, task_button(task, "upload", "强制上传"))
            details.extend([node("div", content=buttons, **{"class": "d-flex flex-wrap ga-2"}),
                            paragraph("强制上传：未秒传就上传文件，占用上行带宽。取消等待不会删除文件。",
                                      **{"class": "text-body-2 mt-2 mb-3"})])
        paths = [node("summary", "路径与日志定位", **{"style": "cursor:pointer;min-height:40px;line-height:40px;font-weight:600;" + INK})]
        for label, value in (("本地源文件", task["source"]), ("115 目标文件", task["target"]), ("任务 ID（可在 MP 日志中搜索）", task["id"])):
            paths.append(node("div", content=[paragraph(label, **{"class": "text-caption mb-1"}),
                paragraph(str(value), **{"style": "line-height:1.6;overflow-wrap:anywhere;user-select:text"})], **{"class": "my-3"}))
        if task.get("backup_files"):
            paths.append(paragraph(f"旧版本备份 {len(task['backup_files'])} 个，位于目标目录 .mp115-backups。"))
        details.append(node("details", content=paths, **{"class": "mt-3"}))
        panels.append(node("VExpansionPanel", content=[title, node("VExpansionPanelText", content=details)]))
    if panels:
        content.append(node("VExpansionPanels", content=panels, variant="accordion",
                            **{"class": "border rounded-lg", "elevation": 0}))
    else:
        content.append(node("VSheet", content=[
            node("VIcon", "mdi-cloud-upload-outline", size=32, **{"class": "mb-3"}),
            node("div", "还没有整理任务", **{"class": "text-subtitle-1 font-weight-bold mb-2"}),
            paragraph("先确认 MP 内置 115 已授权，再启用插件并整理一个视频文件。"),
        ], **{"class": "border rounded-lg pa-6 text-center"}))
    if tasks:
        content.append(paragraph("页面快照 " + datetime.fromtimestamp(now).astimezone().strftime("%m-%d %H:%M:%S %z") +
                                 " · 重新打开可更新；MP 日志搜索「115秒传等待」查看过程。",
                                 **{"class": "text-caption mt-3"}))
    return [node("VContainer", content=content, **{"class": "pa-0", "style": INK})]
