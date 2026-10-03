"""Native MoviePilot/Vuetify views, with no upload or queue mutations."""
from datetime import datetime
from pathlib import PurePosixPath
import time


LABELS = {"queued": "等待首次秒传", "waiting": "等待重试", "running": "正在执行",
          "paused": "已暂停 · 待处理", "completed": "整理成功", "failed": "整理失败", "cancelled": "已取消",
          "upload_queued": "等待手动上传", "uploading": "手动处理中"}
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
    return node("VCol", content=content, cols=12, sm=sm)


def field(component, model, label, hint, **props):
    # Native input hints use tightly spaced 12px text. Keep explanations readable
    # using ordinary theme-aware body text, without modifying MoviePilot's CSS.
    return node("div", content=[
        node(component, model=model, label=label,
             **{"variant": "outlined", "density": "comfortable", "hide-details": "auto", **props}),
        paragraph(hint, **{"class": "text-body-2 mt-2"}),
    ])


def filename(path):
    return PurePosixPath(str(path).replace("\\", "/")).name


def config_form(tasks, error=""):
    choices = [{"title": f"{filename(t['source'])} · {LABELS.get(t['state'], t['state'])}"
                         f" · 整理记录 #{t['history_id'] or '待生成'}", "value": t["id"],
                "props": {"disabled": t["state"] in ("running", "uploading")}}
               for t in tasks if t["state"] in ACTIVE]
    intro = node("div", content=[
        node("div", content=[
            node("span", "让整理在后台等待秒传", **{"class": "text-subtitle-1 font-weight-bold"}),
            node("VChip", "MP V2.15.6 · 内置 115", size="small", variant="outlined"),
        ], **{"class": "d-flex align-center justify-space-between flex-wrap ga-2 mb-2"}),
        paragraph("自动尝试秒传，用完重试次数后暂停。你可以随时对等待任务强制上传，其他整理继续进行。"),
        node("div", content=[
            node("VChip", "等待：原记录显示失败", size="small", variant="tonal"),
            node("VIcon", "mdi-arrow-right", size=18),
            node("VChip", "整理完成：原记录更新成功", size="small", variant="tonal"),
        ], **{"class": "d-flex align-center flex-wrap ga-2 mt-3"}),
    ], **{"class": "mb-5"})
    switches = node("VRow", content=[
        col([
            node("VSwitch", model="enabled", label="启用秒传等待", color="primary",
                 **{"inset": True, "hide-details": True, "density": "comfortable"}),
            paragraph("接管本地 → 内置 115 的视频整理。首次尝试也由后台执行。"),
        ]),
        col([
            node("VSwitch", model="notify", label="任务暂停时通知我", color="primary",
                 **{"inset": True, "hide-details": True, "density": "comfortable"}),
            paragraph("重试用完、等待到期、授权异常或手动上传失败时通知；每次未秒传不通知。"),
        ]),
    ], **{"class": "mb-4"})
    strategy = node("div", content=[
        node("div", "等待策略", **{"class": "text-subtitle-1 font-weight-bold mb-1"}),
        paragraph("自动流程只试秒传。达到次数或时限就暂停，普通上传必须手动触发。", **{"class": "text-body-2 mb-4"}),
        node("VRow", content=[
            col([field("VTextField", "retry_intervals", "未秒传后，隔多久重试",
                       "按顺序使用，不够时沿用最后一项。默认间隔约 1、3、10、30 分钟；每轮结束后计时。",
                       placeholder="60,180,600,1800", suffix="秒",
                       **{"spellcheck": False})]),
            col([field("VTextField", "max_wait_hours", "最多自动等待多久",
                       "默认 24 小时，到期暂停并保留文件。填 0 只取消时限，仍受重试次数限制。",
                       type="number", suffix="小时", min=0, max=8760, step=1)]),
        ]),
        node("VRow", content=[
            col([field("VTextField", "max_retries", "最多自动重试次数",
                       "默认重试 3 次，共尝试 4 次。用完后暂停，需手动处理；填 0 表示首次未成功就暂停。",
                       type="number", suffix="次", min=0, max=100, step=1)]),
        ], **{"class": "mt-2"}),
        paragraph("重试间隔用英文逗号分隔，每项为 30～86400 秒；实际间隔会有约 ±10% 浮动。",
                  **{"class": "text-body-2 mt-3"}),
    ], **{"class": "mb-5"})
    scope = node("VExpansionPanel", content=[
        node("VExpansionPanelTitle", "文件范围与使用说明"),
        node("VExpansionPanelText", content=[
            field("VTextarea", "extensions", "需要等待秒传的视频格式",
                  "填写文件后缀，用英文逗号分隔，不需要加点。默认已包含常见视频和 ISO，通常不用修改。",
                  rows=2, **{"auto-grow": True, "spellcheck": False}),
            paragraph("例如：mkv,mp4,iso。未列出的格式仍走 MP 原流程。", **{"class": "text-body-2 mt-3"}),
            node("VDivider", **{"class": "my-4"}),
            paragraph("只支持本地文件整理到 MP 内置 115，整理方式为复制或移动。字幕、NFO 和图片沿用 MP。"),
            paragraph("等待期间保留本地视频。自动任务只尝试秒传；点强制上传后，未秒传就通过网络上传文件。蓝光原盘目录暂不支持。", **{"class": "text-body-2 mt-2"}),
            paragraph("覆盖时旧文件保存在目标目录的 .mp115-backups 中，确认无误后可以自行清理。",
                      **{"class": "text-body-2 mt-2"}),
        ]),
    ])
    operations = node("VExpansionPanel", content=[
        node("VExpansionPanelTitle", "单个任务操作"),
        node("VExpansionPanelText", content=[
            paragraph("推荐点「查看数据」，在任务详情直接强制上传或管理等待任务。这里的操作只在保存时执行一次。",
                      **{"class": "text-body-2 mb-4"}),
            node("VRow", content=[
                col([field("VSelect", "task_id", "选择需要操作的文件",
                           "正在执行的任务暂不能操作，请等本轮结束。" if choices else "暂无可操作任务。启用插件后照常发起整理即可。",
                           items=choices, **{"item-title": "title", "item-value": "value",
                                            "clearable": True, "disabled": not choices})], sm=8),
                col([field("VSelect", "action", "对这个任务做什么", ACTION_HINT,
                           items=[{"title": "强制上传（未秒传就上传）", "value": "upload"},
                                  {"title": "暂停自动重试", "value": "pause"},
                                  {"title": "继续等待秒传", "value": "resume"},
                                  {"title": "取消等待", "value": "cancel"}],
                           **{"disabled": "{{ !task_id }}"})], sm=4),
            ]),
            node("VCheckbox", model="apply_action", label="保存时执行所选操作（仅一次）",
                 **{"color": "primary", "class": "mt-3", "hide-details": True,
                    "disabled": "{{ !enabled || !task_id }}"}),
            paragraph("需要先启用插件；只选文件或操作，不勾选这一项，就不会执行。执行后勾选会自动关闭。"),
        ]),
    ])
    content = [intro]
    if error:
        content.append(node("VAlert", error, type="error", variant="tonal", **{"class": "mb-4"}))
    content.extend([switches, node("VDivider", **{"class": "mb-5"}), strategy,
                    node("VExpansionPanels", content=[scope, operations], variant="accordion",
                         **{"class": "border rounded-lg", "elevation": 0})])
    return [node("VForm", content=content)]


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
        return "当前进度", "手动处理中" if state == "uploading" else "本轮正在执行", "primary"
    next_at = task.get("next_at") or 0
    if next_at <= now:
        return "下次尝试", "已到时间 · 待调度", "primary"
    return "下次重试 · MP 时间", datetime.fromtimestamp(next_at).strftime("%m-%d %H:%M:%S"), "primary"


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
        paragraph(f"最近 {len(tasks)} 条 · {active} 条未结束。展开任务可操作，路径默认收起。" if tasks else
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
            ], **{"style": "min-width:0;flex:1"}),
        ], **{"class": "ga-3 align-start py-4"})
        details = [node("div", content=[paragraph("最新结果", **{"class": "text-caption mb-1"}),
            paragraph(task["message"] or ("等待首次执行" if status == "queued" else LABELS.get(status, status)),
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
