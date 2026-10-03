"""Native MoviePilot/Vuetify views, with no upload or queue mutations."""
from datetime import datetime
from pathlib import PurePosixPath
import time


LABELS = {"queued": "等待首次秒传", "waiting": "等待重试", "running": "正在执行",
          "paused": "已暂停", "completed": "整理成功", "failed": "整理失败", "cancelled": "已取消"}
ACTIVE = {"queued", "waiting", "running", "paused"}
ACTION_HINT = "暂停后可恢复；恢复会重置等待时限；取消后不再重试，文件和记录保留。"


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
                "props": {"disabled": t["state"] == "running"}}
               for t in tasks if t["state"] in ACTIVE]
    intro = node("div", content=[
        node("div", content=[
            node("span", "让整理在后台等待秒传", **{"class": "text-subtitle-1 font-weight-bold"}),
            node("VChip", "MP V2.15.6 · 内置 115", size="small", variant="outlined"),
        ], **{"class": "d-flex align-center justify-space-between flex-wrap ga-2 mb-2"}),
        paragraph("启用后照常在 MP 整理。本地视频未秒传时留在队列中，其他整理继续进行。"),
        node("div", content=[
            node("VChip", "等待：原记录显示失败", size="small", variant="tonal"),
            node("VIcon", "mdi-arrow-right", size=18),
            node("VChip", "秒传成功：原记录更新成功", size="small", variant="tonal"),
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
            paragraph("授权失效、文件变化或等待到期时发系统消息；每次未秒传不通知。"),
        ]),
    ], **{"class": "mb-4"})
    strategy = node("div", content=[
        node("div", "等待策略", **{"class": "text-subtitle-1 font-weight-bold mb-1"}),
        paragraph("一般保持默认即可。重试只能再次尝试秒传，不能保证最终命中。", **{"class": "text-body-2 mb-4"}),
        node("VRow", content=[
            col([field("VTextField", "retry_intervals", "未秒传后，隔多久重试",
                       "按顺序使用，之后一直沿用最后一项。默认约 1、3、10、30 分钟；每轮结束后计时。",
                       placeholder="60,180,600,1800", suffix="秒",
                       **{"spellcheck": False})]),
            col([field("VTextField", "max_wait_hours", "最多自动等待多久",
                       "默认 24 小时，到期暂停并保留文件。填 0 可一直等待，手动恢复后重新计时。",
                       type="number", suffix="小时", min=0, max=8760, step=1)]),
        ]),
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
            paragraph("等待期间保留本地视频，已接管的视频只尝试秒传。蓝光原盘目录暂不支持。", **{"class": "text-body-2 mt-2"}),
            paragraph("覆盖时旧文件保存在目标目录的 .mp115-backups 中，确认无误后可以自行清理。",
                      **{"class": "text-body-2 mt-2"}),
        ]),
    ])
    operations = node("VExpansionPanel", content=[
        node("VExpansionPanelTitle", "单个任务操作"),
        node("VExpansionPanelText", content=[
            paragraph("也可以点左下角「查看数据」，在任务详情里直接暂停、恢复或取消。这里的操作只在保存时执行一次。",
                      **{"class": "text-body-2 mb-4"}),
            node("VRow", content=[
                col([field("VSelect", "task_id", "选择需要操作的文件",
                           "正在执行的任务暂不能操作，请等本轮结束。" if choices else "暂无可操作任务。启用插件后照常发起整理即可。",
                           items=choices, **{"item-title": "title", "item-value": "value",
                                            "clearable": True, "disabled": not choices})], sm=8),
                col([field("VSelect", "action", "对这个任务做什么", ACTION_HINT,
                           items=[{"title": "暂停自动重试", "value": "pause"},
                                  {"title": "恢复自动重试", "value": "resume"},
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


def retry_caption(task):
    state = task["state"]
    if state == "running":
        return "正在计算哈希、尝试秒传或完成整理"
    if state in ("waiting", "queued"):
        next_at = task.get("next_at") or 0
        if next_at <= time.time():
            return "已到执行时间，等待后台调度"
        return "下次尝试 " + datetime.fromtimestamp(next_at).strftime("%m-%d %H:%M:%S")
    if state == "paused":
        return "已停止自动重试，可手动恢复"
    if state == "completed":
        return "原整理记录已更新为成功"
    return "这个任务已结束"


def task_button(task, action, label):
    button = node("VBtn", label, variant="outlined",
                  **{"size": "small", "style": "min-height:36px"})
    button["events"] = {"click": {"api": f"plugin/P115InstantWait/tasks/{task['id']}/{action}", "method": "POST"}}
    return button


def task_page(tasks, enabled, error=""):
    active = sum(t["state"] in ACTIVE for t in tasks)
    content = [node("div", content=[
        node("div", "整理任务", **{"class": "text-h6 font-weight-bold"}),
        node("VChip", "运行中" if enabled else "插件已关闭", size="small", variant="tonal"),
    ], **{"class": "d-flex align-center justify-space-between ga-2 mb-2"}),
        paragraph(f"显示最近 {len(tasks)} 条任务，其中 {active} 条尚未结束。展开文件可查看路径和操作。" if tasks else
                  "启用插件后，在 MP 发起本地 → 内置 115 的视频整理，任务会出现在这里。",
                  **{"class": "text-body-2 mb-4"})]
    if error:
        content.append(node("VAlert", error, type="error", variant="tonal", **{"class": "mb-4"}))
    if tasks and not enabled:
        content.append(node("VAlert", "插件关闭期间队列不会重试。请到配置页启用插件。",
                            type="info", variant="tonal", **{"class": "mb-4"}))
    panels = []
    for task in sorted(tasks, key=lambda t: t["state"] not in ACTIVE):
        status = task["state"]
        title = node("VExpansionPanelTitle", content=[
            node("div", content=[
                node("div", filename(task["source"]),
                     **{"class": "text-subtitle-2 font-weight-bold", "style": "overflow-wrap:anywhere;line-height:1.5"}),
                node("div", content=[
                    paragraph(f"整理记录 #{task['history_id'] or '待生成'} · 已尝试 {task['attempts']} 次"),
                    node("VChip", LABELS.get(status, status), size="small", variant="tonal"),
                ], **{"class": "d-flex align-center flex-wrap ga-2 mt-2"}),
            ], **{"style": "min-width:0;flex:1"}),
        ], **{"class": "ga-3"})
        details = [paragraph(retry_caption(task), **{"class": "text-body-2 mb-3 font-weight-medium"})]
        for label, value in (("当前说明", task["message"] or "等待后台首次尝试秒传"),
                             ("本地源文件", task["source"]), ("115 目标文件", task["target"]), ("任务 ID", task["id"])):
            details.append(node("div", content=[
                node("div", label, **{"class": "text-caption mb-1"}),
                paragraph(str(value), **{"style": "line-height:1.6;overflow-wrap:anywhere;user-select:text"}),
            ], **{"class": "mb-3"}))
        if task.get("backup_files"):
            details.append(paragraph(f"已保留 {len(task['backup_files'])} 个旧版本文件，位于目标目录的 .mp115-backups。",
                                     **{"class": "text-body-2 mb-3"}))
        if status in ("queued", "waiting", "paused") and enabled:
            buttons = [task_button(task, "resume", "恢复重试") if status == "paused" else task_button(task, "pause", "暂停重试"),
                       task_button(task, "cancel", "取消等待")]
            details.extend([node("div", content=buttons, **{"class": "d-flex flex-wrap ga-2 mt-4"}),
                            paragraph("取消只停止后续重试，保留源文件、远端已有文件和整理记录。",
                                      **{"class": "text-body-2 mt-3"})])
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
    return [node("VContainer", content=content, **{"class": "pa-0"})]
