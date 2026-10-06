"""Theme-aware native MoviePilot views. Rendering never mutates the queue."""
from datetime import datetime
from pathlib import PurePosixPath
import time

from .records import latest_result


LABELS = {"queued": "等待首次秒传", "waiting": "等待重试", "running": "正在执行",
          "paused": "已暂停 · 待处理", "completed": "整理成功", "failed": "整理失败",
          "cancelled": "已取消", "upload_queued": "强制上传排队", "uploading": "强制上传中"}
ACTIVE = {"queued", "waiting", "running", "paused", "upload_queued", "uploading"}
TONES = {"waiting": ("info", "mdi-clock-outline"), "queued": ("info", "mdi-clock-outline"),
         "paused": ("warning", "mdi-pause-circle-outline"), "running": ("primary", "mdi-sync"),
         "uploading": ("primary", "mdi-cloud-upload-outline"),
         "upload_queued": ("info", "mdi-cloud-clock-outline"),
         "completed": ("success", "mdi-check-circle-outline"),
         "failed": ("error", "mdi-alert-circle-outline"),
         "cancelled": ("secondary", "mdi-close-circle-outline")}
INK = "color:rgb(var(--v-theme-on-surface));opacity:1;"
ACTION_HINT = "继续等待会重置自动尝试次数和时限；强制上传先试秒传，未命中就上传文件；取消任务保留文件。"

# Scope all rules to this plugin; let Vuetify own controls, icons and theme colors.
# Native details work in both FormRender and PageRender without a custom bundle.
STYLES = """
.p115-ui{--p115-line:rgba(var(--v-theme-on-surface),.14);--p115-muted:rgba(var(--v-theme-on-surface),.78);color:rgb(var(--v-theme-on-surface));font-size:14px;line-height:1.6;max-width:100%;container-type:inline-size}
.p115-ui *{box-sizing:border-box}
.p115-ui h2,.p115-ui h3,.p115-ui p{margin:0}
.p115-ui h2{font-size:22px;line-height:1.35;font-weight:700;letter-spacing:-.02em;text-wrap:balance}
.p115-ui h3{font-size:16px;line-height:1.5;font-weight:650}
.p115-ui .p115-muted{color:var(--p115-muted);opacity:1}
.p115-ui .p115-copy{max-width:72ch;overflow-wrap:anywhere}
.p115-ui .p115-header{display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:16px;margin-bottom:24px}
.p115-ui .p115-heading{display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:6px}
.p115-ui .p115-section{padding:24px 0;border-top:1px solid var(--p115-line)}
.p115-ui .p115-section-head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:16px}
.p115-ui .p115-section-label{display:flex;align-items:center;gap:8px}
.p115-ui .p115-switches{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;padding:20px;background:rgba(var(--v-theme-primary),.055);border-radius:12px;margin-bottom:24px}
.p115-ui .p115-toggle{display:flex;align-items:center;justify-content:space-between;gap:16px;min-width:0}
.p115-ui .p115-toggle label{font-size:15px;font-weight:650;cursor:pointer}
.p115-ui .p115-switches .v-switch{flex:0 0 auto}
.p115-ui .p115-fields{display:grid;grid-template-columns:repeat(2,minmax(0,260px));gap:20px 24px}
.p115-ui .p115-field{min-width:0}
.p115-ui .p115-full{grid-column:1 / -1}
.p115-ui .p115-field-label{display:block;font-weight:600;margin-bottom:8px}
.p115-ui .p115-hint{font-size:13px;color:var(--p115-muted);margin-top:8px;line-height:1.6;overflow-wrap:anywhere}
.p115-ui.p115-settings .p115-full .v-input{max-width:520px}
.p115-ui.p115-settings .p115-full .v-select{max-width:420px}
.p115-ui .v-field__input{opacity:1}
.p115-ui input::placeholder,.p115-ui textarea::placeholder{color:var(--p115-muted);opacity:1}
.p115-ui .p115-note{display:flex;align-items:flex-start;gap:10px;padding:14px 16px;background:rgba(var(--v-theme-on-surface),.045);border-radius:8px;margin-top:16px}
.p115-ui .p115-note .v-icon{flex-shrink:0;margin-top:2px}
.p115-ui .p115-disclosure{border-top:1px solid var(--p115-line)}
.p115-ui .p115-disclosure>summary{display:flex;align-items:center;gap:10px;min-height:56px;padding:14px 0;cursor:pointer;list-style:none;font-weight:600}
.p115-ui summary::-webkit-details-marker{display:none}
.p115-ui .p115-disclosure>summary::after,.p115-ui .p115-task>summary::after{content:'›';font-size:24px;line-height:1;display:block;flex-shrink:0;margin-left:auto;transition:transform .18s ease-out}
.p115-ui details[open]>summary::after{transform:rotate(90deg)}
.p115-ui .p115-disclosure-body{padding:0 0 24px}
.p115-ui summary:focus-visible,.p115-ui button:focus-visible{outline:2px solid rgb(var(--v-theme-primary));outline-offset:3px;border-radius:6px}
.p115-ui .p115-toolbar{display:flex;align-items:center;flex-wrap:wrap;gap:8px;padding:12px 0;border-top:1px solid var(--p115-line);border-bottom:1px solid var(--p115-line);margin-bottom:24px}
.p115-ui .p115-group{margin-bottom:28px}
.p115-ui .p115-count{display:inline-flex;align-items:center;justify-content:center;font-size:12px;font-weight:650;min-width:24px;padding:1px 7px;border-radius:6px;background:rgba(var(--v-theme-on-surface),.065);font-variant-numeric:tabular-nums}
.p115-ui .p115-list{border:1px solid var(--p115-line);border-radius:10px;overflow:clip}
.p115-ui .p115-task+.p115-task{border-top:1px solid var(--p115-line)}
.p115-ui .p115-task>summary{display:flex;align-items:center;gap:16px;padding:18px 20px;cursor:pointer;list-style:none;transition:background-color .18s ease-out}
.p115-ui .p115-task>summary:hover,.p115-ui .p115-task[open]>summary{background:rgba(var(--v-theme-on-surface),.035)}
.p115-ui .p115-task>summary:focus-visible{outline-offset:-3px}
.p115-ui .p115-task-icon{display:flex;align-items:center;justify-content:center;width:40px;height:44px;border-radius:8px;background:rgba(var(--v-theme-on-surface),.045);flex-shrink:0}
.p115-ui .p115-task-main{min-width:0;flex:1}
.p115-ui .p115-task-name{font-weight:650;font-size:15px;line-height:1.5;overflow-wrap:anywhere;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.p115-ui .p115-task-meta{display:flex;align-items:center;flex-wrap:wrap;gap:4px 12px;margin-top:7px;font-size:12px;color:var(--p115-muted)}
.p115-ui .p115-schedule{flex:0 0 175px;min-width:0;text-align:right;font-variant-numeric:tabular-nums}
.p115-ui .p115-schedule-label{font-size:12px;color:var(--p115-muted)}
.p115-ui .p115-schedule-value{font-size:14px;font-weight:600;overflow-wrap:anywhere}
.p115-ui .p115-task-body{padding:4px 20px 20px}
.p115-ui .p115-result{padding:14px 16px;border-radius:8px;background:rgba(var(--v-theme-on-surface),.045);overflow-wrap:anywhere}
.p115-ui .p115-result-label{font-size:12px;font-weight:600;margin-bottom:4px}
.p115-ui .p115-actions{display:flex;align-items:flex-start;flex-wrap:wrap;gap:8px;margin-top:16px}
.p115-ui .p115-actions .v-btn{min-height:40px;letter-spacing:0}
.p115-ui .p115-confirm{flex:1 1 180px;min-width:0}
.p115-ui .p115-confirm>summary{display:flex;align-items:center;justify-content:center;min-height:40px;border:1px solid var(--p115-line);border-radius:6px;padding:7px 14px;cursor:pointer;list-style:none;font-size:14px;font-weight:500;transition:background-color .18s ease-out}
.p115-ui .p115-confirm>summary:hover{background:rgba(var(--v-theme-on-surface),.045)}
.p115-ui .p115-confirm-body{padding:12px 0;max-width:46ch}
.p115-ui .p115-path{display:block;font-family:ui-monospace,SFMono-Regular,Consolas,monospace;font-size:13px;line-height:1.7;overflow-wrap:anywhere;white-space:pre-wrap;user-select:text;margin:4px 0 16px}
.p115-ui .p115-times{display:flex;flex-wrap:wrap;gap:4px 24px;font-size:12px;color:var(--p115-muted);font-variant-numeric:tabular-nums;margin:16px 0}
.p115-ui .p115-empty{text-align:center;padding:40px 20px;border:1px dashed var(--p115-line);border-radius:10px}
.p115-ui .p115-empty h3{margin:12px 0 8px}
.p115-ui .p115-empty p{margin-inline:auto}
.p115-ui .p115-help-list{display:grid;gap:16px;margin:0;padding-left:20px;max-width:72ch}
.p115-ui .p115-footer{font-size:12px;color:var(--p115-muted);margin-top:20px}
@container (max-width:600px){
 .p115-ui .p115-switches{grid-template-columns:1fr;padding:16px;gap:20px}
 .p115-ui .p115-task>summary{flex-wrap:wrap;gap:10px;padding:16px}
 .p115-ui .p115-task-icon{display:none}
 .p115-ui .p115-task-main{flex-basis:calc(100% - 32px)}
 .p115-ui .p115-schedule{flex-basis:100%;text-align:left;display:flex;align-items:baseline;gap:8px;flex-wrap:wrap;order:1}
 .p115-ui .p115-task-body{padding:0 16px 16px}
}
@container (max-width:440px){
 .p115-ui .p115-fields{grid-template-columns:1fr;gap:20px}
 .p115-ui .p115-header{gap:12px;margin-bottom:20px}
 .p115-ui .p115-section{padding:20px 0}
 .p115-ui .p115-confirm{flex-basis:100%}
 .p115-ui .p115-actions .v-btn,.p115-ui .p115-confirm>summary{min-height:44px}
}
@media(prefers-reduced-motion:reduce){.p115-ui summary,.p115-ui summary::after{transition:none!important}}
"""


def node(component, text=None, content=None, **props):
    result = {"component": component}
    if props:
        result["props"] = props
    if text is not None:
        result["text"] = text
    if content is not None:
        result["content"] = content
    return result


def box(content, cls="", **props):
    return node("div", content=content, **{"class": cls, **props})


def paragraph(text, **props):
    return node("p", text, **{"class": "p115-copy", **props})


def icon(name, **props):
    return node("VIcon", name, **{"size": 20, "aria-hidden": "true", **props})


def filename(path):
    return PurePosixPath(str(path).replace("\\", "/")).name


def disclosure(title, children, icon_name=None, opened=False, **props):
    heading = ([icon(icon_name)] if icon_name else []) + [node("span", title)]
    return node("details", content=[node("summary", content=heading),
                                   box(children, "p115-disclosure-body")],
                **{"class": "p115-disclosure", "open": opened, **props})


def note(text, icon_name="mdi-information-outline", **props):
    return box([icon(icon_name), paragraph(text)], "p115-note", **props)


def section(title, description, children):
    return node("section", content=[
        box([node("h3", title)], "p115-section-head"),
        paragraph(description, **{"class": "p115-copy p115-muted mb-4"}), *children,
    ], **{"class": "p115-section"})


def field(component, model, label, hint, **props):
    control_id = f"p115wait-{model}"
    hint_id = control_id + "-hint"
    return box([
        node("label", label, **{"for": control_id, "class": "p115-field-label"}),
        node(component, model=model, id=control_id,
             **{"aria-label": label, "aria-describedby": hint_id,
                "variant": "outlined", "density": "comfortable", "hide-details": "auto",
                "color": "primary", **props}),
        paragraph(hint, id=hint_id, **{"class": "p115-hint"}),
    ], "p115-field")


def toggle(model, title, hint):
    control_id = f"p115wait-{model}"
    return box([
        box([node("label", title, **{"for": control_id}),
             paragraph(hint, **{"class": "p115-hint"})]),
        node("VSwitch", model=model, id=control_id, color="primary",
             **{"aria-label": title, "inset": True, "hide-details": True}),
    ], "p115-toggle")


def config_form(tasks, error="", batch_result=None, history_result=None):
    choices = [{"title": f"{filename(t['source'])} · {LABELS.get(t['state'], t['state'])}"
                         f" · 整理记录 #{t['history_id'] or '待生成'}", "value": t["id"],
                "props": {"disabled": t["state"] in ("running", "uploading")}}
               for t in tasks if t["state"] in ACTIVE]
    selectable = sum(not t["props"]["disabled"] for t in choices)
    history_choices = [{"title": f"{filename(t['source'])} · {LABELS.get(t['state'], t['state'])}",
                        "value": t["id"]} for t in tasks
                       if t["state"] in ("completed", "failed", "cancelled")]
    content = [node("style", STYLES),
        box([box([node("h2", "让秒传按计划等待"),
                  paragraph("设置自动尝试的节奏；需要介入时，在任务详情中处理。",
                            **{"class": "p115-copy p115-muted mt-2"})])], "p115-header"),
        box([toggle("enabled", "启用秒传等待", "后台处理视频，其他整理照常进行。"),
             toggle("notify", "推送任务通知", "首次等待、达到上限或异常时通知。")], "p115-switches")]
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-5", "role": "alert"}))

    strategy = section("自动重试", "次数或等待时限任一达到，即执行下方的上限策略。", [
        box([
            field("VTextField", "max_retries", "最多自动重试", "不含首次尝试；默认 3 次，共尝试 4 次。",
                  type="number", suffix="次", min=0, max=100, step=1, inputmode="numeric"),
            field("VTextField", "max_wait_hours", "最长等待时间", "0 表示不限时，仍受重试次数限制。",
                  type="number", suffix="小时", min=0, max=8760, step=1, inputmode="decimal"),
            box([field("VTextField", "retry_intervals", "重试间隔", "英文逗号分隔，每项 30～86400 秒；用完后沿用最后一项。",
                       placeholder="60,180,600,1800", suffix="秒", spellcheck=False)], "p115-full"),
            box([field("VSelect", "limit_action", "达到上限后", "修改策略不会自动恢复已暂停的任务。",
                       items=[{"title": "暂停，等待我处理", "value": "manual"},
                              {"title": "自动强制上传", "value": "upload"}])], "p115-full"),
        ], "p115-fields"),
        note("达到上限后保留本地文件。你可以继续等待、强制上传或取消任务。",
             "mdi-pause-circle-outline", **{"show": "{{ limit_action === 'manual' }}"}),
        note("先尝试秒传，未命中就上传视频正文，会占用上行带宽。上传失败或文件、授权异常时仍会暂停。",
             "mdi-cloud-upload-outline", **{"show": "{{ limit_action === 'upload' }}"}),
    ])

    batch_children = [
        paragraph("多选任务，保存配置时统一执行。单个任务也可在详情页直接处理。",
                  **{"class": "p115-copy p115-muted mb-4"}),
        field("VAutocomplete", "task_ids", "选择任务", "可按文件名搜索；执行中的任务不可选。" if selectable else "暂无可操作任务，执行中的任务需等待本轮结束。",
              items=choices, multiple=True, chips=True, clearable=True,
              **{"item-title": "title", "item-value": "value", "closable-chips": True,
                 "disabled": not selectable, "no-data-text": "没有匹配的任务"}),
        box([field("VSelect", "action", "对所选任务执行", "状态不支持的任务会跳过；继续等待仅适用于已暂停任务。",
                   items=[{"title": "暂停自动重试", "value": "pause"},
                          {"title": "继续等待秒传", "value": "resume"},
                          {"title": "强制上传", "value": "upload"},
                          {"title": "取消任务", "value": "cancel"}],
                   **{"disabled": "{{ !task_ids || !task_ids.length }}"})], "mt-5"),
        note(ACTION_HINT),
        node("VCheckbox", model="apply_action", label="保存时对所选任务执行一次",
             **{"color": "primary", "class": "mt-3", "hide-details": True,
                "disabled": "{{ !enabled || !task_ids || !task_ids.length }}"}),
        paragraph("需启用插件；执行后自动清空选择，重新打开配置可查看结果。", **{"class": "p115-hint"}),
    ]
    if batch_result:
        actions = {"upload": "强制上传", "resume": "继续等待", "pause": "暂停重试", "cancel": "取消任务"}
        title = (f"上次{actions.get(batch_result['action'], '批量操作')}：接受 {batch_result['accepted']} · "
                 f"跳过 {batch_result['skipped']} · 异常 {batch_result['failed']}")
        batch_children.insert(0, disclosure(title, [
            paragraph(batch_result.get("at", "") + " · 已接受表示操作已提交，不代表传输完成。",
                      **{"class": "p115-hint mb-3"}),
            *[paragraph(f"{item['name']}：{item['message']}", **{"class": "p115-copy mb-2"})
              for item in batch_result["items"]],
        ], opened=True, **{"class": "p115-disclosure mb-4"}))

    maintenance = [
        note("只删除插件中的已结束记录，不影响 MP 整理历史、本地文件及网盘文件。未完成任务和批次恢复需要的成功记录会跳过。"),
        box([field("VSelect", "history_mode", "清理方式", "只执行当前选择的清理方式。",
                   items=[{"title": "选择指定记录", "value": "selected"},
                          {"title": "按状态和保留天数", "value": "filtered"}])], "mt-5"),
        box([field("VAutocomplete", "history_ids", "选择已结束记录", "可搜索最近 200 条任务中的结束记录；更早的记录可按条件清理。",
                   items=history_choices, multiple=True, chips=True, clearable=True,
                   **{"item-title": "title", "item-value": "value", "closable-chips": True,
                      "disabled": not history_choices, "no-data-text": "没有匹配的记录"})],
            "mt-5", show="{{ history_mode === 'selected' }}"),
        box([
            field("VSelect", "history_states", "清理哪些状态", "只匹配勾选的结束状态。",
                  items=[{"title": "整理成功", "value": "completed"}, {"title": "整理失败", "value": "failed"},
                         {"title": "已取消", "value": "cancelled"}], multiple=True, chips=True),
            field("VTextField", "history_days", "保留最近记录", "按结束时间计算；0 清理全部匹配记录。",
                  type="number", suffix="天", min=0, max=36500, step=1, inputmode="numeric"),
        ], "p115-fields mt-5", show="{{ history_mode === 'filtered' }}"),
        node("VCheckbox", model="cleanup_history", label="保存时清理一次（插件记录不可恢复）",
             **{"color": "warning", "hide-details": True, "class": "mt-3",
                "disabled": "{{ history_mode === 'selected' ? (!history_ids || !history_ids.length) : (!history_states || !history_states.length) }}"}),
        paragraph("插件关闭时也可清理；执行后自动取消勾选。", **{"class": "p115-hint"}),
    ]
    if history_result:
        title = (f"上次清理{'未完成' if history_result.get('status') == 'failed' else '已结束'}："
                 f"删除 {history_result.get('deleted', 0)} · 跳过 {history_result.get('skipped', 0)}")
        maintenance.insert(0, disclosure(title, [paragraph(history_result.get("at", ""), **{"class": "p115-hint mb-3"}),
            *[paragraph(item, **{"class": "p115-copy mb-2"}) for item in history_result.get("items", [])]], opened=True))

    help_items = [
        ("整理记录", "等待时显示失败；成功后更新同一条记录。本地文件保留到整理完成。"),
        ("重试规则", "次数不含首次，0 表示只尝试首次。间隔从每轮结束后计时，实际有 ±10% 浮动。"),
        ("通知设置", "在 MP 通知渠道中开启「整理入库」和「手动处理」。中间重试不重复推送；成功与最终失败沿用 MP 原生通知。"),
        ("支持范围", "MP V2.15.6，本地到内置 115 的复制或移动。字幕、NFO、图片沿用 MP；蓝光原盘目录暂不支持。"),
        ("目标文件", "直接写入正式目录和文件名，覆盖及旧版本清理遵循 MP 设置。不会创建暂存或备份目录。"),
    ]
    content.extend([
        strategy,
        disclosure(f"批量任务操作 · {selectable} 个可选", batch_children, "mdi-playlist-check"),
        disclosure("视频格式", [field("VTextarea", "extensions", "接管的视频后缀", "英文逗号分隔，不加点号，例如 mkv,mp4,iso；未列出的格式走 MP 原流程。",
                    rows=2, spellcheck=False, **{"auto-grow": True})], "mdi-file-video-outline"),
        disclosure("清理已结束记录", maintenance, "mdi-broom"),
        disclosure("使用说明与通知设置", [node("ul", content=[node("li", content=[node("h3", title),
                   paragraph(text, **{"class": "p115-copy p115-muted mt-1"})]) for title, text in help_items],
                   **{"class": "p115-help-list"})], "mdi-help-circle-outline"),
        paragraph("修改设置后，点击 MoviePilot 配置窗口底部的「保存」生效。", **{"class": "p115-footer"}),
    ])
    return [node("VForm", content=content, **{"class": "p115-ui p115-settings"})]


def tone_style(tone, background=False):
    # Darken/lighten semantic text towards theme ink for contrast in both themes.
    style = INK + f"color:color-mix(in srgb,rgb(var(--v-theme-{tone})) 35%,rgb(var(--v-theme-on-surface)))!important;"
    if background:
        style += f"background:rgba(var(--v-theme-{tone}),.12);"
    return style


def status_chip(state, label=None):
    tone, name = TONES.get(state, ("secondary", "mdi-circle-outline"))
    return node("VChip", content=[icon(name, size=16, **{"class": "mr-1"}),
                                 node("span", label or LABELS.get(state, state))],
                variant="flat", size="small", **{"style": tone_style(tone, True) + "font-weight:600"})


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
    button = node("VBtn", label, variant="tonal" if action in ("resume", "upload") else "outlined",
                  color="error" if action == "cancel" else "primary", size="small",
                  **{"aria-label": f"{label}：{filename(task['source'])}",
                     "style": tone_style("error" if action == "cancel" else "primary")})
    button["events"] = {"click": {"api": f"plugin/P115InstantWait/tasks/{task['id']}/{action}", "method": "POST"}}
    return button


def confirm_action(task, action, label, explanation, confirm_label):
    return node("details", content=[node("summary", label),
        box([paragraph(explanation, **{"class": "p115-copy p115-muted mb-3"}),
             task_button(task, action, confirm_label)], "p115-confirm-body")],
        **{"class": "p115-confirm"})


def task_row(task, enabled, now, max_retries):
    status = task["state"]
    label, value, tone = schedule_summary(task, enabled, now)
    auto = task.get("auto_attempts", task["attempts"])
    budget = (f"本轮自动尝试 {auto} / {max_retries + 1} 次"
              if max_retries is not None and status in ACTIVE else f"自动尝试 {auto} 次")
    summary = node("summary", content=[
        box([icon("mdi-file-video-outline", size=22)], "p115-task-icon"),
        box([node("div", filename(task["source"]), **{"class": "p115-task-name"}),
             box([status_chip(status), node("span", f"累计 {task['attempts']} 次"),
                  node("span", f"记录 #{task['history_id']}" if task['history_id'] else "整理记录待生成")],
                 "p115-task-meta")], "p115-task-main"),
        box([node("div", label, **{"class": "p115-schedule-label"}),
             node("div", value, **{"class": "p115-schedule-value", "style": tone_style(tone)})], "p115-schedule"),
    ])
    details = [box([node("div", "暂停原因" if status == "paused" else "最新结果",
                       **{"class": "p115-result-label", "style": tone_style(TONES.get(status, ("secondary", ""))[0])}),
                    paragraph(latest_result(task).removeprefix("暂停原因：") if status == "paused"
                              else latest_result(task))], "p115-result")]
    if status in ("queued", "waiting", "paused", "upload_queued") and enabled:
        actions = [task_button(task, "resume", "继续等待秒传") if status == "paused"
                   else task_button(task, "pause", "暂停重试")]
        if status != "upload_queued":
            actions.append(confirm_action(task, "upload", "强制上传…",
                "先尝试秒传，未命中就上传视频正文，会占用上行带宽。", "确认强制上传"))
        actions.append(confirm_action(task, "cancel", "取消任务…",
            "停止此任务，不再自动尝试。保留本地及网盘文件；需要重新处理时，请在 MP 再次发起整理。", "确认取消任务"))
        details.append(box(actions, "p115-actions"))
        details.append(paragraph("继续等待会重置次数和时限。" if status == "paused" else
                                 "强制上传和取消任务需展开后确认。", **{"class": "p115-hint"}))
    elif status in ("running", "uploading"):
        details.append(note("本轮执行中，暂不可操作；结束后刷新查看结果。", "mdi-sync"))
    elif status in ACTIVE and not enabled:
        details.append(note("启用插件后才能继续处理此任务。", "mdi-pause-circle-outline"))
    paths = [paragraph(budget, **{"class": "p115-copy p115-muted mb-3"})]
    for title, path in (("本地源文件", task["source"]), ("115 目标文件", task["target"]),
                        ("任务 ID · 可在 MP 日志中搜索", task["id"])):
        paths.extend([node("div", title, **{"class": "p115-muted"}), node("code", str(path), **{"class": "p115-path"})])
    paths.append(box([
        node("span", "入队时间：" + record_time(task.get("created"))),
        node("span", ("完成时间：" if status == "completed" else "结束时间：" if status in ("failed", "cancelled") else "更新时间：") +
                     record_time(task.get("updated"))),
    ], "p115-times"))
    if task.get("backup_files"):
        paths.append(paragraph(f"旧版本备份 {len(task['backup_files'])} 个，位于目标目录 .mp115-backups。"))
    details.append(disclosure("路径、时间与日志定位", paths, "mdi-folder-outline",
                              **{"class": "p115-disclosure mt-4"}))
    return node("details", content=[summary, box(details, "p115-task-body")],
                **{"class": "p115-task", "open": status in ("paused", "failed")})


def task_page(tasks, enabled, error="", max_retries=None):
    now = time.time()
    refresh = node("VBtn", "刷新队列", variant="outlined", size="small",
                   **{"prepend-icon": "mdi-refresh", "style": "min-height:40px", "aria-label": "刷新任务状态"})
    # PageRender uses MP's authenticated API and emits action to reload the page.
    refresh["events"] = {"click": {"api": "plugin/P115InstantWait/tasks", "method": "GET"}}
    active_count = sum(t["state"] in ACTIVE for t in tasks)
    content = [node("style", STYLES), box([
        box([box([node("h2", "等待队列"),
                  status_chip("running" if enabled else "cancelled", "后台运行中" if enabled else "插件已关闭")], "p115-heading"),
             paragraph(f"最近 {len(tasks)} 条任务 · {active_count} 条未结束。" if tasks else "把等待留给后台，把结果带回原整理记录。",
                       **{"class": "p115-copy p115-muted"})]), refresh,
    ], "p115-header")]
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-5", "role": "alert"}))
    if not enabled:
        content.append(node("VAlert", "请在配置页启用插件；关闭期间队列不会自动重试。",
                            type="info", variant="tonal", **{"class": "mb-5"}))

    attention = [t for t in tasks if t["state"] in ("paused", "failed")]
    pending = [t for t in tasks if t["state"] in ACTIVE and t["state"] != "paused"]
    ended = [t for t in tasks if t["state"] not in ACTIVE and t["state"] != "failed"]
    if tasks:
        content.append(box([
            status_chip("paused", f"待处理 {len(attention)}"),
            status_chip("waiting", f"进行中 {len(pending)}"),
            status_chip("completed", f"已结束 {len(ended)}"),
        ], "p115-toolbar", **{"aria-label": "最近任务状态汇总"}))
    for title, description, group in (
        ("需要你处理", "暂停任务可以继续等待或强制上传；失败任务请根据原因在 MP 重新整理。", attention),
        ("正在等待与执行", "后台按计划依次处理。展开文件查看结果和操作。", pending),
    ):
        if not group:
            continue
        # In-flight jobs first, then due jobs. Keep failures' supplied recency order.
        if group is pending:
            group = sorted(group, key=lambda t: (t["state"] not in ("running", "uploading"), t.get("next_at") or 0))
        content.append(node("section", content=[
            box([box([node("h3", title), node("span", str(len(group)), **{"class": "p115-count"})], "p115-section-label")], "p115-section-head"),
            paragraph(description, **{"class": "p115-copy p115-muted mb-3"}),
            box([task_row(t, enabled, now, max_retries) for t in group], "p115-list"),
        ], **{"class": "p115-group"}))
    if ended:
        content.append(disclosure(f"已结束记录 · {len(ended)}", [
            paragraph("整理成功与已取消的任务。清理列表记录请到配置页，不影响 MP 整理历史。",
                      **{"class": "p115-copy p115-muted mb-3"}),
            box([task_row(t, enabled, now, max_retries) for t in ended], "p115-list"),
        ], "mdi-history"))
    if not tasks:
        content.append(box([
            icon("mdi-cloud-clock-outline", size=36, color="primary"),
            node("h3", "等待你的第一个视频"),
            paragraph("确认 MP 内置 115 已授权，在配置页启用插件，再发起本地到 115 的视频整理。任务会自动出现在这里。",
                      **{"class": "p115-copy p115-muted"}),
        ], "p115-empty"))
    elif not pending and not attention:
        content.insert(-1, note("当前没有等待中的任务，已结束记录收在下方。", "mdi-check-circle-outline"))
    content.append(paragraph("更新于 " + datetime.fromtimestamp(now).astimezone().strftime("%m-%d %H:%M:%S %z") +
                             " · 时间按 MP 所在时区显示；点击刷新获取最新状态。",
                             **{"class": "p115-footer"}))
    return [node("VContainer", content=content, **{"class": "p115-ui pa-0"})]
