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
         "paused": ("error", "mdi-pause-circle-outline"), "running": ("primary", "mdi-sync"),
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
.p115-ui{--p115-line:rgba(var(--v-theme-on-surface),.12);--p115-muted:rgba(var(--v-theme-on-surface),.74);color:rgb(var(--v-theme-on-surface));font-size:14px;line-height:1.55;max-width:100%;container-type:inline-size}
.p115-ui *{box-sizing:border-box}
.p115-ui h2,.p115-ui h3,.p115-ui p{margin:0}
.p115-ui h2{font-size:22px;line-height:1.4;font-weight:700;letter-spacing:-.02em}
.p115-ui h3{font-size:15px;font-weight:650}
.p115-ui .p115-muted{color:var(--p115-muted);opacity:1}
.p115-ui .p115-copy{overflow-wrap:anywhere}
.p115-ui .p115-header{display:flex;align-items:center;gap:14px;margin-bottom:20px}
.p115-ui .p115-header-main{flex:1;min-width:0}
.p115-ui .p115-brand-icon{display:flex;align-items:center;justify-content:center;width:44px;height:44px;border-radius:12px;background:rgba(var(--v-theme-primary),.1);color:rgb(var(--v-theme-primary));flex-shrink:0}
.p115-ui .p115-eyebrow{font-size:12px;color:var(--p115-muted);margin-bottom:2px;letter-spacing:.04em}
.p115-ui .p115-heading{display:flex;align-items:center;flex-wrap:wrap;gap:10px}
.p115-ui .p115-section{padding:20px 0;border-top:1px solid var(--p115-line)}
.p115-ui .p115-section-head{display:flex;align-items:center;gap:8px;margin-bottom:8px}
.p115-ui .p115-switches{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-bottom:20px}
.p115-ui .p115-toggle{display:flex;align-items:center;justify-content:space-between;gap:12px;min-width:0;padding:14px 16px;border:1px solid var(--p115-line);border-radius:10px}
.p115-ui .p115-toggle label{font-size:14px;font-weight:650;cursor:pointer}
.p115-ui .p115-switches .v-switch{flex:0 0 auto}
.p115-ui .p115-fields{display:grid;grid-template-columns:repeat(2,minmax(0,320px));gap:18px 20px}
.p115-ui .p115-field{min-width:0}
.p115-ui.p115-settings .p115-field>.v-input{max-width:640px}
.p115-ui.p115-settings .p115-field>.v-input:has(input[type=number]){max-width:160px}
.p115-ui.p115-settings .p115-field>.v-input:has(#p115wait-retry_intervals){max-width:320px}
.p115-ui.p115-settings .p115-field>.v-select:not(.v-autocomplete){max-width:280px}
.p115-ui .p115-field-label{display:block;font-weight:600;margin-bottom:6px}
.p115-ui .p115-hint{font-size:12px;color:var(--p115-muted);margin-top:6px;line-height:1.6;overflow-wrap:anywhere}
.p115-ui .v-field__input{opacity:1}
.p115-ui input::placeholder,.p115-ui textarea::placeholder{color:var(--p115-muted);opacity:1}
.p115-ui .p115-note{display:flex;align-items:flex-start;gap:10px;padding:12px 14px;background:rgba(var(--v-theme-on-surface),.04);border-radius:8px;margin-top:14px;font-size:13px}
.p115-ui .p115-note .v-icon{flex-shrink:0;margin-top:2px}
.p115-ui .p115-disclosure{border-top:1px solid var(--p115-line)}
.p115-ui .p115-disclosure>summary{display:flex;align-items:center;gap:8px;min-height:48px;padding:12px 0;cursor:pointer;list-style:none;font-weight:600;font-size:13px}
.p115-ui summary::-webkit-details-marker{display:none}
.p115-ui .p115-disclosure>summary::after{content:'›';font-size:22px;line-height:1;flex-shrink:0;margin-left:auto;transition:transform .18s ease-out}
.p115-ui details[open]>summary::after{transform:rotate(90deg)}
.p115-ui .p115-disclosure-body{padding:0 0 18px}
.p115-ui summary:focus-visible,.p115-ui button:focus-visible,.p115-ui input:focus-visible{outline:2px solid rgb(var(--v-theme-primary));outline-offset:3px;border-radius:5px}
.p115-ui .p115-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:18px}
.p115-ui .p115-metric{padding:13px 14px;border:1px solid var(--p115-line);border-radius:10px;min-width:0}
.p115-ui .p115-metric-label{display:flex;align-items:center;justify-content:space-between;gap:6px;color:var(--p115-muted);font-size:12px}
.p115-ui .p115-metric-value{font-size:28px;line-height:1.35;font-weight:700;font-variant-numeric:tabular-nums;margin:3px 0}
.p115-ui .p115-metric-caption{font-size:12px;color:var(--p115-muted);overflow-wrap:anywhere}
.p115-ui .p115-metric-attention{background:rgba(var(--v-theme-error),.035);border-color:rgba(var(--v-theme-error),.25)}
.p115-ui .p115-toolbar{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:12px}
.p115-ui .p115-filters{display:flex;flex-wrap:wrap;gap:3px;border:1px solid var(--p115-line);background:rgba(var(--v-theme-on-surface),.03);padding:3px;border-radius:9px}
.p115-ui .p115-filter{position:relative;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:5px;padding:7px 10px;border-radius:6px;font-size:13px;min-height:36px;color:var(--p115-muted)}
.p115-ui .p115-filter input{position:absolute;opacity:0;width:1px;height:1px}
.p115-ui .p115-filter:has(input:checked){color:rgb(var(--v-theme-on-surface));background:rgb(var(--v-theme-surface));box-shadow:0 1px 3px rgba(0,0,0,.08);font-weight:600}
.p115-ui .p115-filter:has(input:focus-visible){outline:2px solid rgb(var(--v-theme-primary));outline-offset:2px}
.p115-ui .p115-count{font-size:12px;font-variant-numeric:tabular-nums;opacity:.8}
.p115-ui .p115-list{display:grid;gap:8px}
.p115-ui .p115-task{position:relative;border:1px solid var(--p115-line);border-radius:10px;padding:12px 14px;min-width:0}
.p115-ui .p115-task[data-category~=attention]{border-color:rgba(var(--v-theme-error),.24);background:rgba(var(--v-theme-error),.035)}
.p115-ui .p115-row{display:flex;align-items:center;gap:12px;min-width:0;padding-right:36px}
.p115-ui .p115-task-icon{display:flex;align-items:center;justify-content:center;width:34px;height:38px;border-radius:8px;background:rgba(var(--v-theme-on-surface),.04);flex-shrink:0}
.p115-ui .p115-task-main{min-width:0;flex:1}
.p115-ui .p115-task-name{font-weight:650;font-size:14px;line-height:1.5;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;user-select:text}
.p115-ui .p115-task-meta{display:flex;align-items:center;flex-wrap:wrap;gap:4px 10px;margin-top:5px;font-size:12px;color:var(--p115-muted);font-variant-numeric:tabular-nums}
.p115-ui .p115-task-context{font-size:12px;margin-top:5px;color:var(--p115-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.p115-ui .p115-row-actions{display:flex;align-items:flex-start;flex-wrap:wrap;gap:6px;flex-shrink:0;max-width:270px}
.p115-ui .v-btn{letter-spacing:0}
.p115-ui .p115-row-actions .v-btn{min-height:36px}
.p115-ui .p115-confirm{min-width:0;font-size:13px}
.p115-ui .p115-confirm>summary{display:flex;align-items:center;justify-content:center;min-height:36px;border:1px solid var(--p115-line);border-radius:6px;padding:6px 12px;cursor:pointer;list-style:none;font-weight:500}
.p115-ui .p115-confirm-upload>summary{color:color-mix(in srgb,rgb(var(--v-theme-primary)) 35%,rgb(var(--v-theme-on-surface)));background:rgba(var(--v-theme-primary),.08);border-color:rgba(var(--v-theme-primary),.2)}
.p115-ui .p115-confirm[open]{flex-basis:100%}
.p115-ui .p115-confirm-body{padding:10px 0;max-width:270px;font-size:13px;overflow-wrap:anywhere}
.p115-ui .p115-task-details>summary{position:absolute;right:8px;top:15px;display:flex;align-items:center;justify-content:center;gap:5px;width:36px;min-height:36px;font-size:12px;color:var(--p115-muted);cursor:pointer;list-style:none}
.p115-ui .p115-task-details>summary span{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden;white-space:nowrap}
.p115-ui .p115-task-details>summary .v-icon{transition:transform .18s ease-out}
.p115-ui .p115-task-details[open]>summary .v-icon{transform:rotate(180deg)}
.p115-ui .p115-task-body{border-top:1px solid var(--p115-line);padding-top:12px;margin-top:12px}
.p115-ui .p115-result{padding:10px 12px;border-radius:6px;background:rgba(var(--v-theme-on-surface),.04);overflow-wrap:anywhere;margin-bottom:12px}
.p115-ui .p115-result-label{font-size:12px;font-weight:650;margin-bottom:4px}
.p115-ui .p115-path{display:block;font-family:ui-monospace,Consolas,monospace;font-size:12px;line-height:1.7;overflow-wrap:anywhere;white-space:pre-wrap;user-select:text;margin:4px 0 12px}
.p115-ui .p115-times{display:flex;flex-wrap:wrap;gap:4px 20px;font-size:12px;color:var(--p115-muted);font-variant-numeric:tabular-nums;margin:12px 0}
.p115-ui .p115-empty{text-align:center;padding:32px 18px;border:1px dashed var(--p115-line);border-radius:10px}
.p115-ui .p115-empty h3{margin:10px 0 6px}
.p115-ui .p115-filter-empty{display:none}
.p115-ui:has(input[value=waiting]:checked) .p115-task:not([data-category~=waiting]),.p115-ui:has(input[value=attention]:checked) .p115-task:not([data-category~=attention]),.p115-ui:has(input[value=transfer]:checked) .p115-task:not([data-category~=transfer]),.p115-ui:has(input[value=ended]:checked) .p115-task:not([data-category~=ended]){display:none}
.p115-ui:has(input[value=waiting]:checked) .p115-filter-empty[data-filter=waiting],.p115-ui:has(input[value=attention]:checked) .p115-filter-empty[data-filter=attention],.p115-ui:has(input[value=transfer]:checked) .p115-filter-empty[data-filter=transfer],.p115-ui:has(input[value=ended]:checked) .p115-filter-empty[data-filter=ended]{display:block}
.p115-ui .p115-bulk-actions{display:flex;align-items:flex-start;flex-wrap:wrap;gap:12px;margin-top:12px}
.p115-ui .p115-bulk-actions .p115-confirm{flex:1 1 220px}
.p115-ui .p115-bulk-actions .p115-confirm-body{max-width:100%}
.p115-ui .p115-help-list{display:grid;gap:12px;margin:0;padding-left:20px}
.p115-ui .p115-footer{font-size:12px;color:var(--p115-muted);margin-top:16px;overflow-wrap:anywhere}
@media(hover:hover) and (pointer:fine){.p115-ui .p115-filter:hover,.p115-ui .p115-confirm>summary:hover{background:rgba(var(--v-theme-primary),.07)}.p115-ui .p115-task-details>summary:hover{color:rgb(var(--v-theme-primary))}}
@container (max-width:700px){.p115-ui .p115-row{flex-wrap:wrap;gap:10px}.p115-ui .p115-task-main{flex-basis:calc(100% - 50px)}.p115-ui .p115-row-actions{max-width:100%;margin-left:46px}.p115-ui .p115-task-context{white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}.p115-ui .p115-row-actions .p115-confirm-body{max-width:46ch}}
@container (max-width:480px){.p115-ui .p115-metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.p115-ui .p115-metric{padding:10px 12px}.p115-ui .p115-fields,.p115-ui .p115-switches{grid-template-columns:1fr}.p115-ui .p115-header{gap:10px;flex-wrap:wrap}.p115-ui .p115-header-main{flex-basis:calc(100% - 56px)}.p115-ui .p115-heading .v-chip{margin-top:4px}.p115-ui h2{font-size:20px}.p115-ui .p115-task{padding:12px}.p115-ui .p115-task-icon{display:none}.p115-ui .p115-task-main{flex-basis:100%}.p115-ui .p115-task-name{white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow-wrap:anywhere}.p115-ui .p115-row-actions{margin-left:0}.p115-ui .p115-filter,.p115-ui .p115-row-actions .v-btn,.p115-ui .p115-confirm>summary,.p115-ui .p115-task-details>summary{min-height:44px}.p115-ui .p115-task-meta{gap:6px 10px}}
@container (max-width:480px){.p115-ui .p115-row{padding-right:0}.p115-ui .p115-task-details{margin-top:6px}.p115-ui .p115-task-details>summary{position:static;width:fit-content}.p115-ui .p115-task-details>summary span{position:static;width:auto;height:auto;clip-path:none;overflow:visible;white-space:normal}}
@media(prefers-reduced-motion:reduce){.p115-ui summary,.p115-ui summary::after,.p115-ui summary .v-icon{transition:none!important}}
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
        box([box([icon("mdi-cloud-clock-outline", size=26)], "p115-brand-icon"),
             box([paragraph("115 秒传等待 / 参数设置", **{"class": "p115-eyebrow"}),
                  node("h2", "设置等待的节奏"),
                  paragraph("常用设置集中在这里，任务操作可在监控页直接完成。",
                            **{"class": "p115-copy p115-muted mt-2"})], "p115-header-main")], "p115-header"),
        box([toggle("enabled", "启用秒传等待", "后台处理视频，其他整理照常进行。"),
             toggle("notify", "推送任务通知", "首次等待、达到上限或异常时通知。")], "p115-switches")]
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-5", "role": "alert"}))

    strategy = section("重试与上限策略", "次数或等待时限任一达到，即执行所选策略。", [
        box([
            field("VTextField", "max_retries", "最多自动重试", "不含首次尝试；默认 3 次，共尝试 4 次。",
                  type="number", suffix="次", min=0, max=100, step=1, inputmode="numeric"),
            field("VTextField", "max_wait_hours", "最长等待时间", "0 表示不限时，仍受重试次数限制。",
                  type="number", suffix="小时", min=0, max=8760, step=1, inputmode="decimal"),
            field("VTextField", "retry_intervals", "重试间隔序列", "英文逗号分隔，每项 30～86400 秒；最后一项可重复使用。",
                       placeholder="60,180,600,1800", suffix="秒", spellcheck=False),
            field("VSelect", "limit_action", "达到上限后", "修改策略不会自动恢复已暂停的任务。",
                       items=[{"title": "暂停，等待我处理", "value": "manual"},
                              {"title": "自动强制上传", "value": "upload"}]),
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
        disclosure("视频格式", [field("VTextarea", "extensions", "接管的视频后缀", "英文逗号分隔，不加点号，例如 mkv,mp4,iso；未列出的格式走 MP 原流程。",
                    rows=2, spellcheck=False, **{"auto-grow": True})], "mdi-file-video-outline"),
        disclosure(f"批量任务操作 · {selectable} 个可选", batch_children, "mdi-playlist-check"),
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


def record_time(value, compact=False):
    if value is None:
        return "未记录"
    try:
        return datetime.fromtimestamp(value).astimezone().strftime("%m-%d %H:%M" if compact else "%Y-%m-%d %H:%M:%S %z")
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
    result = confirmation(label, explanation, task_button(task, action, confirm_label))
    if action == "upload":
        result["props"]["class"] += " p115-confirm-upload"
    return result


def confirmation(label, explanation, button):
    return node("details", content=[node("summary", label),
        box([paragraph(explanation, **{"class": "p115-copy p115-muted mb-3"}), button],
            "p115-confirm-body")], **{"class": "p115-confirm"})


def categories(task):
    state = task["state"]
    if state in ("queued", "waiting"):
        return "waiting"
    if state in ("running", "upload_queued", "uploading"):
        return "transfer"
    if state == "failed":
        return "attention ended"
    return "attention" if state == "paused" else "ended"


def task_row(task, enabled, now, max_retries):
    status = task["state"]
    label, value, tone = schedule_summary(task, enabled, now)
    auto = task.get("auto_attempts", task["attempts"])
    budget = (f"自动尝试 {auto}/{max_retries + 1} 次"
              if max_retries is not None and status in ACTIVE else f"自动尝试 {auto} 次")
    result = latest_result(task)
    meta = [status_chip(status), node("span", f"#{task['history_id']}" if task['history_id'] else "记录待生成"),
            node("span", budget)]
    if status in ACTIVE:
        meta.append(node("span", value, **{"style": tone_style(tone), "title": label}))
    else:
        meta.append(node("span", "结束 " + record_time(task.get("updated"), compact=True),
                         **{"title": record_time(task.get("updated"))}))
    main = [node("div", filename(task["source"]), **{"class": "p115-task-name", "title": filename(task["source"])}),
            box(meta, "p115-task-meta")]
    if status in ("paused", "failed", "completed"):
        main.append(paragraph(result, **{"class": "p115-task-context", "title": result}))
    actions = []
    if status in ("queued", "waiting", "paused", "upload_queued") and enabled:
        actions.append(task_button(task, "resume", "继续等待") if status == "paused"
                       else task_button(task, "pause", "暂停"))
        if status != "upload_queued":
            actions.append(confirm_action(task, "upload", "强制上传…",
                "先尝试秒传，未命中就上传视频正文，会占用上行带宽。", "确认强制上传"))
    row = [box([icon(TONES.get(status, ("secondary", "mdi-file-video-outline"))[1], size=21,
                          style=tone_style(TONES.get(status, ("secondary", ""))[0]))], "p115-task-icon"),
           box(main, "p115-task-main")]
    if actions:
        row.append(box(actions, "p115-row-actions", **{"aria-label": "任务操作"}))
    details = [box([node("div", "暂停原因" if status == "paused" else "最新结果",
                          **{"class": "p115-result-label"}), paragraph(result)], "p115-result"),
               paragraph(f"{budget} · 累计尝试 {task['attempts']} 次", **{"class": "p115-hint mb-3"})]
    if status in ("running", "uploading"):
        details.append(paragraph("本轮执行中，结束后刷新查看结果。", **{"class": "p115-hint mb-3"}))
    elif status in ACTIVE and not enabled:
        details.append(paragraph("请在配置页启用插件后继续处理。", **{"class": "p115-hint mb-3"}))
    if status == "failed":
        details.append(paragraph("请根据失败原因，在 MP 重新发起整理。", **{"class": "p115-hint mb-3"}))
    for title, path in (("本地源文件", task["source"]), ("115 目标文件", task["target"]),
                        ("任务 ID · 可在 MP 日志中搜索", task["id"])):
        details.extend([node("div", title, **{"class": "p115-muted"}),
                        node("code", str(path), **{"class": "p115-path"})])
    details.append(box([node("span", "入队时间：" + record_time(task.get("created"))),
                        node("span", ("完成时间：" if status == "completed" else "结束时间：" if status in ("failed", "cancelled") else "更新时间：") +
                             record_time(task.get("updated")))], "p115-times"))
    if task.get("backup_files"):
        details.append(paragraph(f"旧版本备份 {len(task['backup_files'])} 个，位于目标目录 .mp115-backups。"))
    if status in ("queued", "waiting", "paused", "upload_queued") and enabled:
        details.append(confirm_action(task, "cancel", "取消任务…",
            "停止此任务，不再自动尝试。保留本地及网盘文件；需要重新处理时，请在 MP 再次发起整理。", "确认取消任务"))
    if status == "paused":
        details.append(paragraph("继续等待会重置自动尝试次数和等待时限。", **{"class": "p115-hint"}))
    return box([box(row, "p115-row"), node("details", content=[
        node("summary", content=[icon("mdi-chevron-down", size=16), node("span", "详情与路径")]),
        box(details, "p115-task-body")], **{"class": "p115-task-details"})],
        "p115-task", role="listitem", **{"data-category": categories(task)})


def metric(title, count, caption, tone, name, attention=False):
    return box([
        box([node("span", title), icon(name, size=17, style=tone_style(tone))], "p115-metric-label"),
        node("div", str(count), **{"class": "p115-metric-value", "style": tone_style(tone)}),
        paragraph(caption, **{"class": "p115-metric-caption"}),
    ], "p115-metric" + (" p115-metric-attention" if attention and count else ""))


def batch_feedback(result):
    names = {"upload": "强制上传", "resume": "继续等待", "pause": "暂停", "cancel": "取消任务"}
    title = (f"上次{names.get(result['action'], '批量操作')}：接受 {result['accepted']} · "
             f"跳过 {result['skipped']} · 异常 {result['failed']}")
    return disclosure(title, [
        paragraph(result.get("at", "") + " · 已接受表示操作已提交，不代表传输完成。", **{"class": "p115-hint mb-3"}),
        *[paragraph(f"{item['name']}：{item['message']}", **{"class": "p115-hint"}) for item in result.get("items", [])],
    ], "mdi-playlist-check", opened=bool(result["failed"] or result["skipped"]))


def bulk_tools(tasks, enabled):
    paused = [t["id"] for t in tasks if t["state"] == "paused"]
    uploadable = [t["id"] for t in tasks if t["state"] in ("queued", "waiting", "paused")]
    controls = []
    for action, keys, title, explanation in (
        ("resume", paused, "批量继续等待", "重置这些暂停任务的自动尝试次数和等待时限，继续只尝试秒传。请先确认暂停原因已处理。"),
        ("upload", uploadable, "批量强制上传", "这些等待或暂停任务将先试秒传，未命中就上传正文，占用上行带宽；依次进入现有上传队列。"),
    ):
        button = node("VBtn", f"确认{title} {len(keys)} 项", variant="tonal", color="primary", size="small",
                      style=tone_style("primary"))
        button["events"] = {"click": {"api": f"plugin/P115InstantWait/batch/{action}",
                                       "method": "POST", "params": {"keys": keys}}}
        if keys and enabled:
            controls.append(confirmation(f"{title} · {len(keys)} 项…", explanation, button))
    children = [paragraph("作用于本页最近 200 条记录中的适用任务，不随状态筛选变化。提交时会检查最新状态；不适用项跳过，新入队任务不包含在内。",
                          **{"class": "p115-hint"})]
    if controls:
        children.append(box(controls, "p115-bulk-actions"))
    else:
        children.append(paragraph("暂无可批量操作的任务。" if enabled else "启用插件后可批量处理。", **{"class": "p115-hint"}))
    children.append(paragraph("指定任务的多选、暂停和取消，以及历史记录清理，请在配置页展开对应选项。", **{"class": "p115-hint"}))
    return disclosure("批量操作与记录管理", children, "mdi-playlist-edit")


def task_page(tasks, enabled, error="", max_retries=None, batch_result=None):
    now = time.time()
    refresh = node("VBtn", "刷新", variant="outlined", size="small",
                   **{"prepend-icon": "mdi-refresh", "style": "min-height:40px", "aria-label": "刷新任务状态"})
    # PageRender uses MP's authenticated API and reloads after each action.
    refresh["events"] = {"click": {"api": "plugin/P115InstantWait/tasks", "method": "GET"}}
    content = [node("style", STYLES), box([
        box([icon("mdi-cloud-clock-outline", size=26)], "p115-brand-icon"),
        box([paragraph("115 秒传等待 / 任务监控", **{"class": "p115-eyebrow"}),
             box([node("h2", "任务监控"),
                  status_chip("running" if enabled else "cancelled", "后台运行中" if enabled else "插件已关闭")], "p115-heading")],
            "p115-header-main"), refresh,
    ], "p115-header")]
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-4", "role": "alert"}))
    if not enabled:
        content.append(node("VAlert", "请在配置页启用插件；关闭期间队列不会自动重试。",
                            type="info", variant="tonal", **{"class": "mb-4"}))
    groups = {group: [t for t in tasks if group in categories(t).split()]
              for group in ("waiting", "transfer", "attention", "ended")}
    completed = [t for t in tasks if t["state"] == "completed"]
    waiting = groups["waiting"]
    if waiting and enabled:
        next_at = min(t.get("next_at") or 0 for t in waiting)
        wait_caption = "已到时间，等待调度" if next_at <= now else "下次 " + datetime.fromtimestamp(next_at).strftime("%H:%M:%S")
    else:
        wait_caption = "启用后继续调度" if waiting else "暂无等待任务"
    instant = sum(t.get("transfer_method") == "instant" for t in completed)
    upload = sum(t.get("transfer_method") == "upload" for t in completed)
    content.append(box([
        metric("等待秒传", len(waiting), wait_caption, "info", "mdi-clock-outline"),
        metric("执行与上传", len(groups["transfer"]), f"执行中 {sum(t['state'] in ('running', 'uploading') for t in tasks)} · 上传排队 {sum(t['state'] == 'upload_queued' for t in tasks)}", "primary", "mdi-cloud-upload-outline"),
        metric("需要处理", len(groups["attention"]), f"暂停 {sum(t['state'] == 'paused' for t in tasks)} · 失败 {sum(t['state'] == 'failed' for t in tasks)}", "error", "mdi-alert-circle-outline", True),
        metric("整理完成", len(completed), f"秒传 {instant} · 上传 {upload}" + (f" · 未记录 {len(completed)-instant-upload}" if len(completed) > instant+upload else ""), "success", "mdi-check-circle-outline"),
    ], "p115-metrics", **{"aria-label": "最近任务状态概览"}))
    filters = []
    for value, title, count in [("all", "全部", len(tasks)), ("waiting", "等待", len(waiting)),
                                 ("attention", "待处理", len(groups["attention"])),
                                 ("transfer", "执行/上传", len(groups["transfer"])),
                                 ("ended", "已结束", len(groups["ended"]))]:
        # Native radios + scoped CSS work in PageRender without reactive bindings.
        filters.append(node("label", content=[
            node("input", type="radio", name="p115-task-filter", value=value, checked=value == "all",
                 **{"aria-label": f"{title} {count} 条任务"}),
            node("span", title), node("span", str(count), **{"class": "p115-count"})],
            **{"class": "p115-filter"}))
    content.append(box([box(filters, "p115-filters", role="radiogroup", **{"aria-label": "筛选任务状态"}),
                        node("span", f"最近 {len(tasks)} 条", **{"class": "p115-hint"})], "p115-toolbar"))
    if batch_result:
        content.append(batch_feedback(batch_result))
    if tasks:
        rank = {"paused": 0, "failed": 1, "running": 2, "uploading": 2, "upload_queued": 3, "queued": 4, "waiting": 4}
        ordered = sorted(tasks, key=lambda t: (rank.get(t["state"], 5),
                         t.get("next_at") or 0 if t["state"] in ("waiting", "queued") else -(t.get("updated") or 0)))
        content.append(box([task_row(t, enabled, now, max_retries) for t in ordered], "p115-list", role="list",
                           **{"aria-label": "任务列表"}))
        empty_copy = {"waiting": "暂无等待任务", "attention": "没有需要处理的任务", "transfer": "暂无执行或上传任务", "ended": "暂无已结束记录"}
        for group, title in empty_copy.items():
            if not groups[group]:
                content.append(box([icon("mdi-check-circle-outline", size=28), node("h3", title),
                    paragraph("切换到其他状态查看任务。", **{"class": "p115-muted"})],
                    "p115-empty p115-filter-empty", **{"data-filter": group}))
    else:
        content.append(box([icon("mdi-cloud-clock-outline", size=32, color="primary"),
            node("h3", "还没有等待任务"),
            paragraph("在配置页启用插件，确认 MP 内置 115 已授权，再发起本地到 115 的视频整理。",
                      **{"class": "p115-copy p115-muted"})], "p115-empty"))
    content.append(bulk_tools(tasks, enabled))
    content.append(paragraph("概览仅统计本页最近 200 条记录 · 更新于 " + datetime.fromtimestamp(now).astimezone().strftime("%m-%d %H:%M:%S %z") +
                             " · 时间按 MP 时区显示，点击刷新更新。", **{"class": "p115-footer"}))
    return [node("VContainer", content=content, **{"class": "p115-ui pa-0"})]
