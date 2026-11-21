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
@keyframes p115-spin{to{transform:rotate(360deg)}}
@keyframes p115-pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.55;transform:scale(.9)}}
@keyframes p115-reveal{from{opacity:0;transform:translateY(-4px)}to{opacity:1;transform:translateY(0)}}
@keyframes p115-shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
.p115-ui{
  --p115-ease:cubic-bezier(.22,1,.36,1);
  --p115-surface:rgb(var(--v-theme-surface));
  --p115-subtle-bg:color-mix(in srgb,rgb(var(--v-theme-on-surface)) 2.6%,rgb(var(--v-theme-surface)));
  --p115-well:color-mix(in srgb,rgb(var(--v-theme-on-surface)) 4.5%,rgb(var(--v-theme-surface)));
  --p115-line:rgba(var(--v-theme-on-surface),.07);
  --p115-line-strong:rgba(var(--v-theme-on-surface),.10);
  --p115-line-hover:rgba(var(--v-theme-on-surface),.18);
  --p115-muted:color-mix(in srgb,rgb(var(--v-theme-on-surface)) 58%,rgb(var(--v-theme-surface)));
  --p115-faint:color-mix(in srgb,rgb(var(--v-theme-on-surface)) 40%,rgb(var(--v-theme-surface)));
  --p115-shadow-sm:0 1px 2px rgba(0,0,0,.035);
  color:rgb(var(--v-theme-on-surface));font-size:14px;line-height:1.55;max-width:100%;container-type:inline-size
}
.p115-ui *{box-sizing:border-box}
.p115-ui h2,.p115-ui h3,.p115-ui p{margin:0}
.p115-ui h2{font-size:18px;line-height:1.35;font-weight:680;letter-spacing:-.02em}
.p115-ui h3{font-size:14.5px;font-weight:660;letter-spacing:-.01em}
.p115-ui .p115-muted{color:var(--p115-muted);opacity:1}
.p115-ui .p115-copy{overflow-wrap:anywhere}
.p115-ui .p115-header{
  display:flex;align-items:center;justify-content:space-between;gap:12px;
  margin-bottom:16px;padding:2px 0 14px;border-bottom:1px solid var(--p115-line)
}
.p115-ui .p115-header-main{flex:1;min-width:0}
.p115-ui .p115-heading{display:flex;align-items:center;flex-wrap:wrap;gap:10px}
.p115-ui .p115-refresh-btn{
  border-radius:8px!important;background:var(--p115-surface)!important;
  border-color:var(--p115-line-strong)!important;box-shadow:var(--p115-shadow-sm);min-height:34px!important
}
.p115-ui .p115-badge{
  display:inline-flex;align-items:center;gap:6px;height:22px;padding:0 8px;border-radius:6px;
  font-size:12px;font-weight:600;line-height:1;background:var(--p115-subtle-bg);
  border:1px solid var(--p115-line-strong);
  color:color-mix(in srgb,rgb(var(--v-theme-on-surface)) 82%,rgb(var(--v-theme-surface)));
  white-space:nowrap
}
.p115-ui .p115-dot{width:6px;height:6px;border-radius:999px;flex-shrink:0;background:var(--p115-faint)}
.p115-ui .p115-badge[data-tone=success] .p115-dot{
  background:color-mix(in srgb,rgb(var(--v-theme-success)) 75%,rgb(var(--v-theme-on-surface)))
}
.p115-ui .p115-badge[data-tone=info] .p115-dot{
  background:color-mix(in srgb,rgb(var(--v-theme-info)) 75%,rgb(var(--v-theme-on-surface)))
}
.p115-ui .p115-badge[data-tone=primary] .p115-dot{
  background:rgb(var(--v-theme-primary));animation:p115-pulse 1.8s ease-in-out infinite
}
.p115-ui .p115-badge[data-tone=warning]{
  background:rgba(var(--v-theme-warning),.08);border-color:rgba(var(--v-theme-warning),.24)
}
.p115-ui .p115-badge[data-tone=warning] .p115-dot{background:rgb(var(--v-theme-warning))}
.p115-ui .p115-badge[data-tone=error]{
  background:rgba(var(--v-theme-error),.06);border-color:rgba(var(--v-theme-error),.22);
  color:color-mix(in srgb,rgb(var(--v-theme-error)) 72%,rgb(var(--v-theme-on-surface)))
}
.p115-ui .p115-badge[data-tone=error] .p115-dot{background:rgb(var(--v-theme-error))}
.p115-ui .p115-section{
  padding:18px 20px;margin-bottom:14px;border:1px solid var(--p115-line-strong);
  border-radius:12px;background:var(--p115-surface);box-shadow:var(--p115-shadow-sm)
}
.p115-ui .p115-section-head{display:flex;align-items:center;gap:8px;margin-bottom:6px}
.p115-ui .p115-switches{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-bottom:16px}
.p115-ui .p115-toggle{
  display:flex;align-items:center;justify-content:space-between;gap:14px;min-width:0;padding:14px 16px;
  border:1px solid var(--p115-line-strong);border-radius:12px;background:var(--p115-surface);
  box-shadow:var(--p115-shadow-sm);transition:border-color .15s ease
}
.p115-ui .p115-toggle:has(input:checked){border-color:var(--p115-line-hover)}
.p115-ui .p115-toggle label{font-size:14px;font-weight:650;cursor:pointer}
.p115-ui .p115-switches .v-switch{flex:0 0 auto}
.p115-ui .p115-fields{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px 18px}
.p115-ui .p115-field{min-width:0}
.p115-ui.p115-settings .p115-field>.v-input{max-width:100%}
.p115-ui .p115-field-label{display:block;font-weight:620;font-size:13px;margin-bottom:6px}
.p115-ui .p115-hint{font-size:12px;color:var(--p115-muted);margin-top:6px;line-height:1.55;overflow-wrap:anywhere}
.p115-ui .v-field{border-radius:9px!important;background:var(--p115-surface)}
.p115-ui .v-field__input{opacity:1}
.p115-ui input::placeholder,.p115-ui textarea::placeholder{color:var(--p115-muted);opacity:1}
.p115-ui .p115-note{
  display:flex;align-items:flex-start;gap:10px;padding:11px 14px;
  background:var(--p115-subtle-bg);border:1px solid var(--p115-line-strong);
  border-radius:10px;margin-top:14px;font-size:13px;color:var(--p115-muted)
}
.p115-ui .p115-note .v-icon{flex-shrink:0;margin-top:2px;color:var(--p115-muted)}
.p115-ui .p115-disclosure{
  border:1px solid var(--p115-line-strong);border-radius:12px;background:var(--p115-surface);
  margin-bottom:12px;box-shadow:var(--p115-shadow-sm);overflow:hidden;transition:border-color .15s ease
}
.p115-ui .p115-disclosure[open]{border-color:var(--p115-line-hover)}
.p115-ui .p115-disclosure>summary{
  display:flex;align-items:center;gap:10px;min-height:48px;padding:11px 16px;
  cursor:pointer;list-style:none;font-weight:640;font-size:13.5px;user-select:none;transition:background .15s ease
}
.p115-ui .p115-disclosure>summary .v-icon{width:18px;height:18px;color:var(--p115-muted);flex-shrink:0}
.p115-ui .p115-disclosure[open]>summary{
  border-bottom:1px solid var(--p115-line);background:var(--p115-subtle-bg)
}
.p115-ui summary::-webkit-details-marker{display:none}
.p115-ui .p115-disclosure>summary::after{
  content:'›';display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;
  border-radius:6px;background:var(--p115-well);color:var(--p115-muted);font-size:17px;line-height:1;
  flex-shrink:0;margin-left:auto;transition:transform .2s cubic-bezier(.2,.8,.2,1),background .15s ease
}
.p115-ui details[open]>summary::after{transform:rotate(90deg);background:var(--p115-well);color:rgb(var(--v-theme-on-surface))}
.p115-ui .p115-disclosure-body{padding:16px 18px 18px}
.p115-ui .p115-disclosure[open]>.p115-disclosure-body,.p115-ui .p115-task-details[open]>.p115-task-body,.p115-ui .p115-confirm[open]>.p115-confirm-body{animation:p115-reveal .18s var(--p115-ease)}
.p115-ui summary:focus-visible,.p115-ui button:focus-visible,.p115-ui input:focus-visible{outline:2px solid rgb(var(--v-theme-primary));outline-offset:2px;border-radius:6px}
.p115-ui .p115-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:14px}
.p115-ui .p115-metric{
  position:relative;padding:14px 16px 12px;border:1px solid var(--p115-line-strong);
  border-radius:12px;min-width:0;background:var(--p115-surface);
  box-shadow:var(--p115-shadow-sm);transition:border-color .15s ease
}
.p115-ui .p115-metric-label{display:flex;align-items:center;justify-content:space-between;gap:8px;color:var(--p115-muted);font-size:12.5px;font-weight:580}
.p115-ui .p115-metric-label .v-icon{color:var(--p115-faint);flex-shrink:0}
.p115-ui .p115-metric-value{
  font-size:26px;line-height:1.2;font-weight:700;letter-spacing:-.025em;
  font-variant-numeric:tabular-nums;margin:6px 0 8px;color:rgb(var(--v-theme-on-surface))
}
.p115-ui .p115-metric-zero{color:var(--p115-muted);font-weight:620}
.p115-ui .p115-metric-caption{
  font-size:12px;color:var(--p115-muted);overflow-wrap:anywhere;
  padding-top:7px;border-top:1px solid var(--p115-line)
}
.p115-ui .p115-metric-attention{border-color:rgba(var(--v-theme-error),.28)}
.p115-ui .p115-metric-attention .p115-metric-value,.p115-ui .p115-metric-attention .p115-metric-label .v-icon{
  color:color-mix(in srgb,rgb(var(--v-theme-error)) 78%,rgb(var(--v-theme-on-surface)))
}
.p115-ui .p115-toolbar{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;margin-bottom:14px}
.p115-ui .p115-filters{
  display:flex;flex-wrap:wrap;gap:4px;border:1px solid var(--p115-line-strong);
  background:var(--p115-well);padding:3px;border-radius:10px
}
.p115-ui .p115-filter{
  position:relative;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:6px;
  padding:5px 11px;border-radius:7px;font-size:12.5px;min-height:32px;color:var(--p115-muted);
  user-select:none;transition:background .15s ease,color .15s ease
}
.p115-ui .p115-filter input{position:absolute;opacity:0;width:1px;height:1px}
.p115-ui .p115-filter:has(input:checked){
  color:rgb(var(--v-theme-on-surface));background:var(--p115-surface);
  box-shadow:0 1px 3px rgba(0,0,0,.08),0 0 0 1px var(--p115-line-strong);font-weight:640
}
.p115-ui .p115-filter:has(input:focus-visible){outline:2px solid rgb(var(--v-theme-primary));outline-offset:2px}
.p115-ui .p115-count{
  display:inline-flex;align-items:center;justify-content:center;min-width:18px;height:18px;
  padding:0 5px;border-radius:999px;background:rgba(var(--v-theme-on-surface),.06);
  font-size:11px;font-weight:600;color:var(--p115-muted);font-variant-numeric:tabular-nums
}
.p115-ui .p115-filter:has(input:checked) .p115-count{
  background:var(--p115-well);color:rgb(var(--v-theme-on-surface))
}
.p115-ui .p115-list{display:grid;gap:8px;margin-bottom:14px}
.p115-ui .p115-task{
  position:relative;border:1px solid var(--p115-line-strong);border-radius:12px;
  padding:12px 16px;min-width:0;background:var(--p115-surface);
  box-shadow:var(--p115-shadow-sm);transition:border-color .15s ease
}
.p115-ui .p115-task[data-category~=attention]{border-color:rgba(var(--v-theme-error),.26)}
.p115-ui .p115-row{display:flex;align-items:center;gap:14px;min-width:0;padding-right:38px}
.p115-ui .p115-row:has(.p115-confirm[open]){flex-wrap:wrap}
.p115-ui .p115-task-main{min-width:0;flex:1}
.p115-ui .p115-task-name{font-weight:620;font-size:14px;line-height:1.45;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;user-select:text}
.p115-ui .p115-task-meta{display:flex;align-items:center;flex-wrap:wrap;gap:4px 8px;margin-top:6px;font-size:12px;color:var(--p115-muted);font-variant-numeric:tabular-nums}
.p115-ui .p115-meta-item{display:inline-flex;align-items:center;color:var(--p115-muted)}
.p115-ui .p115-meta-item+.p115-meta-item::before{content:'·';margin-right:8px;color:var(--p115-faint)}
.p115-ui .p115-task-context{
  display:block;font-size:12px;margin-top:8px;padding:2px 0 2px 9px;
  border-left:2px solid rgba(var(--v-theme-error),.45);
  color:var(--p115-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap
}
.p115-ui .p115-task:has(.p115-task-details[open]) .p115-task-context{display:none}
.p115-ui .p115-row-actions{display:flex;align-items:flex-start;flex-wrap:wrap;gap:8px;flex-shrink:0}
.p115-ui .p115-row-actions>.v-btn{min-width:80px}
.p115-ui .p115-row-actions:has(.p115-confirm[open]){flex-basis:100%;width:100%;max-width:100%;margin-left:0;padding-left:0;margin-top:4px}
.p115-ui .p115-row-actions .p115-confirm[open]{flex:1 1 100%;width:100%}
.p115-ui .p115-row-actions .p115-confirm-body{margin-left:0;width:100%}
.p115-ui .v-btn{letter-spacing:0;border-radius:8px}
.p115-ui .p115-row-actions .v-btn{min-height:34px;font-weight:580}
.p115-ui .p115-confirm{min-width:0;font-size:13px}
.p115-ui .p115-confirm>summary{
  display:inline-flex;align-items:center;justify-content:center;min-height:34px;
  border:1px solid var(--p115-line-strong);border-radius:8px;padding:5px 12px;
  background:var(--p115-surface);cursor:pointer;list-style:none;font-weight:580;
  user-select:none;transition:background .15s ease,border-color .15s ease
}
.p115-ui .p115-confirm-upload>summary{
  color:rgb(var(--v-theme-on-surface));background:var(--p115-subtle-bg);border-color:var(--p115-line-strong)
}
.p115-ui .p115-confirm[open]{flex-basis:100%;width:100%}
.p115-ui .p115-confirm-body{
  display:flex;align-items:center;justify-content:space-between;gap:12px 16px;flex-wrap:wrap;
  margin-top:8px;padding:12px 14px;border-radius:10px;
  background:var(--p115-subtle-bg);border:1px solid var(--p115-line-strong);
  max-width:100%;font-size:13px;overflow-wrap:anywhere
}
.p115-ui .p115-confirm-body>p{flex:1 1 260px;margin:0!important}
.p115-ui .p115-task-details>summary{
  position:absolute;right:12px;top:12px;display:flex;align-items:center;justify-content:center;gap:5px;
  width:30px;min-height:30px;border-radius:7px;background:transparent;
  border:1px solid transparent;font-size:12px;color:var(--p115-muted);cursor:pointer;list-style:none;
  transition:background .15s ease,border-color .15s ease,color .15s ease
}
.p115-ui .p115-task-details[open]>summary{
  background:var(--p115-well);border-color:var(--p115-line-strong);color:rgb(var(--v-theme-on-surface))
}
.p115-ui .p115-task-details>summary span{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden;white-space:nowrap}
.p115-ui .p115-task-details>summary .v-icon{transition:transform .2s cubic-bezier(.2,.8,.2,1)}
.p115-ui .p115-task-details[open]>summary .v-icon{transform:rotate(180deg)}
.p115-ui .p115-task-body{border-top:1px solid var(--p115-line);padding-top:12px;margin-top:12px}
.p115-ui .p115-result{
  padding:10px 13px;border-radius:10px;background:var(--p115-subtle-bg);
  border:1px solid var(--p115-line-strong);overflow-wrap:anywhere;margin-bottom:12px
}
.p115-ui .p115-result-label{font-size:12px;font-weight:660;margin-bottom:3px;color:var(--p115-muted)}
.p115-ui .p115-path-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:10px 0 12px}
.p115-ui .p115-path-card{
  padding:10px 13px;border-radius:10px;background:var(--p115-subtle-bg);
  border:1px solid var(--p115-line);min-width:0
}
.p115-ui .p115-path-card:last-child:nth-child(odd){grid-column:1/-1}
.p115-ui .p115-path-label{display:flex;align-items:center;gap:6px;font-size:12px;font-weight:600;color:var(--p115-muted);margin-bottom:4px}
.p115-ui .p115-path{
  display:block;font-family:ui-monospace,"Cascadia Code",Consolas,monospace;font-size:12px;
  line-height:1.65;overflow-wrap:anywhere;white-space:pre-wrap;user-select:all;cursor:pointer;margin:0
}
.p115-ui .p115-cooldown-banner{
  display:flex;align-items:center;gap:12px;padding:12px 15px;margin-bottom:14px;border-radius:11px;
  border:1px solid rgba(var(--v-theme-warning),.32);
  background:rgba(var(--v-theme-warning),.07);
  box-shadow:var(--p115-shadow-sm);animation:p115-reveal .2s var(--p115-ease)
}
.p115-ui .p115-cooldown-banner .v-icon{color:rgb(var(--v-theme-warning));flex-shrink:0;animation:p115-pulse 2s ease-in-out infinite}
.p115-ui .p115-progress{margin-top:9px}
.p115-ui .p115-progress-head{
  display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;
  font-size:11.5px;font-weight:600;color:var(--p115-muted);font-variant-numeric:tabular-nums;margin-bottom:5px
}
.p115-ui .p115-progress-track{
  position:relative;height:5px;border-radius:999px;background:rgba(var(--v-theme-on-surface),.08);overflow:hidden
}
.p115-ui .p115-progress-fill{
  height:100%;border-radius:999px;min-width:6px;
  background:rgb(var(--v-theme-primary));
  transition:width .3s var(--p115-ease)
}
.p115-ui .p115-task[data-state=uploading] .p115-progress-fill{
  background:linear-gradient(90deg,rgb(var(--v-theme-primary)) 0%,color-mix(in srgb,rgb(var(--v-theme-primary)) 65%,white) 50%,rgb(var(--v-theme-primary)) 100%);
  background-size:200% 100%;animation:p115-shimmer 2.2s linear infinite
}
.p115-ui .p115-ratio-bar{
  display:flex;height:3px;border-radius:999px;overflow:hidden;gap:2px;margin:0 0 8px;
  background:rgba(var(--v-theme-on-surface),.06)
}
.p115-ui .p115-ratio-instant{background:rgba(var(--v-theme-on-surface),.34);border-radius:999px}
.p115-ui .p115-ratio-upload{background:rgba(var(--v-theme-on-surface),.14);border-radius:999px}
.p115-ui .p115-toolbar-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.p115-ui .p115-density-toggle>summary{
  display:inline-flex;align-items:center;gap:5px;padding:5px 10px;min-height:32px;border-radius:8px;
  border:1px solid var(--p115-line-strong);background:var(--p115-surface);color:var(--p115-muted);
  font-size:12.5px;font-weight:580;cursor:pointer;list-style:none;user-select:none;transition:background .15s ease,border-color .15s ease
}
.p115-ui .p115-density-toggle[open]>summary{
  color:rgb(var(--v-theme-on-surface));background:var(--p115-well);border-color:var(--p115-line-hover)
}
.p115-ui .p115-quick-bulk{
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;
  padding:10px 14px;margin-bottom:12px;border-radius:11px;
  border:1px solid var(--p115-line-strong);background:var(--p115-subtle-bg)
}
.p115-ui .p115-quick-bulk .v-icon{color:var(--p115-muted)}
.p115-ui .p115-inline-tools{display:flex;align-items:center;flex-wrap:wrap;gap:8px}
.p115-ui .p115-exec-bar{
  display:flex;align-items:center;flex-wrap:wrap;gap:10px;margin-top:14px;padding:12px 14px;
  border-radius:10px;border:1px solid var(--p115-line-strong);background:var(--p115-subtle-bg)
}
.p115-ui:has(.p115-density-toggle[open]) .p115-list{gap:5px}
.p115-ui:has(.p115-density-toggle[open]) .p115-task{padding:8px 14px}
.p115-ui:has(.p115-density-toggle[open]) .p115-task-context{display:none}
.p115-ui:has(.p115-density-toggle[open]) .p115-task-details>summary{top:8px}
.p115-ui .p115-times{display:flex;flex-wrap:wrap;gap:8px 12px;font-size:12px;color:var(--p115-muted);font-variant-numeric:tabular-nums;margin:12px 0}
.p115-ui .p115-time-pill{
  display:inline-flex;align-items:center;padding:3px 9px;border-radius:6px;
  background:var(--p115-subtle-bg);border:1px solid var(--p115-line)
}
.p115-ui .p115-empty{
  text-align:center;padding:36px 20px;border:1px dashed var(--p115-line-strong);
  border-radius:12px;background:var(--p115-surface);margin-bottom:14px
}
.p115-ui .p115-empty h3{margin:12px 0 6px}
.p115-ui .p115-filter-empty{display:none}
.p115-ui:has(input[value=waiting]:checked) .p115-task:not([data-category~=waiting]),.p115-ui:has(input[value=attention]:checked) .p115-task:not([data-category~=attention]),.p115-ui:has(input[value=transfer]:checked) .p115-task:not([data-category~=transfer]),.p115-ui:has(input[value=ended]:checked) .p115-task:not([data-category~=ended]){display:none}
.p115-ui:has(input[value=waiting]:checked) .p115-filter-empty[data-filter=waiting],.p115-ui:has(input[value=attention]:checked) .p115-filter-empty[data-filter=attention],.p115-ui:has(input[value=transfer]:checked) .p115-filter-empty[data-filter=transfer],.p115-ui:has(input[value=ended]:checked) .p115-filter-empty[data-filter=ended]{display:block}
.p115-ui .p115-bulk-actions{display:flex;align-items:flex-start;flex-wrap:wrap;gap:12px;margin-top:12px}
.p115-ui .p115-bulk-actions .p115-confirm{flex:1 1 220px}
.p115-ui .p115-bulk-actions .p115-confirm-body{max-width:100%}
.p115-ui .p115-help-list{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin:0;padding:0;list-style:none}
.p115-ui .p115-help-card{
  padding:12px 14px;border-radius:10px;background:var(--p115-subtle-bg);
  border:1px solid var(--p115-line)
}
.p115-ui .p115-help-card:last-child:nth-child(odd){grid-column:1/-1}
.p115-ui .p115-help-card h3{display:flex;align-items:center;gap:7px;font-size:13.5px}
.p115-ui .p115-help-card h3::before{
  content:'';width:5px;height:5px;border-radius:999px;background:var(--p115-faint);flex-shrink:0
}
.p115-ui .p115-footer{
  font-size:12px;color:var(--p115-muted);margin-top:14px;padding:10px 14px;
  border-radius:10px;background:var(--p115-subtle-bg);border:1px solid var(--p115-line);
  overflow-wrap:anywhere
}
@media(hover:hover) and (pointer:fine){
  .p115-ui .p115-filter:hover,.p115-ui .p115-confirm>summary:hover,.p115-ui .p115-disclosure>summary:hover{background:var(--p115-well)}
  .p115-ui .p115-metric:hover,.p115-ui .p115-task:hover,.p115-ui .p115-toggle:hover{border-color:var(--p115-line-hover)}
  .p115-ui .p115-task-details>summary:hover{background:var(--p115-well);color:rgb(var(--v-theme-on-surface))}
}
@container (max-width:700px){
  .p115-ui .p115-row{flex-wrap:wrap;gap:10px}
  .p115-ui .p115-task-main{flex-basis:100%}
  .p115-ui .p115-row-actions,.p115-ui .p115-row-actions:has(.p115-confirm[open]){max-width:100%;margin-left:0;padding-left:0}
  .p115-ui .p115-task-context{white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical}
  .p115-ui .p115-help-list,.p115-ui .p115-path-grid{grid-template-columns:1fr}
}
@container (max-width:480px){
  .p115-ui .p115-metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
  .p115-ui .p115-metric{padding:12px}
  .p115-ui .p115-fields,.p115-ui .p115-switches{grid-template-columns:1fr}
  .p115-ui .p115-header{padding:2px 0 12px;gap:10px;flex-wrap:wrap}
  .p115-ui h2{font-size:17px}
  .p115-ui .p115-section{padding:14px}
  .p115-ui .p115-task{padding:11px 13px}
  .p115-ui .p115-task-main{flex-basis:100%}
  .p115-ui .p115-task-name{white-space:normal;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow-wrap:anywhere}
  .p115-ui .p115-row-actions,.p115-ui .p115-row-actions:has(.p115-confirm[open]){margin-left:0;padding-left:0}
  .p115-ui .p115-filter,.p115-ui .p115-row-actions .v-btn,.p115-ui .p115-confirm>summary,.p115-ui .p115-task-details>summary{min-height:44px}
  .p115-ui .p115-task-meta{gap:4px 8px}
  .p115-ui .p115-row{padding-right:0}
  .p115-ui .p115-task-details{margin-top:8px}
  .p115-ui .p115-task-details>summary{position:static;width:fit-content;padding:0 12px}
  .p115-ui .p115-task-details>summary span{position:static;width:auto;height:auto;clip-path:none;overflow:visible;white-space:normal}
}
@media(prefers-reduced-motion:reduce){
  .p115-ui *,.p115-ui *::before,.p115-ui *::after{animation:none!important;transition:none!important}
}
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


def parent_label(path):
    """Return immediate parent directory name for disambiguating episodes/remuxes."""
    name = PurePosixPath(str(path or "").replace("\\", "/")).parent.name
    return name if name and name not in (".", "/") else ""


def concise_folder(source_path):
    """Omit redundant parent folder names that already mirror the filename prefix."""
    folder = parent_label(source_path)
    if not folder:
        return ""
    file = filename(source_path)
    norm = lambda text: "".join(ch for ch in text.lower() if ch.isalnum())
    norm_folder, norm_file = norm(folder), norm(file)
    if len(norm_folder) >= 10 and norm_file.startswith(norm_folder[:10]):
        return ""
    return folder[:26] + "…" if len(folder) > 28 else folder


def format_bytes(size):
    """Format byte size using compact IEC units for UI pills and progress."""
    if not isinstance(size, (int, float)) or isinstance(size, bool) or size < 0:
        return ""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{int(value)} B" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return ""


def format_eta(seconds):
    """Render positive remaining seconds as a compact Chinese duration label."""
    sec = max(1, int(seconds))
    if sec < 60:
        return f"{sec} 秒后"
    if sec < 3600:
        return f"{sec // 60} 分钟后"
    if sec < 86400:
        hours, rem_min = sec // 3600, (sec % 3600) // 60
        return f"{hours} 小时 {rem_min} 分后" if rem_min else f"{hours} 小时后"
    return f"{sec // 86400} 天后"


def disclosure(title, children, icon_name=None, opened=False, **props):
    heading = ([icon(icon_name)] if icon_name else []) + [node("span", title)]
    return node("details", content=[node("summary", content=heading),
                                   box(children, "p115-disclosure-body")],
                **{"class": "p115-disclosure", "open": opened, **props})


def note(text, icon_name="mdi-information-outline", **props):
    return box([icon(icon_name), paragraph(text)], "p115-note", **props)


def section(title, description, children, icon_name="mdi-tune-variant"):
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


def record_choices(tasks):
    return [{"title": f"{filename(t['source'])} · {LABELS.get(t['state'], t['state'])}"
                      f" · 整理记录 #{t['history_id'] or '待生成'}", "value": t["id"],
             "props": {"disabled": t["state"] in ("running", "uploading")}}
            for t in tasks]


def record_search_model(scope, result):
    values = dict(
        query="", state="", applied_query="", applied_state="", busy=False, error="",
        exec_busy=False, exec_error="", exec_message="", exec_items=[], confirm_clear=False,
        **result,
    )
    return {f"_records_{scope}_{key}": value for key, value in values.items()}


def record_search_handler(scope, direction=0):
    prefix = f"_records_{scope}_"
    selection, flag = ("task_ids", "apply_action") if scope == "tasks" else ("history_ids", "cleanup_history")
    # FormRender runs handlers with the dialog's model in scope. Use MP's
    # authenticated API instance; no tokens, global selection or saved query.
    return """async () => {
        const p = PREFIX;
        if (model[p+'busy']) return;
        model[p+'busy'] = true; model[p+'error'] = ''; model[p+'confirm_clear'] = false;
        model[SELECTION] = []; model[FLAG] = false;
        const query = model[p+'query'] || '', state = model[p+'state'] || '';
        const same = query === model[p+'applied_query'] && state === model[p+'applied_state'];
        const page = DIRECTION === 0 || !same ? 1 : model[p+'page'] + DIRECTION;
        try {
            if (!window.MoviePilotAPI) throw new Error('当前前端不支持查询，请更新 MoviePilot 前端');
            const data = await window.MoviePilotAPI.get('plugin/P115InstantWait/records',
                {params: {scope: SCOPE, query, state, page}});
            for (const key of ['items','total','page','pages']) model[p+key] = data[key];
            model[p+'applied_query'] = query; model[p+'applied_state'] = state;
        } catch (error) {
            model[p+'items'] = []; model[p+'total'] = 0; model[p+'page'] = 1; model[p+'pages'] = 1;
            const detail = error.response?.data?.detail;
            model[p+'error'] = typeof detail === 'string' ? detail : !window.MoviePilotAPI
                ? '当前前端不支持查询，请更新 MoviePilot 前端' : '查询失败，请检查连接后重试';
        } finally { model[p+'busy'] = false; }
    }""".replace("PREFIX", repr(prefix)).replace("SELECTION", repr(selection)).replace("FLAG", repr(flag)).replace("DIRECTION", str(direction)).replace("SCOPE", repr(scope))


def record_select_handler(scope, select_all=True):
    prefix = f"_records_{scope}_"
    selection = "task_ids" if scope == "tasks" else "history_ids"
    return """() => {
        const p = PREFIX;
        model[p+'confirm_clear'] = false;
        if (!SELECT_ALL) { model[SELECTION] = []; return; }
        const items = Array.isArray(model[p+'items']) ? model[p+'items'] : [];
        model[SELECTION] = items.filter((item) => !(item.props && item.props.disabled)).map((item) => item.value);
    }""".replace("PREFIX", repr(prefix)).replace("SELECTION", repr(selection)).replace("SELECT_ALL", "true" if select_all else "false")


def form_batch_exec_handler():
    return """async () => {
        const p = '_records_tasks_';
        if (model[p+'exec_busy'] || model[p+'busy']) return;
        model[p+'exec_error'] = ''; model[p+'exec_message'] = ''; model[p+'exec_items'] = [];
        if (!model.enabled) { model[p+'exec_error'] = '请先开启上方「启用秒传等待」后再执行任务操作'; return; }
        const keys = Array.isArray(model.task_ids) ? model.task_ids : [];
        if (!keys.length) { model[p+'exec_error'] = '请先勾选至少一个任务'; return; }
        model[p+'exec_busy'] = true;
        try {
            if (!window.MoviePilotAPI) throw new Error('当前前端不支持此操作，请更新 MoviePilot 前端');
            const action = model.action || 'pause';
            const raw = await window.MoviePilotAPI.post('plugin/P115InstantWait/form/tasks/' + action, {keys});
            if (raw && raw.success === false) throw new Error(raw.message || '批量操作未完成');
            const res = raw && raw.data && raw.data.accepted !== undefined ? raw.data : raw;
            const names = {upload: '强制上传', resume: '继续等待', pause: '暂停', cancel: '取消任务'};
            let msg = (names[res?.action || action] || '批量操作') + '已提交：接受 ' + (res?.accepted ?? 0) + ' · 跳过 ' + (res?.skipped ?? 0) + ' · 异常 ' + (res?.failed ?? 0);
            if (res?.notice) msg += '（' + res.notice + '）';
            model[p+'exec_message'] = msg;
            model[p+'exec_items'] = Array.isArray(res?.items) ? res.items.slice(0, 12).map((i) => i.name + '：' + i.message) : [];
            model.task_ids = []; model.apply_action = false;
            try {
                const query = model[p+'applied_query'] || '', state = model[p+'applied_state'] || '', page = model[p+'page'] || 1;
                const data = await window.MoviePilotAPI.get('plugin/P115InstantWait/records', {params: {scope: 'tasks', query, state, page}});
                for (const key of ['items','total','page','pages']) model[p+key] = data[key];
            } catch (_) {}
        } catch (error) {
            const detail = error.response?.data?.detail;
            model[p+'exec_error'] = typeof detail === 'string' ? detail : (error?.message && !window.MoviePilotAPI ? error.message : error?.message || '操作失败，请检查连接后重试');
        } finally { model[p+'exec_busy'] = false; }
    }"""


def form_history_clear_handler(cancel_confirm=False):
    if cancel_confirm:
        return "() => { model._records_history_confirm_clear = false; }"
    return """async () => {
        const p = '_records_history_';
        if (model[p+'exec_busy'] || model[p+'busy']) return;
        model[p+'exec_error'] = ''; model[p+'exec_message'] = ''; model[p+'exec_items'] = [];
        const mode = model.history_mode || 'selected';
        const keys = Array.isArray(model.history_ids) ? model.history_ids : [];
        const states = Array.isArray(model.history_states) ? model.history_states : [];
        if (mode === 'selected' && !keys.length) { model[p+'exec_error'] = '请先选择至少一条要清理的记录'; return; }
        if (mode === 'filtered' && !states.length) { model[p+'exec_error'] = '请先选择至少一种要清理的状态'; return; }
        if (!model[p+'confirm_clear']) { model[p+'confirm_clear'] = true; return; }
        model[p+'confirm_clear'] = false; model[p+'exec_busy'] = true;
        try {
            if (!window.MoviePilotAPI) throw new Error('当前前端不支持此操作，请更新 MoviePilot 前端');
            const raw = await window.MoviePilotAPI.post('plugin/P115InstantWait/form/history/clear',
                {mode, keys, states, days: Number(model.history_days ?? 30)});
            if (raw && raw.success === false) throw new Error(raw.message || '清理未完成');
            const res = raw && raw.data && raw.data.deleted !== undefined ? raw.data : raw;
            let msg = '清理已完成：删除 ' + (res?.deleted ?? 0) + ' · 跳过 ' + (res?.skipped ?? 0);
            if (res?.notice) msg += '（' + res.notice + '）';
            model[p+'exec_message'] = msg;
            model[p+'exec_items'] = Array.isArray(res?.items) ? res.items.slice(0, 12) : [];
            model.history_ids = []; model.cleanup_history = false;
            try {
                const query = model[p+'applied_query'] || '', state = model[p+'applied_state'] || '', page = model[p+'page'] || 1;
                const data = await window.MoviePilotAPI.get('plugin/P115InstantWait/records', {params: {scope: 'history', query, state, page}});
                for (const key of ['items','total','page','pages']) model[p+key] = data[key];
            } catch (_) {}
        } catch (error) {
            const detail = error.response?.data?.detail;
            model[p+'exec_error'] = typeof detail === 'string' ? detail : (error?.message || '清理失败，请检查参数后重试');
        } finally { model[p+'exec_busy'] = false; }
    }"""


def record_search_controls(scope):
    p = f"_records_{scope}_"
    states = [s for s in LABELS if (s in ACTIVE) == (scope == "tasks")]
    return [
        box([
            field("VTextField", p + "query", "全库搜索", "按文件名、源路径、任务 ID 或整理记录编号搜索，最多 200 字。",
                  maxlength=200, clearable=True, **{"onKeydown": "async (event) => { if (event.key === 'Enter') { event.preventDefault(); await (" + record_search_handler(scope) + ")(); } }",
                                                  "disabled": "{{ " + p + "busy }}"}),
            field("VSelect", p + "state", "筛选状态", "选择后点击搜索；留空显示此范围内全部状态。",
                  items=[{"title": "全部状态", "value": ""}] + [{"title": LABELS[s], "value": s} for s in states],
                  **{"disabled": "{{ " + p + "busy }}"}),
        ], "p115-fields"),
        box([
            node("VBtn", "搜索", variant="tonal", color="primary", **{"onClick": record_search_handler(scope), "loading": "{{ " + p + "busy }}"}),
            node("VBtn", "上一页", variant="outlined", **{"onClick": record_search_handler(scope, -1), "disabled": "{{ " + p + "busy || " + p + "page <= 1 }}"}),
            node("VBtn", "下一页", variant="outlined", **{"onClick": record_search_handler(scope, 1), "disabled": "{{ " + p + "busy || " + p + "page >= " + p + "pages }}"}),
        ], "p115-bulk-actions mt-3"),
        node("p", **{"class": "p115-hint mt-3", "aria-live": "polite",
                      "textContent": "{{ '共 ' + " + p + "total + ' 条 · 第 ' + " + p + "page + ' / ' + " + p + "pages + ' 页 · 每页 50 条' }}"}),
        node("VAlert", type="error", variant="tonal", **{"show": "{{ !!" + p + "error }}", "text": "{{ " + p + "error }}", "class": "mt-3"}),
        paragraph("搜索与翻页不会执行任务；重新查询会清空本区选择及执行勾选。", **{"class": "p115-hint mb-4"}),
    ]


def config_form(tasks, error="", batch_result=None, history_result=None, notice=""):
    content = [node("style", STYLES),
        box([box([node("h2", "参数设置"),
                  paragraph("本地视频整理至 115 时后台等待秒传，不阻塞常规整理。",
                            **{"class": "p115-copy p115-muted mt-1"})], "p115-header-main")], "p115-header"),
        box([toggle("enabled", "启用秒传等待", "后台处理视频，其他整理照常进行。"),
             toggle("notify", "推送任务通知", "开启后按下方勾选的触发条件推送。")], "p115-switches")]
    if notice:
        content.append(node("VAlert", notice, type="info", variant="tonal", **{"class": "mb-4", "role": "status"}))
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-5", "role": "alert"}))

    notify_section = box([
        box([node("h3", "通知触发条件")], "p115-section-head"),
        paragraph("仅推送勾选的场景；同一次状态变化若同时命中多项，只发送一条通知。",
                  **{"class": "p115-copy p115-muted mb-4"}),
        field("VSelect", "notify_events", "触发通知的场景",
              "默认全选；留空表示不发送插件主动通知。整理成功与最终整理失败仍沿用 MP 原生通知。",
              items=[
                  {"title": "遇到不秒传（每轮首次）", "value": "not_instant"},
                  {"title": "达到重试 / 等待上限", "value": "max_retries"},
                  {"title": "触发自动强制上传", "value": "auto_upload"},
                  {"title": "暂停 / 等待手动操作", "value": "manual_action"},
              ],
              multiple=True, chips=True, **{"closable-chips": True}),
    ], "p115-section", show="{{ notify }}")

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
        note("先尝试秒传，未命中就上传视频正文，会占用上行带宽。若上次请求结果未知，将在目标当前不存在时重试；延迟出现的远端文件可能造成重复，请先核对目标。上传失败或文件、授权异常时仍会暂停。",
             "mdi-cloud-upload-outline", **{"show": "{{ limit_action === 'upload' }}"}),
    ])

    batch_children = [
        paragraph("支持全选本页后原地立即执行，无需保存重载插件；单个任务也可在监控页直接处理。",
                  **{"class": "p115-copy p115-muted mb-4"}),
        *record_search_controls("tasks"),
        field("VSelect", "task_ids", "选择本页任务", "可多选当前页任务；执行中的任务不可选。查找更早任务请使用上方全库搜索或翻页。",
              items="{{ _records_tasks_items }}", multiple=True, chips=True, clearable=True,
              **{"item-title": "title", "item-value": "value", "closable-chips": True,
                 "disabled": "{{ _records_tasks_busy || !_records_tasks_items.length }}", "no-data-text": "没有匹配的任务"}),
        box([
            node("VBtn", "全选本页可选", variant="outlined", size="small",
                 **{"onClick": record_select_handler("tasks", True), "disabled": "{{ _records_tasks_busy || !_records_tasks_items.length }}"}),
            node("VBtn", "清空已选", variant="text", size="small",
                 **{"onClick": record_select_handler("tasks", False), "disabled": "{{ !task_ids || !task_ids.length }}"}),
            node("span", **{"class": "p115-hint", "textContent": "{{ '已选 ' + (task_ids ? task_ids.length : 0) + ' 项' }}"}),
        ], "p115-inline-tools mt-2"),
        box([field("VSelect", "action", "对所选任务执行", "状态不支持的任务会跳过；继续等待仅适用于已暂停任务。",
                   items=[{"title": "暂停自动重试", "value": "pause"},
                          {"title": "继续等待秒传", "value": "resume"},
                          {"title": "强制上传", "value": "upload"},
                          {"title": "取消任务", "value": "cancel"}],
                   **{"disabled": "{{ !task_ids || !task_ids.length }}"})], "mt-5"),
        note(ACTION_HINT),
        box([
            node("VBtn", "立即执行所选操作", variant="tonal", color="primary",
                 **{"prepend-icon": "mdi-play-circle-outline", "onClick": form_batch_exec_handler(),
                    "loading": "{{ _records_tasks_exec_busy }}",
                    "disabled": "{{ _records_tasks_exec_busy || !enabled || !task_ids || !task_ids.length }}"}),
            paragraph("原地提交后立即刷新本页列表，无需保存或重启插件。", **{"class": "p115-hint"}),
        ], "p115-exec-bar"),
        node("VAlert", type="success", variant="tonal", **{"show": "{{ !!_records_tasks_exec_message }}", "text": "{{ _records_tasks_exec_message }}", "class": "mt-3"}),
        node("p", **{"class": "p115-hint mt-2", "show": "{{ _records_tasks_exec_items && _records_tasks_exec_items.length }}",
                      "textContent": "{{ (_records_tasks_exec_items || []).join('；') }}"}),
        node("VAlert", type="error", variant="tonal", **{"show": "{{ !!_records_tasks_exec_error }}", "text": "{{ _records_tasks_exec_error }}", "class": "mt-3"}),
        node("VCheckbox", model="apply_action", label="或：保存配置时对所选任务执行一次",
             **{"color": "primary", "class": "mt-3", "hide-details": True,
                "disabled": "{{ !enabled || !task_ids || !task_ids.length }}"}),
        paragraph("需启用插件；执行后自动清空选择，重新打开配置也可查看历史摘要。", **{"class": "p115-hint"}),
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
        box([*record_search_controls("history"),
             field("VSelect", "history_ids", "选择本页已结束记录", "全库查询结果，每页 50 条；只清理本页勾选的记录。",
                   items="{{ _records_history_items }}", multiple=True, chips=True, clearable=True,
                   **{"item-title": "title", "item-value": "value", "closable-chips": True,
                      "disabled": "{{ _records_history_busy || !_records_history_items.length }}", "no-data-text": "没有匹配的记录"}),
             box([
                 node("VBtn", "全选本页记录", variant="outlined", size="small",
                      **{"onClick": record_select_handler("history", True), "disabled": "{{ _records_history_busy || !_records_history_items.length }}"}),
                 node("VBtn", "清空已选", variant="text", size="small",
                      **{"onClick": record_select_handler("history", False), "disabled": "{{ !history_ids || !history_ids.length }}"}),
                 node("span", **{"class": "p115-hint", "textContent": "{{ '已选 ' + (history_ids ? history_ids.length : 0) + ' 条' }}"}),
             ], "p115-inline-tools mt-2")],
            "mt-5", show="{{ history_mode === 'selected' }}"),
        box([
            field("VSelect", "history_states", "清理哪些状态", "只匹配勾选的结束状态。",
                  items=[{"title": "整理成功", "value": "completed"}, {"title": "整理失败", "value": "failed"},
                         {"title": "已取消", "value": "cancelled"}], multiple=True, chips=True),
            field("VTextField", "history_days", "保留最近记录", "按结束时间计算；0 清理全部匹配记录。",
                  type="number", suffix="天", min=0, max=36500, step=1, inputmode="numeric"),
        ], "p115-fields mt-5", show="{{ history_mode === 'filtered' }}"),
        box([
            node("VBtn", "立即清理所选记录", variant="tonal", color="warning",
                 **{"prepend-icon": "mdi-broom", "onClick": form_history_clear_handler(False),
                    "loading": "{{ _records_history_exec_busy }}",
                    "disabled": "{{ _records_history_exec_busy || (history_mode === 'selected' ? (!history_ids || !history_ids.length) : (!history_states || !history_states.length)) }}"}),
            paragraph("仅清理插件记录，二次确认后原地完成并刷新列表。", **{"class": "p115-hint"}),
        ], "p115-exec-bar", show="{{ !_records_history_confirm_clear }}"),
        box([
            node("VBtn", "确认永久删除插件记录", variant="tonal", color="error",
                 **{"prepend-icon": "mdi-delete-outline", "onClick": form_history_clear_handler(False),
                    "loading": "{{ _records_history_exec_busy }}"}),
            node("VBtn", "取消", variant="outlined",
                 **{"onClick": form_history_clear_handler(True), "disabled": "{{ _records_history_exec_busy }}"}),
            paragraph("将清理所选插件记录（不影响本地与 115 文件），确认后不可恢复。", **{"class": "p115-hint"}),
        ], "p115-exec-bar", show="{{ !!_records_history_confirm_clear }}"),
        node("VAlert", type="success", variant="tonal", **{"show": "{{ !!_records_history_exec_message }}", "text": "{{ _records_history_exec_message }}", "class": "mt-3"}),
        node("p", **{"class": "p115-hint mt-2", "show": "{{ _records_history_exec_items && _records_history_exec_items.length }}",
                      "textContent": "{{ (_records_history_exec_items || []).join('；') }}"}),
        node("VAlert", type="error", variant="tonal", **{"show": "{{ !!_records_history_exec_error }}", "text": "{{ _records_history_exec_error }}", "class": "mt-3"}),
        node("VCheckbox", model="cleanup_history", label="或：保存配置时清理一次（插件记录不可恢复）",
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
        ("通知设置", "支持按场景勾选触发条件，遇到不秒传每轮仅首次通知，同一次状态转换命中多项只发送一条；请在 MP 通知渠道中开启「整理入库」和「手动处理」。成功与最终失败沿用 MP 原生通知。"),
        ("支持范围", "MP V2.15.6，本地到内置 115 的复制或移动。字幕、NFO、图片沿用 MP；蓝光原盘目录暂不支持。"),
        ("目标文件", "直接写入正式目录和文件名，覆盖及旧版本清理遵循 MP 设置。不会创建暂存或备份目录。"),
    ]
    content.extend([
        notify_section,
        strategy,
        disclosure("视频格式", [field("VTextarea", "extensions", "接管的视频后缀", "英文逗号分隔，不加点号，例如 mkv,mp4,iso；未列出的格式走 MP 原流程。",
                    rows=2, spellcheck=False, **{"auto-grow": True})], "mdi-file-video-outline"),
        disclosure("批量任务操作 · 全库查询", batch_children, "mdi-playlist-check"),
        disclosure("清理已结束记录", maintenance, "mdi-broom"),
        disclosure("使用说明与通知设置", [node("ul", content=[node("li", content=[node("h3", title),
                   paragraph(text, **{"class": "p115-copy p115-muted mt-1"})],
                   **{"class": "p115-help-card"}) for title, text in help_items],
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


def status_chip(state, label=None, tone_override=None, icon_override=None, transfer_method=None):
    default_tone, _ = TONES.get(state, ("secondary", "mdi-circle-outline"))
    tone = tone_override or default_tone
    text = label
    if text is None and state == "completed":
        text = {"instant": "秒传完成", "upload": "上传完成"}.get(transfer_method, "整理成功")
    return node("span", content=[node("span", "", **{"class": "p115-dot", "aria-hidden": "true"}),
                                 node("span", text or LABELS.get(state, state))],
                **{"class": "p115-badge", "data-tone": tone})


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
    next_dt = datetime.fromtimestamp(next_at).astimezone()
    now_dt = datetime.fromtimestamp(now).astimezone()
    clock = next_dt.strftime("%H:%M:%S" if next_dt.date() == now_dt.date() else "%m-%d %H:%M")
    return "下次重试 · MP 时间", f"{format_eta(next_at - now)} · {clock}", "primary"


def record_time(value, compact=False):
    if value is None:
        return "未记录"
    try:
        return datetime.fromtimestamp(value).astimezone().strftime("%m-%d %H:%M" if compact else "%Y-%m-%d %H:%M:%S %z")
    except (TypeError, ValueError, OverflowError, OSError):
        return "未记录"


def task_button(task, action, label):
    props = {"aria-label": f"{label}：{filename(task['source'])}"}
    if action == "cancel":
        props["color"] = "error"
    elif action in ("resume", "upload"):
        props["color"] = "primary"
    button = node("VBtn", label, variant="tonal" if action in ("resume", "upload") else "outlined",
                  size="small", **props)
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


def task_row(task, enabled, now, max_retries, notice=""):
    status = task["state"]
    tone_key, icon_name = TONES.get(status, ("secondary", "mdi-file-video-outline"))
    label, value, tone = schedule_summary(task, enabled, now)
    if notice and status in ACTIVE:
        label, value, tone = "后续安排", "等待更新完成", "secondary"
    auto = task.get("auto_attempts", task["attempts"])
    budget = (f"自动尝试 {auto}/{max_retries + 1} 次"
              if max_retries is not None and status in ACTIVE else f"自动尝试 {auto} 次")
    result = latest_result(task)
    meta = [status_chip(status, transfer_method=task.get("transfer_method")),
            node("span", f"#{task['history_id']}" if task['history_id'] else "记录待生成", **{"class": "p115-meta-item"})]
    folder_name = concise_folder(task.get("source"))
    if folder_name:
        meta.append(node("span", folder_name, **{"class": "p115-meta-item", "title": "所在目录：" + parent_label(task.get("source"))}))
    size_text = format_bytes(task.get("size"))
    if size_text:
        meta.append(node("span", size_text, **{"class": "p115-meta-item", "title": "源文件大小"}))
    meta.append(node("span", budget, **{"class": "p115-meta-item"}))
    if status in ACTIVE:
        meta.append(node("span", value, **{"class": "p115-meta-item", "title": label}))
    else:
        meta.append(node("span", record_time(task.get("updated"), compact=True),
                         **{"class": "p115-meta-item", "title": "结束 " + record_time(task.get("updated"))}))
    main = [node("div", filename(task["source"]), **{"class": "p115-task-name", "title": filename(task["source"])}),
            box(meta, "p115-task-meta")]
    progress = task.get("upload_progress")
    if progress:
        total_parts = progress.get("total_parts", 0)
        done_parts = progress.get("done_parts", 0)
        if total_parts > 0:
            percent = progress.get("percent", 0)
            left_text = "正在提交上传合并…" if progress.get("completing") else f"分片上传进度 · {percent}%"
            right_text = f"{format_bytes(progress.get('uploaded_bytes'))} / {format_bytes(progress.get('total_bytes'))} · {done_parts}/{total_parts} 片"
            bar_width = f"width:{max(2, min(100, percent))}%"
        else:
            left_text = "已有断点续传分片"
            right_text = f"已保存 {done_parts} 片"
            bar_width = "width:15%"
        main.append(box([
            box([node("span", left_text), node("span", right_text)], "p115-progress-head"),
            box([box([], "p115-progress-fill", style=bar_width)], "p115-progress-track"),
        ], "p115-progress"))
    if status in ("paused", "failed"):
        main.append(paragraph(result, **{"class": "p115-task-context", "title": result}))
    actions = []
    if status in ("queued", "waiting", "paused", "upload_queued") and enabled:
        actions.append(task_button(task, "resume", "继续等待") if status == "paused"
                       else task_button(task, "pause", "暂停"))
        if status != "upload_queued":
            actions.append(confirm_action(task, "upload", "强制上传…",
                "先尝试秒传，未命中就上传视频正文，会占用上行带宽。若上次请求结果未知，将在目标当前不存在时重试；延迟出现的远端文件可能造成重复，请先核对目标。", "确认强制上传"))
    row = [box(main, "p115-task-main")]
    if actions:
        row.append(box(actions, "p115-row-actions", **{"aria-label": "任务操作"}))
    details = [box([node("div", "暂停原因" if status == "paused" else "最新结果",
                          **{"class": "p115-result-label"}), paragraph(result)], "p115-result"),
               paragraph(f"{budget} · 累计尝试 {task['attempts']} 次", **{"class": "p115-hint mb-3"})]
    if status in ("running", "uploading"):
        details.append(paragraph("本轮执行中，结束后刷新查看结果。", **{"class": "p115-hint mb-3"}))
    elif status in ACTIVE and not enabled:
        details.append(paragraph("更新完成后可继续操作。" if notice else "请在配置页启用插件后继续处理。", **{"class": "p115-hint mb-3"}))
    if status == "failed":
        details.append(paragraph("请根据失败原因，在 MP 重新发起整理。", **{"class": "p115-hint mb-3"}))
    path_cards = []
    for title, path, path_icon in (
        ("本地源文件 · 单击全选", task["source"], "mdi-folder-outline"),
        ("115 目标文件 · 单击全选", task["target"], "mdi-cloud-outline"),
        ("任务 ID · 单击全选搜日志", task["id"], "mdi-identifier"),
    ):
        path_cards.append(box([
            box([icon(path_icon, size=15), node("span", title)], "p115-path-label"),
            node("code", str(path), **{"class": "p115-path"}),
        ], "p115-path-card"))
    details.append(box(path_cards, "p115-path-grid"))
    details.append(box([node("span", "入队时间：" + record_time(task.get("created")), **{"class": "p115-time-pill"}),
                        node("span", ("完成时间：" if status == "completed" else "结束时间：" if status in ("failed", "cancelled") else "更新时间：") +
                             record_time(task.get("updated")), **{"class": "p115-time-pill"})], "p115-times"))
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
        "p115-task", role="listitem", **{"data-category": categories(task), "data-state": status, "data-tone": tone_key})


def metric(title, count, caption, tone, name, attention=False, ratio=None):
    value_cls = "p115-metric-value" + (" p115-metric-zero" if not count else "")
    children = [
        box([node("span", title), icon(name, size=16)], "p115-metric-label"),
        node("div", str(count), **{"class": value_cls}),
    ]
    if ratio:
        instant_pct, upload_pct = ratio
        children.append(box([
            box([], "p115-ratio-instant", style=f"width:{instant_pct}%"),
            box([], "p115-ratio-upload", style=f"width:{upload_pct}%"),
        ], "p115-ratio-bar"))
    children.append(paragraph(caption, **{"class": "p115-metric-caption"}))
    return box(children, "p115-metric" + (" p115-metric-attention" if attention and count else ""), **{"data-tone": tone})


def batch_feedback(result):
    names = {"upload": "强制上传", "resume": "继续等待", "pause": "暂停", "cancel": "取消任务"}
    title = (f"上次{names.get(result['action'], '批量操作')}：接受 {result['accepted']} · "
             f"跳过 {result['skipped']} · 异常 {result['failed']}")
    return disclosure(title, [
        paragraph(result.get("at", "") + " · 已接受表示操作已提交，不代表传输完成。", **{"class": "p115-hint mb-3"}),
        *[paragraph(f"{item['name']}：{item['message']}", **{"class": "p115-hint"}) for item in result.get("items", [])],
    ], "mdi-playlist-check", opened=bool(result["failed"] or result["skipped"]))


def batch_confirmations(tasks, enabled, min_count=1):
    if not enabled:
        return []
    paused = [t["id"] for t in tasks if t["state"] == "paused"]
    uploadable = [t["id"] for t in tasks if t["state"] in ("queued", "waiting", "paused")]
    controls = []
    for action, keys, title, explanation in (
        ("resume", paused, "批量继续等待", "重置这些暂停任务的自动尝试次数和等待时限，继续只尝试秒传。请先确认暂停原因已处理。"),
        ("upload", uploadable, "批量强制上传", "这些任务将先试秒传，未命中则上传正文。上次请求结果未知时，目标当前不存在也可能延迟出现，请先核对目标，避免重复。"),
    ):
        if len(keys) < min_count:
            continue
        keys = keys[:200]
        button = node("VBtn", f"确认{title} {len(keys)} 项", variant="tonal", color="primary", size="small")
        button["events"] = {"click": {"api": f"plugin/P115InstantWait/batch/{action}",
                                       "method": "POST", "params": {"keys": keys}}}
        controls.append(confirmation(f"{title} · {len(keys)} 项…", explanation, button))
    return controls


def bulk_tools(tasks, enabled):
    controls = batch_confirmations(tasks, enabled, min_count=1)
    children = [paragraph("作用于本页显示的适用任务（每次最多 200 项），不随状态筛选变化。提交时会检查最新状态；不适用项跳过，新入队任务不包含在内。",
                          **{"class": "p115-hint"})]
    if controls:
        children.append(box(controls, "p115-bulk-actions"))
    else:
        children.append(paragraph("暂无可批量操作的任务。" if enabled else "启用插件后可批量处理。", **{"class": "p115-hint"}))
    children.append(paragraph("指定任务的多选、暂停和取消，以及历史记录清理，请在配置页展开对应选项。", **{"class": "p115-hint"}))
    return disclosure("批量操作与记录管理", children, "mdi-playlist-edit")


def task_page(tasks, enabled, error="", max_retries=None, batch_result=None, summary=None, notice="", cooldown_until=0.0):
    now = time.time()
    cooling = bool(enabled and cooldown_until and cooldown_until > now)
    refresh = node("VBtn", "刷新", variant="outlined", size="small",
                   **{"prepend-icon": "mdi-refresh", "class": "p115-refresh-btn", "aria-label": "刷新任务状态"})
    # PageRender uses MP's authenticated API and reloads after each action.
    refresh["events"] = {"click": {"api": "plugin/P115InstantWait/tasks", "method": "GET"}}
    if cooling:
        cooldown_clock = datetime.fromtimestamp(cooldown_until).astimezone().strftime("%H:%M:%S")
        header_chip = status_chip("paused", f"风控冷却至 {cooldown_clock}", tone_override="warning", icon_override="mdi-shield-alert-outline")
    else:
        header_chip = status_chip("running" if enabled else "cancelled",
                                  "后台运行中" if enabled else "等待任务结束" if notice else "插件已关闭")
    content = [node("style", STYLES), box([
        box([node("h2", "任务监控"), header_chip], "p115-heading"), refresh,
    ], "p115-header")]
    if cooling:
        cooldown_clock = datetime.fromtimestamp(cooldown_until).astimezone().strftime("%H:%M:%S")
        remaining = format_eta(cooldown_until - now).removesuffix("后")
        content.append(box([
            icon("mdi-shield-alert-outline", size=22),
            box([
                node("strong", f"115 访问风控冷却中 · 约 {remaining}后恢复（{cooldown_clock}）"),
                paragraph("冷却期间已暂停领取队列任务，也不会消耗自动重试次数；恢复后将自动平滑退避调度。",
                          **{"class": "p115-copy p115-muted mt-1"}),
            ]),
        ], "p115-cooldown-banner", role="status"))
    if notice:
        content.append(node("VAlert", notice, type="info", variant="tonal", **{"class": "mb-4", "role": "status"}))
    if error:
        content.append(node("VAlert", error, title="插件需要检查", type="error", variant="tonal",
                            **{"class": "mb-4", "role": "alert"}))
    if not enabled and not notice:
        content.append(node("VAlert", "请在配置页启用插件；关闭期间队列不会自动重试。",
                            type="info", variant="tonal", **{"class": "mb-4"}))
    groups = {group: [t for t in tasks if group in categories(t).split()]
              for group in ("waiting", "transfer", "attention", "ended")}
    waiting = groups["waiting"]
    if summary is None:
        # Standalone previews can derive a summary from their supplied sample.
        summary = dict(states={s: sum(t["state"] == s for t in tasks) for s in LABELS},
                       methods={m: sum(t["state"] == "completed" and t.get("transfer_method") == m for t in tasks)
                                for m in ("instant", "upload")}, total=len(tasks),
                       next_at=min((t.get("next_at") or 0 for t in waiting), default=None))
    states, methods = summary["states"], summary["methods"]
    count = lambda *values: sum(states.get(value, 0) for value in values)
    waiting_count, completed_count = count("queued", "waiting"), count("completed")
    if cooling and waiting_count:
        wait_caption = "冷却至 " + datetime.fromtimestamp(cooldown_until).astimezone().strftime("%H:%M:%S")
    elif waiting_count and enabled:
        next_at = summary["next_at"] or 0
        wait_caption = "已到时间，等待调度" if next_at <= now else "下次 " + format_eta(next_at - now)
    else:
        wait_caption = "启用后继续调度" if waiting_count else "暂无等待任务"
    instant, upload = methods.get("instant", 0), methods.get("upload", 0)
    known_methods = instant + upload
    if completed_count and known_methods > 0:
        instant_pct = round(instant * 100 / known_methods, 1)
        upload_pct = round(100.0 - instant_pct, 1)
        completed_ratio = (instant_pct, upload_pct)
        completed_caption = f"秒传率 {int(round(instant_pct))}% · 秒传 {instant} · 上传 {upload}" + (
            f" · 未记录 {completed_count - known_methods}" if completed_count > known_methods else ""
        )
    else:
        completed_ratio = None
        completed_caption = f"秒传 {instant} · 上传 {upload}" + (
            f" · 未记录 {completed_count - known_methods}" if completed_count > known_methods else ""
        )
    content.append(box([
        metric("等待秒传", waiting_count, wait_caption, "info", "mdi-clock-outline"),
        metric("执行与上传", count("running", "uploading", "upload_queued"), f"执行中 {count('running', 'uploading')} · 上传排队 {count('upload_queued')}", "primary", "mdi-cloud-upload-outline"),
        metric("需要处理", count("paused", "failed"), f"暂停 {count('paused')} · 失败 {count('failed')}", "error", "mdi-alert-circle-outline", True),
        metric("整理完成", completed_count, completed_caption, "success", "mdi-check-circle-outline", ratio=completed_ratio),
    ], "p115-metrics", **{"aria-label": "全部保留记录状态概览"}))
    content.append(paragraph(f"全库共 {summary['total']} 条记录 · 成功任务仅显示最近完成的 50 条，其他状态全部显示；筛选与批量操作作用于当前显示的记录。",
                             **{"class": "p115-hint mb-3"}))
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
    density_toggle = node("details", content=[
        node("summary", content=[icon("mdi-format-list-bulleted-square", size=15), node("span", "紧凑视图")])
    ], **{"class": "p115-density-toggle", "title": "收起摘要行，单屏浏览更多任务"})
    content.append(box([
        box(filters, "p115-filters", role="radiogroup", **{"aria-label": "筛选任务状态"}),
        box([density_toggle, node("span", f"显示 {len(tasks)} 条", **{"class": "p115-hint"})], "p115-toolbar-meta"),
    ], "p115-toolbar"))
    if batch_result:
        content.append(batch_feedback(batch_result))
    quick_controls = batch_confirmations(tasks, enabled, min_count=2)
    if quick_controls:
        content.append(box([
            box([icon("mdi-lightning-bolt-outline", size=17),
                 node("strong", "快捷批量处理")], "p115-inline-tools"),
            box(quick_controls, "p115-inline-tools"),
        ], "p115-quick-bulk"))
    if tasks:
        rank = {"paused": 0, "failed": 1, "running": 2, "uploading": 2, "upload_queued": 3, "queued": 4, "waiting": 4}
        ordered = sorted(tasks, key=lambda t: (rank.get(t["state"], 5),
                         t.get("next_at") or 0 if t["state"] in ("waiting", "queued") else -(t.get("updated") or 0)))
        content.append(box([task_row(t, enabled, now, max_retries, notice) for t in ordered], "p115-list", role="list",
                           **{"aria-label": "任务列表"}))
        empty_copy = {"waiting": "暂无等待任务", "attention": "没有需要处理的任务", "transfer": "暂无执行或上传任务", "ended": "暂无已结束记录"}
        for group, title in empty_copy.items():
            if not groups[group]:
                content.append(box([icon("mdi-check-circle-outline", size=28), node("h3", title),
                    paragraph("当前显示的记录中没有匹配项；切换状态查看其他任务。", **{"class": "p115-muted"})],
                    "p115-empty p115-filter-empty", **{"data-filter": group}))
    else:
        content.append(box([icon("mdi-cloud-clock-outline", size=32),
            node("h3", "还没有等待任务"),
            paragraph("在配置页启用插件，确认 MP 内置 115 已授权，再发起本地到 115 的视频整理。",
                      **{"class": "p115-copy p115-muted"})], "p115-empty"))
    content.append(bulk_tools(tasks, enabled))
    content.append(paragraph("概览统计全部保留记录，手动清理后数量会减少；完整整理结果也可在 MP 整理历史查看。更新于 " + datetime.fromtimestamp(now).astimezone().strftime("%m-%d %H:%M:%S %z") +
                             " · 时间按 MP 时区显示，点击刷新更新。", **{"class": "p115-footer"}))
    return [node("VContainer", content=content, **{"class": "p115-ui pa-0"})]
