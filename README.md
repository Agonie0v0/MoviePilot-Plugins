# MoviePilot Plugins

这是 [MoviePilot](https://github.com/jxxghp/MoviePilot) 的个人插件仓库，当前以 V2 插件为主。插件索引位于 [`package.v2.json`](./package.v2.json)，源码位于 [`plugins.v2`](./plugins.v2)。

## 插件列表

| 插件 | ID | 当前版本 | 作用 |
| --- | --- | --- | --- |
| 115 秒传整理 | `u115instant` | `1.5.0` | 本地视频移动到内置 115 时优先秒传，未命中则保留源文件并延迟重试 |
| Emby 智能入库删种 | `autocleanunlinkedseed` | `1.0` | 监听 Webhook 并定时清理已失去硬链接的下载任务，支持标签过滤 |
| 阡陌居签到 | `qmjsign` | `1.2.7` | 自动签到、领取威望红包、账号密码登录续期 Cookie 和历史记录 |

## 安装

推荐使用 MoviePilot 的第三方插件源：

1. 打开 MoviePilot 的“设置 -> 插件 -> 插件源”。
2. 添加仓库地址：`https://github.com/Agonie0v0/MoviePilot-Plugins`
3. 刷新插件市场，搜索插件名称并安装。

MoviePilot V2 会读取仓库根目录的 `package.v2.json`，插件源码对应 `plugins.v2/<插件 ID>`。手动部署时，请保持这个目录结构，不要把 V2 插件复制到 V1 或 V3 目录。

## 115 秒传整理

插件只保护“本地 -> 内置 115”的视频移动整理。115 上传初始化返回未命中秒传时，插件会立即结束本次普通上传，保留本地源文件并进入等待队列。

- 默认首次重试：10 分钟。
- 第二次重试和后续间隔：配置页可分别设置，默认 120 分钟和 360 分钟。
- 默认最多自动重试：5 次，不含首次整理；超过上限后转人工处理。
- `/115instant_force`：人工确认后跳过秒传预检，执行 MP 原生上传。
- 115 请求层使用进程内并发锁，自动重试和人工强制上传不会同时进入 115。
- 插件读取 MP 内置 115 的风控截止时间，并维护自己的默认 1 小时冷却；冷却期间只顺延任务，不消耗重试次数。

配置对应媒体库整理方式建议选择“移动”，覆盖模式选择“不覆盖”。字幕、NFO、封面等附加文件仍按 MoviePilot 原生逻辑处理。

详细说明见 [`plugins.v2/u115instant/README.md`](./plugins.v2/u115instant/README.md)。

## 其他插件

### Emby 智能入库删种

监听 MoviePilot Webhook，并按定时周期扫描下载器。检测到下载完成且源文件已经失去硬链接时，自动删除下载任务及其文件。支持 qBittorrent、Transmission、目录映射、包含标签和排除标签。

命令：`/clean_unlinked_seeds`

### 阡陌居签到

支持 Cookie 或账号密码登录，自动签到、领取每日威望红包、更新 Cookie，并在插件页面展示签到历史与财富信息。

详细说明见 [`plugins.v2/qmjsign/README.md`](./plugins.v2/qmjsign/README.md)。

## 当前验证状态

115 账号目前处于风控冷却，暂时无法执行真实接口测试。当前已完成插件代码编译、索引 JSON 校验，以及并发锁和冷却顺延逻辑的本地行为测试；真实 115 秒传、风控响应和强制上传结果需要冷却解除后再验证。

## 相关链接

- [MoviePilot](https://github.com/jxxghp/MoviePilot)
- [本插件仓库](https://github.com/Agonie0v0/MoviePilot-Plugins)
- [115 秒传整理插件源码](./plugins.v2/u115instant/__init__.py)
