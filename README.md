# MoviePilot Plugins

这是 [MoviePilot](https://github.com/jxxghp/MoviePilot) 的个人插件仓库，提供独立的 V2 和 V3 插件版本。

## 安装

推荐使用 MoviePilot 的第三方插件源：

1. 打开 MoviePilot 的“设置 -> 插件 -> 插件源”。
2. 添加仓库地址：`https://github.com/Agonie0v0/MoviePilot-Plugins`
3. 刷新插件市场，搜索需要的插件并安装。

MoviePilot 会按系统版本读取 `package.v2.json` 或 `package.v3.json`，自动安装对应源码。升级 V3 后刷新插件市场并更新这三个插件即可。保留原插件配置和数据目录，不要手工把 V2 源码复制到 V3。

| 插件 | V2 版本 | V3 版本 | V3 支持范围 |
| --- | --- | --- | --- |
| 115秒传等待 | 0.2.6 | 1.0.2 测试版 | 3.1.x |
| 阡陌居签到 | 1.2.7 | 2.0.0 | >=3.1.0,<4.0.0 |
| Emby智能入库删种 | 原版本保留 | 2.0.0 | >=3.1.0,<4.0.0 |

V3 实现按官方 V3.1.0 源码验证。115 插件使用受版本与接口校验保护的整理接管，因此暂不放开 V3.2 及以上版本。详见 [V3 兼容性与升级说明](docs/v3-compatibility.md)。

升级前请先完成或取消 115 插件的 V2 等待任务。旧队列会保留并暂停，不会在 V3 中用旧整理快照自动执行。签到插件保留原账号配置、Cookie 与历史；删种插件保留原标签和目录映射。

## 目录结构

```text
package.v2.json       # V2 插件索引
package.v3.json       # V3 插件索引
plugins.v2/            # MoviePilot V2 插件源码
plugins/               # 兼容旧版的插件源码
plugins.v3/            # MoviePilot V3 插件源码
icons/                 # 插件图标资源
```

各插件的功能、配置项和使用限制请查看对应插件目录中的 README。仓库版本以对应的 `package*.json` 索引为准。

[115秒传等待](plugins.v2/p115instantwait/README.md)：适配 MoviePilot V2.15.6 内置 115，限次等待秒传，达到次数或时间上限后可自动强制上传或待人工处理，成功后更新原整理记录。当前为 0.2.6 测试版，支持清理历史空暂存目录及选择性删除插件已结束记录。

V3 使用说明：[115秒传等待](plugins.v3/p115instantwait/README.md)、[阡陌居签到](plugins.v3/qmjsign/README.md)、[Emby智能入库删种](plugins.v3/autocleanunlinkedseed/README.md)。

## 相关链接

- [MoviePilot](https://github.com/jxxghp/MoviePilot)
- [本插件仓库](https://github.com/Agonie0v0/MoviePilot-Plugins)
