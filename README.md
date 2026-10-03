# MoviePilot Plugins

这是 [MoviePilot](https://github.com/jxxghp/MoviePilot) 的个人插件仓库，当前以 V2 插件为主。

## 安装

推荐使用 MoviePilot 的第三方插件源：

1. 打开 MoviePilot 的“设置 -> 插件 -> 插件源”。
2. 添加仓库地址：`https://github.com/Agonie0v0/MoviePilot-Plugins`
3. 刷新插件市场，搜索需要的插件并安装。

MoviePilot V2 会读取仓库根目录的 `package.v2.json`，插件源码位于 `plugins.v2/<插件 ID>`。手动部署时，请保持仓库目录结构，不要把 V2 插件复制到 V1 或 V3 目录。

## 目录结构

```text
package.v2.json       # V2 插件索引
plugins.v2/            # MoviePilot V2 插件源码
plugins/               # 兼容旧版的插件源码
plugins.v3/            # MoviePilot V3 插件源码
icons/                 # 插件图标资源
```

各插件的功能、配置项和使用限制请查看对应插件目录中的 README。仓库版本以对应的 `package*.json` 索引为准。

[115秒传等待](plugins.v2/p115instantwait/README.md)：适配 MoviePilot V2.15.6 内置 115，限次等待秒传，支持手动强制上传，成功后更新原整理记录。当前为 0.2.0 测试版，尚待真实账号验证。

## 相关链接

- [MoviePilot](https://github.com/jxxghp/MoviePilot)
- [本插件仓库](https://github.com/Agonie0v0/MoviePilot-Plugins)
