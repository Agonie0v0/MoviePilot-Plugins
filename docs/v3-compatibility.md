# MoviePilot V3 兼容性报告

本次适配以官方 `v3` 分支的 **V3.1.0** 为依据，固定源码提交 `3f206171e7962be4ae901682c266385e1fcc1c1a`。验证日期：2026-10-04。

| 插件 | V3 版本 | 系统版本范围 | 改动 |
| --- | --- | --- | --- |
| P115InstantWait | 1.0.0 测试版 | >=3.1.0,<3.2.0 | 独立暂存队列、原生持久计划重放、同历史 ID 完成、备份与校验 |
| qmjsign | 2.0.0 | >=3.1.0,<4.0.0 | SDK 配置/事件/日志/基类、MessageType、API response_model |
| autocleanunlinkedseed | 2.0.0 | >=3.1.0,<4.0.0 | SDK 下载器服务、事件/通知、扫描停止保护、删除回执计数 |

V3 使用独立目录及 `package.v3.json`；旧索引显式 `v3:false`，避免缺少 V3 版本时错误回退。原 V2 源码及版本保持原状。

## 115 整理接管

V3 的整理已迁移为持久准入、冻结计划、步骤回执、租约及原子终态结算。V2 的序列化任务和直接数据库回写不能继续沿用。

插件在原生宿主计划执行之前返回失败等待结果；后台只准备并校验文件，再通过公开 `redo_transfer_history` 登记原持久任务的重试意图。宿主恢复调度器执行原计划并完成结算。原生历史 upsert 更新同 ID；成功后宿主会清除该记录的失败任务映射，因此成功确认按保存的历史 ID 查询。

当前没有公开异步上传闸门，仍需对少量内部方法做可撤销接管。安装时校验版本、接口签名、查询端口和其他插件的接管情况；停用时等待工作线程及原生整理退出后恢复接口。不能将此实现视为任意 V3 小版本均兼容。

原生步骤的执行、重放、租约和历史写入仍由宿主负责。插件对云盘落地提供精确文件 ID、大小和可取得的 SHA1 证据；覆盖和单文件旧目标清理变为可恢复备份。目录替换会提前拒绝，不调用永久删除。

## 验证范围

28 项 Python 3.14 隔离测试使用固定官方源码中的 FileItem、TransferInfo、执行证据类、宿主计划执行方法、TransHandler 整理与覆盖方法、原生步骤 runner 的不确定结果探测，以及真实 TransferHistory 模型和 upsert 方法，在临时 SQLite 数据库验证。媒体识别、调度器、完整终态事务/outbox、任务租约及网络服务使用隔离端口，未启动完整 MP 服务器。原有 V2 的 78 项回归测试也已通过。

测试覆盖原失败记录到同 ID 成功、等待期间不显示成功、copy/move、回写中断与步骤回执重放、次数/时间上限的两种策略、批量强制上传、并行任务不被上传阻塞、源变化暂停、覆盖与旧目标备份、目录清理拒绝、V2 遗留队列保留、重启恢复、原生人工复核、停用补丁恢复、V3 SDK 导出、HTTP 返回模型，以及签到和删种入口。V2 现有回归测试单独执行。

**尚未验证**：真实 MP V3 部署的完整调度和数据库迁移、115 授权/OSS/限流及上传投递、实际消息渠道投递、论坛账号签到、真实下载器删种。测试通过表示上述隔离合同通过，不代表已完成实机验收。

## 运行验证

在独立测试环境安装 `tests/v3/requirements.txt`，准备固定上游源码：

```powershell
git clone --branch v3 --depth 1 https://github.com/jxxghp/MoviePilot.git C:/Temp/mp-v3-reference
git -C C:/Temp/mp-v3-reference fetch --depth 1 origin 3f206171e7962be4ae901682c266385e1fcc1c1a
git -C C:/Temp/mp-v3-reference checkout --detach 3f206171e7962be4ae901682c266385e1fcc1c1a
$env:MP_V3_REFERENCE = 'C:/Temp/mp-v3-reference'
python -m pytest tests/v3 -q
python scripts/build.py --generation v3
```

V2 与 V3 测试各有独立的宿主模块隔离器，请分进程运行。V2 命令为 `python -m unittest tests.test_queue_and_api tests.test_ui_schedule tests.test_mp_integration`，需设置原 V2 README 记录的 `MP115_REFERENCE`。

## 升级步骤

1. 备份 MP 数据库、插件配置和数据目录，在 V2 完成或取消 115 等待任务。
2. 按官方升级流程升级至 V3.1.x；保留原配置和插件数据，不使用 V2 本地安装脚本。
3. 刷新本仓库插件市场，更新三款插件；确认 115 为 1.0.0，另两款为 2.0.0。
4. 检查通知渠道、目录映射和标签；用一部测试视频验证等待、手动上传、同一条整理记录成功的全流程，再恢复日常任务。

删种插件的硬链接数不能证明云盘已上传。请将自动删种范围限制为硬链接入库任务，并排除 115 等待任务的标签。

官方参考：[V3 适配指南](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/docs/V3_Plugin_Adaptation.md)、[插件开发指南](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/docs/Plugin_Development.md)、[固定 V3 源码](https://github.com/jxxghp/MoviePilot/tree/3f206171e7962be4ae901682c266385e1fcc1c1a)。
