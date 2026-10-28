# MoviePilot V3 兼容性报告

## 2026-10-28：115秒传等待 1.2.1 / MP 3.2.1

当前适配基线为官方 V3.2.1 提交 `be65e047b0e64b216420ba38bec7a087409552ff`，同时回归下文的 V3.1.0 固定源码。插件清单沿用 1.2.0 的 V3 范围及接口探测，不重新增加小版本硬限制；未测试的版本不能据此视为已验证。

- **根因**：`TransferPlanningOwner._TransferChain__select_storage_oper()` 在没有外部存储接管时返回 `None`，由文件管理模块稍后选择内置存储。旧插件在该模块之前以 `isinstance` 检查，误把两个正常空值判为接管冲突。现在仅对 `None` 补全内置 LocalStorage / U115Pan，实际外部对象（包括布尔值为假的对象）仍会拒绝。
- **覆盖预执行**：3.2.1 会先暂存、备份、提升，再清理旧目标。插件在内存中模拟这些路径及文件身份，保存待删除项时还原其原始正式路径，不执行真实重命名、不创建远端暂存文件。如果原生暂存或备份路径已有遗留文件则暂停。后台仍按插件既有语义删除原生允许覆盖的精确旧文件 ID，再直传正式路径；不提供 MP 原生暂存覆盖的旧版本回滚保证。
- **移动与重放**：准备好的正式文件在原生覆盖判定前隐藏，在 `materialize_target` 执行或回执重放完成后可见，以满足 3.2.1 删除本地源前的目标大小及文件 ID 校验。宿主仍负责持久步骤和原历史 ID 结算。
- **升级恢复**：安装 1.2.1 后重启 MP，确认插件版本并启用，在 MP 整理历史中重试此前误报的失败任务。这些任务此前尚未入插件队列，无需删除队列或更换存储配置。

验证使用 Python 3.14，测试隔离媒体服务、调度和 115 网络，执行真实官方存储选择、原生计划执行、TransHandler、持久步骤恢复及历史模型方法。MP 3.2.1：`python -m pytest tests/v3 -q` **51 通过**；MP 3.1.0：**48 通过、3 跳过**（3.2 专属检查）；`python -m unittest tests.test_direct_upload -q` **26 通过**。两套源码分别通过 `MP_V3_REFERENCE` 指定。测试覆盖空值与显式内置存储、真实外部接管拒绝、copy/move、覆盖裁决、无远端重命名、移动前目标被替换、暂存遗留保护及成功回执重放。

没有连接用户实际 MP 服务或真实 115 账号，完整调度、账号授权、OSS 上传及消息投递仍需实机确认。其他两款插件未修改。

复现 3.2.1 验证时，准备官方源码并检出上述提交，再执行：

```powershell
$env:MP_V3_REFERENCE = 'C:/Temp/mp-v3.2.1-reference'
python -m pytest tests/v3 -q
python -m unittest tests.test_direct_upload -q
python scripts/build.py --generation v3
```

## 初始适配记录（2026-10-07，保留历史）

本次适配以官方 `v3` 分支的 **V3.1.0** 为依据，固定源码提交 `3f206171e7962be4ae901682c266385e1fcc1c1a`。验证日期：2026-10-07。

| 插件 | V3 版本 | 系统版本范围 | 改动 |
| --- | --- | --- | --- |
| P115InstantWait | 1.1.2 测试版 | >=3.1.0,<3.2.0 | 正式路径直传、原生持久计划重放、同历史 ID 完成、原生覆盖判断与校验 |
| qmjsign | 2.0.0 | >=3.1.0,<4.0.0 | SDK 配置/事件/日志/基类、MessageType、API response_model |
| autocleanunlinkedseed | 2.0.0 | >=3.1.0,<4.0.0 | SDK 下载器服务、事件/通知、扫描停止保护、删除回执计数 |

V3 使用独立目录及 `package.v3.json`；旧索引显式 `v3:false`，避免缺少 V3 版本时错误回退。V2 同步更新至 0.3.1，使用各自宿主的独立桥接逻辑。

## 115 整理接管

V3 的整理已迁移为持久准入、冻结计划、步骤回执、租约及原子终态结算。V2 的序列化任务和直接数据库回写不能继续沿用。

插件在原生宿主计划执行之前返回失败等待结果；后台只准备并校验文件，再通过公开 `redo_transfer_history` 登记原持久任务的重试意图。宿主恢复调度器执行原计划并完成结算。原生历史 upsert 更新同 ID；成功后宿主会清除该记录的失败任务映射，因此成功确认按保存的历史 ID 查询。

当前没有公开异步上传闸门，仍需对少量内部方法做可撤销接管。安装时校验版本、接口签名、查询端口和其他插件的接管情况；停用时等待工作线程及原生整理退出后恢复接口。不能将此实现视为任意 V3 小版本均兼容。

原生步骤的执行、重放、租约和历史写入仍由宿主负责。插件对云盘落地提供精确文件 ID、大小和可取得的 SHA1 证据；直接写正式目录和文件名，复用原生计划预执行收集允许删除的文件，不提交预执行回执或删除源文件。后台核对并删除这些精确文件 ID，再直接上传；最终由原生持久计划回写。不再创建暂存或备份目录。目录替换提前拒绝；文件覆盖沿用 MP 先删除再上传的顺序，上传失败时不提供旧版本回滚。

## 验证范围

V3 Python 3.14 隔离测试使用固定官方源码中的 FileItem、TransferInfo、执行证据类、宿主计划执行方法、TransHandler 整理与覆盖方法、原生步骤 runner 的不确定结果探测，以及真实 TransferHistory 模型和 upsert 方法，在临时 SQLite 数据库验证。媒体识别、调度器、完整终态事务/outbox、任务租约及网络服务使用隔离端口，未启动完整 MP 服务器。V2 回归和两代直接上传保护测试单独执行。

测试覆盖原失败记录到同 ID 成功、等待期间不显示成功、copy/move、回写中断与步骤回执重放、次数/时间上限的两种策略、批量强制上传、并行任务不被上传阻塞、源变化暂停、四种原生覆盖策略、覆盖事件否决、旧目标清理、删除进度恢复及身份变化保护、无临时目录或远端移动、旧暂存任务禁止恢复、目录清理拒绝、V2 遗留队列保留、重启恢复、原生人工复核、停用补丁恢复、V3 SDK 导出、HTTP 返回模型，以及签到和删种入口。V2 现有回归测试单独执行。

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
3. 刷新本仓库插件市场，更新三款插件；确认 115 为 1.1.2，另两款为 2.0.0。
4. 检查通知渠道、目录映射和标签；用一部测试视频验证等待、手动上传、同一条整理记录成功的全流程，再恢复日常任务。

删种插件的硬链接数不能证明云盘已上传。请将自动删种范围限制为硬链接入库任务，并排除 115 等待任务的标签。

官方参考：[V3 适配指南](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/docs/V3_Plugin_Adaptation.md)、[插件开发指南](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/docs/Plugin_Development.md)、[固定 V3 源码](https://github.com/jxxghp/MoviePilot/tree/3f206171e7962be4ae901682c266385e1fcc1c1a)。

## 2026-10-12 记录管理更新

V2 0.3.6 / V3 1.1.2 增加全库统计、50 条预览及配置页全库分页搜索。V3 保留独立 SDK 与整理桥接，新增搜索及批量接口使用显式 response_model；finalizing 纳入运行统计、保持不可操作且不可清理。两版保留各自宿主版本限制。

本次验证通过：149 项通用/V2 回归测试及 39 项 V3 固定源码兼容测试；两版安装包构建通过。V2 全量回归修正了旧通知测试对 FastAPI 的不完整替身，使用真实 Body/HTTPException 合同。前端按 V2 FormRender 和 V3 前端提交 `7de709c124b382ea931730926f4d894c83e3d52e` 的 FormRender、MoviePilotAPI 返回合同核对；未连接用户正在运行的 MP 或真实 115 账号。
