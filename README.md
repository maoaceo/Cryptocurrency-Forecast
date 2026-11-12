# 币安预测市场自动交易

使用 Binance 官方 Prediction Trading SAPI REST API 获取行情，默认通过 SAPI 执行；也可选择宿主机 Binance Agentic Wallet CLI（baw）执行路径。没有手动下单界面。默认 **PAPER（模拟），引擎关闭**。一个 FastAPI 进程、一个 SQLite 数据库、一个容器，仪表盘只发布到 `127.0.0.1:8790`。

## 安装与启动

```bash
cd ~/workspace/prediction-autotrader-opensource
cp .env.example .env
# 编辑 .env，至少填写 APP_PASSWORD；设置独立、足够长的管理密码。
mkdir -p data secrets
# 容器以 UID 10001 运行，确保挂载目录可写。
sudo chown 10001:10001 data
chmod 700 data secrets
chmod 600 .env
# 如使用 Vertex，把服务账户 JSON 放进 secrets/vertex-sa.json，确保 UID 10001 可读。
docker compose up -d --build
```

打开 <http://127.0.0.1:8790>，通过密码表单登录。会话有效期 8 小时，cookie 为 HttpOnly / SameSite=Strict；写操作额外校验会话 CSRF token。未配置密码时拒绝所有登录。支持纯 HTTP 直连（内网/本地）：HTTP 请求不附加 Secure，实际 HTTPS 或反向代理传入 `X-Forwarded-Proto=https` 时附加 Secure。公网暴露建议套 HTTPS，代理应覆盖客户端传入的转发头。

环境变量：

| 变量 | 用途 |
| --- | --- |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | 具有预测交易权限的 HMAC API Key |
| `APP_PASSWORD` | 仪表盘密码，必填 |
| `CPA_BASE_URL` / `CPA_API_KEY` | OpenAI-compatible 网关，基础地址可以包含 `/v1` |
| `WB_BASE_URL` / `WB_API_KEY` | WorkBuddy OpenAI-compatible 网关 |
| `VERTEX_SA_JSON` | 容器中的服务账户路径，例如 `/app/secrets/vertex-sa.json` |
| `DB_PATH` | 可选；默认 `/app/data/trader.db`，STOP 文件与数据库同目录 |

未提供模型配置或模型调用失败时对应投票弃权。模型名称严格按需求传给供应商；是否有这些模型的访问权取决于网关和 Google 项目。Vertex 使用全局 `aiplatform.googleapis.com` endpoint，服务账户需要相应 Vertex AI 权限、启用 API 和计费，以及所请求模型与 Google Search grounding 的访问权。服务账户 JWT 用系统 OpenSSL 签名，Python 直接依赖仅 fastapi、uvicorn、httpx。

不使用 Docker 时：Python 3.11+、系统 OpenSSL，运行 `python3 -m venv .venv`、`.venv/bin/pip install -r requirements.txt`，再运行 `.venv/bin/uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8790 --workers 1`。必须只启动一个 worker；不支持多副本共享交易状态。

## 独立测试部署

在本开源副本目录准备独立 `.env`、`data/` 和 `secrets/`，不要复用其他实例的数据库或凭据。设置测试专用 `APP_PASSWORD`，HTTP 会话自动适配，无需设置 Cookie 环境变量，保持默认 PAPER 与引擎关闭。

```bash
APP_PORT=18790 docker compose -p prediction-autotrader-opensource up -d --build
```

访问 <http://127.0.0.1:18790>。`APP_PORT` 只影响宿主机端口，数据仍写入本副本的 `data/`。项目名、端口和数据均与其他实例隔离。运行离线回归：

```bash
.venv/bin/python smoke_test.py
.venv/bin/python signals_test.py
.venv/bin/python baw_test.py
.venv/bin/python dashboard_test.py
```

## 后台自定义配置

登录仪表盘后，先在「模型服务商」卡片添加名称、Base URL、API Key 和开关，点击「获取模型」获取该服务商的模型列表（15 秒超时），再点击保存全部配置。获取模型支持尚未保存的地址与密钥；密钥框留空会使用该服务商已保存的密钥。模型列表保存在服务商的 `models` 字段，多个槽位共用，无需重复添加服务商。

| 配置区块 | 在后台填写 | 环境变量回退 |
| --- | --- | --- |
| 模型服务商（`providers`） | `id`、`name`、`base_url`、`api_key`、`enabled`、`models: []`；支持增删改，允许空密钥草稿 | 运行时继续使用各槽位原有环境变量回退 |
| 搜索 API（`search_api`） | 搜索方式 `provider`、服务商 `provider_id`、`model`、`enabled`；`openai_compatible` 使用服务商与模型下拉框，模型可手填；Vertex Grounding 路径不变 | `SEARCH_API_PROVIDER`、`SEARCH_API_BASE_URL`、`SEARCH_API_API_KEY`、`SEARCH_API_MODEL`、`SEARCH_API_ENABLED`；Vertex 凭据使用 `VERTEX_SA_JSON` |
| 模型 API（`model_apis`） | 投票者 `id`、显示名 `name`、服务商 `provider_id`、`model`、`enabled`；槽位不再输入地址或密钥 | CPA 两票使用 `CPA_BASE_URL` / `CPA_API_KEY`、`CPA_ASTRA_MODEL` / `CPA_SOL_MODEL`、`CPA_ASTRA_ENABLED` / `CPA_SOL_ENABLED`；WorkBuddy 使用 `WB_BASE_URL` / `WB_API_KEY` / `WB_MODEL` / `WB_ENABLED` |
| 币安钱包（`baw_bridge`） | 桥接/钱包状态、钱包地址与登录按钮 | 地址和令牌仅由 `BAW_BRIDGE_URL`、`BAW_BRIDGE_TOKEN` 环境变量提供；`BAW_BRIDGE_ENABLED` 控制启用 |
| 币安 API（`binance_api`） | `api_key`、`api_secret`、`enabled` | `BINANCE_API_KEY`、`BINANCE_API_SECRET`、`BINANCE_API_ENABLED` |

加载旧内联配置时，会按地址与密钥合并服务商，并把槽位改为引用；加载只做内存迁移，下次保存时落盘。运行时地址和密钥依次按服务商引用、旧内联字段、现有环境变量解析。普通字段留空会回退环境变量，密钥输入框留空保留已保存值，勾选清除才删除密钥。默认模型仍为 `gpt-6-astra`、`gpt-6.1-sol` 和 `deepseek-v4.1-flash`。禁用服务商会停用其引用槽位。

只有槽位自身 `enabled` 明确为 `true` 时，保存才要求有效服务商引用与该服务商的密钥；未选服务商、服务商不存在或缺密钥时，错误指明槽位与服务商。服务商草稿允许空密钥；`enabled` 为 `null`、未设置或 `false` 的槽位不因缺少凭据阻断保存。SAPI/BAW 只在各自 `enabled=true` 时按环境变量合并后的凭据硬校验；`executor_mode` 选择本身不要求凭据，也不会自动启用服务。全新默认设置可直接保存。非空 URL 仍校验 HTTP(S) 格式。

设置保存到 SQLite 的 `meta.settings` JSON，下一轮请求热生效；修改 `.env` 仍需重建应用进程。操作员 state 的服务商密钥只返回空值及 `api_key_set` 布尔值；游客 state 不包含服务商或其余配置区块。

保存接口要求登录及 CSRF 校验。配置密钥在数据库中以明文持久化，必须保护 `data/`、数据库备份、`.env` 和 `secrets/`，切勿提交到 GitHub；这些路径与日志已由 `.gitignore` 排除。仅填入你信任的端点，自定义模型/搜索端点会收到市场信息与提示词；桥接跨机器部署应使用 HTTPS 或受保护的私有网络，并使用独立随机 token。公开服务官方域名和本地回环地址属于协议或部署示例，仓库不包含实际部署服务器地址或有效凭据。

## 开通 Prediction Trading 权限与 SAS

1. 在 Binance 官方 API Management 创建 HMAC API Key，限制到运行机器的出口 IP，并申请/启用 **Prediction Trading / PREDICTION_TRADE** 权限。普通现货交易权限不足以替代此权限。若账户看不到该项，联系 Binance 支持或对接人员，确认账户、地区及产品访问资格；本程序不能代开权限。
2. 在 Binance 官方预测交易钱包流程中完成钱包注册与 **SAS 授权**，授权的 walletAddress、walletId 必须属于此 API Key 对应账户。官方 Place Order 与 Batch Cancel 明确要求 SAS；它独立于 API Key 的 HMAC 签名。程序从 `wallet/list` 选取并缓存钱包，不收集助记词，不自动执行 SAS 授权。
3. 配置 API Key 后启动系统，等待“系统状态”显示钱包及完整成功同步。`-1002` 显示“权限待开通”；`-2015` 检查 API Key、IP 白名单及权限；`-3026` 检查账户与钱包是否匹配。
4. 确认 SPOT 账户有足够 USDT。执行器显式使用 `fundingSource=CEX`、`accountType=SPOT`；不自动转账。

官方公开 REST 文档未给出 SAS 授权界面的完整点击路径或自助开通 API；具体入口以 Binance 账户当前界面或官方支持指引为准。本系统不会把 API 连通误报为 SAS 已授权：只有实际交易请求能验证最终授权，失败会记录审计。

## 可选 baw 执行路径（宿主机桥接）

后台分段单选保存 `executor_mode=sapi|baw`，执行与同步下一次请求即生效；设置优先于 `EXECUTOR_PATH`，未设置时回退到该环境变量，默认 sapi。选择 sapi 保存必须有已解析的 API Key/Secret（支持环境变量回退）；选择 baw 不要求 SAPI 密钥。钱包卡从 bridge `/status` 获取状态，登录按钮启动宿主机 `baw auth signin`，展示 CLI 链接/文本并轮询结果，会话 5 分钟过期。

`EXECUTOR_PATH=sapi` 保留原有路径。SAPI CEX-funding 路径平台暂停时，可设置 `EXECUTOR_PATH=baw`：仅真钱执行与账户快照改用宿主机 baw；SAPI 行情、信号、策略、风控限额和仪表盘 HTML 保持原样，仍需配置 SAPI 行情 API Key。根据运营方提供的验证，baw 1.12.0 的实时 quote + place 已于 2026-11-09 成功；本仓库新增验证均为离线测试。

baw 需要 Node 与已配对的 `~/.baw`，不能运行在当前 `python:3.11-slim` 应用容器内。先在宿主机安装 Node/npm，然后以桥接服务使用的 **root** 身份安装、配对与检查（服务读取 `/root/.baw`）：

```bash
sudo npm i -g @binance/agentic-wallet
sudo -H baw auth signin
sudo -H baw wallet status --json
sudo -H baw wallet balance --json
```

将本项目代码放在 `/opt/prediction-autotrader`，安装服务。`bridge.env` 与应用 `.env` 中设置同一个独立、足够长的随机 `BAW_BRIDGE_TOKEN`（例如用 `openssl rand -hex 32` 生成）。不要使用示例空值；未配置 token 时桥接拒绝所有请求。

```bash
cd /opt/prediction-autotrader
sudo install -m 600 bridge/bridge.env.example bridge/bridge.env
sudoedit bridge/bridge.env
# 填写 BAW_BRIDGE_TOKEN；BAW_BIN 建议设为 baw 的绝对路径。
# systemd 不继承交互式 shell/nvm PATH；确保 Node 也在服务 PATH 中。
sudo install -m 644 bridge/baw-bridge.service /etc/systemd/system/baw-bridge.service
sudo systemctl daemon-reload
sudo systemctl enable --now baw-bridge
sudo systemctl status baw-bridge
```

应用 `.env`：

```dotenv
EXECUTOR_PATH=baw
BAW_BRIDGE_URL=http://host.docker.internal:8791
BAW_BRIDGE_TOKEN=<与 bridge.env 相同的 token>
```

桥接地址与 token 仅通过环境变量配置，不在后台填写；修改 `.env` 后运行 `docker compose up -d --build`，修改桥接进程环境后重启桥接服务。桥接默认监听 `127.0.0.1:8791`；容器访问时在 `bridge.env` 中显式配置 `BAW_BRIDGE_BIND` 为宿主机私有接口地址，用 `X-Bridge-Token` 常量时间比较鉴权；仅允许应用容器所在的受信网络访问此宿主机端口。默认 URL 使用通用宿主机别名 `host.docker.internal`；Compose 已通过 `extra_hosts: ["host.docker.internal:host-gateway"]` 配置 Linux Docker 宿主机别名。`BAW_BRIDGE_PORT` 在 `bridge.env` 中配置，默认 `8791`；更改后同步修改 URL 并重启服务。

桥接独立限制 BUY 报价不超过 100 USDT，每 UTC 日成功 BUY 下单累计不超过 5000 USDT；日账本原子保存到 `BAW_BRIDGE_LEDGER`（默认 `/opt/prediction-autotrader/bridge/bridge_ledger.json`，目录需可写）。place 必须匹配本进程 10 分钟内签发的报价，报价使用一次后失效；重启后需重新报价。SELL 与 redeem 不受金额上限限制。最多同时运行 4 个 baw 子进程，请求线程与监听 backlog 均限制为 16。

桥接提供 GET `/status`、`/balance`、`/positions`、`/orders` 与 POST `/quote`、`/place`。quote 使用链 56、**人类可读 USDT 字符串**，不发送 wei、`fundingSource` 或 `fundTransferAmount`；place 使用报价返回的 `slippageBps`，超出配置上限则拒绝下单。报价约 60 秒过期，原有有效期、STOP、模式、策略阈值与账户新鲜度复查仍在 place 前执行。place 不自动重试；超时或无法确定结果时保留 UNKNOWN 审计、额度与预留，需人工核对。

账户同步将 baw `positions`/`summary` 映射到现有 `positions.list`/`positions.summary`，订单记录保存到 `order_history`；链 56 USDT 余额映射为 `payment_options.items` 的 `CeDeFi` 可用余额。余额没有 `walletAddress` 时保留原有 SAPI wallet meta。今日盈亏为 `summary.todayRealizedPnl` 加每个持仓当前价值减成本（价值字段支持 `currentValue`、`positionValue`、`value`；成本字段支持 `cost`、`totalCost`、`costBasis`、`initialValue`）。订单历史为最新一页展示数据；当 `total` 大于返回订单数时保留原顺序并标记 `truncated: true`，不视为同步失败。桥接请求 `--limit 200`（实际页大小可能受 CLI 限制）。订单历史列表格式或总数无效、任何持仓缺少可推导字段、钱包未连接或同步失败，均拒绝真钱开仓；无持仓时未实现盈亏为零。150 秒新鲜度要求不变；baw 模式不标记 SAPI 权限待开通。

模拟账户的 `paper_initial_balance` 默认为 100.0 美元，可在交易设置中修改。主概览随模式切换：模拟显示纸面现金、累计盈亏、已实现及未实现盈亏，真钱显示场馆余额和盈亏；模拟账本摘要始终显示初始资金、现金、权益和已实现盈亏。游客沿用真钱视图的字段脱敏规则。

`/api/state` 的 `paper_account` 基于当前 `paper_orders` 全表计算：现金 = 初始资金 + 已结算盈亏 − OPEN 订单投入，权益 = 现金 + OPEN 订单份额 × 当前价格。设置区的「重置模拟余额」经确认后调用 `POST /api/paper/reset`（登录会话及 CSRF 必需），清空模拟订单并在设置中记录 `paper_reset_at`；重置后现金和权益回到已保存的初始资金。重置仅影响模拟账本，日支出计数和真钱记录继续保留。

## 从模拟切换到真钱

先在模拟模式启用引擎，检查决策、价格和模拟结算。缺少 Prediction Trading 权限时仍能登录查看缓存，但需要权限才能拉取相关行情/账户数据，程序不会编造行情。

准备真钱交易时：先停止引擎；核对完整账户同步无错误、钱包与资金、今日额度和所有未结算模拟仓位；保存模式“真钱”与参数，再启用引擎。模式与参数持久化到 SQLite。切换模式不清除旧记录、未平仓记录或今日已用额度；模拟/真钱共享日支出计数，防止切换绕过上限。面板无手动下单功能。

紧急停止：

```bash
touch data/STOP
```

STOP 在新评估前及实际下单前检查；已提交给 Binance 的订单无法由 STOP 撤回。移除文件并在面板启用引擎才可恢复。停止引擎不影响账户同步及模拟结算。

## 策略与风控

仅选择 BTCUSDT / ETHUSDT 加密涨跌主题、明确开放交易的变体，剩余时间严格大于该周期 25%。可设置最短/最长周期。YES/NO 根据明确的 UP/DOWN 命题映射；无法确定代币方向时跳过。

市场开盘后，一经发现立即运行一次投票，不额外等待；300 秒和 900 秒周期均如此。首次发现时剩余不足 60 秒则不评估。同一标的窗口在引擎进程内只评估一次；提前发现的窗口仅在开盘时评估，不进行开盘前评估。模型完成后仍刷新市场价格，真钱下单时仍获取新报价并执行原有风控。

新闻先行：Vertex `gemini-3.8-flash` + Google Search grounding；每个标的缓存 120 秒，其事实简报注入后续模型提示词。六个本地一分钟收盘 K 线投票为 EMA 趋势、Wilder RSI(14)、MACD、一小时动量、布林带位置和成交量趋势。CPA 使用 `gpt-6-astra`、`gpt-6.1-sol`，WorkBuddy 使用 `deepseek-v4.1-flash`，共 10 个投票者。每次模型投票最多 45 秒，只接受严格 JSON `{p_up, reason}`，非法概率和失败调用弃权。

集成概率为所有 `status=ok` 投票的加权平均。每次评估重新读取 `meta.voter_stats`：少于 30 个有效评分窗口的投票者权重为 1.0，否则为 `clamp(2 × correct / scored, 0.3, 1.5)`；当前权重通过 `/api/state` 的 `meta.voter_weights` 返回。全部弃权则跳过。这些启发式概率未经过回测校准，不代表收益保证。

P(up) ≥ 0.5 选择涨，否则选择跌。有效投票中，与集成概率方向相同且 `abs(p_up - 0.5) ≥ 0.02` 的票数必须达到 `min_agree_votes`（整数 0–12，默认 9）；不足时在查询最新价格、报价或下单前跳过，原因为“共识不足”。设置为 0 关闭共识门槛。选定方向概率必须达到可配置的概率阈值（默认 0.62），概率减当前结果价格及真钱新报价的优势必须达到最低优势 `edge_min`（范围 -1–1，默认 0.05）。模型完成后重新查询市场价格。每个变体的评估至少间隔 600 秒，冷却与全部决策持久化，包括跳过、失败及中断中的评估；已有本系统未平仓或待确认订单时不重新评估。

每次完成的评估（成交或跳过）追加到 `eval_history`，保留最新 5000 条；最近决策列表的保留方式不变。结算价格回填也处理这些评估窗口，沿用相同现货开盘、结束价格来源：结束价高于开盘价为涨，否则为跌。投票 `p_up > 0.52` 为涨、`p_up < 0.48` 为跌，其余及弃权不计分。评分单独持久化，`meta.voter_stats` 统计最新 300 个已评分窗口；同一投票者在同一窗口重复评估只保留最近一次有效调用的评分。历史数据中残留的已删除投票者 `gemini-3.8-flash`、`gemini-4-argon-high` 不参与评分统计和权重计算。

硬限制：每笔 1–100 USDT（默认 10），UTC 每日支出上限可设为 1–5000 USDT（默认 100），当日已实现加未实现盈亏 ≤ 亏损停机金额的负值时停机（可设为 0.01–1000 USDT，默认 50），最多 50 个本系统未平仓变体（默认 5）。日额度预留、订单占用和审计先于外部下单持久化。真钱时账户/PnL 不完整、过期或权限异常则拒绝开仓。MARKET 大约最低 1.5 USDT，实际取决于流动性，因此允许配置的 1 USDT 可能被 Binance 拒绝。

真钱执行为 get-quote → place-order-bundle MARKET/FOK；费率参数 200 bps，金额为 18 位 wei 字符串。金额按两位小数向下截断。报价/读取可限次退避；提交订单和撤单永不自动重试。网络故障、服务端超时等可能已成交的结果保留 UNKNOWN 占用和额度，避免重复投注；进程中断后的 PENDING 同样保持占用。运营人员必须先在官方账户核对，保留审计与数据库备份后再处理，不应直接删除数据库重新开始。

模拟订单按当前代币价格成交，份额=投入/价格；模拟账本不模拟手续费、滑点与深度，因此净收益通常较真钱乐观。每分钟轮询市场详情更新估值，仅在明确结算且唯一胜出代币已知时结算，按每份 1 USDT 支付，绝不以到期或价格 1 猜测胜者。多胜者/拆分赔付在缺少明确比例时保持待确认。

真钱当日风控使用 `position/list.summary.todayRealizedPnl` + `pnl/portfolio.totalUnrealizedPnl`。`pnl/query` 是累计记录，不能直接当作今天的已实现盈亏。面板每 15 秒刷新 SQLite 缓存，账户循环每 60 秒同步；单个失败保留最近成功数据与错误/时间，`-1002` 不会使面板崩溃。

## 文件与端点

- `app/binance_client.py`：签名、错误映射、时间同步、金额与所有 API 包装。
- `app/db.py`、`app/risk.py`、`app/sync.py`：持久化、风控/执行/模拟账本、账户缓存。
- `engine/markets.py`、`engine/signals.py`、`engine/runner.py`：选市场、投票、决策循环。
- `app/main.py`、`static/index.html`：密码登录、会话/CSRF、中文面板与设置 API。
- `smoke_test.py`、`signals_test.py`：离线集成与信号测试。
- `Dockerfile`、`docker-compose.yml`、`.env.example`：部署配置。

所有预测接口都位于 `https://api.binance.com/sapi/v1/w3w/wallet/prediction`，包含 `X-MBX-APIKEY`、毫秒 timestamp、recvWindow=5000 与参数原始字节的 HMAC-SHA256 signature。GET 放 query，POST 放 form body；batch-cancel 的 `cancelInfoList[0].orderId` 等键保留原始方括号并通过 raw content 发送。

| 方法 | 相对路径 | 客户端方法 |
| --- | --- | --- |
| GET | `/category/list` | `category_list` |
| GET | `/market/list` | `market_list` |
| GET | `/market/detail` | `market_detail` |
| GET | `/order-book` | `order_book` |
| GET | `/order-book/last-trade-price` | `last_trade_price` |
| POST | `/trade/get-quote` | `get_quote` |
| POST | `/trade/place-order-bundle` | `place_order_bundle` |
| POST | `/trade/batch-cancel` | `batch_cancel` |
| GET | `/order/list` | `order_list` |
| GET | `/order/history` | `order_history` |
| GET | `/position/list` | `position_list` |
| GET | `/position/settled-history` | `settled_history` |
| GET | `/pnl/portfolio` | `pnl_portfolio` |
| GET | `/pnl/query` | `pnl_query` |
| GET | `/wallet/list` | `wallet_list` |
| GET | `/balance/payment-options` | `payment_options` |

另外使用公开 `/api/v3/time` 同步偏移及 `/api/v3/klines` 获取技术指标数据。

## 验证与文档差异

```bash
.venv/bin/python smoke_test.py
.venv/bin/python signals_test.py
.venv/bin/python baw_test.py
.venv/bin/python dashboard_test.py
```

SAPI 测试使用本地 ASGI 桩服务与 httpx ASGITransport，验证真实请求字节签名；baw 测试使用自建内存 HTTP 桩和真实 stdlib HTTP handler 字节解析，模拟 CLI 子进程，不绑定端口、不访问真实网络。包含权限待开通、真钱模拟响应、每日限额、停止开关、模拟结算及面板认证测试。它们验证实现与固定协议样例，不代表真实账户/SAS、供应商模型可用性或实际交易收益已验证。

参考官方 [Trade](https://developers.binance.com/en/docs/catalog/web3-wallet-prediction-trading/api/rest-api/trade)、[Market Data](https://developers.binance.com/en/docs/catalog/web3-wallet-prediction-trading/api/rest-api/market-data)、[Position](https://developers.binance.com/en/docs/catalog/web3-wallet-prediction-trading/api/rest-api/position)、[Wallet](https://developers.binance.com/en/docs/catalog/web3-wallet-prediction-trading/api/rest-api/wallet)。已处理的差异：

- 当前官方详情为 `markets[]`，需求为 `variants[]`：兼容二者；`tradingStatus=OPEN` 与注册状态分开处理。
- `fundingSource` 文档默认 MPC，SPOT 不是资金来源选择器：执行器明确使用 CEX。
- `position/list` 公开页只举出 `ONGOING`，未完整列举 tab 枚举；本实现同时请求 `PENDING_CLAIM`、`ENDED`，若服务拒绝，缓存错误且真钱保持禁止，需按实际账户返回修订。
- 胜者布尔字段与完整拆分赔付比例并不等价：只对唯一明确胜者自动模拟结算。
- SAS 开通步骤和请求指定的模型可用性不由此 REST schema 保证。

本次交付验证（2026-11-09）：Python 3.12 本机环境运行 `smoke_test.py` **11/11 通过**、`signals_test.py` **10/10 通过**、`baw_test.py` **11/11 通过**；全部应用与桥接模块编译通过；Node 对仪表盘脚本语法及账户/持仓/订单/权限待开通样例渲染验证通过。测试依赖来自本机 pip wheel 缓存。Docker 基础镜像按需求为 Python 3.11，但当前主机缺少 Compose 插件且无 Docker daemon 访问权限，未执行镜像构建或容器启动；也未验证真实 Binance/SAS 或真实模型调用。目录未配置真实密钥、未启用引擎、未发送真实订单。
