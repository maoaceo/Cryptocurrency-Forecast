# Security audit — prediction-autotrader

Audit date: 2026-11-09 (UTC). Scope: `app/`, `engine/`, `static/index.html`, `bridge/`, deployment examples, and relevant offline tests. This is a source audit of the workspace snapshot, not an assessment of a running deployment. No live HTTP requests, exchange requests, orders, bridge commands, deployments, or production database operations were performed. Only this report was written inside the project. Actual firewall rules, proxy/TLS configuration, environment values, installed `baw` behavior, and production data were not inspected.

Severity reflects exploit prerequisites and live-money impact. No unconditional critical vulnerability was established. There are **2 high, 6 medium, and 2 low findings** below. A stolen bridge token is particularly dangerous because the bridge can execute trades independently of the application's safety checks.

## Findings

### F1 — High: bridge bearer token and trade commands cross plaintext HTTP; listener binds every IPv4 interface

**Location:** `app/baw_client.py:22,35-36`; `bridge/baw_bridge.py:135`; `docker-compose.yml:12`.

**Exact evidence — `app/baw_client.py`:**

```text
app/baw_client.py:22:         self.base_url = (base_url or os.getenv('BAW_BRIDGE_URL', 'http://host.docker.internal:8791')).rstrip('/')
app/baw_client.py:23:         self.token = token if token is not None else os.getenv('BAW_BRIDGE_TOKEN', '')
app/baw_client.py:24:         self.http = http or httpx.AsyncClient(timeout=35.0, trust_env=False)
app/baw_client.py:35:             response = await self.http.request(method, self.base_url + path,
app/baw_client.py:36:                 headers={'X-Bridge-Token': self.token}, **({'json': body} if body is not None else {}))
```

**Exact evidence — `bridge/baw_bridge.py`:**

```text
bridge/baw_bridge.py:134: if __name__ == '__main__':
bridge/baw_bridge.py:135:     ThreadingHTTPServer(('0.0.0.0', int(os.getenv('BAW_BRIDGE_PORT', '8791'))), Handler).serve_forever()
```

**Attack scenario:** A party able to observe or alter the app-to-host network captures `X-Bridge-Token` from the default HTTP connection, then replays it to obtain account snapshots or submit `/quote`, `/place`, and `/redeem` requests. An active intermediary can also alter account data or quote responses. Binding `0.0.0.0` exposes the service on every host IPv4 interface unless an external firewall prevents access. The repository describes a trusted-network restriction in `README.md:82`, but the service and compose files do not enforce it. This finding does not assert that the actual host is publicly reachable.

**Specific fix:** Prefer a Unix-domain socket with restrictive ownership, or mutually authenticated TLS between app and host. Make the bind address configurable and default to loopback or a dedicated private interface; enforce an allowlist for the application network at the host firewall. Require HTTPS for nonlocal TCP URLs, and rotate the bridge token after moving off plaintext.

### F2 — High: bridge trading authority bypasses all application risk limits and emergency stop

**Location:** `bridge/baw_bridge.py:11-14,24-27,49-66,83-89,108`; contrast `app/risk.py:34-64,114-127`.

**Exact evidence — `bridge/baw_bridge.py`:**

```text
bridge/baw_bridge.py:11: def decimal_string(value, name):
bridge/baw_bridge.py:12:     if not isinstance(value, str) or len(value) > 100 or not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value) or Decimal(value) <= 0:
bridge/baw_bridge.py:13:         raise ValueError(f'{name} must be a positive decimal string')
bridge/baw_bridge.py:14:     return value
bridge/baw_bridge.py:24: def slippage(value):
bridge/baw_bridge.py:25:     if type(value) is not int or not 1 <= value <= 10000:
bridge/baw_bridge.py:26:         raise ValueError('slippageBps must be an integer from 1 to 10000')
bridge/baw_bridge.py:27:     return str(value)
bridge/baw_bridge.py:49:     if path == '/quote':
bridge/baw_bridge.py:50:         if body.get('side') not in ('BUY', 'SELL'):
bridge/baw_bridge.py:51:             raise ValueError('side must be BUY or SELL')
bridge/baw_bridge.py:52:         args = ['prediction', 'trade', 'quote', '--binanceChainId', '56',
bridge/baw_bridge.py:53:                 '--tokenId', identifier(body.get('tokenId'), 'tokenId', True),
bridge/baw_bridge.py:54:                 '--marketTopicId', identifier(body.get('marketTopicId'), 'marketTopicId', True),
bridge/baw_bridge.py:55:                 '--side', body['side'], '--amount', decimal_string(body.get('amount'), 'amount'), '--orderType', kind]
bridge/baw_bridge.py:56:     else:
bridge/baw_bridge.py:57:         args = ['prediction', 'trade', 'place-order', '--quoteId', identifier(body.get('quoteId'), 'quoteId')]
bridge/baw_bridge.py:58:         if 'slippageBps' not in body:
bridge/baw_bridge.py:59:             raise ValueError('slippageBps is required')
bridge/baw_bridge.py:83:     def handle_request(self, post=False):
bridge/baw_bridge.py:84:         token = os.environ.get('BAW_BRIDGE_TOKEN', '')
bridge/baw_bridge.py:85:         supplied = self.headers.get('X-Bridge-Token', '')
bridge/baw_bridge.py:86:         if not token or not hmac.compare_digest(token.encode(), supplied.encode()):
bridge/baw_bridge.py:87:             self.reply(401, {'error': 'unauthorized'})
bridge/baw_bridge.py:88:             return
bridge/baw_bridge.py:89:         paths = ('/quote', '/place', '/redeem') if post else ('/status', '/balance', '/positions', '/orders', '/account')
bridge/baw_bridge.py:108:                 result = subprocess.run([os.getenv('BAW_BIN', 'baw'), *args], capture_output=True, timeout=30, check=False)
```

**Attack scenario:** An attacker holding the bridge token (for example through F1 or compromise of the web-app container) directly requests a quote for `amount="1000000"` with `slippageBps=10000`, then places it. The bridge validates syntax but imposes no app limit of 25 USDT per trade, 100 USDT per day, loss stop, maximum positions, allowed market universe, engine enablement, PAPER/LIVE mode, or `data/STOP`. Exchange balance and CLI restrictions still apply; the requested million-USDT order is not claimed to succeed. Repeated smaller trades can similarly exceed the app's daily cap. `/place` accepts any syntactically valid quote ID without binding it to a bridge-approved risk reservation. The bridge has full trading authority by design; this is a missing independent safety boundary, not an authentication bypass.

**Specific fix:** Put a persistent, atomic risk ledger at the bridge/execution boundary. Check per-order and daily caps, allowed chain/markets, loss/position limits, enablement, and a host-side kill switch immediately before placement. Store issued quote IDs with approved amount, token, expiry, and reservation; reject unmatched or reused placements. Separate read-only account credentials from trade authority. App-container access alone should not grant unrestricted host-wallet trading.

### F3 — Medium: anonymous state response publishes raw account data and internal metadata

**Location:** `app/main.py:38,78-84,125-131`; producers `app/sync.py:32-40,70-78,113-121`; `app/risk.py:136,144`; `engine/signals.py:295-312`.

**Exact evidence — `app/main.py`:**

```text
app/main.py:38:     public_readonly = os.getenv('PUBLIC_READONLY', '0') == '1'
app/main.py:78:             public_request = public_readonly and request.method in ('GET', 'HEAD') and (
app/main.py:79:                 request.url.path in ('/', '/api/state') or request.url.path.startswith('/static/'))
app/main.py:80:             if not session and not public_request:
app/main.py:81:                 if request.url.path == '/':
app/main.py:82:                     return RedirectResponse('/login', status_code=303)
app/main.py:83:                 return JSONResponse({'detail': '请先登录'}, status_code=401)
app/main.py:84:             request.state.session = dict(session[0]) if session else {'token': 'public', 'csrf': 'public'}
app/main.py:125:     @app.get('/api/state')
app/main.py:126:     async def state(request: Request):
app/main.py:127:         meta = {r['key']: json.loads(r['value']) for r in store.rows('SELECT * FROM meta')}
app/main.py:128:         return {'public': public_readonly, 'settings': store.settings(), 'meta': meta, 'decisions': [dict(r) for r in store.rows('SELECT * FROM decisions ORDER BY ts DESC, id DESC LIMIT 10')],
app/main.py:129:                 'paper_orders': [dict(r) for r in store.rows('SELECT * FROM paper_orders ORDER BY ts DESC LIMIT 200')],
app/main.py:130:                 'orders_audit': [dict(r) for r in store.rows('SELECT * FROM orders_audit ORDER BY ts DESC, id DESC LIMIT 10')],
app/main.py:131:                 'daily_spend': [dict(r) for r in store.rows('SELECT * FROM daily_spend')], 'csrf': request.state.session['csrf'], 'now': time.time()}
```

**Exact evidence — `app/sync.py`:**

```text
app/sync.py:32:     async def _call(self,name,method,**params):
app/sync.py:33:         try:
app/sync.py:34:             result=unwrap(await method(**params))
app/sync.py:35:             self.store.set_meta(name,result)
app/sync.py:36:             self.store.set_meta(name+'_status',{'ts':time.time(),'error':None,'permission_pending':False})
app/sync.py:37:             return result,True
app/sync.py:38:         except Exception as exc:
app/sync.py:39:             old=self.store.meta(name+'_status',{})
app/sync.py:40:             self.store.set_meta(name+'_status',{'ts':old.get('ts',0),'last_attempt':time.time(),'error':str(exc),'permission_pending':getattr(exc,'code',None)==-1002})
app/sync.py:70:             wallet_data,wallet_ok=await self._call('wallets',self.client.wallet_list)
app/sync.py:71:             wallets=items(wallet_data,('wallets','walletList'))
app/sync.py:72:             if isinstance(wallet_data,dict) and wallet_data.get('walletAddress'):wallets=[wallet_data]
app/sync.py:73:             # Keep a selected wallet stable; changing wallet while orders exist is unsafe.
app/sync.py:74:             wallet=self.store.meta('wallet',{})
app/sync.py:75:             selected=next((w for w in wallets if w.get('walletId')==wallet.get('walletId')),None)
app/sync.py:76:             if selected is None and not wallet:selected=next((w for w in wallets if w.get('walletAddress') and w.get('walletId')),None)
app/sync.py:77:             if selected:
app/sync.py:78:                 wallet=selected;self.store.set_meta('wallet',wallet)
app/sync.py:116:         async def account_snapshot():
app/sync.py:117:             data=await client.account()
app/sync.py:118:             if not isinstance(data,dict):raise ValueError('baw account response is incomplete')
app/sync.py:119:             return data
app/sync.py:120: 
app/sync.py:121:         account,account_ok=await self._call('baw_account',account_snapshot)
```

**Exact evidence — `app/risk.py`:**

```text
app/risk.py:136:                 self.store.execute("UPDATE orders_audit SET status='SUCCESS',order_id=?,quote_id=?,response_json=? WHERE id=?",(order_id,str(quote_id),json.dumps(result),audit))
app/risk.py:144:                 self.store.execute('UPDATE orders_audit SET status=?,quote_id=?,error=? WHERE id=?',(status,str(quote_id) if quote_id else None,str(exc),audit))
```

**Exact evidence — `engine/signals.py`:**

```text
engine/signals.py:298:         record = {'ts': time.time(), 'error_type': type(exc).__name__, 'error_str': str(exc),
engine/signals.py:299:                   'raw_response_truncated_2000_chars': json.dumps(response, ensure_ascii=False, default=str)[:2000],
engine/signals.py:300:                   'extracted_text_truncated_500': text[:500]}
engine/signals.py:312:             self.store.set_meta('news_debug', records[-4:] + [record])
```

**Attack scenario:** When `PUBLIC_READONLY=1`, any visitor requests `/api/state` and downloads the entire `meta` table, operator settings, recent model decisions, paper orders, raw order-audit rows, and all daily-spend rows. Removing operator HTML at `app/main.py:121-122` does not remove these fields from JSON. A visitor can link the operator to wallet addresses and wallet IDs, inspect balances, positions, trading history, strategy thresholds and model reasoning, and collect troubleshooting output that the page never displays. Raw CLI errors can include internal executable/file paths: `bridge/baw_bridge.py:124-125` returns `str(exc)`, which the Baw client and sync code persist. Raw provider/CLI responses may contain additional sensitive fields now or after an upstream schema change.

**Specific fix:** Create a distinct public serializer with an explicit field allowlist. Return only approved display values (for example aggregate PnL and sanitized public position/order fields). Exclude wallet identifiers, raw `baw_account`/wallet/payment responses, settings, decisions, debug records, raw errors, `response_json`, and operational metadata. Keep the complete operator endpoint authenticated. Use opaque public error codes and server-side logs for details.

**Precise exposure inventory:**

| Item | Result of serialization trace |
| --- | --- |
| Wallet addresses / wallet IDs | Exposed when populated: raw `wallets`, selected `wallet`, `baw_status`, and `baw_account` are returned. Portfolio snapshots can also include `walletAddress` (`app/sync.py:242`). |
| Balances, positions, orders, PnL, payment data | Exposed in raw or normalized account metadata; not limited to what the public page renders. |
| Binance API key and secret | No intentional serialization found. They remain on the client object (`app/binance_client.py:84`); `/api/state` does not serialize that object or the environment. |
| CPA / WB keys, Vertex private key and OAuth access token | No intentional serialization found. They are used in outbound requests / in-memory credentials (`engine/signals.py:200,209,227,235-237,252,362-369`), not assigned to metadata. |
| Bridge token / app password | No intentional serialization found: values remain in environment/client/closure (`app/baw_client.py:23`, `app/main.py:37`). They are not CLI arguments. |
| Unexpected upstream credentials | No actual credential leak established; raw response passthrough means there is no serializer guarantee excluding such fields. An offline synthetic credential field survived unchanged. |
| Internal paths | Not directly enumerated from environment. Can appear in unredacted CLI/system errors; debug metadata also exposes bounded raw model responses. |
| Session tokens and CSRF | `sessions` is not selected. Anonymous CSRF is the literal `public`; it cannot authorize mutations because middleware first requires a real unexpired session. Authenticated callers receive their own CSRF secret, as intended. |

With `PUBLIC_READONLY=0` (the code/compose default), anonymous `/api/state` returns 401. This is a conditional disclosure, not an always-public endpoint or proven API-key leak.

### F4 — Medium: settings can weaken documented strategy and position limits

**Location:** `app/db.py:71-81`; enforcement `app/risk.py:51,123-126`; `engine/runner.py:65-66`; claimed policy `README.md:110-112`, `static/index.html:57`.

**Exact evidence — `app/db.py`:**

```text
app/db.py:71:             elif k=='max_open_positions':
app/db.py:72:                 v=int(v)
app/db.py:73:                 if not 1<=v<=50: raise ValueError('最大持仓必须在 1 到 50 之间')
app/db.py:74:             else:
app/db.py:75:                 v=float(v)
app/db.py:76:                 bounds={'bet_usdt':(1,25),'daily_spend_cap':(1,100),
app/db.py:77:                     'daily_loss_stop':(0.01,50),'prob_threshold':(0.50,1),
app/db.py:78:                     'edge_min':(-0.2,1),'cooldown_seconds':(600,86400*365),
app/db.py:79:                     'slippage_bps':(1,1200),'min_duration':(0,31536000),'max_duration':(1,31536000)}
app/db.py:80:                 low,high=bounds[k]
app/db.py:81:                 if not math.isfinite(v) or not low<=v<=high: raise ValueError(f'{k} 必须在 {low} 到 {high} 之间')
```

**Exact evidence — `app/risk.py`:**

```text
app/risk.py:51:         if count+(0 if reserved else 1)>s['max_open_positions']: return False,'持仓数量已达上限'
app/risk.py:52:         if self.spent()+(0 if reserved else amount)>min(100,s['daily_spend_cap'])+1e-9:return False,'今日额度不足'
app/risk.py:123:             if prob is not None and (prob<latest['prob_threshold'] or prob-price<latest['edge_min']):raise ValueError('策略阈值已收紧')
app/risk.py:124:             if quote.get('averagePrice') is not None and prob is not None:
app/risk.py:125:                 quoted=float(quote['averagePrice'])
app/risk.py:126:                 if not math.isfinite(quoted) or not 0<quoted<1 or prob-quoted<latest['edge_min']:raise ValueError('报价优势不足')
```

**Exact evidence — `README.md`:**

```text
README.md:110: P(up) ≥ 0.5 选择涨，否则选择跌。仅当选定方向概率 ≥ 0.62 且概率减当前结果价格 ≥ 0.05 才允许交易；两阈值只能收紧。模型完成后重新查询市场价格。每个变体的评估至少间隔 600 秒，冷却与全部决策持久化，包括跳过、失败及中断中的评估；已有本系统未平仓或待确认订单时不重新评估。
README.md:111: 
README.md:112: 硬限制：每笔 1–25 USDT（默认 10），UTC 每日支出 ≤ 100（默认 100），当日已实现加未实现盈亏 ≤ -50 时停机（默认 50，可缩小损失额度），最多 5 个本系统未平仓变体。日额度预留、订单占用和审计先于外部下单持久化。真钱时账户/PnL 不完整、过期或权限异常则拒绝开仓。MARKET 大约最低 1.5 USDT，实际取决于流动性，因此允许配置的 1 USDT 可能被 Binance 拒绝。
```

**Attack scenario:** An authenticated operator, stolen session, or malicious same-origin script saves `{"prob_threshold":0.5,"edge_min":-0.2,"max_open_positions":50}`. The server accepts all three. Trades can then pass with probability below the documented 0.62 minimum and negative modeled edge; up to 50 positions are permitted despite the documented hard limit of five. For example, a selected-side probability of 0.55 and price 0.70 passes the weakened strategy checks. This requires authenticated, CSRF-authorized settings access; it is not an anonymous override. Cash, daily-spend, loss, and cooldown hard limits remain separately enforced.

**Specific fix:** Enforce probability >= 0.62, edge >= 0.05, and positions <= 5 in server-side validation and execution-time risk checks if those are the intended hard limits. Use strict numeric types (reject booleans and fractional values for integer fields), validate every `durations` entry, and make the UI and documentation match the enforced policy. If broader tuning is intentional, explicitly distinguish operator-adjustable values from non-overridable execution caps.

### F5 — Medium: session cookie defaults to allowing insecure HTTP transport

**Location:** `app/main.py:115`; `.env.example:9`.

**Exact evidence — `app/main.py`:**

```text
app/main.py:115:         response.set_cookie('session', token, max_age=28800, httponly=True, samesite='strict', secure=os.getenv('COOKIE_SECURE', '0') == '1')
```

**Exact evidence — `.env.example`:**

```text
.env.example:9: COOKIE_SECURE=0
```

**Attack scenario:** A remotely accessed deployment keeps the shipped `COOKIE_SECURE=0` setting or omits it. The browser can send its eight-hour operator cookie over HTTP, exposing it to a network observer who can reuse it and fetch the matching CSRF token. The login password itself is also exposed if login uses HTTP. Loopback-only use limits this risk; a TLS reverse proxy does not by itself add the cookie's Secure attribute.

**Specific fix:** Default Secure to true and reject nonlocal production operation without TLS. Explicitly allow an insecure development mode only for local use. Set the cookie using the `__Host-` prefix with Secure, HttpOnly, Path=/, and SameSite=Strict. Configure HTTP-to-HTTPS redirects and HSTS at the TLS edge.

### F6 — Medium: password rotation and fresh login do not revoke previously issued sessions

**Location:** `app/main.py:39,77,111-115,141-145`.

**Exact evidence — `app/main.py`:**

```text
app/main.py:39:     store.execute('CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, csrf TEXT NOT NULL, expires REAL NOT NULL)')
app/main.py:77:             session = store.rows('SELECT * FROM sessions WHERE token=? AND expires>?', (token, time.time()))
app/main.py:111:         token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
app/main.py:112:         store.execute('DELETE FROM sessions WHERE expires<?', (now,))
app/main.py:113:         store.execute('INSERT INTO sessions VALUES(?,?,?)', (token, csrf, now + 28800))
app/main.py:114:         response = RedirectResponse('/', status_code=303)
app/main.py:115:         response.set_cookie('session', token, max_age=28800, httponly=True, samesite='strict', secure=os.getenv('COOKIE_SECURE', '0') == '1')
app/main.py:141:     @app.post('/logout')
app/main.py:142:     async def logout(request: Request):
app/main.py:143:         store.execute('DELETE FROM sessions WHERE token=?', (request.cookies.get('session'),))
app/main.py:144:         response = JSONResponse({'message': '已退出'})
app/main.py:145:         response.delete_cookie('session')
```

**Attack scenario:** An attacker copies a session cookie or obtains one before the operator changes `APP_PASSWORD`. After the app is restarted with a new password, the SQLite session remains valid until its original eight-hour expiry because validation checks only token and time. A new login also leaves prior sessions active; logout deletes only the presented token. The operator can believe changing the compromised password has ended attacker access while the attacker continues changing live trading settings.

**Specific fix:** Bind sessions to a persisted authentication generation, and increment it on password change / administrative revocation. Provide a revoke-all-sessions operation and use it during credential compromise recovery. Revoke the current prior session on successful reauthentication; add a suitable idle timeout alongside the existing absolute timeout. Store only a hash of the bearer session token to reduce the impact of database-read compromise.

### F7 — Medium: host bridge runs the wallet CLI as root without service confinement

**Location:** `bridge/baw-bridge.service:8-11`; `bridge/baw_bridge.py:108`.

**Exact evidence — `bridge/baw-bridge.service`:**

```text
bridge/baw-bridge.service:6: [Service]
bridge/baw-bridge.service:7: Type=simple
bridge/baw-bridge.service:8: User=root
bridge/baw-bridge.service:9: Environment=HOME=/root
bridge/baw-bridge.service:10: EnvironmentFile=/opt/prediction-autotrader/bridge/bridge.env
bridge/baw-bridge.service:11: ExecStart=/usr/bin/python3 /opt/prediction-autotrader/bridge/baw_bridge.py
bridge/baw-bridge.service:12: Restart=always
bridge/baw-bridge.service:13: RestartSec=5
```

**Exact evidence — `bridge/baw_bridge.py`:**

```text
bridge/baw_bridge.py:108:                 result = subprocess.run([os.getenv('BAW_BIN', 'baw'), *args], capture_output=True, timeout=30, check=False)
```

**Attack scenario:** A vulnerability in the installed `baw`/Node dependency chain, or replacement of its executable through a writable installation/PATH directory, executes with root privileges when a bridge request invokes the CLI. That expands a wallet/bridge compromise to control of the host and other secrets. No remote code execution or writable PATH was established in this audit; root execution is the confirmed unnecessary impact amplifier.

**Specific fix:** Pair the wallet under a dedicated nonroot service account, set an absolute administrator-owned `BAW_BIN`, and grant access only to that account's wallet state. Add systemd confinement appropriate for the CLI, including `NoNewPrivileges=yes`, an empty capability bounding set, `ProtectSystem=strict`, `PrivateTmp=yes`, and narrowly scoped writable wallet paths. Validate CLI compatibility before enabling filesystem restrictions.

### F8 — Medium: bridge permits unauthenticated connection/thread exhaustion and unbounded authenticated subprocess concurrency

**Location:** `bridge/baw_bridge.py:83-86,96-99,107-108,135`.

**Exact evidence — `bridge/baw_bridge.py`:**

```text
bridge/baw_bridge.py:96:                 length = int(self.headers.get('Content-Length', '0'))
bridge/baw_bridge.py:97:                 if not 0 < length <= 16384 or self.headers.get('Transfer-Encoding'):
bridge/baw_bridge.py:98:                     raise ValueError('invalid body length')
bridge/baw_bridge.py:99:                 body = json.loads(self.rfile.read(length))
bridge/baw_bridge.py:107:             def run(args):
bridge/baw_bridge.py:108:                 result = subprocess.run([os.getenv('BAW_BIN', 'baw'), *args], capture_output=True, timeout=30, check=False)
bridge/baw_bridge.py:135:     ThreadingHTTPServer(('0.0.0.0', int(os.getenv('BAW_BRIDGE_PORT', '8791'))), Handler).serve_forever()
```

**Attack scenario:** Any network-reachable party opens many TCP connections and sends incomplete HTTP headers. `ThreadingHTTPServer` assigns threads before token authentication, with no configured socket/header-read deadline or concurrency ceiling. A token holder can additionally hold request-body reads open or launch many parallel CLI processes; each `/account` request invokes five CLI commands sequentially (`bridge/baw_bridge.py:117-120`). The per-command 30-second timeout and 16 KiB request-body limit do not bound connection/thread/process count. Resource exhaustion makes account snapshots stale and blocks the trading application's safe operation. No flooding was performed.

**Specific fix:** Restrict network reachability, set short connection/header/body deadlines, and place the service behind a proxy with connection and request limits. Bound active handlers and CLI processes with a fixed worker pool/semaphore and reject excess work. Limit `/account` concurrency, cap captured output, and enforce an overall request deadline rather than only a per-command deadline.

### F9 — Low: weak-password configurations and distributed guessing exceed the protection of the local login limiter

**Location:** `app/main.py:37,71,100-110`; bridge counterpart `bridge/baw_bridge.py:84-87`.

**Exact evidence — `app/main.py`:**

```text
app/main.py:37:     password = password if password is not None else os.getenv('APP_PASSWORD', '')
app/main.py:71:     failures = {}
app/main.py:100:         ip = request.client.host if request.client else 'local'
app/main.py:101:         now = time.time()
app/main.py:102:         attempts = [t for t in failures.get(ip, []) if t > now - 60]
app/main.py:103:         failures[ip] = attempts
app/main.py:104:         if len(attempts) >= 10:
app/main.py:105:             return HTMLResponse('尝试过于频繁，请稍后重试', status_code=429)
app/main.py:106:         body = (await request.body()).decode('utf-8', errors='replace')
app/main.py:107:         supplied = parse_qs(body).get('password', [''])[0]
app/main.py:108:         if not password or not hmac.compare_digest(supplied.encode(), password.encode()):
app/main.py:109:             attempts.append(now)
app/main.py:110:             return HTMLResponse(LOGIN.replace('请输入管理密码', '密码错误或尚未配置管理密码'), status_code=401)
```

**Exact evidence — `bridge/baw_bridge.py`:**

```text
bridge/baw_bridge.py:84:         token = os.environ.get('BAW_BRIDGE_TOKEN', '')
bridge/baw_bridge.py:85:         supplied = self.headers.get('X-Bridge-Token', '')
bridge/baw_bridge.py:86:         if not token or not hmac.compare_digest(token.encode(), supplied.encode()):
bridge/baw_bridge.py:87:             self.reply(401, {'error': 'unauthorized'})
```

**Attack scenario:** If an administrator chooses a short/common nonempty password, an attacker distributes guesses across source IPs; each IP receives ten failures per minute. Restarting the app clears the in-memory history. There is no account-wide budget, progressive penalty, minimum password strength, or second factor. Similarly, a weak nonempty bridge token is accepted with no bridge-side guessing limit. A properly generated 32-byte bridge secret (as recommended in `README.md:60`) makes online guessing infeasible; token quality in the actual deployment is unknown. A proxy that collapses clients to one address instead permits denial of login to everyone; forwarded-header handling is a deployment consideration, not a demonstrated spoofing bypass.

**Specific fix:** Require a strong generated admin secret and at least 32 random bytes for bridge tokens at provisioning/startup. Add a persistent shared account-wide attempt budget plus per-IP throttling and progressive backoff; bound and expire limiter entries. Configure trusted proxy handling explicitly. Consider MFA for remote operator access. Fail closed for empty credentials, as the existing code already does.

### F10 — Low: login is exempt from CSRF/origin validation

**Location:** `app/main.py:32,75,98-115`.

**Exact evidence — `app/main.py`:**

```text
app/main.py:75:         if request.url.path != '/login':
app/main.py:106:         body = (await request.body()).decode('utf-8', errors='replace')
app/main.py:107:         supplied = parse_qs(body).get('password', [''])[0]
app/main.py:108:         if not password or not hmac.compare_digest(supplied.encode(), password.encode()):
app/main.py:109:             attempts.append(now)
app/main.py:110:             return HTMLResponse(LOGIN.replace('请输入管理密码', '密码错误或尚未配置管理密码'), status_code=401)
app/main.py:111:         token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
app/main.py:112:         store.execute('DELETE FROM sessions WHERE expires<?', (now,))
app/main.py:113:         store.execute('INSERT INTO sessions VALUES(?,?,?)', (token, csrf, now + 28800))
```

**Attack scenario:** An external page submits an ordinary cross-site form POST to `/login`. The handler accepts it without a pre-login token or Origin check, and successful authentication issues a new session. An attacker who knows the shared password can cause a browser to enter/replace its session without the user's action; invalid submissions can also consume the login rate budget for that source address. Because this app has only one shared operator identity, the usual multi-account login-CSRF/account-confusion attack is not established here. This does not bypass the password check or the CSRF checks on settings/logout. An in-memory request with a hostile Origin was accepted when supplied the correct test password; browser-specific cookie acceptance was not tested.

**Specific fix:** Validate Origin/Referer against the configured application origin on login and use a one-time pre-authentication CSRF token in the login form. Preserve SameSite=Strict and the existing per-session CSRF checks for authenticated mutations.

## Areas reviewed with no exploitable issue established

### Authentication, session fixation, expiry, and protected mutations

- Password comparison fails closed for an unset/empty password and uses `hmac.compare_digest` over bytes (`app/main.py:108`). No password, token, or hash is returned by the login response. The configured shared secret is kept in environment/closure rather than a user-password table; no password-storage hashing claim is made.
- Successful login creates independent 32-byte random session and CSRF secrets (`app/main.py:111`). It does not accept a caller-chosen session token, so classic session fixation was not found. Retention/revocation is covered in F6.
- Session lookup requires `expires > time.time()` (`app/main.py:77`), and login stores an eight-hour absolute expiration (`:113`). Expired sessions cannot mutate state even if the browser still sends the cookie. HttpOnly and SameSite=Strict are enabled (`:115`); Secure is addressed in F5.
- The login limiter does work within its stated bounds: ten incorrect attempts returned 401 and attempt eleven returned 429 in the offline test. F9 describes its limitations, not a total absence of rate limiting.

All exposed state-changing routes were traced:

| Endpoint | Authentication and CSRF result |
| --- | --- |
| `POST /api/settings` | Requires unexpired session and matching `X-CSRF-Token` before handler execution. |
| `POST /logout` | Same checks; deletes the matching session token. |
| `POST /login` | Password checked, no login CSRF/origin validation (F10). |
| Bridge `POST /quote`, `/place`, `/redeem` | Require `X-Bridge-Token`; do not use browser session cookies. Ordinary forms cannot provide that secret custom header, and no CORS permission is emitted. No browser-session CSRF issue established. |

No GET handler directly changes trading settings, logs out, or places a trade. Background trading and redemption are service activity, not browser endpoints. The middleware excludes only `/login` and checks every other non-GET/HEAD request:

**Exact evidence — `app/main.py`:**

```text
app/main.py:85:             if request.method not in ('GET', 'HEAD'):
app/main.py:86:                 if not hmac.compare_digest(request.headers.get('x-csrf-token', ''), request.state.session['csrf']):
app/main.py:87:                     return JSONResponse({'detail': '会话校验失败，请刷新'}, status_code=403)
```

The anonymous `csrf="public"` value does not bypass authentication: an anonymous settings POST carrying that exact header returned 401. Authenticated settings and logout POSTs missing the token returned 403. No permissive CORS middleware was found.

### SQL injection and database queries

No attacker-controlled SQL syntax was found. Callers bind user, market, model, and venue values as parameters. `Store.rows/execute` accept SQL text internally, but no HTTP input is passed as that SQL argument. The schema `executescript` at `app/db.py:24-43` contains only fixed DDL.

**Exact evidence — `app/db.py`:**

```text
app/db.py:44:     def rows(self, sql, args=()):
app/db.py:45:         with self.lock:
app/db.py:46:             return [dict(r) for r in self.conn.execute(sql,args).fetchall()]
app/db.py:47:     def execute(self, sql, args=()):
app/db.py:48:         with self.lock:
app/db.py:49:             return self.conn.execute(sql,args)
app/db.py:88:             for k,v in normalized.items(): self.conn.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',(k,json.dumps(v)))
app/db.py:98:             name='txn_'+uuid4().hex
app/db.py:99:             self.conn.execute('SAVEPOINT '+name)
```

**Exact evidence — `app/sync.py`:**

```text
app/sync.py:277:         market_ids={str(row['marketId']) for row in items(snapshot,('positions',)) if row.get('marketId') is not None}
app/sync.py:278:         placeholders=','.join('?' for _ in market_ids)
app/sync.py:279:         self.store.execute(
app/sync.py:280:             "DELETE FROM reservations WHERE status='PLACED' AND ts < ?"
app/sync.py:281:             +(f' AND market_id NOT IN ({placeholders})' if market_ids else ''),
app/sync.py:282:             (time.time()-600,*market_ids))
```

The dynamically assembled `IN` clause contains only generated `?` placeholders; market IDs are bound arguments. Savepoint names concatenate only `uuid4().hex`, not external text. Other queries in `app/main.py`, `app/risk.py`, `app/sync.py`, and `engine/runner.py` were checked for the same distinction.

### XSS in the dashboard

No reachable XSS sink from the reviewed venue/model/news strings was found. The two HTML-writing locations are `static/index.html:66` (`html` helper) and `:116` (decisions rendering). Their call sites use escaping, numeric formatting, or fixed enumerated classes.

**Exact evidence — `static/index.html`:**

```text
static/index.html:65: const text=(id,value)=>{const node=el(id);if(node)node.textContent=value};
static/index.html:66: const html=(id,value)=>{const node=el(id);if(node)node.innerHTML=value};
static/index.html:67: const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
static/index.html:76: const badge=mode=>'<span class="badge '+(mode==='LIVE'?'live':'paper')+'">'+tr(mode)+'</span>';
static/index.html:77: const direction=side=>'<span class="badge '+(['UP','YES','涨','是'].includes(side)?'up':['DOWN','NO','跌','否'].includes(side)?'down':'')+'">'+esc(tr(side))+'</span>';
static/index.html:83: function positionCard(r){return '<li class="position"><div class="badges">'+badge(r.mode)+direction(r.side)+'</div><h3 class="market">'+esc(title(r.title))+'</h3><dl class="position-values"><div><dt>份额</dt><dd>'+num(r.shares)+'</dd></div><div><dt>现价</dt><dd>'+num(r.price)+'</dd></div><div><dt>未实现盈亏</dt><dd class="'+tone(r.pnl)+'">'+signed(r.pnl)+'</dd></div></dl></li>'}
static/index.html:84: function orderCard(r){return '<li class="order"><div class="record-top"><div><div class="badges">'+badge(r.mode)+(r.audit?'<span class="badge">审计</span>':'')+'</div><h3 class="market">'+esc(title(r.title))+'</h3></div><div class="order-amount">'+num(r.amount)+'<small>USDT</small></div></div><div class="order-foot"><time class="order-meta">'+esc(date(r.ts))+'</time><span class="badge">'+esc(tr(r.status))+'</span></div>'+(r.error?'<p class="order-error">'+esc(r.error)+'</p>':'')+'</li>'}
static/index.html:107: function votes(raw){try{const parsed=typeof raw==='string'?JSON.parse(raw):raw,rows=Array.isArray(parsed)?parsed:Object.values(parsed||{});return rows.length?rows.map(v=>'<div class="voter"><div class="voter-head"><strong>'+esc(v.name)+'</strong><span class="'+(v.status==='ok'?'good':'muted')+'">'+(v.status==='ok'?'有效 · 上涨概率 '+num(v.p_up):'弃权 · '+esc(v.status||'未知状态'))+'</span></div><p>'+esc(v.reason)+'</p></div>').join(''):'暂无投票明细'}catch{return '暂无投票明细'}}
```

Specifically, market titles go through `esc(title(...))` in position/order/decision cards (`:83-84,116`); model/news names, reasons and failure snippets go through `esc` (`:107`); decision reasons and `data-key` attribute values are escaped (`:116`); wallet IDs/addresses and sync/engine errors are escaped (`:120`); notices and setting error messages use `textContent` (`:65,104,122-124`). Numbers are converted/formatted; CSS classes are fixed conditional strings. Grounding sources and their `renderedContent` are not inserted into the DOM.

`badge()` at `:76` does not escape `tr(mode)`, but all current card callers create `mode` as the literal `LIVE` or `PAPER` (`:92,95`), so no untrusted input reaches it today. Escape that helper too if its input contract expands. No Content-Security-Policy is set by `app/main.py:89-91`; adding a nonce/hash-based policy is useful defense in depth, but its absence is not evidence of an XSS exploit. The existing offline Node rendering test passed with malicious-looking titles and reason/error fields; it is a simulated DOM harness, not a full browser exploit test.

### Other settings validation and risk checks

`app/db.py:55-56` rejects unknown setting names; mode has a PAPER/LIVE allowlist; engine enablement requires a boolean; symbols are normalized to BTCUSDT/ETHUSDT; floating-point settings reject NaN and infinity and enforce bounds (`:59-81`). Duration ordering is checked after merging (`:85-86`). Invalid mode changes while enabled are rejected unless disabling the engine in the same operation (`:84`).

Remaining minor input-quality gaps: `max_open_positions` uses `int(v)`, which accepts booleans and truncates fractional values; general float fields accept numeric strings/booleans; `durations` validates only that it is a list and is not used by market selection (`engine/markets.py:55-99`). These do not bypass the existing cash caps. A non-object JSON settings body can raise uncaught AttributeError/OverflowError and return 500; use a strict request schema for controlled 400/422 responses. The material policy mismatch is F4.

Execution adds separate protections: amount <= 25 (`app/risk.py:38`), daily spend <= 100 (`:52`), loss stop <= 50 (`:62-64`), cooldown >= 600 (`engine/runner.py:40`), fresh account/PnL requirements (`app/risk.py:54-60`), persistent pre-placement reservations (`:85-95`), and immediate checks for STOP/disable/mode/changed thresholds (`:114-127`). No JSON settings bypass of these hard cash bounds was established. The bridge's separate authority remains F2.

### Bridge authentication and command construction

Missing/empty bridge tokens fail closed, and comparison is constant time (`bridge/baw_bridge.py:84-88`). No default nonempty token exists in the examples. Token strength is a provisioning issue (F9), while interception is F1. Only exact endpoint paths and known methods are handled (`:89-92,127-131`). Body size is capped at 16 KiB and Transfer-Encoding is rejected (`:96-99`). Quote/place reject unknown fields (`:42-45`); redeem requires exactly a nonempty token-ID list (`:37-41`).

No shell injection was found. `subprocess.run` receives an argument list, with no shell enabled (`:108`). Numeric IDs, quote IDs, decimals, side, order type, and slippage have restrictive validators (`:11-27,46-65`); identifiers cannot begin with a dash, and decimals cannot carry shell metacharacters or CLI options. An offline pure-function check rejected injected `tokenId` and `marketTopicId` values. `BAW_BIN` is administrator-controlled environment, not request input. The downstream CLI implementation itself was not audited.

### File/path handling, static serving, and subprocess/exec inventory

- `app/main.py:120` reads only the fixed `ROOT / 'static/index.html'`; no path parameter is joined onto it. There is **no StaticFiles mount, catch-all file handler, or FileResponse**. The middleware's public `/static/` prefix (`:79`) is only an authorization condition; no such serving route exists, so it does not expose arbitrary files. No static traversal route was found.
- `load_env` reads only the configured/default local `.env` (`app/main.py:16-22`); DB location comes from trusted `DB_PATH` (`:35`); the STOP path derives from that database directory (`app/risk.py:37`); Vertex credentials come from trusted `VERTEX_SA_JSON` (`engine/signals.py:206-212`). No browser/venue-controlled filename reaches these reads. A local actor able to alter environment/database directories is outside this remote-input boundary.
- Vertex signing uses a private `NamedTemporaryFile`, an argv-based OpenSSL invocation, and stdin for JWT content, avoiding shell interpolation and command-line private-key contents:

**Exact evidence — `engine/signals.py`:**

```text
engine/signals.py:225:             # A private temporary file is necessary for OpenSSL; never log the key/JWT.
engine/signals.py:226:             with tempfile.NamedTemporaryFile(mode='w', prefix='vertex-key-', suffix='.pem') as handle:
engine/signals.py:227:                 handle.write(credential['private_key'])
engine/signals.py:228:                 handle.flush()
engine/signals.py:229:                 process = await asyncio.create_subprocess_exec('openssl', 'dgst', '-sha256', '-sign', handle.name,
engine/signals.py:230:                     stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
engine/signals.py:231:                 signature, _ = await asyncio.wait_for(process.communicate(message), timeout=10)
engine/signals.py:232:                 if process.returncode:
engine/signals.py:233:                     raise ValueError('Vertex JWT 签名失败')
```

- Production subprocess calls: the above OpenSSL signing call and `bridge/baw_bridge.py:108` are the only process launches found. The OpenSSL timeout awaits `communicate` without explicitly killing/waiting for the child on timeout; add cleanup if hardening against a stuck executable. This is a bounded local reliability issue, not an established remote command-execution vector. Bridge process privilege/concurrency are F7/F8.
- Test-only launches: `signals_test.py:424-425` runs argv-based OpenSSL test-key generation; `dashboard_test.py:385,429` runs fixed Node test harnesses via argv. `baw_test.py:460` patches `subprocess.run` and uses fake results. No Python `eval`, dynamic `exec`, `os.system`, `os.popen`, `shell=True`, or shell-based subprocess API was found in the reviewed project source. `Executor.execute` and database `execute` are ordinary methods, not dynamic Python execution.
- `.dockerignore:1-6,8` excludes `.env`, `.git`, `.venv`, cache/data/secrets directories, and `bridge/bridge.env` from the build context. Compose mounts secrets read-only (`docker-compose.yml:16`) and publishes the app only to loopback (`:7`); the Dockerfile runs the app as user `trader` (`Dockerfile:7-8`). This does not protect the separate root bridge.

## Validation and limits

1. Reviewed source with numbered lines; searched every project Python/HTML source outside the virtual environment/cache for routes, SQL execution, HTML sinks, file reads, dynamic execution, and process launches. No exploit was sent to the live service.
2. Ran an inline offline verification using an in-memory SQLite store, ASGI transport, fake client, disabled background tasks, and synthetic sentinel values. Confirmed private/public state behavior, unrestricted metadata serialization, settings-bound mismatch, CSRF denials, absolute expiry, session rotation with previous-session retention, missing Secure at the default setting, and the ten-failure login throttle. Only command-argument builders were called for the bridge; no CLI process was launched.
3. Ran `PYTHONDONTWRITEBYTECODE=1 EXECUTOR_PATH=sapi .venv/bin/python -m unittest dashboard_test.DashboardTests.test_rendering_controls_and_missing_elements`: **1 test passed**. It uses mocked HTTP, temporary SQLite files outside the project, and a local Node rendering harness.
4. Did not inspect production secret contents, exercise password guessing against a deployed host, scan host ports, test network flooding, run full integration suites, audit dependency CVEs, or verify external CLI source. Network reachability and upstream response contents are explicitly conditional where relevant. Quoted evidence is from this workspace's source and may need line-number updates after subsequent edits.
