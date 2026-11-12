"""Single-process authenticated dashboard; start with exactly one worker."""
from app.service_config import safe_settings, _url
from app.baw_client import BawClient, BawError
import httpx
import asyncio
import contextlib
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent.parent

def load_env(path=ROOT / '.env'):
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"\''))

load_env()
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from app.binance_client import BinanceClient
from app.db import Store
from app.sync import Sync
from engine.runner import Engine
from engine.signals import voter_weights
from engine.markets import timestamp

LOGIN = '''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>登录 · 预测交易</title><style>body{background:#101820;color:#edf2f7;font:18px system-ui;max-width:360px;margin:15vh auto}input,button{padding:14px;box-sizing:border-box;width:100%;margin:10px 0}button{background:#f0b90b;border:0}</style><h1>预测交易控制台</h1><p>请输入管理密码</p><form method="post" action="/login"><input name="password" type="password" required autocomplete="current-password" aria-label="管理密码"><button>登录</button></form></html>'''

def price_context(row, meta):
    mid = str(row.get('market_id', row.get('marketId', '')))
    return {'open_price': meta.get('market_prices', {}).get(mid, {}).get('start_price'),
            'settlement_price': meta.get('settlement_prices', {}).get(mid)}


def live_title(row):
    """Apply the existing static/index.html title() market rules to LIVE state."""
    translated = row.get('translatedMarketTopicTitle')
    if translated and str(translated).strip():
        return translated
    value = row.get('marketTopicTitle') or row.get('marketTitle')
    if value is None:
        return None
    months = dict(zip(('January February March April May June July August September '
                       'October November December').split(), range(1, 13)))
    value = re.sub(r'(' + '|'.join(months) + r')\s+(\d{1,2})',
                   lambda m: f'{months[m[1]]}月{m[2]}日', str(value))
    # Keep the original time range: AM/PM attached to digits have no word boundary.
    for pattern, replacement in ((r'\bAM\b', '上午'), (r'\bPM\b', '下午'),
                                 (r'\bET\b', '美东时间')):
        value = re.sub(pattern, replacement, value, flags=re.ASCII)
    for pattern, replacement in ((r'Up or Down', '涨还是跌'),
                                 (r'\bBitcoin\b|\bBTC(?:USDT)?\b', '比特币'),
                                 (r'\bEthereum\b|\bETH(?:USDT)?\b', '以太坊'),
                                 (r'\bPrice\b', '价格'), (r'\b(\d+)h\b', r'\1 小时'),
                                 (r'\b(\d+)m\b', r'\1 分钟'),
                                 (r'\bUP\b', '涨'), (r'\bDOWN\b', '跌')):
        value = re.sub(pattern, replacement, value, flags=re.I | re.ASCII)
    for pattern, replacement in ((r'\bYES\b', '是'), (r'\bNO\b', '否')):
        value = re.sub(pattern, replacement, value, flags=re.ASCII)
    return value


def paper_details(store):
    """Read-only projection; never substitute today's settings for order-time context."""
    orders = store.rows('SELECT * FROM paper_orders ORDER BY ts DESC LIMIT 200')
    decisions = store.rows("SELECT * FROM decisions WHERE action='TRADE' ORDER BY ts DESC, id DESC")
    history = store.rows('SELECT * FROM eval_history ORDER BY ts DESC, id DESC')
    for row in orders:
        ts, mid = row.get('ts'), row.get('market_id')
        end = row.get('window_end')
        if end is None:
            end = row.get('end_time')
        start = row.get('window_start')
        evaluations = [h for h in history if mid is not None and h['market_id'] == mid
                       and ts is not None and h['ts'] <= ts
                       and h['window_start'] <= ts < h['window_end']
                       and (end is None or h['window_end'] == end)
                       and (start is None or h['window_start'] == start)]
        evaluation = evaluations[0] if evaluations else {}
        end = end if end is not None else evaluation.get('window_end')
        start = start if start is not None else evaluation.get('window_start')
        matches = [d for d in decisions if mid is not None and d['market_id'] == mid
                   and ts is not None and d['ts'] <= ts and end is not None
                   and d['variant_end_time'] == end
                   and (start is None or d['ts'] >= start)
                   and (row.get('side') is None or str(d['side']).upper() == str(row['side']).upper())]
        decision = matches[0] if matches else {}
        if decision:
            # The ensemble is p(UP); decisions store p(the purchased direction).
            evaluation = next((h for h in evaluations if h['ts'] == decision['ts']), {})
        if row.get('title') is None:
            row['title'] = decision.get('market')
        if row.get('side') is None:
            row['side'] = decision.get('side')
        row['window_start'], row['window_end'] = start, end
        p_up = evaluation.get('ensemble_p')
        side = str(row.get('side') or '').upper()
        probability = (p_up if side == 'UP' else 1 - p_up if side == 'DOWN' else None) if p_up is not None else None
        for key, fallback in (('model_prob', decision.get('model_prob') if decision.get('model_prob') is not None else probability),
                              ('market_price', decision.get('market_price')), ('edge', decision.get('edge'))):
            if row.get(key) is None:
                row[key] = fallback
        if row['edge'] is None and row['model_prob'] is not None and row['market_price'] is not None:
            row['edge'] = row['model_prob'] - row['market_price']
        if row.get('invert') is None:
            row['invert'] = ('反买' in decision['reason']) if decision.get('reason') else None
            if row['invert'] is None and p_up is not None and side in ('UP', 'DOWN'):
                row['invert'] = side != ('UP' if p_up >= .5 else 'DOWN')
        else:
            row['invert'] = bool(row['invert'])
        row['result'] = None
        if row.get('status') == 'SETTLED':
            payout = row.get('current_price')
            recorded_pnl = row.get('realized_pnl')
            if payout not in (0, 1) and recorded_pnl is not None and recorded_pnl != 0:
                payout = 1 if recorded_pnl > 0 else 0
            if payout in (0, 1):
                row['result'] = 'WIN' if payout == 1 else 'LOSS'
            amount, price = row.get('amount'), row.get('price')
            row['realized_pnl'] = recorded_pnl if row['result'] is not None else None
            if payout == 0 and amount is not None:
                row['realized_pnl'] = -amount
            elif payout == 1 and amount is not None and price is not None and price > 0:
                row['realized_pnl'] = amount / price - amount
        else:
            row['realized_pnl'] = None
    return orders


def paper_account(store):
    """Project the current paper ledger, independently of the history display limit."""
    def number(value):
        return float(value) if value is not None else 0.0

    with store.transaction():
        initial = store.settings()['paper_initial_balance']
        orders = store.rows('SELECT * FROM paper_orders')
    realized = sum(number(r['realized_pnl']) for r in orders if r['status'] == 'SETTLED')
    opened = [r for r in orders if r['status'] == 'OPEN']
    cost = sum(number(r['amount']) for r in opened)
    value = sum(number(r['shares']) * number(r['current_price']) for r in opened)
    cash = initial + realized - cost
    return dict(initial=initial, cash=cash, equity=cash + value, realized=realized,
                unrealized=value - cost, open_count=len(opened))


def live_decision_details(store, orders, meta=None):
    """Project display fields using historical context and exact order IDs."""
    def number(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (ValueError, TypeError):
            return None

    meta = meta or {}
    outcomes = {}
    for row in store.rows("SELECT h.market_id,s.outcome FROM eval_scores s JOIN eval_history h ON h.id=s.eval_id WHERE s.outcome IN ('UP','DOWN') ORDER BY s.eval_id DESC"):
        outcomes.setdefault(str(row['market_id']), row['outcome'])
    decisions, history = {}, {}
    for row in store.rows("SELECT market_id,market,ts,variant_end_time,side,model_prob,market_price,edge,reason FROM decisions WHERE action='TRADE' ORDER BY ts DESC, id DESC"):
        if row['market_id'] is not None:
            decisions.setdefault(str(row['market_id']), []).append(row)
    for row in store.rows('SELECT market_id,ts,window_start,window_end FROM eval_history ORDER BY ts DESC, id DESC'):
        if row['market_id'] is not None:
            history.setdefault(str(row['market_id']), []).append(row)
    audits = {}
    for row in store.rows('SELECT order_id,market_id,ts FROM orders_audit WHERE order_id IS NOT NULL ORDER BY ts DESC, id DESC'):
        audits.setdefault(str(row['order_id']), row)
    details = []
    for order in orders:
        mid = order.get('market_id', order.get('marketId'))
        order_id = order.get('orderId')
        audit = audits.get(str(order_id), {}) if order_id is not None else {}
        market_ids = [str(value) for value in (mid, audit.get('market_id')) if value is not None]
        candidates = next((decisions[key] for key in market_ids if decisions.get(key)), [])
        ts = timestamp(audit.get('ts') or order.get('createTime'))
        side = str(order.get('outcomeName') or order.get('outcome') or '').upper()
        decision = next((d for d in candidates if (not ts or d['ts'] <= ts)
                         and (side not in ('UP', 'DOWN') or str(d['side']).upper() == side)), {})
        detail = {key: decision.get(key) for key in ('model_prob', 'market_price', 'edge', 'side', 'reason')}
        if detail['edge'] is None and detail['model_prob'] is not None and detail['market_price'] is not None:
            detail['edge'] = detail['model_prob'] - detail['market_price']
        detail['invert'] = ('反买' in decision['reason']) if decision.get('reason') else None
        evaluations = next((history[key] for key in market_ids if history.get(key)), [])
        evaluation = next((row for row in evaluations if not ts or row['ts'] <= ts),
                          evaluations[0] if evaluations else {})
        start, end = evaluation.get('window_start'), evaluation.get('window_end')
        if end is None:
            end = number(decision.get('variant_end_time'))
            # Use the recorded market duration, never today's settings.
            duration = re.search(r'\b(5|15)\s*(?:m(?:in(?:ute)?s?)?|分钟)\b', decision.get('market') or '', re.I)
            if start is None and end is not None and duration:
                start = end - int(duration[1]) * 60
        detail['window_start'], detail['window_end'] = start, end
        filled, shares = number(order.get('filledUsdtAmount')), number(order.get('filledShareQty'))
        detail['price'] = filled / shares if filled is not None and filled > 0 and shares is not None and shares > 0 else number(decision.get('market_price'))
        detail['realized_pnl'] = detail['result'] = None
        outcome = next((outcomes[key] for key in market_ids if key in outcomes), None)
        if outcome is None:
            context = price_context(order, meta)
            opening = number(order.get('open_price'))
            closing = number(order.get('settlement_price'))
            opening = opening if opening is not None else number(context['open_price'])
            closing = closing if closing is not None else number(context['settlement_price'])
            if opening is not None and closing is not None and opening != closing:
                outcome = 'UP' if closing > opening else 'DOWN'
        purchased = side if side in ('UP', 'DOWN') else str(detail['side'] or '').upper()
        if (str(order.get('status') or '').upper() == 'FILLED'
                and outcome in ('UP', 'DOWN') and purchased in ('UP', 'DOWN')
                and filled is not None and filled > 0 and shares is not None and shares > 0):
            won = purchased == outcome
            detail['result'] = 'WIN' if won else 'LOSS'
            detail['realized_pnl'] = shares - filled if won else -filled
        details.append(detail)
    return details


def public_state(store, meta, paper=None):
    def cached(name):
        value = meta.get('cache:' + name, meta.get(name, {}))
        return value.get('data', value) if isinstance(value, dict) else value

    def rows(value, key):
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            value = value.get(key, value.get('list', []))
            return value if isinstance(value, list) else []
        return []

    def number(value):
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (ValueError, TypeError):
            return None

    paper = paper_details(store) if paper is None else paper
    pnl = cached('pnl_portfolio')
    pnl = {key: number(pnl.get(key)) for key in ('totalPnl', 'totalRealizedPnl', 'totalUnrealizedPnl')} if isinstance(pnl, dict) else {}
    positions = rows(cached('positions:ONGOING'), 'positions') or rows(cached('positions'), 'positions')
    display = [dict(mode='LIVE', title=r.get('marketTopicTitle') or r.get('marketTitle'),
                    side=r.get('outcomeName'), shares=number(r.get('shares')),
                    price=number(r.get('currentPrice')), pnl=number(r.get('pnl')),
                    **price_context(r, meta)) for r in positions]
    for r in paper:
        if r['status'] == 'OPEN':
            shares, price, amount = (number(r[k]) for k in ('shares', 'current_price', 'amount'))
            gain = shares * price - amount if None not in (shares, price, amount) else None
            display.append(dict(mode='PAPER', title=r['title'], side=r['side'], shares=shares, price=price, pnl=gain, **price_context(r, meta)))
    venue_orders, seen = [], set()
    for r in rows(cached('order_history'), 'orders') + rows(cached('orders'), 'orders'):
        order_id = r.get('orderId')
        if order_id is not None and order_id in seen:
            continue
        seen.add(order_id)
        venue_orders.append(r)
    orders = []
    for r, detail in zip(venue_orders, live_decision_details(store, venue_orders, meta)):
        orders.append(dict(mode='LIVE', title=live_title(r),
                           **detail,
                           ts=r.get('createTime'), amount=r.get('filledUsdtAmount') or r.get('makerUsdtAmount'), status=r.get('status'), **price_context(r, meta)))
    # Keep the public projection explicit: paper_details also carries internal IDs.
    paper_fields = ('title', 'ts', 'window_start', 'window_end', 'side', 'amount',
                    'price', 'model_prob', 'market_price', 'edge', 'invert',
                    'status', 'result', 'realized_pnl')
    orders += [dict(mode='PAPER', **{k: r.get(k) for k in paper_fields},
                    **price_context(r, meta)) for r in paper]
    account = meta.get('baw_account', {})
    balances = rows(account.get('balance'), 'balances') if isinstance(account, dict) else []
    usdt = next((r for r in balances if r.get('symbol') == 'USDT'), {})
    audit = store.rows('SELECT market_id,ts,side,amount,status,error FROM orders_audit ORDER BY ts DESC, id DESC LIMIT 10')
    account_summary = paper_account(store)
    return dict(public=True, mode=store.settings()['mode'], pnl=pnl, paper_account=account_summary,
                paper_pnl={key: account_summary[key] for key in ('realized', 'unrealized')},
                positions=display, orders=orders, orders_audit=[{**r, **price_context(r, meta)} for r in audit],
                account_balance_usdt=number(usdt.get('balance', usdt.get('value'))),
                last_sync=number(meta.get('sync_state', {}).get('last_success')),
                csrf='public', now=time.time())

def create_app(store=None, client=None, background=True, password=None):
    store = store or Store(os.getenv('DB_PATH', str(ROOT / 'data' / 'trader.db')))
    client = client or BinanceClient(store=store)
    configured_password = password

    def current_password():
        return configured_password if configured_password is not None else os.getenv('APP_PASSWORD', '')

    def password_fingerprint():
        return hashlib.sha256(current_password().encode()).hexdigest()
    public_readonly = os.getenv('PUBLIC_READONLY', '0') == '1'
    store.execute('CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, csrf TEXT NOT NULL, expires REAL NOT NULL)')
    if 'password_fingerprint' not in {r['name'] for r in store.rows('PRAGMA table_info(sessions)')}:
        store.execute("ALTER TABLE sessions ADD COLUMN password_fingerprint TEXT NOT NULL DEFAULT ''")
    sync, engine = Sync(store, client), Engine(store, client)

    async def loop(fn, interval, name):
        while True:
            try:
                await fn()
            except Exception as e:
                store.set_meta(name + '_error', {'ts': time.time(), 'error': str(e)[:300]})
            await asyncio.sleep(interval)

    @asynccontextmanager
    async def lifespan(app):
        tasks = []
        if background:
            tasks = [asyncio.create_task(loop(sync.run_once, 60, 'sync')), asyncio.create_task(loop(engine.tick, 30, 'engine'))]
        yield
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if hasattr(engine.ensemble, 'close'):
            await engine.ensemble.close()
        await sync.baw_client.close()
        await engine.executor.baw_client.close()
        await client.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.store, app.state.engine, app.state.sync = store, engine, sync
    failures = {}

    @app.middleware('http')
    async def authenticate(request, call_next):
        if request.url.path != '/login':
            token = request.cookies.get('session', '')
            session = store.rows('SELECT * FROM sessions WHERE token=? AND expires>? AND password_fingerprint=?', (token, time.time(), password_fingerprint()))
            public_request = public_readonly and request.method in ('GET', 'HEAD') and (
                request.url.path in ('/', '/api/state') or request.url.path.startswith('/static/'))
            if not session and not public_request:
                if request.url.path == '/':
                    return RedirectResponse('/login', status_code=303)
                return JSONResponse({'detail': '请先登录'}, status_code=401)
            request.state.session = dict(session[0]) if session else {'token': 'public', 'csrf': 'public'}
            if request.method not in ('GET', 'HEAD') or request.url.path.startswith('/wallet/'):
                if not hmac.compare_digest(request.headers.get('x-csrf-token', ''), request.state.session['csrf']):
                    return JSONResponse({'detail': '会话校验失败，请刷新'}, status_code=403)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    @app.get('/login')
    async def login_form():
        return HTMLResponse(LOGIN)

    @app.post('/login')
    async def login(request: Request):
        value = request.headers.get('origin')
        if value is None:
            value = request.headers.get('referer')
        if value is not None:
            try:
                origin_host = urlsplit(value).hostname
                expected_host = urlsplit('//' + request.headers.get('host', '')).hostname
                if not origin_host or not expected_host or origin_host.lower() != expected_host.lower():
                    return HTMLResponse('来源校验失败', status_code=403)
            except ValueError:
                return HTMLResponse('来源校验失败', status_code=403)
        body = (await request.body()).decode('utf-8', errors='replace')
        forwarded_for = request.headers.get('x-forwarded-for')
        ip = forwarded_for.split(',')[0].strip() if forwarded_for else (request.client.host if request.client else 'local')
        now = time.time()
        attempts = [t for t in failures.get(ip, []) if t > now - 900]
        failures[ip] = attempts
        if len(attempts) >= 5:
            return HTMLResponse('尝试过于频繁，请稍后重试', status_code=429)
        supplied = parse_qs(body).get('password', [''])[0]
        password = current_password()
        if not password or not hmac.compare_digest(supplied.encode(), password.encode()):
            attempts.append(now)
            return HTMLResponse(LOGIN.replace('请输入管理密码', '密码错误或尚未配置管理密码'), status_code=401)
        token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        store.execute('DELETE FROM sessions WHERE expires<?', (now,))
        store.execute('INSERT INTO sessions (token,csrf,expires,password_fingerprint) VALUES(?,?,?,?)', (token, csrf, now + 28800, password_fingerprint()))
        response = RedirectResponse('/', status_code=303)
        response.set_cookie('session', token, max_age=28800, httponly=True, samesite='strict', secure=request.url.scheme == 'https' or request.headers.get('x-forwarded-proto', '').split(',')[0].strip().lower() == 'https')
        return response

    @app.get('/')
    async def index(request: Request):
        html = (ROOT / 'static/index.html').read_text()
        if public_readonly and request.state.session['token'] == 'public':
            html = re.sub(r'<!--OPERATOR-ONLY-->.*?<!--/OPERATOR-ONLY-->', '', html, flags=re.DOTALL)
        return HTMLResponse(html)

    @app.get('/api/state')
    async def state(request: Request):
        meta = {r['key']: json.loads(r['value']) for r in store.rows('SELECT * FROM meta')}
        if request.state.session['token'] == 'public':
            return public_state(store, meta)
        if 'settings' in meta:
            meta['settings'] = safe_settings(meta['settings'])
        meta['voter_weights'] = voter_weights(meta.get('voter_stats', {}))
        paper = paper_details(store)
        display = public_state(store, meta, paper=paper)
        orders = [r for r in display['orders'] if r['mode'] != 'PAPER']
        orders += [{**r, 'mode': 'PAPER', **price_context(r, meta)} for r in paper]
        return {'positions': display['positions'], 'orders': orders,
                'paper_account': display['paper_account'], 'paper_pnl': display['paper_pnl'],
                'public': public_readonly, 'settings': safe_settings(store.settings()), 'meta': meta, 'decisions': [dict(r) for r in store.rows('SELECT * FROM decisions ORDER BY ts DESC, id DESC LIMIT 10')],
                'paper_orders': paper,
                'orders_audit': [{**r, **price_context(r, meta)} for r in store.rows('SELECT * FROM orders_audit ORDER BY ts DESC, id DESC LIMIT 10')],
                'daily_spend': [dict(r) for r in store.rows('SELECT * FROM daily_spend')], 'csrf': request.state.session['csrf'], 'now': time.time()}

    @app.post('/api/settings')
    async def settings(request: Request):
        try:
            updated = store.update_settings(await request.json())
            return {'settings': safe_settings(updated or store.settings()), 'message': '已保存'}
        except (ValueError, TypeError, KeyError) as e:
            return JSONResponse({'detail': '参数无效：' + str(e)}, status_code=400)

    @app.post('/api/services/models')
    async def service_models(request: Request):
        key = ''
        try:
            body = await request.json()
            base, key = body.get('base_url', ''), body.get('api_key', '')
            if body.get('provider_id') and key == '':
                saved = next((p for p in store.settings().get('providers', []) if p['id'] == body['provider_id']), {})
                key = saved.get('api_key', '')
            if not isinstance(base, str) or not isinstance(key, str) or not base.strip():
                raise ValueError('base_url 和 api_key 必须为文本，base_url 不能为空')
            base = base.strip().rstrip('/')
            _url(base, 'base_url')
            async with httpx.AsyncClient(timeout=15, trust_env=False) as upstream:
                response = await upstream.get(base + '/models', headers={'Authorization': 'Bearer ' + key})
            if response.is_error:
                detail = response.text.replace(key, '[redacted]') if key else response.text
                return JSONResponse({'detail': f'HTTP {response.status_code}: ' + detail}, status_code=response.status_code)
            data = response.json()
            models = data.get('data') if isinstance(data, dict) else None
            if not isinstance(models, list):
                raise ValueError('上游 /models 未返回 data 列表')
            return {'models': list(dict.fromkeys(item['id'] for item in models if isinstance(item, dict) and isinstance(item.get('id'), str)))}
        except httpx.TimeoutException:
            return JSONResponse({'detail': '获取模型超时（15s）'}, status_code=504)
        except httpx.HTTPError:
            return JSONResponse({'detail': '获取模型失败：无法连接上游'}, status_code=502)
        except (ValueError, TypeError, AttributeError) as exc:
            message = str(exc)
            return JSONResponse({'detail': message.replace(key, '[redacted]') if isinstance(key, str) and key else message}, status_code=400)

    async def wallet_proxy(method, path, body=None):
        bridge = BawClient(store=store)
        try:
            return await bridge._request(method, path, body)
        except BawError as exc:
            return JSONResponse({'detail': exc.msg}, status_code=exc.status_code if exc.status_code and exc.status_code >= 400 else 502)
        finally:
            await bridge.close()

    @app.get('/wallet/status')
    async def wallet_status():
        return await wallet_proxy('GET', '/status')

    @app.post('/wallet/login')
    async def wallet_login():
        return await wallet_proxy('POST', '/wallet/login', {})

    @app.get('/wallet/login/status')
    async def wallet_login_status(request: Request):
        session_id = request.query_params.get('session_id', '')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', session_id):
            return JSONResponse({'detail': '无效登录会话'}, status_code=400)
        return await wallet_proxy('GET', '/wallet/login/status?session_id=' + session_id)

    @app.post('/api/paper/reset')
    async def reset_paper():
        with store.transaction():
            store.execute('DELETE FROM paper_orders')
            store.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',
                          ('paper_reset_at', json.dumps(time.time())))
        return {'message': '模拟余额已重置'}

    @app.post('/logout')
    async def logout(request: Request):
        store.execute('DELETE FROM sessions WHERE token=?', (request.cookies.get('session'),))
        response = JSONResponse({'message': '已退出'})
        response.delete_cookie('session')
        return response
    return app

# Factory avoids creating state during test imports.
