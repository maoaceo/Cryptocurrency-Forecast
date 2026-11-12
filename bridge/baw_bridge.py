"""Authenticated host-side baw adapter. Requires only the Python standard library."""
import contextlib
import fcntl
import hmac
import json
import os
import re
import secrets
from urllib.parse import urlsplit, parse_qs
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


BAW_SLOTS = threading.BoundedSemaphore(4)
QUOTES = {}
QUOTE_LOCK = threading.Lock()
PLACEMENT_LOCK = threading.Lock()


LOGIN_LOCK = threading.Lock()
LOGIN_SESSIONS = {}


def login_snapshot(session):
    return {key: session.get(key) for key in ('session_id', 'status', 'text', 'url', 'address', 'error')}


def wallet_address(wallet):
    if isinstance(wallet, dict):
        address = wallet.get('walletAddress') or wallet.get('address')
        if isinstance(address, str) and address.strip():
            return address


def wallet_logged_in(wallet):
    return isinstance(wallet, dict) and (
        wallet.get('status') == 'CONNECTED' or wallet.get('loggedIn') is True or bool(wallet_address(wallet)))


def start_login():
    with LOGIN_LOCK:
        now = time.monotonic()
        for identity, old in list(LOGIN_SESSIONS.items()):
            if now - old['created'] > 300:
                del LOGIN_SESSIONS[identity]
            elif old['status'] == 'pending':
                return login_snapshot(old)
        if not BAW_SLOTS.acquire(blocking=False):
            raise RuntimeError('baw 正忙，请稍后重试')
        try:
            process = subprocess.Popen([os.getenv('BAW_BIN', 'baw'), 'auth', 'signin'],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                       text=True, encoding='utf-8', errors='replace')
        except OSError:
            BAW_SLOTS.release()
            raise
        identity = secrets.token_urlsafe(24)
        session = dict(session_id=identity, status='pending', text='', url=None, address=None,
                       error=None, created=now)
        LOGIN_SESSIONS[identity] = session

    def read_output():
        while True:
            chunk = process.stdout.read(1)
            if not chunk:
                break
            with LOGIN_LOCK:
                session['text'] = (session['text'] + chunk)[-16384:]
                match = re.search(r'https?://[^\s<>"\x1b]+', session['text'])
                if match:
                    session['url'] = match[0]

    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()

    def finish():
        try:
            code = process.wait(timeout=300)
            reader.join(timeout=1)
            with LOGIN_LOCK:
                already_logged_in = 'already logged in' in session['text'].lower()
            address = None
            try:
                result = subprocess.run([os.getenv('BAW_BIN', 'baw'), 'wallet', 'status', '--json'],
                                        capture_output=True, timeout=max(1, 300-(time.monotonic()-now)), check=False)
                payload = json.loads(result.stdout)
                if result.returncode or not isinstance(payload, dict) or payload.get('success') is not True:
                    raise ValueError('无法确认钱包登录状态')
                wallet = payload.get('data')
                address = wallet_address(wallet)
                if not wallet_logged_in(wallet) and not already_logged_in:
                    raise ValueError('钱包尚未登录')
            except (OSError, ValueError, subprocess.SubprocessError):
                if not already_logged_in:
                    if code:
                        raise ValueError('baw 登录失败（退出码 '+str(code)+'）')
                    raise
            with LOGIN_LOCK:
                session.update(status='done', address=address)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            if process.poll() is None:
                process.kill()
                process.wait()
            with LOGIN_LOCK:
                session.update(status='failed', error='登录超时（5分钟）' if isinstance(exc, subprocess.TimeoutExpired) else str(exc))
        finally:
            reader.join(timeout=1)
            process.stdout.close()
            BAW_SLOTS.release()

    threading.Thread(target=finish, daemon=True).start()
    return login_snapshot(session)


def login_status(identity):
    with LOGIN_LOCK:
        session = LOGIN_SESSIONS.get(identity)
        if not session or time.monotonic() - session['created'] > 300:
            return dict(session_id=identity, status='failed', error='登录会话已过期（5分钟）')
        return login_snapshot(session)


class BridgeServer(ThreadingHTTPServer):
    request_queue_size = 16

    def __init__(self, *args, **kwargs):
        self.request_slots = threading.BoundedSemaphore(16)
        super().__init__(*args, **kwargs)

    def process_request(self, request, client_address):
        # Reject overload before ThreadingMixIn can create another thread.
        if not self.request_slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.request_slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.request_slots.release()


def listen_address():
    return os.getenv('BAW_BRIDGE_BIND', '127.0.0.1'), int(os.getenv('BAW_BRIDGE_PORT', '8791'))


def remember_quote(data, body):
    payload = data.get('data')
    if not isinstance(payload, dict):
        raise ValueError('baw quote missing quoteId')
    quote_id = identifier(payload.get('quoteId'), 'quoteId')
    with QUOTE_LOCK:
        now = time.monotonic()
        for key in list(QUOTES):
            if QUOTES[key][2] <= now:
                del QUOTES[key]
        QUOTES[quote_id] = (body['side'], Decimal(body['amount']), now + 600)


@contextlib.contextmanager
def placement(quote_id):
    # Serialize check, CLI placement and ledger commit, including across processes.
    with PLACEMENT_LOCK:
        with QUOTE_LOCK:
            quote = QUOTES.pop(quote_id, None)
        if quote is None or quote[2] <= time.monotonic():
            raise ValueError('unknown or expired quoteId')
        side, amount, _ = quote
        if side == 'SELL':
            yield
            return
        path = Path(os.getenv('BAW_BRIDGE_LEDGER', '/opt/prediction-autotrader/bridge/bridge_ledger.json'))
        with open(str(path) + '.lock', 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            ledger = json.loads(path.read_text()) if path.exists() else {}
            if not isinstance(ledger, dict):
                raise ValueError('invalid bridge ledger')
            day = datetime.now(timezone.utc).date().isoformat()
            try:
                spent = Decimal(str(ledger.get(day, '0')))
            except InvalidOperation as exc:
                raise ValueError('invalid bridge ledger') from exc
            if not spent.is_finite() or spent < 0 or spent + amount > 5000:
                raise ValueError('daily BUY cap of 5000 USDT exceeded or invalid ledger')
            # Prepare durable storage before executing a money-moving command.
            with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as tmp:
                temp_path = Path(tmp.name)
                try:
                    ledger[day] = str(spent + amount)
                    json.dump(ledger, tmp)
                    tmp.flush()
                    os.fsync(tmp.fileno())
                    yield
                    os.replace(temp_path, path)
                    directory = os.open(path.parent, os.O_DIRECTORY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                finally:
                    temp_path.unlink(missing_ok=True)


def decimal_string(value, name):
    if not isinstance(value, str) or len(value) > 100 or not re.fullmatch(r'[0-9]+(?:\.[0-9]+)?', value) or Decimal(value) <= 0:
        raise ValueError(f'{name} must be a positive decimal string')
    return value


def identifier(value, name, digits=False):
    pattern = r'[0-9]{1,256}' if digits else r'[A-Za-z0-9][A-Za-z0-9_-]{0,255}'
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(f'invalid {name}')
    return value


def slippage(value):
    if type(value) is not int or not 1 <= value <= 10000:
        raise ValueError('slippageBps must be an integer from 1 to 10000')
    return str(value)


def command(path, body=None):
    reads = {'/status': ['wallet', 'status'], '/balance': ['wallet', 'balance'],
             '/positions': ['prediction', 'position', 'list'], '/pnl': ['prediction', 'position', 'pnl'], '/orders': ['prediction', 'order', 'history', '--limit', '100']}
    if body is None:
        return reads[path] + ['--json']
    if not isinstance(body, dict):
        raise ValueError('expected a JSON object')
    if path == '/redeem':
        if body.keys() != {'tokenIds'} or not isinstance(body['tokenIds'], list) or not body['tokenIds']:
            raise ValueError('tokenIds must be a nonempty list')
        tokens = [identifier(value, 'tokenId', True) for value in body['tokenIds']]
        return ['prediction', 'trade', 'redeem', '--binanceChainId', '56', '--tokenIds', ','.join(tokens), '--json']
    allowed = ({'tokenId', 'marketTopicId', 'side', 'amount', 'orderType', 'slippageBps'} if path == '/quote'
               else {'quoteId', 'slippageBps', 'orderType', 'priceLimit'})
    if body.keys() - allowed:
        raise ValueError('unknown fields')
    kind = body.get('orderType', 'MARKET')
    if kind not in ('MARKET', 'LIMIT'):
        raise ValueError('orderType must be MARKET or LIMIT')
    if path == '/quote':
        if body.get('side') not in ('BUY', 'SELL'):
            raise ValueError('side must be BUY or SELL')
        amount = decimal_string(body.get('amount'), 'amount')
        if body['side'] == 'BUY' and Decimal(amount) > 100:
            raise ValueError('BUY amount exceeds 100 USDT')
        args = ['prediction', 'trade', 'quote', '--binanceChainId', '56',
                '--tokenId', identifier(body.get('tokenId'), 'tokenId', True),
                '--marketTopicId', identifier(body.get('marketTopicId'), 'marketTopicId', True),
                '--side', body['side'], '--amount', decimal_string(body.get('amount'), 'amount'), '--orderType', kind]
    else:
        args = ['prediction', 'trade', 'place-order', '--quoteId', identifier(body.get('quoteId'), 'quoteId')]
        if 'slippageBps' not in body:
            raise ValueError('slippageBps is required')
        if 'orderType' in body:
            args += ['--orderType', kind]
        if 'priceLimit' in body:
            args += ['--priceLimit', decimal_string(body['priceLimit'], 'priceLimit')]
    if 'slippageBps' in body:
        args += ['--slippageBps', slippage(body['slippageBps'])]
    return args + ['--json']


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        # Do not log request headers, bodies or subprocess output.
        pass

    def reply(self, status, value, raw=False):
        payload = value if raw else json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(payload)

    def handle_request(self, post=False):
        token = os.environ.get('BAW_BRIDGE_TOKEN', '')
        supplied = self.headers.get('X-Bridge-Token', '')
        if not token or not hmac.compare_digest(token.encode(), supplied.encode()):
            self.reply(401, {'error': 'unauthorized'})
            return
        path = urlsplit(self.path)
        if post and path.path == '/wallet/login':
            try:
                self.reply(200, {'success': True, 'data': start_login()})
            except OSError:
                self.reply(502, {'error': '无法启动 baw 登录进程'})
            except RuntimeError as exc:
                self.reply(503, {'error': str(exc)})
            return
        if not post and path.path == '/wallet/login/status':
            self.reply(200, {'success': True, 'data': login_status(parse_qs(path.query).get('session_id', [''])[0])})
            return
        paths = ('/quote', '/place', '/redeem') if post else ('/status', '/balance', '/positions', '/orders', '/account')
        if self.path not in paths:
            self.reply(404, {'error': 'not found'})
            return
        try:
            body = None
            if post:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 16384 or self.headers.get('Transfer-Encoding'):
                    raise ValueError('invalid body length')
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise ValueError('expected a JSON object')
            args = command(self.path, body) if self.path != '/account' else None
        except (ValueError, TypeError) as exc:
            self.reply(400, {'error': str(exc)})
            return
        try:
            def run(args):
                with BAW_SLOTS:
                    result = subprocess.run([os.getenv('BAW_BIN', 'baw'), *args], capture_output=True, timeout=30, check=False)
                try:
                    data = json.loads(result.stdout)
                except ValueError:
                    raise ValueError(result.stderr.decode('utf-8', errors='replace').strip() or 'baw returned invalid JSON')
                if result.returncode or not isinstance(data, dict) or data.get('success') is not True:
                    raise ValueError(str(data.get('error', data.get('message', data))) if isinstance(data, dict) else 'baw failed')
                return data, result.stdout

            if self.path == '/account':
                account = {key: run(command('/' + key))[0].get('data')
                           for key in ('status', 'balance', 'positions', 'orders', 'pnl')}
                self.reply(200, {'success': True, 'data': account})
            else:
                with placement(body['quoteId']) if self.path == '/place' else contextlib.nullcontext():
                    data, raw = run(args)
                    if self.path == '/quote':
                        remember_quote(data, body)
                    elif self.path == '/status':
                        wallet = data.get('data')
                        address = wallet_address(wallet)
                        if wallet_logged_in(wallet) and address:
                            wallet['address'] = address
                            raw = json.dumps(data).encode()
                self.reply(200, raw, raw=True)
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            self.reply(502, {'error': str(exc)})

    def do_GET(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request(post=True)


if __name__ == '__main__':
    BridgeServer(listen_address(), Handler).serve_forever()
