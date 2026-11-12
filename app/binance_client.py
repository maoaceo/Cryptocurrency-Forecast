"""Small async client for Binance's signed Prediction Trading REST API.

Every signature covers precisely the form/query bytes sent on the wire. In
particular batch-cancel's bracketed keys remain raw, as required by SAPI.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal, ROUND_DOWN
import hashlib
import hmac
import time
from typing import Any
from urllib.parse import quote

import httpx
from app.service_config import resolve_binance

PREFIX = "/sapi/v1/w3w/wallet/prediction"
WEI = Decimal("1000000000000000000")


def money_down(amount: Any) -> Decimal:
    """Truncate API monetary inputs to two decimals, without binary floats."""
    value = Decimal(str(amount))
    if not value.is_finite():
        raise ValueError("金额必须为有限数字")
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def usdt_to_wei(amount: Any) -> str:
    value = Decimal(str(amount))
    if not value.is_finite() or value < 0:
        raise ValueError("金额必须为非负有限数字")
    return str(int((value * WEI).to_integral_value(rounding=ROUND_DOWN)))


def wei_to_usdt(amount: Any) -> Decimal:
    return Decimal(str(amount)) / WEI


class BinanceError(Exception):
    def __init__(self, code: int | str, msg: str, status_code: int | None = None):
        try:
            self.code = int(code)
        except (ValueError, TypeError):
            self.code = code
        self.msg = str(msg)
        self.status_code = status_code
        mapping = {
            -1002: "权限不足（预测交易权限待开通）",
            -1022: "签名无效",
            -2015: "API Key/IP/权限无效",
            -3026: "参数/钱包不匹配",
            -1021: "请求时间与服务器不同步",
            -1003: "请求频率过高，请稍后重试",
            -1001: "币安服务暂时不可用",
        }
        lower = self.msg.lower()
        if (self.code in {-2010, -2018, -2019, -4026} and any(
            term in lower for term in ("balance", "margin", "fund")
        )) or any(term in lower for term in ("insufficient balance", "insufficient fund", "not enough balance", "余额不足")):
            self.user_message = "余额不足"
        else:
            self.user_message = mapping.get(self.code, "币安请求失败")
        super().__init__(f"{self.user_message} ({self.code}): {self.msg}")

    @property
    def permission_pending(self) -> bool:
        return self.code == -1002


def _canonical(params: dict[str, Any], raw_brackets: bool = False) -> str:
    def value_text(value: Any) -> str:
        return str(value).lower() if isinstance(value, bool) else str(value)
    return "&".join(
        f"{quote(str(key), safe='[]' if raw_brackets else '')}={quote(value_text(value), safe='')}"
        for key, value in params.items() if value is not None
    )


class BinanceClient:
    def __init__(self, key: str | None = None, secret: str | None = None, base_url: str = "https://api.binance.com",
                 http: httpx.AsyncClient | None = None, store=None):
        self.store = store
        self._key_override, self._secret_override = key, secret
        config = resolve_binance(store.settings() if store is not None else {})
        self.key = key if key is not None else config['api_key']
        self.secret = secret if secret is not None else config['api_secret']
        self.base_url = base_url.rstrip("/")
        self.http = http or httpx.AsyncClient(timeout=20.0)
        self._owns_http = http is None
        self.time_offset_ms = 0
        self._last_time_sync = 0.0
        self._time_lock = asyncio.Lock()

    async def close(self) -> None:
        if self._owns_http:
            await self.http.aclose()

    async def sync_time(self) -> None:
        async with self._time_lock:
            started = time.time() * 1000
            try:
                response = await self.http.get(self.base_url + "/api/v3/time")
                response.raise_for_status()
                server_time = int(response.json()["serverTime"])
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                raise BinanceError(-1001, "服务器时间同步失败") from exc
            self.time_offset_ms = int(server_time - (started + time.time() * 1000) / 2)
            self._last_time_sync = time.monotonic()

    async def _request(self, method: str, path: str, params: dict | None = None,
                       *, retry: bool = True, raw_brackets: bool = False,
                       signed: bool = True) -> Any:
        key, secret = self.key, self.secret
        if signed:
            config = resolve_binance(self.store.settings() if self.store is not None else {})
            if not config['enabled']:
                raise BinanceError(-1002, "Binance API is disabled")
            if self.store is not None:
                self.key = self._key_override if self._key_override is not None else config['api_key']
                self.secret = self._secret_override if self._secret_override is not None else config['api_secret']
            key, secret = self.key, self.secret
            if not key or not secret:
                raise BinanceError(-1002, "BINANCE_API_KEY and BINANCE_API_SECRET are not configured")
        if signed and (not self._last_time_sync or time.monotonic() - self._last_time_sync > 300):
            await self.sync_time()
        attempts = 3 if retry else 1
        for attempt in range(attempts):
            fields = dict(params or {})
            if signed:
                fields["timestamp"] = int(time.time() * 1000) + self.time_offset_ms
                fields["recvWindow"] = 5000
            payload = _canonical(fields, raw_brackets)
            if signed:
                signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
                payload += "&signature=" + signature
            headers = {"X-MBX-APIKEY": key} if signed else {}
            request_args: dict[str, Any] = {"headers": headers}
            url = self.base_url + path
            if method == "GET":
                url = httpx.URL(url, query=payload.encode())
            else:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                request_args["content"] = payload.encode()
            try:
                response = await self.http.request(method, url, **request_args)
            except httpx.HTTPError as exc:
                # Never retry a mutation after an ambiguous network outcome.
                if attempt + 1 < attempts:
                    await asyncio.sleep(0.5 * 2 ** attempt)
                    continue
                raise BinanceError(-1001, "网络请求失败；下单结果可能未知，请核对订单") from exc
            try:
                data = response.json()
            except ValueError:
                data = {"code": -1001, "msg": f"HTTP {response.status_code} 非 JSON 响应"}
            code = data.get("code", 0) if isinstance(data, dict) else 0
            failed = str(code) not in {"0", "000000", "None"}
            if response.status_code == 429 or str(code) == "-1003":
                if attempt + 1 < attempts:
                    try:
                        delay = min(60.0, max(0.5, float(response.headers.get("Retry-After", 2 ** attempt))))
                    except ValueError:
                        delay = 2 ** attempt
                    await asyncio.sleep(delay)
                    continue
            if str(code) == "-1021" and attempt + 1 < attempts:
                await self.sync_time()
                continue
            if failed or response.is_error:
                raise BinanceError(code if failed else -1003 if response.status_code == 429 else -1001,
                                   data.get("msg", data.get("message", f"HTTP {response.status_code}"))
                                   if isinstance(data, dict) else str(data), response.status_code)
            # SAPI installations may use a code/data envelope or return directly.
            return data["data"] if isinstance(data, dict) and "data" in data and "code" in data else data

    async def category_list(self, **kwargs):
        return await self._request("GET", PREFIX + "/category/list", kwargs)

    async def market_list(self, **kwargs):
        return await self._request("GET", PREFIX + "/market/list", kwargs)

    async def market_detail(self, marketTopicId=None, **kwargs):
        return await self._request("GET", PREFIX + "/market/detail", {"marketTopicId": marketTopicId, **kwargs})

    async def order_book(self, vendor=None, marketId=None, tokenId=None, **kwargs):
        return await self._request("GET", PREFIX + "/order-book", {"vendor": vendor, "marketId": marketId, "tokenId": tokenId, **kwargs})

    async def last_trade_price(self, marketId=None, **kwargs):
        return await self._request("GET", PREFIX + "/order-book/last-trade-price", {"marketId": marketId, **kwargs})

    async def get_quote(self, **kwargs):
        fields = {"chainId": "56", "feeRateBps": 200, "orderType": "MARKET", "slippageBps": 1200, **kwargs}
        return await self._request("POST", PREFIX + "/trade/get-quote", fields)

    async def place_order_bundle(self, **kwargs):
        fields = {"accountType": "SPOT", "orderType": "MARKET", "slippageBps": 1200, **kwargs}
        fields.setdefault("timeInForce", "FOK" if fields["orderType"] == "MARKET" else "GTC")
        if fields["timeInForce"] != ("FOK" if fields["orderType"] == "MARKET" else "GTC"):
            raise ValueError("MARKET 必须使用 FOK，LIMIT 必须使用 GTC")
        return await self._request("POST", PREFIX + "/trade/place-order-bundle", fields, retry=False)

    async def batch_cancel(self, walletAddress=None, walletId=None, cancelInfoList=None, **kwargs):
        fields = {"walletAddress": walletAddress, "walletId": walletId, **kwargs}
        for index, item in enumerate(cancelInfoList or []):
            fields[f"cancelInfoList[{index}].orderId"] = item["orderId"] if isinstance(item, dict) else item
        return await self._request("POST", PREFIX + "/trade/batch-cancel", fields, retry=False, raw_brackets=True)

    async def order_list(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/order/list", {"walletAddress": walletAddress, **kwargs})

    async def order_history(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/order/history", {"walletAddress": walletAddress, **kwargs})

    async def position_list(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/position/list", {"walletAddress": walletAddress, **kwargs})

    async def settled_history(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/position/settled-history", {"walletAddress": walletAddress, **kwargs})

    async def pnl_portfolio(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/pnl/portfolio", {"walletAddress": walletAddress, **kwargs})

    async def pnl_query(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/pnl/query", {"walletAddress": walletAddress, **kwargs})

    async def wallet_list(self, **kwargs):
        return await self._request("GET", PREFIX + "/wallet/list", kwargs)

    async def payment_options(self, walletAddress=None, **kwargs):
        return await self._request("GET", PREFIX + "/balance/payment-options", {"walletAddress": walletAddress, **kwargs})

    async def klines(self, symbol, interval="5m", limit=120, **kwargs):
        return await self._request("GET", "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit, **kwargs}, signed=False)
