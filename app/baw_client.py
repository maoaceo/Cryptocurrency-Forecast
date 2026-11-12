"""Async client for the host baw bridge. Placement is never retried."""
import os
import httpx
from app.service_config import resolve_baw


class BawError(Exception):
    def __init__(self, message, status_code=None):
        self.msg = str(message)
        self.status_code = status_code
        super().__init__(self.msg)


def executor_path(settings=None):
    value = (settings or {}).get('executor_mode') or os.getenv('EXECUTOR_PATH', 'sapi')
    if value not in ('sapi', 'baw'):
        raise ValueError('EXECUTOR_PATH must be sapi or baw')
    return value


class BawClient:
    def __init__(self, base_url=None, token=None, http=None, store=None):
        self.store = store
        self._base_url_override = base_url
        self._token_override = token
        config = resolve_baw(store.settings() if store is not None else {})
        self.base_url = (base_url if base_url is not None else config['bridge_url']).rstrip('/')
        self.token = token if token is not None else config['bridge_token']
        self.http = http or httpx.AsyncClient(timeout=35.0, trust_env=False)
        self._owns_http = http is None

    async def close(self):
        if self._owns_http:
            await self.http.aclose()

    async def _request(self, method, path, body=None):
        config = resolve_baw(self.store.settings() if self.store is not None else {})
        if not config['enabled']:
            raise BawError('BAW bridge is disabled')
        # Explicit arguments retain their original behavior for injected clients.
        if self.store is not None:
            self.base_url = (self._base_url_override if self._base_url_override is not None
                             else config['bridge_url']).rstrip('/')
            self.token = self._token_override if self._token_override is not None else config['bridge_token']
        if not self.base_url:
            raise BawError('BAW_BRIDGE_URL is not configured')
        if not self.token:
            raise BawError('BAW_BRIDGE_TOKEN is not configured')
        try:
            response = await self.http.request(method, self.base_url + path,
                headers={'X-Bridge-Token': self.token}, **({'json': body} if body is not None else {}))
        except httpx.HTTPError as exc:
            raise BawError(str(exc)) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise BawError(f'HTTP {response.status_code}: invalid bridge JSON', response.status_code) from exc
        if response.is_error or not isinstance(payload, dict) or payload.get('success') is not True:
            message = payload.get('error', payload.get('message', str(payload))) if isinstance(payload, dict) else str(payload)
            raise BawError(message, response.status_code)
        return payload.get('data')

    async def get_quote(self, **kwargs):
        return await self._request('POST', '/quote', kwargs)

    async def place_order_bundle(self, **kwargs):
        return await self._request('POST', '/place', kwargs)

    async def position_list(self):
        return await self._request('GET', '/positions')

    async def order_history(self):
        return await self._request('GET', '/orders')

    async def wallet_balance(self):
        return await self._request('GET', '/balance')

    async def status(self):
        return await self._request('GET', '/status')

    async def account(self):
        return await self._request('GET', '/account')

    async def redeem(self, tokenIds):
        return await self._request('POST', '/redeem', {'tokenIds': tokenIds})
