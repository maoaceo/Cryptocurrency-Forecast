"""Offline baw integration tests: in-memory HTTP only, never invoke the real CLI."""
from concurrent.futures import ThreadPoolExecutor
import threading
import hashlib
import hmac
from types import SimpleNamespace
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch, Mock, AsyncMock

import httpx
from app.baw_client import BawClient, BawError
from app.binance_client import BinanceClient, BinanceError
from app.db import Store
from app.risk import Executor, Risk
from app.sync import Sync, slug_window
from bridge.baw_bridge import Handler
from bridge import baw_bridge as bridge

TOKEN = 'offline-bridge-token'


class Stub:
    """Tiny HTTP bridge stub, served through httpx without opening sockets."""
    def __init__(self, fixture):
        self.fixture = fixture

    def __call__(self, request):
        fixture = self.fixture
        path = request.url.path
        body = json.loads(request.content or '{}')
        fixture.calls.append((path, body))
        assert request.headers.get('X-Bridge-Token') == TOKEN
        if path == '/quote' and fixture.on_quote:
            fixture.on_quote()
        if path == '/account':
            return httpx.Response(200, json={'success': True, 'data': {
                key: fixture.payloads['/' + key]['data'] for key in ('status', 'positions', 'orders', 'balance', 'pnl')
                if '/' + key in fixture.payloads}})
        return httpx.Response(fixture.place_status if path == '/place' else 200,
                              json=fixture.payloads[path])


class Connection:
    """Feed real HTTP bytes to the stdlib handler without a listening socket."""
    def __init__(self, raw):
        self.input = BytesIO(raw)
        self.output = bytearray()

    def makefile(self, *args):
        return self.input

    def sendall(self, data):
        self.output.extend(data)


class BawTests(unittest.IsolatedAsyncioTestCase):
    def test_slug_window_parsing(self):
        for asset, duration, seconds in [('btc', '5m', 300), ('btc', '15m', 900), ('eth', '15m', 900),
                                         ('eth', '1h', 3600), ('btc', '1d', 86400)]:
            with self.subTest(asset=asset, duration=duration):
                self.assertEqual(slug_window(f'{asset}-updown-{duration}-1794224100'),
                                 (asset.upper() + 'USDT', 1794224100, 1794224100 + seconds))
        for slug in [None, '', 'sol-updown-5m-1794224100', 'btc-updown-0m-1794224100',
                     'btc-updown-5m-0', 'prefix-btc-updown-5m-1794224100',
                     'btc-updown-5m-1794224100-extra', 'btc-updown-5m-1794224100\n']:
            with self.subTest(slug=slug):
                self.assertIsNone(slug_window(slug))

    async def asyncSetUp(self):
        self.env = patch.dict(os.environ, {'EXECUTOR_PATH': 'baw'})
        self.env.start()
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / 'test.db')
        self.calls = []
        self.on_quote = None
        self.place_status = 200
        self.payloads = {path: {'success': True, 'data': data} for path, data in {
            '/status': {'status': 'CONNECTED'},
            '/balance': [{'symbol': 'USDT', 'binanceChainId': 56, 'balance': '42.25'}],
            '/positions': {'summary': {'todayRealizedPnl': '4'}, 'positions': [
                {'marketId': 'external', 'tokenId': '456', 'currentValue': '12', 'totalCost': '9'}]},
            '/orders': {'total': 0, 'orders': []},
            '/pnl': {'pnl': None, 'pnlList': []},
            '/quote': {'quoteId': 'q1', 'slippageBps': 600, 'averagePrice': '.4', 'amountOut': '25',
                'feeAmount': '.2', 'minReceive': '22', 'expireAt': datetime.fromtimestamp(time.time()+60, timezone.utc).isoformat()},
            '/place': {'orderId': 'o1'},
            '/redeem': {'batchId': 'batch-1'},
        }.items()}
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(Stub(self)))
        self.client = BawClient('http://offline-bridge', TOKEN, self.http)
        # Any accidental SAPI account request fails the test.
        self.sapi = object()
        self.sync = Sync(self.store, self.sapi, self.client)
        self.executor = Executor(self.store, self.sapi, self.client)
        self.market = {'market_id': '1', 'topic_id': '2', 'title': 'offline',
            'start_time': time.time()-60, 'end_time': time.time()+3600,
            'outcomes': [{'name': 'UP', 'price': '.4', 'tokenId': '123'}]}

    async def asyncTearDown(self):
        await self.client.close()
        await self.http.aclose()
        self.store.close()
        self.temp.cleanup()
        self.env.stop()

    async def test_eval_history_backfill_scoring_and_no_calls(self):
        end=int(time.time()//300)*300-300
        voters=[{'name':name,'status':'ok','p_up':p} for name,p in
                [('up',.8),('down',.2),('neutral',.5),('upper-boundary',.52),('lower-boundary',.48)]]
        voters.append({'name':'failed','status':'abstain','p_up':.9})
        for index,close in enumerate((110,90,100)):
            window_end=end-index*300
            market={'market_id':f'eval-{index}','symbol':'BTCUSDT',
                    'start_time':window_end-300,'end_time':window_end}
            self.store.record_evaluation(window_end-290,market,voters,.7)
        original=self.store.rows('SELECT * FROM eval_history ORDER BY id')
        closes={int((end-i*300)*1000)-1:c for i,c in enumerate((110,90,100))}
        async def klines(symbol,interval,limit,startTime,endTime):
            return [[startTime,'100','120','80',str(closes[endTime]),'1',endTime]]
        source=Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client=source
        await self.sync._settlement_prices()
        self.assertEqual(source.klines.await_count,3)
        self.assertEqual(self.store.rows('SELECT outcome FROM eval_scores ORDER BY eval_id'),
                         [{'outcome':'UP'},{'outcome':'DOWN'},{'outcome':'DOWN'}])
        self.assertEqual(self.store.meta('voter_stats'),
                         {'up':{'scored':3,'correct':1},'down':{'scored':3,'correct':2}})
        self.assertEqual(self.store.rows('SELECT * FROM eval_history ORDER BY id'),original)
        await self.sync._settlement_prices()
        self.assertEqual(source.klines.await_count,3)
        self.assertEqual(self.store.meta('voter_stats')['up']['scored'],3)

    def test_retired_voters_ignored_in_pending_and_historical_scores(self):
        end = int(time.time() // 300) * 300 - 300
        retired = ('gemini-3.8-flash', 'gemini-4-argon-high', 'EMA趋势', 'RSI(14)',
                   'MACD', '一小时动量', '布林带位置', '成交量趋势')
        market = {'market_id': 'retired', 'symbol': 'BTCUSDT',
                  'start_time': end - 300, 'end_time': end}
        voters = [{'name': name, 'status': 'ok', 'p_up': .8}
                  for name in ('gpt-6-astra', *retired)]
        self.store.record_evaluation(end - 290, market, voters, .8)
        self.store.set_meta('market_prices', {'retired': {'start_price': 100}})
        self.store.set_meta('settlement_prices', {'retired': 110})
        self.store.score_evaluations()
        row = self.store.rows('SELECT * FROM eval_scores')[0]
        self.assertEqual(json.loads(row['votes_json']), {'gpt-6-astra': 1})
        # Emulate already persisted pre-removal scores, without rewriting history.
        historical = json.dumps({'gpt-6-astra': 1, **{name: None for name in retired}})
        self.store.execute('UPDATE eval_scores SET votes_json=? WHERE eval_id=?',
                           (historical, row['eval_id']))
        self.store.set_meta('voter_stats', {name: {'scored': 30, 'correct': 30} for name in retired})
        # A second evaluation shares the window; only the latest active vote counts.
        self.store.record_evaluation(end - 280, market,
                                     [{'name': 'gpt-6-astra', 'status': 'ok', 'p_up': .2}], .2)
        for _ in range(2):
            self.store.score_evaluations()
            self.assertEqual(self.store.meta('voter_stats'),
                             {'gpt-6-astra': {'scored': 1, 'correct': 0}})
            self.assertEqual(self.store.rows('SELECT votes_json FROM eval_scores WHERE eval_id=?',
                                             (row['eval_id'],))[0]['votes_json'], historical)

    def test_voter_stats_roll_over_300_windows_and_survive_restart(self):
        end=int(time.time()//300)*300-300
        starts={};ends={}
        for i in range(301):
            market={'market_id':str(i),'symbol':'BTCUSDT','start_time':end-i*300-300,'end_time':end-i*300}
            voters=[{'name':'voter','status':'ok','p_up':.8 if i==300 else .2}]
            self.store.record_evaluation(i,market,voters,.6)
            starts[str(i)]={'start_price':100};ends[str(i)]=110
        self.store.set_meta('market_prices',starts)
        self.store.set_meta('settlement_prices',ends)
        self.store.score_evaluations()
        self.assertEqual(self.store.meta('voter_stats'),{'voter':{'scored':300,'correct':0}})
        # Re-evaluating the newest window replaces its sample rather than adding one.
        newest={'market_id':'0','symbol':'BTCUSDT','start_time':end-300,'end_time':end}
        self.store.record_evaluation(999,newest,[{'name':'voter','status':'ok','p_up':.9}],.9)
        self.store.score_evaluations()
        self.assertEqual(self.store.meta('voter_stats'),{'voter':{'scored':300,'correct':1}})
        reopened=Store(self.store.path)
        try:
            reopened.score_evaluations()
            self.assertEqual(reopened.meta('voter_stats'),{'voter':{'scored':300,'correct':1}})
        finally:reopened.close()

    async def test_settlement_fetch_is_cached_and_failures_are_nonfatal(self):
        end = int(time.time() // 300) * 300 - 300
        row = self.payloads['/positions']['data']['positions'][0]
        row.update(endDate=datetime.fromtimestamp(end, timezone.utc).isoformat(),
                   marketTitle='Bitcoin Up or Down')
        source = Mock()
        source.klines = AsyncMock(return_value=[[(end-300)*1000, '1', '2', '1', '123.45', '1', end*1000-1]])
        self.sync.client = source
        first = await self.sync.run_once()
        self.assertFalse(first['errors'])
        self.assertEqual(self.store.meta('settlement_prices'), {'external': 123.45})
        source.klines.assert_awaited_once_with('BTCUSDT', interval='5m', limit=1,
                                             startTime=(end-300)*1000, endTime=end*1000-1)
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 1)
        # A new expired market can fail without affecting sync health.
        row['marketId'] = 'failed'
        source.klines.side_effect = RuntimeError('price endpoint unavailable')
        state = await self.sync.run_once()
        self.assertFalse(state['errors'])
        self.assertNotIn('failed', self.store.meta('settlement_prices'))

    async def test_settlement_fetch_limit_and_symbol_inference(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': str(i), 'endDate': end, 'question': 'Ethereum Up or Down'}
            for i in range(5)]
        source = Mock()
        source.klines = AsyncMock(side_effect=RuntimeError('unavailable'))
        self.sync.client = source
        state = await self.sync.run_once()
        self.assertFalse(state['errors'])
        self.assertEqual(source.klines.await_count, 3)
        self.assertTrue(all(call.args == ('ETHUSDT',) for call in source.klines.await_args_list))

    async def test_price_backfill_position_day_and_audit_decision_are_cached(self):
        end = int(time.time() // 300) * 300 - 300
        row = self.payloads['/positions']['data']['positions'][0]
        row.update(startDate=datetime.fromtimestamp(end - 86400, timezone.utc).isoformat(),
                   endDate=end * 1000, question='Bitcoin Up or Down 1d')
        for mid, title in [('audit', 'Ethereum Up or Down 5m'), ('explicit', 'Bitcoin Up or Down 15m')]:
            self.store.execute('INSERT INTO orders_audit (ts, market_id, status) VALUES (?, ?, ?)',
                               (end, mid, 'SUCCESS'))
            self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                               (end, mid, title, end, 'BUY'))

        async def klines(symbol, interval, limit, startTime, endTime):
            return [[ts, '100', '120', '90', '110', '1', ts + 299999]
                    for ts in range(startTime, endTime, 300000)]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        state = await self.sync.run_once()
        self.assertFalse(state['errors'])
        source.klines.assert_any_await('BTCUSDT', interval='5m', limit=288,
                                     startTime=(end - 86400)*1000, endTime=end*1000-1)
        source.klines.assert_any_await('ETHUSDT', interval='5m', limit=1,
                                     startTime=(end - 300)*1000, endTime=end*1000-1)
        prices = self.store.meta('market_prices')
        self.assertEqual(set(prices), {'external', 'audit', 'explicit'})
        self.assertTrue(all(p['start_price'] == 100 for p in prices.values()))
        self.assertEqual(prices['audit']['symbol'], 'ETHUSDT')
        self.assertEqual(self.store.meta('settlement_prices'), dict.fromkeys(prices, 110))
        self.assertEqual(set(self.store.meta('price_backfill_done')), set(prices))
        # Even removal of price context must not trigger a completed backfill.
        self.store.set_meta('market_prices', {})
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 3)

    async def test_audit_backfill_paginates_long_window(self):
        end = int(time.time() // 300) * 300 - 300
        self.store.execute('INSERT INTO orders_audit (ts, market_id) VALUES (?, ?)', (end, 'week'))
        self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                           (end, 'week', 'Bitcoin Up or Down 7-day', end, 'BUY'))

        async def klines(symbol, interval, limit, startTime, endTime):
            return [[ts, '100', '120', '90', '110', '1', ts+299999]
                    for ts in range(startTime, min(endTime+1, startTime+limit*300000), 300000)]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(source.klines.await_count, 3)
        self.assertEqual(self.store.meta('market_prices')['week']['start_price'], 100)
        self.assertEqual(self.store.meta('settlement_prices')['week'], 110)
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 3)

    async def test_audit_history_backfills_opening_and_settlement_without_decisions(self):
        end = int(time.time() // 300) * 300 - 300
        self.payloads['/positions']['data']['positions'] = []
        self.store.execute('INSERT INTO orders_audit (ts, market_id, order_id, topic_id) VALUES (?, ?, ?, ?)',
                           (end, 'history', 'exact-order', 'shared-topic'))
        self.payloads['/orders']['data'] = {'total': 2, 'orders': [
            {'orderId': 'other-order', 'marketTopicId': 'shared-topic', 'slug': f'eth-updown-1h-{end-3600}'},
            {'orderId': 'exact-order', 'marketTopicId': 'different-topic', 'slug': f'btc-updown-5m-{end-300}'}]}
        source = Mock(klines=AsyncMock(return_value=[
            [(end-300)*1000, '100', '120', '90', '110', '1', end*1000-1]]))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        source.klines.assert_awaited_once_with('BTCUSDT', interval='5m', limit=1,
                                             startTime=(end-300)*1000, endTime=end*1000-1)
        self.assertEqual(self.store.meta('market_prices')['history'], {'symbol': 'BTCUSDT', 'start_price': 100})
        self.assertEqual(self.store.meta('settlement_prices'), {'history': 110})
        self.assertEqual(self.store.rows('SELECT * FROM decisions'), [])
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 1)

    async def test_fifteen_minute_history_slug_overrides_ambiguous_or_stale_decision(self):
        end = int(time.time() // 900) * 900 - 900
        self.payloads['/positions']['data']['positions'] = []
        self.store.update_settings({'min_duration': 300, 'max_duration': 300})
        for title in ('Bitcoin Up or Down', 'Bitcoin Up or Down 5m'):
            with self.subTest(title=title):
                self.store.execute('DELETE FROM orders_audit')
                self.store.execute('DELETE FROM decisions')
                self.store.set_meta('market_prices', {})
                self.store.set_meta('settlement_prices', {})
                self.store.set_meta('price_backfill_done', [])
                self.store.execute('INSERT INTO orders_audit (ts, market_id, order_id) VALUES (?, ?, ?)',
                                   (end, 'history', 'exact-order'))
                self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                                   (end, 'history', title, end, 'BUY'))
                self.payloads['/orders']['data'] = {'total': 1, 'orders': [
                    {'orderId': 'exact-order', 'slug': f'btc-updown-15m-{end-900}'}]}
                source = Mock(klines=AsyncMock(return_value=[
                    [(end-900)*1000, '100', '120', '90', '101', '1', (end-600)*1000-1],
                    [(end-600)*1000, '101', '120', '90', '102', '1', (end-300)*1000-1],
                    [(end-300)*1000, '102', '120', '90', '110', '1', end*1000-1]]))
                self.sync.client = source
                self.assertFalse((await self.sync.run_once())['errors'])
                source.klines.assert_awaited_once_with('BTCUSDT', interval='5m', limit=3,
                                                     startTime=(end-900)*1000, endTime=end*1000-1)
                self.assertEqual(self.store.meta('market_prices')['history']['start_price'], 100)
                self.assertEqual(self.store.meta('settlement_prices'), {'history': 110})

    async def test_ambiguous_historical_duration_does_not_guess_from_settings(self):
        end = int(time.time() // 900) * 900 - 900
        self.payloads['/positions']['data']['positions'] = []
        self.store.update_settings({'min_duration': 900, 'max_duration': 900})
        self.store.execute('INSERT INTO orders_audit (ts, market_id) VALUES (?, ?)', (end, 'unknown'))
        self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                           (end, 'unknown', 'Bitcoin Up or Down', end, 'BUY'))
        source = Mock(klines=AsyncMock())
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        source.klines.assert_not_awaited()
        self.assertEqual(self.store.meta('price_backfill_attempts', {}), {})

    async def test_audit_history_topic_fallback_backfills_only_missing_settlement(self):
        end = int(time.time() // 300) * 300 - 300
        self.payloads['/positions']['data']['positions'] = []
        self.store.execute('INSERT INTO orders_audit (ts, market_id, order_id, topic_id) VALUES (?, ?, ?, ?)',
                           (end, 'history', 'absent-order', '123'))
        self.payloads['/orders']['data'] = {'total': 1, 'orders': [
            {'orderId': 'venue-order', 'marketTopicId': 123, 'slug': f'eth-updown-15m-{end-900}'}]}
        context = {'history': {'start_price': 99, 'last_spot': 109}}
        self.store.set_meta('market_prices', context)
        self.store.set_meta('price_backfill_done', ['history'])
        source = Mock(klines=AsyncMock(return_value=[
            [(end-300)*1000, 'invalid-open', '120', '90', '110', '1', end*1000-1]]))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        source.klines.assert_awaited_once_with('ETHUSDT', interval='5m', limit=1,
                                             startTime=(end-300)*1000, endTime=end*1000-1)
        self.assertEqual(self.store.meta('market_prices'), context)
        self.assertEqual(self.store.meta('settlement_prices'), {'history': 110})

    async def test_failed_audit_without_matching_venue_order_never_fetches(self):
        end = int(time.time() // 300) * 300 - 300
        self.payloads['/positions']['data']['positions'] = []
        self.store.execute('INSERT INTO orders_audit (ts, market_id, topic_id, status) VALUES (?, ?, ?, ?)',
                           (end, 'failed', 'absent-topic', 'FAILED'))
        self.payloads['/orders']['data'] = {'total': 1, 'orders': [
            {'marketTopicId': 'other-topic', 'slug': f'btc-updown-5m-{end-300}'}]}
        source = Mock(klines=AsyncMock())
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        source.klines.assert_not_awaited()
        self.assertEqual(self.store.meta('price_backfill_attempts', {}), {})
        self.assertEqual(self.store.meta('settlement_prices', {}), {})

    async def test_audit_history_shares_newest_first_budget_and_gives_up(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': 'position', 'startDate': end-600, 'endDate': end-300, 'title': 'Bitcoin'}]
        self.store.set_meta('market_prices', {'position': {'start_price': 99}})
        orders = []
        # Audit insertion order differs from window order to check global sorting.
        for mid, offset in [('old', 900), ('new', 0), ('middle', 600)]:
            self.store.execute('INSERT INTO orders_audit (ts, market_id, order_id) VALUES (?, ?, ?)',
                               (end, mid, mid))
            orders.append({'orderId': mid, 'slug': f'btc-updown-5m-{end-offset-300}'})
        self.payloads['/orders']['data'] = {'total': len(orders), 'orders': orders}
        source = Mock(klines=AsyncMock(side_effect=RuntimeError('offline')))
        self.sync.client = source
        for attempt in range(1, 4):
            self.assertFalse((await self.sync.run_once())['errors'])
            self.assertEqual(source.klines.await_count, attempt*3)
            self.assertEqual(self.store.meta('price_backfill_attempts'),
                             dict.fromkeys(('new', 'position', 'middle'), attempt))
        self.assertEqual([call.kwargs['endTime'] for call in source.klines.await_args_list[:3]],
                         [(end-offset)*1000-1 for offset in (0, 300, 600)])
        self.assertEqual(set(self.store.meta('price_backfill_done')), {'new', 'position', 'middle'})
        source.klines.side_effect = None
        source.klines.return_value = [[(end-1200)*1000, '100', '120', '90', '110', '1', (end-900)*1000-1]]
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(source.klines.await_count, 10)
        self.assertEqual(self.store.meta('settlement_prices'), {'old': 110})

    async def test_audit_backfills_only_settlement_after_position_rotates_out(self):
        boundary = int(time.time() // 300) * 300 - 1200
        end = boundary + 937.456
        self.payloads['/positions']['data']['positions'] = []
        self.store.execute('INSERT INTO orders_audit (ts, market_id) VALUES (?, ?)', (end, 'redeemed'))
        self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                           (end, 'redeemed', 'Ethereum Up or Down 1d', end, 'BUY'))
        context = {'redeemed': {'start_price': 100, 'last_spot': 109, 'updated_ts': 123}}
        self.store.set_meta('market_prices', context)
        # An opening backfill may have completed while the window was open.
        self.store.set_meta('price_backfill_done', ['redeemed'])
        self.store.set_meta('price_backfill_attempts', {'redeemed': 1})
        source = Mock(klines=AsyncMock(return_value=[
            [(boundary + 600)*1000, 'invalid-open', '120', '90', '110', '1', (boundary + 900)*1000-1]]))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        source.klines.assert_awaited_once_with('ETHUSDT', interval='5m', limit=1,
                                             startTime=(boundary + 600)*1000, endTime=int(end*1000)-1)
        self.assertEqual(self.store.meta('settlement_prices'), {'redeemed': 110})
        self.assertEqual(self.store.meta('market_prices'), context)
        self.assertEqual(self.store.meta('price_backfill_attempts'), {})
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 1)

    async def test_settlement_only_candidates_share_budget_and_window_priority(self):
        end = int(time.time() // 300) * 300 - 300
        self.payloads['/positions']['data']['positions'] = []
        for mid, offset in [('old-opening', 900), ('settlement', 0), ('opening', 300), ('older-settlement', 600)]:
            self.store.execute('INSERT INTO orders_audit (ts, market_id) VALUES (?, ?)', (end, mid))
            self.store.execute('INSERT INTO decisions (ts, market_id, market, variant_end_time, action) VALUES (?, ?, ?, ?, ?)',
                               (end, mid, 'Bitcoin Up or Down 1d', end-offset, 'BUY'))
        self.store.set_meta('market_prices', {mid: {'start_price': 99} for mid in ('settlement', 'older-settlement')})

        async def klines(symbol, interval, limit, startTime, endTime):
            return [[ts, '100', '120', '90', '110', '1', ts+299999]
                    for ts in range(startTime, endTime, 300000)]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual([call.kwargs['endTime'] for call in source.klines.await_args_list],
                         [(end-offset)*1000-1 for offset in (0, 300, 600)])
        self.assertEqual([call.kwargs['limit'] for call in source.klines.await_args_list], [1, 288, 1])
        self.assertEqual(set(self.store.meta('settlement_prices')), {'settlement', 'opening', 'older-settlement'})

    async def test_settlement_only_gives_up_after_three_failures_and_releases_budget(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': str(i), 'startDate': end-600, 'endDate': end,
             'marketTitle': 'Bitcoin'} for i in range(3)] + [
            {**original, 'marketId': 'good', 'startDate': end-900, 'endDate': end-300,
             'marketTitle': 'Ethereum'}]
        self.store.set_meta('market_prices', {str(i): {'start_price': 100} for i in range(3)})
        source = Mock(klines=AsyncMock(side_effect=RuntimeError('offline')))
        self.sync.client = source
        for attempt in range(1, 4):
            self.assertFalse((await self.sync.run_once())['errors'])
            self.assertEqual(source.klines.await_count, 3*attempt)
            self.assertEqual(self.store.meta('price_backfill_attempts'), dict.fromkeys(('0', '1', '2'), attempt))
        self.assertEqual(set(self.store.meta('price_backfill_done')), {'0', '1', '2'})
        self.sync = Sync(self.store, source, self.client)
        source.klines.side_effect = None
        source.klines.return_value = [[(end-900)*1000, '100', '120', '90', '110', '1', (end-300)*1000-1]]
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(source.klines.await_count, 10)
        self.assertEqual(self.store.meta('settlement_prices'), {'good': 110})

    async def test_open_position_backfill_preserves_spot_and_defers_settlement(self):
        start = int(time.time() // 300) * 300 - 300
        row = self.payloads['/positions']['data']['positions'][0]
        row.update(startDate=start, endDate=start+86400, title='Ethereum Up or Down')
        self.store.set_meta('market_prices', {'external': {'start_price': None, 'last_spot': 105, 'updated_ts': 123}})
        self.store.set_meta('price_backfill_attempts', {'external': 2, 'other': 1})

        async def klines(*args, **kwargs):
            # Simulate an evaluation publishing a quote while the fetch awaits.
            self.store.set_meta('market_prices', {'external': {'start_price': None, 'last_spot': 107, 'updated_ts': 456}})
            self.store.set_meta('price_backfill_done', ['other'])
            return [[start*1000, '100', '110', '90', '105', '1', (start+300)*1000-1]]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(self.store.meta('market_prices')['external'],
                         {'symbol': 'ETHUSDT', 'start_price': 100, 'last_spot': 107, 'updated_ts': 456})
        self.assertEqual(self.store.meta('price_backfill_attempts'), {'other': 1})
        self.assertEqual(set(self.store.meta('price_backfill_done')), {'other', 'external'})
        self.assertNotIn('external', self.store.meta('settlement_prices', {}))
        await self.sync.run_once()
        self.assertEqual(source.klines.await_count, 1)

    async def test_backfill_and_settlement_share_budget_and_retry_failures(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': str(i), 'startDate': end-86400, 'endDate': end,
             'marketTitle': 'Bitcoin Up or Down'} for i in range(4)] + [
            {**original, 'marketId': 'settlement-only', 'endDate': end, 'marketTitle': 'Bitcoin'}]
        source = Mock(klines=AsyncMock(side_effect=RuntimeError('offline')))
        self.sync.client = source
        for count in (3, 6):
            state = await self.sync.run_once()
            self.assertFalse(state['errors'])
            self.assertEqual(source.klines.await_count, count)
            self.assertFalse(self.store.meta('price_backfill_done', []))

    async def test_backfill_accepts_containing_or_next_candle_for_misaligned_start(self):
        boundary = int(time.time() // 300) * 300 - 1200
        row = self.payloads['/positions']['data']['positions'][0]
        row.update(startDate=boundary + 37.123, endDate=boundary + 937.456,
                   marketTitle='Bitcoin Up or Down')
        for first_offset in (0, 300):
            with self.subTest(first_offset=first_offset):
                self.store.set_meta('price_backfill_done', [])
                self.store.set_meta('market_prices', {})
                self.store.set_meta('settlement_prices', {})
                candles = [[ts * 1000, '100', '120', '90', str(110 + i), '1', (ts + 300) * 1000 - 1]
                           for i, ts in enumerate(range(boundary + first_offset, boundary + 1200, 300))]
                source = Mock(klines=AsyncMock(return_value=candles))
                self.sync.client = source
                self.assertFalse((await self.sync.run_once())['errors'])
                source.klines.assert_awaited_once_with('BTCUSDT', interval='5m', limit=4,
                                                     startTime=boundary * 1000,
                                                     endTime=int((boundary + 937.456) * 1000) - 1)
                self.assertEqual(self.store.meta('market_prices')['external']['start_price'], 100)
                # The final returned candle closes after the window; use its predecessor.
                self.assertEqual(self.store.meta('settlement_prices')['external'], 112 - first_offset // 300)
                self.assertEqual(self.store.meta('price_backfill_done'), ['external'])

    async def test_backfill_abandons_failures_after_three_attempts_and_releases_budget(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': str(i), 'startDate': end - 300, 'endDate': end,
             'marketTitle': 'Bitcoin'} for i in range(3)] + [
            {**original, 'marketId': 'good', 'startDate': end - 600, 'endDate': end - 300,
             'marketTitle': 'Ethereum'}]

        async def klines(symbol, interval, limit, startTime, endTime):
            if symbol == 'BTCUSDT':
                raise RuntimeError('permanently unavailable')
            return [[startTime, '100', '120', '90', '110', '1', endTime]]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        for attempt in range(1, 4):
            self.assertFalse((await self.sync.run_once())['errors'])
            self.assertEqual(source.klines.await_count, 3 * attempt)
            self.assertEqual(self.store.meta('price_backfill_attempts'), dict.fromkeys(('0', '1', '2'), attempt))
        self.assertEqual(set(self.store.meta('price_backfill_done')), {'0', '1', '2'})
        # A fresh Sync must honor the persisted give-up state in both price loops.
        self.sync = Sync(self.store, self.sapi, self.client)
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(source.klines.await_count, 10)
        self.assertEqual(self.store.meta('market_prices')['good']['start_price'], 100)
        self.assertEqual(self.store.meta('settlement_prices'), {'good': 110})
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual(source.klines.await_count, 10)

    async def test_backfill_processes_newest_windows_first(self):
        end = int(time.time() // 300) * 300 - 300
        original = self.payloads['/positions']['data']['positions'][0]
        self.payloads['/positions']['data']['positions'] = [
            {**original, 'marketId': str(offset), 'startDate': end - offset - 300,
             'endDate': end - offset, 'marketTitle': 'Bitcoin'} for offset in (900, 0, 600, 300)]

        async def klines(symbol, interval, limit, startTime, endTime):
            return [[startTime, '100', '120', '90', '110', '1', endTime]]

        source = Mock(klines=AsyncMock(side_effect=klines))
        self.sync.client = source
        self.assertFalse((await self.sync.run_once())['errors'])
        self.assertEqual([call.kwargs['endTime'] for call in source.klines.await_args_list],
                         [(end - offset) * 1000 - 1 for offset in (0, 300, 600)])
        self.assertEqual(set(self.store.meta('price_backfill_done')), {'0', '300', '600'})

    async def enable(self):
        state = await self.sync.run_once()
        self.assertFalse(state['errors'], state)
        self.store.update_settings({'mode': 'LIVE', 'engine_enabled': True})

    async def test_history_reconciles_zero_and_partial_fills(self):
        cases = [
            ('FAILED', '0', 10, 'FAILED', '未成交（场馆 FAILED）'),
            ('CANCELLED', 0, 10, 'FAILED', '未成交（场馆 CANCELLED）'),
            ('EXPIRED', '0.000', 10, 'FAILED', '未成交（场馆 EXPIRED）'),
            ('FAILED', '2.5000', 10, 'SUCCESS', '部分成交 2.5/10'),
            ('CANCELLED', '0.000001', 1, 'SUCCESS', '部分成交 0.000001/1'),
            ('PARTIALLY_FILLED', '2.125', 10.5, 'SUCCESS', '部分成交 2.125/10.5'),
        ]
        for status, filled, amount, expected_status, error in cases:
            with self.subTest(status=status, filled=filled):
                self.store.execute('DELETE FROM orders_audit')
                self.store.execute('INSERT INTO orders_audit (order_id, amount, status) VALUES (?, ?, ?)',
                                   ('order1', amount, 'SUCCESS'))
                self.payloads['/orders']['data'] = {'total': 1, 'orders': [
                    {'orderId': 'order1', 'status': status, 'filledUsdtAmount': filled}]}
                # Fill reconciliation must also work on a capped history page.
                self.payloads['/orders']['data']['total'] = 28
                await self.sync.run_once()
                self.assertEqual(self.store.rows('SELECT status,error FROM orders_audit'),
                                 [{'status': expected_status, 'error': error}])

    async def test_history_leaves_filled_submitted_and_unmatched_audits_untouched(self):
        orders = [
            {'orderId': 'filled', 'status': 'FILLED', 'filledUsdtAmount': '10'},
            {'orderId': 'filled-partial', 'status': 'FILLED', 'filledUsdtAmount': '2'},
            {'orderId': 'submitted', 'status': 'SUBMITTED', 'filledUsdtAmount': '0'},
            {'orderId': 'submitted-partial', 'status': 'SUBMITTED', 'filledUsdtAmount': '2'},
            {'orderId': None, 'status': 'FAILED', 'filledUsdtAmount': '0'},
            {'orderId': '', 'status': 'FAILED', 'filledUsdtAmount': '0'},
        ]
        for order_id in ('filled', 'filled-partial', 'submitted', 'submitted-partial', 'missing', None, ''):
            self.store.execute('INSERT INTO orders_audit (order_id, amount, status, error) VALUES (?, ?, ?, ?)',
                               (order_id, 10, 'SUCCESS', 'existing annotation'))
        before = self.store.rows('SELECT * FROM orders_audit')
        self.payloads['/orders']['data'] = {'total': len(orders), 'orders': orders}
        await self.sync.run_once()
        self.assertEqual(self.store.rows('SELECT * FROM orders_audit'), before)

    async def test_failed_history_refresh_does_not_reconcile_cached_orders(self):
        self.store.execute('INSERT INTO orders_audit (order_id, amount, status) VALUES (?, ?, ?)',
                           ('cached', 10, 'SUCCESS'))
        self.store.set_meta('order_history', {'orders': [
            {'orderId': 'cached', 'status': 'FAILED', 'filledUsdtAmount': '0'}]})
        self.payloads['/orders']['data'] = {'total': 1}
        await self.sync.run_once()
        self.assertEqual(self.store.rows('SELECT status,error FROM orders_audit'),
                         [{'status': 'SUCCESS', 'error': None}])

    async def test_sync_mapping_and_wallet_preservation(self):
        self.store.set_meta('wallet', {'walletAddress': 'old-address', 'walletId': 'old-id'})
        await self.enable()
        rows = [{**row, 'pnl': 3.0} for row in self.payloads['/positions']['data']['positions']]
        self.assertEqual(self.store.meta('positions'), {'positions': rows, 'list': rows, 'summary': {'todayRealizedPnl': '4'}})
        self.assertNotIn('walletAddress', self.store.meta('pnl_portfolio'))
        self.assertEqual(self.store.meta('payment_options')['items'], [{'accountType': 'CeDeFi', 'availableBalanceDisplay': '42.25'}])
        self.assertEqual(self.store.meta('live_daily_pnl')['value'], 7)
        self.assertEqual(self.store.meta('wallet')['walletAddress'], 'old-address')
        self.assertFalse(self.store.meta('sync_state')['permission_pending'])
        self.payloads['/balance']['data'][0]['walletAddress'] = 'baw-address'
        await self.sync.run_once()
        self.assertEqual(self.store.meta('wallet'), {'walletAddress': 'baw-address'})
        self.assertEqual(self.store.meta('pnl_portfolio')['walletAddress'], 'baw-address')

    async def test_sync_prunes_only_stale_placed_reservations_absent_from_positions(self):
        self.store.update_settings({'mode': 'LIVE', 'engine_enabled': True, 'max_open_positions': 1})
        risk = Risk(self.store)
        for status, age, present, kept in [
            ('PLACED', 1200, False, False),
            ('PLACED', 1200, True, True),
            ('PLACED', 60, False, True),
            ('PENDING', 1200, False, True),
            ('UNKNOWN', 1200, False, True),
        ]:
            with self.subTest(status=status, age=age, present=present):
                self.store.execute('DELETE FROM reservations')
                self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)',
                                   ('old', time.time()-age, 10, status, 'order-old', 'token'))
                position = {'marketId': 'old', 'shares': 2, 'currentValue': '2', 'totalCost': '2'}
                self.store.set_meta('positions', {'list': [position]})
                self.payloads['/positions']['data']['positions'] = [position] if present else []
                self.assertEqual(risk.check(10, 'new'), (False, '持仓数量已达上限'))
                state = await self.sync.run_once()
                self.assertFalse(state['errors'], state)
                self.assertEqual(bool(self.store.rows('SELECT * FROM reservations')), kept)
                self.assertEqual(risk.check(10, 'new')[0], not kept)

    async def test_failed_position_refresh_does_not_prune_reservations(self):
        self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)',
                           ('old', time.time()-1200, 10, 'PLACED', 'order-old', 'token'))
        self.store.set_meta('positions', {'list': []})
        self.payloads['/positions']['data'] = {'positions': []}
        state = await self.sync.run_once()
        self.assertIn('positions', state['errors'])
        self.assertEqual(len(self.store.rows('SELECT * FROM reservations')), 1)

    async def test_dashboard_positions_and_portfolio_totals(self):
        rows = [
            {'outcomeName': 'Up', 'positionStatus': 'OPEN', 'shares': '6.92',
             'totalCost': '2', 'value': '1.9376', 'unrealizedPnl': '-0.0624', 'realizedPnl': '0'},
            {'outcomeName': 'Down', 'shares': '1.32', 'totalCost': '1',
             'value': '0.9504', 'unrealizedPnl': '-0.0496'},
        ]
        self.payloads['/positions']['data']['positions'] = rows
        self.payloads['/status']['data']['walletAddress'] = 'agentic-address'
        # A stale SAPI snapshot must be replaced using only bridge data.
        self.store.set_meta('pnl_portfolio', {'totalCurrentValue': '0', 'walletAddress': 'sapi-address'})
        started = time.time()
        await self.enable()
        positions = self.store.meta('positions')
        expected = [{**rows[0], 'pnl': -0.0624}, {**rows[1], 'pnl': -0.0496}]
        self.assertEqual(positions['positions'], expected)
        self.assertEqual(positions['list'], expected)
        self.assertNotIn('pnl', rows[0])  # Source response is not mutated.
        portfolio = self.store.meta('pnl_portfolio')
        self.assertEqual(portfolio, {
            'activePositionsCount': 2, 'totalCostBasis': '3.0', 'totalCurrentValue': '2.888',
            'totalUnrealizedPnl': '-0.112', 'totalRealizedPnl': '0.0', 'totalPnl': '-0.112',
            'positions': expected, 'walletAddress': 'agentic-address',
        })
        status = self.store.meta('pnl_portfolio_status')
        self.assertGreaterEqual(status['ts'], started)
        self.assertIsNone(status['error'])
        self.assertFalse(status['permission_pending'])
        self.assertEqual(self.store.meta('sync_state')['endpoints']['pnl_portfolio'], status)
        self.assertEqual([path for path, _ in self.calls], ['/account'])

    async def test_live_cap_ignores_resolved_positions_from_sync(self):
        base = {'shares': '2', 'currentValue': '1', 'totalCost': '1'}
        resolved = [
            {**base, 'positionStatus': status}
            for status in ('RESOLVED', 'SETTLED', 'CLOSED')
        ] + [
            {**base, 'isResolved': True},
            {**base, 'marketId': None, 'positionStatus': 'OPEN', 'isResolved': True},
            {**base, 'marketId': 'old', 'positionStatus': 'SETTLED', 'isResolved': False},
        ]
        opened = {**base, 'marketId': 'open', 'positionStatus': 'OPEN', 'isResolved': False}
        self.payloads['/positions']['data']['positions'] = resolved + [opened]
        await self.enable()
        self.store.update_settings({'max_open_positions': 10})
        self.assertEqual(len(self.store.meta('positions')['list']), 7)
        self.assertEqual(Risk(self.store).check(10, 'new'), (True, '允许'))

    async def test_live_cap_honors_ten_open_positions_and_missing_market_ids(self):
        base = {'currentShares': '2', 'shares': '3', 'currentValue': '1', 'totalCost': '1',
                'positionStatus': 'OPEN', 'isResolved': False}
        # Anonymous open rows each occupy a slot; shares-only rows are also open.
        rows = [{**base, 'marketId': str(i)} for i in range(6)]
        rows += [{**base}, {**base, 'marketId': None},
                 {'shares': '2', 'currentValue': '1', 'totalCost': '1', 'marketId': 'statusless'}]
        self.payloads['/positions']['data']['positions'] = rows
        await self.enable()
        self.store.update_settings({'max_open_positions': 10})
        risk = Risk(self.store)
        self.assertEqual(risk.check(10, 'new'), (True, '允许'))
        rows.append({**base, 'marketId': 'tenth'})
        await self.sync.run_once()
        self.assertEqual(risk.check(10, 'new'), (False, '持仓数量已达上限'))

    async def test_live_cap_requires_positive_finite_current_shares(self):
        base = {'currentValue': '1', 'totalCost': '1', 'positionStatus': 'OPEN'}
        rows = [{**base, 'marketId': str(i), 'shares': '2'} for i in range(9)]
        self.store.update_settings({'max_open_positions': 10})
        ignored = [
            {'shares': '0'}, {'shares': '-1'}, {'shares': 'NaN'}, {'shares': 'Infinity'},
            {'shares': 'invalid'}, {}, {'shares': '2', 'currentShares': '0'},
            {'shares': '2', 'currentShares': '-1'}, {'shares': '2', 'currentShares': 'NaN'},
            {'shares': '2', 'currentShares': None},
        ]
        for fields in ignored:
            with self.subTest(fields=fields):
                self.payloads['/positions']['data']['positions'] = rows + [{**base, **fields}]
                await self.enable()
                self.assertEqual(Risk(self.store).check(10, 'new'), (True, '允许'))

    async def test_portfolio_pnl_fallback_realized_and_open_count(self):
        self.payloads['/positions']['data']['positions'] = [
            {'positionStatus': 'OPEN', 'value': '1.9376', 'totalCost': '2', 'realizedPnl': '0.25'},
            {'positionStatus': 'CLOSED', 'value': '0.9504', 'totalCost': '1',
             'unrealizedPnl': '-0.2', 'realizedPnl': '-0.1'},
        ]
        for pnl in ({'pnl': None, 'pnlList': []}, {'pnl': None}, None):
            with self.subTest(pnl=pnl):
                if pnl is None:
                    self.payloads.pop('/pnl', None)
                else:
                    self.payloads['/pnl']['data'] = pnl
                await self.enable()
                portfolio = self.store.meta('pnl_portfolio')
                self.assertEqual(portfolio['activePositionsCount'], 1)
                self.assertAlmostEqual(portfolio['positions'][0]['pnl'], -0.0624)
                self.assertEqual(portfolio['positions'][1]['pnl'], -0.2)
                self.assertEqual(portfolio['totalUnrealizedPnl'], '-0.2624')
                self.assertEqual(portfolio['totalRealizedPnl'], '0.15')
                self.assertEqual(portfolio['totalPnl'], '-0.1124')

    async def test_empty_portfolio_and_failed_snapshot_preservation(self):
        self.payloads['/positions']['data']['positions'] = []
        await self.enable()
        portfolio = self.store.meta('pnl_portfolio')
        self.assertEqual(portfolio['positions'], [])
        self.assertEqual(portfolio['activePositionsCount'], 0)
        for key in ('totalCostBasis', 'totalCurrentValue', 'totalUnrealizedPnl', 'totalRealizedPnl', 'totalPnl'):
            self.assertEqual(float(portfolio[key]), 0)
        positions = self.store.meta('positions')
        status = self.store.meta('pnl_portfolio_status')
        del self.payloads['/positions']['data']['positions']
        state = await self.sync.run_once()
        self.assertIn('pnl_portfolio', state['errors'])
        self.assertEqual(self.store.meta('positions'), positions)
        self.assertEqual(self.store.meta('pnl_portfolio'), portfolio)
        self.assertEqual(self.store.meta('pnl_portfolio_status')['ts'], status['ts'])

    def pnl_entry(self, **overrides):
        return dict({'tokenId': '800', 'marketTopicId': '2', 'currentShares': 7.77,
                     'avgPrice': .4, 'currentPrice': 1, 'buyAmtUsd': 3.108,
                     'realizedPnl': 0, 'unrealizedPnl': None, 'totalPnl': None,
                     'isResolved': True, 'isWinner': True, 'redeemCount': 0,
                     'redeemShares': 0, 'redeemPayoutUsd': 0}, **overrides)

    def venue_pnl(self, entries):
        self.payloads['/pnl'] = {'success': True, 'data': {'pnl': None, 'pnlList': entries}}

    async def test_live_pnl_list_aggregation(self):
        realized = [-0.9128, 1.8286, 1.4146, 1.8421, -1, 0]
        unrealized = [-0.001, None, None, None, None, 0.1125]
        rows = [self.pnl_entry(tokenId=str(900+i), realizedPnl=r, unrealizedPnl=u,
                               isResolved=False, isWinner=False)
                for i, (r, u) in enumerate(zip(realized, unrealized))]
        self.venue_pnl(rows)
        await self.enable()
        portfolio = self.store.meta('pnl_portfolio')
        # These exact supplied values sum to 3.1725 and .1115, not 3.1726 and .1116.
        self.assertEqual([portfolio[key] for key in ('totalRealizedPnl', 'totalUnrealizedPnl', 'totalPnl')],
                         ['3.1725', '0.1115', '3.284'])
        self.assertEqual(self.store.meta('live_daily_pnl')['value'], 7)
        self.assertNotIn('/redeem', [path for path, _ in self.calls])

    async def test_pnl_list_winner_redemption_dedupe_and_history_cap(self):
        winner = self.pnl_entry()
        rows = [winner, dict(winner), {**winner, 'tokenId': '801', 'isResolved': False},
                {**winner, 'tokenId': '802', 'currentShares': 0, 'redeemCount': 1},
                {**winner, 'tokenId': '803', 'currentShares': -1},
                {**winner, 'tokenId': '804', 'currentShares': 'NaN'},
                {**winner, 'tokenId': '805', 'isWinner': False},
                {**winner, 'tokenId': '806'},
                {**winner, 'tokenId': '807', 'currentShares': 1},
                {**winner, 'tokenId': None},
                {**winner, 'tokenId': '808', 'currentShares': None}]
        rows[0] = {**winner, 'realizedPnl': .09, 'unrealizedPnl': -.02, 'totalPnl': .08}
        rows[1] = {**winner, 'realizedPnl': 1, 'totalPnl': 0}
        self.venue_pnl(rows)
        previous = [str(i) for i in range(200)] + ['806']
        self.store.set_meta('redeemed_token_ids', previous)
        await self.enable()
        self.assertEqual([body for path, body in self.calls if path == '/redeem'], [{'tokenIds': ['800', '807']}])
        self.assertEqual(self.store.meta('redeemed_token_ids'), (previous + ['800', '807'])[-200:])
        last = self.store.meta('last_redeem')
        self.assertEqual((last['count'], last['batchId']), (2, 'batch-1'))
        self.assertGreater(last['ts'], 0)
        portfolio = self.store.meta('pnl_portfolio')
        self.assertEqual([portfolio[key] for key in ('totalRealizedPnl', 'totalUnrealizedPnl', 'totalPnl')],
                         ['1.09', '-0.02', '0.08'])
        self.assertNotIn('pnl', rows[0])
        await self.sync.run_once()
        self.assertEqual(len([p for p, _ in self.calls if p == '/redeem']), 1)
        # A redeemed winner stays ineligible even without the local dedupe cache.
        self.store.set_meta('redeemed_token_ids', [])
        self.venue_pnl([self.pnl_entry(currentShares=0, redeemCount=1, redeemShares=7.77, redeemPayoutUsd=7.77)])
        await self.sync.run_once()
        self.assertEqual(len([p for p, _ in self.calls if p == '/redeem']), 1)

    async def test_failed_redeem_is_logged_and_retryable(self):
        self.venue_pnl([self.pnl_entry(currentShares=1)])
        self.payloads['/redeem'] = {'success': False, 'error': 'redeem rejected'}
        state = await self.sync.run_once()
        self.assertFalse(state['errors'], state)
        self.assertEqual(self.store.meta('redeem_error')['error'], 'redeem rejected')
        self.assertEqual(self.store.meta('redeemed_token_ids', []), [])
        self.assertIsNone(self.store.meta('last_redeem'))
        self.payloads['/redeem'] = {'success': True, 'data': {}}
        await self.sync.run_once()
        self.assertEqual(self.store.meta('redeemed_token_ids'), ['800'])
        self.assertIsNone(self.store.meta('redeem_error'))
        self.assertNotIn('batchId', self.store.meta('last_redeem'))

    async def test_malformed_account_preserves_previous_snapshot(self):
        await self.enable()
        previous = self.store.meta('pnl_portfolio')
        async def invalid_account(): return []
        with patch.object(self.client, 'account', invalid_account):
            state = await self.sync.run_once()
        self.assertIn('baw_account', state['errors'])
        self.assertEqual(self.store.meta('pnl_portfolio'), previous)

    async def test_present_but_invalid_pnl_does_not_fall_back(self):
        await self.enable()
        previous = self.store.meta('pnl_portfolio')
        for entry in (None, self.pnl_entry(realizedPnl=None), self.pnl_entry(unrealizedPnl='NaN'),
                      self.pnl_entry(totalPnl='Infinity')):
            with self.subTest(entry=entry):
                self.venue_pnl([entry])
                state = await self.sync.run_once()
                self.assertIn('pnl_portfolio', state['errors'])
                self.assertEqual(self.store.meta('pnl_portfolio'), previous)

    async def test_executor_quote_place_audit_and_reservation(self):
        await self.enable()
        def inspect_reservation():
            self.assertEqual(self.store.rows('SELECT status FROM reservations')[0]['status'], 'PENDING')
            self.assertEqual(self.store.rows('SELECT status FROM orders_audit')[0]['status'], 'ATTEMPT')
            self.assertEqual(Risk(self.store).spent(), 10)
        self.on_quote = inspect_reservation
        result = await self.executor.execute(self.market, 'UP', .8)
        self.assertTrue(result['ok'], result)
        trade = [(path, body) for path, body in self.calls if path in ('/quote', '/place')]
        self.assertEqual(trade, [('/quote', {'tokenId': '123', 'marketTopicId': '2', 'side': 'BUY', 'amount': '10.0', 'orderType': 'MARKET', 'slippageBps': 1200}),
            ('/place', {'quoteId': 'q1', 'slippageBps': 600})])
        self.assertEqual(self.store.rows('SELECT * FROM reservations')[0]['order_id'], 'o1')
        row = self.store.rows('SELECT * FROM orders_audit')[0]
        self.assertEqual((row['status'], row['quote_id'], row['order_id']), ('SUCCESS', 'q1', 'o1'))
        self.assertFalse((await self.executor.execute(self.market, 'UP', .8))['ok'])

    async def test_executor_setting_overrides_market_slippage(self):
        self.store.update_settings({'slippage_bps': 2500})
        self.market['slippage_bps'] = 1000
        self.payloads['/quote']['data']['slippageBps'] = 2500
        await self.enable()
        result = await self.executor.execute(self.market, 'UP', .8)
        self.assertTrue(result['ok'], result)
        trade = [(path, body['slippageBps']) for path, body in self.calls
                 if path in ('/quote', '/place')]
        self.assertEqual(trade, [('/quote', 2500), ('/place', 2500)])

    async def test_btc_eth_five_minute_orders_use_each_markets_tokens(self):
        await self.enable()
        for symbol, mid, topic, side, tokens in (
                ('BTCUSDT', '101', '201', 'UP', ('1001', '1002')),
                ('ETHUSDT', '102', '202', 'DOWN', ('2001', '2002'))):
            now = time.time()
            market = dict(self.market, symbol=symbol, market_id=mid, topic_id=topic,
                          title=f'{symbol} Up or Down', start_time=now - 60, end_time=now + 240,
                          outcomes=[{'name': 'UP', 'price': '.4', 'tokenId': tokens[0]},
                                    {'name': 'DOWN', 'price': '.4', 'tokenId': tokens[1]}])
            result = await self.executor.execute(market, side, .9)
            self.assertTrue(result['ok'], result)
        quotes = [body for path, body in self.calls if path == '/quote']
        self.assertEqual([(q['marketTopicId'], q['tokenId']) for q in quotes],
                         [('201', '1001'), ('202', '2002')])
        self.assertEqual(len([p for p, _ in self.calls if p == '/place']), 2)
        self.assertEqual(Risk(self.store).spent(), 20)
        rows = self.store.rows('SELECT market_id,token_id,side,status FROM orders_audit ORDER BY id')
        self.assertEqual([(r['market_id'], r['token_id'], r['side'], r['status']) for r in rows],
                         [('101', '1001', 'UP', 'SUCCESS'), ('102', '2002', 'DOWN', 'SUCCESS')])

    async def test_unknown_place_is_not_retried_and_keeps_budget(self):
        await self.enable()
        self.place_status = 502
        self.payloads['/place'] = {'error': 'raw baw timeout: outcome unknown'}
        result = await self.executor.execute(self.market, 'UP', .8)
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertEqual(result['reason'], 'raw baw timeout: outcome unknown')
        self.assertEqual(len([p for p, _ in self.calls if p == '/place']), 1)
        self.assertEqual(Risk(self.store).spent(), 10)
        self.assertEqual(self.store.rows('SELECT status FROM reservations')[0]['status'], 'UNKNOWN')
        self.assertEqual(self.store.rows('SELECT status FROM orders_audit')[0]['status'], 'UNKNOWN')

    async def test_stop_during_quote_refunds_without_placement(self):
        await self.enable()
        self.on_quote = lambda: (self.store.data_dir / 'STOP').touch()
        result = await self.executor.execute(self.market, 'UP', .8)
        self.assertEqual(result['status'], 'FAILED')
        self.assertEqual(Risk(self.store).spent(), 0)
        self.assertEqual(self.store.rows('SELECT * FROM reservations'), [])
        self.assertNotIn('/place', [p for p, _ in self.calls])

    async def test_expired_quote_and_excess_slippage_fail_before_place(self):
        await self.enable()
        self.payloads['/quote']['data']['expireAt'] = '2020-01-01T00:00:00Z'
        self.assertEqual((await self.executor.execute(self.market, 'UP'))['status'], 'FAILED')
        self.payloads['/quote']['data']['slippageBps'] = 1201
        self.assertEqual((await self.executor.execute(self.market, 'UP'))['status'], 'FAILED')
        self.assertNotIn('/place', [p for p, _ in self.calls])
        self.assertEqual(Risk(self.store).spent(), 0)

    async def test_missing_cost_fails_closed_even_with_recent_good_pnl(self):
        await self.enable()
        del self.payloads['/positions']['data']['positions'][0]['totalCost']
        state = await self.sync.run_once()
        self.assertIn('live_daily_pnl', state['errors'])
        self.assertFalse(Risk(self.store).check(10, 'new')[0])
        self.assertEqual(self.store.meta('live_daily_pnl')['value'], 7)  # Last good data retained.

    async def test_capped_history_is_accepted_and_clears_previous_sync_error(self):
        self.store.set_meta('order_history_status', {'ts': 1, 'error': 'baw order history is truncated'})
        self.store.set_meta('sync_state', {'last_success': 1, 'errors': {'order_history': 'baw order history is truncated'}})
        orders = [{'orderId': str(i), 'status': 'FILLED'} for i in range(50, 0, -1)]
        self.payloads['/orders']['data'] = {'total': 57, 'orders': orders}
        state = await self.sync.run_once()
        self.assertFalse(state['errors'], state)
        self.assertGreater(state['last_success'], 1)
        self.assertIsNone(state['endpoints']['order_history']['error'])
        self.assertEqual(self.store.meta('order_history'), {'total': 57, 'orders': orders, 'truncated': True})
        self.store.update_settings({'mode': 'LIVE', 'engine_enabled': True})
        self.assertTrue(Risk(self.store).check(10, 'new')[0])

    async def test_invalid_history_total_still_fails(self):
        await self.enable()
        for total in (None, 'invalid', -1, 0):
            with self.subTest(total=total):
                self.payloads['/orders']['data'] = {'total': total, 'orders': [{'orderId': 'o1'}]}
                state = await self.sync.run_once()
                self.assertEqual(state['errors']['order_history'], 'baw orders total is invalid')
        self.payloads['/orders']['data'] = {'orders': []}
        state = await self.sync.run_once()
        self.assertIn('order_history', state['errors'])

    async def test_malformed_history_disconnected_or_missing_balance_blocks(self):
        await self.enable()
        last = self.store.meta('sync_state')['last_success']
        self.payloads['/orders']['data'] = {'total': 57, 'orders': {}}
        state = await self.sync.run_once()
        self.assertIn('order_history', state['errors'])
        self.assertEqual(state['errors']['order_history'], 'baw orders response is incomplete')
        self.assertEqual(state['last_success'], last)
        self.assertEqual(self.store.meta('order_history'), {'total': 0, 'orders': []})
        self.payloads['/status']['data']['status'] = 'DISCONNECTED'
        self.payloads['/balance']['data'] = []
        state = await self.sync.run_once()
        self.assertIn('baw_status', state['errors'])
        self.assertIn('payment_options', state['errors'])
        self.assertFalse(state['permission_pending'])
        self.assertFalse(Risk(self.store).check(10, 'new')[0])

    async def test_client_fails_closed_without_token(self):
        self.client.token = ''
        with self.assertRaisesRegex(BawError, 'not configured'):
            await self.client.status()
        self.assertEqual(self.calls, [])


class ExternalClientConfigTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_and_sync_switch_with_persisted_settings(self):
        from app.baw_client import executor_path
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'EXECUTOR_PATH':'baw'}, clear=True):
            store = Store(Path(tmp)/'settings.db')
            sapi, baw = SimpleNamespace(), SimpleNamespace()
            try:
                executor, sync = Executor(store, sapi, baw), Sync(store, sapi, baw)
                self.assertEqual(executor.path, 'baw')
                self.assertIs(sync.account_client, baw)
                # Executor selection never changes explicit service validation.
                for mode in ('baw', 'sapi'):
                    for block, missing in (('binance_api', 'api_key.*api_secret'),
                                           ('baw_bridge', 'bridge_token')):
                        with self.subTest(mode=mode, block=block):
                            with self.assertRaisesRegex(ValueError, missing):
                                store.update_settings({'executor_mode': mode, block: {'enabled': True}})
                store.update_settings({'executor_mode':'baw', 'binance_api':{'enabled':None}})
                self.assertIs(executor.execution_client, baw)
                with patch.dict(os.environ, {'BINANCE_API_KEY':'offline-key','BINANCE_API_SECRET':'offline-secret'}):
                    store.update_settings({'executor_mode':'sapi'})
                    self.assertEqual(executor_path(store.settings()), 'sapi')
                    self.assertIs(executor.execution_client, sapi)
                    self.assertIs(sync.account_client, sapi)
                store.update_settings({'executor_mode':'baw'})
                self.assertIs(executor.execution_client, baw)
                self.assertIs(sync.account_client, baw)
            finally:
                store.close()

    async def test_bridge_settings_reload_environment_fallback_and_disable(self):
        settings = {}
        store = SimpleNamespace(settings=lambda: settings)
        calls = []
        def handler(request):
            calls.append((str(request.url), request.headers['X-Bridge-Token']))
            return httpx.Response(200, json={'success': True, 'data': {}})
        with patch.dict(os.environ, {'BAW_BRIDGE_URL': 'http://env-bridge.example:8791',
                                    'BAW_BRIDGE_TOKEN': 'test-env-token', 'BAW_BRIDGE_ENABLED': 'true'}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                client = BawClient(http=http, store=store)
                await client.status()
                self.assertEqual(calls[-1], ('http://env-bridge.example:8791/status', 'test-env-token'))
                settings['baw_bridge'] = {'bridge_url': 'https://custom-bridge.example',
                                          'bridge_token': 'test-custom-token', 'enabled': True}
                await client.status()
                self.assertEqual(calls[-1], ('http://env-bridge.example:8791/status', 'test-env-token'))
                settings['baw_bridge']['bridge_token'] = ''
                await client.status()
                self.assertEqual(calls[-1][1], 'test-env-token')
                settings['baw_bridge']['enabled'] = False
                with self.assertRaisesRegex(BawError, 'disabled'):
                    await client.place_order_bundle(quoteId='test-quote')
                self.assertEqual(len(calls), 3)
                settings.clear()
                with patch.dict(os.environ, {'BAW_BRIDGE_ENABLED': 'false'}):
                    with self.assertRaisesRegex(BawError, 'disabled'):
                        await client.status()
                self.assertEqual(len(calls), 3)

    async def test_sapi_settings_reload_signed_requests_and_disable(self):
        settings = {}
        store = SimpleNamespace(settings=lambda: settings)
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={'code': 0, 'data': {}})
        def verify(request, key, secret):
            self.assertEqual(request.headers['X-MBX-APIKEY'], key)
            payload, signature = request.url.query.decode().rsplit('&signature=', 1)
            self.assertEqual(signature, hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest())
        with patch.dict(os.environ, {'BINANCE_API_KEY': 'test-env-key', 'BINANCE_API_SECRET': 'test-env-secret',
                                    'BINANCE_API_ENABLED': 'true'}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                client = BinanceClient(http=http, store=store)
                client._last_time_sync = time.monotonic()
                await client.wallet_list()
                verify(calls[-1], 'test-env-key', 'test-env-secret')
                settings['binance_api'] = {'api_key': 'test-custom-key', 'api_secret': 'test-custom-secret', 'enabled': True}
                await client.wallet_list()
                verify(calls[-1], 'test-custom-key', 'test-custom-secret')
                settings['binance_api']['api_secret'] = ''
                await client.wallet_list()
                verify(calls[-1], 'test-custom-key', 'test-env-secret')
                settings['binance_api']['enabled'] = False
                with self.assertRaisesRegex(BinanceError, 'disabled'):
                    await client.place_order_bundle(quoteId='test-quote')
                self.assertEqual(len(calls), 3)
                # Public price data is still available while signed SAPI is disabled.
                await client.klines('BTCUSDT')
                self.assertNotIn('X-MBX-APIKEY', calls[-1].headers)
                settings.clear()
                with patch.dict(os.environ, {'BINANCE_API_ENABLED': 'false'}):
                    with self.assertRaisesRegex(BinanceError, 'disabled'):
                        await client.wallet_list()
                self.assertEqual(len(calls), 4)


class BridgeTests(unittest.TestCase):
    def test_wallet_login_session_output_completion_expiration_and_auth(self):
        from io import StringIO
        bridge.LOGIN_SESSIONS.clear()
        release = threading.Event()
        process = Mock()
        process.stdout = StringIO('请打开 https://wallet.invalid/login?code=offline\n')
        process.wait.side_effect = lambda timeout=None: (release.wait(2) and 0)
        process.poll.return_value = 0
        self.cli.return_value = subprocess.CompletedProcess([], 0, b'{"success":true,"data":{"walletAddress":"0xabc"}}', b'')
        try:
            with patch('bridge.baw_bridge.subprocess.Popen', return_value=process) as spawn:
                code, raw = self.request('/wallet/login', {}, token='wrong')
                self.assertEqual(code, 401)
                spawn.assert_not_called()
                code, raw = self.request('/wallet/login', {})
                self.assertEqual(code, 200)
                data = json.loads(raw)['data']
                identity = data['session_id']
                spawn.assert_called_once()
                self.assertEqual(spawn.call_args.args[0], ['offline-baw','auth','signin'])
                # Repeated clicks reuse the running process.
                self.assertEqual(json.loads(self.request('/wallet/login', {})[1])['data']['session_id'], identity)
                release.set()
                for _ in range(100):
                    status = bridge.login_status(identity)
                    if status['status'] != 'pending': break
                    time.sleep(.01)
                self.assertEqual(status['status'], 'done')
                self.assertEqual(status['address'], '0xabc')
                self.assertIn('请打开', status['text'])
                self.assertEqual(status['url'], 'https://wallet.invalid/login?code=offline')
                self.assertEqual(json.loads(self.request('/wallet/login/status?session_id='+identity)[1])['data']['status'], 'done')
                with patch('bridge.baw_bridge.time.monotonic', return_value=time.monotonic()+301):
                    self.assertEqual(bridge.login_status(identity)['status'], 'failed')
                self.assertEqual(bridge.login_status('missing')['status'], 'failed')
        finally:
            release.set()
            bridge.LOGIN_SESSIONS.clear()

    def test_wallet_login_already_logged_in_and_status_confirmation(self):
        from io import StringIO
        cases = [
            ('Already logged in\n', 1, {'status': 'CONNECTED', 'walletAddress': '0xexisting'}, 'done', '0xexisting'),
            ('signin exited\n', 1, {'status': 'CONNECTED', 'address': '0xconfirmed'}, 'done', '0xconfirmed'),
            ('Already logged in\n', 1, {'status': 'CONNECTED'}, 'done', None),
            ('Already logged in\n', 1, {'status': 'DISCONNECTED'}, 'done', None),
            ('signin rejected\n', 1, {'status': 'DISCONNECTED'}, 'failed', None),
            ('signin exited\n', 0, {'status': 'DISCONNECTED'}, 'failed', None),
        ]
        for output, exit_code, wallet, expected, address in cases:
            with self.subTest(output=output, wallet=wallet, exit_code=exit_code):
                bridge.LOGIN_SESSIONS.clear()
                process = Mock()
                process.stdout = StringIO(output)
                process.wait.return_value = exit_code
                process.poll.return_value = exit_code
                self.cli.return_value = subprocess.CompletedProcess(
                    [], 0, json.dumps({'success': True, 'data': wallet}).encode(), b'')
                with patch('bridge.baw_bridge.subprocess.Popen', return_value=process):
                    code, raw = self.request('/wallet/login', {})
                    self.assertEqual(code, 200)
                    identity = json.loads(raw)['data']['session_id']
                    for _ in range(100):
                        code, raw = self.request('/wallet/login/status?session_id='+identity)
                        status = json.loads(raw)['data']
                        if status['status'] != 'pending': break
                        time.sleep(.01)
                    self.assertEqual(code, 200)
                    self.assertEqual(status['status'], expected)
                    self.assertEqual(status['address'], address)
                bridge.LOGIN_SESSIONS.clear()

    def test_status_adds_available_address_and_preserves_connected_without_address(self):
        for wallet in ({'status': 'CONNECTED', 'walletAddress': '0xexisting'},
                       {'loggedIn': True, 'address': '0xexisting'},
                       {'status': 'CONNECTED'}):
            with self.subTest(wallet=wallet):
                self.cli.return_value = subprocess.CompletedProcess(
                    [], 0, json.dumps({'success': True, 'data': wallet}).encode(), b'')
                code, raw = self.request('/status')
                self.assertEqual(code, 200)
                data = json.loads(raw)['data']
                if wallet.get('walletAddress') or wallet.get('address'):
                    self.assertEqual(data['address'], '0xexisting')
                else:
                    self.assertNotIn('address', data)
                self.assertEqual(self.cli.call_args.args[0], ['offline-baw', 'wallet', 'status', '--json'])

    def test_wallet_login_timeout_kills_process_and_fails(self):
        from io import StringIO
        bridge.LOGIN_SESSIONS.clear()
        process = Mock()
        process.stdout = StringIO('login link https://wallet.invalid/\n')
        process.wait.side_effect = [subprocess.TimeoutExpired('baw',300), 0]
        process.poll.return_value = None
        with patch('bridge.baw_bridge.subprocess.Popen', return_value=process):
            data = bridge.start_login()
            for _ in range(100):
                status = bridge.login_status(data['session_id'])
                if status['status'] != 'pending': break
                time.sleep(.01)
            self.assertEqual(status['status'], 'failed')
            self.assertIn('5分钟', status['error'])
            process.kill.assert_called_once()
            self.assertEqual(process.wait.call_count, 2)
        bridge.LOGIN_SESSIONS.clear()

    def test_order_history_requests_larger_page(self):
        self.assertEqual(bridge.command('/orders'),
                         ['prediction', 'order', 'history', '--limit', '100', '--json'])

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        bridge.QUOTES.clear()
        self.ledger = Path(self.tmp.name) / 'ledger.json'
        self.env = patch.dict(os.environ, {'BAW_BRIDGE_TOKEN': TOKEN, 'BAW_BIN': 'offline-baw', 'BAW_BRIDGE_LEDGER': str(self.ledger)})
        self.env.start()
        self.run = patch('bridge.baw_bridge.subprocess.run')
        self.cli = self.run.start()
        self.raw = b'{"success":true,"data":{"quoteId":"q1"}}\n'
        self.cli.return_value = subprocess.CompletedProcess([], 0, self.raw, b'')

    def tearDown(self):
        self.run.stop()
        self.env.stop()

    def request(self, path, body=None, token=TOKEN):
        payload = json.dumps(body).encode() if body is not None else b''
        method = 'POST' if body is not None else 'GET'
        raw = (f'{method} {path} HTTP/1.0\r\nX-Bridge-Token: {token}\r\nContent-Length: {len(payload)}\r\n\r\n').encode() + payload
        conn = Connection(raw)
        Handler(conn, ('127.0.0.1', 1), None)
        headers, data = bytes(conn.output).split(b'\r\n\r\n', 1)
        return int(headers.split()[1]), data

    def quote(self, **kwargs):
        return dict(tokenId='123', marketTopicId='456', amount='1', side='BUY', orderType='MARKET', **kwargs)

    def test_bind_defaults_to_loopback_and_can_be_configured(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(bridge.listen_address(), ('127.0.0.1', 8791))
            with patch.dict(os.environ, {'BAW_BRIDGE_BIND': '192.0.2.2', 'BAW_BRIDGE_PORT': '9000'}):
                self.assertEqual(bridge.listen_address(), ('192.0.2.2', 9000))

    def test_buy_caps_quote_binding_expiry_and_persistence(self):
        body = self.quote(); body['amount'] = '100.0000001'
        self.assertEqual(self.request('/quote', body)[0], 400)
        self.cli.assert_not_called()
        place = {'quoteId': 'q1', 'slippageBps': 600}
        self.assertEqual(self.request('/place', place)[0], 502)
        body['amount'] = '100'
        for _ in range(50):
            self.assertEqual(self.request('/quote', body)[0], 200)
            self.assertEqual(self.request('/place', place)[0], 200)
            self.assertEqual(self.request('/place', place)[0], 502)
        self.assertEqual(list(json.loads(self.ledger.read_text()).values()), ['5000'])
        bridge.QUOTES.clear()  # Restart loses quotes, but must retain the daily total.
        self.request('/quote', body)
        calls = self.cli.call_count
        self.assertEqual(self.request('/place', place)[0], 502)
        self.assertEqual(self.cli.call_count, calls)
        body['side'] = 'SELL'; body['amount'] = '1000'
        self.assertEqual(self.request('/quote', body)[0], 200)
        self.assertEqual(self.request('/place', place)[0], 200)
        self.assertEqual(list(json.loads(self.ledger.read_text()).values()), ['5000'])
        self.request('/quote', body)
        with patch('bridge.baw_bridge.time.monotonic', return_value=time.monotonic() + 601):
            calls = self.cli.call_count
            self.assertEqual(self.request('/place', place)[0], 502)
            self.assertEqual(self.cli.call_count, calls)
        # A prior UTC day does not consume today's allowance.
        self.ledger.write_text('{"2000-01-01":"5000"}')
        body['side'] = 'BUY'; body['amount'] = '100'
        self.request('/quote', body)
        self.assertEqual(self.request('/place', place)[0], 200)
        self.assertEqual(sorted(json.loads(self.ledger.read_text()).values()), ['100', '5000'])

    def test_failed_placement_does_not_commit_and_bad_ledger_fails_closed(self):
        place = {'quoteId': 'q1', 'slippageBps': 600}
        self.request('/quote', self.quote())
        self.cli.return_value = subprocess.CompletedProcess([], 1, b'{"success":false}', b'')
        self.assertEqual(self.request('/place', place)[0], 502)
        self.assertFalse(self.ledger.exists())
        self.cli.return_value = subprocess.CompletedProcess([], 0, self.raw, b'')
        self.request('/quote', self.quote())
        self.ledger.write_text('broken')
        calls = self.cli.call_count
        self.assertEqual(self.request('/place', place)[0], 502)
        self.assertEqual(self.cli.call_count, calls)

    def test_concurrent_placements_cannot_exceed_daily_cap(self):
        day = datetime.now(timezone.utc).date().isoformat()
        self.ledger.write_text(json.dumps({day: '4600'}))
        for i in range(8):
            bridge.QUOTES[str(i)] = ('BUY', bridge.Decimal('100'), time.monotonic() + 600)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: self.request('/place', {'quoteId': str(i), 'slippageBps': 600})[0], range(8)))
        self.assertEqual(results.count(200), 4)
        self.assertEqual(self.cli.call_count, 4)
        self.assertEqual(list(json.loads(self.ledger.read_text()).values()), ['5000'])

    def test_subprocess_concurrency_is_four(self):
        active = peak = 0
        lock = threading.Lock()
        full = threading.Event()
        def run(*args, **kwargs):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 4:
                    full.set()
            self.assertTrue(full.wait(2))
            time.sleep(.02)
            with lock:
                active -= 1
            return subprocess.CompletedProcess([], 0, self.raw, b'')
        self.cli.side_effect = run
        with ThreadPoolExecutor(max_workers=12) as pool:
            self.assertEqual(list(pool.map(lambda _: self.request('/status')[0], range(12))), [200] * 12)
        self.assertEqual(peak, 4)

    def test_request_threads_are_bounded_before_authentication(self):
        server = object.__new__(bridge.BridgeServer)
        server.request_slots = threading.BoundedSemaphore(16)
        server.shutdown_request = Mock()
        with patch.object(bridge.ThreadingHTTPServer, 'process_request') as spawn:
            for _ in range(17):
                server.process_request('socket', ('127.0.0.1', 1))
            self.assertEqual(spawn.call_count, 16)
            server.shutdown_request.assert_called_once()
        self.assertEqual(server.request_queue_size, 16)
        with patch.object(bridge.ThreadingHTTPServer, 'process_request_thread', side_effect=ValueError):
            with self.assertRaises(ValueError):
                server.process_request_thread('socket', ('127.0.0.1', 1))
        self.assertTrue(server.request_slots.acquire(blocking=False))

    def test_validation_and_auth_reject_without_cli(self):
        for key, value in [('tokenId', '1;rm'), ('tokenId', 123), ('amount', '-1'), ('amount', 'NaN'), ('amount', '1e3'),
                           ('amount', 1), ('amount', '0'), ('side', 'buy'), ('orderType', '--help'), ('slippageBps', True)]:
            body = self.quote(); body[key] = value
            self.assertEqual(self.request('/quote', body)[0], 400, body)
        self.assertEqual(self.request('/place', {'quoteId': '-bad', 'slippageBps': 0})[0], 400)
        self.assertEqual(self.request('/status', token='wrong')[0], 401)
        with patch.dict(os.environ, {'BAW_BRIDGE_TOKEN': ''}):
            self.assertEqual(self.request('/status')[0], 401)
        self.cli.assert_not_called()

    def test_verbatim_json_and_argv(self):
        self.assertEqual(self.request('/quote', self.quote()), (200, self.raw))
        self.cli.assert_called_once_with(['offline-baw', 'prediction', 'trade', 'quote', '--binanceChainId', '56',
            '--tokenId', '123', '--marketTopicId', '456', '--side', 'BUY', '--amount', '1', '--orderType', 'MARKET', '--json'],
            capture_output=True, timeout=30, check=False)
        self.request('/place', {'quoteId': 'q1', 'slippageBps': 600})
        self.assertEqual(self.cli.call_args.args[0][-4:], ['q1', '--slippageBps', '600', '--json'])

    def test_redeem_validation_auth_and_response(self):
        for body in ({}, {'tokenIds': []}, {'tokenIds': '123'}, {'tokenIds': [123]},
                     {'tokenIds': ['1;bad']}, {'tokenIds': ['123'], 'extra': True}):
            self.assertEqual(self.request('/redeem', body)[0], 400, body)
        self.assertEqual(self.request('/redeem', {'tokenIds': ['123']}, token='wrong')[0], 401)
        self.cli.assert_not_called()
        self.raw = b'{"success":true,"data":{"batchId":"batch-1"}}'
        self.cli.return_value = subprocess.CompletedProcess([], 0, self.raw, b'')
        status, raw = self.request('/redeem', {'tokenIds': ['123', '456']})
        self.assertEqual((status, json.loads(raw)), (200, {'success': True, 'data': {'batchId': 'batch-1'}}))
        self.cli.assert_called_once_with(['offline-baw', 'prediction', 'trade', 'redeem',
            '--binanceChainId', '56', '--tokenIds', '123,456', '--json'],
            capture_output=True, timeout=30, check=False)
        self.cli.return_value = subprocess.CompletedProcess([], 0, b'{"success":false,"error":"rejected"}', b'')
        self.assertEqual(self.request('/redeem', {'tokenIds': ['123']})[0], 502)

    def test_account_preserves_snapshots_and_adds_pnl(self):
        values = [{'status': 'CONNECTED'}, [], {'positions': []}, {'orders': []},
                  {'pnl': None, 'pnlList': []}]
        self.cli.side_effect = [subprocess.CompletedProcess([], 0, json.dumps({'success': True, 'data': data}).encode(), b'')
                                for data in values]
        status, raw = self.request('/account')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw), {'success': True, 'data': dict(zip(
            ('status', 'balance', 'positions', 'orders', 'pnl'), values))})
        self.assertEqual(self.cli.call_args.args[0], ['offline-baw', 'prediction', 'position', 'pnl', '--json'])

    def test_cli_failure_and_timeout_return_502(self):
        self.cli.return_value = subprocess.CompletedProcess([], 1, b'{"success":false,"error":"raw failure"}', b'')
        status, raw = self.request('/status')
        self.assertEqual((status, json.loads(raw)), (502, {'error': 'raw failure'}))
        self.cli.return_value = subprocess.CompletedProcess([], 1, b'', b'raw CLI error')
        status, raw = self.request('/status')
        self.assertEqual((status, json.loads(raw)), (502, {'error': 'raw CLI error'}))
        self.request('/quote', self.quote())
        self.cli.side_effect = subprocess.TimeoutExpired('offline-baw', 30)
        self.assertEqual(self.request('/place', {'quoteId': 'q1', 'slippageBps': 600})[0], 502)


if __name__ == '__main__':
    unittest.main(verbosity=2)
