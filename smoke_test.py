"""Offline integration smoke suite: .venv/bin/python smoke_test.py.

The local Binance server is a FastAPI ASGI application, reached in memory rather
than through sockets. Socket/DNS guards make accidental real network use fail.
"""
import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path
import socket
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.binance_client import BinanceClient, BinanceError, money_down, usdt_to_wei, wei_to_usdt
from app.db import Store
from app.risk import Executor, Risk, mark_paper, settle_paper
from app.sync import Sync
from engine.markets import normalize_markets, select_markets
from engine.signals import Ensemble, aggregate, strict_vote

KEY, SECRET = 'offline-key', 'offline-secret'
PREFIX = '/sapi/v1/w3w/wallet/prediction/'


def detail_fixture():
    now = time.time()
    return {'marketTopicId': 'topic1', 'title': 'Bitcoin Up or Down',
            'symbol': 'BTCUSDT', 'chartType': 'CRYPTO_UP_DOWN', 'vendor': 'PREDICT_FUN',
            'variants': [{'marketId': 'variant1', 'status': 'OPEN',
                'startTime': now - 600, 'endTime': now + 3000, 'slippageBps': 900,
                'outcomes': [{'name': 'UP', 'price': '0.40', 'tokenId': 'up1'},
                             {'name': 'DOWN', 'price': '0.60', 'tokenId': 'down1'}]}]}


def candles():
    now = int(time.time() * 1000) - 120 * 60000
    return [[now + i * 60000, '100', '102', '99', str(100 + i * .05),
             str(1000 + i), now + (i + 1) * 60000 - 1] for i in range(120)]


def five_minute_fixture(symbol):
    detail = detail_fixture()
    asset, name = {'BTCUSDT': ('btc', 'Bitcoin'), 'ETHUSDT': ('eth', 'Ethereum')}[symbol]
    detail.update(marketTopicId=f'{asset}-topic', title=f'{name} Up or Down', symbol=symbol)
    variant = detail['variants'][0]
    now = time.time()
    variant.update(marketId=f'{asset}-5m', startTime=now - 60, endTime=now + 240)
    for outcome in variant['outcomes']:
        outcome['tokenId'] = f"{asset}-{outcome['name'].lower()}"
    return detail


class StubBinance:
    """Exact-wire signature verifier and controllable account/market responses."""
    def __init__(self):
        self.app = FastAPI()
        self.pending = False
        self.fail_place = False
        self.quote_price = .4
        self.throttle = 0
        self.calls = []
        self.detail = detail_fixture()
        self.details = {}
        self.kline_symbols = []
        self.portfolio = {'totalUnrealizedPnl': '3.00', 'totalRealizedPnl': '4.00'}
        self.active_orders = []
        self.order_history = []
        self.positions = []

        @self.app.api_route('/{path:path}', methods=['GET', 'POST'])
        async def respond(request: Request, path: str):
            route = request.url.path
            if route == '/api/v3/time':
                return {'serverTime': int(time.time() * 1000) + 750}
            if route == '/api/v3/klines':
                symbol = request.query_params['symbol']
                self.kline_symbols.append(symbol)
                rows = candles()
                if symbol == 'ETHUSDT':
                    for i, row in enumerate(rows):
                        row[4] = str(2000 - i)
                return rows
            raw = request.scope['query_string'].decode() if request.method == 'GET' else (await request.body()).decode()
            self.calls.append((route, raw))
            if request.headers.get('x-mbx-apikey') != KEY:
                return JSONResponse({'code': -2015, 'msg': 'Invalid API-key'}, status_code=401)
            try:
                payload, signature = raw.rsplit('&signature=', 1)
                assert hmac.compare_digest(signature, hmac.new(SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest())
                fields = parse_qs(payload)
                assert fields['recvWindow'] == ['5000']
                assert abs(int(fields['timestamp'][0]) - (time.time() * 1000 + 750)) < 5000
                if route.endswith('batch-cancel'):
                    assert 'cancelInfoList[0].orderId=' in payload
                    assert '%5B' not in payload and '%5D' not in payload
            except (AssertionError, KeyError, ValueError):
                return JSONResponse({'code': -1022, 'msg': 'Signature for this request is not valid'}, status_code=400)
            if self.pending:
                return JSONResponse({'code': -1002, 'msg': 'You are not authorized'}, status_code=401)
            if self.throttle:
                self.throttle -= 1
                return JSONResponse({'code': -1003, 'msg': 'Too many requests'}, status_code=429, headers={'Retry-After': '0'})
            tail = route.removeprefix(PREFIX)
            if tail == 'category/list': result = {'categories': [{'name': 'crypto'}]}
            elif tail == 'market/list':
                result = {'marketTopics': [
                    {'marketTopicId': d['marketTopicId'], 'title': d['title']}
                    for d in self.details.values()] if self.details else
                    [{'marketTopicId': 'topic1', 'symbol': 'BTCUSDT'}], 'hasMore': False}
            elif tail == 'market/detail': result = self.details[fields['marketTopicId'][0]] if self.details else self.detail
            elif tail == 'order-book': result = {'bids': [['0.39', '100']], 'asks': [['0.40', '100']]}
            elif tail == 'order-book/last-trade-price': result = {'price': '0.40'}
            elif tail == 'trade/get-quote':
                assert fields['amountIn'] == [usdt_to_wei(10)]
                assert fields['chainId'] == ['56'] and fields['feeRateBps'] == ['200']
                result = {'quoteId': 'quote1', 'amountOut': usdt_to_wei(25),
                    'feeAmount': usdt_to_wei('.2'), 'averagePrice': self.quote_price,
                    'minReceive': usdt_to_wei(22), 'expireAt': int(time.time() * 1000) + 30000}
            elif tail == 'trade/place-order-bundle':
                assert fields['quoteId'] == ['quote1'] and fields['timeInForce'] == ['FOK']
                assert fields['accountType'] == ['SPOT'] and fields['orderType'] == ['MARKET']
                if self.fail_place:
                    return JSONResponse({'code': -2010, 'msg': 'Account has insufficient balance'}, status_code=400)
                result = {'orderId': 'order1'}
            elif tail == 'trade/batch-cancel': result = {'canceled': fields['cancelInfoList[0].orderId'], 'failed': []}
            elif tail == 'wallet/list': result = {'wallets': [{'walletAddress': '0xabc', 'walletId': 'wallet1'}]}
            elif tail == 'position/list': result = {'positions': self.positions, 'total': len(self.positions), 'summary': {'todayRealizedPnl': '4.00'}}
            elif tail == 'position/settled-history': result = {'settledPositions': [], 'total': 0}
            elif tail == 'order/list': result = {'orders': self.active_orders, 'total': len(self.active_orders)}
            elif tail == 'order/history': result = {'orders': self.order_history, 'total': len(self.order_history)}
            elif tail == 'pnl/portfolio': result = self.portfolio
            elif tail == 'pnl/query': result = {'points': []}
            elif tail == 'balance/payment-options': result = {'availableBalance': '1000.00'}
            else: return JSONResponse({'code': -1102, 'msg': 'Unexpected offline route'}, status_code=404)
            return {'code': '000000', 'data': result}


class Smoke(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='prediction-smoke-')
        self.store = Store(Path(self.directory.name) / 'test.sqlite3')
        self.stub = StubBinance()
        self.http = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.stub.app))
        self.client = BinanceClient(KEY, SECRET, 'http://binance-stub', http=self.http)

    async def asyncTearDown(self):
        await self.http.aclose()
        self.store.close()
        self.directory.cleanup()

    def enable(self, **values):
        return self.store.update_settings({'engine_enabled': True, **values})

    def market(self):
        return normalize_markets(self.stub.detail)[0]

    def test_invert_signal_settings_validation_and_roundtrip(self):
        self.assertIs(self.store.settings()['invert_signal'], False)
        for value in (True, False):
            self.assertIs(self.store.update_settings({'invert_signal': value})['invert_signal'], value)
            reopened = Store(self.store.path)
            try:
                self.assertIs(reopened.settings()['invert_signal'], value)
            finally:
                reopened.close()
        for value in (0, 1, 'true', 'false', None, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.update_settings({'invert_signal': value})

    async def test_invert_signal_execution_and_original_gates(self):
        from engine.runner import Engine
        from unittest.mock import AsyncMock
        market = self.market()
        engine = Engine(self.store, self.client)
        for p_up, price, count, expected in (
                (.8, .1, 9, 'TRADE'), (.2, .1, 9, 'TRADE'),
                (.8, .3, 9, '反买优势不足'), (.2, .3, 9, '反买优势不足'),
                (.6, .1, 9, '模型概率低于阈值'), (.4, .1, 9, '模型概率低于阈值'),
                (.8, .1, 8, '共识不足'), (.2, .1, 8, '共识不足')):
            with self.subTest(p_up=p_up, price=price, count=count):
                settings = self.enable(invert_signal=True)
                side = 'DOWN' if p_up >= .5 else 'UP'
                for outcome in self.stub.detail['variants'][0]['outcomes']:
                    outcome['price'] = str(price if outcome['name'] == side else .95)
                voters = [{'name': f'v{i}', 'status': 'ok', 'p_up': p_up} for i in range(count)]
                eid = self.store.execute("INSERT INTO decisions(ts,action) VALUES (0,'SKIP')").lastrowid
                with patch.object(engine.executor, 'execute', new=AsyncMock(
                        return_value={'ok': True, 'reason': '模拟成交'})) as execute:
                    row = await engine._finish(market, market['market_id'], time.time(), eid,
                                               {'p_up': p_up, 'voters': voters}, settings)
                    self.assertEqual(row['side'], side)
                    self.assertAlmostEqual(row['model_prob'], min(p_up, 1-p_up))
                    self.assertEqual(row['action'], 'TRADE' if expected == 'TRADE' else 'SKIP')
                    self.assertEqual(row['reason'], '模拟成交（反买）' if expected == 'TRADE' else expected)
                    if expected == 'TRADE':
                        execute.assert_awaited_once_with(market, side, row['model_prob'], price)
                    else:
                        execute.assert_not_awaited()
                    if count == 9:
                        self.assertEqual(row['market_price'], price)
                        self.assertAlmostEqual(row['edge'], row['model_prob'] - price)
                saved = self.store.rows('SELECT * FROM decisions WHERE id=?', (eid,))[0]
                for field in ('side', 'model_prob', 'market_price', 'edge', 'reason'):
                    self.assertEqual(saved[field], row[field])
                history = self.store.rows('SELECT * FROM eval_history ORDER BY id DESC')[0]
                self.assertEqual(history['ensemble_p'], p_up)
                self.assertEqual(json.loads(history['voters_json']), voters)

        self.store.set_meta('market_prices', {market['market_id']: {'start_price': 100}})
        self.store.set_meta('settlement_prices', {market['market_id']: 110})
        with patch('app.db.time.time', return_value=market['end_time'] + 1):
            self.store.score_evaluations()
        scores = self.store.rows('SELECT s.*,h.ensemble_p FROM eval_scores s JOIN eval_history h ON h.id=s.eval_id')
        self.assertEqual(len(scores), 8)
        for score in scores:
            self.assertEqual(score['outcome'], 'UP')
            self.assertTrue(all(value == int(score['ensemble_p'] > .5)
                                for value in json.loads(score['votes_json']).values()))

    async def test_invert_signal_uses_opposite_token_in_paper_and_live_orders(self):
        from engine.runner import Engine
        await Sync(self.store, self.client).run_once()
        market = self.market()
        self.stub.quote_price = .1
        for mode, p_up, side, token in (('PAPER', .8, 'DOWN', 'down1'), ('LIVE', .2, 'UP', 'up1')):
            with self.subTest(mode=mode):
                self.store.update_settings({'engine_enabled': False})
                settings = self.enable(mode=mode, invert_signal=True)
                self.store.execute('DELETE FROM reservations')
                self.store.execute('DELETE FROM paper_orders')
                for outcome in self.stub.detail['variants'][0]['outcomes']:
                    outcome['price'] = '.1' if outcome['name'] == side else '.95'
                eid = self.store.execute("INSERT INTO decisions(ts,action) VALUES (0,'SKIP')").lastrowid
                voters = [{'name': f'v{i}', 'status': 'ok', 'p_up': p_up} for i in range(9)]
                row = await Engine(self.store, self.client)._finish(
                    market, market['market_id'], time.time(), eid, {'p_up': p_up, 'voters': voters}, settings)
                self.assertEqual(row['action'], 'TRADE', row)
                self.assertEqual(row['reason'], ('模拟成交' if mode == 'PAPER' else '真钱订单已提交') + '（反买）')
                table = 'paper_orders' if mode == 'PAPER' else 'orders_audit'
                order = self.store.rows(f'SELECT * FROM {table} ORDER BY id DESC')[0]
                self.assertEqual((order['side'], order['token_id']), (side, token))
                if mode == 'LIVE':
                    quotes = [parse_qs(raw) for route, raw in self.stub.calls if route.endswith('get-quote')]
                    self.assertEqual(quotes[-1]['tokenId'], [token])

    def test_edge_floor_and_consensus_settings(self):
        self.assertEqual(self.store.update_settings({'edge_min': -.5})['edge_min'], -.5)
        with self.assertRaises(ValueError):self.store.update_settings({'edge_min': -1.5})
        self.assertEqual(self.store.settings()['min_agree_votes'], 9)
        for value in (0,9,12):
            self.assertEqual(self.store.update_settings({'min_agree_votes': value})['min_agree_votes'],value)
        for value in (-1,13,8.5,True,'nan'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                self.store.update_settings({'min_agree_votes': value})

    async def test_consensus_gate_and_append_only_evaluations(self):
        from engine.runner import Engine
        from unittest.mock import AsyncMock
        market=self.market()
        self.enable()
        engine=Engine(self.store,self.client)
        for index,(p_up,count,minimum,action) in enumerate([
                (.9,8,9,'SKIP'),(.9,9,9,'TRADE'),(.1,8,9,'SKIP'),
                (.1,9,9,'TRADE'),(.9,0,0,'TRADE')]):
            with self.subTest(p_up=p_up,count=count,minimum=minimum):
                settings=self.store.update_settings({'min_agree_votes':minimum})
                voters=[{'name':f'v{i}','status':'ok','p_up':p_up} for i in range(count)]
                voters += [{'name':'opposite','status':'ok','p_up':1-p_up},
                           {'name':'neutral','status':'ok','p_up':.51 if p_up>.5 else .49},
                           {'name':'failed','status':'abstain','p_up':p_up}]
                eid=self.store.execute("INSERT INTO decisions(ts,action) VALUES (?,'SKIP')",(index,)).lastrowid
                with patch.object(self.client,'market_detail',wraps=self.client.market_detail) as detail, patch.object(
                        engine.executor,'execute',new=AsyncMock(return_value={'ok':True,'reason':'fixture'})) as execute:
                    row=await engine._finish(market,market['market_id'],index,eid,
                                             {'p_up':p_up,'voters':voters},settings)
                    self.assertEqual(row['action'],action)
                    self.assertEqual(execute.await_count,int(action=='TRADE'))
                    self.assertEqual(detail.call_count,int(action=='TRADE'))
                    if action=='SKIP':self.assertEqual(row['reason'],'共识不足')
                history=self.store.rows('SELECT * FROM eval_history ORDER BY id')
                self.assertEqual(len(history),index+1)
                self.assertEqual(json.loads(history[-1]['voters_json']),voters)
                self.assertEqual(history[-1]['ensemble_p'],p_up)
                self.assertEqual(history[-1]['window_start'],market['start_time'])
                if index==0:first=history[0]
                self.assertEqual(history[0],first)

    def test_evaluation_history_prunes_only_after_5000(self):
        market=self.market()
        with self.store.transaction():
            self.store.conn.executemany('INSERT INTO eval_history(ts,market_id,symbol,window_start,window_end,voters_json,ensemble_p) VALUES (?,?,?,?,?,?,?)',
                [(i,'m','BTCUSDT',i,i+300,'[]',.6) for i in range(5000)])
        self.store.record_evaluation(5000,market,[],.7)
        rows=self.store.rows('SELECT id FROM eval_history ORDER BY id')
        self.assertEqual(len(rows),5000)
        self.assertEqual((rows[0]['id'],rows[-1]['id']),(2,5001))

    def test_symbol_settings_normalization(self):
        self.assertEqual(self.store.settings()['symbols'], ['BTCUSDT', 'ETHUSDT'])
        for supplied, expected in [
            (['BTCUSDT'], ['BTCUSDT']),
            ([' ethusdt ', 'btcusdt', 'ETHUSDT'], ['ETHUSDT', 'BTCUSDT']),
            (['SOLUSDT', 'ETHUSDT'], ['ETHUSDT']),
            ([], ['ETHUSDT']),
            (['SOLUSDT'], ['ETHUSDT']),
        ]:
            with self.subTest(symbols=supplied):
                self.assertEqual(self.store.update_settings({'symbols': supplied})['symbols'], expected)
                self.assertEqual(self.store.settings()['symbols'], expected)
        updated = self.store.update_settings({'symbols': [], 'bet_usdt': 12})
        self.assertEqual(updated['symbols'], ['ETHUSDT'])
        self.assertEqual(updated['bet_usdt'], 12)
        for supplied in ('BTCUSDT', None, {'BTCUSDT': True}):
            with self.subTest(symbols=supplied), self.assertRaises(ValueError):
                self.store.update_settings({'symbols': supplied})

    async def test_symbol_whitelist_market_selection(self):
        class MarketsClient:
            def __init__(self):
                self.calls = []
            async def market_list(self, **params):
                return {'marketTopics': [
                    {'marketTopicId': symbol, 'symbol': symbol}
                    for symbol in ('BTCUSDT', 'ETHUSDT')], 'hasMore': False}
            async def market_detail(self, topic_id):
                self.calls.append(topic_id)
                return dict(detail_fixture(), marketTopicId=topic_id, symbol=topic_id)

        client = MarketsClient()
        selected = await select_markets(client, {'symbols': ['BTCUSDT']})
        self.assertEqual([market['symbol'] for market in selected], ['BTCUSDT'])
        self.assertEqual(client.calls, ['BTCUSDT'])
        eth = dict(detail_fixture(), symbol='ETHUSDT')
        self.assertEqual(normalize_markets(eth, settings={'symbols': ['BTCUSDT']}), [])
        for settings in ({}, {'symbols': []}):
            with self.subTest(settings=settings):
                self.assertEqual(len(normalize_markets(eth, settings=settings)), 1)
                self.assertEqual({market['symbol'] for market in await select_markets(client, settings)},
                                 {'BTCUSDT', 'ETHUSDT'})

    async def test_signatures_endpoint_map_and_raw_cancel(self):
        await self.client.category_list()
        await self.client.market_list(l1Category='crypto', l2Category='up-down', sortBy='END_DATE', orderBy='ASC', offset=0, limit=20)
        await self.client.market_detail('topic1')
        await self.client.order_book(vendor='PREDICT_FUN', marketId='variant1', tokenId='up1')
        await self.client.last_trade_price('variant1')
        quote = await self.client.get_quote(walletAddress='0xabc', tokenId='up1', side='BUY', amountIn=usdt_to_wei(10))
        result = await self.client.place_order_bundle(walletAddress='0xabc', walletId='wallet1', quoteId=quote['quoteId'])
        self.assertEqual(result['orderId'], 'order1')
        canceled = await self.client.batch_cancel('0xabc', 'wallet1', [{'orderId': 'order1'}, {'orderId': 'order2'}])
        self.assertEqual(canceled['canceled'], ['order1'])
        for method in (self.client.order_list, self.client.order_history, self.client.position_list,
                       self.client.settled_history, self.client.pnl_portfolio, self.client.pnl_query,
                       self.client.wallet_list, self.client.payment_options):
            await method(walletAddress='0xabc')
        self.assertEqual(len(self.stub.calls), 16)
        self.assertAlmostEqual(self.client.time_offset_ms, 750, delta=100)
        self.assertEqual(usdt_to_wei('1.2345678901234567899'), '1234567890123456789')
        self.assertEqual(wei_to_usdt('1000000000000000000'), 1)
        self.assertEqual(str(money_down('10.999')), '10.99')

    async def test_five_minute_discovery_voters_and_mocked_live_trades(self):
        from engine.runner import Engine

        # Exercise real discovery, ensemble, decision, quote and placement.
        # Only model responses and the exchange transport are replaced.
        for symbols in (['BTCUSDT'], ['ETHUSDT'], ['BTCUSDT', 'ETHUSDT'],
                        ['ETHUSDT', 'BTCUSDT']):
            with self.subTest(symbols=symbols), patch.dict(os.environ, {
                    'EXECUTOR_PATH': 'sapi', 'VERTEX_SA_JSON': '/unused-fixture.json',
                    'CPA_BASE_URL': 'http://model-stub', 'CPA_API_KEY': 'fixture',
                    'WB_BASE_URL': 'http://model-stub', 'WB_API_KEY': 'fixture'}, clear=True):
                for table in ('decisions', 'eval_history', 'reservations', 'orders_audit', 'daily_spend'):
                    self.store.execute(f'DELETE FROM {table}')
                self.stub.calls.clear()
                self.stub.kline_symbols.clear()
                self.stub.details = {d['marketTopicId']: d for d in map(five_minute_fixture, symbols)}
                if 'ETHUSDT' in symbols:
                    eth = self.stub.details['eth-topic']
                    eth.pop('symbol')  # Title-only topics and feed metadata both occur.
                    if len(symbols) > 1:
                        eth['variantData'] = {'priceFeedSymbol': 'ETHUSDT'}
                # This fixture has four directional model/news votes.
                self.enable(mode='LIVE', symbols=symbols, min_duration=300, max_duration=300,
                            min_agree_votes=4)
                await Sync(self.store, self.client).run_once()
                prompts = []

                async def vote(prompt, grounded=True):
                    symbol = 'ETHUSDT' if 'Estimate probability that ETHUSDT' in prompt else 'BTCUSDT'
                    prompts.append((symbol, grounded, prompt))
                    return {'p_up': .99 if symbol == 'BTCUSDT' else .01,
                            'reason': f'offline {symbol}', 'sources': [{'query': symbol}]}

                async def model_vote(base, key, model, prompt):
                    return await vote(prompt, grounded=False)

                ensemble = Ensemble(self.client)
                with patch.object(ensemble, '_vertex', vote), patch.object(ensemble, '_openai', model_vote):
                    engine = Engine(self.store, self.client, ensemble=ensemble)
                    decisions = await engine.tick()
                    self.assertEqual(len(decisions), len(symbols))
                    self.assertTrue(all(d['action'] == 'TRADE' for d in decisions), decisions)
                    self.assertEqual(self.stub.kline_symbols, symbols)
                    for symbol in symbols:
                        asset = symbol[:3].lower()
                        side = 'UP' if symbol == 'BTCUSDT' else 'DOWN'
                        decision = next(d for d in decisions if d['market_id'] == f'{asset}-5m')
                        self.assertEqual(decision['side'], side)
                        voters = json.loads(decision['voters_json'])
                        self.assertEqual(len(voters), 4)
                        self.assertTrue(all(v['status'] == 'ok' for v in voters))
                        self.assertEqual(len([p for s, _, p in prompts if s == symbol]), 4)
                        self.assertEqual(ensemble.news_cache[symbol][1]['reason'], f'offline {symbol}')
                    audits = self.store.rows('SELECT market_id,token_id,status FROM orders_audit ORDER BY id')
                    tokens = [f"{s[:3].lower()}-{'up' if s == 'BTCUSDT' else 'down'}" for s in symbols]
                    self.assertEqual([a['token_id'] for a in audits], tokens)
                    self.assertTrue(all(a['status'] == 'SUCCESS' for a in audits))
                    quotes = [parse_qs(raw) for route, raw in self.stub.calls if route.endswith('get-quote')]
                    self.assertEqual([q['tokenId'][0] for q in quotes], tokens)
                    self.assertEqual(len([r for r, _ in self.stub.calls if r.endswith('place-order-bundle')]), len(symbols))
                    self.assertEqual(Risk(self.store).spent(), 10 * len(symbols))
                    self.assertEqual(await engine.tick(), [])  # Occupancy remains per market.

    async def test_mixed_cycle_cooldown_and_global_risk_limits(self):
        from engine.runner import Engine

        class Voter:
            async def evaluate(inner, symbol, context):
                calls.append(symbol)
                return {'p_up': .9, 'voters': [{'name': f'fixture-{i}', 'status': 'ok', 'p_up': .9} for i in range(9)]}

        for limit, cap, expected in ((5, 100, ['TRADE', 'TRADE']),
                                     (1, 100, ['TRADE', 'SKIP']),
                                     (5, 10, ['TRADE', 'SKIP'])):
            with self.subTest(limit=limit, cap=cap):
                for table in ('decisions', 'paper_orders', 'daily_spend'):
                    self.store.execute(f'DELETE FROM {table}')
                calls = []
                self.stub.details = {d['marketTopicId']: d for d in
                                     map(five_minute_fixture, ('BTCUSDT', 'ETHUSDT'))}
                self.enable(mode='PAPER', symbols=['BTCUSDT', 'ETHUSDT'],
                            min_duration=300, max_duration=300,
                            max_open_positions=limit, daily_spend_cap=cap)
                engine = Engine(self.store, self.client, ensemble=Voter())
                decisions = await engine.tick()
                self.assertEqual(calls, ['BTCUSDT', 'ETHUSDT'])
                self.assertEqual([d['action'] for d in decisions], expected)
                self.assertEqual(await engine.tick(), [])
                self.assertEqual(len(calls), 2)
                # BTC stays occupied; a recent ETH decision cools only ETH.
                self.store.execute("DELETE FROM paper_orders WHERE market_id='eth-5m'")
                self.assertEqual(await engine.tick(), [])
                self.store.execute("UPDATE decisions SET ts=? WHERE market_id='eth-5m'", (time.time() - 601,))
                self.assertEqual(await engine.tick(), [])  # One decision per window even after cooldown.
                self.assertEqual(calls, ['BTCUSDT', 'ETHUSDT'])

    async def test_errors_backoff_and_never_retry_place(self):
        self.stub.throttle = 1
        self.assertIn('categories', await self.client.category_list())
        self.assertEqual(len(self.stub.calls), 2)
        self.stub.fail_place = True
        with self.assertRaises(BinanceError) as caught:
            await self.client.place_order_bundle(walletAddress='0xabc', walletId='wallet1', quoteId='quote1')
        self.assertEqual(caught.exception.user_message, '余额不足')
        self.assertEqual(len([r for r, _ in self.stub.calls if r.endswith('place-order-bundle')]), 1)
        wrong = BinanceClient(KEY, 'wrong', 'http://binance-stub', http=self.http)
        with self.assertRaises(BinanceError) as bad:
            await wrong.category_list()
        self.assertEqual(bad.exception.code, -1022)
        for code in (-1002, -1022, -2015, -3026):
            self.assertNotEqual(BinanceError(code, 'test').user_message, '币安请求失败')

    def test_slippage_settings_validation(self):
        for value in (1, 2500, 10000):
            with self.subTest(value=value):
                self.assertEqual(self.store.update_settings({'slippage_bps': value})['slippage_bps'], value)
        for value in (0, 10001):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.store.update_settings({'slippage_bps': value})
        self.assertEqual(self.store.settings()['slippage_bps'], 10000)

    def test_prob_threshold_validation(self):
        self.assertEqual(self.store.settings()['prob_threshold'], 0.62)
        with self.assertRaises(ValueError):
            self.store.update_settings({'prob_threshold': 0.49})
        for value in (0.50, 0.55, 0.60):
            with self.subTest(value=value):
                self.assertEqual(self.store.update_settings({'prob_threshold': value})['prob_threshold'], value)

    async def test_hard_caps_daily_budget_loss_and_occupancy(self):
        self.enable(bet_usdt=100, daily_spend_cap=5000, daily_loss_stop=1000)
        risk = Risk(self.store)
        self.assertTrue(risk.check(100, 'new', pnl=-999.99)[0])
        self.assertFalse(risk.check(100.01, 'new')[0])
        for invalid in ({'bet_usdt': 100.01}, {'daily_spend_cap': 5000.01}, {'daily_loss_stop': 1000.01},
                        {'bet_usdt': 0.99}, {'daily_spend_cap': 0.99}, {'daily_loss_stop': 0},
                        {'max_open_positions': 51}, {'cooldown_seconds': 599}):
            with self.assertRaises(ValueError): self.store.update_settings(invalid)
        risk.charge(4900)
        self.assertTrue(risk.check(100, 'new')[0])
        risk.charge(90)
        self.assertFalse(risk.check(11, 'new')[0])
        self.assertTrue(risk.check(10, 'new')[0])
        self.assertFalse(risk.check(10, 'new', pnl=-1000)[0])
        self.assertFalse(self.store.settings()['engine_enabled'])
        self.enable(max_open_positions=10)
        self.store.execute('DELETE FROM daily_spend')
        for i in range(5):
            self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)', (str(i), time.time(), 1, 'PLACED', str(i), 'token'))
        self.assertTrue(risk.check(10, 'new')[0])
        for i in range(5,10):
            self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)', (str(i), time.time(), 1, 'PLACED', str(i), 'token'))
        self.assertEqual(risk.check(10, 'new'), (False, '持仓数量已达上限'))
        self.assertTrue(risk.occupied('0'))

    async def test_sync_reconciles_audit_fills_after_history_refresh(self):
        self.store.execute('INSERT INTO orders_audit (order_id, amount, status) VALUES (?, ?, ?)',
                           ('failed-order', 10, 'SUCCESS'))
        self.stub.order_history = [{'orderId': 'failed-order', 'status': 'FAILED', 'filledUsdtAmount': '0'}]
        # An unrelated endpoint failure must not hide authoritative fill evidence.
        with patch.object(self.client, 'pnl_query', side_effect=RuntimeError('unrelated endpoint failed')):
            state = await Sync(self.store, self.client).run_once()
        self.assertIn('pnl_query', state['errors'])
        row = self.store.rows('SELECT status,error FROM orders_audit')[0]
        self.assertEqual(row, {'status': 'FAILED', 'error': '未成交（场馆 FAILED）'})

    async def test_sync_prunes_only_stale_placed_reservations_absent_from_positions(self):
        self.enable(mode='LIVE', max_open_positions=1)
        risk = Risk(self.store)
        sync = Sync(self.store, self.client)
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
                self.store.set_meta('positions', {'list': [{'marketId': 'old', 'shares': 2}]})
                self.stub.positions = [{'marketId': 'old', 'shares': 2}] if present else []
                self.assertEqual(risk.check(10, 'new'), (False, '持仓数量已达上限'))
                state = await sync.run_once()
                self.assertFalse(state['errors'], state)
                self.assertEqual(bool(self.store.rows('SELECT * FROM reservations')), kept)
                self.assertEqual(risk.check(10, 'new')[0], not kept)

    async def test_failed_position_refresh_does_not_prune_reservations(self):
        self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)',
                           ('old', time.time()-1200, 10, 'PLACED', 'order-old', 'token'))
        self.store.set_meta('positions', {'list': []})
        with patch.object(self.client, 'position_list', side_effect=RuntimeError('snapshot unavailable')):
            state = await Sync(self.store, self.client).run_once()
        self.assertIn('positions', state['errors'])
        self.assertEqual(len(self.store.rows('SELECT * FROM reservations')), 1)

    async def test_paper_fill_mark_settlement_and_polling(self):
        self.enable()
        executor = Executor(self.store, self.client)
        self.assertTrue((await executor.execute(self.market(), 'UP'))['ok'])
        row = self.store.rows('SELECT * FROM paper_orders')[0]
        self.assertEqual(row['amount'], 10)
        self.assertEqual(row['shares'], 25)
        self.assertEqual(Risk(self.store).spent(), 10)
        self.assertFalse((await executor.execute(self.market(), 'UP'))['ok'])
        mark_paper(self.store, 'variant1', {'up1': .2})
        self.assertAlmostEqual(Risk(self.store).paper_pnl(), -5)
        # A price of 1 without explicit resolution never settles a market.
        self.stub.detail['variants'][0]['outcomes'][0]['price'] = '1'
        await Sync(self.store, self.client).run_once()
        self.assertEqual(self.store.rows('SELECT status FROM paper_orders')[0]['status'], 'OPEN')
        self.stub.detail['variants'][0]['status'] = 'RESOLVED'
        for outcome in self.stub.detail['variants'][0]['outcomes']: outcome['isWinner'] = True
        await Sync(self.store, self.client).run_once()
        self.assertEqual(self.store.rows('SELECT status FROM paper_orders')[0]['status'], 'OPEN')
        for outcome in self.stub.detail['variants'][0]['outcomes']: outcome.pop('isWinner')
        self.stub.detail['variants'][0].update(status='RESOLVED', winningTokenId='up1')
        await Sync(self.store, self.client).run_once()
        row = self.store.rows('SELECT * FROM paper_orders')[0]
        self.assertEqual(row['status'], 'SETTLED')
        self.assertEqual(row['realized_pnl'], 15)
        settle_paper(self.store, 'variant1', 'up1')
        self.assertEqual(Risk(self.store).paper_pnl(), 15)
        self.assertFalse(any('/trade/' in route for route, _ in self.stub.calls))

    def paper_fallback_order(self, title, end, side='UP', market_id='fallback'):
        return self.store.execute(
            "INSERT INTO paper_orders(ts,market_id,topic_id,title,token_id,side,amount,price,shares,status,current_price,end_time) VALUES (?,?,?,?,?,?,10,.4,25,'OPEN',.4,?)",
            (end-60, market_id, 'topic1', title, 'up1', side, end)).lastrowid

    def paper_fallback_score(self, symbol, start, outcome):
        self.store.execute('INSERT INTO eval_scores(symbol,window_start,outcome,votes_json) VALUES (?,?,?,?)',
                           (symbol, start, outcome, '{}'))

    async def test_paper_eval_fallback_win_loss_and_idempotence(self):
        now = 2000000000
        end = now-7200
        for symbol, duration, side, outcome in (
                ('BTC', 5, 'UP', 'UP'), ('ETH', 15, 'DOWN', 'UP'),
                ('BTC', 15, 'DOWN', 'DOWN'), ('ETH', 5, 'UP', 'DOWN')):
            with self.subTest(symbol=symbol, duration=duration, side=side, outcome=outcome):
                self.store.execute('DELETE FROM paper_orders')
                self.store.execute('DELETE FROM eval_scores')
                oid = self.paper_fallback_order(f'{symbol} Up or Down {duration}m', end, side)
                self.paper_fallback_score(symbol+'USDT', end-duration*60, outcome)
                sync = Sync(self.store, self.client)
                with patch('app.sync.time.time', return_value=now), \
                        patch.object(self.client, 'market_detail', new=AsyncMock(side_effect=RuntimeError('venue unavailable'))), \
                        self.assertLogs('app.sync', level='WARNING') as logs:
                    await sync._paper_poll()
                row = self.store.rows('SELECT * FROM paper_orders WHERE id=?', (oid,))[0]
                self.assertEqual((row['status'], row['settled_at']), ('SETTLED', now))
                self.assertEqual(row['realized_pnl'], 15 if side==outcome else -10)
                self.assertEqual(row['current_price'], 1 if side==outcome else 0)
                self.assertEqual(len(logs.output), 1)
                self.assertIn(f'order_id={oid} outcome={outcome}', logs.output[0])
                with self.assertNoLogs('app.sync'):
                    await sync._paper_poll()
                self.assertEqual(self.store.rows('SELECT * FROM paper_orders')[0], row)

    async def test_paper_eval_fallback_grace_missing_outcome_and_retry(self):
        now = 2000000000
        for age, title, score_symbol, score_offset, outcome in (
                (60, 'BTC Up or Down 5m', 'BTCUSDT', 0, 'UP'),
                (120, 'BTC Up or Down 5m', 'BTCUSDT', 0, 'UP'),
                (7200, 'BTC Up or Down 5m', None, 0, 'UP'),
                (7200, 'BTC Up or Down 5m', 'ETHUSDT', 0, 'UP'),
                (7200, 'BTC Up or Down 5m', 'BTCUSDT', 300, 'UP'),
                (7200, 'BTC Up or Down 5m', 'BTCUSDT', 0, 'UNKNOWN'),
                (7200, 'BTC Up or Down', 'BTCUSDT', 0, 'UP'),
                (7200, 'SOL Up or Down 5m', 'BTCUSDT', 0, 'UP')):
            with self.subTest(age=age, title=title, symbol=score_symbol, offset=score_offset, outcome=outcome):
                self.store.execute('DELETE FROM paper_orders')
                self.store.execute('DELETE FROM eval_scores')
                end = now-age
                self.paper_fallback_order(title, end)
                if score_symbol:
                    self.paper_fallback_score(score_symbol, end-300+score_offset, outcome)
                before = self.store.rows('SELECT * FROM paper_orders')[0]
                sync = Sync(self.store, self.client)
                with patch('app.sync.time.time', return_value=now), self.assertNoLogs('app.sync'):
                    await sync._paper_poll()
                self.assertEqual(self.store.rows('SELECT * FROM paper_orders')[0], before)
                if score_symbol is None:
                    self.paper_fallback_score('BTCUSDT', end-300, 'UP')
                    with patch('app.sync.time.time', return_value=now), self.assertLogs('app.sync'):
                        await sync._paper_poll()
                    self.assertEqual(self.store.rows('SELECT status FROM paper_orders')[0]['status'], 'SETTLED')

    async def test_paper_eval_fallback_keeps_venue_result_priority(self):
        now = 2000000000
        end = now-7200
        self.paper_fallback_order('BTC Up or Down 5m', end, market_id='variant1')
        self.paper_fallback_score('BTCUSDT', end-300, 'UP')
        self.stub.detail['variants'][0].update(status='RESOLVED', winningTokenId='down1')
        with self.assertNoLogs('app.sync'):
            await Sync(self.store, self.client)._paper_poll()
        row = self.store.rows('SELECT * FROM paper_orders')[0]
        self.assertEqual((row['status'], row['current_price'], row['realized_pnl']), ('SETTLED', 0, -10))

    async def test_live_execution_prunes_order_audit_after_each_insert(self):
        await Sync(self.store, self.client).run_once()
        self.enable(mode='LIVE')
        for index in range(15):
            self.store.execute('INSERT INTO orders_audit (ts, status) VALUES (?, ?)',
                               (time.time() + 1000 - index, 'SUCCESS'))
        executor = Executor(self.store, self.client)
        # Definitive failures release reservations and budget, allowing repeated attempts.
        self.stub.fail_place = True
        for index in range(12):
            result = await executor.execute(dict(self.market(), market_id=f'prune-{index}'), 'UP')
            self.assertEqual(result['status'], 'FAILED')
            rows = self.store.rows('SELECT * FROM orders_audit ORDER BY id DESC')
            newest_id = 16 + index
            self.assertEqual(len(rows), 10)
            self.assertEqual([row['id'] for row in rows], list(range(newest_id, newest_id - 10, -1)))
            self.assertEqual(rows[0]['market_id'], f'prune-{index}')
            self.assertEqual(rows[0]['status'], 'FAILED')

    async def test_live_setting_overrides_market_slippage(self):
        await Sync(self.store, self.client).run_once()
        self.enable(mode='LIVE', slippage_bps=2500)
        market = dict(self.market(), slippage_bps=1000)
        result = await Executor(self.store, self.client).execute(market, 'UP')
        self.assertTrue(result['ok'], result)
        trade = [(route.removeprefix(PREFIX), parse_qs(raw)['slippageBps'])
                 for route, raw in self.stub.calls
                 if route.endswith(('get-quote', 'place-order-bundle'))]
        self.assertEqual(trade, [('trade/get-quote', ['2500']),
                                 ('trade/place-order-bundle', ['2500'])])

    async def test_live_execution_audit_and_failures(self):
        await Sync(self.store, self.client).run_once()
        self.enable(mode='LIVE')
        executor = Executor(self.store, self.client)
        result = await executor.execute(self.market(), 'UP')
        self.assertTrue(result['ok'], result)
        self.assertEqual(self.store.rows('SELECT status FROM orders_audit')[0]['status'], 'SUCCESS')
        self.assertTrue(Risk(self.store).occupied('variant1'))
        self.assertFalse((await executor.execute(self.market(), 'UP'))['ok'])
        second = dict(self.market(), market_id='variant2')
        self.stub.fail_place = True
        self.assertFalse((await executor.execute(second, 'UP'))['ok'])
        self.assertEqual(self.store.rows('SELECT status FROM orders_audit ORDER BY id')[-1]['status'], 'FAILED')
        self.assertEqual(Risk(self.store).spent(), 10)

    async def test_permission_pending_preserves_account_snapshot(self):
        sync = Sync(self.store, self.client)
        good = await sync.run_once()
        self.assertFalse(good['permission_pending'])
        self.assertEqual(self.store.meta('live_daily_pnl')['value'], 7)
        old = self.store.meta('pnl_portfolio')
        self.stub.pending = True
        result = await sync.run_once()
        self.assertTrue(result['permission_pending'])
        self.assertEqual(self.store.meta('pnl_portfolio'), old)
        self.assertEqual(self.store.meta('wallet')['walletId'], 'wallet1')
        self.enable(mode='LIVE')
        self.assertFalse(Risk(self.store).check(10, 'new')[0])

    async def test_selector_validation_and_missing_voters(self):
        detail = detail_fixture()
        self.assertEqual(len(normalize_markets(detail)), 1)
        detail['variants'][0]['startTime'] = time.time() - 3000
        detail['variants'][0]['endTime'] = time.time() + 500
        self.assertEqual(normalize_markets(detail), [])
        detail = detail_fixture(); detail['symbol'] = 'SOLUSDT'
        self.assertEqual(normalize_markets(detail), [])
        detail = detail_fixture(); detail['variants'][0]['status'] = 'CLOSED'
        self.assertEqual(normalize_markets(detail), [])
        for raw in ('{"p_up":true,"reason":"x"}', '{"p_up":1.1,"reason":"x"}', '{"p_up":0.5,"reason":"x","extra":1}'):
            with self.assertRaises(ValueError): strict_vote(raw)
        names = ('CPA_BASE_URL', 'CPA_API_KEY', 'WB_BASE_URL', 'WB_API_KEY', 'VERTEX_SA_JSON')
        with patch.dict(os.environ, {name: '' for name in names}):
            result = await Ensemble(self.client).evaluate('BTCUSDT', self.market())
        self.assertEqual(len(result['voters']), 4)
        self.assertEqual(sum(v['status'] == 'abstain' for v in result['voters']), 4)
        self.assertIsNone(result['p_up'])
        self.assertIsNone(aggregate([]))

    async def test_news_first_cached_briefing_injected_and_voter_errors_abstain(self):
        ensemble = Ensemble(self.client)
        events, prompts = [], []
        briefing = '2026-11-09：离线新闻测试，事件存在不确定性'

        async def vertex(prompt):
            events.append('news')
            return {'p_up': .7, 'reason': briefing,
                    'sources': [{'web': {'uri': 'https://offline.invalid/source'}}]}

        async def gateway(base, key, model, prompt):
            events.append(model); prompts.append(prompt)
            if model == 'gpt-6-astra': raise ValueError('simulated failed voter')
            return {'p_up': .8, 'reason': '离线模型'}

        ensemble._vertex, ensemble._openai = vertex, gateway
        environment = {'CPA_BASE_URL': 'http://offline.invalid', 'CPA_API_KEY': 'fixture',
                       'WB_BASE_URL': 'http://offline.invalid', 'WB_API_KEY': 'fixture',
                       'VERTEX_SA_JSON': '/offline-unused-fixture'}
        with patch.dict(os.environ, environment):
            result = await ensemble.evaluate('BTCUSDT', self.market())
            await ensemble.evaluate('BTCUSDT', self.market())
        self.assertEqual(events[0], 'news')
        self.assertEqual(events.count('news'), 1)
        self.assertEqual(len(prompts), 6)
        self.assertTrue(all(briefing in prompt and 'technical_votes' not in prompt for prompt in prompts))
        failed = next(v for v in result['voters'] if v['name'] == 'gpt-6-astra')
        self.assertEqual(failed['status'], 'abstain')
        llm = [v['p_up'] for v in result['voters'] if v['group'] == 'llm' and v['status'] == 'ok']
        self.assertTrue(all(v['group'] != 'ta' for v in result['voters']))
        expected = (sum(llm) + .7) / (len(llm) + 1)
        self.assertAlmostEqual(result['p_up'], expected)

    async def test_dashboard_auth_chinese_fixtures_and_csrf(self):
        from app.main import create_app
        await Sync(self.store, self.client).run_once()
        self.enable()
        await Executor(self.store, self.client).execute(self.market(), 'UP')
        with patch.dict(os.environ, {'PUBLIC_READONLY': '0'}):
            app = create_app(self.store, self.client, background=False, password='test')
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://dashboard') as web:
            self.assertEqual((await web.get('/')).status_code, 303)
            self.assertEqual((await web.get('/api/state')).status_code, 401)
            self.assertEqual((await web.get('/static/index.html')).status_code, 401)
            self.assertEqual((await web.post('/login', content='password=wrong')).status_code, 401)
            response = await web.post('/login', content='password=test')
            self.assertEqual(response.status_code, 303)
            self.assertIn('HttpOnly', response.headers['set-cookie'])
            html = (await web.get('/')).text
            for text in ('盈利总览', '持仓', '订单记录', '引擎', '系统状态', '模拟', '真钱'):
                self.assertIn(text, html)
            state = (await web.get('/api/state')).json()
            self.assertIs(state['public'], False)
            self.assertEqual(state['paper_orders'][0]['amount'], 10)
            self.assertEqual(state['meta']['wallet']['walletId'], 'wallet1')
            self.assertEqual((await web.post('/api/settings', json={'engine_enabled': False})).status_code, 403)
            headers = {'x-csrf-token': state['csrf']}
            response = await web.post('/api/settings', headers=headers, json={'bet_usdt': 2, 'daily_spend_cap': 1000})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(self.store.settings()['bet_usdt'], 2)
            self.assertEqual(self.store.settings()['daily_spend_cap'], 1000)
            self.assertEqual((await web.post('/api/settings', headers=headers, json={'engine_enabled': False})).status_code, 200)
            self.assertEqual((await web.post('/api/settings', headers=headers, json={'bet_usdt': 101})).status_code, 400)
            await web.post('/logout', headers=headers)
            self.assertEqual((await web.get('/api/state')).status_code, 401)

    async def test_public_readonly_dashboard(self):
        from app.main import create_app
        for flag in ('1', '0', 'true', ''):
            with self.subTest(flag=flag), patch.dict(os.environ, {'PUBLIC_READONLY': flag}):
                app = create_app(self.store, self.client, background=False, password='test')
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://dashboard') as web:
                    response = await web.get('/')
                    state = await web.get('/api/state')
                    if flag == '1':
                        self.assertEqual(response.status_code, 200)
                        self.assertIn('盈利总览', response.text)
                        self.assertEqual(state.status_code, 200)
                        self.assertIs(state.json()['public'], True)
                        self.assertEqual(state.json()['csrf'], 'public')
                        # Static files have no route today, but pass authentication.
                        self.assertEqual((await web.get('/static/missing.css')).status_code, 404)
                        self.assertEqual((await web.head('/api/state')).status_code, 405)
                    else:
                        self.assertEqual(response.status_code, 303)
                        self.assertEqual(response.headers['location'], '/login')
                        self.assertEqual(state.status_code, 401)
                    self.assertEqual((await web.get('/api/other')).status_code, 401)
                    self.assertEqual((await web.post('/api/settings', json={'symbols': ['BTCUSDT']},
                                                     headers={'x-csrf-token': 'public'})).status_code, 401)
                    self.assertEqual((await web.post('/api/state')).status_code, 401)
                    if flag == '1':
                        await web.post('/login', content='password=test')
                        csrf = (await web.get('/api/state')).json()['csrf']
                        self.assertNotEqual(csrf, 'public')
                        self.assertEqual((await web.post('/api/settings', json={})).status_code, 403)
                        self.assertEqual((await web.post('/api/settings', json={},
                                                         headers={'x-csrf-token': csrf})).status_code, 200)

    async def test_engine_stop_gates_cooldown_decisions_and_open_variant(self):
        from engine.runner import Engine
        calls = []
        market = self.market()
        store = self.store

        class Voter:
            stop_after = False
            p = .58
            async def evaluate(inner, symbol, context):
                calls.append(symbol)
                if inner.stop_after: (store.data_dir / 'STOP').touch()
                return {'p_up': inner.p, 'voters': [{'name': '测试', 'p_up': inner.p, 'reason': '固定离线信号'}]}

        voter = Voter()
        engine = Engine(store, self.client, ensemble=voter)
        store.update_settings({'min_agree_votes': 0})
        # Disabled and STOP gates are evaluated before selection or a new decision.
        self.assertEqual(await engine.tick(), [])
        self.enable()
        (store.data_dir / 'STOP').touch()
        self.assertEqual(await engine.tick(), [])
        self.assertEqual(store.rows('SELECT * FROM decisions'), [])
        (store.data_dir / 'STOP').unlink()

        async def selector(*args): return [market]
        with patch('engine.runner.select_markets', selector):
            result = await engine.tick()
            self.assertEqual(result[0]['action'], 'SKIP')
            self.assertIn('阈值', result[0]['reason'])
            await engine.tick()
            self.assertEqual(len(calls), 1)
            store.execute('UPDATE decisions SET ts=?', (time.time() - 601,))
            market = dict(market, start_time=market['start_time'] + 1)
            voter.p = .8; voter.stop_after = True
            result = await engine.tick()
            self.assertEqual(result[0]['action'], 'SKIP')
            self.assertIn('停止', result[0]['reason'])
            self.assertEqual(store.rows('SELECT * FROM paper_orders'), [])
            (store.data_dir / 'STOP').unlink()
            voter.stop_after = False
            market = dict(market, start_time=market['start_time'] + 1)
            store.execute('UPDATE decisions SET ts=?', (time.time() - 601,))
            result = await engine.tick()
            self.assertEqual(result[0]['action'], 'TRADE')
            self.assertAlmostEqual(result[0]['edge'], .4)
            self.assertEqual(json.loads(result[0]['voters_json'])[0]['name'], '测试')
            store.execute('UPDATE decisions SET ts=?', (time.time() - 601,))
            self.assertEqual(await engine.tick(), [])
            self.assertEqual(len(calls), 3)

    async def test_runner_prunes_decisions_after_each_insert(self):
        from engine.runner import Engine

        class Abstain:
            async def evaluate(inner, symbol, context):
                return {'p_up': None, 'voters': {}}

        self.enable()
        for index in range(15):
            self.store.execute('INSERT INTO decisions (ts, market_id, action) VALUES (?, ?, ?)',
                               (time.time() + 1000 - index, f'old-{index}', 'SKIP'))
        engine = Engine(self.store, self.client, ensemble=Abstain())
        for index in range(12):
            market = dict(self.market(), market_id=f'prune-{index}', start_time=time.time()-600-index)
            async def selector(*args):
                return [market]
            with patch('engine.runner.select_markets', selector):
                result = await engine.tick()
            self.assertEqual(len(result), 1)
            rows = self.store.rows('SELECT * FROM decisions ORDER BY id DESC')
            newest_id = 16 + index
            self.assertEqual(len(rows), 10)
            self.assertEqual([row['id'] for row in rows], list(range(newest_id, newest_id - 10, -1)))
            self.assertEqual(rows[0]['market_id'], market['market_id'])
            self.assertEqual(rows[0]['reason'], '没有可用信号')

    async def test_runner_trades_up_and_down_with_sufficient_edge(self):
        from engine.runner import Engine
        market = self.market()
        class Voter:
            p = .9
            async def evaluate(inner, symbol, context):
                return {'p_up': inner.p, 'voters': [{'name': f'fixture-{i}', 'status': 'ok', 'p_up': inner.p} for i in range(9)]}
        voter = Voter()
        engine = Engine(self.store, self.client, ensemble=voter)
        self.enable()
        async def selector(*args): return [market]
        for p_up, side, price in ((.9, 'UP', .4), (.1, 'DOWN', .6)):
            with self.subTest(side=side):
                voter.p = p_up
                engine = Engine(self.store, self.client, ensemble=voter)
                self.store.execute('DELETE FROM paper_orders')
                self.store.execute('DELETE FROM decisions')
                with patch('engine.runner.select_markets', selector):
                    decisions = await engine.tick()
                self.assertEqual(len(decisions), 1)
                self.assertEqual((decisions[0]['action'], decisions[0]['side']), ('TRADE', side))
                self.assertAlmostEqual(decisions[0]['edge'], .9 - price)
                self.assertEqual(self.store.rows('SELECT side FROM paper_orders')[0]['side'], side)

    async def test_live_stop_during_quote_and_unknown_order_keep_reservation(self):
        await Sync(self.store, self.client).run_once()
        self.enable(mode='LIVE')
        original_quote = self.client.get_quote
        original_place = self.client.place_order_bundle
        async def stop_quote(**kwargs):
            result = await original_quote(**kwargs)
            (self.store.data_dir / 'STOP').touch()
            return result
        self.client.get_quote = stop_quote
        result = await Executor(self.store, self.client).execute(self.market(), 'UP')
        self.assertFalse(result['ok'])
        self.assertEqual(len([route for route, _ in self.stub.calls if route.endswith('place-order-bundle')]), 0)
        self.assertEqual(Risk(self.store).spent(), 0)
        (self.store.data_dir / 'STOP').unlink()
        self.client.get_quote = original_quote
        async def unknown_place(**kwargs):
            raise BinanceError(-1001, '网络请求失败；下单结果可能未知，请核对订单')
        self.client.place_order_bundle = unknown_place
        result = await Executor(self.store, self.client).execute(self.market(), 'UP')
        self.assertEqual(result['status'], 'UNKNOWN')
        self.assertEqual(Risk(self.store).spent(), 10)
        self.assertTrue(Risk(self.store).occupied('variant1'))
        self.assertEqual(self.store.rows('SELECT status FROM orders_audit ORDER BY id')[-1]['status'], 'UNKNOWN')
        self.client.place_order_bundle = original_place



class ConcurrentPredictionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='prediction-concurrent-')
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / 'test.sqlite3')
        self.addCleanup(self.store.close)
        self.store.update_settings({'engine_enabled': True, 'min_agree_votes': 0})
        self.details = {symbol: five_minute_fixture(symbol)
                        for symbol in ('BTCUSDT', 'ETHUSDT')}
        self.markets = [normalize_markets(detail)[0] for detail in self.details.values()]
        self.events = []
        outer = self

        class Client:
            async def market_detail(self, topic_id):
                outer.events.append(('refresh', topic_id))
                return next(d for d in outer.details.values() if d['marketTopicId'] == topic_id)

            async def klines(self, symbol, **kwargs):
                await asyncio.sleep(0)
                rows = candles()
                if symbol == 'ETHUSDT':
                    for row in rows:
                        row[4] = str(float(row[4]) * 20)
                return rows

        self.client = Client()

    async def tick(self, voter, execute=None):
        from engine.runner import Engine
        from unittest.mock import AsyncMock
        engine = Engine(self.store, self.client, ensemble=voter)
        if execute is not None:
            engine.executor.execute = execute
        with patch('engine.runner.select_markets', AsyncMock(return_value=self.markets)):
            return await asyncio.wait_for(engine.tick(), timeout=2)

    def assert_persisted(self, decisions):
        rows = self.store.rows('SELECT * FROM decisions ORDER BY id')
        self.assertEqual([{k: v for k, v in row.items() if k != 'id'} for row in rows], decisions)

    async def test_all_due_evaluations_start_before_either_finishes(self):
        started = []
        both_started = asyncio.Event()
        outer = self

        class Voter:
            async def evaluate(self, symbol, context):
                started.append(symbol)
                if len(started) == 2:
                    both_started.set()
                await both_started.wait()
                rows = outer.store.rows('SELECT reason FROM decisions ORDER BY id')
                outer.assertEqual([r['reason'] for r in rows], ['评估进行中'] * 2)
                return {'p_up': None, 'voters': []}

        decisions = await self.tick(Voter())
        self.assertEqual(started, ['BTCUSDT', 'ETHUSDT'])
        self.assertEqual([d['market_id'] for d in decisions], ['btc-5m', 'eth-5m'])
        self.assertEqual([d['reason'] for d in decisions], ['没有可用信号'] * 2)
        self.assert_persisted(decisions)

    async def test_failed_evaluation_does_not_affect_other_market(self):
        class Voter:
            async def evaluate(self, symbol, context):
                await asyncio.sleep(0)
                if symbol == 'BTCUSDT':
                    raise RuntimeError('BTC unavailable')
                return {'p_up': .9, 'voters': [{'name': 'ETH signal'}]}

        decisions = await self.tick(Voter())
        failed, completed = decisions
        self.assertEqual((failed['action'], failed['reason']), ('SKIP', '评估失败: BTC unavailable'))
        for field in ('model_prob', 'market_price', 'edge', 'side'):
            self.assertIsNone(failed[field])
        self.assertEqual(failed['voters_json'], '{}')
        self.assertEqual((completed['action'], completed['reason']), ('TRADE', '模拟成交'))
        self.assertEqual(json.loads(completed['voters_json']), [{'name': 'ETH signal'}])
        self.assert_persisted(decisions)

    async def test_concurrent_tick_preserves_both_price_contexts(self):
        self.markets[0]['start_price'] = 100
        self.markets[1]['start_price'] = 2000
        self.store.set_meta('market_prices', {'btc-5m': {'start_price': 99},
                                              'older': {'start_price': 10}})
        ensemble = Ensemble(self.client, store=self.store)
        original_meta = self.store.meta

        def checked_meta(key, default=None):
            if key == 'market_prices':
                self.assertTrue(self.store.conn.in_transaction,
                                'price context must be read inside the merge transaction')
            return original_meta(key, default)

        with patch.dict(os.environ, {}, clear=True), patch.object(self.store, 'meta', checked_meta):
            decisions = await self.tick(ensemble)
        prices = self.store.meta('market_prices')
        self.assertEqual(set(prices), {'btc-5m', 'eth-5m', 'older'})
        self.assertEqual(prices['btc-5m']['start_price'], 99)
        self.assertEqual(prices['eth-5m']['start_price'], 2000)
        for market in self.markets:
            price = prices[market['market_id']]
            self.assertEqual(price['symbol'], market['symbol'])
            self.assertGreater(price['last_spot'], 0)
            self.assertGreater(price['updated_ts'], 0)
        self.assertNotEqual(prices['btc-5m']['last_spot'], prices['eth-5m']['last_spot'])
        self.assert_persisted(decisions)

    async def test_passing_markets_execute_serially_in_selection_order(self):
        outer = self
        eth_finished = asyncio.Event()
        active = 0

        class Voter:
            async def evaluate(self, symbol, context):
                if symbol == 'BTCUSDT':
                    await eth_finished.wait()
                else:
                    eth_finished.set()
                outer.events.append(('evaluated', symbol))
                return {'p_up': .9, 'voters': [{'name': f'fixture-{i}', 'status': 'ok', 'p_up': .9} for i in range(9)]}

        async def execute(market, side, prob, price):
            nonlocal active
            self.assertEqual(active, 0)
            active += 1
            self.events.append(('execute', market['market_id']))
            await asyncio.sleep(0)
            active -= 1
            return {'ok': True, 'reason': 'placed'}

        decisions = await self.tick(Voter(), execute)
        self.assertEqual(self.events, [('evaluated', 'ETHUSDT'), ('evaluated', 'BTCUSDT'),
                                      ('refresh', 'btc-topic'), ('execute', 'btc-5m'),
                                      ('refresh', 'eth-topic'), ('execute', 'eth-5m')])
        self.assertEqual([d['action'] for d in decisions], ['TRADE', 'TRADE'])
        self.assert_persisted(decisions)

    async def test_batch_retains_only_ten_completed_decisions(self):
        self.markets = [dict(self.markets[0], market_id=f'batch-{i}',
                             start_time=self.markets[0]['start_time']-i) for i in range(12)]

        class Voter:
            async def evaluate(self, symbol, context):
                return {'p_up': None, 'voters': {}}

        decisions = await self.tick(Voter())
        self.assertEqual(len(decisions), 12)
        self.assert_persisted(decisions[-10:])


class EntryTimingTests(unittest.IsolatedAsyncioTestCase):
    duration = 300

    async def asyncSetUp(self):
        from engine.runner import Engine
        from unittest.mock import AsyncMock
        self.directory = tempfile.TemporaryDirectory(prefix='prediction-timing-')
        self.addCleanup(self.directory.cleanup)
        self.store = Store(Path(self.directory.name) / 'test.sqlite3')
        self.addCleanup(self.store.close)
        self.store.update_settings({'engine_enabled': True, 'min_agree_votes': 0, 'min_duration': self.duration,
                                    'max_duration': self.duration})
        self.start = 1800000900.0
        self.now = self.start - 90
        self.details = {s: five_minute_fixture(s) for s in ('BTCUSDT', 'ETHUSDT')}
        for detail in self.details.values():
            detail['variants'][0].update(startTime=self.start, endTime=self.start + self.duration)
        self.markets = [normalize_markets(d, now=self.start)[0] for d in self.details.values()]
        self.calls = []
        self.placed = []
        self.scans = []
        self.reveal_after = 5
        self.p_up = .9
        self.evaluation_latency = 0
        self.fail_evaluation = False
        outer = self

        class Voter:
            async def evaluate(self, symbol, context):
                outer.calls.append((symbol, outer.now, dict(context)))
                if outer.fail_evaluation:
                    raise RuntimeError('voter unavailable')
                outer.now += outer.evaluation_latency
                return {'p_up': outer.p_up, 'voters': [{'name': symbol, 'status': 'ok', 'p_up': outer.p_up}]}

        class Client:
            async def market_detail(self, topic_id):
                outer.assertGreaterEqual(outer.now, outer.start)
                return next(d for d in outer.details.values() if d['marketTopicId'] == topic_id)

        self.engine = Engine(self.store, Client(), ensemble=Voter())
        execute = self.engine.executor.execute

        async def tracked_execute(market, side, prob, price):
            outer.placed.append((market['market_id'], outer.now, price))
            return await execute(market, side, prob, price)

        self.engine.executor.execute = tracked_execute

        async def discover(*args):
            outer.scans.append(outer.now)
            return outer.markets if outer.now >= outer.start + outer.reveal_after else []

        self.selector = AsyncMock(side_effect=discover)
        self.clock_patch = patch('engine.runner.time.time', side_effect=lambda: self.now)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.selector_patch = patch('engine.runner.select_markets', self.selector)
        self.selector_patch.start()
        self.addCleanup(self.selector_patch.stop)

    async def spin(self):
        for _ in range(4):
            await asyncio.sleep(0)

    async def discover_now(self):
        self.engine.next_discovery = 0
        rows = await self.engine.tick()
        await self.spin()
        return rows + await self.engine.tick()

    async def test_discovered_at_five_seconds_evaluates_immediately_once(self):
        self.now = self.start + 5
        decisions = await self.discover_now()
        self.assertEqual(len(decisions), 2)
        self.assertEqual([(s, ts) for s, ts, _ in self.calls],
                         [('BTCUSDT', self.start + 5), ('ETHUSDT', self.start + 5)])
        self.assertTrue(all(row['ts'] == self.start + 5 for row in decisions))
        self.assertTrue(all(row['action'] == 'TRADE' for row in decisions))
        self.assertTrue(all(context['market_id'] and context['outcomes'] for _, _, context in self.calls))
        self.assertTrue(all(isinstance(json.loads(row['voters_json']), list) for row in decisions))
        self.assertEqual(self.engine.upcoming, {})
        # Repeated discovery cannot repeat a window, even after cooldown expires.
        self.store.execute('DELETE FROM paper_orders')
        self.store.execute('UPDATE decisions SET ts=?', (self.now - 1000,))
        self.now += 10
        self.assertEqual(await self.discover_now(), [])
        self.assertEqual(len(self.calls), 2)
        self.assertEqual(len(self.store.rows('SELECT * FROM decisions')), 2)

    async def test_discovered_at_seventy_seconds_evaluates_immediately(self):
        self.now = self.start + 70
        decisions = await self.engine.tick()
        self.assertEqual(len(decisions), 2)
        self.assertTrue(all(ts == self.start + 70 for _, ts, _ in self.calls))
        self.assertTrue(all(row['action'] == 'TRADE' for row in decisions))
        self.assertEqual(await self.discover_now(), [])
        self.assertEqual(len(self.calls), 2)

    async def test_less_than_sixty_seconds_remaining_skips_evaluation(self):
        self.now = self.start + self.duration - 59.999
        self.assertEqual(await self.discover_now(), [])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.placed, [])
        self.assertEqual(self.engine.upcoming, {})
        self.assertEqual(self.store.rows('SELECT * FROM decisions'), [])

    async def test_exactly_sixty_seconds_remaining_still_evaluates(self):
        self.now = self.start + self.duration - 60
        decisions = await self.engine.tick()
        self.assertEqual(len(decisions), 2)
        self.assertEqual(len(self.calls), 2)
        # The existing placement freshness gate remains stricter than evaluation.
        self.assertTrue(all('允许交易时间' in row['reason'] for row in decisions))
        self.assertEqual(self.placed, [])

    async def test_future_discovery_waits_only_until_opening(self):
        self.reveal_after = -90
        self.now = self.start - 30
        self.assertEqual(await self.engine.tick(), [])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.store.rows('SELECT * FROM decisions'), [])
        self.now = self.start
        self.assertEqual(len(await self.engine.tick()), 2)
        self.assertTrue(all(ts == self.start for _, ts, _ in self.calls))

    async def test_loop_wakes_at_opening_with_distant_discovery(self):
        self.reveal_after = -90
        self.now = self.start - 30
        await self.discover_now()
        self.now = self.start - 1
        self.engine.next_discovery = self.start + 20
        original_sleep = asyncio.sleep
        delays = []

        async def fake_sleep(delay):
            await original_sleep(0)
            if delay:
                if self.calls:
                    raise asyncio.CancelledError
                delays.append(delay)
                self.now += delay
                self.assertLessEqual(self.now, self.start)
            await original_sleep(0)

        with patch('engine.runner.asyncio.sleep', fake_sleep):
            with self.assertRaises(asyncio.CancelledError):
                await self.engine.run()
        self.assertAlmostEqual(sum(delays), 1)
        self.assertTrue(all(0 < delay <= 1 for delay in delays))
        self.assertTrue(all(ts == self.start for _, ts, _ in self.calls))
        self.assertEqual(len(self.calls), 2)

    async def test_slow_discovery_does_not_delay_evaluation_at_opening(self):
        self.reveal_after = -90
        self.now = self.start - 30
        await self.discover_now()
        never = asyncio.Event()

        async def slow_discovery(*args):
            await never.wait()

        self.selector.side_effect = slow_discovery
        self.now = self.start
        self.engine.next_discovery = 0
        try:
            decisions = await self.engine.tick()
            self.assertEqual(len(decisions), 2)
            self.assertTrue(all(ts == self.start for _, ts, _ in self.calls))
        finally:
            self.engine.discovery_task.cancel()
            await asyncio.gather(self.engine.discovery_task, return_exceptions=True)

    async def test_loop_does_not_round_up_a_subsecond_opening_deadline(self):
        self.reveal_after = -90
        self.now = self.start - 30
        await self.discover_now()
        self.now = self.start - .1
        self.engine.next_discovery = self.start + 20
        self.assertAlmostEqual(self.engine._delay(), .1, places=6)

    async def test_failed_evaluation_is_not_retried(self):
        self.fail_evaluation = True
        self.now = self.start + 5
        decisions = await self.engine.tick()
        self.assertTrue(all('voter unavailable' in row['reason'] for row in decisions))
        self.store.execute('UPDATE decisions SET ts=?', (self.now - 1000,))
        self.assertEqual(await self.discover_now(), [])
        self.assertEqual(len(self.calls), 2)

    async def test_down_evaluation_binds_to_fresh_down_price(self):
        self.p_up = .1
        self.now = self.start + 5
        self.details['BTCUSDT']['variants'][0]['outcomes'][1]['price'] = '.65'
        decisions = await self.engine.tick()
        self.assertEqual(decisions[0]['side'], 'DOWN')
        self.assertAlmostEqual(decisions[0]['model_prob'], .9)
        self.assertAlmostEqual(decisions[0]['market_price'], .65)
        self.assertAlmostEqual(decisions[0]['edge'], .25)

    async def test_fresh_price_and_edge_gate_apply_on_immediate_evaluation(self):
        self.now = self.start + 5
        self.details['BTCUSDT']['variants'][0]['outcomes'][0]['price'] = '.89'
        decisions = await self.engine.tick()
        self.assertEqual(decisions[0]['action'], 'SKIP')
        self.assertAlmostEqual(decisions[0]['market_price'], .89)
        self.assertAlmostEqual(decisions[0]['edge'], .01)
        self.assertIn('优势', decisions[0]['reason'])
        self.assertEqual(decisions[1]['action'], 'TRADE')

    async def test_slow_evaluation_cannot_place_past_freshness_cutoff(self):
        self.now = self.start + 70
        self.evaluation_latency = self.duration * .75
        decisions = await self.engine.tick()
        self.assertTrue(all('允许交易时间' in row['reason'] for row in decisions))
        self.assertEqual(self.placed, [])

    async def test_discovery_burst_is_bounded_when_markets_never_appear(self):
        self.reveal_after = 1000
        self.now = self.start - 2
        self.assertEqual(self.engine._burst_start(self.store.settings(), self.now), self.start)
        self.now = self.start + 30
        self.assertEqual(self.engine._burst_start(self.store.settings(), self.now), self.start)
        self.now += .1
        self.assertIsNone(self.engine._burst_start(self.store.settings(), self.now))

    async def test_discovery_keeps_open_and_only_next_window_per_symbol(self):
        import copy
        details = []
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            for offset in (-60, 30, 60, 91):
                detail = copy.deepcopy(self.details[symbol])
                detail['marketTopicId'] = f'{symbol}-{offset}'
                detail['variants'][0].update(marketId=f'{symbol}-{offset}',
                    startTime=self.now + offset, endTime=self.now + offset + 300)
                details.append(detail)
        class Client:
            async def market_list(self, **kwargs):
                return {'marketTopics': details, 'hasMore': False}
            async def market_detail(self, topic_id):
                return next(d for d in details if d['marketTopicId'] == topic_id)
        selected = await select_markets(Client(), {'min_duration': 300, 'max_duration': 300})
        self.assertEqual({m['market_id'] for m in selected},
                         {f'{s}-{offset}' for s in ('BTCUSDT', 'ETHUSDT') for offset in (-60, 30)})
        self.assertEqual(sum(m['pending'] for m in selected), 2)
        self.assertEqual(await select_markets(Client(), {'max_duration': 299}), [])


class FifteenMinuteEntryTimingTests(EntryTimingTests):
    duration = 900


def deny_network(*args, **kwargs):
    raise AssertionError('Offline smoke suite forbids sockets and DNS')


if __name__ == '__main__':
    with patch.object(socket.socket, 'connect', deny_network), \
         patch.object(socket.socket, 'connect_ex', deny_network), \
         patch.object(socket, 'getaddrinfo', deny_network):
        unittest.main(verbosity=2)
