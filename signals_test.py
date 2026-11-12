"""Offline ensemble/selector regression tests; run: python signals_test.py."""
import asyncio
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from app.db import Store
from engine.markets import normalize_markets, select_markets
from engine.signals import CPA_MODELS, VOTER_NAMES, Ensemble, aggregate, strict_vote, voter_weights


def candles():
    return [[0, 0, 0, 0, 100 + i * .01, 100 + i, 1] for i in range(120)]


class Spot:
    async def klines(self, symbol, **kwargs):
        return candles()


class MathTests(unittest.TestCase):
    def test_named_assets_share_configured_market_window(self):
        now = 1800000000
        for seconds in (300, 900):
            for symbol, titles in (('BTCUSDT', ('Bitcoin Up or Down', 'BTC Up or Down')),
                                   ('ETHUSDT', ('Ethereum Up or Down', 'ETH Up or Down'))):
                for title in titles:
                    with self.subTest(title=title, seconds=seconds):
                        detail = {'marketTopicId': title, 'title': title, 'variants': [
                            {'marketId': title, 'status': 'OPEN', 'startTime': now - 60,
                             'endTime': now - 60 + seconds, 'outcomes': [
                                 {'name': 'UP', 'price': '.4', 'tokenId': 'up'},
                                 {'name': 'DOWN', 'price': '.6', 'tokenId': 'down'}]}]}
                        settings = {'symbols': [symbol], 'min_duration': seconds, 'max_duration': seconds}
                        self.assertEqual(normalize_markets(detail, now, settings)[0]['symbol'], symbol)
                        self.assertEqual(len(normalize_markets(detail, now - 60 + seconds * .75 - 1, settings)), 1)
                        self.assertEqual(normalize_markets(detail, now - 60 + seconds * .75, settings), [])
                        self.assertTrue(normalize_markets(detail, now - 61, settings)[0]['pending'])
                        self.assertEqual(normalize_markets(detail, now - 151, settings), [])
                        other = 'BTCUSDT' if symbol == 'ETHUSDT' else 'ETHUSDT'
                        self.assertEqual(normalize_markets(detail, now, dict(settings, symbols=[other])), [])
                        # An explicit unsupported feed must never fall back to title matching.
                        self.assertEqual(normalize_markets(dict(detail, symbol='SOLUSDT'), now, settings), [])
                        self.assertEqual(normalize_markets(dict(detail, title='Bitcoin Ethereum Up or Down'), now, settings), [])
                        self.assertEqual(normalize_markets(dict(detail, title='Something Up or Down'), now, settings), [])

    def test_dynamic_weight_formula_and_weighted_direction(self):
        stats={'new':{'scored':29,'correct':0},'floor':{'scored':30,'correct':0},
               'ceiling':{'scored':30,'correct':30},'middle':{'scored':40,'correct':24}}
        weights=voter_weights(stats)
        self.assertEqual(weights['new'],1.0)
        self.assertEqual(weights['floor'],.3)
        self.assertEqual(weights['ceiling'],1.5)
        self.assertEqual(weights['middle'],1.2)
        self.assertEqual(weights['Google搜索新闻'],1.0)
        voters=[{'name':'ceiling','status':'ok','p_up':.9},
                {'name':'floor','status':'ok','p_up':.05}]
        self.assertLess(aggregate(voters),.5)
        self.assertGreater(aggregate(voters,weights),.5)
        self.assertAlmostEqual(aggregate(voters,weights),(.9*1.5+.05*.3)/1.8)

    def test_voter_mean_and_abstentions(self):
        voters=[{'name': str(i), 'status': 'ok', 'p_up': p}
                for i,p in enumerate([.8]*6+[.6]*5+[.4])]
        self.assertAlmostEqual(aggregate(voters), (6*.8+5*.6+.4)/12)
        voters[-1].update(status='abstain',p_up=None)
        self.assertAlmostEqual(aggregate(voters), (6*.8+5*.6)/11)
        self.assertIsNone(aggregate([]))

    def test_current_roster_and_retired_voter_weights(self):
        expected = ('Google搜索新闻', 'gpt-6-astra', 'gpt-6.1-sol', 'deepseek-v4.1-flash')
        self.assertEqual(CPA_MODELS, ('gpt-6-astra', 'gpt-6.1-sol'))
        self.assertEqual(VOTER_NAMES, expected)
        # Ignore retired entries before reading counts, including stale bad data.
        stats = {'gemini-3.8-flash': None, 'gemini-4-argon-high': {'scored': 'old'},
                 'EMA趋势': None, 'RSI(14)': {'scored': 'old'}, 'MACD': None,
                 '一小时动量': None, '布林带位置': None, '成交量趋势': None,
                 'Google搜索新闻': {'scored': 30, 'correct': 18}}
        weights = voter_weights(stats)
        self.assertEqual(set(weights), set(expected))
        self.assertEqual(weights['Google搜索新闻'], 1.2)
        self.assertEqual(voter_weights(stats, names=tuple(stats)), {'Google搜索新闻': 1.2})

    def test_strict_json_rejects_invalid_probabilities_and_extra_fields(self):
        for payload in ('{"p_up":NaN,"reason":"x"}', '{"p_up":true,"reason":"x"}',
                        '{"p_up":1.1,"reason":"x"}', '{"p_up":0.5,"reason":""}',
                        '{"p_up":0.5,"reason":"x","extra":1}', '```json\n{}\n```'):
            with self.subTest(payload=payload), self.assertRaises((ValueError, TypeError)):
                strict_vote(payload)


class GroundedResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_context_reaches_voters_and_records_abstention(self):
        ensemble = Ensemble(Spot())
        prompts = []
        async def openai(base, key, model, prompt):
            prompts.append(prompt)
            # An individual voter may abstain while the other signals continue.
            return strict_vote('{"p_up":null,"reason":"required market input unavailable"}')
        context = {'symbol':'BTCUSDT', 'start_time':1800000300, 'end_time':1800000600,
                   'market_id':'live-market', 'start_price':123456789,
                   'outcomes':[{'tokenId':'live-token', 'price':.7}], 'raw':{'secret':'DO_NOT_USE'}}
        with patch.dict(os.environ, {'CPA_BASE_URL':'https://offline.invalid', 'CPA_API_KEY':'test',
                                    'WB_BASE_URL':'', 'VERTEX_SA_JSON':''}, clear=True), \
             patch.object(ensemble, '_openai', side_effect=openai):
            result = await ensemble.evaluate('BTCUSDT', context)
        self.assertEqual(len(prompts), 2)
        self.assertTrue(all('DO_NOT_USE' not in p for p in prompts))
        self.assertTrue(all('live-market' in p and '123456789' in p and
                            'live-token' in p and 'latest_spot_close' in p for p in prompts))
        required = [v for v in result['voters'] if v['name'] in CPA_MODELS]
        self.assertTrue(all(v['status']=='abstain' and v['p_up'] is None and
                            v['reason']=='required market input unavailable' for v in required))
        self.assertIsNone(result['p_up'])  # All four voters abstained.

    async def test_evaluation_persists_market_prices_and_preserves_start(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'test.db')
            self.addCleanup(store.close)
            store.set_meta('market_prices', {'other': {'start_price': 10}})
            ensemble = Ensemble(Spot(), store=store)
            context = {'market_id': 'btc', 'symbol': 'BTCUSDT', 'start_price': 100}
            with patch.dict(os.environ, {'CPA_BASE_URL': '', 'WB_BASE_URL': '', 'VERTEX_SA_JSON': ''}):
                await ensemble.evaluate('BTCUSDT', context)
                first = store.meta('market_prices')['btc']
                self.assertEqual(first['start_price'], 100)
                self.assertEqual(first['last_spot'], candles()[-1][4])
                self.assertEqual(first['symbol'], 'BTCUSDT')
                self.assertGreater(first['updated_ts'], 0)
                context['start_price'] = 999
                await ensemble.evaluate('BTCUSDT', context)
            self.assertEqual(store.meta('market_prices')['btc']['start_price'], 100)
            self.assertEqual(store.meta('market_prices')['other'], {'start_price': 10})


    async def test_market_symbol_controls_all_voters_and_news_cache(self):
        spot = Spot()
        spot.klines = AsyncMock(return_value=candles())
        ensemble = Ensemble(spot)
        prompts = []

        async def vertex(prompt, grounded=True):
            symbol = 'ETHUSDT' if 'Estimate probability that ETHUSDT' in prompt else 'BTCUSDT'
            prompts.append((symbol, grounded, prompt))
            return {'p_up': .7, 'reason': f'{symbol} news' if grounded else 'model', 'sources': []}

        async def model(base, key, name, prompt):
            return await vertex(prompt, grounded=False)

        with patch.dict(os.environ, {'VERTEX_SA_JSON': '/unused-fixture.json',
                'CPA_BASE_URL': 'http://fixture', 'CPA_API_KEY': 'fixture',
                'WB_BASE_URL': 'http://fixture', 'WB_API_KEY': 'fixture'}, clear=True), \
             patch.object(ensemble, '_vertex', vertex), patch.object(ensemble, '_openai', model):
            for symbol in ('BTCUSDT', 'ETHUSDT', 'ETHUSDT'):
                # A stale caller default must not override the actual market.
                result = await ensemble.evaluate('BTCUSDT', {'symbol': symbol})
                self.assertEqual(result['briefing'], f'{symbol} news')
                self.assertEqual(spot.klines.call_args.args, (symbol,))
                self.assertTrue(all(v['status'] == 'ok' for v in result['voters']))
        self.assertEqual(set(ensemble.news_cache), {'BTCUSDT', 'ETHUSDT'})
        self.assertEqual([(s, g) for s, g, _ in prompts if g], [('BTCUSDT', True), ('ETHUSDT', True)])
        for symbol, grounded, prompt in prompts:
            if not grounded:
                self.assertIn(f'News briefing: {symbol} news', prompt)
                self.assertNotIn('technical_votes', prompt)
                self.assertNotIn('BTCUSDT' if symbol == 'ETHUSDT' else 'ETHUSDT', prompt)

    async def evaluate_fixture(self, candidate, store=None, calls=None, prompt=None):
        grounded_calls = calls if calls is not None else []

        def deny_network(*args, **kwargs):
            raise AssertionError('Grounded response tests forbid sockets and DNS')

        def handler(request):
            body = json.loads(request.content)
            if 'tools' in body:
                grounded_calls.append(body)
                self.assertNotIn('responseMimeType', body['generationConfig'])
                current = candidate[min(len(grounded_calls) - 1, len(candidate) - 1)] if isinstance(candidate, list) else candidate
                # A later candidate must never supply a missing/invalid vote.
                return httpx.Response(200, json={'candidates': [current, {
                    'content': {'parts': [{'text': '{"p_up":0.99,"reason":"wrong candidate"}'}]}}]})
            return httpx.Response(200, json={'candidates': [{
                'content': {'parts': [{'text': '{"p_up":0.5,"reason":"ordinary vote"}'}]}}]})

        with patch.dict(os.environ, {'VERTEX_SA_JSON': '/unused-fixture.json'}, clear=True), \
             patch.object(socket.socket, 'connect', deny_network), \
             patch.object(socket.socket, 'connect_ex', deny_network), \
             patch.object(socket, 'getaddrinfo', deny_network):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                ensemble = Ensemble(Spot(), http_client=http, store=store)
                ensemble._credentials = AsyncMock(return_value={'project_id': 'fixture'})
                ensemble._access_token = AsyncMock(return_value='fixture-token')
                if prompt is not None:
                    ensemble._prompt = lambda *args: prompt
                return await ensemble.evaluate('BTCUSDT', {})

    @staticmethod
    def candidate(parts):
        return {'content': {'parts': parts}, 'finishReason': 'STOP',
                'groundingMetadata': {'groundingChunks': [
                    {'web': {'uri': 'https://fixture.invalid/news', 'title': 'Fixture news'}}]}}

    async def test_grounded_response_text_formats_and_briefing(self):
        reason = '2026-11-09: news {uncertain}, "quoted" and \\escaped'
        raw = json.dumps({'p_up': .61, 'reason': reason})
        fixtures = {
            'pure JSON': [{'text': raw}],
            'fenced JSON': [{'text': f'```json\n{raw}\n```'}],
            'prose and JSON': [{'text': f'Fresh news follows:\n{raw}\nSources above.'}],
            'prose and fenced JSON': [{'text': f'News:\n```json\n{raw}\n```\nDone.'}],
            'JSON in second part': [{'text': ''}, {'text': raw}],
            'split JSON with other parts': [
                {'text': '{"p_up":0.99}', 'thought': True}, {'text': None},
                {'functionCall': {'name': 'search'}}, {'text': raw[:12]}, {'text': raw[12:]}],
        }
        for label, parts in fixtures.items():
            with self.subTest(label=label):
                candidate = self.candidate(parts)
                result = await self.evaluate_fixture(candidate)
                news = result['voters'][0]
                self.assertEqual(news, {'name': 'Google搜索新闻', 'group': 'news',
                    'status': 'ok', 'p_up': .61, 'reason': reason,
                    'sources': candidate['groundingMetadata']['groundingChunks']})
                self.assertEqual(result['briefing'], reason)

    async def test_search_proof_shapes(self):
        chunks = [{'web': {'uri': 'https://fixture.invalid/news'}}]
        supports = [{'segment': {'text': 'Latest news'}}]
        fixtures = [
            ({'groundingChunks': chunks}, chunks),
            ({'webSearchQueries': ['BTC latest news']}, [{'query': 'BTC latest news'}]),
            ({'groundingSupports': supports}, [{'support': supports[0]}]),
            ({'searchEntryPoint': {'renderedContent': '<div>Search results</div>'}},
             [{'renderedContent': '<div>Search results</div>'}]),
            ({'groundingChunks': chunks, 'webSearchQueries': ['BTC news']}, chunks),
            ({'groundingChunks': [], 'webSearchQueries': ['BTC news']}, [{'query': 'BTC news'}]),
        ]
        for metadata, sources in fixtures:
            with self.subTest(metadata=metadata):
                candidate = self.candidate([{'text': '{"p_up":0.53,"reason":"news"}'}])
                candidate['groundingMetadata'] = metadata
                calls = []
                news = (await self.evaluate_fixture(candidate, calls=calls))['voters'][0]
                self.assertEqual(news['status'], 'ok')
                self.assertEqual(news['sources'], sources)
                self.assertEqual(len(calls), 1)

    async def test_missing_proof_retries_before_json_parsing(self):
        sentence = 'Use Google Search to check the latest news before answering.'
        for raw in ('{"p_up":0.1,"reason":"first vote"}', 'invalid JSON'):
            for prompt in ('fixture prompt', 'fixture prompt\n' + sentence):
                with self.subTest(raw=raw, prompt=prompt):
                    first = {'content': {'parts': [{'text': raw}]}}
                    retry = self.candidate([{'text': '{"p_up":0.73,"reason":"retry vote"}'}])
                    calls = []
                    result = await self.evaluate_fixture([first, retry], calls=calls, prompt=prompt)
                    news = result['voters'][0]
                    self.assertEqual(news['status'], 'ok')
                    self.assertEqual(news['p_up'], .73)
                    self.assertEqual(result['briefing'], 'retry vote')
                    self.assertEqual(len(calls), 2)
                    expected = json.loads(json.dumps(calls[0]))
                    expected['contents'][0]['parts'][0]['text'] = prompt if sentence in prompt else prompt + '\n' + sentence
                    self.assertEqual(calls[1], expected)
                    self.assertEqual(calls[1]['contents'][0]['parts'][0]['text'].count(sentence), 1)

    async def test_both_missing_proof_abstain_and_record_both_attempts(self):
        for raw in ('{"p_up":0.53,"reason":"news"}', 'invalid JSON'):
            with self.subTest(raw=raw), tempfile.TemporaryDirectory() as directory:
                store = Store(Path(directory) / 'test.db')
                first = {'content': {'parts': [{'text': raw}]}, 'groundingMetadata': {}}
                retry = {'content': {'parts': [{'text': 'retry: ' + raw}]}, 'groundingMetadata': {
                    'groundingChunks': [], 'webSearchQueries': [], 'groundingSupports': [],
                    'searchEntryPoint': {'renderedContent': ''}}}
                calls = []
                news = (await self.evaluate_fixture([first, retry], store, calls))['voters'][0]
                self.assertEqual(news['status'], 'abstain')
                self.assertIsNone(news['p_up'])
                self.assertEqual(news['reason'], '新闻投票两次未返回搜索来源')
                self.assertEqual(len(calls), 2)
                records = store.meta('news_debug')
                self.assertEqual(len(records), 1)
                record = records[0]
                self.assertEqual(record['error_str'], news['reason'])
                self.assertEqual([a['attempt'] for a in record['attempts']], [1, 2])
                for attempt, candidate in zip(record['attempts'], (first, retry)):
                    self.assertFalse(attempt['has_search_proof'])
                    response = json.loads(attempt['raw_response_truncated_2000_chars'])
                    self.assertEqual(response['candidates'][0], candidate)
                    self.assertEqual(attempt['extracted_text_truncated_500'],
                                     candidate['content']['parts'][0]['text'])

    async def test_empty_or_invalid_grounded_responses_abstain(self):
        invalid = ('', 'No forecast available', '{"p_up":0.6',
                   '{"p_up":true,"reason":"x"}',
                   '{"p_up":NaN,"reason":"x"}', '{"p_up":Infinity,"reason":"x"}',
                   '{"p_up":0.6,"reason":""}',
                   '{"p_up":0.6,"reason":{"nested":"invalid"}}',
                   'Prose {invalid} then {"p_up":0.6,"reason":"must not skip"}')
        candidates = [self.candidate([{'text': raw}]) for raw in invalid]
        candidates.extend([self.candidate([]), self.candidate([{'text': None}]),
                           {'finishReason': 'SAFETY'},
                           {'content': {'parts': [{'text': '{"p_up":0.6,"reason":"no sources"}'}]}}])
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                result = await self.evaluate_fixture(candidate)
                news = result['voters'][0]
                self.assertEqual(news['status'], 'abstain')
                self.assertIsNone(news['p_up'])
                if candidate.get('groundingMetadata'):
                    self.assertIn('投票失败：', news['reason'])
                else:
                    self.assertEqual(news['reason'], '新闻投票两次未返回搜索来源')
                self.assertNotIn('sources', news)
                self.assertEqual(result['briefing'], '新闻来源暂不可用；不得假设存在实时新闻。')

    async def test_news_probability_aliases_strings_and_clamping(self):
        for key in ('probability', 'p_up', 'prob', 'up_probability', 'upProbability'):
            for value, expected in (('0.61', .61), (.61, .61), (1.4, .99), (-.2, .01)):
                with self.subTest(key=key, value=value):
                    raw = json.dumps({key: value, 'reason': 'fixture news', 'extra': 'allowed'})
                    result = await self.evaluate_fixture(self.candidate([{'text': raw}]))
                    news = result['voters'][0]
                    self.assertEqual(news['status'], 'ok')
                    self.assertEqual(news['p_up'], expected)

    async def test_missing_probability_has_distinct_reason_and_debug(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'test.db')
            raw = '{"reason":"missing probability"}'
            result = await self.evaluate_fixture(self.candidate([{'text': raw}]), store)
            news = result['voters'][0]
            self.assertEqual(news['status'], 'abstain')
            self.assertIsNone(news['p_up'])
            self.assertEqual(news['reason'], '新闻响应缺少概率字段')
            record = store.meta('news_debug')[0]
            self.assertEqual(record['error_type'], 'MissingNewsProbability')
            self.assertEqual(record['error_str'], '新闻响应缺少概率字段')
            self.assertEqual(record['extracted_text_truncated_500'], raw)

    async def test_non_json_debug_is_truncated_and_keeps_last_five(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'test.db')
            for index in range(7):
                raw = f'No forecast {index}: ' + '新闻' * 1200
                candidate = self.candidate([{'text': raw}])
                result = await self.evaluate_fixture(candidate, store)
                news = result['voters'][0]
                self.assertEqual(news['status'], 'abstain')
                self.assertEqual(news['reason'], f'投票失败：ValueError：{raw[:60]}')
                records = store.meta('news_debug')
                self.assertEqual(len(records), min(index + 1, 5))
                record = records[-1]
                self.assertEqual(set(record), {'ts', 'error_type', 'error_str',
                    'raw_response_truncated_2000_chars', 'extracted_text_truncated_500'})
                self.assertIsInstance(record['ts'], (int, float))
                self.assertEqual(record['error_type'], 'ValueError')
                self.assertEqual(record['error_str'], '新闻回复缺少 JSON 对象')
                response = {'candidates': [candidate, {'content': {'parts': [
                    {'text': '{"p_up":0.99,"reason":"wrong candidate"}'}]}}]}
                self.assertEqual(record['raw_response_truncated_2000_chars'],
                                 json.dumps(response, ensure_ascii=False)[:2000])
                self.assertEqual(record['extracted_text_truncated_500'], raw[:500])
            self.assertTrue(records[0]['extracted_text_truncated_500'].startswith('No forecast 2:'))

    async def test_unexpected_shapes_and_validation_failures_write_debug(self):
        fixtures = [([], 'ValueError'), (None, 'ValueError'),
                    ({'probability': 'invalid', 'reason': 'x'}, 'ValueError'),
                    ({'probability': [], 'reason': 'x'}, 'ValueError'),
                    ({'probability': '.6', 'reason': {}}, 'ValueError')]
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'test.db')
            candidates = [(self.candidate([{'text': json.dumps(value)}]), json.dumps(value), error)
                          for value, error in fixtures]
            candidates.extend([(None, '', 'NewsResponseError'),
                               ({'content': {'parts': [None]}}, '', 'NewsResponseError'),
                               (self.candidate([{'text': '{"p_up":0.6'}]), '{"p_up":0.6', 'JSONDecodeError'),
                               (self.candidate([{'text': '{"p_up":"NaN","reason":"x"}'}]),
                                '{"p_up":"NaN","reason":"x"}', 'ValueError'),
                               ({'content': {'parts': [{'text': '{}'}]}}, '', 'NewsResponseError'),
                               ({'content': {'parts': [{'text': '{"p_up":0.6,"reason":"x"}'}]}},
                                '', 'NewsResponseError')])
            for candidate, raw, error in candidates:
                with self.subTest(candidate=candidate):
                    result = await self.evaluate_fixture(candidate, store)
                    news = result['voters'][0]
                    self.assertEqual(news['status'], 'abstain')
                    if error == 'NewsResponseError':
                        self.assertEqual(news['reason'], '新闻投票两次未返回搜索来源')
                    elif error != 'MissingNewsProbability':
                        self.assertEqual(news['reason'], f'投票失败：{error}：{raw[:60]}')
                    self.assertEqual(store.meta('news_debug')[-1]['error_type'], error)


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_engine_threads_store_to_default_ensemble(self):
        from engine.runner import Engine
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'test.db')
            self.assertIs(Engine(store, Spot()).ensemble.store, store)
            custom = Ensemble(Spot())
            self.assertIs(Engine(store, Spot(), ensemble=custom).ensemble, custom)

    async def test_ensemble_loads_current_weights_each_evaluation(self):
        with tempfile.TemporaryDirectory() as directory:
            store=Store(Path(directory)/'weights.db')
            self.addCleanup(store.close)
            ensemble=Ensemble(Spot(),store=store)
            vote={'name':'Google搜索新闻','status':'ok','p_up':.9,'reason':'fixture'}
            model={'p_up':.1,'reason':'fixture'}
            env={'CPA_BASE_URL':'http://fixture','CPA_API_KEY':'fixture'}
            with patch.dict(os.environ,env,clear=True), \
                 patch.object(ensemble,'_news',AsyncMock(return_value=vote)), \
                 patch.object(ensemble,'_openai',AsyncMock(return_value=model)):
                first=await ensemble.evaluate('BTCUSDT',{})
                self.assertAlmostEqual(first['p_up'],(.9+.1*2)/3)
                store.set_meta('voter_stats',{'Google搜索新闻':{'scored':30,'correct':30},
                    **{name:{'scored':30,'correct':0} for name in CPA_MODELS}})
                second=await ensemble.evaluate('BTCUSDT',{})
                self.assertAlmostEqual(second['p_up'],(.9*1.5+.1*.3*2)/2.1)

    async def test_missing_voters_and_no_successful_votes(self):
        with patch.dict(os.environ, {}, clear=True):
            result = await Ensemble(Spot()).evaluate('BTCUSDT', {})
            self.assertEqual(len(result['voters']), 4)
            self.assertEqual(sum(v['status'] == 'ok' for v in result['voters']), 0)
            self.assertIsNone(result['p_up'])
            class BrokenSpot:
                async def klines(self, *args, **kwargs):
                    raise RuntimeError('offline')
            result = await Ensemble(BrokenSpot()).evaluate('BTCUSDT', {})
            self.assertIsNone(result['p_up'])
            self.assertTrue(all(v['status'] == 'abstain' for v in result['voters']))

    async def test_news_first_cached_briefing_injected_in_every_llm(self):
        events = []
        class FixtureEnsemble(Ensemble):
            async def _vertex(self, prompt):
                events.append('news_started')
                await asyncio.sleep(0)
                events.append('news_finished')
                return {'p_up': .65, 'reason': '2026年11月9日：已核实新闻摘要', 'sources': [{'web': {'uri': 'https://fixture.invalid'}}]}
            async def _openai(self, base, key, model, prompt):
                self_outer.assertIn('news_finished', events)
                self_outer.assertIn('已核实新闻摘要', prompt)
                events.append(model)
                return {'p_up': .7, 'reason': '综合分析'}
        self_outer = self
        env = {'VERTEX_SA_JSON': '/unused-fixture.json', 'CPA_BASE_URL': 'https://fixture.invalid/v1',
               'CPA_API_KEY': 'test', 'WB_BASE_URL': 'https://fixture.invalid/v1', 'WB_API_KEY': 'test'}
        with patch.dict(os.environ, env, clear=True):
            ensemble = FixtureEnsemble(Spot())
            first = await ensemble.evaluate('BTCUSDT', {'end_time': time.time() + 600})
            second = await ensemble.evaluate('BTCUSDT', {})
            self.assertEqual(events.count('news_started'), 1)
            self.assertEqual(events[0:2], ['news_started', 'news_finished'])
            self.assertEqual(sum(v['status'] == 'ok' for v in first['voters']), 4)
            self.assertEqual({v['name'] for v in first['voters']}, set(VOTER_NAMES))
            self.assertEqual(events[2:], [*CPA_MODELS, 'deepseek-v4.1-flash'] * 2)
            self.assertEqual(first['briefing'], second['briefing'])
            ensemble.news_cache['BTCUSDT'] = (time.monotonic() - 121, ensemble.news_cache['BTCUSDT'][1])
            await ensemble.evaluate('BTCUSDT', {})
            self.assertEqual(events.count('news_started'), 2)

    async def test_custom_services_route_requests_and_reload_each_round(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'services.db')
            self.addCleanup(store.close)
            search = {'provider': 'openai_compatible', 'base_url': 'https://search.invalid/v1',
                      'api_key': 'fixture-search-key', 'model': 'search-fixture'}
            model = {'id': 'cpa_astra', 'name': '自定义投票者', 'base_url': 'https://models.invalid/v1',
                     'api_key': 'fixture-model-key', 'model': 'custom-fixture', 'enabled': True}
            disabled = [{'id': 'cpa_sol', 'enabled': False}, {'id': 'workbuddy', 'enabled': False}]
            requests = []

            def handler(request):
                body = json.loads(request.content)
                requests.append((str(request.url), body, request.headers['authorization']))
                reason = 'dated fixture briefing' if request.url.host == 'search.invalid' else 'model fixture'
                return httpx.Response(200, json={'choices': [{'message': {'content':
                    json.dumps({'p_up': .65, 'reason': reason})}}]})

            with patch.dict(os.environ, {}, clear=True):
                store.update_settings({'search_api': search, 'model_apis': [model, *disabled]})
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                    ensemble = Ensemble(Spot(), http_client=http, store=store)
                    first = await ensemble.evaluate('BTCUSDT', {})
                    self.assertEqual(first['briefing'], 'dated fixture briefing')
                    self.assertEqual([(url, body['model']) for url, body, _ in requests], [
                        ('https://search.invalid/v1/chat/completions', 'search-fixture'),
                        ('https://models.invalid/v1/chat/completions', 'custom-fixture')])
                    self.assertEqual(requests[0][2], 'Bearer fixture-search-key')
                    self.assertEqual(requests[1][2], 'Bearer fixture-model-key')
                    self.assertIn('dated fixture briefing', requests[1][1]['messages'][0]['content'])
                    self.assertTrue(any(v['name'] == '自定义投票者' and v['status'] == 'ok'
                                        for v in first['voters']))
                    self.assertEqual(sum(v['reason'] == '已禁用' for v in first['voters']), 2)
                    await ensemble.evaluate('BTCUSDT', {})
                    self.assertEqual(len(requests), 3)  # Search brief cached, model called again.
                    search = dict(search, model='changed-search')
                    model = dict(model, base_url='https://changed.invalid/v1', model='changed-model')
                    store.update_settings({'search_api': search, 'model_apis': [model, *disabled]})
                    await ensemble.evaluate('BTCUSDT', {})
                    self.assertEqual(requests[-2][1]['model'], 'changed-search')
                    self.assertEqual(requests[-1][0], 'https://changed.invalid/v1/chat/completions')
                    self.assertEqual(requests[-1][1]['model'], 'changed-model')
                    model['enabled'] = False
                    store.update_settings({'model_apis': [model, *disabled]})
                    result = await ensemble.evaluate('BTCUSDT', {})
                    self.assertEqual(len(requests), 5)
                    self.assertTrue(all(v['status'] == 'abstain' for v in result['voters'][1:]))

    async def test_search_disabled_by_settings_or_environment(self):
        for settings, env in [({'search_api': {'enabled': False}}, {'SEARCH_API_ENABLED': 'true'}),
                              ({'search_api': {'enabled': None}}, {'SEARCH_API_ENABLED': 'false'})]:
            with self.subTest(settings=settings, env=env), patch.dict(os.environ, env, clear=True):
                ensemble = Ensemble(Spot())
                with patch.object(ensemble, '_settings', return_value=settings), \
                     patch.object(ensemble, '_vertex', AsyncMock()) as vertex, \
                     patch.object(ensemble, '_openai', AsyncMock()) as model:
                    vote = await ensemble._news('BTCUSDT', {})
                self.assertEqual(vote['status'], 'abstain')
                self.assertEqual(vote['reason'], '已禁用')
                vertex.assert_not_awaited()
                model.assert_not_awaited()

    async def test_blank_service_values_preserve_environment_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(Path(directory) / 'fallback.db')
            self.addCleanup(store.close)
            env = {'VERTEX_SA_JSON': '/unused-fixture.json', 'CPA_BASE_URL': 'https://cpa.invalid/v1',
                   'CPA_API_KEY': 'fixture-cpa', 'WB_BASE_URL': 'https://wb.invalid/v1',
                   'WB_API_KEY': 'fixture-wb'}
            with patch.dict(os.environ, env, clear=True):
                store.update_settings({'search_api': {'provider': 'vertex_grounding', 'base_url': '',
                                       'api_key': '', 'model': ''}, 'model_apis': [
                    {'id': name, 'name': '', 'base_url': '', 'api_key': '', 'model': '', 'enabled': None}
                    for name in ('cpa_astra', 'cpa_sol', 'workbuddy')]})
                ensemble = Ensemble(Spot(), store=store)
                with patch.object(ensemble, '_vertex', AsyncMock(return_value={
                        'p_up': .6, 'reason': 'default Vertex briefing'})) as vertex, \
                     patch.object(ensemble, '_openai', AsyncMock(return_value={
                        'p_up': .6, 'reason': 'default model'})) as model:
                    result = await ensemble.evaluate('BTCUSDT', {})
                vertex.assert_awaited_once()
                self.assertEqual([(call.args[0], call.args[2]) for call in model.await_args_list], [
                    ('https://cpa.invalid/v1', 'gpt-6-astra'),
                    ('https://cpa.invalid/v1', 'gpt-6.1-sol'),
                    ('https://wb.invalid/v1', 'deepseek-v4.1-flash')])
                self.assertEqual({v['name'] for v in result['voters']}, set(VOTER_NAMES))

    async def test_gateway_json_and_failure_abstention(self):
        calls = []
        def handler(request):
            body = json.loads(request.content)
            calls.append(body)
            self.assertEqual(request.url.path, '/v1/chat/completions')
            self.assertEqual(request.headers['authorization'], 'Bearer fixture')
            if body['model'] == 'gpt-6.1-sol':
                return httpx.Response(200, json={'choices': [{'message': {'content': '{"p_up":2,"reason":"invalid"}'}}]})
            return httpx.Response(200, json={'choices': [{'message': {'content': '{"p_up":0.7,"reason":"信号偏多"}'}}]})
        with patch.dict(os.environ, {'CPA_BASE_URL': 'https://fixture.invalid/v1', 'CPA_API_KEY': 'fixture'}, clear=True):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                ensemble = Ensemble(Spot(), http_client=http)
                result = await ensemble.evaluate('BTCUSDT', {})
                self.assertEqual(len(calls), 2)
                self.assertEqual(sum(v['status'] == 'ok' and v['group'] == 'llm' for v in result['voters']), 1)
                self.assertTrue(all(v['response_format'] == {'type': 'json_object'} for v in calls))
                await ensemble.close()
                self.assertFalse(http.is_closed)

    async def test_vertex_oauth_global_endpoint_and_grounding(self):
        # The only subprocess is local OpenSSL. All HTTP is intercepted in memory.
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / 'key.pem'
            subprocess.run(['openssl', 'genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', str(pem)],
                           check=True, timeout=10, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            sa = Path(directory) / 'sa.json'
            sa.write_text(json.dumps({'project_id': 'fixture-project', 'client_email': 'fixture@fixture.iam.gserviceaccount.com', 'private_key': pem.read_text()}))
            oauth_calls, model_calls = [], []
            def handler(request):
                if request.url.host == 'oauth2.googleapis.com':
                    from urllib.parse import parse_qs
                    form = parse_qs(request.content.decode())
                    assertion = form['assertion'][0]
                    self.assertEqual(len(assertion.split('.')), 3)
                    oauth_calls.append(assertion)
                    return httpx.Response(200, json={'access_token': 'fixture-token', 'expires_in': 3600})
                self.assertEqual(request.url.host, 'aiplatform.googleapis.com')
                self.assertIn('/projects/fixture-project/locations/global/', request.url.path)
                self.assertIn('gemini-3.8-flash:generateContent', request.url.path)
                self.assertEqual(request.headers['authorization'], 'Bearer fixture-token')
                body = json.loads(request.content)
                model_calls.append(body)
                candidate = {'content': {'parts': [{'text': '{"p_up":0.61,"reason":"有日期的新闻摘要"}'}]}}
                if 'tools' in body:
                    self.assertEqual(body['tools'], [{'googleSearch': {}}])
                    candidate['groundingMetadata'] = {'groundingChunks': [{'web': {'uri': 'https://fixture.invalid/source'}}]}
                return httpx.Response(200, json={'candidates': [candidate]})
            with patch.dict(os.environ, {'VERTEX_SA_JSON': str(sa)}, clear=True):
                async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
                    ensemble = Ensemble(Spot(), http_client=http)
                    grounded = await ensemble._vertex('fixture news prompt')
                    self.assertTrue(grounded['sources'])
                    self.assertEqual(grounded['p_up'], .61)
                    self.assertEqual(len(oauth_calls), 1)
                    self.assertEqual(len(model_calls), 1)
                    self.assertNotIn('responseMimeType', model_calls[0]['generationConfig'])
                    def ungrounded(request):
                        return httpx.Response(200, json={'candidates': [{'content': {'parts': [{'text': '{"p_up":0.6,"reason":"无来源"}'}]}}]})
                    async with httpx.AsyncClient(transport=httpx.MockTransport(ungrounded)) as other:
                        ensemble.http = other
                        with self.assertRaises(ValueError):
                            await ensemble._vertex('fixture')

    async def test_official_selector_and_quarter_time_cutoff(self):
        # Whole seconds keep the exact 25% boundary stable through milliseconds conversion.
        now = int(time.time())
        detail = {'marketTopicId': 7, 'variantData': {'priceFeedSymbol': 'ETHUSDT', 'type': 'CRYPTO_UP_DOWN', 'startPrice': '100'},
                  'title': 'Ethereum Up or Down', 'startDate': (now - 600) * 1000, 'endDate': (now + 400) * 1000,
                  'markets': [{'marketId': 8, 'title': 'UP', 'status': 'REGISTERED', 'tradingStatus': 'OPEN',
                    'outcomes': [{'name': 'YES', 'price': '.4', 'tokenId': 'y'}, {'name': 'NO', 'price': '.6', 'tokenId': 'n'}]}]}
        class MarketsClient:
            async def market_list(self, **params):
                self_outer.assertNotIn('l2Category', params)
                return {'marketTopics': [{'marketTopicId': 7}], 'hasMore': False}
            async def market_detail(self, topic_id):
                return detail
        self_outer = self
        selected = await select_markets(MarketsClient(), {})
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]['symbol'], 'ETHUSDT')
        self.assertEqual(selected[0]['outcomes'][0]['name'], 'UP')
        self.assertEqual(selected[0]['outcomes'][1]['name'], 'DOWN')
        self.assertEqual(selected[0]['start_price'], '100')
        self.assertEqual(normalize_markets(detail, now=now + 150), [])  # Exactly 25% remains.
        self.assertEqual(len(normalize_markets(detail, now=now + 149)), 1)
        self.assertEqual(normalize_markets(detail, now=now, settings={'max_duration': 900}), [])
        detail['markets'][0]['tradingStatus'] = 'CANCEL_ONLY'
        self.assertEqual(normalize_markets(detail, now=now), [])
        detail['markets'][0]['tradingStatus'] = 'OPEN'
        detail['markets'][0]['title'] = 'Up or Down'
        self.assertEqual(normalize_markets(detail, now=now), [])  # YES proposition ambiguous.


if __name__ == '__main__':
    unittest.main(verbosity=2)
