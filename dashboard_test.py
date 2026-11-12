"""Offline dashboard tests: .venv/bin/python dashboard_test.py."""
import asyncio
from contextlib import asynccontextmanager
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import httpx

from app.binance_client import BinanceClient
from app.db import Store
from app.service_config import BLOCKS, safe_settings, migrate_settings, resolve_models, resolve_search
from app.main import create_app


OPERATOR_IDS = ('parameters', 'toggle', 'logout', 'mode', 'budget', 'budget-progress',
                'engine-state', 'status', 'decisions', 'save', 'save-state')
PUBLIC_HEADINGS = ('盈利总览', '持仓', '订单记录')
OPERATOR_HEADINGS = ('引擎', '最近决策', '系统状态', '交易设置', '外部服务配置')


class Markup(HTMLParser):
    def __init__(self, source):
        super().__init__()
        self.ids = {}
        self.fields = {}
        self.headings = []
        self.heading = False
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if 'id' in attrs:
            if attrs['id'] in self.ids:
                raise AssertionError('Duplicate id: ' + attrs['id'])
            self.ids[attrs['id']] = (tag, attrs)
        if tag in ('select', 'input'):
            self.fields[attrs['name']] = attrs
        if tag in ('h1', 'h2'):
            self.heading = True

    def handle_endtag(self, tag):
        if tag in ('h1', 'h2'):
            self.heading = False

    def handle_data(self, data):
        if self.heading:
            self.headings.append(data.strip())


class DashboardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / 'test.db')
        self.addCleanup(self.store.conn.close)
        self.source = (Path(__file__).parent / 'static/index.html').read_text()

    @asynccontextmanager
    async def web(self, public, base_url='https://dashboard'):
        def unexpected_request(request):
            raise AssertionError('Dashboard tests must not call external APIs')

        client = BinanceClient('', '', http=httpx.AsyncClient(transport=httpx.MockTransport(unexpected_request)))
        with patch.dict(os.environ, {'PUBLIC_READONLY': public}):
            app = create_app(self.store, client, background=False, password='test')
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=base_url, follow_redirects=True) as web:
                yield web

    async def test_executor_selection_validation_priority_and_guest_isolation(self):
        from app.baw_client import executor_path
        with patch.dict(os.environ, {}, clear=True):
            async with self.web('1') as web:
                await web.post('/login', data={'password': 'test'})
                token = (await web.get('/api/state')).json()['csrf']
                headers = {'X-CSRF-Token': token}
                response = await web.post('/api/settings', json={
                    'executor_mode':'sapi', 'binance_api': {'enabled': True}}, headers=headers)
                self.assertEqual(response.status_code, 400)
                self.assertIn('api_key', response.text)
                self.assertIn('api_secret', response.text)
                self.assertFalse(self.store.rows("SELECT key FROM settings WHERE key='executor_mode'"))
                for service in ({}, {'enabled': None}, {'enabled': False}):
                    with self.subTest(service=service):
                        response = await web.post('/api/settings', json={
                            'executor_mode': 'sapi', 'binance_api': service}, headers=headers)
                        self.assertEqual(response.status_code, 200, response.text)
                        # A persisted executor selection must not block later saves.
                        response = await web.post('/api/settings', json={'bet_usdt': 19}, headers=headers)
                        self.assertEqual(response.status_code, 200, response.text)
                response = await web.post('/api/settings', json={'executor_mode':'baw'}, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(executor_path(self.store.settings()), 'baw')
                with patch.dict(os.environ, {'EXECUTOR_PATH':'sapi'}):
                    self.assertEqual(executor_path(self.store.settings()), 'baw')
                with patch.dict(os.environ, {'BINANCE_API_KEY':'env-key','BINANCE_API_SECRET':'env-secret'}):
                    response = await web.post('/api/settings', json={
                        'executor_mode':'sapi', 'binance_api': {'enabled': True}}, headers=headers)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertNotIn('env-secret', response.text)
                await web.post('/logout', json={}, headers=headers)
                self.assertNotIn('executor_mode', (await web.get('/api/state')).text)
                self.assertNotIn('executor_mode', (await web.get('/')).text)

    async def test_models_proxy_auth_csrf_unsaved_values_errors_and_timeout(self):
        upstream_calls = []
        def handler(request):
            upstream_calls.append(request)
            return httpx.Response(200, json={'data':[{'id':'alpha'}, {'id':'beta'}, {'id':'alpha'}]})
        original = httpx.AsyncClient
        async with self.web('1') as web:
            body = {'base_url':'https://models.invalid/v1/', 'api_key':'unsaved-key'}
            self.assertEqual((await web.post('/api/services/models', json=body)).status_code, 401)
            await web.post('/login', data={'password':'test'})
            headers = {'X-CSRF-Token':(await web.get('/api/state')).json()['csrf']}
            self.assertEqual((await web.post('/api/services/models', json=body)).status_code, 403)
            def factory(**kwargs):
                self.assertEqual(kwargs['timeout'], 15)
                return original(transport=httpx.MockTransport(handler), **kwargs)
            with patch('app.main.httpx.AsyncClient', side_effect=factory):
                response = await web.post('/api/services/models', json=body, headers=headers)
            self.assertEqual(response.json(), {'models':['alpha','beta']})
            self.assertEqual(str(upstream_calls[0].url), 'https://models.invalid/v1/models')
            self.assertEqual(upstream_calls[0].headers['Authorization'], 'Bearer unsaved-key')
            self.assertNotIn('unsaved-key', response.text)
            self.assertNotIn('unsaved-key', json.dumps(self.store.settings()))
            self.store.update_settings({'providers':[dict(id='saved', name='Saved', base_url='https://models.invalid/v1',
                                                         api_key='stored-key', enabled=True, models=[])]})
            with patch('app.main.httpx.AsyncClient', side_effect=factory):
                response = await web.post('/api/services/models', json=dict(body, provider_id='saved', api_key=''), headers=headers)
            self.assertEqual(response.json(), {'models':['alpha','beta']})
            self.assertEqual(upstream_calls[-1].headers['Authorization'], 'Bearer stored-key')
            self.assertNotIn('stored-key', response.text)
            def failure(request):
                return httpx.Response(429, text='quota exceeded unsaved-key')
            with patch('app.main.httpx.AsyncClient', side_effect=lambda **kw: original(transport=httpx.MockTransport(failure), **kw)):
                response = await web.post('/api/services/models', json=body, headers=headers)
            self.assertEqual(response.status_code, 429)
            self.assertIn('quota exceeded', response.text)
            self.assertNotIn('unsaved-key', response.text)
            def timeout(request):
                raise httpx.ReadTimeout('timeout')
            with patch('app.main.httpx.AsyncClient', side_effect=lambda **kw: original(transport=httpx.MockTransport(timeout), **kw)):
                response = await web.post('/api/services/models', json=body, headers=headers)
            self.assertEqual(response.status_code, 504)
            self.assertIn('15s', response.text)

    async def test_wallet_login_proxy_status_and_csrf(self):
        original = httpx.AsyncClient
        calls, phase = [], ['pending']
        def handler(request):
            calls.append((request.method, request.url.path))
            self.assertEqual(request.headers['X-Bridge-Token'], 'env-bridge-token')
            if request.url.path == '/status':
                data = {'walletAddress':'0x123', 'loggedIn':True}
            else:
                data = dict(session_id='offline-session', status=phase[0], text='Open https://wallet.invalid/login', url='https://wallet.invalid/login', address='0x123' if phase[0]=='done' else None)
            return httpx.Response(200, json={'success':True,'data':data})
        with patch.dict(os.environ, {'BAW_BRIDGE_URL':'http://bridge.invalid','BAW_BRIDGE_TOKEN':'env-bridge-token'}):
            async with self.web('1') as web:
                self.assertEqual((await web.post('/wallet/login', json={})).status_code, 401)
                self.assertEqual((await web.get('/wallet/login/status?session_id=offline-session')).status_code, 401)
                await web.post('/login', data={'password':'test'})
                headers = {'X-CSRF-Token':(await web.get('/api/state')).json()['csrf']}
                self.assertEqual((await web.post('/wallet/login', json={})).status_code, 403)
                self.assertEqual((await web.get('/wallet/login/status?session_id=offline-session')).status_code, 403)
                with patch('app.baw_client.httpx.AsyncClient', side_effect=lambda **kw: original(transport=httpx.MockTransport(handler), **kw)):
                    response = await web.post('/wallet/login', json={}, headers=headers)
                    self.assertEqual(response.json()['status'], 'pending')
                    self.assertEqual(response.json()['session_id'], 'offline-session')
                    self.assertEqual((await web.get('/wallet/login/status?session_id=offline-session', headers=headers)).json()['status'], 'pending')
                    phase[0]='done'
                    response = await web.get('/wallet/login/status?session_id=offline-session', headers=headers)
                    self.assertEqual(response.json()['address'], '0x123')
                    self.assertEqual(response.json()['status'], 'done')
                    self.assertEqual((await web.get('/wallet/status', headers=headers)).json()['walletAddress'], '0x123')
                    phase[0]='failed'
                    self.assertEqual((await web.get('/wallet/login/status?session_id=offline-session', headers=headers)).json()['status'], 'failed')
                self.assertEqual(calls[0], ('POST','/wallet/login'))

    async def test_adaptive_cookie_http_https_and_forwarded_https(self):
        for base, forwarded, secure in [('http://dashboard',False,False),('https://dashboard',False,True),('http://dashboard',True,True)]:
            with self.subTest(base=base, forwarded=forwarded):
                async with self.web('0', base_url=base) as web:
                    response = await web.post('/login', data={'password':'test'}, headers={'X-Forwarded-Proto':'https'} if forwarded else {}, follow_redirects=False)
                    self.assertEqual('Secure' in response.headers['set-cookie'], secure)
                    self.assertIn('HttpOnly', response.headers['set-cookie'])
                    self.assertIn('SameSite=strict', response.headers['set-cookie'])
                    # A browser uses HTTPS externally for the forwarded case.
                    state = await web.get(('https://dashboard' if forwarded else base)+'/api/state')
                    self.assertEqual(state.status_code, 200)
                    self.assertIn('settings', state.json())
                    self.assertNotEqual(state.json()['csrf'], 'public')

    async def test_external_services_roundtrip_masking_guest_and_csrf(self):
        values = dict(
            search_api=dict(provider='openai_compatible', base_url='https://search.example.invalid/v1', api_key='fixture-search-secret', model='search-model', enabled=True),
            model_apis=[dict(id='cpa_astra', name='Custom voter', base_url='https://llm.example.invalid/v1', api_key='fixture-model-secret', model='custom-model', enabled=True)],
            baw_bridge=dict(enabled=None),
            binance_api=dict(api_key='fixture-binance-key', api_secret='fixture-binance-secret', enabled=True))
        async with self.web('1') as web:
            self.assertEqual((await web.post('/api/settings', json=values)).status_code, 401)
            await web.post('/login', data={'password': 'test'})
            token = (await web.get('/api/state')).json()['csrf']
            self.assertEqual((await web.post('/api/settings', json=values)).status_code, 403)
            response = await web.post('/api/settings', json=values, headers={'X-CSRF-Token': token})
            self.assertEqual(response.status_code, 200, response.text)
            values = migrate_settings(values)
            self.assertEqual(self.store.meta('settings'), values)
            state = (await web.get('/api/state')).json()
            for block in BLOCKS:
                self.assertEqual(self.store.settings()[block], values[block])
                self.assertEqual(response.json()['settings'][block], state['settings'][block])
            for secret in ('fixture-search-secret', 'fixture-model-secret', 'fixture-bridge-secret', 'fixture-binance-key', 'fixture-binance-secret'):
                self.assertNotIn(secret, json.dumps(state))
                self.assertNotIn(secret, response.text)
            self.assertTrue(all(p['api_key_set'] and p['api_key']=='' for p in state['settings']['providers']))
            self.assertEqual(state['settings']['baw_bridge'], {'enabled': None})
            self.assertTrue(state['settings']['binance_api']['api_secret_set'])
            # Saving the entire safe table retains secrets, not masked strings.
            saved = await web.post('/api/settings', json={block:state['settings'][block] for block in BLOCKS}, headers={'X-CSRF-Token':token})
            self.assertEqual(saved.status_code, 200, saved.text)
            self.assertEqual(self.store.meta('settings'), values)
            # Closing and reopening the DB proves durable persistence.
            reopened = Store(self.store.path)
            try:
                self.assertEqual(reopened.meta('settings'), values)
            finally:
                reopened.close()
            await web.post('/logout', json={}, headers={'X-CSRF-Token':token})
            guest = await web.get('/api/state')
            guest_html = (await web.get('/')).text
            for block in BLOCKS:
                self.assertNotIn(block, guest.text)
                self.assertNotIn(block, guest_html)
            self.assertNotIn('external-services', guest_html)

    async def test_external_validation_atomicity_and_secret_clear(self):
        with patch.dict(os.environ, {}, clear=True):
            initial = self.store.settings()
            for block, entry, field in [
                ('baw_bridge', dict(enabled=True), 'bridge_token'),
                ('binance_api', dict(enabled=True), 'api_key'),
                ('search_api', dict(provider='openai_compatible', enabled=True), 'provider_id'),
                ('model_apis', [dict(id='custom', enabled=True)], 'provider_id')]:
                with self.subTest(block=block):
                    with self.assertRaisesRegex(ValueError, field):
                        self.store.update_settings({'bet_usdt': 19, block: entry})
                    self.assertEqual(self.store.settings(), initial)
            for url in ('ftp://example.invalid', 'javascript:alert(1)', 'https://user:pass@example.invalid', 'https://example.invalid?token=secret', 'https://example.invalid:bad', 'https://bad host.invalid', 'https://bad\nhost.invalid'):
                for enabled in (None, True):
                    with self.assertRaisesRegex(ValueError, 'http'):
                        self.store.update_settings({'search_api': dict(enabled=enabled, base_url=url)})
                self.store.update_settings({'search_api': dict(enabled=False, base_url=url)})
            with self.assertRaisesRegex(ValueError, '唯一'):
                self.store.update_settings({'model_apis':[dict(id='voter',enabled=False),dict(id=' voter ',enabled=False)]})
            self.store.update_settings({'binance_api':dict(enabled=False,api_key='fixture-key',api_secret='fixture-secret')})
            self.store.update_settings({'binance_api':dict(enabled=False,api_key='',api_secret='')})
            self.assertEqual(self.store.settings()['binance_api']['api_secret'], 'fixture-secret')
            self.store.update_settings({'binance_api':dict(enabled=False,api_key=None,api_secret=None)})
            self.assertEqual(self.store.settings()['binance_api']['api_secret'], '')

    async def test_fresh_service_settings_save_and_explicit_enable_errors(self):
        with patch.dict(os.environ, {}, clear=True):
            async with self.web('0') as web:
                await web.post('/login', data={'password': 'test'})
                token = (await web.get('/api/state')).json()['csrf']
                headers = {'X-CSRF-Token': token}
                initial = self.store.settings()
                self.assertEqual(initial['executor_mode'], 'sapi')
                response = await web.post('/api/settings', json=initial, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(self.store.settings(), initial)
                legacy = {k:v for k,v in initial.items() if k != 'executor_mode'}
                response = await web.post('/api/settings', json=legacy, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(self.store.settings(), initial)
                # Omitted enabled must also stay unset after normalization.
                response = await web.post('/api/settings', json={
                    'bet_usdt': 19, 'model_apis': [{'id': 'custom'}]}, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                self.assertIsNone(self.store.settings()['model_apis'][0]['enabled'])
                for values, label, field in [
                    ({'baw_bridge': {'enabled': True}}, 'baw_bridge', 'bridge_token'),
                    ({'model_apis': [{'id': 'custom', 'enabled': True,
                                     'base_url': 'https://model.invalid/v1', 'model': 'custom'}]},
                     'custom', 'api_key')]:
                    before = self.store.settings()
                    response = await web.post('/api/settings', json=values, headers=headers)
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertIn(label, response.text)
                    self.assertIn(field, response.text)
                    self.assertIn('服务商' if label == 'custom' else '设置或环境变量', response.text)
                    self.assertEqual(self.store.settings(), before)

    async def test_service_enable_validation_uses_resolved_values(self):
        env = {'SEARCH_API_PROVIDER': 'openai_compatible',
               'SEARCH_API_BASE_URL': 'https://search.invalid/v1',
               'SEARCH_API_API_KEY': 'search-fixture', 'SEARCH_API_MODEL': 'search-model',
               'CPA_BASE_URL': 'https://model.invalid/v1', 'CPA_API_KEY': 'model-fixture',
               'BAW_BRIDGE_TOKEN': 'bridge-fixture',
               'BINANCE_API_KEY': 'binance-fixture', 'BINANCE_API_SECRET': 'secret-fixture'}
        with patch.dict(os.environ, env, clear=True):
            self.store.update_settings({
                'search_api': {'enabled': None},
                'model_apis': [{'id': 'cpa_astra', 'enabled': None}],
                'baw_bridge': {'enabled': True}, 'binance_api': {'enabled': True}})
        with patch.dict(os.environ, {}, clear=True):
            # Legacy/environment mode never requires credentials, even when
            # its runtime default is enabled.
            self.store.update_settings({
                'search_api': {'provider': 'openai_compatible'},
                'model_apis': [{'id': 'custom', 'enabled': None}],
                'baw_bridge': {'enabled': None}, 'binance_api': {'enabled': None}})
            for block, field in [('search_api', 'base_url'), ('baw_bridge', 'bridge_url')]:
                with patch.dict(os.environ, {('SEARCH_API_BASE_URL' if block == 'search_api'
                                             else 'BAW_BRIDGE_URL'): 'ftp://invalid'}):
                    with self.assertRaisesRegex(ValueError, field):
                        self.store.update_settings({block: {'enabled': None}})
                    self.store.update_settings({block: {'enabled': False}})
            with patch.dict(os.environ, {'CPA_BASE_URL': 'ftp://invalid'}):
                with self.assertRaisesRegex(ValueError, 'base_url'):
                    self.store.update_settings({'model_apis': [{'id': 'cpa_astra'}]})
                self.store.update_settings({'model_apis': [{'id': 'cpa_astra', 'enabled': False}]})

    async def test_legacy_provider_migration_and_resolution(self):
        legacy = dict(search_api=dict(provider='openai_compatible', base_url='https://old.invalid/v1',
                                      api_key='legacy-key', model='search-model', enabled=True),
                      model_apis=[dict(id='cpa_astra', name='Legacy', base_url='https://old.invalid/v1',
                                       api_key='legacy-key', model='vote-model', enabled=True)])
        self.store.set_meta('settings', legacy)
        with patch.dict(os.environ, {}, clear=True):
            loaded = self.store.settings()
            self.assertEqual(len(loaded['providers']), 1)
            self.assertEqual(self.store.meta('settings'), legacy)  # Loading does not write.
            for slot in [loaded['search_api'], *loaded['model_apis']]:
                self.assertTrue(slot['provider_id'])
                self.assertNotIn('base_url', slot)
                self.assertNotIn('api_key', slot)
            for resolver in (resolve_search, lambda s: resolve_models(s)[0]):
                before, after = resolver(legacy), resolver(loaded)
                for key in ('base_url', 'api_key', 'model', 'enabled'):
                    self.assertEqual(after[key], before[key])
            self.store.update_settings({'bet_usdt': 18})
            self.assertEqual(self.store.meta('settings'), {k:loaded[k] for k in BLOCKS})
            self.assertEqual(migrate_settings(loaded), loaded)
            # Reference wins over inline, which wins over environment.
            mixed = dict(loaded, model_apis=[dict(legacy['model_apis'][0], provider_id=loaded['providers'][0]['id'],
                                                base_url='https://inline.invalid', api_key='inline')])
            self.assertEqual(resolve_models(mixed)[0]['api_key'], 'legacy-key')
            mixed['model_apis'][0]['provider_id'] = 'missing'
            self.assertEqual(resolve_models(mixed)[0]['api_key'], 'inline')

    async def test_provider_drafts_explicit_slot_validation_and_atomic_updates(self):
        with patch.dict(os.environ, {'CPA_API_KEY': 'env-does-not-bypass-reference'}, clear=True):
            async with self.web('1') as web:
                await web.post('/login', data={'password': 'test'})
                headers = {'X-CSRF-Token': (await web.get('/api/state')).json()['csrf']}
                provider = dict(id='draft', name='Draft provider', base_url='https://draft.invalid/v1',
                                api_key='', enabled=True, models=['one','two'])
                response = await web.post('/api/settings', json={'providers':[provider]}, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                for block in ('model_apis', 'search_api'):
                    for enabled in (None, False, True):
                        slot = dict(provider_id='draft', model='one', enabled=enabled)
                        slot.update(id='cpa_astra', name='Voter') if block=='model_apis' else slot.update(provider='openai_compatible')
                        body = {block: [slot] if block=='model_apis' else slot}
                        response = await web.post('/api/settings', json=body, headers=headers)
                        self.assertEqual(response.status_code, 400 if enabled is True else 200, response.text)
                        if enabled is True:
                            self.assertIn(block, response.text)
                            self.assertIn('draft', response.text)
                            self.assertIn('api_key', response.text)
                provider['api_key'] = 'saved-key'
                slot = dict(id='cpa_astra', name='Voter', provider_id='draft', model='two', enabled=True)
                response = await web.post('/api/settings', json={'model_apis':[slot], 'providers':[provider]}, headers=headers)
                self.assertEqual(response.status_code, 200, response.text)
                safe = response.json()['settings']['providers'][0]
                self.assertEqual(safe['api_key'], '')
                self.assertTrue(safe['api_key_set'])
                self.assertNotIn('saved-key', response.text)
                before = self.store.settings()
                for provider_id in ('missing', ''):
                    response = await web.post('/api/settings', json={'model_apis':[dict(slot, provider_id=provider_id)]}, headers=headers)
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertEqual(self.store.settings(), before)
                for update in ({'providers':[]}, {'providers':[dict(provider, api_key=None)]}):
                    response = await web.post('/api/settings', json=update, headers=headers)
                    self.assertEqual(response.status_code, 400, response.text)
                    self.assertEqual(self.store.settings(), before)
                self.assertEqual(resolve_models(before)[0]['model'], 'two')
                await web.post('/logout', json={}, headers=headers)
                guest = (await web.get('/api/state')).text
                for block in BLOCKS:
                    self.assertNotIn(block, guest)

    async def test_external_form_renders_saves_and_preserves_unsaved_edits(self):
        if not shutil.which('node'):
            self.skipTest('Node is required for dashboard JavaScript checks')
        script = re.search(r'<script>(.*?)</script>', self.source, re.S).group(1)
        state = {'settings': safe_settings(self.store.settings()), 'meta': {}, 'csrf':'fixture-csrf'}
        harness = r"""
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const fixture=JSON.parse(fs.readFileSync(0,'utf8'));
const fields={innerHTML:''},status={textContent:''},button={disabled:false};
let focused=false,poll,saved;
const form={contains:()=>focused,elements:{namedItem:name=>({checked:name==='binance_api.api_secret.clear'})}};
const elements={'external-services':form,'external-fields':fields,'external-save':button,'external-state':status};
const entries=[['search_api.provider','vertex_grounding'],['search_api.model',''],
 ['executor_mode','sapi'],
 ['binance_api.api_key',''],['binance_api.api_secret',''],['binance_api.enabled','false']];
fixture.state.settings.model_apis.forEach((item,index)=>{for(const key of ['name','provider_id','model'])entries.push(['model_apis.'+index+'.'+key,item[key]]);entries.push(['model_apis.'+index+'.enabled','false'])});
const context={document:{getElementById:id=>elements[id]||null,activeElement:null},
 FormData:class {constructor(){return entries}},setInterval:fn=>{poll=fn},
 fetch:async(path,args)=>{if(path==='/api/services/models')return {ok:true,status:200,json:async()=>({models:['alpha','beta']})};if(path==='/api/settings'){assert.equal(args.headers['X-CSRF-Token'],'fixture-csrf');saved=JSON.parse(args.body)}return {ok:true,status:200,json:async()=>fixture.state}}};
(async()=>{
 vm.runInNewContext(fixture.script,context);await new Promise(setImmediate);
 for(const name of ['search_api.provider','model_apis.0.name','executor_mode','binance_api.api_secret'])assert.ok(fields.innerHTML.includes('name="'+name+'"'));
 const assertExecutorCards=mode=>{
  const cards=[...fields.innerHTML.matchAll(/<section class="([^"]*)" data-executor="([^"]*)">([\s\S]*?)<\/section>/g)];
  assert.equal(cards.length,2);
  for(const [,classes,executor,body] of cards){
   assert.equal(classes.includes('executor-inactive'),executor!==mode);
   assert.ok(body.includes(executor===mode?'当前执行方式':'未启用'));
   assert.ok(!body.includes('disabled'));assert.ok(!body.includes(' hidden'));
  }
  assert.ok(fields.innerHTML.indexOf('executor-switch')<fields.innerHTML.indexOf('data-executor='));
  assert.ok(fields.innerHTML.includes('name="binance_api.api_key"'));
  assert.ok(fields.innerHTML.includes('id="wallet-status"'));
 };
 assertExecutorCards('sapi');
 entries[2][1]='baw';form.onchange({target:{name:'executor_mode'}});assertExecutorCards('baw');
 entries[2][1]='sapi';form.onchange({target:{name:'executor_mode'}});assertExecutorCards('sapi');
 const wallet={innerHTML:''};elements['wallet-status']=wallet;
 context.fetch=async()=>({ok:true,status:200,json:async()=>({status:'CONNECTED',address:'0xexisting'})});
 await vm.runInNewContext('updateWallet()',context);assert.ok(wallet.innerHTML.includes('0xexisting'));assert.ok(wallet.innerHTML.includes('已登录'));
 context.fetch=async()=>({ok:true,status:200,json:async()=>({status:'CONNECTED'})});
 await vm.runInNewContext('updateWallet()',context);assert.ok(wallet.innerHTML.includes('已登录'));assert.ok(!wallet.innerHTML.includes('class="mono"'));
 delete elements['wallet-status'];
 context.fetch=async(path,args)=>{if(path==='/api/services/models')return {ok:true,status:200,json:async()=>({models:['alpha','beta']})};if(path==='/api/settings'){assert.equal(args.headers['X-CSRF-Token'],'fixture-csrf');saved=JSON.parse(args.body)}return {ok:true,status:200,json:async()=>fixture.state}};
 assert.ok(fields.innerHTML.includes('type="password"'));form.oninput();fields.innerHTML='unsaved edits';await poll();assert.equal(fields.innerHTML,'unsaved edits');
 focused=true;await form.onsubmit({preventDefault(){}});
 assert.equal(saved.executor_mode,'sapi');assert.equal(saved.bet_usdt,fixture.state.settings.bet_usdt);
 assert.ok(!fields.innerHTML.includes('name="baw_bridge.bridge_token"'));
 assert.ok(!fields.innerHTML.includes('name="baw_bridge.bridge_url"'));
 assert.equal(saved.binance_api.api_secret,null);assert.equal(saved.binance_api.enabled,false);assert.equal(saved.baw_bridge.enabled,null);assert.equal(saved.model_apis[0].id,'cpa_astra');
 assert.notEqual(fields.innerHTML,'unsaved edits');assert.equal(button.disabled,false);assert.match(status.textContent,/已保存/);
 assert.ok(!fields.innerHTML.includes('name="model_apis.0.base_url"'));
 assert.ok(!fields.innerHTML.includes('name="search_api.api_key"'));
 const click=async(id,dataset={})=>form.onclick({target:{closest:()=>({id,dataset,disabled:false})}});
 await click('provider-add');assert.ok(fields.innerHTML.includes('name="providers.0.base_url"'));
 await click('',{models:'0'});assert.ok(fields.innerHTML.includes('已缓存 2 个模型'));
 entries.push(['model_apis.0.model_choice','alpha'],['model_apis.0.model','old-manual']);
 await form.onsubmit({preventDefault(){}});assert.deepEqual(saved.providers[0].models,['alpha','beta']);assert.equal(saved.model_apis[0].model,'alpha');
 await click('provider-add');await click('',{providerDelete:'0'});assert.ok(!fields.innerHTML.includes('name="providers.1.base_url"'));

})().catch(error=>{console.error(error);process.exitCode=1});
"""
        result = subprocess.run(['node','-e',harness], input=json.dumps(dict(script=script,state=state)), text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    async def test_paper_account_uses_entire_current_ledger_and_settlement(self):
        from app.risk import mark_paper, settle_paper

        async with self.web('1') as web:
            empty = (await web.get('/api/state')).json()['paper_account']
            self.assertEqual(empty, dict(initial=100, cash=100, equity=100,
                                         realized=0, unrealized=0, open_count=0))
            self.store.execute("INSERT INTO paper_orders (market_id,token_id,amount,price,shares,status,current_price) VALUES ('paper','up',10,.5,20,'OPEN',.5)")
            opened = (await web.get('/api/state')).json()['paper_account']
            self.assertEqual(opened, dict(initial=100, cash=90, equity=100,
                                          realized=0, unrealized=0, open_count=1))
            mark_paper(self.store, 'paper', {'up': .75})
            # Old rows beyond the 200-row history cap must still contribute.
            for index in range(205):
                self.store.execute("INSERT INTO paper_orders (ts,status,realized_pnl) VALUES (?,'SETTLED',1)", (index,))
            self.store.execute("INSERT INTO paper_orders (status,amount,realized_pnl) VALUES ('CANCELED',99,999)")
            guest = (await web.get('/api/state')).json()
            expected = dict(initial=100, cash=295, equity=310, realized=205,
                            unrealized=5, open_count=1)
            self.assertEqual(guest['paper_account'], expected)
            self.assertEqual(guest['paper_pnl'], dict(realized=205, unrealized=5))
            await web.post('/login', data={'password': 'test'})
            operator = (await web.get('/api/state')).json()
            self.assertEqual(operator['paper_account'], expected)
            self.assertEqual(operator['paper_pnl'], guest['paper_pnl'])
            settle_paper(self.store, 'paper', 'up')
            settled = (await web.get('/api/state')).json()['paper_account']
            self.assertEqual(settled, dict(initial=100, cash=315, equity=315,
                                           realized=215, unrealized=0, open_count=0))

            self.store.execute("INSERT INTO paper_orders (market_id,token_id,amount,price,shares,status,current_price) VALUES ('loss','down',10,.5,20,'OPEN',.5)")
            settle_paper(self.store, 'loss', 'up')
            self.assertEqual((await web.get('/api/state')).json()['paper_account'],
                             dict(initial=100, cash=305, equity=305, realized=205, unrealized=0, open_count=0))

    async def test_paper_settings_roundtrip_and_authenticated_reset(self):
        self.store.execute("INSERT INTO paper_orders (amount,status) VALUES (10,'OPEN')")
        self.store.execute("INSERT INTO orders_audit (amount,status) VALUES (7,'SUCCESS')")
        self.store.execute("INSERT INTO daily_spend VALUES ('2026-11-11',10)")
        self.store.execute("INSERT INTO decisions (ts,action) VALUES (1,'SKIP')")
        self.store.set_meta('pnl_portfolio', {'totalPnl': 42})
        preserved = {table: self.store.rows('SELECT * FROM ' + table)
                     for table in ('orders_audit', 'daily_spend', 'decisions', 'meta')}
        for public in ('0', '1'):
            async with self.web(public) as web:
                for headers in ({}, {'x-csrf-token': 'public'}):
                    response = await web.post('/api/paper/reset', json={}, headers=headers)
                    self.assertEqual(response.status_code, 401)
        self.assertEqual(len(self.store.rows('SELECT * FROM paper_orders')), 1)
        async with self.web('1') as web:
            await web.post('/login', data={'password': 'test'})
            state = (await web.get('/api/state')).json()
            headers = {'x-csrf-token': state['csrf']}
            for bad_headers in ({}, {'x-csrf-token': 'wrong'}):
                self.assertEqual((await web.post('/api/paper/reset', json={}, headers=bad_headers)).status_code, 403)
            self.assertEqual(len(self.store.rows('SELECT * FROM paper_orders')), 1)
            self.assertNotIn('paper_reset_at', self.store.settings())
            for balance in (250.75, 0, 100):
                response = await web.post('/api/settings', json={'paper_initial_balance': balance}, headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['settings']['paper_initial_balance'], balance)
                self.assertEqual((await web.get('/api/state')).json()['paper_account']['initial'], balance)
                reopened = Store(self.store.path)
                try:
                    self.assertEqual(reopened.settings()['paper_initial_balance'], balance)
                finally:
                    reopened.close()
            for balance in (-1, 'NaN', 'Infinity', True):
                self.assertEqual((await web.post('/api/settings', json={'paper_initial_balance': balance}, headers=headers)).status_code, 400)
            response = await web.post('/api/paper/reset', json={}, headers=headers)
            self.assertEqual(response.status_code, 200)
            reset = (await web.get('/api/state')).json()
            self.assertEqual(self.store.rows('SELECT * FROM paper_orders'), [])
            self.assertGreater(reset['settings']['paper_reset_at'], state['now'])
            self.assertEqual(reset['paper_account'], dict(initial=100, cash=100, equity=100,
                                                         realized=0, unrealized=0, open_count=0))
            self.assertEqual(reset['paper_pnl'], dict(realized=0, unrealized=0))
            self.assertEqual(reset['settings']['mode'], state['settings']['mode'])
            for table, rows in preserved.items():
                self.assertEqual(self.store.rows('SELECT * FROM ' + table), rows)
            html = (await web.get('/')).text
            self.assertIn('id="paper-reset"', html)
            web.cookies.clear()
            self.assertNotIn('id="paper-reset"', (await web.get('/')).text)
            self.assertNotIn('paper_reset_at', (await web.get('/api/state')).json())

    async def test_order_direction_projection_for_guest_and_operator(self):
        self.store.set_meta('order_history', {'orders': [
            {'orderId': 'up', 'marketId': 'up-market', 'outcomeName': 'Up', 'side': 'BUY'},
            {'orderId': 'unknown', 'side': 'BUY'}]})
        self.store.set_meta('orders', {'orders': [
            {'orderId': 'down', 'marketId': 'down-market', 'outcome': 'Down', 'side': 'BUY'}]})
        for mid, side in (('up-market', 'UP'), ('down-market', 'DOWN')):
            self.store.execute("INSERT INTO decisions (ts,market_id,side,action) VALUES (1,?,?,'TRADE')", (mid, side))
        for ts, side in enumerate(('Up', 'Down', None)):
            self.store.execute('INSERT INTO orders_audit (ts, side) VALUES (?, ?)', (ts, side))
            self.store.execute('INSERT INTO paper_orders (ts, side) VALUES (?, ?)', (ts, side))
        async with self.web('1') as web:
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                self.assertEqual([r['side'] for r in state['orders'] if r['mode'] == 'LIVE'],
                                 ['UP', None, 'DOWN'])
                self.assertEqual([r['side'] for r in state['orders'] if r['mode'] == 'PAPER'],
                                 [None, 'Down', 'Up'])
                self.assertEqual([r['side'] for r in state['orders_audit']], [None, 'Down', 'Up'])

    async def test_audit_fill_status_and_error_exposed_to_guest_and_operator(self):
        expected = [('FAILED', '未成交（场馆 FAILED）'), ('SUCCESS', '部分成交 2.5/10')]
        for ts, (status, error) in enumerate(expected):
            self.store.execute('INSERT INTO orders_audit (ts, status, error) VALUES (?, ?, ?)',
                               (ts, status, error))
        async with self.web('1') as web:
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                self.assertEqual([(row['status'], row['error']) for row in reversed(state['orders_audit'])],
                                 expected)

    async def test_price_context_in_operator_and_guest_views(self):
        self.store.set_meta('market_prices', {'btc': {'start_price': 100, 'last_spot': 105},
                                               'eth': {'start_price': 200, 'last_spot': 210}})
        self.store.set_meta('settlement_prices', {'eth': 205})
        self.store.set_meta('positions', {'list': [
            {'marketId': 'btc', 'marketTitle': 'Bitcoin', 'positionStatus': 'OPEN'},
            {'marketId': 'eth', 'marketTitle': 'Ethereum', 'positionStatus': 'SETTLED'}]})
        for mid in ('btc', 'eth'):
            self.store.execute('INSERT INTO orders_audit (ts, market_id, status) VALUES (?, ?, ?)',
                               (1, mid, 'SUCCESS'))
        async with self.web('1') as web:
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                self.assertEqual([{k: row[k] for k in ('open_price', 'settlement_price')}
                                  for row in state['positions']], [
                    {'open_price': 100, 'settlement_price': None},
                    {'open_price': 200, 'settlement_price': 205}])
                audit = list(reversed(state['orders_audit']))
                self.assertEqual([{k: row[k] for k in ('open_price', 'settlement_price')}
                                  for row in audit], [
                    {'open_price': 100, 'settlement_price': None},
                    {'open_price': 200, 'settlement_price': 205}])
                for row in state['positions'] + state['orders'] + audit:
                    self.assertNotIn('spot_price', row)
                html = (await web.get('/')).text
                self.assertNotIn('当前价格', html)
                for label in ('开盘价', '结算价格', '结束价格'):
                    self.assertIn(label, html)

    async def test_state_omits_spot_prices_with_market_and_symbol_quotes(self):
        self.store.set_meta('market_prices', {
            'old-btc': {'symbol': 'BTCUSDT', 'last_spot': 101, 'updated_ts': 10},
            'fresh-btc': {'symbol': 'BTCUSDT', 'last_spot': 102, 'updated_ts': 20},
            'eth': {'symbol': 'ETHUSDT', 'last_spot': 200, 'updated_ts': 30},
            'empty-btc': {'symbol': 'BTCUSDT', 'last_spot': None, 'updated_ts': 40},
            'own': {'symbol': 'BTCUSDT', 'last_spot': 99, 'updated_ts': 1}})
        self.store.set_meta('positions', {'list': [
            {'marketId': 'legacy', 'question': 'Bitcoin Up or Down', 'positionStatus': 'OPEN'},
            {'marketId': 'own', 'marketTitle': 'Bitcoin'},
            {'marketId': 'ended', 'marketTitle': 'Bitcoin', 'positionStatus': 'SETTLED'},
            {'marketId': 'unknown', 'marketTitle': 'Unrecognized asset'}]})
        self.store.set_meta('orders', {'list': [
            {'marketId': 'legacy-order', 'marketTopicTitle': 'Ethereum Up or Down', 'status': 'OPEN'}]})
        self.store.execute('INSERT INTO orders_audit (ts, market_id, status) VALUES (?, ?, ?)',
                           (1, 'legacy-audit', 'SUCCESS'))
        self.store.execute('INSERT INTO decisions (ts, market_id, market, action) VALUES (?, ?, ?, ?)',
                           (1, 'legacy-audit', 'Bitcoin Up or Down 5m', 'BUY'))
        self.store.execute('INSERT INTO paper_orders (ts, market_id, status) VALUES (?, ?, ?)',
                           (1, 'own', 'OPEN'))
        async with self.web('1') as web:
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                for row in state['positions'] + state['orders'] + state['orders_audit']:
                    self.assertNotIn('spot_price', row)

    async def test_guest_projection_does_not_expose_operator_or_account_secrets(self):
        sentinel = 'PRIVATE-SENTINEL'
        self.store.set_meta('internal', {'secret': sentinel})
        self.store.set_meta('baw_account', {'address': sentinel, 'balance': [
            {'symbol': 'USDT', 'balance': '42.5', 'address': sentinel},
            {'symbol': 'BTC', 'balance': '99'}]})
        self.store.set_meta('pnl_portfolio', {'totalPnl': 2, 'private': sentinel})
        self.store.set_meta('positions', [{'marketTitle': 'Public title', 'address': sentinel}])
        async with self.web('1') as web:
            guest = (await web.get('/api/state')).json()
            self.assertEqual(set(guest), {'public', 'mode', 'pnl', 'paper_account', 'paper_pnl', 'positions', 'orders', 'orders_audit', 'account_balance_usdt', 'last_sync', 'csrf', 'now'})
            self.assertEqual(guest['account_balance_usdt'], 42.5)
            self.assertEqual(guest['pnl']['totalPnl'], 2)
            self.assertEqual(guest['positions'][0]['title'], 'Public title')
            self.assertNotIn(sentinel, json.dumps(guest))
            await web.post('/login', data={'password': 'test'})
            operator = (await web.get('/api/state')).json()
            self.assertIn(sentinel, json.dumps(operator['meta']))
            self.assertIn('settings', operator)

    async def test_live_decision_details_bridge_missing_and_guest_redaction(self):
        secret = 'PRIVATE-LIVE-SENTINEL'
        fields = ('model_prob', 'market_price', 'edge', 'invert', 'side', 'reason')
        for ts, mid, side, reason in ((100, '123', 'DOWN', '成交（反买）'),
                                      (110, 'local', 'UP', '正常成交'),
                                      (300, '123', 'DOWN', '未来决策')):
            self.store.execute('INSERT INTO decisions (ts,market_id,side,model_prob,market_price,edge,action,reason,voters_json) VALUES (?,?,?,?,?,?,?,?,?)',
                               (ts, mid, side, .2, .3, -.1, 'TRADE', reason, secret))
        # A later SKIP must not replace the execution decision.
        self.store.execute("INSERT INTO decisions (ts,market_id,action,reason) VALUES (150,'123','SKIP','跳过')")
        self.store.execute('INSERT INTO orders_audit (ts,order_id,market_id,token_id,quote_id,response_json) VALUES (?,?,?,?,?,?)',
                           (120, 'bridge', 'local', secret, secret, secret))
        self.store.set_meta('order_history', {'orders': [
            {'orderId': 'direct', 'marketId': 123, 'createTime': '1970-01-01T00:03:20Z',
             'outcomeName': 'Down', 'token_id': secret, 'quoteId': secret},
            {'orderId': 'bridge', 'marketId': 'venue-id', 'createTime': 120,
             'outcomeName': 'Up', 'tokenId': secret},
            {'orderId': 'old', 'marketId': 'missing', 'createTime': 'invalid'},
            {'marketId': 'venue-id', 'createTime': 120},
            {'orderId': 'before', 'marketId': '123', 'createTime': 90},
            {'orderId': 'opposite', 'marketId': '123', 'createTime': 200, 'outcomeName': 'Up'}]})
        async with self.web('1') as web:
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                orders = [r for r in state['orders'] if r['mode'] == 'LIVE']
                self.assertEqual({k: orders[0][k] for k in fields},
                                 dict(model_prob=.2, market_price=.3, edge=-.1,
                                      invert=True, side='DOWN', reason='成交（反买）'))
                self.assertEqual(orders[1]['side'], 'UP')
                self.assertFalse(orders[1]['invert'])
                self.assertEqual(orders[1]['reason'], '正常成交')
                for order in orders[2:]:
                    self.assertEqual({k: order[k] for k in fields}, dict.fromkeys(fields))
                for order in orders:
                    self.assertFalse(set(order) & {'token_id', 'tokenId', 'quote_id', 'quoteId',
                                                  'market_id', 'orderId', 'settings', 'voters_json'})
                if not operator:
                    self.assertNotIn(secret, json.dumps(state))
                    self.assertNotIn('settings', state)
                    self.assertNotIn('meta', state)
        self.assertNotIn('orders_audit', self.source)
        self.assertNotIn('审计', self.source)
        self.assertEqual(self.store.rows('SELECT quote_id FROM orders_audit')[0]['quote_id'], secret)

    async def test_live_window_fill_price_pnl_and_original_title(self):
        original_title = '比特币 涨还是跌 - 11月11日, 10:25AM-10:30AM 美东时间'
        for mid, market, end in [('history', 'BTC 5m', 1500), ('five', 'BTC 5m', 1500),
                                 ('fifteen', 'ETH 15m', 2100), ('unknown-duration', 'BTC', 1500)]:
            self.store.execute('INSERT INTO decisions (ts,market_id,market,variant_end_time,side,model_prob,market_price,action,reason) VALUES (?,?,?,?,?,?,?,?,?)',
                               (1250, mid, market, end, 'UP', .8, .4, 'TRADE', '真钱订单已提交'))
        # Evaluation windows take precedence over a conflicting decision end.
        self.store.execute("INSERT INTO eval_history (ts,market_id,symbol,window_start,window_end,voters_json) VALUES (1250,'history','BTCUSDT',1200,1600,'{}')")
        self.store.execute("INSERT INTO orders_audit (ts,order_id,market_id,token_id,quote_id) VALUES (1300,'bridge','fifteen','PRIVATE','PRIVATE')")
        venue = [
            {'orderId': 'filled', 'marketId': 'history', 'filledUsdtAmount': '6', 'filledShareQty': '20', 'makerShareQty': '100', 'pnl': '2.25'},
            {'orderId': 'zero', 'marketId': 'five', 'filledUsdtAmount': '6', 'filledShareQty': '0'},
            {'orderId': 'bridge', 'marketId': 'venue', 'filledUsdtAmount': 'invalid', 'filledShareQty': '20'},
            {'orderId': 'unknown', 'marketId': 'missing', 'price': .9},
            {'orderId': 'duration', 'marketId': 'unknown-duration'},
            {'orderId': 'invalid-pnl', 'marketId': 'missing', 'pnl': 'NaN'},
            {'orderId': 'flat', 'marketId': 'missing', 'pnl': '0'}]
        venue = [dict(r, createTime=1300, outcomeName='Up', status='FILLED', marketTitle=original_title) for r in venue]
        self.store.set_meta('order_history', {'orders': venue})
        self.store.set_meta('positions', {'positions': [
            {'orderId': 'bridge', 'marketId': 'venue', 'pnl': '-1.25'},
            {'marketId': 'missing', 'pnl': '999'}]})
        before = self.store.rows('SELECT * FROM decisions')
        async with self.web('1') as web:
            guest_orders = None
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                orders = (await web.get('/api/state')).json()['orders']
                if guest_orders is None:
                    guest_orders = orders
                self.assertEqual(orders, guest_orders)
                self.assertEqual([(r['window_start'], r['window_end']) for r in orders],
                                 [(1200, 1600), (1200, 1500), (1200, 2100), (None, None),
                                  (None, 1500), (None, None), (None, None)])
                self.assertEqual([r['price'] for r in orders], [.3, .4, .4, None, .4, None, None])
                self.assertEqual([(r['result'], r['realized_pnl']) for r in orders],
                                 [(None, None)] * 7)
                for row in orders:
                    self.assertEqual(row['title'], original_title)
                    self.assertTrue({'token_id', 'quote_id', 'orderId', 'market_id'}.isdisjoint(row))
                self.assertNotIn('PRIVATE', json.dumps(orders))
        self.assertEqual(before, self.store.rows('SELECT * FROM decisions'))

    async def test_live_chinese_titles_and_settled_fill_results(self):
        english = 'Bitcoin Up or Down - November 11, 10:25AM-10:30AM ET'
        chinese = '比特币 涨还是跌 - 11月11日, 10:25AM-10:30AM 美东时间'
        self.store.execute("INSERT INTO eval_history (id,ts,market_id,symbol,window_start,window_end,voters_json) VALUES (1,100,'scored','BTCUSDT',100,200,'{}')")
        self.store.execute("INSERT INTO eval_scores VALUES (1,'BTCUSDT',100,'UP','{}')")
        self.store.execute("INSERT INTO orders_audit (ts,order_id,market_id) VALUES (120,'bridge','scored')")
        # The stored evaluation wins even when the price comparison disagrees.
        samples = [
            dict(marketId='scored', open_price=100, settlement_price=90),
            dict(marketId='scored', outcomeName='DOWN'),
            dict(marketId='price-up', open_price=100, settlement_price=110),
            dict(marketId='price-down', outcomeName='DOWN', open_price=100, settlement_price=90),
            dict(marketId='loss', open_price=100, settlement_price=90),
            dict(marketId='equal', open_price=100, settlement_price=100, pnl=999),
            dict(marketId='unknown', pnl=999),
            dict(marketId='scored', filledUsdtAmount='0'),
            dict(marketId='scored', filledShareQty='0'),
            dict(marketId='scored', filledUsdtAmount=None),
            dict(marketId='scored', filledShareQty=None, makerShareQty=20),
            dict(marketId='scored', filledUsdtAmount='NaN'),
            dict(marketId='scored', filledShareQty='Infinity'),
            dict(marketId='scored', status='SUBMITTED'),
            dict(marketId='scored', status='CANCELED'),
            dict(marketId='scored', outcomeName='unknown'),
            dict(marketId='venue', orderId='bridge'),
            dict(marketId='cached-prices'),
            dict(marketId='scored', filledUsdtAmount='invalid'),
            dict(marketId='scored', filledShareQty='-1'),
        ]
        venue = [dict(dict(orderId=str(i), marketTopicTitle=english,
                           outcomeName='UP', filledUsdtAmount='6', filledShareQty='20',
                           status='FILLED', createTime=120, tokenId='PRIVATE-LIVE'), **sample)
                 for i, sample in enumerate(samples)]
        venue[0]['translatedMarketTopicTitle'] = '场馆已有中文标题'
        venue[1]['translatedMarketTopicTitle'] = ''
        venue[2]['translatedMarketTopicTitle'] = '   '
        venue[3]['marketTopicTitle'] = english.replace('Bitcoin', 'Ethereum').replace('November 11', 'January 2')
        self.store.set_meta('order_history', {'orders': venue})
        self.store.set_meta('market_prices', {'cached-prices': {'start_price': 100}})
        self.store.set_meta('settlement_prices', {'cached-prices': 110})
        # Market-level or order-level PnL is not a substitute for settled fills.
        self.store.set_meta('positions', {'positions': [{'orderId': '6', 'pnl': 999}]})
        self.store.execute("INSERT INTO paper_orders (title,status) VALUES (?, 'OPEN')", (english,))
        expected = [('WIN', 14), ('LOSS', -6), ('WIN', 14), ('WIN', 14), ('LOSS', -6)]
        expected += [(None, None)] * 11 + [('WIN', 14), ('WIN', 14)] + [(None, None)] * 2
        async with self.web('1') as web:
            guest = None
            for operator in (False, True):
                if operator:
                    await web.post('/login', data={'password': 'test'})
                state = (await web.get('/api/state')).json()
                orders = [r for r in state['orders'] if r['mode'] == 'LIVE']
                if guest is None:
                    guest = orders
                self.assertEqual(orders, guest)
                self.assertEqual([(r['result'], r['realized_pnl']) for r in orders], expected)
                self.assertEqual(orders[0]['title'], '场馆已有中文标题')
                self.assertEqual(orders[1]['title'], chinese)
                self.assertEqual(orders[2]['title'], chinese)
                self.assertEqual(orders[3]['title'], '以太坊 涨还是跌 - 1月2日, 10:25AM-10:30AM 美东时间')
                self.assertEqual(next(r for r in state['orders'] if r['mode'] == 'PAPER')['title'], english)
                self.assertNotIn('PRIVATE-LIVE', json.dumps(orders))
                for order in orders:
                    self.assertTrue({'orderId', 'market_id', 'marketId', 'tokenId', 'shares',
                                     'filledShareQty', 'filledUsdtAmount'}.isdisjoint(order))

    async def test_paper_order_details_context_settlement_and_guest_redaction(self):
        sentinel = 'PRIVATE-PAPER-SENTINEL'

        def evaluation(ts, mid='paper', start=100, end=200, probability=.8):
            self.store.execute('INSERT INTO eval_history (ts,market_id,symbol,window_start,window_end,voters_json,ensemble_p) VALUES (?,?,?,?,?,?,?)',
                               (ts, mid, 'BTCUSDT', start, end, '{}', probability))

        def decision(ts, end=200, probability=.2, price=.3, reason='模拟成交（反买）', action='TRADE'):
            self.store.execute('INSERT INTO decisions (ts,market_id,market,variant_end_time,side,model_prob,market_price,edge,action,reason) VALUES (?,?,?,?,?,?,?,?,?,?)',
                               (ts, 'paper', 'Paper <title>', end, 'DOWN', probability, price, probability-price, action, reason))

        evaluation(110)
        evaluation(115, mid='other', probability=.1)
        evaluation(125, probability=.9)  # After order placement: must not leak backwards.
        decision(110)
        decision(115, action='SKIP')
        decision(118, end=300, probability=.9)
        decision(125, probability=.9)
        for status, payout in (('OPEN', .4), ('SETTLED', 1), ('SETTLED', 0)):
            self.store.execute('INSERT INTO paper_orders (ts,market_id,title,side,amount,price,status,current_price,end_time) VALUES (?,?,?,?,?,?,?,?,?)',
                               (120, 'paper', 'Paper <title>', 'DOWN', 10, .25, status, payout, 200))
        # History-only fallback, plus legacy rows without any usable context.
        evaluation(310, mid='history', start=300, end=400, probability=.7)
        self.store.execute("INSERT INTO paper_orders (ts,market_id,side,status) VALUES (320,'history','DOWN','OPEN')")
        self.store.execute("INSERT INTO paper_orders (ts,market_id,status) VALUES (500,'missing','SETTLED')")
        self.store.execute('UPDATE paper_orders SET token_id=?, topic_id=?', (sentinel, sentinel))
        self.store.set_meta('market_prices', {'paper': {'start_price': 60000}})
        self.store.set_meta('settlement_prices', {'paper': 59900})
        before = self.store.rows('SELECT * FROM paper_orders')
        public_fields = {'mode', 'title', 'ts', 'window_start', 'window_end', 'side',
                         'amount', 'price', 'model_prob', 'market_price', 'edge', 'invert',
                         'status', 'result', 'realized_pnl', 'open_price', 'settlement_price'}
        forbidden = {'id', 'token_id', 'market_id', 'topic_id', 'shares', 'current_price',
                     'settled_at', 'end_time', 'settings', 'csrf', 'account_id', 'address',
                     'api_key', 'api_secret', 'secret', 'session'}
        async with self.web('1') as web:
            guest = (await web.get('/api/state')).json()
            for row in guest['orders']:
                self.assertEqual(set(row), public_fields)
                self.assertTrue(forbidden.isdisjoint(row))
            self.assertNotIn(sentinel, json.dumps(guest))
            await web.post('/login', data={'password': 'test'})
            state = (await web.get('/api/state')).json()
        # Every public detail, including historical fallbacks and settlement, uses
        # exactly the operator projection while its internal columns stay private.
        self.assertEqual(guest['orders'], [{k: row[k] for k in public_fields} for row in state['orders']])
        orders = {r['id']: r for r in state['paper_orders']}
        for index in (1, 2, 3):
            row = orders[index]
            self.assertEqual((row['window_start'], row['window_end']), (100, 200))
            self.assertEqual((row['model_prob'], row['market_price']), (.2, .3))
            self.assertAlmostEqual(row['edge'], -.1)
            self.assertTrue(row['invert'])
            self.assertEqual(row['side'], 'DOWN')
        for row in guest['orders']:
            if row['title'] == 'Paper <title>':
                self.assertEqual((row['open_price'], row['settlement_price']), (60000, 59900))
        self.assertEqual([(orders[i]['result'], orders[i]['realized_pnl']) for i in (1, 2, 3)],
                         [(None, None), ('WIN', 30), ('LOSS', -10)])
        self.assertAlmostEqual(orders[4]['model_prob'], .3)
        self.assertTrue(orders[4]['invert'])
        self.assertIsNone(orders[4]['market_price'])
        self.assertIsNone(orders[4]['edge'])
        for key in ('model_prob', 'market_price', 'edge', 'invert', 'result', 'realized_pnl', 'window_start', 'window_end'):
            self.assertIsNone(orders[5][key])
        for row in state['orders']:
            self.assertEqual(row['model_prob'], orders[row['id']]['model_prob'])
        self.assertEqual(before, self.store.rows('SELECT * FROM paper_orders'))

    async def test_paper_existing_context_takes_precedence_and_nullable_legacy_rows_work(self):
        # Some installations may already carry nullable context columns.
        for key in ('window_start', 'window_end', 'model_prob', 'market_price', 'edge', 'invert'):
            self.store.execute(f'ALTER TABLE paper_orders ADD COLUMN {key} REAL')
        self.store.execute("INSERT INTO paper_orders (ts,market_id,side,amount,price,status,end_time,window_start,model_prob,market_price,edge,invert) VALUES (120,'existing','UP',5,.4,'OPEN',200,100,0,0,0,0)")
        self.store.execute("INSERT INTO paper_orders (ts,market_id,status) VALUES (121,'legacy','OPEN')")
        self.store.update_settings({'invert_signal': True})
        async with self.web('1') as web:
            await web.post('/login', data={'password': 'test'})
            response = await web.get('/api/state')
        self.assertEqual(response.status_code, 200)
        legacy, existing = response.json()['paper_orders']
        self.assertEqual((existing['window_start'], existing['window_end']), (100, 200))
        for key in ('model_prob', 'market_price', 'edge', 'invert'):
            self.assertEqual(existing[key], 0)
            self.assertIsNone(legacy[key])
        self.assertEqual(existing['price'], .4)

    async def test_login_secure_cookie_origin_checks_and_local_override(self):
        async with self.web('0') as web:
            for headers in ({'Origin': 'https://evil.example'}, {'Referer': 'https://evil.example/form'},
                            {'Origin': 'null'}):
                response = await web.post('/login', data={'password': 'test'}, headers=headers)
                self.assertEqual(response.status_code, 403)
                self.assertNotIn('set-cookie', response.headers)
            response = await web.post('/login', data={'password': 'test'},
                                      headers={'Origin': 'https://dashboard', 'Referer': 'https://dashboard/login'}, follow_redirects=False)
            self.assertEqual(response.status_code, 303)
            self.assertIn('Secure', response.headers['set-cookie'])
            with patch.dict(os.environ, {'SESSION_COOKIE_SECURE': '0'}):
                response = await web.post('/login', data={'password': 'test'}, follow_redirects=False)
                self.assertIn('Secure', response.headers['set-cookie'])

    async def test_login_origin_hostname_matches_host_behind_https_proxy(self):
        async with self.web('0', base_url='http://internal:8000') as web:
            for headers in (
                {'Origin': 'https://forecast.example.invalid', 'Host': 'forecast.example.invalid'},
                {'Origin': 'https://FORECAST.EXAMPLE.INVALID:443', 'Host': 'forecast.example.invalid:8000'},
                {'Referer': 'https://forecast.example.invalid/login', 'Host': 'FORECAST.EXAMPLE.INVALID'},
                {'Origin': 'https://forecast.example.invalid', 'Referer': 'https://evil.example/form',
                 'Host': 'forecast.example.invalid'},
            ):
                with self.subTest(headers=headers):
                    response = await web.post('/login', data={'password': 'wrong'}, headers=headers)
                    self.assertEqual(response.status_code, 401)
                    self.assertIn('密码错误或尚未配置管理密码', response.text)

    async def test_login_cross_host_origin_rejected_behind_https_proxy(self):
        async with self.web('0', base_url='http://internal:8000') as web:
            for source in (
                {'Origin': 'https://evil.example', 'Referer': 'https://forecast.example.invalid/login'},
                {'Referer': 'https://evil.example/form'},
            ):
                with self.subTest(source=source):
                    response = await web.post('/login', data={'password': 'test'},
                                              headers={'Host': 'forecast.example.invalid', **source})
                    self.assertEqual(response.status_code, 403)
                    self.assertEqual(response.text, '来源校验失败')
                    self.assertNotIn('set-cookie', response.headers)

    async def test_login_five_failures_block_for_fifteen_minutes(self):
        async with self.web('0') as web:
            now = 1800000000
            with patch('app.main.time.time', return_value=now):
                for _ in range(5):
                    self.assertEqual((await web.post('/login', data={'password': 'wrong'})).status_code, 401)
                self.assertEqual((await web.post('/login', data={'password': 'test'})).status_code, 429)
            with patch('app.main.time.time', return_value=now + 899):
                self.assertEqual((await web.post('/login', data={'password': 'test'})).status_code, 429)
            with patch('app.main.time.time', return_value=now + 901):
                self.assertEqual((await web.post('/login', data={'password': 'test'}, follow_redirects=False)).status_code, 303)

    async def test_login_failures_are_limited_by_first_forwarded_ip(self):
        async with self.web('0') as web:
            for index in range(5):
                response = await web.post('/login', data={'password': 'wrong'},
                                          headers={'X-Forwarded-For': f' 198.51.100.10 , 192.0.2.{index + 1}'})
                self.assertEqual(response.status_code, 401)
            response = await web.post('/login', data={'password': 'test'},
                                      headers={'X-Forwarded-For': '198.51.100.10'}, follow_redirects=False)
            self.assertEqual(response.status_code, 429)
            response = await web.post('/login', data={'password': 'test'},
                                      headers={'X-Forwarded-For': '203.0.113.20, 192.0.2.1'}, follow_redirects=False)
            self.assertEqual(response.status_code, 303)

    async def test_concurrent_login_failures_share_the_rate_limit(self):
        async with self.web('0') as web:
            responses = await asyncio.gather(*(web.post('/login', data={'password': 'wrong'}) for _ in range(12)))
            codes = [r.status_code for r in responses]
            self.assertEqual(codes.count(401), 5)
            self.assertEqual(codes.count(429), 7)

    async def test_password_rotation_invalidates_persisted_and_env_sessions(self):
        client = BinanceClient('', '', http=httpx.AsyncClient(transport=httpx.MockTransport(lambda r: None)))
        self.addAsyncCleanup(client.close)
        for password_arg in ('explicit', None):
            with patch.dict(os.environ, {'APP_PASSWORD': 'first', 'PUBLIC_READONLY': '0'}):
                app = create_app(self.store, client, background=False, password=password_arg)
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='https://dashboard') as web:
                    await web.post('/login', data={'password': password_arg or 'first'})
                    self.assertEqual((await web.get('/api/state')).status_code, 200)
                    if password_arg is None:
                        os.environ['APP_PASSWORD'] = 'second'
                    else:
                        app = create_app(self.store, client, background=False, password='changed')
                        web._transport = httpx.ASGITransport(app=app)
                    self.assertEqual((await web.get('/api/state')).status_code, 401)
                    self.assertEqual((await web.post('/logout', headers={'X-CSRF-Token': 'anything'})).status_code, 401)

    async def test_state_limits_recent_decisions_and_order_audit(self):
        # Seed legacy history directly so the API cap is tested independently of pruning.
        for index in range(15):
            self.store.execute('INSERT INTO decisions (ts, market, action) VALUES (?, ?, ?)',
                               (index // 2, f'Decision {index}', 'SKIP'))
            self.store.execute('INSERT INTO orders_audit (ts, market_id, status) VALUES (?, ?, ?)',
                               (index // 2, str(index), 'SUCCESS'))
            self.store.execute('INSERT INTO paper_orders (ts, market_id) VALUES (?, ?)', (index, str(index)))
        self.store.set_meta('fixture', {'value': 123})
        self.store.execute('INSERT INTO daily_spend VALUES (?, ?)', ('2026-11-09', 25))
        async with self.web('1') as web:
            await web.post('/login', data={'password': 'test'})
            response = await web.get('/api/state')
        self.assertEqual(response.status_code, 200)
        state = response.json()
        self.assertEqual(set(state), {'public', 'settings', 'paper_account', 'paper_pnl', 'meta', 'decisions', 'paper_orders',
                                      'orders_audit', 'daily_spend', 'csrf', 'now', 'positions', 'orders'})
        for table in ('decisions', 'orders_audit'):
            self.assertEqual(len(state[table]), 10)
            expected = self.store.rows(f'SELECT * FROM {table} ORDER BY ts DESC, id DESC LIMIT 10')
            if table == 'orders_audit':
                expected = [{**r, 'open_price': None, 'settlement_price': None} for r in expected]
            self.assertEqual(state[table], expected)
            self.assertEqual([row['id'] for row in state[table]], list(range(15, 5, -1)))
            self.assertEqual(len(self.store.rows(f'SELECT * FROM {table}')), 15)
        raw = self.store.rows('SELECT * FROM paper_orders ORDER BY ts DESC LIMIT 200')
        self.assertEqual([r['id'] for r in state['paper_orders']], [r['id'] for r in raw])
        for row in state['paper_orders']:
            for key in ('window_start', 'window_end', 'model_prob', 'market_price', 'edge', 'invert', 'result', 'realized_pnl'):
                self.assertIsNone(row[key])
        self.assertEqual(raw, self.store.rows('SELECT * FROM paper_orders ORDER BY ts DESC LIMIT 200'))
        self.assertEqual(state['settings'], safe_settings(self.store.settings()))
        self.assertEqual(state['meta']['fixture'], {'value': 123})
        self.assertEqual(len(state['meta']['voter_weights']),4)
        self.assertTrue(all(w==1.0 for w in state['meta']['voter_weights'].values()))
        self.assertEqual(state['daily_spend'], [{'day': '2026-11-09', 'amount': 25}])
        self.assertTrue(state['public'])
        self.assertNotEqual(state['csrf'], 'public')
        self.assertIsInstance(state['now'], float)

    async def test_public_anonymous_page_and_real_session(self):
        blocks = re.findall(r'<!--OPERATOR-ONLY-->(.*?)<!--/OPERATOR-ONLY-->', self.source, re.S)
        self.assertGreaterEqual(len(blocks), 6)
        protected = '\n'.join(blocks)
        for element in OPERATOR_IDS:
            self.assertIn(f'id="{element}"', protected)
        async with self.web('1') as web:
            # An invalid cookie must still receive the anonymous page.
            web.cookies.set('session', 'expired-session')
            response = await web.get('/')
            self.assertEqual(response.status_code, 200)
            html = response.text
            self.assertNotIn('OPERATOR-ONLY', html)
            for block in blocks:
                self.assertNotIn(block, html)
            for element in OPERATOR_IDS:
                self.assertNotIn(f'id="{element}"', html)
                self.assertNotRegex(html, rf"el\('{element}'\)\.")
            self.assertNotIn('<form', html)
            self.assertNotIn('<button', html)
            self.assertNotIn('退出登录', html)
            self.assertEqual(Markup(html).headings, list(PUBLIC_HEADINGS))
            for heading in OPERATOR_HEADINGS:
                self.assertNotIn(heading, html)
            self.assertNotIn('/logout', html)
            self.assertNotIn('/api/settings', html)
            self.assertIn('<a href="/login">登录</a>', html)
            for element in ('portfolio', 'positions', 'orders'):
                self.assertIn(f'id="{element}"', html)
            self.assertNotIn('paper-pnl', html)
            self.assertNotIn('模拟账本', html)
            self.assertIn('async function refresh()', html)
            self.assertIn("request('/api/state')", html)
            self.assertIn('setInterval(refresh,15000)', html)
            self.assertEqual((await web.get('/api/state')).status_code, 200)

            self.assertEqual((await web.post('/login', data={'password': 'test'}, follow_redirects=False)).status_code, 303)
            self.assertEqual((await web.get('/')).text, self.source)
            operator_html = (await web.get('/')).text
            self.assertEqual(Markup(operator_html).headings, list(PUBLIC_HEADINGS + OPERATOR_HEADINGS))
            self.assertNotIn('paper-pnl', operator_html)
            self.assertNotIn('模拟账本', operator_html)
            for element in OPERATOR_IDS:
                self.assertIn(f'id="{element}"', operator_html)
            self.assertNotEqual((await web.get('/api/state')).json()['csrf'], 'public')

    async def test_private_mode_unchanged(self):
        async with self.web('0') as web:
            response = await web.get('/', follow_redirects=False)
            self.assertEqual(response.status_code, 303)
            self.assertEqual(response.headers['location'], '/login')
            self.assertEqual((await web.get('/api/state')).status_code, 401)
            await web.post('/login', data={'password': 'test'})
            self.assertEqual((await web.get('/')).text, self.source)
            for element in OPERATOR_IDS:
                self.assertIn(f'id="{element}"', (await web.get('/')).text)

    async def test_state_exposes_fresh_voter_weights(self):
        async with self.web('1') as web:
            await web.post('/login',data={'password':'test'})
            first=(await web.get('/api/state')).json()['meta']['voter_weights']
            self.assertEqual(first['gpt-6-astra'],1.0)
            self.store.set_meta('voter_stats',{'gpt-6-astra':{'scored':30,'correct':0},
                **{name:None for name in ('EMA趋势','RSI(14)','MACD','一小时动量','布林带位置','成交量趋势')},
                'gemini-3.8-flash':None, 'gemini-4-argon-high':{'scored':30,'correct':30}})
            second=(await web.get('/api/state')).json()['meta']['voter_weights']
            self.assertEqual(second['gpt-6-astra'],.3)
            self.assertEqual(len(second),4)
            self.assertNotIn('gemini-3.8-flash',second)
            self.assertNotIn('gemini-4-argon-high',second)

    def test_self_contained_markup_and_settings_contract(self):
        markup = Markup(self.source)
        self.assertEqual(set(markup.fields), {'mode', 'paper_initial_balance', 'invert_signal', 'bet_usdt', 'prob_threshold', 'edge_min',
                                             'min_agree_votes', 'market_window', 'daily_spend_cap', 'daily_loss_stop',
                                             'max_open_positions', 'slippage_bps'})
        ranges = {'bet_usdt': ('1', '100'), 'prob_threshold': ('0.50', '1'),
                  'edge_min': ('-1', '1'), 'min_agree_votes': ('0', '12'), 'daily_spend_cap': ('1', '5000'),
                  'daily_loss_stop': ('0.01', '1000'), 'max_open_positions': ('1', '50'),
                  'slippage_bps': ('1', '10000')}
        for name, (minimum, maximum) in ranges.items():
            with self.subTest(field=name):
                self.assertEqual(markup.fields[name]['min'], minimum)
                self.assertEqual(markup.fields[name]['max'], maximum)
                self.assertIn('required', markup.fields[name])
        self.assertIn('<option value="300">5 分钟</option>', self.source)
        self.assertIn('<option value="900">15 分钟</option>', self.source)
        self.assertEqual(markup.fields['invert_signal']['type'], 'checkbox')
        self.assertIn('反买模式（信号反着下单）', self.source)
        self.assertEqual(markup.fields['max_open_positions']['type'], 'number')
        self.assertEqual(markup.fields['max_open_positions']['step'], '1')
        self.assertIn('<option value="PAPER">模拟</option>', self.source)
        self.assertIn('<option value="LIVE">真钱</option>', self.source)
        self.assertNotRegex(self.source, r'<(?:script|img|iframe)[^>]+src=|<link[^>]+href=|@import|url\(')
        self.assertIn('name="viewport"', self.source)
        self.assertNotIn('模拟账本', self.source)
        self.assertNotIn('paper-ledger', self.source)
        self.assertNotIn('paper-pnl', self.source)
        self.assertIn('单笔上限 100 USDT · 每日花费上限 5000 USDT · 亏损停机上限 1000 USDT · 最多 50 个未平仓标的', self.source)

    @unittest.skipUnless(shutil.which('node'), 'Node is needed to execute the dashboard JavaScript')
    async def test_rendering_controls_and_missing_elements(self):
        async with self.web('1') as web:
            public_html = (await web.get('/')).text
            await web.post('/login', data={'password': 'test'})
            operator_html = (await web.get('/')).text
            state = (await web.get('/api/state')).json()
        # Exercise the legacy raw-meta rendering fallback with this fixture.
        state.pop('positions', None)
        state.pop('orders', None)
        state['meta'] = {
            'baw_account': {'balance': [{'symbol': 'BTC', 'balance': '5'},
                                        {'symbol': 'USDT', 'balance': '987.65', 'value': '100'}]},
            'cache:pnl_portfolio': {'data': {'totalCurrentValue': 1234.5, 'totalPnl': 12.34,
                                           'totalRealizedPnl': 20, 'totalUnrealizedPnl': -7.66}},
            'cache:positions:ONGOING': {'data': {'positions': [
                {'marketTopicTitle': 'Bitcoin Up or Down <img onerror=bad>', 'outcomeName': 'UP',
                 'shares': 2, 'currentPrice': .4, 'pnl': -1.2,
                 'open_price': 12345.6, 'spot_price': 12350.1, 'settlement_price': 12360}]}},
            'cache:order_history': {'data': {'orders': [
                {'orderId': 'same', 'marketTopicTitle': 'Unique order', 'createTime': 1000,
                 'makerUsdtAmount': 3, 'status': 'FILLED',
                 'open_price': 3000, 'spot_price': 3999, 'settlement_price': 3010.5}]}},
            'cache:orders': {'data': {'orders': [
                {'orderId': 'same', 'marketTopicTitle': 'Duplicate order', 'createTime': 1000},
                {'marketTopicTitle': 'No id one', 'createTime': 500, 'open_price': 4000, 'spot_price': 4999},
                {'marketTopicTitle': 'No id two', 'createTime': 400}]}},
            'sync_state': {'last_success': state['now'] - 10, 'endpoints': {
                'positions': {'ts': state['now'] - 5},
                'orders': {'permission_pending': True, 'error': '<script>bad</script>'}}},
            'wallet': {'walletAddress': '<wallet>', 'walletId': 'wallet-1'},
            'engine_error': {'error': '<engine failed>'},
            'paper_sync_error': {'error': '<paper failed>'}}
        state['paper_orders'] = [
            {'id': 1, 'title': 'Paper position', 'side': 'DOWN', 'status': 'OPEN', 'shares': 10,
             'current_price': .7, 'amount': 5, 'realized_pnl': 0, 'ts': 3000},
            {'id': 2, 'title': 'Settled paper', 'status': 'SETTLED', 'realized_pnl': -2, 'ts': 2000}]
        state['orders_audit'] = [{'market_id': 'Audit fixture', 'ts': 2500, 'amount': 2,
                                 'status': 'FAILED', 'error': '<audit failed>',
                                 'open_price': 2000, 'spot_price': 2999, 'settlement_price': 2010.5}]
        state['decisions'] = [{'id': 7, 'ts': 3000, 'market': 'Decision fixture', 'side': 'UP',
                               'action': 'SKIP', 'model_prob': .7, 'market_price': .6, 'edge': .1,
                               'reason': '<decision reason>', 'variant_end_time': 4000,
                               'voters_json': json.dumps([
                                   {'name': 'Voter A', 'status': 'ok', 'p_up': .72, 'reason': '<voter reason>'},
                                   {'name': 'Voter B', 'status': 'timeout', 'reason': 'Timed out fixture'}])}]
        state['settings']['engine_enabled'] = False
        state['settings']['mode'] = 'LIVE'
        state['paper_account'] = dict(initial=100, cash=93, equity=100, realized=-2, unrealized=2, open_count=1)
        fixture = json.dumps({
            'views': [{'ids': list(Markup(page).ids),
                       'script': re.search(r'<script>(.*?)</script>', page, re.S).group(1)}
                      for page in (public_html, operator_html)],
            'fields': list(Markup(operator_html).fields), 'state': state})
        harness = r'''
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const fixture = JSON.parse(fs.readFileSync(0, 'utf8'));
async function exercise(view, operator, missing=false) {
  const state = JSON.parse(JSON.stringify(fixture.state)), calls = [];
  const elements = Object.fromEntries((missing ? [] : view.ids).map(id => [id, {
    innerHTML:'', textContent:'', disabled:true, elements:[],
    contains:()=>false, querySelectorAll:()=>[]
  }]));
  if (elements.parameters) {
    elements.parameters.elements = fixture.fields.map(name => ({name,value:'',type:name==='invert_signal'?'checkbox':'',checked:false}));
    elements.parameters.elements.namedItem = name=>elements.parameters.elements.find(f=>f.name===name);
  }
  let poll, failure = false, unauthorized = false, rejectSave = false, confirmed = false;
  const context = {
    document: {getElementById:id=>elements[id]||null, activeElement:null},
    location: '/',
    confirm:()=>confirmed,
    FormData: class {constructor(form) {return form.elements.filter(f=>f.type!=='checkbox'||f.checked).map(f=>[f.name, String(f.value)]);}},
    fetch: async (path, options) => {
      // Wallet-card checks run alongside rendering; keep control-action assertions focused.
      if (path === '/wallet/status') return {status:200,ok:true,json:async()=>({status:'CONNECTED'})};
      calls.push({path, options});
      if (unauthorized) return {status:401};
      if (failure) throw Error('offline fixture');
      if (path === '/api/settings') {
        if (rejectSave) return {status:400,ok:false,json:async()=>({detail:'参数无效 fixture'})};
        Object.assign(state.settings, JSON.parse(options.body));
      } else assert.ok(['/api/state','/logout','/api/paper/reset'].includes(path));
      return {status:200,ok:true,json:async()=>path==='/api/state'?state:{message:'ok'}};
    },
    setInterval: (fn, ms) => {assert.equal(ms,15000);poll=fn;}
  };
  vm.runInNewContext(view.script, context);
  await new Promise(setImmediate);
  assert.equal(calls.filter(call=>call.path==='/api/state').length,1);
  await poll();
  assert.equal(calls.filter(call=>call.path==='/api/state').length,2);
  if (missing) {
    // Call rendering directly so refresh's error handler cannot hide a null dereference.
    vm.runInNewContext('render()', context);
    failure=true;await poll();return;
  }
  assert.equal(elements.notice.textContent,'');
  // Also remove each individual element while all other elements are present.
  for (const id of Object.keys(elements)) {
    const node=elements[id];delete elements[id];
    vm.runInNewContext('render()', context);
    elements[id]=node;
  }
  vm.runInNewContext('render()', context);
  assert.match(elements.portfolio.innerHTML,/账号余额<\/span><strong[^>]*>987\.65<\/strong>/);
  assert.doesNotMatch(elements.portfolio.innerHTML,/当前持仓价值|1,234\.50/);
  const originalAccount=state.meta.baw_account;
  for (const [account, expected] of [
    [{balance:[{symbol:'USDT',balance:0,value:100}]}, '0.00'],
    [{balance:[{symbol:'USDT',value:'321.09'}]}, '321.09'],
    [{balance:{balances:[{symbol:'USDT',balance:'42'}]}}, '42.00'],
    [{balance:[{symbol:'BTC',balance:5}]}, '—'],
    [{balance:[{symbol:'USDT'}]}, '—'],
    [{balance:[{symbol:'USDT',balance:'invalid'}]}, '—'],
    [{}, '—'], [null, '—']
  ]) {
    state.meta.baw_account=account;
    vm.runInNewContext('render()', context);
    assert.ok(elements.portfolio.innerHTML.includes('账号余额</span><strong class="number ">'+expected+'</strong>'));
  }
  state.meta.baw_account=originalAccount;
  vm.runInNewContext('render()', context);
  assert.match(elements.portfolio.innerHTML,/good[^>]*">\+12\.34/);
  assert.match(elements.portfolio.innerHTML,/bad[^>]*">-7\.66/);
  assert.match(elements.positions.innerHTML,/开盘价 12,345\.60 \/ 结算价格 12,360\.00/);
  assert.doesNotMatch(elements.positions.innerHTML,/当前价格|12,350\.10/);
  assert.doesNotMatch(elements.orders.innerHTML,/Audit fixture|审计|2,010\.50/);
  assert.match(elements.orders.innerHTML,/开盘价 3,000\.00 \/ 结束价格 3,010\.50/);
  assert.match(elements.orders.innerHTML,/开盘价 4,000\.00 \/ 结束价格 —<\/div>/);
  const originalAuditFills=state.orders_audit;
  for (const [status,error,label] of [['FAILED','未成交（场馆 FAILED）','失败'],['SUCCESS','部分成交 2.5/10','已提交']]) {
    state.orders_audit=[{market_id:'Fill audit',ts:2500,amount:10,status,error}];
    vm.runInNewContext('render()', context);
    assert.doesNotMatch(elements.orders.innerHTML,/Fill audit|审计/);
    assert.ok(!elements.orders.innerHTML.includes(error));
  }
  state.orders_audit=originalAuditFills;
  vm.runInNewContext('render()', context);
  assert.doesNotMatch(elements.orders.innerHTML,/当前价格|结算价格|2,999\.00|3,999\.00|4,999\.00/);
  // Directions render for projected and raw venue rows and paper orders; audits stay hidden.
  const directionOrders=state.orders, directionAudit=state.orders_audit, directionPaper=state.paper_orders;
  for (const [side,label,css] of [['Up','涨','up'],['Down','跌','down'],['UP','涨','up'],['DOWN','跌','down'],[null,'-',''],['BUY','-','']]) {
    const orderLabel=label==='-'?'—':'买'+label;
    for (const row of [{side},{outcomeName:side},{outcome:side},{side:'BUY',outcomeName:side}]) {
      state.orders=[{mode:'LIVE',title:'Direction venue',...row}];
      state.orders_audit=[];
      vm.runInNewContext('render()', context);
      assert.ok(elements.orders.innerHTML.includes('<span class="badge '+css+'">'+orderLabel+'</span>'));
    }
    state.orders=[];
    state.orders_audit=[{market_id:'Direction audit',side}];
    state.positions=[{mode:'LIVE',title:'Settled/redeemable position',side}];
    vm.runInNewContext('render()', context);
    assert.doesNotMatch(elements.orders.innerHTML,/Direction audit|审计/);
    assert.ok(elements.positions.innerHTML.includes('<span class="badge '+css+'">'+label+'</span>'));
    if(operator) {
      const originalSide=state.decisions[0].side;
      state.decisions[0].side=side;
      vm.runInNewContext('render()', context);
      assert.ok(elements.decisions.innerHTML.includes('<span class="badge '+css+'">'+label+'</span>'));
      state.decisions[0].side=originalSide;
    }
    delete state.orders;
    state.meta['cache:order_history'].data.orders=[{marketTopicTitle:'Raw venue',outcomeName:side}];
    state.meta['cache:orders'].data.orders=[];
    state.orders_audit=[];
    state.paper_orders=[{title:'Direction paper',side}];
    vm.runInNewContext('render()', context);
    assert.equal((elements.orders.innerHTML.match(new RegExp('<span class="badge '+css+'">'+orderLabel+'</span>','g'))||[]).length,2);
    if(label==='-')assert.match(elements.orders.innerHTML,/<span class="badge ">—<\/span>/);
  }
  state.meta['cache:order_history']=fixture.state.meta['cache:order_history'];
  state.meta['cache:orders']=fixture.state.meta['cache:orders'];
  if(directionOrders===undefined)delete state.orders;else state.orders=directionOrders;
  state.orders_audit=directionAudit;state.paper_orders=directionPaper;delete state.positions;
  vm.runInNewContext('render()', context);
  // Projected positions ignore legacy spot quotes for open and settled rows.
  for (const settlement of [{}, {settlement_price:null}, {settlement_price:'invalid'}, {settlement_price:0}, {settlement_price:12360}]) {
    state.positions=[{mode:'LIVE',title:'Projected position',open_price:12345.6,spot_price:12350.1,...settlement}];
    vm.runInNewContext('render()', context);
    assert.match(elements.positions.innerHTML,/开盘价 12,345\.60/);
    assert.doesNotMatch(elements.positions.innerHTML,/当前价格|12,350\.10/);
    const settled=settlement.settlement_price===0||settlement.settlement_price===12360;
    assert.equal((elements.positions.innerHTML.match(/结算价格/g)||[]).length,settled?1:0);
    if(settlement.settlement_price===0)assert.match(elements.positions.innerHTML,/结算价格 0\.00/);
  }
  delete state.positions;
  // Projected venue rows render while legacy audit payloads remain hidden.
  state.orders=[{mode:'LIVE',title:'Projected venue',open_price:5000,spot_price:5999,settlement_price:5010.5}];
  vm.runInNewContext('render()', context);
  assert.match(elements.orders.innerHTML,/开盘价 5,000\.00 \/ 结束价格 5,010\.50/);
  assert.doesNotMatch(elements.orders.innerHTML,/Audit fixture|审计|2,010\.50/);
  assert.doesNotMatch(elements.orders.innerHTML,/当前价格|结算价格|5,999\.00/);
  // Missing, null and invalid end prices never substitute the live spot price.
  const originalAuditPrices=state.orders_audit;
  for (const settlement of [{}, {settlement_price:null}, {settlement_price:'invalid'}, {settlement_price:0}]) {
    state.orders=[{mode:'LIVE',title:'Projected venue',open_price:5000,spot_price:5999,...settlement}];
    state.orders_audit=[{market_id:'Audit prices',open_price:2000,spot_price:2999,...settlement}];
    vm.runInNewContext('render()', context);
    assert.doesNotMatch(elements.orders.innerHTML,/当前价格|结算价格|5,999\.00|2,999\.00/);
    assert.equal((elements.orders.innerHTML.match(/结束价格/g)||[]).length,1);
    if(settlement.settlement_price===0)assert.match(elements.orders.innerHTML,/结束价格 0\.00/);
  }
  delete state.orders;state.orders_audit=originalAuditPrices;
  vm.runInNewContext('render()', context);
  assert.match(elements.positions.innerHTML,/badge live/);
  assert.match(elements.positions.innerHTML,/badge paper/);
  assert.match(elements.positions.innerHTML,/badge up[^>]*">涨/);
  assert.match(elements.positions.innerHTML,/badge down[^>]*">跌/);
  assert.match(elements.positions.innerHTML,/&lt;img onerror=bad&gt;/);
  assert.doesNotMatch(elements.positions.innerHTML,/<img/);
  assert.equal(elements['positions-count'].textContent,2);
  assert.equal(elements['orders-count'].textContent,5);
  assert.doesNotMatch(elements.orders.innerHTML,/Duplicate order/);
  assert.match(elements.orders.innerHTML,/No id one/);
  assert.match(elements.orders.innerHTML,/No id two/);
  assert.doesNotMatch(elements.orders.innerHTML,/&lt;audit failed&gt;|Audit fixture|审计/);
  assert.ok(elements.orders.innerHTML.indexOf('Paper position') < elements.orders.innerHTML.indexOf('Unique order'));
  assert.equal(elements['paper-pnl'],undefined);
  assert.match(elements.updated.textContent,/账户同步于/);
  // Enriched paper cards work for both the API projection and raw fallback.
  const detailOrders=state.orders, detailPaper=state.paper_orders, detailAudit=state.orders_audit;
  const detail={mode:'PAPER',title:'Paper <detail>',ts:3000,side:'DOWN',amount:10,price:.25,
    window_start:100,window_end:200,model_prob:.2,market_price:.3,edge:-.1,invert:true,
    status:'SETTLED',result:'WIN',realized_pnl:30,reason:'真钱订单已提交'};
  for(const projected of [true,false]) {
    if(projected)state.orders=[detail];else delete state.orders;
    state.paper_orders=[detail];state.orders_audit=[];
    vm.runInNewContext('render()',context);
    for(const label of ['窗口','入场价','模型概率','市价','优势','反买','盈亏','买跌','已结算','赢 · +30.00','Paper &lt;detail&gt;'])
      assert.ok(elements.orders.innerHTML.includes(label),label);
    assert.doesNotMatch(elements.orders.innerHTML,/真钱订单已提交|class="reason"/);
    assert.match(elements.orders.innerHTML,/0\.25/);
    assert.match(elements.orders.innerHTML,/-0\.10/);
  }
  // LIVE uses the same detail renderer in both guest and operator pages.
  const liveTitle='Bitcoin Up or Down - November 11, 10:25AM-10:30AM ET';
  state.orders=[{...detail,mode:'LIVE',title:liveTitle,reason:'真钱订单已提交'}];
  state.orders_audit=[{market_id:'Hidden audit',ts:9999}];
  vm.runInNewContext('render()',context);
  for(const label of ['模型概率','市价','优势','反买','买跌','盈亏','赢 · +30.00',liveTitle])
    assert.ok(elements.orders.innerHTML.includes(label),label);
  assert.doesNotMatch(elements.orders.innerHTML,/纸面盈亏|Hidden audit|审计|<reason>|真钱订单已提交|class="reason"/);
  const liveDetailMarkup=vm.runInNewContext('orderDetails({...state.orders[0],mode:"LIVE"})',context);
  const paperDetailMarkup=vm.runInNewContext('orderDetails({...state.orders[0],mode:"PAPER"})',context);
  assert.equal(liveDetailMarkup,paperDetailMarkup);
  let previousLabel=-1;
  for(const label of ['窗口','入场价','模型概率','市价','优势','反买','盈亏','开盘价','结束价格']) {
    const index=elements.orders.innerHTML.indexOf(label);
    assert.ok(index>previousLabel,label+' field order');previousLabel=index;
  }
  state.orders=[{mode:'LIVE',status:'FILLED'}];
  vm.runInNewContext('render()',context);
  for(const label of ['模型概率','市价','优势','反买'])
    assert.ok(elements.orders.innerHTML.includes(label+'</dt><dd'+(label==='优势'?' class=""':'')+'>—'));
  assert.match(elements.orders.innerHTML,/盈亏<\/dt><dd class="">—/);
  assert.doesNotMatch(elements.orders.innerHTML,/undefined|NaN/);
  state.orders=[{mode:'PAPER',status:'OPEN'}];
  vm.runInNewContext('render()',context);
  assert.match(elements.orders.innerHTML,/窗口 — → —/);
  assert.match(elements.orders.innerHTML,/<span class="badge ">—<\/span>/);
  assert.match(elements.orders.innerHTML,/入场价<\/dt><dd>—/);
  assert.match(elements.orders.innerHTML,/模型概率<\/dt><dd>—/);
  assert.doesNotMatch(elements.orders.innerHTML,/undefined|NaN|赢 ·|输 ·/);
  state.orders=[{...detail,result:'LOSS',realized_pnl:-10}];
  vm.runInNewContext('render()',context);
  assert.match(elements.orders.innerHTML,/输 · −10\.00/);
  if(detailOrders===undefined)delete state.orders;else state.orders=detailOrders;
  state.paper_orders=detailPaper;state.orders_audit=detailAudit;
  vm.runInNewContext('render()',context);
  // Limit the combined sources after sorting, including mixed timestamp formats.
  const originalMeta=state.meta, originalAudit=state.orders_audit, originalPaper=state.paper_orders;
  const ts=i=>i%3===0 ? (1700000000+i)*1000 : i%3===1 ? 1700000000+i : new Date((1700000000+i)*1000).toISOString();
  state.meta={...state.meta,
    'cache:order_history':{data:{orders:Array.from({length:8},(_,i)=>({
      orderId:'limit-'+i, marketTopicTitle:'Limited '+(i*3+1), createTime:ts(i*3+1)
    }))}},
    'cache:orders':{data:{orders:[]}}
  };
  state.orders_audit=Array.from({length:8},(_,i)=>({market_id:'Limited '+(i*3+2),ts:ts(i*3+2)}));
  state.paper_orders=Array.from({length:8},(_,i)=>({title:'Limited '+(i*3+3),ts:ts(i*3+3)}));
  vm.runInNewContext('render()', context);
  assert.equal(elements['orders-count'].textContent,10);
  assert.equal((elements.orders.innerHTML.match(/<li class="order">/g)||[]).length,10);
  assert.deepEqual(Array.from(elements.orders.innerHTML.matchAll(/<h3 class="market">Limited (\d+)<\/h3>/g),m=>Number(m[1])),
    [24,22,21,19,18,16,15,13,12,10]);
  state.meta=originalMeta;state.orders_audit=originalAudit;state.paper_orders=originalPaper;
  vm.runInNewContext('render()', context);
  failure=true;await poll();failure=false;
  assert.match(elements.notice.textContent,/offline fixture/);
  assert.match(elements.portfolio.innerHTML,/12\.34/);
  await poll();assert.equal(elements.notice.textContent,'');
  state.settings.mode='PAPER';
  await poll();
  assert.equal((elements.portfolio.innerHTML.match(/<div class="metric(?: major)?">/g)||[]).length,4);
  assert.match(elements.portfolio.innerHTML,/账号余额<\/span><strong[^>]*>93\.00<\/strong>/);
  assert.match(elements.portfolio.innerHTML,/累计盈亏<\/span><strong[^>]*>0\.00<\/strong>/);
  assert.match(elements.portfolio.innerHTML,/已实现盈亏<\/span><strong[^>]*>-2\.00<\/strong>/);
  assert.match(elements.portfolio.innerHTML,/未实现盈亏<\/span><strong[^>]*>\+2\.00<\/strong>/);
  assert.equal(elements['account-mode'].textContent,'模拟账户');
  if (operator) {
    assert.equal(elements.toggle.textContent,'启用引擎');
    assert.equal(elements.save.disabled,false);
    assert.match(elements.decisions.innerHTML,/Voter A/);
    assert.match(elements.decisions.innerHTML,/上涨概率 0\.72/);
    assert.match(elements.decisions.innerHTML,/弃权 · timeout/);
    assert.match(elements.decisions.innerHTML,/Timed out fixture/);
    assert.match(elements.decisions.innerHTML,/&lt;voter reason&gt;/);
    assert.match(elements.decisions.innerHTML,/&lt;decision reason&gt;/);
    elements.decisions.querySelectorAll=()=>[{dataset:{key:'7'}}];
    await poll();assert.match(elements.decisions.innerHTML,/data-key="7" open/);
    assert.match(elements.status.innerHTML,/&lt;wallet&gt;/);
    assert.match(elements.status.innerHTML,/权限待开通/);
    assert.match(elements.status.innerHTML,/&lt;engine failed&gt;/);
    assert.match(elements.status.innerHTML,/&lt;paper failed&gt;/);
    const marketWindow = elements.parameters.elements.find(f=>f.name==='market_window');
    for (const [minimum, maximum, selected] of [[300,300,300],[900,900,900],[900,1800,900],[300,900,300],[600,900,300],[0,31536000,300]]) {
      state.settings.min_duration=minimum;state.settings.max_duration=maximum;
      await poll();assert.equal(Number(marketWindow.value),selected);
    }
    state.settings.min_duration=900;state.settings.max_duration=900;
    await poll();assert.equal(Number(marketWindow.value),900);
    marketWindow.value='300';elements.parameters.onchange();await poll();
    assert.equal(Number(marketWindow.value),300);
    marketWindow.value='900';
    const invert = elements.parameters.elements.namedItem('invert_signal');
    assert.equal(invert.checked,false);
    invert.checked=true;
    const bet = elements.parameters.elements.find(f=>f.name==='bet_usdt');
    const maxOpen = elements.parameters.elements.find(f=>f.name==='max_open_positions');
    const slippage = elements.parameters.elements.find(f=>f.name==='slippage_bps');
    assert.equal(slippage.value,state.settings.slippage_bps);
    slippage.value='2500';
    assert.equal(maxOpen.value,state.settings.max_open_positions);
    maxOpen.value='50';
    bet.value='8.25';elements.parameters.oninput();await poll();
    assert.equal(bet.value,'8.25');
    await elements.parameters.onsubmit({preventDefault(){},target:elements.parameters});
    let saved=calls.filter(c=>c.path==='/api/settings').at(-1);
    assert.equal(saved.options.method,'POST');
    assert.equal(saved.options.headers['X-CSRF-Token'],state.csrf);
    assert.equal(saved.options.headers['Content-Type'],'application/json');
    const body=JSON.parse(saved.options.body);
    assert.equal(body.bet_usdt,8.25);assert.equal(body.mode,'PAPER');
    assert.equal(body.invert_signal,true);
    assert.equal(invert.checked,true);
    assert.equal(body.max_open_positions,50);
    assert.equal(body.slippage_bps,2500);
    assert.equal(body.min_duration,900);assert.equal(body.max_duration,900);
    assert.equal(body.market_window,undefined);
    assert.deepEqual(Object.keys(body).sort(),fixture.fields.filter(f=>f!=='market_window').concat(['min_duration','max_duration']).sort());
    for(const [key,value] of Object.entries(body))if(key!=='mode')assert.equal(typeof value,key==='invert_signal'?'boolean':'number');
    assert.equal(elements['save-state'].textContent,'参数已保存');
    invert.checked=false;elements.parameters.onchange();
    await elements.parameters.onsubmit({preventDefault(){},target:elements.parameters});
    assert.equal(JSON.parse(calls.filter(c=>c.path==='/api/settings').at(-1).options.body).invert_signal,false);
    assert.equal(invert.checked,false);
    rejectSave=true;bet.value='9';elements.parameters.oninput();
    await elements.parameters.onsubmit({preventDefault(){},target:elements.parameters});
    assert.equal(elements['save-state'].textContent,'参数无效 fixture');
    assert.equal(bet.value,'9');rejectSave=false;
    await elements.toggle.onclick();
    assert.deepEqual(JSON.parse(calls.filter(c=>c.path==='/api/settings').at(-1).options.body),{engine_enabled:true});
    assert.equal(elements.toggle.textContent,'停止引擎');
    assert.equal(elements.toggle.disabled,false);
    const beforeReset=calls.length;
    await elements['paper-reset'].onclick();assert.equal(calls.length,beforeReset);
    confirmed=true;await elements['paper-reset'].onclick();
    assert.equal(calls.at(-2).path,'/api/paper/reset');
    assert.equal(calls.at(-2).options.method,'POST');
    assert.equal(calls.at(-2).options.headers['X-CSRF-Token'],state.csrf);
    assert.equal(calls.at(-1).path,'/api/state');
    assert.equal(elements['paper-reset'].disabled,false);
    await elements.logout.onclick();
    assert.equal(calls.at(-1).path,'/logout');
    assert.equal(calls.at(-1).options.body,'{}');
    assert.equal(calls.at(-1).options.headers['X-CSRF-Token'],state.csrf);
    assert.equal(context.location,'/login');
  } else assert.equal(elements.parameters,undefined);
  unauthorized=true;await poll();assert.equal(context.location,'/login');
}
(async()=>{
  for (let i=0;i<fixture.views.length;i++) {
    await exercise(fixture.views[i],i===1);
    await exercise(fixture.views[i],i===1,true);
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
'''
        result = subprocess.run(['node', '-e', harness], input=fixture, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(shutil.which('node'), 'Node is needed to execute the dashboard JavaScript')
    async def test_stripped_javascript_refreshes_data(self):
        self.store.set_meta('baw_account', {'balance': [{'symbol': 'USDT', 'balance': '42.5'}]})
        self.store.update_settings({'mode': 'LIVE'})
        self.store.set_meta('pnl_portfolio', {'totalPnl': 12.34})
        self.store.set_meta('positions', [{'marketTitle': 'Position fixture', 'shares': 2}])
        self.store.execute("INSERT INTO decisions (ts, market, action) VALUES (?, ?, ?)", (1, 'Decision fixture', 'SKIP'))
        async with self.web('1') as web:
            html = (await web.get('/')).text
            state = (await web.get('/api/state')).json()
        # Execute the served script against a DOM containing only the served ids.
        # Missing operator elements return null, just as they do in the browser.
        script = re.search(r'<script>(.*?)</script>', html, re.S).group(1)
        fixture = json.dumps({'ids': re.findall(r'id="([^"]+)"', html), 'script': script, 'state': state})
        harness = r'''
const fs = require('fs'), vm = require('vm'), assert = require('assert');
const fixture = JSON.parse(fs.readFileSync(0, 'utf8'));
const elements = Object.fromEntries(fixture.ids.map(id => [id, {innerHTML:'', textContent:''}]));
let poll, calls = 0;
const context = {
  document: {getElementById: id => elements[id] || null, activeElement: null},
  fetch: async path => {
    assert.equal(path, '/api/state'); calls++;
    return {status:200, ok:true, json:async () => fixture.state};
  },
  setInterval: (fn, ms) => {assert.equal(ms, 15000); poll = fn;}
};
(async () => {
  vm.runInNewContext(fixture.script, context);
  await new Promise(setImmediate);
  assert.equal(calls, 1);
  await poll();
  assert.equal(calls, 2);
  assert.equal(elements.notice.textContent, '');
  assert.match(elements.portfolio.innerHTML, /12\.34/);
  assert.match(elements.portfolio.innerHTML, /42\.50/);
  assert.match(elements.positions.innerHTML, /Position fixture/);
  assert.match(elements.orders.innerHTML, /暂无记录/);
  assert.equal(elements['paper-pnl'], undefined);
  for (const id of ['mode', 'budget', 'status', 'toggle', 'parameters', 'decisions']) {
    assert.equal(elements[id], undefined);
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
'''
        result = subprocess.run(['node', '-e', harness], input=fixture, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)


def deny_network(*args, **kwargs):
    raise AssertionError('Offline dashboard suite forbids sockets and DNS')


if __name__ == '__main__':
    with patch.object(socket.socket, 'connect', deny_network), \
         patch.object(socket.socket, 'connect_ex', deny_network), \
         patch.object(socket, 'getaddrinfo', deny_network):
        unittest.main(verbosity=2)
