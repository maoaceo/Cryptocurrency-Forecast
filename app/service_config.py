"""External service configuration and safe operator projection."""
import copy
import os
from urllib.parse import urlsplit

BLOCKS = ('providers', 'search_api', 'model_apis', 'baw_bridge', 'binance_api')
SECRETS = {'api_key', 'api_secret', 'bridge_token'}
MODEL_DEFAULTS = [dict(id=identity, name=model, provider_id='', model='', enabled=None)
                  for identity, model in [('cpa_astra','gpt-6-astra'), ('cpa_sol','gpt-6.1-sol'), ('workbuddy','deepseek-v4.1-flash')]]
DEFAULTS = dict(providers=[], search_api=dict(provider='', provider_id='', model='', enabled=None), model_apis=MODEL_DEFAULTS,
                baw_bridge=dict(enabled=None), binance_api=dict(api_key='', api_secret='', enabled=None))


def defaults():
    return copy.deepcopy(DEFAULTS)


def migrate_settings(settings):
    """Project legacy inline credentials into stable, shared provider entries."""
    result = copy.deepcopy(settings)
    providers = result.setdefault('providers', [])
    slots = [('search', result.get('search_api', {}))]
    slots += [(item.get('id', str(index)), item) for index, item in enumerate(result.get('model_apis', []))]
    for label, slot in slots:
        if slot.get('base_url') or slot.get('api_key'):
            provider = next((p for p in providers if p.get('base_url', '') == slot.get('base_url', '')
                             and p.get('api_key', '') == slot.get('api_key', '')), None)
            if not slot.get('provider_id'):
                if provider is None:
                    identity = 'legacy_' + label
                    while any(p['id'] == identity for p in providers):
                        identity += '_'
                    provider = dict(id=identity, name=slot.get('name') or label,
                                    base_url=slot.get('base_url', ''), api_key=slot.get('api_key', ''),
                                    enabled=True, models=[])
                    providers.append(provider)
                slot['provider_id'] = provider['id']
                if slot.get('model') and slot['model'] not in provider['models']:
                    provider['models'].append(slot['model'])
        slot.pop('base_url', None)
        slot.pop('api_key', None)
        slot.setdefault('provider_id', '')
    return result


def _provider(settings, slot):
    return next((p for p in settings.get('providers', []) if p.get('id') == slot.get('provider_id')), {})


def validate_slots(settings):
    slots = [('model_apis[' + s['id'] + ']', s) for s in settings.get('model_apis', [])]
    search = settings.get('search_api', {})
    if resolve_search(settings)['provider'] == 'openai_compatible':
        slots.append(('search_api', search))
    for label, slot in slots:
        if slot.get('enabled') is not True:
            continue
        identity = slot.get('provider_id')
        provider = _provider(settings, slot)
        if not identity or not provider or not provider.get('api_key'):
            raise ValueError(f'{label} 服务商 {identity or "（未选择）"} 缺少有效 provider_id 引用或 api_key，请先选择服务商并在服务商卡片配置密钥')


def _enabled(config, key):
    return config['enabled'] if config.get('enabled') is not None else os.getenv(key, 'true').lower() not in ('0','false','no','off')


def resolve_search(settings):
    source = settings.get('search_api', {})
    provider = _provider(settings, source)
    result = {key: (provider.get(key) if key in ('base_url', 'api_key') else None) or source.get(key) or os.getenv('SEARCH_API_' + key.upper(), 'vertex_grounding' if key == 'provider' else '')
              for key in ('provider','base_url','api_key','model')}
    result['enabled'] = _enabled(source, 'SEARCH_API_ENABLED')
    if result['provider'] == 'openai_compatible' and provider.get('enabled') is False:
        result['enabled'] = False
    return result


def resolve_models(settings):
    result = []
    fallbacks = {'cpa_astra': ('CPA','CPA_ASTRA_MODEL','gpt-6-astra','CPA_ASTRA_ENABLED'),
                 'cpa_sol': ('CPA','CPA_SOL_MODEL','gpt-6.1-sol','CPA_SOL_ENABLED'),
                 'workbuddy': ('WB','WB_MODEL','deepseek-v4.1-flash','WB_ENABLED')}
    for source in settings.get('model_apis', MODEL_DEFAULTS):
        item = dict(source)
        prefix, model_key, model, enabled_key = fallbacks.get(item.get('id'), ('','','',''))
        for field in ('base_url','api_key'):
            item[field] = _provider(settings, source).get(field) or item.get(field) or (os.getenv(prefix+'_'+field.upper(), '') if prefix else '')
        item['model'] = item.get('model') or os.getenv(model_key, model)
        item['name'] = item.get('name') or item['model']
        item['enabled'] = _enabled(item, enabled_key) and _provider(settings, source).get('enabled') is not False
        result.append(item)
    return result


def _resolve(settings, block, fields, enabled_key):
    source = settings.get(block, {})
    result = {key: source.get(key) or os.getenv(env, default) for key, env, default in fields}
    result['enabled'] = _enabled(source, enabled_key)
    return result


def resolve_baw(settings):
    return _resolve({'baw_bridge': {'enabled': settings.get('baw_bridge', {}).get('enabled')}}, 'baw_bridge', [('bridge_url','BAW_BRIDGE_URL','http://host.docker.internal:8791'), ('bridge_token','BAW_BRIDGE_TOKEN','')], 'BAW_BRIDGE_ENABLED')


def resolve_binance(settings):
    return _resolve(settings, 'binance_api', [('api_key','BINANCE_API_KEY',''), ('api_secret','BINANCE_API_SECRET','')], 'BINANCE_API_ENABLED')


def safe_settings(settings):
    result = copy.deepcopy(settings)
    resolved = {'search_api': [resolve_search(settings)], 'model_apis': resolve_models(settings),
                'binance_api': [resolve_binance(settings)], 'providers': settings.get('providers', [])}
    for block in BLOCKS:
        entries = result.get(block, []) if block in ('model_apis', 'providers') else [result.get(block, {})]
        for index, entry in enumerate(entries):
            if block == 'providers':
                entry['api_key_set'] = bool(entry.get('api_key'))
                entry['api_key'] = ''
                continue
            if block == 'baw_bridge':
                entry.pop('bridge_url', None)
                entry.pop('bridge_token', None)
            for field in SECRETS & entry.keys():
                entry[field+'_set'] = bool(entry[field] or resolved.get(block, [{}])[index].get(field))
                entry[field] = ''
    return result


def _url(value, label):
    if not value:
        return
    try:
        if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError()
        parsed = urlsplit(value)
        if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError()
        parsed.port
    except ValueError:
        raise ValueError(label+' 必须是有效 http(s) URL，不能包含认证信息、查询串或片段') from None


def normalize_block(block, value, current):
    def entry(source, previous, schema, label):
        if not isinstance(source, dict):
            raise ValueError(label+' 必须是对象')
        unknown = set(source)-set(schema)-{key+'_set' for key in SECRETS}
        if unknown:
            raise ValueError(label+' 未知字段: '+', '.join(sorted(unknown)))
        result = {}
        for key, default in schema.items():
            raw = source.get(key, default)
            if key == 'models':
                if not isinstance(raw, list) or any(not isinstance(model, str) for model in raw):
                    raise ValueError(label+'.models 必须是模型 id 文本列表')
                raw = list(dict.fromkeys(raw))
            elif key == 'enabled':
                if raw is not None and not isinstance(raw, bool):
                    raise ValueError(label+'.enabled 必须是布尔值')
            else:
                if raw is None and key in SECRETS:
                    raw = ''
                elif not isinstance(raw, str):
                    raise ValueError(label+'.'+key+' 必须是文本')
                elif key in SECRETS and raw == '':
                    raw = previous.get(key, '')
                raw = raw.strip()
            result[key] = raw
        return result
    def require(config, keys, label):
        missing = [key for key in keys if not config.get(key)]
        if missing:
            raise ValueError(label+' 已启用，缺少: '+', '.join(missing)+'（可填设置或环境变量）')
    if block == 'providers':
        if not isinstance(value, list) or len(value) > 100:
            raise ValueError('providers 必须是最多100条的列表')
        previous = {p['id']: p for p in current.get(block, [])}
        result, identities = [], set()
        for index, source in enumerate(value):
            identity = source.get('id', '') if isinstance(source, dict) else ''
            item = entry(source, previous.get(identity, {}) if isinstance(identity, str) else {},
                         dict(id='', name='', base_url='', api_key='', enabled=None, models=[]), f'providers[{index}]')
            if not item['id'] or item['id'] in identities:
                raise ValueError('providers.id 必须非空且唯一')
            identities.add(item['id'])
            _url(item['base_url'], 'providers['+item['id']+'].base_url')
            result.append(item)
        return result
    if block == 'model_apis':
        if not isinstance(value, list) or len(value)>20:
            raise ValueError('model_apis 必须是最多20条的列表')
        previous = {item['id']: item for item in current[block]}
        result, identities = [], set()
        for index, source in enumerate(value):
            identity = source.get('id','') if isinstance(source, dict) else ''
            if not isinstance(identity, str):
                raise ValueError(f'model_apis[{index}].id 必须是文本')
            identity = identity.strip()
            item = entry(source, previous.get(identity, {}), dict(id='',name='',provider_id='',base_url='',api_key='',model='',enabled=None), f'model_apis[{index}]')
            if not identity or identity in identities:
                raise ValueError('model_apis.id 必须非空且唯一')
            identities.add(identity)
            resolved = resolve_models(current | {'model_apis':[item]})[0]
            if item['enabled'] is not False:
                _url(resolved['base_url'], identity+'.base_url')
            result.append(item)
        return result
    schema = DEFAULTS[block]
    if block == 'search_api':
        schema = schema | dict(base_url='', api_key='')
    result = entry(value, current[block], schema, block)
    if result.get('enabled') is False:
        return result
    if block == 'search_api':
        resolved = resolve_search(current | {block:result})
        if resolved['provider'] not in ('vertex_grounding','openai_compatible'):
            raise ValueError('search_api.provider 必须为 vertex_grounding 或 openai_compatible')
        _url(resolved['base_url'], block+'.base_url')
    else:
        resolved = resolve_baw({block:result}) if block=='baw_bridge' else resolve_binance({block:result})
        if block=='baw_bridge':
            _url(resolved['bridge_url'], block+'.bridge_url')
        if result.get('enabled') is True:
            require(resolved, ('bridge_url','bridge_token') if block=='baw_bridge' else ('api_key','api_secret'), block)
    return result
