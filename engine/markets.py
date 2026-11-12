"""Conservative normalization of Binance topic and tradable variant schemas."""
import re
import time
from datetime import datetime


SYMBOL_TOPICS = {'BTCUSDT': r'\b(?:Bitcoin|BTC(?:USDT)?)\b',
                 'ETHUSDT': r'\b(?:Ethereum|ETH(?:USDT)?)\b'}


def market_symbol(detail):
    """Prefer explicit feed metadata; infer only an unambiguous named asset."""
    symbol = detail.get('symbol') or (detail.get('variantData') or {}).get('priceFeedSymbol')
    if symbol:
        return str(symbol).strip().upper()
    text = str(detail.get('title', '')) + ' ' + str(detail.get('slug', ''))
    matches = [symbol for symbol, pattern in SYMBOL_TOPICS.items() if re.search(pattern, text, re.I)]
    return matches[0] if len(matches) == 1 else ''


def unwrap(value):
    return value.get('data', value) if isinstance(value, dict) else value


def timestamp(value):
    if value is None:
        return 0.0
    try:
        result = float(value)
        return result / 1000 if result > 10**11 else result
    except (ValueError, TypeError):
        try:
            return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
        except ValueError:
            return 0.0


def outcome_direction(outcome, variant):
    """UP/DOWN tokens directly, or YES/NO relative to a directional proposition."""
    name = str(outcome.get('name', '')).strip().upper()
    if name in ('UP', '上涨', '涨'):
        return 'UP'
    if name in ('DOWN', '下跌', '跌'):
        return 'DOWN'
    proposition = str(variant.get('title', '')) + ' ' + str(variant.get('question', ''))
    # An ambiguous "Up or Down" title cannot establish what YES means.
    has_up = bool(re.search(r'\bUP\b', proposition, re.I))
    has_down = bool(re.search(r'\bDOWN\b', proposition, re.I))
    if name in ('YES', 'NO') and has_up != has_down:
        up = has_up if name == 'YES' else not has_up
        return 'UP' if up else 'DOWN'
    return None


def normalize_markets(detail, now=None, settings=None):
    now, settings = (time.time() if now is None else now), (settings or {})
    detail = unwrap(detail)
    if not isinstance(detail, dict):
        return []
    symbol = market_symbol(detail)
    if symbol not in set(settings.get('symbols') or ('BTCUSDT', 'ETHUSDT')):
        return []
    kind = str(detail.get('chartType') or detail.get('variantData', {}).get('type', ''))
    text = str(detail.get('title', '')) + ' ' + str(detail.get('slug', ''))
    if kind != 'CRYPTO_UP_DOWN' and not re.search(r'up[\s-]*(?:or[\s-]*)?down', text, re.I):
        return []
    result = []
    for variant in detail.get('variants', detail.get('markets', [])):
        if str(variant.get('tradingStatus', variant.get('status', ''))).upper() != 'OPEN':
            continue
        start = timestamp(variant.get('startDate', variant.get('startTime', detail.get('startDate', detail.get('startTime')))))
        end = timestamp(variant.get('endDate', variant.get('endTime', detail.get('endDate', detail.get('endTime')))))
        duration = end - start
        if duration <= 0 or start > now + 90 or end - now <= .25 * duration:
            continue
        if duration < float(settings.get('min_duration', 0)) or duration > float(settings.get('max_duration', 86400 * 365)):
            continue
        outcomes = []
        for outcome in variant.get('outcomes', []):
            direction = outcome_direction(outcome, variant)
            try:
                price = float(outcome.get('price', outcome.get('chance')))
            except (ValueError, TypeError):
                continue
            if direction and outcome.get('tokenId') and 0 < price < 1:
                outcomes.append({**outcome, 'name': direction, 'original_name': outcome.get('name'), 'price': price})
        if {x['name'] for x in outcomes} != {'UP', 'DOWN'}:
            continue
        market_id = variant.get('marketId')
        if market_id is None:
            continue
        result.append({'topic_id': detail.get('marketTopicId'), 'market_id': market_id,
                       'title': detail.get('title') or detail.get('question') or f'{symbol} Up or Down', 'symbol': symbol,
                       'start_time': start, 'end_time': end, 'pending': start > now, 'outcomes': outcomes,
                       'slippage_bps': int(variant.get('slippageBps', detail.get('slippageBps', 1200))),
                       'vendor': detail.get('vendor', 'PREDICT_FUN'),
                       'start_price': detail.get('variantData', {}).get('startPrice'),
                       'question': variant.get('question', detail.get('question', '')), 'raw': variant})
    return result


async def select_markets(client, settings):
    selected, offset = [], 0
    allowed_symbols = set(settings.get('symbols') or ('BTCUSDT', 'ETHUSDT'))
    # Pagination bounded to avoid a malfunctioning upstream creating an endless scan.
    for _ in range(20):
        # END_DATE is a documented enum. Omit optional l2Category: category IDs
        # can change independently, and detail chartType supplies the strict filter.
        page = unwrap(await client.market_list(l1Category='crypto',
                    sortBy='END_DATE', orderBy='ASC', offset=offset, limit=100))
        topics = page if isinstance(page, list) else page.get('marketTopics', page.get('markets', []))
        for topic in topics:
            # Only explicit metadata is safe for the list prefilter: detail may
            # supply a feed symbol even when a summary title is ambiguous.
            symbol = str(topic.get('symbol') or (topic.get('variantData') or {}).get('priceFeedSymbol') or '').strip().upper()
            if symbol and symbol not in allowed_symbols:
                continue
            topic_id = topic.get('marketTopicId')
            if topic_id is None:
                continue
            detail = unwrap(await client.market_detail(topic_id))
            selected.extend(normalize_markets({**topic, **detail}, settings=settings))
        if isinstance(page, list) or not page.get('hasMore', len(topics) == 100):
            break
        offset += len(topics)
        if not topics:
            break
    # Keep every eligible open market, but only the nearest future window per asset.
    upcoming = {}
    for market in selected:
        if market['pending']:
            symbol = market['symbol']
            if symbol not in upcoming or market['start_time'] < upcoming[symbol]['start_time']:
                upcoming[symbol] = market
    selected = [m for m in selected if not m['pending']] + list(upcoming.values())
    return sorted(selected, key=lambda x: x['end_time'])
