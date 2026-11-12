"""Best-effort account snapshots. Partial failures never overwrite last good data."""
import asyncio
import logging
import math
import re
import time
from datetime import datetime,timezone
from decimal import Decimal, InvalidOperation
from engine.markets import market_symbol, timestamp
from app.risk import mark_paper, settle_paper, settle_paper_outcome
from app.baw_client import BawClient, executor_path

logger=logging.getLogger(__name__)


def unwrap(value):
    return value.get('data',value) if isinstance(value,dict) else value

def items(value,keys=()):
    value=unwrap(value)
    if isinstance(value,list):return value
    if isinstance(value,dict):
        for key in (*keys,'list','rows','items','records'):
            if isinstance(value.get(key),list):return value[key]
    return []

def finite(value):
    try:
        n=float(value)
        return n if math.isfinite(n) else None
    except (TypeError,ValueError):return None

def slug_window(slug):
    """Decode the venue's complete BTC/ETH up/down window slug."""
    match = re.fullmatch(r'(btc|eth)-updown-(\d+)(m|h|d)-(\d+)', str(slug or ''))
    if not match:
        return None
    symbol, duration, unit, epoch = match.groups()
    start = int(epoch)
    seconds = int(duration) * {'m': 60, 'h': 3600, 'd': 86400}[unit]
    if start <= 0 or seconds <= 0:
        return None
    return symbol.upper() + 'USDT', start, start + seconds

class Sync:
    def __init__(self,store,client,baw_client=None):
        self.store=store;self.client=client;self.lock=asyncio.Lock()
        self.baw_client=baw_client or BawClient(store=store)
    @property
    def path(self):
        return executor_path(self.store.settings())
    @property
    def account_client(self):
        return self.baw_client if self.path=='baw' else self.client
    async def _call(self,name,method,**params):
        try:
            result=unwrap(await method(**params))
            self.store.set_meta(name,result)
            self.store.set_meta(name+'_status',{'ts':time.time(),'error':None,'permission_pending':False})
            return result,True
        except Exception as exc:
            old=self.store.meta(name+'_status',{})
            self.store.set_meta(name+'_status',{'ts':old.get('ts',0),'last_attempt':time.time(),'error':str(exc),'permission_pending':getattr(exc,'code',None)==-1002})
            return self.store.meta(name),False
    async def _pages(self,name,method,**params):
        """Use documented offset/limit and reject truncated account snapshots."""
        records=[];summary={};offset=0
        try:
            for _ in range(100):
                page=unwrap(await method(offset=offset,limit=100,**params))
                if not isinstance(page,(dict,list)):raise ValueError('账户响应格式无效')
                if isinstance(page,dict) and not any(isinstance(page.get(k),list) for k in ('positions','orders','settledPositions','settledHistory','history','list','rows','items','records')):raise ValueError('账户响应缺少列表字段')
                batch=items(page,('positions','orders','settledPositions','settledHistory','history'))
                if isinstance(page,dict):summary.update(page.get('summary',{}))
                records.extend(batch)
                has_more=(page.get('hasMore') if isinstance(page,dict) else None)
                total=finite(page.get('total')) if isinstance(page,dict) else None
                if has_more is False or (total is not None and len(records)>=total) or (has_more is None and len(batch)<100):break
                if not batch:raise ValueError('分页响应没有进展')
                offset+=len(batch)
            else:raise ValueError('账户分页超过安全上限')
            result={'list':records,'summary':summary,'complete':True}
            self.store.set_meta(name,result)
            self.store.set_meta(name+'_status',{'ts':time.time(),'error':None,'permission_pending':False})
            return result,True
        except Exception as exc:
            old=self.store.meta(name+'_status',{})
            self.store.set_meta(name+'_status',{'ts':old.get('ts',0),'last_attempt':time.time(),'error':str(exc),'permission_pending':getattr(exc,'code',None)==-1002})
            return self.store.meta(name),False
    async def run_once(self):
        async with self.lock:
            if self.path=='baw':return await self._baw_once()
            wallet_data,wallet_ok=await self._call('wallets',self.client.wallet_list)
            wallets=items(wallet_data,('wallets','walletList'))
            if isinstance(wallet_data,dict) and wallet_data.get('walletAddress'):wallets=[wallet_data]
            # Keep a selected wallet stable; changing wallet while orders exist is unsafe.
            wallet=self.store.meta('wallet',{})
            selected=next((w for w in wallets if w.get('walletId')==wallet.get('walletId')),None)
            if selected is None and not wallet:selected=next((w for w in wallets if w.get('walletAddress') and w.get('walletId')),None)
            if selected:
                wallet=selected;self.store.set_meta('wallet',wallet)
            checks=[wallet_ok]
            if wallet.get('walletAddress') and wallet.get('walletId'):
                params={'walletAddress':wallet['walletAddress']}
                open_positions,ok=await self._pages('positions',self.client.position_list,**params,tab='ONGOING')
                if ok:self._prune_placed_reservations(open_positions)
                checks.append(ok)
                redeemable,ok=await self._pages('positions_redeemable',self.client.position_list,**params,tab='PENDING_CLAIM')
                checks.append(ok)
                ended,ok=await self._pages('positions_ended',self.client.position_list,**params,tab='ENDED')
                checks.append(ok)
                orders,ok=await self._pages('orders',self.client.order_list,**params);checks.append(ok)
                history,ok=await self._pages('order_history',self.client.order_history,**params);checks.append(ok)
                if ok:self._reconcile_order_audit(items(history,('orders',)))
                today=datetime.now(timezone.utc).strftime('%Y-%m-%d')
                earliest=self.store.rows('SELECT MIN(ts) AS ts FROM reservations')
                since=datetime.fromtimestamp(earliest[0]['ts'],timezone.utc).strftime('%Y-%m-%d') if earliest and earliest[0]['ts'] else today
                settled,ok=await self._pages('settled_history',self.client.settled_history,**params,startDate=since,endDate=today);checks.append(ok)
                portfolio,ok=await self._call('pnl_portfolio',self.client.pnl_portfolio,**params);checks.append(ok)
                _,ok=await self._call('pnl_query',self.client.pnl_query,**params);checks.append(ok)
                _,ok=await self._call('payment_options',self.client.payment_options,**params);checks.append(ok)
                # These fields are explicitly today's realized PnL and total open PnL.
                realized=finite((open_positions or {}).get('summary',{}).get('todayRealizedPnl'))
                unrealized=finite((portfolio or {}).get('totalUnrealizedPnl')) if isinstance(portfolio,dict) else None
                if all(checks) and realized is not None and unrealized is not None:
                    self.store.set_meta('live_daily_pnl',{'ts':time.time(),'value':realized+unrealized,'day':today})
                if all(checks):self._reconcile(items(orders),items(open_positions)+items(redeemable),items(history),items(settled))
            await self._settlement_prices()
            status_keys=['wallets','positions','positions_redeemable','positions_ended','orders','order_history','settled_history','pnl_portfolio','pnl_query','payment_options']
            statuses={k:self.store.meta(k+'_status',{}) for k in status_keys}
            previous=self.store.meta('sync_state',{})
            state={'last_attempt':time.time(),'last_success':time.time() if len(checks)>1 and all(checks) else previous.get('last_success',0),
                   'permission_pending':any(v.get('permission_pending') for v in statuses.values()),
                   'errors':{k:v['error'] for k,v in statuses.items() if v.get('error')},'endpoints':statuses}
            self.store.set_meta('sync_state',state)
            await self._paper_poll()
            return state
    async def _baw_once(self):
        client=self.account_client
        wallet_address=None
        async def account_snapshot():
            data=await client.account()
            if not isinstance(data,dict):raise ValueError('baw account response is incomplete')
            return data

        account,account_ok=await self._call('baw_account',account_snapshot)

        def account_data(key):
            if not account_ok or not isinstance(account,dict):
                raise ValueError('baw account unavailable')
            return unwrap(account.get(key))

        pnl=account_data('pnl') if account_ok else None
        pnl_rows=pnl.get('pnlList') if isinstance(pnl,dict) else None
        if isinstance(pnl_rows,list):
            redeemed=self.store.meta('redeemed_token_ids',[])
            seen={str(token) for token in redeemed}
            winners=[]
            for row in pnl_rows:
                if not isinstance(row,dict):continue
                token=row.get('tokenId')
                shares=finite(row.get('currentShares'))
                if (row.get('isResolved') is True and row.get('isWinner') is True
                        and shares is not None and shares>0 and token is not None and str(token) not in seen):
                    winners.append(str(token));seen.add(str(token))
            if winners:
                try:
                    result=await client.redeem(winners)
                    self.store.set_meta('redeemed_token_ids',(redeemed+winners)[-200:])
                    last={'ts':time.time(),'count':len(winners)}
                    if isinstance(result,dict) and result.get('batchId') is not None:last['batchId']=result['batchId']
                    self.store.set_meta('last_redeem',last)
                    self.store.set_meta('redeem_error',None)
                except Exception as exc:
                    self.store.set_meta('redeem_error',{'ts':time.time(),'error':str(exc)})

        def amount(row,keys):
            value=next((finite(row[k]) for k in keys if finite(row.get(k)) is not None),None)
            if value is None:raise ValueError('baw position value/cost unavailable for PnL')
            return value

        async def status():
            nonlocal wallet_address
            data=account_data('status')
            if not isinstance(data,dict) or data.get('status')!='CONNECTED':
                raise ValueError('baw wallet is not CONNECTED')
            wallet_address=data.get('walletAddress')
            return data

        async def positions():
            data=account_data('positions')
            if not isinstance(data,dict) or not isinstance(data.get('positions'),list) or not isinstance(data.get('summary'),dict):
                raise ValueError('baw positions response is incomplete')
            rows=[]
            for row in data['positions']:
                pnl=finite(row.get('unrealizedPnl'))
                if pnl is None:
                    pnl=amount(row,('value','currentValue','positionValue'))-amount(row,('totalCost','cost','costBasis','initialValue'))
                if not math.isfinite(pnl):raise ValueError('baw PnL is not finite')
                rows.append({**row,'pnl':pnl})
            return {'positions':rows,'list':rows,'summary':data['summary']}

        async def history():
            data=account_data('orders')
            if not isinstance(data,dict) or not isinstance(data.get('orders'),list):
                raise ValueError('baw orders response is incomplete')
            total=finite(data.get('total'))
            if total is None or total<len(data['orders']):
                raise ValueError('baw orders total is invalid')
            # History is a newest-first display page, not an accounting snapshot.
            if total>len(data['orders']):
                return {**data,'truncated':True}
            return data

        async def balance():
            nonlocal wallet_address
            data=account_data('balance')
            rows=data if isinstance(data,list) else data.get('balances') if isinstance(data,dict) else None
            if not isinstance(rows,list):raise ValueError('baw balance response is incomplete')
            # Only chain 56 USDT is spendable by this execution path.
            usdt=[finite(r.get('balance')) for r in rows if r.get('symbol')=='USDT' and str(r.get('binanceChainId'))=='56']
            if len(usdt)!=1 or usdt[0] is None or usdt[0]<0:raise ValueError('baw USDT balance unavailable')
            address=data.get('walletAddress') if isinstance(data,dict) else None
            if not address:
                address=next((r.get('walletAddress') for r in rows if r.get('walletAddress')),None)
            if address:
                wallet_address=address
                self.store.set_meta('wallet',{'walletAddress':address})
            return {'items':[{'accountType':'CeDeFi','availableBalanceDisplay':str(usdt[0])}]}

        checks=[account_ok]
        _,ok=await self._call('baw_status',status);checks.append(ok)
        snapshot,positions_ok=await self._call('positions',positions);checks.append(positions_ok)
        if positions_ok:self._prune_placed_reservations(snapshot)
        orders,ok=await self._call('order_history',history);checks.append(ok)
        if ok:self._reconcile_order_audit(items(orders,('orders',)))
        _,ok=await self._call('payment_options',balance);checks.append(ok)

        async def portfolio():
            if not positions_ok:raise ValueError('baw positions unavailable for PnL')
            rows=snapshot['positions']
            cost=math.fsum(amount(row,('totalCost','cost','costBasis','initialValue')) for row in rows)
            value=math.fsum(amount(row,('value','currentValue','positionValue')) for row in rows)
            if pnl is not None and not isinstance(pnl,dict):
                raise ValueError('baw PnL response is invalid')
            if pnl_rows is not None and not isinstance(pnl_rows,list):
                raise ValueError('baw pnlList is invalid')
            if pnl_rows:
                realized_values=[];unrealized_values=[];total_values=[]
                for row in pnl_rows:
                    if not isinstance(row,dict):raise ValueError('baw PnL entry is invalid')
                    realized=finite(row.get('realizedPnl'))
                    unrealized=finite(row['unrealizedPnl']) if row.get('unrealizedPnl') is not None else 0
                    total=finite(row['totalPnl']) if row.get('totalPnl') is not None else None
                    if realized is None or unrealized is None or (row.get('totalPnl') is not None and total is None):
                        raise ValueError('baw PnL entry is incomplete or not finite')
                    realized_values.append(realized);unrealized_values.append(unrealized)
                    total_values.append(total if total is not None else realized+unrealized)
                realized=math.fsum(realized_values)
                unrealized=math.fsum(unrealized_values)
                total=math.fsum(total_values)
            else:
                unrealized=math.fsum(row['pnl'] for row in rows)
                realized=math.fsum(finite(row.get('realizedPnl')) or 0 for row in rows)
                total=realized+unrealized
            totals={'totalCostBasis':cost,'totalCurrentValue':value,'totalUnrealizedPnl':unrealized,
                    'totalRealizedPnl':realized,'totalPnl':total}
            if not all(math.isfinite(n) for n in totals.values()):raise ValueError('baw PnL is not finite')
            result={key:repr(round(n,12)) for key,n in totals.items()}
            result.update(activePositionsCount=sum(row.get('positionStatus','OPEN')=='OPEN' for row in rows),positions=rows)
            if wallet_address:result['walletAddress']=wallet_address
            return result

        # PnL needs a current value AND cost for every position. Do not assume zero
        # for a missing field, or use a lifetime realized total as today's PnL.
        async def daily_pnl():
            if not positions_ok:raise ValueError('baw positions unavailable for PnL')
            realized=finite(snapshot['summary'].get('todayRealizedPnl'))
            if realized is None:raise ValueError('baw todayRealizedPnl unavailable')
            unrealized=0
            for row in snapshot['list']:
                value=next((finite(row[k]) for k in ('currentValue','positionValue','value') if finite(row.get(k)) is not None),None)
                cost=next((finite(row[k]) for k in ('cost','totalCost','costBasis','initialValue') if finite(row.get(k)) is not None),None)
                if value is None or cost is None:raise ValueError('baw position value/cost unavailable for PnL')
                unrealized+=value-cost
            total=realized+unrealized
            if not math.isfinite(total):raise ValueError('baw PnL is not finite')
            return {'ts':time.time(),'value':total,'day':datetime.now(timezone.utc).strftime('%Y-%m-%d')}

        # A rejected position snapshot must also mark cached daily PnL stale.
        if all(checks) or not positions_ok:
            _,ok=await self._call('live_daily_pnl',daily_pnl);checks.append(ok)
        _,ok=await self._call('pnl_portfolio',portfolio);checks.append(ok)
        if all(checks):self._reconcile([],items(snapshot),items(orders,('orders',)),[])
        await self._settlement_prices()
        status_keys=['baw_account','baw_status','positions','order_history','payment_options','live_daily_pnl','pnl_portfolio']
        statuses={k:self.store.meta(k+'_status',{}) for k in status_keys}
        previous=self.store.meta('sync_state',{})
        state={'last_attempt':time.time(),'last_success':time.time() if all(checks) else previous.get('last_success',0),
               'permission_pending':False,'errors':{k:v['error'] for k,v in statuses.items() if v.get('error')},'endpoints':statuses}
        self.store.set_meta('sync_state',state)
        await self._paper_poll()
        return state

    async def _backfill_prices(self, attempted):
        """Recover opening and settlement context within the public-price budget."""
        done = set(self.store.meta('price_backfill_done', []))
        candidates = {}
        for key in ('positions', 'positions_redeemable', 'positions_ended', 'settled_history'):
            for row in items(self.store.meta(key, {}), ('positions', 'settledPositions', 'settledHistory', 'history')):
                mid = str(row.get('marketId') or '')
                title = ' '.join(str(row.get(k) or '') for k in ('marketTopicTitle', 'marketTitle', 'title', 'question'))
                start, end = timestamp(row.get('startDate')), timestamp(row.get('endDate'))
                symbol = market_symbol({'title': title})
                if mid and symbol in ('BTCUSDT', 'ETHUSDT') and 0 < start < end:
                    candidates[mid] = (symbol, start, end)
        history = items(self.store.meta('order_history', {}), ('orders',))
        for row in self.store.rows('SELECT market_id, order_id, topic_id FROM orders_audit ORDER BY ts DESC, id DESC LIMIT 20'):
            mid = str(row['market_id'] or '')
            if not mid or mid in candidates:
                continue
            # A matching venue slug contains the historical window, even when
            # a decision title omits its duration or settings have since changed.
            order = next((order for order in history if isinstance(order, dict)
                          and row['order_id'] is not None and order.get('orderId') is not None
                          and str(order['orderId']) == str(row['order_id'])), None)
            if order is None:
                order = next((order for order in history if isinstance(order, dict)
                              and row['topic_id'] is not None and order.get('marketTopicId') is not None
                              and str(order['marketTopicId']) == str(row['topic_id'])), None)
            window = slug_window(order.get('slug')) if order is not None else None
            if window:
                candidates[mid] = window
                continue
            # Decisions are pruned and positions rotate out of snapshots. Use
            # explicit title durations only; an unknown window cannot be priced.
            decisions = self.store.rows('SELECT market, variant_end_time FROM decisions WHERE market_id=? ORDER BY ts DESC, id DESC LIMIT 1', (mid,))
            if decisions:
                decision = decisions[0]
                title = decision['market'] or ''
                duration = re.search(r'\b(\d+)[\s-]*(m(?:in(?:ute)?s?)?|h(?:ours?)?|d(?:ays?)?)\b', title, re.I)
                if not duration:
                    continue
                seconds = int(duration[1]) * {'m': 60, 'h': 3600, 'd': 86400}[duration[2][0].lower()]
                end = timestamp(decision['variant_end_time'])
                symbol = market_symbol({'title': title})
                if symbol in ('BTCUSDT', 'ETHUSDT') and seconds > 0 and end > seconds:
                    candidates[mid] = (symbol, end - seconds, end)
        for row in self.store.rows('SELECT h.* FROM eval_history h LEFT JOIN eval_scores s ON s.eval_id=h.id WHERE s.eval_id IS NULL ORDER BY h.id'):
            candidates.setdefault(row['market_id'],(row['symbol'],row['window_start'],row['window_end']))
        for mid, (symbol, start, end) in sorted(candidates.items(), key=lambda item: item[1][2], reverse=True):
            has_start = self.store.meta('market_prices', {}).get(mid, {}).get('start_price') is not None
            ended = end <= time.time()
            settlement_only = has_start and ended and mid not in self.store.meta('settlement_prices', {})
            # A successful opening backfill can be marked done before the window
            # ends. It must still be eligible for its missing settlement later.
            if (symbol not in ('BTCUSDT', 'ETHUSDT')
                    or self.store.meta('price_backfill_attempts', {}).get(mid, 0) >= 3
                    or (not settlement_only and (mid in done or has_start))):
                continue
            if len(attempted) >= 3:
                break
            attempted.add(mid)
            filled = False
            try:
                start_ms, end_ms = int(start * 1000), int(end * 1000)
                candles = []
                # Five-minute candles are price resolution, independent of the market window.
                opening_time = (((end_ms - 300000) if settlement_only else start_ms) // 300000) * 300000
                cursor = opening_time
                # The budget counts markets; paginate each window past Binance's
                # 1000-candle response limit, stopping at the present for open ones.
                fetch_end = min(end_ms, int(time.time() * 1000))
                while cursor < fetch_end:
                    limit = 1 if settlement_only else min(1000, math.ceil((end_ms - cursor) / 300000))
                    batch = await self.client.klines(symbol, interval='5m', limit=limit,
                                                    startTime=cursor, endTime=end_ms - 1)
                    if not batch:
                        break
                    next_cursor = max(int(c[0]) for c in batch) + 300000
                    if next_cursor <= cursor:
                        break
                    candles.extend(batch)
                    cursor = next_cursor
                    if settlement_only or len(batch) < limit:
                        break
                candles = sorted((c for c in candles if opening_time <= int(c[0]) < end_ms), key=lambda c: int(c[0]))
                if not candles or int(candles[0][0]) not in (opening_time, opening_time + 300000):
                    continue
                opening = finite(candles[0][1]) if not settlement_only else None
                ended = end <= time.time()
                closed = sorted((c for c in candles if int(c[6]) <= end_ms - 1), key=lambda c: int(c[6]))
                closing = finite(closed[-1][4]) if closed else None
                if not settlement_only and (opening is None or opening <= 0):
                    continue
                if ended and (not closed or end_ms - 1 - int(closed[-1][6]) > 300000
                              or closing is None or closing <= 0):
                    continue
                # Reload after the network await so evaluations cannot lose their
                # latest spot quote while historical context is being fetched.
                with self.store.transaction():
                    if not settlement_only:
                        prices = self.store.meta('market_prices', {})
                        prices[mid] = {**prices.get(mid, {}), 'symbol': symbol, 'start_price': opening}
                        self.store.set_meta('market_prices', prices)
                    if ended:
                        settlements = self.store.meta('settlement_prices', {})
                        settlements[mid] = closing
                        self.store.set_meta('settlement_prices', settlements)
                    done = set(self.store.meta('price_backfill_done', []))
                    done.add(mid)
                    self.store.set_meta('price_backfill_done', sorted(done))
                    attempts = self.store.meta('price_backfill_attempts', {})
                    attempts.pop(mid, None)
                    self.store.set_meta('price_backfill_attempts', attempts)
                filled = True
            except Exception:
                pass
            finally:
                if not filled:
                    # Persist validation failures as well as network failures, so
                    # an unavailable market cannot starve the remaining history.
                    with self.store.transaction():
                        attempts = self.store.meta('price_backfill_attempts', {})
                        attempts[mid] = attempts.get(mid, 0) + 1
                        self.store.set_meta('price_backfill_attempts', attempts)
                        if attempts[mid] >= 3:
                            done = set(self.store.meta('price_backfill_done', []))
                            done.add(mid)
                            self.store.set_meta('price_backfill_done', sorted(done))

    async def _settlement_prices(self):
        # Optional public price context must never affect account sync health.
        try:
            attempted = set()
            await self._backfill_prices(attempted)
            prices = self.store.meta('settlement_prices', {})
            backfill_attempts = self.store.meta('price_backfill_attempts', {})
            now = time.time()
            for key in ('positions', 'positions_redeemable', 'positions_ended', 'settled_history'):
                for row in items(self.store.meta(key, {}), ('positions', 'settledPositions', 'settledHistory', 'history')):
                    mid = str(row.get('marketId', ''))
                    end = timestamp(row.get('endDate'))
                    if (not mid or mid in prices or mid in attempted or backfill_attempts.get(mid, 0) >= 3
                            or not 0 < end <= now):
                        continue
                    title = ' '.join(str(row.get(k) or '') for k in ('marketTopicTitle', 'marketTitle', 'title', 'question'))
                    symbol = market_symbol({'title': title})
                    if symbol not in ('BTCUSDT', 'ETHUSDT'):
                        continue
                    if len(attempted) >= 3:
                        return
                    attempted.add(mid)
                    try:
                        # Settlement uses the final five-minute candle for any market duration.
                        start_ms = int((end - 300) * 1000)
                        end_ms = int(end * 1000)
                        candles = await self.client.klines(symbol, interval='5m', limit=1,
                                                          startTime=start_ms, endTime=end_ms - 1)
                        # Reject a different or incomplete candle instead of caching it forever.
                        for candle in candles:
                            if int(candle[0]) != start_ms or int(candle[6]) != end_ms - 1:
                                continue
                            close = finite(candle[4])
                            if close is not None and close > 0:
                                prices[mid] = close
                                self.store.set_meta('settlement_prices', prices)
                                break
                    except Exception:
                        pass
        except Exception:
            pass
        finally:
            self.store.score_evaluations()

    def _prune_placed_reservations(self,snapshot):
        # Allow new fills time to appear; only a fresh snapshot can release slots.
        market_ids={str(row['marketId']) for row in items(snapshot,('positions',)) if row.get('marketId') is not None}
        placeholders=','.join('?' for _ in market_ids)
        self.store.execute(
            "DELETE FROM reservations WHERE status='PLACED' AND ts < ?"
            +(f' AND market_id NOT IN ({placeholders})' if market_ids else ''),
            (time.time()-600,*market_ids))

    def _reconcile_order_audit(self,history):
        """An accepted placement is not evidence that the venue filled it."""
        venue_orders={str(row['orderId']):row for row in history
                      if isinstance(row,dict) and row.get('orderId') is not None}

        def amount(value):
            try:
                number=Decimal(str(value))
                return number if number.is_finite() else None
            except (InvalidOperation,ValueError):
                return None

        def plain(number):
            value=format(number,'f')
            return value.rstrip('0').rstrip('.') if '.' in value else value

        with self.store.transaction():
            for row in self.store.rows("SELECT id,order_id,amount FROM orders_audit WHERE order_id IS NOT NULL AND order_id != ''"):
                order=venue_orders.get(str(row['order_id']))
                if order is None:continue
                status=str(order.get('status','')).upper()
                if status in ('FILLED','SUBMITTED'):continue
                filled=amount(order.get('filledUsdtAmount'))
                if filled is None:continue
                if status in ('FAILED','CANCELLED','EXPIRED') and filled==0:
                    self.store.execute('UPDATE orders_audit SET status=?,error=? WHERE id=?',
                                       ('FAILED',f'未成交（场馆 {status}）',row['id']))
                    continue
                requested=amount(row['amount'])
                if requested is not None and 0<filled<requested:
                    self.store.execute('UPDATE orders_audit SET status=?,error=? WHERE id=?',
                                       ('SUCCESS',f'部分成交 {plain(filled)}/{plain(requested)}',row['id']))

    def _reconcile(self,orders,positions,history,settled):
        """Only our persisted IDs are considered; external trading cannot claim ownership."""
        active_ids={str(o.get('orderId')) for o in orders}
        active_markets={str(p.get('marketId')) for p in positions}
        final={str(o.get('orderId')):str(o.get('status','')).upper() for o in history}
        settled_markets={str(p.get('marketId')) for p in settled}
        for r in self.store.rows('SELECT * FROM reservations'):
            if r['status']=='UNKNOWN':continue
            oid=r['order_id'];mid=r['market_id']
            if oid in active_ids or mid in active_markets:continue
            if mid in settled_markets or final.get(oid) in ('CANCELED','CANCELLED','REJECTED','EXPIRED','FAILED'):
                self.store.execute('DELETE FROM reservations WHERE market_id=?',(mid,))
    async def _paper_poll(self):
        for record in self.store.rows("SELECT DISTINCT topic_id FROM paper_orders WHERE status='OPEN'"):
            try:
                detail=unwrap(await self.client.market_detail(record['topic_id']))
                for variant in detail.get('variants',detail.get('markets',[])):
                    mid=str(variant.get('marketId'));outcomes=variant.get('outcomes',[])
                    prices={str(o['tokenId']):o.get('price',o.get('chance')) for o in outcomes if o.get('tokenId') and finite(o.get('price',o.get('chance'))) is not None}
                    mark_paper(self.store,mid,prices)
                    # Only explicit resolution settles. Price == 1 or clock expiry is insufficient.
                    status=str(variant.get('status',variant.get('tradingStatus',''))).upper()
                    winner=variant.get('winningTokenId') or variant.get('winnerTokenId')
                    flagged=[o.get('tokenId') for o in outcomes if o.get('isWinner') is True or o.get('winner') is True]
                    if len(flagged)>1:
                        self.store.set_meta('paper_sync_error',{'ts':time.time(),'error':f'市场 {mid} 有多个获胜结果，需核对拆分结算比例'})
                        continue
                    if winner and flagged and str(winner)!=str(flagged[0]):
                        self.store.set_meta('paper_sync_error',{'ts':time.time(),'error':f'市场 {mid} 结算获胜结果冲突'})
                        continue
                    if status in ('RESOLVED','SETTLED','FINALIZED') and (winner or flagged):settle_paper(self.store,mid,winner or flagged[0])
            except Exception as exc:self.store.set_meta('paper_sync_error',{'ts':time.time(),'error':str(exc)})
        self._paper_eval_fallback()

    def _paper_eval_fallback(self):
        """Retry overdue paper orders after giving venue resolution priority."""
        now=time.time()
        for row in self.store.rows("SELECT * FROM paper_orders WHERE status='OPEN' AND end_time < ?",(now-120,)):
            title=row['title'] or ''
            symbol=re.match(r'^(BTC|ETH)\b',title,re.I)
            duration=re.search(r'\b(5|15)m\b',title,re.I)
            if not symbol or not duration:continue
            window_start=row['end_time']-int(duration[1])*60
            scores=self.store.rows("SELECT outcome FROM eval_scores WHERE symbol=? AND window_start=? AND outcome IN ('UP','DOWN') ORDER BY eval_id DESC LIMIT 1",
                                   (symbol[1].upper()+'USDT',window_start))
            if not scores:continue
            outcome=scores[0]['outcome']
            if settle_paper_outcome(self.store,row['id'],outcome,now):
                logger.warning('Paper settlement fallback: order_id=%s outcome=%s',row['id'],outcome)

    async def run(self):
        while True:
            try:await self.run_once()
            except Exception as exc:self.store.set_meta('sync_loop_error',{'ts':time.time(),'error':str(exc)})
            await asyncio.sleep(60)
