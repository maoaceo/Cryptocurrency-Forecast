"""Persisted risk checks and paper/live execution. Unknown live outcomes stay reserved."""
import json
import math
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from app.baw_client import BawClient, executor_path


def utc_day(): return datetime.now(timezone.utc).strftime('%Y-%m-%d')
def money(value): return float(Decimal(str(value)).quantize(Decimal('.01'),rounding=ROUND_DOWN))
def usdt_to_wei(value): return str(int(Decimal(str(value))*Decimal(10)**18))

def open_position(row):
    # baw snapshots preserve resolved/closed rows, including their old shares.
    if 'positionStatus' in row and str(row['positionStatus']).upper()!='OPEN':return False
    if str(row.get('isResolved',False)).lower() in ('true','1'):return False
    try:
        shares=float(row.get('currentShares',row.get('shares')))
        return math.isfinite(shares) and shares>0
    except (TypeError,ValueError):return False

class Risk:
    def __init__(self,store): self.store=store
    def paper_pnl(self):
        start=datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
        rows=self.store.rows('SELECT * FROM paper_orders')
        return sum((r['realized_pnl'] or 0) if r['status']=='SETTLED' and (r['settled_at'] or 0)>=start else ((r['current_price'] or 0)-r['price'])*r['shares'] if r['status']=='OPEN' else 0 for r in rows)
    def spent(self):
        rows=self.store.rows('SELECT amount FROM daily_spend WHERE day=?',(utc_day(),))
        return rows[0]['amount'] if rows else 0
    def occupied(self,market_id):
        return bool(self.store.rows('SELECT 1 FROM reservations WHERE market_id=?',(str(market_id),)) or self.store.rows("SELECT 1 FROM paper_orders WHERE market_id=? AND status='OPEN'",(str(market_id),)))
    def check(self,amount,market_id,pnl=None,reserved=False):
        s=self.store.settings()
        if not s['engine_enabled']: return False,'引擎已关闭'
        if (self.store.data_dir/'STOP').exists(): return False,'STOP 文件已触发'
        if not math.isfinite(amount) or amount<1 or amount>s['bet_usdt']: return False,'单笔金额超限'
        if not reserved and self.occupied(market_id): return False,'本市场已有订单或持仓'
        count=len(self.store.rows('SELECT market_id FROM reservations'))+len(self.store.rows("SELECT id FROM paper_orders WHERE status='OPEN'"))
        if s['mode']=='LIVE':
            snapshot=self.store.meta('positions',{})
            account=snapshot.get('list',snapshot.get('positions',[])) if isinstance(snapshot,dict) else snapshot
            known={r['market_id'] for r in self.store.rows('SELECT market_id FROM reservations')}
            known.update(r['market_id'] for r in self.store.rows("SELECT market_id FROM paper_orders WHERE status='OPEN'"))
            for index,row in enumerate(account or []):
                if open_position(row):
                    mid=row.get('marketId')
                    known.add(str(mid) if mid is not None and mid!='' else f'external:{index}')
            count=len(known)
        if count+(0 if reserved else 1)>s['max_open_positions']: return False,'持仓数量已达上限'
        if self.spent()+(0 if reserved else amount)>s['daily_spend_cap']+1e-9:return False,'今日额度不足'
        if s['mode']=='LIVE':
            state=self.store.meta('sync_state',{})
            if state.get('permission_pending'): return False,'预测交易权限待开通'
            if state.get('errors') or time.time()-state.get('last_success',0)>150:return False,'账户同步数据已过期'
            pnl_state=self.store.meta('live_daily_pnl',{})
            if pnl_state.get('day')!=utc_day() or time.time()-pnl_state.get('ts',0)>150:return False,'今日盈亏数据不可用'
            pnl=pnl_state.get('value') if pnl is None else pnl
            if pnl is None or not math.isfinite(float(pnl)):return False,'今日盈亏数据不可用'
        elif pnl is None: pnl=self.paper_pnl()
        if pnl<=-s['daily_loss_stop']:
            self.store.update_settings({'engine_enabled':False})
            return False,'今日亏损止损已触发，已停止引擎'
        return True,'允许'
    def charge(self,amount,day=None):
        self.store.execute('INSERT INTO daily_spend(day,amount) VALUES (?,?) ON CONFLICT(day) DO UPDATE SET amount=amount+excluded.amount',(day or utc_day(),amount))

class Executor:
    def __init__(self,store,client,baw_client=None):
        self.store=store; self.client=client; self.risk=Risk(store)
        self.baw_client=baw_client or BawClient(store=store)
    @property
    def path(self):
        return executor_path(self.store.settings())
    @property
    def execution_client(self):
        return self.baw_client if self.path=='baw' else self.client
    async def execute(self,market,side,prob=None,price=None):
        s=self.store.settings(); amount=money(s['bet_usdt']); mid=str(market['market_id']); charge_day=utc_day()
        path=executor_path(s)
        execution_client=self.baw_client if path=='baw' else self.client
        outcomes=market['outcomes']
        outcome=next((o for o in outcomes if str(o.get('name','')).upper()==side),None)
        if outcome is None:
            aliases={'UP':('涨','上涨'),'DOWN':('跌','下跌')}
            outcome=next((o for o in outcomes if str(o.get('name','')).upper() in aliases[side]),None)
        if outcome is None:return {'ok':False,'reason':'缺少对应结果代币'}
        price=float(outcome['price']) if price is None else float(price)
        if not math.isfinite(price) or not 0<price<1:return {'ok':False,'reason':'市场价格无效'}
        token=str(outcome['tokenId'])
        # Lock check and reservation together; external work occurs only after commit.
        with self.store.transaction():
            ok,reason=self.risk.check(amount,mid)
            if not ok:return {'ok':False,'reason':reason}
            if s['mode']=='PAPER':
                self.store.execute("INSERT INTO paper_orders(ts,market_id,topic_id,title,token_id,side,amount,price,shares,status,current_price,end_time) VALUES (?,?,?,?,?,?,?,?,?,'OPEN',?,?)",(time.time(),mid,str(market['topic_id']),market['title'],token,side,amount,price,amount/price,price,market['end_time']))
                self.risk.charge(amount,charge_day)
                return {'ok':True,'reason':'模拟成交','mode':'PAPER'}
            self.store.execute('INSERT INTO reservations VALUES (?,?,?,?,?,?)',(mid,time.time(),amount,'PENDING',None,token))
            self.risk.charge(amount,charge_day)
            audit=self.store.execute("INSERT INTO orders_audit(ts,market_id,topic_id,token_id,side,amount,status) VALUES (?,?,?,?,?,?,'ATTEMPT')",(time.time(),mid,str(market['topic_id']),token,side,amount)).lastrowid
            self.store.execute('DELETE FROM orders_audit WHERE id NOT IN (SELECT id FROM orders_audit ORDER BY id DESC LIMIT 10)')
        placed=False; quote_id=None
        try:
            wallet=self.store.meta('wallet',{})
            if path=='sapi' and (not wallet.get('walletAddress') or not wallet.get('walletId')):raise ValueError('钱包信息不可用')
            slip=max(1,min(10000,int(s['slippage_bps'])))
            if path=='baw':
                quote=await execution_client.get_quote(tokenId=token,marketTopicId=str(market['topic_id']),side='BUY',amount=format(Decimal(str(amount)), 'f'),orderType='MARKET',slippageBps=slip)
                quoted_slip=quote.get('slippageBps')
                if type(quoted_slip) is not int or not 1<=quoted_slip<=slip:raise ValueError('报价滑点无效或超过上限')
                slip=quoted_slip
            else:
                quote=await execution_client.get_quote(walletAddress=wallet['walletAddress'], tokenId=token,side='BUY',amountIn=usdt_to_wei(amount),orderType='MARKET',slippageBps=slip,chainId='56',feeRateBps=200,fundingSource='CEX')
            quote_id=quote['quoteId']
            expires=quote.get('expireAt')
            if expires is not None:
                from engine.markets import timestamp
                if timestamp(expires)<=time.time()+1:raise ValueError('报价已过期')
            # STOP and disable remain effective even if switched during quote retrieval.
            if not self.store.settings()['engine_enabled'] or (self.store.data_dir/'STOP').exists():raise ValueError('引擎已停止')
            end=float(market['end_time']);start=float(market.get('start_time',end))
            if time.time()>=end or (start<end and end-time.time()<=.25*(end-start)):raise ValueError('已超过允许交易时间')
            if utc_day()!=charge_day:raise ValueError('UTC 日期已切换，请重新评估')
            latest=self.store.settings()
            if executor_path(latest)!=path:raise ValueError('执行方式已变更，请重新评估')
            if latest['mode']!='LIVE':raise ValueError('交易模式已变更')
            ok,reason=self.risk.check(amount,mid,reserved=True)
            if not ok:raise ValueError(reason)
            signal_prob=1-prob if prob is not None and s.get('invert_signal',False) else prob
            if prob is not None and (signal_prob<latest['prob_threshold'] or prob-price<latest['edge_min']):raise ValueError('策略阈值已收紧')
            if quote.get('averagePrice') is not None and prob is not None:
                quoted=float(quote['averagePrice'])
                if not math.isfinite(quoted) or not 0<quoted<1 or prob-quoted<latest['edge_min']:raise ValueError('报价优势不足')
            if int(latest['slippage_bps'])<slip:raise ValueError('滑点上限已收紧，请重新评估')
            placed=True
            if path=='baw':
                result=await execution_client.place_order_bundle(quoteId=quote_id,slippageBps=slip)
            else:
                result=await execution_client.place_order_bundle(walletAddress=wallet['walletAddress'],walletId=wallet['walletId'],quoteId=quote_id,timeInForce='FOK',accountType='SPOT',orderType='MARKET',slippageBps=slip,fundingSource='CEX')
            order_id=str(result['orderId'])
            with self.store.transaction():
                self.store.execute("UPDATE reservations SET status='PLACED',order_id=? WHERE market_id=?",(order_id,mid))
                self.store.execute("UPDATE orders_audit SET status='SUCCESS',order_id=?,quote_id=?,response_json=? WHERE id=?",(order_id,str(quote_id),json.dumps(result),audit))
            return {'ok':True,'reason':'真钱订单已提交','order_id':order_id}
        except Exception as exc:
            # A transport failure during placement may have created an order. Reserve budget
            # and variant until the operator reconciles it; never retry placement blindly.
            definitive_codes={-1002,-1022,-2015,-3026,-1021,-1100,-1101,-1102,-1111,-2010,-2018,-2019,-4026}
            status='UNKNOWN' if placed and (getattr(exc,'code',None) not in definitive_codes or (getattr(exc,'status_code',0) or 0)>=500) else 'FAILED'
            with self.store.transaction():
                self.store.execute('UPDATE orders_audit SET status=?,quote_id=?,error=? WHERE id=?',(status,str(quote_id) if quote_id else None,str(exc),audit))
                if status=='FAILED':
                    self.store.execute('DELETE FROM reservations WHERE market_id=?',(mid,))
                    self.risk.charge(-amount,charge_day)
                else:self.store.execute("UPDATE reservations SET status='UNKNOWN' WHERE market_id=?",(mid,))
            return {'ok':False,'reason':str(exc),'status':status}

def mark_paper(store,market_id,prices):
    for token,price in prices.items():
        if 0<=float(price)<=1:store.execute("UPDATE paper_orders SET current_price=? WHERE market_id=? AND token_id=? AND status='OPEN'",(float(price),str(market_id),str(token)))

def settle_paper(store,market_id,winner_token_id):
    now=time.time()
    for row in store.rows("SELECT * FROM paper_orders WHERE market_id=? AND status='OPEN'",(str(market_id),)):
        payout=row['shares'] if row['token_id']==str(winner_token_id) else 0
        store.execute("UPDATE paper_orders SET status='SETTLED',current_price=?,realized_pnl=?,settled_at=? WHERE id=?",(1 if payout else 0,payout-row['amount'],now,row['id']))

def settle_paper_outcome(store,order_id,outcome,now=None):
    """Settle one still-open paper order using a known window outcome."""
    if outcome not in ('UP','DOWN'):return False
    now=time.time() if now is None else now
    with store.transaction():
        rows=store.rows("SELECT * FROM paper_orders WHERE id=? AND status='OPEN'",(order_id,))
        if not rows:return False
        row=rows[0]
        won=row['side']==outcome
        payout=row['shares'] if won else 0
        store.execute("UPDATE paper_orders SET status='SETTLED',current_price=?,realized_pnl=?,settled_at=? WHERE id=? AND status='OPEN'",
                      (1 if won else 0,payout-row['amount'],now,order_id))
        return True
