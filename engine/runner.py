"""Concurrent market evaluations with serialized trading and persisted decisions."""
import asyncio
import json
import math
import time
from app.risk import Executor, Risk, utc_day
from engine.markets import select_markets

class Engine:
    def __init__(self,store,client,ensemble=None):
        self.store=store;self.client=client;self.lock=asyncio.Lock()
        self.executor=Executor(store,client)
        if ensemble is None:
            from engine.signals import Ensemble
            ensemble=Ensemble(client,store=store)
        self.ensemble=ensemble
        self.upcoming={}
        self.next_discovery=0
        self.discovery_task=None
        self.evaluated_windows=set()
        self.burst_seen=set()
    def enabled(self):
        return self.store.settings()['engine_enabled'] and not (self.store.data_dir/'STOP').exists()
    async def tick(self):
        if not self.enabled():return []
        async with self.lock:
            if not self.enabled():return []
            s=self.store.settings()
            live_pnl=self.store.meta('live_daily_pnl',{})
            pnl=self.executor.risk.paper_pnl() if s['mode']=='PAPER' else live_pnl.get('value') if live_pnl.get('day')==utc_day() else None
            if pnl is not None and math.isfinite(float(pnl)) and pnl<=-min(50,s['daily_loss_stop']):
                self.store.update_settings({'engine_enabled':False})
                self.store.set_meta('engine_error',{'ts':time.time(),'error':'今日亏损止损已触发，已停止引擎'})
                return []
            decisions=[]
            now=time.time()
            duration=self._duration(s)
            markets=list(self.upcoming.values())
            discovered=None
            # A scan already in flight is consumed before starting another one.
            # Clamp the ordinary cadence to T-2 even if the last scan was at T-10.
            burst_start=self._burst_start(s,now)
            interval=2 if burst_start is not None else 15
            if duration:
                next_boundary=(math.floor(now/duration)+1)*duration
                self.next_discovery=min(self.next_discovery,next_boundary-2)
            try:
                if self.discovery_task is not None and self.discovery_task.done():
                    task,self.discovery_task=self.discovery_task,None
                    discovered=task.result()
                    self.next_discovery=time.time()+interval
                elif self.discovery_task is None and (now>=self.next_discovery or not self.upcoming):
                    if self.upcoming:
                        self.discovery_task=asyncio.create_task(select_markets(self.client,s))
                    else:
                        discovered=await select_markets(self.client,s)
                        self.next_discovery=time.time()+interval
            except Exception as exc:
                self.store.set_meta('engine_error',{'ts':time.time(),'error':str(exc)})
                self.next_discovery=time.time()+interval
            if discovered is not None:
                markets=list({str(m['market_id']):m for m in markets+discovered}.values())
                if duration:
                    current=math.floor(time.time()/duration)*duration
                    self.burst_seen={key for key in self.burst_seen if key[1]>=current}
                    self.burst_seen.update((m['symbol'],m['start_time']) for m in discovered
                                           if m['start_time']<=time.time())
            due=[]
            for market in markets:
                if not self.enabled():break
                mid=str(market['market_id'])
                key=(market['symbol'],market['start_time'])
                self.upcoming.pop(mid,None)
                if key in self.evaluated_windows or self.executor.risk.occupied(mid):continue
                now=time.time()
                if market['end_time']-now<60:continue
                # Evaluate discovered markets as soon as their window is open.
                if now<market['start_time']:
                    self.upcoming[mid]=market
                    continue
                last=self.store.rows('SELECT ts FROM decisions WHERE market_id=? ORDER BY ts DESC LIMIT 1',(mid,))
                if last and time.time()-last[0]['ts']<max(600,s['cooldown_seconds']):continue
                started=time.time()
                with self.store.transaction():
                    evaluation_id=self.store.execute('INSERT INTO decisions(ts,market,market_id,variant_end_time,action,reason,voters_json) VALUES (?,?,?,?,?,?,?)',(started,market['title'],mid,market['end_time'],'SKIP','评估进行中','{}')).lastrowid
                    self.store.execute('DELETE FROM decisions WHERE id NOT IN (SELECT id FROM decisions ORDER BY id DESC LIMIT 10)')
                self.evaluated_windows.add(key)
                due.append((market,mid,started,evaluation_id))
            results=await asyncio.gather(*(self.ensemble.evaluate(market['symbol'],market)
                                          for market,_,_,_ in due),return_exceptions=True)
            # Refresh prices and place orders in selection order, keeping risk checks serialized.
            for (market,mid,started,evaluation_id),result in zip(due,results):
                decisions.append(await self._finish(market,mid,started,evaluation_id,result,s))
            self.store.set_meta('engine_last_tick',time.time())
            return decisions
    @staticmethod
    def _duration(settings):
        # The operator's window selector sets an exact duration. A range cannot
        # identify a unique next window, so discovery remains authoritative.
        low,high=settings['min_duration'],settings['max_duration']
        return float(low) if low>0 and low==high else None

    def _burst_start(self,settings,now):
        duration=self._duration(settings)
        if not duration:return None
        current=math.floor(now/duration)*duration
        if now-current<=30 and not all((s,current) in self.burst_seen for s in settings['symbols']):
            return current
        if current+duration-now<=2:return current+duration
        return None

    def _delay(self):
        settings=self.store.settings()
        now=time.time()
        delay=15
        duration=self._duration(settings)
        if duration:
            boundary=(math.floor(now/duration)+1)*duration
            wake=boundary-2 if now<boundary-2 else boundary
            delay=min(delay,max(.25,wake-now))
        if self._burst_start(settings,now) is not None:delay=min(delay,2)
        for market in self.upcoming.values():
            delay=min(delay,max(0,market['start_time']-now))
        if self.discovery_task is not None:delay=min(delay,.25)
        delay=min(delay,max(.25,self.next_discovery-now))
        return delay

    async def _finish(self,market,mid,started,evaluation_id,result,s):
        p=None;p_up=None;price=None;edge=None;side=None;voters={};action='SKIP';reason=''
        try:
            if isinstance(result,Exception):raise result
            voters=result.get('voters',{})
            p_up=result.get('p_up')
            if p_up is None:reason='没有可用信号'
            else:
                p_up=float(p_up)
                if not math.isfinite(p_up) or not 0<=p_up<=1:raise ValueError('模型概率无效')
                side='UP' if p_up>=.5 else 'DOWN';p=max(p_up,1-p_up)
                # Keep signal gates and evaluation history in the original model direction.
                model_p=p
                if s.get('invert_signal',False):
                    side='DOWN' if side=='UP' else 'UP'
                    p=1-model_p
                votes=voters.values() if isinstance(voters,dict) else voters
                agreeing=sum(v.get('status')=='ok' and v.get('p_up') is not None
                             and (float(v['p_up'])-.5)*(p_up-.5)>0
                             and abs(float(v['p_up'])-.5)>=.02 for v in votes)
                if agreeing<s['min_agree_votes']:reason='共识不足'
                else:
                    chosen=next(o for o in market['outcomes'] if o['name']==side)
                    # Models can take 45 seconds: refresh the tradable price before deciding.
                    fresh=await self.client.market_detail(market['topic_id'])
                    if isinstance(fresh,dict) and 'data' in fresh:fresh=fresh['data']
                    variant=next((v for v in fresh.get('variants',fresh.get('markets',[])) if str(v.get('marketId'))==mid),None)
                    if variant is None or str(variant.get('tradingStatus',variant.get('status',''))).upper()!='OPEN':raise ValueError('市场已停止交易')
                    current=next((o for o in variant.get('outcomes',[]) if str(o.get('tokenId'))==str(chosen['tokenId'])),None)
                    if current is None:raise ValueError('最新结果价格不可用')
                    price=float(current.get('price',current.get('chance')))
                    if not math.isfinite(price) or not 0<price<1:raise ValueError('最新结果价格无效')
                    edge=p-price
                    if model_p<s['prob_threshold']:reason='模型概率低于阈值'
                    elif edge<s['edge_min']:reason='反买优势不足' if s.get('invert_signal',False) else '优势低于阈值'
                    elif time.time()>=market['end_time']-.25*(market['end_time']-market['start_time']):reason='已超过允许交易时间'
                    elif not self.enabled():reason='引擎已停止'
                    else:
                        execution=await self.executor.execute(market,side,p,price)
                        action='TRADE' if execution.get('ok') else 'SKIP'
                        reason=execution.get('reason','')
                        if action=='TRADE' and s.get('invert_signal',False):reason+='（反买）'
        except Exception as exc:reason='评估失败: '+str(exc)
        row={'ts':started,'market':market['title'],'market_id':mid,'variant_end_time':market['end_time'],
             'model_prob':p,'market_price':price,'edge':edge,'action':action,'reason':reason,
             'voters_json':json.dumps(voters,ensure_ascii=False),'side':side}
        with self.store.transaction():
            self.store.execute('UPDATE decisions SET ts=?,market=?,market_id=?,variant_end_time=?,model_prob=?,market_price=?,edge=?,action=?,reason=?,voters_json=?,side=? WHERE id=?',(*row.values(),evaluation_id))
            self.store.record_evaluation(started,market,voters,p_up if isinstance(p_up,(int,float)) and math.isfinite(p_up) and 0<=p_up<=1 else None)
        return row

    async def run(self):
        try:
            while True:
                try:await self.tick()
                except Exception as exc:self.store.set_meta('engine_error',{'ts':time.time(),'error':str(exc)})
                await asyncio.sleep(self._delay())
        finally:
            tasks=[]
            if self.discovery_task is not None:tasks.append(self.discovery_task)
            for task in tasks:task.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
            self.discovery_task=None
