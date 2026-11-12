"""Small SQLite state store. Money is recorded in USDT, timestamps in UTC seconds."""
import json
import math
import sqlite3
import threading
from contextlib import contextmanager
from uuid import uuid4
import time
from pathlib import Path

from engine.signals import RETIRED_VOTERS
from app.service_config import BLOCKS, defaults, normalize_block, migrate_settings, validate_slots
from app.baw_client import executor_path

DEFAULTS = dict(paper_initial_balance=100.0, mode='PAPER', engine_enabled=False, invert_signal=False, bet_usdt=10.0,
                prob_threshold=0.62, edge_min=0.05, daily_spend_cap=100.0,
                daily_loss_stop=50.0, max_open_positions=5,
                cooldown_seconds=600, durations=[], symbols=['BTCUSDT','ETHUSDT'], min_duration=0, max_duration=31536000, slippage_bps=1200, min_agree_votes=9)

class Store:
    def __init__(self, path='data/trader.sqlite3'):
        self.path = str(path)
        self.data_dir = Path(path).parent
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript('''
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS decisions (
          id INTEGER PRIMARY KEY,ts REAL NOT NULL,market TEXT,market_id TEXT,
          variant_end_time REAL,model_prob REAL,market_price REAL,edge REAL,
          action TEXT NOT NULL,reason TEXT,voters_json TEXT,side TEXT);
        CREATE INDEX IF NOT EXISTS decision_variant ON decisions(market_id,ts);
        CREATE TABLE IF NOT EXISTS eval_history (
          id INTEGER PRIMARY KEY AUTOINCREMENT,ts REAL NOT NULL,market_id TEXT NOT NULL,
          symbol TEXT NOT NULL,window_start REAL NOT NULL,window_end REAL NOT NULL,
          voters_json TEXT NOT NULL,ensemble_p REAL);
        CREATE TABLE IF NOT EXISTS eval_scores (
          eval_id INTEGER PRIMARY KEY,symbol TEXT NOT NULL,window_start REAL NOT NULL,
          outcome TEXT NOT NULL,votes_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS eval_score_window ON eval_scores(window_start,symbol);
        CREATE TABLE IF NOT EXISTS paper_orders (
          id INTEGER PRIMARY KEY,ts REAL,market_id TEXT,topic_id TEXT,title TEXT,
          token_id TEXT,side TEXT,amount REAL,price REAL,shares REAL,status TEXT,
          current_price REAL,realized_pnl REAL DEFAULT 0,settled_at REAL,end_time REAL);
        CREATE TABLE IF NOT EXISTS orders_audit (
          id INTEGER PRIMARY KEY,ts REAL,market_id TEXT,topic_id TEXT,token_id TEXT,
          side TEXT,amount REAL,status TEXT,order_id TEXT,quote_id TEXT,error TEXT,response_json TEXT);
        CREATE TABLE IF NOT EXISTS reservations (
          market_id TEXT PRIMARY KEY,ts REAL,amount REAL,status TEXT,order_id TEXT,token_id TEXT);
        CREATE TABLE IF NOT EXISTS daily_spend (day TEXT PRIMARY KEY,amount REAL NOT NULL DEFAULT 0);
        ''')
    def rows(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql,args).fetchall()]
    def execute(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql,args)
    def settings(self):
        values = DEFAULTS.copy()
        values.update(defaults())
        values.update({r['key']:json.loads(r['value']) for r in self.rows('SELECT * FROM settings')})
        values.update({k:v for k,v in self.meta('settings', {}).items() if k in BLOCKS})
        values['baw_bridge'] = {'enabled': values.get('baw_bridge', {}).get('enabled')}
        if 'executor_mode' not in values:
            values['executor_mode'] = executor_path()
        return migrate_settings(values)
    def update_settings(self, values):
        if not isinstance(values,dict): raise ValueError('设置必须为对象')
        unknown=set(values)-set(DEFAULTS)-set(BLOCKS)-{'executor_mode'}
        if unknown: raise ValueError('未知设置: '+', '.join(sorted(unknown)))
        current=self.settings()
        normalized={}
        context=current.copy()
        if "providers" in values:
            context["providers"]=normalize_block("providers", values["providers"], current)
        for k,v in values.items():
            if k in BLOCKS:
                v=normalize_block(k,v,context)
            elif k=='executor_mode':
                if v not in ('sapi', 'baw'): raise ValueError('executor_mode 必须是 sapi 或 baw')
            elif k=='mode':
                v=str(v).upper()
                if v not in ('PAPER','LIVE'): raise ValueError('模式必须是 PAPER 或 LIVE')
            elif k in ('engine_enabled','invert_signal'):
                if not isinstance(v,bool): raise ValueError('开关必须为布尔值')
            elif k=='durations':
                if not isinstance(v,list): raise ValueError('周期必须是列表')
            elif k=='symbols':
                if not isinstance(v,list): raise ValueError('交易对必须是列表')
                v=list(dict.fromkeys(str(symbol).strip().upper() for symbol in v))
                v=[symbol for symbol in v if symbol in ('BTCUSDT','ETHUSDT')]
                if not v: continue
            elif k=='paper_initial_balance':
                if isinstance(v,bool): raise ValueError('模拟起始资金必须是非负有限金额')
                v=float(v)
                if not math.isfinite(v) or v<0: raise ValueError('模拟起始资金必须是非负有限金额')
            elif k=='min_agree_votes':
                if isinstance(v,bool): raise ValueError('共识票数必须为 0 到 12 的整数')
                number=float(v)
                if not math.isfinite(number) or not number.is_integer() or not 0<=number<=12:
                    raise ValueError('共识票数必须为 0 到 12 的整数')
                v=int(number)
            elif k=='max_open_positions':
                v=int(v)
                if not 1<=v<=50: raise ValueError('最大持仓必须在 1 到 50 之间')
            else:
                v=float(v)
                bounds={'bet_usdt':(1,100),'daily_spend_cap':(1,5000),
                    'daily_loss_stop':(0.01,1000),'prob_threshold':(0.50,1),
                    'edge_min':(-1,1),'cooldown_seconds':(600,86400*365),
                    'slippage_bps':(1,10000),'min_duration':(0,31536000),'max_duration':(1,31536000)}
                low,high=bounds[k]
                if not math.isfinite(v) or not low<=v<=high: raise ValueError(f'{k} 必须在 {low} 到 {high} 之间')
            normalized[k]=v
        current=self.settings()
        if 'mode' in normalized and normalized['mode']!=current['mode'] and current['engine_enabled'] and normalized.get('engine_enabled') is not False:raise ValueError('切换交易模式前请先关闭引擎')
        merged=migrate_settings(current | normalized)
        validate_slots(merged)
        if merged['min_duration']>merged['max_duration']: raise ValueError('最短周期不能超过最长周期')
        with self.transaction():
            for k,v in normalized.items():
                if k not in BLOCKS: self.conn.execute('INSERT OR REPLACE INTO settings VALUES (?,?)',(k,json.dumps(v)))
            stored_services=self.meta('settings', {})
            if set(normalized) & set(BLOCKS) or any(stored_services.get(k) != merged[k] for k in stored_services if k in BLOCKS):
                self.set_meta('settings',{k:merged[k] for k in BLOCKS})
        return self.settings()
    def record_evaluation(self,ts,market,voters,ensemble_p):
        with self.transaction():
            self.execute('INSERT INTO eval_history(ts,market_id,symbol,window_start,window_end,voters_json,ensemble_p) VALUES (?,?,?,?,?,?,?)',
                         (ts,str(market['market_id']),market['symbol'],market['start_time'],market['end_time'],
                          json.dumps(voters,ensure_ascii=False),ensemble_p))
            self.execute('DELETE FROM eval_history WHERE id NOT IN (SELECT id FROM eval_history ORDER BY id DESC LIMIT 5000)')

    def score_evaluations(self):
        """Use only the opening/closing spot prices supplied by settlement backfill."""
        with self.transaction():
            starts=self.meta('market_prices',{})
            ends=self.meta('settlement_prices',{})
            for row in self.rows('SELECT h.* FROM eval_history h LEFT JOIN eval_scores s ON s.eval_id=h.id WHERE s.eval_id IS NULL ORDER BY h.id'):
                start=starts.get(row['market_id'],{}).get('start_price')
                end=ends.get(row['market_id'])
                if start is None or end is None or row['window_end']>time.time():continue
                start,end=float(start),float(end)
                if not all(math.isfinite(p) and p>0 for p in (start,end)):continue
                outcome='UP' if end>start else 'DOWN'
                voters=json.loads(row['voters_json'])
                voters=voters.values() if isinstance(voters,dict) else voters
                votes={}
                for voter in voters:
                    if voter.get('name') in RETIRED_VOTERS:continue
                    if voter.get('status')!='ok' or voter.get('p_up') is None:continue
                    p=float(voter['p_up'])
                    if not math.isfinite(p) or not 0<=p<=1:continue
                    call='UP' if p>.52 else 'DOWN' if p<.48 else None
                    if call is not None:votes[voter['name']]=int(call==outcome)
                self.execute('INSERT INTO eval_scores VALUES (?,?,?,?,?)',
                             (row['id'],row['symbol'],row['window_start'],outcome,json.dumps(votes)))
            # A window is identified by symbol and start, independent of how many
            # evaluations or venue market IDs referred to it. No-calls add no samples.
            recent="SELECT symbol,window_start FROM eval_scores GROUP BY symbol,window_start ORDER BY window_start DESC,symbol LIMIT 300"
            windows={}
            for row in self.rows('SELECT * FROM eval_scores WHERE (symbol,window_start) IN ('+recent+') ORDER BY eval_id'):
                # Re-evaluating a window must not turn it into multiple samples.
                windows.setdefault((row['symbol'],row['window_start']),{}).update(json.loads(row['votes_json']))
            stats={}
            for votes in windows.values():
                for name,correct in votes.items():
                    if name in RETIRED_VOTERS:continue
                    counts=stats.setdefault(name,{'scored':0,'correct':0})
                    counts['scored']+=1;counts['correct']+=correct
            self.set_meta('voter_stats',stats)
            # Retain markers for all history rows to make scoring idempotent, and
            # retain the rolling stats even after their source history is pruned.
            self.execute('DELETE FROM eval_scores WHERE eval_id NOT IN (SELECT id FROM eval_history) AND (symbol,window_start) NOT IN ('+recent+')')

    def meta(self,key,default=None):
        r=self.rows('SELECT value FROM meta WHERE key=?',(key,))
        return json.loads(r[0]['value']) if r else default
    def set_meta(self,key,value):
        self.execute('INSERT OR REPLACE INTO meta VALUES (?,?)',(key,json.dumps(value,ensure_ascii=False)))
    @contextmanager
    def transaction(self):
        with self.lock:
            name='txn_'+uuid4().hex
            self.conn.execute('SAVEPOINT '+name)
            try:
                yield
                self.conn.execute('RELEASE SAVEPOINT '+name)
            except BaseException:
                self.conn.execute('ROLLBACK TO SAVEPOINT '+name)
                self.conn.execute('RELEASE SAVEPOINT '+name)
                raise
    def close(self): self.conn.close()
