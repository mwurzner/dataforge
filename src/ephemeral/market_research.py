"""Bounded public-data observations for slow carry and payoff-identity research.

No wallet, order, transaction generation or signing methods. All prices remain observations,
not assumed fills. Source payloads retain contract units and settlement conventions.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
import json
import time

import requests


POLY_IDS = ('4052418', '2589810', '2589811', '2589812', '2589813', '2589814')
MAX_PAYLOAD = 65536


class Reader:
    def __init__(self):
        self.rows = []

    def read(self, source, kind, key, url, *, params=None, body=None, keep=True):
        start = time.time()
        mono = time.monotonic()
        payload, error, status = None, None, None
        try:
            if body is not None and url != 'https://clob.polymarket.com/books':
                raise ValueError('Only the public batch-book POST is allowed')
            with requests.request('POST' if body is not None else 'GET', url, params=params,
                                  json=body, timeout=(10, 35), stream=True,
                                  headers={'User-Agent':'dataforge-research/1.0'}) as response:
                status = response.status_code
                response.raise_for_status()
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > 8_000_000:
                        raise ValueError('Response exceeds bounded discovery size')
                    chunks.append(chunk)
                payload = json.loads(b''.join(chunks))
                if isinstance(payload, dict) and payload.get('error'):
                    raise ValueError('Source returned an API error')
        except Exception as exc:
            error = f'{type(exc).__name__}: HTTP {status}'
        received = time.time()
        envelope = dict(source=source, kind=kind, key=str(key), request_started_ts=start,
                        response_received_ts=received, latency_s=time.monotonic()-mono,
                        http_status=status, error=error, source_url=url)
        if keep or error:
            self.add(envelope, payload)
        if error:
            raise RuntimeError(error)
        return payload, envelope

    def add(self, envelope, payload):
        encoded = json.dumps(payload, separators=(',', ':'), sort_keys=True, allow_nan=False)
        if len(encoded.encode()) > MAX_PAYLOAD:
            self.rows.append(dict(envelope, error='PayloadTooLarge', payload_json='null'))
            return
        self.rows.append(dict(envelope, payload_json=encoded))


def ladder(instruments, spot, now):
    """Three strikes, paired puts/calls, two expiries. No naming-based contract parsing."""
    eligible = [i for i in instruments if i.get('is_active') and i.get('kind') == 'option'
                and now+2*86400 <= i['expiration_timestamp']/1000 <= now+90*86400]
    expiries = sorted({i['expiration_timestamp'] for i in eligible})[:2]
    selected = []
    for expiry in expiries:
        group = [i for i in eligible if i['expiration_timestamp'] == expiry]
        paired = {i['strike'] for i in group if i.get('option_type') == 'call'} & {
                  i['strike'] for i in group if i.get('option_type') == 'put'}
        strikes = sorted(paired, key=lambda s:(abs(s-spot),s))[:3]
        selected.extend(i for i in group if i['strike'] in strikes)
    return sorted(selected, key=lambda i:i['instrument_name'])[:12]


def deribit(reader):
    base = 'https://www.deribit.com/api/v2/public/'
    selected = []
    for currency in ('BTC','ETH'):
        instruments, env = reader.read('deribit','discovery',currency,base+'get_instruments',
                                       params={'currency':currency,'kind':'option','expired':'false'},keep=False)
        index, _ = reader.read('deribit','index',currency,base+'get_index_price',
                               params={'index_name':currency.lower()+'_usd'})
        chosen = ladder(instruments['result'], index['result']['index_price'], time.time())
        reader.add(dict(env, kind='universe_selection'),
                   {'available':len(instruments['result']), 'selected':[i['instrument_name'] for i in chosen],
                    'rule':'nearest 3 paired strikes, first 2 expiries in 2..90 days'})
        for instrument in chosen:
            reader.add(dict(env,kind='instrument',key=instrument['instrument_name']),instrument)
            selected.append(instrument['instrument_name'])
        perp, perp_env = reader.read('deribit','instrument',currency+'-PERPETUAL',base+'get_instrument',
                                      params={'instrument_name':currency+'-PERPETUAL'})
        selected.append(currency+'-PERPETUAL')
    # Source timestamps on books, receive timestamps on envelopes; no simultaneous-fill claim.
    def book(name):
        try:
            reader.read('deribit','book',name,base+'get_order_book',params={'instrument_name':name,'depth':5})
        except RuntimeError:
            pass  # The failed attempt is already a row.
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(book, selected))
    for product in ('BTC-USD','ETH-USD'):
        try:
            payload, env = reader.read('coinbase','discovery',product,
                f'https://api.exchange.coinbase.com/products/{product}/book',params={'level':1},keep=False)
            reader.add(dict(env,kind='spot_book'),payload)
        except RuntimeError:
            pass


def take_cost(asks, shares):
    """Actual displayed ask-side depth; return None if the book cannot supply the size."""
    left, cost = Decimal(str(shares)), Decimal(0)
    for level in sorted(asks, key=lambda x:Decimal(str(x['price']))):
        price, size = Decimal(str(level['price'])), Decimal(str(level['size']))
        if price <= 0 or price > 1 or size <= 0:
            continue
        amount = min(left,size)
        cost += amount*price
        left -= amount
        if left == 0:
            return str(cost)
    return None


def polymarket(reader):
    for market_id in POLY_IDS:
        try:
            market, meta_env = reader.read('polymarket','market',market_id,
                f'https://gamma-api.polymarket.com/markets/{market_id}',keep=False)
            # Exact rules retained; remove only display artwork and nested duplicate event cards.
            metadata = {k:v for k,v in market.items() if k not in
                        ('events','image','icon','imageOptimized','iconOptimized')}
            reader.add(dict(meta_env,kind='market'),metadata)
            if market.get('closed') or not market.get('acceptingOrders'):
                continue  # Keep following the fixed cohort through closure/resolution.
            tokens = json.loads(market['clobTokenIds']) if isinstance(market['clobTokenIds'],str) else market['clobTokenIds']
            outcomes = json.loads(market['outcomes']) if isinstance(market['outcomes'],str) else market['outcomes']
            if len(tokens) != 2 or {str(x).lower() for x in outcomes} != {'yes','no'}:
                raise ValueError('Cohort market is not a binary YES/NO pair')
            books, env = reader.read('polymarket','book_batch',market_id,
                'https://clob.polymarket.com/books',body=[{'token_id':t} for t in tokens],keep=False)
            by_token = {b['asset_id']:b for b in books}
            for token,outcome in zip(tokens,outcomes):
                book = by_token[token]
                # Preserve ten best levels on each side, with counts indicating bounded depth.
                compact = {k:v for k,v in book.items() if k not in ('asks','bids')}
                compact.update(outcome=outcome, market_id=market_id,
                    total_ask_levels=len(book['asks']), total_bid_levels=len(book['bids']),
                    asks=sorted(book['asks'],key=lambda x:Decimal(x['price']))[:10],
                    bids=sorted(book['bids'],key=lambda x:Decimal(x['price']),reverse=True)[:10])
                reader.add(dict(env,kind='book',key=token),compact)
                by_token[token] = compact
                try:
                    reader.read('polymarket','fee_parameters',token,
                        'https://clob.polymarket.com/fee-rate',params={'token_id':token})
                except RuntimeError:
                    pass
            checks = []
            for size in (10,100,1000):
                costs = [take_cost(by_token[t]['asks'],size) for t in tokens]
                checks.append({'shares_per_outcome':size,'leg_costs':costs,
                    'gross_surplus_before_all_costs':str(Decimal(size)-sum(Decimal(c) for c in costs))
                        if all(c is not None for c in costs) else None})
            reader.add(dict(env,kind='complete_pair_screen',key=market_id),
                {'checks':checks,'fees_applied':False,'execution_verified':False,
                 'neg_risk':market.get('negRisk'),'tokens':tokens,
                 'book_timestamps':[by_token[t].get('timestamp') for t in tokens]})
        except Exception as exc:
            reader.add(dict(source='polymarket',kind='source_error',key=market_id,
                            response_received_ts=time.time(),error=type(exc).__name__),None)


def pendle(reader):
    markets = []
    observed = {}
    total = None
    for skip in range(0,2000,100):
        body, env = reader.read('pendle','discovery',str(skip),
            'https://api-v2.pendle.finance/core/v2/markets/all',params={'limit':100,'skip':skip},keep=False)
        markets.extend(body['results'])
        for market in body['results']:
            observed[market['pt']] = env
        total = body['total']
        if len(markets) >= total or not body['results']:
            break
        time.sleep(1)  # Be conservative with the provider's free request budget.
    now = datetime.now(timezone.utc)
    eligible = []
    for market in markets:
        expiry = datetime.fromisoformat(market['expiry'].replace('Z','+00:00'))
        if 7 <= (expiry-now).total_seconds()/86400 <= 180:
            eligible.append(market)
    chosen = sorted(eligible,key=lambda m:float(m.get('details',{}).get('liquidity',0)),reverse=True)[:8]
    reader.add(dict(env,kind='universe_selection',key='all'),
               {'discovered':len(markets),'reported_total':total,'pagination_complete':len(markets)>=total,
                'selected':[m['pt'] for m in chosen], 'rule':'top 8 liquidity, maturity 7..180 days'})
    for market in chosen:
        # These are indicative protocol analytics, NOT size-specific executable PT quotes.
        payload = {k:v for k,v in market.items() if k not in ('icon',)}
        reader.add(dict(observed[market['pt']],kind='indicative_yield',key=market['pt']),payload)


def collect():
    def run(fn):
        reader = Reader()
        try:
            fn(reader)
        except Exception as exc:
            reader.add(dict(source=fn.__name__,kind='source_error',key='round',
                            response_received_ts=time.time(),error=type(exc).__name__),None)
        return reader.rows
    with ThreadPoolExecutor(max_workers=3) as pool:
        return [row for rows in pool.map(run,(deribit,polymarket,pendle)) for row in rows]
