# Crypto options surface and order books

Snapshots of listed options on Aevo, including marks, implied volatility, Greeks and a selected set of order books. The tables support analysis of volatility surfaces and the relationship between published marks and quoted prices.

## Contents

| Table | Record |
|---|---|
| `options_surface` | An instrument's strike, expiry, mark, forward, implied volatility and Greeks |
| `options_order_book` | Best bid and ask, available sizes and quoted implied volatility for a selected instrument |

## Using the data

The venue supplies `iv`, `delta`, `gamma`, `vega`, `theta` and `rho`; these values are not recalculated by the collector. Group on `(asset, sampled_ts)` to reconstruct a sampled surface, then add `expiry` to examine a volatility smile. `round_ts` groups assets fetched during the same collection pass, although requests need not complete simultaneously.

The order-book panel covers a near-the-money strike ladder across selected expiries. Join it to the surface on `instrument_name` within the same round. `best_bid_iv`, `best_ask_iv` and `iv_spread` describe the quoted volatility range.

`strike` and `expiry` are parsed from instrument names and remain null when parsing fails. `expiry_ts` is supplied by the venue.

## Limitations

- The dataset covers one venue. A listed mark does not establish liquidity or an executable price.
- Order books cover a subset of the surface. Check both available size and spread before using a quote in execution analysis.
- A one-sided book has a null missing side. Request failures are recorded separately in `error`.
- Sampling misses changes between observations, and collection timestamps include network latency.
- Put and call implied volatilities at the same strike and expiry may reflect the venue's shared pricing model; agreement is not independent validation.
