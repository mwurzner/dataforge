# Fiat on-ramp pricing

Indicative fiat-to-crypto pricing observations from public provider endpoints. The panel includes Mercuryo buy and sell quotes, plus Ramp reference prices and fee bounds.

## Contents

| Table | Record |
|---|---|
| `e12_onramp_quotes` | A provider quote or reference-price observation for an asset and fiat currency, with available amounts and fee fields |

## Using the data

Separate rows by `provider`, `kind` and `side` before comparing prices. A Ramp reference price and fee range should not be treated as a complete, amount-specific purchase quote.

`effective_rate` is expressed as fiat currency per whole coin and is derived from returned amounts. The panel retains the quoted rate and explicit fee separately. Mercuryo's fee inclusion in returned amounts is not fully documented, so inspect `effective_rate`, `rate_net_of_fee` and the underlying amount fields rather than assuming one represents the final charge.

Compare like-for-like currencies, assets, amounts and observation times. Account for both sides' fees when evaluating a buy-and-sell calculation.

## Limitations

- Coverage is limited to endpoints accessible without a customer API key. It is not a survey of the full on-ramp market.
- Quotes are indicative. Identity checks, payment method, issuer charges, limits and price changes can affect an actual purchase or sale.
- Failed requests are retained in `error`. They should not be interpreted as zero prices or successful quotes.
- Provider and asset coverage can change over time. These records alone do not demonstrate executable arbitrage.
