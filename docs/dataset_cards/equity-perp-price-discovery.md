# Equity and pre-IPO perpetual prices

Snapshots of perpetual-futures mark prices, index prices and basis from Aevo. The instrument universe includes equities, ETFs, commodities, foreign exchange, pre-IPO contracts and crypto assets.

## Contents

| Table | Record |
|---|---|
| `perpetual_mark_and_index_prices` | An instrument's mark price, index price and basis at an observation time |

## Using the data

`market_type` identifies the instrument category. `is_rwa` flags the real-world-asset subset. Crypto contracts provide a comparison group when studying equity-linked contracts outside cash-market hours.

`basis_bps` is `(mark - index) / index * 10,000`. Missing inputs produce a null basis. Group by instrument and observation time to study how the venue's mark changes relative to its reference index.

For comparisons with cash markets or other venues, align timestamps and use only information available at the observation time. Inspect the ages of matched observations; a later reference price introduces look-ahead bias.

## Limitations

- Marks are venue reference prices used for margining. They are not executable bids, asks or necessarily recent trades.
- Index construction may differ by instrument. An equity index observed outside cash-market hours should not be assumed to equal the last cash close.
- Pre-IPO contract indices are venue-defined references, not public share prices.
- Instrument listings and coverage change over time. Count the available instruments in each analysis period.
- Basis alone does not establish a tradable opportunity. This table does not provide complete hedge execution, funding or borrowing costs.
