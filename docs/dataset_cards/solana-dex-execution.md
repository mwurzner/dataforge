# Solana DEX quotes and routing

Jupiter swap quotes at fixed SOL input sizes, with the route legs returned for each quote. The panel supports comparisons of quoted output, reported price impact and routing across sizes and observation times.

## Contents

| Table | Record |
|---|---|
| `solana_swap_quotes` | A pair and input amount, with quoted output, price impact, value and route-leg count |
| `solana_swap_routes` | A leg of the chosen route, including venue, amounts and allocation |

## Using the data

Join quotes and routes on `(round_ts, input_symbol, output_symbol, in_amount)`. Inspect `n_legs` and each leg's `percent` when analysing split routes. The configured size ladder is 1, 50 and 1,000 SOL across five pairs, all with SOL as the input.

`price_impact_pct` is Jupiter's reported estimate. `amm_report_raw` preserves the provider's venue report as JSON. Entries may refer to amounts outside the requested pair or contain error strings; they should not be converted directly into competing-venue prices.

## Limitations

- The panel records one aggregator's responses. Its selected route does not establish the best price available across the entire market.
- Quotes are indicative and are not executed by the collector. Transaction landing, priority fees, slippage and intervening pool changes affect actual results.
- SOL-input quotes do not provide both legs of a closed trading cycle.
- Failed requests have explicit error rows with null measurements. Separate these from successful quotes with unusual routes.
- Sampling captures periodic snapshots and misses changes between requests.
