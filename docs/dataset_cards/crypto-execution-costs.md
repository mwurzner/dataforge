# Crypto execution quotes and order-book depth

Ethereum swap quotes, route details and selected perpetual-futures order-book snapshots. The tables support comparisons of quoted output, provider fees, routing choices and available depth at the time of collection.

## Contents

| Table | Record |
|---|---|
| `ethereum_swap_quotes` | A swap quote at a specified size, with available provider-fee details |
| `ethereum_swap_routes` | A route leg, with venue, pool and amount where supplied |
| `perpetual_order_book_depth` | An order-book snapshot summarising spread and resting notional within bands around the midpoint |

## Using the data

Compare quotes for the same pair, direction and size at comparable observation times. Inspect the provider-fee fields before attributing differences in output to routing quality.

Route detail varies by provider. Some responses identify individual pools and amounts; others supply only venue names. Null `pool` and `swap_amount_raw` fields may therefore indicate unavailable detail rather than a failed quote.

Depth fields summarise cumulative resting notional within a specified distance of the midpoint. A narrow band can contain zero depth when the bid-ask spread is wider than that band.

## Limitations

- Quotes and resting orders are observations, not completed trades. Execution costs also depend on latency, slippage, network fees and available size.
- Provider coverage differs across pairs. Check `error` for failed requests and rate-limit skips before calculating comparisons.
- The depth panel covers a single venue and periodic snapshots, not a consolidated or tick-level order book.
- A price difference between quotes does not establish profitable arbitrage. That requires executable prices for the complete trading cycle and all associated costs.
