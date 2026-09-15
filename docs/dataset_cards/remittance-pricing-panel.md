# Remittance pricing panel

Repeated observations from Wise's public remittance comparison service, covering exchange rates, stated fees and estimated recipient amounts across selected currency corridors.

## Contents

| Table | Record |
|---|---|
| `e13_remittance_quotes` | A provider's displayed comparison for a corridor and send amount, with rate, fees and recipient amount |

## Using the data

The collector queries twenty configured currency corridors at specified send amounts. Compare providers within the same corridor, amount and observation round. Recipient amounts provide a combined view of the displayed exchange rate and explicit fees.

Separate the collection timestamp from the source's price-update timestamp. A recently collected row can contain older competitor pricing. Wise's competitor comparisons can use previously collected markups applied to updated reference exchange rates, so these records should not be described as simultaneous executable quotes obtained directly from every provider.

## Limitations

- Wise operates the comparison service and is also one of the providers being compared. Provider selection and comparison methodology reflect that source.
- Competitor information can be stale or reconstructed. Check update times before treating a change as a new provider quote.
- Actual transfer terms can depend on payment method, customer eligibility, limits and verification requirements.
- Provider coverage varies by corridor and date. The configured currency pairs do not constitute a complete survey of remittance markets.
- Differences in displayed recipient amounts describe the comparison service's observations; they do not prove realised customer savings.
