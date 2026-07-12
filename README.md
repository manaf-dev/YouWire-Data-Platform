# YouWire Data Platform

End-to-end **Microsoft Fabric** batch analytics solution for a fictional cross-border payments company.

> Inspired by fintech platforms that enable global B2B payments, virtual accounts, and multi-currency wallets.

## Architecture

```
Bronze (OneLake Files)          Silver (Delta tables)         Gold (Star schema)           Consumption
─────────────────────          ─────────────────────         ──────────────────           ─────────────
customers.csv          ──►      silver_customers      ──►     gold_dim_customer
fx_rates.csv           ──►      silver_fx_rates       ──►     gold_dim_corridor
fee_schedule.csv       ──►      silver_fee_schedule   ──►     gold_dim_currency    ──►   sm_youwire_analytics
payments_*.json        ──►      silver_payments       ──►     gold_dim_date               (Direct Lake)
                                                          ──►   gold_fact_payment      ──►   rpt_youwire_executive
```

Orchestrated by **`pl_youwire_batch_daily`** — a pipeline that runs `nb_bronze_to_silver` → `nb_silver_to_gold`.

## Fabric items

| Item | Type | Purpose |
|---|---|---|
| `lh_youwire` | Lakehouse | Medallion storage (Bronze / Silver / Gold) |
| `nb_bronze_to_silver` | Notebook | Cleanses raw CSV/JSON into Silver Delta tables |
| `nb_silver_to_gold` | Notebook | Builds star schema with surrogate keys |
| `pl_youwire_batch_daily` | Pipeline | Orchestrates Silver + Gold transforms |
| `sm_youwire_analytics` | Semantic model | Direct Lake model on Gold tables |
| `rpt_youwire_executive` | Report | Executive + operations dashboards |

## Data volume (synthetic)

| Layer | Table | Rows |
|---|---|---|
| Bronze | customers | 750 |
| Bronze | payments (4 JSON files) | 48,788 |
| Bronze | fx_rates | 6,579 |
| Bronze | fee_schedule | 16 |
| Gold | gold_fact_payment | 48,788 |

## Key metrics

- Total payment volume (USD)
- Fee revenue (USD)
- Payment count & success rate
- Volume by corridor and customer segment
- Failure rate by corridor
- Average settlement time

## Skills demonstrated

- Medallion lakehouse architecture (Bronze → Silver → Gold)
- PySpark notebooks with data quality assertions
- Star schema modeling with surrogate keys
- Pipeline orchestration with activity dependencies
- Direct Lake semantic model with DAX measures
- Power BI reporting with dimensional filtering

## Tech stack

- Microsoft Fabric (Lakehouse, Notebooks, Pipelines, Semantic model, Power BI)
- Delta Lake tables in OneLake
- PySpark / Python
- DAX

## Business question

*How is YouWire performing by payment corridor, currency, and customer segment?*
