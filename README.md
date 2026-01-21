# latte-flow

Local-first Airflow ETL pipeline for coffee chain POS data.

```
    ( (
     ) )
  .______.
  |      |]
  \      /    bronze → silver → gold → alerts
   `----'
```

## What it does

Ingests daily POS exports (Zettle, Square, etc.) and transforms them through a medallion architecture:

| Layer | Format | Purpose |
|-------|--------|---------|
| **Bronze** | CSV | Raw POS exports, untouched |
| **Silver** | Parquet | Cleaned, typed, enriched with product metadata |
| **Gold** | Parquet | `fact_sales` + `dim_product` for analytics |
| **Alerts** | JSON | Low stock warnings (future SNS integration) |

## Quick start

```bash
# spin up airflow + postgres + localstack
docker-compose up -d

# wait ~30s, then open
open http://localhost:8080
# login: admin / admin
```

Drop a CSV in `data/bronze/`, trigger the DAG, watch the magic.

## CSV format

```csv
date,branch_id,product_id,sales_qty,stock_left,price_eur
2026-01-18,1,101,12,8,2.50
```

## DAG structure

```
alive
  │
  ▼
bronze_to_parquet
  │
  ▼
silver_clean
  │
  ├────────────────┐
  ▼                ▼
gold_fact_sales    gold_dim_product
  │
  ▼
check_stock_alerts
```

## Project structure

```
latte-flow/
├── dags/
│   └── etl_dag.py          # the pipeline
├── data/
│   ├── bronze/             # raw CSVs land here
│   ├── silver/             # parquet (raw + cleaned_*)
│   ├── gold/               # fact_sales, dim_product
│   └── alerts/             # low stock JSON alerts
├── docker-compose.yml
├── requirements.txt
└── .env
```

## Configuration

Edit `etl_dag.py` to customize:

```python
SAFETY_STOCK_THRESHOLD = 5  # alert when stock_left < this

PRODUCT_CATEGORIES = {
    101: {"name": "Espresso", "category": "coffee"},
    # add your products...
}
```

## Alert output

```json
{
  "alerts": [
    {
      "type": "LOW_STOCK",
      "severity": "critical",
      "branch_id": 2,
      "product_id": 102,
      "product_name": "Latte",
      "stock_left": 2,
      "threshold": 5
    }
  ],
  "count": 1
}
```

Severity: `critical` (≤2 units) | `warning` (3-4 units)

## Roadmap

- [ ] SNS alerts via LocalStack → AWS
- [ ] Branch dimension table
- [ ] Incremental processing (skip already-processed CSVs)
- [ ] dbt integration for gold layer
- [ ] Grafana dashboard

## Stack

- **Airflow 2.8** — orchestration
- **Pandas + PyArrow** — transforms
- **PostgreSQL** — Airflow metadata
- **LocalStack** — AWS emulation (SNS/S3)
- **Docker Compose** — local deployment

---

*Built for coffee chains that care about their espresso stock levels.*
