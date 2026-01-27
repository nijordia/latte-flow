# latte-flow

Local-first Airflow ETL pipeline for a Barcelona coffee chain with warehouse logistics.

```
    ( (
     ) )
  .______.
  |      |]
  \      /    warehouse → shipments → branches → sales
   `----'
```

## What it does

Centralizes POS exports from 5 Barcelona coffee shops, tracks ingredient inventory at both warehouse and branch level, and manages shipments with theft tracking.

| Layer | Format | Purpose |
|-------|--------|---------|
| **Bronze** | CSV | Raw POS exports from branches |
| **Silver** | Parquet | Cleaned, typed, computed metrics |
| **Gold** | Parquet | Facts, dimensions, inventory, shipments |
| **Alerts** | JSON | Low warehouse stock warnings |

## Quick start

```bash
# generate fake data (30 days, 500 rows, 5 branches)
python scripts/generate_fake.py

# spin up airflow + postgres + localstack
docker-compose up -d

# wait ~30s, then open
open http://localhost:8080
# login: admin / admin
```

Trigger the DAG, watch shipments flow.

## Inventory model

```
┌─────────────────────────────────────────────────────────────┐
│                    ZONA FRANCA HQ                           │
│                    (Central Warehouse)                      │
│  espresso: 10kg | milk: 200L | sugar: 20kg | flour: 50kg   │
└─────────────────────┬───────────────────────────────────────┘
                      │ shipments (2x min_reorder)
                      │ drivers steal 5-10%
          ┌───────────┼───────────┬───────────┬───────────┐
          ▼           ▼           ▼           ▼           ▼
     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
     │ Gràcia  │ │Eixample │ │ El Born │ │Beachfnt │ │ Sarrià  │
     │ 2kg esp │ │ 2kg esp │ │ 2kg esp │ │ 2kg esp │ │ 2kg esp │
     │ 40L milk│ │ 40L milk│ │ 40L milk│ │ 40L milk│ │ 40L milk│
     └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘
          │           │           │           │           │
          └───────────┴───────────┴───────────┴───────────┘
                              │
                              ▼ sales deduct from branch
```

**Oversold handling:** The pipeline treats POS sales data as historical truth. If a branch sells more units than available ingredients, stock is capped at 0 in `branch_inventory.parquet` and an `OVERSOLD_BRANCH` alert is generated. This reflects real-world retail behavior where baristas may serve the last item before noticing stock is depleted.

## Shipment flow

1. **Nightly DAG run** processes the day's sales
2. **Check branch stock**: if ingredient < min_reorder
3. **Check warehouse**: if enough stock → send 2x min_reorder
4. **Create shipment**: `sent_qty`, `received_qty=None`, `status=pending`
5. **Next day**: confirm shipment with theft (5-10% shrinkage)
6. **Update branch**: `current_stock += confirmed_qty`
7. **If warehouse empty**: alert, no shipment

## Trust but verify

```
sent_qty: 200        ← what left the warehouse
received_qty: 200    ← what should have arrived
confirmed_qty: 186   ← what actually arrived (drivers steal)
shipment_received_at: 2026-01-15
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
  ▼
gold_fact_sales
  │
  ├────────────────────┬────────────────────┐
  ▼                    ▼                    ▼
gold_dim_product    gold_dim_branch    process_inventory_and_shipments
                                           │
                                           ▼
                                    check_warehouse_alerts
```

## Project structure

```
latte-flow/
├── dags/
│   └── etl_dag.py              # the pipeline
├── configs/
│   ├── products.yaml           # product catalog
│   ├── recipes.yaml            # ingredients per product
│   ├── branches.yaml           # 5 Barcelona locations + warehouse
│   └── inventory.yaml          # warehouse + branch stock levels
├── scripts/
│   └── generate_fake.py        # fake data generator
├── data/
│   ├── bronze/                 # raw CSVs
│   ├── silver/                 # parquet
│   ├── gold/                   # fact + dim tables + shipments
│   └── alerts/                 # JSON alerts
├── docker-compose.yml
├── requirements.txt
└── .env
```

## Barcelona branches

| ID | Location | Region |
|----|----------|--------|
| 1 | Gràcia | Barcelona |
| 2 | Eixample | Barcelona |
| 3 | El Born | Barcelona |
| 4 | Beachfront | Barcelona |
| 5 | Sarrià | Barcelona |
| HQ | Zona Franca | Barcelona |

## Gold outputs

| File | Description |
|------|-------------|
| `fact_sales.parquet` | All sales transactions |
| `dim_product.parquet` | Product catalog |
| `dim_branch.parquet` | Branch locations |
| `dim_warehouse.parquet` | Central warehouse stock |
| `branch_inventory.parquet` | Per-branch ingredient stock |
| `shipment.parquet` | All shipments with theft tracking |

## Shipment schema

```
shipment_id         : int
date_sent           : date
branch_id           : int
ingredient_id       : string
sent_qty            : float
received_qty        : float (expected)
confirmed_qty       : float (actual, after theft)
shipment_received_at: date
status              : pending | confirmed
```

## Initial stock levels

**Warehouse (Zona Franca HQ):**
- Espresso: 10kg
- Milk: 200L
- Sugar: 20kg
- Flour: 50kg
- Butter: 20kg
- Oranges: 500 units
- Muffins: 200 units

**Each branch (1/5 of warehouse):**
- Espresso: 2kg
- Milk: 40L
- Sugar: 4kg
- Flour: 10kg
- Butter: 4kg
- Oranges: 100 units
- Muffins: 40 units

## Alert output

```json
{
  "alerts": [
    {
      "type": "WAREHOUSE_EMPTY",
      "severity": "critical",
      "ingredient_id": "milk",
      "ingredient_name": "Whole Milk",
      "warehouse_stock": 0,
      "requested_qty": 4000,
      "branch_id": 3
    }
  ],
  "count": 1
}
```

## Roadmap

- [x] Barcelona-centric branches
- [x] Warehouse/branch inventory split
- [x] Shipment tracking with theft simulation
- [x] Trust but verify (confirmed_qty)
- [ ] SNS alerts via LocalStack → AWS
- [ ] Supplier restocking for warehouse
- [ ] Driver performance tracking (theft rate per driver)
- [ ] Grafana dashboard

## Stack

- **Airflow 2.8** — orchestration
- **Pandas + PyArrow** — transforms
- **PostgreSQL** — Airflow metadata
- **LocalStack** — AWS emulation
- **Docker Compose** — local deployment
- **PyYAML** — config management

**built with**
Nic — the human brain
Claude — the pretty boy
Gork — drunk ghost who outs milk thieves

---

*Built for Barcelona coffee chains that don't trust their drivers.*
