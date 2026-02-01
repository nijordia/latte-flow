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

# spin up airflow + postgres + localstack + grafana
docker-compose up -d

# wait ~30s, then open
open http://localhost:8080   # Airflow (admin / admin)
open http://localhost:3000   # Grafana (admin / admin)
```

Trigger the DAG, watch shipments flow. Gold tables load to PostgreSQL for Grafana.

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

**Oversold handling:** The pipeline treats POS sales data as historical truth. If a branch sells more units than available ingredients, stock is capped at 0 in `branch_inventory.parquet`. At the end of each day, one aggregated `BRANCH_RESTOCK_NEEDED` alert is generated per affected branch, summarizing all ingredients that need restocking. This reflects real-world retail behavior where baristas may serve the last item before noticing stock is depleted.

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
                                     ┌─────┴─────┐
                                     ▼           ▼
                          check_warehouse    load_gold_to
                              _alerts          _postgres
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
├── grafana/                    # Grafana data (auto-created)
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

Alerts are aggregated per branch per day. Each `BRANCH_RESTOCK_NEEDED` alert lists all ingredients that branch needs restocked, with `needed_qty` set to the standard shipment amount (2x min_reorder).

```json
{
  "alerts": [
    {
      "timestamp": "2026-01-15",
      "type": "BRANCH_RESTOCK_NEEDED",
      "branch_id": 2,
      "date": "2026-01-15",
      "severity": "critical",
      "items": [
        {
          "ingredient_id": "milk",
          "ingredient_name": "Whole Milk",
          "unit": "ml",
          "needed_qty": 4000,
          "warehouse_stock": 0,
          "reasons": [
            {"type": "OVERSOLD", "product_id": 102, "attempted_deduction": 180, "was_stock": 50},
            {"type": "WAREHOUSE_EMPTY", "requested_qty": 4000, "warehouse_stock": 0}
          ]
        }
      ]
    }
  ],
  "count": 1
}
```

**Severity levels:**
- `warning` — Branch had oversold events but shipments were fulfilled
- `critical` — Warehouse couldn't fulfill a shipment request

**Reason types:**
- `OVERSOLD` — Branch sold more than available stock
- `WAREHOUSE_EMPTY` — Warehouse couldn't fulfill the restock request

## Grafana dashboards

Gold tables are loaded to PostgreSQL after each DAG run. Connect Grafana to query them.

### Setup datasource

1. Open Grafana: http://localhost:3000 (admin / admin)
2. Go to **Connections** → **Data sources** → **Add data source**
3. Select **PostgreSQL**
4. Configure:
   - Host: `postgres:5432`
   - Database: `airflow`
   - User: `airflow`
   - Password: `airflow`
   - TLS/SSL Mode: `disable`
5. Click **Save & test**

### Tables available

| Table | Description |
|-------|-------------|
| `fact_sales` | All sales transactions |
| `dim_product` | Product catalog |
| `dim_branch` | Branch locations |
| `dim_warehouse` | Warehouse stock levels |
| `branch_inventory` | Per-branch ingredient stock |
| `shipment` | Shipments with theft tracking |

### Example queries

**Sales by branch and product:**
```sql
SELECT
  fs.date,
  db.location_name,
  dp.product_name,
  SUM(fs.qty) as total_qty,
  SUM(fs.revenue) as total_revenue
FROM fact_sales fs
JOIN dim_branch db ON fs.branch_id = db.branch_id
JOIN dim_product dp ON fs.product_id = dp.product_id
GROUP BY fs.date, db.location_name, dp.product_name
ORDER BY fs.date, total_revenue DESC;
```

**Latte sales by branch:**
```sql
SELECT
  fs.date,
  db.location_name,
  dp.product_name,
  fs.qty
FROM fact_sales fs
JOIN dim_branch db ON fs.branch_id = db.branch_id
JOIN dim_product dp ON fs.product_id = dp.product_id
WHERE dp.product_name = 'Latte';
```

**Theft analysis:**
```sql
SELECT
  date_sent,
  branch_id,
  ingredient_id,
  sent_qty,
  confirmed_qty,
  sent_qty - confirmed_qty as stolen,
  ROUND((sent_qty - confirmed_qty) / sent_qty * 100, 1) as theft_pct
FROM shipment
WHERE status = 'confirmed'
ORDER BY theft_pct DESC;
```

## Roadmap

- [x] Barcelona-centric branches
- [x] Warehouse/branch inventory split
- [x] Shipment tracking with theft simulation
- [x] Trust but verify (confirmed_qty)
- [x] Aggregated branch restock alerts (one per branch/day)
- [x] Grafana integration (gold → PostgreSQL)
- [ ] SNS alerts via LocalStack → AWS
- [ ] Supplier restocking for warehouse
- [ ] Driver performance tracking (theft rate per driver)

## Stack

- **Airflow 2.8** — orchestration
- **Pandas + PyArrow** — transforms
- **PostgreSQL** — Airflow metadata + gold tables
- **Grafana** — dashboards
- **LocalStack** — AWS emulation
- **Docker Compose** — local deployment
- **PyYAML** — config management

**built with**  
Nic — the human  
Claude — the pretty boy  
Gork —  the sarcastic ghost  

---

*Built for Barcelona coffee chains that don't trust their drivers.*
