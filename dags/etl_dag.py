import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


# Paths
DATA_DIR = Path("/opt/airflow/data")
CONFIG_DIR = Path("/opt/airflow/configs")
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
ALERTS_DIR = DATA_DIR / "alerts"

# Alert state file for dedup
ALERT_STATE_FILE = ALERTS_DIR / "alert_state.json"

# Initial product stock per branch (warn if sold > this)
INITIAL_PRODUCT_STOCK = 100

# Theft rate: drivers steal 5-10%
THEFT_MIN = 0.05
THEFT_MAX = 0.10


def load_config(name: str) -> dict:
    """Load a YAML config file."""
    path = CONFIG_DIR / f"{name}.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


default_args = {
    "owner": "latte-flow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def alive():
    print("latte-flow is alive - Barcelona edition")
    return "alive"


def bronze_to_parquet():
    """Load raw CSVs from bronze and convert to parquet."""
    csv_files = list(BRONZE_DIR.glob("*.csv"))

    if not csv_files:
        print("No CSV files found in bronze/")
        return []

    processed = []
    for csv_path in csv_files:
        print(f"Processing: {csv_path.name}")
        df = pd.read_csv(csv_path)

        parquet_name = csv_path.stem + ".parquet"
        parquet_path = SILVER_DIR / parquet_name
        df.to_parquet(parquet_path, index=False)

        print(f"Written: {parquet_path}")
        processed.append(str(parquet_path))

    return processed


def silver_clean():
    """Clean, type cast, compute metrics, and enrich silver data."""
    products_cfg = load_config("products")["products"]

    parquet_files = [
        f for f in SILVER_DIR.glob("*.parquet")
        if not f.name.startswith("cleaned_")
    ]

    if not parquet_files:
        print("No parquet files found in silver/")
        return []

    processed = []
    for pq_path in parquet_files:
        print(f"Cleaning: {pq_path.name}")
        df = pd.read_parquet(pq_path)

        df["date"] = pd.to_datetime(df["date"])
        df["branch_id"] = df["branch_id"].astype("int32")
        df["product_id"] = df["product_id"].astype("int32")
        df["qty"] = df["qty"].astype("int32")
        df["total_paid"] = df["total_paid"].astype("float32")

        df["unit_price_paid"] = df["total_paid"] / df["qty"]
        df["base_price"] = df["product_id"].map(
            lambda x: products_cfg.get(x, {}).get("base_price_eur", 0.0)
        ).astype("float32")
        df["discount_rate"] = (1 - df["unit_price_paid"] / df["base_price"]).clip(0, 1)
        df["revenue"] = df["total_paid"]

        df["product_name"] = df["product_id"].map(
            lambda x: products_cfg.get(x, {}).get("name", "Unknown")
        )
        df["category"] = df["product_id"].map(
            lambda x: products_cfg.get(x, {}).get("category", "unknown")
        )

        cleaned_path = SILVER_DIR / f"cleaned_{pq_path.name}"
        df.to_parquet(cleaned_path, index=False)
        print(f"Written: {cleaned_path}")
        processed.append(str(cleaned_path))

    return processed


def gold_fact_sales():
    """Build fact_sales table with cumulative stock tracking."""
    cleaned_files = list(SILVER_DIR.glob("cleaned_*.parquet"))

    if not cleaned_files:
        print("No cleaned parquet files found in silver/")
        return None

    dfs = [pd.read_parquet(f) for f in cleaned_files]
    df = pd.concat(dfs, ignore_index=True)

    df = df.sort_values(["branch_id", "product_id", "date"]).reset_index(drop=True)
    df["cumulative_qty"] = df.groupby(["branch_id", "product_id"])["qty"].cumsum()
    df["stock_left"] = INITIAL_PRODUCT_STOCK - df["cumulative_qty"]

    oversold = df[df["stock_left"] < 0]
    if not oversold.empty:
        for _, row in oversold.drop_duplicates(["branch_id", "product_id"]).iterrows():
            print(f"WARNING: Branch {row['branch_id']} sold more than {INITIAL_PRODUCT_STOCK} "
                  f"of product {row['product_id']} ({row['product_name']})")

    fact_sales = df[[
        "date", "branch_id", "product_id", "qty", "total_paid",
        "unit_price_paid", "base_price", "discount_rate", "revenue", "stock_left",
    ]].copy()
    fact_sales.insert(0, "sale_id", range(1, len(fact_sales) + 1))

    output_path = GOLD_DIR / "fact_sales.parquet"
    fact_sales.to_parquet(output_path, index=False)
    print(f"Written: {output_path} ({len(fact_sales)} rows)")

    return str(output_path)


def gold_dim_product():
    """Build dim_product table from config."""
    products_cfg = load_config("products")["products"]

    dim_product = pd.DataFrame([
        {
            "product_id": int(pid),
            "product_name": info["name"],
            "base_price": info["base_price_eur"],
            "category": info["category"],
        }
        for pid, info in products_cfg.items()
    ])

    dim_product["product_id"] = dim_product["product_id"].astype("int32")
    dim_product["base_price"] = dim_product["base_price"].astype("float32")

    output_path = GOLD_DIR / "dim_product.parquet"
    dim_product.to_parquet(output_path, index=False)
    print(f"Written: {output_path} ({len(dim_product)} rows)")

    return str(output_path)


def gold_dim_branch():
    """Build dim_branch table from config."""
    branches_cfg = load_config("branches")["branches"]

    dim_branch = pd.DataFrame([
        {
            "branch_id": int(bid),
            "location_name": info["name"],
            "region": info["region"],
        }
        for bid, info in branches_cfg.items()
    ])

    dim_branch["branch_id"] = dim_branch["branch_id"].astype("int32")

    output_path = GOLD_DIR / "dim_branch.parquet"
    dim_branch.to_parquet(output_path, index=False)
    print(f"Written: {output_path} ({len(dim_branch)} rows)")

    return str(output_path)


def process_inventory_and_shipments():
    """
    Process inventory with warehouse/branch model and shipments.

    Flow:
    1. Initialize warehouse and branch inventories
    2. Process each day chronologically:
       a. Confirm pending shipments from previous day (with theft)
       b. Deduct today's sales from branch inventory
       c. Check branch stock levels
       d. If low → check warehouse → create shipment (2x min_reorder)
       e. If warehouse empty → accumulate shortage
    3. At end of each date, flush aggregated BRANCH_RESTOCK_NEEDED alerts
    4. Save all outputs
    """
    recipes_cfg = load_config("recipes")["recipes"]
    inventory_cfg = load_config("inventory")
    branches_cfg = load_config("branches")["branches"]

    warehouse_cfg = inventory_cfg["warehouse"]
    branch_defaults = inventory_cfg["branch_defaults"]

    branch_ids = [int(bid) for bid in branches_cfg.keys()]
    ingredient_ids = list(warehouse_cfg.keys())

    # Initialize warehouse stock
    warehouse = {
        ing_id: {
            "name": info["name"],
            "unit": info["unit"],
            "initial_stock": info["initial_stock"],
            "current_stock": info["initial_stock"],
            "min_reorder": info["min_reorder"],
        }
        for ing_id, info in warehouse_cfg.items()
    }

    # Initialize branch stock (each branch starts with branch_defaults)
    branch_inventory = {}
    for branch_id in branch_ids:
        branch_inventory[branch_id] = {}
        for ing_id in ingredient_ids:
            branch_inventory[branch_id][ing_id] = {
                "initial_stock": branch_defaults[ing_id]["initial_stock"],
                "current_stock": branch_defaults[ing_id]["initial_stock"],
                "min_reorder": branch_defaults[ing_id]["min_reorder"],
            }

    # Load fact_sales to get daily consumption
    fact_sales_path = GOLD_DIR / "fact_sales.parquet"
    if not fact_sales_path.exists():
        print("fact_sales.parquet not found, using initial stock only")
        sales_df = pd.DataFrame()
    else:
        sales_df = pd.read_parquet(fact_sales_path)

    # Compute ingredient consumption per sale
    def get_ingredient_usage(product_id, qty):
        recipe = recipes_cfg.get(product_id, {})
        return {ing: amount * qty for ing, amount in recipe.items()}

    # Group sales by date and branch
    shipments = []
    alerts = []
    shipment_id = 0

    if not sales_df.empty:
        # Get unique dates sorted
        dates = sorted(sales_df["date"].unique())

        # Track pending shipments (sent yesterday, arrives today)
        pending_shipments = []

        for current_date in dates:
            date_str = pd.Timestamp(current_date).strftime("%Y-%m-%d")
            print(f"\n=== Processing {date_str} ===")

            # Track which branch/ingredients need restocking: {branch_id: {ing_id: {"reasons": [...]}}}
            branch_shortages = {}

            # 1. Confirm pending shipments from previous day (with theft)
            for pending in pending_shipments:
                theft_rate = random.uniform(THEFT_MIN, THEFT_MAX)
                confirmed_qty = pending["sent_qty"] * (1 - theft_rate)

                # Add to branch inventory
                branch_id = pending["branch_id"]
                ing_id = pending["ingredient_id"]
                branch_inventory[branch_id][ing_id]["current_stock"] += confirmed_qty

                # Update shipment record
                pending["received_qty"] = pending["sent_qty"]  # What should have arrived
                pending["confirmed_qty"] = round(confirmed_qty, 2)  # What actually arrived
                pending["shipment_received_at"] = date_str
                pending["status"] = "confirmed"

                stolen = pending["sent_qty"] - confirmed_qty
                print(f"  SHIPMENT CONFIRMED: {ing_id} to branch {branch_id}: "
                      f"sent={pending['sent_qty']:.0f}, received={confirmed_qty:.0f} "
                      f"(stolen={stolen:.0f}, {theft_rate*100:.1f}%)")

            pending_shipments = []

            # 2. Deduct today's sales from branch inventory
            today_sales = sales_df[sales_df["date"] == current_date]

            for _, sale in today_sales.iterrows():
                branch_id = int(sale["branch_id"])
                product_id = int(sale["product_id"])
                qty = int(sale["qty"])

                usage = get_ingredient_usage(product_id, qty)
                for ing_id, amount in usage.items():
                    if ing_id in branch_inventory[branch_id]:
                        old_stock = branch_inventory[branch_id][ing_id]["current_stock"]
                        new_stock = old_stock - amount

                        if new_stock < 0:
                            # Cap at zero and log warning
                            branch_inventory[branch_id][ing_id]["current_stock"] = 0
                            print(f"  OVERSOLD WARNING: Branch {branch_id} sold {qty} of product "
                                  f"{product_id} but only had {old_stock:.1f} {warehouse[ing_id]['unit']} "
                                  f"of {ing_id} left. Stock capped at 0.")

                            # Flag this ingredient for restock alert
                            if branch_id not in branch_shortages:
                                branch_shortages[branch_id] = {}
                            if ing_id not in branch_shortages[branch_id]:
                                branch_shortages[branch_id][ing_id] = {"reasons": []}
                            branch_shortages[branch_id][ing_id]["reasons"].append({
                                "type": "OVERSOLD",
                                "product_id": product_id,
                                "attempted_deduction": amount,
                                "was_stock": round(old_stock, 2),
                            })
                        else:
                            branch_inventory[branch_id][ing_id]["current_stock"] = new_stock

            # 3. Check branch stock levels and create shipments
            for branch_id in branch_ids:
                for ing_id in ingredient_ids:
                    branch_stock = branch_inventory[branch_id][ing_id]
                    current = branch_stock["current_stock"]
                    min_reorder = branch_stock["min_reorder"]

                    if current < min_reorder:
                        # Need to restock - request 2x min_reorder
                        request_qty = min_reorder * 2

                        # Check warehouse
                        if warehouse[ing_id]["current_stock"] >= request_qty:
                            # Create shipment
                            shipment_id += 1
                            warehouse[ing_id]["current_stock"] -= request_qty

                            shipment = {
                                "shipment_id": shipment_id,
                                "date_sent": date_str,
                                "branch_id": branch_id,
                                "ingredient_id": ing_id,
                                "sent_qty": request_qty,
                                "received_qty": None,
                                "confirmed_qty": None,
                                "shipment_received_at": None,
                                "status": "pending",
                            }
                            shipments.append(shipment)
                            pending_shipments.append(shipment)

                            print(f"  SHIPMENT SENT: {ing_id} to branch {branch_id}: "
                                  f"qty={request_qty:.0f} (branch had {current:.0f}, min={min_reorder:.0f})")
                        else:
                            # Warehouse empty - flag for aggregated alert
                            print(f"  ALERT: Warehouse empty for {ing_id}! "
                                  f"Branch {branch_id} needs {request_qty:.0f}, "
                                  f"warehouse has {warehouse[ing_id]['current_stock']:.0f}")

                            if branch_id not in branch_shortages:
                                branch_shortages[branch_id] = {}
                            if ing_id not in branch_shortages[branch_id]:
                                branch_shortages[branch_id][ing_id] = {"reasons": []}
                            branch_shortages[branch_id][ing_id]["reasons"].append({
                                "type": "WAREHOUSE_EMPTY",
                                "requested_qty": request_qty,
                                "warehouse_stock": warehouse[ing_id]["current_stock"],
                            })

            # 4. Flush aggregated BRANCH_RESTOCK_NEEDED alerts for this date
            for branch_id, ingredients in branch_shortages.items():
                # Determine severity: critical if any WAREHOUSE_EMPTY, else warning
                has_critical = any(
                    any(r["type"] == "WAREHOUSE_EMPTY" for r in ing_data["reasons"])
                    for ing_data in ingredients.values()
                )
                severity = "critical" if has_critical else "warning"

                items = []
                for ing_id, ing_data in ingredients.items():
                    # needed_qty is always 2x min_reorder (standard shipment amount)
                    restock_qty = branch_defaults[ing_id]["min_reorder"] * 2
                    items.append({
                        "ingredient_id": ing_id,
                        "ingredient_name": warehouse[ing_id]["name"],
                        "unit": warehouse[ing_id]["unit"],
                        "needed_qty": restock_qty,
                        "warehouse_stock": warehouse[ing_id]["current_stock"],
                        "reasons": ing_data["reasons"],
                    })

                alert = {
                    "timestamp": date_str,
                    "type": "BRANCH_RESTOCK_NEEDED",
                    "branch_id": branch_id,
                    "date": date_str,
                    "severity": severity,
                    "items": items,
                }
                alerts.append(alert)
                print(f"  AGGREGATED ALERT: Branch {branch_id} -> {len(items)} item(s)")

        # Confirm any remaining pending shipments (simulate next day)
        final_date = pd.Timestamp(dates[-1]) + timedelta(days=1)
        final_date_str = final_date.strftime("%Y-%m-%d")
        for pending in pending_shipments:
            theft_rate = random.uniform(THEFT_MIN, THEFT_MAX)
            confirmed_qty = pending["sent_qty"] * (1 - theft_rate)

            branch_id = pending["branch_id"]
            ing_id = pending["ingredient_id"]
            branch_inventory[branch_id][ing_id]["current_stock"] += confirmed_qty

            pending["received_qty"] = pending["sent_qty"]
            pending["confirmed_qty"] = round(confirmed_qty, 2)
            pending["shipment_received_at"] = final_date_str
            pending["status"] = "confirmed"

    # Save warehouse inventory
    now = datetime.now().isoformat()
    warehouse_rows = []
    for ing_id, info in warehouse.items():
        warehouse_rows.append({
            "ingredient_id": ing_id,
            "ingredient_name": info["name"],
            "unit": info["unit"],
            "initial_stock": info["initial_stock"],
            "current_stock": info["current_stock"],
            "min_reorder": info["min_reorder"],
            "last_updated": now,
        })

    warehouse_df = pd.DataFrame(warehouse_rows)
    warehouse_path = GOLD_DIR / "dim_warehouse.parquet"
    warehouse_df.to_parquet(warehouse_path, index=False)
    print(f"\nWritten: {warehouse_path} ({len(warehouse_df)} rows)")

    # Print warehouse status
    print("\nWarehouse status:")
    for _, row in warehouse_df.iterrows():
        pct = row["current_stock"] / row["initial_stock"] * 100
        status = "LOW" if row["current_stock"] < row["min_reorder"] else "OK"
        print(f"  {row['ingredient_name']}: {row['current_stock']:.0f} {row['unit']} "
              f"({pct:.0f}% remaining) [{status}]")

    # Save branch inventory
    branch_rows = []
    for branch_id in branch_ids:
        for ing_id in ingredient_ids:
            info = branch_inventory[branch_id][ing_id]
            branch_rows.append({
                "branch_id": branch_id,
                "ingredient_id": ing_id,
                "initial_stock": info["initial_stock"],
                "current_stock": round(info["current_stock"], 2),
                "min_reorder": info["min_reorder"],
                "last_updated": now,
            })

    branch_inv_df = pd.DataFrame(branch_rows)
    branch_inv_df["branch_id"] = branch_inv_df["branch_id"].astype("int32")
    branch_inv_path = GOLD_DIR / "branch_inventory.parquet"
    branch_inv_df.to_parquet(branch_inv_path, index=False)
    print(f"Written: {branch_inv_path} ({len(branch_inv_df)} rows)")

    # Save shipments
    if shipments:
        shipment_df = pd.DataFrame(shipments)
        shipment_df["branch_id"] = shipment_df["branch_id"].astype("int32")
        shipment_path = GOLD_DIR / "shipment.parquet"
        shipment_df.to_parquet(shipment_path, index=False)
        print(f"Written: {shipment_path} ({len(shipment_df)} rows)")

        # Shipment stats
        total_sent = shipment_df["sent_qty"].sum()
        total_confirmed = shipment_df["confirmed_qty"].sum()
        total_stolen = total_sent - total_confirmed
        print(f"\nShipment summary: sent={total_sent:.0f}, received={total_confirmed:.0f}, "
              f"stolen={total_stolen:.0f} ({total_stolen/total_sent*100:.1f}%)")
    else:
        print("No shipments created")

    # Save alerts
    if alerts:
        ALERTS_DIR.mkdir(parents=True, exist_ok=True)
        alert_file = ALERTS_DIR / f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(alert_file, "w") as f:
            json.dump({"alerts": alerts, "count": len(alerts)}, f, indent=2)
        print(f"Written: {alert_file} ({len(alerts)} alerts)")

        # Print alert summary
        print("\nAlert summary:")
        for alert in alerts:
            print(f"  BRANCH_RESTOCK_NEEDED: Branch {alert['branch_id']} on {alert['date']} "
                  f"[{alert['severity']}] - {len(alert['items'])} ingredient(s)")

    return {
        "warehouse": str(warehouse_path),
        "branch_inventory": str(branch_inv_path),
        "shipments": len(shipments),
        "alerts": len(alerts),
    }


def check_warehouse_alerts():
    """Check for low warehouse stock and generate alerts (with 48h dedup)."""
    warehouse_path = GOLD_DIR / "dim_warehouse.parquet"

    if not warehouse_path.exists():
        print("dim_warehouse.parquet not found, skipping alerts")
        return []

    df = pd.read_parquet(warehouse_path)
    now = datetime.now()

    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    if ALERT_STATE_FILE.exists():
        with open(ALERT_STATE_FILE) as f:
            alert_state = json.load(f)
    else:
        alert_state = {}

    low_stock = df[df["current_stock"] < df["min_reorder"]]

    if low_stock.empty:
        print("No low warehouse stock alerts")
        return []

    alerts = []
    for _, row in low_stock.iterrows():
        ingredient_id = row["ingredient_id"]

        last_alerted = alert_state.get(f"warehouse_{ingredient_id}")
        if last_alerted:
            last_alerted_dt = datetime.fromisoformat(last_alerted)
            if now - last_alerted_dt < timedelta(hours=48):
                print(f"Skipping alert for {row['ingredient_name']} "
                      f"(alerted {(now - last_alerted_dt).total_seconds() / 3600:.1f}h ago)")
                continue

        alert = {
            "timestamp": now.isoformat(),
            "type": "LOW_WAREHOUSE_STOCK",
            "severity": "critical" if row["current_stock"] <= 0 else "warning",
            "ingredient_id": ingredient_id,
            "ingredient_name": row["ingredient_name"],
            "current_stock": float(row["current_stock"]),
            "min_reorder": float(row["min_reorder"]),
            "unit": row["unit"],
        }
        alerts.append(alert)
        alert_state[f"warehouse_{ingredient_id}"] = now.isoformat()

        print(f"ALERT: Warehouse {alert['ingredient_name']} at {alert['current_stock']:.0f} "
              f"{alert['unit']} (min: {alert['min_reorder']:.0f}) [{alert['severity']}]")

    with open(ALERT_STATE_FILE, "w") as f:
        json.dump(alert_state, f, indent=2)

    if alerts:
        alert_file = ALERTS_DIR / f"warehouse_alerts_{now.strftime('%Y%m%d_%H%M%S')}.json"
        with open(alert_file, "w") as f:
            json.dump({"alerts": alerts, "count": len(alerts)}, f, indent=2)
        print(f"Written: {alert_file} ({len(alerts)} alerts)")

    return alerts


def load_gold_to_postgres():
    """Load all gold parquet files into PostgreSQL for Grafana dashboards."""
    # Use same postgres as Airflow (from docker-compose)
    pg_conn = os.environ.get(
        "POSTGRES_CONN",
        "postgresql://airflow:airflow@postgres:5432/airflow"
    )
    engine = create_engine(pg_conn)

    # Gold parquet files to load
    gold_files = [
        "fact_sales",
        "dim_product",
        "dim_branch",
        "dim_warehouse",
        "branch_inventory",
        "shipment",
    ]

    loaded = []
    for table_name in gold_files:
        parquet_path = GOLD_DIR / f"{table_name}.parquet"
        if not parquet_path.exists():
            print(f"Skipping {table_name}: file not found")
            continue

        df = pd.read_parquet(parquet_path)

        # Handle date columns (convert to proper datetime)
        for col in df.columns:
            if "date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Load to postgres (replace if exists)
        df.to_sql(
            table_name,
            engine,
            schema="public",
            if_exists="replace",
            index=False,
        )
        print(f"Loaded {table_name}: {len(df)} rows")
        loaded.append(table_name)

    engine.dispose()
    print(f"\nLoaded {len(loaded)} tables to PostgreSQL")
    return loaded


with DAG(
    dag_id="latte_flow_etl",
    default_args=default_args,
    description="ETL pipeline for Barcelona coffee chain",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["latte-flow", "etl", "barcelona"],
) as dag:

    alive_task = PythonOperator(
        task_id="alive",
        python_callable=alive,
    )

    bronze_task = PythonOperator(
        task_id="bronze_to_parquet",
        python_callable=bronze_to_parquet,
    )

    silver_task = PythonOperator(
        task_id="silver_clean",
        python_callable=silver_clean,
    )

    gold_fact_task = PythonOperator(
        task_id="gold_fact_sales",
        python_callable=gold_fact_sales,
    )

    gold_dim_product_task = PythonOperator(
        task_id="gold_dim_product",
        python_callable=gold_dim_product,
    )

    gold_dim_branch_task = PythonOperator(
        task_id="gold_dim_branch",
        python_callable=gold_dim_branch,
    )

    inventory_task = PythonOperator(
        task_id="process_inventory_and_shipments",
        python_callable=process_inventory_and_shipments,
    )

    alert_task = PythonOperator(
        task_id="check_warehouse_alerts",
        python_callable=check_warehouse_alerts,
    )

    load_postgres_task = PythonOperator(
        task_id="load_gold_to_postgres",
        python_callable=load_gold_to_postgres,
    )

    # DAG structure
    alive_task >> bronze_task >> silver_task >> gold_fact_task
    gold_fact_task >> [gold_dim_product_task, gold_dim_branch_task, inventory_task]
    inventory_task >> alert_task
    inventory_task >> load_postgres_task
