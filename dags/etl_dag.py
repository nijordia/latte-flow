print("DAG FILE LOADED - BEFORE ANY IMPORT")
import json
import os
import random
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
print("ALL IMPORTS DONE - BEFORE ANY DEF")


# Paths
DATA_DIR = Path("/opt/airflow/data")
CONFIG_DIR = Path("/opt/airflow/configs")
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
ALERTS_DIR = DATA_DIR / "alerts"
STATE_DIR = DATA_DIR / "state"
ARCHIVE_DIR = BRONZE_DIR / "archive"

# Alert state file for dedup
ALERT_STATE_FILE = ALERTS_DIR / "alert_state.json"
PROCESSED_FILES_STATE = STATE_DIR / "processed_files.json"

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


# =============================================================================
# State Management for Incremental Processing
# =============================================================================

def get_processed_files() -> set:
    """Get set of already-processed CSV filenames."""
    if PROCESSED_FILES_STATE.exists():
        with open(PROCESSED_FILES_STATE) as f:
            return set(json.load(f))
    return set()


def mark_files_processed(files: list):
    """Mark CSV files as processed."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    existing = get_processed_files()
    existing.update(files)
    with open(PROCESSED_FILES_STATE, "w") as f:
        json.dump(sorted(existing), f, indent=2)
    print(f"Marked {len(files)} file(s) as processed")


def archive_processed_files(files: list):
    """Move processed CSVs to archive folder organized by date."""
    if not files:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    archive_path = ARCHIVE_DIR / today
    archive_path.mkdir(parents=True, exist_ok=True)

    archived = 0
    for filename in files:
        src = BRONZE_DIR / filename
        if src.exists():
            dst = archive_path / filename
            shutil.move(str(src), str(dst))
            print(f"  Archived: {filename} -> archive/{today}/")
            archived += 1

    print(f"Archived {archived} file(s) to {archive_path}")


def get_postgres_engine():
    """Get PostgreSQL engine for persistent state."""
    pg_conn = os.environ.get(
        "POSTGRES_CONN",
        "postgresql://airflow:airflow@postgres:5432/airflow"
    )
    return create_engine(pg_conn)


def init_stock_tables(engine):
    """Create persistent stock tables if they don't exist."""
    # Use engine.begin() for auto-commit (SQLAlchemy 2.0 compatible)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS current_branch_stock (
                branch_id INTEGER,
                ingredient_id TEXT,
                current_stock NUMERIC,
                last_updated TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (branch_id, ingredient_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS current_warehouse_stock (
                ingredient_id TEXT PRIMARY KEY,
                current_stock NUMERIC,
                last_updated TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS processed_dates (
                date DATE PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """))
    print("Stock tables initialized")


def load_branch_stock_from_db(engine, branch_ids, ingredient_ids, branch_defaults):
    """Load current branch stock from PostgreSQL, initializing if empty."""
    init_stock_tables(engine)

    # Check if table has data
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM current_branch_stock"))
        count = result.scalar()

    if count == 0:
        # Initialize from defaults
        print("Initializing branch stock from defaults...")
        stock = {}
        for branch_id in branch_ids:
            stock[branch_id] = {}
            for ing_id in ingredient_ids:
                stock[branch_id][ing_id] = {
                    "current_stock": branch_defaults[ing_id]["initial_stock"],
                    "min_reorder": branch_defaults[ing_id]["min_reorder"],
                }
        return stock

    # Load from table
    df = pd.read_sql("SELECT * FROM current_branch_stock", engine)
    stock = {}
    for _, row in df.iterrows():
        branch_id = int(row["branch_id"])
        if branch_id not in stock:
            stock[branch_id] = {}
        stock[branch_id][row["ingredient_id"]] = {
            "current_stock": float(row["current_stock"]),
            "min_reorder": branch_defaults[row["ingredient_id"]]["min_reorder"],
        }

    print(f"Loaded branch stock from DB: {len(df)} entries")
    return stock


def save_branch_stock_to_db(engine, branch_inventory):
    """Save current branch stock to PostgreSQL."""
    rows = []
    now = datetime.now()
    for branch_id, ingredients in branch_inventory.items():
        for ing_id, data in ingredients.items():
            rows.append({
                "branch_id": branch_id,
                "ingredient_id": ing_id,
                "current_stock": round(data["current_stock"], 2),
                "last_updated": now,
            })

    df = pd.DataFrame(rows)
    df.to_sql("current_branch_stock", engine, if_exists="replace", index=False)
    print(f"Saved branch stock to DB: {len(rows)} entries")


def load_warehouse_stock_from_db(engine, warehouse_cfg):
    """Load current warehouse stock from PostgreSQL, initializing if empty."""
    init_stock_tables(engine)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM current_warehouse_stock"))
        count = result.scalar()

    if count == 0:
        # Initialize from config
        print("Initializing warehouse stock from config...")
        return {
            ing_id: info["initial_stock"]
            for ing_id, info in warehouse_cfg.items()
        }

    # Load from table
    df = pd.read_sql("SELECT * FROM current_warehouse_stock", engine)
    stock = {row["ingredient_id"]: float(row["current_stock"]) for _, row in df.iterrows()}
    print(f"Loaded warehouse stock from DB: {len(df)} entries")
    return stock


def save_warehouse_stock_to_db(engine, warehouse):
    """Save current warehouse stock to PostgreSQL."""
    rows = []
    now = datetime.now()
    for ing_id, info in warehouse.items():
        rows.append({
            "ingredient_id": ing_id,
            "current_stock": round(info["current_stock"], 2),
            "last_updated": now,
        })

    df = pd.DataFrame(rows)
    df.to_sql("current_warehouse_stock", engine, if_exists="replace", index=False)
    print(f"Saved warehouse stock to DB: {len(rows)} entries")


def get_processed_dates(engine) -> set:
    """Get set of already-processed dates from DB."""
    init_stock_tables(engine)
    try:
        df = pd.read_sql("SELECT date FROM processed_dates", engine)
        return set(pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d"))
    except Exception:
        return set()


def mark_dates_processed(engine, dates: list):
    """Mark dates as processed in DB."""
    if not dates:
        return
    now = datetime.now()

    # Append new dates (avoid duplicates)
    existing = get_processed_dates(engine)
    new_dates = [{"date": d, "processed_at": now} for d in dates if d not in existing]
    if new_dates:
        pd.DataFrame(new_dates).to_sql("processed_dates", engine, if_exists="append", index=False)
        print(f"Marked {len(new_dates)} date(s) as processed")


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
    """
    Load raw CSVs from bronze and convert to parquet.
    INCREMENTAL: Only processes new CSV files not yet seen.
    """
    # Get already-processed files
    already_processed = get_processed_files()

    # Find all CSV files in bronze (excluding archive subdirectory)
    csv_files = sorted([
        f for f in BRONZE_DIR.glob("sales_*.csv")
        if f.name not in already_processed
    ])

    if not csv_files:
        print("No new CSV files to process in bronze/")
        return []

    print(f"Found {len(csv_files)} new CSV file(s) to process")

    # Read and combine all new CSVs
    dfs = []
    new_file_names = []
    for csv_path in csv_files:
        print(f"  Reading: {csv_path.name}")
        df = pd.read_csv(csv_path)
        dfs.append(df)
        new_file_names.append(csv_path.name)

    new_df = pd.concat(dfs, ignore_index=True)
    print(f"  Combined {len(new_df)} rows from {len(csv_files)} file(s)")

    # Append to existing sales.parquet or create new
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    output_path = SILVER_DIR / "sales.parquet"

    if output_path.exists():
        existing_df = pd.read_parquet(output_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        print(f"  Appended to existing: {len(existing_df)} + {len(new_df)} = {len(combined_df)} rows")
    else:
        combined_df = new_df
        print(f"  Created new parquet: {len(combined_df)} rows")

    combined_df.to_parquet(output_path, index=False)
    print(f"Written: {output_path}")

    # Mark files as processed
    mark_files_processed(new_file_names)

    return new_file_names


def silver_clean():
    """Clean, type cast, compute metrics, and enrich silver data."""
    products_cfg = load_config("products")["products"]

    sales_path = SILVER_DIR / "sales.parquet"
    if not sales_path.exists():
        print("No sales.parquet found in silver/")
        return None

    print(f"Cleaning: {sales_path.name}")
    df = pd.read_parquet(sales_path)

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

    cleaned_path = SILVER_DIR / "sales_clean.parquet"
    df.to_parquet(cleaned_path, index=False)
    print(f"Written: {cleaned_path} ({len(df)} rows)")

    return str(cleaned_path)


def gold_fact_sales():
    """Build fact_sales table with cumulative stock tracking."""
    cleaned_path = SILVER_DIR / "sales_clean.parquet"

    if not cleaned_path.exists():
        print("No sales_clean.parquet found in silver/")
        return None

    df = pd.read_parquet(cleaned_path)

    df = df.sort_values(["branch_id", "product_id", "date"]).reset_index(drop=True)
    df["cumulative_qty"] = df.groupby(["branch_id", "product_id"])["qty"].cumsum()
    df["stock_left"] = INITIAL_PRODUCT_STOCK - df["cumulative_qty"]

    oversold = df[df["stock_left"] < 0]
    if not oversold.empty:
        for _, row in oversold.drop_duplicates(["branch_id", "product_id"]).iterrows():
            print(f"WARNING: Branch {row['branch_id']} sold more than {INITIAL_PRODUCT_STOCK} "
                  f"of product {row['product_id']} ({row['product_name']})")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
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
    print("INSIDE FUNCTION - RIGHT AFTER DEF LINE")
    """
    Process inventory with warehouse/branch model and shipments.
    INCREMENTAL: Uses persistent stock from PostgreSQL and only processes new dates.

    Flow:
    1. Load persistent stock from PostgreSQL (or initialize from config)
    2. Get already-processed dates, skip them
    3. Process each NEW day chronologically:
       a. Confirm pending shipments from previous day (with theft)
       b. Deduct today's sales from branch inventory
       c. Check branch stock levels
       d. If low → check warehouse → create shipment (2x min_reorder)
       e. If warehouse empty → accumulate shortage
    4. At end of each date, flush aggregated BRANCH_RESTOCK_NEEDED alerts
    5. Save updated stock back to PostgreSQL
    6. Save all outputs (append to existing shipments)
    """
    
    import traceback
    import sys

    print("=" * 60)
    print("PROCESS_INVENTORY_AND_SHIPMENTS: STARTED")
    print("=" * 60)
    print(f"Python version: {sys.version}")
    print(f"Current working directory: {Path.cwd()}")
    print(f"BRONZE_DIR: {BRONZE_DIR}")
    print(f"BRONZE_DIR exists: {BRONZE_DIR.exists()}")
    print(f"GOLD_DIR: {GOLD_DIR}")
    print(f"GOLD_DIR exists: {GOLD_DIR.exists()}")

    # List CSVs in bronze
    try:
        bronze_csvs = list(BRONZE_DIR.glob("*.csv"))
        print(f"CSVs in BRONZE_DIR: {[f.name for f in bronze_csvs]}")
    except Exception as e:
        print(f"ERROR listing BRONZE_DIR: {e}")

    # Check fact_sales.parquet
    fact_sales_path = GOLD_DIR / "fact_sales.parquet"
    print(f"fact_sales.parquet exists: {fact_sales_path.exists()}")
    if fact_sales_path.exists():
        try:
            test_df = pd.read_parquet(fact_sales_path)
            print(f"fact_sales.parquet rows: {len(test_df)}")
            if len(test_df) > 0:
                print(f"fact_sales.parquet columns: {list(test_df.columns)}")
                print(f"fact_sales date range: {test_df['date'].min()} to {test_df['date'].max()}")
        except Exception as e:
            print(f"ERROR reading fact_sales.parquet: {e}")

    try:
        # Load configs with defensive checks
        print("\nLoading configs...")
        recipes_cfg = load_config("recipes")["recipes"]
        print(f"  recipes_cfg: {len(recipes_cfg)} recipes loaded, keys: {list(recipes_cfg.keys())}")
        inventory_cfg = load_config("inventory")
        print(f"  inventory_cfg keys: {list(inventory_cfg.keys())}")
        branches_cfg = load_config("branches")["branches"]
        print(f"  branches_cfg: {len(branches_cfg)} branches loaded, keys: {list(branches_cfg.keys())}")

        warehouse_cfg = inventory_cfg["warehouse"]
        print(f"  warehouse_cfg: {len(warehouse_cfg)} ingredients")
        branch_defaults = inventory_cfg["branch_defaults"]
        print(f"  branch_defaults: {len(branch_defaults)} defaults")

        branch_ids = [int(bid) for bid in branches_cfg.keys()]
        print(f"  branch_ids: {branch_ids}")
        ingredient_ids = list(warehouse_cfg.keys())
        print(f"  ingredient_ids: {ingredient_ids}")

        # Get PostgreSQL engine for persistent state
        print("\nConnecting to PostgreSQL...")
        engine = get_postgres_engine()
        print("  PostgreSQL engine created")

        # Load persistent branch stock from DB (or initialize)
        branch_stock_db = load_branch_stock_from_db(engine, branch_ids, ingredient_ids, branch_defaults)

        # Load persistent warehouse stock from DB (or initialize)
        warehouse_stock_db = load_warehouse_stock_from_db(engine, warehouse_cfg)

        # Get already-processed dates
        processed_dates = get_processed_dates(engine)
        print(f"Already processed {len(processed_dates)} date(s)")

        # Build warehouse dict with current stock from DB
        warehouse = {
            ing_id: {
                "name": info["name"],
                "unit": info["unit"],
                "initial_stock": info["initial_stock"],
                "current_stock": warehouse_stock_db.get(ing_id, info["initial_stock"]),
                "min_reorder": info["min_reorder"],
            }
            for ing_id, info in warehouse_cfg.items()
        }

        # Build branch inventory dict with current stock from DB
        branch_inventory = {}
        for branch_id in branch_ids:
            branch_inventory[branch_id] = {}
            for ing_id in ingredient_ids:
                if branch_id in branch_stock_db and ing_id in branch_stock_db[branch_id]:
                    current = branch_stock_db[branch_id][ing_id]["current_stock"]
                else:
                    current = branch_defaults[ing_id]["initial_stock"]

                branch_inventory[branch_id][ing_id] = {
                    "initial_stock": branch_defaults[ing_id]["initial_stock"],
                    "current_stock": current,
                    "min_reorder": branch_defaults[ing_id]["min_reorder"],
                }

        # Load fact_sales to get daily consumption
        fact_sales_path = GOLD_DIR / "fact_sales.parquet"
        if not fact_sales_path.exists():
            print("fact_sales.parquet not found, using initial stock only")
            sales_df = pd.DataFrame()
        else:
            sales_df = pd.read_parquet(fact_sales_path)
            print(f"Loaded fact_sales.parquet: {len(sales_df)} rows")

        # Compute ingredient consumption per sale
        def get_ingredient_usage(product_id, qty):
            recipe = recipes_cfg.get(product_id, {})
            return {ing: amount * qty for ing, amount in recipe.items()}

        # Load existing shipments to get next shipment_id and append
        shipment_path = GOLD_DIR / "shipment.parquet"
        if shipment_path.exists():
            existing_shipments_df = pd.read_parquet(shipment_path)
            shipment_id = int(existing_shipments_df["shipment_id"].max())
            existing_shipments = existing_shipments_df.to_dict("records")
        else:
            shipment_id = 0
            existing_shipments = []

        new_shipments = []
        alerts = []
        new_dates_processed = []

        if not sales_df.empty:
            # Get unique dates sorted, filter out already-processed
            all_dates = sorted(sales_df["date"].unique())
            dates_to_process = [
                d for d in all_dates
                if pd.Timestamp(d).strftime("%Y-%m-%d") not in processed_dates
            ]

            if not dates_to_process:
                print("No new dates to process")
            else:
                print(f"Processing {len(dates_to_process)} new date(s)...")

                # Track pending shipments (sent yesterday, arrives today)
                pending_shipments = []

                for current_date in dates_to_process:
                    date_str = pd.Timestamp(current_date).strftime("%Y-%m-%d")
                    print(f"\n=== Processing {date_str} ===")
                    new_dates_processed.append(date_str)

                    # Track which branch/ingredients need restocking
                    branch_shortages = {}

                    # 1. Confirm pending shipments from previous day (with theft)
                    for pending in pending_shipments:
                        theft_rate = random.uniform(THEFT_MIN, THEFT_MAX)
                        confirmed_qty = pending["sent_qty"] * (1 - theft_rate)

                        branch_id = pending["branch_id"]
                        ing_id = pending["ingredient_id"]
                        branch_inventory[branch_id][ing_id]["current_stock"] += confirmed_qty

                        pending["received_qty"] = pending["sent_qty"]
                        pending["confirmed_qty"] = round(confirmed_qty, 2)
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
                                    branch_inventory[branch_id][ing_id]["current_stock"] = 0
                                    print(f"  OVERSOLD WARNING: Branch {branch_id} sold {qty} of product "
                                          f"{product_id} but only had {old_stock:.1f} {warehouse[ing_id]['unit']} "
                                          f"of {ing_id} left. Stock capped at 0.")

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
                                request_qty = min_reorder * 2

                                if warehouse[ing_id]["current_stock"] >= request_qty:
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
                                    new_shipments.append(shipment)
                                    pending_shipments.append(shipment)

                                    print(f"  SHIPMENT SENT: {ing_id} to branch {branch_id}: "
                                          f"qty={request_qty:.0f} (branch had {current:.0f}, min={min_reorder:.0f})")
                                else:
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
                        has_critical = any(
                            any(r["type"] == "WAREHOUSE_EMPTY" for r in ing_data["reasons"])
                            for ing_data in ingredients.values()
                        )
                        severity = "critical" if has_critical else "warning"

                        items = []
                        for ing_id, ing_data in ingredients.items():
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
                if dates_to_process:
                    final_date = pd.Timestamp(dates_to_process[-1]) + timedelta(days=1)
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

        # Save persistent stock back to PostgreSQL
        save_branch_stock_to_db(engine, branch_inventory)
        save_warehouse_stock_to_db(engine, warehouse)

        # Mark dates as processed
        if new_dates_processed:
            mark_dates_processed(engine, new_dates_processed)

        # Save warehouse inventory to parquet
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
            pct = row["current_stock"] / row["initial_stock"] * 100 if row["initial_stock"] > 0 else 0
            status = "LOW" if row["current_stock"] < row["min_reorder"] else "OK"
            print(f"  {row['ingredient_name']}: {row['current_stock']:.0f} {row['unit']} "
                  f"({pct:.0f}% remaining) [{status}]")

        # Save branch inventory to parquet
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

        # Combine existing + new shipments and save
        all_shipments = existing_shipments + new_shipments
        if all_shipments:
            shipment_df = pd.DataFrame(all_shipments)
            shipment_df["branch_id"] = shipment_df["branch_id"].astype("int32")
            shipment_df.to_parquet(shipment_path, index=False)
            print(f"Written: {shipment_path} ({len(shipment_df)} total, {len(new_shipments)} new)")

            if new_shipments:
                new_df = pd.DataFrame(new_shipments)
                total_sent = new_df["sent_qty"].sum()
                confirmed = new_df[new_df["confirmed_qty"].notna()]
                if not confirmed.empty:
                    total_confirmed = confirmed["confirmed_qty"].sum()
                    total_stolen = total_sent - total_confirmed
                    print(f"\nNew shipment summary: sent={total_sent:.0f}, received={total_confirmed:.0f}, "
                          f"stolen={total_stolen:.0f}")
        else:
            print("No shipments")

        # Save alerts
        if alerts:
            ALERTS_DIR.mkdir(parents=True, exist_ok=True)
            alert_file = ALERTS_DIR / f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(alert_file, "w") as f:
                json.dump({"alerts": alerts, "count": len(alerts)}, f, indent=2)
            print(f"Written: {alert_file} ({len(alerts)} alerts)")

            print("\nAlert summary:")
            for alert in alerts:
                print(f"  BRANCH_RESTOCK_NEEDED: Branch {alert['branch_id']} on {alert['date']} "
                      f"[{alert['severity']}] - {len(alert['items'])} ingredient(s)")

        engine.dispose()

        print("\n" + "=" * 60)
        print("PROCESS_INVENTORY_AND_SHIPMENTS: COMPLETED SUCCESSFULLY")
        print("=" * 60)

        return {
            "warehouse": str(warehouse_path),
            "branch_inventory": str(branch_inv_path),
            "new_shipments": len(new_shipments),
            "total_shipments": len(all_shipments),
            "alerts": len(alerts),
            "dates_processed": len(new_dates_processed),
        }

    except Exception as e:
        print("\n" + "=" * 60)
        print("PROCESS_INVENTORY_AND_SHIPMENTS: EXCEPTION CAUGHT")
        print("=" * 60)
        print(f"Exception type: {type(e).__name__}")
        print(f"Exception message: {e}")
        print("\nFull traceback:")
        traceback.print_exc()
        print("=" * 60)
        raise


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


def archive_bronze_files():
    """Archive processed CSV files after successful DAG run."""
    processed = get_processed_files()

    if not processed:
        print("No processed files to archive")
        return []

    # Find files that exist and are processed
    files_to_archive = [
        f for f in processed
        if (BRONZE_DIR / f).exists()
    ]

    if not files_to_archive:
        print("All processed files already archived")
        return []

    archive_processed_files(files_to_archive)
    return files_to_archive


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

    archive_task = PythonOperator(
        task_id="archive_bronze_files",
        python_callable=archive_bronze_files,
    )

    # DAG structure
    # Bronze → Silver → Gold → Inventory/Dims → Alerts/Postgres → Archive
    alive_task >> bronze_task >> silver_task >> gold_fact_task
    gold_fact_task >> [gold_dim_product_task, gold_dim_branch_task, inventory_task]
    inventory_task >> alert_task
    inventory_task >> load_postgres_task
    [alert_task, load_postgres_task] >> archive_task
