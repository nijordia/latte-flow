import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = Path("/opt/airflow/data")
ALERTS_DIR = DATA_DIR / "alerts"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Stock safety threshold
SAFETY_STOCK_THRESHOLD = 5

# Product category mapping
PRODUCT_CATEGORIES = {
    101: {"name": "Espresso", "category": "coffee"},
    102: {"name": "Latte", "category": "coffee"},
    103: {"name": "Cappuccino", "category": "coffee"},
    104: {"name": "Croissant", "category": "pastry"},
    105: {"name": "Muffin", "category": "pastry"},
    106: {"name": "Orange Juice", "category": "beverage"},
}

default_args = {
    "owner": "latte-flow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def alive():
    print("latte-flow is alive")
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
    """Clean, type cast, and enrich silver data with category info."""
    # Only process raw parquets (exclude already cleaned ones)
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

        # Type casting
        df["date"] = pd.to_datetime(df["date"])
        df["branch_id"] = df["branch_id"].astype("int32")
        df["product_id"] = df["product_id"].astype("int32")
        df["sales_qty"] = df["sales_qty"].astype("int32")
        df["stock_left"] = df["stock_left"].astype("int32")
        df["price_eur"] = df["price_eur"].astype("float32")

        # Enrich with product info
        df["product_name"] = df["product_id"].map(
            lambda x: PRODUCT_CATEGORIES.get(x, {}).get("name", "Unknown")
        )
        df["category"] = df["product_id"].map(
            lambda x: PRODUCT_CATEGORIES.get(x, {}).get("category", "unknown")
        )

        # Calculate revenue
        df["revenue_eur"] = df["sales_qty"] * df["price_eur"]

        # Write to cleaned_ prefix (preserves raw parquet)
        cleaned_path = SILVER_DIR / f"cleaned_{pq_path.name}"
        df.to_parquet(cleaned_path, index=False)
        print(f"Written: {cleaned_path}")
        processed.append(str(cleaned_path))

    return processed


def gold_fact_sales():
    """Build fact_sales table from cleaned silver data."""
    cleaned_files = list(SILVER_DIR.glob("cleaned_*.parquet"))

    if not cleaned_files:
        print("No cleaned parquet files found in silver/")
        return None

    # Combine all cleaned files
    dfs = [pd.read_parquet(f) for f in cleaned_files]
    df = pd.concat(dfs, ignore_index=True)

    # fact_sales: transactional grain
    fact_sales = df[[
        "date",
        "branch_id",
        "product_id",
        "sales_qty",
        "stock_left",
        "price_eur",
        "revenue_eur",
    ]].copy()

    # Add surrogate key
    fact_sales["sale_id"] = range(1, len(fact_sales) + 1)

    # Reorder columns
    fact_sales = fact_sales[[
        "sale_id",
        "date",
        "branch_id",
        "product_id",
        "sales_qty",
        "stock_left",
        "price_eur",
        "revenue_eur",
    ]]

    output_path = GOLD_DIR / "fact_sales.parquet"
    fact_sales.to_parquet(output_path, index=False)
    print(f"Written: {output_path} ({len(fact_sales)} rows)")

    return str(output_path)


def gold_dim_product():
    """Build dim_product table from product categories."""
    dim_product = pd.DataFrame([
        {
            "product_id": pid,
            "product_name": info["name"],
            "category": info["category"],
        }
        for pid, info in PRODUCT_CATEGORIES.items()
    ])

    dim_product["product_id"] = dim_product["product_id"].astype("int32")

    output_path = GOLD_DIR / "dim_product.parquet"
    dim_product.to_parquet(output_path, index=False)
    print(f"Written: {output_path} ({len(dim_product)} rows)")

    return str(output_path)


def check_stock_alerts():
    """Check for low stock and generate alerts."""
    fact_sales_path = GOLD_DIR / "fact_sales.parquet"

    if not fact_sales_path.exists():
        print("fact_sales.parquet not found, skipping alerts")
        return []

    df = pd.read_parquet(fact_sales_path)

    # Find low stock items
    low_stock = df[df["stock_left"] < SAFETY_STOCK_THRESHOLD].copy()

    if low_stock.empty:
        print("No low stock alerts")
        return []

    # Enrich with product names
    low_stock["product_name"] = low_stock["product_id"].map(
        lambda x: PRODUCT_CATEGORIES.get(x, {}).get("name", "Unknown")
    )

    # Build alerts
    alerts = []
    for _, row in low_stock.iterrows():
        alert = {
            "timestamp": datetime.now().isoformat(),
            "type": "LOW_STOCK",
            "severity": "critical" if row["stock_left"] <= 2 else "warning",
            "branch_id": int(row["branch_id"]),
            "product_id": int(row["product_id"]),
            "product_name": row["product_name"],
            "stock_left": int(row["stock_left"]),
            "threshold": SAFETY_STOCK_THRESHOLD,
            "date": row["date"].strftime("%Y-%m-%d"),
        }
        alerts.append(alert)
        print(f"ALERT: {alert['product_name']} at branch {alert['branch_id']} "
              f"has {alert['stock_left']} units left ({alert['severity']})")

    # Write alerts to JSON
    ALERTS_DIR.mkdir(parents=True, exist_ok=True)
    alert_file = ALERTS_DIR / f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(alert_file, "w") as f:
        json.dump({"alerts": alerts, "count": len(alerts)}, f, indent=2)

    print(f"Written: {alert_file} ({len(alerts)} alerts)")

    # TODO: SNS hook for cloud deployment
    # boto3.client('sns', endpoint_url='http://localstack:4566').publish(...)

    return alerts


with DAG(
    dag_id="latte_flow_etl",
    default_args=default_args,
    description="ETL pipeline for coffee chain POS data",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["latte-flow", "etl"],
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

    gold_dim_task = PythonOperator(
        task_id="gold_dim_product",
        python_callable=gold_dim_product,
    )

    alert_task = PythonOperator(
        task_id="check_stock_alerts",
        python_callable=check_stock_alerts,
    )

    alive_task >> bronze_task >> silver_task >> [gold_fact_task, gold_dim_task]
    gold_fact_task >> alert_task
