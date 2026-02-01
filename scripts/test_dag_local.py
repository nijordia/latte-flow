#!/usr/bin/env python3
"""
Local test runner for latte-flow DAG functions.
Run: py scripts/test_dag_local.py

Bypasses Airflow and runs the ETL functions directly with local paths.
"""

import sys
from pathlib import Path

# Set up paths for local testing (override Docker paths)
PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR / "dags"))

# Monkey-patch the paths before importing etl_dag
import etl_dag
etl_dag.DATA_DIR = PROJECT_DIR / "data"
etl_dag.CONFIG_DIR = PROJECT_DIR / "configs"
etl_dag.BRONZE_DIR = etl_dag.DATA_DIR / "bronze"
etl_dag.SILVER_DIR = etl_dag.DATA_DIR / "silver"
etl_dag.GOLD_DIR = etl_dag.DATA_DIR / "gold"
etl_dag.ALERTS_DIR = etl_dag.DATA_DIR / "alerts"
etl_dag.ALERT_STATE_FILE = etl_dag.ALERTS_DIR / "alert_state.json"


def main():
    print("=" * 70)
    print("  LATTE-FLOW LOCAL DAG TEST")
    print("=" * 70)
    print(f"  Data dir: {etl_dag.DATA_DIR}")
    print(f"  Config dir: {etl_dag.CONFIG_DIR}")
    print()

    # Run DAG tasks in order
    print("1. alive")
    etl_dag.alive()
    print()

    print("2. bronze_to_parquet")
    etl_dag.bronze_to_parquet()
    print()

    print("3. silver_clean")
    etl_dag.silver_clean()
    print()

    print("4. gold_fact_sales")
    etl_dag.gold_fact_sales()
    print()

    print("5. gold_dim_product")
    etl_dag.gold_dim_product()
    print()

    print("6. gold_dim_branch")
    etl_dag.gold_dim_branch()
    print()

    print("7. process_inventory_and_shipments")
    result = etl_dag.process_inventory_and_shipments()
    print(f"\n  Result: {result}")
    print()

    print("8. check_warehouse_alerts")
    etl_dag.check_warehouse_alerts()
    print()

    print("=" * 70)
    print("  DAG RUN COMPLETE")
    print("=" * 70)

    # Show the latest alerts file
    alerts_files = sorted(etl_dag.ALERTS_DIR.glob("alerts_*.json"))
    if alerts_files:
        latest = alerts_files[-1]
        print(f"\nLatest alerts file: {latest}")
        import json
        with open(latest) as f:
            data = json.load(f)
        print(f"Total alerts: {data['count']}")
        for alert in data["alerts"]:
            if alert["type"] == "BRANCH_RESTOCK_NEEDED":
                print(f"  - Branch {alert['branch_id']} on {alert['date']}: "
                      f"{len(alert['items'])} item(s) [{alert['severity']}]")


if __name__ == "__main__":
    main()
