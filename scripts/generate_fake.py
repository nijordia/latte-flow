#!/usr/bin/env python3
"""
Generate fake POS data for latte-flow testing (Barcelona edition).

Normal mode: realistic 30-day sales across 5 branches, one CSV per day.
Chaos mode: extreme oversold on specific days (hundreds of lattes), still one CSV per day.

1. Generate the initial 30-day baseline (one-time)
    python scripts/generate_fake.py --rows 800 --days 30

2. Simulate "one new day" (daily/ongoing mode) 
    python scripts/generate_fake.py --days 1 --start-date "2026-01-31" --rows 30
"""

import argparse
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
CONFIG_DIR = PROJECT_DIR / "configs"
BRONZE_DIR = PROJECT_DIR / "data" / "bronze"


def load_config(name: str) -> dict:
    """Load a YAML config file."""
    path = CONFIG_DIR / f"{name}.yaml"
    with open(path) as f:
        return yaml.safe_load(f)


def generate_fake_data(
    num_rows: int = 500,
    num_days: int = 30,
    start_date: datetime = None,
    chaos_mode: bool = False,
):
    """Generate fake POS sales, grouped by day for daily CSVs."""
    if start_date is None:
        start_date = datetime(2026, 1, 1)

    products_cfg = load_config("products")["products"]

    # Product weights (coffee dominant)
    product_weights = {
        101: 25,   # Espresso
        102: 30,   # Latte
        103: 20,   # Cappuccino
        104: 12,   # Croissant
        105: 8,    # Muffin
        106: 5,    # Orange Juice
    }
    product_ids = list(product_weights.keys())
    weights = list(product_weights.values())

    branch_ids = [1, 2, 3, 4, 5]

    # Group rows by date
    daily_rows = defaultdict(list)

    # Normal sales loop
    for _ in range(num_rows):
        day_offset = random.randint(0, num_days - 1)
        date = start_date + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")
        branch_id = random.choice(branch_ids)

        product_id = random.choices(product_ids, weights=weights)[0]
        base_price = products_cfg[product_id]["base_price_eur"]
        unit_price = base_price

        # Discounts
        if date.weekday() == 1 and product_id == 104:  # Tuesday croissant
            unit_price = base_price * 0.70
        elif random.random() < 0.1:
            discount = random.uniform(0.05, 0.15)
            unit_price = base_price * (1 - discount)

        # Quantity
        if chaos_mode:
            qty = random.randint(50, 100)  # extreme
        else:
            qty = random.choices([1, 2, 3, 5, 7, 10], weights=[50, 25, 15, 5, 3, 2])[0]

        total_paid = round(unit_price * qty, 2)

        row = {
            "date": date_str,
            "branch_id": branch_id,
            "product_id": product_id,
            "qty": qty,
            "total_paid": total_paid,
        }
        daily_rows[date_str].append(row)

    # Inject oversold chaos days (only in chaos mode)
    if chaos_mode:
        oversell_days = [3, 10, 17, 24]  # Jan 4, 11, 18, 25
        for day_offset in oversell_days:
            date = start_date + timedelta(days=day_offset)
            date_str = date.strftime("%Y-%m-%d")
            for _ in range(80):  # ~1600 extra lattes per day
                daily_rows[date_str].append({
                    "date": date_str,
                    "branch_id": random.choice(branch_ids),
                    "product_id": 102,  # latte = milk killer
                    "qty": 20,
                    "total_paid": round(products_cfg[102]["base_price_eur"] * 20 * 0.95, 2),
                })

    return daily_rows


def write_csv(rows: list, output_path: Path):
    """Write rows to CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write("date,branch_id,product_id,qty,total_paid\n")
        for row in rows:
            f.write(f"{row['date']},{row['branch_id']},{row['product_id']},{row['qty']},{row['total_paid']}\n")
    print(f"Written: {output_path} ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Generate fake POS data for latte-flow.")
    parser.add_argument("--chaos", action="store_true", help="Generate extreme oversold scenario")
    parser.add_argument("--rows", type=int, default=500, help="Base number of sales rows (normal mode)")
    parser.add_argument("--days", type=int, default=30, help="Number of days span (normal mode)")
    args = parser.parse_args()

    branches_cfg = load_config("branches")["branches"]
    products_cfg = load_config("products")["products"]

    print("=" * 70)
    print("  LATTE-FLOW FAKE DATA GENERATOR - Barcelona Edition")
    print("=" * 70)
    print()
    print("Mode:", "CHAOS MODE (oversold test)" if args.chaos else "Normal realistic mode")
    print(f"  - Base rows: {args.rows}")
    if not args.chaos:
        print(f"  - {args.days} days (Jan 2026)")
    print(f"  - 5 Barcelona branches:")
    for bid, info in branches_cfg.items():
        print(f"      {bid}: {info['name']}")
    if not args.chaos:
        print("  - Tuesday croissant 30% off")
    print()

    daily_rows = generate_fake_data(
        num_rows=args.rows,
        num_days=args.days,
        chaos_mode=args.chaos,
    )

    # Write one CSV per day
    total_rows = 0
    for date_str, rows in sorted(daily_rows.items()):
        output_path = BRONZE_DIR / f"sales_{date_str}.csv"
        write_csv(rows, output_path)
        total_rows += len(rows)

    # Stats summary
    print("\n" + "=" * 70)
    print("  STATS SUMMARY")
    print("=" * 70)
    print(f"Total rows generated: {total_rows} across {len(daily_rows)} days")

    all_rows = [row for rows in daily_rows.values() for row in rows]

    print("\nBy product:")
    product_counts = Counter(r["product_id"] for r in all_rows)
    total_qty = sum(r["qty"] for r in all_rows)
    total_revenue = sum(r["total_paid"] for r in all_rows)
    for pid, count in sorted(product_counts.items()):
        name = products_cfg[pid]["name"]
        qty = sum(r["qty"] for r in all_rows if r["product_id"] == pid)
        rev = sum(r["total_paid"] for r in all_rows if r["product_id"] == pid)
        print(f"  {name}: {count} sales, {qty} units, €{rev:.2f}")

    print("\nBy branch:")
    branch_counts = Counter(r["branch_id"] for r in all_rows)
    for bid, count in sorted(branch_counts.items()):
        name = branches_cfg[bid]["name"]
        rev = sum(r["total_paid"] for r in all_rows if r["branch_id"] == bid)
        print(f"  {name}: {count} sales, €{rev:.2f}")

    print(f"\nTotals: {len(all_rows)} transactions, {total_qty} units, €{total_revenue:.2f}")

    if not args.chaos:
        tuesday_croissants = [r for r in all_rows if r["product_id"] == 104 and datetime.strptime(r["date"], "%Y-%m-%d").weekday() == 1]
        discounted = sum(1 for r in tuesday_croissants if r["total_paid"] / r["qty"] < 2.80 * 0.75)
        print(f"\nTuesday croissant discounts: {discounted}/{len(tuesday_croissants)} croissant sales")

    print("\n" + "=" * 70)
    print("  Done! Run the DAG to process.")
    print("=" * 70)


if __name__ == "__main__":
    main()