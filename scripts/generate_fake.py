#!/usr/bin/env python3
"""
Generate fake POS data for latte-flow testing (Barcelona edition).

Normal mode: realistic 30-day sales across 5 branches. Run: python scripts/generate_fake.py
Chaos mode: extreme oversold scenario (hundreds of lattes on one day) to test inventory cap & alerts. Run: python scripts/generate_fake.py --chaos --rows 550
"""

import argparse
import random
from collections import Counter
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
    """Generate fake POS sales data."""
    if start_date is None:
        start_date = datetime(2026, 1, 1)

    products_cfg = load_config("products")["products"]

    # Product weights (coffee dominant)
    product_weights = {
        101: 25,   # Espresso
        102: 30,   # Latte ← most popular
        103: 20,   # Cappuccino
        104: 12,   # Croissant
        105: 8,    # Muffin
        106: 5,    # Orange Juice
    }
    product_ids = list(product_weights.keys())
    weights = list(product_weights.values())

    branch_ids = [1, 2, 3, 4, 5]  # Barcelona branches

    rows = []

    if chaos_mode:
        # Chaos mode: massive latte oversell on one day (Jan 15) in Branch 2 (Eixample)
        chaos_date = start_date + timedelta(days=14)  # Jan 15, 2026
        date_str = chaos_date.strftime("%Y-%m-%d")
        branch_id = 2  # Eixample - let's break its milk fridge

        print(f"CHAOS MODE ACTIVATED: {num_rows} lattes on {date_str} at Branch {branch_id}")

        for _ in range(num_rows):
            product_id = 102  # Force latte
            qty = random.choices([1, 2, 3, 4, 5, 6, 8, 10], weights=[5, 10, 15, 20, 20, 15, 10, 5])[0]

            base_price = products_cfg[product_id]["base_price_eur"]
            unit_price = base_price  # no discount in chaos

            total_paid = round(unit_price * qty, 2)

            rows.append({
                "date": date_str,
                "branch_id": branch_id,
                "product_id": product_id,
                "qty": qty,
                "total_paid": total_paid,
            })

    else:
        # Normal realistic mode
        for _ in range(num_rows):
            day_offset = random.randint(0, num_days - 1)
            date = start_date + timedelta(days=day_offset)
            date_str = date.strftime("%Y-%m-%d")

            branch_id = random.choice(branch_ids)

            product_id = random.choices(product_ids, weights=weights)[0]
            product_info = products_cfg[product_id]
            base_price = product_info["base_price_eur"]

            qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]

            unit_price = base_price

            # Tuesday croissant 30% off
            if date.weekday() == 1 and product_id == 104:  # Tuesday
                unit_price = base_price * 0.70

            # Occasional small random discount (5-15%)
            elif random.random() < 0.1:
                discount = random.uniform(0.05, 0.15)
                unit_price = base_price * (1 - discount)

            total_paid = round(unit_price * qty, 2)

            rows.append({
                "date": date_str,
                "branch_id": branch_id,
                "product_id": product_id,
                "qty": qty,
                "total_paid": total_paid,
            })

    return rows


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
    parser.add_argument("--chaos", action="store_true", help="Generate extreme oversold scenario (lots of lattes on one day)")
    parser.add_argument("--rows", type=int, default=500, help="Number of sales rows (default: 500)")
    parser.add_argument("--days", type=int, default=30, help="Number of days span (normal mode only)")
    args = parser.parse_args()

    branches_cfg = load_config("branches")["branches"]
    products_cfg = load_config("products")["products"]

    print("=" * 60)
    print("  LATTE-FLOW FAKE DATA GENERATOR - Barcelona Edition")
    print("=" * 60)
    print()
    print("Mode:", "CHAOS MODE (oversold test)" if args.chaos else "Normal realistic mode")
    print(f"  - {args.rows} sales transactions")
    if not args.chaos:
        print(f"  - {args.days} days (Jan 2026)")
    print(f"  - 5 Barcelona branches:")
    for bid, info in branches_cfg.items():
        print(f"      {bid}: {info['name']}")
    if not args.chaos:
        print("  - Tuesday croissant 30% off")
    print()

    rows = generate_fake_data(
        num_rows=args.rows,
        num_days=args.days,
        chaos_mode=args.chaos,
    )

    output_path = BRONZE_DIR / "sales_2026_jan.csv"
    write_csv(rows, output_path)

    # Stats
    print("\n" + "=" * 60)
    print("  STATS")
    print("=" * 60)

    print("\nBy product:")
    product_counts = Counter(r["product_id"] for r in rows)
    total_qty = sum(r["qty"] for r in rows)
    total_revenue = sum(r["total_paid"] for r in rows)
    for pid, count in sorted(product_counts.items()):
        name = products_cfg[pid]["name"]
        qty = sum(r["qty"] for r in rows if r["product_id"] == pid)
        rev = sum(r["total_paid"] for r in rows if r["product_id"] == pid)
        print(f"  {name}: {count} sales, {qty} units, €{rev:.2f}")

    print("\nBy branch:")
    branch_counts = Counter(r["branch_id"] for r in rows)
    for bid, count in sorted(branch_counts.items()):
        name = branches_cfg[bid]["name"]
        rev = sum(r["total_paid"] for r in rows if r["branch_id"] == bid)
        print(f"  {name}: {count} sales, €{rev:.2f}")

    print(f"\nTotals: {len(rows)} transactions, {total_qty} units, €{total_revenue:.2f}")

    if not args.chaos:
        tuesday_croissants = [r for r in rows if r["product_id"] == 104]
        discounted = sum(1 for r in tuesday_croissants
                         if r["total_paid"] / r["qty"] < 2.80 * 0.75)
        print(f"\nTuesday croissant discounts: {discounted}/{len(tuesday_croissants)} croissant sales")

    print("\n" + "=" * 60)
    print("  Done! Run the DAG to process.")
    print("=" * 60)


if __name__ == "__main__":
    main()