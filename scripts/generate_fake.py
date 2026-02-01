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
    """Generate fake POS sales with full discount logic and branch/product weights."""
    if start_date is None:
        start_date = datetime(2026, 1, 1)

    products_cfg = load_config("products")["products"]

    # Product weights (as you defined)
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

    rows = []

    # Normal sales loop (most of the data)
    for _ in range(num_rows):
        # 1. Date & Branch Selection
        day_offset = random.randint(0, num_days - 1)
        date = start_date + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")
        branch_id = random.choice(branch_ids)

        # 2. Product Selection
        product_id = random.choices(product_ids, weights=weights)[0]
        base_price = products_cfg[product_id]["base_price_eur"]
        unit_price = base_price

        # 3. APPLY YOUR DISCOUNT DYNAMICS
        # Tuesday croissant 30% off
        if date.weekday() == 1 and product_id == 104:
            unit_price = base_price * 0.70
        # Occasional small random discount (5-15%)
        elif random.random() < 0.1:
            discount = random.uniform(0.05, 0.15)
            unit_price = base_price * (1 - discount)

        # 4. Quantity Selection (Normal vs Peak/Chaos)
        if chaos_mode:
            qty = random.randint(50, 100)  # go insane
        else:
            qty = random.choices(
                [1, 3, 5, 7, 10],
                weights=[60, 20, 10, 7, 3]
            )[0]  # slightly heavier, but not chaos

        total_paid = round(unit_price * qty, 2)

        rows.append({
            "date": date_str,
            "branch_id": branch_id,
            "product_id": product_id,
            "qty": qty,
            "total_paid": total_paid,
        })

    # INJECT OVERSOLD DAYS — this happens AFTER the normal loop
    # These are extra rows that force massive latte sales on specific days
    oversell_days = [3, 10, 17, 24]  # e.g. Jan 4, 11, 18, 25
    for day_offset in oversell_days:
        date = start_date + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")
        for i in range(80):  # 80 big latte sales per oversell day = ~1600 units total per day
            rows.append({
                "date": date_str,
                "branch_id": random.choice(branch_ids),
                "product_id": 102,  # latte = milk assassin
                "qty": 20,
                "total_paid": round(products_cfg[102]["base_price_eur"] * 20 * 0.95, 2),  # slight random discount
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