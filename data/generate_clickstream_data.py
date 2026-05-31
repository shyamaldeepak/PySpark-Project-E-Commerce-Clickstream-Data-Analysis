from __future__ import annotations

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

EVENT_TYPES = ["session_start", "page_view", "search", "product_view", "cart_add", "checkout", "purchase"]
PAGES = ["home", "category", "product", "cart", "checkout", "search"]
PRODUCTS = ["SKU-1001", "SKU-1002", "SKU-1003", "SKU-1004", "SKU-1005"]
CATEGORIES = ["electronics", "fashion", "home", "sports"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "IN", "GB", "DE", "CA"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce clickstream JSON files")
    parser.add_argument("--output", default="data/landing", help="Directory where JSON batches will be written")
    parser.add_argument("--batches", type=int, default=5, help="How many files to generate")
    parser.add_argument("--events-per-batch", type=int, default=50, help="Events per file")
    parser.add_argument("--delay", type=float, default=0.0, help="Seconds to wait between files")
    return parser.parse_args()


def build_event(event_time: datetime, batch_index: int, event_index: int) -> dict:
    event_type = random.choices(
        EVENT_TYPES,
        weights=[18, 28, 12, 20, 10, 5, 7],
        k=1,
    )[0]
    product_id = random.choice(PRODUCTS) if event_type in {"product_view", "cart_add", "checkout", "purchase"} else None
    category = random.choice(CATEGORIES) if product_id else None
    price = round(random.uniform(15.0, 250.0), 2) if product_id else None
    quantity = random.randint(1, 3) if event_type == "purchase" else (1 if product_id else None)
    session_prefix = f"session-{batch_index}-{event_index // 6}"

    return {
        "event_id": str(uuid4()),
        "event_time": event_time.isoformat(),
        "user_id": f"user-{random.randint(1, 20):03d}",
        "session_id": session_prefix,
        "event_type": event_type,
        "page": random.choice(PAGES),
        "product_id": product_id,
        "category": category,
        "device_type": random.choice(DEVICE_TYPES),
        "country": random.choice(COUNTRIES),
        "price": price,
        "quantity": quantity,
    }


def write_batch(output_dir: Path, batch_index: int, events_per_batch: int) -> None:
    base_time = datetime.now(timezone.utc) - timedelta(minutes=events_per_batch)
    events = [
        build_event(base_time + timedelta(seconds=index * 5), batch_index, index)
        for index in range(events_per_batch)
    ]
    batch_path = output_dir / f"clickstream_batch_{batch_index:03d}.json"
    with batch_path.open("w", encoding="utf-8") as file_handle:
        for event in events:
            file_handle.write(json.dumps(event))
            file_handle.write("\n")
    print(f"wrote {batch_path} ({len(events)} events)")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    for batch_index in range(args.batches):
        write_batch(output_dir, batch_index, args.events_per_batch)
        if args.delay > 0 and batch_index < args.batches - 1:
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
