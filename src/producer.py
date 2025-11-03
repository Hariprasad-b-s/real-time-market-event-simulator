"""Synthetic tick producer for Redpanda/Kafka.

This script publishes JSON-encoded tick events to a Kafka topic at a configurable
rate. It is intentionally lightweight so you can tweak symbol lists, pricing
curves, or publish cadence for experiments.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Producer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish synthetic tick events to Kafka.")
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker bootstrap servers (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        default="ticks",
        help="Kafka topic to publish to (default: %(default)s)",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=100.0,
        help="Approximate messages per second to publish (default: %(default)s)",
    )
    parser.add_argument(
        "--symbols",
        type=int,
        default=100,
        help="Number of synthetic symbols to cycle through (default: %(default)s)",
    )
    return parser.parse_args()


def build_symbol_universe(count: int) -> List[str]:
    """Generate a deterministic list of ticker symbols."""
    return [f"SYM{idx:03d}" for idx in range(count)]


def make_producer(broker: str) -> Producer:
    """Create a confluent-kafka Producer with basic delivery logging."""

    def delivery_report(err, msg) -> None:
        if err:
            print(f"Delivery failed: {err}", file=sys.stderr)

    return Producer({"bootstrap.servers": broker, "on_delivery": delivery_report})


def build_tick(symbols: List[str]) -> Dict[str, object]:
    """Construct a synthetic tick payload with basic pseudo-random pricing."""
    symbol = random.choice(symbols)
    base_price = 100 + hash(symbol) % 100  # deterministic offset per symbol
    price_variation = random.uniform(-1.5, 1.5)
    size = random.choice((50, 100, 200, 500, 1000))
    price = round(base_price + price_variation, 2)

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "price": price,
        "size": size,
    }


def main() -> None:
    args = parse_args()
    symbols = build_symbol_universe(args.symbols)
    producer = make_producer(args.broker)

    running = True

    def shutdown(signum, _frame) -> None:
        nonlocal running
        running = False
        print(f"Received signal {signum}; shutting down...")

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(
        f"Publishing to {args.topic} on {args.broker} at ~{args.rate:.0f} msg/s "
        f"across {len(symbols)} symbols. Ctrl+C to exit."
    )

    # Maintain a steady publish rate by measuring time spent per iteration.
    min_interval = 1.0 / args.rate if args.rate > 0 else 0

    try:
        while running:
            loop_start = time.perf_counter()
            tick = build_tick(symbols)
            payload = json.dumps(tick).encode("utf-8")
            producer.produce(args.topic, value=payload, on_delivery=None)
            producer.poll(0)  # trigger background delivery callbacks

            elapsed = time.perf_counter() - loop_start
            sleep_for = max(0.0, min_interval - elapsed)
            if sleep_for:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        shutdown("KeyboardInterrupt", None)
    finally:
        print("Flushing producer...")
        producer.flush(10)


if __name__ == "__main__":
    main()
