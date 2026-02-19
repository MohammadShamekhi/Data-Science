#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import signal
import sys
import time
from typing import Optional

from confluent_kafka import Producer

RUNNING = True


def _handle_shutdown(sig, frame):
    global RUNNING
    RUNNING = False


def delivery_report(err, msg):
    # Optional callback
    if err is not None:
        print(f"[DELIVERY ERROR] {err}", file=sys.stderr)


def guess_key(row: dict, key_field: Optional[str]) -> Optional[bytes]:
    if not key_field:
        return None
    val = row.get(key_field)
    if val is None or str(val).strip() == "":
        return None
    return str(val).encode("utf-8")


def clean_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        s = "" if v is None else str(v).strip()
        out[k] = None if s == "" else s
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Produce CSV rows to Kafka topic as JSON messages (row-by-row)."
    )
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="sales_events", help="Kafka topic name")
    parser.add_argument("--csv", required=True, help="Path to input CSV file")
    parser.add_argument("--key-field", default=None, help="CSV column name to use as Kafka message key (e.g., CustomerId)")
    parser.add_argument("--delay-ms", type=int, default=0, help="Delay between messages (simulate realtime). 0 = no delay")
    parser.add_argument("--limit", type=int, default=0, help="Send only first N rows. 0 = send all")
    args = parser.parse_args()

    # IMPORTANT: visible logs (so you always see something)
    print(f"[START] csv={args.csv} topic={args.topic} bootstrap={args.bootstrap} key_field={args.key_field}")
    sys.stdout.flush()

    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    conf = {
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "linger.ms": 5,
    }

    producer = Producer(conf)

    sent = 0
    start = time.time()

    try:
        with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise ValueError("CSV has no header row.")

            for row in reader:
                global RUNNING
                if not RUNNING:
                    break

                row = clean_row(row)
                payload = json.dumps(row, ensure_ascii=False).encode("utf-8")
                key = guess_key(row, args.key_field)

                producer.produce(
                    topic=args.topic,
                    value=payload,
                    key=key,
                    callback=delivery_report,
                )
                producer.poll(0)

                sent += 1
                if sent % 1000 == 0:
                    print(f"[PROGRESS] sent={sent}")
                    sys.stdout.flush()

                if args.limit and sent >= args.limit:
                    break

                if args.delay_ms > 0:
                    time.sleep(args.delay_ms / 1000.0)

        producer.flush(30)

    except FileNotFoundError:
        print(f"[ERROR] CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        try:
            producer.flush(10)
        except Exception:
            pass
        sys.exit(1)
    finally:
        elapsed = time.time() - start
        rate = sent / elapsed if elapsed > 0 else 0.0
        print(f"[DONE] Sent={sent} messages in {elapsed:.2f}s (~{rate:.1f} msg/s)")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
