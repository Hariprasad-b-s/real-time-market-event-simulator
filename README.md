# Real-Time Market Event Simulator

This project provisions a lightweight market data simulation stack that publishes synthetic tick data to Kafka (via Redpanda) and ingests it with a Spark Structured Streaming job for real-time aggregation. The setup is designed for Mac users who want a minimal reproducible playground for streaming analytics.

## Prerequisites (macOS)

- macOS with Docker Desktop installed and running
- Python 3.10+ with `python3` and `pip`
- Java 8+ (required for Spark)
- Apache Spark 4.0.x downloaded and available on `PATH` (for `spark-submit`)
- Optional: Redpanda CLI (`rpk`) for topic management

## Start Redpanda Broker

```bash
./scripts/start_redpanda.sh
```

The helper script runs `docker compose up -d` and attempts to create the `ticks` topic. If `rpk` is not available, the Kafka topic is created automatically on first use.

## Create Python Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the Tick Producer

```bash
python3 src/producer.py --broker localhost:9092 --topic ticks --rate 100
```

The producer emits ~100 JSON tick events per second across a rotating set of symbols. Use `Ctrl+C` to stop the producer.

## Run the Spark Consumer

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  src/consumer_spark.py \
  --broker localhost:9092 \
  --topic ticks
```

The consumer maintains a sliding aggregation (10s window, 5s slide) and prints metrics to the console in update mode.

## Sample Spark Output

```
-------------------------------------------
Batch: 5
-------------------------------------------
+-------------------+-------------------+------+---------+------+------+
|window.start       |window.end         |symbol|avg_price|volume|msg_ct|
+-------------------+-------------------+------+---------+------+------+
|2024-05-14 13:41:20|2024-05-14 13:41:30|SYM042|176.42   |1200  |10    |
+-------------------+-------------------+------+---------+------+------+
```

Timestamps are in UTC and will vary. Each batch reflects the aggregated ticks for each symbol in the current window.

## Helpful Commands

- Check containers: `docker compose ps`
- Follow Redpanda logs: `docker compose logs -f redpanda`
- Clean up: `docker compose down`

## Troubleshooting

- If Kafka connections fail, confirm Docker Desktop is running and ports `9092` and `9644` are free.
- Ensure the Spark Kafka package version matches your Spark distribution.
- When running multiple terminals, keep the Python virtual environment activated in each shell where you plan to use Python.
