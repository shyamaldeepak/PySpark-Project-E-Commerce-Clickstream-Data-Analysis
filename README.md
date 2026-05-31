# PySpark E-Commerce Clickstream Streaming Project

This repository is a beginner-friendly PySpark project that simulates a real-time e-commerce clickstream and computes useful streaming metrics.

You will learn how to:

- generate synthetic clickstream events
- read new JSON files with Structured Streaming
- use watermarking and window aggregations
- calculate metrics that matter in e-commerce, such as traffic, revenue, and conversion rate

## What It Does

The project uses a file-based stream so you do not need Kafka or cloud services to start.

Pipeline:

1. Generate clickstream JSON batches into `data/landing`
2. Spark watches that folder as a stream
3. Spark computes live KPIs every few seconds
4. Results are shown in the console or written to parquet files

## Project Files

- `src/realtime_clickstream.py` - Structured Streaming job with windowed metrics
- `data/generate_clickstream_data.py` - synthetic event generator
- `requirements.txt` - PySpark dependency

## Setup

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Generate Sample Data

Create a few streaming batches:

```bash
python data/generate_clickstream_data.py --batches 5 --events-per-batch 50
```

## Run the Streaming Job

Start Spark in console mode:

```bash
python src/realtime_clickstream.py --input data/landing --sink console
```

Or write aggregated results to parquet:

```bash
python src/realtime_clickstream.py --input data/landing --sink parquet --output output/metrics
```

## Metrics You Get

- events per minute
- unique active users per minute
- total revenue from purchase events
- cart-to-purchase rate
- top pages by 5-minute window
- session conversion rate by 10-minute window

## Beginner Notes

If you want to practice, try these small upgrades:

1. Add a new event type like `wishlist_add`
2. Group metrics by `country` or `device_type`
3. Save results into a dashboard-friendly format
4. Add anomaly detection for sudden traffic spikes

## Example Output

When the stream runs, Spark prints aggregations like:

- window start and end time
- event counts
- distinct users
- revenue
- conversion rates

## Clean Up

Generated data and outputs are stored under:

- `data/landing`
- `output/`

You can delete those folders whenever you want a fresh run.
