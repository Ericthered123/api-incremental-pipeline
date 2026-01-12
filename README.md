# api-incremental-pipeline
This project simulates a small data ingestion service that pulls data incrementally from an external API and prepares it for analytics consumption
A production-ready data pipeline demonstrating incremental processing, data quality, and scalability patterns for Databricks migration


## Project Overview
This project implements an incremental data pipeline that ingests events from GitHub's public API, processes them through a medallion architecture (Bronze → Silver → Gold), and generates analytics-ready metrics.

## Why This Approach?

Incremental processing: Only processes new data, reducing costs and improving efficiency
Idempotent design: Multiple runs with same input produce same output
Production patterns: Error handling, retry logic, observability, state management
Databricks-ready: Architecture mirrors Delta Lake patterns and can scale to Spark


## Medallion Architecture

```text
┌─────────────────┐
│   GitHub API    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BRONZE Layer   │  ← Raw data ingestion
│  (data/raw/)    │     • API responses as-is
└────────┬────────┘     • Immutable storage
         │              • Audit trail
         ▼
┌─────────────────┐
│  SILVER Layer   │  ← Cleaned & validated
│ (data/curated/) │     • Deduplication
└────────┬────────┘     • Schema validation
         │              • Enrichment
         ▼
┌─────────────────┐
│   GOLD Layer    │  ← Aggregated metrics
│ (data/metrics/) │     • Event type counts
└─────────────────┘     • Repo statistics
                        • Time series


## Component Diagram

```text
┌──────────────────────────────────────────────────────┐
│                   Main Orchestrator                   │
│                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │   Fetcher   │→ │ Transformer │→ │  Metrics    │ │
│  │  (API I/O)  │  │  (Cleaning) │  │(Aggregation)│ │
│  └─────────────┘  └─────────────┘  └─────────────┘ │
│                                                       │
│  ┌─────────────┐  ┌─────────────┐                   │
│  │   State     │  │   Config    │                   │
│  │ (Checkpoint)│  │  (Jsonnet)  │                   │
│  └─────────────┘  └─────────────┘                   │
└──────────────────────────────────────────────────────┘



## Key Concepts Demonstrated

1. Incremental Processing (Core Feature)
The pipeline maintains a checkpoint of the last processed timestamp and only fetches new data on subsequent runs.
How it works:
python# First run (bootstrap)
last_ts = None  # No checkpoint exists
fetch_events(since=None)  # Get all available events
save_checkpoint("2024-01-15T10:00:00Z")

# Second run (incremental)
last_ts = "2024-01-15T10:00:00Z"  # Load from checkpoint
fetch_events(since=last_ts)  # Only get events AFTER this time
save_checkpoint("2024-01-15T11:00:00Z")  # Update checkpoint
Why it matters:

Cost reduction: Fewer API calls, less data processed
Performance: Only process what's new
Correctness: No data duplication

2. Idempotent Design
Running the pipeline multiple times with the same inputs produces the same outputs.
Implementation:

Content-based hashing for deduplication
Atomic checkpoint updates (write-then-rename)
Deterministic transformations

3. Data Quality
Multiple validation layers ensure data correctness:
python# Schema validation
required_fields = ['id', 'type', 'created_at', 'repo']

# Deduplication
seen_ids = set()

# Type validation
datetime.fromisoformat(timestamp)

# Business rules
if event_type in allowed_types: ...
4. Fault Tolerance
Retry logic with exponential backoff:
pythonfor attempt in range(max_retries):
    try:
        response = api.get(...)
        break
    except Exception:
        wait_time = 2 ** attempt
        time.sleep(wait_time)
Checkpoint-based recovery:

If pipeline fails, checkpoint doesn't advance
Next run retries from same point
No data loss

5. Observability
Structured logging:
json{
  "timestamp": "2024-01-15T10:00:00Z",
  "level": "INFO",
  "message": "Processed 100 events",
  "records_processed": 100,
  "run_id": "abc-123"
}
Execution tracking:

Run history (JSONL format)
Duration metrics
Success/failure rates


## Quick Start

## Prerequisites

# Python 3.8+
python --version

# Optional: jsonnet for config management
brew install jsonnet  # macOS
# or
pip install jsonnet-binary


## Installation

# Clone repository
git clone <repo-url>
cd github-events-pipeline

# Install dependencies
pip install -r requirements.txt

Run the Pipeline

# First run (bootstrap)
python src/main.py --env dev

# Subsequent runs (incremental)
python src/main.py --env dev

# Production mode
python src/main.py --env prod


##    Run Tests


# Run all tests
python -m pytest tests/ -v

# Run specific test
python -m pytest tests/test_pipeline.py::TestAPIFetcher -v

# Coverage report
python -m pytest tests/ --cov=src --cov-report=html


##    Example Output

 2024-01-15 10:00:00 - INFO - === Pipeline Started (run_id: abc-123) ===
2024-01-15 10:00:00 - INFO - Incremental run: fetching events after 2024-01-15T09:00:00Z
2024-01-15 10:00:05 - INFO - Step 1/5: Fetching events from API...
2024-01-15 10:00:10 - INFO - Fetched 150 events
2024-01-15 10:00:10 - INFO - Step 2/5: Transforming events...
2024-01-15 10:00:11 - INFO - Batch transformation: 148 valid, 2 filtered, 0 invalid
2024-01-15 10:00:11 - INFO - Step 3/5: Calculating metrics...
2024-01-15 10:00:12 - INFO - Step 4/5: Persisting results...
2024-01-15 10:00:13 - INFO - Step 5/5: Saving checkpoint...
2024-01-15 10:00:13 - INFO - === Pipeline Completed Successfully ===
2024-01-15 10:00:13 - INFO - Processed 148 events

## Metrics Output (JSON)

{
  "summary": {
    "total_events": 148,
    "calculation_timestamp": "2024-01-15T10:00:12Z"
  },
  "event_types": {
    "PushEvent": 85,
    "PullRequestEvent": 42,
    "IssuesEvent": 21
  },
  "repos": {
    "apache/spark": {
      "total_events": 95,
      "unique_contributors": 23,
      "active_contributions": 90
    }
  },
  "rankings": {
    "top_repos": [
      {"repo": "apache/spark", "total_events": 95},
      {"repo": "delta-io/delta", "total_events": 53}
    ],
    "top_contributors": [
      {"contributor": "user1", "total_contributions": 45}
    ]
  }
}

## ## How to Scale to Databricks

This pipeline is designed to be Databricks-ready. Here's the migration path:
1. Ingestion Layer → Databricks Autoloader
Current (Python):
fetcher = APIFetcher(base_url="...")
events = fetcher.fetch_events(...)

Databricks (PySpark):

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .load("s3://bucket/raw/")
)

2. State Management → Delta Lake

Current (JSON checkpoint):

state_manager.save_checkpoint(last_timestamp)

Databricks (Delta):

# Delta Lake has built-in ACID transactions
df.write.format("delta").mode("append").save("/delta/table")

# Time travel
spark.read.format("delta").option("versionAsOf", 5).load("/delta/table")

3. Transformations → PySpark

Current (Python):
for event in events:
    cleaned = clean_event(event)

Databricks (PySpark):

df_cleaned = (df
  .filter(col("type").isin(["PushEvent", "PullRequestEvent"]))
  .withColumn("event_date", to_date(col("created_at")))
  .dropDuplicates(["id"])
)


4. Aggregations → Delta Live Tables

Current (Python):
metrics = calculator.calculate_all_metrics(events)
Databricks (DLT):
@dlt.table
def event_metrics():
  return (
    dlt.read("silver_events")
      .groupBy("event_type", "event_date")
      .agg(count("*").alias("event_count"))
  )

  5. Orchestration → Databricks Workflows

  Current (Python script):
  python main.py --env prod

  Databricks (Job definition):

  {
  "name": "github-events-pipeline",
  "tasks": [{
    "task_key": "ingest",
    "notebook_path": "/Repos/main/pipeline/ingest"
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 * * * ?"
  }
}


##  Configuration Management (Jsonnet)

Configuration is managed declaratively using Jsonnet:

// config.jsonnet
{
  api: {
    base_url: 'https://api.github.com',
    retry: { max_retries: 3, backoff: 2 }
  },
  repos: [
    { owner: 'apache', name: 'spark' },
    { owner: 'delta-io', name: 'delta' }
  ]
}

Benefits:

DRY: Functions and variables
Type safety: Compile-time validation
Environment-specific: Dev/prod configs
Versioned: Git-based change tracking


## Infrastructure (Terraform)

Infrastructure is provisioned as code:

cd terraform

# Preview changes
terraform plan

# Apply infrastructure
terraform apply

# Outputs
terraform output data_lake_bucket

Resources provisioned:

S3 bucket for data lake (with lifecycle rules)
IAM roles and policies
CloudWatch log groups and alarms


## Monitoring & Observability

Logs
Structured JSON logs for easy parsing:

{
  "timestamp": "2024-01-15T10:00:00Z",
  "level": "INFO",
  "module": "fetch",
  "message": "Fetched 150 events",
  "records": 150,
  "duration_ms": 5000
}

Metrics
Key metrics tracked:

Records processed per run
Pipeline duration
Success/failure rate
API rate limit usage

Alerting
CloudWatch alarms for:

Pipeline failures
High error rates
Stale data (no runs in 24h)


## Testing Strategy

Test Coverage

Unit tests: Individual functions (fetch, transform, metrics)
Integration tests: Components working together
Mocking: External dependencies (API, filesystem)
Edge cases: Empty data, malformed events, failures

# All tests
pytest tests/ -v

# Specific module
pytest tests/test_pipeline.py::TestAPIFetcher -v

# Coverage
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html


## Contributing
This is a demonstration project for interviews. For production use, consider:

Authentication: Add OAuth for higher rate limits
Parallel processing: Multi-threading/multiprocessing for repos
Monitoring: Integrate with Datadog, Grafana, or Databricks Observability
Data retention: Implement GDPR compliance, data deletion policies



Built with ❤️ for data engineering excellence