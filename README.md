# event_stream_demo

This is a mock dbt project with a goal of building a dimensional model for event stream data
- Raw events data is generated and  using a Python script (`ingest_mock_data.py`) and ingested into a DuckDB database.
- Then the raw data is transformed with a dbt pipeline to implement a star schema with SCD2 dim tables.


### Data warehouse deliverables

Metrics:
- posts: views, likes, ratio
- authors: total views, total likes, ratio, is_active? slices
- viewers: sessions, activity


### Out of Scope


- Event deduplication - assume that this is handled upstream
(e.g. utilize [exactly-once-delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) Bigquery feature)
- Realistic geographic or temporal distribution of sample data
- hard deletes
- post unlikes


### TODO


If no user attributes changed, just a metric count, what do we do?
A: keep on the user, create a new row whenever there is new activity (metrics changed)
B: keep a separate table with metrics


## Data Testing

SCD2
- continuous?

Test my data assumptions:
- deleted users do not generate events
- there are no new events on deleted posts
- etc..


## Setup

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## How to run
Use the `ingest_mock_data.py` script to generate mock data:

```bash
# First run - creates database with initial data
python ingest_mock_data.py

# Subsequent runs - adds incremental data
python ingest_mock_data.py

# Full refresh - recreates the entire database
python ingest_mock_data.py --full-refresh
```

This creates `event_stream_demo.duckdb` with tables for users, posts, and events.



`ingest_and_dbt_build.sh` runs the ingestion job, followed by `dbt build`.

