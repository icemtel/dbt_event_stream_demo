# event_stream_demo

This is a mock dbt project with a goal of building a dimensional model for event stream data.
Raw data is transformed with a dbt pipeline to implement a star schema with SCD2 dim tables.

- Mock raw data is generated using `ingest_mock_data.py`. 
  Running this script once creates the DuckDB file, and creates some data for day 1, 
  running it again generates data for day 2, and so on.
- Data represents events and relations on a simple social-network platform (users, posts, events [views, likes]).


Script `./ingest_and_dbt_build.sh` runs the ingestion job, followed by `dbt build`.

## Setup

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```



# Design
## Data warehouse deliverables

Metrics:
- posts: views, likes, ratio
- creators: total views, total likes, ratio, is_active? slices
- viewers: sessions, activity (TBD)

## Out of Scope

- Event deduplication - assume that this is handled upstream
(e.g. utilize [exactly-once-delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) Bigquery feature)
- Realistic geographic or temporal distribution of sample data
- post unlikes

## Data pipeline design choices

SCD2 on source tables (users, posts) using dbt snapshots:
1. we'll likeky need those audit & compliance.
2. In case dbt run fails on downstream models, this snapshot will be run first and so we'll know the changes on that day.

SCD2 on analytics dimensions to calculate advanced metrics that require knowledge of past states.

For most analyses, daily-grain event tables should suffice, but 
for advanced queries, event-grain fact table `fct_events` is also exposed to the analytics layer.
It only exposes `raw_events` 

# TODO

Snapshot without metric count.
MAYBE: keep deleted records (can be filtered on the BI layer, e.g. using Metabase Models)

Verify correctness using tests & ad hoc jupyter notebooks
