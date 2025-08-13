# event_stream_demo

This is a mock dbt project with a goal of building a dimensional model for event stream data.
Raw data is transformed with a dbt pipeline to implement a star schema with SCD2 dim tables (Kimball).

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

See also `analyses` folder.

## Out of Scope

- Event deduplication - assume that this is handled upstream
(e.g. utilize [exactly-once-delivery](https://cloud.google.com/pubsub/docs/exactly-once-delivery) Bigquery feature)
- Realistic geographic or temporal distribution of sample data
- post unlikes

## Data pipeline design choices

### Layers

staging:
- standardize column names and values; the structure mirrors source systems
intermediate:
-  introduce facts & dimensions that represent business entities
analytics ("core", "golden"):
- data is pre-aggregated and summarized with business-defined metrics 
- this way everyone uses the same metrics

### Snapshots of source tables (users, posts):

Why?
- Attribute events to correct cohorts (e.g. country) even if that attribute changes.
- These are slowly changing dimensions, so costs of snapshots are low relative to the events volume.
- Even if there are no questions that need history of changes yet, they may appear in the future. 
  => the first order of business is to start snapshotting, and later there will be time to optimize the costs.

How?
- SCD2 - industry standard
- we can make a snapshot independent of the success/failure of the downstream pipeline, making sure we don't miss source changes.
- Using dbt snapshots: simple to implement, does the job


## Facts

For most analyses, daily-grain event tables should suffice, but 
for advanced queries, event-grain fact table `fct_events` is also exposed to the analytics layer.
It only exposes `raw_events` 

## Dimensions 

### Snapshots

SCD2 on analytics dimensions to calculate advanced metrics that require knowledge of past states.

dbt snapshot is using `updated_at` column, so that if only the metric count is updated,
it won' trigger a snapshot.
Indeed, metric counts are expected to change more frequently than the user attributes.


### Current

- Soft-deletes are removed to simplify analyst queries
- Not removed earlier for a proper join with posts in the intermediate layer
  (we assume posts are kept on the platform even if the creator is deleted unless they request that)
 
# TODO

post creator attributes (country, etc) should use user attributes on the date of post creation ?

Verify correctness using tests & ad hoc jupyter notebooks
