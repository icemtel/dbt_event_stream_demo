-- Simulate raw events data
with
{{ synth_column_primary_key(name='event_id') }}
{{ synth_column_string(name='event_timestamp', min_length=1, max_length=30) }}
{{ synth_column_string(name='event_type', min_length=1, max_length=50) }}
{{ synth_column_foreign_key(name='user_id', model_name='raw_users', column='user_id') }}
{{ synth_column_foreign_key(name='post_id', model_name='raw_posts', column='post_id') }}
{{ synth_column_string(name='metadata', min_length=1, max_length=1000) }}
{{ synth_table(rows=500) }}
select *, current_timestamp() as ingested_at from synth_table