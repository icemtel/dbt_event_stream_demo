-- Simulate raw posts data
with
{{ synth_column_primary_key(name='post_id') }}
{{ synth_column_foreign_key(name='user_id', model_name='raw_users', column='user_id') }}
{{ synth_column_string(name='created_at', min_length=1, max_length=30) }}
{{ synth_column_string(name='updated_at', min_length=1, max_length=30) }}
{{ synth_column_string(name='deleted_at', min_length=1, max_length=30) }}
{{ synth_column_string(name='post_text', min_length=1, max_length=500) }}
{{ synth_table(rows=200) }}
select *, current_timestamp() as ingested_at from synth_table