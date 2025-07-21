-- Simulate raw users data
with
{{ synth_column_primary_key(name='user_id') }}
{{ synth_column_string(name='name', min_length=1, max_length=100) }}
{{ synth_column_string(name='country_code', min_length=2, max_length=2) }}
{{ synth_table(rows=100) }}
select * from synth_table