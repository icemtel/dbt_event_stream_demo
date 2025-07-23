"""
python ingest_mock_data.py --help
"""
import argparse
import os
import random
from datetime import datetime, date, time, timedelta
import duckdb
from faker import Faker

DB_FILE = 'event_stream_demo.duckdb'


def parse_args():
    parser = argparse.ArgumentParser(
        description='Generate and ingest mock data into DuckDB event_stream_demo.duckdb')
    parser.add_argument(
        '--full-refresh', action='store_true',
        help='Overwrite database file before ingesting')
    parser.add_argument(
        '--seed', type=int, default=None,
        help='Random seed to use (default: random)')
    return parser.parse_args()


def get_last_job_date(conn):
    res = conn.execute("SELECT MAX(ingest_date) FROM raw.ingestion_audit").fetchone()[0]
    return None if res is None else res


def get_max_id(conn, table, pk_col):
    res = conn.execute(f"SELECT MAX({pk_col}) FROM {table}").fetchone()[0]
    return res if res is not None else 0


def create_tables(conn):
    # Ensure raw schema exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    # Audit table to track ingest metadata
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.ingestion_audit (
        ingest_date DATE,
        table_name TEXT,
        rows_inserted INTEGER,
        real_timestamp TIMESTAMP
    );
    """)

    # Users
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        created_at TIMESTAMP,
        country_code TEXT
    );
    """)

    # Posts
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.posts (
        post_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_text TEXT,
        created_at TIMESTAMP
    );
    """)

    # Events
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_id INTEGER,
        event_ts TIMESTAMP,
        event_type TEXT
    );
    """)


def generate_users(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.users', 'user_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    rows = []
    for uid in range(start_id, start_id + count):
        created_at = fake.date_time_between(start_date=start_time, end_date=end_time)
        rows.append((uid, fake.first_name(), fake.last_name(), created_at, fake.country_code()))
    conn.executemany("INSERT INTO raw.users VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {count} users (IDs {start_id}-{start_id + count - 1})")
    return count


def generate_posts(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.posts', 'post_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    users = conn.execute("SELECT user_id, created_at FROM raw.users").fetchall()
    rows = []
    for pid in range(start_id, start_id + count):
        user_id, user_created = random.choice(users)
        lower = max(start_time, user_created)
        created_at = fake.date_time_between(start_date=lower, end_date=end_time)
        rows.append((pid, user_id, ' '.join(fake.words(5)), created_at))
    conn.executemany("INSERT INTO raw.posts VALUES (?, ?, ?, ?);", rows)
    print(f"Inserted {count} posts (IDs {start_id}-{start_id + count - 1})")
    return count


def generate_events(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.events', 'event_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    event_types = ['like', 'share', 'comment']
    
    existing_events = set(conn.execute("SELECT user_id, post_id, event_type FROM raw.events").fetchall())
    posts = conn.execute("SELECT post_id, created_at FROM raw.posts").fetchall()
    users = [row[0] for row in conn.execute("SELECT user_id FROM raw.users").fetchall()]
    
    rows = [] # list of row to insert
    seen = set()
    event_id = start_id
    while event_id < start_id + count:
        post_id, post_created = random.choice(posts)
        user_id = random.choice(users)
        event_type = random.choice(event_types)
        unique_combination = (user_id, post_id, event_type)
        if unique_combination in existing_events or unique_combination in seen:
            continue # do not add duplicate events

        lower = max(start_time, post_created)
        event_ts = fake.date_time_between(start_date=lower, end_date=end_time)
        rows.append((event_id, user_id, post_id, event_ts, event_type))
        seen.add(unique_combination)
        event_id += 1
    conn.executemany("INSERT INTO raw.events VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} events (IDs {start_id}-{start_id+len(rows)-1})")

    return count


def main():
    args = parse_args()

    if args.full_refresh and os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing database file '{DB_FILE}' for full-refresh.")
    elif not args.full_refresh and not os.path.exists(DB_FILE):
        args.full_refresh = True
        print(f"Database file '{DB_FILE}' not found. Performing full-refresh.")

    conn = duckdb.connect(DB_FILE)
    create_tables(conn)
    last_job = get_last_job_date(conn)
    if args.full_refresh or last_job is None:
        job_date = date(2100, 1, 1)
    else:
        job_date = last_job + timedelta(days=1)

    now = datetime.now()
    start_time = datetime.combine(job_date, time.min)
    end_time = datetime.combine(job_date, time.max)

    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        fake.seed_instance(args.seed)

    users_count = generate_users(conn, fake, start_time, end_time, args.full_refresh)
    posts_count = generate_posts(conn, fake, start_time, end_time, args.full_refresh)
    events_count = generate_events(conn, fake, start_time, end_time, args.full_refresh)

    # Record metadata per table
    for table_name, count in [
        ('users', users_count),
        ('posts', posts_count),
        ('events', events_count)
    ]:
        conn.execute(
            "INSERT INTO raw.ingestion_audit VALUES (?, ?, ?, ?);",
            (job_date, table_name, count, now)
        )

    conn.close()
    print(f"Data ingestion for fictional date {job_date} complete.")


if __name__ == '__main__':
    main()
