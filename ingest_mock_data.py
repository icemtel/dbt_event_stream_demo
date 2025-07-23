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
    res = conn.execute("SELECT MAX(job_date) FROM raw_data_jobs").fetchone()[0]
    return None if res is None else res


def get_max_id(conn, table, pk_col):
    res = conn.execute(f"SELECT MAX({pk_col}) FROM {table}").fetchone()[0]
    return res if res is not None else 0


def create_tables(conn):
    # Table to track job dates
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_data_jobs (
        job_date DATE PRIMARY KEY,
        real_timestamp TIMESTAMP
    );
    """
    )
    # Users
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        created_at TIMESTAMP,
        country_code TEXT
    );
    """
    )
    # Posts
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_posts (
        post_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_text TEXT,
        created_at TIMESTAMP
    );
    """
    )
    # Events
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_id INTEGER,
        event_ts TIMESTAMP,
        event_type TEXT
    );
    """
    )


def generate_users(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw_users', 'user_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(10, 20)
    rows = []
    for uid in range(start_id, start_id + count):
        created_at = fake.date_time_between(start_date=start_time, end_date=end_time)
        rows.append((uid, fake.first_name(), fake.last_name(), created_at, fake.country_code()))
    conn.executemany("INSERT INTO raw_users VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} users (IDs {start_id}-{start_id + len(rows) - 1})")


def generate_posts(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw_posts', 'post_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(10, 20)
    users = conn.execute("SELECT user_id, created_at FROM raw_users").fetchall()
    rows = []
    for pid in range(start_id, start_id + count):
        user_id, user_created = random.choice(users)
        lower = max(start_time, user_created)
        created_at = fake.date_time_between(start_date=lower, end_date=end_time)
        rows.append((pid, user_id, ' '.join(fake.words(5)), created_at))
    conn.executemany("INSERT INTO raw_posts VALUES (?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} posts (IDs {start_id}-{start_id + len(rows) - 1})")


def generate_events(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw_events', 'event_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 150)
    event_types = ['like', 'share', 'comment']
    
    existing_events = set(conn.execute("SELECT user_id, post_id, event_type FROM raw_events").fetchall())
    posts = conn.execute("SELECT post_id, created_at FROM raw_posts").fetchall()
    users = [row[0] for row in conn.execute("SELECT user_id FROM raw_users").fetchall()]
    
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
    conn.executemany("INSERT INTO raw_events VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} events (IDs {start_id}-{start_id+len(rows)-1})")


def main():
    args = parse_args()

    # Handle full-refresh on DB file
    if args.full_refresh and os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing database file '{DB_FILE}' for full-refresh.")
    elif not args.full_refresh and not os.path.exists(DB_FILE):
        args.full_refresh = True
        print(f"Database file '{DB_FILE}' not found. Performing full-refresh.")

    # Determine fictional job date
    conn = duckdb.connect(DB_FILE)
    create_tables(conn)
    last_job = get_last_job_date(conn)
    if args.full_refresh or last_job is None:
        job_date = date(2100, 1, 1)
    else:
        job_date = last_job + timedelta(days=1)

    # Real timestamp for this run
    now = datetime.now()

    # Define interval for data creation on that fictional date
    start_time = datetime.combine(job_date, time.min)
    end_time = datetime.combine(job_date, time.max)

    # Seed randomness
    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        fake.seed_instance(args.seed)

    # Generate data
    generate_users(conn, fake, start_time, end_time, args.full_refresh)
    generate_posts(conn, fake, start_time, end_time, args.full_refresh)
    generate_events(conn, fake, start_time, end_time, args.full_refresh)

    # Record job execution
    conn.execute(
        "INSERT INTO raw_data_jobs VALUES (?, ?);",
        (job_date, now)
    )

    conn.close()
    print(f"Data ingestion for fictional date {job_date} complete.")


if __name__ == '__main__':
    main()
