"""
python ingest_mock_data.py --help
"""
import argparse
import os
import random
from datetime import datetime, timedelta
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


def get_last_ingest(conn, table):
    res = conn.execute(f"SELECT MAX(ingested_at) FROM {table}").fetchone()[0]
    if res is None:
        return datetime.now() - timedelta(hours=1)
    return res


def get_max_id(conn, table, pk_col):
    res = conn.execute(f"SELECT MAX({pk_col}) FROM {table}").fetchone()[0]
    return res if res is not None else 0


def create_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        created_at TIMESTAMP,
        ingested_at TIMESTAMP,
        country_code TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_posts (
        post_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_text TEXT,
        created_at TIMESTAMP,
        ingested_at TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_id INTEGER,
        event_ts TIMESTAMP,
        event_type TEXT,
        ingested_at TIMESTAMP
    );
    """)

def generate_users(conn, fake, now, full_refresh):
    last_ingest = get_last_ingest(conn, 'raw_users')
    start_id = get_max_id(conn, 'raw_users', 'user_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(10, 20)
    rows = []
    for uid in range(start_id, start_id + count):
        created_at = fake.date_time_between(start_date=last_ingest, end_date=now)
        rows.append((uid, fake.first_name(), fake.last_name(), created_at, now, fake.country_code()))
    conn.executemany("INSERT INTO raw_users VALUES (?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} users (IDs {start_id}-{start_id+len(rows)-1})")


def generate_posts(conn, fake, now, full_refresh):
    last_ingest = get_last_ingest(conn, 'raw_posts')
    start_id = get_max_id(conn, 'raw_posts', 'post_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(10, 20)
    users = conn.execute("SELECT user_id, created_at FROM raw_users").fetchall()
    rows = []
    for pid in range(start_id, start_id + count):
        user_id, user_created = random.choice(users)
        lower = max(last_ingest, user_created)
        created_at = fake.date_time_between(start_date=lower, end_date=now)
        rows.append((pid, user_id, ' '.join(fake.words(5)), created_at, now))
    conn.executemany("INSERT INTO raw_posts VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} posts (IDs {start_id}-{start_id+len(rows)-1})")


def generate_events(conn, fake, now, full_refresh):
    last_ingest = get_last_ingest(conn, 'raw_events')
    start_id = get_max_id(conn, 'raw_events', 'event_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 150)
    existing = set(conn.execute("SELECT user_id, post_id, event_type FROM raw_events").fetchall())
    posts = conn.execute("SELECT post_id, created_at FROM raw_posts").fetchall()
    users = [row[0] for row in conn.execute("SELECT user_id FROM raw_users").fetchall()]
    event_types = ['like', 'share', 'comment']
    rows, seen = [], set()
    event_id = start_id
    # generate new events until we have requested number of events
    while event_id < start_id + count:
        post_id, post_created = random.choice(posts)
        user_id = random.choice(users)
        etype = random.choice(event_types)
        unique_combination = (user_id, post_id, etype)
        if unique_combination in existing or unique_combination in seen:
            continue # do not add duplicate events
        else:
            event_ts = fake.date_time_between(start_date=max(last_ingest, post_created), end_date=now)
            rows.append((event_id, user_id, post_id, event_ts, etype, now))
            seen.add(unique_combination)
            event_id += 1
    conn.executemany("INSERT INTO raw_events VALUES (?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} events (IDs {start_id}-{start_id+len(rows)-1})")


def main():
    args = parse_args()
    # On full-refresh, remove existing DB file so DuckDB recreates it
    if args.full_refresh and os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing database file '{DB_FILE}' for full-refresh.")
    # if file does not exist, treat as full-refresh
    elif not args.full_refresh and not os.path.exists(DB_FILE):
        args.full_refresh = True
        print(f"Database file '{DB_FILE}' not found. Performing full-refresh.")
    now = datetime.now()
    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        fake.seed_instance(args.seed)
    conn = duckdb.connect(DB_FILE)
    create_tables(conn)
    generate_users(conn, fake, now, args.full_refresh)
    generate_posts(conn, fake, now, args.full_refresh)
    generate_events(conn, fake, now, args.full_refresh)
    conn.close()
    print("Data ingestion complete.")


if __name__ == '__main__':
    main()