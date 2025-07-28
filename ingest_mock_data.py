import argparse
import os
import random
from datetime import datetime, date, time, timedelta
import duckdb
from faker import Faker

DB_FILE = 'event_stream_demo.duckdb'
UPDATE_FRACTION = 0.05  # fraction of records to update
DELETE_FRACTION = 0.01  # fraction of records to delete


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
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.ingestion_audit (
        ingest_date DATE,
        table_name TEXT,
        rows_inserted INTEGER,
        rows_updated INTEGER,
        rows_deleted INTEGER,
        real_timestamp TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP,
        country_code TEXT,
        favorite_color TEXT
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.posts (
        post_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_text TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_id INTEGER,
        event_ts TIMESTAMP,
        event_type TEXT
    );
    """)


def generate_post_text(fake):
    """Generate post text with random length between 5-15 words."""
    word_count = random.randint(5, 15)
    return ' '.join(fake.words(word_count))

def generate_user_first_name(fake):
    """Generate a random first name."""
    return fake.first_name()

def generate_user_last_name(fake):
    """Generate a random last name."""
    return fake.last_name()

def generate_user_country_code(fake):
    """Generate a random country code."""
    return fake.country_code()

def generate_user_favorite_color(fake):
    """Generate a random favorite color."""
    return fake.safe_color_name()


def insert_users(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.users', 'user_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    rows = []
    for user_id in range(start_id, start_id + count):
        created_at = fake.date_time_between(start_date=start_time, end_date=end_time)
        updated_at = fake.date_time_between(start_date=created_at, end_date=end_time)
        rows.append((
            user_id,
            generate_user_first_name(fake),
            generate_user_last_name(fake),
            created_at,
            updated_at,
            None,
            generate_user_country_code(fake),
            generate_user_favorite_color(fake)
        ))
    conn.executemany("INSERT INTO raw.users VALUES (?, ?, ?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {count} users (IDs {start_id}-{start_id + count - 1})")
    return count


def insert_posts(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.posts', 'post_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    user_rows = conn.execute(
        "SELECT user_id, created_at FROM raw.users WHERE deleted_at IS NULL"
    ).fetchall()
    rows = []
    for post_id in range(start_id, start_id + count):
        user_id, user_created = random.choice(user_rows)
        lower = max(start_time, user_created)
        created_at = fake.date_time_between(start_date=lower, end_date=end_time)
        updated_at = fake.date_time_between(start_date=created_at, end_date=end_time)
        rows.append((
            post_id,
            user_id,
            generate_post_text(fake),
            created_at,
            updated_at,
            None
        ))
    conn.executemany("INSERT INTO raw.posts VALUES (?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {count} posts (IDs {start_id}-{start_id + count - 1})")
    return count


def insert_events(conn, fake, start_time, end_time, full_refresh):
    start_id = get_max_id(conn, 'raw.events', 'event_id') + 1 if not full_refresh else 1
    count = 200 if full_refresh else random.randint(50, 100)
    event_types = ['like', 'share', 'comment']
    existing_events = set(conn.execute(
        "SELECT user_id, post_id, event_type FROM raw.events"
    ).fetchall())
    posts = conn.execute(
        "SELECT post_id, created_at FROM raw.posts WHERE deleted_at IS NULL"
    ).fetchall()
    users = [row[0] for row in conn.execute(
        "SELECT user_id FROM raw.users WHERE deleted_at IS NULL"
    ).fetchall()]

    rows = []
    seen = set()
    event_id = start_id
    while event_id < start_id + count:
        post_id, post_created = random.choice(posts)
        user_id = random.choice(users)
        event_type = random.choice(event_types)
        unique_combination = (user_id, post_id, event_type)
        if unique_combination in existing_events or unique_combination in seen:
            continue

        lower = max(start_time, post_created)
        event_ts = fake.date_time_between(start_date=lower, end_date=end_time)
        rows.append((event_id, user_id, post_id, event_ts, event_type))
        seen.add(unique_combination)
        event_id += 1
    conn.executemany("INSERT INTO raw.events VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} events (IDs {start_id}-{start_id+len(rows)-1})")
    return len(rows)


def update_users(conn, fake, start_time, end_time):
    all_ids = [row[0] for row in conn.execute(
        "SELECT user_id FROM raw.users WHERE deleted_at IS NULL"
    ).fetchall()]
    to_update = random.sample(all_ids, max(1, int(len(all_ids) * UPDATE_FRACTION)))
    for user_id in to_update:
        updates, params = [], []
        if random.choice([True, False]):
            updates.append("first_name = ?"); params.append(generate_user_first_name(fake))
        if random.choice([True, False]):
            updates.append("last_name = ?"); params.append(generate_user_last_name(fake))
        if random.choice([True, False]):
            updates.append("country_code = ?"); params.append(generate_user_country_code(fake))
        if random.choice([True, False]):
            updates.append("favorite_color = ?"); params.append(generate_user_favorite_color(fake))
        updated_at = fake.date_time_between(start_date=start_time, end_date=end_time)
        updates.append("updated_at = ?"); params.append(updated_at)
        params.append(user_id)
        conn.execute(
            f"UPDATE raw.users SET {', '.join(updates)} WHERE user_id = ?;",
            params
        )
    print(f"Updated {len(to_update)} users")
    return len(to_update)


def update_posts(conn, fake, start_time, end_time):
    all_ids = [row[0] for row in conn.execute(
        "SELECT post_id FROM raw.posts WHERE deleted_at IS NULL"
    ).fetchall()]
    to_update = random.sample(all_ids, max(1, int(len(all_ids) * UPDATE_FRACTION)))
    for post_id in to_update:
        new_text = generate_post_text(fake)
        updated_at = fake.date_time_between(start_date=start_time, end_date=end_time)
        conn.execute(
            "UPDATE raw.posts SET post_text = ?, updated_at = ? WHERE post_id = ?;",
            (new_text, updated_at, post_id)
        )
    print(f"Updated {len(to_update)} posts")
    return len(to_update)


def delete_users(conn, now):
    total = conn.execute(
        "SELECT COUNT(*) FROM raw.users WHERE deleted_at IS NULL"
    ).fetchone()[0]
    num = max(1, int(total * DELETE_FRACTION))
    ids = [row[0] for row in conn.execute(
        "SELECT user_id FROM raw.users WHERE deleted_at IS NULL ORDER BY RANDOM() LIMIT ?;", (num,)
    ).fetchall()]
    deleted = len(ids)
    if ids:
        conn.execute(
            "UPDATE raw.users SET deleted_at = ? WHERE user_id IN (%s);" %
            ",".join(["?" for _ in ids]), [now] + ids
        )
    print(f"Deleted {deleted} users")
    return deleted


def delete_posts(conn, now):
    total = conn.execute(
        "SELECT COUNT(*) FROM raw.posts WHERE deleted_at IS NULL"
    ).fetchone()[0]
    num = max(1, int(total * DELETE_FRACTION))
    ids = [row[0] for row in conn.execute(
        "SELECT post_id FROM raw.posts WHERE deleted_at IS NULL ORDER BY RANDOM() LIMIT ?;", (num,)
    ).fetchall()]
    deleted = len(ids)
    if ids:
        conn.execute(
            "UPDATE raw.posts SET deleted_at = ? WHERE post_id IN (%s);" %
            ",".join(["?" for _ in ids]), [now] + ids
        )
    print(f"Deleted {deleted} posts")
    return deleted


def main():
    args = parse_args()
    if args.full_refresh and os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    elif not args.full_refresh and not os.path.exists(DB_FILE):
        args.full_refresh = True

    conn = duckdb.connect(DB_FILE)
    create_tables(conn)
    last_job = get_last_job_date(conn)
    job_date = date(2100, 1, 1) if (args.full_refresh or last_job is None) else last_job + timedelta(days=1)
    now = datetime.now()
    start_time = datetime.combine(job_date, time.min)
    end_time = datetime.combine(job_date, time.max)
    
    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        fake.seed_instance(args.seed)

    if not args.full_refresh:
        updated_users = update_users(conn, fake, start_time, end_time)
        updated_posts = update_posts(conn, fake, start_time, end_time)
    else:
        updated_users = updated_posts = 0

    users_count = insert_users(conn, fake, start_time, end_time, args.full_refresh)
    posts_count = insert_posts(conn, fake, start_time, end_time, args.full_refresh)
    events_count = insert_events(conn, fake, start_time, end_time, args.full_refresh)

    if not args.full_refresh:
        deleted_users = delete_users(conn, now)
        deleted_posts = delete_posts(conn, now)
    else:
        deleted_users = deleted_posts = 0

    for table_name, ins_count, upd_count, del_count in [
        ('users', users_count, updated_users, deleted_users),
        ('posts', posts_count, updated_posts, deleted_posts),
        ('events', events_count, 0, 0)
    ]:
        conn.execute(
            "INSERT INTO raw.ingestion_audit VALUES (?, ?, ?, ?, ?, ?);",
            (job_date, table_name, ins_count, upd_count, del_count, now)
        )

    conn.close()
    print(f"Data ingestion for fictional date {job_date} complete.")


if __name__ == '__main__':
    main()