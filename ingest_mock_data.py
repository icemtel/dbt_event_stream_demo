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
    CREATE TABLE IF NOT EXISTS raw.user (
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
    CREATE TABLE IF NOT EXISTS raw.post (
        post_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_text TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.event (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        post_id INTEGER,
        event_ts TIMESTAMP,
        event_type TEXT
    );
    """)


def fetch_random_ids(conn, table, n_rows):

    return [row[0] for row in conn.execute(
        f"""
        SELECT {table}_id
        FROM raw.{table}
        WHERE deleted_at IS NULL
        ORDER BY RANDOM()
        LIMIT {n_rows};
        """,
    ).fetchall()]

def count_rows(conn, table):
    return conn.execute(
        f"""
        SELECT COUNT(*)
        FROM raw.{table}
        WHERE deleted_at IS NULL
        """
    ).fetchone()[0]

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


def get_updatable_attributes(table):
    """
    Retrieves attributes that are allowed to be updated.
    """
    if table == 'user':
        return     [
        ("first_name",      generate_user_first_name),
        ("last_name",      generate_user_last_name),
        ("country_code",    generate_user_country_code),
        ("favorite_color",  generate_user_favorite_color),
    ]
    elif table == 'post':
        return [("post_text",  generate_post_text)]



def insert_users(conn, fake, start_dt, end_dt, full_refresh):
    start_id = get_max_id(conn, 'raw.user', 'user_id') + 1 if not full_refresh else 1
    n_rows = 200 if full_refresh else random.randint(50, 100)
    rows = []
    for user_id in range(start_id, start_id + n_rows):
        created_at = fake.date_time_between(start_date=start_dt, end_date=end_dt)
        updated_at = fake.date_time_between(start_date=created_at, end_date=end_dt)
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
    conn.executemany("INSERT INTO raw.user VALUES (?, ?, ?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {n_rows} rows in user (IDs {start_id}-{start_id + n_rows - 1}).")
    return n_rows


def insert_posts(conn, fake, start_dt, end_dt, full_refresh):
    start_id = get_max_id(conn, 'raw.post', 'post_id') + 1 if not full_refresh else 1
    n_rows = 200 if full_refresh else random.randint(50, 100)
    user_rows = conn.execute(
        "SELECT user_id, created_at FROM raw.user WHERE deleted_at IS NULL"
    ).fetchall()
    rows = []
    for post_id in range(start_id, start_id + n_rows):
        user_id, user_created = random.choice(user_rows)
        lower = max(start_dt, user_created)
        created_at = fake.date_time_between(start_date=lower, end_date=end_dt)
        updated_at = fake.date_time_between(start_date=created_at, end_date=end_dt)
        rows.append((
            post_id,
            user_id,
            generate_post_text(fake),
            created_at,
            updated_at,
            None
        ))
    conn.executemany("INSERT INTO raw.post VALUES (?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {n_rows} rows in post (IDs {start_id}-{start_id + n_rows - 1}).")
    return n_rows


def insert_events(conn, fake, start_dt, end_dt, full_refresh):
    start_id = get_max_id(conn, 'raw.event', 'event_id') + 1 if not full_refresh else 1
    n_rows = 200 if full_refresh else random.randint(50, 100)
    event_types = ['like', 'share', 'comment']
    existing_events = set(conn.execute(
        "SELECT user_id, post_id, event_type FROM raw.event"
    ).fetchall())
    posts = conn.execute(
        "SELECT post_id, created_at FROM raw.post WHERE deleted_at IS NULL"
    ).fetchall()
    users = [row[0] for row in conn.execute(
        "SELECT user_id FROM raw.user WHERE deleted_at IS NULL"
    ).fetchall()]

    rows = []
    seen = set()
    event_id = start_id
    while event_id < start_id + n_rows:
        post_id, post_created = random.choice(posts)
        user_id = random.choice(users)
        event_type = random.choice(event_types)
        unique_combination = (user_id, post_id, event_type)
        if unique_combination in existing_events or unique_combination in seen:
            continue

        lower = max(start_dt, post_created)
        event_ts = fake.date_time_between(start_date=lower, end_date=end_dt)
        rows.append((event_id, user_id, post_id, event_ts, event_type))
        seen.add(unique_combination)
        event_id += 1
    conn.executemany("INSERT INTO raw.event VALUES (?, ?, ?, ?, ?);", rows)
    print(f"Inserted {len(rows)} rows in event (IDs {start_id}-{start_id+len(rows)-1}).")
    return len(rows)


def update_rows(conn, table, fake, start_dt, end_dt):
    updatable_attribute_generators =  get_updatable_attributes(table)
    total = count_rows(conn, table)
    n_rows = random.randint(1, 5) + int(total * UPDATE_FRACTION)
    ids = fetch_random_ids(conn, table, n_rows)

    for id in ids:
        # randomly select the number of attributes to update and generate new values
        num_attributes_to_update = random.randint(1,  len(updatable_attribute_generators))
        picks = random.sample(updatable_attribute_generators, num_attributes_to_update)
        updates = [f"{label} = ?" for label, gen in picks]
        params = [gen(fake) for label, gen in picks]

        updated_at = fake.date_time_between(start_date=start_dt, end_date=end_dt)
        updates.append("updated_at = ?"); params.append(updated_at)
        params.append(id)
        conn.execute(
            f"UPDATE raw.{table} SET {', '.join(updates)} WHERE {table}_id = ?;",
            params
        )
    print(f"Updated {n_rows} rows in {table}.")
    return n_rows


def delete_rows(conn, table, now):
    total = count_rows(conn, table)
    n_rows = random.randint(1, 5) + int(total * DELETE_FRACTION)
    ids = fetch_random_ids(conn, table, n_rows)

    if ids:
        conn.execute(
            f"UPDATE raw.{table} SET deleted_at = ? WHERE {table}_id IN (%s);" %
            ",".join(["?" for _ in ids]), [now] + ids
        )
    print(f"Deleted {n_rows} rows in {table}.")
    return n_rows



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
    start_dt = datetime.combine(job_date, time.min)
    end_dt = datetime.combine(job_date, time.max)

    fake = Faker()
    if args.seed is not None:
        random.seed(args.seed)
        fake.seed_instance(args.seed)

    if not args.full_refresh:
        updated_users = update_rows(conn, 'user', fake, start_dt, end_dt)
        updated_posts = update_rows(conn, 'post',fake, start_dt, end_dt)
    else:
        updated_users = updated_posts = 0

    inserted_users = insert_users(conn, fake, start_dt, end_dt, args.full_refresh)
    inserted_posts = insert_posts(conn, fake, start_dt, end_dt, args.full_refresh)
    inserted_events = insert_events(conn, fake, start_dt, end_dt, args.full_refresh)

    if not args.full_refresh:
        deleted_users = delete_rows(conn, 'user', now)
        deleted_posts = delete_rows(conn, 'post', now)
    else:
        deleted_users = deleted_posts = 0

    for table_name, ins_count, upd_count, del_count in [
        ('user', inserted_users, updated_users, deleted_users),
        ('post', inserted_posts, updated_posts, deleted_posts),
        ('event', inserted_events, 0, 0)
    ]:
        conn.execute(
            "INSERT INTO raw.ingestion_audit VALUES (?, ?, ?, ?, ?, ?);",
            (job_date, table_name, ins_count, upd_count, del_count, now)
        )

    conn.close()
    print(f"Data ingestion for fictional date {job_date} complete.")


if __name__ == '__main__':
    main()