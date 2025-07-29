import argparse
import os
import random
from datetime import datetime, date, time, timedelta
import duckdb
from faker import Faker

DB_FILE = 'event_stream_demo.duckdb'
UPDATE_FRACTION = 0.05  # fraction of records to update
DELETE_FRACTION = 0.01  # fraction of records to delete
ACTIVE_FRACTION = 0.05  # fraction of users that views posts each day
MAX_EVENTS_PER_USER = 10 # max views per user each day
LIKE_RATIO = 0.1

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
        user_id VARCHAR(16) PRIMARY KEY,
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
        post_id VARCHAR(16) PRIMARY KEY,
        user_id VARCHAR(16) REFERENCES raw.user(user_id),
        post_text TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        deleted_at TIMESTAMP
    );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw.event (
        event_id VARCHAR(16) PRIMARY KEY,
        user_id VARCHAR(16) REFERENCES raw.user(user_id),
        post_id VARCHAR(16) REFERENCES raw.post(post_id),
        created_at TIMESTAMP,
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

def count_rows(conn, table, include_deleted=False):
    """
    Return the number of rows in the table.
    """
    if include_deleted:
        sql_where = ''
    else:
        sql_where = 'WHERE deleted_at IS NULL'
    return conn.execute(
        f"""
        SELECT COUNT(*)
        FROM raw.{table}
        {sql_where}
        """
    ).fetchone()[0]

def generate_id(fake):
    """
    Generate a sequence of 16 random symbols to use as ID
    """
    return fake.pystr(16, 16)

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
    n_rows = 200 if full_refresh else random.randint(50, 100)

    rows = []
    for _ in range(n_rows):
        user_id = generate_id(fake)
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

    rows.sort(key=lambda x: x[3]) # sort by created_at
    conn.executemany("INSERT INTO raw.user VALUES (?, ?, ?, ?, ?, ?, ?, ?);", rows)
    print(f"Inserted {n_rows} rows in user.")
    return n_rows


def insert_posts(conn, fake, start_dt, end_dt, full_refresh):
    user_rows = conn.execute(
            "SELECT user_id, created_at FROM raw.user WHERE deleted_at IS NULL"
        ).fetchall()

    if full_refresh:
        n_rows = 200
    else:
        n_users = len(user_rows)
        # make a random number of new posts that scales with the number of active users
        # keep this number high so that there are always posts to view
        n_rows = int((1 + random.random()) * ACTIVE_FRACTION * n_users)

    rows = []
    for _ in range(n_rows):
        post_id = generate_id(fake)
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

    rows.sort(key=lambda x: x[3]) # sort by created_at
    conn.executemany("INSERT INTO raw.post VALUES (?, ?, ?, ?, ?, ?);", rows)

    print(f"Inserted {n_rows} rows in post.")
    return n_rows


def insert_events(conn, fake, start_dt, end_dt, full_refresh):
    """
    Insert events by mimicking user sessions.

    1. Randomly select "active" users
    2. Active users view a random number of posts.
    3. Random portion of viewed posts is liked.
    """

    if full_refresh:
        n_active_users = 10
    else:
        n_users = count_rows(conn, 'user')
        n_active_users = int(ACTIVE_FRACTION * n_users) + random.randint(1, 5)

    active_user_ids = fetch_random_ids(conn, 'user', n_active_users)
    rows = [] # collect rows to insert
    for user_id in active_user_ids:

        # draw how many posts will be viewed
        n_posts = random.randint(1, MAX_EVENTS_PER_USER)
        session_start = fake.date_time_between(start_date=start_dt, end_date=end_dt) # session start
        session_end = session_start + timedelta(seconds=n_posts * random.randint(60, 600))
        session_end = min(session_end, end_dt) # make sure all events are before the ingestion date

        # select random posts that were created before the session started
        posts_sample = conn.execute(
        """
        SELECT post_id, created_at
        FROM raw.post
        WHERE deleted_at IS NULL and created_at < ?
        ORDER BY RANDOM()
        LIMIT ?
        """, (session_start, n_posts)
        ).fetchall()

        for post_id, post_created_at in posts_sample:
            view_dt = fake.date_time_between(max(session_start, post_created_at), session_end)

            rows.append((generate_id(fake), user_id, post_id, view_dt, 'view'))

            # some posts are liked
            if random.random() < LIKE_RATIO:
                like_dt = view_dt + timedelta(seconds=random.randint(1, 300))

                if like_dt < end_dt: # check that ts is before ingestion
                    rows.append((generate_id(fake), user_id, post_id, like_dt, 'like'))

    # TODO: remove duplicate likes
    # In the current model, a user may hypothetically like the same post more than once
    # we can deduplicate rows in this step, or we can just ensure a high number of users and posts

    rows.sort(key=lambda x: x[3]) # sort by created_at
    conn.executemany("INSERT INTO raw.event VALUES (?, ?, ?, ?, ?);", rows)

    print(f"Inserted {len(rows)} rows in event.")
    return len(rows)


def update_rows(conn, table, fake, start_dt, end_dt, full_refresh):
    """
    Randomly updates a subset of rows in `raw.{table}`:
    - pick rows to update
    - pick attributes to update
    - update upated_at timestamp
    - return number of rows updated

    TODO: exclude posts of deleted users
    """
    if full_refresh:
        return 0

    updatable_attribute_generators = get_updatable_attributes(table)
    total = count_rows(conn, table)
    n_rows = random.randint(1, 5) + int(total * UPDATE_FRACTION)
    ids = fetch_random_ids(conn, table, n_rows)

    for id in ids:
        # randomly select the number of attributes to update and generate new values
        num_attributes_to_update = random.randint(1,  len(updatable_attribute_generators))
        picks = random.sample(updatable_attribute_generators, num_attributes_to_update)

        update_values = [gen(fake) for label, gen in picks]
        updated_at = fake.date_time_between(start_date=start_dt, end_date=end_dt)
        update_values.append(updated_at)
        update_values.append(id) # will be included in the WHERE statement

        update_templates = [f"{label} = ?" for label, gen in picks]
        update_templates.append("updated_at = ?")
        sql = f"UPDATE raw.{table} SET {', '.join(update_templates)} WHERE {table}_id = ?;"
        conn.execute(sql, update_values)
    print(f"Updated {n_rows} rows in {table}.")
    return n_rows


def delete_rows(conn, table, fake, start_dt, end_dt, full_refresh):
    """
    Randomly marks a subset of rows in `raw.{table}` as deleted by setting a deletion timestamp.
    """
    if full_refresh:
        return 0

    total = count_rows(conn, table)
    n_rows = random.randint(1, 5) + int(total * DELETE_FRACTION)
    ids = fetch_random_ids(conn, table, n_rows)

    if ids:
        rows = [(fake.date_time_between(start_date=start_dt, end_date=end_dt), row_id)
                for row_id in ids]
        sql = f"UPDATE raw.{table} SET deleted_at = ? WHERE {table}_id = ?;"
        conn.executemany(sql, rows)

    print(f"Deleted {len(ids)} rows in {table}.")
    return len(ids)

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

    deleted_users = delete_rows(conn, 'user', fake, start_dt, end_dt,  args.full_refresh)
    deleted_posts = delete_rows(conn, 'post', fake, start_dt, end_dt,  args.full_refresh)

    updated_users = update_rows(conn, 'user', fake, start_dt, end_dt,  args.full_refresh)
    updated_posts = update_rows(conn, 'post', fake, start_dt, end_dt,  args.full_refresh)

    inserted_users = insert_users(conn, fake, start_dt, end_dt, args.full_refresh)
    inserted_posts = insert_posts(conn, fake, start_dt, end_dt, args.full_refresh)
    inserted_events = insert_events(conn, fake, start_dt, end_dt, args.full_refresh)

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

    print("===Total table sizes===")
    with duckdb.connect(DB_FILE) as summary_conn:
        for table in ["user", "post"]:
            total = count_rows(summary_conn, table, include_deleted=True)
            active = count_rows(summary_conn, table, include_deleted=False)
            deleted = total - active
            print(f"{table}: {active} active rows (+ {deleted} deleted rows)")

        # event table does not have deleted_at
        table = 'event'
        total = count_rows(summary_conn, table, include_deleted=True)
        print(f"{table}: {total} active rows")

if __name__ == '__main__':
    main()