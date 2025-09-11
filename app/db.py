import os
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import UniqueViolation, ForeignKeyViolation

from dotenv import load_dotenv
load_dotenv()

def get_db_config():
    return {
        "host": os.getenv("POSTGRESQL_HOST"),
        "database": os.getenv("POSTGRESQL_DATABASE"),
        "user": os.getenv("POSTGRESQL_USER"),
        "password": os.getenv("POSTGRESQL_PASSWORD"),
        "port": os.getenv("POSTGRESQL_PORT", 5432),
        "sslmode": "require",
        "options": f"endpoint={os.getenv('POSTGRESQL_ENDPOINT')}"
    }

def get_conn():
    return psycopg2.connect(**get_db_config())


def run_query(query, params=None, fetch_one=False, fetch_all=False):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())

                if fetch_one:
                    row = cur.fetchone()
                    if not row:
                        return None
                    cols = [desc[0] for desc in cur.description]
                    return dict(zip(cols, row))

                if fetch_all:
                    rows = cur.fetchall()
                    cols = [desc[0] for desc in cur.description]
                    return [dict(zip(cols, r)) for r in rows]

                conn.commit()
                return None
    except UniqueViolation:
        raise ValueError(f"Already exists")
    except ForeignKeyViolation:
        raise ValueError(f"Invalid foreign key")
    except Exception as e:
        raise RuntimeError(f"Failed to insert: {str(e)}")

def run_many_query(query: str, data: list, page_size=1000):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            print(f"Preparing to insert {len(data)} rows using execute_values...")
            execute_values(
                cur,
                query,
                data,
                page_size=page_size
            )
            conn.commit()
            print(f"Successfully inserted {len(data)} rows.")
            
    except UniqueViolation:
        print("Database error: Record already exists (UniqueViolation).")
        if conn:
            conn.rollback()
    except ForeignKeyViolation:
        print("Database error: Invalid foreign key.")
        if conn:
            conn.rollback()
    except Exception as error:
        print(f"An unexpected database error occurred: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")