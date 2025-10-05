from datetime import datetime, timedelta
import pandas as pd
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

# ------------------------------
# Timezone
# ------------------------------
tz = pytz.timezone("Asia/Jakarta")

# ------------------------------
# Database configs (hardcoded)
# ------------------------------
SOURCE_DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres-source",
    "port": 5432,
    "db": "source"
}

DWH_DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres-dwh",
    "port": 5432,
    "db": "dwh"
}

# ------------------------------
# Engines
# ------------------------------
def get_source_engine():
    return create_engine(
        f"postgresql+psycopg2://{SOURCE_DB_CONFIG['user']}:{SOURCE_DB_CONFIG['password']}"
        f"@{SOURCE_DB_CONFIG['host']}:{SOURCE_DB_CONFIG['port']}/{SOURCE_DB_CONFIG['db']}"
    )

def get_dwh_engine():
    return create_engine(
        f"postgresql+psycopg2://{DWH_DB_CONFIG['user']}:{DWH_DB_CONFIG['password']}"
        f"@{DWH_DB_CONFIG['host']}:{DWH_DB_CONFIG['port']}/{DWH_DB_CONFIG['db']}"
    )

# ------------------------------
# DAG definition
# ------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_retail_transactions",
    default_args=default_args,
    description="ETL: source -> dwh for retail_transactions",
    start_date=datetime(2025, 10, 5, tzinfo=tz),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
)

# ------------------------------
# Tasks
# ------------------------------
def extract(**context):
    engine = get_source_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM public.retail_transactions", conn)

    if df.empty:
        print("No data in source table")
        context["ti"].xcom_push(key="file_path", value=None)
        return

    # Add snapshot timestamp
    df["snapshot_ts"] = datetime.now(tz)

    # Save temporarily to parquet file
    tmp_file = "/tmp/retail_transformed.parquet"
    df.to_parquet(tmp_file, index=False)
    print(f"Extracted {len(df)} rows")
    context["ti"].xcom_push(key="file_path", value=tmp_file)

def transform(**context):
    ti = context["ti"]
    tmp_file = ti.xcom_pull(task_ids="extract_task", key="file_path")
    if not tmp_file:
        print("No file to transform")
        context["ti"].xcom_push(key="file_path_transformed", value=None)
        return

    df = pd.read_parquet(tmp_file)

    # Cleaning rules
    df["last_status"] = df["last_status"].fillna("UNKNOWN").str.upper().replace({"DELIVRD": "DELIVERED"})
    df["pos_origin"] = df["pos_origin"].fillna("UNKNOWN").str.upper().replace({"JKT": "JAKARTA", "BDG": "BANDUNG"})
    df["pos_destination"] = df["pos_destination"].fillna("UNKNOWN").str.upper().replace({"JKT": "JAKARTA", "BDG": "BANDUNG"})

    now = datetime.now(tz)
    df["created_at"] = now
    df["updated_at"] = now

    tmp_file_transformed = "/tmp/retail_final.parquet"
    df.to_parquet(tmp_file_transformed, index=False)
    print(f"Transformed {len(df)} rows")
    context["ti"].xcom_push(key="file_path_transformed", value=tmp_file_transformed)

def load(**context):
    ti = context["ti"]
    tmp_file_transformed = ti.xcom_pull(task_ids="transform_task", key="file_path_transformed")
    if not tmp_file_transformed:
        print("No file to load")
        return

    df = pd.read_parquet(tmp_file_transformed)
    if df.empty:
        print("No rows to load")
        return

    dwh = get_dwh_engine()

    upsert_sql = """
        INSERT INTO public.retail_transactions
            (id, customer_id, last_status, pos_origin, pos_destination, created_at, updated_at, deleted_at)
        VALUES %s
        ON CONFLICT (id) DO UPDATE
        SET customer_id = EXCLUDED.customer_id,
            last_status = EXCLUDED.last_status,
            pos_origin = EXCLUDED.pos_origin,
            pos_destination = EXCLUDED.pos_destination,
            updated_at = EXCLUDED.updated_at,
            deleted_at = NULL;
    """

    values = [
        (
            row["id"], row["customer_id"], row["last_status"], row["pos_origin"],
            row["pos_destination"], row["created_at"], row["updated_at"], None
        )
        for _, row in df.iterrows()
    ]

    # Use raw_connection properly
    conn = dwh.raw_connection()
    try:
        cur = conn.cursor()
        execute_values(cur, upsert_sql, values)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    # Soft delete for missing IDs
    ids = tuple(df["id"].tolist())
    if ids:
        with dwh.begin() as conn:
            conn.execute(text("""
                UPDATE public.retail_transactions
                SET deleted_at = :now
                WHERE id NOT IN :ids AND deleted_at IS NULL
            """), {"now": datetime.now(tz), "ids": ids})

    print(f"Loaded {len(df)} rows to DWH")

# ------------------------------
# Operators
# ------------------------------
extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load,
    dag=dag,
)

# ------------------------------
# Task ordering
# ------------------------------
extract_task >> transform_task >> load_task
