from datetime import datetime, timedelta
import pandas as pd
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
import pytz

# ------------------------------
# Database config
# ------------------------------
DB_CONFIG = {
    "user": "airflow",
    "password": "airflow",
    "host": "postgres-dwh",
    "port": 5432,
    "db": "dwh"
}

RAW_JSON_PATH = "/opt/airflow/json_files"
TRANSFORMED_DATA_PATH = "/opt/airflow/transformed_data.json"  # temporary file between transform & load

# ------------------------------
# Engine
# ------------------------------
def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
    )

# ------------------------------
# DAG definition
# ------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='json_to_postgres_monthly',
    default_args=default_args,
    description='ETL pipeline for Lion Parcel JSON files with Jakarta timezone',
    start_date=datetime(2025, 10, 1),
    schedule='0 0 1 * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
)

# ------------------------------
# Tasks
# ------------------------------
def extract_json():
    engine = get_engine()
    files = [f for f in os.listdir(RAW_JSON_PATH) if f.lower().endswith(".json")]
    
    if not files:
        print("No JSON files found")
        return
    
    for file in files:
        with open(os.path.join(RAW_JSON_PATH, file), "r") as f:
            content = json.load(f)
        
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO raw_json_data(file_name, json_content) VALUES (:file, :content)"),
                {"file": file, "content": json.dumps(content)}
            )
    print(f"Loaded {len(files)} JSON files into raw_json_data table")

def transform_data():
    engine = get_engine()
    
    # Pull raw data
    with engine.connect() as conn:
        result = conn.execute(text("SELECT json_content FROM raw_json_data"))
        rows = result.fetchall()
    
    all_records = []
    tz = pytz.timezone("Asia/Jakarta")
    
    for row in rows:
        data = row[0]
        metric_results = data.get("MetricDataResults", [])
        messages = data.get("Messages", [])
        msg_text = json.dumps(messages)
        
        for metric in metric_results:
            id_val = metric.get("Id")
            timestamps = metric.get("Timestamps") or [""]  # handle blank
            values = metric.get("Values") or [0]  # handle blank
            
            # ensure same length
            if len(timestamps) != len(values):
                max_len = max(len(timestamps), len(values))
                timestamps += [""] * (max_len - len(timestamps))
                values += [0] * (max_len - len(values))
            
            for ts, val in zip(timestamps, values):
                if ts:
                    ts_dt = pd.to_datetime(ts)
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.tz_localize('UTC')
                    ts_dt = ts_dt.tz_convert(tz)
                    runtime_date = ts_dt.date().isoformat()  # convert to string
                else:
                    runtime_date = datetime.now(tz).date().isoformat()
                
                try:
                    load_time = float(val) / 1000 / 60
                except:
                    load_time = 0
                
                all_records.append({
                    "id": id_val,
                    "runtime_date": runtime_date,
                    "load_time": load_time,
                    "message": msg_text
                })

    
    # Save transformed data to temp file
    with open(TRANSFORMED_DATA_PATH, "w") as f:
        json.dump(all_records, f)
    
    print(f"Transformed {len(all_records)} records and saved to temp file")

def load_transformed():
    engine = get_engine()
    
    if not os.path.exists(TRANSFORMED_DATA_PATH):
        print("No transformed data found")
        return
    
    with open(TRANSFORMED_DATA_PATH, "r") as f:
        all_records = json.load(f)
    
    if not all_records:
        print("No records to load")
        return
    
    df = pd.DataFrame(all_records)
    
    # Aggregate average load_time per id and date
    df_agg = df.groupby(["id", "runtime_date"], as_index=False).agg({
        "load_time": "mean",
        "message": lambda x: "; ".join(x.unique())
    })
    
    # Upsert into transformed table
    with engine.begin() as conn:
        for _, row in df_agg.iterrows():
            conn.execute(text("""
                INSERT INTO transformed_metrics(id, runtime_date, load_time, message)
                VALUES (:id, :runtime_date, :load_time, :message)
                ON CONFLICT (id, runtime_date) DO UPDATE
                SET load_time = EXCLUDED.load_time,
                    message = EXCLUDED.message
            """), row.to_dict())
    
    print(f"Inserted/Updated {len(df_agg)} rows into transformed_metrics table")

# ------------------------------
# Operators
# ------------------------------
extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_json,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_task",
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id="load_task",
    python_callable=load_transformed,
    dag=dag
)

# ------------------------------
# Task ordering
# ------------------------------
extract_task >> transform_task >> load_task
