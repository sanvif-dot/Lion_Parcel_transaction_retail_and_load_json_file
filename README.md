# Airflow ETL Project

This project contains an Airflow DAG for ETL from a source PostgreSQL database to a DWH PostgreSQL database and json files to DWH PostgreSQL.

## Architecture

transaction_retail
![My First Board](https://github.com/user-attachments/assets/74563900-fec0-4109-8953-675c3995ca45)

load_json_file
![My First Board (1)](https://github.com/user-attachments/assets/2d6b9d9a-8c5f-46d5-83c1-7f3db7b00bbb)


## Requirements

- Docker
- Docker Compose

## How to run

1. Clone this repository:

git clone https://github.com/sanvif-dot/Lion_Parcel_transaction_retail_and_load_json_file.git

cd Lion_Parcel_transaction_retail_and_load_json_file

2. Initialize Airflow
docker-compose up airflow-init

3. Start all services
docker-compose up -d

4. Create Table in database Source and DWH

## Database Source
CREATE TABLE IF NOT EXISTS public.retail_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    last_status VARCHAR(20),
    pos_origin VARCHAR(50),
    pos_destination VARCHAR(50)
);

## Insert Batch 1
INSERT INTO public.retail_transactions (id, customer_id, last_status, pos_origin, pos_destination)
VALUES
(1, 101, 'DELIVRD', 'JKT', 'BDG'),
(2, 102, 'PENDING', 'BDG', 'JKT'),
(3, 103, 'SHIPPED', 'JKT', 'JKT'),
(4, 104, 'DELIVRD', 'BDG', 'BDG');

## Insert Batch 2
INSERT INTO public.retail_transactions (id, customer_id, last_status, pos_origin, pos_destination)
VALUES
(1, 101, 'DELIVERED', 'JAKARTA', 'BANDUNG'), -- same as before
(2, 102, 'DELIVERED', 'BANDUNG', 'JAKARTA'), -- updated
(4, 104, 'DELIVERED', 'BANDUNG', 'BANDUNG'), -- same
(5, 105, 'PENDING', 'JKT', 'JKT');           -- new row

## Database DWH
CREATE TABLE IF NOT EXISTS public.retail_transactions (
    id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    last_status VARCHAR(20),
    pos_origin VARCHAR(50),
    pos_destination VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_json_data (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    json_content JSONB,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transformed_metrics (
    id VARCHAR(50),
    runtime_date DATE,
    load_time NUMERIC,
    message TEXT,
    PRIMARY KEY (id, runtime_date)
);

6. Open Airflow UI
Go to: http://localhost:8080

Username: airflow

Password: airflow


8. Trigger the DAG

In the Airflow UI, search for json_to_postgres_monthly and etl_retail_transactions.
Unpause the DAG.
Click Trigger DAG to run it manually.


etl_retail_transactions
<img width="2878" height="1070" alt="image" src="https://github.com/user-attachments/assets/3552d64d-23dc-40b7-b940-f5b95c2c068b" />

json_to_postgres_monthly
<img width="2879" height="1221" alt="image" src="https://github.com/user-attachments/assets/fb2702d8-fa19-4a98-95d7-20a7df4fb15f" />

8. Scenario
   ## etl_retail_transactions
  - Insert Batch 1 data into Source Table 1, then run the DAG.
    The Data Warehouse (DWH) will now contain 4 cleaned rows (typos corrected).
    
    Source Data
    
    This data have some typo in last_status and pos_origin
    <img width="1435" height="530" alt="image" src="https://github.com/user-attachments/assets/de02240e-eb68-4ef7-8f70-c7666adeb0dc" />

    Target table
    
    Data in this table has been cleansed
    <img width="2197" height="504" alt="image" src="https://github.com/user-attachments/assets/deb94347-f58c-4c31-91c3-39d2e264ed31" />

  - Insert Batch 2 data into the Source Table, then run the DAG again.
    
    The DWH will now demonstrate the Change Data Capture (CDC) concept:
    
      created_at → shows when a new row is inserted.
    
      updated_at → shows when an existing row is updated.
    
      deleted_at → shows when a row is deleted in the source.
    
    
    Source Data
    
    This data I add row same as before, updated column, and delete 1 rows
    <img width="1413" height="492" alt="image" src="https://github.com/user-attachments/assets/4c3fc83a-92f2-4ea1-89b8-a876d0d68e0c" />
    
    Target table
    
    Data in this table will have cdc concept (created_at, updated_at, deleted_at)
    <img width="2201" height="533" alt="image" src="https://github.com/user-attachments/assets/0904a954-b986-4c91-99cf-455c4fea58b5" />



    ## json_to_postgres_monthly
    - Reads JSON files from your local machine.
    - Loads them into a raw JSON table.
    - Cleanses/transforms the data into a transformed table.
    - Runs automatically on the first day of every month.

    source data
    
    there are 8 json files
    
    <img width="1790" height="739" alt="image" src="https://github.com/user-attachments/assets/1ace64a9-89b4-44f6-bb32-08abb068a3c9" />

    target table
    
    column runtime_date transformed into timestamp date and column load_time transformed from miliseconds to minutes
    
    <img width="1368" height="702" alt="image" src="https://github.com/user-attachments/assets/c5276c97-e649-475d-be9f-78634d5676e8" />

