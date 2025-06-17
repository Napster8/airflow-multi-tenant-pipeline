import pandas as pd
import sqlite3
from airflow.decorators import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from utils.helper_functions import hash_value, anonymize_data

# 5 tenants
tenants = ['tenant1', 'tenant2', 'tenant3', 'tenant4', 'tenant5']

# Database path - use absolute path for Astro
DB_PATH = '/usr/local/airflow/data/sqlite.db'
DATA_DIR = '/usr/local/airflow/data/raw_data'

@dag(
    dag_id='pii_data_processing_pipeline',
    description='Process CSV data with PII protection and generate KPIs',
    schedule=timedelta(days=1),
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=2),  # Fixed: was 'delay'
    },
    tags=['02_silver_stage', 'pii-transformations'],
    start_date=datetime(2025, 6, 16),
)

def pii_data_processing_pipeline():
    
    @task
    def create_table(table_name: str):
        """Create SQLite table for tenant"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Create table with schema
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INTEGER,
                user_id TEXT,
                event_time TEXT,
                value REAL,
                country TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        print(f"Table {table_name} created successfully")
        return table_name

    @task
    def validate_csv_schema(csv_file: str):
        """Validate CSV file schema"""
        validation_list = ['id', 'user_id', 'event_time', 'value', 'country']  # Fixed typo
        column_names = pd.read_csv(csv_file, nrows=0).columns.tolist()
        
        if validation_list != column_names:
            raise ValueError(f"CSV file {csv_file} does not match required schema: {validation_list}")
        
        print(f"Schema validation passed for {csv_file}")
        return csv_file

    @task
    def load_and_pseudonymize_data(csv_file: str, table_name: str):
        """Load CSV and apply PII protection"""
        # Load CSV into DataFrame
        df = pd.read_csv(csv_file)
        
        # Pseudonymize user_id column using your helper function
        df['user_id'] = df['user_id'].apply(lambda x: hash_value(x, salt='shuru'))
        
        # Save to SQLite
        conn = sqlite3.connect(DB_PATH)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        
        print(f"Data from {csv_file} loaded and pseudonymized into {table_name}")
        return len(df)

    @task_group
    def tenant_pipeline(tenant: str):
        """Process data for a single tenant"""
        
        # Define file path
        csv_file_path = f'{DATA_DIR}/{tenant}.csv'
        
        # Create tasks
        start_task = EmptyOperator(task_id=f'start_{tenant}')
        end_task = EmptyOperator(task_id=f'end_{tenant}')
        
        # Main processing tasks
        table_created = create_table(tenant)
        schema_validated = validate_csv_schema(csv_file_path)
        data_loaded = load_and_pseudonymize_data(csv_file_path, tenant)
        
        # Define dependencies
        start_task >> table_created >> schema_validated >> data_loaded >> end_task
        
        return end_task

    # Create tenant pipelines dynamically
    tenant_tasks = []
    for tenant in tenants:
        tenant_task = tenant_pipeline(tenant)
        tenant_tasks.append(tenant_task)
    
    # Optional: Add overall start/end tasks
    overall_start = EmptyOperator(task_id='pipeline_start')
    overall_end = EmptyOperator(task_id='pipeline_end')
    
    # All tenant pipelines run in parallel
    overall_start >> tenant_tasks >> overall_end

# Instantiate the DAG
pii_processing_dag = pii_data_processing_pipeline()
