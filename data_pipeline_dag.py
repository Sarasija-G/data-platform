"""
Data Pipeline DAG for Data Platform
This DAG orchestrates the complete data pipeline from raw data to business metrics.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import os

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-team@company.com']
}

# DAG definition
dag = DAG(
    'data_platform_pipeline',
    default_args=default_args,
    description='Complete data pipeline from raw to business metrics',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['data-platform', 'dbt', 'bigquery']
)

# Environment variables
PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'your-project-id')
DATASET_RAW = 'raw'
DATASET_STAGING = 'staging'
DATASET_CURATED = 'curated'
DATASET_MART = 'mart'

# Task 1: Check for new data files
check_new_data = FileSensor(
    task_id='check_new_data',
    filepath='/tmp/data/events/',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=300,
    dag=dag
)

# Task 2: Load raw data to BigQuery (if using batch ingestion)
load_raw_events = BigQueryInsertJobOperator(
    task_id='load_raw_events',
    configuration={
        "query": {
            "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET_RAW}.events`
                SELECT * FROM `{PROJECT_ID}.{DATASET_RAW}.events_staging`
                WHERE event_date = CURRENT_DATE() - 1
            """,
            "useLegacySql": False,
        }
    },
    dag=dag
)

# Task 3: Run dbt staging models
dbt_staging = BashOperator(
    task_id='dbt_staging',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt run --models staging --target prod
    """,
    dag=dag
)

# Task 4: Run dbt tests for staging
dbt_test_staging = BashOperator(
    task_id='dbt_test_staging',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt test --models staging --target prod
    """,
    dag=dag
)

# Task 5: Run dbt curated models
dbt_curated = BashOperator(
    task_id='dbt_curated',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt run --models curated --target prod
    """,
    dag=dag
)

# Task 6: Run dbt tests for curated
dbt_test_curated = BashOperator(
    task_id='dbt_test_curated',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt test --models curated --target prod
    """,
    dag=dag
)

# Task 7: Run dbt mart models
dbt_mart = BashOperator(
    task_id='dbt_mart',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt run --models mart --target prod
    """,
    dag=dag
)

# Task 8: Run dbt tests for mart
dbt_test_mart = BashOperator(
    task_id='dbt_test_mart',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt test --models mart --target prod
    """,
    dag=dag
)

# Task 9: Generate dbt documentation
dbt_docs = BashOperator(
    task_id='dbt_docs',
    bash_command=f"""
        cd /opt/airflow/dbt_project
        dbt docs generate --target prod
    """,
    dag=dag
)

# Task 10: Data quality validation
data_quality_check = BigQueryInsertJobOperator(
    task_id='data_quality_check',
    configuration={
        "query": {
            "query": f"""
                WITH quality_metrics AS (
                    SELECT
                        'events' as table_name,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validity_percentage
                    FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_events`
                    WHERE event_date = CURRENT_DATE() - 1
                    
                    UNION ALL
                    
                    SELECT
                        'users' as table_name,
                        COUNT(*) as total_rows,
                        SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) as valid_rows,
                        ROUND(SUM(CASE WHEN is_valid = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as validity_percentage
                    FROM `{PROJECT_ID}.{DATASET_STAGING}.stg_users`
                    WHERE user_date = CURRENT_DATE() - 1
                )
                SELECT * FROM quality_metrics
            """,
            "useLegacySql": False,
        }
    },
    dag=dag
)

# Task 11: Send success notification
send_success_notification = BashOperator(
    task_id='send_success_notification',
    bash_command='echo "Data pipeline completed successfully for $(date)"',
    dag=dag
)

# Task 12: Clean up temporary files
cleanup_temp_files = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='rm -rf /tmp/data/events/*',
    dag=dag
)

# Define task dependencies
check_new_data >> load_raw_events >> dbt_staging >> dbt_test_staging
dbt_test_staging >> dbt_curated >> dbt_test_curated
dbt_test_curated >> dbt_mart >> dbt_test_mart
dbt_test_mart >> dbt_docs >> data_quality_check
data_quality_check >> send_success_notification >> cleanup_temp_files
