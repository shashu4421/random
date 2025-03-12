from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Define default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# PostgreSQL Connection Details
PG_CONN = {
    "host": "your-postgres-host",
    "database": "your-database",
    "user": "your-username",
    "password": "your-password",
    "port": "5432"
}

# Function to create a partition for last week's data (Sunday to Saturday)
def create_last_week_partition():
    """Creates a partition for last week's data (Sunday to Saturday)."""

    # Compute the partition range (last Sunday's 00:00:00 to this Sunday's 00:00:00)
    today = datetime.utcnow()
    this_sunday = today - timedelta(days=today.weekday() + 1)  # Current Sunday (00:00:00)
    last_sunday = this_sunday - timedelta(days=7)  # Last Sunday (00:00:00)

    partition_name = f"my_table_{last_sunday.strftime('%Y_%m_%d')}"
    start_date = last_sunday.strftime('%Y-%m-%d 00:00:00')
    end_date = this_sunday.strftime('%Y-%m-%d 00:00:00')  # Current Sunday 00:00:00 (exclusive)

    create_partition_sql = f"""
        CREATE TABLE IF NOT EXISTS {partition_name} 
        PARTITION OF my_table 
        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """

    # Connect to PostgreSQL and execute the query
    try:
        conn = psycopg2.connect(**PG_CONN)
        cursor = conn.cursor()
        cursor.execute(create_partition_sql)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Partition {partition_name} created successfully for {start_date} to {end_date}.")
    except Exception as e:
        print(f"Error creating partition: {e}")

# Define the DAG
dag = DAG(
    'weekly_partition_creation',
    default_args=default_args,
    description='Creates weekly partitions for last week (Sunday to Saturday) every Sunday morning',
    schedule_interval='0 6 * * 0',  # Runs at 6 AM UTC every Sunday
    catchup=False
)

# Define the task
create_partition_task = PythonOperator(
    task_id='create_last_week_partition',
    python_callable=create_last_week_partition,
    dag=dag
)

# Set task dependencies
create_partition_task
