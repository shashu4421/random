from datetime import datetime, timedelta

# ... (your connection and variable logic)

today = datetime.now().date()
last_sunday = today - timedelta(days=today.weekday() + 1)
last_monday = last_sunday - timedelta(days=6)

start_date = last_monday.strftime('%Y-%m-%d 00:00:00')
end_date = (last_sunday + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

for table in tables:
    partition_table_name = f"{table}_{last_sunday.strftime('%Y_%m_%d')}"
    create_partition_query = f"""
    CREATE TABLE IF NOT EXISTS {partition_table_name}
    PARTITION OF {table}
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """
    cursor.execute(create_partition_query)
