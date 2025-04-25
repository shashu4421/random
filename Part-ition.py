from datetime import datetime, timedelta

today = datetime.now().date()

# Find most recent Sunday (including today if it's Sunday)
this_sunday = today - timedelta(days=today.weekday() + 1 if today.weekday() != 6 else 0)

# Go back one week for the partition range
last_sunday = this_sunday - timedelta(days=7)

start_date = last_sunday.strftime('%Y-%m-%d 00:00:00')
end_date = this_sunday.strftime('%Y-%m-%d 00:00:00')  # end is exclusive


for table in tables:
    partition_table_name = f"{table}_{last_sunday.strftime('%Y_%m_%d')}"
    create_partition_query = f"""
    CREATE TABLE IF NOT EXISTS {partition_table_name}
    PARTITION OF {table}
    FOR VALUES FROM ('{start_date}') TO ('{end_date}');
    """
    cursor.execute(create_partition_query)
