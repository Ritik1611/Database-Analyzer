import sqlite3
import pandas as pd

def copy_by_id_range(source_db, target_db, id_column='id', start_id=1000, end_id=2000):
    source_conn = sqlite3.connect(source_db)
    target_conn = sqlite3.connect(target_db)
    
    # Get all table names
    cursor = source_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f"Processing {table_name}...")
        
        # Check if the table has the specified ID column
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [col[1] for col in cursor.fetchall()]
        
        if id_column not in columns:
            print(f"Skipping {table_name} - no {id_column} column found")
            continue
        
        # Get table schema
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_statement = cursor.fetchone()[0]
        target_conn.execute(create_statement)
        
        # Copy records in ID range
        df = pd.read_sql_query(f"""
            SELECT * FROM {table_name} 
            WHERE {id_column} BETWEEN {start_id} AND {end_id}
            ORDER BY {id_column}
        """, source_conn)
        
        if not df.empty:
            df.to_sql(table_name, target_conn, if_exists='append', index=False)
            print(f"Copied {len(df)} records from {table_name}")
        else:
            print(f"No records found in range for {table_name}")
    
    source_conn.close()
    target_conn.close()
    print(f"Created {target_db} with ID range {start_id}-{end_id}")

# Usage
copy_by_id_range('agent_logs.db', 'agent_logs_237_477.db', 'id', 2777, 3611)