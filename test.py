import cx_Oracle
import csv

# Oracle connection setup
dsn = cx_Oracle.makedsn("203.193.184.82", 1523, service_name="ancldb")  # Replace with actual host/service
connection = cx_Oracle.connect("PROP", "prop", dsn)    # Replace with your credentials
cursor = connection.cursor()

# Get all AOMS* tables
cursor.execute("""
    SELECT table_name 
    FROM all_tables 
    WHERE owner = 'PROP' 
    AND table_name LIKE 'AOMS%'
""")
aoms_tables = [row[0] for row in cursor.fetchall()]

# Prepare CSV output
output_file = "aoms_schema.csv"
with open(output_file, "w", newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_LENGTH"])

    for table in aoms_tables:
        query = f"""
            SELECT column_name, data_type, data_length 
            FROM all_tab_columns 
            WHERE owner = 'PROP' 
            AND table_name = '{table}'
        """
        cursor.execute(query)
        
        for column_name, data_type, data_length in cursor.fetchall():
            writer.writerow([table, column_name, data_type, data_length])


print(f"Schema written to: {output_file}")

# Clean up
cursor.close()
connection.close()
