import oracledb
import csv

# Database configuration
DB_CONFIG = {
    "host": "203.193.184.82",
    "port": "1523",
    "service_name": "ancldb",
    "user": "PROP",
    "password": "prop"
}

# Build the DSN
dsn = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['service_name']}"

try:
    # Connect to the Oracle DB
    connection = oracledb.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        dsn=dsn
    )
    print("✅ Connection successful!")

    cursor = connection.cursor()
    query = """
    SELECT * 
    FROM PROP.AOMS_PROPGENMOD_DET 
    NATURAL JOIN PROP.AOMS_PROPGENMOD_MST 
    NATURAL JOIN PROP.AOMS_PROPMODTYPE_MAS
    """
    cursor.execute(query)

    columns = [col[0] for col in cursor.description]
    all_rows = cursor.fetchall()

    # Deduplication check
    seen = set()
    duplicates = []
    test_rows = []

    for row in all_rows:
        row_tuple = tuple(row)
        if row_tuple in seen:
            duplicates.append(row)
        else:
            seen.add(row_tuple)

        # Check for case-insensitive "test" in any string column
        if any(isinstance(cell, str) and "test" in cell.lower() for cell in row):
            test_rows.append(row)

    print(f"\n📊 Total rows fetched: {len(all_rows)}")
    print(f"🔁 Duplicate rows found: {len(duplicates)}")
    print(f"🧪 Rows containing 'test': {len(test_rows)}")

    if duplicates:
        print("\n🚨 Duplicate records (first 5 shown):")
        for row in duplicates[:5]:
            print(dict(zip(columns, row)))

    if test_rows:
        print("\n🔍 Rows with 'test' (first 5 shown):")
        for row in test_rows[:5]:
            print(dict(zip(columns, row)))

        with open('test_rows.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Write headers
            writer.writerows(test_rows)

except oracledb.Error as e:
    print("❌ Connection failed:", e)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()
