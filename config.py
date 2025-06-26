import oracledb

# Database configuration
DB_CONFIG = {
    "host": "203.193.184.82",
    "port": "1523",
    "service_name": "ancldb",
    "user": "PROP",
    "password": "prop"
}

# Build the DSN (Data Source Name)
dsn = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['service_name']}"

try:
    # Connect to the Oracle database
    connection = oracledb.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        dsn=dsn
    )

    print("✅ Connection successful!")

    # Run a test query
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM all_tables WHERE owner = 'PROP'")
    tables = cursor.fetchall()

    x = []

    print("📄 Tables in PROP schema:")
    for table in tables:
        x.append(table[0])

    print(len(x))

except oracledb.Error as e:
    print("❌ Connection failed:", e)

finally:
    # Clean up
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals():
        connection.close()