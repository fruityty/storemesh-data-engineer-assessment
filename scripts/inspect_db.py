import sqlite3

# Connect to the source SQLite database.
conn = sqlite3.connect("data/shopdata.db")
cursor = conn.cursor()

print("=== Views ===")

# Retrieve all views in the database.
cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='view'
""")

# Print the name of each view.
for row in cursor.fetchall():
    print(row[0])

# Views for the technical assessment.
views = [
    "vw_raw_customers",
    "vw_raw_orders",
    "vw_exchange_rates"
]

# Display the schema and sample data for each view.
for view in views:

    print("\n")
    print("=" * 60)
    print(view)
    print("=" * 60)

    # Retrieve column information for the current view.
    cursor.execute(f"PRAGMA table_info({view})")

    print("Columns")

    # Print column metadata:
    # (column_id, column_name, data_type, not_null, default_value, primary_key)
    for column in cursor.fetchall():
        print(column)

    # Display a few sample records to understand the data.
    cursor.execute(f"SELECT * FROM {view} LIMIT 5")

    print("\nSample Data")

    for row in cursor.fetchall():
        print(row)

# Close the database connection.
conn.close()