import sqlite3

conn = sqlite3.connect("data/shopdata.db")
cursor = conn.cursor()

print("=== Views ===")

cursor.execute("""
SELECT name
FROM sqlite_master
WHERE type='view'
""")

for row in cursor.fetchall():
    print(row[0])

views = [
    "vw_raw_customers",
    "vw_raw_orders",
    "vw_exchange_rates"
]

for view in views:

    print("\n")
    print("=" * 60)
    print(view)
    print("=" * 60)

    cursor.execute(f"PRAGMA table_info({view})")

    print("Columns")

    for column in cursor.fetchall():
        print(column)

    cursor.execute(f"SELECT * FROM {view} LIMIT 5")

    print("\nSample Data")

    for row in cursor.fetchall():
        print(row)

conn.close()