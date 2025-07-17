import pyodbc

try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=34.58.50.83,1433;'
        'DATABASE=eodhd_data;'
        'UID=sqlserver;PWD=EvokeIntern@2025;',
        'Encrypt=no;',
        'TrustServerCertificate=yes;',
        timeout=5
    )
    print("✅ Connected to SQL Server!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:")
    print(e)
