import pandas as pd
import json
from datetime import datetime, timezone
import pyodbc

# Load JSON file
with open('data.json') as f:
    data = json.load(f)

# Recursive parser to collect modules with hasPermission
def extract_modules(user_id, modules, results, parent_permission=None):
    for module in modules:
        label = module.get("label")
        has_permission = module.get("hasPermission", parent_permission)
        if label is not None:
            results.append({
                "user_id": user_id,
                "module_name": label,
                "module_permission": bool(has_permission)
            })
        if "children" in module and isinstance(module["children"], list):
            extract_modules(user_id, module["children"], results, has_permission)

# Extract user/module/permission records
rows = []
for user in data["users"]:
    extract_modules(user["user_id"], user.get("permissions", []), rows)

# Create DataFrame
df = pd.DataFrame(rows)
df["updated_timestamp"] = datetime.now(timezone.utc)

# SQL Server connection
conn_str = (
    "Driver={SQL Server};"
    "Server=MSDBDEV\\SQL14;"
    "Database=RiskDashBoard;"
    "UID=fmdq;"
    "PWD=fmdq@123;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Create temporary staging table
cursor.execute("""
    IF OBJECT_ID('tempdb..#TempUserPermissions') IS NOT NULL DROP TABLE #TempUserPermissions;
    CREATE TABLE #TempUserPermissions (
        user_id NVARCHAR(255),
        module_name NVARCHAR(255),
        module_permission BIT,
        updated_timestamp DATETIMEOFFSET
    );
""")
conn.commit()

# Bulk insert DataFrame into staging table using fast_executemany
insert_temp = """
    INSERT INTO #TempUserPermissions (user_id, module_name, module_permission, updated_timestamp)
    VALUES (?, ?, ?, ?)
"""

cursor.fast_executemany = True
cursor.executemany(insert_temp, df.values.tolist())
conn.commit()

# Merge from temp table into target table
cursor.execute("""
    MERGE JC.Users_permissions AS Target
    USING #TempUserPermissions AS Source
    ON Target.user_id = Source.user_id AND Target.module_name = Source.module_name
    WHEN MATCHED AND Target.module_permission <> Source.module_permission THEN
        UPDATE SET 
            Target.module_permission = Source.module_permission,
            Target.updated_timestamp = Source.updated_timestamp
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (user_id, module_name, module_permission, updated_timestamp)
        VALUES (Source.user_id, Source.module_name, Source.module_permission, Source.updated_timestamp);
""")
conn.commit()

cursor.close()
conn.close()

print("Upload complete. Data merged using temp table.")

# Save result to CSV
df.to_csv("sqlpermissions.csv", index=False)
print(df.to_csv(index=False))
