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

conn.commit()

# Bulk insert into staging table
insert_stmt = """
    INSERT INTO JC.Staging_User_permissions (user_id, module_name, module_permission, updated_timestamp)
    VALUES (?, ?, ?, ?)
"""

cursor.fast_executemany = True
cursor.executemany(insert_stmt, df.values.tolist())
conn.commit()

# Calling the stored procedure to Merge into target table
cursor.execute("EXEC JC.MergeUserPermissions;")
conn.commit()

cursor.close()
conn.close()

print("Upload complete. Data merged via Stored Procedure.")


# Save result to CSV
df.to_csv("sqlpermissions.csv", index=False)
print(df.to_csv(index=False))
