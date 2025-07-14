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
        # Use module's own hasPermission if present, otherwise inherit from parent
        has_permission = module.get("hasPermission", parent_permission)
        # Only record if label is present (you can adjust this as needed)
        if label is not None:
            results.append({
                "user_id": user_id,
                "module_name": label,
                "module_permission": bool(has_permission)
            })
        # Recursively process children, passing down the current has_permission
        if "children" in module and isinstance(module["children"], list):
            extract_modules(user_id, module["children"], results, has_permission)

# Extract user/module/permission records
rows = []
for user in data["users"]:
    user_id = user["user_id"]
    extract_modules(user_id, user.get("permissions", []), rows, parent_permission=None)

# Create DataFrame
df = pd.DataFrame(rows)
df["updated_timestamp"] = datetime.now(timezone.utc)

print(df.head())  # Preview

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

# Insert all rows
insert_query = """
    INSERT INTO JC.Users_permissions (user_id, module_name, module_permission, updated_timestamp)
    VALUES (?, ?, ?, ?)
"""

for _, row in df.iterrows():
    cursor.execute("""
        MERGE JC.Users_permissions AS Target
        USING (
            SELECT ? AS user_id, ? AS module_name, ? AS module_permission, ? AS updated_timestamp
        ) AS Source
        ON Target.user_id = Source.user_id AND Target.module_name = Source.module_name
        WHEN MATCHED AND Target.module_permission <> Source.module_permission THEN
            UPDATE SET 
                Target.module_permission = Source.module_permission,
                Target.updated_timestamp = Source.updated_timestamp
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (user_id, module_name, module_permission, updated_timestamp)
            VALUES (Source.user_id, Source.module_name, Source.module_permission, Source.updated_timestamp);
    """, row["user_id"], row["module_name"], row["module_permission"], row["updated_timestamp"])

conn.commit()
cursor.close()
conn.close()

print("Upload complete. New data inserted into SQL Server.")
