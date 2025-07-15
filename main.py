"""
Extracts user module permissions from a nested JSON structure using custom inheritance rules.
Traverses each user's permission tree with conditional logic to flatten it into a user-module matrix.
The result is saved as a CSV file with users as rows and module permissions as boolean columns.
"""
import json
import pandas as pd


# Load the JSON Data
with open('data.json') as f:
    data = json.load(f)

# Store each user's module_id permissions
user_rows = []


# Checking if JSON contains one or multiple users and loads accordingly.
if "users" in data:
    users_data = data["users"]
else:
    users_data = [{"user_id": 1, "permissions": data["permissions"]}]


# DFS Funtion to traverse the nested JSON tree, but with conditional override

def dfs(node, parent_permission = None, permissions_dict = None):
    """
    Inheritance logic defined here:
    If parent permission is True, yet child's is False, respect child permission;
    If parent permission is False and child's is any, True or False, override it to False.
    Fills permissions_dict for the current user.
    """
    node_permission = node.get("hasPermission")

    # Applying the inheritance rules
    if parent_permission is True:
        if node_permission is None:
            current_permission = True
        else:
            current_permission = node_permission # Child explicitly false or true is respected

    elif parent_permission is False:
        current_permission = False # Parent being False overrides all child permissions to be False

    else:
        if node_permission is not None:
            current_permission = node_permission # No parent permission info, just use child's, if available, otherwise False
        else:
            current_permission = False

# Adding module_id and its permission to result
    module_id = node.get("module_id")
    if module_id:
        permissions_dict[module_id] = current_permission

    for child in node.get("children", []):
        dfs(child, current_permission, permissions_dict)

# Starting dfs for each user
for user in users_data:
    user_id = user.get("user_id", "unknown")
    permissions_dict = {}
    for module in user["permissions"]:
        dfs(module, None, permissions_dict)

# Adding user ID or Serial numbers as a column
    permissions_dict["user"] = user_id
    user_rows.append(permissions_dict)



# Creating dataframe and put user column first
df = pd.DataFrame(user_rows)
cols = df.columns.tolist()
cols = [c for c in cols if c != "user"]
df = df[["user"] + cols]

# Replacing missing permissions with False
df = df.fillna(False)


print(df)

df.to_csv("permissions.csv", index = False)

