from config.sql_server import  get_sql_server_connection
import pyodbc
from tabulate import tabulate

conn = get_sql_server_connection()

def show_roles_sql_server():
    """
    Connects to SQL Server and shows the list of roles in the current database excluding system roles.
    """
    try:
        with conn.cursor() as cursor:
            # Query to fetch roles in the current database Exclude system-defined roles
            query = """
            SELECT name AS RoleName
            FROM sys.database_principals
            WHERE type = 'R' -- Roles
              AND is_fixed_role = 0 -- Exclude system-defined roles
            ORDER BY name;
            """
            cursor.execute(query)
            tables = cursor.fetchall()

            print("Roles in the current database:")
            if tables:
                for table in tables:
                    print(table[0])
            else:
                print("No roles found in the current database.")

    except pyodbc.Error as e:
        print("Error accessing SQL Server:", e)


def show_users_sql_server():
        """
        Connects to SQL Server and shows the list of roles in the current database excluding system roles.
        """
        try:
            with conn.cursor() as cursor:
                # Query to fetch roles in the current database Exclude system-defined roles
                query = """
                SELECT name AS UserName
                FROM sys.database_principals
                WHERE type IN ('S', 'U') -- 'S' for SQL user, 'U' for Windows user/group
                  AND principal_id > 0 -- Exclude system objects
                ORDER BY name;
                """
                cursor.execute(query)
                tables = cursor.fetchall()

                print("Roles in the current database:")
                if tables:
                    for table in tables:
                        print(table[0])
                else:
                    print("No roles found in the current database.")

        except pyodbc.Error as e:
            print("Error accessing SQL Server:", e)

def show_databases_sql_server():
    """
    Connects to SQL Server and shows the list of databases.
    """
    try:
        with conn.cursor() as cursor:
            query = "SELECT name FROM sys.databases ORDER BY name;"
            cursor.execute(query)
            databases = cursor.fetchall()

            print("Databases in SQL Server:")
            if databases:
                print(tabulate(databases, headers=["Database Name"], tablefmt="grid"))
            else:
                print("No databases found.")
    except pyodbc.Error as e:
        print("Error accessing SQL Server:", e)


def show_schemas_sql_server(database_name):
    """
    Connects to SQL Server and shows the list of schemas in the specified database.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE [{database_name}]")  # Switch to the specified database
            query = "SELECT name FROM sys.schemas ORDER BY name;"
            cursor.execute(query)
            schemas = cursor.fetchall()

            print(f"Schemas in database '{database_name}':")
            if schemas:
                print(tabulate(schemas, headers=["Schema Name"], tablefmt="grid"))
            else:
                print(f"No schemas found in the database '{database_name}'.")
    except pyodbc.Error as e:
        print(f"Error accessing database '{database_name}':", e)


def show_tables_sql_server(database_name):
    """
    Connects to SQL Server and shows the list of tables in the specified database.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE [{database_name}]")  # Switch to the specified database
            query = """
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_SCHEMA, TABLE_NAME;
            """
            cursor.execute(query)
            tables = cursor.fetchall()

            print(f"Tables in database '{database_name}':")
            if tables:
                print(tabulate(tables, headers=["Schema", "Table Name"], tablefmt="grid"))
            else:
                print(f"No tables found in the database '{database_name}'.")
    except pyodbc.Error as e:
        print(f"Error accessing database '{database_name}':", e)

def show_logins_sql_server():
    """
    Connects to SQL Server and shows the list of logins.
    """
    try:
        with conn.cursor() as cursor:
            # Query to fetch logins from sys.server_principals
            query = """
            SELECT name AS LoginName,
                   type_desc AS LoginType,
                   is_disabled AS IsDisabled
            FROM sys.server_principals
            WHERE type IN ('S', 'U', 'G') -- S: SQL Login, U: Windows Login, G: Group
            ORDER BY name;
            """
            cursor.execute(query)
            logins = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]  # Get column names dynamically

            print("Logins in SQL Server:")
            if logins:
                print(tabulate(logins, headers=columns, tablefmt="grid"))
            else:
                print("No logins found.")
    except pyodbc.Error as e:
        print("Error accessing SQL Server:", e)

# show_databases()
# show_schemas("migration_data")
# show_tables("migration_data")
# show_logins()

def show_privileges_for_roles_sql_server(database_name):
    """
    Connects to SQL Server and shows privileges for roles in the specified database.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE [{database_name}]")  # Switch to the specified database
            query = """
            SELECT dp.name AS RoleName,
                   dp.type_desc AS RoleType,
                   p.permission_name AS Permission,
                   p.state_desc AS PermissionState,
                   o.name AS ObjectName,
                   o.type_desc AS ObjectType
            FROM sys.database_permissions p
            JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
            LEFT JOIN sys.objects o ON p.major_id = o.object_id
            WHERE dp.type = 'R' -- Roles
            ORDER BY dp.name, o.name, p.permission_name;
            """
            cursor.execute(query)
            privileges = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]  # Get column names dynamically

            print(f"Privileges for roles in database '{database_name}':")
            if privileges:
                print(tabulate(privileges, headers=columns, tablefmt="grid"))
            else:
                print(f"No privileges found for roles in the database '{database_name}'.")
    except pyodbc.Error as e:
        print(f"Error accessing database '{database_name}':", e)

def show_masking_policies_sql_server(database_name):
    """
    Connects to SQL Server and shows masking policies in the specified database.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE [{database_name}]")  # Switch to the specified database
            query = """
            SELECT t.name AS TableName,
                   c.name AS ColumnName,
                   mc.masking_function AS MaskingFunction
            FROM sys.masked_columns mc
            JOIN sys.columns c ON mc.column_id = c.column_id AND mc.object_id = c.object_id
            JOIN sys.tables t ON c.object_id = t.object_id
            WHERE mc.is_masked = 1
            ORDER BY t.name, c.name;
            """
            cursor.execute(query)
            policies = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]  # Get column names dynamically

            print(f"Masking Policies in database '{database_name}':")
            if policies:
                print(tabulate(policies, headers=columns, tablefmt="grid"))
            else:
                print(f"No masking policies found in the database '{database_name}'.")
    except pyodbc.Error as e:
        print(f"Error accessing database '{database_name}':", e)


def check_masking_policies_roles_sql_server(database_name):
    """
    Connects to SQL Server and checks which roles have permissions on columns with masking policies applied.
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"USE [{database_name}]")  # Switch to the specified database

            # Query to check roles and permissions on masked columns
            query = """
            SELECT 
                t.name AS TableName,
                c.name AS ColumnName,
                mc.masking_function AS MaskingFunction,
                dp.name AS RoleName,
                dp.type_desc AS RoleType,
                p.permission_name AS Permission,
                p.state_desc AS PermissionState
            FROM sys.masked_columns mc
            JOIN sys.columns c ON mc.column_id = c.column_id AND mc.object_id = c.object_id
            JOIN sys.tables t ON c.object_id = t.object_id
            JOIN sys.database_permissions p ON p.major_id = c.object_id
            JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
            WHERE mc.is_masked = 1 -- Masked columns only
              AND dp.type = 'R'  -- Roles only
            ORDER BY t.name, c.name, dp.name;
            """
            cursor.execute(query)
            policies_roles = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]  # Get column names dynamically

            print(f"Masking policies and role permissions in database '{database_name}':")
            if policies_roles:
                print(tabulate(policies_roles, headers=columns, tablefmt="grid"))
            else:
                print(f"No roles found with permissions on masked columns in the database '{database_name}'.")
    except pyodbc.Error as e:
        print(f"Error accessing database '{database_name}':", e)


# show_masking_policies("migration_data")
# show_privileges_for_roles("migration_data")
#check_masking_policies_roles_sql_server("migration_data")

