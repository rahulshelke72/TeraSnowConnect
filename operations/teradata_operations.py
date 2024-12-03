from tabulate import tabulate
from config.teradata import get_teradata_connection
import teradatasql

def show_databases():
    """
    Fetches and returns a list of all databases in Teradata.
    """
    try:
        with get_teradata_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT DatabaseName FROM DBC.DatabasesV ORDER BY DatabaseName"
                cursor.execute(query)
                databases = cursor.fetchall()
                return [db[0] for db in databases]
    except teradatasql.Error as e:
        print("Error fetching databases from Teradata:", e)
        return []


def show_tables(database_name):
    """
    Fetches and returns a list of all tables in a specific Teradata database.
    """
    try:
        with get_teradata_connection() as conn:
            with conn.cursor() as cursor:
                query = f"""
                SELECT TableName
                FROM DBC.TablesV
                WHERE DatabaseName = '{database_name}'
                ORDER BY TableName
                """
                cursor.execute(query)
                tables = cursor.fetchall()
                return [table[0] for table in tables]
    except teradatasql.Error as e:
        print(f"Error fetching tables for database '{database_name}':", e)
        return []


def show_roles():
    """
    Fetches and returns a list of roles in Teradata.
    """
    try:
        with get_teradata_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT RoleName FROM DBC.RolesV ORDER BY RoleName"
                cursor.execute(query)
                roles = cursor.fetchall()
                return [role[0] for role in roles]
    except teradatasql.Error as e:
        print("Error fetching roles from Teradata:", e)
        return []


def show_users():
    """
    Fetches and returns a list of users in Teradata.
    """
    try:
        with get_teradata_connection() as conn:
            with conn.cursor() as cursor:
                query = "SELECT UserName FROM DBC.UsersV ORDER BY UserName"
                cursor.execute(query)
                users = cursor.fetchall()
                return [user[0] for user in users]
    except teradatasql.Error as e:
        print("Error fetching users from Teradata:", e)
        return []


def print_results_in_table(title, data):
    """
    Prints data in a tabular format with a title.
    """
    print(f"\n{title}")
    print(tabulate([[item] for item in data], headers=["Name"], tablefmt="grid"))


# Example Usage
if __name__ == "__main__":
    databases = show_databases()
    print_results_in_table("Databases", databases)

    database_name = "sample_db"  # Replace with an actual database name
    tables = show_tables(database_name)
    print_results_in_table(f"Tables in {database_name}", tables)

    roles = show_roles()
    print_results_in_table("Roles", roles)

    users = show_users()
    print_results_in_table("Users", users)
