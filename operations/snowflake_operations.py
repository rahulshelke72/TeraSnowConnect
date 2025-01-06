import snowflake.connector
from config.snowflake import get_snowflake_connection
from tabulate import tabulate

def show_databases_snowflake():
    """
    Fetches and returns a list of all databases in Snowflake.
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            return [db[1] for db in databases]  # Snowflake returns database names in the second column
    except snowflake.connector.Error as e:
        print("Error fetching databases from Snowflake:", e)
        return []


def show_tables_snowflake(database_name):
    """
    Fetches and returns a list of all tables in a specific Snowflake database.
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SHOW TABLES IN DATABASE {database_name}")
            tables = cursor.fetchall()
            return [table[1] for table in tables]  # Table names are in the second column
    except snowflake.connector.Error as e:
        print(f"Error fetching tables for database '{database_name}':", e)
        return []


def show_roles_snowflake():
    """
    Fetches and returns a list of roles in Snowflake.
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW ROLES")
            roles = cursor.fetchall()
            return [role[1] for role in roles]  # Role names are in the second column
    except snowflake.connector.Error as e:
        print("Error fetching roles from Snowflake:", e)
        return []


def show_users_snowflake():
    """
    Fetches and returns a list of users in Snowflake.
    """
    try:
        with get_snowflake_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW USERS")
            users = cursor.fetchall()
            return [user[0] for user in users]  # User names are in the second column
    except snowflake.connector.Error as e:
        print("Error fetching users from Snowflake:", e)
        return []


def print_results_in_table(title, data):
    """
    Prints data in a tabular format with a title.
    """
    print(f"\n{title}")
    print(tabulate([[item] for item in data], headers=["Name"], tablefmt="grid"))


# Example Usage
if __name__ == "__main__":
    # Show and print databases
    databases = show_databases_snowflake()
    print_results_in_table("Databases", databases)

    # Replace with an actual database name to test show_tables
    database_name = "RAHUL"  # You can change this to any valid Snowflake database
    tables = show_tables_snowflake(database_name)
    print_results_in_table(f"Tables in {database_name}", tables)

    # Show and print roles
    roles = show_roles_snowflake()
    print_results_in_table("Roles", roles)

    # Show and print users
    users = show_users_snowflake()
    print_results_in_table("Users", users)

