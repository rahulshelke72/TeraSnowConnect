from operations.teradata_operations import show_databases as td_show_databases, \
    show_tables as td_show_tables, show_roles as td_show_roles, show_users as td_show_users

from operations.snowflake_operations import show_databases as sf_show_databases, \
    show_tables as sf_show_tables, show_roles as sf_show_roles, show_users as sf_show_users


def test_teradata_operations():
    print("\nTesting Teradata Operations:")

    # Test: Show Databases
    print("Fetching Teradata databases...")
    teradata_databases = td_show_databases()
    print(f"Databases: {teradata_databases}")

    if teradata_databases:
        # Test: Show Tables
        print(f"Fetching tables for database: {teradata_databases[0]}...")
        teradata_tables = td_show_tables(teradata_databases[0])
        print(f"Tables: {teradata_tables}")
    else:
        print("No databases available in Teradata for further testing.")

    # Test: Show Roles
    print("Fetching Teradata roles...")
    teradata_roles = td_show_roles()
    print(f"Roles: {teradata_roles}")

    # Test: Show Users
    print("Fetching Teradata users...")
    teradata_users = td_show_users()
    print(f"Users: {teradata_users}")


def test_snowflake_operations():
    print("\nTesting Snowflake Operations:")

    # Test: Show Databases
    print("Fetching Snowflake databases...")
    snowflake_databases = sf_show_databases()
    print(f"Databases: {snowflake_databases}")

    if snowflake_databases:
        # Test: Show Tables
        print(f"Fetching tables for database: {snowflake_databases[0]}...")
        snowflake_tables = sf_show_tables(snowflake_databases[0])
        print(f"Tables: {snowflake_tables}")
    else:
        print("No databases available in Snowflake for further testing.")

    # Test: Show Roles
    print("Fetching Snowflake roles...")
    snowflake_roles = sf_show_roles()
    print(f"Roles: {snowflake_roles}")

    # Test: Show Users
    print("Fetching Snowflake users...")
    snowflake_users = sf_show_users()
    print(f"Users: {snowflake_users}")


if __name__ == "__main__":
    print("Starting Migration Tests...")

    # Test Teradata Operations
    test_teradata_operations()

    # Test Snowflake Operations
    #test_snowflake_operations()

    print("\nMigration Tests Completed.")
