import os
import logging
from operations.sql_server_operations import (
    show_roles_sql_server,
    show_users_sql_server,
    show_logins_sql_server
)
from operations.snowflake_operations import (
    show_roles_snowflake,
    show_users_snowflake
)
from config.sql_server import get_sql_server_connection
from config.snowflake import get_snowflake_connection

# Ensure the 'scripts/logs' folder exists
log_folder = 'C:/Users/Lenovo/PycharmProjects/Teradata_Snowflake/scripts/logs'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Define the log file path
log_file_path = os.path.join(log_folder, 'migration_log.log')

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),  # Log to file
        logging.StreamHandler()  # Log to console
    ]
)

logger = logging.getLogger(__name__)


def get_sql_server_roles(sql_conn, database_name):
    """
    Fetch all roles and their privileges from SQL Server
    """
    try:
        with sql_conn.cursor() as cursor:
            # Get roles
            cursor.execute(f"USE [{database_name}]")
            query = """
            SELECT 
                dp.name AS role_name,
                dp.type_desc AS role_type,
                OBJECT_SCHEMA_NAME(p.major_id) as schema_name,
                OBJECT_NAME(p.major_id) as object_name,
                p.permission_name,
                p.state_desc
            FROM sys.database_principals dp
            LEFT JOIN sys.database_permissions p 
                ON p.grantee_principal_id = dp.principal_id
            WHERE dp.type = 'R' 
                AND dp.is_fixed_role = 0
            ORDER BY dp.name;
            """
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error fetching SQL Server roles: {str(e)}")
        raise


def create_snowflake_role(snow_conn, role_name):
    """
    Create a role in Snowflake
    """
    try:
        with snow_conn.cursor() as cursor:
            # Check if role exists
            cursor.execute(f"SHOW ROLES LIKE '{role_name}'")
            if not cursor.fetchone():
                cursor.execute(f"CREATE ROLE IF NOT EXISTS {role_name}")
                logger.info(f"Created role: {role_name}")
            else:
                logger.info(f"Role already exists: {role_name}")
    except Exception as e:
        logger.error(f"Error creating Snowflake role {role_name}: {str(e)}")
        raise


def map_sql_server_to_snowflake_privileges(permission_name, state_desc):
    """
    Map SQL Server permissions to Snowflake privileges
    """
    privilege_mapping = {
        'SELECT': 'SELECT',
        'INSERT': 'INSERT',
        'UPDATE': 'UPDATE',
        'DELETE': 'DELETE',
        'EXECUTE': 'EXECUTE',
        'CONTROL': 'OWNERSHIP',  # Most privileged access in SQL Server
    }

    # Default to the same privilege name if no mapping exists
    privilege = privilege_mapping.get(permission_name, permission_name)

    # Handle GRANT/DENY/REVOKE states
    if state_desc == 'DENY':
        return f"REVOKE {privilege}"
    return f"GRANT {privilege}"


def grant_snowflake_privileges(snow_conn, role_name, schema_name, object_name, privilege):
    """
    Grant privileges to a role in Snowflake
    """
    try:
        with snow_conn.cursor() as cursor:
            if schema_name and object_name:
                grant_sql = f"{privilege} ON {schema_name}.{object_name} TO ROLE {role_name}"
            else:
                grant_sql = f"{privilege} ON ALL TABLES IN SCHEMA {schema_name} TO ROLE {role_name}"

            cursor.execute(grant_sql)
            logger.info(f"Granted privilege: {grant_sql}")
    except Exception as e:
        logger.error(f"Error granting privilege to role {role_name}: {str(e)}")
        raise


def migrate_roles(source_db_name):
    """
    Main function to migrate roles from SQL Server to Snowflake
    """
    logger.info("Starting role migration process...")

    # Get connections
    sql_conn = get_sql_server_connection()
    snow_conn = get_snowflake_connection()

    try:
        # Get all SQL Server roles and their privileges
        sql_server_roles = get_sql_server_roles(sql_conn, source_db_name)

        # Process each role
        current_role = None
        for role_data in sql_server_roles:
            role_name, role_type, schema_name, object_name, permission_name, state_desc = role_data

            # Create role in Snowflake if it's a new role
            if current_role != role_name:
                create_snowflake_role(snow_conn, role_name)
                current_role = role_name

            # Skip if no permissions are assigned
            if not permission_name:
                continue

            # Map and grant privileges
            snowflake_privilege = map_sql_server_to_snowflake_privileges(
                permission_name,
                state_desc
            )

            # Grant privileges to Snowflake
            grant_snowflake_privileges(
                snow_conn,
                role_name,
                schema_name,  # Use schema_name from the query results
                object_name,
                snowflake_privilege
            )

        logger.info("Role migration completed successfully")

    except Exception as e:
        logger.error(f"Error during role migration: {str(e)}")
        raise
    finally:
        sql_conn.close()
        snow_conn.close()


if __name__ == "__main__":
    # Example usage
    migrate_roles(
        source_db_name="migration_data"  # No need for target_schema here
    )
