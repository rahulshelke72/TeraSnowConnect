import pandas as pd
import logging
from config.snowflake import get_snowflake_connection
from config.sql_server import get_sql_server_connection
import os

# Define the file path for your Excel file
EXCEL_FILE_PATH = "E:/64_Square_llc_projects/Role Assignment - Streamlit/Role_Users.xlsx"
LOGS_DIR = "C:/Users/Lenovo/PycharmProjects/Teradata_Snowflake/logs"

# Create logs directory if it doesn't exist
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

# Set up logging configuration
log_filename = os.path.join(LOGS_DIR, "migration_roles_users.log")

# Create a custom logger
logger = logging.getLogger()

# Set the log level
logger.setLevel(logging.INFO)

# Create handlers
file_handler = logging.FileHandler(log_filename)
console_handler = logging.StreamHandler()

# Set the log level for handlers
file_handler.setLevel(logging.INFO)
console_handler.setLevel(logging.INFO)

# Create a formatter and set it for the handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def log_query_status(query_type, query, status, error_message=None):
    """
    Log the status of the query execution.
    """
    if status == 'success':
        logger.info(f"{query_type} query executed successfully: {query}")
    else:
        logger.error(f"Failed to execute {query_type} query: {query}")
        if error_message:
            logger.error(f"Error: {error_message}")

def run_snowflake_queries(excel_file_path, sheet_name="Test"):
    """
    Reads and executes Snowflake queries from the specified sheet in the Excel file.
    """
    # Read the specified sheet from the Excel file
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    # Clean column names to avoid issues with spaces or hidden characters
    df.columns = df.columns.str.strip()

    # Validate the necessary column
    if "Snowflake Query" not in df.columns:
        raise ValueError(f"The sheet '{sheet_name}' must contain a 'Snowflake Query' column.")

    # Connect to Snowflake
    snowflake_conn = get_snowflake_connection()

    try:
        with snowflake_conn.cursor() as cursor:
            for query in df["Snowflake Query"]:
                if pd.notna(query):  # Skip empty queries
                    try:
                        logger.info(f"Executing Snowflake Query: {query}")
                        cursor.execute(query)
                        log_query_status('Snowflake', query, 'success')
                    except Exception as e:
                        log_query_status('Snowflake', query, 'failure', str(e))
    finally:
        snowflake_conn.close()


def run_sql_server_queries(excel_file_path, sheet_name="Test"):
    """
    Reads and executes SQL Server queries from the specified sheet in the Excel file.
    """
    # Read the specified sheet from the Excel file
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    # Clean column names to avoid issues with spaces or hidden characters
    df.columns = df.columns.str.strip()

    # Validate the necessary column
    if "SQL Server Query" not in df.columns:
        raise ValueError(f"The sheet '{sheet_name}' must contain a 'SQL Server Query' column.")

    # Connect to SQL Server
    sql_server_conn = get_sql_server_connection()

    try:
        with sql_server_conn.cursor() as cursor:
            for query in df["SQL Server Query"]:
                if pd.notna(query):  # Skip empty queries
                    try:
                        logger.info(f"Executing SQL Server Query: {query}")
                        cursor.execute(query)
                        sql_server_conn.commit()
                        log_query_status('SQL Server', query, 'success')
                    except Exception as e:
                        log_query_status('SQL Server', query, 'failure', str(e))
    finally:
        sql_server_conn.close()


def display_snowflake_queries(excel_file_path, sheet_name="Test"):
    """
    Reads and displays Snowflake queries from the specified sheet in the Excel file.
    """
    # Read the specified sheet from the Excel file
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    # Clean column names to avoid issues with spaces or hidden characters
    df.columns = df.columns.str.strip()

    # Validate the necessary column
    if "Snowflake Query" not in df.columns:
        raise ValueError(f"The sheet '{sheet_name}' must contain a 'Snowflake Query' column.")

    # Display Snowflake queries
    print("Snowflake Queries from the 'Test' sheet:")
    for query in df["Snowflake Query"]:
        if pd.notna(query):  # Skip empty queries
            print(f"Snowflake Query: {query}")

def display_sql_server_queries(excel_file_path, sheet_name="Test"):
    """
    Reads and displays SQL Server queries from the specified sheet in the Excel file.
    """
    # Read the specified sheet from the Excel file
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

    # Clean column names to avoid issues with spaces or hidden characters
    df.columns = df.columns.str.strip()

    # Validate the necessary column
    if "SQL Server Query" not in df.columns:
        raise ValueError(f"The sheet '{sheet_name}' must contain a 'SQL Server Query' column.")

    # Display SQL Server queries
    print("SQL Server Queries from the 'Test' sheet:")
    for query in df["SQL Server Query"]:
        if pd.notna(query):  # Skip empty queries
            print(f"SQL Server Query: {query}")


if __name__ == "__main__":
    try:
        # Run Snowflake queries
        logger.info("Starting Snowflake queries execution...")
        run_snowflake_queries(EXCEL_FILE_PATH, sheet_name="Test")
        logger.info("Snowflake queries execution completed.")

        # Run SQL Server queries
        logger.info("\nStarting SQL Server queries execution...")
        run_sql_server_queries(EXCEL_FILE_PATH, sheet_name="Test")
        logger.info("SQL Server queries execution completed.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
