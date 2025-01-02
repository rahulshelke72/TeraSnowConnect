import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def get_sql_server_connection():
    """
    Creates and returns a SQL Server connection using environment variables.
    """
    connection_params = {
        'server': os.getenv('SQL_SERVER_HOST'),  # Fetch server from env
        'database': os.getenv('SQL_SERVER_DATABASE'),  # Fetch database from env
        'user': os.getenv('SQL_SERVER_USER'),  # Fetch user from env
        'password': os.getenv('SQL_SERVER_PASSWORD')  # Fetch password from env
    }

    # Validate required parameters
    if not all(connection_params.values()):
        raise ValueError("One or more connection parameters are missing. Check your environment variables.")

    # Create the connection string
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={connection_params['server']};"
        f"DATABASE={connection_params['database']};"
        f"UID={connection_params['user']};"
        f"PWD={connection_params['password']};"
    )

    # Establish and return the connection
    return pyodbc.connect(conn_str)


def show_tables():
    """
    Connects to SQL Server and shows the list of tables in the current database.
    """
    try:
        with get_sql_server_connection() as conn:
            with conn.cursor() as cursor:
                # Query to fetch tables in the current database
                query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """
                cursor.execute(query)
                tables = cursor.fetchall()

                print("Tables in the current database:")
                if tables:
                    for table in tables:
                        print(table[0])
                else:
                    print("No tables found in the current database.")

    except pyodbc.Error as e:
        print("Error accessing SQL Server:", e)


if __name__ == "__main__":
    show_tables()
