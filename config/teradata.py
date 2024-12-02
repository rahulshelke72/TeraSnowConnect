import os
import teradatasql
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()


def get_teradata_connection():
    """
    Creates and returns a Teradata connection using environment variables.
    """
    connection_params = {
        'host': os.getenv('TERADATA_HOST'),  # Fetch host from env
        'user': os.getenv('TERADATA_USER'),  # Fetch user from env
        'password': os.getenv('TERADATA_PASSWORD')  # Fetch password from env
    }

    # Validate required parameters
    if not all(connection_params.values()):
        raise ValueError("One or more connection parameters are missing. Check your environment variables.")

    # Establish and return the connection
    return teradatasql.connect(**connection_params)


def show_tables(database_name):
    """
    Connects to Teradata and shows the list of tables in a specific database.
    """
    try:
        with get_teradata_connection() as conn:
            with conn.cursor() as cursor:
                # Query to fetch tables in the specified database
                query = f"""
                SELECT TableName
                FROM DBC.TablesV
                WHERE DatabaseName = '{database_name}'
                ORDER BY TableName
                """
                cursor.execute(query)
                tables = cursor.fetchall()

                print(f"Tables in database '{database_name}':")
                if tables:
                    for table in tables:
                        print(table[0])
                else:
                    print(f"No tables found in database '{database_name}'.")

    except teradatasql.Error as e:
        print("Error accessing Teradata:", e)


if __name__ == "__main__":
    # Specify the database name
    database_name = input("Enter the database name: ").strip()
    show_tables(database_name)