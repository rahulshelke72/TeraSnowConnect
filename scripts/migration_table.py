import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import time
from config.teradata import get_teradata_connection
from config.snowflake import get_snowflake_connection
from logs.migration_table_logs import  setup_logger

def get_total_row_count(teradata_conn, table_name, logger):
    """
    Get total row count of the source table
    """
    try:
        cursor = teradata_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = cursor.fetchone()[0]
        cursor.close()
        return total_rows
    except Exception as e:
        logger.error(f"Error getting row count: {e}")
        raise

def migrate_table_in_batches(
        source_table,
        target_table,
        batch_size=1000,
        sort_column='Customer_Index'
):
    """
    Migrate table from Teradata to Snowflake in batches
    """
    # Setup logging
    log_file = f'migration_{source_table}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    # Initialize connection and cursor variables to None to prevent reference before assignment
    teradata_conn = None
    snowflake_conn = None
    teradata_cursor = None
    snowflake_cursor = None

    try:
        # Load environment variables
        load_dotenv()

        # Establish connections
        teradata_conn = get_teradata_connection()
        snowflake_conn = get_snowflake_connection()

        # Get total row count
        total_rows = get_total_row_count(teradata_conn, source_table, logger)
        logger.info(f"Total rows to migrate: {total_rows}")

        # Cursor for Teradata and Snowflake
        teradata_cursor = teradata_conn.cursor()
        snowflake_cursor = snowflake_conn.cursor()

        # Track migration progress
        migrated_rows = 0
        start_time = time.time()

        # Migrate in batches
        while migrated_rows < total_rows:
            # Fetch batch from Teradata
            query = f"""
            SELECT * FROM {source_table}
            WHERE {sort_column} > {migrated_rows}
            ORDER BY {sort_column}
            FETCH FIRST {batch_size} ROWS ONLY
            """
            teradata_cursor.execute(query)
            batch_data = teradata_cursor.fetchall()

            if not batch_data:
                break

            # Prepare batch for Snowflake insertion
            columns = [desc[0] for desc in teradata_cursor.description]

            # Use Snowflake's bulk insert for performance
            insert_sql = f"""
            INSERT INTO {target_table} ({','.join(columns)})
            VALUES ({','.join(['%s'] * len(columns))})
            """
            snowflake_cursor.executemany(insert_sql, batch_data)

            # Update progress
            migrated_rows += len(batch_data)

            # Log progress
            logger.info(f"Migrated {migrated_rows}/{total_rows} rows ({migrated_rows / total_rows * 100:.2f}%)")

        # Final logging
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Migration completed in {total_time:.2f} seconds")
        logger.info(f"Average migration speed: {migrated_rows / total_time:.2f} rows/second")

    except Exception as e:
        if logger:
            logger.error(f"Migration Error: {e}")
        else:
            print(f"Migration Error: {e}")
    finally:
        # Close connections safely
        try:
            if teradata_cursor:
                teradata_cursor.close()
        except Exception:
            pass

        try:
            if snowflake_cursor:
                snowflake_cursor.close()
        except Exception:
            pass

        try:
            if teradata_conn:
                teradata_conn.close()
        except Exception:
            pass

        try:
            if snowflake_conn:
                snowflake_conn.close()
        except Exception:
            pass

# Main execution
if __name__ == "__main__":
    migrate_table_in_batches(
        source_table='sample_db.customers',
        target_table='customers',
        batch_size=100
    )