import os
import logging
from dotenv import load_dotenv
from datetime import datetime
import time
from config.teradata import get_teradata_connection
from config.snowflake import get_snowflake_connection
from logs.migration_table_logs import setup_logger


def get_sample_row_count(teradata_conn, table_name, sample_size, logger):
    """
    Get sample row count for testing migration
    """
    try:
        cursor = teradata_conn.cursor()
        query = f"""
        SELECT COUNT(*) 
        FROM (
            SELECT * FROM {table_name}
            SAMPLE {sample_size} ROWS
        ) AS sample_data
        """
        cursor.execute(query)
        total_rows = cursor.fetchone()[0]
        cursor.close()
        return total_rows
    except Exception as e:
        logger.error(f"Error getting sample row count: {e}")
        raise


def migrate_table_in_test_batches(
        source_table,
        target_table,
        test_rows=10,
        sort_column='Customer_Index'
):
    """
    Migrate a sample of rows from Teradata to Snowflake for testing
    """
    # Determine the log directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.normpath(os.path.join(current_dir, '../logs'))

    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)

    # Setup logging with full path
    log_file = f'migration_{source_table}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = os.path.join(log_dir, log_file)
    logger = setup_logger(log_path)

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

        # Get sample row count
        total_rows = get_sample_row_count(teradata_conn, source_table, test_rows, logger)
        logger.info(f"Total sample rows to migrate: {total_rows}")

        # Cursor for Teradata and Snowflake
        teradata_cursor = teradata_conn.cursor()
        snowflake_cursor = snowflake_conn.cursor()

        # Fetch sample data from Teradata
        query = f"""
        SELECT TOP {test_rows} * FROM {source_table}
        ORDER BY {sort_column}
        """
        teradata_cursor.execute(query)
        sample_data = teradata_cursor.fetchall()

        # Prepare sample data for Snowflake insertion
        columns = [desc[0] for desc in teradata_cursor.description]

        # Use Snowflake's bulk insert for performance
        insert_sql = f"""
        INSERT INTO {target_table} ({','.join(columns)})
        VALUES ({','.join(['%s'] * len(columns))})
        """

        # Execute batch insert
        snowflake_cursor.executemany(insert_sql, sample_data)

        # Log results
        logger.info(f"Migrated {len(sample_data)} sample rows")
        logger.info(f"Sample data migration completed")
        logger.info(f"Log file saved at: {log_path}")

    except Exception as e:
        if logger:
            logger.error(f"Migration Test Error: {e}")
        else:
            print(f"Migration Test Error: {e}")
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
    migrate_table_in_test_batches(
        source_table='sample_db.customers',
        target_table='customers',
        test_rows=10  # Specify number of rows to test
    )