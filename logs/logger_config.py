import os
import logging
from datetime import datetime


class MigrationLogger:
    @staticmethod
    def setup_logger(table_name, log_level=logging.INFO):
        """
        Set up a logger for the migration process.

        Args:
            table_name (str): Name of the table being migrated
            log_level (int): Logging level (default: logging.INFO)

        Returns:
            logging.Logger: Configured logger
        """
        # Create logs directory if it doesn't exist
        log_dir = os.path.dirname(os.path.abspath(__file__))
        os.makedirs(log_dir, exist_ok=True)

        # Create a unique log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(log_dir, f"{table_name}_migration_{timestamp}.log")

        # Configure logger
        logger = logging.getLogger(f'{table_name}_migration')
        logger.setLevel(log_level)
        logger.handlers.clear()  # Clear any existing handlers

        # Create file handler
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(log_level)

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    @staticmethod
    def log_migration_details(logger, migration_config):
        """
        Log detailed migration configuration.

        Args:
            logger (logging.Logger): Logger instance
            migration_config (dict): Migration configuration details
        """
        logger.info("Migration Configuration Details:")
        for key, value in migration_config.items():
            logger.info(f"{key}: {value}")
