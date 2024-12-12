import os
import logging

# Setup logging BEFORE using logger
def setup_logger(log_path):
    """
    Set up a comprehensive logger with both file and console logging
    """
    # Ensure the directory for the log file exists
    log_dir = os.path.dirname(log_path)
    os.makedirs(log_dir, exist_ok=True)

    # Configure logger
    logger = logging.getLogger('migration_logger')
    logger.setLevel(logging.INFO)

    # Clear any existing handlers to prevent duplicate logs
    logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)

    # File Handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    file_handler.setFormatter(file_formatter)

    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
