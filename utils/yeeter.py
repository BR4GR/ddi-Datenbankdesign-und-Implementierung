import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


class Yeeter:
    def __init__(
        self,
        log_filename="scraper.log",
        log_dir="logs",
        max_bytes=5000000,
        backup_count=5,
    ):
        self.log_dir = log_dir
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        log_filepath = os.path.join(self.log_dir, log_filename)

        # Setting up the logger
        self.logger = logging.getLogger("Yeeter")
        self.logger.setLevel(logging.DEBUG)

        # Log formatting
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        # File handler with log rotation
        file_handler = RotatingFileHandler(
            log_filepath, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # Add handlers to logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def yeet(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)

    def bureport(self, message: str):
        """Log a debug message."""
        self.logger.debug(message)

    def alarm(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)

    def clear_log_files(self) -> None:
        """Delete all log files in the log directory."""
        for log_file in os.listdir(self.log_dir):
            log_file_path = os.path.join(self.log_dir, log_file)
            if os.path.isfile(log_file_path):
                os.remove(log_file_path)  # Delete the file
                self.yeet(f"Deleted log file: {log_file_path}")
