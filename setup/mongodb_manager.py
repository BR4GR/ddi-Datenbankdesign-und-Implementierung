import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import BulkWriteError, ConnectionFailure

from setup.database_config import DatabaseConfig
from setup.dataloader import DataLoader

config = DatabaseConfig()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MongoDBManager:
    """Handles MongoDB connections and operations."""

    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        """Establish connection to MongoDB."""
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ismaster")
            self.db = self.client[self.db_name]
            logger.info(f"Connected to MongoDB: {self.db_name}")
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def disconnect(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def clear_collections(self, collections: List[str]):
        """Clear specified collections."""
        for collection in collections:
            result = self.db[collection].delete_many({})
            logger.info(f"Cleared {result.deleted_count} documents from {collection}")

    def insert_batch(
        self, collection: str, documents: List[Dict], batch_size: int = 1000
    ):
        """Insert documents in batches."""
        total_inserted = 0
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            try:
                result = self.db[collection].insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
                logger.info(
                    f"Inserted batch {i//batch_size + 1}: {len(batch)} documents into {collection}"
                )
            except BulkWriteError as e:
                logger.error(f"Bulk write error in batch {i//batch_size + 1}: {e}")

        logger.info(f"Total inserted into {collection}: {total_inserted}")
        return total_inserted
