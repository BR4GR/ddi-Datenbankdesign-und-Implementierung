import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from psycopg2.extras import RealDictCursor

from models.product_factory import ProductFactory
from setup.database_config import DatabaseConfig
from setup.dataloader import DataLoader
from setup.postgresql_manager import PostgreSQLManager

config = DatabaseConfig()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ProductProcessor:
    """Handles product processing and database insertion."""

    def __init__(self, db_manager: PostgreSQLManager, product_factory: ProductFactory):
        self.db_manager = db_manager
        self.product_factory = product_factory

    def process_products(
        self, documents: List[Dict], dbname: str, batch_size: int = 100
    ):
        """Process products in batches with proper transaction handling."""
        if not documents:
            logger.warning("No documents to process")
            return

        total_processed = 0
        total_failed = 0

        try:
            with self.db_manager.connect(dbname) as conn:
                conn.autocommit = False

                for i in range(0, len(documents), batch_size):
                    batch = documents[i : i + batch_size]
                    batch_num = i // batch_size + 1

                    logger.info(f"Processing batch {batch_num}: {len(batch)} products")

                    try:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            batch_processed = 0

                            for document in batch:
                                try:
                                    product_name = document.get("name", "Unknown")
                                    date_added = document.get("dateAdded", "Unknown")

                                    logger.debug(
                                        f"Processing: {product_name} (scraped: {date_added})"
                                    )

                                    product = (
                                        self.product_factory.create_product_from_json(
                                            document, cur
                                        )
                                    )
                                    breadcrumbs = document.get("breadcrumb", [])
                                    for breadcrump in breadcrumbs:
                                        category_id = breadcrump.get("id")
                                        if not category_id:
                                            logger.warning(
                                                f"Breadcrumb without ID in {product_name}"
                                            )
                                            continue
                                        try:
                                            cur.execute(
                                                """
                                                INSERT INTO product_category (product_id, scraped_at, category_id)
                                                VALUES (%s, %s, %s)
                                                ON CONFLICT (product_id, scraped_at, category_id) DO NOTHING
                                            """,
                                                (
                                                    product.migros_id,
                                                    product.scraped_at,
                                                    category_id,
                                                ),
                                            )
                                        except Exception as e:
                                            logger.warning(
                                                f"Failed to link product {product.id} to category {category_id}: {e}"
                                            )

                                    batch_processed += 1

                                except Exception as e:
                                    logger.error(
                                        f"Error processing product '{product_name}': {e}"
                                    )
                                    total_failed += 1
                                    # Continue with next product in batch

                            # Commit the entire batch
                            conn.commit()
                            total_processed += batch_processed
                            logger.info(
                                f"Batch {batch_num} completed: {batch_processed} products saved"
                            )

                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Batch {batch_num} failed, rolling back: {e}")
                        total_failed += len(batch)

        except Exception as e:
            logger.error(f"Critical error during product processing: {e}")
            raise

        logger.info(
            f"Processing complete - Success: {total_processed}, Failed: {total_failed}"
        )
        return total_processed, total_failed

    def process_categories(
        self, documents: List[Dict], dbname: str, batch_size: int = 100
    ):
        """Process categories in batches."""
        if not documents:
            logger.warning("No category documents to process")
            return

        total_processed = 0
        total_failed = 0

        try:
            with self.db_manager.connect(dbname) as conn:
                conn.autocommit = False

                for i in range(0, len(documents), batch_size):
                    batch = documents[i : i + batch_size]
                    batch_num = i // batch_size + 1

                    logger.info(
                        f"Processing category batch {batch_num}: {len(batch)} categories"
                    )

                    try:
                        with conn.cursor(cursor_factory=RealDictCursor) as cur:
                            batch_processed = 0

                            for document in batch:
                                try:
                                    category_name = document.get("name", "Unknown")
                                    category_id = document.get("id", None)
                                    slug = document.get("slug", "")
                                    path = document.get("path", "")

                                    cur.execute(
                                        """
                                        INSERT INTO category (id, name, slug, path)
                                        VALUES (%(id)s, %(name)s, %(slug)s, %(path)s)
                                        ON CONFLICT (id) DO NOTHING
                                    """,
                                        document,
                                    )

                                    batch_processed += 1
                                    logger.debug(f"Processed category: {category_name}")

                                except Exception as e:
                                    logger.error(
                                        f"Error processing category '{category_name}': {e}"
                                    )
                                    total_failed += 1

                            conn.commit()
                            total_processed += batch_processed
                            logger.info(
                                f"Category batch {batch_num} completed: {batch_processed} categories saved"
                            )

                    except Exception as e:
                        conn.rollback()
                        logger.error(
                            f"Category batch {batch_num} failed, rolling back: {e}"
                        )
                        total_failed += len(batch)

        except Exception as e:
            logger.error(f"Critical error during category processing: {e}")
            raise

        logger.info(
            f"Category processing complete - Success: {total_processed}, Failed: {total_failed}"
        )
        return total_processed, total_failed


def initialize_database(
    db_manager: PostgreSQLManager, target_db: str, force_recreate: bool = True
):
    """Initialize the database with proper checks."""
    try:
        if db_manager.database_exists(target_db):
            if force_recreate:
                logger.info(f"Database {target_db} exists - recreating...")
                db_manager.create_database(target_db, drop_if_exists=True)
            else:
                logger.info(f"Database {target_db} already exists - skipping creation")
        else:
            logger.info(f"Creating new database: {target_db}")
            db_manager.create_database(target_db, drop_if_exists=False)

        db_manager.execute_script(target_db, config.SQL_INIT_SCRIPT)

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise


def create_sql_db(
    limit_products: Optional[int] = None,
    limit_categories: Optional[int] = None,
    force_recreate: bool = True,
):
    """Main function to create and populate SQL database."""
    logger.info("Starting SQL database creation process")

    db_manager = PostgreSQLManager(config)
    product_factory = ProductFactory()
    processor = ProductProcessor(db_manager, product_factory)

    try:
        initialize_database(db_manager, config.PG_DB_NAME, force_recreate)

        logger.info("Loading and processing categories...")
        category_documents = DataLoader.load_documents_from_folder(
            config.CATEGORIES_PATH, limit=limit_categories
        )

        if category_documents:
            processor.process_categories(
                category_documents, config.PG_DB_NAME, config.BATCH_SIZE
            )
        else:
            logger.warning("No categories to process")

        logger.info("Loading and processing products...")
        product_documents = DataLoader.load_documents_from_folder(
            config.PRODUCTS_PATH, limit=limit_products
        )

        if product_documents:
            processor.process_products(
                product_documents, config.PG_DB_NAME, config.BATCH_SIZE
            )
        else:
            logger.warning("No products to process")

        logger.info("SQL database creation completed successfully")

    except Exception as e:
        logger.error(f"SQL database creation failed: {e}")
        raise


def main():
    print("goto main.py")


if __name__ == "__main__":
    main()
