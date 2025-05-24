import json
import logging
import os
import sys

import psycopg2
from psycopg2.extensions import connection, cursor
from psycopg2.extras import RealDictCursor

from models.product_factory import ProductFactory

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Set paths to load data
PRODUCTS_PATH = "data/product/"
CATEGORIES_PATH = "data/categorie/"
INITIALIZATION_SCRIPT_PATH = "setup/sql/createdb.sql"

# Database configuration using environment variables
PG_DB_NAME = os.getenv("PG_DB_NAME", "postgres")
PG_DB_USER = os.getenv("PG_DB_USER", "postgres")
PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD", "password")
PG_DB_HOST = os.getenv("PG_DB_HOST", "localhost")
PG_DB_PORT = os.getenv("PG_DB_PORT", "5432")


def connect_to_postgres(dbname=PG_DB_NAME):
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=PG_DB_USER,
            password=PG_DB_PASSWORD,
            host=PG_DB_HOST,
            port=PG_DB_PORT,
        )
        conn.autocommit = False  # Ensure transactions are used
        return conn
    except psycopg2.DatabaseError as e:
        logging.error(f"Database connection failed: {e}")
        sys.exit(1)


def initialize_database():
    """
    Connect to the default database and create the 'productsandcategories' database
    if it doesn't exist using the SQL script.
    """
    try:
        # Connect to the default 'postgres' database
        conn: connection = connect_to_postgres()
        conn.autocommit = (
            True  # Set autocommit to True to execute non-transactional statements
        )
        cursor = conn.cursor()

        cursor.execute("DROP DATABASE IF EXISTS productsandcategories;")
        cursor.execute("CREATE DATABASE productsandcategories;")
        cursor.close()
        conn.close()

        logging.info("Database 'productsandcategories' created successfully.")

        # Connect to the new 'productsandcategories' database
        with connect_to_postgres("productsandcategories") as conn:
            conn.autocommit = True
            with conn.cursor() as cursor:
                # Read the SQL script for creating the database
                with open(INITIALIZATION_SCRIPT_PATH, "r") as file:
                    initialization_script = file.read()

                # Execute the SQL script
                cursor.execute(initialization_script)
                logging.info("Initialization script executed successfully.")

    except Exception as error:
        logging.error(f"Error initializing database: {error}")
        sys.exit(1)


def load_documents_from_folder(folder_path):
    """Load JSON documents from the specified folder."""
    documents = []
    try:
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, "r") as f:
                    documents.append(json.load(f))
        return documents
    except Exception as e:
        logging.error(f"Error loading documents: {e}")
        return []


def create_sql_db():
    initialize_database()
    documents = load_documents_from_folder(PRODUCTS_PATH)
    if not documents:
        logging.warning("No documents found to process.")
        return

    product_factory = ProductFactory()

    try:
        with connect_to_postgres("productsandcategories") as conn:
            conn.autocommit = False  # Ensure transactions are used
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Process each document
                for document in documents:
                    try:
                        logging.info(
                            f"Processing product: {document['name']} scraped at {document['dateAdded']}"
                        )

                        product = product_factory.create_product_from_json(
                            document, cursor
                        )
                        conn.commit()
                        logging.info(f"Product {product.name} saved to database.")
                    except Exception as e:
                        conn.rollback()
                        logging.error(
                            f"Error processing product '{product.name}': {e}",
                            exc_info=True,
                        )
    except Exception as e:
        logging.error(f"Error processing products: {e}", exc_info=True)


if __name__ == "__main__":
    create_sql_db()
