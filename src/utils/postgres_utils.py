import json
import os

import psycopg2

# Set paths to load data
PRODUCTS_PATH = "src/data/product/"
CATEGORIES_PATH = "src/data/categorie/"
INITIALIZATION_SCRIPT_PATH = "src/setup/sql/createdb.sql"


def connect_to_postgres(dbname="postgres"):
    """Connect to the PostgreSQL database."""
    return psycopg2.connect(
        dbname=dbname,
        user="postgres",
        password="password",
        host="localhost",
        port="5432",
    )


def initialize_database():
    """Connect to the default database and create the 'productdb' database if it doesn't exist using the SQL script."""
    try:
        # Connect to the default 'postgres' database
        conn = connect_to_postgres()
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute("DROP DATABASE IF EXISTS productsandcategories;")
            cursor.execute("CREATE DATABASE productsandcategories;")

        conn = connect_to_postgres("productsandcategories")
        conn.autocommit = True
        with conn.cursor() as cursor:
            # Read the SQL script for creating the database
            with open(INITIALIZATION_SCRIPT_PATH, "r") as file:
                initialization_script = file.read()

            # Execute the SQL script
            cursor.execute(initialization_script)
            print("Initialization script executed successfully.")
    except Exception as error:
        print(f"Error initializing database: {error}")
    finally:
        conn.close()


# Load JSON files
def load_documents_from_folder(folder_path):
    """Load JSON documents from the specified folder."""
    documents = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            with open(os.path.join(folder_path, filename), "r") as f:
                documents.append(json.load(f))
    return documents
