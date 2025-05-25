import os
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    # MongoDB Configuration
    MONGO_DB_URI: str = "mongodb://localhost:27017/"
    MONGO_DB_NAME: str = "productdb"
    MONGO_PRODUCT_COLLECTION: str = "products"
    MONGO_CATEGORY_COLLECTION: str = "categories"

    # PostgreSQL Configuration
    PG_DB_NAME: str = os.getenv("PG_DB_NAME", "productsandcategories")
    PG_DEFAULT_DB_NAME: str = os.getenv("PG_DEFAULT_DB_NAME", "postgres")
    PG_DB_USER: str = os.getenv("PG_DB_USER", "postgres")
    PG_DB_PASSWORD: str = os.getenv("PG_DB_PASSWORD", "password")
    PG_DB_HOST: str = os.getenv("PG_DB_HOST", "localhost")
    PG_DB_PORT: str = os.getenv("PG_DB_PORT", "5432")

    # Data Paths
    PRODUCTS_PATH: str = "data/product/"
    CATEGORIES_PATH: str = "data/categorie/"
    SQL_INIT_SCRIPT: str = "setup/sql/createdb.sql"

    # Processing Configuration
    BATCH_SIZE: int = 1000
