import json
import os

from pymongo import MongoClient

# Set MongoDB connection for local database
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "productdb"
PRODUCT_COLLECTION = "products"
CATEGORY_COLLECTION = "categories"

# Set paths to load data
PRODUCTS_PATH = "src/data/product/"
CATEGORIES_PATH = "src/data/categorie/"


def connect_to_local_mongo():
    """Connect to local MongoDB using standard credentials."""
    client = MongoClient(LOCAL_MONGO_URI)
    return client[DATABASE_NAME]


def load_documents_from_folder(folder_path):
    """Load JSON documents from the specified folder."""
    documents = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            with open(os.path.join(folder_path, filename), "r") as f:
                documents.append(json.load(f))
    return documents


def save_data_to_local_mongo():
    """Save JSON files from local folder to local MongoDB."""
    db = connect_to_local_mongo()
    db[PRODUCT_COLLECTION].delete_many({})
    db[CATEGORY_COLLECTION].delete_many({})

    # Load and save product data
    product_documents = load_documents_from_folder(PRODUCTS_PATH)
    if product_documents:
        db[PRODUCT_COLLECTION].insert_many(product_documents)

    # Load and save category data
    category_documents = load_documents_from_folder(CATEGORIES_PATH)
    if category_documents:
        db[CATEGORY_COLLECTION].insert_many(category_documents)


def main():
    save_data_to_local_mongo()


if __name__ == "__main__":
    main()
