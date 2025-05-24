import json
import os

from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

# Get MongoDB credentials and other configurations from environment variables
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
PRODUCT_COLLECTION = os.getenv("PRODUCT_COLLECTION")
CATEGORY_COLLECTION = os.getenv("CATEGORY_COLLECTION")

# Set paths to save data
PRODUCTS_PATH = "data/product/"
CATEGORIES_PATH = "data/categorie/"


def connect_to_mongo():
    """Connect to MongoDB using credentials from environment variables."""
    client = MongoClient(MONGO_URI)
    return client[DATABASE_NAME]


def save_documents_to_folder(documents, folder_path, id_field):
    """Save MongoDB documents to JSON files in the specified folder."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for document in documents:
        # Ensure document is a dictionary
        if isinstance(document, dict):
            # Remove MongoDB-specific _id field for better compatibility
            document.pop("_id", None)

            filename = os.path.join(folder_path, f"{document[id_field]}.json")
            if id_field == "migrosId":
                filename = os.path.join(
                    folder_path, f"{document[id_field]}-{document["dateAdded"]}.json"
                )
            with open(filename, "w") as f:
                json.dump(document, f, indent=4, ensure_ascii=False)


def download_and_save_data():
    """Download data from MongoDB and save them as JSON files."""
    db = connect_to_mongo()

    # Fetch and save product data
    product_documents = list(db[PRODUCT_COLLECTION].find())
    save_documents_to_folder(product_documents, PRODUCTS_PATH, "migrosId")

    # Fetch and save category data
    category_documents = list(db[CATEGORY_COLLECTION].find())
    save_documents_to_folder(category_documents, CATEGORIES_PATH, "id")


def download_and_save_test_data():
    """Download a single document from each collection and save as JSON files for testing."""
    db = connect_to_mongo()

    # Fetch and save a single product document
    product_document = db[PRODUCT_COLLECTION].find_one()
    if product_document:
        save_documents_to_folder([product_document], PRODUCTS_PATH, "migrosId")

    # Fetch and save a single category document
    category_document = db[CATEGORY_COLLECTION].find_one()
    if category_document:
        save_documents_to_folder([category_document], CATEGORIES_PATH, "id")


def main():
    # Uncomment the line below to download all data
    download_and_save_data()

    # Run the test download
    # download_and_save_test_data()
    print("already done. nothing to do")


if __name__ == "__main__":
    main()
