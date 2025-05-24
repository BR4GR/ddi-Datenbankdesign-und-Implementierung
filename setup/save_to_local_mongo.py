import json
import os
import re
import logging
from datetime import datetime

from pymongo import MongoClient

# Set MongoDB connection for local database
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "productdb"
PRODUCT_COLLECTION = "products"
CATEGORY_COLLECTION = "categories"

# Set paths to load data
PRODUCTS_PATH = "data/product/"
CATEGORIES_PATH = "data/categorie/"

# Set up logging (if not already configured globally)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def connect_to_local_mongo():
    """Connect to local MongoDB using standard credentials."""
    client = MongoClient(LOCAL_MONGO_URI)
    return client[DATABASE_NAME]


def load_documents_from_folder(folder_path):
    """Load JSON documents from the specified folder."""
    documents = []
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, "r") as f:
                    documents.append(json.load(f))
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON from {file_path}: {e}")
            except Exception as e:
                logging.error(f"Error loading document from {file_path}: {e}")
    return documents

def extract_number(quantity_str):
    """
    Extract a number from a string.
    If it is a float, return a float; if an int, return an int.
    If no number is found, return None.
    If it is already a number, return it as is.
    """
    if isinstance(quantity_str, (int, float)):
        return quantity_str

    match = re.search(r"(\d+(\.\d+)?)", quantity_str)
    if match:
        number_str = match.group(1)
        try:
            return float(number_str)
        except ValueError:
            logging.error(
                f"Could not convert extracted number '{number_str}' to float."
            )
            return None
    return None

def process_product_for_mongo(product_json, categories_lookup):
    """
    Processes a raw product JSON into a MongoDB-optimized document
    by embedding nutrition, offer, and category information.
    """
    mongo_doc = {
        "migrosId": product_json.get("migrosId"),
        "name": product_json.get("name"),
        "brand": product_json.get("brand") or product_json.get("brandLine"),
        "title": product_json.get("title"),
        "description": product_json.get("description", None),
        "origin": product_json.get("productInformation", {})
        .get("mainInformation", {})
        .get("origin"),
        "ingredients": product_json.get("productInformation", {})
        .get("mainInformation", {})
        .get("ingredients", None),
        "gtins": product_json.get("gtins", []),
        "scraped_at": datetime.fromisoformat(product_json["dateAdded"]) if product_json.get("dateAdded") else datetime.now()
    }

    # --- Process Nutrition Information ---
    nutrients_table = (
        product_json.get("productInformation", {})
        .get("nutrientsInformation", {})
        .get("nutrientsTable", None)
    )
    if nutrients_table and nutrients_table.get("headers"):
        headers = nutrients_table.get("headers", [])
        unit_index = None
        unit = None
        quantity = None

        # Determine the correct column index for per 100g/ml values
        for i, header in enumerate(headers):
            if "100 g" in header or "100ml" in header: # Adjusted to match '100 g' from sample
                unit_index = i
                quantity = 100
                if "g" in header:
                    unit = "g"
                elif "ml" in header:
                    unit = "ml"
                break
            elif "1 Becher" in header: # Fallback for '1 Becher (125 g)' if 100g/ml isn't primary
                unit_index = i
                # Try to extract quantity from header, e.g., "125 g"
                q_match = re.search(r"\((\d+(\.\d+)?)\s*(g|ml)\)", header)
                if q_match:
                    quantity = float(q_match.group(1))
                    unit = q_match.group(3)
                else:
                    quantity = 1 # Default to 1 if not found
                    unit = "serving" # Generic unit
                break

        if unit_index is None:
            logging.warning(f"No suitable header found for nutrients for product {mongo_doc['migrosId']}. Skipping nutrition data.")
        else:
            nutrient_data = {
                "unit": unit,
                "quantity": quantity,
                "kcal": None,
                "kJ": None,
                "fat": None,
                "saturates": None,
                "carbohydrate": None,
                "sugars": None,
                "fibre": None,
                "protein": None,
                "salt": None,
            }

            rows = nutrients_table.get("rows", [])
            for row in rows:
                label = row.get("label", "").lower()
                values = row.get("values", [])
                if len(values) > unit_index:
                    value = values[unit_index]
                    if "energy" in label:
                        energy_match = re.search(
                            r"(\d+(\.\d+)?)\s*kJ.*?\((\d+(\.\d+)?)\s*kcal\)",
                            value,
                            re.IGNORECASE,
                        )
                        if energy_match:
                            nutrient_data["kJ"] = float(energy_match.group(1))
                            nutrient_data["kcal"] = float(energy_match.group(3))
                        else:
                            logging.warning(
                                f"Energy values not found in '{value}' for product {mongo_doc['migrosId']}."
                            )
                    else:
                        extracted_value = extract_number(value)
                        if extracted_value is not None:
                            if "fat" in label and "saturates" not in label:
                                nutrient_data["fat"] = extracted_value
                            elif "saturates" in label:
                                nutrient_data["saturates"] = extracted_value
                            elif "carbohydrate" in label and "sugars" not in label:
                                nutrient_data["carbohydrate"] = extracted_value
                            elif "sugars" in label:
                                nutrient_data["sugars"] = extracted_value
                            elif "fibre" in label:
                                nutrient_data["fibre"] = extracted_value
                            elif "protein" in label:
                                nutrient_data["protein"] = extracted_value
                            elif "salt" in label:
                                nutrient_data["salt"] = extracted_value
            mongo_doc["nutrition"] = nutrient_data

    # --- Process Offer Information ---
    offer_json = product_json.get("offer", {})
    if offer_json:
        price = offer_json.get("price", {}).get("value")
        quantity_str = offer_json.get("quantity")
        quantity = extract_number(quantity_str)

        unit_price = offer_json.get("price", {}).get("unitPrice", {}).get("value") # Direct access from new JSON

        # Original logic for promotion price isn't in new JSON, so assuming it's not available
        promotion_price = offer_json.get("promotionPrice", {}).get("value") # Keep for robustness
        promotion_unit_price = None # No direct promotion unit price in new JSON

        mongo_doc["offer"] = {
            "price": price,
            "quantity": quantity_str,
            "unit_price": unit_price,
            "promotion_price": promotion_price,
            "promotion_unit_price": promotion_unit_price,
        }

    # --- Process Categories from Breadcrumb ---
    embedded_categories = []
    for breadcrumb_item in product_json.get("breadcrumb", []):
        cat_id = breadcrumb_item.get("id")
        cat_name = breadcrumb_item.get("name")
        cat_slug = breadcrumb_item.get("slugs", [])[0] if breadcrumb_item.get("slugs") else None

        # Look up full category info from our categories_lookup if needed, otherwise use what's available
        # The 'slugs' array in breadcrumb gives us the path to the category
        if cat_slug and cat_slug in categories_lookup:
            # We found the category in our separate collection, embed its full details
            embedded_categories.append(categories_lookup[cat_slug])
        elif cat_id and cat_name and cat_slug:
            # If not found in lookup, but we have enough info, embed what's available
            embedded_categories.append({"id": cat_id, "name": cat_name, "slug": cat_slug})

    # Ensure uniqueness in case of overlapping breadcrumbs
    unique_categories = []
    seen_ids = set()
    for cat in embedded_categories:
        if cat.get("id") not in seen_ids:
            unique_categories.append(cat)
            seen_ids.add(cat.get("id"))

    mongo_doc["categories"] = unique_categories


    return mongo_doc


def create_mongo_db():
    """Save JSON files from local folder to local MongoDB."""
    db = connect_to_local_mongo()
    db[PRODUCT_COLLECTION].delete_many({})
    db[CATEGORY_COLLECTION].delete_many({})

    # 1. Load category data and create a lookup dictionary for efficient embedding
    category_documents = load_documents_from_folder(CATEGORIES_PATH)
    categories_lookup = {}
    if category_documents:
        db[CATEGORY_COLLECTION].insert_many(category_documents)
        logging.info(f"Loaded {len(category_documents)} categories into MongoDB.")
        # Create a lookup for categories by slug for efficient embedding
        for cat in category_documents:
            if "slug" in cat:
                categories_lookup[cat["slug"]] = {"id": cat.get("id"), "name": cat.get("name"), "slug": cat.get("slug")}
    else:
        logging.warning("No category documents found. Products might not have embedded category data.")

    # 2. Load and process product data for embedding categories, nutrition, and offers
    product_documents_raw = load_documents_from_folder(PRODUCTS_PATH)
    products_to_insert = []
    if product_documents_raw:
        for doc in product_documents_raw[1]:
            try:
                processed_doc = process_product_for_mongo(doc, categories_lookup)
                products_to_insert.append(processed_doc)
            except Exception as e:
                logging.error(f"Error processing product {doc.get('migrosId', 'N/A')} for MongoDB: {e}")

        if products_to_insert:
            db[PRODUCT_COLLECTION].insert_many(products_to_insert)
            logging.info(f"Inserted {len(products_to_insert)} products into MongoDB.")
    else:
        logging.warning("No product documents found to process for MongoDB.")


def main():
    create_mongo_db()


if __name__ == "__main__":
    main()
