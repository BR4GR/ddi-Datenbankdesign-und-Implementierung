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
from setup.mongodb_manager import MongoDBManager

config = DatabaseConfig()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class NutritionProcessor:
    """Handles nutrition data processing."""

    NUTRIENT_MAPPING = {
        "energy": ("kJ", "kcal"),
        "fat": "fat",
        "saturates": "saturates",
        "carbohydrate": "carbohydrate",
        "sugars": "sugars",
        "fibre": "fibre",
        "protein": "protein",
        "salt": "salt",
    }

    @staticmethod
    def extract_number(value: Any) -> Optional[float]:
        """Extract numeric value from string or return if already numeric."""
        if isinstance(value, (int, float)):
            return float(value)

        if not isinstance(value, str):
            return None

        match = re.search(r"(\d+(?:\.\d+)?)", value)
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                logger.warning(f"Could not convert '{match.group(1)}' to float")
        return None

    @classmethod
    def process_nutrition(cls, product_json: Dict) -> Optional[Dict]:
        """Process nutrition information from product JSON."""
        nutrients_table = (
            product_json.get("productInformation", {})
            .get("nutrientsInformation", {})
            .get("nutrientsTable")
        )

        if not nutrients_table or not nutrients_table.get("headers"):
            return None

        unit_info = cls._find_nutrition_column(nutrients_table.get("headers", []))
        if not unit_info:
            return None

        unit_index, unit, quantity = unit_info

        nutrition_data = {
            "unit": unit,
            "quantity": quantity,
            **{
                nutrient: None
                for nutrient in cls.NUTRIENT_MAPPING.values()
                if isinstance(nutrient, str)
            },
            "kJ": None,
            "kcal": None,
        }

        for row in nutrients_table.get("rows", []):
            cls._process_nutrition_row(row, unit_index, nutrition_data)

        return nutrition_data

    @classmethod
    def _find_nutrition_column(cls, headers: List[str]) -> Optional[tuple]:
        """Find the best column for nutrition values."""
        for i, header in enumerate(headers):
            if "100 g" in header:
                return (i, "g", 100.0)
            elif "100ml" in header or "100 ml" in header:
                return (i, "ml", 100.0)

        for i, header in enumerate(headers):
            if "Becher" in header or "serving" in header.lower():
                quantity_match = re.search(r"\((\d+(?:\.\d+)?)\s*(g|ml)\)", header)
                if quantity_match:
                    return (i, quantity_match.group(2), float(quantity_match.group(1)))
                return (i, "serving", 1.0)

        return None

    @classmethod
    def _process_nutrition_row(cls, row: Dict, unit_index: int, nutrition_data: Dict):
        """Process a single nutrition row."""
        label = row.get("label", "").lower()
        values = row.get("values", [])

        if len(values) <= unit_index:
            return

        value = values[unit_index]

        if "energy" in label:
            cls._process_energy_value(value, nutrition_data)
        else:
            numeric_value = cls.extract_number(value)
            if numeric_value is not None:
                cls._map_nutrient_value(label, numeric_value, nutrition_data)

    @classmethod
    def _process_energy_value(cls, value: str, nutrition_data: Dict):
        """Process energy value containing both kJ and kcal."""
        energy_match = re.search(
            r"(\d+(?:\.\d+)?)\s*kJ.*?\((\d+(?:\.\d+)?)\s*kcal\)", value, re.IGNORECASE
        )
        if energy_match:
            nutrition_data["kJ"] = float(energy_match.group(1))
            nutrition_data["kcal"] = float(energy_match.group(2))

    @classmethod
    def _map_nutrient_value(cls, label: str, value: float, nutrition_data: Dict):
        """Map nutrient label to nutrition data field."""
        for key, field in cls.NUTRIENT_MAPPING.items():
            if isinstance(field, str) and key in label and field not in label:
                if key == "fat" and "saturates" in label:
                    continue
                if key == "carbohydrate" and "sugars" in label:
                    continue
                nutrition_data[field] = value
                break


class ProductProcessor:
    """Handles product document processing."""

    @staticmethod
    def process_product(product_json: Dict, categories_lookup: Dict) -> Dict:
        """Process raw product JSON into MongoDB document."""
        try:
            mongo_doc = {
                "migrosId": product_json.get("migrosId"),
                "name": product_json.get("name"),
                "brand": product_json.get("brand") or product_json.get("brandLine"),
                "title": product_json.get("title"),
                "description": product_json.get("description"),
                "origin": (
                    product_json.get("productInformation", {})
                    .get("mainInformation", {})
                    .get("origin")
                ),
                "ingredients": (
                    product_json.get("productInformation", {})
                    .get("mainInformation", {})
                    .get("ingredients")
                ),
                "gtins": product_json.get("gtins", []),
                "scraped_at": ProductProcessor._parse_date(
                    product_json.get("dateAdded")
                ),
            }

            nutrition = NutritionProcessor.process_nutrition(product_json)
            if nutrition:
                mongo_doc["nutrition"] = nutrition

            offer = ProductProcessor._process_offer(product_json.get("offer", {}))
            if offer:
                mongo_doc["offer"] = offer

            categories = ProductProcessor._process_categories(
                product_json.get("breadcrumb", []), categories_lookup
            )
            mongo_doc["categories"] = categories

            return mongo_doc

        except Exception as e:
            logger.error(
                f"Error processing product {product_json.get('migrosId', 'unknown')}: {e}"
            )
            raise

    @staticmethod
    def _parse_date(date_str: Optional[str]) -> datetime:
        """Parse date string or return current time."""
        if date_str:
            try:
                return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            except ValueError:
                logger.warning(f"Invalid date format: {date_str}")
        return datetime.now()

    @staticmethod
    def _process_offer(offer_json: Dict) -> Optional[Dict]:
        """Process offer information."""
        if not offer_json:
            return None

        price_info = offer_json.get("price", {})
        quantity = offer_json.get("quantity", None)
        unit_price_info = price_info.get("unitPrice", {}).get("value")
        promotion_price = offer_json.get("promotionPrice", {}).get("value")
        promotion_unit_price = None
        if promotion_price and quantity:
            promotion_unit_price = promotion_price / quantity
        return {
            "price": price_info.get("value"),
            "quantity": quantity,
            "unit_price": unit_price_info,
            "promotion_price": promotion_price,
            "promotion_unit_price": promotion_unit_price,
        }

    @staticmethod
    def _process_categories(
        breadcrumb: List[Dict], categories_lookup: Dict
    ) -> List[Dict]:
        """Process category information from breadcrumb."""
        categories = []
        seen_ids = set()

        for item in breadcrumb:
            cat_id = item.get("id")
            cat_name = item.get("name")
            cat_slug = item.get("slugs", [])[-1] if item.get("slugs") else None

            if cat_id and cat_id not in seen_ids:
                if cat_slug in categories_lookup:
                    category_data = categories_lookup[cat_slug].copy()
                else:
                    category_data = {"id": cat_id, "name": cat_name, "slug": cat_slug}

                categories.append(category_data)
                seen_ids.add(cat_id)

        return categories


class CategoryProcessor:
    """Handles category processing."""

    @staticmethod
    def create_categories_lookup(category_documents: List[Dict]) -> Dict[str, Dict]:
        """Create lookup dictionary for categories by slug."""
        lookup = {}
        for cat in category_documents:
            slug = cat.get("slug")
            if slug:
                lookup[slug] = {
                    "id": cat.get("id"),
                    "name": cat.get("name"),
                    "slug": slug,
                }
        return lookup


def create_mongo_db(
    limit_products: Optional[int] = None,
    limit_categories: Optional[int] = None,
    force_recreate: bool = True,
):
    """Main function to load data into MongoDB."""
    db_manager = MongoDBManager(config.MONGO_DB_URI, config.MONGO_DB_NAME)

    try:
        db_manager.connect()

        db_manager.clear_collections(
            [config.MONGO_PRODUCT_COLLECTION, config.MONGO_CATEGORY_COLLECTION]
        )

        logger.info("Loading categories...")
        category_documents = DataLoader.load_documents_from_folder(
            config.CATEGORIES_PATH
        )
        categories_lookup = {}

        if category_documents:
            db_manager.insert_batch(
                config.MONGO_CATEGORY_COLLECTION, category_documents, config.BATCH_SIZE
            )
            categories_lookup = CategoryProcessor.create_categories_lookup(
                category_documents
            )
            logger.info(f"Created lookup for {len(categories_lookup)} categories")
        else:
            logger.warning(
                "No categories loaded - products will have limited category data"
            )

        logger.info("Loading and processing products...")
        product_documents_raw = DataLoader.load_documents_from_folder(
            config.PRODUCTS_PATH, limit=limit_products
        )

        if not product_documents_raw:
            logger.warning("No products to process")
            return

        processed_products = []
        failed_count = 0

        for i, product_doc in enumerate(product_documents_raw):
            try:
                processed_doc = ProductProcessor.process_product(
                    product_doc, categories_lookup
                )
                processed_products.append(processed_doc)

                if len(processed_products) >= config.BATCH_SIZE:
                    db_manager.insert_batch(
                        config.MONGO_PRODUCT_COLLECTION,
                        processed_products,
                        config.BATCH_SIZE,
                    )
                    processed_products = []

            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to process product {i+1}: {e}")

        if processed_products:
            db_manager.insert_batch(
                config.MONGO_PRODUCT_COLLECTION, processed_products, config.BATCH_SIZE
            )

        logger.info(f"Processing complete. Failed products: {failed_count}")

    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise
    finally:
        db_manager.disconnect()


def main():
    print("goto main.py")


if __name__ == "__main__":
    main()
