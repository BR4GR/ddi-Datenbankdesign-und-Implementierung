import logging
import re
from datetime import datetime

from src.models.nutrition import Nutrition
from src.models.offer import Offer
from src.models.product import Product

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


class ProductFactory:
    @staticmethod
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

    @staticmethod
    def create_product_from_json(product_json, cursor):
        nutrient_id = None
        offer_id = None

        # Extract nutrients
        nutrition = None
        offer = None
        try:
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
                    if "100" in header:
                        unit_index = i
                        quantity = 100
                        if "g" in header:
                            unit = "g"
                        elif "ml" in header:
                            unit = "ml"
                        break  # Stop after finding the correct header
                    else:
                        unit_index = 0
                        quantity = 1
                        # cutt of unit at 15 characters
                        unit = headers[0][:15]

                if unit_index is None:
                    logging.warning("No per 100g/ml header found in nutrients table.")
                    raise ValueError("Missing per 100g/ml header.")

                rows = nutrients_table.get("rows", [])
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

                for row in rows:
                    label = row.get("label", "").lower()
                    values = row.get("values", [])
                    if len(values) > unit_index:
                        value = values[unit_index]
                        if "energy" in label:
                            # Extract kJ and kcal values
                            energy_match = re.search(
                                r"(\d+(\.\d+)?)\s*kJ.*?(\d+(\.\d+)?)\s*kcal",
                                value,
                                re.IGNORECASE,
                            )
                            if energy_match:
                                nutrient_data["kJ"] = float(energy_match.group(1))
                                nutrient_data["kcal"] = float(energy_match.group(3))
                            else:
                                logging.warning(
                                    f"Energy values not found in '{value}'."
                                )
                        else:
                            extracted_value = ProductFactory.extract_number(value)
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
                                else:
                                    continue

                # Create and save Nutrition object
                nutrition = Nutrition(**nutrient_data)
        except Exception as e:
            logging.error(f"Error processing nutrients: {e}")

        # Extract offer
        try:
            offer_json = product_json.get("offer", {})
            if offer_json:
                price = offer_json.get("price", {}).get("value")
                quantity_str = offer_json.get("quantity")
                quantity = (
                    ProductFactory.extract_number(quantity_str)
                    if quantity_str
                    else None
                )

                unit_price = (price * 100 / quantity) if price and quantity else None
                promotion_price = offer_json.get("promotionPrice", {}).get("value")
                promotion_unit_price = (
                    (promotion_price * 100 / quantity)
                    if promotion_price and quantity
                    else None
                )

                offer = Offer(
                    price=price,
                    quantity=quantity_str,
                    unit_price=unit_price,
                    promotion_price=promotion_price,
                    promotion_unit_price=promotion_unit_price,
                )
        except Exception as e:
            logging.error(f"Error processing offer: {e}")

        # Extract product
        try:
            scraped_at_str = product_json.get("dateAdded")
            if scraped_at_str:
                try:
                    scraped_at = datetime.fromisoformat(scraped_at_str)
                except ValueError:
                    logging.warning(
                        f"Invalid date format for 'dateAdded': {scraped_at_str}"
                    )
                    scraped_at = datetime.now()
            else:
                scraped_at = datetime.now()

            gtins = product_json.get("gtins", [])
            gtins_str = ",".join(gtins) if isinstance(gtins, list) else gtins

            product = Product(
                migros_id=product_json.get("migrosId"),
                name=product_json.get("name"),
                brand=product_json.get("brand") or product_json.get("brandLine"),
                title=product_json.get("title"),
                origin=product_json.get("productInformation", {})
                .get("mainInformation", {})
                .get("origin"),
                description=product_json.get("description", None),
                ingredients=product_json.get("productInformation", {})
                .get("mainInformation", {})
                .get("ingredients", None),
                nutrition=nutrition,
                offer=offer,
                gtins=gtins_str,
                scraped_at=scraped_at,
            )
            product.save_to_db(cursor)
        except Exception as e:
            logging.error(f"Error processing product: {e}")

        return product
