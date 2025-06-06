import logging
from dataclasses import dataclass, field


@dataclass
class Offer:
    price: float = None
    quantity: str = None
    unit_price: float = None
    promotion_price: float = None
    promotion_unit_price: float = None
    id: int = field(init=False, default=None)

    def save_to_db(self, cursor):
        """Insert offer data into PostgreSQL and return the offer ID."""
        try:
            cursor.execute(
                """
                INSERT INTO offer (
                    price, quantity, unit_price, promotion_price, promotion_unit_price
                ) VALUES (%s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    self.price,
                    self.quantity,
                    self.unit_price,
                    self.promotion_price,
                    self.promotion_unit_price,
                ),
            )
            result = cursor.fetchone()
            if result is None:
                raise Exception("Failed to fetch offer ID after insert.")
            self.id = result["id"]
            return self.id
        except Exception as e:
            logging.error(f"Error inserting offer: {e}", exc_info=True)
            raise
