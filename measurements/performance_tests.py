import logging

from measurements.base_measurement import BaseMeasurement

logger = logging.getLogger(__name__)


class SimpleCountTest(BaseMeasurement):
    """Test simple counting operations."""

    def run_mongodb_test(self):
        """Count all products in MongoDB."""
        self.mongo_manager.connect()
        try:
            count = self.mongo_manager.db.products.count_documents({})
            return count
        finally:
            self.mongo_manager.disconnect()

    def run_postgresql_test(self):
        """Count all products in PostgreSQL."""
        with self.postgres_manager.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM product")
                return cur.fetchone()[0]


class SingleProductRetrievalTest(BaseMeasurement):
    """Test retrieving a single product with all related data."""

    def run_mongodb_test(self):
        """Get product with embedded data from MongoDB."""
        self.mongo_manager.connect()
        try:
            # Get first product with all embedded data
            product = self.mongo_manager.db.products.find_one({})
            return product
        finally:
            self.mongo_manager.disconnect()

    def run_postgresql_test(self):
        """Get product with joins from PostgreSQL."""
        with self.postgres_manager.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        p.migros_id,
                        p.name,
                        p.brand,
                        p.scraped_at,
                        n.kcal, n.kj, n.fat, n.protein,
                        o.price, o.quantity, o.unit_price
                    FROM product p
                    LEFT JOIN nutrients n ON p.nutrient_id = n.id
                    LEFT JOIN offer o ON p.offer_id = o.id
                    LIMIT 1
                """
                )
                return cur.fetchone()


class CategoryFilterTest(BaseMeasurement):
    """Test filtering products by category."""

    def run_mongodb_test(self):
        """Filter products by category in MongoDB."""
        self.mongo_manager.connect()
        try:
            # Find products in a specific category
            products = list(
                self.mongo_manager.db.products.find(
                    {"categories.name": {"$regex": "Snacks", "$options": "i"}}
                )
            )
            return len(products)
        finally:
            self.mongo_manager.disconnect()

    def run_postgresql_test(self):
        """Filter products by category in PostgreSQL."""
        with self.postgres_manager.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM product p
                    JOIN product_category pc ON (p.migros_id = pc.product_id AND p.scraped_at = pc.scraped_at)
                    JOIN category c ON pc.category_id = c.id
                    WHERE c.name ILIKE '%Snacks%'
                """
                )
                return cur.fetchone()[0]
