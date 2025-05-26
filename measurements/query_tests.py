import logging

from measurements.base_measurement import BaseMeasurement

logger = logging.getLogger(__name__)


class AggregationTest(BaseMeasurement):
    """Test aggregation queries."""

    def run_mongodb_test(self):
        """MongoDB aggregation pipeline."""
        self.mongo_manager.connect()
        try:
            pipeline = [
                {
                    "$group": {
                        "_id": "$brand",
                        "product_count": {"$sum": 1},
                        "avg_price": {"$avg": "$offer.price"},
                    }
                },
                {"$sort": {"product_count": -1}},
                {"$limit": 10},
            ]
            results = list(self.mongo_manager.db.products.aggregate(pipeline))
            return len(results)
        finally:
            self.mongo_manager.disconnect()

    def run_postgresql_test(self):
        """PostgreSQL aggregation query."""
        with self.postgres_manager.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 
                        p.brand,
                        COUNT(*) as product_count,
                        AVG(o.price) as avg_price
                    FROM product p
                    LEFT JOIN offer o ON p.offer_id = o.id
                    WHERE p.brand IS NOT NULL
                    GROUP BY p.brand
                    ORDER BY product_count DESC
                    LIMIT 10
                """
                )
                return len(cur.fetchall())


class ComplexSearchTest(BaseMeasurement):
    """Test complex search with multiple criteria."""

    def run_mongodb_test(self):
        """MongoDB complex search."""
        self.mongo_manager.connect()
        try:
            results = list(
                self.mongo_manager.db.products.find(
                    {
                        "$and": [
                            {"nutrition.protein": {"$gte": 10}},
                            {"offer.price": {"$lte": 5.0}},
                            {"categories.name": {"$regex": "dairy", "$options": "i"}},
                        ]
                    }
                ).limit(50)
            )
            return len(results)
        finally:
            self.mongo_manager.disconnect()

    def run_postgresql_test(self):
        """PostgreSQL complex search."""
        with self.postgres_manager.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM product p
                    JOIN nutrients n ON p.nutrient_id = n.id
                    JOIN offer o ON p.offer_id = o.id
                    JOIN product_category pc ON (p.migros_id = pc.product_id AND p.scraped_at = pc.scraped_at)
                    JOIN category c ON pc.category_id = c.id
                    WHERE CAST(n.protein AS FLOAT) >= 10
                    AND o.price <= 5.0
                    AND c.name ILIKE '%dairy%'
                """
                )
                return cur.fetchone()[0]
