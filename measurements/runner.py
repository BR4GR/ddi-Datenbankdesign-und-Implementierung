import logging
from typing import List, Dict
from measurements.base_measurement import MeasurementResult
from measurements.performance_tests import (
    SimpleCountTest,
    SingleProductRetrievalTest,
    CategoryFilterTest,
)
from measurements.query_tests import AggregationTest, ComplexSearchTest
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class MeasurementRunner:
    """Runs all measurements and generates reports."""

    def __init__(self):
        self.test_classes = [
            SimpleCountTest,
            SingleProductRetrievalTest,
            CategoryFilterTest,
            AggregationTest,
            ComplexSearchTest,
        ]

    def run_all_tests(self, iterations: int = 5) -> List[MeasurementResult]:
        """Run all measurement tests."""
        results = []

        logger.info(f"Starting measurement suite with {iterations} iterations per test")

        for test_class in self.test_classes:
            try:
                test_instance = test_class()
                result = test_instance.run_comparison(iterations)
                results.append(result)

                logger.info(
                    f"✅ {result.name}: {result.winner} wins "
                    f"(MongoDB: {result.mongodb_time:.4f}s, "
                    f"PostgreSQL: {result.postgresql_time:.4f}s)"
                )

            except Exception as e:
                logger.error(f"❌ {test_class.__name__} failed: {e}")

        return results

    def generate_report(self, results: List[MeasurementResult]) -> Dict:
        """Generate comprehensive report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_tests": len(results),
                "mongodb_wins": sum(1 for r in results if r.winner == "MongoDB"),
                "postgresql_wins": sum(1 for r in results if r.winner == "PostgreSQL"),
            },
            "detailed_results": [],
        }

        for result in results:
            report["detailed_results"].append(
                {
                    "test_name": result.name,
                    "winner": result.winner,
                    "mongodb_time": result.mongodb_time,
                    "postgresql_time": result.postgresql_time,
                    "performance_ratio": result.performance_ratio,
                    "mongodb_error": result.mongodb_error,
                    "postgresql_error": result.postgresql_error,
                }
            )

        return report

    def save_report(self, report: Dict, filename: str = None):
        """Save report to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"measurement_report_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump(report, f, indent=2)

        logger.info(f"Report saved to {filename}")
        return filename


def main():
    """Main execution function."""
    logging.basicConfig(level=logging.INFO)

    runner = MeasurementRunner()
    results = runner.run_all_tests(iterations=3)

    report = runner.generate_report(results)
    filename = runner.save_report(report)

    # Print summary
    print("\n" + "=" * 60)
    print("MEASUREMENT RESULTS SUMMARY")
    print("=" * 60)
    print(f"MongoDB wins: {report['summary']['mongodb_wins']}")
    print(f"PostgreSQL wins: {report['summary']['postgresql_wins']}")
    print(f"Full report saved to: {filename}")


if __name__ == "__main__":
    main()
