import logging
import statistics
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List

from setup.database_config import DatabaseConfig
from setup.mongodb_manager import MongoDBManager
from setup.postgresql_manager import PostgreSQLManager

logger = logging.getLogger(__name__)


@dataclass
class MeasurementResult:
    """Container for measurement results."""

    name: str
    mongodb_time: float
    postgresql_time: float
    mongodb_result: Any
    postgresql_result: Any
    mongodb_error: str = None
    postgresql_error: str = None

    @property
    def performance_ratio(self) -> float:
        """MongoDB time / PostgreSQL time (lower is better for MongoDB)."""
        if self.postgresql_time > 0:
            return self.mongodb_time / self.postgresql_time
        return float("inf")

    @property
    def winner(self) -> str:
        """Which database performed better."""
        if self.mongodb_error and not self.postgresql_error:
            return "PostgreSQL"
        elif self.postgresql_error and not self.mongodb_error:
            return "MongoDB"
        elif self.mongodb_time < self.postgresql_time:
            return "MongoDB"
        else:
            return "PostgreSQL"


class BaseMeasurement(ABC):
    """Base class for all database measurements."""

    def __init__(self):
        self.config = DatabaseConfig()
        self.mongo_manager = MongoDBManager(
            self.config.MONGO_DB_URI, self.config.MONGO_DB_NAME
        )
        self.postgres_manager = PostgreSQLManager(self.config)

    def measure_execution_time(self, func, *args, **kwargs) -> tuple[Any, float, str]:
        """Measure execution time of a function."""
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            end_time = time.perf_counter()
            return result, end_time - start_time, None
        except Exception as e:
            end_time = time.perf_counter()
            return None, end_time - start_time, str(e)

    @abstractmethod
    def run_mongodb_test(self) -> Any:
        """Run test on MongoDB - must be implemented by subclasses."""
        pass

    @abstractmethod
    def run_postgresql_test(self) -> Any:
        """Run test on PostgreSQL - must be implemented by subclasses."""
        pass

    def run_comparison(self, iterations: int = 5) -> MeasurementResult:
        """Run comparison between MongoDB and PostgreSQL."""
        logger.info(f"Running measurement: {self.__class__.__name__}")

        # MongoDB test
        mongo_times = []
        mongo_result = None
        mongo_error = None

        for i in range(iterations):
            result, exec_time, error = self.measure_execution_time(
                self.run_mongodb_test
            )
            if error:
                mongo_error = error
                logger.error(f"MongoDB test failed: {error}")
                break
            else:
                mongo_times.append(exec_time)
                if i == 0:  # Store first result
                    mongo_result = result

        # PostgreSQL test
        postgres_times = []
        postgres_result = None
        postgres_error = None

        for i in range(iterations):
            result, exec_time, error = self.measure_execution_time(
                self.run_postgresql_test
            )
            if error:
                postgres_error = error
                logger.error(f"PostgreSQL test failed: {error}")
                break
            else:
                postgres_times.append(exec_time)
                if i == 0:  # Store first result
                    postgres_result = result

        # Calculate average times
        avg_mongo_time = statistics.mean(mongo_times) if mongo_times else float("inf")
        avg_postgres_time = (
            statistics.mean(postgres_times) if postgres_times else float("inf")
        )

        return MeasurementResult(
            name=self.__class__.__name__,
            mongodb_time=avg_mongo_time,
            postgresql_time=avg_postgres_time,
            mongodb_result=mongo_result,
            postgresql_result=postgres_result,
            mongodb_error=mongo_error,
            postgresql_error=postgres_error,
        )
