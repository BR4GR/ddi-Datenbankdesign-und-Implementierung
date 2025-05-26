import logging
import os

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import RealDictCursor

from setup.database_config import DatabaseConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PostgreSQLManager:
    """Handles PostgreSQL connections and operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection_params = {
            "user": config.PG_DB_USER,
            "password": config.PG_DB_PASSWORD,
            "host": config.PG_DB_HOST,
            "port": config.PG_DB_PORT,
        }
        self.logger = logger

    def connect(self, dbname: str = None) -> connection:
        """Connect to PostgreSQL database."""
        db_name = dbname or self.config.PG_DB_NAME
        try:
            conn = psycopg2.connect(dbname=db_name, **self.connection_params)
            self.logger.info(f"Connected to PostgreSQL database: {db_name}")
            return conn
        except psycopg2.DatabaseError as e:
            self.logger.error(f"Database connection failed: {e}")
            raise

    def database_exists(self, dbname: str) -> bool:
        """Check if database exists."""
        try:
            with self.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM pg_database WHERE datname = %s", (dbname,)
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking database existence: {e}")
            return False

    def create_database(self, dbname: str, drop_if_exists: bool = True):
        """Create database with proper error handling."""
        try:
            conn = self.connect(self.config.PG_DEFAULT_DB_NAME)
            conn.autocommit = True

            try:
                with conn.cursor() as cur:
                    if drop_if_exists:
                        cur.execute(
                            sql.SQL(
                                """
                            SELECT pg_terminate_backend(pg_stat_activity.pid)
                            FROM pg_stat_activity
                            WHERE pg_stat_activity.datname = %s
                            AND pid <> pg_backend_pid()
                        """
                            ),
                            (dbname,),
                        )

                        cur.execute(
                            sql.SQL("DROP DATABASE IF EXISTS {}").format(
                                sql.Identifier(dbname)
                            )
                        )
                        self.logger.info(f"Dropped existing database: {dbname}")

                    cur.execute(
                        sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
                    )
                    self.logger.info(f"Created database: {dbname}")
            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"Error creating database: {e}")
            raise

    def execute_script(self, dbname: str, script_path: str):
        """Execute SQL script on specified database."""
        try:
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"SQL script not found: {script_path}")

            with open(script_path, "r", encoding="utf-8") as file:
                script_content = file.read()

            with self.connect(dbname) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(script_content)
                    self.logger.info(f"Successfully executed script: {script_path}")
        except Exception as e:
            self.logger.error(f"Error executing script {script_path}: {e}")
            raise
