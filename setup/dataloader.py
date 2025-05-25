import json
import logging
import os
from typing import Dict, List, Optional

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataLoader:
    """Handles loading and validation of JSON documents."""

    @staticmethod
    def load_documents_from_folder(
        folder_path: str, limit: Optional[int] = None
    ) -> List[Dict]:
        """Load and validate JSON documents from folder."""
        if not os.path.exists(folder_path):
            logger.error(f"Folder does not exist: {folder_path}")
            return []

        documents = []
        json_files = [f for f in os.listdir(folder_path) if f.endswith(".json")]

        if not json_files:
            logger.warning(f"No JSON files found in {folder_path}")
            return []

        # Apply limit to number of files to process
        if limit and limit > 0:
            json_files = json_files[:limit]
            logger.info(f"Processing limited to {limit} files")

        logger.info(f"Loading {len(json_files)} JSON files from {folder_path}")

        for filename in json_files:
            file_path = os.path.join(folder_path, filename)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    document = json.load(f)
                    if DataLoader._validate_document(document, filename):
                        documents.append(document)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {filename}: {e}")
            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")

        logger.info(f"Successfully loaded {len(documents)} valid documents")
        return documents

    @staticmethod
    def _validate_document(document: Dict, filename: str) -> bool:
        """Basic validation for documents."""
        if not isinstance(document, dict):
            logger.warning(f"Invalid document format in {filename}")
            return False

        return True
