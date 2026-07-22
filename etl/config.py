from pathlib import Path

# Resolve the project root regardless of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input database provided for the assessment.
SOURCE_DATABASE_PATH = PROJECT_ROOT / "data" / "shopdata.db"

# Output database that will be created during the Load stage.
ANALYTICS_DATABASE_PATH = PROJECT_ROOT / "analytics.db"