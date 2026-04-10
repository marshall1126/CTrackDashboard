import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
print(f"Inserting path: {root}")
print(f"analysis_scripts exists: {(root / 'analysis_scripts').exists()}")
sys.path.insert(0, str(root))

from analysis_scripts.models import StoryAllFinal
import analysis_scripts.constants as constants
import analysis_scripts.database.sql_wrapper as sql_wrapper

# LOGGER
from logger import get_logger
logger = get_logger(__name__)

def main():
    logger.info("Starting")
    # read the stories
    ok, records = sql_wrapper.sql_select(table_name=constants.TableNames.SCRAPE_STORIES_ALL_FINAL, dataclass=StoryAllFinal)
    logger.info("Ending")
    
if __name__ == "__main__":
    main()