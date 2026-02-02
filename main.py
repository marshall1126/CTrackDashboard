import sys

from logger import get_logger
logger = get_logger(__name__)
from analysis_scripts.analysis import Analysis

from enviro import EnvKey, load_env

def main() -> None:
    # Load environment variables
    try:
        load_env()
    except Exception as e:
        logger.error(f"Could not load environement variables. {e}")
        return
    analysis = Analysis()
    analysis.run_analysis()
    logger.info("main: DONE")
    
if __name__ == "__main__":
    main()