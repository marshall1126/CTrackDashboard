#  main.py

import asyncio
import sys
import threading

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
    
    logger.info("Threads alive at exit: %s",
                [t.name for t in threading.enumerate()])
    
    try:
        loop = asyncio.get_running_loop()
        logger.info("Event loop still running: %s", loop)
    except RuntimeError:
        logger.info("No running asyncio loop")
        
    logger.info("main: DONE")
    
if __name__ == "__main__":
    main()