import sys


from logger import get_logger
logger = get_logger(__name__)
from analysis_scripts.analysis import Analysis

def main() -> None:
    analysis = Analysis()
    analysis.run_analysis()

if __name__ == "__main__":
    main()