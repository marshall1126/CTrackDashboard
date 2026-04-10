import os
from enum import Enum

from logger import get_logger
logger = get_logger(__name__)

class Mode(str, Enum):
    DEVELOPMENT = "Development"
    PRODUCTION  = "Production"

# Change here or drive it from an env var (recommended)
def _detect_mode() -> Mode:
    # Railway always sets this env var
    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_GIT_COMMIT_SHA"):
        return Mode.PRODUCTION
    return Mode.DEVELOPMENT

MODE: Mode = _detect_mode()
logger.info(f'MODE: {MODE}')

TEST_TBL_STORIES_ALL_FINAL   = "test_stories_all_final"
TEST_SCRAPE_STORIES_ALL_FINAL = "test_scrape_stories_all_final"
TEST_TBL_POLICIES            = "test_policies"
TEST_TBL_POLICIES_ERRORS     = "test_policies_errors"
TEST_ANALYSIS_LAST_UPDT     = "test_analysis_last_updt"

class _TableNames:
    _tables = {
        Mode.DEVELOPMENT: dict(
            TBL_STORIES_ALL_FINAL   = TEST_TBL_STORIES_ALL_FINAL,
            SCRAPE_STORIES_ALL_FINAL = TEST_SCRAPE_STORIES_ALL_FINAL,
            TBL_POLICIES            = TEST_TBL_POLICIES,
            TBL_POLICIES_ERRORS     = TEST_TBL_POLICIES_ERRORS,
            TBL_ANALYSIS_LAST_UPDT =TEST_ANALYSIS_LAST_UPDT
        ),
        Mode.PRODUCTION: dict(
            TBL_STORIES_ALL_FINAL   = "stories_all_final",
            SCRAPE_STORIES_ALL_FINAL = "scrape_stories_all_final",
            TBL_POLICIES            = "policies",
            TBL_POLICIES_ERRORS     = "policies_errors",
            TBL_ANALYSIS_LAST_UPDT ='analysis_last_updt'
        ),
    }

    def __getattr__(self, name: str) -> str:
        try:
            return self._tables[MODE][name]
        except KeyError:
            raise AttributeError(f"No table constant '{name}' for mode '{MODE}'")

TableNames = _TableNames()