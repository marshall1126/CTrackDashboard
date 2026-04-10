import os
from enum import Enum

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

TEST_TBL_STORIES_ALL_FINAL   = "test_stories_all_final"
TEST_SCRAPE_STORIES_ALL_FINAL = "test_scrape_stories_all_final"
TEST_TBL_POLICIES            = "test_policies"
TEST_TBL_POLICIES_ERRORS     = "test_policies_errors"

class _TableNames:
    _tables = {
        Mode.DEVELOPMENT: dict(
            TBL_STORIES_ALL_FINAL   = TEST_TBL_STORIES_ALL_FINAL,
            SCRAPE_STORIES_ALL_FINAL = TEST_SCRAPE_STORIES_ALL_FINAL,
            TBL_POLICIES            = TEST_TBL_POLICIES,
            TBL_POLICIES_ERRORS     = TEST_TBL_POLICIES_ERRORS,
        ),
        Mode.PRODUCTION: dict(
            TBL_STORIES_ALL_FINAL   = "stories_all_final",
            SCRAPE_STORIES_ALL_FINAL = "scrape_stories_all_final",
            TBL_POLICIES            = "policies",
            TBL_POLICIES_ERRORS     = "policies_errors",
        ),
    }

    def __getattr__(self, name: str) -> str:
        try:
            return self._tables[MODE][name]
        except KeyError:
            raise AttributeError(f"No table constant '{name}' for mode '{MODE}'")

TableNames = _TableNames()