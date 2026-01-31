PROD_TBL_POLICIES_ERRORS = 'policies_errors'
PROD_TBL_STORIES = "stories"
PROD_TBL_STORIES_ALL = "stories_all"
PROD_TBL_LAST_UPDATE = "last_updt_tbl"
PROD_TBL_STORIES_ALL_FINAL = "stories_all_final"

TEST_TBL_POLICIES = 'test_policies'
TEST_TBL_POLICIES_ERRORS =  'test_policies_errors'
TEST_TBL_STORIES_ALL_FINAL = "test_stories_all_final"

TBL_POLICIES_READ = "policies"

DEVELOPMENT_MODE = "Development"
PRODUCTION_MODE = "Production"

mode = DEVELOPMENT_MODE

class TableNames:
    if mode == DEVELOPMENT_MODE:
        TBL_STORIES_ALL_FINAL = TEST_TBL_STORIES_ALL_FINAL
        TBL_POLICIES_READONLY = 'policies'
        TBL_POLICIES_ERRORS = TEST_TBL_POLICIES_ERRORS
        TBL_POLICIES = TEST_TBL_POLICIES
    elif mode == PRODUCTION_MODE:
        TBL_STORIES_ALL_FINAL = '' # PROD_TBL_STORIES_ALL_FINAL
        TBL_POLICIES_READONLY = '' # 'policies'
        TBL_POLICIES_ERRORS = '' # PROD_TBL_POLICIES_ERRORS
        TBL_POLICIES = '' # 'policies'
    else:
        raise ValueError(f"Unknown mode: {mode}")