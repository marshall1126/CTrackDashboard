####################################################################################################
# DB_SUPA
####################################################################################################

# Supabase’s Python client is stateless and connectionless. Your current implementation is
# already correct and safe.You do not close a Supabase table or client.

#import sys
import asyncio
from pathlib import Path
import sys
from supabase import create_client, Client
import threading
from typing import Any, Optional, Tuple

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER
from logger import get_logger
logger = get_logger(__name__)

# LOCAL IMPORTS
from analysis_scripts import constants
from analysis_scripts.jsonfileio import read_from_jsonl
from enviro import EnvKey, get as get_env_value

# Constants
MAX_ROWS = 100

#try:
    #PROJECT_ROOT = Path(__file__).resolve().parents[1] # CWD is CtrackAnalyze per your output
    #ENV_PATH = PROJECT_ROOT / ".env"
    
    #loaded = load_dotenv(dotenv_path=ENV_PATH, override=False)
    
    ##print(f"load_dotenv loaded={loaded} path={ENV_PATH} exists={ENV_PATH.exists()}")
    ##print("CWD:", os.getcwd())
    ##print("SUPABASE_SERVICE_ROLE_KEY present:", bool(os.getenv("SUPABASE_SERVICE_ROLE_KEY")))
#except:
    #logger.error("Could not load environment variables")
    #raise

# Your Supabase credentials
# url = "https://ygarfocydkkpoejbngkz.supabase.co"
#print(f"URL: {url}")
#print(f"Key found: {key is not None}")
#print(f"Key length: {len(key) if key else 0}")
#print(f"All env vars containing SUPABASE: {[k for k in os.environ.keys() if 'SUPABASE' in k]}")

# Create Supabase client
_supabase: Client | None = None
_SUPA_SEM: asyncio.Semaphore | None = None
_SUPA_LOCK = threading.Lock()


##############################################################################################
## INIT_SUPA_CONCURRENCY
## COMMENTED OUT FOR FUTURE USE
##############################################################################################
#def init_supa_concurrency(max_concurrency: int = 8) -> None:
    #global _SUPA_SEM
    #if _SUPA_SEM is None:
        #_SUPA_SEM = asyncio.Semaphore(max_concurrency)
        #logger.info("Initialized Supabase concurrency gate (max=%s)", max_concurrency)

##############################################################################################
## _SUPA_CALL
## COMMENTED OUT FOR FUTURE USE
##############################################################################################        
#async def _supa_call(fn, *args, **kwargs):
    #"""
    #Run a blocking Supabase call in a thread, with concurrency limiting.
    #"""
    #init_supa_concurrency(8)
    #assert _SUPA_SEM is not None

    #async with _SUPA_SEM:
        #return await asyncio.to_thread(fn, *args, **kwargs)

#############################################################################################
# GET_SUPABASE
# Returns supa client
#############################################################################################        
def get_supabase() -> Client | None:
    """
        Returns a shared Supabase client.
        
        - On success: Returns the Client instance.
        - On failure (missing env vars, creation error): Logs details and returns None.
        - Callers must check for None before use.
    """
    
    global _supabase
    try:
        if _supabase is not None:
            return _supabase

        with _SUPA_LOCK:
            if _supabase is not None:
                return _supabase

            # key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            key = get_env_value(EnvKey.SUPABASE_SERVICE_ROLE_KEY)
            if not key:
                raise ValueError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set or is empty")

            logger.info("Creating Supabase client")
            url = get_env_value(EnvKey.SUPABASE_URL)
            _supabase = create_client(url, key)
            logger.info("Supabase client created")
            return _supabase

    except Exception as e:
        logger.error(f"get_supabase: error encountered. {e}")
        return None

###############################################################################################
# SUPA_DELETE_ONE_RECORD
# must be called from sync code
# Delete all records in the specified table
###############################################################################################    
def supa_delete_one_record(
    table_name: str,
    where_column: str,
    where_value: Any,
) -> bool:
    """
    Delete a single record from a Supabase table based on a WHERE clause.

    Args:
        table_name: Name of the table
        where_column: Column name for WHERE clause
        where_value: Value to match

    Returns:
        (ok, rows_deleted)
    """
    try:
        supabase = get_supabase()
        if not supabase:
            logger.error("supa_delete_one_record: No Supabase client")
            return False

        result = (
            supabase
            .table(table_name)
            .delete()
            .eq(where_column, where_value)
            .execute()
        )
        # Select it to confirm deletion
        resp = (
            supabase
            .table(table_name)
            .select(where_column)
            .eq(where_column, where_value)
            .limit(1)
            .execute()
        )
        
        rows = resp.data or []        

        if rows != []:
            logger.error(
                "supa_delete_one_record: Row not deleted from table %s when %s=%s",
                table_name, where_column, where_value
            )
            return False

        logger.info(
            "supa_delete_one_record: Deleted row from %s where %s=%s",
             table_name, where_column, where_value
        )
        return True

    except Exception:
        logger.exception("supa_delete_one_record: Error encountered")
        return False

###############################################################################################
# SUPA_DELETE_ALL_RECORDS
# Delete all records in the specified table
###############################################################################################
def supa_delete_all_records(table_name) -> Tuple[bool, int]:
    """
    Delete all records from a Supabase table and reset the id autoincrement sequence to 1.
    
    Args:
        table_name (str): Name of the table to clear
    """
    try:
        supabase = get_supabase()
        if not supabase:
            logger.error("supa_delete_all_records: No Supabase client")
            return False, -1        
        
        # Delete all records from the table
        results = supabase.rpc('delete_all_and_reset_id', {
            'table_name': table_name
            }).execute()
        
        row_count = 0
        if results.data and results.data['deleted_count']:
            row_count = results.data['deleted_count']
        logger.info(f"supa_delete_all_records. Deleted {row_count} rows from {results.data['table_name']}. id to start from 1")
        return True, row_count
    except Exception as e:
        logger.error(f"supa_delete_all_records: Error: {e}")
        return False, 0

##############################################################################
# SUPA_INSERT
##############################################################################
def supa_insert(
    table_name: str,
    rows: Any,
) -> tuple[bool, int]:
    """
    Insert one or more records into a Supabase table.

    Args:
        table_name: Supabase table name
        rows: dict (single row) or list[dict] (batch insert)

    Returns:
        (ok, rows_inserted)
    """
    try:
        supabase = get_supabase()
        if not supabase:
            logger.error("supa_insert: Supabase client is None")
            return False, 0

        resp = supabase.table(table_name).insert(rows).execute()

        inserted = len(resp.data) if resp.data else 0

        if inserted == 0:
            logger.warning(
                "supa_insert: Insert affected 0 rows (table=%s)",
                table_name,
            )
            return False, 0

        logger.info(
            "supa_insert: Inserted %d row(s) into %s",
            inserted,
            table_name,
        )
        return True, inserted

    except Exception:
        logger.exception(
            "supa_insert: error encountered (table=%s)",
            table_name,
        )
        return False, 0


##############################################################################
# SUPA_READ_RECORDS
##############################################################################    
def supa_read_records(
    table_name: str,
    where: dict | None = None,
    limit: int | None = None,
):
    """
    Read records from a Supabase table.

    :param table_name: name of the table
    :param where: optional dict of column=value filters (ANDed together)
    :param limit: optional max number of rows
    """
    try:
        supabase = get_supabase()
        if not supabase:
            logger.error("supa_read_records: Supabase client is None")
            return False, 0
        
        effective_limit = limit or MAX_ROWS
        logger.info(f"supa_read_records: Reading up to {effective_limit} rows from {table_name}...")

        query = supabase.table(table_name).select("*")

        if where:
            for col, val in where.items():
                query = query.eq(col, val)

        query = query.limit(effective_limit)

        resp = query.execute()

        rows = resp.data or []
        logger.info(f"Fetched {len(rows)} rows from {table_name}")
        return True, rows

    except Exception:
        logger.exception(f"Could not read records from {table_name}")
        return False, None
    
##############################################################################
# SUPA_SELECT
##############################################################################
#def supa_select(table_name, fields, where_conditions=None):
    #try:
        #supabase = get_supabase()
        
        #query = supabase.table(table_name).select(fields)

        ## Add where conditions if provided
        #if where_conditions:
            #for column, value in where_conditions.items():
                #query = query.eq(column, value)

        #response = query.execute()
        ## print(response.data)
        #return True, response.data

    #except Exception as e:
        #logger.error(f"Error during select from {table_name}: {e}")
        #raise e
    
##############################################################################
# SUPA_SELECT_COUNT
##############################################################################
def supa_select_count(table_name: str, where_conditions=None) -> Tuple[bool, int]:
    """
    Count the number of records that match the supplied where conditions.
    
    Args:
        table_name: Name of the table to query
        where_conditions: Optional dict of column-value pairs to filter by
        
    Returns:
        Integer count of matching records
        
    Example:
        ok: select returned normally
        count = supa_select_count("test_policies", {"status": "active"})
    """
    if not table_name:
        logger.error("supa_select_count: No table name")
        return False, -1
    
    try:
        supabase = get_supabase()
        
        # Use count="exact" to get the total count
        query = supabase.table(table_name).select("count", count="exact")
        
        # Add where conditions if provided
        if where_conditions:
            for column, value in where_conditions.items():
                query = query.eq(column, value)
        
        response = query.execute()
        
        # Return the count from the response
        return True, response.count if response.count is not None else 0
        
    except Exception as e:
        logger.error(f"Error during count from {table_name}: {e}")
        return False, -1
            
# Run the function to create the table
if __name__ == "__main__":
    table_name = constants.TableNames.TBL_STORIES_ALL_FINAL
    #result, records = read_records(table_name=table_name)
    #print (len(records))
    output_file = 'testdata/'+ table_name +'.jsonl'
    # status, record_count = export_table_to_jsonl(table_name=table_name, output_file=output_file)

    #supa_delete_all_records(table_name)
    #status, row_count = read_from_jsonl(table_name, input_file=output_file)
    ok, count = supa_select_count(table_name=table_name)
    print("TESTS DONE")