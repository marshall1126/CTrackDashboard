from pathlib import Path
import sys

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

from analysis_scripts.db import load_from_jsonl
from analysis_scripts import constants
from analysis_scripts.jsonfileio import write_to_jsonl, read_from_jsonl
from analysis_scripts.models import StoryAllFinal
from analysis_scripts.database.neon_manager import NeonManager
from analysis_scripts.models import Policies

def read_policy():
    db_manager = NeonManager()
    db_manager.db_connect()
    table_name = constants.TableNames.TBL_POLICIES
    where_clause = 'id= 19492'
    ok, record = db_manager.db_select(table_name=table_name, model=Policies, where_clause=where_clause)
    if not ok:
        print(f"Could not record record with where clause {where_clause}")
        return False, None
    db_manager.db_close()
    return ok, record

# DELETE AL THE RECORDS IN THE TABLE DATABASE
def setup1():
    db_manager = NeonManager()
    db_manager.db_connect()    
    table_name = constants.TEST_TBL_POLICIES
    ok, row_count = db_manager.db_delete(table_name)
    print(f"Deleted {row_count} from {table_name}")
    
# WRITE ERRORS DATA TO OUTPUT FILE    
def save_errors_data():
    table_name = constants.TEST_TBL_POLICIES_ERRORS
    output_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = write_to_jsonl(table_name=table_name, output_file=output_file)
    print(f"Exported {row_count} from {table_name}")

# WRITE DATA
def save_data():
    table_name = constants.TEST_TBL_STORIES_ALL_FINAL
    output_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = write_to_jsonl(table_name=table_name, output_file=output_file)
    print(f"Exported {row_count} from {table_name}")

# WRITE ERRORS DATA TO OUTPUT FILE    
def save_errors_data():
    table_name = constants.TEST_TBL_POLICIES_ERRORS
    output_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = write_to_jsonl(table_name=table_name, output_file=output_file)
    print(f"Exported {row_count} from {table_name}")
    
########################################################################
# RELOAD_ERRORS
# Delete errors data from DB and REREAD ERRORS DATA FROM OUTPUT FILE
########################################################################    
def reload_errors():
    db_manager = NeonManager()
    db_manager.db_connect()    
    table_name = constants.TEST_TBL_POLICIES_ERRORS
    # DELETE
    ok, row_count = db_manager.db_delete(table_name)
    print(f"delete status: {ok}")
    if ok:
        print(f"{row_count} records deleted")
    db_manager.db_close()
    # READ
    input_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = read_from_jsonl(table_name=table_name, input_file=input_file)
    print(f"Imported {row_count} from {table_name}")
    
# DELETE SCRAPED DATA FROM SUPA DB and REREAD SCRAPED DATA FROM OUTPUT FILE    
def reload_db():
    try:
        db_manager = NeonManager()
        db_manager.db_connect()        
        table_name = constants.TEST_SCRAPE_STORIES_ALL_FINAL
        # DELETE
        ok, row_count = db_manager.db_delete(table_name)
        print(f"delete status: {ok}")
        if not ok:
            print(f"reload_db: Failed to delete records")
            return False
        
        print(f"{row_count} records deleted")
        # READ data from json
        input_dir = "testdata"
        filename_prefix = table_name
        ok, records = load_from_jsonl(input_dir=input_dir, filename_prefix=filename_prefix, model_type=StoryAllFinal)
        if not ok:
            print("Reload_db: Failed to load records")
            return False
        if not records:
            print("Reload_db: No records found")
            return False            
        ok, row_count = db_manager.db_insert(table_name, records)
        print(f"Imported {row_count} from {table_name}")
        db_manager.db_close()
        return True
    except Exception as e:
        logger.error(f"Error encountered. {e}")
        return False

########################################################################
# RELOAD_ALL
# Returns Tuple[bool, int]
########################################################################        
def reload_all():
    try:
        db_manager = NeonManager()
        db_manager.db_connect()                
        
        # Load up scraping data
        reload_db()
        # clear the errors table
        errors_table_name = constants.TEST_TBL_POLICIES_ERRORS
        ok, row_count = db_manager.db_delete(errors_table_name)
        if not ok:
            print (f"Could not clear errors table")
            return
        ok = db_manager.db_reset_identity(table_name=errors_table_name, id_column="id")
        if not ok:
            print (f"Could not reset table {errors_table_name}")
            return        
        print(f"Deleted {row_count} from {errors_table_name}")
        policies_table_name = constants.TEST_TBL_POLICIES
        ok, row_count = db_manager.db_delete(table_name=policies_table_name)
        if not ok:
            print (f"Could not clear policies table")
            return
        ok = db_manager.db_reset_identity(table_name=policies_table_name, id_column="id")
        if not ok:
            print (f"Could not reset table {policies_table_name}")
            return        
        print(f"Deleted {row_count} from {policies_table_name}")
        db_manager.db_close()
    except Exception as e:
        print(f"Error encountered: {e}")
  
if __name__ == "__main__":
    reload_all()
    
    #ok, record = read_policy()
    
    #if not ok:
        #exit(0)
    
    #save_policy_to_json(record)
    #record = load_policy_from_json()
    