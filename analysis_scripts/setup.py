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
from analysis_scripts.db_neon_wrapper import read_all
from analysis_scripts import constants
from analysis_scripts.db_neon_wrapper import Policies
from analysis_scripts.db_neon_pooler import neon_delete, neon_reset_identity
from analysis_scripts.db_supa import supa_delete_all_records
from analysis_scripts.db_supa_wrapper import StoryAllFinal, supa_insert_to_db
from analysis_scripts.jsonfileio import write_to_jsonl, read_from_jsonl

def read_policy():
    table_name = constants.TableNames.TBL_POLICIES
    where_clause = 'id= 19492'
    ok, record = read_all(table_name=table_name, model=Policies, where_clause=where_clause)
    if not ok:
        print(f"Could not record record with where clause {where_clause}")
        return False, None
    return ok, record

# DELETE AL THE RECORDS IN THE TABLE DATABASE
def setup1():
    table_name = constants.TEST_TBL_POLICIES
    ok, row_count = neon_delete(table_name)
    print(f"Deleted {row_count} from {table_name}")
    
# WRITE ERRORS DATA TO OUTPUT FILE    
def save_errors_data():
    table_name = constants.TEST_TBL_POLICIES_ERRORS
    output_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = write_to_jsonl(table_name=table_name, output_file=output_file)
    print(f"Exported {row_count} from {table_name}")

# WRITE SUPA DATA
def save_supa_data():
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
    table_name = constants.TEST_TBL_POLICIES_ERRORS
    # DELETE
    ok, row_count = neon_delete(table_name)
    print(f"delete status: {ok}")
    if ok:
        print(f"{row_count} records deleted")
    # READ
    input_file = "testdata/" + table_name + ".jsonl"
    ok, row_count = read_from_jsonl(table_name=table_name, input_file=input_file)
    print(f"Imported {row_count} from {table_name}")
    
# DELETE SCRAPED DATA FROM SUPA DB and REREAD SCRAPED DATA FROM OUTPUT FILE    
def reload_supa():
    try:
        table_name = constants.TEST_TBL_STORIES_ALL_FINAL
        # DELETE
        ok, row_count = supa_delete_all_records(table_name)
        print(f"delete status: {ok}")
        if not ok:
            print(f"reload_supa: Failed to delete records")
            return False
        print(f"{row_count} records deleted")
        # READ data from json
        input_dir = "testdata"
        filename_prefix = table_name
        ok, records = load_from_jsonl(input_dir=input_dir, filename_prefix=filename_prefix, model_type=StoryAllFinal)
        if not ok:
            print("Reload_supa: Failed to load records")
            return False
        if not records:
            print("Reload_supa: No records found")
            return False            
        ok, row_count = supa_insert_to_db(table_name, records)
        print(f"Imported {row_count} from {table_name}")
        return True
    except Exception as e:
        logger.error(f"reload_supa: Error encountered. {e}")
        return False

########################################################################
# RELOAD_ALL
# Returns Tuple[bool, int]
########################################################################        
def reload_all():
    try:
        # Load up scraping data
        reload_supa()
        # clear the errors table
        errors_table_name = constants.TEST_TBL_POLICIES_ERRORS
        ok, row_count = neon_delete(errors_table_name)
        if not ok:
            print (f"Could not clear errors table")
            return
        ok = neon_reset_identity(table_name=errors_table_name, id_column="id")
        if not ok:
            print (f"Could not reset table {errors_table_name}")
            return        
        print(f"Deleted {row_count} from {errors_table_name}")
        policies_table_name = constants.TEST_TBL_POLICIES
        ok, row_count = neon_delete(table_name=policies_table_name)
        if not ok:
            print (f"Could not clear policies table")
            return
        ok = neon_reset_identity(table_name=policies_table_name, id_column="id")
        if not ok:
            print (f"Could not reset table {policies_table_name}")
            return        
        print(f"Deleted {row_count} from {policies_table_name}")
        
    except Exception as e:
        print(f"Error encountered: {e}")
  
if __name__ == "__main__":
    reload_all()
    
    #ok, record = read_policy()
    
    #if not ok:
        #exit(0)
    
    #save_policy_to_json(record)
    #record = load_policy_from_json()
    