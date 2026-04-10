# test/test_dummy.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import analysis_scripts.database.sql_wrapper as sql_wrapper
from logger import get_logger
logger = get_logger(__name__)

# analysis_scripts/classes/Dummy.py
from pydantic import BaseModel, ConfigDict
from typing import Optional

class Dummy(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    id: Optional[int] = None
    name: str = ""

TABLE = "test_dummy"

def test_dummy():
    ok = sql_wrapper.connect()
    assert ok, "Failed to connect"
    
    # --- DELETE ALL ---
    logger.info("Deleting all records")
    ok, _ = sql_wrapper.sql_delete(TABLE)
    assert ok, "Failed to delete all records"

    # --- INSERT ---
    logger.info("Inserting Linda")
    ok = sql_wrapper.sql_insert(TABLE, {"name": "Linda"})
    assert ok, "Failed to insert Linda"

    logger.info("Inserting Paul")
    ok = sql_wrapper.sql_insert(TABLE, {"name": "Paul"})
    assert ok, "Failed to insert Paul"

    # --- SELECT ALL ---
    logger.info("Selecting all records")
    ok, records = sql_wrapper.sql_select(TABLE, dataclass=Dummy)
    assert ok, "Failed to select all records"
    assert len(records) == 2, f"Expected 2 records, got {len(records)}"
    logger.info(f"All records: {records}")

    # --- SELECT PAUL ---
    logger.info("Selecting Paul")
    ok, records = sql_wrapper.sql_select(TABLE, where={"name": "Paul"}, dataclass=Dummy)
    assert ok, "Failed to select Paul"
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"
    assert records[0].name == "Paul"
    logger.info(f"Paul: {records[0]}")

    # --- SELECT JOHN (should return empty) ---
    logger.info("Selecting John")
    ok, records = sql_wrapper.sql_select(TABLE, where={"name": "John"}, dataclass=Dummy)
    assert ok, "Failed to select John"
    assert len(records) == 0, f"Expected 0 records, got {len(records)}"
    logger.info("John not found, as expected")

    # --- DELETE PAUL ---
    logger.info("Deleting Paul")
    ok, _ = sql_wrapper.sql_delete(TABLE, where={"name": "Paul"})
    assert ok, "Failed to delete Paul"

    # --- SELECT ALL AFTER DELETE ---
    logger.info("Selecting all records after deleting Paul")
    ok, records = sql_wrapper.sql_select(TABLE, dataclass=Dummy)
    assert ok, "Failed to select all records after delete"
    assert len(records) == 1, f"Expected 1 record, got {len(records)}"
    assert records[0].name == "Linda"
    logger.info(f"Remaining records: {records}")
    
    # --- DELETE ALL ---
    logger.info("Deleting all records")
    ok, _ = sql_wrapper.sql_delete(TABLE)
    assert ok, "Failed to delete all records"    

    sql_wrapper.close()
    logger.info("All tests passed!")

if __name__ == "__main__":
    test_dummy()