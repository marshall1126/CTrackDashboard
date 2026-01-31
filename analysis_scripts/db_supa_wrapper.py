from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict
import sys
from typing import Any, ClassVar, Optional, Sequence, Tuple

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

# LOCAL IMPORTS
from analysis_scripts import constants
from analysis_scripts.db_supa import supa_read_records, supa_insert

class StoryAllFinal(BaseModel):
    model_config = ConfigDict(extra="ignore")  # ignore columns you don't use
    FIELD_LINK: ClassVar[str] = "link"
    
    id: int
    title: str = ""
    date: datetime | None = None
    dept: str = ""
    link: str = ""
    storytext: str = ""
    title: str = ""
    success: bool

##################################################################################################
# NORMALIZE SUPA ROWS
##################################################################################################
def _normalize_supa_rows(rows: Any) -> list[dict]:
    """
    Normalize rows into list[dict] suitable for supabase.table(...).insert(...).

    Accepts:
      - Pydantic model (has model_dump)
      - dict
      - list/tuple of Pydantic models and/or dicts
    """
    if rows is None:
        raise ValueError("rows is None")

    # Single pydantic model
    if hasattr(rows, "model_dump"):
        return [rows.model_dump(mode="json")]

    # Single dict
    if isinstance(rows, dict):
        return [rows]

    # Batch (list/tuple). Avoid treating strings as sequences of chars.
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
        payload: list[dict] = []
        for idx, item in enumerate(rows):
            if hasattr(item, "model_dump"):
                payload.append(item.model_dump(mode="json"))
            elif isinstance(item, dict):
                payload.append(item)
            else:
                raise TypeError(f"Invalid row type at index {idx}: {type(item)}")
        return payload

    raise TypeError(f"Invalid rows type: {type(rows)}")

##################################################################################################
# SUPA_INSERT_TO_DB
##################################################################################################
def supa_insert_to_db(table_name: str, rows: Any) -> Tuple[bool, int]:
    if not rows:
        print("supa_insert_to_db: No records found")
        return False, 0
    if not table_name:
        print("supa_insert_to_db: No tablename")
        return False, 0
    try:
        payload = _normalize_supa_rows(rows)
        if not payload:
            logger.warning("supa_insert_to_db: empty payload (nothing to insert)")
            return True, 0

        ok, inserted = supa_insert(table_name, payload)
        if not ok:
            logger.error("supa_insert_to_db: insert failed (table=%s)", table_name)
            return False, 0

        logger.info("supa_insert_to_db: inserted %s row(s) into %s", inserted, table_name)
        return True, inserted
    except Exception as e:
        logger.error(f"supa_insert_to_db: Errpr encountered. {e}")
        return False, 0
    
def read_all_final_stories(limit: Optional[int] = None) -> tuple[bool, list[StoryAllFinal]]:
    try:
        table_name = constants.TableNames.TBL_STORIES_ALL_FINAL
        result, records = supa_read_records(table_name=table_name, limit=limit)
        if not result:
            return True, []
        stories: list[StoryAllFinal] = []
        for r in records:
            stories.append(StoryAllFinal.model_validate(r))
        return True, stories
            
    except Exception as e:
        logger.error(f"Error encountered. {e}")
        return False, None
    
# Run the function to create the table
if __name__ == "__main__":
    result, records = read_all_final_stories()
    print (len(records))
    # print ("HELLO")
    # select_supa(TBL_LAST_UPDATE, "*")
