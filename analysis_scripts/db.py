import json
from pathlib import Path
import sys
from typing import Any, List, Optional, Tuple, Type, TypeVar, Union
try:
    # Pydantic v2
    from pydantic import BaseModel
except Exception:
    BaseModel = object  # type: ignore

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from analysis_scripts.jsonfileio import read_from_jsonl

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

T = TypeVar("T", bound=BaseModel)

def load_from_jsonl(
    input_dir: str = "",
    filename_prefix: str = "erase",
    model_type: Optional[Type[T]] = None,
) -> Tuple[bool, Optional[Union[Any, List[T]]]]:
    """
    Loads a JSON export for a given table and optionally rehydrates into Pydantic models.

    Filename convention:
        <input_dir>/<table_name>_<filename_prefix>.json
    (Change this one line if your convention differs.)

    Args:
        input_dir: optional folder containing JSON exports
        filename_prefix: suffix/prefix portion of filename
        model_type: optional Pydantic model class (v2) to parse into

    Returns:
        If model_type is None: (ok, raw_data)
        If model_type is provided: (ok, list[model_type])
    """
    # you can tune this naming convention:
    ok, raw = read_from_jsonl(input_dir=input_dir, filename_prefix=filename_prefix)
    if not ok or raw is None:
        return False, None

    if model_type is None:
        return True, raw

    # Normalize raw into a list of dicts for model_validate
    try:
        if isinstance(raw, list):
            items = raw
        else:
            # if file contains a single object, treat as one-item list
            items = [raw]

        models: List[T] = []
        for item in items:
            if not isinstance(item, dict):
                errmsg = f"Expected dict item for model parsing, got {type(item)}"
                logger.error(f"load_from_jsonl: {errmsg}")
                return False, None
            # Pydantic v2
            models.append(model_type.model_validate(item))  # type: ignore[attr-defined]

        return True, models

    except Exception as e:
        logger.error(f"load_from_jsonl: Excpetion {e}")
        return False, None

if __name__ == "__main__":
    ok, data = load_from_jsonl(filename_prefix='policy_new')
    
