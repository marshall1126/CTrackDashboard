import json
from pathlib import Path
import sys
from typing import Any, Optional, Tuple

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

##################################################################################
# READ_FROM_JSONL
##################################################################################
import json
from pathlib import Path
from typing import Any, Optional, Tuple, List

def read_from_jsonl(
    input_dir: str = "",
    filename_prefix: str = "erase"
) -> Tuple[bool, Optional[List[Any]]]:
    """
    Load records from a JSONL file.

    Returns:
        (ok, list_of_objects)
    """
    try:
        file_path = Path(input_dir) / f"{filename_prefix}.jsonl"

        if not file_path.exists():
            print(f"JSONL file not found: {file_path}")
            return False, None

        records: List[Any] = []

        with file_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"JSONL parse error at line {line_no}: {e}")
                    return False, None

        return True, records

    except Exception as e:
        print(f"Failed to load JSONL: {e}")
        return False, None

##################################################################################
# WRITE TO JSONL    
##################################################################################
def write_to_jsonl(
    record: Any,
    output_dir: str = "",
    filename_prefix: str = "erase"
) -> bool:
    """
    Write record(s) to a JSONL file (one JSON object per line).
    Args:
        record: Pydantic model OR list of Pydantic models
        output_dir: target directory
        filename_prefix: output filename prefix (without extension)
    Returns:
        True on success, False on failure
    """
    try:
        records = record if isinstance(record, list) else [record]
        if not records:
            return False
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / f"{filename_prefix}.jsonl"
        
        with file_path.open("a", encoding="utf-8") as f:
            for obj in records:
                if hasattr(obj, "model_dump"):
                    data = obj.model_dump(mode="json")
                else:
                    data = obj
                # Add default=str to handle datetime objects
                f.write(json.dumps(data, ensure_ascii=False, default=str))
                f.write("\n")
        
        return True
        
    except Exception as e:
        print(f"Failed to write JSONL: {e}")
        return False

 
if __name__ == "__main__":
    ok, data = read_from_jsonl(filename_prefix='policy_new')
    
