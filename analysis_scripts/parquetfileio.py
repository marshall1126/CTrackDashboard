import pandas as pd
from pathlib import Path

def write_to_parquet(
    records: list,
    output_dir: str = "",
    filename_prefix: str = "erase"
) -> bool:
    """Write records to Parquet - handles all types automatically."""
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        file_path = output_path / f"{filename_prefix}.parquet"
        
        # Convert to DataFrame (handles datetime automatically)
        df = pd.DataFrame(records)
        df.to_parquet(file_path, index=False)
        
        return True
    except Exception as e:
        print(f"Failed to write Parquet: {e}")
        return False

def read_from_parquet(
    output_dir: str = "",
    filename_prefix: str = "erase"
) -> tuple[bool, list]:
    """Read from Parquet - datetime types preserved automatically."""
    try:
        file_path = Path(output_dir) / f"{filename_prefix}.parquet"
        
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return False, []
        
        df = pd.read_parquet(file_path)
        records = df.to_dict('records')  # List of dicts with correct types
        
        return True, records
    except Exception as e:
        print(f"Failed to read Parquet: {e}")
        return False, []