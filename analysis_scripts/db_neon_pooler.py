# DB_NEON

# NEON_DELETE -> Tuple[bool, int]
# NEON_EXPORT_TABLE_TO_JSON -> Tuple[bool, int]

from collections.abc import Mapping as ABCMapping  # optional clarity
from contextlib import contextmanager
from datetime import datetime, date    
import json
import os
from pathlib import Path
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import sys
from typing import Any, Tuple, Optional

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

# PROJECT INCLUDES
from analysis_scripts import constants
from enviro import EnvKey, get as get_env_value

@contextmanager
def get_db_connection():
    """
    Uses Neon's pooled endpoint — open/close per use; PgBouncer handles reuse.
    """
    conn = psycopg.connect(
        get_env_value(EnvKey.NEON_DATABASE_URL_POOLER),  # your pooled URL
        connect_timeout=15,  # buffer for Neon cold starts
    )
    try:
        # If you need statement_timeout (or other session params)
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 20000;")
        yield conn
        conn.commit()  # optional auto-commit on success path
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()  # ← release immediately so PgBouncer can reuse

########################################################################
# NEON_DELETE
# Returns Tuple[bool, int]
########################################################################
def neon_delete(
    table_name: str,
    where_clause: Optional[str] = None,
    params: Optional[Any] = None
) -> Tuple[bool, int]:

    # Validate WHERE argument pairing
    if (where_clause is None) ^ (params is None):
        errmsg =  "where_column and where_value must be provided together"
        logger.error(errmsg)
        raise ValueError(errmsg)

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                stmt = sql.SQL("DELETE FROM {}").format(sql.Identifier(table_name))

                query_params = list(params) if params else []

                if where_clause:
                    stmt += sql.SQL(" WHERE ") + sql.SQL(where_clause)

                cur.execute(stmt, query_params)

                # rowcount is available immediately after execute for DML
                deleted = cur.rowcount                

        return True, deleted

    except Exception as e:
        logger.exception("neon_delete failed", extra={"table": table_name, "err": e})
        return False, -1

########################################################################
# NEON_INSERT_RECORD
# Returns bool
########################################################################  
def neon_insert_record(
    table_name: str,
    data: Any,
    exclude_list: Optional[list[str]] = None, 
    auto_json: Optional[bool] = True,   # wrap dict values into Json(...) automatically
) -> bool:
    """
    Insert a single record into a PostgreSQL table using exactly the fields/values in `data`.

    - Uses pooled connections via `with get_db_connection() as conn:`
    - Quotes identifiers safely (table + columns)
    - Optionally wraps dict values as JSONB via psycopg2.extras.Json
    - Ensures rollback on failure so pooled connections are not returned in an aborted state
    """
    
    if exclude_list is None:
        exclude_list = []
        
    # Normalize input ONCE
    if hasattr(data, "model_dump"):
        row = data.model_dump(exclude_none=True, by_alias=True)
    elif isinstance(data, ABCMapping):
        row = dict(data)
    else:
        errmsg = f"neon_insert_record: data must be Mapping or Pydantic model, got {type(data)}"
        logger.error(errmsg)
        raise TypeError(errmsg)

    if not row:
        errmsg = "neon_insert_record: No data provided for insert"
        logger.error(errmsg)
        raise ValueError(errmsg)
    
    for idx, excl in enumerate(exclude_list, 1):
        row.pop(excl, None)

    # Optional auto-wrapping for JSON-ish values (dict -> Json)
    # NOTE: Do NOT auto-wrap lists here; many of your *_tags fields are text[].
    if auto_json:
        for k, v in list(row.items()):
            # jsonb object
            if isinstance(v, dict):
                row[k] = Jsonb(v)
    
            # jsonb array-of-objects (your impact_analysis case)
            elif isinstance(v, list) and any(isinstance(x, dict) for x in v):
                row[k] = Jsonb(v)

    columns = list(row.keys())
    values = [row[c] for c in columns]

    try:
        with get_db_connection() as conn:
            stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )

            for k, v in row.items():
                if isinstance(v, dict):
                    logger.error("RAW dict still present at key=%s", k)
                if isinstance(v, list) and any(isinstance(x, dict) for x in v):
                    logger.error("LIST contains dicts at key=%s", k)

            with conn.cursor() as cur:
                cur.execute(stmt, values)

        logger.info("neon_insert_record: Inserted 1 row into %s", table_name)
        return True

    except Exception as e:
        errmsg = f"neon_insert_record failed (table={table_name}): {e}"
        logger.exception(errmsg)
        return False

########################################################################
# NEON_RESET_IDENTITY
# Returns bool
# ########################################################################      
def neon_reset_identity(
    table_name: str,
    id_column: str,
    reset_val: Optional[int] = 1
) -> bool:
    """
    Reset an auto-incrementing ID column so the next insert starts at a specified value.
    Works for SERIAL / BIGSERIAL columns backed by a sequence.
    
    Args:
        table_name: target table
        id_column: identity/serial column name
        reset_val: value to restart the sequence at (default: 1)
    Returns:
        True on success, False on failure
    """
    if not id_column or not table_name:
        logger.error("neon_reset_identity: table_name and id_column are required")
        return False
    
    if reset_val is None:
        reset_val = 1
    if not isinstance(reset_val, int) or reset_val < 1:
        logger.error("neon_reset_identity: reset_val must be a positive integer")
        return False
    
    try:
        with get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Get column information
                    cur.execute("""
                        SELECT column_default, is_identity
                        FROM information_schema.columns
                        WHERE table_name = %s AND column_name = %s
                    """, (table_name, id_column))
                    
                    result = cur.fetchone()
                    if not result:
                        logger.error("Column %s.%s does not exist", table_name, id_column)
                        return False
                    
                    column_default, is_identity = result
                    seq_name = None
                    
                    # Handle IDENTITY columns
                    if is_identity == 'YES':
                        stmt = sql.SQL(
                            "ALTER TABLE {} ALTER COLUMN {} RESTART WITH {}"
                        ).format(
                            sql.Identifier(table_name),
                            sql.Identifier(id_column),
                            sql.Literal(reset_val)
                        )
                        cur.execute(stmt)
                        logger.info(
                            "neon_reset_identity: reset IDENTITY column %s.%s to %s",
                            table_name, id_column, reset_val
                        )
                        conn.commit()
                        return True
                    
                    # For SERIAL columns, extract sequence name from column_default
                    if column_default and 'nextval' in column_default:
                        # Extract sequence name from: nextval('test_policies_id_seq'::regclass)
                        import re
                        match = re.search(r"nextval\('([^']+)'", column_default)
                        if match:
                            seq_name = match.group(1)
                    
                    # Fallback: try pg_get_serial_sequence
                    if not seq_name:
                        cur.execute(
                            "SELECT pg_get_serial_sequence(%s, %s)",
                            (table_name, id_column)
                        )
                        seq_result = cur.fetchone()
                        seq_name = seq_result[0] if seq_result else None
                    
                    if not seq_name:
                        logger.error(
                            "neon_reset_identity: No sequence found for %s.%s",
                            table_name, id_column
                        )
                        return False
                    
                    # Reset the sequence
                    stmt = sql.SQL(
                        "ALTER SEQUENCE {} RESTART WITH {}"
                    ).format(
                        sql.Identifier(seq_name),
                        sql.Literal(reset_val)
                    )
                    cur.execute(stmt)
                    
                    logger.info(
                        "neon_reset_identity: reset sequence %s for %s.%s to %s",
                        seq_name, table_name, id_column, reset_val
                    )
                    
                conn.commit()
                return True
                
            except Exception:
                conn.rollback()
                raise
                
    except Exception:
        logger.exception(
            "neon_reset_identity: error resetting identity (table=%s, column=%s)",
            table_name, id_column
        )
        return False

########################################################################
# NEON_SELECT
# Returns Tuple[bool, []]
########################################################################
def neon_select(
    table_name: str,
    where_clause: Optional[str | None] = None,
    params: Optional[tuple | None] = None,
    limit: int | None = None,
    order_by: str | None = None,
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Fetch records from a Neon/PostgreSQL table.
    
    SECURITY IMPORTANT:
    where_clause is inserted literally → caller MUST:
    - use only %s placeholders
    - NEVER interpolate user input into where_clause
    - pass all values via params tuple
    """
    if not where_clause or not params:
        where_clause = None
        params = None
        
    if where_clause and not params and '%' not in (where_clause or ''):
        logger.warning("where_clause without placeholders – possible bug", extra={"table": table_name})

    try:
        with get_db_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                stmt = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                query_params = list(params or ())

                if where_clause:
                    stmt += sql.SQL(" WHERE ") + sql.SQL(where_clause)

                if order_by:
                    # Very basic validation – improve based on your needs
                    if ' --' in order_by or ';' in order_by:
                        raise ValueError("Suspicious characters in order_by")
                    stmt += sql.SQL(" ORDER BY {}").format(sql.SQL(order_by))

                if limit is not None:
                    if not isinstance(limit, int) or limit < 0:
                        raise ValueError("limit must be non-negative integer")
                    stmt += sql.SQL(" LIMIT %s")
                    query_params.append(limit)

                cur.execute(stmt, query_params)
                rows = cur.fetchall()
                return True, [dict(row) for row in rows]

    except Exception:
        logger.exception("neon_select failed", extra={"table": table_name})
        return False, []
    
########################################################################
# NEON_UPDATE
# Returns Tuple[bool, int]
########################################################################
def neon_update(
    table_name: str,
    data: dict[str, Any],
    where_clause: Optional[str] = None,
    params: Optional[Any] = None
) -> Tuple[bool, int]:
    """
    Update records in table.
    
    Args:
        table_name: Table to update
        data: Dict of {column: value} to SET
        where_clause: WHERE condition with %s placeholders
        params: Tuple/list of values for WHERE placeholders
    
    Returns:
        (success, rows_updated)
    """
    # Validate WHERE argument pairing
    if (where_clause is None) ^ (params is None):
        errmsg = "where_clause and params must be provided together"
        logger.error(errmsg)
        raise ValueError(errmsg)
    
    if not data:
        logger.error("data dict cannot be empty")
        return False, 0
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Build SET clause
                set_parts = [sql.SQL("{} = %s").format(sql.Identifier(col)) for col in data.keys()]
                set_clause = sql.SQL(", ").join(set_parts)
                
                # Build full statement
                stmt = sql.SQL("UPDATE {} SET {}").format(
                    sql.Identifier(table_name),
                    set_clause
                )
                
                # Combine SET values + WHERE params
                query_params = list(data.values())
                if where_clause:
                    stmt += sql.SQL(" WHERE ") + sql.SQL(where_clause)
                    query_params.extend(list(params) if params else [])
                
                cur.execute(stmt, query_params)
                updated = cur.rowcount
                
        return True, updated
        
    except Exception as e:
        logger.exception("neon_update failed", extra={"table": table_name, "err": e})
        return False, -1

# Run the function to create the table
if __name__ == "__main__":
    table_name = 'test_dummy_table'
    from pydantic import BaseModel, ConfigDict
    class DummyData(BaseModel):
        """Pydantic model with default empty lists instead of None."""
        model_config = ConfigDict(validate_assignment=True)
        
        id: int = 1
        name: str = ''
        
    try:
        
        with get_db_connection() as conn:
            print ("HERE")
            dummyData = DummyData()
            import random
            number = random.randint(1, 1000000000)        
            dummyData.id = number
            dummyData.name = f"Text for {number}"
            ok = neon_insert_record(table_name='test_dummy_table', data=dummyData)
            print (f"Insert result: {'Passed' if ok else 'Failed'}")
            ok, result = neon_select(table_name='test_dummy_table', where_clause='id=%s', params=(number, ))
            print (f"Select result: {'Passed' if ok else 'Failed'}")
            ok, result = neon_delete(table_name='test_dummy_table', where_clause='id=%s', params=(number,))
            print (f"Delete result: {'Passed' if ok else 'Failed'}")            
            pass
    except Exception as e:
        print (e)
    pass