# DB_NEON

# NEON_DELETE -> Tuple[bool, int]
# NEON_EXPORT_TABLE_TO_JSON -> Tuple[bool, int]

from collections.abc import Mapping as ABCMapping  # optional clarity
from contextlib import contextmanager
from datetime import datetime, date    
from dotenv import load_dotenv
import json
import os
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
import sys
from typing import Any, Tuple, Optional

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

from analysis_scripts import constants

# Dot environment
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[1] # CWD is CtrackAnalyze per your output
    ENV_PATH = PROJECT_ROOT / ".env"
    
    loaded = load_dotenv(dotenv_path=ENV_PATH, override=False)
    
    #print(f"load_dotenv loaded={loaded} path={ENV_PATH} exists={ENV_PATH.exists()}")
    #print("CWD:", os.getcwd())
    #print("DATABASE_URL present:", bool(os.getenv("DATABASE_URL")))
except:
    pass

_DB_CONN = None
_POOL: ThreadedConnectionPool | None = None

def init_db_pool(minconn: int = 1, maxconn: int = 10) -> None:
    """Initialize a per-process connection pool (safe for concurrent tasks)."""
    global _POOL
    if _POOL is not None:
        return

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")

    _POOL = ThreadedConnectionPool(
        minconn=minconn,
        maxconn=maxconn,
        dsn=database_url,
        # connect kwargs (psycopg2 passes these through)
        connect_timeout=10,
        options="-c statement_timeout=30000",
    )
    logger.info("Initialized Neon DB pool (min=%s max=%s)", minconn, maxconn)
    
def configure_db_pool_for_concurrency(max_concurrency: int, headroom: int = 1) -> None:
    """
    Ensure the Neon pool is initialized with enough connections for the intended concurrency.
    Idempotent: if already initialized, it won't resize (psycopg2 pools can't be resized).
    """
    desired_max = max(1, int(max_concurrency) + int(headroom))
    init_db_pool(minconn=1, maxconn=desired_max)

@contextmanager
def get_db_connection():
    """
    Context-managed pooled connection.
    Usage:
        with get_db_connection() as conn:
            ...
    Always returns the connection to the pool.
    """
    global _POOL
    if _POOL is None:
        # pick sane defaults; tune later
        init_db_pool(minconn=1, maxconn=10)

    conn = _POOL.getconn()
    try:
        yield conn
    finally:
        # IMPORTANT: return to pool; do not close
        _POOL.putconn(conn)

def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None
        logger.info("Closed Neon DB pool")

#def _create_connection(verbose: bool = False):
    #"""Establishes a PostgreSQL connection using DATABASE_URL."""
    #try:
        #database_url = os.getenv("DATABASE_URL")
        #if not database_url:
            #raise ValueError("DATABASE_URL environment variable is not set")

        #conn = psycopg2.connect(
            #database_url,
            #client_encoding="UTF8",
            #options="-c statement_timeout=30000",  # 30s max per statement
            #connect_timeout=10)

        #if verbose:
            #with conn.cursor() as cur:
                #cur.execute(
                    #"SELECT current_database(), current_user, version()")
                #result = cur.fetchone()
                #if result:
                    #db_name, db_user, version = result
                    #logger.info("=== CONNECTED TO DATABASE ===")
                    #logger.info(f"Database Name: {db_name}")
                    #logger.info(f"Database User: {db_user}")
                    #logger.info(f"PostgreSQL Version: {version}")
                    #logger.info("=============================")

        #return conn
    #except psycopg2.Error as e:
        #logger.critical(f"Database connection failed: {e}")
        #logger.critical(f"PG error code: {e.pgcode}, detail: {e.pgerror}")
        #raise
    #except Exception as e:
        #logger.critical(f"Database connection failed: {e}")
        #raise
    
class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder to handle datetime objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)
    
    from typing import Optional, Any, Tuple

########################################################################
# NEON_DELETE
# Returns Tuple[bool, int]
########################################################################
def neon_delete(
    table_name: str,
    where_column: Optional[str] = None,
    where_value: Optional[Any] = None
) -> Tuple[bool, int]:

    # Validate WHERE argument pairing
    if (where_column is None) ^ (where_value is None):
        errmsg =  "where_column and where_value must be provided together"
        logger.error(errmsg)
        raise ValueError(errmsg)

    try:
        with get_db_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if where_column is None:
                        # DELETE ALL
                        stmt = sql.SQL("DELETE FROM {table}").format(
                            table=sql.Identifier(table_name)
                        )
                        cur.execute(stmt)
                    else:
                        stmt = sql.SQL("DELETE FROM {table} WHERE {col} = %s").format(
                            table=sql.Identifier(table_name),
                            col=sql.Identifier(where_column),
                        )
                        cur.execute(stmt, (where_value,))

                conn.commit()
                return True, cur.rowcount

            except Exception:
                conn.rollback()  # IMPORTANT for pooled conns
                raise

    except Exception as e:
        logger.exception(f"neon_delete: error (table=%s) {table_name} {e}")
        return False, -1

########################################################################
# NEON_INSERT_RECORD
# Returns bool
########################################################################  
def neon_insert_record(
    table_name: str,
    data: Any,
    exclude_list: Optional[list[str]] = None, 
    commit: Optional[bool] = True,
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
                row[k] = Json(v)
    
            # jsonb array-of-objects (your impact_analysis case)
            elif isinstance(v, list) and any(isinstance(x, dict) for x in v):
                row[k] = Json(v)

    columns = list(row.keys())
    values = [row[c] for c in columns]

    try:
        with get_db_connection() as conn:
            try:
                stmt = sql.SQL("INSERT INTO {table} ({cols}) VALUES ({vals})").format(
                    table=sql.Identifier(table_name),
                    cols=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                    vals=sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )

                for k, v in row.items():
                    if isinstance(v, dict):
                        logger.error("RAW dict still present at key=%s", k)
                    if isinstance(v, list) and any(isinstance(x, dict) for x in v):
                        logger.error("LIST contains dicts at key=%s", k)

                with conn.cursor() as cur:
                    cur.execute(stmt, values)

                if commit:
                    conn.commit()

                logger.info("neon_insert_record: Inserted 1 row into %s", table_name)
                return True

            except Exception:
                # Critical for pooled connections: reset transaction state
                if commit:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise

    except Exception as e:
        errmsg = f"neon_insert_record: error encountered (table=%s) {table_name}. {e}"
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
# Returns Tuple[bool, list[dict[str,Any]]]
########################################################################  
def neon_select(
    table_name: str,
    where_clause: str | None = None,
    params: tuple | None = None,
    limit: Optional[int] = None,
    order_by: Optional[str] = None,
) -> tuple[bool, list[dict[str, Any]]]:
    """
    Reads records from a Neon (Postgres) table with an optional WHERE clause.

    Notes:
    - Uses pooled connections via `with get_db_connection() as conn:`
    - Safely quotes table/ORDER BY identifiers
    - `where_clause` is raw SQL (no Identifier quoting inside it); values must be parameterized via `params`
      Example: where_clause="source_url = %s AND status = %s", params=(url, 1)

    Returns:
        (ok, rows_as_dicts)
    """
    if where_clause and not params:
        # Allow where_clause without params only if caller truly has no placeholders,
        # but it's usually a bug. Keep this as a warning, not a hard error.
        logger.warning("neon_read_records: where_clause provided without params (table=%s)", table_name)

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Base statement
                stmt = sql.SQL("SELECT * FROM {table}").format(
                    table=sql.Identifier(table_name)
                )

                query_params: list[Any] = []

                # WHERE (raw SQL fragment)
                if where_clause:
                    stmt = stmt + sql.SQL(" WHERE ") + sql.SQL(where_clause)
                    if params:
                        query_params.extend(list(params))

                # ORDER BY (identifier only)
                if order_by:
                    if not order_by.isidentifier():
                        raise ValueError(f"Invalid order_by column name: {order_by}")
                    stmt = stmt + sql.SQL(" ORDER BY {col}").format(
                        col=sql.Identifier(order_by)
                    )

                # LIMIT
                if limit is not None:
                    stmt = stmt + sql.SQL(" LIMIT %s")
                    query_params.append(limit)

                cur.execute(stmt, query_params)
                rows = cur.fetchall()

                # RealDictCursor returns RealDictRow; normalize to dict
                data = [dict(r) for r in rows]
                return True, data

    except Exception:
        logger.exception("neon_read_records: failed reading records from %s", table_name)
        return False, []

    
# Run the function to create the table
if __name__ == "__main__":
    try:
        rows = neon_select(table_name=constants.TableNames.TBL_POLICIES_READONLY, limit=5)
        if not rows:
            logger.error("ERROR: no rows found")
            exit(0)
        logger.info(f"SUCCESS: {len(rows)} rows found. 5 expected")
    except Exception as e:
        print (f"error encountered. {e}")
   