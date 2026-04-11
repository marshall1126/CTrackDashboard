# analysis_scripts/database/neon_manager.py

from collections.abc import Mapping as ABCMapping
from enum import Enum
from typing import Any, Optional
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import re

from logger import get_logger
from enviro import get as get_env_value, EnvKey
from analysis_scripts.database.base_database import BaseDatabaseManager, T

logger = get_logger(__name__)

class NeonConnectionMode(Enum):
    POOLER = "pooler"
    DIRECT = "direct"

class NeonManager(BaseDatabaseManager):
    def __init__(self, mode: NeonConnectionMode = NeonConnectionMode.POOLER):
        self._mode = mode
        self._conn: Optional[psycopg.Connection] = None

    def __repr__(self) -> str:
        return f"NeonManager(mode={self._mode.value}, connected={self._is_connected})"

    # ---------------------------------------------------------------
    # PRIVATE HELPERS
    # ---------------------------------------------------------------

    @property
    def _is_connected(self) -> bool:
        return self._conn is not None and not self._conn.closed

    def _get_connection_string(self) -> str:
        if self._mode == NeonConnectionMode.POOLER:
            return get_env_value(EnvKey.NEON_DATABASE_URL_POOLER)
        return get_env_value(EnvKey.NEON_DATABASE_URL)

    def _ensure_connected(self) -> bool:
        try:
            if self._is_connected:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
        except Exception:
            logger.warning("connection lost — reconnecting")
            self._conn = None
        return self.db_connect()

    # ---------------------------------------------------------------
    # DB_CLOSE
    # ---------------------------------------------------------------
    def db_close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
                logger.info("NeonManager connection closed")
            except Exception as e:
                logger.exception("db_close failed: %s", e)
            finally:
                self._conn = None

    # ---------------------------------------------------------------
    # DB_COMMIT
    # ---------------------------------------------------------------
    def db_commit(self) -> bool:
        try:
            if not self._ensure_connected():
                logger.error("db_commit: not connected")
                return False
            self._conn.commit()
            return True
        except Exception as e:
            logger.exception("db_commit failed: %s", e)
            self._conn.rollback()
            return False

    # ---------------------------------------------------------------
    # DB_CONNECT
    # ---------------------------------------------------------------
    def db_connect(self) -> bool:
        try:
            self._conn = psycopg.connect(
                self._get_connection_string(),
                connect_timeout=15,
            )
            with self._conn.cursor() as cur:
                cur.execute("SET statement_timeout = 20000;")
            logger.info("NeonManager connected (mode=%s)", self._mode.value)
            return True
        except Exception as e:
            logger.exception("db_connect failed: %s", e)
            return False
    
    # ---------------------------------------------------------------
    # DB_COUNT
    # ---------------------------------------------------------------    
    def db_count(self,
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        try:
            if not self._ensure_connected():
                logger.error("db_count: not connected")
                return False, 0
    
            stmt = sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
            params = []
    
            if where:
                conditions = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(col))
                    for col in where
                )
                stmt += sql.SQL(" WHERE ") + conditions
                params = list(where.values())
    
            with self._conn.cursor() as cur:
                cur.execute(stmt, params)
                count = cur.fetchone()[0]
            return True, count
    
        except Exception as e:
            logger.exception("db_count failed (table=%s): %s", table_name, e)
            return False, 0    

    # ---------------------------------------------------------------
    # DB_DELETE
    # ---------------------------------------------------------------
    def db_delete(
        self,
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        try:
            if not self._ensure_connected():
                logger.error("db_delete: not connected")
                return False, -1

            with self._conn.cursor() as cur:
                stmt = sql.SQL("DELETE FROM {table}").format(
                    table=sql.Identifier(table_name)
                )
                params = []
                if where:
                    conditions = sql.SQL(" AND ").join(
                        sql.SQL("{} = %s").format(sql.Identifier(col))
                        for col in where
                    )
                    stmt += sql.SQL(" WHERE ") + conditions
                    params = list(where.values())
                cur.execute(stmt, params)
                deleted = cur.rowcount
            self._conn.commit()
            return True, deleted
        except Exception as e:
            logger.exception("db_delete failed (table=%s): %s", table_name, e)
            self._conn.rollback()
            return False, -1

    # ---------------------------------------------------------------
    # DB_INSERT
    # ---------------------------------------------------------------
    def db_insert(
        self,
        table_name: str,
        data: Any,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
        commit: Optional[bool] = True,
    ) -> bool:
        try:
            if not self._ensure_connected():
                logger.error("db_insert: not connected")
                return False

            if exclude_list is None:
                exclude_list = []

            if hasattr(data, "model_dump"):
                row = data.model_dump(exclude_none=True, by_alias=True)
            elif isinstance(data, ABCMapping):
                row = dict(data)
            else:
                raise TypeError(f"data must be Mapping or Pydantic model, got {type(data)}")

            if not row:
                raise ValueError("No data provided for insert")

            for excl in exclude_list:
                row.pop(excl, None)

            if auto_json:
                for k, v in list(row.items()):
                    if isinstance(v, dict):
                        row[k] = Jsonb(v)
                    elif isinstance(v, list) and any(isinstance(x, dict) for x in v):
                        row[k] = Jsonb(v)

            columns = list(row.keys())
            values = [row[c] for c in columns]

            stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )

            with self._conn.cursor() as cur:
                cur.execute(stmt, values)
            if commit:
                self._conn.commit()

            logger.debug("db_insert: inserted 1 row into %s", table_name)
            return True

        except Exception as e:
            logger.exception("db_insert failed (table=%s): %s", table_name, e)
            if commit:
                self._conn.rollback()
            return False

    # ---------------------------------------------------------------
    # DB_INSERT_BATCH
    # ---------------------------------------------------------------
    def db_insert_batch(
        self,
        table_name: str,
        data: list[Any],
        commit: bool = True,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
    ) -> tuple[bool, int]:
        if not data:
            logger.warning("db_insert_batch: no data provided")
            return True, 0

        inserted = 0
        for item in data:
            if not self._ensure_connected():
                logger.error("db_insert_batch: could not reconnect")
                return False, inserted
            ok = self.db_insert(
                table_name=table_name,
                data=item,
                commit=commit,
                exclude_list=exclude_list,
                auto_json=auto_json,
            )
            if ok:
                inserted += 1
                if inserted % 10 == 0:
                    logger.info("db_insert_batch: inserted %d of %d", inserted, len(data))
            else:
                logger.error("db_insert_batch: failed on row %d into %s", inserted + 1, table_name)

        logger.info("db_insert_batch: inserted %d/%d row(s) into %s", inserted, len(data), table_name)
        return True, inserted

    # ---------------------------------------------------------------
    # DB_REFRESH
    # ---------------------------------------------------------------
    def db_refresh(self) -> bool:
        logger.info("NeonManager refreshing connection (mode=%s)", self._mode.value)
        self.db_close()
        return self.db_connect()

    # ---------------------------------------------------------------
    # DB_RESET_IDENTITY
    # ---------------------------------------------------------------
    def db_reset_identity(
        self,
        table_name: str,
        identity_col: str,
        reset_val: Optional[int] = 1,
    ) -> bool:
        try:
            if not self._ensure_connected():
                logger.error("db_reset_identity: not connected")
                return False

            if not identity_col or not table_name:
                logger.error("table_name and identity_col are required")
                return False

            if reset_val is None:
                reset_val = 1
            if not isinstance(reset_val, int) or reset_val < 1:
                logger.error("reset_val must be a positive integer")
                return False

            with self._conn.cursor() as cur:
                cur.execute("""
                    SELECT column_default, is_identity
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = %s
                """, (table_name, identity_col))

                result = cur.fetchone()
                if not result:
                    logger.error("Column %s.%s does not exist", table_name, identity_col)
                    return False

                column_default, is_identity = result

                if is_identity == 'YES':
                    stmt = sql.SQL(
                        "ALTER TABLE {} ALTER COLUMN {} RESTART WITH {}"
                    ).format(
                        sql.Identifier(table_name),
                        sql.Identifier(identity_col),
                        sql.Literal(reset_val)
                    )
                    cur.execute(stmt)
                    self._conn.commit()
                    logger.info("reset IDENTITY column %s.%s to %s", table_name, identity_col, reset_val)
                    return True

                seq_name = None
                if column_default and 'nextval' in column_default:
                    match = re.search(r"nextval\('([^']+)'", column_default)
                    if match:
                        seq_name = match.group(1)

                if not seq_name:
                    cur.execute("SELECT pg_get_serial_sequence(%s, %s)", (table_name, identity_col))
                    seq_result = cur.fetchone()
                    seq_name = seq_result[0] if seq_result else None

                if not seq_name:
                    logger.error("No sequence found for %s.%s", table_name, identity_col)
                    return False

                stmt = sql.SQL("ALTER SEQUENCE {} RESTART WITH {}").format(
                    sql.Identifier(seq_name),
                    sql.Literal(reset_val)
                )
                cur.execute(stmt)
                self._conn.commit()
                logger.info("reset sequence %s for %s.%s to %s", seq_name, table_name, identity_col, reset_val)
                return True

        except Exception:
            logger.exception("db_reset_identity failed (table=%s, column=%s)", table_name, identity_col)
            self._conn.rollback()
            return False

    # ---------------------------------------------------------------
    # DB_SELECT
    # ---------------------------------------------------------------
    def db_select(
        self,
        table_name: str,
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        order_dir: Optional[str] = "ASC",  # ← add this
        dataclass: Optional[type[T]] = None,
    ) -> tuple[bool, list[Any]]:
        try:
            if not self._ensure_connected():
                logger.error("db_select: not connected")
                return False, []

            stmt = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
            query_params = []

            if where:
                conditions = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(col))
                    for col in where
                )
                stmt += sql.SQL(" WHERE ") + conditions
                query_params.extend(where.values())

            if order_by:
                if ' --' in order_by or ';' in order_by:
                    raise ValueError("Suspicious characters in order_by")
                direction = "DESC" if order_dir and order_dir.upper() == "DESC" else "ASC"
                stmt += sql.SQL(" ORDER BY {} {}").format(sql.SQL(order_by), sql.SQL(direction))

            if limit is not None:
                if not isinstance(limit, int) or limit < 0:
                    raise ValueError("limit must be a non-negative integer")
                stmt += sql.SQL(" LIMIT %s")
                query_params.append(limit)

            with self._conn.cursor(row_factory=dict_row) as cur:
                cur.execute(stmt, query_params)
                rows = [dict(row) for row in cur.fetchall()]

            if dataclass is not None:
                return True, [dataclass.model_validate(r) for r in rows]
            return True, rows

        except Exception as e:
            logger.exception("db_select failed (table=%s): %s", table_name, e)
            return False, []

    # ---------------------------------------------------------------
    # DB_SELECT_RANGE
    # ---------------------------------------------------------------
    def db_select_range(
        self,
        table_name: str,
        column: str,
        min_val: Any,
        max_val: Any,
        select_cols: Optional[list[str]] = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        try:
            if not self._ensure_connected():
                logger.error("db_select_range: not connected")
                return False, []

            cols = (
                sql.SQL(", ").join(sql.Identifier(c) for c in select_cols)
                if select_cols
                else sql.SQL("*")
            )
            stmt = sql.SQL("SELECT {} FROM {} WHERE {} BETWEEN %s AND %s").format(
                cols,
                sql.Identifier(table_name),
                sql.Identifier(column),
            )
            with self._conn.cursor(row_factory=dict_row) as cur:
                cur.execute(stmt, (min_val, max_val))
                return True, [dict(row) for row in cur.fetchall()]

        except Exception as e:
            logger.exception("db_select_range failed (table=%s): %s", table_name, e)
            return False, []

    # ---------------------------------------------------------------
    # DB_TEST_CONNECTION
    # ---------------------------------------------------------------
    def db_test_connection(self) -> bool:
        try:
            if not self._ensure_connected():
                return False
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                logger.info("NeonManager connection test successful: %s", result)
                return True
        except Exception as e:
            logger.exception("db_test_connection failed: %s", e)
            return False

    # ---------------------------------------------------------------
    # DB_UPDATE
    # ---------------------------------------------------------------
    def db_update(
        self,
        table_name: str,
        data: dict[str, Any],
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        if not data:
            logger.error("db_update: data dict cannot be empty")
            return False, -1

        try:
            if not self._ensure_connected():
                logger.error("db_update: not connected")
                return False, -1

            set_clause = sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(col))
                for col in data.keys()
            )
            stmt = sql.SQL("UPDATE {} SET {}").format(
                sql.Identifier(table_name),
                set_clause,
            )
            query_params = list(data.values())

            if where:
                conditions = sql.SQL(" AND ").join(
                    sql.SQL("{} = %s").format(sql.Identifier(col))
                    for col in where
                )
                stmt += sql.SQL(" WHERE ") + conditions
                query_params.extend(where.values())

            with self._conn.cursor() as cur:
                cur.execute(stmt, query_params)
                updated = cur.rowcount
            self._conn.commit()
            return True, updated

        except Exception as e:
            logger.exception("db_update failed (table=%s): %s", table_name, e)
            self._conn.rollback()
            return False, -1