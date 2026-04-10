# WebData/sql_wrapper.py
from typing import Any, Optional

from logger import get_logger
from analysis_scripts.database.base_database import BaseDatabaseManager, T

# LOGGER    
logger = get_logger(__name__)

class SqlWrapper:
    def __init__(self, db_manager: BaseDatabaseManager):
        self.db_manager = db_manager        
    
    ########################################################################
    # DB_CLOSE
    # Returns None
    ########################################################################
    def db_close(self):
        """Public wrapper for connecting to database"""
        self.db_manager.db_close()
    
    ########################################################################
    # DB_COMMIT
    # Returns bool
    ########################################################################
    def db_commit(self) -> bool:
        """Public wrapper for committing database changes"""
        return self.db_manager.db_commit()
    
    ########################################################################
    # DB_CONNECT
    # Returns bool
    ########################################################################
    def db_connect(self) -> bool:
        """Public wrapper for connecting to database"""
        return self.db_manager.db_connect()
    
    ########################################################################
    # SQL_COUNT
    # Returns tuple[bool, int]
    ########################################################################
    def sql_count(self,
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Returns count of records matching the where clause."""
        return self.db_manager.db_count(table_name, where)    
    
    ########################################################################
    # DB_DELETE
    # Returns tuple[bool, int]
    ########################################################################
    def db_delete(self, 
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Public wrapper for deleting rows from a table via Neon."""
        return self.db_manager.db_delete(table_name=table_name,
                           where=where)
    
    ########################################################################
    # DB_INSERT
    # Returns bool
    ########################################################################
    def db_insert(self, 
        table_name: str,
        data: Any,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
        commit: Optional[bool] = True
    ) -> bool:
        """Public wrapper for inserting a single record into a table."""
        return self.db_manager.db_insert(table_name=table_name, data=data, commit=commit, exclude_list=exclude_list, auto_json=auto_json)
    
    ########################################################################
    # DB_INSERT_BATCH
    # Returns tuple[bool, int]
    ########################################################################
    def db_insert_batch(self, 
        table_name: str,
        data: list[Any],
        commit: bool = True,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
    ) -> tuple[bool, int]:
        """Public wrapper for inserting a list of records into a table."""
        return self.db_manager.db_insert_batch(table_name, data, commit, exclude_list, auto_json)
    
    ########################################################################
    # DB_REFRESH
    # Returns bool
    ########################################################################
    def db_refresh(self) -> bool:
        """Force disconnect and reconnect."""
        return self.db_manager.db_refresh()    
    
    ########################################################################
    # DB_RESET_IDENTITY
    # Returns bool
    # ########################################################################      
    def db_reset_identity(self, 
        table_name: str,
        identity_col: str,
        reset_val: Optional[int] = 1
    ) -> bool:
        """Public wrapper for resetting the auto-index increment key to 1."""
        return self.db_manager.db_reset_identity(table_name=table_name, identity_col=identity_col, reset_val=reset_val)
    
    ########################################################################
    # DB_SELECT
    # Returns tuple[bool, []]
    ########################################################################
    def db_select(self, 
        table_name: str,
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        dataclass: Optional[type[T]] = None
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Public wrapper for selecting records from sql database."""
        ok, records = self.db_manager.db_select(table_name, where, limit, order_by, dataclass)
        return ok, records
    
    ########################################################################
    # DB_SELECT_RANGE
    # Returns tuple[bool, list[dict]]
    ########################################################################
    def db_select_range(self, 
        table_name: str,
        column: str,
        min_val: Any,
        max_val: Any,
        select_cols: Optional[list[str]] = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Public wrapper for selecting records within a range from sql database."""
        return self.db_manager.db_select_range(table_name, column, min_val, max_val, select_cols)
    
    ########################################################################
    # DB_TEST_CONNECTION
    # Returns bool
    ########################################################################
    def db_test_connection(self) -> bool:
        """Public wrapper to test database connection."""
        return self.db_manager.db_test_connection()
        
    ########################################################################
    # DB_UPDATE
    # Returns tuple[bool, int]
    ########################################################################
    def db_update(self, 
        table_name: str,
        data: dict[str, Any],
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Public wrapper for updating records in sql database."""
        return self.db_manager.db_update(table_name, data, where)