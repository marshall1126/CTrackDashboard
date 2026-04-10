# analysis_scripts/database/base_database.py
from typing import Any, Optional, TypeVar
from pydantic import BaseModel

from logger import get_logger
from abc import ABC, abstractmethod

T = TypeVar("T", bound=BaseModel)

# LOGGER    
logger = get_logger(__name__)

class BaseDatabaseManager(ABC):
    ########################################################################
    # _ENSURE_CONNECTED
    # Returns bool
    ########################################################################
    @abstractmethod
    def _ensure_connected(self) -> bool:
        """Check connection is alive, reconnect if needed. Returns True if connected."""
        ...    
    
    ########################################################################
    # DB_CLOSE
    # Returns None
    ########################################################################
    @abstractmethod
    def db_close(self):
        """Public wrapper for connecting to database"""
        ...
    
    ########################################################################
    # DB_COMMIT
    # Returns bool
    ########################################################################
    @abstractmethod
    def db_commit(self) -> bool:
        """Public wrapper for committing database changes"""
        ...
    
    ########################################################################
    # DB_CONNECT
    # Returns bool
    ########################################################################
    @abstractmethod
    def db_connect(self) -> bool:
        """Public wrapper for connecting to database"""
        ...
        
    ########################################################################
    # DB_COUNT
    # Returns tuple[bool, int]
    ########################################################################
    @abstractmethod
    def db_count(self,
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Returns count of records matching the where clause."""
        ...    
    
    ########################################################################
    # DB_DELETE
    # Returns tuple[bool, int]
    ########################################################################
    @abstractmethod
    def db_delete(
        self, 
        table_name: str,
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Public wrapper for deleting rows from a table via Neon."""
        ...
    
    ########################################################################
    # DB_INSERT
    # Returns bool
    ########################################################################
    @abstractmethod
    def db_insert(
        self, 
        table_name: str,
        data: Any,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
        commit: Optional[bool] = True
    ) -> bool:
        """Public wrapper for inserting a single record into a table."""
        ...
    
    ########################################################################
    # DB_INSERT_BATCH
    # Returns tuple[bool, int]
    ########################################################################
    @abstractmethod
    def db_insert_batch(
        self, 
        table_name: str,
        data: list[Any],
        commit: bool = True,
        exclude_list: Optional[list[str]] = None,
        auto_json: Optional[bool] = True,
    ) -> tuple[bool, int]:
        """Public wrapper for inserting a list of records into a table."""
        ...
    
    #######################################################################
    # DB_REFRESH
    # Returns bool
    # ######################################################################    
    @abstractmethod
    def db_refresh(self) -> bool:
        """Drop and re-establish the connection. Returns True on success."""
        ...    
    
    #######################################################################
    # DB_RESET_IDENTITY
    # Returns bool
    # ######################################################################
    @abstractmethod
    def db_reset_identity(
        self, 
        table_name: str,
        identity_col: str,
        reset_val: Optional[int] = 1
    ) -> bool:
        """Public wrapper for resetting the auto-index increment key to 1."""
        ...
            
    ########################################################################
    # DB_SELECT
    # Returns tuple[bool, []]
    ########################################################################
    @abstractmethod
    def db_select(
        self, 
        table_name: str,
        where: Optional[dict] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        dataclass: Optional[type[T]] = None
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Public wrapper for selecting records from sql database."""
        ...
    
    ########################################################################
    # DB_SELECT_RANGE
    # Returns tuple[bool, list[dict]]
    ########################################################################
    @abstractmethod
    def db_select_range(
        self, 
        table_name: str,
        column: str,
        min_val: Any,
        max_val: Any,
        select_cols: Optional[list[str]] = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        """Public wrapper for selecting records within a range from sql database."""
        ...
    
    ########################################################################
    # DB_TEST_CONNECTION
    # Returns bool
    ########################################################################
    @abstractmethod
    def db_test_connection(self) -> bool:
        """Public wrapper to test database connection."""
        ...
        
    ########################################################################
    # DB_UPDATE
    # Returns tuple[bool, int]
    ########################################################################
    @abstractmethod
    def db_update(
        self, 
        table_name: str,
        data: dict[str, Any],
        where: Optional[dict] = None,
    ) -> tuple[bool, int]:
        """Public wrapper for updating records in sql database."""
        ...
