from datetime import datetime
import json
# from psycopg2.extras import Json
from psycopg.types.json import Jsonb
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
import sys
from typing import Any, ClassVar, Optional, overload, TypeVar, Type, List, Dict

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

# LOCAL IMPORTS
from analysis_scripts import constants
from analysis_scripts.db_neon_pooler import neon_select,  neon_insert_record
from analysis_scripts.jsonfileio import read_from_jsonl

T = TypeVar("T", bound=BaseModel)

class KeyPoint(BaseModel):
    kp_label: str = ''
    details: str = ''
    
class PolicyImpact(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    tag: str = ''
    type_: str = Field(default="", alias="type")
    details: str = ''

class PoliciesErrors(BaseModel):
    model_config = ConfigDict(extra="ignore")  # ignore columns you don't use
    id: Optional[int] = None   # ← correct for IDENTITY / BIGINT
    anlys_date: datetime = Field(default_factory=datetime.now)
    chinese_original: str = ""
    dept_en: str = ""
    errmsg: str = ""
    source_url: str = ""
    status:  int = 0
    time: str = ""
    title_cn: str = ""

# print(policy.model_dump(mode='json', indent=2))   # newer style    
class Policies2(BaseModel):
    model_config = ConfigDict(extra="ignore")  # ignore columns you don't use
    id: int = -1
    time: Optional[datetime] = None
    continent_tags: list[str]= Field(default_factory=list)
    country_tags: list[str]= Field(default_factory=list)
    country_regions_tags: list[str]= Field(default_factory=list)
    dept_en: str = ""
    industry_ICB_tags: list[str]= Field(default_factory=list)
    importance_score: int = 5
    key_points: list[KeyPoint] = Field(default_factory=list)
    impact_analysis: list[PolicyImpact] = Field(default_factory=list)
    province_tags: list[str]= Field(default_factory=list)
    regions_tags: list[str]= Field(default_factory=list)
    source_url: str = ""
    summary: str = ''
    title_cn: str = ""
    title_en: str = ""
    topics_tags:  list[str]= Field(default_factory=list)
    chinese_original: str = ""
    english_translation: str = ""
    anlys_date: datetime = Field(default_factory=datetime.now)
    success: bool = True
    
# If you want to use default_factory for lists to avoid None
class Policies(BaseModel):
    """Pydantic model with default empty lists instead of None."""
    model_config = ConfigDict(validate_assignment=True)
    
    id: int = Field(
        default=-1,
        json_schema_extra={'primary_key': True, 'auto_increment': True}
    )
    
    department_id: int = -1
    title: str = ''
    time: str = ''
    description: str = ''
    tags: Dict[str, Any] = Field(default_factory=dict)
    impact: int = 5
    search_vector: str = ""
    
    full_content: Optional[Dict[str, Any]] = None
    chinese_original: Optional[str] = None
    english_translation: Optional[str] = None
    storytext: Optional[str] = None
    importance_score: int = 5
    source_url: Optional[str] = None
    
    # Use empty lists as defaults instead of None
    province_tags: List[str] = Field(default_factory=list)
    region_tags: List[str] = Field(default_factory=list)
    country_tags: List[str] = Field(default_factory=list)
    country_region_tags: List[str] = Field(default_factory=list)
    continent_tags: List[str] = Field(default_factory=list)
    industry_ICB_tags: List[str] = Field(default_factory=list)
    
    anlys_date: Optional[datetime] = None
    impact_analysis: List[Dict[str, Any]] = Field(default_factory=list)
    status: Optional[int] = None
    
class FieldRef:
    """Dynamic field reference generator."""
    def __init__(self, model: Type[BaseModel]):
        self._model = model
        # Dynamically create attributes for each field
        for field_name in model.model_fields.keys():
            setattr(self, field_name.upper(), field_name)
    
def read_all(table_name,
             model: Optional[Type[T]] = None,
             where_clause: Optional[str] = None
             ) -> tuple[bool, list[T]]:
    try:
        ok, records = neon_select(table_name=table_name, where_clause=where_clause)
        if not ok:
            return False, []
        if not records:
            return True, []
        if model:
            final_records: list[T] = []
            for r in records:
                final_records.append(model.model_validate(r))
        else:
            final_records = records
        return True, final_records
            
    except Exception as e:
        logger.error(f"Error encountered. {e}")
        return False, None    

@overload
def insert(x: Policies) -> int: ...
@overload
def insert(x: PoliciesErrors) -> str: ...

def insert(data_obj) -> bool:
    if isinstance(data_obj, Policies):
        ok = insert_policy(data_obj)
    elif isinstance(data_obj, str):
        ok = insert_policy_error(data_obj)
    else:
        raise TypeError
    return ok

# INSERT function for a Policies object
def insert_policy(policy: Policies) -> bool:
    """
    Insert a Policies object into the Neon database.
    
    Args:
        policy: Policies object to insert
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert Pydantic models to JSON
        ## key_points_json = [kp.model_dump() for kp in policy.key_points]
        impact_analysis_json = [ia.model_dump() for ia in policy.impact_analysis]
        # tags_json = [ia.model_dump() for ia in policy.tags]
        
        insert_data = {
            "time": policy.time,
            "chinese_original": policy.chinese_original,
            "continent_tags": policy.continent_tags,
            "country_tags": policy.country_tags,
            "country_region_tags": policy.country_region_tags,
            "description": policy.description,
            "department_id": policy.department_id,
            "english_translation": policy.english_translation,
            "industry_ICB_tags": policy.industry_ICB_tags,
            "importance_score": policy.importance_score,
            # "key_points": Jsonb(key_points_json),                # jsonb
            "impact_analysis": Jsonb(impact_analysis_json),      # jsonb
            "province_tags": policy.province_tags,
            "region_tags": policy.region_tags,
            "source_url": policy.source_url,
            "tags": Jsonb(policy.tags or {}),
            # "title_cn": policy.title_cn,
            "title": policy.title,
            "anlys_date": policy.anlys_date,
            "status": policy.status,
        }        
        
        tbl_name = constants.TableNames.TBL_POLICIES
        ok = neon_insert_record(table_name=tbl_name, data=insert_data)
        
        if not ok:
            logger.error("insert_policy FAILED.")
            return False

        logger.info(f"Successfully inserted/updated policy ID: {policy.id}")
        return True
      
    except Exception as e:
        logger.error(f"Error inserting policy ID {policy.id}: {e}")
        return False
    
# INSERT function for a Policies object
def insert_policy_error(policy_errors: PoliciesErrors) -> bool:
    """
    Insert a Policies object into the Neon database.
    
    Args:
        policy: Policies object to insert
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Convert Pydantic models to JSON
        insert_data = policy_errors.model_dump(exclude={"id"}, exclude_none=True)  
        
        tbl_name = constants.TableNames.TBL_POLICIES_ERRORS
        ok = neon_insert_record(tbl_name=tbl_name, data=insert_data)
        
        if not ok:
            logger.error("insert_policy_error: FAILED.")
            return False

        logger.info(f"insert_policy_error: Successfully inserted/updated policy error")
        return True
      
    except Exception as e:
        logger.error(f"Error inserting policy error: {e}")
        return False
            
# Run the function to create the table
if __name__ == "__main__":
    filename_prefix =  'policy_new'
    ok, data = read_from_jsonl(filename_prefix=filename_prefix)
    if not ok:
        print("Error: Could not load data from {filename_prefix}")
        exit(0)
    # Rehydrate Pydantic model
    policy = Policies.model_validate(data)
    ok = insert_policy(policy)
    exit(0)
    
    ok, records = read_all(table_name=constants.TableNames.TBL_POLICIES_ERRORS, model=PoliciesErrors)
    if not ok:
        print ("error encountered")
        exit(0)
    print (len(records))
    # print ("HELLO")
    # select_supa(TBL_LAST_UPDATE, "*")
