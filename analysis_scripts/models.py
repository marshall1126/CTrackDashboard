from datetime import datetime, date, timezone
from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Any, ClassVar, Dict, List, Optional

###############################################################################
# AnalysisLastUpdt
###############################################################################
class AnalysisLastUpdt(BaseModel):
    model_config = ConfigDict(extra="ignore")
    
    idx: Optional[int] = None
    last_updt: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = 0

###############################################################################
# KeyPoint
###############################################################################
class KeyPoint(BaseModel):
    kp_label: str = ''
    details: str = ''
    
###############################################################################
# PolicyImpact
###############################################################################
class PolicyImpact(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    tag: str = ''
    type_: str = Field(default="", alias="type")
    details: str = ''

###############################################################################
# POLICIES
###############################################################################
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

###############################################################################
# POLICIES_ERRORS
###############################################################################    
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

###############################################################################
# PolicyAnalysisData
###############################################################################
class PolicyAnalysisData(BaseModel):
    model_config = ConfigDict(extra="ignore", validate_assignment=True)  # ignore columns you don't use
    
    id: int = -1
    time: str | None = None
    anlys_datet: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    chinese_original: str = ""
    comprehensive_analysis: str = ''
    continents_mentioned: list[str]= Field(default_factory=list)
    continent_tags: list[str]= Field(default_factory=list)
    countries_mentioned: list[str]= Field(default_factory=list)
    country_tags: list[str]= Field(default_factory=list)
    country_regions_mentioned: list[str]= Field(default_factory=list)
    country_region_tags: list[str]= Field(default_factory=list)
    dept_en: str = ""
    department_id: int = -1
    description: str = ''
    english_title: str = ''
    english_translation: str = ""
    industry_ICB_tags: list[str]= Field(default_factory=list)
    importance_score: int = 5
    key_points: list[KeyPoint] = Field(default_factory=list)
    impact_analysis: list[PolicyImpact] = Field(default_factory=list)
    provinces_mentioned: list[str]= Field(default_factory=list)
    province_tags: list[str]= Field(default_factory=list)
    regions_mentioned: list[str]= Field(default_factory=list)
    region_tags: list[str]= Field(default_factory=list)
    source_url: str = ""
    success: bool = True
    title_cn: str = ""
    title_en: str = ""
    topics_tags:  list[str]= Field(default_factory=list)
    errmsg: str = ""
  
    @field_validator("time", mode="before")
    @classmethod
    def coerce_time(cls, v):
        if v is None:
            return ""
        if isinstance(v, (datetime, date)):
            return v.strftime("%Y-%m-%d")
        return str(v)    

###############################################################################
# STORY_ALL_FINAL
###############################################################################
class StoryAllFinal(BaseModel):
    model_config = ConfigDict(extra="ignore")  # ignore columns you don't use
    FIELD_LINK: ClassVar[str] = "link"
    
    id: int
    title: str = ""
    date: datetime | None = None
    dept: str = ""
    link: str = ""
    story_text: str = ""
    success: bool
    

