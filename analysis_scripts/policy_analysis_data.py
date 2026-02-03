from datetime import date, datetime, timezone
import json
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, ConfigDict
import sys
from typing import TypeVar

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logger import get_logger
logger = get_logger(__name__)
from analysis_scripts.db_neon_wrapper import KeyPoint, PolicyImpact

T = TypeVar("T", bound=BaseModel)

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