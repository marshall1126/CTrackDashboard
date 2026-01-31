from pathlib import Path
import sys

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
   
from logger import get_logger
logger = get_logger(__name__)
logger.propagate = False

from typing import Dict, Any, Optional
from analysis_scripts.db_neon_wrapper import read_all

_ref_data_cache: Optional["RefData"] = None

REF_CONTINENTS = "continents"
REF_COUNTRIES = "countries"
REF_COUNTRY_REGIONS = "country_regions"
REF_DEPARTMENTS = "departments"
REF_INDUSTRIES = "industries"
REF_INDUSTRY_ICB = "industry_icb"
REF_PROVINCES = "provinces"
REF_REGIONS = "regions"
REF_TOPICS = "topics"

class RefData:
    def __init__(self):
        self.continents = None
        self.countries = None
        self.country_regions = None   # fixed typo
        self.departments = None
        self.industries = None
        self.provinces = None         # fixed typo
        self.regions = None
        self.topics = None
        
    @classmethod
    def load_from_db(cls) -> "RefData":
        """Loads all reference data from DB and returns a populated RefData."""
        obj = cls()
        conn = None
        cur = None
        try:
            ok, results = read_all(table_name=REF_INDUSTRIES)
            if not ok:
                logger.error(f"Failed to read {REF_INDUSTRIES}")
                return False, {}        # cur.execute(f"SELECT id, name FROM {REF_INDUSTRIES} ORDER BY name")
            industries = {r["name"]: r["id"] for r in results}
    
            ok, results = read_all(table_name=REF_TOPICS)
            if not ok:
                logger.error(f"Failed to read {REF_TOPICS}")
                return False, {}
            topics = {r["name_en"]: r["id"] for r in results}
    
     
            ###################################################################################
            # DEPARTMENTS
            ###################################################################################
            ok, results = read_all(table_name=REF_DEPARTMENTS)
            if not ok:
                logger.error(f"Failed to read {REF_DEPARTMENTS}")
                return False, {}
            
            departments = {}
            dept_rows = results
            for row in dept_rows:
                if not isinstance(row, dict):
                    logger.error(f"Row is not a dict, it's a {type(row)}: {row}")
                    continue
                code = row.get('code')
                if code:
                    departments[code] = {
                        'id': row.get('id'),
                        'code': code,
                        'name_zh': row.get('name_zh', ''),
                        'name_en': row.get('name_en') or row.get('full_name', ''),
                        'url_pattern': row.get('url_pattern')
                    }
            #logger.info(f"Fetched {len(departments)} departments")
    
            ###################################################################################
            # provinces
            ###################################################################################
            ok, results = read_all(table_name=REF_PROVINCES)
            if not ok:
                logger.error(f"Failed to read {REF_PROVINCES}")
                return False, {}
            provinces = {}
            #logger.info(f"Retrieved {len(province_rows)} province rows")
    
            for row in results:
                if not isinstance(row, dict):
                    logger.error(
                        f"Province row is not a dict, it's a {type(row)}: {row}")
                    continue
                province_name = row.get('province_name')
                if province_name:
                    provinces[province_name] = {
                        'id': row.get('id'),
                        'province_name': province_name,
                        'province_code': row.get('province_code', ''),
                        'province_type': row.get('province_type', ''),
                        'region_name': row.get('region_name'),
                        'region_code': row.get('region_code')
                    }
            #logger.info(f"Fetched {len(provinces)} provinces")
    
            ###################################################################################
            # REGIONS
            ###################################################################################
            regions = {}
            for idx, province in enumerate(provinces.values(), 1):
                region_name = province.get('region_name')
                if not region_name:
                    continue
                if region_name in regions:
                    continue
                regions[region_name] = {
                        'region_name': region_name,
                        'region_code': province.get('region_code', '')
                    }
            #logger.info(f"Fetched {len(regions)} distinct regions")
    
            ###################################################################################
            # COUNNTRIES
            ###################################################################################
            ok, results = read_all(table_name=REF_COUNTRIES)
            if not ok:
                logger.error(f"Failed to read {REF_COUNTRIES}")
                return False, {}
            countries = {}
            country_regions = {}
            continents = {}
    
            country_rows = results
            #logger.info(f"Retrieved {len(country_rows)} country rows")
    
            for row in country_rows:
                if not isinstance(row, dict):
                    logger.error(
                        f"Country row is not a dict, it's a {type(row)}: {row}")
                    continue
                country_name = row.get('country_name')
                if country_name:
                    countries[country_name] = {
                        'id': row.get('id'),
                        'country_name': country_name,
                        'country_code': row.get('country_code', ''),
                        'country_region_name': row.get('region'),
                        'country_region_code': row.get('region_code'),
                        'continent_name': row.get('continent'),
                        'continent_code': row.get('continent_code')
                    }
    
                    country_region_name = row.get('region')
                    if country_region_name and country_region_name not in country_regions:
                        country_regions[country_region_name] = {
                            'country_region_name': country_region_name,
                            'country_region_code': row.get('region_code')or '',
                            'continent_name': row.get('continent'),
                            'continent_code': row.get('continent_code')
                        }
    
                    continent_name = row.get('continent')
                    if continent_name and continent_name not in continents:
                        continents[continent_name] = {
                            'continent_name': continent_name,
                            'continent_code': row.get('continent_code') or ''
                        }
            logger.info(f"Fetched {len(countries)} countries")
            logger.info(f"Fetched {len(country_regions)} countrie regions")
            logger.info(f"Fetched {len(continents)} continents")
    
            #logger.info("Fetching ICB industries...")
            ok, results = read_all(table_name=REF_INDUSTRY_ICB)
            if not ok:
                logger.error(f"Failed to read {REF_INDUSTRY_ICB}")
                return False, {}        
            icb_industries = {}
            icb_rows = results
            #logger.info(f"Retrieved {len(icb_rows)} ICB industry rows")
    
            for row in icb_rows:
                if not isinstance(row, dict):
                    logger.error(
                        f"ICB row is not a dict, it's a {type(row)}: {row}")
                    continue
                subsector = row.get('subsector')
                if subsector:
                    icb_industries[subsector] = {
                        'id': row.get('id'),
                        'industry': row.get('industry', ''),
                        'group_name': row.get('group_name', ''),
                        'subsector': row.get('subsector', ''),
                        'icb_code': row.get('icb_code', ''),
                        'scope': row.get('scope', '')
                    }
            logger.info(f"Fetched {len(icb_industries)} ICB industries")

            obj.continents = continents
            obj.countries = countries
            obj.country_regions = country_regions
            obj.departments =  departments
            obj.icb_industries = icb_industries
            obj.industries = industries
            obj.provinces = provinces
            obj.regions = regions
            obj.topics = topics

            logger.info("Fetch completed successfully")
    
        except Exception as e:
            logger.error(f"Error fetching reference data: {e}", exc_info=True)
            return {}
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
        return obj

#########################################################################
# NON CLASS METHODS
#########################################################################
def load_ref_data_once() -> RefData:
    """
    Call this in the parent process *before* spawning workers.
    Loads from DB once and stores in a module-global cache.
    """
    global _ref_data_cache
    if _ref_data_cache is None:
        _ref_data_cache = RefData.load_from_db()
        logger.info("Reference data loaded into cache")
    return _ref_data_cache


def get_ref_data() -> RefData:
    """
    Call this in workers to access the already-loaded cache.
    """
    if _ref_data_cache is None:
        raise RuntimeError("Ref data not loaded. Call load_ref_data_once() before spawning workers.")
    return _ref_data_cache
            
if __name__ == "__main__":
    ref_data_obj = RefData()
    ref_data = load_ref_data_once()
    print (ref_data)
