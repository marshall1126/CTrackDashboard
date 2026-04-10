import json
from pathlib import Path
import re
import sys

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

from analysis_scripts.ai_master import AIMaster
from analysis_scripts.ai_model_params import AIModelParams
from analysis_scripts.models import PolicyAnalysisData
from analysis_scripts.reference_data import get_ref_data,  RefData, load_ref_data_once

def get_user_message(text_en: str) -> str:
    user_message = f""" 
            POLICY_TEXT:\n{text_en}\n\n
            TASK:\n
            Extract geography exactly as defined in the system rules.
            Return ONLY the JSON object with the five *_mentioned fields.
            Do not include any other fields.
            """
    return user_message

async def ai_analysis_phase1(ai_master: AIMaster,
                       ai_model_params: AIModelParams, 
                       policy_analysis_data: PolicyAnalysisData
                       ):

    if not policy_analysis_data:
        logger.error("No policy found")
        return False
    logger.info(f"id={policy_analysis_data.id}: ### PHASE 1 ANALYSIS START #####################")
    english_translation = policy_analysis_data.english_translation
    if not english_translation:
        policy_analysis_data.errmsg =  f"id={policy_analysis_data.id}: No english_translation found"
        logger.error(policy_analysis_data.errmsg)
        policy_analysis_data.success = False
        return False
    try:
        ref_data = get_ref_data ()
        
        # GET PROMPT
        user_message = get_user_message(policy_analysis_data.english_translation)
        system_message = get_system_message(ref_data)

        # SUBMIT ANALYSIS
         # Build proper OpenAI-style messages list
        messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ]                
        result = await ai_master.execute(messages=messages, model_params=ai_model_params)
        if not result:
            logger.error(
                    f"id={policy_analysis_data.id}: No geo context")
            return None, None, None
        
        
        json_result = json.loads(result)        
        provinces_mentioned = json_result.get("provinces_mentioned", [])
        regions_mentioned = json_result.get("regions_mentioned", [])
        countries_mentioned = json_result.get("countries_mentioned", [])
        country_regions_mentioned = json_result.get("country_regions_mentioned", [])
        continents_mentioned = json_result.get("continents_mentioned", [])
        
        set_provinces_codes = set()
        set_regions_codes = set()
        set_countries_codes = set()
        set_country_regions_codes = set()
        set_continents_codes = set()
        
        for idx, province_mentioned in enumerate(provinces_mentioned, 1):
            key = canon_geo_name(province_mentioned)
            ref_province_item = ref_data.provinces.get(key)
            if not ref_province_item:
                logger.warning(f"id={policy_analysis_data.id}: Could not find province {key}")
                continue
            province_code = ref_province_item.get('province_code')
            region_code = ref_province_item.get('region_code')
            set_provinces_codes.add(province_code)
            set_regions_codes.add(region_code)
            
        for idx, region_mentioned in enumerate(regions_mentioned, 1):
            key = canon_geo_name(region_mentioned)
            ref_regions_item = ref_data.regions.get(key)
            if not ref_regions_item:
                logger.warning(f"id={policy_analysis_data.id}: Could not find region {key}")
                continue
            region_code = ref_regions_item.get('region_code')
            set_regions_codes.add(region_code)
            
        for idx, country_mentioned in enumerate(countries_mentioned, 1):
            ref_country_item = ref_data.countries.get(country_mentioned)
            if not ref_country_item:
                logger.warning(f"id={policy_analysis_data.id}: {country_mentioned} not found in country list")
                continue
            country_code = ref_country_item.get('country_code')
            country_region_code = ref_country_item.get('country_region_code')
            continent_code = ref_country_item.get('continent_code')
            set_countries_codes.add(country_code)
            set_country_regions_codes.add(country_region_code)
            set_continents_codes.add(continent_code)
            
        for idx, country_region_mentioned in enumerate(country_regions_mentioned, 1):
            ref_country_region_item = ref_data.country_regions.get(country_region_mentioned)
            if not ref_country_region_item:
                logger.warning(f"id={policy_analysis_data.id}: {country_region_mentioned} not found in country region list")
                continue
            country_region_code = ref_country_region_item.get('country_region_code')
            continent_code = ref_country_region_item.get('continent_code')
            set_country_regions_codes.add(country_region_code)
            set_continents_codes.add(continent_code)
            
        for idx, continent_mentioned in enumerate(continents_mentioned, 1):
            ref_continent_item = ref_data.continents.get(continent_mentioned)
            if not ref_continent_item:
                logger.warning(f"id={policy_analysis_data.id}: {ref_continent_item} not found in continent list")
                continue            
            continent_code = ref_continent_item.get('continent_code')
            set_continents_codes.add(continent_code)                

        # logger.info(json_str)
        # create return data structure
        
        policy_analysis_data.provinces_mentioned = provinces_mentioned
        policy_analysis_data.regions_mentioned = regions_mentioned
        policy_analysis_data.countries_mentioned = countries_mentioned
        policy_analysis_data.country_regions_mentioned = country_regions_mentioned
        policy_analysis_data.continents_mentioned = continents_mentioned
        
        policy_analysis_data.province_tags = list(set_provinces_codes)
        policy_analysis_data.region_tags = list(set_regions_codes)
        policy_analysis_data.country_tags = list(set_countries_codes)
        policy_analysis_data.country_region_tags = list(set_country_regions_codes)
        policy_analysis_data.continent_tags = list(set_continents_codes)

        
        logger.debug(f"id={policy_analysis_data.id}: province_tags: {policy_analysis_data.province_tags}")
        logger.debug(f"id={policy_analysis_data.id}: region_tags: {policy_analysis_data.region_tags}")
        logger.debug(f"id={policy_analysis_data.id}: country_tags: {policy_analysis_data.country_tags}")
        logger.debug(f"id={policy_analysis_data.id}: country_region_tags: {policy_analysis_data.country_region_tags}")
        logger.debug(f"id={policy_analysis_data.id}: continent_tags: {policy_analysis_data.continent_tags}")

        policy_analysis_data.success = True
    except Exception as e:
        errmsg = f"policy_analysis id={policy_analysis_data.id}: Error encountered. {e}"
        policy_analysis_data.success = False
        policy_analysis_data.errmsg = errmsg
        logger.error(errmsg)
    
    logger.debug(f"id={policy_analysis_data.id}: Phase 1 complete. success={policy_analysis_data.success}")
    logger.info(f"id={policy_analysis_data.id}: ### PHASE 1 ANALYSIS END #####################")
    return policy_analysis_data.success

def get_system_message(ref_data):
    
    formatted_provinces = "\n".join(ref_data.provinces.keys())
    formatted_regions = "\n".join(ref_data.regions.keys())
    formatted_countries = "\n".join(ref_data.countries.keys())
    formatted_country_regions = "\n".join(ref_data.country_regions.keys())
    formatted_continents = "\n".join(ref_data.continents.keys())

    prompt = f"""
        You are an expert policy analyst. Your task is to extract geographic names from POLICY_TEXT
with maximum accuracy and consistency. Output ONE JSON object using the structure below.

        {{
          "provinces_mentioned": [],
          "regions_mentioned": [],
          "countries_mentioned": [],
          "country_regions_mentioned": [],
          "continents_mentioned": []
        }}
        
        --------------------------------------------------------------------
        GENERAL RULES
        --------------------------------------------------------------------
        1. Extract ONLY geography names **explicitly referenced** in the text.
        2. You MUST return **all** geographic references that appear in any form.
        3. NO inference, NO guesswork, NO logical deduction.
        4. Match names to the **allowed lists** exactly (provided below).
        5. If a referenced name is NOT in the allowed list, you MUST ignore it.
           Example: “Chang-Zhu-Tan” is not an allowed name → ignore it (do not output).
        6. Do not append words like “Province”, “Region”, “City”. Output canonical names exactly as in the allowed list.
           Example: output "Hubei", not "Hubei Province" or "Hubei Region".   
        
        --------------------------------------------------------------------
        TEXT-MATCHING RULES (CRITICAL)
        --------------------------------------------------------------------
        A reference MUST be included if it appears in any of these forms:
        
        • exact name  
        • possessive form  
            - “Ukraine’s”, “Canada’s”, “Japan’s leadership”  
        • hyphenated or compound form  
            - “China–France relations”, “US–Japan dialogue”  
        • adjectival / demonym form (only for countries)  
            - “French”, “Ukrainian”, “Japanese”, “Canadian”, “Russian”, “German”
        • plural demonyms  
            - “the French”, “the Japanese”, “Americans”  
        • abbreviations that map deterministically:
            - US, U.S., USA, U.S.A. → United States  
            - UK, U.K. → United Kingdom  
            - UAE → United Arab Emirates  
            - EU → Europe (continent)  
        
        If ANY of these forms appear, you MUST include the corresponding canonical
        allowed name (e.g., “French” → “France”, “Ukrainian” → “Ukraine”).
        
        --------------------------------------------------------------------
        MANDATORY SPECIAL RULES
        --------------------------------------------------------------------
        • China is excluded from countries_mentioned (ignore it completely).  
        • if Taiwan is explicitly referenced (including as 'Taiwanese'), ALWAYS place it under provinces_mentioned
        • If “Taiwanese” appears, treat it as “Taiwan”.  
        • If a country abbreviation maps to an allowed country, include the canonical name.  
        • If a demonym refers to an allowed country, include the canonical name.  
        • DO NOT reclassify continents, regions, or provinces from context.
        • **THE CANONICAL NAME "EUROPE" MUST ONLY BE PLACED IN continents_mentioned. IT IS NOT ALLOWED IN country_regions_mentioned.**
        • DO NOT infer a continent from a country (e.g., "China" does not imply "Asia"). Only include continents if their name appears explicitly or is mapped via an abbreviation (like EU).
        • THE NAME "EUROPE" MUST NOT BE PLACED IN country_regions_mentioned under any circumstances.

        SPECIAL NORMALIZATION RULES:
        - "EU" → "Europe" (ALWAYS assigned to continents_mentioned **AND NO other list**)
        
        --------------------------------------------------------------------
        ALLOWED GEOGRAPHY LISTS
        (You MUST match extracted names ONLY against these lists)
        --------------------------------------------------------------------
        
        PROVINCES:
        {formatted_provinces}
        
        REGIONS:
        {formatted_regions}
        
        COUNTRIES:
        {formatted_countries}
        
        COUNTRY REGIONS:
        **- NOTE: The canonical name "Europe" is explicitly excluded from this list. It is only allowed in CONTINENTS.**
        {formatted_country_regions}
        
        CONTINENTS:
        {formatted_continents}
        
        --------------------------------------------------------------------
        STRICT OUTPUT FORMAT
        --------------------------------------------------------------------
        • Return ONE JSON object exactly matching the schema above.
        • All arrays MUST contain only plain strings.
        • DO NOT include duplicates.
        • DO NOT include explanations, comments, or markdown.

    """
    #logger.info("#####################################")
    #logger.info(prompt)
    return prompt

_SUFFIXES = (
    " Region",
    " Province",
    " City",
    " Municipality",
    " Autonomous Region",
    " Special Administrative Region",
    " SAR",
)

def canon_geo_name(name: str) -> str:
    if not name:
        return name
    s = name.strip()

    # collapse internal whitespace
    s = re.sub(r"\s+", " ", s)

    # strip common English suffixes the model likes to append
    for suf in _SUFFIXES:
        if s.endswith(suf):
            s = s[: -len(suf)].strip()
            break

    return s

if __name__ == "__main__":
    from analysis_scripts.database.neon_manager import NeonManager, NeonConnectionMode
    try:
        db_manager = NeonManager(NeonConnectionMode.POOLER)
        if not db_manager:
            logger.info("No database connection")
            exit
        ok = db_manager.db_connect()
        if not ok:
            logger.info("No database connection")
            exit    
        text = """
        From January 6 to 8, 2026, Zhai Jun, the Chinese government's Special Envoy for Middle East Affairs, visited Israel, where he met with Israeli Foreign Minister Eli Cohen, Director General of the Ministry of Foreign Affairs Alon Ushpiz, and Deputy Chairman of the National Security Council Eyal Hulata to conduct in-depth exchanges on bilateral relations. 
        
        Zhai Jun stated that the Chinese nation and the Jewish nation have a long-standing friendship, and maintaining healthy and stable development of China-Israel relations is in the fundamental interests of both peoples. China is willing to work together with Israel to maintain exchanges and mutually beneficial cooperation between the two countries and to continue the friendship between the peoples.
        
        The Israeli side expressed high importance to the development of China-Israel relations, reaffirmed its commitment to the one-China policy, and expressed willingness to further strengthen exchanges between various departments and levels of both countries, promoting new progress in practical cooperation across various fields.
        
        The two sides also exchanged views on regional hotspot issues.
        """
        
        # INITIALILZE REFERENCE DATA
        ref_data_obj = RefData()
        ref_data = load_ref_data_once()    
        
        policy_analysis_data = PolicyAnalysisData()
        policy_analysis_data.id = 1234
        policy_analysis_data.english_translation = text
        
        ai_master =  AIMaster()
        ai_model_params = AIModelParams()
        
        ai_analysis_phase1(ai_master=ai_master, ai_model_params=ai_model_params, policy=policy_analysis_data)
    except Exception as e:
        logger.error(f"Error encountered. {e}")
    finally:
        if db_manager:
            db_manager.db_close()    
