import asyncio
import json
from pathlib import Path
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
from analysis_scripts.db_neon_wrapper import Policies, KeyPoint,  PolicyImpact
from analysis_scripts.policy_analysis_data import PolicyAnalysisData
from analysis_scripts.reference_data import get_ref_data,  RefData, load_ref_data_once

COMPREHENSIVE_WORD_COUNT_MIN = 75
COMPREHENSIVE_WORD_COUNT_MAX = 175

RESPONSE_KEY_POINTS = "key_points"
RESPONSE_IMPACT_ANALYSIS = "impact_analysis"
RESPONSE_COMPREHENSIVE = "comprehensive_analysis"
RESPONSE_TITLE = "title"

def get_user_message(text_en, tag_data):
    user_message = f"""
      POLICY_TEXT:\n{text_en}\n\n
      TAG_DATA:\n{json.dumps(tag_data, ensure_ascii=False)}\n\n
      TASK:\n
      - Use POLICY_TEXT + TAG_DATA.\n
      - Produce:\n
      1. {RESPONSE_KEY_POINTS} (1–3)\n"
      2. {RESPONSE_IMPACT_ANALYSIS} (ONE entry per tag in TAG_DATA)\n"
      3. {RESPONSE_COMPREHENSIVE} ({COMPREHENSIVE_WORD_COUNT_MIN}-{COMPREHENSIVE_WORD_COUNT_MAX} words)\n
      4. {RESPONSE_TITLE}
      """
    return user_message


async def ai_analysis_phase5(ai_master: AIMaster,
                       ai_model_params: AIModelParams, 
                       policy_analysis_data: PolicyAnalysisData
                       ):

    
    if not policy_analysis_data:
        logger.error("No policy found")
        return False
    logger.info (f"ai_analysis_phase5: id={policy_analysis_data.id}: ### PHASE 5 ANALYSIS START #####################")
    english_translation = policy_analysis_data.english_translation
    if not english_translation:
        policy_analysis_data.errmsg =  f"id={policy_analysis_data.id}: No english_translation found"
        logger.error(policy_analysis_data.errmsg)
        policy_analysis_data.success = False
        return False
    try:
        # GET PROMPT
        tag_data = policy_analysis_data.industry_icb_tags
        user_message = get_user_message(policy_analysis_data.english_translation, tag_data)
        system_message = get_system_message()
        response_format = get_response_format()
        # SUBMIT ANALYSIS
         # Build proper OpenAI-style messages list
        messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ]                
        result = await ai_master.execute(messages=messages, model_params=ai_model_params, response_format=response_format)
        if not result:
            logger.error(
                    f"ai_analysis_phase5: id={policy_analysis_data.id}: no results")
            return None, None, None
        
        
        json_result = json.loads(result)
        xxx = json_result.keys()
        policy_analysis_data.title_en = json_result.get(RESPONSE_TITLE)
        policy_analysis_data.comprehensive_analysis = json_result.get(RESPONSE_COMPREHENSIVE)
        key_points_info = json_result.get(RESPONSE_KEY_POINTS)
        #print (key_points_info[0])
        key_points = [
            KeyPoint(kp_label=kp.get('kp_label'), details=kp.get('content'))
            for kp in key_points_info
        ]
        policy_analysis_data.key_points = key_points
        impact_analysis_info = json_result.get(RESPONSE_IMPACT_ANALYSIS)
        impact_analysis = [
            PolicyImpact(tag=impact.get('tag'), type_="industry", details=impact.get('details'))
            for impact in impact_analysis_info
        ]
        #print (f"policy_impact_info[0]: {impact_analysis_info[0]}")
        #print (f"policy_impact[0]: {impact_analysis[0]}")
        policy_analysis_data.impact_analysis = impact_analysis
        
        
        logger.info(f'ai_analysis_phase5: id={policy_analysis_data.id}: title: {policy_analysis_data.title_en}')
        logger.info(f"ai_analysis_phase5: id={policy_analysis_data.id}: key_points: {policy_analysis_data.key_points}"[:150])
        logger.info(f"ai_analysis_phase5: id={policy_analysis_data.id}: comprehensive analysis: {policy_analysis_data.comprehensive_analysis}"[:150])
        logger.info(f"ai_analysis_phase5: id={policy_analysis_data.id}: impact_analysis: {policy_analysis_data.impact_analysis}"[:150])
        
        policy_analysis_data.success = True
    except Exception as e:
        errmsg = f"policy_analysis id={policy_analysis_data.id}: Error encountered. {e}"
        logger.error(errmsg)
        policy_analysis_data.success = False
        policy_analysis_data.errmsg = errmsg
    
    logger.info (f"ai_analysis_phase5: id={policy_analysis_data.id}: complete. success={policy_analysis_data.success}")
    logger.info (f"ai_analysis_phase5: id={policy_analysis_data.id}: ### PHASE 5 ANALYSIS END #####################")
    return policy_analysis_data.success

def get_system_message():
    # ref_data = get_ref_data ()

    prompt = f"""
      You are an expert policy analyst. Output ONE JSON object with EXACTLY:
      
      {{
        "title": "<YOUR TITLE HERE — MUST BE A NON-EMPTY STRING>",
        "key_points": [],
        "impact_analysis": [],
        "comprehensive_analysis": ""
      }}
    
    
      CRITICAL TITLE REQUIREMENTS (READ CAREFULLY):
      - The "title" field is **MANDATORY** and **MUST NEVER be empty, missing, null, or an empty string**.
      - It must be a concise (≤150 characters), specific, and descriptive string that clearly states the policy's main action, beneficiary, and scope.
      - Good examples:
        - "Indonesia Extends Tax Incentives for Electric Vehicle Manufacturers Until 2030"
        - "EU Imposes 35% Tariff on Chinese Electric Vehicles Effective July 2026"
        - "Thailand Raises Minimum Wage in Tourism Provinces by 15% from January 2026"
      - Bad examples (never use): "", "Policy Update", "Government Notice", "New Regulation"
      
      RULES:
      - Use POLICY_TEXT + TAG_DATA only.
      - NEVER invent tags.
      - NEVER omit tags.
      - NEVER repeat tags.
      - No markdown. No extra fields.
      
      -----------------------------
      KEY POINTS RULES
      -----------------------------
      - Return 1–3 key points.
      - Format each as: {{"kp_label": "<short>", "content": "<150–250 chars>"}}
      - Each key point MUST reference at least one tag from TAG_DATA.
      - Must be factual, concise, compelling, and interesting to an expert political or expert economic analyst.
      
      -----------------------------
      IMPACT ANALYSIS RULES
      -----------------------------
      - You MUST produce **one impact object for EVERY TAG** in TAG_DATA.
        (sector_tags + industry_ICB_names +
         provinces_mentioned + regions_mentioned + countries_mentioned +
         country_regions_mentioned + continents_mentioned)
      
      - Format for each:
        {{
          "tag": "<tag>",
          "details": "<impact from policy text, ≤200 chars>"
        }}
      
      - details MUST be derived from POLICY_TEXT.
      - No speculation.
      - No repeating the POLICY_TEXT verbatim.
      - No missing tags. No extra tags.
      - Must provide factual, concise, compelling, and interesting commentary to an expert political or expert economic analyst.
      
      -----------------------------
      COMPREHENSIVE ANALYSIS RULES
      -----------------------------
      - Length: {COMPREHENSIVE_WORD_COUNT_MIN}–{COMPREHENSIVE_WORD_COUNT_MAX} words.
      - Must synthesize: event significance + tags’ roles + implications.
      - No bullet points, no headers.
      - Cohesive prose only.
      - Must provide factual, concise, compelling, and interesting commentary to an expert political or expert economic analyst.
      
      -----------------------------
      TITLE RULES
      -----------------------------
      - The "title" field MUST NEVER be empty, null, or missing.
      - Length: 150 characters or less.
      - Generate a descriptive, specific title that clearly conveys the document's purpose and primary topic.
      - Avoid generic titles like "Government Announcement" or "Policy Notice".
      - Make it concise yet informative (max 150 chars).
    
    If you generate an empty or missing title, you have failed the task. Always self-check before outputting.
    
    FINAL INSTRUCTION:
    Before outputting, perform this exact self-check:
    1. Is "title" present? → Yes/No
    2. Is "title" a non-empty string longer than 10 characters? → Yes/No
    3. Is it specific and descriptive (not generic)? → Yes/No
    
    If any answer is No, generate a proper title immediately. Only output the corrected JSON.
    
    Output only the JSON object. No text before or after.
      """

    return prompt

def get_response_format() -> str:
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name":
            "policy_analysis",  # This is just a label for OpenAI — does NOT appear in output
            "strict": True,  # Crucial: rejects any deviation
            "schema": {
                "type":
                "object",
                "additionalProperties":
                False,  # Prevents extra fields
                "required": [
                    RESPONSE_KEY_POINTS, RESPONSE_IMPACT_ANALYSIS,
                    RESPONSE_COMPREHENSIVE, RESPONSE_TITLE
                ],
                "properties": {
                    RESPONSE_KEY_POINTS: {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 3,
                        "items": {
                            "type": "object",
                            "required": ["kp_label", "content"],
                            "additionalProperties": False,
                            "properties": {
                                "kp_label": {
                                    "type": "string",
                                    # "minLength": 10,
                                    # "maxLength": 100
                                },
                                "content": {
                                    "type": "string",
                                    #"minLength": 150,
                                    #"maxLength": 250
                                }
                            }
                        }
                    },
                    RESPONSE_IMPACT_ANALYSIS: {
                        "type": "array",
                        "minItems":
                        0,  # Allow empty if no tags (though your rules say one per tag)
                        "items": {
                            "type": "object",
                            "required": ["tag", "details"],
                            "additionalProperties": False,
                            "properties": {
                                "tag": {
                                    "type": "string",
                                    "minLength": 1
                                },
                                "details": {
                                    "type": "string",
                                    #"minLength": 20,
                                    #"maxLength": 200
                                }
                            }
                        }
                    },
                    RESPONSE_COMPREHENSIVE: {
                        "type": "string",
                        #"minLength": 300,   # Adjust to match your word count needs
                        #"maxLength": 1500
                    },
                    RESPONSE_TITLE: {
                        "type":
                        "string",
                        "minLength":
                        20,  # Prevents empty or trivial titles
                        "maxLength":
                        150,
                        "description":
                        "Specific, descriptive policy title — never generic or empty"
                    }
                }
            }
        }
    }
    # print(response_format)
    return response_format

if __name__ == "__main__":
    from analysis_scripts.db_neon_wrapper import read_all
    from analysis_scripts import constants
    
    table_name = constants.TableNames.TBL_POLICIES_READONLY
    where_clause = 'id= 19492'
    ok, record = read_all(table_name=table_name, model=Policies, where_clause=where_clause)
    if not ok or not record:
        logger.error("could not find record")
    record0 = record[0]
    text = record0.english_translation
    industry_icb_tags = record0.industry_icb_tags
    
    # INITIALILZE REFERENCE DATA
    #ref_data_obj = RefData()
    #ref_data = load_ref_data_once()    
    
    policy = PolicyAnalysisData()
    policy.id = 1234
    policy.english_translation = text
    policy.industry_icb_tags = industry_icb_tags
    
    ai_master =  AIMaster()
    ai_model_params = AIModelParams()
    
    asyncio.run(ai_analysis_phase5(ai_master=ai_master, ai_model_params=ai_model_params, policy_analysis_data=policy))
