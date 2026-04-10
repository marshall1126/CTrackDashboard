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
from analysis_scripts.models import PolicyAnalysisData
from analysis_scripts.reference_data import get_ref_data,  RefData, load_ref_data_once

TITLE_CHARACTER_COUNT_MAX = 150

def get_user_message(text_en: str) -> str:
    user_message = f""" 
            POLICY_TEXT:
            {text_en}

            Return the JSON now using the rules from the system instructions.
            """
    return user_message

async def ai_analysis_phase3(ai_master: AIMaster,
                       ai_model_params: AIModelParams, 
                       policy_analysis_data: PolicyAnalysisData
                       ):
    if not policy_analysis_data:
        logger.error("No policy found")
        return False
    logger.info(f"id={policy_analysis_data.id}: ### PHASE 3 ANALYSIS START #####################")
    english_translation = policy_analysis_data.english_translation
    if not english_translation:
        policy_analysis_data.errmsg =  f"id={policy_analysis_data.id}: No english_translation found"
        logger.error(policy_analysis_data.errmsg)
        policy_analysis_data.success = False
        return False
    try:
        # GET PROMPT
        user_message = get_user_message(policy_analysis_data.english_translation)
        system_message = get_system_message()

        # SUBMIT ANALYSIS
         # Build proper OpenAI-style messages list
        messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message},
            ]                
        result = await ai_master.execute(messages=messages, model_params=ai_model_params)
        if not result:
            logger.error(
                    f"ai_analysis_phase3: id={policy_analysis_data.id}: No data returned")
            return None, None, None
        
        
        json_result = json.loads(result)        
        
        policy_analysis_data.topics_tags = json_result.get('topics_tags', [])
        
        logger.debug (f"id={policy_analysis_data.id}: topics_tags: {policy_analysis_data.topics_tags}"[:100])
                
        policy_analysis_data.success = True
    except Exception as e:
        errmsg = f"ai_analysis_phase2: id={policy_analysis_data.id}: Error encountered. {e}"
        logger.error(errmsg)
        policy_analysis_data.success = False
        policy_analysis_data.errmsg = errmsg
    
    logger.debug(f"id={policy_analysis_data.id}: complete. success={policy_analysis_data.success}")
    logger.info(f"ai_analysis_phase3: id={policy_analysis_data.id}: ### PHASE 3 ANALYSIS END #####################")            
    return policy_analysis_data.success

def get_system_message():
    ref_data = get_ref_data ()
    formatted_topics = "\n".join(ref_data.topics.keys())
    prompt = f"""
           You are an expert policy analyst performing the final tagging phase.

           YOUR ONLY JOB is to return exactly one valid JSON object and nothing else.
       
           Required JSON format:
           {{
             "topics_tags": [],
           }}
       
           MANDATORY RULES (you must obey every single one):
       
           1. topics_tags
              • Always return 1–3 tags
              • Choose ONLY from this exact list: {formatted_topics}
              • YOU MUST include "Diplomatic Relations" if the policy mentions diplomacy, foreign ministers, bilateral/multilateral meetings, international relations, summits, ambassadors, strategic dialogue, or anything similar
              • NEVER return an empty topics_tags array
       
           Correct examples (do not copy – follow the same pattern):
           {{ "topics_tags": ["Diplomatic Relations"]}}
           {{ "topics_tags": ["Monetary Policy", "Fintech/Digital Banking"]}}
       
           Now read the policy text and return ONLY the JSON object:
    """.strip()

    #logger.info("#####################################")
    #logger.info(prompt)
    return prompt

if __name__ == "__main__":
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
    
    ai_analysis_phase3(ai_master=ai_master, ai_model_params=ai_model_params, policy=policy_analysis_data)
