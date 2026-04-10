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
            POLICY_TEXT:\n{text_en}
            --- TASK: PHASE 2 — TITLE, SUMMARY, IMPORTANCE ONLY ---
            You must generate ONLY the following fields for this policy text:
            1. english_title (fewer than {TITLE_CHARACTER_COUNT_MAX} characters)
            2. summary
            3. importance_score
            You MUST output a single JSON object that exactly matches the schema
            defined in the system prompt for Phase 2. No additional fields.
            Do NOT extract geography, tags, industries, sectors, or key points in this phase.
            """
    return user_message

async def ai_analysis_phase2(ai_master: AIMaster,
                       ai_model_params: AIModelParams, 
                       policy_analysis_data: PolicyAnalysisData
                       ):

    if not policy_analysis_data:
        logger.error(f"No policy found")
        return False
    logger.debug (f"id={policy_analysis_data.id}: ### PHASE 2 ANALYSIS START #####################")
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
                    f"ai_analysis_phase2: id={policy_analysis_data.id}: No data returned")
            return None, None, None
        
        
        json_result = json.loads(result)        
        
        policy_analysis_data.english_title = json_result.get('english_title', '')
        policy_analysis_data.importance_score = json_result.get('importance_score', 5)
        policy_analysis_data.description = json_result.get('summary', '')
                
        #logger.info(f"ai_analysis_phase2: id={policy_analysis_data.id}: english_title: {policy_analysis_data.english_title}"[:100])
        #logger.info(f"ai_analysis_phase2: id={policy_analysis_data.id}: importance_score: {policy_analysis_data.importance_score}")
        #logger.info(f"ai_analysis_phase2: id={policy_analysis_data.id}: description: {policy_analysis_data.description}"[:100])
                
        policy_analysis_data.success = True
    except Exception as e:
        errmsg = f"ai_analysis_phase2: id={policy_analysis_data.id}: Error encountered. {e}"
        logger.error(errmsg)
        policy_analysis_data.success = False
        policy_analysis_data.errmsg = errmsg
    
    logger.debug(f"id={policy_analysis_data.id}: complete. success={policy_analysis_data.success}")
    logger.info (f"id={policy_analysis_data.id}: ### PHASE 2 ANALYSIS END #####################")        
    return policy_analysis_data.success

def get_system_message():
    prompt = f"""
        You are an expert policy analyst.
    
        Your task in this phase is to extract ONLY the following fields from the POLICY_TEXT:
    
        {{
          "english_title": "",
          "summary": "",
          "importance_score": 0
        }}
    
        ---------------------------------------
        FIELD REQUIREMENTS
        ---------------------------------------
    
        1. english_title
           - Maximum {TITLE_CHARACTER_COUNT_MAX} characters
           - Factual headline (actor + action)
           - No adjectives, no analysis, no commentary
    
        2. summary
           - Maximum 300 characters
           - Concise, factual description of what happened and why
           - No opinions, no predictions, no rhetorical wording
    
        3. importance_score
           - Integer between 1 and 10
           - Based ONLY on the policy text
           - Scale:
               1–3: minor, routine, symbolic
               4–6: moderate regulatory, bilateral, or domestic relevance
               7–8: significant strategic, economic, or security impact
               9–10: major global importance or crisis-level development
    
        ---------------------------------------
        STRICT OUTPUT RULES
        ---------------------------------------
        - Output MUST be one JSON object only
        - No markdown, no comments, no explanations
        - Do NOT include any additional fields
        """
    #logger.info("#####################################")
    #logger.info(prompt)
    return prompt

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
            
            policy = PolicyAnalysisData()
            policy.id = 1234
            policy.english_translation = text
            
            ai_master =  AIMaster()
            ai_model_params = AIModelParams()
            
            ai_analysis_phase2(ai_master=ai_master, ai_model_params=ai_model_params, policy=policy)
    except Exception as e:
        logger.error(f"Error encountered. {e}")
    finally:
        if db_manager:
            db_manager.db_close()    
