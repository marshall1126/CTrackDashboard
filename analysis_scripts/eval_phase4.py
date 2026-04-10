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
from analysis_scripts.models import PolicyAnalysisData
from analysis_scripts.reference_data import get_ref_data,  RefData, load_ref_data_once

TITLE_CHARACTER_COUNT_MAX = 150

def get_user_message(text_en: str) -> str:
    user_message = f""" 
            POLICY_TEXT:\n{text_en}
            PHASE 4 — ICB ONLY
            Return ONE JSON object:
            {{\"industry_ICB_names\": []}}
            RULES:\n
            - Pick ONLY from the allowed ICB list in the system prompt.
            - **CRITICAL: The list format is INDUSTRY_ICB_NAME | SCOPE. Output ONLY the INDUSTRY_ICB_NAME (the text BEFORE the '|').**
            - Use the SCOPE (after the '|') to determine if the policy matches that industry.
            - Output = JSON array of plain strings.
            - Choose 1–3 if the policy text provides clear evidence of regulatory, operational, or transactional impact that aligns
              with the industry's scope definition.
            - If none apply or evidence is too generic, return [].
            - No other fields. No objects. No explanations. No markdown.
        """

    return user_message

async def ai_analysis_phase4(ai_master: AIMaster,
                       ai_model_params: AIModelParams, 
                       policy_analysis_data: PolicyAnalysisData
                       ):
    
    if not policy_analysis_data:
        logger.error("No policy found")
        return False
    logger.debug(f"ai_analysis_phase4: id={policy_analysis_data.id}: ### PHASE 4 ANALYSIS START #####################")
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
        result =await ai_master.execute(messages=messages, model_params=ai_model_params)
        if not result:
            logger.error(
                    "PolicyAnalysis: error during phase 4 for {policy_analysis.id}")
            return None, None, None
        
        
        json_result = json.loads(result)        
        policy_analysis_data.industry_ICB_tags = json_result.get('industry_ICB_names')
        logger.debug(f"id={policy_analysis_data.id}: industry_icb_tags: {policy_analysis_data.industry_ICB_tags}")
        
        policy_analysis_data.success = True
    except Exception as e:
        errmsg = f"policy id={policy_analysis_data.id}: Error encountered. {e}"
        logger.error(errmsg)
        policy_analysis_data.success = False
        policy_analysis_data.errmsg = errmsg
    
    logger.debug(f"id={policy_analysis_data.id}: complete. success={policy_analysis_data.success}")
    logger.info (f"id={policy_analysis_data.id}: ### PHASE 4 ANALYSIS END #####################")
    return policy_analysis_data.success

def get_system_message():
    ref_data = get_ref_data ()

    # Format scope definitions as a JSON object for the prompt
    items =  ref_data.icb_industries.items()
    icb_scope_list = [
        f"{subsector} | {meta.get('scope', '')}"
        for subsector, meta in sorted(ref_data.icb_industries.items(), key=lambda kv: kv[0])
    ]
    formatted_icb_industries = "\n".join(icb_scope_list)

    prompt = f"""
        You are an expert policy analyst specializing in economic and industrial classification.

        Your task in THIS PHASE ONLY is to output exactly ONE field inside ONE JSON object:

        {{
          "industry_ICB_names": []
        }}

        The scope definitions are provided as a JSON object mapping industry INDUSTRY_ICB_NAME
        (must match the ALLOWED list INDUSTRY_ICB_NAME exactly) to scope definition.

        ------------------------------------------------------------
        ICB INDUSTRY RULES — FOLLOW EXACTLY
        ------------------------------------------------------------

        1. Your output MUST be a JSON array of **plain strings only**.
        2. You MUST select only from the ALLOWED ICB industry list shown below.
        3. You MUST match industry names EXACTLY as written.
        4. **CRITICAL:** The ALLOWED ICB list uses the format INDUSTRY_ICB_NAME : SCOPE. You MUST only return the INDUSTRY_ICB_NAME
           portion (the text after the colon) for your JSON output.
        5. You MUST NOT return objects. NEVER output:
               {{ "tag_name": "…" }}
               {{ "details": "…" }}
        6. You MUST NOT substitute, generalize, or infer common industry names not in the list. For example, NEVER output: **"Shipbuilding"**, "Defense Technology", "Retail Banking", or "E-commerce".
        7. You MUST NOT output topic tags (e.g., "Diplomatic Relations"), any labels not in the ALLOWED ICB list, geographic names, or political categories.
        8. You MUST NOT guess or infer. Only select industries explicitly supported by the POLICY_TEXT with concrete, industry-specific evidence.
        9. SCOPE-GATED SELECTION (MANDATORY):
           Every allowed ICB industry has a scope definition. You MUST use the scope definition as a hard constraint. Only select an industry if the POLICY_TEXT clearly matches that scope.

        10. If the POLICY_TEXT is broadly related but does NOT match the scope definition tightly, you MUST NOT select the industry.

        11. If the POLICY_TEXT matches multiple industries, prefer the ones whose scope definition is the closest match (most specific, least stretched).

        12. You MUST NOT select an industry solely because it appears in a scope definition. The POLICY_TEXT must contain independent evidence that matches the scope.

        ------------------------------------------------------------
        SELECTION REQUIREMENTS
        ------------------------------------------------------------

        - Choose **1 to 3** industries ONLY when BOTH conditions hold:
            (A) POLICY_TEXT provides direct, clear evidence of impact on the industry, AND
            (B) the evidence fits the industry's scope definition.

        - If evidence is generic, administrative, diplomatic, or macro without an industry mechanism, return [].

        - If more than 3 industries fit, choose the strongest 3 based on:
            1) explicitness of linkage to the scope,
            2) directness of the policy mechanism (funding, regulation, standards, procurement, licensing),
            3) specificity (named activity, product, service, or supply-chain node).

        ------------------------------------------------------------
        FORMAT EXAMPLES (REAL INDUSTRIES)
        ------------------------------------------------------------

        Correct:
          "industry_ICB_names": ["Telecommunications Services"]
          "industry_ICB_names": ["Coal", "Oil Equipment and Services"]
          "industry_ICB_names": ["Trucking", "Marine Transportation", "Airlines"]
          "industry_ICB_names": []

        Incorrect (DO NOT DO THIS):
          "industry_ICB_names": "Telecommunications Services"
          "industry_ICB_names": ["Diplomatic Relations"]
          "industry_ICB_names": [{{"tag_name": "Trucking"}}]

        ------------------------------------------------------------
        ALLOWED ICB INDUSTRIES WITH SCOPE DEFINITIONS
        ------------------------------------------------------------
        
        Each entry below shows: INDUSTRY_ICB_NAME | SCOPE_DEFINITION
        
        - You MUST output only the INDUSTRY_ICB_NAME (before the colon)
        - You MUST use the SCOPE_DEFINITION (after the '|') to determine if the policy fits
        - The scope definitions are mandatory constraints for selection
        
        BEGIN_ICB_SCOPE_DEFINITIONS
        {formatted_icb_industries}
        END_ICB_SCOPE_DEFINITIONS

        ------------------------------------------------------------
        STRICT OUTPUT FORMAT
        ------------------------------------------------------------

        - Output ONLY one JSON object.
        - The ONLY field allowed is:
              "industry_ICB_names"
        - No markdown. No analysis. No commentary.
        - If no industries apply, output:
              {{"industry_ICB_names": []}}
        - You MUST NOT output or repeat the scope definitions or the allowed list.
        - The scope definitions are read-only reference data and must not be modified or reinterpreted.
        """
        # logger.info(prompt)

    return prompt

if __name__ == "__main__":
    try:
        from analysis_scripts import constants
        from analysis_scripts.models import Policies
        from analysis_scripts.database.neon_manager import NeonManager, NeonConnectionMode
        
        db_manager = NeonManager(NeonConnectionMode.POOLER)
        if not db_manager:
            logger.info("No database connection")
            exit
        ok = db_manager.db_connect()
        if not ok:
            logger.info("No database connection")
            exit
        
        table_name = constants.TableNames.TBL_POLICIES
        where_clause = {id: 19492}
        ok, record = db_manager.db_select(table_name=table_name, model=Policies, where=where_clause)
        if not ok or not record:
            logger.error("could not find record")
        record0 = record[0]
        text = record0.english_translation
        
        # INITIALILZE REFERENCE DATA
        ref_data_obj = RefData()
        ref_data = load_ref_data_once()    
        
        policy = PolicyAnalysisData()
        policy.id = 1234
        policy.english_translation = text
        
        ai_master =  AIMaster()
        ai_model_params = AIModelParams()
        
        asyncio.run(ai_analysis_phase4(ai_master=ai_master, ai_model_params=ai_model_params, policy_analysis_data=policy))
    except Exception as e:
        logger.error(f"Error encountered. {e}")
    finally:
        if db_manager:
            db_manager.db_close()    
