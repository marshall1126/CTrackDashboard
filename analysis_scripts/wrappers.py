import asyncio
from pathlib import Path
import sys
from typing import Optional

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logger import get_logger
logger = get_logger(__name__)

from analysis_scripts.db_neon_wrapper import Policies
from analysis_scripts.translator import Translator

class Wrapper:
    def __init__(self, translator: Translator):
        self.translator:Translator = translator
        
    def wrap_translate(self, policy: Policies):
        if not policy:
            logger.error("No policy found")
            return False
        id = policy.id
        if not id:
            logger.error("No policy id found")
            return False    
        text = policy.chinese_original
        if not text:
            logger.error(f"{id}: no policy text found")
            return False
        
        ok, trans_text, errmsg = asyncio.run(self.translator.translate(text, 
                    story_id=id))
        
        if not ok:
            policy.status = Policies.FAILED
            policy.errmsg = errmsg
            return False
        
        policy.english_translation = trans_text
        