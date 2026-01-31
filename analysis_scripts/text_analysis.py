import asyncio
from pathlib import Path
import sys

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# LOGGER    
from logger import get_logger

logger = get_logger(__name__)

from analysis_scripts import constants
from analysis_scripts.ai_master import AIMaster
from analysis_scripts.db_neon_wrapper import Policies
from analysis_scripts.reference_data import RefData
from analysis_scripts.eval_phase1 import eval_phase1

class TextAnalysis:
    def __init__(self, ai_client:  AIMaster):
        self.ai_client = ai_client
        
    def analyze_text(policy: Policies):
        text = policy.english_translation
        if not text:
            errmsg =  f"No text to analylze"
            policy.success = False
            policy.errmsg = errmsg
            logger.error(errmsg)
            return False
        
        ok = eval_phase1(ai_client, policy)
        if not ok:
            return False
            
        