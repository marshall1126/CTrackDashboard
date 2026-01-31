from pathlib import Path
import sys

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
from analysis_scripts.ai_models_list import ai_models

class AIModelParams:
    temperature: float = .1
    model_type: str = ai_models.CHAT_GPT_4o_MINI
    max_completion_tokens: int = 3500
    top_p: float = 1.0