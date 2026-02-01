import asyncio
import sys
from typing import Optional

from openai import AsyncOpenAI, APIError, RateLimitError, APIStatusError

from logger import get_logger
logger = get_logger(__name__)

# PROJECT IMPORTS
from analysis_scripts.ai_model_params import AIModelParams
from enviro import EnvKey, get as get_env_value

TRANSLATION_CHUNK_SIZE = 15000

class AIMaster:
  def __init__(self):
    try:
      # --- OpenAI Setup ---
      # key = os.getenv("OPENAI_API_KEY")
      key = get_env_value(EnvKey.OPENAI_API_KEY)
      if not key:
        logger.critical("OPENAI_API_KEY environment variable not set")
        sys.exit("OPENAI_API_KEY not set")
      self.ai_client = AsyncOpenAI(
            api_key=key,
            timeout=90.0,          # generous but not forever
            max_retries=0          # we handle retries ourselves
        )
      
    except Exception as e:
      logger.error(f"AIManager: could nnot initialize. {e}")

  def get_client(self):
    return self.ai_client

  async def execute(self,
                    messages,
                    model_params: AIModelParams,
                    response_format: Optional[dict] = None                    ):
    max_retries = 3
    backoff = 1.5
    
    params = {
                "model": model_params.model_type,
                "messages": messages,
                "temperature": model_params.temperature,
                "max_tokens": model_params.max_completion_tokens,  # note name change
            }
    if response_format is not None:
      params["response_format"] = response_format    

    for attempt in range(max_retries + 1):
      try:
        response = await self.ai_client.chat.completions.create(**params)
    
        content = response.choices[0].message.content
        if not content or not content.strip():
          logger.warning("Received empty content from API")
          return None
    
        return content.strip()
    
      except RateLimitError as e:
        logger.warning(f"Rate limit (attempt {attempt+1}/{max_retries+1})")
        if attempt == max_retries:
          raise
        await asyncio.sleep(backoff + 0.5 * attempt)  # light jitter
        backoff *= 2.3
    
      except APIStatusError as e:
        code = getattr(e, 'status_code', 0)
        if code in (400, 401, 403, 404):  # client errors → no retry
          logger.error(f"Non-retryable error {code}: {e}")
          raise
        logger.warning(f"Server error {code} (attempt {attempt+1})")
        await asyncio.sleep(backoff)
        backoff *= 2
    
      except APIError as e:
        logger.error(f"OpenAI API error: {e}")
        if attempt == max_retries:
          raise
        await asyncio.sleep(backoff)
        backoff *= 1.8
    
      except Exception as e:
        logger.exception(f"Unexpected error (attempt {attempt+1})")
        if attempt == max_retries:
          raise
        await asyncio.sleep(backoff)

      return None


