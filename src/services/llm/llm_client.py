"""
LLM Client

Thin wrapper that dispatches LLM calls to Anthropic or Ollama based on
the ModelConfig's provider. Consolidates three existing call patterns
into one function.

Usage:
    from src.services.llm.model_router import ModelRouter
    from src.services.llm.llm_client import call_llm

    router = ModelRouter()
    model = router.route('summary', sensitivity=0)
    response = call_llm(model, 'You are a commodity analyst.', 'Summarize corn S&D.')
    print(response.text)
"""

import logging
import os
import time
from dataclasses import dataclass

from src.services.llm.model_router import ModelConfig

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Standardized response from any LLM provider."""
    text: str
    tokens_in: int
    tokens_out: int
    latency_ms: int
    model_id: str
    provider: str


def call_llm(
    model: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int = None,
    temperature: float = None,
) -> LLMResponse:
    """
    Call an LLM and return a standardized response.

    Dispatches to Anthropic SDK or Ollama REST based on model.provider.
    Uses lazy imports to avoid loading unused SDKs.

    Args:
        model: ModelConfig from ModelRouter.route()
        system_prompt: System-level instruction
        user_prompt: User message / prompt body
        max_tokens: Override model default (optional)
        temperature: Override model default (optional)

    Returns:
        LLMResponse with text, token counts, and latency

    Raises:
        RuntimeError: On API failure or missing credentials
    """
    effective_max_tokens = max_tokens or model.max_tokens
    effective_temperature = temperature if temperature is not None else model.temperature

    if model.provider == 'anthropic':
        return _call_anthropic(model, system_prompt, user_prompt,
                               effective_max_tokens, effective_temperature)
    elif model.provider == 'ollama':
        return _call_ollama(model, system_prompt, user_prompt,
                            effective_max_tokens, effective_temperature)
    else:
        raise ValueError(f"Unknown provider: {model.provider}")


def _call_anthropic(
    model: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float,
) -> LLMResponse:
    """Call Anthropic Claude API."""
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in environment")

    client = anthropic.Anthropic(api_key=api_key)

    start = time.perf_counter()
    message = client.messages.create(
        model=model.model_id,
        max_tokens=max_tokens,
        temperature=temperature,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    latency_ms = int((time.perf_counter() - start) * 1000)

    return LLMResponse(
        text=message.content[0].text,
        tokens_in=message.usage.input_tokens,
        tokens_out=message.usage.output_tokens,
        model_id=model.model_id,
        provider='anthropic',
        latency_ms=latency_ms,
    )


def _call_ollama(
    model: ModelConfig,
    system_prompt: str,
    user_prompt: str,
    max_tokens: int,
    temperature: float,
) -> LLMResponse:
    """Call Ollama local LLM via REST API."""
    import requests

    base_url = os.environ.get("OLLAMA_URL", "http://localhost:11434")

    start = time.perf_counter()
    response = requests.post(
        f"{base_url}/api/generate",
        json={
            "model": model.model_id,
            "prompt": f"{system_prompt}\n\n{user_prompt}",
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        },
        timeout=300,
    )
    latency_ms = int((time.perf_counter() - start) * 1000)

    if response.status_code != 200:
        raise RuntimeError(
            f"Ollama returned {response.status_code}: {response.text[:200]}"
        )

    data = response.json()
    text = data.get("response", "")

    # Ollama may or may not return token counts depending on version
    tokens_in = data.get("prompt_eval_count", 0)
    tokens_out = data.get("eval_count", 0)

    return LLMResponse(
        text=text,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        model_id=model.model_id,
        provider='ollama',
        latency_ms=latency_ms,
    )
