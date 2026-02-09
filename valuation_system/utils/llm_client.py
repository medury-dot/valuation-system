"""
LLM Client - Unified interface for Grok (primary), Ollama (fallback), OpenAI (fallback).
All LLM calls go through this client for consistent behavior and fallback chain.
"""

import os
import json
import logging
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI

from valuation_system.utils.resilience import retry_with_backoff, check_internet

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'config', '.env'))


class LLMClient:
    """
    Unified LLM client with fallback chain: grok → ollama → openai.

    Uses OpenAI SDK for all providers (Grok and Ollama support OpenAI-compatible APIs).
    """

    def __init__(self):
        self.provider = os.getenv('LLM_PROVIDER', 'grok')
        self.model = os.getenv('LLM_MODEL', 'grok-2-1212')
        self.fallback_chain = os.getenv('LLM_FALLBACK_CHAIN', 'grok,ollama,openai').split(',')
        self.last_call_metadata = {}

        # Initialize clients for each provider
        self._clients = {}
        self._init_clients()

    def _init_clients(self):
        """Initialize OpenAI SDK clients for each available provider."""
        # Grok (via x.ai OpenAI-compatible endpoint)
        grok_key = os.getenv('GROK_API_KEY')
        if grok_key:
            self._clients['grok'] = {
                'client': OpenAI(
                    api_key=grok_key,
                    base_url='https://api.x.ai/v1',
                ),
                'model': os.getenv('LLM_MODEL', 'grok-2-1212'),
            }

        # Ollama (local, no API key needed)
        ollama_url = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
        self._clients['ollama'] = {
            'client': OpenAI(
                api_key='ollama',
                base_url=f'{ollama_url}/v1',
            ),
            'model': os.getenv('OLLAMA_MODEL', 'mistral:7b'),
        }

        # OpenAI
        openai_key = os.getenv('OPENAI_API_KEY')
        if openai_key:
            self._clients['openai'] = {
                'client': OpenAI(api_key=openai_key),
                'model': 'gpt-4o',
            }

    def analyze(self, prompt: str, system_prompt: str = None,
                temperature: float = 0.3, max_tokens: int = 2000,
                response_format: str = None) -> str:
        """
        Send a prompt to the LLM and get a response.
        Tries providers in fallback chain order.

        Args:
            prompt: User prompt
            system_prompt: System-level instruction
            temperature: 0.0 = deterministic, 1.0 = creative
            max_tokens: Max response length
            response_format: 'json' to request JSON output

        Returns:
            Response text from the LLM.
        """
        messages = []
        if system_prompt:
            messages.append({'role': 'system', 'content': system_prompt})
        messages.append({'role': 'user', 'content': prompt})

        for provider in self.fallback_chain:
            provider = provider.strip()
            if provider not in self._clients:
                continue

            # Skip cloud providers if no internet
            if provider in ('grok', 'openai') and not check_internet(timeout=3):
                logger.warning(f"No internet, skipping {provider}")
                continue

            try:
                result = self._call_provider(
                    provider, messages, temperature, max_tokens, response_format
                )
                if result:
                    return result
            except Exception as e:
                logger.warning(f"LLM provider '{provider}' failed: {e}")
                continue

        logger.error("All LLM providers failed")
        raise RuntimeError("All LLM providers unavailable")

    def analyze_json(self, prompt: str, system_prompt: str = None,
                     temperature: float = 0.1) -> dict:
        """
        Get structured JSON response from LLM.
        Parses the response and returns a dict.
        """
        if system_prompt is None:
            system_prompt = "You are an equity research analyst. Respond ONLY with valid JSON."

        full_prompt = prompt + "\n\nRespond with valid JSON only. No markdown, no code blocks."

        response = self.analyze(full_prompt, system_prompt, temperature,
                                response_format='json')

        # Try to parse JSON from response
        return self._extract_json(response)

    @retry_with_backoff(max_retries=2, base_delay=2.0)
    def _call_provider(self, provider: str, messages: list,
                       temperature: float, max_tokens: int,
                       response_format: str = None) -> Optional[str]:
        """Call a specific LLM provider."""
        config = self._clients[provider]
        client = config['client']
        model = config['model']

        kwargs = {
            'model': model,
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens,
        }

        # Some providers support response_format for JSON mode
        if response_format == 'json' and provider in ('grok', 'openai'):
            kwargs['response_format'] = {'type': 'json_object'}

        logger.debug(f"Calling LLM provider '{provider}' (model={model})")

        response = client.chat.completions.create(**kwargs)

        # Extract usage metadata (zero extra API calls — already in response)
        usage = getattr(response, 'usage', None)
        self.last_call_metadata = {
            'model': getattr(response, 'model', model),
            'prompt_tokens': getattr(usage, 'prompt_tokens', 0) if usage else 0,
            'completion_tokens': getattr(usage, 'completion_tokens', 0) if usage else 0,
            'total_tokens': getattr(usage, 'total_tokens', 0) if usage else 0,
        }
        logger.debug(f"LLM usage: model={self.last_call_metadata['model']}, "
                      f"tokens={self.last_call_metadata['total_tokens']}")

        content = response.choices[0].message.content

        if not content:
            logger.warning(f"Empty response from {provider}")
            return None

        logger.debug(f"LLM response from {provider}: {content[:200]}...")
        return content

    def _extract_json(self, text: str) -> dict:
        """Extract JSON from LLM response, handling markdown code blocks."""
        # Remove markdown code fences if present
        cleaned = text.strip()
        if cleaned.startswith('```'):
            # Remove opening fence
            lines = cleaned.split('\n')
            start = 1
            end = len(lines)
            for i, line in enumerate(lines):
                if i > 0 and line.strip() == '```':
                    end = i
                    break
            cleaned = '\n'.join(lines[start:end])

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as e:
            # Try to find JSON object/array in the text
            for start_char, end_char in [('{', '}'), ('[', ']')]:
                start_idx = cleaned.find(start_char)
                end_idx = cleaned.rfind(end_char)
                if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                    try:
                        return json.loads(cleaned[start_idx:end_idx + 1])
                    except json.JSONDecodeError:
                        continue

            logger.error(f"Failed to parse JSON from LLM response: {e}\n"
                         f"Response: {text[:500]}")
            return {'error': 'json_parse_failed', 'raw_response': text[:1000]}
