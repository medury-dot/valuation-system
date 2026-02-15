"""
Quick test: Verify LLM auto-detection works
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from valuation_system.utils.llm_client import LLMClient

print("=" * 60)
print("Testing LLM Auto-Detection")
print("=" * 60)

# Initialize client (will auto-detect providers)
client = LLMClient()

print(f"\nConfigured fallback chain: {client.fallback_chain}")
print(f"Daily budget: ${client.daily_budget_usd}")
print(f"Available clients: {list(client._clients.keys())}")

# Test a simple call
print("\n" + "=" * 60)
print("Testing LLM call with auto-detection...")
print("=" * 60)

try:
    response = client.analyze(
        prompt="What is 2+2? Answer in one word.",
        temperature=0.1,
        max_tokens=10
    )
    print(f"\n✅ LLM call succeeded!")
    print(f"Response: {response}")
    print(f"Provider used: {client.last_call_metadata.get('model', 'unknown')}")
    print(f"Tokens used: {client.last_call_metadata.get('total_tokens', 0)}")
except Exception as e:
    print(f"\n❌ LLM call failed: {e}")

print("\n" + "=" * 60)
print("Test complete!")
print("=" * 60)
