"""Quick test of the ORCHESTRATOR_DISPATCHER_AGENT via REST API."""
import asyncio
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestrator.backend.services.cortex_agent_client import CortexAgentClient

os.environ["SNOWFLAKE_CONNECTION_NAME"] = "my_snowflake"
os.environ["CORTEX_AGENT_NAME"] = "ORCHESTRATOR_DISPATCHER_AGENT"


async def test_dispatch():
    client = CortexAgentClient()

    prompts = [
        "We need to build a patient risk stratification system from our FHIR EHR data with real-time analytics",
        "Help us detect adverse drug events from FDA FAERS reports for our oncology medications",
        "Process and extract structured data from 10,000 radiology reports and discharge summaries",
    ]

    for prompt in prompts:
        print(f"\n{'='*80}")
        print(f"PROMPT: {prompt}")
        print(f"{'='*80}")

        full_text = ""
        tool_results = []
        async for event in client.run_agent(prompt):
            etype = event.get("type", "")
            if etype == "text":
                content = event.get("content", "")
                full_text += content
                print(content, end="", flush=True)
            elif etype == "thinking":
                pass
            elif etype == "tool_result":
                tool_results.append(event)
                print(f"\n  [TOOL: {event.get('tool_name', '?')}]")
                content = event.get("content", "")
                print(f"  {content[:300]}")
            elif etype == "tool_use":
                print(f"\n  [CALLING: {event.get('tool_name', '?')}]")
            elif etype == "status":
                status = event.get("status", "")
                msg = event.get("message", "")
                if status in ("planning", "executing_tools"):
                    print(f"\n  [{status}] {msg}")
            elif etype == "error":
                print(f"\n  [ERROR: {event.get('content', '')}]")

        print(f"\n\n--- Summary: text={len(full_text)} chars, tools={len(tool_results)} ---")


if __name__ == "__main__":
    asyncio.run(test_dispatch())
