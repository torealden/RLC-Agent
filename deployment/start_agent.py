#!/usr/bin/env python3
"""
RLC Master Agent - Tool-Calling Agent for RLC-SERVER

This agent has REAL capabilities - it can read files, run collectors,
query databases, and search the web. It uses local Ollama for reasoning
and calls tools to actually DO things.

Usage:
    python start_agent.py              # Interactive mode with tools
    python start_agent.py --no-tools   # Chat-only mode (no tool calling)
    python start_agent.py --task "collect CFTC data"  # Single task
"""

import asyncio
import os
import sys
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import argparse

# Add deployment directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Import tools
try:
    from agent_tools import TOOLS, execute_tool, get_tools_description, ToolResult
    TOOLS_AVAILABLE = True
except ImportError:
    TOOLS_AVAILABLE = False
    print("Warning: agent_tools module not found. Running in chat-only mode.")

# Configuration
DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5:7b"
FALLBACK_MODELS = ["llama3.1:8b", "phi3:mini"]


class OllamaClient:
    """Client for local Ollama API with tool-calling support."""

    def __init__(self, base_url: str = DEFAULT_OLLAMA_URL, model: str = DEFAULT_MODEL):
        self.base_url = base_url
        self.model = model
        self._session = None

    async def _ensure_session(self):
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def is_available(self) -> bool:
        try:
            session = await self._ensure_session()
            async with session.get(f"{self.base_url}/api/tags", timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def list_models(self) -> list:
        try:
            session = await self._ensure_session()
            async with session.get(f"{self.base_url}/api/tags") as resp:
                data = await resp.json()
                return [m["name"] for m in data.get("models", [])]
        except Exception:
            return []

    async def chat(
        self,
        messages: list,
        system: Optional[str] = None,
        temperature: float = 0.7,
    ) -> str:
        session = await self._ensure_session()

        all_messages = []
        if system:
            all_messages.append({"role": "system", "content": system})
        all_messages.extend(messages)

        payload = {
            "model": self.model,
            "messages": all_messages,
            "stream": False,
            "options": {"temperature": temperature}
        }

        async with session.post(f"{self.base_url}/api/chat", json=payload) as resp:
            if resp.status != 200:
                error = await resp.text()
                raise Exception(f"Ollama error: {error}")
            data = await resp.json()
            return data["message"]["content"]


class RLCMasterAgent:
    """
    Master Agent with Tool-Calling Capabilities.

    This agent can:
    - Read and write files
    - Run data collectors
    - Query the commodity database
    - Search the web
    - Execute Python code for analysis
    """

    def __init__(
        self,
        ollama_url: str = DEFAULT_OLLAMA_URL,
        model: str = DEFAULT_MODEL,
        enable_tools: bool = True
    ):
        self.llm = OllamaClient(ollama_url, model)
        self.enable_tools = enable_tools and TOOLS_AVAILABLE
        self.conversation_history: List[Dict[str, str]] = []
        self.max_tool_iterations = 5  # Prevent infinite loops

        # Paths
        self.project_root = Path(__file__).parent.parent
        self.data_dir = self.project_root / "data"
        self.transcripts_dir = Path("C:/RLC/whisper/transcripts")

        # Build system prompt with tools
        self._build_system_prompt()

    def _build_system_prompt(self):
        """Build system prompt with tool descriptions."""
        base_prompt = """You are the RLC Master Agent, an AI assistant for Round Lakes Companies.

You help with three businesses:
1. B2B Commodity Analysis - Fundamental analysis of grain and oilseed markets
2. Physical Ranch Operations (future)
3. Statistical Arbitrage Trading (future)

Current focus: Building supply/demand balance sheets for corn, wheat, and soybean markets."""

        if self.enable_tools:
            tools_desc = get_tools_description()
            self.system_prompt = f"""{base_prompt}

YOU HAVE ACCESS TO TOOLS. When you need to:
- Read files → use read_file
- List directories → use list_directory
- Run data collectors → use run_collector
- Query the database → use query_database
- Search the web → use search_web

TO USE A TOOL, respond with this exact format:
<tool>tool_name</tool>
<params>
{{"param1": "value1", "param2": "value2"}}
</params>

After I execute the tool, I'll show you the result, and you can continue reasoning.

{tools_desc}

IMPORTANT:
- Only use one tool at a time
- Wait for the tool result before continuing
- If a tool fails, explain what went wrong and try an alternative
- Be concise and action-oriented"""
        else:
            self.system_prompt = f"""{base_prompt}

You are in chat-only mode (no tools available). You can:
- Answer questions about commodity markets
- Help plan data collection strategies
- Discuss analysis approaches

To actually execute tasks, the user needs to run commands manually or enable tool mode."""

    async def initialize(self) -> bool:
        """Initialize the agent."""
        print("\n" + "=" * 60)
        print("  RLC Master Agent - Initializing")
        print("=" * 60)

        # Check Ollama
        if not await self.llm.is_available():
            print("❌ Cannot connect to Ollama")
            print(f"   URL: {self.llm.base_url}")
            return False

        # Find suitable model
        models = await self.llm.list_models()
        model_found = False

        for m in models:
            if self.llm.model in m or m in self.llm.model:
                self.llm.model = m
                model_found = True
                break

        if not model_found:
            for fallback in FALLBACK_MODELS:
                for m in models:
                    if fallback in m:
                        self.llm.model = m
                        model_found = True
                        break
                if model_found:
                    break

        if not model_found and models:
            self.llm.model = models[0]

        print(f"✅ Connected to Ollama ({self.llm.model})")
        print(f"✅ Tools: {'ENABLED' if self.enable_tools else 'DISABLED'}")

        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)

        return True

    def _parse_tool_call(self, response: str) -> Optional[Dict[str, Any]]:
        """Parse a tool call from the LLM response."""
        # Look for <tool>...</tool> and <params>...</params>
        tool_match = re.search(r'<tool>\s*(\w+)\s*</tool>', response, re.IGNORECASE)
        params_match = re.search(r'<params>\s*(\{.*?\})\s*</params>', response, re.IGNORECASE | re.DOTALL)

        if tool_match:
            tool_name = tool_match.group(1).strip()
            params = {}

            if params_match:
                try:
                    params = json.loads(params_match.group(1))
                except json.JSONDecodeError:
                    # Try to fix common JSON issues
                    params_str = params_match.group(1)
                    params_str = params_str.replace("'", '"')
                    try:
                        params = json.loads(params_str)
                    except:
                        params = {}

            return {"tool": tool_name, "params": params}

        return None

    async def process_with_tools(self, user_input: str) -> str:
        """Process user input with potential tool calls."""
        self.conversation_history.append({"role": "user", "content": user_input})

        iterations = 0
        final_response = ""

        while iterations < self.max_tool_iterations:
            iterations += 1

            # Get LLM response
            response = await self.llm.chat(
                messages=self.conversation_history,
                system=self.system_prompt,
                temperature=0.3  # Lower temperature for more consistent tool use
            )

            # Check for tool call
            tool_call = self._parse_tool_call(response)

            if tool_call:
                tool_name = tool_call["tool"]
                params = tool_call["params"]

                print(f"\n🔧 Calling tool: {tool_name}")
                if params:
                    print(f"   Params: {json.dumps(params, indent=2)}")

                # Execute the tool
                result = execute_tool(tool_name, **params)

                # Format result for display
                result_str = str(result)
                if len(result_str) > 1000:
                    result_str = result_str[:1000] + "\n... (truncated)"

                print(f"\n📋 Tool Result:\n{result_str}")

                # Add to conversation for context
                self.conversation_history.append({
                    "role": "assistant",
                    "content": response
                })
                self.conversation_history.append({
                    "role": "user",
                    "content": f"Tool result for {tool_name}:\n{result_str}\n\nContinue with your analysis or next action."
                })

            else:
                # No tool call - this is the final response
                final_response = response
                self.conversation_history.append({
                    "role": "assistant",
                    "content": response
                })
                break

        return final_response

    async def process_simple(self, user_input: str) -> str:
        """Process without tool calling (chat only)."""
        self.conversation_history.append({"role": "user", "content": user_input})

        response = await self.llm.chat(
            messages=self.conversation_history,
            system=self.system_prompt
        )

        self.conversation_history.append({"role": "assistant", "content": response})
        return response

    async def run_interactive(self):
        """Run interactive mode."""
        print("\n" + "=" * 60)
        print("  RLC Master Agent - Ready")
        print("=" * 60)

        if self.enable_tools:
            print("\n🔧 TOOL MODE ENABLED - I can actually DO things!")
            print("   Try: 'List the available data collectors'")
            print("   Try: 'What tables are in the database?'")
            print("   Try: 'Read the DATA_SOURCE_REGISTRY.md file'")
        else:
            print("\n💬 CHAT MODE - Planning and discussion only")

        print("\nCommands:")
        print("  'tools'   - List available tools")
        print("  'status'  - Check system status")
        print("  'clear'   - Clear conversation history")
        print("  'quit'    - Exit")
        print()

        while True:
            try:
                user_input = input("You: ").strip()

                if not user_input:
                    continue

                # Handle special commands
                if user_input.lower() in ("quit", "exit", "q"):
                    print("\nGoodbye! Your AI business partner will be here when you return.")
                    break

                if user_input.lower() == "tools":
                    if self.enable_tools:
                        print("\n" + get_tools_description() + "\n")
                    else:
                        print("\nTools are disabled. Start with: python start_agent.py\n")
                    continue

                if user_input.lower() == "status":
                    await self._show_status()
                    continue

                if user_input.lower() == "clear":
                    self.conversation_history = []
                    print("\n✅ Conversation cleared.\n")
                    continue

                # Process the request
                print("\n🔄 Processing...")

                if self.enable_tools:
                    response = await self.process_with_tools(user_input)
                else:
                    response = await self.process_simple(user_input)

                print(f"\n🤖 Agent: {response}\n")

            except KeyboardInterrupt:
                print("\n\nInterrupted. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}\n")
                import traceback
                traceback.print_exc()

    async def _show_status(self):
        """Show system status."""
        print("\n--- System Status ---")
        print(f"Model: {self.llm.model}")
        print(f"Ollama: {self.llm.base_url}")
        print(f"Tools: {'Enabled' if self.enable_tools else 'Disabled'}")
        print(f"Conversation History: {len(self.conversation_history)} messages")

        if self.enable_tools:
            # Use the status tool
            result = execute_tool("get_system_status")
            if result.success:
                print(f"\nSystem Info:")
                for key, value in result.data.items():
                    print(f"  {key}: {value}")
        print()

    async def shutdown(self):
        """Clean shutdown."""
        await self.llm.close()


async def main():
    parser = argparse.ArgumentParser(description="RLC Master Agent with Tools")
    parser.add_argument(
        "--ollama-url",
        default=os.environ.get("OLLAMA_URL", DEFAULT_OLLAMA_URL),
        help="Ollama API URL"
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("OLLAMA_MODEL", DEFAULT_MODEL),
        help="LLM model to use"
    )
    parser.add_argument(
        "--no-tools",
        action="store_true",
        help="Disable tool calling (chat only)"
    )
    parser.add_argument(
        "--task",
        help="Run a specific task and exit"
    )

    args = parser.parse_args()

    agent = RLCMasterAgent(
        ollama_url=args.ollama_url,
        model=args.model,
        enable_tools=not args.no_tools
    )

    if not await agent.initialize():
        print("\n❌ Failed to initialize. Exiting.")
        sys.exit(1)

    try:
        if args.task:
            print(f"\n🔄 Running task: {args.task}")
            if agent.enable_tools:
                response = await agent.process_with_tools(args.task)
            else:
                response = await agent.process_simple(args.task)
            print(f"\n🤖 Agent: {response}")
        else:
            await agent.run_interactive()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
