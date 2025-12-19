#!/usr/bin/env python3
"""
RLC Master Agent - Main Entry Point for RLC-SERVER

This is the main entry point that runs on RLC-SERVER. It:
1. Connects to the local Ollama LLM
2. Initializes all agent teams (data collection, analysis, reporting)
3. Runs the scheduler for automated data pulls
4. Provides an interactive interface for ad-hoc requests

Usage:
    # Start in interactive mode
    python start_agent.py

    # Start with scheduler only (background mode)
    python start_agent.py --scheduler-only

    # Run a specific task
    python start_agent.py --task collect-usda
    python start_agent.py --task generate-weekly-report
"""

import asyncio
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
import argparse
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Configuration
DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5:7b"
FALLBACK_MODELS = ["llama3.1:8b", "phi3:mini"]


class OllamaClient:
    """Simple client for local Ollama API."""

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
        """Check if Ollama is running and accessible."""
        try:
            session = await self._ensure_session()
            async with session.get(f"{self.base_url}/api/tags", timeout=5) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def list_models(self) -> list:
        """List available models."""
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
        stream: bool = False
    ) -> str:
        """Send a chat request to Ollama."""
        session = await self._ensure_session()

        payload = {
            "model": self.model,
            "messages": messages,
            "stream": stream,
            "options": {"temperature": temperature}
        }

        if system:
            payload["messages"] = [{"role": "system", "content": system}] + messages

        async with session.post(
            f"{self.base_url}/api/chat",
            json=payload
        ) as resp:
            if resp.status != 200:
                error = await resp.text()
                raise Exception(f"Ollama error: {error}")

            data = await resp.json()
            return data["message"]["content"]

    async def generate(self, prompt: str, system: Optional[str] = None) -> str:
        """Simple text generation."""
        return await self.chat(
            messages=[{"role": "user", "content": prompt}],
            system=system
        )


class RLCMasterAgent:
    """
    Master orchestrator for RLC business operations.

    Uses local Ollama LLM for:
    - Task routing and planning
    - Data interpretation
    - Report generation assistance
    - Voice command processing (from Whisper transcripts)
    """

    def __init__(self, ollama_url: str = DEFAULT_OLLAMA_URL, model: str = DEFAULT_MODEL):
        self.llm = OllamaClient(ollama_url, model)
        self.data_dir = Path(__file__).parent / "data"
        self.transcripts_dir = Path("C:/RLC/whisper/transcripts")
        self.logs_dir = Path("C:/RLC/logs")

        # System prompt for the master agent
        self.system_prompt = """You are the RLC Master Agent, an AI assistant for Round Lakes Companies.

Your responsibilities:
1. Coordinate data collection from commodity market sources (USDA, trade data, etc.)
2. Monitor market conditions and alert on significant changes
3. Generate reports and presentations
4. Process voice commands from transcribed audio
5. Manage scheduling of automated tasks

You have access to these agent teams:
- Data Collection Team: Collects from USDA, FGIS, South American trade sources
- Database Team: Stores and retrieves historical data
- Analysis Team: Runs market analysis and identifies trends
- Reporting Team: Generates weekly reports and presentations

When given a task, break it into steps and coordinate with the appropriate teams.
Always be concise and action-oriented."""

    async def initialize(self) -> bool:
        """Initialize the master agent and verify connections."""
        print("\n🔄 Initializing RLC Master Agent...")

        # Check Ollama connection
        if not await self.llm.is_available():
            print("❌ Cannot connect to Ollama. Is the OllamaLLM service running?")
            print(f"   Tried: {self.llm.base_url}")
            return False

        # Check model availability
        models = await self.llm.list_models()
        if self.llm.model not in models:
            print(f"⚠️  Model {self.llm.model} not found. Available: {models}")
            # Try fallback models
            for fallback in FALLBACK_MODELS:
                if fallback in models:
                    print(f"   Using fallback model: {fallback}")
                    self.llm.model = fallback
                    break
            else:
                print("❌ No suitable models available")
                return False

        print(f"✅ Connected to Ollama ({self.llm.model})")

        # Create directories
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)

        print("✅ Directories initialized")

        return True

    async def process_request(self, request: str) -> str:
        """Process a user request through the LLM."""
        response = await self.llm.chat(
            messages=[{"role": "user", "content": request}],
            system=self.system_prompt
        )
        return response

    async def check_transcripts(self) -> list:
        """Check for new voice transcripts to process."""
        if not self.transcripts_dir.exists():
            return []

        today = datetime.now().strftime("%Y-%m-%d")
        transcript_file = self.transcripts_dir / f"transcript_{today}.jsonl"

        if not transcript_file.exists():
            return []

        transcripts = []
        with open(transcript_file, "r") as f:
            for line in f:
                transcripts.append(json.loads(line))

        return transcripts

    async def run_interactive(self):
        """Run the interactive command loop."""
        print("\n" + "=" * 60)
        print("🤖 RLC Master Agent - Interactive Mode")
        print("=" * 60)
        print("\nCommands:")
        print("  Type any request in natural language")
        print("  'status'  - Check system status")
        print("  'collect' - Run data collection")
        print("  'report'  - Generate weekly report")
        print("  'voice'   - Process today's voice transcripts")
        print("  'quit'    - Exit")
        print()

        while True:
            try:
                user_input = input("You: ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ("quit", "exit", "q"):
                    print("\nGoodbye!")
                    break

                if user_input.lower() == "status":
                    await self._show_status()
                    continue

                if user_input.lower() == "voice":
                    await self._process_voice_transcripts()
                    continue

                # Process through LLM
                print("\n🔄 Processing...")
                response = await self.process_request(user_input)
                print(f"\n🤖 Agent: {response}\n")

            except KeyboardInterrupt:
                print("\n\nInterrupted. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Error: {e}\n")

    async def _show_status(self):
        """Show current system status."""
        print("\n--- System Status ---")
        print(f"LLM: {self.llm.model} @ {self.llm.base_url}")
        print(f"LLM Available: {await self.llm.is_available()}")
        print(f"Data Directory: {self.data_dir}")
        print(f"Transcripts: {self.transcripts_dir}")

        # Check for today's transcripts
        transcripts = await self.check_transcripts()
        print(f"Today's Voice Transcripts: {len(transcripts)}")
        print()

    async def _process_voice_transcripts(self):
        """Process voice transcripts through the LLM for action items."""
        transcripts = await self.check_transcripts()

        if not transcripts:
            print("No transcripts found for today.")
            return

        print(f"\nProcessing {len(transcripts)} transcripts...")

        # Combine recent transcripts
        combined_text = "\n".join([t["text"] for t in transcripts[-10:]])

        prompt = f"""Review these voice transcripts and identify any action items,
tasks, or important notes for Round Lakes Companies:

{combined_text}

List any actionable items with priority (high/medium/low)."""

        response = await self.llm.chat(
            messages=[{"role": "user", "content": prompt}],
            system=self.system_prompt
        )

        print(f"\n📋 Action Items:\n{response}\n")

    async def shutdown(self):
        """Clean shutdown."""
        await self.llm.close()


async def main():
    parser = argparse.ArgumentParser(description="RLC Master Agent")
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
        "--scheduler-only",
        action="store_true",
        help="Run scheduler without interactive mode"
    )
    parser.add_argument(
        "--task",
        help="Run a specific task and exit"
    )

    args = parser.parse_args()

    agent = RLCMasterAgent(args.ollama_url, args.model)

    if not await agent.initialize():
        print("\n❌ Failed to initialize. Exiting.")
        sys.exit(1)

    try:
        if args.task:
            # Run specific task
            print(f"\n🔄 Running task: {args.task}")
            response = await agent.process_request(f"Execute task: {args.task}")
            print(f"\n{response}")
        elif args.scheduler_only:
            print("\n🔄 Scheduler mode (not yet implemented)")
            print("   This will run automated data pulls based on schedule")
        else:
            # Interactive mode
            await agent.run_interactive()
    finally:
        await agent.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
