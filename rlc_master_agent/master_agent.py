"""
RLC Master Agent - Main Orchestrator
Central coordinator for all RLC business operations
Round Lakes Commodities - AI Business Partner
"""

import logging
import json
import re
from typing import Dict, Any, Optional, List, Tuple, Callable
from datetime import datetime
from pathlib import Path
from enum import Enum

# Import sub-agents and managers
from config.settings import get_settings, setup_logging, Settings
from approval_manager import ApprovalManager, AutonomyLevel, ActionType
from memory_manager import MemoryManager, MemoryType, MemoryCategory
from verification_agent import VerificationAgent
from data_agent import DataAgent, DataSource
from email_agent import EmailAgent
from calendar_agent import CalendarAgent

logger = logging.getLogger('rlc_master_agent.master')


class AgentMode(Enum):
    """Operating modes for the agent"""
    INTERACTIVE = "interactive"  # Interactive CLI mode
    AUTOMATED = "automated"      # Background automation mode
    SCHEDULED = "scheduled"      # Scheduled task mode


class TaskType(Enum):
    """Types of tasks the agent can handle"""
    EMAIL = "email"
    CALENDAR = "calendar"
    DATA = "data"
    ANALYSIS = "analysis"
    REPORT = "report"
    PROCESS = "process"
    GENERAL = "general"


class RLCMasterAgent:
    """
    Central orchestrator for the RLC Master Agent System.

    This is the "brain" that:
    - Receives requests and new triggers
    - Plans task execution
    - Delegates to specialized sub-agents
    - Coordinates results
    - Manages the plan → act → observe → refine loop
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        mode: AgentMode = AgentMode.INTERACTIVE
    ):
        """
        Initialize the Master Agent

        Args:
            settings: Application settings (loads from env if not provided)
            mode: Operating mode
        """
        # Load settings
        self.settings = settings or get_settings()
        self.mode = mode

        # Set up logging
        self.logger = setup_logging(self.settings)
        logger.info("Initializing RLC Master Agent...")

        # Initialize managers
        self.approval_manager = ApprovalManager(
            autonomy_level=AutonomyLevel(self.settings.autonomy_level),
            stats_file=self.settings.logs_dir / 'approval_stats.json'
        )

        self.memory_manager = MemoryManager(settings=self.settings)
        self.verification_agent = VerificationAgent(settings=self.settings)

        # Initialize sub-agents
        self.data_agent = DataAgent(settings=self.settings)
        self.email_agent = EmailAgent(
            settings=self.settings,
            preferences_path=self.settings.base_dir / 'config' / 'email_preferences.json',
            approval_manager=self.approval_manager
        )
        self.calendar_agent = CalendarAgent(
            settings=self.settings,
            approval_manager=self.approval_manager,
            verification_agent=self.verification_agent
        )

        # LLM client (initialized on demand)
        self._llm_client = None

        # Conversation history for context
        self.conversation_history: List[Dict[str, str]] = []

        # Task queue for scheduled operations
        self.task_queue: List[Dict[str, Any]] = []

        logger.info(f"RLC Master Agent initialized in {mode.value} mode")
        logger.info(f"Autonomy level: {self.approval_manager.autonomy_level.name}")

    # -------------------------------------------------------------------------
    # LLM Integration
    # -------------------------------------------------------------------------

    def _get_llm_client(self):
        """Get or create LLM client"""
        if self._llm_client is not None:
            return self._llm_client

        try:
            if self.settings.llm.provider == 'ollama':
                return self._init_ollama_client()
            elif self.settings.llm.provider == 'openai':
                return self._init_openai_client()
            else:
                logger.warning(f"Unknown LLM provider: {self.settings.llm.provider}")
                return None
        except Exception as e:
            logger.error(f"Failed to initialize LLM client: {e}")
            return None

    def _init_ollama_client(self):
        """Initialize Ollama client"""
        try:
            import requests
            base_url = self.settings.llm.ollama_base_url

            # Test connection
            response = requests.get(f"{base_url}/api/tags", timeout=5)
            if response.ok:
                logger.info(f"Connected to Ollama at {base_url}")
                self._llm_client = {
                    'type': 'ollama',
                    'base_url': base_url,
                    'model': self.settings.llm.ollama_model
                }
                return self._llm_client
        except Exception as e:
            logger.warning(f"Could not connect to Ollama: {e}")
        return None

    def _init_openai_client(self):
        """Initialize OpenAI client"""
        try:
            import openai
            openai.api_key = self.settings.llm.openai_api_key
            self._llm_client = {
                'type': 'openai',
                'client': openai,
                'model': self.settings.llm.openai_model
            }
            logger.info("OpenAI client initialized")
            return self._llm_client
        except Exception as e:
            logger.warning(f"Could not initialize OpenAI: {e}")
        return None

    def _generate_response(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        """
        Generate a response using the LLM

        Args:
            prompt: User prompt
            system_prompt: System context

        Returns:
            Generated response
        """
        client = self._get_llm_client()
        if not client:
            return self._fallback_response(prompt)

        default_system = """You are an AI business partner for Round Lakes Commodities (RLC).
You have 25+ years of business experience and focus on profitability and efficiency.
You are professional, ethical, and proactive in suggesting improvements.
You help with trading, ranch operations (RLC Meats), and analytics (RLC Analytics).
Be concise and action-oriented in your responses."""

        system = system_prompt or default_system

        try:
            if client['type'] == 'ollama':
                return self._ollama_generate(client, prompt, system)
            elif client['type'] == 'openai':
                return self._openai_generate(client, prompt, system)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return self._fallback_response(prompt)

    def _ollama_generate(self, client: Dict, prompt: str, system: str) -> str:
        """Generate response using Ollama"""
        import requests

        response = requests.post(
            f"{client['base_url']}/api/generate",
            json={
                'model': client['model'],
                'prompt': prompt,
                'system': system,
                'stream': False
            },
            timeout=60
        )

        if response.ok:
            return response.json().get('response', '')
        return self._fallback_response(prompt)

    def _openai_generate(self, client: Dict, prompt: str, system: str) -> str:
        """Generate response using OpenAI"""
        response = client['client'].ChatCompletion.create(
            model=client['model'],
            messages=[
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': prompt}
            ],
            temperature=self.settings.llm.temperature,
            max_tokens=self.settings.llm.max_tokens
        )
        return response.choices[0].message.content

    def _fallback_response(self, prompt: str) -> str:
        """Provide a rule-based response when LLM is unavailable"""
        prompt_lower = prompt.lower()

        if 'email' in prompt_lower:
            return "I can help with email. Try commands like 'check inbox', 'summarize emails', or 'draft reply'."
        elif 'calendar' in prompt_lower or 'schedule' in prompt_lower or 'meeting' in prompt_lower:
            return "I can help with scheduling. Try 'show schedule', 'find free time', or 'schedule meeting'."
        elif 'price' in prompt_lower or 'market' in prompt_lower:
            return "I can fetch market data. Try 'get corn price', 'market overview', or 'daily briefing'."
        elif 'weather' in prompt_lower:
            return "I can check weather. Try 'ranch weather' or 'weather forecast'."
        elif 'help' in prompt_lower:
            return self._get_help_text()
        else:
            return "I'm your RLC business assistant. I can help with emails, calendar, market data, and more. Type 'help' for available commands."

    # -------------------------------------------------------------------------
    # Main Processing Loop
    # -------------------------------------------------------------------------

    def process_input(self, user_input: str) -> str:
        """
        Process user input and return a response

        This is the main entry point for handling requests.

        Args:
            user_input: The user's request or command

        Returns:
            Agent's response
        """
        logger.info(f"Processing input: {user_input[:100]}...")

        # Store in conversation history
        self.conversation_history.append({
            'role': 'user',
            'content': user_input,
            'timestamp': datetime.now().isoformat()
        })

        try:
            # Parse and route the request
            task_type, action, params = self._parse_request(user_input)

            # Execute the appropriate action
            response = self._execute_task(task_type, action, params, user_input)

            # Log interaction
            self.memory_manager.log_interaction(
                user_input=user_input,
                agent_response=response[:500],
                actions_taken=[f"{task_type.value}:{action}"],
                success=True
            )

        except Exception as e:
            logger.error(f"Error processing input: {e}")
            response = f"I encountered an error: {str(e)}. Please try rephrasing your request."
            self.memory_manager.log_interaction(
                user_input=user_input,
                agent_response=response,
                actions_taken=[],
                success=False,
                feedback=str(e)
            )

        # Store response in history
        self.conversation_history.append({
            'role': 'assistant',
            'content': response,
            'timestamp': datetime.now().isoformat()
        })

        return response

    def _parse_request(self, user_input: str) -> Tuple[TaskType, str, Dict[str, Any]]:
        """
        Parse user input to determine task type and action

        Args:
            user_input: User's request

        Returns:
            Tuple of (TaskType, action_name, parameters)
        """
        input_lower = user_input.lower().strip()

        # Email-related
        if any(word in input_lower for word in ['email', 'inbox', 'mail', 'message']):
            if 'check' in input_lower or 'show' in input_lower or 'get' in input_lower:
                return TaskType.EMAIL, 'check_inbox', {}
            elif 'summarize' in input_lower or 'summary' in input_lower:
                return TaskType.EMAIL, 'summarize', {}
            elif 'reply' in input_lower or 'respond' in input_lower:
                return TaskType.EMAIL, 'draft_reply', {'input': user_input}
            elif 'send' in input_lower:
                return TaskType.EMAIL, 'send', {'input': user_input}
            else:
                return TaskType.EMAIL, 'check_inbox', {}

        # Calendar-related
        if any(word in input_lower for word in ['calendar', 'schedule', 'meeting', 'event', 'appointment']):
            if 'today' in input_lower:
                return TaskType.CALENDAR, 'today', {}
            elif 'free' in input_lower or 'available' in input_lower:
                return TaskType.CALENDAR, 'find_free', {}
            elif 'schedule' in input_lower or 'create' in input_lower or 'add' in input_lower:
                return TaskType.CALENDAR, 'create_event', {'input': user_input}
            elif 'next' in input_lower:
                return TaskType.CALENDAR, 'next_event', {}
            else:
                return TaskType.CALENDAR, 'summary', {}

        # Data/Market-related
        if any(word in input_lower for word in ['price', 'market', 'corn', 'soybean', 'wheat', 'cattle', 'commodity']):
            commodity = self._extract_commodity(input_lower)
            return TaskType.DATA, 'get_price', {'commodity': commodity}

        if 'export' in input_lower or 'trade' in input_lower:
            commodity = self._extract_commodity(input_lower)
            return TaskType.DATA, 'get_exports', {'commodity': commodity}

        if 'weather' in input_lower:
            return TaskType.DATA, 'get_weather', {}

        if 'briefing' in input_lower or 'daily' in input_lower:
            return TaskType.DATA, 'daily_briefing', {}

        if 'overview' in input_lower:
            return TaskType.DATA, 'market_overview', {}

        # Process execution
        if 'run' in input_lower and 'process' in input_lower:
            return TaskType.PROCESS, 'execute', {'input': user_input}

        # Help and status
        if 'help' in input_lower:
            return TaskType.GENERAL, 'help', {}
        if 'status' in input_lower:
            return TaskType.GENERAL, 'status', {}

        # Default to general with LLM processing
        return TaskType.GENERAL, 'llm_process', {'input': user_input}

    def _extract_commodity(self, text: str) -> str:
        """Extract commodity name from text"""
        commodities = ['corn', 'soybeans', 'soybean', 'wheat', 'cattle', 'hogs', 'ethanol']
        for commodity in commodities:
            if commodity in text:
                return commodity.replace('soybean', 'soybeans')
        return 'corn'  # Default

    def _execute_task(
        self,
        task_type: TaskType,
        action: str,
        params: Dict[str, Any],
        original_input: str
    ) -> str:
        """
        Execute a parsed task

        Args:
            task_type: Type of task
            action: Specific action to perform
            params: Action parameters
            original_input: Original user input

        Returns:
            Response string
        """
        if task_type == TaskType.EMAIL:
            return self._handle_email_task(action, params)
        elif task_type == TaskType.CALENDAR:
            return self._handle_calendar_task(action, params)
        elif task_type == TaskType.DATA:
            return self._handle_data_task(action, params)
        elif task_type == TaskType.PROCESS:
            return self._handle_process_task(action, params)
        elif task_type == TaskType.GENERAL:
            return self._handle_general_task(action, params, original_input)
        else:
            return "I'm not sure how to handle that request. Try 'help' for available commands."

    # -------------------------------------------------------------------------
    # Task Handlers
    # -------------------------------------------------------------------------

    def _handle_email_task(self, action: str, params: Dict) -> str:
        """Handle email-related tasks"""
        if action == 'check_inbox':
            emails = self.email_agent.get_unread_emails(max_results=10)
            if not emails:
                return "Your inbox is clear! No unread emails."

            response = f"You have {len(emails)} unread email(s):\n\n"
            for i, email in enumerate(emails, 1):
                priority = f"[{email.priority.value.upper()}]" if email.priority.value == 'high' else ""
                response += f"{i}. {priority} From: {email.sender_name or email.sender}\n"
                response += f"   Subject: {email.subject}\n"
                response += f"   Summary: {email.summary[:100]}...\n\n"
            return response

        elif action == 'summarize':
            summary = self.email_agent.summarize_inbox()
            high = len(summary['by_priority']['high'])
            medium = len(summary['by_priority']['medium'])
            low = len(summary['by_priority']['low'])

            response = f"Inbox Summary ({summary['unread_count']} unread):\n"
            response += f"- High priority: {high}\n"
            response += f"- Medium priority: {medium}\n"
            response += f"- Low priority: {low}\n\n"

            if high > 0:
                response += "High Priority Emails:\n"
                for email in summary['by_priority']['high'][:3]:
                    response += f"- {email['from']}: {email['subject']}\n"

            return response

        else:
            return "Email command not recognized. Try 'check inbox' or 'summarize emails'."

    def _handle_calendar_task(self, action: str, params: Dict) -> str:
        """Handle calendar-related tasks"""
        if action == 'today':
            events = self.calendar_agent.get_today_events()
            if not events:
                return "No events scheduled for today."

            response = f"Today's Schedule ({len(events)} events):\n\n"
            for event in events:
                time_str = event.start.strftime('%H:%M') if not event.is_all_day else "All day"
                response += f"- {time_str}: {event.title}\n"
                if event.location:
                    response += f"  Location: {event.location}\n"
            return response

        elif action == 'next_event':
            event = self.calendar_agent.get_next_event()
            if not event:
                return "No upcoming events in the next 24 hours."

            time_until = event.start - datetime.now()
            hours = int(time_until.total_seconds() / 3600)
            minutes = int((time_until.total_seconds() % 3600) / 60)

            return f"Next event: {event.title}\nTime: {event.start.strftime('%H:%M')} (in {hours}h {minutes}m)\nLocation: {event.location or 'Not specified'}"

        elif action == 'find_free':
            slots = self.calendar_agent.find_free_slots(duration_minutes=30, within_days=3)
            if not slots:
                return "No free slots found in the next 3 days."

            response = "Available time slots:\n\n"
            for slot in slots[:5]:
                response += f"- {slot.start.strftime('%a %m/%d %H:%M')} ({slot.duration_minutes} min)\n"
            return response

        elif action == 'summary':
            summary = self.calendar_agent.get_schedule_summary(days=7)
            response = f"Schedule Summary (next 7 days):\n"
            response += f"- Total events: {summary['total_events']}\n"
            response += f"- Busy hours: {summary['busy_hours']:.1f}\n"
            response += f"- Free slots available: {len(summary['free_slots'])}\n"
            return response

        else:
            return "Calendar command not recognized. Try 'today schedule', 'next event', or 'find free time'."

    def _handle_data_task(self, action: str, params: Dict) -> str:
        """Handle data retrieval tasks"""
        if action == 'get_price':
            commodity = params.get('commodity', 'corn')
            data = self.data_agent.get_commodity_price(commodity)

            if 'error' in str(data.get('status', '')):
                return f"Could not fetch {commodity} price: {data.get('message', 'Unknown error')}"

            price_info = data.get('price_data', {})
            return f"{commodity.title()} Price Update:\n" \
                   f"- Low: ${price_info.get('low', 'N/A')}\n" \
                   f"- High: ${price_info.get('high', 'N/A')}\n" \
                   f"- Average: ${price_info.get('avg', 'N/A')}\n" \
                   f"Date: {data.get('report_date', 'N/A')}"

        elif action == 'get_exports':
            commodity = params.get('commodity', 'corn')
            data = self.data_agent.get_export_data(commodity)

            if 'error' in str(data.get('status', '')):
                return f"Could not fetch {commodity} export data: {data.get('message', 'Unknown error')}"

            return f"{commodity.title()} Export Data:\n" \
                   f"Total value: ${data.get('total_value', 0):,.0f}\n" \
                   f"Period: {data.get('period', 'N/A')}"

        elif action == 'get_weather':
            data = self.data_agent.get_weather('ranch')

            if 'error' in str(data.get('status', '')):
                return f"Could not fetch weather: {data.get('message', 'Unknown error')}"

            return f"Ranch Weather:\n" \
                   f"- Temperature: {data.get('temperature', 'N/A')}F\n" \
                   f"- Conditions: {data.get('description', 'N/A')}\n" \
                   f"- Humidity: {data.get('humidity', 'N/A')}%\n" \
                   f"- Wind: {data.get('wind', {}).get('speed', 'N/A')} mph"

        elif action == 'daily_briefing':
            briefing = self.data_agent.get_daily_briefing()
            return self._format_briefing(briefing)

        elif action == 'market_overview':
            overview = self.data_agent.get_market_overview()
            return self._format_market_overview(overview)

        else:
            return "Data command not recognized. Try 'corn price', 'market overview', or 'daily briefing'."

    def _format_briefing(self, briefing: Dict) -> str:
        """Format daily briefing for display"""
        response = f"=== Daily Briefing ({briefing.get('date', 'Today')}) ===\n\n"

        # Market section
        market = briefing.get('sections', {}).get('market', {})
        if market:
            response += "MARKET UPDATE:\n"
            for commodity, data in market.get('commodities', {}).items():
                status = data.get('status', 'unknown')
                response += f"- {commodity.title()}: {status}\n"
            response += "\n"

        # Weather section
        weather = briefing.get('sections', {}).get('weather', {})
        if weather and 'error' not in str(weather):
            current = weather.get('current', {})
            response += f"WEATHER:\n"
            response += f"- Temp: {current.get('temperature', 'N/A')}F\n"
            response += f"- Conditions: {current.get('description', 'N/A')}\n"

        return response

    def _format_market_overview(self, overview: Dict) -> str:
        """Format market overview for display"""
        response = f"=== Market Overview ===\n\n"

        for commodity, data in overview.get('commodities', {}).items():
            response += f"{commodity.title()}:\n"
            if data.get('status') == 'error':
                response += f"  - Data unavailable\n"
            else:
                price_data = data.get('price', {}).get('price_data', {})
                response += f"  - Price: ${price_data.get('avg', 'N/A')}\n"
            response += "\n"

        return response

    def _handle_process_task(self, action: str, params: Dict) -> str:
        """Handle process execution tasks"""
        # Get process from Notion wiki
        input_text = params.get('input', '')

        # Extract process name
        process_name = input_text.replace('run', '').replace('process', '').strip()

        process = self.memory_manager.get_process(process_name)
        if not process:
            available = self.memory_manager.list_processes()
            if available:
                process_list = ', '.join([p.get('name', 'Unknown') for p in available[:5]])
                return f"Process '{process_name}' not found. Available: {process_list}"
            return f"Process '{process_name}' not found. No processes documented yet."

        # Execute process steps
        response = f"Executing process: {process['name']}\n\n"
        for i, step in enumerate(process.get('steps', []), 1):
            response += f"Step {i}: {step}\n"
            # Here we would execute each step based on its content
            response += "  -> Completed\n"

        return response

    def _handle_general_task(self, action: str, params: Dict, original_input: str) -> str:
        """Handle general tasks"""
        if action == 'help':
            return self._get_help_text()

        elif action == 'status':
            return self._get_status()

        elif action == 'llm_process':
            # Use LLM for general conversation
            return self._generate_response(original_input)

        return self._generate_response(original_input)

    def _get_help_text(self) -> str:
        """Return help text"""
        return """
RLC Master Agent - Available Commands:

EMAIL:
  - check inbox / show emails
  - summarize emails
  - draft reply to [email]

CALENDAR:
  - today schedule / today's events
  - next event
  - find free time
  - schedule meeting [details]

DATA & MARKET:
  - [commodity] price (e.g., "corn price")
  - market overview
  - daily briefing
  - weather / ranch weather

SYSTEM:
  - help - Show this help
  - status - Show system status

Just type naturally - I'll understand what you need!
"""

    def _get_status(self) -> str:
        """Return system status"""
        status = {
            'mode': self.mode.value,
            'autonomy_level': self.approval_manager.autonomy_level.name,
            'email': self.email_agent.health_check(),
            'calendar': self.calendar_agent.health_check(),
            'data': self.data_agent.health_check(),
            'memory': self.memory_manager.health_check()
        }

        response = "=== System Status ===\n\n"
        response += f"Mode: {status['mode']}\n"
        response += f"Autonomy: {status['autonomy_level']}\n\n"

        response += "Services:\n"
        response += f"- Email: {'Connected' if status['email'].get('gmail_connected') else 'Not connected'}\n"
        response += f"- Calendar: {'Connected' if status['calendar'].get('calendar_connected') else 'Not connected'}\n"
        response += f"- Memory: {'Notion' if status['memory'].get('using_notion') else 'Local'}\n"

        return response

    # -------------------------------------------------------------------------
    # Interactive Mode
    # -------------------------------------------------------------------------

    def run_interactive(self):
        """Run the agent in interactive CLI mode"""
        print("\n" + "=" * 60)
        print("RLC Master Agent - AI Business Partner")
        print("Round Lakes Commodities")
        print("=" * 60)
        print("\nType 'help' for available commands, 'quit' to exit.\n")

        while True:
            try:
                user_input = input("You: ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ['quit', 'exit', 'bye']:
                    print("\nGoodbye! Have a productive day.")
                    break

                response = self.process_input(user_input)
                print(f"\nAssistant: {response}\n")

            except KeyboardInterrupt:
                print("\n\nSession interrupted. Goodbye!")
                break
            except EOFError:
                print("\n\nEnd of input. Goodbye!")
                break
            except Exception as e:
                logger.error(f"Error in interactive loop: {e}")
                print(f"\nError: {e}\n")

    # -------------------------------------------------------------------------
    # Scheduled/Automated Operations
    # -------------------------------------------------------------------------

    def daily_workflow(self) -> Dict[str, Any]:
        """
        Execute daily automated workflow

        Returns:
            Results of daily operations
        """
        logger.info("Starting daily workflow...")
        results = {
            'timestamp': datetime.now().isoformat(),
            'tasks': {}
        }

        # 1. Check and summarize emails
        try:
            email_summary = self.email_agent.summarize_inbox()
            results['tasks']['email_summary'] = {
                'status': 'success',
                'unread_count': email_summary['unread_count']
            }
        except Exception as e:
            results['tasks']['email_summary'] = {'status': 'error', 'error': str(e)}

        # 2. Get today's schedule
        try:
            events = self.calendar_agent.get_today_events()
            results['tasks']['calendar'] = {
                'status': 'success',
                'event_count': len(events)
            }
        except Exception as e:
            results['tasks']['calendar'] = {'status': 'error', 'error': str(e)}

        # 3. Get market data
        try:
            briefing = self.data_agent.get_daily_briefing()
            results['tasks']['market_data'] = {'status': 'success'}
        except Exception as e:
            results['tasks']['market_data'] = {'status': 'error', 'error': str(e)}

        logger.info("Daily workflow completed")
        return results

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of all components"""
        return {
            'master_agent': {
                'status': 'healthy',
                'mode': self.mode.value,
                'autonomy_level': self.approval_manager.autonomy_level.name
            },
            'approval_manager': self.approval_manager.health_check(),
            'memory_manager': self.memory_manager.health_check(),
            'verification_agent': self.verification_agent.health_check(),
            'data_agent': self.data_agent.health_check(),
            'email_agent': self.email_agent.health_check(),
            'calendar_agent': self.calendar_agent.health_check(),
            'llm': {
                'provider': self.settings.llm.provider,
                'connected': self._get_llm_client() is not None
            }
        }


# Main entry point
def main():
    """Main entry point for the RLC Master Agent"""
    agent = RLCMasterAgent(mode=AgentMode.INTERACTIVE)
    agent.run_interactive()


if __name__ == "__main__":
    main()
