"""
Memory Manager for RLC Master Agent
Manages long-term memory and learned preferences using Notion
Round Lakes Commodities
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

logger = logging.getLogger('rlc_master_agent.memory')


class MemoryType(Enum):
    """Types of memories the agent can store"""
    PREFERENCE = "preference"           # User preferences
    FACT = "fact"                       # Learned facts
    DECISION = "decision"               # Past decisions and outcomes
    OBSERVATION = "observation"         # Patterns observed
    PROCESS = "process"                 # Business process information
    CONTACT = "contact"                 # Contact information/preferences
    FEEDBACK = "feedback"               # User feedback on agent actions


class MemoryCategory(Enum):
    """Categories for organizing memories"""
    TRADING = "trading"
    RANCH = "ranch"
    ANALYTICS = "analytics"
    COMMUNICATIONS = "communications"
    SCHEDULING = "scheduling"
    GENERAL = "general"


class MemoryManager:
    """
    Manages the agent's long-term memory using Notion as the backend.

    Features:
    - Store and retrieve facts, preferences, and patterns
    - Track interaction history
    - Manage business process documentation
    - Learn from user feedback
    """

    def __init__(self, notion_client: Optional[Any] = None, settings: Optional[Any] = None):
        """
        Initialize Memory Manager

        Args:
            notion_client: Initialized Notion client (optional)
            settings: Settings object with Notion configuration
        """
        self.client = notion_client
        self.settings = settings
        self._local_cache: Dict[str, Any] = {}
        self._use_local_fallback = notion_client is None

        # Database IDs from settings
        if settings and hasattr(settings, 'notion'):
            self.memory_db_id = settings.notion.memory_db_id
            self.interactions_db_id = settings.notion.interactions_db_id
            self.wiki_db_id = settings.notion.wiki_db_id
            self.tasks_db_id = settings.notion.tasks_db_id
        else:
            self.memory_db_id = None
            self.interactions_db_id = None
            self.wiki_db_id = None
            self.tasks_db_id = None

        logger.info(f"Memory Manager initialized (local_fallback={self._use_local_fallback})")

    def _ensure_notion_client(self):
        """Ensure Notion client is available, or fall back to local storage"""
        if self.client is None:
            try:
                from notion_client import Client
                from config.settings import get_settings
                settings = get_settings()
                if settings.notion.api_key:
                    self.client = Client(auth=settings.notion.api_key)
                    self._use_local_fallback = False
                    logger.info("Notion client initialized on demand")
            except Exception as e:
                logger.warning(f"Could not initialize Notion client: {e}")
                self._use_local_fallback = True

    # -------------------------------------------------------------------------
    # Memory Operations
    # -------------------------------------------------------------------------

    def store_memory(
        self,
        content: str,
        memory_type: MemoryType,
        category: MemoryCategory = MemoryCategory.GENERAL,
        source: Optional[str] = None,
        confidence: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Store a new memory

        Args:
            content: The memory content
            memory_type: Type of memory (preference, fact, etc.)
            category: Category for organization
            source: Where this memory came from
            confidence: Confidence score (0-1)
            metadata: Additional metadata

        Returns:
            Created memory record
        """
        memory_record = {
            'content': content,
            'type': memory_type.value,
            'category': category.value,
            'source': source or 'agent_observation',
            'confidence': confidence,
            'created_at': datetime.now().isoformat(),
            'metadata': metadata or {}
        }

        if self._use_local_fallback or not self.memory_db_id:
            return self._store_local_memory(memory_record)

        try:
            self._ensure_notion_client()
            properties = {
                'Name': {'title': [{'text': {'content': content[:100]}}]},
                'Type': {'select': {'name': memory_type.value}},
                'Category': {'select': {'name': category.value}},
                'Content': {'rich_text': [{'text': {'content': content}}]},
                'Source': {'rich_text': [{'text': {'content': source or 'agent'}}]},
                'Confidence': {'number': confidence},
                'Created': {'date': {'start': datetime.now().isoformat()}}
            }

            response = self.client.pages.create(
                parent={'database_id': self.memory_db_id},
                properties=properties
            )

            memory_record['notion_id'] = response['id']
            logger.info(f"Stored memory in Notion: {content[:50]}...")
            return memory_record

        except Exception as e:
            logger.error(f"Failed to store memory in Notion: {e}")
            return self._store_local_memory(memory_record)

    def _store_local_memory(self, memory_record: Dict[str, Any]) -> Dict[str, Any]:
        """Store memory in local cache"""
        memory_id = f"mem_{datetime.now().timestamp()}"
        memory_record['id'] = memory_id
        self._local_cache[memory_id] = memory_record
        logger.debug(f"Stored memory locally: {memory_id}")
        return memory_record

    def recall_memories(
        self,
        query: Optional[str] = None,
        memory_type: Optional[MemoryType] = None,
        category: Optional[MemoryCategory] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Recall memories matching criteria

        Args:
            query: Search query
            memory_type: Filter by type
            category: Filter by category
            limit: Maximum number of results

        Returns:
            List of matching memories
        """
        if self._use_local_fallback or not self.memory_db_id:
            return self._recall_local_memories(query, memory_type, category, limit)

        try:
            self._ensure_notion_client()

            # Build filter
            filters = []
            if memory_type:
                filters.append({
                    'property': 'Type',
                    'select': {'equals': memory_type.value}
                })
            if category:
                filters.append({
                    'property': 'Category',
                    'select': {'equals': category.value}
                })

            filter_obj = None
            if filters:
                filter_obj = {'and': filters} if len(filters) > 1 else filters[0]

            response = self.client.databases.query(
                database_id=self.memory_db_id,
                filter=filter_obj,
                page_size=limit,
                sorts=[{'property': 'Created', 'direction': 'descending'}]
            )

            memories = []
            for page in response.get('results', []):
                props = page.get('properties', {})
                memory = {
                    'id': page['id'],
                    'content': self._get_rich_text(props.get('Content', {})),
                    'type': self._get_select(props.get('Type', {})),
                    'category': self._get_select(props.get('Category', {})),
                    'source': self._get_rich_text(props.get('Source', {})),
                    'confidence': props.get('Confidence', {}).get('number', 1.0),
                    'created_at': self._get_date(props.get('Created', {}))
                }

                # Filter by query if provided
                if query:
                    if query.lower() in memory['content'].lower():
                        memories.append(memory)
                else:
                    memories.append(memory)

            return memories[:limit]

        except Exception as e:
            logger.error(f"Failed to recall memories from Notion: {e}")
            return self._recall_local_memories(query, memory_type, category, limit)

    def _recall_local_memories(
        self,
        query: Optional[str],
        memory_type: Optional[MemoryType],
        category: Optional[MemoryCategory],
        limit: int
    ) -> List[Dict[str, Any]]:
        """Recall memories from local cache"""
        results = []
        for memory in self._local_cache.values():
            # Apply filters
            if memory_type and memory.get('type') != memory_type.value:
                continue
            if category and memory.get('category') != category.value:
                continue
            if query and query.lower() not in memory.get('content', '').lower():
                continue
            results.append(memory)

        # Sort by created_at descending
        results.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        return results[:limit]

    def get_preference(self, key: str, default: Any = None) -> Any:
        """
        Get a specific user preference

        Args:
            key: Preference key
            default: Default value if not found

        Returns:
            Preference value
        """
        memories = self.recall_memories(
            query=key,
            memory_type=MemoryType.PREFERENCE,
            limit=1
        )

        if memories:
            # Try to parse as JSON for complex preferences
            content = memories[0].get('content', '')
            try:
                if ':' in content:
                    return content.split(':', 1)[1].strip()
                return content
            except:
                return content

        return default

    def set_preference(self, key: str, value: Any, source: str = "user") -> Dict[str, Any]:
        """
        Set a user preference

        Args:
            key: Preference key
            value: Preference value
            source: Source of this preference

        Returns:
            Created memory record
        """
        content = f"{key}: {value}"
        return self.store_memory(
            content=content,
            memory_type=MemoryType.PREFERENCE,
            category=MemoryCategory.GENERAL,
            source=source,
            confidence=1.0,
            metadata={'key': key, 'value': value}
        )

    # -------------------------------------------------------------------------
    # Interaction History
    # -------------------------------------------------------------------------

    def log_interaction(
        self,
        user_input: str,
        agent_response: str,
        actions_taken: List[str] = None,
        tools_used: List[str] = None,
        approval_required: bool = False,
        success: bool = True,
        feedback: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Log an interaction to the history

        Args:
            user_input: What the user asked
            agent_response: What the agent responded/did
            actions_taken: List of actions performed
            tools_used: List of tools used
            approval_required: Whether approval was needed
            success: Whether the interaction was successful
            feedback: Any user feedback

        Returns:
            Created interaction record
        """
        interaction = {
            'timestamp': datetime.now().isoformat(),
            'user_input': user_input,
            'agent_response': agent_response[:500],  # Truncate for storage
            'actions_taken': actions_taken or [],
            'tools_used': tools_used or [],
            'approval_required': approval_required,
            'success': success,
            'feedback': feedback
        }

        if self._use_local_fallback or not self.interactions_db_id:
            return self._log_local_interaction(interaction)

        try:
            self._ensure_notion_client()
            properties = {
                'Name': {'title': [{'text': {'content': user_input[:100]}}]},
                'Date': {'date': {'start': datetime.now().isoformat()}},
                'User Input': {'rich_text': [{'text': {'content': user_input}}]},
                'Agent Response': {'rich_text': [{'text': {'content': agent_response[:2000]}}]},
                'Actions': {'multi_select': [{'name': a[:100]} for a in (actions_taken or [])[:5]]},
                'Tools Used': {'multi_select': [{'name': t[:100]} for t in (tools_used or [])[:5]]},
                'Approval Required': {'checkbox': approval_required},
                'Success': {'checkbox': success}
            }

            if feedback:
                properties['Feedback'] = {'rich_text': [{'text': {'content': feedback}}]}

            response = self.client.pages.create(
                parent={'database_id': self.interactions_db_id},
                properties=properties
            )

            interaction['notion_id'] = response['id']
            logger.debug(f"Logged interaction to Notion")
            return interaction

        except Exception as e:
            logger.error(f"Failed to log interaction to Notion: {e}")
            return self._log_local_interaction(interaction)

    def _log_local_interaction(self, interaction: Dict[str, Any]) -> Dict[str, Any]:
        """Log interaction locally"""
        interaction_id = f"int_{datetime.now().timestamp()}"
        interaction['id'] = interaction_id

        # Keep in memory (could also append to a file)
        if 'interactions' not in self._local_cache:
            self._local_cache['interactions'] = []
        self._local_cache['interactions'].append(interaction)

        # Keep only last 100 interactions in memory
        self._local_cache['interactions'] = self._local_cache['interactions'][-100:]

        return interaction

    def get_recent_interactions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent interaction history"""
        if self._use_local_fallback or not self.interactions_db_id:
            interactions = self._local_cache.get('interactions', [])
            return list(reversed(interactions[-limit:]))

        try:
            self._ensure_notion_client()
            response = self.client.databases.query(
                database_id=self.interactions_db_id,
                page_size=limit,
                sorts=[{'property': 'Date', 'direction': 'descending'}]
            )

            interactions = []
            for page in response.get('results', []):
                props = page.get('properties', {})
                interactions.append({
                    'id': page['id'],
                    'timestamp': self._get_date(props.get('Date', {})),
                    'user_input': self._get_rich_text(props.get('User Input', {})),
                    'agent_response': self._get_rich_text(props.get('Agent Response', {})),
                    'success': props.get('Success', {}).get('checkbox', False)
                })

            return interactions

        except Exception as e:
            logger.error(f"Failed to get interactions: {e}")
            interactions = self._local_cache.get('interactions', [])
            return list(reversed(interactions[-limit:]))

    # -------------------------------------------------------------------------
    # Process Wiki
    # -------------------------------------------------------------------------

    def get_process(self, process_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a business process from the wiki

        Args:
            process_name: Name of the process

        Returns:
            Process details or None
        """
        if self._use_local_fallback or not self.wiki_db_id:
            return self._local_cache.get(f'process_{process_name}')

        try:
            self._ensure_notion_client()
            response = self.client.databases.query(
                database_id=self.wiki_db_id,
                filter={
                    'property': 'Name',
                    'title': {'contains': process_name}
                },
                page_size=1
            )

            if response.get('results'):
                page = response['results'][0]
                props = page.get('properties', {})

                # Get page content (steps)
                blocks = self.client.blocks.children.list(page['id'])
                steps = []
                for block in blocks.get('results', []):
                    if block['type'] == 'bulleted_list_item':
                        text = block['bulleted_list_item']['rich_text']
                        if text:
                            steps.append(text[0]['plain_text'])

                return {
                    'id': page['id'],
                    'name': self._get_title(props.get('Name', {})),
                    'category': self._get_select(props.get('Category', {})),
                    'frequency': self._get_select(props.get('Frequency', {})),
                    'automation_status': self._get_select(props.get('Automation Status', {})),
                    'steps': steps,
                    'last_run': self._get_date(props.get('Last Run', {}))
                }

            return None

        except Exception as e:
            logger.error(f"Failed to get process: {e}")
            return None

    def list_processes(self) -> List[Dict[str, Any]]:
        """List all documented processes"""
        if self._use_local_fallback or not self.wiki_db_id:
            return [v for k, v in self._local_cache.items() if k.startswith('process_')]

        try:
            self._ensure_notion_client()
            response = self.client.databases.query(
                database_id=self.wiki_db_id,
                page_size=50
            )

            processes = []
            for page in response.get('results', []):
                props = page.get('properties', {})
                processes.append({
                    'id': page['id'],
                    'name': self._get_title(props.get('Name', {})),
                    'category': self._get_select(props.get('Category', {})),
                    'automation_status': self._get_select(props.get('Automation Status', {}))
                })

            return processes

        except Exception as e:
            logger.error(f"Failed to list processes: {e}")
            return []

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _get_rich_text(self, prop: Dict) -> str:
        """Extract text from Notion rich_text property"""
        rich_text = prop.get('rich_text', [])
        if rich_text:
            return rich_text[0].get('plain_text', '')
        return ''

    def _get_title(self, prop: Dict) -> str:
        """Extract text from Notion title property"""
        title = prop.get('title', [])
        if title:
            return title[0].get('plain_text', '')
        return ''

    def _get_select(self, prop: Dict) -> str:
        """Extract value from Notion select property"""
        select = prop.get('select')
        if select:
            return select.get('name', '')
        return ''

    def _get_date(self, prop: Dict) -> str:
        """Extract date from Notion date property"""
        date = prop.get('date')
        if date:
            return date.get('start', '')
        return ''

    def health_check(self) -> Dict[str, Any]:
        """Check memory system health"""
        status = {
            'using_notion': not self._use_local_fallback,
            'local_cache_size': len(self._local_cache),
            'databases_configured': {
                'memory': bool(self.memory_db_id),
                'interactions': bool(self.interactions_db_id),
                'wiki': bool(self.wiki_db_id),
                'tasks': bool(self.tasks_db_id)
            }
        }

        if not self._use_local_fallback:
            try:
                self._ensure_notion_client()
                if self.client:
                    # Test connection
                    self.client.users.me()
                    status['notion_connected'] = True
                else:
                    status['notion_connected'] = False
            except Exception as e:
                status['notion_connected'] = False
                status['notion_error'] = str(e)

        return status

    def clear_local_cache(self):
        """Clear the local memory cache"""
        self._local_cache = {}
        logger.info("Local memory cache cleared")

    def export_local_cache(self) -> Dict[str, Any]:
        """Export local cache for backup"""
        return {
            'exported_at': datetime.now().isoformat(),
            'data': self._local_cache
        }

    def import_to_local_cache(self, data: Dict[str, Any]):
        """Import data to local cache"""
        if 'data' in data:
            self._local_cache.update(data['data'])
            logger.info(f"Imported {len(data['data'])} items to local cache")
