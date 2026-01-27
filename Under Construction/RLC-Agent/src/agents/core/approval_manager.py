"""
Approval Manager for RLC Master Agent
Manages autonomy levels and human-in-the-loop approvals
Round Lakes Commodities
"""

import json
import logging
from typing import Dict, Any, Optional, List, Tuple, Callable
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger('rlc_master_agent.approval')


class AutonomyLevel(Enum):
    """Autonomy levels for the agent"""
    SUPERVISED = 1      # Everything needs approval
    PARTIAL = 2         # Routine tasks auto-execute, important ones need approval
    AUTONOMOUS = 3      # Full autonomy, only escalate exceptions


class ActionType(Enum):
    """Types of actions the agent can take"""
    EMAIL_READ = "email_read"
    EMAIL_SEND = "email_send"
    EMAIL_ARCHIVE = "email_archive"
    CALENDAR_READ = "calendar_read"
    CALENDAR_CREATE = "calendar_create"
    CALENDAR_MODIFY = "calendar_modify"
    CALENDAR_DELETE = "calendar_delete"
    DATA_FETCH = "data_fetch"
    DATA_ANALYZE = "data_analyze"
    NOTION_READ = "notion_read"
    NOTION_WRITE = "notion_write"
    REPORT_GENERATE = "report_generate"
    FILE_WRITE = "file_write"
    EXTERNAL_API = "external_api"


@dataclass
class ApprovalRequest:
    """Represents a request for user approval"""
    id: str
    action_type: ActionType
    description: str
    details: Dict[str, Any]
    confidence: float
    created_at: datetime = field(default_factory=datetime.now)
    status: str = "pending"  # pending, approved, rejected, timeout
    user_response: Optional[str] = None
    resolved_at: Optional[datetime] = None


@dataclass
class ApprovalStats:
    """Statistics about approval decisions"""
    total_requests: int = 0
    approved: int = 0
    rejected: int = 0
    auto_approved: int = 0
    approval_rate: float = 0.0
    average_confidence: float = 0.0


class ApprovalManager:
    """
    Manages the gradual transition from supervised to autonomous operation.

    Features:
    - Three autonomy levels with configurable thresholds
    - Per-action-type approval rules
    - Learning from approval patterns
    - Statistics tracking for trust building
    """

    # Default approval requirements by action type
    DEFAULT_RULES = {
        # Actions that typically don't need approval
        ActionType.EMAIL_READ: {'min_level': AutonomyLevel.SUPERVISED, 'auto_approve': True},
        ActionType.CALENDAR_READ: {'min_level': AutonomyLevel.SUPERVISED, 'auto_approve': True},
        ActionType.DATA_FETCH: {'min_level': AutonomyLevel.SUPERVISED, 'auto_approve': True},
        ActionType.DATA_ANALYZE: {'min_level': AutonomyLevel.SUPERVISED, 'auto_approve': True},
        ActionType.NOTION_READ: {'min_level': AutonomyLevel.SUPERVISED, 'auto_approve': True},

        # Actions that need approval at lower autonomy levels
        ActionType.EMAIL_ARCHIVE: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.9},
        ActionType.CALENDAR_CREATE: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.8},
        ActionType.NOTION_WRITE: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.85},
        ActionType.REPORT_GENERATE: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.9},

        # Actions that typically need approval
        ActionType.EMAIL_SEND: {'min_level': AutonomyLevel.AUTONOMOUS, 'confidence_threshold': 0.95},
        ActionType.CALENDAR_MODIFY: {'min_level': AutonomyLevel.AUTONOMOUS, 'confidence_threshold': 0.9},
        ActionType.CALENDAR_DELETE: {'min_level': AutonomyLevel.AUTONOMOUS, 'confidence_threshold': 0.95},
        ActionType.FILE_WRITE: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.9},
        ActionType.EXTERNAL_API: {'min_level': AutonomyLevel.PARTIAL, 'confidence_threshold': 0.85},
    }

    def __init__(
        self,
        autonomy_level: AutonomyLevel = AutonomyLevel.SUPERVISED,
        approval_callback: Optional[Callable[[ApprovalRequest], bool]] = None,
        stats_file: Optional[Path] = None
    ):
        """
        Initialize the Approval Manager

        Args:
            autonomy_level: Initial autonomy level
            approval_callback: Function to call for user approval
            stats_file: Path to persist statistics
        """
        self.autonomy_level = autonomy_level
        self.approval_callback = approval_callback or self._default_approval_prompt
        self.stats_file = stats_file

        # Custom rules (can override defaults)
        self.custom_rules: Dict[ActionType, Dict[str, Any]] = {}

        # Pending requests
        self.pending_requests: Dict[str, ApprovalRequest] = {}

        # History for learning
        self.approval_history: List[ApprovalRequest] = []

        # Statistics by action type
        self.stats: Dict[ActionType, ApprovalStats] = {
            action_type: ApprovalStats() for action_type in ActionType
        }

        # Load persisted stats if available
        if stats_file and stats_file.exists():
            self._load_stats()

        logger.info(f"Approval Manager initialized at level {autonomy_level.name}")

    def should_auto_execute(
        self,
        action_type: ActionType,
        confidence: float = 1.0,
        context: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, str]:
        """
        Determine if an action should execute automatically

        Args:
            action_type: Type of action
            confidence: Confidence score (0-1)
            context: Additional context about the action

        Returns:
            Tuple of (can_auto_execute, reason)
        """
        # Get rules for this action type
        rules = self.custom_rules.get(action_type, self.DEFAULT_RULES.get(action_type, {}))

        # Check if always auto-approve
        if rules.get('auto_approve', False):
            return True, "Action type is always auto-approved"

        # Check autonomy level requirement
        min_level = rules.get('min_level', AutonomyLevel.AUTONOMOUS)
        if self.autonomy_level.value < min_level.value:
            return False, f"Current autonomy level ({self.autonomy_level.name}) below required ({min_level.name})"

        # Check confidence threshold
        confidence_threshold = rules.get('confidence_threshold', 0.9)
        if confidence < confidence_threshold:
            return False, f"Confidence ({confidence:.0%}) below threshold ({confidence_threshold:.0%})"

        # Check past success rate for this action type
        stats = self.stats[action_type]
        if stats.total_requests > 10 and stats.approval_rate < 0.8:
            return False, f"Historical approval rate too low ({stats.approval_rate:.0%})"

        return True, "Meets all criteria for auto-execution"

    def request_approval(
        self,
        action_type: ActionType,
        description: str,
        details: Optional[Dict[str, Any]] = None,
        confidence: float = 1.0
    ) -> Tuple[bool, str]:
        """
        Request approval for an action

        Args:
            action_type: Type of action
            description: Human-readable description
            details: Additional details
            confidence: Confidence score

        Returns:
            Tuple of (approved, response_message)
        """
        # First check if auto-approval is possible
        can_auto, reason = self.should_auto_execute(action_type, confidence, details)

        if can_auto:
            self._record_approval(action_type, confidence, True, auto=True)
            return True, f"Auto-approved: {reason}"

        # Create approval request
        request_id = f"req_{datetime.now().timestamp()}"
        request = ApprovalRequest(
            id=request_id,
            action_type=action_type,
            description=description,
            details=details or {},
            confidence=confidence
        )

        self.pending_requests[request_id] = request

        # Request user approval
        try:
            approved = self.approval_callback(request)

            request.status = "approved" if approved else "rejected"
            request.resolved_at = datetime.now()

            self._record_approval(action_type, confidence, approved, auto=False)

            if approved:
                return True, "User approved the action"
            else:
                return False, "User rejected the action"

        except Exception as e:
            logger.error(f"Error during approval request: {e}")
            request.status = "error"
            return False, f"Approval error: {str(e)}"

        finally:
            # Move to history
            self.approval_history.append(request)
            if request_id in self.pending_requests:
                del self.pending_requests[request_id]

    def _default_approval_prompt(self, request: ApprovalRequest) -> bool:
        """
        Default approval prompt (console-based)

        Args:
            request: The approval request

        Returns:
            True if approved, False otherwise
        """
        print("\n" + "=" * 60)
        print("APPROVAL REQUIRED")
        print("=" * 60)
        print(f"Action: {request.action_type.value}")
        print(f"Description: {request.description}")
        print(f"Confidence: {request.confidence:.0%}")

        if request.details:
            print("\nDetails:")
            for key, value in request.details.items():
                print(f"  {key}: {value}")

        print("=" * 60)

        while True:
            response = input("Approve this action? [y/n/d for details]: ").strip().lower()
            if response == 'y':
                return True
            elif response == 'n':
                return False
            elif response == 'd':
                print(f"\nFull details: {json.dumps(request.details, indent=2, default=str)}")
            else:
                print("Please enter 'y' for yes, 'n' for no, or 'd' for more details")

    def _record_approval(
        self,
        action_type: ActionType,
        confidence: float,
        approved: bool,
        auto: bool = False
    ):
        """Record an approval decision for statistics"""
        stats = self.stats[action_type]
        stats.total_requests += 1

        if approved:
            stats.approved += 1
            if auto:
                stats.auto_approved += 1
        else:
            stats.rejected += 1

        # Update averages
        stats.approval_rate = stats.approved / stats.total_requests
        stats.average_confidence = (
            (stats.average_confidence * (stats.total_requests - 1) + confidence)
            / stats.total_requests
        )

        # Persist if configured
        if self.stats_file:
            self._save_stats()

    def set_autonomy_level(self, level: AutonomyLevel):
        """
        Set the autonomy level

        Args:
            level: New autonomy level
        """
        old_level = self.autonomy_level
        self.autonomy_level = level
        logger.info(f"Autonomy level changed: {old_level.name} -> {level.name}")

    def graduate_autonomy(self, threshold_requests: int = 100, threshold_rate: float = 0.95):
        """
        Automatically graduate to higher autonomy based on performance

        Args:
            threshold_requests: Minimum requests before graduation
            threshold_rate: Required approval rate
        """
        if self.autonomy_level == AutonomyLevel.AUTONOMOUS:
            return  # Already at max

        # Calculate overall stats
        total_requests = sum(s.total_requests for s in self.stats.values())
        total_approved = sum(s.approved for s in self.stats.values())

        if total_requests < threshold_requests:
            logger.debug(f"Not enough requests ({total_requests}) for autonomy graduation")
            return

        overall_rate = total_approved / total_requests if total_requests > 0 else 0

        if overall_rate >= threshold_rate:
            new_level = AutonomyLevel(self.autonomy_level.value + 1)
            self.set_autonomy_level(new_level)
            logger.info(
                f"Graduated to {new_level.name} autonomy "
                f"(rate: {overall_rate:.0%}, requests: {total_requests})"
            )

    def set_custom_rule(
        self,
        action_type: ActionType,
        auto_approve: Optional[bool] = None,
        min_level: Optional[AutonomyLevel] = None,
        confidence_threshold: Optional[float] = None
    ):
        """
        Set a custom rule for an action type

        Args:
            action_type: Action type to customize
            auto_approve: Whether to always auto-approve
            min_level: Minimum autonomy level for auto-execution
            confidence_threshold: Required confidence for auto-execution
        """
        if action_type not in self.custom_rules:
            self.custom_rules[action_type] = {}

        if auto_approve is not None:
            self.custom_rules[action_type]['auto_approve'] = auto_approve
        if min_level is not None:
            self.custom_rules[action_type]['min_level'] = min_level
        if confidence_threshold is not None:
            self.custom_rules[action_type]['confidence_threshold'] = confidence_threshold

        logger.info(f"Updated rules for {action_type.value}: {self.custom_rules[action_type]}")

    def get_approval_stats(self, action_type: Optional[ActionType] = None) -> Dict[str, Any]:
        """
        Get approval statistics

        Args:
            action_type: Specific action type or None for all

        Returns:
            Statistics dictionary
        """
        if action_type:
            stats = self.stats[action_type]
            return {
                'action_type': action_type.value,
                'total_requests': stats.total_requests,
                'approved': stats.approved,
                'rejected': stats.rejected,
                'auto_approved': stats.auto_approved,
                'approval_rate': round(stats.approval_rate * 100, 1),
                'average_confidence': round(stats.average_confidence * 100, 1)
            }

        # Aggregate all stats
        total = sum(s.total_requests for s in self.stats.values())
        approved = sum(s.approved for s in self.stats.values())
        rejected = sum(s.rejected for s in self.stats.values())
        auto_approved = sum(s.auto_approved for s in self.stats.values())

        return {
            'total_requests': total,
            'approved': approved,
            'rejected': rejected,
            'auto_approved': auto_approved,
            'approval_rate': round(approved / total * 100, 1) if total > 0 else 0,
            'autonomy_level': self.autonomy_level.name,
            'by_action_type': {
                at.value: {
                    'requests': self.stats[at].total_requests,
                    'approval_rate': round(self.stats[at].approval_rate * 100, 1)
                }
                for at in ActionType if self.stats[at].total_requests > 0
            }
        }

    def get_pending_requests(self) -> List[ApprovalRequest]:
        """Get all pending approval requests"""
        return list(self.pending_requests.values())

    def resolve_pending(self, request_id: str, approved: bool, response: Optional[str] = None):
        """
        Resolve a pending approval request

        Args:
            request_id: Request ID
            approved: Whether approved
            response: Optional user response message
        """
        if request_id not in self.pending_requests:
            raise ValueError(f"No pending request with ID: {request_id}")

        request = self.pending_requests[request_id]
        request.status = "approved" if approved else "rejected"
        request.user_response = response
        request.resolved_at = datetime.now()

        self._record_approval(request.action_type, request.confidence, approved)

        self.approval_history.append(request)
        del self.pending_requests[request_id]

    def _save_stats(self):
        """Save statistics to file"""
        data = {
            'autonomy_level': self.autonomy_level.value,
            'stats': {
                at.value: {
                    'total_requests': s.total_requests,
                    'approved': s.approved,
                    'rejected': s.rejected,
                    'auto_approved': s.auto_approved,
                    'approval_rate': s.approval_rate,
                    'average_confidence': s.average_confidence
                }
                for at, s in self.stats.items()
            },
            'custom_rules': {
                at.value: {
                    k: v.value if isinstance(v, AutonomyLevel) else v
                    for k, v in rules.items()
                }
                for at, rules in self.custom_rules.items()
            },
            'saved_at': datetime.now().isoformat()
        }

        try:
            self.stats_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.stats_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save stats: {e}")

    def _load_stats(self):
        """Load statistics from file"""
        try:
            with open(self.stats_file, 'r') as f:
                data = json.load(f)

            self.autonomy_level = AutonomyLevel(data.get('autonomy_level', 1))

            for at_value, stats_data in data.get('stats', {}).items():
                try:
                    at = ActionType(at_value)
                    self.stats[at] = ApprovalStats(
                        total_requests=stats_data.get('total_requests', 0),
                        approved=stats_data.get('approved', 0),
                        rejected=stats_data.get('rejected', 0),
                        auto_approved=stats_data.get('auto_approved', 0),
                        approval_rate=stats_data.get('approval_rate', 0.0),
                        average_confidence=stats_data.get('average_confidence', 0.0)
                    )
                except ValueError:
                    pass

            logger.info("Loaded approval stats from file")

        except Exception as e:
            logger.warning(f"Could not load stats file: {e}")

    def health_check(self) -> Dict[str, Any]:
        """Check approval manager health"""
        return {
            'autonomy_level': self.autonomy_level.name,
            'pending_requests': len(self.pending_requests),
            'history_size': len(self.approval_history),
            'custom_rules_count': len(self.custom_rules),
            'stats_persisted': bool(self.stats_file and self.stats_file.exists())
        }
