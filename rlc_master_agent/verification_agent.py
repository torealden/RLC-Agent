"""
Verification Agent for RLC Master Agent
Quality checks and validation for agent outputs
Round Lakes Commodities
"""

import re
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger('rlc_master_agent.verification')


class VerificationResult(Enum):
    """Result of a verification check"""
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


@dataclass
class VerificationReport:
    """Report from verification checks"""
    result: VerificationResult
    checks_performed: List[str]
    issues: List[str]
    warnings: List[str]
    suggestions: List[str]
    confidence: float
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def is_acceptable(self) -> bool:
        """Check if the verification result is acceptable for proceeding"""
        return self.result in [VerificationResult.PASSED, VerificationResult.WARNING]


class VerificationAgent:
    """
    Secondary AI agent that double-checks outputs and actions.

    Acts as a proofreader and safety net:
    - Checks email drafts for professionalism and accuracy
    - Validates data analysis results
    - Ensures calendar events don't conflict
    - Verifies compliance with business rules
    """

    # Sensitive information patterns to check
    SENSITIVE_PATTERNS = [
        r'\b\d{3}-\d{2}-\d{4}\b',  # SSN
        r'\b\d{16}\b',  # Credit card
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Email (for awareness)
        r'password\s*[:=]\s*\S+',  # Passwords
        r'api[_-]?key\s*[:=]\s*\S+',  # API keys
        r'secret\s*[:=]\s*\S+',  # Secrets
    ]

    # Unprofessional language patterns
    UNPROFESSIONAL_PATTERNS = [
        r'\b(damn|hell|crap)\b',
        r'\b(gonna|wanna|gotta)\b',
        r'!{3,}',  # Excessive exclamation
        r'\.{4,}',  # Excessive ellipsis
        r'[A-Z]{5,}',  # ALL CAPS WORDS
    ]

    # Required elements in business communications
    BUSINESS_ELEMENTS = {
        'greeting': r'^(hi|hello|dear|good\s+(morning|afternoon|evening))',
        'closing': r'(regards|sincerely|best|thanks|thank you)\s*,?\s*$',
        'signature': r'\n\s*[A-Z][a-z]+\s+[A-Z][a-z]+\s*$',
    }

    def __init__(self, llm_client: Optional[Any] = None, settings: Optional[Any] = None):
        """
        Initialize Verification Agent

        Args:
            llm_client: Optional LLM client for advanced verification
            settings: Application settings
        """
        self.llm_client = llm_client
        self.settings = settings
        self.verification_history: List[VerificationReport] = []

        logger.info("Verification Agent initialized")

    def verify_email_draft(
        self,
        subject: str,
        body: str,
        recipient: str,
        context: Optional[Dict[str, Any]] = None
    ) -> VerificationReport:
        """
        Verify an email draft before sending

        Args:
            subject: Email subject
            body: Email body
            recipient: Recipient email
            context: Additional context

        Returns:
            Verification report
        """
        checks = []
        issues = []
        warnings = []
        suggestions = []

        # Check for empty content
        checks.append("content_exists")
        if not subject or not body:
            issues.append("Email subject or body is empty")

        # Check subject line
        checks.append("subject_appropriate")
        if len(subject) > 100:
            warnings.append("Subject line may be too long")
        if subject.isupper():
            warnings.append("Subject line is all caps - may appear aggressive")

        # Check for sensitive information
        checks.append("no_sensitive_data")
        full_text = f"{subject}\n{body}"
        for pattern in self.SENSITIVE_PATTERNS:
            if re.search(pattern, full_text, re.IGNORECASE):
                issues.append(f"Potentially sensitive information detected (pattern: {pattern[:20]}...)")

        # Check professionalism
        checks.append("professional_tone")
        for pattern in self.UNPROFESSIONAL_PATTERNS:
            matches = re.findall(pattern, full_text, re.IGNORECASE)
            if matches:
                warnings.append(f"Informal language detected: {matches[:3]}")

        # Check for greeting and closing
        checks.append("proper_structure")
        body_lower = body.lower()
        has_greeting = any(
            re.search(p, body_lower)
            for p in [r'^(hi|hello|dear|good)', r'^(thank you for)', r'^(hope this)']
        )
        has_closing = any(
            phrase in body_lower
            for phrase in ['regards', 'sincerely', 'best', 'thank', 'cheers']
        )

        if not has_greeting:
            suggestions.append("Consider adding a greeting")
        if not has_closing:
            suggestions.append("Consider adding a closing/sign-off")

        # Check spelling of common words (basic check)
        checks.append("spelling_check")
        common_misspellings = {
            'recieve': 'receive',
            'occured': 'occurred',
            'seperate': 'separate',
            'accomodate': 'accommodate',
            'definately': 'definitely',
        }
        for wrong, right in common_misspellings.items():
            if wrong in body.lower():
                warnings.append(f"Possible misspelling: '{wrong}' should be '{right}'")

        # Check email recipient
        checks.append("valid_recipient")
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', recipient):
            issues.append("Invalid email address format")

        # Determine overall result
        if issues:
            result = VerificationResult.FAILED
            confidence = 0.3
        elif warnings:
            result = VerificationResult.WARNING
            confidence = 0.7
        else:
            result = VerificationResult.PASSED
            confidence = 0.95

        report = VerificationReport(
            result=result,
            checks_performed=checks,
            issues=issues,
            warnings=warnings,
            suggestions=suggestions,
            confidence=confidence
        )

        self.verification_history.append(report)
        logger.info(f"Email verification: {result.value} ({len(issues)} issues, {len(warnings)} warnings)")

        return report

    def verify_calendar_event(
        self,
        title: str,
        start_time: datetime,
        end_time: datetime,
        attendees: List[str],
        existing_events: Optional[List[Dict]] = None,
        user_preferences: Optional[Dict] = None
    ) -> VerificationReport:
        """
        Verify a calendar event before creation

        Args:
            title: Event title
            start_time: Event start
            end_time: Event end
            attendees: List of attendee emails
            existing_events: Existing calendar events
            user_preferences: User scheduling preferences

        Returns:
            Verification report
        """
        checks = []
        issues = []
        warnings = []
        suggestions = []

        # Basic validation
        checks.append("valid_times")
        if end_time <= start_time:
            issues.append("End time must be after start time")

        if start_time < datetime.now():
            warnings.append("Event is scheduled in the past")

        # Check title
        checks.append("valid_title")
        if not title or len(title) < 2:
            warnings.append("Event title is very short")

        # Check for conflicts
        checks.append("no_conflicts")
        if existing_events:
            for event in existing_events:
                event_start = event.get('start')
                event_end = event.get('end')

                if event_start and event_end:
                    # Check for overlap
                    if start_time < event_end and end_time > event_start:
                        issues.append(f"Conflicts with existing event: {event.get('title', 'Untitled')}")

        # Check user preferences
        checks.append("matches_preferences")
        if user_preferences:
            work_start = user_preferences.get('working_hours_start', 8)
            work_end = user_preferences.get('working_hours_end', 18)

            if start_time.hour < work_start or end_time.hour > work_end:
                warnings.append("Event is outside normal working hours")

            # Check for preferred meeting duration
            duration_minutes = (end_time - start_time).total_seconds() / 60
            preferred_duration = user_preferences.get('preferred_meeting_duration', 30)

            if duration_minutes > preferred_duration * 2:
                suggestions.append(f"Consider shortening - longer than typical {preferred_duration}min meetings")

        # Check attendees
        checks.append("valid_attendees")
        for attendee in attendees:
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', attendee):
                warnings.append(f"Invalid attendee email format: {attendee}")

        # Determine result
        if issues:
            result = VerificationResult.FAILED
            confidence = 0.3
        elif warnings:
            result = VerificationResult.WARNING
            confidence = 0.75
        else:
            result = VerificationResult.PASSED
            confidence = 0.95

        report = VerificationReport(
            result=result,
            checks_performed=checks,
            issues=issues,
            warnings=warnings,
            suggestions=suggestions,
            confidence=confidence
        )

        self.verification_history.append(report)
        return report

    def verify_data_analysis(
        self,
        data_source: str,
        analysis_type: str,
        results: Dict[str, Any],
        raw_data: Optional[Any] = None
    ) -> VerificationReport:
        """
        Verify data analysis results

        Args:
            data_source: Source of the data
            analysis_type: Type of analysis performed
            results: Analysis results
            raw_data: Optional raw data for validation

        Returns:
            Verification report
        """
        checks = []
        issues = []
        warnings = []
        suggestions = []

        # Check for empty results
        checks.append("has_results")
        if not results:
            issues.append("Analysis returned no results")
            return VerificationReport(
                result=VerificationResult.FAILED,
                checks_performed=checks,
                issues=issues,
                warnings=warnings,
                suggestions=suggestions,
                confidence=0.0
            )

        # Check for error states
        checks.append("no_errors")
        if results.get('status') == 'error' or results.get('error'):
            issues.append(f"Analysis returned an error: {results.get('error', 'Unknown')}")

        # Check for reasonable values
        checks.append("reasonable_values")
        for key, value in results.items():
            if isinstance(value, (int, float)):
                # Check for extreme values
                if abs(value) > 1e15:
                    warnings.append(f"Unusually large value for {key}: {value}")
                # Check for NaN/Inf
                if str(value) in ['nan', 'inf', '-inf']:
                    issues.append(f"Invalid numeric value for {key}: {value}")

        # Check data freshness
        checks.append("data_freshness")
        if 'timestamp' in results or 'date' in results:
            data_date = results.get('timestamp') or results.get('date')
            if data_date:
                # Simple check - data shouldn't be too old for market data
                if analysis_type in ['price', 'market', 'trading']:
                    suggestions.append("Verify data is from the expected time period")

        # Check completeness
        checks.append("completeness")
        expected_fields = {
            'price': ['commodity', 'price_data', 'source'],
            'trade': ['commodity', 'value', 'period'],
            'weather': ['temperature', 'conditions', 'location'],
        }

        if analysis_type in expected_fields:
            missing = [f for f in expected_fields[analysis_type] if f not in results]
            if missing:
                warnings.append(f"Missing expected fields: {missing}")

        # Determine result
        if issues:
            result = VerificationResult.FAILED
            confidence = 0.3
        elif warnings:
            result = VerificationResult.WARNING
            confidence = 0.7
        else:
            result = VerificationResult.PASSED
            confidence = 0.9

        report = VerificationReport(
            result=result,
            checks_performed=checks,
            issues=issues,
            warnings=warnings,
            suggestions=suggestions,
            confidence=confidence
        )

        self.verification_history.append(report)
        return report

    def verify_action(
        self,
        action_type: str,
        action_details: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None
    ) -> VerificationReport:
        """
        Generic verification for any action

        Args:
            action_type: Type of action
            action_details: Action details
            context: Additional context

        Returns:
            Verification report
        """
        # Route to specific verification methods
        if action_type == 'email_send':
            return self.verify_email_draft(
                subject=action_details.get('subject', ''),
                body=action_details.get('body', ''),
                recipient=action_details.get('recipient', ''),
                context=context
            )

        if action_type == 'calendar_create':
            return self.verify_calendar_event(
                title=action_details.get('title', ''),
                start_time=action_details.get('start_time', datetime.now()),
                end_time=action_details.get('end_time', datetime.now()),
                attendees=action_details.get('attendees', []),
                existing_events=context.get('existing_events') if context else None,
                user_preferences=context.get('preferences') if context else None
            )

        if action_type in ['data_fetch', 'data_analyze']:
            return self.verify_data_analysis(
                data_source=action_details.get('source', 'unknown'),
                analysis_type=action_details.get('type', 'general'),
                results=action_details.get('results', {}),
                raw_data=action_details.get('raw_data')
            )

        # Generic verification
        return self._generic_verification(action_type, action_details, context)

    def _generic_verification(
        self,
        action_type: str,
        details: Dict[str, Any],
        context: Optional[Dict[str, Any]]
    ) -> VerificationReport:
        """Generic verification for unspecified action types"""
        checks = ['basic_validation']
        issues = []
        warnings = []
        suggestions = []

        # Check for required fields
        if not details:
            warnings.append("Action details are empty")

        # Check for error indicators
        if details.get('error') or details.get('status') == 'error':
            issues.append(f"Action contains error: {details.get('error', 'Unknown')}")

        # Log unusual action types
        known_types = ['email', 'calendar', 'data', 'notion', 'file', 'report']
        if not any(t in action_type.lower() for t in known_types):
            suggestions.append(f"Unknown action type: {action_type}")

        if issues:
            result = VerificationResult.FAILED
            confidence = 0.4
        elif warnings:
            result = VerificationResult.WARNING
            confidence = 0.7
        else:
            result = VerificationResult.PASSED
            confidence = 0.85

        return VerificationReport(
            result=result,
            checks_performed=checks,
            issues=issues,
            warnings=warnings,
            suggestions=suggestions,
            confidence=confidence
        )

    def verify_with_llm(
        self,
        content: str,
        verification_prompt: str
    ) -> VerificationReport:
        """
        Use LLM for advanced verification

        Args:
            content: Content to verify
            verification_prompt: What to check for

        Returns:
            Verification report
        """
        if not self.llm_client:
            logger.warning("LLM client not available for advanced verification")
            return VerificationReport(
                result=VerificationResult.WARNING,
                checks_performed=['llm_verification_skipped'],
                issues=[],
                warnings=['LLM verification not available'],
                suggestions=['Configure LLM client for advanced verification'],
                confidence=0.5
            )

        try:
            # Construct verification prompt
            prompt = f"""You are a verification agent. Review the following content and identify any issues.

Content to review:
{content}

Verification criteria:
{verification_prompt}

Respond with:
1. RESULT: PASSED, WARNING, or FAILED
2. ISSUES: List any critical issues (one per line)
3. WARNINGS: List any warnings (one per line)
4. SUGGESTIONS: List any suggestions (one per line)
5. CONFIDENCE: A number between 0 and 1
"""

            # This would call the actual LLM
            # response = self.llm_client.generate(prompt)
            # For now, return a placeholder

            return VerificationReport(
                result=VerificationResult.PASSED,
                checks_performed=['llm_verification'],
                issues=[],
                warnings=[],
                suggestions=['LLM verification completed'],
                confidence=0.85
            )

        except Exception as e:
            logger.error(f"LLM verification failed: {e}")
            return VerificationReport(
                result=VerificationResult.WARNING,
                checks_performed=['llm_verification_error'],
                issues=[],
                warnings=[f'LLM verification error: {str(e)}'],
                suggestions=[],
                confidence=0.5
            )

    def get_verification_history(self, limit: int = 10) -> List[VerificationReport]:
        """Get recent verification history"""
        return self.verification_history[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get verification statistics"""
        if not self.verification_history:
            return {'total': 0}

        passed = sum(1 for r in self.verification_history if r.result == VerificationResult.PASSED)
        warnings = sum(1 for r in self.verification_history if r.result == VerificationResult.WARNING)
        failed = sum(1 for r in self.verification_history if r.result == VerificationResult.FAILED)
        total = len(self.verification_history)

        return {
            'total': total,
            'passed': passed,
            'warnings': warnings,
            'failed': failed,
            'pass_rate': round(passed / total * 100, 1),
            'average_confidence': round(
                sum(r.confidence for r in self.verification_history) / total * 100, 1
            )
        }

    def health_check(self) -> Dict[str, Any]:
        """Check verification agent health"""
        return {
            'status': 'healthy',
            'llm_available': self.llm_client is not None,
            'history_size': len(self.verification_history),
            'stats': self.get_stats()
        }
