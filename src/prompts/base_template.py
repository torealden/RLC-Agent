"""
Base Prompt Template

Defines the contract for all prompt templates in the pipeline. Each template
declares its ID, version, task type, sensitivity, and variable placeholders.
Rendering validates required variables and produces a RenderedPrompt with
a deterministic prompt_hash for audit logging.

Usage:
    class WASDEAnalysis(BasePromptTemplate):
        TEMPLATE_ID = 'wasde_analysis'
        TEMPLATE_VERSION = '1.0'
        TASK_TYPE = 'analysis'
        DEFAULT_SENSITIVITY = 0
        SYSTEM_PROMPT = 'You are a USDA WASDE analyst.'
        USER_TEMPLATE = 'Analyze the {commodity} balance sheet: {data}'
        REQUIRED_VARIABLES = ['commodity', 'data']
        OPTIONAL_VARIABLES = ['prior_analysis']

    template = WASDEAnalysis()
    rendered = template.render({'commodity': 'corn', 'data': '...'})
"""

import hashlib
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RenderedPrompt:
    """Result of rendering a BasePromptTemplate with variables."""
    template_id: str
    version: str
    task_type: str
    system_prompt: str
    user_prompt: str
    sensitivity: int
    context_keys: List[str]
    prompt_hash: str          # SHA-256 of system_prompt + user_prompt


class BasePromptTemplate:
    """
    Abstract base for all prompt templates.

    Subclasses must define the class-level attributes. The render() method
    substitutes variables into USER_TEMPLATE and returns a RenderedPrompt.
    """

    TEMPLATE_ID: str = ''
    TEMPLATE_VERSION: str = '1.0'
    TASK_TYPE: str = ''           # Maps to ModelRouter task types
    DEFAULT_SENSITIVITY: int = 0
    SYSTEM_PROMPT: str = ''
    USER_TEMPLATE: str = ''       # Uses {variable} placeholders
    REQUIRED_VARIABLES: List[str] = []
    OPTIONAL_VARIABLES: List[str] = []

    def render(self, variables: Dict, context_keys: Optional[List[str]] = None) -> RenderedPrompt:
        """
        Render the template with provided variables.

        Args:
            variables: Dict of variable_name -> value
            context_keys: Optional list of KG node keys used in this prompt

        Returns:
            RenderedPrompt ready for call_llm()

        Raises:
            ValueError: If required variables are missing
        """
        errors = self.validate_variables(variables)
        if errors:
            raise ValueError(
                f"Template '{self.TEMPLATE_ID}' missing required variables: {errors}"
            )

        # Build variable dict with defaults for optional vars
        render_vars = {}
        for var in self.REQUIRED_VARIABLES:
            render_vars[var] = variables[var]
        for var in self.OPTIONAL_VARIABLES:
            render_vars[var] = variables.get(var, '')

        user_prompt = self.USER_TEMPLATE.format(**render_vars)
        prompt_hash = self._hash_prompt(self.SYSTEM_PROMPT, user_prompt)

        return RenderedPrompt(
            template_id=self.TEMPLATE_ID,
            version=self.TEMPLATE_VERSION,
            task_type=self.TASK_TYPE,
            system_prompt=self.SYSTEM_PROMPT,
            user_prompt=user_prompt,
            sensitivity=self.DEFAULT_SENSITIVITY,
            context_keys=context_keys or [],
            prompt_hash=prompt_hash,
        )

    def validate_variables(self, variables: Dict) -> List[str]:
        """Return list of missing required variable names (empty if valid)."""
        return [v for v in self.REQUIRED_VARIABLES if v not in variables]

    @staticmethod
    def _hash_prompt(system_prompt: str, user_prompt: str) -> str:
        """Deterministic SHA-256 of combined prompts."""
        combined = f"{system_prompt}\n---\n{user_prompt}"
        return hashlib.sha256(combined.encode('utf-8')).hexdigest()
