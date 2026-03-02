"""
WASDE Analysis Prompt Template v1

Generates a structured narrative analysis of US corn, soybean, and wheat
balance sheet changes from the monthly USDA WASDE report. Designed for
the autonomous pipeline: delta handler populates variables, LLM renders
a professional commodity analyst narrative.
"""

from src.prompts.base_template import BasePromptTemplate


class WASDeAnalysisV1(BasePromptTemplate):
    """Prompt template for WASDE monthly balance sheet analysis."""

    TEMPLATE_ID = 'wasde_analysis_v1'
    TEMPLATE_VERSION = '1.0'
    TASK_TYPE = 'analysis'
    DEFAULT_SENSITIVITY = 0  # WASDE is public data

    SYSTEM_PROMPT = (
        "You are a senior grain and oilseed analyst at a commodity trading firm. "
        "You write factual, concise market commentary in a professional tone. "
        "Your output uses markdown formatting. You focus on what changed, why it "
        "matters, and what the implications are for the market. Never speculate "
        "beyond what the data supports. Use exact numbers from the data provided."
    )

    USER_TEMPLATE = (
        "## USDA WASDE Report Analysis — {report_date}\n"
        "**Marketing Year:** {marketing_year}\n\n"
        "### US Balance Sheet Changes (1000 MT)\n"
        "{balance_sheet_table}\n\n"
        "### Month-over-Month Revisions\n"
        "{delta_summary}\n\n"
        "### Global Context\n"
        "{global_context}\n\n"
        "### Knowledge Graph Context\n"
        "{kg_context}\n\n"
        "### Special Considerations\n"
        "August WASDE (methodology shift to survey-based yield): {is_august_wasde}\n\n"
        "---\n"
        "Write a structured analysis with these sections:\n"
        "1. **Headline Changes** — The 2-3 most significant revisions\n"
        "2. **US Corn** — Balance sheet changes, stocks-to-use, implications\n"
        "3. **US Soybeans** — Balance sheet changes, crush/export dynamics\n"
        "4. **US Wheat** — Balance sheet changes, global competitiveness\n"
        "5. **Global Context** — Key changes in Brazil, Argentina, China\n"
        "6. **Market Implications** — Bulleted takeaways for traders\n\n"
        "Keep the analysis under 600 words. Be specific with numbers."
    )

    REQUIRED_VARIABLES = [
        'report_date',
        'marketing_year',
        'balance_sheet_table',
        'delta_summary',
        'global_context',
        'kg_context',
        'is_august_wasde',
    ]

    OPTIONAL_VARIABLES = []
