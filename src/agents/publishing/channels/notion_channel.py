"""
Notion Channel

Publishes reports as Notion pages using the Notion MCP server.
Requires the Notion MCP integration to be configured.

This module provides a programmatic interface for creating Notion pages
from report content. For interactive use, Claude can directly use the
Notion MCP tools (notion-create-pages, notion-update-page).
"""

import logging
import json
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def publish_to_notion(
    title: str,
    content_markdown: str,
    parent_page_id: str = None,
    database_id: str = None,
    properties: Dict = None,
) -> Dict:
    """
    Create a Notion page with report content.

    This function prepares the page structure. In the autonomous pipeline,
    the actual creation happens via MCP. For direct programmatic use,
    it requires the notion-client package.

    Args:
        title: Page title
        content_markdown: Report content in markdown
        parent_page_id: Notion page ID to nest under
        database_id: Notion database ID (alternative to page parent)
        properties: Additional Notion page properties

    Returns:
        Dict with page_id, url, or error
    """
    try:
        from notion_client import Client
        import os

        token = os.environ.get('NOTION_TOKEN')
        if not token:
            return {"error": "NOTION_TOKEN not set in environment"}

        notion = Client(auth=token)

        # Build parent
        if database_id:
            parent = {"database_id": database_id}
        elif parent_page_id:
            parent = {"page_id": parent_page_id}
        else:
            return {"error": "Either parent_page_id or database_id required"}

        # Convert markdown to Notion blocks (simplified)
        blocks = _markdown_to_notion_blocks(content_markdown)

        # Build properties
        page_properties = {
            "title": {"title": [{"text": {"content": title}}]}
        }
        if properties:
            page_properties.update(properties)

        # Create page
        page = notion.pages.create(
            parent=parent,
            properties=page_properties,
            children=blocks,
        )

        return {
            "success": True,
            "page_id": page["id"],
            "url": page.get("url", ""),
        }

    except ImportError:
        return {
            "error": "notion-client not installed. Use: pip install notion-client. "
                     "Or use Claude with Notion MCP for interactive publishing."
        }
    except Exception as e:
        return {"error": str(e)}


def _markdown_to_notion_blocks(markdown: str) -> list:
    """
    Convert markdown text to Notion block objects.
    Handles headings, paragraphs, and bullet lists.
    """
    blocks = []
    lines = markdown.split('\n')

    for line in lines:
        line = line.rstrip()
        if not line:
            continue

        if line.startswith('### '):
            blocks.append({
                "object": "block",
                "type": "heading_3",
                "heading_3": {"rich_text": [{"text": {"content": line[4:]}}]}
            })
        elif line.startswith('## '):
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": line[3:]}}]}
            })
        elif line.startswith('# '):
            blocks.append({
                "object": "block",
                "type": "heading_1",
                "heading_1": {"rich_text": [{"text": {"content": line[2:]}}]}
            })
        elif line.startswith('- ') or line.startswith('* '):
            blocks.append({
                "object": "block",
                "type": "bulleted_list_item",
                "bulleted_list_item": {
                    "rich_text": [{"text": {"content": line[2:]}}]
                }
            })
        elif line.startswith('|'):
            # Table rows — append as code for now (Notion API table support is complex)
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"text": {"content": line}, "annotations": {"code": True}}]
                }
            })
        else:
            # Truncate to Notion's 2000-char block limit
            content = line[:2000]
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": content}}]}
            })

    return blocks[:100]  # Notion API limit per request
