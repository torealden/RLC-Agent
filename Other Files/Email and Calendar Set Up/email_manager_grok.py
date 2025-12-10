import os
import base64
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from langchain.agents import initialize_agent, Tool, AgentType
from langchain_community.llms import Ollama
from langchain_community.tools.gmail.utils import build_resource_service, get_gmail_credentials
from langchain_community.tools.gmail.search import GmailSearch
from langchain_community.tools.gmail.get_message import GmailGetMessage
from langchain_google_community import CalendarToolkit

# Scopes (readonly for Gmail; calendar for workspace)
SCOPES_WORKSPACE = ['https://www.googleapis.com/auth/calendar.events', 
                    'https://www.googleapis.com/auth/gmail.readonly']
SCOPES_PERSONAL = ['https://www.googleapis.com/auth/gmail.readonly']

def setup_credentials(credentials_file, scopes, token_file):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, scopes)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
        creds = flow.run_local_server(port=8080)
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    return creds

# Setup credentials
creds_workspace = setup_credentials('credentials.json', SCOPES_WORKSPACE, 'token_workspace.json')
creds_personal = setup_credentials('credentials_personal.json', SCOPES_PERSONAL, 'token_personal.json')

# Build API resources
api_workspace = build_resource_service(credentials=creds_workspace)
api_personal = build_resource_service(credentials=creds_personal)

# Gmail tools (read-only due to scopes; for full access, update scopes to include gmail.modify/send)
workspace_search = GmailSearch(api_resource=api_workspace)
workspace_search.name = "workspace_gmail_search"
workspace_search.description = "Search for recent emails in workspace Gmail account."

workspace_get = GmailGetMessage(api_resource=api_workspace)
workspace_get.name = "workspace_gmail_get_message"
workspace_get.description = "Get full details of an email by ID from workspace Gmail."

personal_search = GmailSearch(api_resource=api_personal)
personal_search.name = "personal_gmail_search"
personal_search.description = "Search for recent emails in personal Gmail account."

personal_get = GmailGetMessage(api_resource=api_personal)
personal_get.name = "personal_gmail_get_message"
personal_get.description = "Get full details of an email by ID from personal Gmail."

# Calendar tools (workspace only)
calendar_toolkit = CalendarToolkit(credentials=creds_workspace)
calendar_tools = calendar_toolkit.get_tools()

# All tools for agent
tools = [
    workspace_search,
    workspace_get,
    personal_search,
    personal_get,
] + calendar_tools

# Ollama LLM (use a valid model; adjust if needed)
llm = Ollama(model="gemma3:4b")  # Assuming gemma3:4b; update based on your Ollama setup

# Initialize agent
agent = initialize_agent(
    tools, 
    llm, 
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Prompt for agent to check emails, summarize, and propose actions
prompt = """
Check the last 5 emails from both workspace and personal Gmail accounts.
For each email, retrieve the ID, subject, sender, and a 1-2 sentence summary of the body.
Then, propose an action: 
- Delete the email (note: cannot execute due to readonly scopes; manual action needed)
- No action needed
- Draft a response (provide the draft text; do not send)
- Add to calendar (provide event details like summary, date, time; do not create yet)
- Other (specify)
Do not execute any actions; just propose them for approval.
If an email mentions a meeting or event, consider adding to workspace calendar.
Output in a structured format for each email.
"""

# Run the agent
result = agent.run(prompt)
print(result)

# Note: To enable execution (e.g., create drafts or events), update scopes and add tools like GmailCreateDraft/GmailSendMessage.
# For automation, integrate with a master agent that handles approvals via user input or learned preferences.