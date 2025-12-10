import sqlite3
from langchain_ollama import OllamaLLM
from langchain.agents import create_react_agent, AgentExecutor
from langchain.tools import Tool
from langchain.prompts import PromptTemplate
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from langchain_google_community import CalendarToolkit

# Load creds (run google_api_setup.py first if token.json missing)
creds = Credentials.from_authorized_user_file('token.json')

# LLM setup
llm = OllamaLLM(model="gemma3:4b", temperature=0.3)

# Calendar toolkit tools
calendar_toolkit = CalendarToolkit(credentials=creds)
calendar_tools = calendar_toolkit.get_tools()  # E.g., create_event, list_events

# Gmail tool
def fetch_emails(query=""):
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(userId='me', q=query, maxResults=5).execute()
    messages = results.get('messages', [])
    summaries = []
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='minimal').execute()
        subject = next((h['value'] for h in msg['payload']['headers'] if h['name'] == 'Subject'), 'No Subject')
        summaries.append(subject)
    return f"Recent emails: {', '.join(summaries)}" if summaries else "No emails found."

# Example SQL tool (from your original)
def execute_sql(sql_code):
    conn = sqlite3.connect('commodity.db')
    c = conn.cursor()
    try:
        c.executescript(sql_code)
        conn.commit()
        return "SQL executed successfully."
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        conn.close()

# All tools
tools = [
    Tool(name="FetchEmails", func=fetch_emails, description="Get recent emails. Input: optional search query."),
    Tool(name="ExecuteSQL", func=execute_sql, description="Run SQL on database."),
] + calendar_tools  # Add Calendar tools

# Prompt for agent
prompt = PromptTemplate.from_template(
    """You are a personal assistant monitoring communications across accounts and managing calendar/to-dos.
    Use tools to fetch emails, analyze for events/to-dos, add to calendar/DB.

    Tools: {tools}

    Use this format:
    Question: {input}
    Thought: [reasoning]
    Action: the action to take, should be one of [{tool_names}]
    Action Input: [input for the tool]
    Observation: [result from tool]
    ... (repeat Thought/Action/Action Input/Observation as needed)
    Thought: I now know the final answer
    Final Answer: [summary or output]

    Question: {input}
    {agent_scratchpad}"""
)


agent = create_react_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True, max_iterations=5)

# Test
response = agent_executor.invoke({"input": "Check recent emails for meetings and add to calendar if any."})
print(response['output'])