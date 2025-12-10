from langchain.agents import initialize_agent, Tool
from langchain_community.tools.gmail.utils import build_resource_service, get_gmail_credentials
from langchain_community.tools.gmail.search import GmailSearch
from langchain_community.llms import Ollama
from langchain.agents import AgentType

# Load credentials (use your .json file from OAuth setup)
credentials = get_gmail_credentials(
    token_file="token.json",  # From your OAuth flow
    scopes=["https://mail.google.com/"],
    client_secrets_file="credentials.json"  # Your downloaded OAuth file
)
api_resource = build_resource_service(credentials=credentials)

# Gmail tool for searching/fetching emails
gmail_search = GmailSearch(api_resource=api_resource)

# Define tools
tools = [
    Tool(
        name="Gmail_Search",
        func=gmail_search.run,
        description="Search for recent emails in Gmail."
    )
]

# Ollama LLM
llm = Ollama(model="gemma3:4b")  # Or your pulled model

# Initialize agent
agent = initialize_agent(
    tools, 
    llm, 
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
    verbose=True
)

# Run query
result = agent.run("Tell me about my last email.")  # Or any prompt
print(result)