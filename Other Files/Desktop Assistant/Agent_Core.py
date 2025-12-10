from langchain_ollama import OllamaLLM
from langchain.agents import create_react_agent, AgentExecutor
from langchain.tools import Tool
from langchain.prompts import PromptTemplate
from googleapiclient.discovery import build
import whisper  # For local transcription; install pip install openai-whisper

llm = OllamaLLM(model="gemma:27b")

# Gmail tool example
def fetch_emails(creds, count=5):
    service = build('gmail', 'v1', credentials=creds)
    results = service.users().messages().list(userId='me', maxResults=count).execute()
    # Process messages...
    return "Emails fetched: [summaries]"

# Transcription tool
def transcribe_audio(file_path):
    model = whisper.load_model("base")
    result = model.transcribe(file_path)
    return result["text"]

tools = [
    Tool(name="FetchEmails", func=fetch_emails, description="Get recent emails."),
    Tool(name="TranscribeAudio", func=transcribe_audio, description="Transcribe phone recording."),
    # Add your existing ExecuteSQL, Calendar tools
]

prompt = PromptTemplate.from_template(  # Update your ReAct prompt
    """You are a personal assistant. Analyze communications for events/to-dos/notes.
    Tools: {tools}
    Question: {input}
    {agent_scratchpad}"""
)

agent = create_react_agent(llm, tools, prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Run example: executor.invoke({"input": "Check emails and add any meetings to calendar."})