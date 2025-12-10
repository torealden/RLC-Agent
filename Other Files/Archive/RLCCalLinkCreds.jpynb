import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from langchain_google_community import CalendarToolkit

# If no token, run OAuth flow
if not os.path.exists('token.json'):
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scopes=['https://www.googleapis.com/auth/calendar'])
    creds = flow.run_local_server(port=8080)
    with open('token.json', 'w') as token:
        token.write(creds.to_json())
else:
    creds = Credentials.from_authorized_user_file('token.json')

toolkit = CalendarToolkit(credentials=creds)
tools = toolkit.get_tools()
print([t.name for t in tools])  # Should list create_event, etc.