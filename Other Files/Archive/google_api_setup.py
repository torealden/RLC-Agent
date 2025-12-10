import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from langchain_google_community import CalendarToolkit

# Scopes (readonly for safety, and puuusssiiiess)
SCOPES_WORKSPACE = ['https://www.googleapis.com/auth/calendar.events', 
                    'https://www.googleapis.com/auth/gmail.readonly',
                    'https://www.googleapis.com/auth/gmail.metadata',
                    'https://www.googleapis.com/auth/gmail.modify'
                    ]
SCOPES_PERSONAL = ['https://www.googleapis.com/auth/gmail.readonly',
                   'https://www.googleapis.com/auth/gmail.metadata',
                   'https://www.googleapis.com/auth/gmail.modify'
                   ]

def setup_credentials(credentials_file, scopes, token_file):
    if not os.path.exists(token_file):
        flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes)
        creds = flow.run_local_server(port=8080)
        with open(token_file, 'w') as token:
            token.write(creds.to_json())
    else:
        creds = Credentials.from_authorized_user_file(token_file, scopes)
    return creds

# Setup Workspace (Calendar + Gmail)
creds_workspace = setup_credentials('credentials.json', SCOPES_WORKSPACE, 'token_workspace.json')

# Test Workspace Calendar
toolkit = CalendarToolkit(credentials=creds_workspace)
tools = toolkit.get_tools()
print("Workspace Calendar tools:", [t.name for t in tools])

# Test Workspace Gmail
gmail_workspace = build('gmail', 'v1', credentials=creds_workspace)
results_ws = gmail_workspace.users().messages().list(userId='me', maxResults=5).execute()
messages_ws = results_ws.get('messages', [])
if messages_ws:
    print('Recent Workspace emails:')
    for message in messages_ws:
        msg = gmail_workspace.users().messages().get(userId='me', id=message['id'], format='minimal').execute()
        subject = next((header['value'] for header in msg['payload']['headers'] if header['name'] == 'Subject'), 'No Subject')
        print(f'- {subject}')
else:
    print('No Workspace emails found.')

# Setup Personal Gmail
creds_personal = setup_credentials('credentials_personal.json', SCOPES_PERSONAL, 'token_personal.json')

# Test Personal Gmail
gmail_personal = build('gmail', 'v1', credentials=creds_personal)
results_p = gmail_personal.users().messages().list(userId='me', maxResults=5).execute()
messages_p = results_p.get('messages', [])
if messages_p:
    print('Recent Personal emails:')
    for message in messages_p:
        msg = gmail_personal.users().messages().get(userId='me', id=message['id'], format='minimal').execute()
        subject = next((header['value'] for header in msg['payload']['headers'] if header['name'] == 'Subject'), 'No Subject')
        print(f'- {subject}')
else:
    print('No Personal emails found.')