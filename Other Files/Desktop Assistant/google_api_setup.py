import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from langchain_google_community import CalendarToolkit

# Scopes (readonly for safety)
SCOPES_WORKSPACE = ['https://www.googleapis.com/auth/calendar.events', 
                    'https://www.googleapis.com/auth/gmail.readonly', 
                    ]
SCOPES_PERSONAL = ['https://www.googleapis.com/auth/gmail.readonly'
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
        try:
            msg = gmail_workspace.users().messages().get(userId='me', id=message['id'], format='metadata', metadataHeaders=['Subject']).execute()
            headers = msg.get('payload', {}).get('headers', [])
            subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'No Subject')
            print(f'- {subject}')
        except Exception as e:
            print(f'- Error fetching message: {str(e)}')
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
        try:
            msg = gmail_personal.users().messages().get(userId='me', id=message['id'], format='metadata', metadataHeaders=['Subject']).execute()
            headers = msg.get('payload', {}).get('headers', [])
            subject = next((header['value'] for header in headers if header['name'] == 'Subject'), 'No Subject')
            print(f'- {subject}')
        except Exception as e:
            print(f'- Error fetching message: {str(e)}')
else:
    print('No Personal emails found.')