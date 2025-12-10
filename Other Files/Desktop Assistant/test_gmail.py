from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Load existing creds (from google_api_setup.py)
creds = Credentials.from_authorized_user_file('token.json')

# Build Gmail service
service = build('gmail', 'v1', credentials=creds)

# List recent emails (last 5)
results = service.users().messages().list(userId='me', maxResults=5).execute()
messages = results.get('messages', [])

if not messages:
    print('No messages found.')
else:
    print('Recent emails:')
    for message in messages:
        msg = service.users().messages().get(userId='me', id=message['id'], format='minimal').execute()
        subject = next((header['value'] for header in msg['payload']['headers'] if header['name'] == 'Subject'), 'No Subject')
        print(f'- {subject}')