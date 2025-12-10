def setup_credentials(credentials_file, scopes, token_file, port=8080):
    """Setup OAuth2 credentials with better error handling"""
    creds = None
    
    #Try to load existing token
    if os.path.exists(token_file):
        try:
            creds = Credentials.from_authorized_user_file(token_file, scopes)
        except Exception as e:
            print(f"Error loading token from {token_file}: {e}")
            creds = None
            
    #Check validity of credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print ("Token refreshed successfully")
            except Exception as e:
                print(f"Error refreshing token:{e}")
                creds = None
                
        #If credentials are still not valid, run OAuth flow
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials_file, scopes
                )
                #Fix: Use separate prompt values
                creds = flow.run_local_server(
                    port=port,
                    authorization_prompt_message='Please visit this URL to authorize this application: {url}',
                    success_message='The auth flow is complete; you may close this window.',
                    open_browser=True
                    )
            
                #Save the credentials for next run
                with open(token_file, 'w') as token:
                    token.write(creds.to_json())
                print(f"New token save to {token_file}")
                
            except Exception as e:
                print(f"Error during OAuth flow: {e}")
                raise
        
    return creds