# Quick test script - run this first to test authentication
def test_auth():
    """Test authentication with minimal scopes."""
    print("=" * 50)
    print("AUTHENTICATION TEST")
    print("=" * 50)
    
    # Test with just Gmail first
    GMAIL_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'openid',
        'https://www.googleapis.com/auth/userinfo.email'
    ]
    
    print("\nTesting Gmail authentication...")
    try:
        creds = setup_credentials(
            "credentials.json",  # Your credentials file
            GMAIL_SCOPES,
            "token_test.json",
            port=8080
        )
        print("\n✅ Authentication successful!")
        
        # Test API call
        gmail_service = build('gmail', 'v1', credentials=creds)
        messages = gmail_service.users().messages().list(userId='me', maxResults=1).execute()
        print(f"✅ Successfully retrieved messages from Gmail")
        
    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Run authentication test
        test_auth()
    else:
        # Run main program
        main()