# Quick test script
if __name__ == "__main__":
    # Test with just Gmail first
    GMAIL_SCOPES = [
        'https://www.googleapis.com/auth/gmail.readonly',
        'openid',
        'https://www.googleapis.com/auth/userinfo.email'
    ]
    
    print("Testing Gmail authentication...")
    try:
        creds = setup_credentials(
            "credentials.json",  # Your credentials file
            GMAIL_SCOPES,
            "token_test.json",
            port=8080
        )
        print("\n✅ Authentication successful!")
    except Exception as e:
        print(f"\n❌ Authentication failed: {e}")