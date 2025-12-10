#!/usr/bin/env python3
"""
Fix OAuth Invalid Client Error
Diagnostic and resolution script for OAuth authentication issues
"""

import json
import os
import sys
from datetime import datetime

def analyze_credentials_file(filepath):
    """Analyze a credentials file for common issues."""
    print(f"\n{'='*60}")
    print(f"Analyzing: {filepath}")
    print('='*60)
    
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return False
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if 'web' in data:
            web_data = data['web']
            print("📋 OAuth Client Details:")
            print(f"   Type: Web Application")
            print(f"   Client ID: {web_data.get('client_id', 'MISSING')[:50]}...")
            
            # Check for client_secret
            if 'client_secret' not in web_data:
                print("   ❌ MISSING: client_secret")
                print("      This is likely causing the invalid_client error!")
            else:
                secret = web_data['client_secret']
                print(f"   Client Secret: {'*' * 20} (present)")
                if len(secret) < 10:
                    print("   ⚠️  WARNING: Client secret seems too short")
            
            # Check project ID
            project_id = web_data.get('project_id', 'MISSING')
            print(f"   Project ID: {project_id}")
            
            # Check redirect URIs
            redirect_uris = web_data.get('redirect_uris', [])
            print(f"   Redirect URIs:")
            for uri in redirect_uris:
                print(f"      - {uri}")
                
            # Check auth URIs
            print(f"   Auth URI: {web_data.get('auth_uri', 'MISSING')}")
            print(f"   Token URI: {web_data.get('token_uri', 'MISSING')}")
            
            return True
            
        elif 'installed' in data:
            installed_data = data['installed']
            print("📋 OAuth Client Details:")
            print(f"   Type: Desktop Application")
            print(f"   Client ID: {installed_data.get('client_id', 'MISSING')[:50]}...")
            
            if 'client_secret' not in installed_data:
                print("   ⚠️  Note: Desktop apps might not have client_secret")
            else:
                print(f"   Client Secret: {'*' * 20} (present)")
                
            return True
            
        else:
            print("❌ Invalid credentials file structure")
            print(f"   Found keys: {list(data.keys())}")
            return False
            
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in file: {e}")
        return False
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return False

def create_desktop_credentials_converter():
    """Create a script to convert web credentials to desktop format."""
    
    converter_script = '''#!/usr/bin/env python3
"""
Convert Web OAuth credentials to Desktop format
This can help resolve invalid_client errors
"""

import json
import sys

def convert_web_to_desktop(input_file, output_file):
    """Convert web credentials to desktop format."""
    
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        if 'web' not in data:
            print(f"Error: {input_file} is not a web credentials file")
            return False
        
        web_data = data['web']
        
        # Create desktop format
        desktop_data = {
            "installed": {
                "client_id": web_data.get('client_id'),
                "project_id": web_data.get('project_id'),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uris": ["http://localhost"]
            }
        }
        
        # Add client_secret if present
        if 'client_secret' in web_data:
            desktop_data['installed']['client_secret'] = web_data['client_secret']
        
        with open(output_file, 'w') as f:
            json.dump(desktop_data, f, indent=2)
        
        print(f"✓ Converted {input_file} to desktop format: {output_file}")
        return True
        
    except Exception as e:
        print(f"Error converting file: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_credentials.py <input_web_creds.json> <output_desktop_creds.json>")
        sys.exit(1)
    
    convert_web_to_desktop(sys.argv[1], sys.argv[2])
'''
    
    with open('convert_credentials.py', 'w') as f:
        f.write(converter_script)
    
    print("\n✓ Created convert_credentials.py")
    print("  Use: python convert_credentials.py credentials.json credentials_desktop.json")

def main():
    print("\n" + "="*60)
    print("OAuth Invalid Client Error - Diagnostic Tool")
    print("="*60)
    
    print("\n🔍 IDENTIFYING THE PROBLEM")
    print("-"*40)
    print("\nThe 'invalid_client' error typically means one of:")
    print("1. Missing or incorrect client_secret in credentials file")
    print("2. OAuth client was deleted or disabled in Google Cloud Console")
    print("3. Credentials are from a different project")
    print("4. OAuth client type mismatch (Web vs Desktop)")
    
    # Analyze both credential files
    creds_files = ["credentials.json", "credentials_personal.json"]
    
    for cred_file in creds_files:
        if os.path.exists(cred_file):
            analyze_credentials_file(cred_file)
    
    print("\n" + "="*60)
    print("SOLUTION STEPS")
    print("="*60)
    
    print("\n📝 Step 1: Verify OAuth Client in Google Cloud Console")
    print("-"*40)
    print("1. Go to: https://console.cloud.google.com")
    print("2. Select your project: rlccalendarlink")
    print("3. Navigate to: APIs & Services > Credentials")
    print("4. Check your OAuth 2.0 Client IDs:")
    print("   - Verify the Client ID matches what's in your credentials.json")
    print("   - Check if the client is active (not deleted)")
    print("   - Note the Application Type (Web or Desktop)")
    
    print("\n📝 Step 2: Download Fresh Credentials")
    print("-"*40)
    print("If the client exists but still doesn't work:")
    print("1. In Google Cloud Console > APIs & Services > Credentials")
    print("2. Click on your OAuth 2.0 Client ID")
    print("3. Click 'DOWNLOAD JSON' button")
    print("4. Save as 'credentials_new.json'")
    print("5. Compare with your existing credentials.json")
    
    print("\n📝 Step 3: Create Desktop Application (Recommended)")
    print("-"*40)
    print("Desktop apps are easier to work with for local scripts:")
    print("1. In Google Cloud Console > APIs & Services > Credentials")
    print("2. Click '+ CREATE CREDENTIALS' > 'OAuth client ID'")
    print("3. Choose 'Desktop app' as Application type")
    print("4. Name it: 'Email Manager Desktop'")
    print("5. Download the JSON and save as 'credentials_desktop.json'")
    
    print("\n📝 Step 4: Fix Web Application Redirect URIs")
    print("-"*40)
    print("If you must use Web application type:")
    print("1. Edit your OAuth 2.0 Client ID")
    print("2. Add ALL these Authorized redirect URIs:")
    print("   - http://localhost:8080")
    print("   - http://localhost:8080/")
    print("   - http://localhost:8081")
    print("   - http://localhost:8081/")
    print("   - http://127.0.0.1:8080")
    print("   - http://127.0.0.1:8080/")
    print("3. Save and wait 5 minutes for propagation")
    
    print("\n📝 Step 5: Verify Project Settings")
    print("-"*40)
    print("1. Ensure Gmail API is enabled")
    print("2. Ensure Calendar API is enabled")
    print("3. OAuth consent screen is configured")
    print("4. Your email is in test users list")
    
    # Create converter script
    create_desktop_credentials_converter()
    
    print("\n" + "="*60)
    print("QUICK FIX ATTEMPT")
    print("="*60)
    
    print("\n🔧 Try this simplified test script:")
    
    test_script = '''import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Use the EXACT scopes from your OAuth consent screen
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def simple_auth_test():
    """Minimal authentication test."""
    creds = None
    
    # Delete old token to force re-authentication
    if os.path.exists('token_test_simple.json'):
        os.remove('token_test_simple.json')
    
    # Start OAuth flow
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json',  # or try 'credentials_desktop.json'
        SCOPES
    )
    
    # Use run_console for debugging
    print("Starting authentication flow...")
    print("Copy the URL to your browser if it doesn't open automatically")
    
    creds = flow.run_local_server(port=8080, open_browser=False)
    
    # Test the credentials
    service = build('gmail', 'v1', credentials=creds)
    profile = service.users().getProfile(userId='me').execute()
    print(f"Success! Authenticated as: {profile['emailAddress']}")
    
    # Save token
    with open('token_test_simple.json', 'w') as token:
        token.write(creds.to_json())

if __name__ == "__main__":
    simple_auth_test()
'''
    
    with open('simple_auth_test.py', 'w') as f:
        f.write(test_script)
    
    print("✓ Created: simple_auth_test.py")
    print("\nRun: python simple_auth_test.py")
    
    print("\n" + "="*60)
    print("MOST COMMON FIX")
    print("="*60)
    
    print("\n🎯 The fastest solution is usually:")
    print("1. Create a NEW Desktop OAuth client in Google Cloud Console")
    print("2. Download its credentials as 'credentials_desktop.json'")
    print("3. Update your script to use 'credentials_desktop.json'")
    print("4. Run the authentication again")
    
    print("\n💡 Desktop apps don't require redirect URI configuration")
    print("   and are much simpler for local scripts!")

if __name__ == "__main__":
    main()