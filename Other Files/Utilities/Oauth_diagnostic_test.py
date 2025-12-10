#!/usr/bin/env python3
"""
Minimal OAuth Test - Shows exact error details
This will reveal what's causing the invalid_client error
"""

import json
import os
from google_auth_oauthlib.flow import InstalledAppFlow

def inspect_credentials(filename):
    """Show what's in the credentials file."""
    print(f"\n{'='*60}")
    print(f"Inspecting: {filename}")
    print('='*60)
    
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return None
        
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
        
        if 'web' in data:
            web = data['web']
            print("Type: Web Application")
            print(f"Client ID: {web.get('client_id', 'MISSING')[:60]}...")
            
            # This is the critical field
            if 'client_secret' in web:
                secret = web['client_secret']
                print(f"Client Secret: {'*' * 10} [PRESENT - {len(secret)} chars]")
            else:
                print("Client Secret: ❌ MISSING - THIS IS YOUR PROBLEM!")
                print("\nTO FIX:")
                print("1. Go to Google Cloud Console")
                print("2. Find this OAuth client and click it")
                print("3. Look for 'Client secret' field")
                print("4. If it shows 'No client secret', you need to:")
                print("   - Delete this OAuth client")
                print("   - Create a new one")
                print("   - Download the JSON immediately after creation")
                
            print(f"Redirect URIs: {web.get('redirect_uris', [])}")
            
        elif 'installed' in data:
            installed = data['installed']
            print("Type: Desktop Application")
            print(f"Client ID: {installed.get('client_id', 'MISSING')[:60]}...")
            
            if 'client_secret' in installed:
                secret = installed['client_secret']
                print(f"Client Secret: {'*' * 10} [PRESENT - {len(secret)} chars]")
            else:
                print("Client Secret: ❌ MISSING")
                
        return data
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return None

def test_oauth(filename):
    """Try to authenticate and show the exact error."""
    print(f"\n{'='*60}")
    print(f"Testing OAuth with: {filename}")
    print('='*60)
    
    if not os.path.exists(filename):
        print(f"❌ File not found: {filename}")
        return
        
    try:
        # Minimal test with one scope
        flow = InstalledAppFlow.from_client_secrets_file(
            filename,
            ['https://www.googleapis.com/auth/gmail.readonly']
        )
        
        print("Starting OAuth flow...")
        print("If browser doesn't open, copy the URL from the output")
        
        # Try to run the server
        creds = flow.run_local_server(port=8080, open_browser=False)
        
        print("✅ SUCCESS! OAuth is working!")
        print(f"Access token obtained: {creds.token[:20]}...")
        
    except Exception as e:
        error_str = str(e)
        print(f"❌ OAuth failed with error: {error_str}")
        
        # Diagnose specific errors
        if "invalid_client" in error_str:
            print("\n🔍 DIAGNOSIS: Invalid Client Error")
            print("This means one of:")
            print("1. client_secret is missing from your credentials file")
            print("2. OAuth client was deleted in Google Cloud Console")
            print("3. client_secret doesn't match what's in Cloud Console")
            
        elif "redirect_uri_mismatch" in error_str:
            print("\n🔍 DIAGNOSIS: Redirect URI Mismatch")
            print("The redirect URI doesn't match Cloud Console configuration")
            
        elif "access_denied" in error_str:
            print("\n🔍 DIAGNOSIS: Access Denied")
            print("You cancelled the authorization or aren't a test user")

def main():
    print("\n" + "="*60)
    print("OAuth Diagnostic Test")
    print("="*60)
    
    # Check all possible credential files
    files_to_test = [
        "credentials.json",
        "credentials_personal.json",
        "credentials_desktop.json"
    ]
    
    print("\nLooking for credential files...")
    found_files = []
    
    for f in files_to_test:
        if os.path.exists(f):
            print(f"✓ Found: {f}")
            found_files.append(f)
        else:
            print(f"✗ Not found: {f}")
    
    if not found_files:
        print("\n❌ No credential files found in current directory!")
        print(f"Current directory: {os.getcwd()}")
        print("\nMake sure you're running this script in the right folder")
        return
    
    # Inspect and test each file
    for filename in found_files:
        data = inspect_credentials(filename)
        
        if data:
            response = input(f"\nTest OAuth with {filename}? (y/n): ")
            if response.lower() == 'y':
                test_oauth(filename)
    
    print("\n" + "="*60)
    print("Recommendations")
    print("="*60)
    
    print("\nIf you're getting 'invalid_client' errors:")
    print("\n1. IMMEDIATE FIX - Create a Desktop OAuth Client:")
    print("   - Go to Google Cloud Console")
    print("   - APIs & Services → Credentials")
    print("   - Create OAuth client ID → Desktop app")
    print("   - Download as 'credentials_desktop.json'")
    print("   - Desktop apps work better for local scripts!")
    
    print("\n2. OR Fix Your Web Application Client:")
    print("   - Your Web OAuth client might not have a client_secret")
    print("   - Some Web clients are created without secrets")
    print("   - You may need to delete and recreate the OAuth client")
    
    print("\n3. Make Sure in Cloud Console:")
    print("   - Gmail API is enabled")
    print("   - OAuth consent screen is configured")
    print("   - Your email is in test users list")

if __name__ == "__main__":
    main()