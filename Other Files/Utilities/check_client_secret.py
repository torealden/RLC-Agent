#!/usr/bin/env python3
"""
Check OAuth Credentials for client_secret
This is the most common cause of invalid_client errors
"""

import json
import os
import sys

def check_credentials_file(filepath):
    """Check if credentials file has all required fields."""
    print(f"\n{'='*60}")
    print(f"Checking: {filepath}")
    print('='*60)
    
    if not os.path.exists(filepath):
        print(f"❌ File not found")
        return False
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Check for web application
        if 'web' in data:
            web = data['web']
            print("✓ Type: Web Application")
            
            required_fields = {
                'client_id': web.get('client_id'),
                'client_secret': web.get('client_secret'),
                'project_id': web.get('project_id'),
                'auth_uri': web.get('auth_uri'),
                'token_uri': web.get('token_uri')
            }
            
            print("\nRequired Fields Check:")
            all_present = True
            
            for field, value in required_fields.items():
                if value:
                    if field == 'client_secret':
                        # Don't show the actual secret
                        print(f"  ✓ {field}: {'*' * 20} (length: {len(value)})")
                        if len(value) < 10:
                            print(f"    ⚠️  WARNING: Secret seems too short!")
                    elif field == 'client_id':
                        print(f"  ✓ {field}: {value[:50]}...")
                    else:
                        print(f"  ✓ {field}: {value}")
                else:
                    print(f"  ❌ {field}: MISSING!")
                    all_present = False
                    if field == 'client_secret':
                        print(f"    🚨 THIS IS CAUSING YOUR INVALID_CLIENT ERROR!")
            
            print(f"\nRedirect URIs: {web.get('redirect_uris', [])}")
            
            if not all_present:
                print("\n🔧 FIX NEEDED:")
                print("1. Go to Google Cloud Console > APIs & Services > Credentials")
                print(f"2. Find the OAuth client with ID: {web.get('client_id', 'unknown')}")
                print("3. Click DOWNLOAD JSON again")
                print("4. Make sure the downloaded file has the client_secret field")
                
            return all_present
            
        # Check for desktop application
        elif 'installed' in data:
            installed = data['installed']
            print("✓ Type: Desktop Application")
            
            required_fields = {
                'client_id': installed.get('client_id'),
                'client_secret': installed.get('client_secret'),
                'project_id': installed.get('project_id')
            }
            
            print("\nRequired Fields Check:")
            all_present = True
            
            for field, value in required_fields.items():
                if value:
                    if field == 'client_secret':
                        print(f"  ✓ {field}: {'*' * 20} (length: {len(value)})")
                    else:
                        print(f"  ✓ {field}: {value}")
                else:
                    print(f"  ❌ {field}: MISSING!")
                    all_present = False
                    
            return all_present
            
        else:
            print(f"❌ Unknown format. Keys found: {list(data.keys())}")
            return False
            
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("OAuth Client Secret Checker")
    print("="*60)
    
    files_to_check = [
        "credentials.json",
        "credentials_personal.json",
        "credentials_desktop.json"
    ]
    
    found_files = []
    for f in files_to_check:
        if os.path.exists(f):
            found_files.append(f)
            
    if not found_files:
        print("❌ No credentials files found!")
        print("\nMake sure you're running this in the right directory")
        return
    
    print(f"\nFound {len(found_files)} credential file(s)")
    
    all_valid = True
    for filepath in found_files:
        if not check_credentials_file(filepath):
            all_valid = False
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    if all_valid:
        print("\n✅ All credential files have required fields!")
        print("\nIf you're still getting invalid_client errors:")
        print("1. The OAuth client might be disabled in Cloud Console")
        print("2. Try deleting all token*.json files and re-authenticating")
        print("3. Make sure you're using the right Google account")
    else:
        print("\n❌ Some credential files are missing required fields!")
        print("\nTO FIX:")
        print("1. Go to: https://console.cloud.google.com")
        print("2. Navigate to APIs & Services > Credentials")
        print("3. Click on your OAuth 2.0 Client ID")
        print("4. Make sure it's not disabled")
        print("5. Click DOWNLOAD JSON")
        print("6. Save the file and check it has client_secret field")
        
        print("\n⚠️  IMPORTANT: Sometimes the client_secret is not included if:")
        print("   - The OAuth client was just created (wait a minute)")
        print("   - You're downloading from the wrong screen")
        print("   - The client was created without a secret")
        
        print("\n💡 ALTERNATIVE: Create a Desktop OAuth Client")
        print("   Desktop apps are simpler and work better for local scripts")
        print("   They don't need redirect URI configuration!")

if __name__ == "__main__":
    main()