#!/usr/bin/env python3
"""
OAuth Configuration Diagnostics
Checks for common OAuth setup issues
"""

import os
import json
import sys

def check_environment():
    """Check Python environment and required packages."""
    print("\n" + "="*60)
    print("Environment Check")
    print("="*60)
    
    print(f"Python version: {sys.version}")
    print(f"Current directory: {os.getcwd()}")
    
    # Check required packages
    packages = {
        'google.auth': 'google-auth',
        'google_auth_oauthlib': 'google-auth-oauthlib',
        'googleapiclient': 'google-api-python-client',
        'langchain': 'langchain',
        'langchain_community': 'langchain-community'
    }
    
    print("\nRequired packages:")
    missing = []
    for module, package in packages.items():
        try:
            __import__(module)
            print(f"✓ {module} is installed")
        except ImportError:
            print(f"✗ {module} is NOT installed")
            missing.append(package)
    
    if missing:
        print(f"\nInstall missing packages with:")
        print(f"pip install {' '.join(missing)}")
    
    return len(missing) == 0

def check_credentials_files():
    """Check for OAuth credentials files."""
    print("\n" + "="*60)
    print("Credentials Files Check")
    print("="*60)
    
    files_to_check = [
        "credentials.json",
        "credentials_personal.json",
        "token_workspace.json",
        "token_personal.json"
    ]
    
    for file in files_to_check:
        if os.path.exists(file):
            print(f"✓ Found: {file}")
            
            # If it's a credentials file, check its structure
            if file.startswith("credentials"):
                try:
                    with open(file, 'r') as f:
                        data = json.load(f)
                    
                    if 'installed' in data:
                        print(f"  Type: Desktop application")
                        print(f"  Client ID: {data['installed'].get('client_id', 'Not found')[:50]}...")
                        
                        # Check redirect URIs
                        redirect_uris = data['installed'].get('redirect_uris', [])
                        print(f"  Redirect URIs: {redirect_uris}")
                        
                    elif 'web' in data:
                        print(f"  Type: Web application")
                        print(f"  Client ID: {data['web'].get('client_id', 'Not found')[:50]}...")
                        
                        # Check redirect URIs
                        redirect_uris = data['web'].get('redirect_uris', [])
                        print(f"  Redirect URIs: {redirect_uris}")
                        
                        # Check for localhost in redirect URIs
                        localhost_uris = [uri for uri in redirect_uris if 'localhost' in uri]
                        if not localhost_uris:
                            print(f"  ⚠ WARNING: No localhost redirect URIs found!")
                            print(f"    Add these in Google Cloud Console:")
                            print(f"    - http://localhost:8080")
                            print(f"    - http://localhost:8081")
                    else:
                        print(f"  ⚠ WARNING: Unknown credential type")
                        print(f"    Keys found: {list(data.keys())}")
                        
                except json.JSONDecodeError:
                    print(f"  ✗ ERROR: Invalid JSON in {file}")
                except Exception as e:
                    print(f"  ✗ ERROR reading {file}: {e}")
                    
        else:
            print(f"✗ Not found: {file}")
            
            if file.startswith("credentials"):
                print(f"  To create this file:")
                print(f"  1. Go to Google Cloud Console")
                print(f"  2. APIs & Services > Credentials")
                print(f"  3. Create or select OAuth 2.0 Client ID")
                print(f"  4. Download JSON and save as '{file}'")

def check_oauth_config():
    """Provide OAuth configuration checklist."""
    print("\n" + "="*60)
    print("OAuth Configuration Checklist")
    print("="*60)
    
    print("\n✓ Required Google Cloud Console Settings:")
    print("1. APIs to Enable:")
    print("   ✓ Gmail API")
    print("   ✓ Google Calendar API")
    print("   (From your screenshot, these are already enabled ✓)")
    
    print("\n2. OAuth Consent Screen:")
    print("   ✓ Publishing status: Testing")
    print("   ✓ User type: External")
    print("   ✓ Test users added: your email addresses")
    print("   (From your screenshot, this is configured ✓)")
    
    print("\n3. OAuth Scopes:")
    print("   Required scopes (from your screenshot, these are added ✓):")
    print("   ✓ .../auth/gmail.readonly")
    print("   ✓ .../auth/gmail.modify")
    print("   ✓ .../auth/gmail.metadata")
    print("   ✓ .../auth/calendar.events")
    print("   ✓ .../auth/userinfo.email")
    print("   ✓ openid")
    
    print("\n4. OAuth 2.0 Client ID Configuration:")
    print("   Application type: Desktop app (recommended) or Web app")
    print("   If Web app, add redirect URIs:")
    print("   - http://localhost:8080")
    print("   - http://localhost:8081")
    print("   - http://localhost:8080/")  
    print("   - http://localhost:8081/")
    
def check_common_issues():
    """Check for common OAuth issues."""
    print("\n" + "="*60)
    print("Common Issues Check")
    print("="*60)
    
    issues = []
    
    # Check if running in correct directory
    if not any(os.path.exists(f) for f in ["credentials.json", "email_manager_claude.py"]):
        issues.append(("Working Directory", 
                      "You might be running the script from the wrong directory"))
    
    # Check for port conflicts
    import socket
    for port in [8080, 8081]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        if result == 0:
            issues.append((f"Port {port}", 
                          f"Port {port} is already in use. Use a different port or stop the service using it"))
    
    if issues:
        print("\n⚠ Potential issues found:")
        for issue, description in issues:
            print(f"  - {issue}: {description}")
    else:
        print("\n✓ No obvious issues detected")
    
    print("\n" + "="*60)
    print("Additional Troubleshooting Tips")
    print("="*60)
    
    print("\n1. If authentication keeps failing:")
    print("   - Delete all token*.json files and try again")
    print("   - Make sure you're using the correct Google account")
    print("   - Check that the account is added as a test user")
    
    print("\n2. If you get 403 errors:")
    print("   - Verify APIs are enabled in Google Cloud Console")
    print("   - Check that scopes are properly configured")
    print("   - Ensure OAuth consent screen is set up")
    
    print("\n3. If browser doesn't open:")
    print("   - Copy the URL from terminal and paste in browser")
    print("   - Try a different port number")
    print("   - Check firewall settings")
    
    print("\n4. If you get redirect URI mismatch:")
    print("   - Add http://localhost:YOUR_PORT to redirect URIs in Cloud Console")
    print("   - Wait a few minutes for changes to propagate")
    print("   - Use Desktop app type instead of Web app")

def main():
    print("\n" + "="*60)
    print("OAuth Diagnostics Tool")
    print("="*60)
    
    # Run all checks
    env_ok = check_environment()
    check_credentials_files()
    check_oauth_config()
    check_common_issues()
    
    print("\n" + "="*60)
    print("Diagnostics Complete")
    print("="*60)
    
    if not env_ok:
        print("\n❌ Fix package installation issues first")
    else:
        print("\n✓ Environment is ready")
        print("\nNext steps:")
        print("1. Run: python gmail_auth_test.py")
        print("2. Choose option 1 (Gmail only) to test basic auth")
        print("3. If that works, test option 2 (Full scopes)")
        print("4. Once auth works, run your main email_manager_claude.py")

if __name__ == "__main__":
    main()