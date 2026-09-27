#!/usr/bin/env python
# scripts/get_upstox_token.py
# Helps you get your Upstox access token step by step.
# Run: python scripts/get_upstox_token.py

print("""
============================================================
 Upstox Access Token Helper
============================================================

STEP 1: Create an Upstox Developer App
---------------------------------------
1. Go to: https://developer.upstox.com/
2. Log in with your Upstox trading account
3. Click "Create App"
4. Fill in:
   - App Name: Optiscalper
   - Redirect URL: http://localhost:8000/upstox-callback
   - Description: Trading Bot
5. Submit and note down your:
   - API Key (Client ID)
   - API Secret (Client Secret)

""")

api_key    = input("Paste your API Key here: ").strip()
api_secret = input("Paste your API Secret here: ").strip()

auth_url = (
    f"https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code"
    f"&client_id={api_key}"
    f"&redirect_uri=http://localhost:8000/upstox-callback"
)

print(f"""
STEP 2: Get Authorization Code
-------------------------------
1. Open this URL in your browser:

   {auth_url}

2. Log in with your Upstox credentials
3. Allow the app
4. You will be redirected to a URL like:
   http://localhost:8000/upstox-callback?code=XXXXXXXX
5. Copy the code from the URL (the part after ?code=)
""")

auth_code = input("Paste the authorization code here: ").strip()

print("\nFetching access token...")

import requests

resp = requests.post(
    "https://api.upstox.com/v2/login/authorization/token",
    data={
        "code":          auth_code,
        "client_id":     api_key,
        "client_secret": api_secret,
        "redirect_uri":  "http://localhost:8000/upstox-callback",
        "grant_type":    "authorization_code",
    },
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)

if resp.status_code == 200:
    data         = resp.json()
    access_token = data.get("access_token", "")
    print(f"""
✅ SUCCESS! Your access token:
-------------------------------
{access_token}
-------------------------------

Copy this token and:
1. Open http://localhost:8000
2. Log in to your account
3. Go to Settings
4. Paste the token in "Upstox Access Token" field
5. Click Save

⚠️  This token is valid only for today.
    Get a new token each morning before 9:15 AM.
""")
else:
    print(f"""
❌ Failed to get token.
   Status: {resp.status_code}
   Response: {resp.text}

Common reasons:
- Wrong API Key or Secret
- Authorization code already used (get a new one)
- Redirect URL mismatch (must be exactly: http://localhost:8000/upstox-callback)
""")
