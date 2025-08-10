import os
from google_auth_oauthlib.flow import InstalledAppFlow

CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Set GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET env vars before running.")
        return
    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        },
        SCOPES,
    )
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline", include_granted_scopes="true")
    print("\n================== COPY THIS VALUE ==================")
    print("GMAIL_REFRESH_TOKEN=", creds.refresh_token, sep="")
    print("=====================================================\n")

if __name__ == "__main__":
    main()