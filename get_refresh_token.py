import os
import sys
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request as GARequest

# Required env vars:
#   GMAIL_CLIENT_ID
#   GMAIL_CLIENT_SECRET
# Optional:
#   GMAIL_SENDER          -> email to use as sender (should be the same account you auth with)
#   TEST_SEND_TO          -> if set, the script will send a test email to this address
#   SEND_TEST             -> set to "1" to enable test email (requires TEST_SEND_TO)

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
GMAIL_SENDER = os.getenv("GMAIL_SENDER")
SEND_TEST = os.getenv("SEND_TEST", "0") == "1"
TEST_SEND_TO = os.getenv("TEST_SEND_TO")

def fatal(msg: str):
    print(f"❌ {msg}")
    sys.exit(1)

def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        fatal("Set GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET env vars before running.")

    # Build an InstalledAppFlow using client_config (Desktop app style)
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

    # Force refresh token issuance:
    # - access_type="offline" requests a refresh token
    # - prompt="consent" forces Google to show the consent screen (even if already granted)
    # - include_granted_scopes="true" is harmless here but fine
    creds = flow.run_local_server(
        port=0,
        prompt="consent",
        access_type="offline",
        include_granted_scopes="true",
    )

    # Google sometimes omits refresh_token if it thinks one already exists.
    # If that happens: remove app access at https://myaccount.google.com/permissions and try again.
    if not creds.refresh_token:
        fatal(
            "Google did not return a refresh token.\n"
            "Tip: Remove this app’s access at https://myaccount.google.com/permissions and retry."
        )

    print("\n================== COPY THIS VALUE ==================")
    print("GMAIL_REFRESH_TOKEN=", creds.refresh_token, sep="")
    print("=====================================================\n")

    # Optional: quick live test (send email using the new creds)
    if SEND_TEST:
        if not GMAIL_SENDER:
            fatal("SEND_TEST=1 but GMAIL_SENDER is not set.")
        if not TEST_SEND_TO:
            fatal("SEND_TEST=1 but TEST_SEND_TO is not set.")

        # Build a Credentials object using only refresh_token + client info (like your bot will)
        test_creds = Credentials(
            token=None,
            refresh_token=creds.refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            scopes=SCOPES,
        )
        # Refresh to obtain access token
        test_creds.refresh(GARequest())

        from email.message import EmailMessage
        import base64

        service = build("gmail", "v1", credentials=test_creds, cache_discovery=False)

        msg = EmailMessage()
        msg["From"] = GMAIL_SENDER
        msg["To"] = TEST_SEND_TO
        msg["Subject"] = "Gmail OAuth test OK"
        msg.set_content("If you received this, the refresh token + sender match correctly. ✅")

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        print(f"✅ Test email sent from {GMAIL_SENDER} to {TEST_SEND_TO}")

if __name__ == "__main__":
    main()