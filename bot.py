import os
import json
import logging
import asyncio
import nest_asyncio
from datetime import datetime, time, timedelta
import pytz
from telegram import Update, Chat
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Timezone
TIMEZONE = pytz.timezone("America/Chicago")

# Office hours
WEEKDAY_START = time(9, 0)
WEEKDAY_END = time(17, 0)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
AUTHORIZED_GROUPS = {
    "-4206463598": "AIS Trucking",
    "-4181350900": "Another Authorized Group"
}

GMAIL_CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.getenv("GMAIL_REFRESH_TOKEN")
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "aisgenie.telegram@gmail.com")

EMAIL_TO = os.getenv("EMAIL_TO", "info@myaisagency.com")
EMAIL_ENDORSEMENT = os.getenv("EMAIL_ENDORSEMENT", "endorsement@myaisagency.com")

# Globals
known_group_chats = {}
last_command_time = {}

# Utility: Restrict to authorized groups only
def is_authorized_group(chat_id: str):
    return str(chat_id) in AUTHORIZED_GROUPS

# Cooldown check
def cooldown_passed(chat_id, command, cooldown_seconds=3600):
    now = datetime.now()
    key = f"{chat_id}:{command}"
    last_used = last_command_time.get(key)
    if last_used and (now - last_used).total_seconds() < cooldown_seconds:
        return False
    last_command_time[key] = now
    return True

# Send Gmail via OAuth2
def send_gmail_oauth(to_addr, subject, body, attachment_bytes=None, attachment_name=None):
    creds = Credentials(
        None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GMAIL_CLIENT_ID,
        client_secret=GMAIL_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/gmail.send"]
    )
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
    service = build("gmail", "v1", credentials=creds)
    message = MIMEMultipart()
    message["To"] = to_addr
    message["From"] = GMAIL_SENDER
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))
    if attachment_bytes and attachment_name:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={attachment_name}")
        message.attach(part)
    raw_message = {"raw": encoders.encode_base64(message.as_bytes()).decode()}
    service.users().messages().send(userId="me", body=raw_message).execute()

# Transcript rendering
def render_transcript_image(chat_title, messages):
    font_title = ImageFont.load_default()
    font_text = ImageFont.load_default()
    padding = 10
    width = 800
    height = padding * 2 + len(messages) * 20
    image = Image.new("RGB", (width, height), color="white")
    draw = ImageDraw.Draw(image)
    draw.text((padding, padding), f"Chat: {chat_title}", font=font_title, fill="black")
    y = padding + 20
    for m in messages:
        draw.text((padding, y), f"{m['from']}: {m['text']}", font=font_text, fill="black")
        y += 20
    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()

# Command handlers
async def restricted_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_authorized_group(chat_id):
        await update.message.reply_text("🚫 You are not authorized to use this bot here.")
        return False
    return True

async def ssinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await restricted_command(update, context):
        return
    if not cooldown_passed(update.effective_chat.id, "ssinfo"):
        await update.message.reply_text("⏳ Please wait before using /ssinfo again.")
        return
    chat_id = update.effective_chat.id
    messages = context.chat_data.get("recent_messages", [])[-5:]
    if not messages:
        await update.message.reply_text("No recent messages to capture.")
        return
    png_bytes = render_transcript_image(update.effective_chat.title, messages)
    send_gmail_oauth(EMAIL_TO, "AIS Genie Bot Transcript (/ssinfo)", "Attached is the transcript.", png_bytes, "transcript.png")
    await update.message.reply_text("📤 Transcript sent to info email.")

async def ssendo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await restricted_command(update, context):
        return
    if not cooldown_passed(update.effective_chat.id, "ssendo"):
        await update.message.reply_text("⏳ Please wait before using /ssendo again.")
        return
    messages = context.chat_data.get("recent_messages", [])[-5:]
    if not messages:
        await update.message.reply_text("No recent messages to capture.")
        return
    png_bytes = render_transcript_image(update.effective_chat.title, messages)
    send_gmail_oauth(EMAIL_ENDORSEMENT, "AIS Genie Bot Transcript (/ssendo)", "Attached is the transcript.", png_bytes, "transcript.png")
    await update.message.reply_text("📤 Transcript sent to endorsement email.")

async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await restricted_command(update, context):
        return
    await update.message.reply_text(f"Your ID: {update.effective_user.id}")

# Message tracker
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if not is_authorized_group(chat_id):
        return
    msg_list = context.chat_data.setdefault("recent_messages", [])
    msg_list.append({
        "from": update.effective_user.full_name,
        "text": update.message.text or ""
    })
    if len(msg_list) > 25:
        msg_list.pop(0)

# Main
def main():
    nest_asyncio.apply()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("ssinfo", ssinfo_command))
    app.add_handler(CommandHandler("ssendo", ssendo_command))
    app.add_handler(CommandHandler("myid", myid_command))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), message_handler))
    logger.info("Bot started.")
    app.run_polling()

if __name__ == "__main__":
    main()