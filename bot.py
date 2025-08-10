import os
import json
import logging
import asyncio
from datetime import datetime, time
from collections import deque, defaultdict
from typing import Dict, Any, Optional, Set
import pytz
from email.message import EmailMessage
import base64
import io
import textwrap

from telegram import Update, Chat
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# Optional image rendering for transcript (falls back to text if Pillow missing)
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_OK = True
except Exception:
    PIL_OK = False

# Gmail OAuth
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request as GARequest
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- Config ----------------
TIMEZONE = pytz.timezone("America/Chicago")

def _csv_env(name: str) -> Set[str]:
    raw = os.getenv(name, "") or ""
    return {s.strip() for s in raw.split(",") if s.strip()}

# AIS/team groups (users seen here become authorized)
AIS_TEAM_CHAT_IDS = _csv_env("AIS_TEAM_CHAT_IDS") or {"-4206463598", "-4181350900"}

# Authorized group chats = command-only mode (log + respond to commands, ignore everything else)
SILENT_GROUP_IDS = _csv_env("SILENT_GROUP_IDS") or set(AIS_TEAM_CHAT_IDS)

# Preloaded authorized user IDs (comma-separated env)
PREAUTHORIZED_USER_IDS = {int(x) for x in _csv_env("AUTHORIZED_USER_IDS")} if _csv_env("AUTHORIZED_USER_IDS") else set()

# Office hours (CT)
WEEKDAY_START = time(9, 0)
WEEKDAY_END = time(17, 0)
WEEKDAY_CUTOFF = time(16, 30)
LUNCH_START = time(12, 30)
LUNCH_END = time(13, 30)

# Env vars
BOT_TOKEN = os.getenv("BOT_TOKEN")
GMAIL_CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.getenv("GMAIL_REFRESH_TOKEN")
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "aisgenie.telegram@gmail.com")
EMAIL_DEFAULT_TO = os.getenv("EMAIL_TO", "info@myaisagency.com")
EMAIL_ENDORSEMENT = os.getenv("EMAIL_ENDORSEMENT", "endorsements@myaisagency.com")

# ---------------- Messages ----------------
CLOSED_MESSAGE = (
    "⏰ Our agency is currently closed.\n\n"
    "Business Hours:\n"
    "🕘 Monday to Friday: 9:00 AM – 5:00 PM\n"
    "🛑 Saturday & Sunday: Closed\n\n"
    "⚠️ Your endorsement request was not processed.\n"
    "Please reach out during business hours so it wont be overlooked. Thank you!"
)
AFTER_CUTOFF_MESSAGE = (
    "⚠️ Sorry, your endorsement was received outside the cutoff period.\n\n"
    "It will be processed the next business day. Thank you for your understanding!"
)
WEEKEND_MESSAGE = (
    "Thank you for reaching out. 😉\n\n"
    "🔒 We’re currently closed for the weekend (Saturday & Sunday). Our office will resume regular hours on Monday at 9:00 a.m.\n\n"
    "⚠️ Please note that your request was not processed, and we kindly ask that you resend it Monday morning to ensure it’s handled promptly.\n\n"
    "Thank you for your understanding! 🤗"
)
COI_REMINDER = (
    "📩 For Certificate of Insurance (COI) or certificate requests, please email us at: info@myaisagency.com\n\n"
    "📬 Kindly include:\n"
    "• COI holder’s name\n"
    "• Complete mailing address\n"
    "• Any special wording or instructions\n"
    "• The email address where we should send the certificate\n\n"
    "This helps us process your request securely and efficiently. Thank you!"
)
RULES_MESSAGE = (
    "📜 *Advanced Insurance Solutions Telegram Rules*\n\n"
    "‼️ IF THE CHANGE WAS NOT CONFIRMED OVER EMAIL, IT DID NOT HAPPEN.\n"
    "⏳ Please allow 10–15 minutes for a response.\n\n"
    "🔹 Telegram is for communication only. Policy changes must be confirmed by email.\n\n"
    "📌 *Guidelines:*\n"
    "1. All COI requests must be emailed to coi@myaisagency.com\n"
    "2. No editing of posts\n"
    "3. Do not reply to old posts, Kindly resend the request instead\n"
    "4. Don’t send photos of VINs, type Year, Make, and VIN\n"
    "_Use this format for policy changes:_\n"
    "• Remove VIN: 4V4NC9TH5KN216424\n"
    "• Add VIN: 1FUJHHDR3LLLH8454\n"
    "• Remove driver: Phillip Moore\n"
    "• Add driver: RUBENS ESTIME\n"
    "5. If your policy requires MVR, attach it. If not, we’ll order one and charge $30\n"
    "6. Send CDL with driver’s name clearly\n"
    "7. We don’t work weekends, resend requests on Monday\n"
    "8. Physical Damage coverage is not automatically added\n"
    "9. We accept changes Mon–Fri, 9:00 AM–4:30 PM (4:00 PM Friday)\n"
    "10. No change is valid unless confirmed by email"
)
LAST_CALL_MESSAGE = (
    "📢 *Last Call for Changes!*\n\n"
    "Please submit any policy changes before our cut-off time:\n"
    "🗓️ Weekdays: 4:30 PM\n\n"
    "Changes after this time will be processed the next business day."
)
LUNCH_MESSAGE = (
    "🍽️ Our team is currently on lunch break (12:30 PM – 1:30 PM CT).\n\n"
    "We’ll respond once we’re back. To make sure we don’t miss anything, feel free to email us too.\n"
    "📧 info@myaisagency.com"
)
EMAILS_MESSAGE = (
    "📧 *PLEASE USE THE FOLLOWING EMAIL TO GET YOUR REQUEST PROCESSED ASAP.*\n\n"
    "• coi@myaisagency.com – For all CERTIFICATES requests please send your request\n"
    "• Info@myaisagency.com – For general Questions and Binding\n"
    "• Endorsements@myaisagency.com – For policy CHANGES / QUOTES / DRIVER & TRUCK LIST on an existing policy\n"
    "• Claims@myaisagency.com – For all CLAIMS related questions and requotes"
)
SIGN_MESSAGE = (
    "📬 Please check your email, we’ve sent your documents for **e-signature**.\n"
    "Kindly review and sign at your earliest convenience. If you have any questions, reply here and we’ll help. "
    "Thank you! ✍️😊"
)
COMMAND_MESSAGES = {
    "lt": "📄 Please send us the Lease Termination to proceed with removal. This is required.",
    "apdinfo": (
        "📝 Please send the following details to Pavel@myaisagency.com:\n"
        "- Corporation name\n"
        "- Phone number\n"
        "- Email address\n"
        "- CDLs\n"
        "- Truck VINs with values\n\n"
        "✅ Kindly include everything in one email."
    ),
    "mvr": (
        "📋 Please send us MVRs for the drivers you'd like to add to the policy.\n\n"
        "If you’d like us to order the MVR:\n"
        "🛠️ Send all necessary driver info\n"
        "💵 Note: $30 fee applies per MVR\n"
        "🧾 PA drivers must include the last 4 digits of their SSN"
    ),
    "sign": SIGN_MESSAGE,
    "emails": EMAILS_MESSAGE,
}

# ---------------- State ----------------
chat_last_response: Dict[str, Dict[str, str]] = {}
TRANSCRIPT_MAX_MESSAGES = 5
chat_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=TRANSCRIPT_MAX_MESSAGES))
known_group_chats: Dict[str, Dict[str, Any]] = {}
team_user_ids: set[int] = set(PREAUTHORIZED_USER_IDS)
LAST_CUSTOMER_MESSAGE_AT: Dict[str, datetime] = {}
LAST_AUTH_REPLY_AT: Dict[str, datetime] = {}
PENDING_REMINDER_TASKS: Dict[str, asyncio.Task] = {}
LAST_CHAT_ACTIVITY: Dict[str, str] = {}

# ---------------- Helpers ----------------
def now_in_timezone():
    return datetime.now(TIMEZONE)
def is_weekend():
    return now_in_timezone().weekday() >= 5
def is_office_open():
    now = now_in_timezone()
    if is_weekend():
        return False, False
    t = now.time()
    open_ = WEEKDAY_START <= t <= WEEKDAY_END
    before_cutoff = t <= WEEKDAY_CUTOFF
    return open_, before_cutoff
def is_lunch_time():
    t = now_in_timezone().time()
    return LUNCH_START <= t <= LUNCH_END
def is_authorized_user(user_id: int) -> bool:
    return (user_id in team_user_ids) or (user_id in PREAUTHORIZED_USER_IDS)
def maybe_record_team_member(update: Update):
    chat_id = str(update.effective_chat.id)
    user = update.effective_user
    if chat_id in AIS_TEAM_CHAT_IDS and user:
        if user.id not in team_user_ids:
            logger.info(f"[AIS TEAM] New authorized member from {chat_id}: {user.full_name} (ID: {user.id})")
        team_user_ids.add(user.id)
def record_message_for_transcript(update: Update):
    chat = update.effective_chat
    msg = update.effective_message
    if not chat or not msg:
        return
    txt = msg.text or msg.caption
    if not txt:
        return
    chat_id = str(chat.id)
    entry = {
        "ts": datetime.fromtimestamp(msg.date.timestamp(), tz=TIMEZONE).strftime("%Y-%m-%d %I:%M %p"),
        "name": (update.effective_user.full_name or update.effective_user.username or str(update.effective_user.id))[:80],
        "text": txt.strip()
    }
    chat_buffers[chat_id].append(entry)
def is_simple_hello(text: str) -> bool:
    if not text:
        return False
    t = text.strip().lower()
    return t in {"hi", "hello", "hey", "yo", "good morning", "good evening", "good afternoon"}

# ---- Gmail API ----
def _gmail_credentials() -> Credentials:
    creds = Credentials(
        token=None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GMAIL_CLIENT_ID,
        client_secret=GMAIL_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/gmail.send"],
    )
    creds.refresh(GARequest())
    return creds
async def send_email_async(subject: str, body: str, to_addr: Optional[str] = None,
                           attach_name: Optional[str] = None, attach_bytes: Optional[bytes] = None):
    def _send():
        try:
            service = build("gmail", "v1", credentials=_gmail_credentials(), cache_discovery=False)
            msg = EmailMessage()
            msg["From"] = GMAIL_SENDER
            msg["To"] = to_addr or EMAIL_DEFAULT_TO
            msg["Subject"] = subject
            msg.set_content(body)
            if attach_bytes and attach_name:
                msg.add_attachment(attach_bytes, maintype="image", subtype="png", filename=attach_name)
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            return True, None
        except HttpError as he:
            logger.exception("Gmail API error")
            return False, f"Gmail API error: {he}"
        except Exception as e:
            logger.exception("Gmail send failed")
            return False, str(e)
    return await asyncio.to_thread(_send)

# ---- Transcript Rendering ----
def render_transcript_image(chat_id: str) -> Optional[bytes]:
    if not PIL_OK:
        return None
    msgs = list(chat_buffers.get(chat_id, []))
    if not msgs:
        return None
    width = 800
    padding = 20
    try:
        font = ImageFont.truetype("arial.ttf", 20)
    except Exception:
        font = ImageFont.load_default()
    lines = []
    for m in msgs:
        lines.append(f"[{m['ts']}] {m['name']}: {m['text']}")
    text = "\n".join(lines)
    wrapper = textwrap.TextWrapper(width=90)
    wrapped = "\n".join(wrapper.wrap(text))
    lines_wrapped = wrapped.split("\n")
    img_height = padding * 2 + len(lines_wrapped) * 24
    img = Image.new("RGB", (width, img_height), color="white")
    draw = ImageDraw.Draw(img)
    y = padding
    for line in lines_wrapped:
        draw.text((padding, y), line, fill="black", font=font)
        y += 24
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

# ---- Reminder Logic ----
async def schedule_reminder(chat_id: str):
    if chat_id in PENDING_REMINDER_TASKS:
        return
    async def _reminder():
        try:
            await asyncio.sleep(900)  # 15 minutes
            today_str = now_in_timezone().strftime("%Y-%m-%d")
            if (chat_id in LAST_CUSTOMER_MESSAGE_AT and
                LAST_CUSTOMER_MESSAGE_AT[chat_id].strftime("%Y-%m-%d") == today_str and
                (chat_id not in LAST_AUTH_REPLY_AT or LAST_AUTH_REPLY_AT[chat_id] < LAST_CUSTOMER_MESSAGE_AT[chat_id])):
                transcript_img = render_transcript_image(chat_id)
                sent, err = await send_email_async(
                    subject=f"[Telegram] Unanswered chat reminder {chat_id}",
                    body=f"Unanswered message in chat {chat_id}",
                    to_addr=EMAIL_ENDORSEMENT,
                    attach_name="transcript.png" if transcript_img else None,
                    attach_bytes=transcript_img
                )
                if not sent:
                    logger.error(f"Failed to send reminder email: {err}")
        finally:
            PENDING_REMINDER_TASKS.pop(chat_id, None)
    task = asyncio.create_task(_reminder())
    PENDING_REMINDER_TASKS[chat_id] = task

# ---- Command Handlers ----
async def generic_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    maybe_record_team_member(update)
    record_message_for_transcript(update)
    chat_id = str(update.effective_chat.id)
    cmd = update.message.text.lstrip("/").split()[0].lower()
    if cmd in COMMAND_MESSAGES:
        await update.message.reply_text(COMMAND_MESSAGES[cmd])
    LAST_AUTH_REPLY_AT[chat_id] = now_in_timezone()

async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    maybe_record_team_member(update)
    record_message_for_transcript(update)
    await update.message.reply_text(RULES_MESSAGE, parse_mode="Markdown")
    LAST_AUTH_REPLY_AT[str(update.effective_chat.id)] = now_in_timezone()

async def transcript_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    transcript_img = render_transcript_image(chat_id)
    if transcript_img:
        await update.message.reply_photo(photo=transcript_img)
    else:
        msgs = chat_buffers.get(chat_id)
        if msgs:
            text = "\n".join(f"[{m['ts']}] {m['name']}: {m['text']}" for m in msgs)
            await update.message.reply_text(text)
        else:
            await update.message.reply_text("No transcript available.")

# ---- Message Handler ----
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    chat_id = str(update.effective_chat.id)
    user_id = update.effective_user.id
    text = update.message.text or ""

    maybe_record_team_member(update)
    record_message_for_transcript(update)

    # Track daily activity for last call targeting
    LAST_CHAT_ACTIVITY[chat_id] = now_in_timezone().strftime("%Y-%m-%d")

    # If in silent group (command-only)
    if chat_id in SILENT_GROUP_IDS and not update.message.text.startswith("/"):
        return

    if is_authorized_user(user_id):
        LAST_AUTH_REPLY_AT[chat_id] = now_in_timezone()
        return

    # Non-authorized user logic
    LAST_CUSTOMER_MESSAGE_AT[chat_id] = now_in_timezone()
    await schedule_reminder(chat_id)

    if is_simple_hello(text) and is_weekend():
        await update.message.reply_text(WEEKEND_MESSAGE)
        return

    open_, before_cutoff = is_office_open()
    if not open_:
        if is_weekend():
            await update.message.reply_text(WEEKEND_MESSAGE)
            return
        else:
            await update.message.reply_text(CLOSED_MESSAGE)
            return

    if is_lunch_time():
        await update.message.reply_text(LUNCH_MESSAGE)
        return

    if not before_cutoff:
        await update.message.reply_text(AFTER_CUTOFF_MESSAGE)
        return

    if "coi" in text.lower() or "certificate" in text.lower():
        await update.message.reply_text(COI_REMINDER)
        return

# ---- Scheduler ----
async def last_call_scheduler(app):
    while True:
        now = now_in_timezone()
        try:
            if now.weekday() < 5 and now.time().hour == 16 and now.time().minute == 0:
                today_str = now.strftime("%Y-%m-%d")
                targets = [cid for cid, d in LAST_CHAT_ACTIVITY.items()
                           if d == today_str and cid not in SILENT_GROUP_IDS]
                for chat_id in targets:
                    try:
                        await app.bot.send_message(chat_id=int(chat_id), text=LAST_CALL_MESSAGE, parse_mode="Markdown")
                    except Exception as e:
                        logger.error(f"Failed to send last call to {chat_id}: {e}")
            await asyncio.sleep(60)
        except Exception as e:
            logger.exception("last_call_scheduler loop error")
            await asyncio.sleep(60)

# ---- Main ----
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler(["lt", "apdinfo", "mvr", "sign", "emails"], generic_command_handler))
    app.add_handler(CommandHandler("rules", rules_command))
    app.add_handler(CommandHandler("transcript", transcript_command))
    app.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), message_handler))
    app.job_queue.run_repeating(lambda ctx: asyncio.create_task(last_call_scheduler(app)), interval=60, first=0)
    app.run_polling()

if __name__ == "__main__":
    main()