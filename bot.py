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

# Env vars (NO SECRETS HARDCODED)
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
    "3. Do not reply to old posts — Kindly resend the request instead\n"
    "4. Don’t send photos of VINs — type Year, Make, and VIN\n"
    "_Use this format for policy changes:_\n"
    "• Remove VIN: 4V4NC9TH5KN216424\n"
    "• Add VIN: 1FUJHHDR3LLLH8454\n"
    "• Remove driver: Phillip Moore\n"
    "• Add driver: RUBENS ESTIME\n"
    "5. If your policy requires MVR, attach it. If not, we’ll order one and charge $30\n"
    "6. Send CDL with driver’s name clearly\n"
    "7. We don’t work weekends — resend requests on Monday\n"
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

# New /check command message
CHECK_MESSAGE = (
    "📬 Please check your email — we’ve sent your documents for **e-signature**.\n"
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
    "sign": "✍️ We sent you documents to sign via email. Please complete them ASAP!",
    "emails": EMAILS_MESSAGE,
    "check": CHECK_MESSAGE,  # <-- new command
}

# ---------------- State ----------------
chat_last_response: Dict[str, Dict[str, str]] = {}
TRANSCRIPT_MAX_MESSAGES = 5
chat_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=TRANSCRIPT_MAX_MESSAGES))
known_group_chats: Dict[str, Dict[str, Any]] = {}

# Authorized users (seen in AIS team chats) + preloaded env IDs
team_user_ids: set[int] = set(PREAUTHORIZED_USER_IDS)

# For 15-minute reminder
LAST_CUSTOMER_MESSAGE_AT: Dict[str, datetime] = {}
LAST_AUTH_REPLY_AT: Dict[str, datetime] = {}
PENDING_REMINDER_TASKS: Dict[str, asyncio.Task] = {}

# Track chats that had activity today (key: chat_id, value: YYYY-MM-DD)
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
    if not (GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET and GMAIL_REFRESH_TOKEN and GMAIL_SENDER):
        raise RuntimeError("Missing Gmail OAuth vars")
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

# ---- Transcript rendering ----
def render_transcript_image(chat_title: str, entries: deque) -> Optional[bytes]:
    if not entries or not PIL_OK:
        return None

    width = 1000
    margin = 40
    line_spacing = 8
    title_size = 36
    text_size = 24
    bg = (255, 255, 255)
    title_color = (20, 20, 20)
    meta_color = (90, 90, 90)
    text_color = (0, 0, 0)

    try:
        font_title = ImageFont.truetype("arial.ttf", title_size)
        font_meta = ImageFont.truetype("arial.ttf", text_size)
        font_text = ImageFont.truetype("arial.ttf", text_size)
    except Exception:
        font_title = ImageFont.load_default()
        font_meta = ImageFont.load_default()
        font_text = ImageFont.load_default()

    lines = [("title", f"Chat: {chat_title or '(untitled)'}")]
    for e in entries:
        lines.append(("meta", f"[{e['ts']}] {e['name']}:"))
        for w in (textwrap.wrap(e["text"], width=70) or [""]):
            lines.append(("text", w))
        lines.append(("spacer", ""))

    def text_h(draw, content, font):
        bbox = draw.textbbox((0, 0), content, font=font)
        return bbox[3] - bbox[1]

    img_tmp = Image.new("RGB", (width, 10), bg)
    dtmp = ImageDraw.Draw(img_tmp)
    y = margin
    for t, content in lines:
        if t == "title":
            h = text_h(dtmp, content, font_title)
        elif t == "meta":
            h = text_h(dtmp, content, font_meta)
        elif t == "text":
            h = text_h(dtmp, content, font_text)
        else:
            h = max(4, text_size // 2)
        y += h + line_spacing
    height = y + margin

    img = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(img)
    y = margin
    for t, content in lines:
        if t == "title":
            draw.text((margin, y), content, font=font_title, fill=title_color)
            h = text_h(draw, content, font_title)
        elif t == "meta":
            draw.text((margin, y), content, font=font_meta, fill=meta_color)
            h = text_h(draw, content, font_meta)
        elif t == "text":
            draw.text((margin, y), content, font=font_text, fill=text_color)
            h = text_h(draw, content, font_text)
        else:
            h = max(4, text_size // 2)
        y += h + line_spacing

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()

# ---------------- Authorization + cooldown helpers (auto-spiels still use cooldown) ----------------
def require_authorized(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        maybe_record_team_member(update)
        user = update.effective_user
        if not user or not is_authorized_user(user.id):
            await update.message.reply_text("Not authorized.")
            return
        return await func(update, context)
    return wrapper

def already_sent(chat_id: str, tag: str, window_sec: int = 3600) -> bool:
    last = chat_last_response.get(chat_id, {})
    when = last.get(tag)
    if not when:
        return False
    try:
        delta = now_in_timezone() - datetime.fromisoformat(when)
        return delta.total_seconds() < window_sec
    except Exception:
        return False

def mark_sent(chat_id: str, tag: str):
    chat_last_response.setdefault(chat_id, {})[tag] = now_in_timezone().isoformat()

# --- 15-minute reminder helpers ---
def mark_customer_activity(chat_id: str):
    LAST_CUSTOMER_MESSAGE_AT[chat_id] = now_in_timezone()

def mark_authorized_reply(chat_id: str):
    LAST_AUTH_REPLY_AT[chat_id] = now_in_timezone()

def schedule_no_reply_reminder(chat_id: str, app_context: ContextTypes.DEFAULT_TYPE):
    task = PENDING_REMINDER_TASKS.get(chat_id)
    if task and not task.done():
        task.cancel()

    async def reminder_job():
        try:
            await asyncio.sleep(15 * 60)
            last_customer = LAST_CUSTOMER_MESSAGE_AT.get(chat_id)
            last_auth = LAST_AUTH_REPLY_AT.get(chat_id)
            if last_customer and (not last_auth or last_auth < last_customer):
                subject = f"[No Reply 15m] Chat {chat_id}"
                body = (
                    f"No authorized reply in chat {chat_id} for 15 minutes after a customer message.\n"
                    f"Time (local): {now_in_timezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
                )
                entries = list(chat_buffers.get(chat_id, []))
                png_bytes = render_transcript_image("", entries) if entries else None
                await send_email_async(
                    subject=subject,
                    body=body if not png_bytes else (body + "\n(Transcript image attached.)"),
                    to_addr=EMAIL_ENDORSEMENT,
                    attach_name=f"no_reply_{chat_id}.png" if png_bytes else None,
                    attach_bytes=png_bytes
                )
        except asyncio.CancelledError:
            pass
        finally:
            PENDING_REMINDER_TASKS.pop(chat_id, None)

    PENDING_REMINDER_TASKS[chat_id] = asyncio.create_task(reminder_job())

# ---------------- Commands (authorized-only) — NO COOLDOWN ----------------
@require_authorized
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hello! I'm your agency assistant bot.\nType /help to see available commands.")

@require_authorized
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Available commands (AIS TEAM only):\n"
        "/start – Welcome\n"
        "/help – This list\n"
        "/myid – Your chat ID\n"
        "/rules – Send & pin rules\n"
        "/lt /apdinfo /mvr /sign /emails – Quick replies\n"
        "/check – Ask client to check email & sign\n"
        "/ssinfo – Email transcript to info@\n"
        "/ssendo – Email transcript to endorsements@"
    )

@require_authorized
async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 Your chat ID is: `{update.effective_chat.id}`", parse_mode="Markdown")

@require_authorized
async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # No cooldown: always post & attempt to pin
    sent = await update.message.reply_text(RULES_MESSAGE, parse_mode="Markdown")
    try:
        await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent.message_id, disable_notification=True)
    except Exception as e:
        logger.warning(f"Unable to pin rules in chat {update.effective_chat.id}: {e}")

@require_authorized
async def generic_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    record_message_for_transcript(update)
    chat_id = str(update.effective_chat.id)
    cmd = (update.message.text or "").split()[0].lstrip("/").lower()

    # Commands are allowed everywhere (including SILENT_GROUP_IDS) — NO cooldown
    if cmd in COMMAND_MESSAGES:
        await update.message.reply_text(COMMAND_MESSAGES[cmd])

    # Authorized command counts as an authorized reply → cancel 15-min timer if any
    mark_authorized_reply(chat_id)
    task = PENDING_REMINDER_TASKS.pop(chat_id, None)
    if task and not task.done():
        task.cancel()

async def _send_transcript_email(update: Update, to_addr: str):
    chat = update.effective_chat
    chat_id = str(chat.id)
    entries = list(chat_buffers.get(chat_id, []))
    if not entries:
        await update.message.reply_text("No recent text messages to capture for this chat.")
        return
    await update.message.reply_text("⏳ Preparing transcript…")

    png_bytes = render_transcript_image(chat.title or "", entries)
    ts = now_in_timezone().strftime("%Y%m%d-%H%M%S")
    if png_bytes:
        ok, err = await send_email_async(
            subject=f"[Telegram] Transcript – {chat.title or ''} ({chat_id})",
            body=f"Attached is the transcript image of the last {len(entries)} message(s).",
            to_addr=to_addr,
            attach_name=f"telegram_transcript_{chat_id}_{ts}.png",
            attach_bytes=png_bytes,
        )
    else:
        body = "\n".join([f"[{e['ts']}] {e['name']}: {e['text']}" for e in entries])
        ok, err = await send_email_async(
            subject=f"[Telegram] Transcript (text) – {chat.title or ''} ({chat_id})",
            body=body,
            to_addr=to_addr,
        )
    if ok:
        await update.message.reply_text(f"✅ Transcript sent to {to_addr}")
    else:
        await update.message.reply_text(f"❌ Failed to send email to {to_addr}: {err or 'Unknown error'}")

@require_authorized
async def ssinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_DEFAULT_TO)

@require_authorized
async def ssendo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_ENDORSEMENT)

# ---------------- Message handler (auto spiels) ----------------
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    chat_id = str(chat.id)
    text_raw = update.message.text or ""
    text = text_raw.lower()
    now = now_in_timezone()

    # Track group + transcript + (maybe) authorize AIS members
    if chat.type in (Chat.GROUP, Chat.SUPERGROUP):
        if chat_id not in known_group_chats:
            known_group_chats[chat_id] = {"title": chat.title or "", "added_on": now.isoformat()}
            logger.info(f"Saved new group: {chat_id}")
    maybe_record_team_member(update)
    record_message_for_transcript(update)

    # Mark this chat as active today
    LAST_CHAT_ACTIVITY[chat_id] = now.strftime("%Y-%m-%d")

    is_auth = bool(user and is_authorized_user(user.id))
    is_silent_chat = chat_id in SILENT_GROUP_IDS

    # --- COMMAND-ONLY MODE FOR AUTHORIZED GROUPS ---
    if is_silent_chat:
        # Ignore all regular messages in these chats (commands handled by command handlers)
        return

    # --- DYNAMIC SILENCE for non-silent chats: authorized normal messages don't trigger spiels ---
    if is_auth:
        mark_authorized_reply(chat_id)
        task = PENDING_REMINDER_TASKS.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
        return  # no auto-spiels to authorized talk in non-silent chats

    # From here, sender is non-authorized in a non-silent chat.
    async def send_once(tag: str, msg: str, md: bool = False):
        if not already_sent(chat_id, tag):
            if md:
                await update.message.reply_text(msg, parse_mode="Markdown")
            else:
                await update.message.reply_text(msg)
            mark_sent(chat_id, tag)

    # COI keywords
    if "coi" in text or "certificate" in text:
        await send_once("coi", COI_REMINDER)
        mark_customer_activity(chat_id)
        schedule_no_reply_reminder(chat_id, context)
        return

    # Weekend: reply only to simple greetings
    if is_weekend():
        if is_simple_hello(text_raw):
            await send_once("weekend_hello", WEEKEND_MESSAGE)
        mark_customer_activity(chat_id)
        schedule_no_reply_reminder(chat_id, context)
        return

    # Lunch
    if is_lunch_time():
        await send_once("lunch", LUNCH_MESSAGE)
        mark_customer_activity(chat_id)
        schedule_no_reply_reminder(chat_id, context)
        return

    # Business hours
    open_, before_cutoff = is_office_open()
    if not open_:
        await send_once("closed", CLOSED_MESSAGE)
        mark_customer_activity(chat_id)
        schedule_no_reply_reminder(chat_id, context)
        return
    if not before_cutoff:
        await send_once("cutoff", AFTER_CUTOFF_MESSAGE)
        mark_customer_activity(chat_id)
        schedule_no_reply_reminder(chat_id, context)
        return

    # Normal ack
    await send_once("normal", "✅ Message received. We’ll take care of it shortly!")
    mark_customer_activity(chat_id)
    schedule_no_reply_reminder(chat_id, context)

# ---------------- Scheduler: 4:00 PM CT last call (weekdays, only chats active today) ----------------
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

# ---------------- Main ----------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)

async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN not set"); return
    # Optional: simple env presence log (no secrets)
    missing = [k for k in ("GMAIL_CLIENT_ID","GMAIL_CLIENT_SECRET","GMAIL_REFRESH_TOKEN","GMAIL_SENDER") if not os.getenv(k)]
    if missing:
        logger.warning(f"Gmail env missing: {missing}")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("Rules", rules_command))
    app.add_handler(CommandHandler("rules", rules_command))
    app.add_handler(CommandHandler(["lt", "apdinfo", "mvr", "sign", "emails", "check"], generic_command_handler))
    app.add_handler(CommandHandler("ssinfo", ssinfo_command))
    app.add_handler(CommandHandler("ssendo", ssendo_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    app.add_error_handler(on_error)
    asyncio.create_task(last_call_scheduler(app))
    logger.info("✅ Bot running: command-only authorized groups, dynamic silent mode, no command cooldown, /check added, 15-min endorsements reminder, and 'Last Call' only to active chats.")
    await app.run_polling()

if __name__ == "__main__":
    try:
        import nest_asyncio; nest_asyncio.apply()
    except Exception:
        pass
    asyncio.run(main())