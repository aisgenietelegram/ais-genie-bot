import os
import re
import logging
import asyncio
from datetime import datetime, time, timedelta
from collections import deque, defaultdict
from typing import Dict, Any, Optional, Set, Tuple, List
import pytz
import io
import textwrap
import socket
import base64

import httpx  # SendGrid HTTPS API

from telegram import Update, Chat
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

# Optional image rendering for transcript (falls back to text if Pillow missing)
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_OK = True
except Exception:
    PIL_OK = False

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
WEEKDAY_CUTOFF = time(16, 30)  # 4:30 PM
LAST_CALL_TIME = time(16, 0)   # 4:00 PM
LUNCH_START = time(12, 30)
LUNCH_END = time(13, 30)

# Env vars (NO SECRETS HARDCODED)
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Email config (SendGrid)
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")  # <<< set this in Railway
FROM_EMAIL = os.getenv("FROM_EMAIL", os.getenv("GMAIL_SENDER", "aisgenie.telegram@gmail.com"))

EMAIL_DEFAULT_TO = os.getenv("EMAIL_TO", "info@myaisagency.com")
EMAIL_ENDORSEMENT = os.getenv("EMAIL_ENDORSEMENT", "endorsements@myaisagency.com")
EMAIL_COI = os.getenv("EMAIL_COI", "coi@myaisagency.com")

# Optional simple same-host conflict guard
CONFLICT_GUARD_PORT = int(os.getenv("CONFLICT_GUARD_PORT", "37219"))

# ---------------- Messages ----------------
CLOSED_MESSAGE_AM = (
    "🌅 Good morning!\n"
    "Our office opens at 9:00 AM CT.\n\n"
    "📌 Please resend your request after we open so we can handle it promptly.\n"
    "Thank you for your patience! 🙏"
    "📌 Requests sent after hours will be handled on the next business day.\n"
    "Thank you for your understanding! 🙏"
)

CLOSED_MESSAGE_PM = (
    "🌙 Our office is now closed for the day.\n"
    "We’ll be back tomorrow at 9:00 AM CT.\n\n"
    "📌 Please resend your request after we open so we can handle it promptly.\n"
    "📌 Requests sent after hours will be handled on the next business day.\n"
    "Thank you for your understanding! 🙏"
)

AFTER_CUTOFF_MESSAGE = (
    "⚠️ Sorry, your **Request** was received outside the cut-off period.\n\n"
    "It will be processed on the next business day. Thank you for your understanding!"
)

WEEKEND_MESSAGE = (
    "🌙 We’re closed on weekends (Sat–Sun).\n"
    "We’ll be back Monday at 9:00 AM CT.\n"
    "Please resend your request then so we don’t miss it. Thank you! 🙏"
)

COI_TEXT = (
    "📩 *Certificate of Insurance (COI) / Certificates*\n\n"
    f"Please email: **{EMAIL_COI or 'coi@myaisagency.com'}**\n\n"
    "Kindly include:\n"
    "• COI holder’s name\n"
    "• Complete mailing address\n"
    "• Any special wording or instructions\n"
    "• The email address where we should send the certificate\n"
)

RULES_MESSAGE = (
    "📜 *Advanced Insurance Solutions — Chat Guidelines*\n\n"
    "⚠️ *If a change is not confirmed by email, it did not happen.*\n\n"
    "*How to Use This Chat*\n"
    "• Telegram is for quick communication. Policy changes must be confirmed via email.\n"
    "• Please avoid editing old posts. Start a new message for new requests.\n\n"
    "*COI (Certificates)*\n"
    f"• Email: {EMAIL_COI or 'coi@myaisagency.com'}\n"
    "• Include: holder name, full mailing address, any special wording, and the delivery email.\n\n"
    "*Driver & Vehicle Changes*\n"
    "• Type VINs (no photos). Example:\n"
    "  – Remove VIN: 4V4NC9TH5KN216424\n"
    "  – Add VIN: 1FUJHHDR3LLLH8454\n"
    "  – Remove driver: Phillip Moore\n"
    "  – Add driver: Rubens Estime\n"
    "• If MVRs are required, attach them. If we order them, a $30 fee applies per MVR.\n"
    "• Send CDL with driver’s name clearly visible.\n"
    "• Physical Damage coverage is not automatically added.\n\n"
    "*Timing*\n"
    "• We accept changes Mon–Fri, 9:00 AM – 4:30 PM (Last Call 4:00 PM).\n"
    "• We do not work weekends. Please resend your request Monday.\n\n"
    "✅ *No change is valid unless confirmed by email.*"
)

LAST_CALL_MESSAGE = (
    "📢 *Last Call – 4:30 PM Cut-off*\n\n"
    "Please send any remaining requests before **4:30 PM CT**.\n"
    "After that, they’ll be handled the next business day.\n\n"
    "📌 Please resend your request after we open so we can handle it promptly.\n"
    "🔔 *Starting September 1:* This will be strictly enforced. Thank you! 🙏"
    "📌 Requests sent after hours will be handled on the next business day.\n"
    "Thank you for your understanding! 🙏"
)

LUNCH_MESSAGE = (
    "🍽️ Our team is currently on lunch break (12:30 PM – 1:30 PM CT).\n\n"
    "We’ll respond once we’re back."
)

EMAILS_MESSAGE = (
    "📧 *PLEASE USE THE FOLLOWING EMAILS TO GET YOUR REQUEST PROCESSED ASAP:*\n\n"
    f"• {EMAIL_COI or 'coi@myaisagency.com'} – COI / Certificates\n"
    f"• {EMAIL_DEFAULT_TO or 'info@myaisagency.com'} – General Questions / Binding\n"
    f"• {EMAIL_ENDORSEMENT or 'endorsements@myaisagency.com'} – Policy changes / quotes / driver & truck list\n"
    "• claims@myaisagency.com – Claims"
)

COMMAND_MESSAGES = {
    "lt": "📄 Please send us the Lease Termination to proceed with removal. This is required.",
    "apd": (
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
    "sign": (
        "📬 Please check your email — we’ve sent your documents for **e-signature**.\n"
        "Kindly review and sign at your earliest convenience. If you have any questions, reply here and we’ll help. "
        "Thank you! ✍️😊"
    ),
    "emails": EMAILS_MESSAGE,
}

# ---------------- State ----------------
chat_last_response: Dict[str, Dict[str, str]] = {}
TRANSCRIPT_MAX_MESSAGES = 5
chat_buffers: Dict[str, deque] = defaultdict(lambda: deque(maxlen=TRANSCRIPT_MAX_MESSAGES))
known_group_chats: Dict[str, Dict[str, Any]] = {}

# Authorized users (seen in AIS team chats) + preloaded env IDs
team_user_ids: set[int] = set(PREAUTHORIZED_USER_IDS)

# Track chats that had activity today (key: chat_id, value: YYYY-MM-DD)
LAST_CHAT_ACTIVITY: Dict[str, str] = {}

# Once-per-day closed messages, tracked separately for AM(before shift) and PM(after shift)
CLOSED_SENT_TODAY_AM: Dict[str, str] = {}  # group chat_id -> YYYY-MM-DD
CLOSED_SENT_TODAY_PM: Dict[str, str] = {}  # group chat_id -> YYYY-MM-DD

# Track last authorized message timestamp per chat (CT)
LAST_AUTH_MSG_AT: Dict[str, datetime] = {}

# Flood buffer inactivity window
FLOOD_BUFFER_SECONDS = 5 * 60  # 5 minutes

# After-hours suppression window when an authorized user posts (2h), and 1h threshold to allow spiel if non-auth messages
AFTER_HOURS_SUPPRESSION_WINDOW_HOURS = 2
AFTER_HOURS_MIN_SILENCE_FOR_SPIEL_HOURS = 1

# --------------- Debounce tokens & task registry ---------------
# We key by (chat_id, period) where period in {"AM","PM","WE","LUNCH","CUTOFF"}
DEBOUNCE_TOKEN: Dict[Tuple[str, str], str] = {}
PENDING_TASK: Dict[Tuple[str, str], asyncio.Task] = {}

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
        "text": (txt or "").strip()
    }
    chat_buffers[chat_id].append(entry)

# ---- Email via SendGrid HTTPS API ----
def _send_email_sendgrid(subject: str, body: str, to_addr: str,
                         attach_name: Optional[str] = None, attach_bytes: Optional[bytes] = None) -> tuple[bool, Optional[str]]:
    """
    Sends email via SendGrid API (HTTPS).
    Returns (ok, error_message_if_any).
    """
    api_key = SENDGRID_API_KEY
    if not api_key:
        return False, "Missing SENDGRID_API_KEY"
    from_email = FROM_EMAIL
    if not from_email:
        return False, "Missing FROM_EMAIL"

    data = {
        "personalizations": [{"to": [{"email": to_addr}]}],
        "from": {"email": from_email},
        "subject": subject or "",
        "content": [{"type": "text/plain", "value": body or ""}],
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    try:
        r = httpx.post("https://api.sendgrid.com/v3/mail/send", headers=headers, json=data, timeout=30.0)
        if r.status_code == 202:
            return True, None
        return False, f"SendGrid error {r.status_code}: {r.text}"
    except Exception as e:
        logger.exception("SendGrid send failed")
        return False, str(e)

async def send_email_async(subject: str, body: str, to_addr: Optional[str] = None,
                           attach_name: Optional[str] = None, attach_bytes: Optional[bytes] = None):
    """
    Async wrapper to send via SendGrid. Keeps the same signature used elsewhere.
    """
    def _send():
        return _send_email_sendgrid(
            subject=subject,
            body=body,
            to_addr=to_addr or EMAIL_DEFAULT_TO,
            attach_name=attach_name,
            attach_bytes=attach_bytes
        )
    return await asyncio.to_thread(_send)

# ---- Transcript rendering (kept for future image use; now we send text emails) ----
def render_transcript_image(chat_title: str, entries: deque) -> Optional[bytes]:
    return None  # explicitly disabled; we now email plain text

# ---------------- Debounce helpers ----------------
def _period_key(chat_id: str, period: str) -> Tuple[str, str]:
    return (chat_id, period)

def _new_token() -> str:
    return now_in_timezone().isoformat()

def _set_debounce(chat_id: str, period: str) -> str:
    key = _period_key(chat_id, period)
    token = _new_token()
    DEBOUNCE_TOKEN[key] = token
    task = PENDING_TASK.pop(key, None)
    if task and not task.done():
        task.cancel()
    return token

def _is_latest_token(chat_id: str, period: str, token: str) -> bool:
    return DEBOUNCE_TOKEN.get(_period_key(chat_id, period)) == token

def _clear_debounce(chat_id: str, period: str):
    key = _period_key(chat_id, period)
    PENDING_TASK.pop(key, None)

def cancel_all_pending_for_chat(chat_id: str):
    """Cancel ALL pending buffered prompts for this chat (AM/PM/WE/LUNCH/CUTOFF)."""
    for period in ("AM", "PM", "WE", "LUNCH", "CUTOFF"):
        key = _period_key(chat_id, period)
        task = PENDING_TASK.pop(key, None)
        if task and not task.done():
            task.cancel()
        DEBOUNCE_TOKEN[key] = _new_token()

# ---------------- Authorization + cooldown helpers ----------------
def require_authorized(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        maybe_record_team_member(update)
        user = update.effective_user
        if not user or not is_authorized_user(user.id):
            await update.message.reply_text("Not authorized.")
            return
        return await func(update, context)
    return wrapper

def already_sent(chat_id: str, tag: str, window_sec: int = 7200) -> bool:  # 2-hour cooldown per group
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

def set_last_auth_msg(chat_id: str):
    LAST_AUTH_MSG_AT[chat_id] = now_in_timezone()

def last_auth_msg_age(chat_id: str) -> Optional[timedelta]:
    ts = LAST_AUTH_MSG_AT.get(chat_id)
    if not ts:
        return None
    return now_in_timezone() - ts

def within_after_hours_suppression(chat_id: str) -> bool:
    """Outside business hours (incl. weekends) AND within 2h since last authorized message in this chat."""
    open_, _ = is_office_open()
    if open_:
        return False
    age = last_auth_msg_age(chat_id)
    if age is None:
        return False
    return age <= timedelta(hours=AFTER_HOURS_SUPPRESSION_WINDOW_HOURS)

def allow_after_hours_spiel(chat_id: str) -> bool:
    """Outside hours: if within 2h suppression, allow only after 1h silence; else allow."""
    open_, _ = is_office_open()
    if open_:
        return False
    age = last_auth_msg_age(chat_id)
    if age is None:
        return True
    if age >= timedelta(hours=AFTER_HOURS_SUPPRESSION_WINDOW_HOURS):
        return True
    return age >= timedelta(hours=AFTER_HOURS_MIN_SILENCE_FOR_SPIEL_HOURS)

# --------------- Buffered scheduling for all prompts ---------------
async def _buffer_then_send(chat_id: str, period: str, token: str, context: ContextTypes.DEFAULT_TYPE):
    """period in {"AM","PM","WE","LUNCH","CUTOFF"}"""
    try:
        await asyncio.sleep(FLOOD_BUFFER_SECONDS)
        if not _is_latest_token(chat_id, period, token):
            return

        now_local = now_in_timezone()
        # Window checks & sending
        if period == "AM":
            open_, _ = is_office_open()
            if open_:
                return
            if chat_id not in known_group_chats:
                return
            if CLOSED_SENT_TODAY_AM.get(chat_id) == now_local.strftime("%Y-%m-%d"):
                return
            if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                return
            await context.bot.send_message(chat_id=int(chat_id), text=CLOSED_MESSAGE_AM)
            CLOSED_SENT_TODAY_AM[chat_id] = now_local.strftime("%Y-%m-%d")
            return

        if period == "PM":
            open_, _ = is_office_open()
            if open_:
                return
            if chat_id not in known_group_chats:
                return
            if CLOSED_SENT_TODAY_PM.get(chat_id) == now_local.strftime("%Y-%m-%d"):
                return
            if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                return
            await context.bot.send_message(chat_id=int(chat_id), text=CLOSED_MESSAGE_PM)
            CLOSED_SENT_TODAY_PM[chat_id] = now_local.strftime("%Y-%m-%d")
            return

        if period == "WE":
            if not is_weekend():
                return
            open_, _ = is_office_open()
            if open_:
                return
            if chat_id not in known_group_chats:
                return
            if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                return
            if not already_sent(chat_id, "weekend", window_sec=7200):
                await context.bot.send_message(chat_id=int(chat_id), text=WEEKEND_MESSAGE)
                mark_sent(chat_id, "weekend")
            return

        if period == "LUNCH":
            t = now_local.time()
            if not (LUNCH_START <= t <= LUNCH_END):
                return
            if chat_id not in known_group_chats:
                return
            if not already_sent(chat_id, "lunch"):
                await context.bot.send_message(chat_id=int(chat_id), text=LUNCH_MESSAGE)
                mark_sent(chat_id, "lunch")
            return

        if period == "CUTOFF":
            t = now_local.time()
            open_, before_cutoff = is_office_open()
            if not open_ or t < WEEKDAY_CUTOFF or t > WEEKDAY_END:
                return
            if chat_id not in known_group_chats:
                return
            # Suppress if an authorized user initiated on/before cutoff today
            ts = LAST_AUTH_MSG_AT.get(chat_id)
            if ts:
                ts_local = ts.astimezone(TIMEZONE)
                if ts_local.strftime("%Y-%m-%d") == now_local.strftime("%Y-%m-%d") and ts_local.time() <= WEEKDAY_CUTOFF:
                    return
            if not already_sent(chat_id, "cutoff"):
                await context.bot.send_message(chat_id=int(chat_id), text=AFTER_CUTOFF_MESSAGE, parse_mode="Markdown")
                mark_sent(chat_id, "cutoff")
            return

    except asyncio.CancelledError:
        pass
    finally:
        _clear_debounce(chat_id, period)

def schedule_buffered(chat_id: str, period: str, context: ContextTypes.DEFAULT_TYPE):
    """Create/refresh a single timer using a debounce token so only the latest fires."""
    token = _set_debounce(chat_id, period)
    key = _period_key(chat_id, period)
    PENDING_TASK[key] = asyncio.create_task(_buffer_then_send(chat_id, period, token, context))

# ---------------- Commands (authorized-only) — NO COOLDOWN ----------------
def require_and_record(func):
    @require_authorized
    async def inner(update: Update, context: ContextTypes.DEFAULT_TYPE):
        record_message_for_transcript(update)
        return await func(update, context)
    return inner

@require_and_record
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hello! I'm your agency assistant bot.\nType /help to see available commands.")

@require_and_record
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Available commands (AIS TEAM only):\n"
        "/start – Welcome\n"
        "/help – This list\n"
        "/myid – Your chat ID\n"
        "/rules – Send & pin rules\n"
        "/lt /apd /mvr /sign /emails – Quick replies\n"
        "/time /hours – Business hours\n"
        "/coi – COI instructions\n"
        "/ssi – Email transcript to info@\n"
        "/sse – Email transcript to endorsements@\n"
        "/ssc – Email transcript to coi@\n"
        "/who – List known groups (title + ID)\n"
        "/broadcast – Send one-time announcement to all groups\n"
        "/broadcastpin – Broadcast and try to pin in all groups\n"
        "/broadcastto – Targeted broadcast to specific group(s) by ID or name"
    )

@require_and_record
async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 Your chat ID is: `{update.effective_chat.id}`", parse_mode="Markdown")

@require_and_record
async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    sent = await update.message.reply_text(RULES_MESSAGE, parse_mode="Markdown")
    try:
        await context.bot.pin_chat_message(chat_id=update.effective_chat.id, message_id=sent.message_id, disable_notification=True)
    except Exception as e:
        logger.warning(f"Unable to pin rules in chat {update.effective_chat.id}: {e}")

@require_and_record
async def generic_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(update.effective_chat.id)
    cmd = (update.message.text or "").split()[0].lstrip("/").lower()
    if cmd in COMMAND_MESSAGES:
        await update.message.reply_text(COMMAND_MESSAGES[cmd], parse_mode="Markdown")
    set_last_auth_msg(chat_id)
    cancel_all_pending_for_chat(chat_id)

@require_and_record
async def time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🕒 *Business Hours (CT)*\n"
        "Mon–Fri: 9:00 AM – 5:00 PM\n"
        "Lunch: 12:30 PM – 1:30 PM\n"
        "Weekends: Closed",
        parse_mode="Markdown"
    )

@require_and_record
async def coi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(COI_TEXT, parse_mode="Markdown")

# ---------- Broadcast helpers ----------
def _find_targets_by_names_or_ids(targets_raw: str) -> Tuple[List[int], List[str]]:
    """
    Parse targets: either comma-separated IDs, or quoted names (partial match).
    Returns (chat_ids, errors).
    Examples:
      ids:    -100123,-100456
      names:  "Dispatch Room","AIS Pilots"
      mixed not supported (keep simple).
    """
    errors: List[str] = []
    chat_ids: List[int] = []

    # Look for quoted names
    names = re.findall(r'"([^"]+)"', targets_raw)
    if names:
        lowered = {cid: (meta.get("title") or "").lower() for cid, meta in known_group_chats.items()}
        for name in names:
            name_l = name.strip().lower()
            matched = [int(cid) for cid, ttl in lowered.items() if name_l in ttl]
            if not matched:
                errors.append(f'No group matched name "{name}"')
            else:
                chat_ids.extend(matched)
        # Dedup
        chat_ids = list(dict.fromkeys(chat_ids))
        return chat_ids, errors

    # Else expect IDs
    try:
        for part in targets_raw.split(","):
            p = part.strip()
            if not p:
                continue
            chat_ids.append(int(p))
        chat_ids = list(dict.fromkeys(chat_ids))
    except Exception:
        errors.append("Could not parse IDs. Use comma-separated integers or quoted names.")
    return chat_ids, errors

@require_and_record
async def who_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not known_group_chats:
        await update.message.reply_text("No known groups yet. Add me to a group and send any message to register it.")
        return
    lines = ["📋 *Known Groups:*"]
    for cid, meta in known_group_chats.items():
        title = meta.get("title") or "(untitled)"
        lines.append(f"• {title} — `{cid}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

@require_and_record
async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (update.message.text or "").split(" ", 1)
    if len(msg) < 2 or not msg[1].strip():
        await update.message.reply_text("Usage:\n/broadcast Your announcement text")
        return
    text = msg[1].strip()
    total = 0
    ok = 0
    fail = 0
    for cid in list(known_group_chats.keys()):
        try:
            await context.bot.send_message(chat_id=int(cid), text=text)
            ok += 1
        except Exception as e:
            fail += 1
            logger.error(f"/broadcast failed for chat {cid}: {e}")
        total += 1
    await update.message.reply_text(f"📣 Broadcast sent.\n✅ {ok} succeeded • ❌ {fail} failed • 📦 {total} groups total.")

@require_and_record
async def broadcastpin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (update.message.text or "").split(" ", 1)
    if len(msg) < 2 or not msg[1].strip():
        await update.message.reply_text("Usage:\n/broadcastpin Your announcement text")
        return
    text = msg[1].strip()
    total = 0
    ok = 0
    fail = 0
    for cid in list(known_group_chats.keys()):
        try:
            sent = await context.bot.send_message(chat_id=int(cid), text=text)
            try:
                await context.bot.pin_chat_message(chat_id=int(cid), message_id=sent.message_id, disable_notification=True)
            except Exception as pe:
                logger.warning(f"/broadcastpin: pin failed for {cid}: {pe}")
            ok += 1
        except Exception as e:
            fail += 1
            logger.error(f"/broadcastpin failed for chat {cid}: {e}")
        total += 1
    await update.message.reply_text(f"📌 Broadcast (pinned) done.\n✅ {ok} succeeded • ❌ {fail} failed • 📦 {total} groups total.")

@require_and_record
async def broadcastto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /broadcastto -100123,-100456 Your message here
      /broadcastto "Dispatch Room","AIS Pilots" Your message here
      /broadcastto "dispatch" Quick test 🚀
    """
    raw = (update.message.text or "")
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text(
            "Usage:\n"
            "/broadcastto -100123,-100456 Your message here\n"
            '/broadcastto "Dispatch Room","AIS Pilots" Your message here\n'
            '/broadcastto "dispatch" Quick test 🚀'
        )
        return

    arg = parts[1].strip()

    # If we have quoted names, message starts after the last closing quote
    quoted_names = re.findall(r'"([^"]+)"', arg)
    if quoted_names:
        last_quote = arg.rfind('"')
        msg_text = arg[last_quote+1:].strip().lstrip(",").strip()
        targets_str = ",".join([f'"{n}"' for n in quoted_names])
        targets_ids, errors = _find_targets_by_names_or_ids(targets_str)
    else:
        # Split first space: targets then message
        subparts = arg.split(" ", 1)
        if len(subparts) < 2:
            await update.message.reply_text("Please provide targets and a message.\nExample: /broadcastto -100123,-100456 Hello")
            return
        targets_str, msg_text = subparts[0].strip(), subparts[1].strip()
        targets_ids, errors = _find_targets_by_names_or_ids(targets_str)

    if errors:
        await update.message.reply_text("⚠️ " + " | ".join(errors))
        return
    if not targets_ids:
        await update.message.reply_text("No valid targets found.")
        return
    if not msg_text:
        await update.message.reply_text("Please provide a message to send.")
        return

    ok = 0
    fail = 0
    for cid in targets_ids:
        try:
            await context.bot.send_message(chat_id=int(cid), text=msg_text)
            ok += 1
        except Exception as e:
            fail += 1
            logger.error(f"/broadcastto failed for chat {cid}: {e}")

    await update.message.reply_text(f"🎯 Targeted broadcast sent.\n✅ {ok} succeeded • ❌ {fail} failed • 🎯 {len(targets_ids)} groups targeted.")

# ---- Transcript emailers (plain text; subject = group title only) ----
async def _send_transcript_email(update: Update, to_addr: str):
    chat = update.effective_chat
    chat_id = str(chat.id)
    entries = list(chat_buffers.get(chat_id, []))
    if not entries:
        await update.message.reply_text("No recent text messages to capture for this chat.")
        return
    await update.message.reply_text("⏳ Preparing transcript…")

    chat_title = known_group_chats.get(chat_id, {}).get("title") or (chat.title or "")
    subject = chat_title  # subject = group name only
    body_lines = []
    for e in entries:
        body_lines.append(f"[{e['ts']}] {e['name']}: {e['text']}")
    body = "\n".join(body_lines)

    ok, err = await send_email_async(
        subject=subject,
        body=body,
        to_addr=to_addr,
        attach_name=None,
        attach_bytes=None,
    )
    if ok:
        await update.message.reply_text(f"✅ Transcript sent to {to_addr}")
    else:
        await update.message.reply_text(f"❌ Failed to send email to {to_addr}: {err or 'Unknown error'}")

@require_and_record
async def ssi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_DEFAULT_TO)

@require_and_record
async def sse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_ENDORSEMENT)

@require_and_record
async def ssc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_COI)

# ---------------- Message handler (auto spiels) ----------------
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    chat_id = str(chat.id)
    now = now_in_timezone()

    # Track groups + transcript + (maybe) authorize AIS members
    if chat.type in (Chat.GROUP, Chat.SUPERGROUP):
        if chat_id not in known_group_chats:
            known_group_chats[chat_id] = {"title": chat.title or "", "added_on": now.isoformat()}
            logger.info(f"Saved new group: {chat_id}")
    maybe_record_team_member(update)
    record_message_for_transcript(update)

    # Mark this chat as active today (any chat)
    LAST_CHAT_ACTIVITY[chat_id] = now.strftime("%Y-%m-%d")

    is_auth = bool(user and is_authorized_user(user.id))
    is_silent_chat = chat_id in SILENT_GROUP_IDS
    is_group = chat.type in (Chat.GROUP, Chat.SUPERGROUP)

    # COMMAND-ONLY MODE FOR AUTHORIZED GROUPS
    if is_silent_chat:
        return

    # Authorized messages: never auto-spiel; cancel any pending buffers; record timestamp
    if is_auth:
        set_last_auth_msg(chat_id)
        cancel_all_pending_for_chat(chat_id)
        return

    # From here, sender is non-authorized.

    # Weekend: only act in group chats
    if is_weekend():
        if is_group:
            schedule_buffered(chat_id, "WE", context)
        return

    # Lunch: only act in group chats
    if is_lunch_time():
        if is_group:
            schedule_buffered(chat_id, "LUNCH", context)
        return

    # Business hours / cutoff handling
    open_, before_cutoff = is_office_open()

    def authorized_initiated_on_or_before_cutoff_today() -> bool:
        ts = LAST_AUTH_MSG_AT.get(chat_id)
        if not ts:
            return False
        local_ts = ts.astimezone(TIMEZONE)
        today_str = now.strftime("%Y-%m-%d")
        return local_ts.strftime("%Y-%m-%d") == today_str and local_ts.time() <= WEEKDAY_CUTOFF

    if open_:
        if not before_cutoff:
            # 4:30–5:00 PM: only in groups; buffer cutoff unless authorized initiated on/before cutoff
            if is_group:
                if not authorized_initiated_on_or_before_cutoff_today():
                    schedule_buffered(chat_id, "CUTOFF", context)
            return
        else:
            return  # before cutoff during open hours → stay silent

    # After-hours (weekday): only in groups, schedule AM/PM
    is_pm_after_shift = now.time() >= WEEKDAY_END  # >= 5:00 PM
    is_am_before_shift = now.time() < WEEKDAY_START  # < 9:00 AM

    if is_group:
        if is_pm_after_shift:
            schedule_buffered(chat_id, "PM", context)
        elif is_am_before_shift:
            schedule_buffered(chat_id, "AM", context)
    return

# ---------------- Scheduler: 4:00 PM CT last call (weekdays, only chats active today) ----------------
async def last_call_scheduler(app):
    while True:
        now_local = now_in_timezone()
        try:
            # Weekdays only, at 16:00 local time
            if now_local.weekday() < 5 and now_local.time().hour == LAST_CALL_TIME.hour and now_local.time().minute == LAST_CALL_TIME.minute:
                today_str = now_local.strftime("%Y-%m-%d")
                targets = [cid for cid, d in LAST_CHAT_ACTIVITY.items()
                           if d == today_str and cid not in SILENT_GROUP_IDS]
                for chat_id in targets:
                    try:
                        await app.bot.send_message(chat_id=int(chat_id), text=LAST_CALL_MESSAGE, parse_mode="Markdown")
                    except Exception as e:
                        logger.error(f"Failed to send last call to {chat_id}: {e}")
            await asyncio.sleep(60)
        except Exception:
            logger.exception("last_call_scheduler loop error")
            await asyncio.sleep(60)

# ---------------- Main ----------------
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error", exc_info=context.error)

def _acquire_conflict_guard(port: int) -> Optional[socket.socket]:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port))
        s.listen(1)
        logger.info(f"Conflict guard bound on 127.0.0.1:{port}")
        return s
    except Exception as e:
        logger.error(f"Another process appears to be running (conflict guard port {port} busy): {e}")
        return None

async def main():
    if not BOT_TOKEN:
        print("❌ BOT_TOKEN not set"); return

    # Email API check
    if not SENDGRID_API_KEY:
        logger.warning("SENDGRID_API_KEY missing — transcript emails will fail until set.")

    # Optional conflict guard (same-host only)
    guard_sock = _acquire_conflict_guard(CONFLICT_GUARD_PORT)
    if guard_sock is None:
        logger.error("Exiting due to conflict guard.")
        return

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Commands (authorized only)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("myid", myid))
    app.add_handler(CommandHandler("Rules", rules_command))
    app.add_handler(CommandHandler("rules", rules_command))
    app.add_handler(CommandHandler(["lt", "apd", "mvr", "sign", "emails"], generic_command_handler))
    app.add_handler(CommandHandler("time", time_command))
    app.add_handler(CommandHandler("hours", time_command))  # alias
    app.add_handler(CommandHandler("coi", coi_command))
    app.add_handler(CommandHandler("ssi", ssi_command))
    app.add_handler(CommandHandler("sse", sse_command))
    app.add_handler(CommandHandler("ssc", ssc_command))
    app.add_handler(CommandHandler("who", who_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("broadcastpin", broadcastpin_command))
    app.add_handler(CommandHandler("broadcastto", broadcastto_command))

    # Messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    app.add_error_handler(on_error)

    # Kick off last-call scheduler
    asyncio.create_task(last_call_scheduler(app))

    logger.info(
        "✅ Bot running: command-only authorized groups; 2h cooldown per group; "
        "no auto-ack; /time(/hours) and /coi commands; "
        "Last Call 4:00 PM CT (active chats only); cutoff at 4:30 PM; "
        "ALL auto prompts use 5-min flood buffer (cancelled if an authorized user speaks; AM cancels at opening); "
        "after-hours: 2h suppression after authorized posts with 1h silence threshold; "
        "AM/PM spiels once per day per *group chat* with strong debouncing; "
        "SendGrid email (plain-text transcripts, subject=group title); "
        "/who, /broadcast, /broadcastpin, and /broadcastto."
    )
    await app.run_polling()

if __name__ == "__main__":
    try:
        import nest_asyncio; nest_asyncio.apply()
    except Exception:
        pass
    asyncio.run(main())