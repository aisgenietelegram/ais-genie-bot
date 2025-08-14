import os
import logging
import asyncio
from datetime import datetime, time, timedelta
from collections import deque, defaultdict
from typing import Dict, Any, Optional, Set
import pytz
import io
import textwrap
import socket
import smtplib
from email.message import EmailMessage

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

# App Password SMTP (OAuth removed)
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "aisgenie.telegram@gmail.com")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

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
)

CLOSED_MESSAGE_PM = (
    "🌙 Our office is now closed for the day.\n"
    "We’ll be back tomorrow at 9:00 AM CT.\n\n"
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
    "Please email: **info@myaisagency.com**\n\n"
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
    "🔔 *Starting September 1:* This will be strictly enforced. Thank you! 🙏"
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
CLOSED_SENT_TODAY_AM: Dict[str, str] = {}  # chat_id -> YYYY-MM-DD
CLOSED_SENT_TODAY_PM: Dict[str, str] = {}  # chat_id -> YYYY-MM-DD

# Track last authorized message timestamp per chat (CT)
LAST_AUTH_MSG_AT: Dict[str, datetime] = {}

# Track last unauthorized message timestamp per chat (CT) for flood buffer
LAST_UNAUTH_MSG_AT: Dict[str, datetime] = {}

# Pending delayed spiels tasks for AM/PM/Weekend flood buffer
PENDING_AM_SPIEL: Dict[str, asyncio.Task] = {}
PENDING_PM_SPIEL: Dict[str, asyncio.Task] = {}
PENDING_WEEKEND_SPIEL: Dict[str, asyncio.Task] = {}

# After-hours suppression window when an authorized user posts (2h), and 1h threshold to allow spiel if non-auth messages
AFTER_HOURS_SUPPRESSION_WINDOW_HOURS = 2
AFTER_HOURS_MIN_SILENCE_FOR_SPIEL_HOURS = 1

# Flood buffer inactivity window
FLOOD_BUFFER_SECONDS = 5 * 60  # 5 minutes

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

# ---- Email via SMTP (App Password) ----
def _send_email_smtp(subject: str, body: str, to_addr: str,
                     attach_name: Optional[str] = None, attach_bytes: Optional[bytes] = None) -> tuple[bool, Optional[str]]:
    sender = GMAIL_SENDER
    app_pw = GMAIL_APP_PASSWORD
    if not sender or not app_pw:
        return False, "Missing GMAIL_SENDER or GMAIL_APP_PASSWORD"

    try:
        msg = EmailMessage()
        msg["From"] = sender
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.set_content(body)

        if attach_bytes and attach_name:
            msg.add_attachment(attach_bytes, maintype="image", subtype="png", filename=attach_name)

        # Gmail SMTP over SSL (465)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender, app_pw)
            server.send_message(msg)

        return True, None
    except Exception as e:
        logger.exception("SMTP send failed")
        return False, str(e)

async def send_email_async(subject: str, body: str, to_addr: Optional[str] = None,
                           attach_name: Optional[str] = None, attach_bytes: Optional[bytes] = None):
    def _send():
        return _send_email_smtp(
            subject=subject,
            body=body,
            to_addr=to_addr or EMAIL_DEFAULT_TO,
            attach_name=attach_name,
            attach_bytes=attach_bytes
        )
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
    """
    Returns True if we are outside business hours (incl. weekends) AND within 2h since last authorized message in this chat.
    """
    open_, _ = is_office_open()
    if open_:
        return False
    age = last_auth_msg_age(chat_id)
    if age is None:
        return False
    return age <= timedelta(hours=AFTER_HOURS_SUPPRESSION_WINDOW_HOURS)

def allow_after_hours_spiel(chat_id: str) -> bool:
    """
    Outside business hours (incl. weekends):
    - If within suppression window (2h since last authorized), allow spiel only if >=1h since last authorized.
    - If not within suppression window, allow spiel normally.
    """
    open_, _ = is_office_open()
    if open_:
        return False  # only used after-hours
    age = last_auth_msg_age(chat_id)
    if age is None:
        return True
    if age >= timedelta(hours=AFTER_HOURS_SUPPRESSION_WINDOW_HOURS):
        return True
    # within 2h suppression: allow only after 1h
    return age >= timedelta(hours=AFTER_HOURS_MIN_SILENCE_FOR_SPIEL_HOURS)

def sent_closed_today(chat_id: str, is_pm: bool) -> bool:
    today = now_in_timezone().strftime("%Y-%m-%d")
    if is_pm:
        return CLOSED_SENT_TODAY_PM.get(chat_id) == today
    else:
        return CLOSED_SENT_TODAY_AM.get(chat_id) == today

def mark_closed_sent_today(chat_id: str, is_pm: bool):
    today = now_in_timezone().strftime("%Y-%m-%d")
    if is_pm:
        CLOSED_SENT_TODAY_PM[chat_id] = today
    else:
        CLOSED_SENT_TODAY_AM[chat_id] = today

def cancel_task(task: Optional[asyncio.Task]):
    if task and not task.done():
        task.cancel()

# Buffer schedulers for AM/PM/Weekend spiels
def schedule_buffered_spiel(chat_id: str, period: str, context: ContextTypes.DEFAULT_TYPE):
    """
    period: 'AM', 'PM', 'WE'
    Schedules a 5-min buffer before sending the appropriate closed/ weekend spiel,
    with cancellation rules:
      - AM: cancel if office opens before fire (>= 9:00 AM)
      - PM: normal (no cancel for opening)
      - WE: cancel if the weekend ends and office opens (Mon 9:00 AM)
    Also respects:
      - after-hours 2h suppression with 1h threshold
      - once-per-day per chat (AM/PM); weekend still respects 2h per-chat cooldown tag.
    """
    now_local = now_in_timezone()
    if period == "AM":
        # once-per-day AM
        if sent_closed_today(chat_id, is_pm=False):
            return
        # cancel any existing AM timer, then schedule
        cancel_task(PENDING_AM_SPIEL.get(chat_id))

        async def am_job():
            try:
                await asyncio.sleep(FLOOD_BUFFER_SECONDS)
                # If office opened, cancel (do not send)
                open_, _ = is_office_open()
                if open_:
                    return
                # Suppression: only allow after-hours spiel if allowed
                if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                    return
                await context.bot.send_message(chat_id=int(chat_id), text=CLOSED_MESSAGE_AM)
                mark_closed_sent_today(chat_id, is_pm=False)
            except asyncio.CancelledError:
                pass
            finally:
                PENDING_AM_SPIEL.pop(chat_id, None)

        PENDING_AM_SPIEL[chat_id] = asyncio.create_task(am_job())

    elif period == "PM":
        if sent_closed_today(chat_id, is_pm=True):
            return
        cancel_task(PENDING_PM_SPIEL.get(chat_id))

        async def pm_job():
            try:
                await asyncio.sleep(FLOOD_BUFFER_SECONDS)
                # Still after-hours?
                now_pm = now_in_timezone().time() >= WEEKDAY_END or is_weekend()
                if not now_pm:
                    return
                if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                    return
                await context.bot.send_message(chat_id=int(chat_id), text=CLOSED_MESSAGE_PM)
                mark_closed_sent_today(chat_id, is_pm=True)
            except asyncio.CancelledError:
                pass
            finally:
                PENDING_PM_SPIEL.pop(chat_id, None)

        PENDING_PM_SPIEL[chat_id] = asyncio.create_task(pm_job())

    else:  # 'WE' weekend
        # Use 2h cooldown tag for weekend; plus buffer
        if already_sent(chat_id, "weekend"):
            return
        cancel_task(PENDING_WEEKEND_SPIEL.get(chat_id))

        async def we_job():
            try:
                await asyncio.sleep(FLOOD_BUFFER_SECONDS)
                # Cancel if not weekend anymore, or office is open (Mon 9am)
                if not is_weekend():
                    # If it's Monday before 9, it's AM-beforeshift; let AM handler cover.
                    return
                open_, _ = is_office_open()
                if open_:
                    return
                if within_after_hours_suppression(chat_id) and not allow_after_hours_spiel(chat_id):
                    return
                await context.bot.send_message(chat_id=int(chat_id), text=WEEKEND_MESSAGE)
                mark_sent(chat_id, "weekend")
            except asyncio.CancelledError:
                pass
            finally:
                PENDING_WEEKEND_SPIEL.pop(chat_id, None)

        PENDING_WEEKEND_SPIEL[chat_id] = asyncio.create_task(we_job())

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
        "/lt /apd /mvr /sign /emails – Quick replies\n"
        "/time /hours – Business hours\n"
        "/coi – COI instructions\n"
        "/ssi – Email transcript to info@\n"
        "/sse – Email transcript to endorsements@\n"
        "/ssc – Email transcript to coi@"
    )

@require_authorized
async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"🆔 Your chat ID is: `{update.effective_chat.id}`", parse_mode="Markdown")

@require_authorized
async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    if cmd in COMMAND_MESSAGES:
        await update.message.reply_text(COMMAND_MESSAGES[cmd], parse_mode="Markdown")

    set_last_auth_msg(chat_id)  # authorized activity timestamp

@require_authorized
async def time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🕒 *Business Hours (CT)*\n"
        "Mon–Fri: 9:00 AM – 5:00 PM\n"
        "Lunch: 12:30 PM – 1:30 PM\n"
        "Weekends: Closed",
        parse_mode="Markdown"
    )

@require_authorized
async def coi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(COI_TEXT, parse_mode="Markdown")

async def _send_transcript_email(update: Update, to_addr: str):
    chat = update.effective_chat
    chat_id = str(chat.id)
    entries = list(chat_buffers.get(chat_id, []))
    if not entries:
        await update.message.reply_text("No recent text messages to capture for this chat.")
        return
    await update.message.reply_text("⏳ Preparing transcript…")

    chat_title = known_group_chats.get(chat_id, {}).get("title") or (chat.title or "")
    png_bytes = render_transcript_image(chat_title, entries)
    ts = now_in_timezone().strftime("%Y%m%d-%H%M%S")
    if png_bytes:
        ok, err = await send_email_async(
            subject=f"[Telegram] Transcript – {chat_title} ({chat_id})",
            body=f"Attached is the transcript image of the last {len(entries)} message(s).",
            to_addr=to_addr,
            attach_name=f"telegram_transcript_{chat_id}_{ts}.png",
            attach_bytes=png_bytes,
        )
    else:
        body = "\n".join([f"[{e['ts']}] {e['name']}: {e['text']}" for e in entries])
        ok, err = await send_email_async(
            subject=f"[Telegram] Transcript (text) – {chat_title} ({chat_id})",
            body=body,
            to_addr=to_addr,
        )
    if ok:
        await update.message.reply_text(f"✅ Transcript sent to {to_addr}")
    else:
        await update.message.reply_text(f"❌ Failed to send email to {to_addr}: {err or 'Unknown error'}")

@require_authorized
async def ssi_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_DEFAULT_TO)

@require_authorized
async def sse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_ENDORSEMENT)

@require_authorized
async def ssc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_transcript_email(update, EMAIL_COI)

# ---------------- Message handler (auto spiels) ----------------
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    chat_id = str(chat.id)
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

    # COMMAND-ONLY MODE FOR AUTHORIZED GROUPS
    if is_silent_chat:
        return

    # Authorized messages: never auto-spiel; record timestamp
    if is_auth:
        set_last_auth_msg(chat_id)
        return

    # From here, sender is non-authorized in a non-silent chat.

    # Weekend (now buffered + suppression-aware)
    if is_weekend():
        # schedule weekend spiel with 5-min flood buffer (and 2h suppression, 1h threshold)
        schedule_buffered_spiel(chat_id, "WE", context)
        return

    # Lunch notice (kept instant and gentle)
    if is_lunch_time():
        if not already_sent(chat_id, "lunch"):
            await update.message.reply_text(LUNCH_MESSAGE)
            mark_sent(chat_id, "lunch")
        return

    # Business hours / cutoff handling
    open_, before_cutoff = is_office_open()

    # If between cutoff (4:30) and close (5:00) and an authorized user initiated on/before cutoff today, suppress nudges
    def authorized_initiated_on_or_before_cutoff_today() -> bool:
        ts = LAST_AUTH_MSG_AT.get(chat_id)
        if not ts:
            return False
        local_ts = ts.astimezone(TIMEZONE)
        today_str = now.strftime("%Y-%m-%d")
        return (
            local_ts.strftime("%Y-%m-%d") == today_str
            and local_ts.time() <= WEEKDAY_CUTOFF
        )

    if open_:
        if not before_cutoff:
            # 4:30–5:00 PM
            if authorized_initiated_on_or_before_cutoff_today():
                return
            if not already_sent(chat_id, "cutoff"):
                await update.message.reply_text(AFTER_CUTOFF_MESSAGE, parse_mode="Markdown")
                mark_sent(chat_id, "cutoff")
            return
        else:
            # Before cutoff during open hours → stay silent (no auto-ack)
            return

    # After-hours (weekday) — send AM/PM with 5-min buffer and suppression awareness
    is_pm_after_shift = now.time() >= WEEKDAY_END  # >= 5:00 PM
    is_am_before_shift = now.time() < WEEKDAY_START  # < 9:00 AM

    # Respect the 2h suppression window after any authorized message outside hours.
    # The schedule_buffered_spiel itself will re-check suppression at fire time.
    if is_pm_after_shift:
        schedule_buffered_spiel(chat_id, "PM", context)
        return
    elif is_am_before_shift:
        schedule_buffered_spiel(chat_id, "AM", context)
        return
    else:
        return  # Safety no-op

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
    """
    Best-effort same-host guard. If bind fails, another instance likely holds it.
    Returns the bound socket on success (keep it open), or None on failure.
    """
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

    # SMTP vars check
    missing = []
    if not GMAIL_SENDER:
        missing.append("GMAIL_SENDER")
    if not GMAIL_APP_PASSWORD:
        missing.append("GMAIL_APP_PASSWORD")
    if missing:
        logger.warning(f"Missing email SMTP env: {missing}")

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

    # Messages
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    app.add_error_handler(on_error)

    # Kick off last-call scheduler
    asyncio.create_task(last_call_scheduler(app))

    logger.info(
        "✅ Bot running: command-only authorized groups; 2h cooldown per group; "
        "no auto-ack; /time(/hours) and /coi commands; "
        "Last Call 4:00 PM CT (active chats only); cutoff at 4:30 PM; "
        "AM/PM/Weekend spiels use 5-min flood buffer (cancel AM if opening starts); "
        "after-hours: 2h suppression after authorized posts with 1h silence threshold."
    )
    await app.run_polling()

if __name__ == "__main__":
    try:
        import nest_asyncio; nest_asyncio.apply()
    except Exception:
        pass
    asyncio.run(main())