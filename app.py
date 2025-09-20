import pandas as pd
import os
from datetime import datetime
import telegram
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, Document, InputFile
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters,
    ContextTypes, CallbackQueryHandler
)
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from gridfs import GridFS
import io
import importlib.metadata
import json
import time
import logging
import asyncio
from typing import Dict, List, Any, Optional
import requests
import pdfplumber

# Configure logging with better formatting
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot_logs.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Check python-telegram-bot version
try:
    telegram_version = importlib.metadata.version("python-telegram-bot")
    logger.info(f"Using python-telegram-bot version: {telegram_version}")
    DOCUMENT_FILTER = filters.Document.ALL
except Exception as e:
    logger.error(f"Error: python-telegram-bot not installed correctly: {e}")
    raise ImportError("Please install python-telegram-bot==20.7")

# CONFIG - Validate environment variables
def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} is required but not set")
    return value

try:
    BOT_TOKEN = get_env_var('BOT_TOKEN')
    ADMIN_ID = int(get_env_var('ADMIN_ID'))
    ADMIN_USERNAME = "@Darksniperrx"
    MONGO_URI = get_env_var('MONGO_URI')
    MONGO_DB = get_env_var('MONGO_DB', 'telegram_bot')
    PORT = int(get_env_var('PORT', '8443'))
    WEBHOOK_URL = get_env_var('WEBHOOK_URL', '')
    USE_WEBHOOK = WEBHOOK_URL.lower() == 'true'
    BASE_URL = "http://erp.imsec.ac.in/salary_slip/print_salary_slip/"
    SAVE_DIR = "salary_slips"
    LOG_DIR = "logs"
except ValueError as e:
    logger.error(f"Configuration error: {e}")
    raise

# Create directories for salary slips and logs
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# MongoDB Setup with connection pooling and retry mechanism
class MongoDBManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.fs = None
        self.users_collection = None
        self.access_collection = None
        self.logs_collection = None
        self.feedback_collection = None
        self.blocked_collection = None
        self.connect()

    def connect(self):
        try:
            self.client = MongoClient(
                MONGO_URI, 
                w='majority', 
                wtimeoutms=5000,
                maxPoolSize=100,
                socketTimeoutMS=30000,
                connectTimeoutMS=30000,
                serverSelectionTimeoutMS=30000,
                retryWrites=True
            )
            self.client.admin.command('ping')
            self.db = self.client[MONGO_DB]
            self.fs = GridFS(self.db)
            self.users_collection = self.db['authorized_users']
            self.access_collection = self.db['access_count']
            self.logs_collection = self.db['logs']
            self.feedback_collection = self.db['feedback']
            self.blocked_collection = self.db['blocked_users']
            logger.info("MongoDB connected successfully")
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            raise

    def ensure_connection(self):
        try:
            self.client.admin.command('ping')
        except Exception:
            logger.warning("MongoDB connection lost, reconnecting...")
            self.connect()

# Initialize MongoDB manager
mongo_manager = MongoDBManager()

# GLOBAL DATA
df = pd.DataFrame()
blocked_users = set()
user_list = set()

# Salary Slip Functions
def extract_name_from_pdf(file_path):
    """Extract sender's name from the PDF using pdfplumber."""
    try:
        with pdfplumber.open(file_path) as pdf:
            first_page = pdf.pages[0]
            text = first_page.extract_text()
            for line in text.split('\n'):
                if "Name:" in line:
                    return line.split("Name:")[1].strip()
            return None
    except Exception as e:
        logger.error(f"Error extracting name from PDF {file_path}: {e}")
        return None

def download_pdf(emp_id):
    url = f"{BASE_URL}{emp_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        if "application/pdf" in res.headers.get("Content-Type", ""):
            temp_filename = os.path.join(SAVE_DIR, f"temp_{emp_id}.pdf")
            with open(temp_filename, "wb") as f:
                f.write(res.content)
            
            sender_name = extract_name_from_pdf(temp_filename)
            if sender_name:
                sender_name = ''.join(c for c in sender_name if c.isalnum() or c in (' ', '_')).replace(' ', '_')
                filename = os.path.join(SAVE_DIR, f"{sender_name}_{emp_id}.pdf")
            else:
                filename = os.path.join(SAVE_DIR, f"sniper_{emp_id}.pdf")
            
            os.rename(temp_filename, filename)
            return filename
        return None
    except Exception as e:
        logger.error(f"Error downloading PDF for {emp_id}: {e}")
        return None

def log_usage(user, action):
    log_file = os.path.join(LOG_DIR, f"{user.id}.log")
    with open(log_file, "a", encoding='utf-8') as f:
        f.write(f"[{datetime.now().isoformat()}] {user.full_name} (@{user.username or 'NoUsername'}) (ID: {user.id}) -> {action}\n")

# Helper function to format student record into sections with emojis
def format_student_record(record):
    sections = {
        "Personal Details": [
            "Name", "Gender", "Category", "Date Of Birth", "Religion", "Nationality", "Blood Group",
            "Student Aadhar No.", "Student Email", "Student Mobile"
        ],
        "Academic Details": [
            "Course", "Stream", "Year", "Section", "Sub Section", "Admission No.", "Roll No.",
            "Enrollment No", "Admission Date", "Admission Through", "State Rank", "ABC ID",
            "Enquiry No", "Form No", "UPSEE Admitted Amount", "Status", "Sub-Status", "Remark"
        ],
        "School & Marks": [
            "10th Board", "10th Passing Year", "10th School Name", "10th State", "10th Roll No.",
            "10th Obt. Marks", "10th Max. Marks", "10th Percent Marks", "12th Board",
            "12th Passing Year", "12th School Name", "12th State", "12th Roll No.",
            "12th Obt. Marks", "12th Max. Marks", "12th Percent Marks", "PCM/PCB Option",
            "12th PCM/PCB Percent", "English Marks", "Physics Marks", "Chemistry Marks",
            "Maths/Bio. Marks", "Applied for any improvement paper", "Subject", "Result",
            "Any grace in Qualifying Exam", "Details"
        ],
        "Family Details": [
            "Father Name", "Father Occupation", "Father Mobile", "Father Email",
            "Father Home Telephone", "Father Work Telephone", "Mother Name", "Mother Occupation",
            "Mother Mobile", "Mother Email", "Mother Home Telephone", "Mother Work Telephone",
            "Parents Income/ Lacs/PA"
        ],
        "Address": [
            "Local Address", "Local City", "Local State", "Local Pincode",
            "Permanent Address", "Permanent City", "Permanent State", "Permanent Pincode"
        ],
        "Hostel & Other Details": [
            "Hostel Required", "Hostel Type", "Room Type", "Transport Required", "Shift", "TFW",
            "EWS", "Appeared Entrance Exam", "Background", "State of Domicile",
            "Local Guardian Name", "Relation with student", "Guardian Contact No",
            "Guardian Telephone", "Guardian Address", "City", "State", "Pincode", "Exam Name",
            "Roll No", "Category Rank", "Verification Center", "Graduation University",
            "Graduation Passing Year", "Graduation College Name", "Graduation State",
            "Graduation Roll No.", "Graduation Obt. Marks", "Graduation Max. Marks",
            "Graduation Percent Marks", "Diploma University", "Diploma Passing Year",
            "Diploma College Name", "Diploma State", "Diploma Roll No.", "Diploma Obt. Marks",
            "Diploma Max. Marks", "Diploma Percent Marks", "SR No."
        ]
    }
    emojis = {
        "Personal Details": "👤",
        "Academic Details": "🎓",
        "School & Marks": "🏫",
        "Family Details": "👨‍👩‍👧",
        "Address": "🏠",
        "Hostel & Other Details": "🛏"
    }

    output = []
    for section, fields in sections.items():
        section_output = [f"{emojis[section]} {section}:"]
        for field in fields:
            value = record.get(field, "Not Available")
            if pd.isna(value) or value == "":
                value = "Not Available"
            section_output.append(f"- {field}: {value}")
        output.append("\n".join(section_output))
    
    return output

# Helper to send admin notification for searches
async def notify_admin_search(context, user_id, username, query, column, student_record, is_full_record=False):
    if is_full_record:
        formatted_sections = format_student_record(student_record)
        full_record_text = "\n\n".join(formatted_sections)
        message = (
            f"🔍 *Full Search Result Alert:*\n"
            f"🆔 User ID: {user_id}\n"
            f"👤 User: {username or 'N/A'}\n"
            f"🔍 Query: {query} (in {column})\n"
            f"⏰ Timestamp: {datetime.now().isoformat()}\n\n"
            f"📋 *Student Record:*\n{full_record_text}"
        )
    else:
        message = (
            f"🔍 *Search Alert:*\n"
            f"�ID User ID: {user_id}\n"
            f"👤 User: {username or 'N/A'}\n"
            f"🔍 Query: {query} (in {column})\n"
            f"⏰ Timestamp: {datetime.now().isoformat()}"
        )
    for attempt in range(3):
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode='Markdown')
            logger.info(f"Sent admin notification for user {user_id}, query {query}")
            break
        except telegram.error.BadRequest as e:
            logger.error(f"Error sending admin search notification for user {user_id}, attempt {attempt + 1}: {e}")
            if attempt == 2:
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Failed to send admin search notification: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
            await asyncio.sleep(1)

# Improved logging function with better structure
def save_log(log_type: str, log_data: Dict[str, Any]):
    db = get_db()
    try:
        if 'user_id' in log_data:
            user_doc = db.users_collection.find_one({'user_id': log_data['user_id']})
            if user_doc:
                log_data['user_name'] = user_doc.get('user_name', 'N/A')
                log_data['user_username'] = user_doc.get('user_username', 'N/A')
        
        log_entry = {
            'type': log_type,
            'data': log_data,
            'timestamp': datetime.now().isoformat(),
            'readable_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        db.logs_collection.update_one(
            {},
            {'$push': {'logs': log_entry}},
            upsert=True
        )
        logger.info(f"📝 Saved log: {log_type} - {log_data}")
    except Exception as e:
        logger.error(f"Error saving log {log_type}: {str(e)}")
        with open('fallback_logs.json', 'a', encoding='utf-8') as f:
            json.dump(log_entry, f, default=str, indent=2)
            f.write('\n')

# ---------- Helpers ------------
def get_db():
    mongo_manager.ensure_connection()
    return mongo_manager

def load_all_excels():
    db = get_db()
    dfs = []
    for filename in get_excel_files():
        try:
            file_data = db.fs.find_one({"filename": filename})
            if file_data:
                file_stream = io.BytesIO(file_data.read())
                excel_dfs = pd.read_excel(file_stream, sheet_name=None, engine='openpyxl')
                for sheet_name, sheet_df in excel_dfs.items():
                    if not sheet_df.empty:
                        logger.info(f"Loaded sheet '{sheet_name}' from {filename} with {len(sheet_df)} rows")
                        key_cols = ['Name', 'Student Mobile', 'Student Email']
                        available_keys = [col for col in key_cols if col in sheet_df.columns]
                        if available_keys:
                            sheet_df = sheet_df.drop_duplicates(subset=available_keys)
                        dfs.append(sheet_df)
                    else:
                        logger.warning(f"Sheet '{sheet_name}' in {filename} is empty")
            else:
                logger.warning(f"No data found for {filename} in GridFS")
        except Exception as e:
            logger.error(f"Error loading excel {filename}: {str(e)}")
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined DataFrame with {len(combined_df)} rows and columns: {list(combined_df.columns)}")
        for col in combined_df.select_dtypes(include=['object']).columns:
            combined_df[col] = combined_df[col].str.lower()
        initial_rows = len(combined_df)
        key_cols = ['Name', 'Student Mobile', 'Student Email', 'Roll No.']
        available_keys = [col for col in key_cols if col in combined_df.columns]
        if available_keys:
            combined_df = combined_df.drop_duplicates(subset=available_keys)
        final_rows = len(combined_df)
        duplicates_removed = initial_rows - final_rows
        logger.info(f"Deduplication: Removed {duplicates_removed} duplicate rows. Final DataFrame has {final_rows} rows.")
        return combined_df
    logger.warning("No data loaded into DataFrame")
    return pd.DataFrame()

def save_excel_to_gridfs(file_data, filename):
    db = get_db()
    try:
        if db.fs.exists({"filename": filename}):
            db.fs.delete(db.fs.find_one({"filename": filename})._id)
        db.fs.put(file_data, filename=filename)
        logger.info(f"Excel '{filename}' saved to GridFS.")
    except Exception as e:
        logger.error(f"Error saving excel {filename}: {e}")
        raise

def get_excel_files():
    db = get_db()
    files = [f.filename for f in db.fs.find()]
    logger.info(f"Found {len(files)} Excel files in GridFS: {files}")
    return files

def load_excel_on_startup():
    global df
    df = load_all_excels()
    logger.info(f"DataFrame on startup: {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}")
    return df

# ---------- MongoDB Helper Functions ------------
def load_authorized_users():
    db = get_db()
    try:
        users = list(db.users_collection.find({}, {'user_id': 1, 'user_name': 1, 'user_username': 1}))
        user_list = {u['user_id']: {'name': u.get('user_name', 'N/A'), 'username': u.get('user_username', 'N/A')} for u in users}
        logger.info(f"Loaded authorized users: {len(user_list)} users")
        return user_list
    except Exception as e:
        logger.error(f"Error loading authorized users: {str(e)}")
        save_log("errors", {"error": f"Failed to load authorized users: {str(e)}"})
        return {}

def save_authorized_user(user_id, user_name, user_username, retries=3):
    db = get_db()
    for attempt in range(retries):
        try:
            db.users_collection.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id, 'user_name': user_name, 'user_username': user_username, 'added_at': datetime.now()}},
                upsert=True
            )
            updated_doc = db.users_collection.find_one({'user_id': user_id})
            if updated_doc:
                logger.info(f"Successfully saved authorized user: {user_id} ({user_name} @{user_username})")
                return True
            else:
                logger.warning(f"Verification failed for saving authorized user: {user_id}, attempt {attempt + 1}")
                if attempt == retries - 1:
                    raise Exception("Failed to verify saved authorized user after retries")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error saving authorized user {user_id}, attempt {attempt + 1}: {str(e)}")
            if attempt == retries - 1:
                save_log("errors", {"user_id": user_id, "error": f"Failed to save authorized user: {str(e)}"})
                raise
            time.sleep(1)
    return False

def load_blocked_users():
    db = get_db()
    try:
        blocked = [user['user_id'] for user in db.blocked_collection.find({}, {'user_id': 1})]
        logger.info(f"Loaded blocked users: {len(blocked)} users")
        return set(blocked)
    except Exception as e:
        logger.error(f"Error loading blocked users: {str(e)}")
        save_log("errors", {"error": f"Failed to load blocked users: {str(e)}"})
        return set()

def save_blocked_user(user_id):
    db = get_db()
    try:
        db.blocked_collection.update_one(
            {'user_id': user_id},
            {'$set': {'user_id': user_id, 'blocked_at': datetime.now()}},
            upsert=True
        )
        logger.info(f"Saved blocked user: {user_id}")
    except Exception as e:
        logger.error(f"Error saving blocked user {user_id}: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Failed to save blocked user: {str(e)}"})
        raise

def remove_blocked_user(user_id):
    db = get_db()
    try:
        db.blocked_collection.delete_one({'user_id': user_id})
        logger.info(f"Removed blocked user: {user_id}")
    except Exception as e:
        logger.error(f"Error removing blocked user {user_id}: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Failed to remove blocked user: {str(e)}"})
        raise

def load_access_count():
    db = get_db()
    try:
        counts = {}
        for doc in db.access_collection.find({}, {'user_id': 1, 'count': 1, 'total_limit': 1, 'last_updated': 1}):
            user_id = str(doc['user_id'])
            counts[user_id] = {
                'count': doc.get('count', 0),
                'total_limit': doc.get('total_limit', 1),
                'last_updated': doc.get('last_updated', datetime.now())
            }
        logger.info(f"Loaded access counts for {len(counts)} users")
        return counts
    except Exception as e:
        logger.error(f"Error loading access counts: {str(e)}")
        save_log("errors", {"error": f"Failed to load access counts: {str(e)}"})
        return {}

def save_access_count(user_id, count, total_limit, retries=3):
    db = get_db()
    for attempt in range(retries):
        try:
            db.access_collection.update_one(
                {'user_id': user_id},
                {'$set': {
                    'count': count, 
                    'total_limit': total_limit,
                    'last_updated': datetime.now()
                }},
                upsert=True
            )
            updated_doc = db.access_collection.find_one({'user_id': user_id})
            if updated_doc and updated_doc['count'] == count and updated_doc['total_limit'] == total_limit:
                logger.info(f"Successfully saved access count for user {user_id}: count={count}, total_limit={total_limit}")
                return True
            else:
                logger.warning(f"Verification failed for user {user_id}: expected count={count}, total_limit={total_limit}, got {updated_doc}, attempt {attempt + 1}")
                if attempt == retries - 1:
                    raise Exception("Failed to verify saved access count after retries")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error saving access count for user {user_id}, attempt {attempt + 1}: {str(e)}")
            if attempt == retries - 1:
                save_log("errors", {"user_id": user_id, "error": f"Failed to save access count: {str(e)}"})
                raise
            time.sleep(1)
    return False

def load_logs():
    db = get_db()
    try:
        log_doc = db.logs_collection.find_one({}, {'logs': {'$slice': -100}}) or {'logs': []}
        logger.info(f"Loaded recent logs: {len(log_doc.get('logs', []))} entries")
        return log_doc.get('logs', [])
    except Exception as e:
        logger.error(f"Error loading logs: {str(e)}")
        save_log("errors", {"error": f"Failed to load logs: {str(e)}"})
        return []

def load_feedback():
    db = get_db()
    try:
        feedback = list(db.feedback_collection.find().sort("timestamp", -1).limit(100))
        logger.info(f"Loaded feedback: {len(feedback)} entries")
        return feedback
    except Exception as e:
        logger.error(f"Error loading feedback: {str(e)}")
        save_log("errors", {"error": f"Failed to load feedback: {str(e)}"})
        return []

def save_feedback_data(feedback_data):
    db = get_db()
    try:
        db.feedback_collection.insert_one(feedback_data)
        logger.info(f"Saved feedback: {feedback_data}")
    except Exception as e:
        logger.error(f"Error saving feedback: {str(e)}")
        save_log("errors", {"error": f"Failed to save feedback: {str(e)}"})
        raise

# Broadcast online status to all authorized users
async def broadcast_online_status(application):
    authorized = load_authorized_users()
    total_sent = 0
    failed_users = []
    online_message = (
        "🚀 *sniper's Bot is BACK ONLINE!* 🔥\n\n"
        "Your ultimate bot is ready to roll! 💪\n"
        "Use these commands to unleash its power:\n\n"
        "🔐 /start - Request access\n"
        "🔍 /name <query> - Search by name\n"
        "📧 /email <query> - Search by email\n"
        "📱 /phone <query> - Search by phone\n"
        "📄 /downloadone <id> (8000-9600) - Grab a salary slip (unlimited!) 💥\n"
        "📊 /profile - Check your usage stats\n"
        "📝 /feedback <message> - Drop your thoughts\n"
        "ℹ️ /help - Get the command list\n\n"
        f"🤖 *Powered by {ADMIN_USERNAME}* - Stay sharp! 🦅"
    )

    for uid in authorized:
        for attempt in range(3):
            try:
                await application.bot.send_message(
                    chat_id=uid,
                    text=online_message,
                    parse_mode='Markdown'
                )
                total_sent += 1
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending online status to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    failed_users.append(uid)
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to send online status: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)

    logger.info(f"Online status broadcast sent to {total_sent} users. Failed: {len(failed_users)}")
    save_log("online_broadcast", {
        "total_sent": total_sent,
        "failed_users": failed_users,
        "timestamp": datetime.now().isoformat()
    })

# ---------- Bot Commands -------------
async def check_blocked(user_id, update, context):
    if user_id == ADMIN_ID:
        logger.info(f"Admin {user_id} bypassed block check")
        return False
    blocked = load_blocked_users()
    if user_id in blocked:
        await update.message.reply_text("❌ You are blocked from using this bot.")
        logger.warning(f"Blocked user {user_id} attempted to use command")
        return True
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    user_list.add(user_id)
    if await check_blocked(user_id, update, context):
        return

    authorized = load_authorized_users()
    if user_id in authorized or user_id == ADMIN_ID:
        await update.message.reply_text(
            f"🔥 Welcome to sniper's Bot! 🔥\n\n"
            f"✅ You already have access.\n\n"
            "📋 Commands:\n"
            f"🔍 /name <query> - Search by name\n"
            f"📧 /email <query> - Search by email\n"
            f"📱 /phone <query> - Search by phone\n"
            f"📄 /downloadone <id> - Download salary slip (8000-9600, unlimited for users)\n"
            f"📊 /profile - View usage stats\n"
            f"📝 /feedback <message> - Send feedback\n"
            f"ℹ️ /help - Show commands\n\n"
            f"🤖 *Contact {ADMIN_USERNAME} for support*"
        )
        save_authorized_user(user_id, user.full_name, user.username or 'N/A')
    else:
        try:
            await update.message.reply_text("🔥 Welcome to sniper's Bot! 🔥\n\n🔐 Access request sent to admin. Please wait for approval.")
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
            ]])
            msg = (
                f"🔐 New Access Request:\n"
                f"👤 Name: {user.full_name}\n"
                f"🔗 Username: @{user.username or 'N/A'}\n"
                f"🆔 ID: {user_id}"
            )
            for attempt in range(3):
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text=msg, reply_markup=keyboard)
                    save_log("access_requests", {
                        "user_id": user_id,
                        "name": user.full_name,
                        "username": user.username or 'N/A',
                        "timestamp": update.message.date.isoformat()
                    })
                    break
                except telegram.error.BadRequest as e:
                    logger.error(f"Error sending access request to admin {ADMIN_ID}, attempt {attempt + 1}: {e}")
                    if attempt == 2:
                        await update.message.reply_text(f"⚠️ Failed to send access request to admin. Contact {ADMIN_USERNAME}.")
                        save_log("errors", {"user_id": user_id, "error": f"Failed to send access request: {str(e)}"})
                        return
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error in start command for user {user_id}: {str(e)}")
            await update.message.reply_text(f"❌ An error occurred. Contact {ADMIN_USERNAME}.")
            save_log("errors", {"user_id": user_id, "error": f"Start command failed: {str(e)}"})

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.info(f"Help command invoked by user {user_id}")
    if await check_blocked(user_id, update, context):
        logger.warning(f"User {user_id} blocked from accessing /help")
        return

    help_text = (
        "📋 *Bot Commands by sniper* 📋\n\n"
        "🔐 /start - Request access\n"
        "🔍 /name <query> - Search by name\n"
        "📧 /email <query> - Search by email\n"
        "📱 /phone <query> - Search by phone\n"
        "📄 /downloadone <id> (8000-9600) - Download salary slip (unlimited) 💥\n"
        "📊 /profile - View usage stats\n"
        "📝 /feedback <message> - Send feedback\n"
        "ℹ️ /help - Show this message\n\n"
        f"🤖 *Coded by {ADMIN_USERNAME}* 🔥"
    )
    if user_id == ADMIN_ID:
        logger.info(f"Admin {user_id} accessing admin help menu")
        help_text = (
            "📋 *Bot Commands by sniper* 📋\n\n"
            "🔐 /start - Request access\n"
            "🔍 /name <query> - Search by name\n"
            "📧 /email <query> - Search by email\n"
            "📱 /phone <query> - Search by phone\n"
            "📄 /downloadone <id> - Download single salary slip\n"
            "📥 /downloadall <start> <end> - Download multiple slips (admin only)\n"
            "📄 /listexcel - List available Excel files (admin)\n"
            "🔄 /reload - Reload all Excel data (admin)\n"
            "📊 /profile - Your usage stats\n"
            "👤 /userinfo <user_id> - View user info (admin)\n"
            "📝 /feedback <message> - Send feedback\n"
            "📢 /broadcast <msg> - Admin-only broadcast\n"
            "➕ /addaccess <user_id> <count> - Admin adds access count\n"
            "🚫 /block <user_id> - Admin blocks user\n"
            "✅ /unblock <user_id> - Admin unblocks user\n"
            "📜 /logs - View recent logs with user details (admin)\n"
            "📊 /analytics - View bot stats with per-user searches (admin)\n"
            "📩 /replyfeedback <user_id> <msg> - Reply to feedback (admin)\n"
            "📤 /exportusers - Export authorized users (admin)\n"
            "🏥 /health - Check bot health (admin)\n"
            "📢 /sharecommands - Share command list to all users (admin)\n"
            "ℹ️ /help - Show this message\n\n"
            f"🤖 *Coded by {ADMIN_USERNAME}* 🔥"
        )

    for attempt in range(3):
        try:
            await update.message.reply_text(help_text, parse_mode='Markdown')
            logger.info(f"Help command executed for user {user_id}")
            save_log("help_command", {
                "user_id": user_id,
                "user_name": update.message.from_user.full_name,
                "user_username": update.message.from_user.username or 'N/A',
                "timestamp": datetime.now().isoformat()
            })
            return
        except telegram.error.BadRequest as e:
            logger.error(f"Error sending help message to user {user_id}, attempt {attempt + 1}: {e}")
            if attempt == 2:
                await update.message.reply_text(f"❌ Failed to display help. Contact {ADMIN_USERNAME}.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Failed to send help message: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error in help command for user {user_id}: {str(e)}")
            await update.message.reply_text(f"❌ Error displaying help. Contact {ADMIN_USERNAME}.")
            save_log("errors", {
                "user_id": user_id,
                "error": f"Help command failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
            return

async def sharecommands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can share commands.")
        return

    command_message = (
        "🔥 *sniper's Bot Command List* 🔥\n\n"
        "Welcome to the ultimate bot experience! 🚀\n"
        "Here are your commands to unleash the power:\n\n"
        "🔐 /start - Kickstart your journey with access request\n"
        "🔍 /name <query> - Hunt down records by name\n"
        "📧 /email <query> - Track records by email\n"
        "📱 /phone <query> - Find records by phone number\n"
        "📄 /downloadone <id> (8000-9600) - Grab a salary slip (unlimited uses!) 💥\n"
        "📊 /profile - Check your usage stats like a pro\n"
        "📝 /feedback <message> - Drop your thoughts to sniper\n"
        "ℹ️ /help - Get this awesome command list again\n\n"
        f"🤖 *Powered by {ADMIN_USERNAME}* - Stay sharp! 🦅"
    )

    authorized = load_authorized_users()
    total_sent = 0
    failed_users = []

    for uid in authorized:
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=command_message,
                    parse_mode='Markdown'
                )
                total_sent += 1
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending command list to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    failed_users.append(uid)
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to send command list: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)

    await update.message.reply_text(
        f"📢 Command list shared with {total_sent} users.\n"
        f"{'⚠️ Failed to send to: ' + ', '.join(map(str, failed_users)) if failed_users else '✅ All successful!'}"
    )
    save_log("broadcast_commands", {
        "admin_id": user_id,
        "total_sent": total_sent,
        "failed_users": failed_users,
        "timestamp": datetime.now().isoformat()
    })

async def listexcel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can list Excel files.")
        return
    excel_files = get_excel_files()
    if not excel_files:
        await update.message.reply_text("❌ No Excel files found.")
        return
    files_list = "\n".join([f"- {f}" for f in excel_files])
    await update.message.reply_text(f"📄 Available Excel files:\n{files_list}")

async def reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can reload data.")
        return
    global df
    df = load_all_excels()
    await update.message.reply_text(f"✅ Reloaded data from all Excel files. DataFrame has {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}.")

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    authorized = load_authorized_users()
    if user_id not in authorized and user_id != ADMIN_ID:
        await update.message.reply_text("🔒 You are not authorized. Use /start to request access.")
        return
    access_count = load_access_count()
    user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
    count = user_data['count']
    total_limit = user_data['total_limit']
    remaining = "Unlimited" if user_id == ADMIN_ID else max(0, total_limit - count)
    await update.message.reply_text(
        f"👤 User ID: {user_id}\n"
        f"🔎 Searches used: {count}\n"
        f"📊 Total limit: {total_limit if user_id != ADMIN_ID else 'Unlimited'}\n"
        f"📉 Remaining: {remaining}"
    )

async def userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view user info.")
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /userinfo <user_id>")
        return
    try:
        target_user = int(context.args[0])
        access_count = load_access_count()
        user_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        count = user_data['count']
        total_limit = user_data['total_limit']
        remaining = max(0, total_limit - count)
        authorized_users = load_authorized_users()
        authorized = target_user in authorized_users
        blocked = target_user in load_blocked_users()
        user_info = authorized_users.get(target_user, {})
        user_name = user_info.get('name', 'N/A')
        user_username = user_info.get('username', 'N/A')
        feedback = [f for f in load_feedback() if f['user_id'] == target_user]
        feedback_text = "\n".join([f["message"] + f" ({f['timestamp']})" for f in feedback[-3:]]) or "No recent feedback"
        await update.message.reply_text(
            f"👤 User ID: {target_user}\n"
            f"👤 Name: {user_name}\n"
            f"🔗 Username: @{user_username}\n"
            f"🔓 Authorized: {'Yes' if authorized else 'No'}\n"
            f"🚫 Blocked: {'Yes' if blocked else 'No'}\n"
            f"🔎 Searches used: {count}\n"
            f"📊 Total limit: {total_limit}\n"
            f"📉 Remaining: {remaining}\n"
            f"📝 Recent Feedback:\n{feedback_text}"
        )
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching user info: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Userinfo failed: {str(e)}"})

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if len(context.args) == 0:
        await update.message.reply_text("Usage: /feedback <your message>")
        return
    msg = " ".join(context.args)
    feedback_data = {
        "user_id": user_id,
        "message": msg,
        "timestamp": update.message.date.isoformat(),
        "username": update.message.from_user.username or 'N/A',
        "name": update.message.from_user.full_name
    }
    try:
        save_feedback_data(feedback_data)
        await update.message.reply_text("✅ Feedback received. Thank you!")
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"📢 *New Feedback from User:*\n👤 {update.message.from_user.full_name} (@{update.message.from_user.username or 'N/A'})\n🆔 ID: {user_id}\n\n📝 {msg}",
                    parse_mode='Markdown'
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending feedback to admin {ADMIN_ID}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {"user_id": user_id, "error": f"Failed to send feedback to admin: {str(e)}"})
                    break
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in feedback for user {user_id}: {str(e)}")
        await update.message.reply_text("❌ Error saving feedback. Please try again.")
        save_log("errors", {"user_id": user_id, "error": f"Feedback command failed: {str(e)}"})

async def perform_search(update: Update, context: ContextTypes.DEFAULT_TYPE, column: str):
    global df
    user_id = update.message.from_user.id
    username = update.message.from_user.username or 'N/A'
    user_name = update.message.from_user.full_name
    if await check_blocked(user_id, update, context):
        return

    authorized = load_authorized_users()
    access_count = load_access_count()
    user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
    count = user_data['count']
    total_limit = user_data['total_limit']
    logger.info(f"Performing search for user {user_id} ({user_name} @{username}): count={count}, total_limit={total_limit}, column={column}")

    if user_id != ADMIN_ID and user_id not in authorized:
        await update.message.reply_text("🔒 You are not authorized. Use /start to request access.")
        return

    if user_id != ADMIN_ID and count >= total_limit:
        await update.message.reply_text(
            f"⚠️ Your search limit is reached. Current: count={count}, total_limit={total_limit}.\n\n"
            "💌 *Create & Send Redeem Code!* 🔑✨\n\n"
            "3 Searches → ₹50 💰\n"
            "10 Searches → ₹100 💸\n"
            "50 Searches → ₹200 💵\n"
            "Full Database Access → ₹1000 🏆\n"
            "Make a Bot Like Mine with Full Database → ₹1200 🤖⚡\n\n"
            f"Contact {ADMIN_USERNAME} for more searches or to purchase."
        )
        logger.warning(f"Search blocked for user {user_id}: count={count}, total_limit={total_limit}")
        return

    logger.info(f"DataFrame state before search: {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}")
    
    if df.empty:
        logger.info(f"DataFrame is empty when searching for column {column}. Reloading data...")
        df = load_all_excels()
        logger.info(f"DataFrame state after reload: {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}")
        if df.empty:
            logger.warning("DataFrame still empty after reload")
            await update.message.reply_text(f"❗ No Excel data loaded. Contact {ADMIN_USERNAME} to upload Excel files.")
            return

    if not context.args:
        await update.message.reply_text(f"Usage: /{column.lower()} <query>")
        return

    try:
        query = " ".join(context.args).strip().lower()
        logger.info(f"Searching for query '{query}' in column '{column}' by user {user_id}")
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame. Available columns: {list(df.columns)}")
            await update.message.reply_text(f"❌ Column '{column}' not found in Excel data. Available columns: {', '.join(df.columns)}")
            return

        matches = df[df[column].fillna('').astype(str).str.lower().str.contains(query, na=False)]
        logger.info(f"Found {len(matches)} matches for query '{query}' in column '{column}'")

        if matches.empty:
            await update.message.reply_text("❌ No matching records found.")
            await notify_admin_search(context, user_id, username, query, column, None)
            save_log("searches", {
                "user_id": user_id,
                "user_name": user_name,
                "user_username": username,
                "query": query,
                "column": column,
                "result_count": 0,
                "student_name": "No match",
                "timestamp": datetime.now().isoformat()
            })
            return

        context.user_data['search_results'] = matches.to_dict(orient='records')
        context.user_data['search_query'] = query
        context.user_data['search_column'] = column
        context.user_data['current_page'] = 0
        context.user_data['results_per_page'] = 10

        if user_id != ADMIN_ID and len(matches) == 1:
            if not save_access_count(user_id, count + 1, total_limit):
                await update.message.reply_text("❌ Error updating search count. Please try again.")
                return
            logger.info(f"Incremented search count for user {user_id} to {count + 1}/{total_limit} for single result")

        if len(matches) == 1:
            record = matches.iloc[0].to_dict()
            formatted_sections = format_student_record(record)
            for section in formatted_sections:
                await update.message.reply_text(section)
            await notify_admin_search(context, user_id, username, query, column, record, is_full_record=True)
            save_log("searches", {
                "user_id": user_id,
                "user_name": user_name,
                "user_username": username,
                "query": query,
                "column": column,
                "student_name": record.get('Name', 'Unknown'),
                "result_count": len(matches),
                "timestamp": datetime.now().isoformat()
            })
        else:
            await send_paginated_results(update, context)
            await notify_admin_search(context, user_id, username, query, column, None)
            save_log("searches", {
                "user_id": user_id,
                "user_name": user_name,
                "user_username": username,
                "query": query,
                "column": column,
                "student_name": "Multiple matches",
                "result_count": len(matches),
                "timestamp": datetime.now().isoformat()
            })
            return

    except Exception as e:
        logger.error(f"Error in search for user {user_id}: {str(e)}")
        await update.message.reply_text(f"❌ Search failed: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Search failed: {str(e)}"})

async def send_paginated_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results = context.user_data.get('search_results', [])
    query = context.user_data.get('search_query', '')
    column = context.user_data.get('search_column', '')
    current_page = context.user_data.get('current_page', 0)
    results_per_page = context.user_data.get('results_per_page', 10)

    if not results:
        await update.message.reply_text("❌ No search results available.")
        return

    total_results = len(results)
    total_pages = (total_results + results_per_page - 1) // results_per_page
    start_idx = current_page * results_per_page
    end_idx = min(start_idx + results_per_page, total_results)

    summary_text = f"Found {total_results} matches for '{query}' in {column}. Showing {start_idx + 1}-{end_idx} of {total_results}:\n\n"
    buttons = []
    for idx, record in enumerate(results[start_idx:end_idx], start=start_idx):
        course = record.get('Course', 'Unknown')
        name = record.get('Name', 'Unknown')
        summary_text += f"{idx + 1}. {name} ({course})\n"
        buttons.append([InlineKeyboardButton(f"{name} ({course})", callback_data=f"select_{idx}")])

    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{current_page - 1}"))
    if end_idx < total_results:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{current_page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    keyboard = InlineKeyboardMarkup(buttons)
    logger.info(f"Sending paginated results: page {current_page + 1}/{total_pages}, showing {start_idx + 1}-{end_idx}")
    
    try:
        if isinstance(update, telegram.Update) and update.callback_query:
            await update.callback_query.edit_message_text(summary_text, reply_markup=keyboard)
        else:
            await update.message.reply_text(summary_text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error sending paginated results: {str(e)}")
        save_log("errors", {"error": f"Failed to send paginated results: {str(e)}"})

async def search_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Name')

async def search_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Student Email')

async def search_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Student Mobile')

async def download_one(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    if await check_blocked(user_id, update, context):
        return

    if len(context.args) != 1:
        await update.message.reply_text("❗ Usage: /downloadone <id> (8000-9600)\nEnter an ID between 8000 and 9600 to download salary slip.")
        return

    try:
        emp_id = int(context.args[0])
        if not (8000 <= emp_id <= 9600):
            await update.message.reply_text("❗ Please provide an ID between 8000 and 9600.")
            return
    except ValueError:
        await update.message.reply_text("❗ Please provide a valid employee ID (8000-9600).")
        return

    file_path = download_pdf(emp_id)
    if file_path:
        filename = os.path.basename(file_path)
        await update.message.reply_document(document=open(file_path, "rb"), filename=filename)
        log_usage(user, f"Downloaded salary slip: {filename}")
        await notify_admin(context, user, f"Downloaded salary slip: {filename} (ID: {emp_id})")
        save_log("salary_downloads", {
            "user_id": user_id,
            "user_name": user.full_name,
            "user_username": user.username or 'N/A',
            "emp_id": emp_id,
            "filename": filename,
            "timestamp": datetime.now().isoformat()
        })
    else:
        await update.message.reply_text(f"❌ Salary slip not found for ID {emp_id}.")

async def download_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can download multiple slips.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("❗ Usage: /downloadall <start> <end> (8000-9600)")
        return

    try:
        start_id = int(context.args[0])
        end_id = int(context.args[1])
        if not (8000 <= start_id <= end_id <= 9600):
            await update.message.reply_text("❗ IDs must be between 8000 and 9600, start <= end.")
            return
    except ValueError:
        await update.message.reply_text("❗ Please provide valid start and end IDs (8000-9600).")
        return

    await update.message.reply_text(f"⏬ Downloading slips from {start_id} to {end_id}...")

    successful_downloads = 0
    for i in range(start_id, end_id + 1):
        file_path = download_pdf(i)
        if file_path:
            filename = os.path.basename(file_path)
            await update.message.reply_document(document=open(file_path, "rb"), filename=filename)
            log_usage(update.effective_user, f"Downloaded batch slip: {filename}")
            await notify_admin(context, update.effective_user, f"Downloaded batch slip: {filename} (ID: {i})")
            successful_downloads += 1
        else:
            await update.message.reply_text(f"❌ ID {i} Not Found.")
        await asyncio.sleep(0.5)

    await update.message.reply_text(f"✅ Batch download complete. Successful: {successful_downloads}/{end_id - start_id + 1}")
    save_log("batch_downloads", {
        "admin_id": user_id,
        "start_id": start_id,
        "end_id": end_id,
        "successful": successful_downloads,
        "timestamp": datetime.now().isoformat()
    })

async def notify_admin(context, user, action):
    for attempt in range(3):
        try:
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"📢 *User Alert:*\n👤 @{user.username or 'NoUsername'}\n🆔 {user.id}\n🎯 Action: `{action}`",
                parse_mode="Markdown"
            )
            break
        except telegram.error.BadRequest as e:
            logger.error(f"Error notifying admin for user {user.id}, attempt {attempt + 1}: {e}")
            if attempt == 2:
                save_log("errors", {"user_id": user.id, "error": f"Failed to notify admin: {str(e)}"})

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can upload files.")
        return

    doc: Document = update.message.document
    file_name = doc.file_name
    is_csv = file_name.lower().endswith(".csv")
    is_xlsx = file_name.lower().endswith(".xlsx")

    if not (is_csv or is_xlsx):
        await update.message.reply_text("❌ Only .csv or .xlsx files allowed.")
        return

    try:
        file = await doc.get_file()
        file_data = await file.download_as_bytearray()
        file_stream = io.BytesIO(file_data)

        if is_csv:
            try:
                csv_df = pd.read_csv(file_stream)
                logger.info(f"Read CSV file {file_name} with {len(csv_df)} rows, columns: {list(csv_df.columns)}")
            except Exception as e:
                error_msg = f"❌ Error reading CSV file: {str(e)}"
                logger.error(error_msg)
                await update.message.reply_text(error_msg)
                save_log("errors", {"user_id": user_id, "error": f"CSV read failed: {str(e)}"})
                return

            columns_found = set(csv_df.columns)
            required_columns = {'Name', 'Student Email', 'Student Mobile', 'Course'}
            if not required_columns.issubset(columns_found):
                missing = required_columns - columns_found
                await update.message.reply_text(f"❌ File missing required columns: {', '.join(missing)}")
                return

            xlsx_stream = io.BytesIO()
            csv_df.to_excel(xlsx_stream, index=False, engine='openpyxl')
            xlsx_stream.seek(0)
            xlsx_file_name = file_name.rsplit('.', 1)[0] + '.xlsx'
            save_excel_to_gridfs(xlsx_stream, xlsx_file_name)
            await update.message.reply_text(f"✅ CSV file {file_name} converted to {xlsx_file_name} and uploaded.")
        else:
            excel_dfs = pd.read_excel(file_stream, sheet_name=None, engine='openpyxl')
            file_stream.seek(0)
            columns_found = set()
            row_counts = []
            for sheet_name, sheet_df in excel_dfs.items():
                columns_found.update(sheet_df.columns)
                row_counts.append(len(sheet_df))
                logger.info(f"Sheet '{sheet_name}' in {file_name} has {len(sheet_df)} rows, columns: {list(sheet_df.columns)}")
            
            required_columns = {'Name', 'Student Email', 'Student Mobile', 'Course'}
            if not required_columns.issubset(columns_found):
                missing = required_columns - columns_found
                await update.message.reply_text(f"❌ Excel file missing required columns: {', '.join(missing)}")
                return

            save_excel_to_gridfs(file_stream, file_name)
            await update.message.reply_text(f"✅ Excel file {file_name} uploaded.")

        global df
        df = load_all_excels()
        await update.message.reply_text(f"✅ Data reloaded. DataFrame has {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}.")
    except Exception as e:
        error_msg = f"❌ Error processing file {file_name}: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)
        save_log("errors", {"user_id": user_id, "error": f"File upload failed: {str(e)}"})

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can broadcast.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return

    msg = " ".join(context.args)
    authorized = load_authorized_users()
    total_sent = 0

    for uid in authorized:
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast from {ADMIN_USERNAME}:\n\n{msg}")
                total_sent += 1
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Broadcast error to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {"user_id": uid, "error": f"Broadcast failed: {str(e)}"})
                await asyncio.sleep(1)

    await update.message.reply_text(f"Broadcast sent to {total_sent} users.")

async def addaccess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can add access.")
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /addaccess <user_id> <count>")
        return

    try:
        target_user = int(context.args[0])
        additional_count = int(context.args[1])
        if additional_count <= 0:
            await update.message.reply_text("❌ Count must be positive.")
            return

        access_count = load_access_count()
        user_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        current_count = user_data['count']
        current_limit = user_data['total_limit']
        new_limit = current_limit + additional_count

        if save_access_count(target_user, current_count, new_limit):
            await update.message.reply_text(
                f"✅ Added {additional_count} searches for user {target_user}. New limit: {new_limit}"
            )
            save_log("access_update", {
                "admin_id": user_id,
                "target_user_id": target_user,
                "additional_count": additional_count,
                "new_limit": new_limit,
                "timestamp": datetime.now().isoformat()
            })
            try:
                await context.bot.send_message(
                    chat_id=target_user,
                    text=f"🎉 Good news! Your search limit has been increased by {additional_count}. New total limit: {new_limit}. Enjoy! 🚀"
                )
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying user {target_user} about access update: {e}")
                save_log("errors", {"user_id": target_user, "error": f"Failed to notify user about access update: {str(e)}"})
        else:
            await update.message.reply_text("❌ Error updating access count.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID or count.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Addaccess failed: {str(e)}"})

async def block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can block users.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /block <user_id>")
        return

    try:
        target_user = int(context.args[0])
        if target_user == ADMIN_ID:
            await update.message.reply_text("❌ Cannot block the admin!")
            return
        save_blocked_user(target_user)
        await update.message.reply_text(f"✅ User {target_user} blocked.")
        save_log("block_user", {
            "admin_id": user_id,
            "target_user_id": target_user,
            "timestamp": datetime.now().isoformat()
        })
        try:
            await context.bot.send_message(
                chat_id=target_user,
                text=f"🚫 Your access to sniper's Bot has been blocked. Contact {ADMIN_USERNAME} for assistance."
            )
        except telegram.error.BadRequest as e:
            logger.error(f"Error notifying user {target_user} about block: {e}")
            save_log("errors", {"user_id": target_user, "error": f"Failed to notify user about block: {str(e)}"})
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Block failed: {str(e)}"})

async def unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can unblock users.")
        return

    if len(context.args) != 1:
        await update.message.reply_text("Usage: /unblock <user_id>")
        return

    try:
        target_user = int(context.args[0])
        remove_blocked_user(target_user)
        await update.message.reply_text(f"✅ User {target_user} unblocked.")
        save_log("unblock_user", {
            "admin_id": user_id,
            "target_user_id": target_user,
            "timestamp": datetime.now().isoformat()
        })
        try:
            await context.bot.send_message(
                chat_id=target_user,
                text=f"🎉 Your access to sniper's Bot has been restored! Start using /help to see commands. 🚀"
            )
        except telegram.error.BadRequest as e:
            logger.error(f"Error notifying user {target_user} about unblock: {e}")
            save_log("errors", {"user_id": target_user, "error": f"Failed to notify user about unblock: {str(e)}"})
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Unblock failed: {str(e)}"})

async def logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view logs.")
        return

    logs = load_logs()
    if not logs:
        await update.message.reply_text("📜 No logs found.")
        return

    log_text = "📜 *Recent Logs* (Last 100):\n\n"
    for log in logs:
        log_data = log['data']
        log_type = log['type']
        timestamp = log.get('readable_timestamp', log['timestamp'])
        user_name = log_data.get('user_name', 'N/A')
        user_username = log_data.get('user_username', 'N/A')
        user_id = log_data.get('user_id', 'N/A')
        action = log_data.get('action', 'N/A')
        if log_type == "searches":
            query = log_data.get('query', 'N/A')
            column = log_data.get('column', 'N/A')
            result_count = log_data.get('result_count', 0)
            student_name = log_data.get('student_name', 'N/A')
            log_text += (
                f"[{timestamp}] 🔍 Search by {user_name} (@{user_username}, ID: {user_id})\n"
                f"Query: {query} (in {column}), Results: {result_count}, Student: {student_name}\n\n"
            )
        elif log_type == "salary_downloads":
            emp_id = log_data.get('emp_id', 'N/A')
            filename = log_data.get('filename', 'N/A')
            log_text += (
                f"[{timestamp}] 📄 Salary Slip Download by {user_name} (@{user_username}, ID: {user_id})\n"
                f"Employee ID: {emp_id}, File: {filename}\n\n"
            )
        elif log_type == "batch_downloads":
            start_id = log_data.get('start_id', 'N/A')
            end_id = log_data.get('end_id', 'N/A')
            successful = log_data.get('successful', 0)
            log_text += (
                f"[{timestamp}] 📥 Batch Download by Admin (ID: {user_id})\n"
                f"Range: {start_id}-{end_id}, Successful: {successful}\n\n"
            )
        elif log_type == "access_requests":
            log_text += (
                f"[{timestamp}] 🔐 Access Request by {user_name} (@{user_username}, ID: {user_id})\n\n"
            )
        elif log_type == "errors":
            error = log_data.get('error', 'N/A')
            log_text += (
                f"[{timestamp}] ⚠️ Error for User ID: {user_id}\n"
                f"Details: {error}\n\n"
            )
        elif log_type == "broadcast_commands":
            total_sent = log_data.get('total_sent', 0)
            failed_users = log_data.get('failed_users', [])
            log_text += (
                f"[{timestamp}] 📢 Command List Broadcast by Admin (ID: {user_id})\n"
                f"Sent to: {total_sent} users, Failed: {len(failed_users)}\n\n"
            )
        elif log_type == "online_broadcast":
            total_sent = log_data.get('total_sent', 0)
            failed_users = log_data.get('failed_users', [])
            log_text += (
                f"[{timestamp}] 🚀 Online Status Broadcast\n"
                f"Sent to: {total_sent} users, Failed: {len(failed_users)}\n\n"
            )
        else:
            log_text += f"[{timestamp}] {log_type}: {action}\n\n"

    await update.message.reply_text(log_text, parse_mode='Markdown')

async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view analytics.")
        return

    access_count = load_access_count()
    authorized = load_authorized_users()
    logs = load_logs()
    total_searches = sum(user_data['count'] for user_data in access_count.values())
    total_users = len(authorized)
    active_users = len([uid for uid, data in access_count.items() if data['count'] > 0])
    recent_logs = len([log for log in logs if log['type'] == 'searches' and 
                       (datetime.now() - datetime.fromisoformat(log['timestamp'])).total_seconds() < 24*3600])

    analytics_text = (
        f"📊 *Bot Analytics* 📊\n\n"
        f"👥 Total Authorized Users: {total_users}\n"
        f"🔎 Total Searches: {total_searches}\n"
        f"🧑‍💼 Active Users (with searches): {active_users}\n"
        f"📈 Searches in Last 24 Hours: {recent_logs}\n\n"
        f"🔍 *Per-User Search Stats*:\n"
    )

    for uid, data in access_count.items():
        user_info = authorized.get(int(uid), {'name': 'N/A', 'username': 'N/A'})
        analytics_text += (
            f"👤 {user_info['name']} (@{user_info['username']}, ID: {uid})\n"
            f"   Searches: {data['count']}/{data['total_limit']}\n"
        )

    await update.message.reply_text(analytics_text, parse_mode='Markdown')

async def replyfeedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can reply to feedback.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Usage: /replyfeedback <user_id> <message>")
        return

    try:
        target_user = int(context.args[0])
        msg = " ".join(context.args[1:])
        await context.bot.send_message(
            chat_id=target_user,
            text=f"📩 *Reply from {ADMIN_USERNAME}:*\n\n{msg}",
            parse_mode='Markdown'
        )
        await update.message.reply_text(f"✅ Reply sent to user {target_user}.")
        save_log("feedback_reply", {
            "admin_id": user_id,
            "target_user_id": target_user,
            "message": msg,
            "timestamp": datetime.now().isoformat()
        })
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
    except telegram.error.BadRequest as e:
        await update.message.reply_text("❌ Failed to send reply. User may have blocked the bot.")
        save_log("errors", {"user_id": target_user, "error": f"Failed to send feedback reply: {str(e)}"})
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")
        save_log("errors", {"user_id": user_id, "error": f"Replyfeedback failed: {str(e)}"})

async def exportusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can export users.")
        return

    authorized = load_authorized_users()
    access_count = load_access_count()
    export_data = []
    for uid, info in authorized.items():
        user_data = access_count.get(str(uid), {'count': 0, 'total_limit': 1})
        export_data.append({
            'user_id': uid,
            'name': info['name'],
            'username': info['username'],
            'searches_used': user_data['count'],
            'total_limit': user_data['total_limit']
        })

    export_file = io.StringIO()
    export_df = pd.DataFrame(export_data)
    export_df.to_csv(export_file, index=False)
    export_file.seek(0)
    filename = f"users_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    await update.message.reply_document(
        document=InputFile(export_file, filename=filename),
        caption=f"📊 Exported {len(export_data)} authorized users."
    )
    save_log("export_users", {
        "admin_id": user_id,
        "total_users": len(export_data),
        "filename": filename,
        "timestamp": datetime.now().isoformat()
    })

async def health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can check bot health.")
        return

    try:
        # Check MongoDB connection
        mongo_manager.client.admin.command('ping')
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = f"❌ Disconnected: {str(e)}"

    # Check DataFrame
    df_status = f"✅ {len(df)} rows, {len(df.columns)} columns" if not df.empty else "❌ Empty"

    # Check authorized users
    authorized = load_authorized_users()
    auth_status = f"✅ {len(authorized)} users" if authorized else "❌ No users"

    # Check blocked users
    blocked = load_blocked_users()
    blocked_status = f"✅ {len(blocked)} users" if blocked is not None else "❌ Failed to load"

    # Check recent logs
    logs = load_logs()
    recent_logs = len([log for log in logs if (datetime.now() - datetime.fromisoformat(log['timestamp'])).total_seconds() < 3600])

    health_text = (
        f"🏥 *Bot Health Check* 🏥\n\n"
        f"🤖 Bot Status: ✅ Running\n"
        f"📡 MongoDB: {mongo_status}\n"
        f"📊 DataFrame: {df_status}\n"
        f"👥 Authorized Users: {auth_status}\n"
        f"🚫 Blocked Users: {blocked_status}\n"
        f"📜 Logs in Last Hour: {recent_logs}\n"
        f"⏰ Last Checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await update.message.reply_text(health_text, parse_mode='Markdown')
    save_log("health_check", {
        "admin_id": user_id,
        "mongo_status": mongo_status,
        "df_status": df_status,
        "auth_status": auth_status,
        "blocked_status": blocked_status,
        "recent_logs": recent_logs,
        "timestamp": datetime.now().isoformat()
    })

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if await check_blocked(user_id, update, context):
        return

    data = query.data
    logger.info(f"Callback query from user {user_id}: {data}")

    if data.startswith("approve_"):
        if user_id != ADMIN_ID:
            await query.message.reply_text("❌ Only admin can approve users.")
            return
        target_user = int(data.split("_")[1])
        authorized = load_authorized_users()
        if target_user in authorized:
            await query.message.reply_text("✅ User already authorized.")
            return
        try:
            user_info = await context.bot.get_chat(target_user)
            save_authorized_user(target_user, user_info.full_name, user_info.username or 'N/A')
            save_access_count(target_user, 0, 1)
            await query.message.reply_text(f"✅ Approved user {target_user}.")
            await context.bot.send_message(
                chat_id=target_user,
                text=f"🎉 Access granted! Use /help to see commands. 🚀"
            )
            save_log("approve_user", {
                "admin_id": user_id,
                "target_user_id": target_user,
                "timestamp": datetime.now().isoformat()
            })
        except telegram.error.BadRequest as e:
            await query.message.reply_text(f"❌ Failed to approve user {target_user}: {str(e)}")
            save_log("errors", {"user_id": target_user, "error": f"Failed to approve user: {str(e)}"})
        except Exception as e:
            await query.message.reply_text(f"❌ Error approving user: {str(e)}")
            save_log("errors", {"user_id": target_user, "error": f"Approve user failed: {str(e)}"})

    elif data.startswith("reject_"):
        if user_id != ADMIN_ID:
            await query.message.reply_text("❌ Only admin can reject users.")
            return
        target_user = int(data.split("_")[1])
        try:
            await context.bot.send_message(
                chat_id=target_user,
                text=f"❌ Access request denied. Contact {ADMIN_USERNAME} for assistance."
            )
            await query.message.reply_text(f"✅ Rejected user {target_user}.")
            save_log("reject_user", {
                "admin_id": user_id,
                "target_user_id": target_user,
                "timestamp": datetime.now().isoformat()
            })
        except telegram.error.BadRequest as e:
            await query.message.reply_text(f"❌ Failed to reject user {target_user}: {str(e)}")
            save_log("errors", {"user_id": target_user, "error": f"Failed to reject user: {str(e)}"})
        except Exception as e:
            await query.message.reply_text(f"❌ Error rejecting user: {str(e)}")
            save_log("errors", {"user_id": target_user, "error": f"Reject user failed: {str(e)}"})

    elif data.startswith("page_"):
        context.user_data['current_page'] = int(data.split("_")[1])
        await send_paginated_results(update, context)

    elif data.startswith("select_"):
        idx = int(data.split("_")[1])
        results = context.user_data.get('search_results', [])
        if idx < 0 or idx >= len(results):
            await query.message.reply_text("❌ Invalid selection.")
            return
        record = results[idx]
        formatted_sections = format_student_record(record)
        for section in formatted_sections:
            await query.message.reply_text(section)
        
        user_id = query.from_user.id
        username = query.from_user.username or 'N/A'
        query_text = context.user_data.get('search_query', 'N/A')
        column = context.user_data.get('search_column', 'N/A')
        
        if user_id != ADMIN_ID:
            access_count = load_access_count()
            user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
            count = user_data['count']
            total_limit = user_data['total_limit']
            if save_access_count(user_id, count + 1, total_limit):
                logger.info(f"Incremented search count for user {user_id} to {count + 1}/{total_limit} for selected result")
            else:
                await query.message.reply_text("❌ Error updating search count.")
                return

        await notify_admin_search(context, user_id, username, query_text, column, record, is_full_record=True)
        save_log("searches", {
            "user_id": user_id,
            "user_name": query.from_user.full_name,
            "user_username": username,
            "query": query_text,
            "column": column,
            "student_name": record.get('Name', 'Unknown'),
            "result_count": 1,
            "timestamp": datetime.now().isoformat()
        })

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error: {context.error}")
    save_log("errors", {
        "update": str(update),
        "error": str(context.error),
        "timestamp": datetime.now().isoformat()
    })
    if update and update.effective_message:
        await update.effective_message.reply_text(
            f"❌ An error occurred. Contact {ADMIN_USERNAME} for assistance."
        )

async def broadcast_online_status(application):
    authorized = load_authorized_users()
    total_sent = 0
    failed_users = []
    online_message = (
        "🚀 *sniper's Bot is BACK ONLINE!* 🔥\n\n"
        "Your ultimate bot is ready to roll! 💪\n"
        "Use these commands to unleash its power:\n\n"
        "🔐 /start - Request access\n"
        "🔍 /name <query> - Search by name\n"
        "📧 /email <query> - Search by email\n"
        "📱 /phone <query> - Search by phone\n"
        "📄 /downloadone <id> (8000-9600) - Grab a salary slip (unlimited!) 💥\n"
        "📊 /profile - Check your usage stats\n"
        "📝 /feedback <message> - Drop your thoughts\n"
        "ℹ️ /help - Get the command list\n\n"
        f"🤖 *Powered by {ADMIN_USERNAME}* - Stay sharp! 🦅"
    )

    for uid in authorized:
        for attempt in range(3):
            try:
                await application.bot.send_message(
                    chat_id=uid,
                    text=online_message,
                    parse_mode='Markdown'
                )
                total_sent += 1
                logger.info(f"Sent online status to user {uid}")
                break
            except telegram.error.Forbidden as e:
                logger.warning(f"User {uid} blocked the bot: {str(e)}")
                failed_users.append(uid)
                save_log("errors", {
                    "user_id": uid,
                    "error": f"Failed to send online status: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                break  # Skip retrying for this user
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending online status to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    failed_users.append(uid)
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to send online status: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error sending online status to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    failed_users.append(uid)
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Unexpected error sending online status: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)

    logger.info(f"Online status broadcast sent to {total_sent} users. Failed: {len(failed_users)}")
    save_log("online_broadcast", {
        "total_sent": total_sent,
        "failed_users": failed_users,
        "timestamp": datetime.now().isoformat()
    })
    try:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"📢 Online status broadcast completed: {total_sent} users reached, {len(failed_users)} failed."
        )
    except Exception as e:
        logger.error(f"Failed to notify admin about broadcast completion: {str(e)}")
        save_log("errors", {
            "user_id": ADMIN_ID,
            "error": f"Failed to notify admin about broadcast: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

# Main function to set up and run the bot
async def main():
    while True:
        try:
            global df
            df = load_all_excels()
            application = ApplicationBuilder().token(BOT_TOKEN).build()

            # Register command handlers
            application.add_handler(CommandHandler("start", start))
            application.add_handler(CommandHandler("help", help_command))
            application.add_handler(CommandHandler("name", search_name))
            application.add_handler(CommandHandler("email", search_email))
            application.add_handler(CommandHandler("phone", search_phone))
            application.add_handler(CommandHandler("downloadone", download_one))
            application.add_handler(CommandHandler("downloadall", download_all))
            application.add_handler(CommandHandler("listexcel", listexcel))
            application.add_handler(CommandHandler("reload", reload))
            application.add_handler(CommandHandler("profile", profile))
            application.add_handler(CommandHandler("userinfo", userinfo))
            application.add_handler(CommandHandler("feedback", feedback))
            application.add_handler(CommandHandler("broadcast", broadcast))
            application.add_handler(CommandHandler("addaccess", addaccess))
            application.add_handler(CommandHandler("block", block))
            application.add_handler(CommandHandler("unblock", unblock))
            application.add_handler(CommandHandler("logs", logs))
            application.add_handler(CommandHandler("analytics", analytics))
            application.add_handler(CommandHandler("replyfeedback", replyfeedback))
            application.add_handler(CommandHandler("exportusers", exportusers))
            application.add_handler(CommandHandler("health", health))
            application.add_handler(CommandHandler("sharecommands", sharecommands))
            application.add_handler(MessageHandler(DOCUMENT_FILTER, handle_document))
            application.add_handler(CallbackQueryHandler(button_callback))
            application.add_handler(error_handler)

            # Broadcast online status
            await broadcast_online_status(application)

            # Start the bot
            if USE_WEBHOOK:
                logger.info(f"Starting bot with webhook on port {PORT}")
                await application.run_webhook(
                    listen="0.0.0.0",
                    port=PORT,
                    url_path=BOT_TOKEN,
                    webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}"
                )
            else:
                logger.info("Starting bot with polling")
                await application.run_polling()

        except telegram.error.Forbidden as e:
            logger.error(f"Forbidden error in main: {str(e)}")
            save_log("errors", {"error": f"Main function failed: {str(e)}", "timestamp": datetime.now().isoformat()})
            logger.info(f"BOT STOPPED at {datetime.now().strftime('%a %d %b %Y %I:%M:%S %p UTC')} — restarting in 5s")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.error(f"Error in main: {str(e)}")
            save_log("errors", {"error": f"Main function failed: {str(e)}", "timestamp": datetime.now().isoformat()})
            logger.info(f"BOT STOPPED at {datetime.now().strftime('%a %d %b %Y %I:%M:%S %p UTC')} — restarting in 5s")
            await asyncio.sleep(5)
            continue

if __name__ == '__main__':
    asyncio.run(main())
