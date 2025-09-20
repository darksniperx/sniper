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

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Check python-telegram-bot version
try:
    telegram_version = importlib.metadata.version("python-telegram-bot")
    logger.info(f"Using python-telegram-bot version: {telegram_version}")
    DOCUMENT_FILTER = filters.Document.ALL
except Exception as e:
    logger.error(f"Error: python-telegram-bot not installed correctly: {e}")
    raise ImportError("Please install python-telegram-bot==22.3")

# CONFIG - Validate environment variables
def get_env_var(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} is required but not set")
    return value

try:
    BOT_TOKEN = get_env_var('BOT_TOKEN')
    ADMIN_ID = int(get_env_var('ADMIN_ID'))
    MONGO_URI = get_env_var('MONGO_URI')
    MONGO_DB = get_env_var('MONGO_DB', 'telegram_bot')
    PORT = int(get_env_var('PORT', '8443'))
    WEBHOOK_URL = get_env_var('WEBHOOK_URL', '')
    USE_WEBHOOK = WEBHOOK_URL.lower() == 'true'
except ValueError as e:
    logger.error(f"Configuration error: {e}")
    raise

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
        logger.info("Attempting MongoDB connection")
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
            logger.error(f"MongoDB connection error: {str(e)}")
            save_log("errors", {
                "error": f"MongoDB connection failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })
            raise

    def ensure_connection(self):
        try:
            self.client.admin.command('ping')
            logger.debug("MongoDB connection check passed")
        except Exception:
            logger.warning("MongoDB connection lost, reconnecting...")
            self.connect()

# Initialize MongoDB manager
mongo_manager = MongoDBManager()

# GLOBAL DATA
df = pd.DataFrame()

# Helper function to escape MarkdownV2 special characters
def escape_markdown(text: str) -> str:
    """Escape all special characters for MarkdownV2."""
    if not text:
        return 'N/A'
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text.replace('\U0001fabd', '\\🪽')  # Explicitly escape emoji

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
        section_output = [f"{emojis[section]} {section}\:"]
        for field in fields:
            value = record.get(field, "Not Available")
            if pd.isna(value) or value == "":
                value = "Not Available"
            section_output.append(f"\- {escape_markdown(str(field))}\: {escape_markdown(str(value))}")
        output.append("\n".join(section_output))
    
    return output

# Helper to send admin notification
async def notify_admin(context, user_id, username, query, column, student_name):
    message = (
        f"📢 New Search by User\:\n"
        f"🆔 User ID\: {user_id}\n"
        f"🔗 Username\: {escape_markdown(f'@{username}' if username else 'N/A')}\n"
        f"🔍 Query\: {escape_markdown(query)} (in {escape_markdown(column)})\n"
        f"👤 Student\: {escape_markdown(student_name)}\n"
        f"⏰ Timestamp\: {datetime.now().isoformat()}"
    )
    for attempt in range(3):
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=message, parse_mode='MarkdownV2')
            logger.info(f"Sent admin notification for user {user_id}, student {student_name}")
            break
        except telegram.error.BadRequest as e:
            logger.error(f"Error sending admin notification for user {user_id}, attempt {attempt + 1}: {e}")
            if attempt == 2:
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Failed to send admin notification for student {student_name} after 3 attempts: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
            await asyncio.sleep(1)

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
        combined_df = combined_df.drop_duplicates()
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
        users = [user['user_id'] for user in db.users_collection.find()]
        logger.info(f"Loaded authorized users: {users}")
        return users
    except Exception as e:
        logger.error(f"Error loading authorized users: {str(e)}")
        save_log("errors", {
            "error": f"Failed to load authorized users: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        return []

def save_authorized_user(user_id, retries=3):
    db = get_db()
    for attempt in range(retries):
        try:
            db.users_collection.update_one(
                {'user_id': user_id},
                {'$set': {'user_id': user_id, 'added_at': datetime.now()}},
                upsert=True
            )
            updated_doc = db.users_collection.find_one({'user_id': user_id})
            if updated_doc:
                logger.info(f"Successfully saved authorized user: {user_id}")
                return True
            else:
                logger.warning(f"Verification failed for saving authorized user: {user_id}, attempt {attempt + 1}")
                if attempt == retries - 1:
                    raise Exception("Failed to verify saved authorized user after retries")
                time.sleep(1)
        except Exception as e:
            logger.error(f"Error saving authorized user {user_id}, attempt {attempt + 1}: {str(e)}")
            if attempt == retries - 1:
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Failed to save authorized user after {retries} attempts: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                raise
            time.sleep(1)
    return False

def load_blocked_users():
    db = get_db()
    try:
        blocked = [user['user_id'] for user in db.blocked_collection.find()]
        logger.info(f"Loaded blocked users: {blocked}")
        return blocked
    except Exception as e:
        logger.error(f"Error loading blocked users: {str(e)}")
        save_log("errors", {
            "error": f"Failed to load blocked users: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        return []

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
        save_log("errors", {
            "user_id": user_id,
            "error": f"Failed to save blocked user: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        raise

def remove_blocked_user(user_id):
    db = get_db()
    try:
        db.blocked_collection.delete_one({'user_id': user_id})
        logger.info(f"Removed blocked user: {user_id}")
    except Exception as e:
        logger.error(f"Error removing blocked user {user_id}: {str(e)}")
        save_log("errors", {
            "user_id": user_id,
            "error": f"Failed to remove blocked user: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        raise

def load_access_count():
    db = get_db()
    try:
        counts = {}
        for doc in db.access_collection.find():
            user_id = str(doc['user_id'])
            counts[user_id] = {
                'count': doc.get('count', 0),
                'total_limit': doc.get('total_limit', 1),
                'last_updated': doc.get('last_updated', datetime.now())
            }
        logger.info(f"Freshly loaded access counts: {counts}")
        return counts
    except Exception as e:
        logger.error(f"Error loading access counts: {str(e)}")
        save_log("errors", {
            "error": f"Failed to load access counts: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
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
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Failed to save access count after {retries} attempts: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                raise
            time.sleep(1)
    return False

def load_logs():
    db = get_db()
    try:
        log_doc = db.logs_collection.find_one() or {
            "access_requests": [], "searches": [], "approvals": [], "feedbacks": [], "errors": []
        }
        logger.info(f"Loaded logs: {list(log_doc.keys())}")
        return log_doc
    except Exception as e:
        logger.error(f"Error loading logs: {str(e)}")
        save_log("errors", {
            "error": f"Failed to load logs: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        return {"access_requests": [], "searches": [], "approvals": [], "feedbacks": [], "errors": []}

def save_log(log_type, log_data):
    db = get_db()
    try:
        db.logs_collection.update_one(
            {},
            {'$push': {log_type: log_data}},
            upsert=True
        )
        logger.info(f"Saved log type {log_type}: {log_data}")
    except Exception as e:
        logger.error(f"Error saving log type {log_type}: {str(e)}")
        try:
            db.logs_collection.update_one(
                {},
                {'$push': {"errors": {
                    "error": f"Failed to save log type {log_type}: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                }}},
                upsert=True
            )
        except Exception as inner_e:
            logger.error(f"Failed to save error log: {inner_e}")

def load_feedback():
    db = get_db()
    try:
        feedback = list(db.feedback_collection.find().sort("timestamp", -1).limit(100))
        logger.info(f"Loaded feedback: {len(feedback)} entries")
        return feedback
    except Exception as e:
        logger.error(f"Error loading feedback: {str(e)}")
        save_log("errors", {
            "error": f"Failed to load feedback: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        return []

def save_feedback_data(feedback_data):
    db = get_db()
    try:
        db.feedback_collection.insert_one(feedback_data)
        logger.info(f"Saved feedback: {feedback_data}")
    except Exception as e:
        logger.error(f"Error saving feedback: {str(e)}")
        save_log("errors", {
            "error": f"Failed to save feedback: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        raise

# ---------- Bot Commands -------------
async def check_blocked(user_id, update, context):
    blocked = load_blocked_users()
    if user_id in blocked:
        await update.message.reply_text("❌ You are blocked from using this bot\\.", parse_mode='MarkdownV2')
        logger.warning(f"Blocked user {user_id} attempted to use command")
        return True
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return

    authorized = load_authorized_users()
    if user_id in authorized or user_id == ADMIN_ID:
        await update.message.reply_text(
            f"🔥 Welcome to sniper's Bot\\! 🔥\n\n"
            f"✅ You already have access\\.\n\n"
            f"📋 Commands\\:\n"
            f"/name \\<query\\> \\- Search by name\n"
            f"/email \\<query\\> \\- Search by email\n"
            f"/phone \\<query\\> \\- Search by phone\n"
            f"/profile \\- View usage stats\n"
            f"/feedback \\<message\\> \\- Send feedback\n"
            f"/help \\- Show commands",
            parse_mode='MarkdownV2'
        )
    else:
        try:
            await update.message.reply_text("🔥 Welcome to sniper's Bot\\! 🔥\n\n🔐 Access request sent to admin\\. Please wait for approval\\.", parse_mode='MarkdownV2')
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
            ]])
            msg = (
                f"🔐 New Access Request\\:\n"
                f"👤 Name\\: {escape_markdown(update.message.from_user.full_name)}\n"
                f"🔗 Username\\: {escape_markdown(f'@{update.message.from_user.username}' if update.message.from_user.username else 'N/A')}\n"
                f"🆔 ID\\: {user_id}"
            )
            for attempt in range(3):
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text=msg, reply_markup=keyboard, parse_mode='MarkdownV2')
                    save_log("access_requests", {
                        "user_id": user_id,
                        "name": update.message.from_user.full_name,
                        "username": update.message.from_user.username or 'N/A',
                        "timestamp": update.message.date.isoformat()
                    })
                    break
                except telegram.error.BadRequest as e:
                    logger.error(f"Error sending access request to admin {ADMIN_ID}, attempt {attempt + 1}: {e}")
                    if attempt == 2:
                        await update.message.reply_text(
                            f"⚠️ Failed to send access request to admin\\. Please try again later or contact {escape_markdown('@Darksniperrx')}\\.",
                            parse_mode='MarkdownV2'
                        )
                        save_log("errors", {
                            "user_id": user_id,
                            "error": f"Failed to send access request to admin after 3 attempts: {str(e)}",
                            "timestamp": datetime.now().isoformat()
                        })
                        return
                    await asyncio.sleep(1)
        except Exception as e:
            error_msg = escape_markdown(str(e))
            logger.error(f"Error in start command for user {user_id}: {error_msg}")
            await update.message.reply_text(f"❌ An error occurred while processing your request\\. Contact {escape_markdown('@Darksniperrx')}\\.", parse_mode='MarkdownV2')
            save_log("errors", {
                "user_id": user_id,
                "error": f"Start command failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    logger.info(f"Help command invoked by user {user_id} ({update.message.from_user.full_name} @{update.message.from_user.username or 'N/A'})")
    if await check_blocked(user_id, update, context):
        return

    admin_username = escape_markdown("@Darksniperrx")
    if user_id == ADMIN_ID:
        help_text = (
            f"🔐 Admin Commands 🔐\n\n"
            f"\/start \\- Request access to the bot\n"
            f"\/help \\- Show this help menu\n"
            f"\/name \\<query\\> \\- Search by name\n"
            f"\/email \\<query\\> \\- Search by email\n"
            f"\/phone \\<query\\> \\- Search by phone\n"
            f"\/listexcel \\- List available Excel files\n"
            f"\/reload \\- Reload Excel data\n"
            f"\/profile \\- View your usage stats\n"
            f"\/userinfo \\<user_id\\> \\- View user details\n"
            f"\/feedback \\<message\\> \\- Submit feedback\n"
            f"\/broadcast \\<message\\> \\- Send message to all users\n"
            f"\/addaccess \\<user_id\\> \\<count\\> \\- Grant user access\n"
            f"\/block \\<user_id\\> \\- Block a user\n"
            f"\/unblock \\<user_id\\> \\- Unblock a user\n"
            f"\/logs \\- View recent logs\n"
            f"\/analytics \\- View bot analytics\n"
            f"\/replyfeedback \\<user_id\\> \\<message\\> \\- Reply to feedback\n"
            f"\/exportusers \\- Export authorized users\n"
            f"\/health \\- Check bot health\n\n"
            f"🤖 Powered by {admin_username}\\!"
        )
    else:
        help_text = (
            f"🔍 Sniper's Bot Commands 🔍\n\n"
            f"\/start \\- Request access to the bot\n"
            f"\/help \\- Show this help menu\n"
            f"\/name \\<query\\> \\- Search by name\n"
            f"\/email \\<query\\> \\- Search by email\n"
            f"\/phone \\<query\\> \\- Search by phone\n"
            f"\/profile \\- View your usage stats\n"
            f"\/feedback \\<message\\> \\- Submit feedback\n\n"
            f"🤖 Powered by {admin_username}\\!"
        )

    try:
        logger.info(f"Sending help text (length: {len(help_text)}): {help_text[:500]}...")  # Debug log
        await update.message.reply_text(help_text, parse_mode='MarkdownV2')
        save_log("help_command", {
            "user_id": user_id,
            "user_name": update.message.from_user.full_name,
            "user_username": update.message.from_user.username or 'N/A',
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Help command failed: {error_msg}")
        await update.message.reply_text(f"❌ Error displaying help menu: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Failed to send help message: {str(e)}",
            "timestamp": datetime.now().isoformat(),
            "user_name": update.message.from_user.full_name,
            "user_username": update.message.from_user.username or 'N/A'
        })

async def listexcel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can list Excel files\\.", parse_mode='MarkdownV2')
        return
    excel_files = get_excel_files()
    if not excel_files:
        await update.message.reply_text("❌ No Excel files found\\.", parse_mode='MarkdownV2')
        return
    files_list = "\n".join([f"\- {escape_markdown(f)}" for f in excel_files])
    await update.message.reply_text(f"📄 Available Excel files\\:\n{files_list}", parse_mode='MarkdownV2')

async def reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can reload data\\.", parse_mode='MarkdownV2')
        return
    global df
    df = load_all_excels()
    await update.message.reply_text(
        f"✅ Reloaded data from all Excel files\\. DataFrame has {len(df)} rows, columns: {', '.join([escape_markdown(c) for c in df.columns]) if not df.empty else 'None'}\\.",
        parse_mode='MarkdownV2'
    )

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    authorized = load_authorized_users()
    if user_id not in authorized and user_id != ADMIN_ID:
        await update.message.reply_text("🔒 You are not authorized\\. Use /start to request access\\.", parse_mode='MarkdownV2')
        return
    access_count = load_access_count()
    user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
    count = user_data['count']
    total_limit = user_data['total_limit']
    remaining = max(0, user_data['total_limit'] - count) if user_id != ADMIN_ID else "Unlimited"
    await update.message.reply_text(
        f"👤 User ID\\: {user_id}\n"
        f"🔎 Searches used\\: {count}\n"
        f"📊 Total limit\\: {total_limit}\n"
        f"📉 Remaining\\: {remaining}",
        parse_mode='MarkdownV2'
    )

async def userinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view user info\\.", parse_mode='MarkdownV2')
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /userinfo \\<user_id\\>", parse_mode='MarkdownV2')
        return
    try:
        target_user = int(context.args[0])
        access_count = load_access_count()
        user_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        count = user_data['count']
        total_limit = user_data['total_limit']
        remaining = max(0, total_limit - count)
        authorized = target_user in load_authorized_users()
        blocked = target_user in load_blocked_users()
        feedback = [f for f in load_feedback() if f['user_id'] == target_user]
        feedback_text = "\n".join([f"{escape_markdown(f['message'])} ({f['timestamp']})" for f in feedback]) or "No feedback"
        await update.message.reply_text(
            f"👤 User ID\\: {target_user}\n"
            f"🔓 Authorized\\: {'Yes' if authorized else 'No'}\n"
            f"🚫 Blocked\\: {'Yes' if blocked else 'No'}\n"
            f"🔎 Searches used\\: {count}\n"
            f"📊 Total limit\\: {total_limit}\n"
            f"📉 Remaining\\: {remaining}\n"
            f"📝 Feedback\\:\n{escape_markdown(feedback_text)}",
            parse_mode='MarkdownV2'
        )
    except ValueError:
        await update.message.reply_text("Invalid user ID\\.", parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error fetching user info\\: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Userinfo failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if len(context.args) == 0:
        await update.message.reply_text("Usage: /feedback \\<your message\\>", parse_mode='MarkdownV2')
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
        await update.message.reply_text("✅ Feedback received\\. Thank you\\!", parse_mode='MarkdownV2')
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"📢 New feedback from {escape_markdown(update.message.from_user.full_name)} ({escape_markdown(f'@{update.message.from_user.username}' if update.message.from_user.username else 'N/A')})\\:\n{escape_markdown(msg)}",
                    parse_mode='MarkdownV2'
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending feedback to admin {ADMIN_ID}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {
                        "user_id": user_id,
                        "error": f"Failed to send feedback to admin after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                    break
                await asyncio.sleep(1)
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error in feedback for user {user_id}: {error_msg}")
        await update.message.reply_text(f"❌ Error saving feedback\\. Please try again\\.", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Feedback command failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def perform_search(update: Update, context: ContextTypes.DEFAULT_TYPE, column: str):
    global df
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return

    authorized = load_authorized_users()
    access_count = load_access_count()
    user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
    count = user_data['count']
    total_limit = user_data['total_limit']
    logger.info(f"Performing search for user {user_id} ({update.message.from_user.full_name} @{update.message.from_user.username or 'N/A'}): count={count}, total_limit={total_limit}, column={column}")

    if user_id != ADMIN_ID and user_id not in authorized:
        await update.message.reply_text("🔒 You are not authorized\\. Use /start to request access\\.", parse_mode='MarkdownV2')
        return

    if user_id != ADMIN_ID and count >= total_limit:
        await update.message.reply_text(
            f"⚠️ Your search limit is reached\\. Current: count={count}, total_limit={total_limit}\\. Contact {escape_markdown('@Darksniperrx')} for more searches\\.", parse_mode='MarkdownV2'
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
            await update.message.reply_text("❗ No Excel data loaded\\. Contact admin to upload Excel files\\.", parse_mode='MarkdownV2')
            return

    if not context.args:
        await update.message.reply_text(f"Usage: /{column.lower()} \\<query\\>", parse_mode='MarkdownV2')
        return

    try:
        query = " ".join(context.args).strip().lower()
        logger.info(f"Searching for query '{query}' in column '{column}'")
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame. Available columns: {list(df.columns)}")
            await update.message.reply_text(
                f"❌ Column '{escape_markdown(column)}' not found in Excel data\\. Available columns: {', '.join([escape_markdown(c) for c in df.columns])}",
                parse_mode='MarkdownV2'
            )
            return

        matches = df[df[column].fillna('').astype(str).str.lower().str.contains(query, na=False)]
        logger.info(f"Found {len(matches)} matches for query '{query}' in column '{column}'")

        if matches.empty:
            await update.message.reply_text(f"❌ No matching records found for '{escape_markdown(query)}' in {column}\\", parse_mode='MarkdownV2')
            return

        context.user_data['search_results'] = matches.to_dict(orient='records')
        context.user_data['search_query'] = query
        context.user_data['search_column'] = column
        context.user_data['current_page'] = 0
        context.user_data['results_per_page'] = 10

        if user_id != ADMIN_ID and len(matches) == 1:
            if not save_access_count(user_id, count + 1, total_limit):
                await update.message.reply_text("❌ Error updating search count\\. Please try again\\.", parse_mode='MarkdownV2')
                return
            logger.info(f"Incremented search count for user {user_id} to {count + 1}/{total_limit} for single result")

        if len(matches) == 1:
            formatted_sections = format_student_record(matches.iloc[0])
            for section in formatted_sections:
                await update.message.reply_text(section, parse_mode='MarkdownV2')
            # Notify admin
            if user_id != ADMIN_ID:
                student_name = escape_markdown(matches.iloc[0].get('Name', 'Unknown'))
                await notify_admin(context, user_id, update.message.from_user.username, query, column, student_name)
            # Save log with student name
            save_log("searches", {
                "user_id": user_id,
                "query": query,
                "column": column,
                "student_name": matches.iloc[0].get('Name', 'Unknown'),
                "result_count": len(matches),
                "timestamp": datetime.now().isoformat()
            })
        else:
            await send_paginated_results(update, context)
            return

    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error in search for user {user_id}: {error_msg}")
        await update.message.reply_text(f"❌ Search failed: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Search failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def send_paginated_results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    results = context.user_data.get('search_results', [])
    query = context.user_data.get('search_query', '')
    column = context.user_data.get('search_column', '')
    current_page = context.user_data.get('current_page', 0)
    results_per_page = context.user_data.get('results_per_page', 10)

    if not results:
        await update.message.reply_text("❌ No search results available\\.", parse_mode='MarkdownV2')
        return

    total_results = len(results)
    total_pages = (total_results + results_per_page - 1) // results_per_page
    start_idx = current_page * results_per_page
    end_idx = min(start_idx + results_per_page, total_results)

    summary_text = f"Found {total_results} matches for '{escape_markdown(query)}' in {escape_markdown(column)}\\. Showing {start_idx + 1}\-{end_idx} of {total_results}\\:\n\n"
    buttons = []
    for idx, record in enumerate(results[start_idx:end_idx], start=start_idx):
        course = escape_markdown(record.get('Course', 'Unknown'))
        name = escape_markdown(record.get('Name', 'Unknown'))
        summary_text += f"{idx + 1}\\. {name} ({course})\n"
        buttons.append([InlineKeyboardButton(f"{name} ({course})", callback_data=f"select_{idx}")])

    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"page_{current_page - 1}"))
    if end_idx < total_results:
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"page_{current_page + 1}"))
    if nav_buttons:
        buttons.append(nav_buttons)

    keyboard = InlineKeyboardMarkup(buttons)
    logger.info(f"Sending paginated results: page {current_page + 1}/{total_pages}, showing {start_idx + 1}-{end_idx}, text length: {len(summary_text)}")
    
    try:
        if isinstance(update, telegram.Update) and update.callback_query:
            await update.callback_query.edit_message_text(summary_text, reply_markup=keyboard, parse_mode='MarkdownV2')
        else:
            await update.message.reply_text(summary_text, reply_markup=keyboard, parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error sending paginated results: {error_msg}")
        await update.message.reply_text(f"❌ Failed to send results: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "error": f"Failed to send paginated results: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def search_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Name')

async def search_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Student Email')

async def search_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await perform_search(update, context, 'Student Mobile')

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can upload files\\.", parse_mode='MarkdownV2')
        return

    doc: Document = update.message.document
    file_name = doc.file_name
    is_csv = file_name.lower().endswith(".csv")
    is_xlsx = file_name.lower().endswith(".xlsx")

    if not (is_csv or is_xlsx):
        await update.message.reply_text("❌ Only \\.csv or \\.xlsx files allowed\\.", parse_mode='MarkdownV2')
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
                error_msg = escape_markdown(f"Error reading CSV file: {str(e)}")
                logger.error(error_msg)
                await update.message.reply_text(f"❌ {error_msg}", parse_mode='MarkdownV2')
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"CSV read failed: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                return

            columns_found = set(csv_df.columns)
            required_columns = {'Name', 'Student Email', 'Student Mobile', 'Course'}
            if not required_columns.issubset(columns_found):
                missing = required_columns - columns_found
                await update.message.reply_text(
                    f"❌ File missing required columns: {', '.join([escape_markdown(c) for c in missing])}",
                    parse_mode='MarkdownV2'
                )
                return

            xlsx_stream = io.BytesIO()
            csv_df.to_excel(xlsx_stream, index=False, engine='openpyxl')
            xlsx_stream.seek(0)
            xlsx_file_name = file_name.rsplit('.', 1)[0] + '.xlsx'
            save_excel_to_gridfs(xlsx_stream, xlsx_file_name)
            await update.message.reply_text(
                f"✅ CSV file {escape_markdown(file_name)} converted to {escape_markdown(xlsx_file_name)} and uploaded\\.",
                parse_mode='MarkdownV2'
            )
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
                await update.message.reply_text(
                    f"❌ Excel file missing required columns: {', '.join([escape_markdown(c) for c in missing])}",
                    parse_mode='MarkdownV2'
                )
                return

            save_excel_to_gridfs(file_stream, file_name)
            await update.message.reply_text(f"✅ Excel file {escape_markdown(file_name)} uploaded\\.", parse_mode='MarkdownV2')

        global df
        df = load_all_excels()
        await update.message.reply_text(
            f"✅ Data reloaded\\. DataFrame has {len(df)} rows, columns: {', '.join([escape_markdown(c) for c in df.columns]) if not df.empty else 'None'}\\.",
            parse_mode='MarkdownV2'
        )
    except Exception as e:
        error_msg = escape_markdown(f"Error processing file {file_name}: {str(e)}")
        logger.error(error_msg)
        await update.message.reply_text(f"❌ {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"File upload failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can broadcast\\.", parse_mode='MarkdownV2')
        return

    if not context.args:
        await update.message.reply_text("Usage: /broadcast \\<message\\>", parse_mode='MarkdownV2')
        return

    msg = " ".join(context.args)
    authorized = load_authorized_users()
    total_sent = 0
    failed_users = []

    for uid in authorized:
        for attempt in range(3):
            try:
                broadcast_text = f"📢 Broadcast from sniper\\:\n\n{escape_markdown(msg)}"
                logger.info(f"Sending broadcast to user {uid} (length: {len(broadcast_text)}): {broadcast_text[:100]}...")
                await context.bot.send_message(chat_id=uid, text=broadcast_text, parse_mode='MarkdownV2')
                total_sent += 1
                break
            except telegram.error.Forbidden as e:
                logger.warning(f"Broadcast failed for user {uid}: Bot is blocked")
                failed_users.append(uid)
                save_log("errors", {
                    "user_id": uid,
                    "error": f"Broadcast failed: Bot is blocked by user",
                    "timestamp": datetime.now().isoformat()
                })
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Broadcast error to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    failed_users.append(uid)
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Broadcast failed after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)

    await update.message.reply_text(
        f"✅ Broadcast sent to {total_sent}/{len(authorized)} users\\.\n"
        f"{'Failed users: ' + ', '.join(map(str, failed_users)) if failed_users else 'No failures\\.'}",
        parse_mode='MarkdownV2'
    )
    save_log("broadcast", {
        "admin_id": user_id,
        "message": msg,
        "success_count": total_sent,
        "failed_users": failed_users,
        "timestamp": datetime.now().isoformat()
    })

async def addaccess(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can add access\\.", parse_mode='MarkdownV2')
        return

    if len(context.args) != 2:
        await update.message.reply_text("Usage: /addaccess \\<user_id\\> \\<count\\>", parse_mode='MarkdownV2')
        return

    try:
        target_user = int(context.args[0])
        add_count = int(context.args[1])
        if add_count <= 0:
            await update.message.reply_text("Count must be positive\\.", parse_mode='MarkdownV2')
            return
    except ValueError:
        await update.message.reply_text("Invalid arguments\\. User ID and count must be numbers\\.", parse_mode='MarkdownV2')
        return

    try:
        access_count = load_access_count()
        user_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        current_count = user_data['count']
        current_limit = user_data['total_limit']
        new_limit = current_limit + add_count
        if not save_access_count(target_user, current_count, new_limit):
            await update.message.reply_text(f"❌ Failed to update limit for user {target_user}\\. Please try again\\.", parse_mode='MarkdownV2')
            return
        
        access_count = load_access_count()
        updated_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        if updated_data['total_limit'] != new_limit:
            logger.error(f"Error: total_limit not updated correctly for user {target_user}. Expected {new_limit}, got {updated_data['total_limit']}")
            await update.message.reply_text(f"❌ Failed to verify updated limit for user {target_user}\\. Please try again\\.", parse_mode='MarkdownV2')
            save_log("errors", {
                "user_id": target_user,
                "error": f"Failed to verify total_limit: expected {new_limit}, got {updated_data['total_limit']}",
                "timestamp": datetime.now().isoformat()
            })
            return

        await update.message.reply_text(
            f"✅ Added {add_count} searches for user {target_user}\\. Total limit: {new_limit}, Used: {current_count}, Remaining: {new_limit - current_count}",
            parse_mode='MarkdownV2'
        )
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=target_user,
                    text=f"✅ sniper has added {add_count} searches to your limit\\. Total limit: {new_limit}, Used: {current_count}, Remaining: {new_limit - current_count}",
                    parse_mode='MarkdownV2'
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying user {target_user}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    await update.message.reply_text(
                        f"⚠️ Added searches but could not notify user {target_user}: {escape_markdown(str(e))}",
                        parse_mode='MarkdownV2'
                    )
                    save_log("errors", {
                        "user_id": target_user,
                        "error": f"Failed to notify user after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error in addaccess for user {target_user}: {error_msg}")
        await update.message.reply_text(f"❌ Error adding access for user {target_user}: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": target_user,
            "error": f"Addaccess failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can block users\\.", parse_mode='MarkdownV2')
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /block \\<user_id\\>", parse_mode='MarkdownV2')
        return
    try:
        uid = int(context.args[0])
        if uid == ADMIN_ID:
            await update.message.reply_text("❌ Cannot block the admin\\.", parse_mode='MarkdownV2')
            return
        save_blocked_user(uid)
        await update.message.reply_text(f"✅ Blocked user {uid}", parse_mode='MarkdownV2')
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text="❌ You have been blocked from using sniper's Bot\\.", parse_mode='MarkdownV2')
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying blocked user {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to notify blocked user after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except ValueError:
        await update.message.reply_text("Invalid user ID\\.", parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error blocking user: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Block user failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can unblock users\\.", parse_mode='MarkdownV2')
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /unblock \\<user_id\\>", parse_mode='MarkdownV2')
        return
    try:
        uid = int(context.args[0])
        remove_blocked_user(uid)
        await update.message.reply_text(f"✅ Unblocked user {uid}", parse_mode='MarkdownV2')
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text="✅ You have been unblocked and can now use sniper's Bot\\.", parse_mode='MarkdownV2')
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying unblocked user {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to notify unblocked user after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except ValueError:
        await update.message.reply_text("Invalid user ID\\.", parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error unblocking user: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Unblock user failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view logs\\.", parse_mode='MarkdownV2')
        return
    try:
        logs = load_logs()
        text = "📜 Recent Logs by sniper\\:\n"
        log_types = ["access_requests", "searches", "approvals", "feedbacks", "errors"]
        for log_type in log_types:
            entries = logs.get(log_type, [])
            if entries:
                text += f"\n🔹 {escape_markdown(log_type.upper())} (Last {min(len(entries), 5)})\\:\n"
                for entry in entries[-5:]:
                    entry_text = escape_markdown(json.dumps(entry, indent=2, default=str))
                    if len(text) + len(entry_text) + 100 < 4000:
                        text += entry_text + "\n"
                    else:
                        text += "... (Truncated due to message length)\n"
                        break
            else:
                text += f"\n🔹 {escape_markdown(log_type.upper())}\\:\ None\n"
        if text == "📜 Recent Logs by sniper\\:\n":
            text = "📜 No logs available\\."
        await update.message.reply_text(text[:4000], parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error fetching logs: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Logs command failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def analytics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can view analytics\\.", parse_mode='MarkdownV2')
        return
    try:
        db = get_db()
        log_doc = db.logs_collection.find_one() or {
            "access_requests": [], "searches": [], "approvals": [], "feedbacks": [], "errors": []
        }
        total_searches = len(log_doc.get("searches", []))
        total_feedbacks = db.feedback_collection.count_documents({})
        total_users = db.users_collection.count_documents({})
        total_excel_files = len(get_excel_files())
        total_blocked = db.blocked_collection.count_documents({})
        analytics_text = (
            f"📊 Sniper's Bot Stats\\:\n\n"
            f"👥 Authorized Users\\: {total_users}\n"
            f"🚫 Blocked Users\\: {total_blocked}\n"
            f"🔍 Searches\\: {total_searches}\n"
            f"📝 Feedbacks\\: {total_feedbacks}\n"
            f"📄 Excel Files\\: {total_excel_files}"
        )
        logger.info(f"Sending analytics text (length: {len(analytics_text)}): {analytics_text[:500]}...")
        await update.message.reply_text(analytics_text, parse_mode='MarkdownV2')
        save_log("analytics", {
            "admin_id": user_id,
            "total_users": total_users,
            "total_blocked": total_blocked,
            "total_searches": total_searches,
            "total_feedbacks": total_feedbacks,
            "total_excel_files": total_excel_files,
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Analytics failed: {error_msg}")
        await update.message.reply_text(f"❌ Error fetching analytics: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Analytics failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def replyfeedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can reply to feedback\\.", parse_mode='MarkdownV2')
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /replyfeedback \\<user_id\\> \\<message\\>", parse_mode='MarkdownV2')
        return
    try:
        uid = int(context.args[0])
        msg = escape_markdown(" ".join(context.args[1:]))
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 Reply from sniper\\:\n{msg}",
                    parse_mode='MarkdownV2'
                )
                await update.message.reply_text("✅ Feedback reply sent\\.", parse_mode='MarkdownV2')
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending feedback reply to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    await update.message.reply_text(
                        f"❌ Could not send message to user {uid}: {escape_markdown(str(e))}",
                        parse_mode='MarkdownV2'
                    )
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to send feedback reply after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error replying to feedback: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Reply feedback failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def exportusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can export users\\.", parse_mode='MarkdownV2')
        return
    try:
        users = load_authorized_users()
        if not users:
            await update.message.reply_text("❌ No authorized users found to export\\.", parse_mode='MarkdownV2')
            return
        csv_buffer = io.StringIO()
        csv_buffer.write("user_id\n")
        for u in users:
            csv_buffer.write(f"{u}\n")
        csv_buffer.seek(0)
        await update.message.reply_document(
            document=InputFile(csv_buffer, filename="authorized_users.csv"),
            caption="✅ Exported authorized users\\.",
            parse_mode='MarkdownV2'
        )
        csv_buffer.close()
    except Exception as e:
        error_msg = escape_markdown(str(e))
        await update.message.reply_text(f"❌ Error exporting users: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Export users failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    if await check_blocked(user_id, update, context):
        await query.edit_message_text("❌ You are blocked from using this bot\\.", parse_mode='MarkdownV2')
        return

    try:
        if data.startswith("search_"):
            parts = data.split("_", 3)
            if len(parts) < 4:
                await query.edit_message_text("❌ Invalid search data\\.", parse_mode='MarkdownV2')
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid search data format: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            column, query_text, student_name = parts[1], parts[2], parts[3]
            context.user_data['current_query'] = {'column': column, 'query_text': query_text, 'student_name': student_name}
            context.user_data['current_page'] = 1
            await send_paginated_results(update, context)
            save_log("searches", {
                "user_id": user_id,
                "query": query_text,
                "column": column,
                "student_name": student_name,
                "result_count": 1,
                "timestamp": datetime.now().isoformat()
            })
        elif data.startswith("page_"):
            try:
                page = int(data.split("_")[1])
            except ValueError:
                await query.edit_message_text("❌ Invalid page number\\.", parse_mode='MarkdownV2')
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid page number in callback: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            context.user_data['current_page'] = page
            await send_paginated_results(update, context)
        else:
            await query.edit_message_text("❌ Invalid callback data\\.", parse_mode='MarkdownV2')
            save_log("errors", {
                "user_id": user_id,
                "error": f"Invalid callback data: {data}",
                "timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error in callback_handler for user {user_id}, data {data}: {error_msg}")
        await query.edit_message_text(f"❌ Error processing callback: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Callback handler failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can check health\\.", parse_mode='MarkdownV2')
        return
    try:
        db = get_db()
        db.client.admin.command('ping')
        excel_files = get_excel_files()
        total_users = db.users_collection.count_documents({})
        total_blocked = db.blocked_collection.count_documents({})
        total_searches = len(load_logs().get("searches", []))
        health_text = (
            f"🩺 Bot Health Check\\:\n\n"
            f"✅ Bot is running\n"
            f"📄 Excel Files: {len(excel_files)}\n"
            f"👥 Authorized Users: {total_users}\n"
            f"🚫 Blocked Users: {total_blocked}\n"
            f"🔍 Total Searches: {total_searches}\n"
            f"🗄 DataFrame Rows: {len(df)}\n"
            f"📡 MongoDB: Connected"
        )
        logger.info(f"Health check: {health_text[:500]}...")
        await update.message.reply_text(health_text, parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Health check failed: {error_msg}")
        await update.message.reply_text(f"❌ Health check failed: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Health check failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        error = context.error
        user_id = update.message.from_user.id if update and update.message else None
        error_msg = escape_markdown(str(error))
        logger.error(f"Update {update} caused error: {error_msg}")
        save_log("errors", {
            "user_id": user_id or "Unknown",
            "error": f"Bot error: {str(error)}",
            "update": str(update)[:500],
            "timestamp": datetime.now().isoformat()
        })
        if update and update.message:
            await update.message.reply_text(
                f"❌ An error occurred: {error_msg}\nPlease contact {escape_markdown('@Darksniperrx')}\\.",
                parse_mode='MarkdownV2'
            )
    except Exception as e:
        logger.error(f"Error in error_handler: {str(e)}")
        save_log("errors", {
            "error": f"Error handler failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def downloadone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    if user_id not in range(8000, 9601):  # IDs 8000-9600 have unlimited access
        authorized = load_authorized_users()
        if user_id not in authorized and user_id != ADMIN_ID:
            await update.message.reply_text("🔒 You are not authorized\\. Use /start to request access\\.", parse_mode='MarkdownV2')
            return
        access_count = load_access_count()
        user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
        count = user_data['count']
        total_limit = user_data['total_limit']
        if user_id != ADMIN_ID and count >= total_limit:
            await update.message.reply_text(
                f"⚠️ Your search limit is reached\\. Current: count={count}, total_limit={total_limit}\\. Contact {escape_markdown('@Darksniperrx')} for more searches\\.",
                parse_mode='MarkdownV2'
            )
            logger.warning(f"Download blocked for user {user_id}: count={count}, total_limit={total_limit}")
            return
    if not context.args:
        await update.message.reply_text("Usage: /downloadone \\<index\\>", parse_mode='MarkdownV2')
        return
    try:
        idx = int(context.args[0])
        if idx < 0 or idx >= len(df):
            await update.message.reply_text("❌ Invalid index\\.", parse_mode='MarkdownV2')
            return
        record = df.iloc[idx]
        formatted_sections = format_student_record(record)
        csv_buffer = io.StringIO()
        record.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        student_name = escape_markdown(record.get('Name', 'Unknown'))
        await update.message.reply_document(
            document=InputFile(csv_buffer, filename=f"{student_name}.csv"),
            caption=f"✅ Record for {student_name}",
            parse_mode='MarkdownV2'
        )
        csv_buffer.close()
        for section in formatted_sections:
            await update.message.reply_text(section, parse_mode='MarkdownV2')
        if user_id not in range(8000, 9601) and user_id != ADMIN_ID:
            if not save_access_count(user_id, count + 1, total_limit):
                await update.message.reply_text("❌ Error updating search count\\. Please try again\\.", parse_mode='MarkdownV2')
                return
            logger.info(f"Incremented search count for user {user_id} to {count + 1}/{total_limit} for downloadone")
            await notify_admin(context, user_id, update.message.from_user.username, f"Index {idx}", "downloadone", student_name)
            save_log("searches", {
                "user_id": user_id,
                "query": f"Index {idx}",
                "column": "downloadone",
                "student_name": student_name,
                "result_count": 1,
                "timestamp": datetime.now().isoformat()
            })
    except ValueError:
        await update.message.reply_text("Invalid index\\. Must be a number\\.", parse_mode='MarkdownV2')
    except Exception as e:
        error_msg = escape_markdown(str(e))
        logger.error(f"Error in downloadone for user {user_id}: {error_msg}")
        await update.message.reply_text(f"❌ Error downloading record: {error_msg}", parse_mode='MarkdownV2')
        save_log("errors", {
            "user_id": user_id,
            "error": f"Downloadone failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def main():
    try:
        logger.info("Starting bot initialization")
        app = ApplicationBuilder().token(BOT_TOKEN).build()
        
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("name", search_name))
        app.add_handler(CommandHandler("email", search_email))
        app.add_handler(CommandHandler("phone", search_phone))
        app.add_handler(CommandHandler("listexcel", listexcel))
        app.add_handler(CommandHandler("reload", reload))
        app.add_handler(CommandHandler("profile", profile))
        app.add_handler(CommandHandler("userinfo", userinfo))
        app.add_handler(CommandHandler("feedback", feedback))
        app.add_handler(CommandHandler("broadcast", broadcast))
        app.add_handler(CommandHandler("addaccess", addaccess))
        app.add_handler(CommandHandler("block", block))
        app.add_handler(CommandHandler("unblock", unblock))
        app.add_handler(CommandHandler("logs", logs))
        app.add_handler(CommandHandler("analytics", analytics))
        app.add_handler(CommandHandler("replyfeedback", replyfeedback))
        app.add_handler(CommandHandler("exportusers", exportusers))
        app.add_handler(CommandHandler("health", health_check))
        app.add_handler(CommandHandler("downloadone", downloadone))
        app.add_handler(MessageHandler(DOCUMENT_FILTER, handle_document))
        app.add_handler(CallbackQueryHandler(callback_handler))
        app.add_error_handler(error_handler)
        
        logger.info("Loading Excel data on startup")
        load_excel_on_startup()
        
        if USE_WEBHOOK:
            logger.info(f"Starting webhook on port {PORT}")
            await app.run_webhook(
                listen="0.0.0.0",
                port=PORT,
                webhook_url=WEBHOOK_URL,
                drop_pending_updates=True
            )
        else:
            logger.info("Starting polling")
            await app.run_polling(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        save_log("errors", {
            "error": f"Main function failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

if __name__ == '__main__':
    logger.info("Bot script started")
    asyncio.run(main())
