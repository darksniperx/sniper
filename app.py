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
            # Test connection
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

def remove_authorized_user(user_id):
    db = get_db()
    try:
        db.users_collection.delete_one({'user_id': user_id})
        logger.info(f"Removed authorized user: {user_id}")
    except Exception as e:
        logger.error(f"Error removing authorized user {user_id}: {str(e)}")
        save_log("errors", {
            "user_id": user_id,
            "error": f"Failed to remove authorized user: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        raise

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
        await update.message.reply_text("❌ You are blocked from using this bot.")
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
            f"🔥 Welcome to sniper's Bot! 🔥\n\n"
            f"✅ You already have access.\n\n"
            "📋 Commands:\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/profile - View usage stats\n"
            "/feedback <message> - Send feedback\n"
            "/help - Show commands\n"
            "/logout - Remove access"
        )
    else:
        try:
            await update.message.reply_text("🔥 Welcome to sniper's Bot! 🔥\n\n🔐 Access request sent to admin. Please wait for approval.")
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
            ]])
            msg = (
                f"🔐 New Access Request:\n"
                f"👤 Name: {update.message.from_user.full_name}\n"
                f"🔗 Username: @{update.message.from_user.username or 'N/A'}\n"
                f"🆔 ID: {user_id}"
            )
            for attempt in range(3):
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text=msg, reply_markup=keyboard)
                    save_log("access_requests", {
                        "user_id": user_id,
                        "name": update.message.from_user.full_name,
                        "username": update.message.from_user.username or 'N/A',
                        "timestamp": update.message.date.isoformat()
                    })
                    break
                except telegram.error.BadRequest as e:
                    logger.error(f"Error sending access request to admin {ADMIN_ID}, attempt {attempt + 1}: {str(e)}")
                    if attempt == 2:
                        await update.message.reply_text("⚠️ Failed to send access request to admin. Please try again later or contact @Darksniperrx.")
                        save_log("errors", {
                            "user_id": user_id,
                            "error": f"Failed to send access request to admin after 3 attempts: {str(e)}",
                            "timestamp": datetime.now().isoformat()
                        })
                        return
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error in start command for user {user_id}: {str(e)}")
            await update.message.reply_text("❌ An error occurred while processing your request. Contact @Darksniperrx.")
            save_log("errors", {
                "user_id": user_id,
                "error": f"Start command failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return

    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "📋 Bot Commands by sniper:\n"
            "/start - Request access\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/listexcel - List available Excel files (admin)\n"
            "/reload - Reload all Excel data (admin)\n"
            "/profile - Your usage stats\n"
            "/userinfo <user_id> - View user info (admin)\n"
            "/feedback <message> - Send feedback\n"
            "/broadcast <msg> - Admin only broadcast\n"
            "/addaccess <user_id> <count> - Admin adds access count\n"
            "/block <user_id> - Admin blocks user\n"
            "/unblock <user_id> - Admin unblocks user\n"
            "/logs - View recent logs (admin)\n"
            "/analytics - View bot stats (admin)\n"
            "/replyfeedback <user_id> <msg> - Reply to feedback (admin)\n"
            "/exportusers - Export authorized users (admin)\n"
            "/logout - Remove access\n"
            "/help - Show this message"
        )
    else:
        await update.message.reply_text(
            "📋 Bot Commands by sniper:\n"
            "/start - Request access\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/profile - View usage stats\n"
            "/feedback <message> - Send feedback\n"
            "/logout - Remove access\n"
            "/help - Show this message"
        )

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


# Flask app setup
app = Flask(__name__)
application = None  # Will hold the Telegram Application instance

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

def remove_authorized_user(user_id):
    db = get_db()
    try:
        db.users_collection.delete_one({'user_id': user_id})
        logger.info(f"Removed authorized user: {user_id}")
    except Exception as e:
        logger.error(f"Error removing authorized user {user_id}: {str(e)}")
        save_log("errors", {
            "user_id": user_id,
            "error": f"Failed to remove authorized user: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })
        raise

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
        await update.message.reply_text("❌ You are blocked from using this bot.")
        logger.warning(f"Blocked user {user_id} attempted to use command")
        return True
    return False

async def check_authorized(user_id, update, context):
    authorized = load_authorized_users()
    if user_id not in authorized and user_id != ADMIN_ID:
        await update.message.reply_text("❌ You are not authorized to use this command. Please use /start to request access.")
        logger.warning(f"Unauthorized user {user_id} attempted to use command")
        return False
    return True

async def check_access_limit(user_id, update, context):
    access_counts = load_access_count()
    user_data = access_counts.get(str(user_id), {'count': 0, 'total_limit': 1})
    if user_data['count'] >= user_data['total_limit']:
        await update.message.reply_text("❌ You have reached your access limit. Contact @Darksniperrx for more access.")
        logger.warning(f"User {user_id} exceeded access limit: {user_data['count']}/{user_data['total_limit']}")
        return False
    user_data['count'] += 1
    save_access_count(user_id, user_data['count'], user_data['total_limit'])
    logger.info(f"User {user_id} access count updated: {user_data['count']}/{user_data['total_limit']}")
    return True

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return

    authorized = load_authorized_users()
    if user_id in authorized or user_id == ADMIN_ID:
        await update.message.reply_text(
            f"🔥 Welcome to sniper's Bot! 🔥\n\n"
            f"✅ You already have access.\n\n"
            "📋 Commands:\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/profile - View usage stats\n"
            "/feedback <message> - Send feedback\n"
            "/help - Show commands\n"
            "/logout - Remove access"
        )
    else:
        try:
            await update.message.reply_text("🔥 Welcome to sniper's Bot! 🔥\n\n🔐 Access request sent to admin. Please wait for approval.")
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Approve", callback_data=f"approve_{user_id}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"reject_{user_id}")
            ]])
            msg = (
                f"🔐 New Access Request:\n"
                f"👤 Name: {update.message.from_user.full_name}\n"
                f"🔗 Username: @{update.message.from_user.username or 'N/A'}\n"
                f"🆔 ID: {user_id}"
            )
            for attempt in range(3):
                try:
                    await context.bot.send_message(chat_id=ADMIN_ID, text=msg, reply_markup=keyboard)
                    save_log("access_requests", {
                        "user_id": user_id,
                        "name": update.message.from_user.full_name,
                        "username": update.message.from_user.username or 'N/A',
                        "timestamp": update.message.date.isoformat()
                    })
                    break
                except telegram.error.BadRequest as e:
                    logger.error(f"Error sending access request to admin {ADMIN_ID}, attempt {attempt + 1}: {str(e)}")
                    if attempt == 2:
                        await update.message.reply_text("⚠️ Failed to send access request to admin. Please try again later or contact @Darksniperrx.")
                        save_log("errors", {
                            "user_id": user_id,
                            "error": f"Failed to send access request to admin after 3 attempts: {str(e)}",
                            "timestamp": datetime.now().isoformat()
                        })
                        return
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Error in start command for user {user_id}: {str(e)}")
            await update.message.reply_text("❌ An error occurred while processing your request. Contact @Darksniperrx.")
            save_log("errors", {
                "user_id": user_id,
                "error": f"Start command failed: {str(e)}",
                "timestamp": datetime.now().isoformat()
            })

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return

    if user_id == ADMIN_ID:
        await update.message.reply_text(
            "📋 Bot Commands by sniper:\n"
            "/start - Request access\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/listexcel - List available Excel files (admin)\n"
            "/reload - Reload all Excel data (admin)\n"
            "/profile - Your usage stats\n"
            "/userinfo <user_id> - View user info (admin)\n"
            "/feedback <message> - Send feedback\n"
            "/broadcast <msg> - Admin only broadcast\n"
            "/addaccess <user_id> <count> - Admin adds access count\n"
            "/block <user_id> - Admin blocks user\n"
            "/unblock <user_id> - Admin unblocks user\n"
            "/logs - View recent logs (admin)\n"
            "/analytics - View bot stats (admin)\n"
            "/replyfeedback <user_id> <msg> - Reply to feedback (admin)\n"
              "/exportusers - Export authorized users (admin)\n"
            "/logout - Remove access\n"
            "/help - Show this message"
        )
    else:
        await update.message.reply_text(
            "📋 Bot Commands by sniper:\n"
            "/start - Request access\n"
            "/name <query> - Search by name\n"
            "/email <query> - Search by email\n"
            "/phone <query> - Search by phone\n"
            "/profile - View usage stats\n"
            "/feedback <message> - Send feedback\n"
            "/logout - Remove access\n"
            "/help - Show this message"
        )

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

async def logout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if await check_blocked(user_id, update, context):
        return
    db = get_db()
    if user_id in load_authorized_users():
        remove_authorized_user(user_id)
        db.access_collection.delete_one({'user_id': user_id})
        await update.message.reply_text("❌ Access removed.")
    else:
        await update.message.reply_text("⚠️ You are not authorized.")

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
    remaining = max(0, user_data['total_limit'] - count) if user_id != ADMIN_ID else "Unlimited"
    await update.message.reply_text(
        f"👤 User ID: {user_id}\n"
        f"🔎 Searches used: {count}\n"
        f"📊 Total limit: {total_limit}\n"
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
        authorized = target_user in load_authorized_users()
        blocked = target_user in load_blocked_users()
        feedback = [f for f in load_feedback() if f['user_id'] == target_user]
        feedback_text = "\n".join([f["message"] + f" ({f['timestamp']})" for f in feedback]) or "No feedback"
        await update.message.reply_text(
            f"👤 User ID: {target_user}\n"
            f"🔓 Authorized: {'Yes' if authorized else 'No'}\n"
            f"🚫 Blocked: {'Yes' if blocked else 'No'}\n"
            f"🔎 Searches used: {count}\n"
            f"📊 Total limit: {total_limit}\n"
            f"📉 Remaining: {remaining}\n"
            f"📝 Feedback:\n{feedback_text}"
        )
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching user info: {str(e)}")
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
                    text=f"📢 New feedback from {update.message.from_user.full_name} (@{update.message.from_user.username or 'N/A'}):\n{msg}"
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending feedback to admin {ADMIN_ID}, attempt {attempt + 1}: {str(e)}")
                if attempt == 2:
                    save_log("errors", {
                        "user_id": user_id,
                        "error": f"Failed to send feedback to admin after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                    break
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in feedback for user {user_id}: {str(e)}")
        await update.message.reply_text("❌ Error saving feedback. Please try again.")
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
    logger.info(f"Performing search for user {user_id}: count={count}, total_limit={total_limit}, column={column}")

    if user_id != ADMIN_ID and user_id not in authorized:
        await update.message.reply_text("🔒 You are not authorized. Use /start to request access.")
        return

    if user_id != ADMIN_ID and count >= total_limit:
        await update.message.reply_text(
            f"⚠️ Your search limit is reached. Current: count={count}, total_limit={total_limit}. Contact @Darksniperrx for more searches."
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
            await update.message.reply_text("❗ No Excel data loaded. Contact admin to upload Excel files.")
            return

    if not context.args:
        await update.message.reply_text(f"Usage: /{column.lower()} <query>")
        return

    try:
        query = " ".join(context.args).strip().lower()
        logger.info(f"Searching for query '{query}' in column '{column}'")
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in DataFrame. Available columns: {list(df.columns)}")
            await update.message.reply_text(f"❌ Column '{column}' not found in Excel data. Available columns: {', '.join(df.columns)}")
            return

        matches = df[df[column].fillna('').astype(str).str.lower().str.contains(query, na=False)]
        logger.info(f"Found {len(matches)} matches for query '{query}' in column '{column}'")

        if matches.empty:
            await update.message.reply_text("❌ No matching records found.")
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
            json_text = json.dumps(context.user_data['search_results'], indent=2, default=str)
            logger.info(f"Sending single result, JSON length: {len(json_text)}")
            await update.message.reply_text(json_text)
        else:
            await send_paginated_results(update, context)
            return

        save_log("searches", {
            "user_id": user_id,
            "query": query,
            "column": column,
            "result_count": len(matches),
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"Error in search for user {user_id}: {str(e)}")
        await update.message.reply_text(f"❌ Search failed: {str(e)}")
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

        # Handle CSV or XLSX
        if is_csv:
            # Read CSV and convert to XLSX
            try:
                csv_df = pd.read_csv(file_stream)
                logger.info(f"Read CSV file {file_name} with {len(csv_df)} rows, columns: {list(csv_df.columns)}")
            except Exception as e:
                error_msg = f"❌ Error reading CSV file: {str(e)}"
                logger.error(error_msg)
                await update.message.reply_text(error_msg)
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"CSV read failed: {str(e)}",
                    "timestamp": datetime.now().isoformat()
                })
                return

            # Validate required columns
            columns_found = set(csv_df.columns)
            required_columns = {'Name', 'Student Email', 'Student Mobile', 'Course'}
            if not required_columns.issubset(columns_found):
                missing = required_columns - columns_found
                await update.message.reply_text(f"❌ File missing required columns: {', '.join(missing)}")
                return

            # Convert to XLSX
            xlsx_stream = io.BytesIO()
            csv_df.to_excel(xlsx_stream, index=False, engine='openpyxl')
            xlsx_stream.seek(0)
            # Use .xlsx extension for storage
            xlsx_file_name = file_name.rsplit('.', 1)[0] + '.xlsx'
            save_excel_to_gridfs(xlsx_stream, xlsx_file_name)
            await update.message.reply_text(f"✅ CSV file {file_name} converted to {xlsx_file_name} and uploaded.")
        else:
            # Handle XLSX directly
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

        # Reload data
        global df
        df = load_all_excels()
        await update.message.reply_text(f"✅ Data reloaded. DataFrame has {len(df)} rows, columns: {list(df.columns) if not df.empty else 'None'}.")
    except Exception as e:
        error_msg = f"❌ Error processing file {file_name}: {str(e)}"
        logger.error(error_msg)
        await update.message.reply_text(error_msg)
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
                await context.bot.send_message(chat_id=uid, text=f"📢 Broadcast from sniper:\n\n{msg}")
                total_sent += 1
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Broadcast error to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Broadcast failed after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
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
        add_count = int(context.args[1])
        if add_count <= 0:
            await update.message.reply_text("Count must be positive.")
            return
    except ValueError:
        await update.message.reply_text("Invalid arguments. User ID and count must be numbers.")
        return

    try:
        access_count = load_access_count()
        user_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        current_count = user_data['count']
        current_limit = user_data['total_limit']
        new_limit = current_limit + add_count
        if not save_access_count(target_user, current_count, new_limit):
            await update.message.reply_text(f"❌ Failed to update limit for user {target_user}. Please try again.")
            return
        
        access_count = load_access_count()
        updated_data = access_count.get(str(target_user), {'count': 0, 'total_limit': 1})
        if updated_data['total_limit'] != new_limit:
            logger.error(f"Error: total_limit not updated correctly for user {target_user}. Expected {new_limit}, got {updated_data['total_limit']}")
            await update.message.reply_text(f"❌ Failed to verify updated limit for user {target_user}. Please try again.")
            save_log("errors", {
                "user_id": target_user,
                "error": f"Failed to verify total_limit: expected {new_limit}, got {updated_data['total_limit']}",
                "timestamp": datetime.now().isoformat()
            })
            return

        await update.message.reply_text(
            f"✅ Added {add_count} searches for user {target_user}. Total limit: {new_limit}, Used: {current_count}, Remaining: {new_limit - current_count}"
        )
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=target_user,
                    text=f"✅ sniper has added {add_count} searches to your limit. Total limit: {new_limit}, Used: {current_count}, Remaining: {new_limit - current_count}"
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying user {target_user}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    await update.message.reply_text(f"⚠️ Added searches but could not notify user {target_user}: {str(e)}")
                    save_log("errors", {
                        "user_id": target_user,
                        "error": f"Failed to notify user after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Error in addaccess for user {target_user}: {str(e)}")
        await update.message.reply_text(f"❌ Error adding access for user {target_user}: {str(e)}")
        save_log("errors", {
            "user_id": target_user,
            "error": f"Addaccess failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can block users.")
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /block <user_id>")
        return
    try:
        uid = int(context.args[0])
        if uid == ADMIN_ID:
            await update.message.reply_text("❌ Cannot block the admin.")
            return
        save_blocked_user(uid)
        await update.message.reply_text(f"✅ Blocked user {uid}")
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text="❌ You have been blocked from using sniper's Bot.")
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
        await update.message.reply_text("Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error blocking user: {str(e)}")
        save_log("errors", {
            "user_id": user_id,
            "error": f"Block user failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def unblock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can unblock users.")
        return
    if len(context.args) != 1:
        await update.message.reply_text("Usage: /unblock <user_id>")
        return
    try:
        uid = int(context.args[0])
        remove_blocked_user(uid)
        await update.message.reply_text(f"✅ Unblocked user {uid}")
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text="✅ You have been unblocked and can now use sniper's Bot.")
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
        await update.message.reply_text("Invalid user ID.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error unblocking user: {str(e)}")
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
        await update.message.reply_text("❌ Only admin can view logs.")
        return
    try:
        logs = load_logs()
        text = "📜 Recent Logs by sniper:\n"
        log_types = ["access_requests", "searches", "approvals", "feedbacks", "errors"]
        for log_type in log_types:
            entries = logs.get(log_type, [])
            if entries:
                text += f"\n🔹 {log_type.upper()} (Last {min(len(entries), 5)}):\n"
                for entry in entries[-5:]:
                    entry_text = json.dumps(entry, indent=2, default=str)
                    if len(text) + len(entry_text) + 100 < 4000:
                        text += entry_text + "\n"
                    else:
                        text += "... (Truncated due to message length)\n"
                        break
            else:
                text += f"\n🔹 {log_type.upper()}: None\n"
        if text == "📜 Recent Logs by sniper:\n":
            text = "📜 No logs available."
        await update.message.reply_text(text[:4000])
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching logs: {str(e)}")
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
        await update.message.reply_text("❌ Only admin can view analytics.")
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
        await update.message.reply_text(
            f"📊 sniper's Bot Stats:\n"
            f"👥 Authorized Users: {total_users}\n"
            f"🚫 Blocked Users: {total_blocked}\n"
            f"🔍 Searches: {total_searches}\n"
            f"📝 Feedbacks: {total_feedbacks}\n"
            f"📄 Excel Files: {total_excel_files}"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Error fetching analytics: {str(e)}")
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
        await update.message.reply_text("❌ Only admin can reply to feedback.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /replyfeedback <user_id> <message>")
        return
    try:
        uid = int(context.args[0])
        msg = " ".join(context.args[1:])
        for attempt in range(3):
            try:
                await context.bot.send_message(chat_id=uid, text=f"📢 Reply from sniper:\n{msg}")
                await update.message.reply_text("✅ Feedback reply sent.")
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error sending feedback reply to {uid}, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    await update.message.reply_text(f"❌ Could not send message to user {uid}: {str(e)}")
                    save_log("errors", {
                        "user_id": uid,
                        "error": f"Failed to send feedback reply after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)
    except Exception as e:
        await update.message.reply_text(f"❌ Error replying to feedback: {str(e)}")
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
        await update.message.reply_text("❌ Only admin can export users.")
        return
    try:
        users = load_authorized_users()
        if not users:
            await update.message.reply_text("❌ No authorized users found to export.")
            return
        csv_buffer = io.StringIO()
        csv_buffer.write("user_id\n")
        for u in users:
            csv_buffer.write(f"{u}\n")
        csv_buffer.seek(0)
        await update.message.reply_document(
            document=InputFile(csv_buffer, filename="authorized_users.csv"),
            caption="✅ Exported authorized users."
        )
        csv_buffer.close()
    except Exception as e:
        await update.message.reply_text(f"❌ Error exporting users: {str(e)}")
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
        return

    try:
        if data.startswith("approve_"):
            if user_id != ADMIN_ID:
                await query.edit_message_text("❌ Only admin can approve requests.")
                return
            try:
                uid = int(data.split("_")[1])
            except ValueError:
                await query.edit_message_text("❌ Invalid user ID in approve request.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid user ID in approve callback: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            authorized = load_authorized_users()
            if uid not in authorized:
                if not save_authorized_user(uid):
                    await query.edit_message_text(f"❌ Failed to authorize user {uid}. Please try again.")
                    return
                if not save_access_count(uid, 0, 1):
                    await query.edit_message_text(f"❌ Failed to set search limit for user {uid}. Please try again.")
                    return
                save_log("approvals", {
                    "user_id": uid,
                    "action": "approved",
                    "timestamp": datetime.now().isoformat()
                })
                for attempt in range(3):
                    try:
                        await context.bot.send_message(
                            chat_id=uid,
                            text="✅ Access Approved by sniper! You have 1 search limit. Contact @Darksniperrx for more searches."
                        )
                        break
                    except telegram.error.BadRequest as e:
                        logger.error(f"Error notifying approved user {uid}, attempt {attempt + 1}: {e}")
                        if attempt == 2:
                            await query.edit_message_text(f"✅ Approved user {uid}, but could not notify user: {str(e)}")
                            save_log("errors", {
                                "user_id": uid,
                                "error": f"Failed to notify approved user after 3 attempts: {str(e)}",
                                "timestamp": datetime.now().isoformat()
                            })
                        await asyncio.sleep(1)
                await query.edit_message_text(f"✅ Approved user {uid}")
            else:
                await query.edit_message_text(f"⚠️ User {uid} is already authorized.")
        elif data.startswith("reject_"):
            if user_id != ADMIN_ID:
                await query.edit_message_text("❌ Only admin can reject requests.")
                return
            try:
                uid = int(data.split("_")[1])
            except ValueError:
                await query.edit_message_text("❌ Invalid user ID in reject request.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid user ID in reject callback: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            save_log("approvals", {
                "user_id": uid,
                "action": "rejected",
                "timestamp": datetime.now().isoformat()
            })
            for attempt in range(3):
                try:
                    await context.bot.send_message(
                        chat_id=uid,
                        text="❌ Access Denied by sniper."
                    )
                    break
                except telegram.error.BadRequest as e:
                    logger.error(f"Error notifying rejected user {uid}, attempt {attempt + 1}: {e}")
                    if attempt == 2:
                        await query.edit_message_text(f"❌ Rejected user {uid}, but could not notify user: {str(e)}")
                        save_log("errors", {
                            "user_id": uid,
                            "error": f"Failed to notify rejected user after 3 attempts: {str(e)}",
                            "timestamp": datetime.now().isoformat()
                        })
                    await asyncio.sleep(1)
            await query.edit_message_text(f"❌ Rejected user {uid}")
        elif data.startswith("select_"):
            access_count = load_access_count()
            user_data = access_count.get(str(user_id), {'count': 0, 'total_limit': 1})
            count = user_data['count']
            total_limit = user_data['total_limit']
            logger.info(f"Checking limit for user {user_id} on selection: count={count}, total_limit={total_limit}")
            if user_id != ADMIN_ID and count >= total_limit:
                await query.message.reply_text(
                    f"⚠️ Your search limit is reached. Current: count={count}, total_limit={total_limit}. Contact @Darksniperrx for more searches."
                )
                logger.warning(f"Selection blocked for user {user_id}: count={count}, total_limit={total_limit}")
                return
            try:
                idx = int(data.split("_")[1])
            except ValueError:
                await query.edit_message_text("❌ Invalid selection index.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid selection index in callback: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            search_results = context.user_data.get('search_results', [])
            if not search_results or idx < 0 or idx >= len(search_results):
                await query.edit_message_text("❌ Invalid selection or no search results available.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid selection: index {idx}, results length {len(search_results)}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            selected_record = search_results[idx]
            json_text = json.dumps(selected_record, indent=2, default=str)
            logger.info(f"Sending selected result index {idx}, JSON length: {len(json_text)}")
            await query.message.reply_text(json_text)
            await query.edit_message_text(f"✅ Details sent for selected record.")
            if user_id != ADMIN_ID:
                if not save_access_count(user_id, count + 1, total_limit):
                    await query.message.reply_text("❌ Error updating search count. Please try again.")
                    return
                logger.info(f"Incremented search count for user {user_id} to {count + 1}/{total_limit} after selection")
        elif data.startswith("page_"):
            try:
                page = int(data.split("_")[1])
            except ValueError:
                await query.edit_message_text("❌ Invalid page number.")
                save_log("errors", {
                    "user_id": user_id,
                    "error": f"Invalid page number in callback: {data}",
                    "timestamp": datetime.now().isoformat()
                })
                return
            context.user_data['current_page'] = page
            await send_paginated_results(update, context)
        else:
            await query.edit_message_text("❌ Invalid callback data.")
            save_log("errors", {
                "user_id": user_id,
                "error": f"Invalid callback data: {data}",
                "timestamp": datetime.now().isoformat()
            })
    except Exception as e:
        logger.error(f"Error in callback_handler for user {user_id}, data {data}: {str(e)}")
        await query.edit_message_text(f"❌ Error processing action: {str(e)}")
        save_log("errors", {
            "user_id": user_id,
            "error": f"Callback handler failed for data {data}: {str(e)}",
            "timestamp": datetime.now().isoformat()
        })

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error: {context.error}")
    save_log("errors", {
        "error": f"Bot error: {str(context.error)}",
        "timestamp": datetime.now().isoformat()
    })
    if isinstance(context.error, telegram.error.Conflict):
        for attempt in range(3):
            try:
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text="❌ Conflict: Multiple bot instances running. Please ensure only one instance is active."
                )
                break
            except telegram.error.BadRequest as e:
                logger.error(f"Error notifying admin of conflict, attempt {attempt + 1}: {e}")
                if attempt == 2:
                    save_log("errors", {
                        "error": f"Failed to notify admin of conflict after 3 attempts: {str(e)}",
                        "timestamp": datetime.now().isoformat()
                    })
                await asyncio.sleep(1)

async def health_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Health check command to verify bot and database status"""
    user_id = update.message.from_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Only admin can run health check.")
        return
    
    try:
        # Test MongoDB connection
        db = get_db()
        db.client.admin.command('ping')
        mongo_status = "✅ Connected"
    except Exception as e:
        mongo_status = f"❌ Error: {str(e)}"
    
    # Check DataFrame status
    df_status = f"✅ Loaded ({len(df)} rows, {len(df.columns) if not df.empty else 0} columns)" if not df.empty else "❌ Empty"
    
    # Check collections
    try:
        users_count = db.users_collection.count_documents({})
        access_count = db.access_collection.count_documents({})
        feedback_count = db.feedback_collection.count_documents({})
        collections_status = f"✅ Users: {users_count}, Access: {access_count}, Feedback: {feedback_count}"
    except Exception as e:
        collections_status = f"❌ Error: {str(e)}"
    
    await update.message.reply_text(
        f"🏥 Bot Health Status:\n\n"
        f"🤖 Bot: ✅ Running\n"
        f"🗄️ MongoDB: {mongo_status}\n"
        f"📊 DataFrame: {df_status}\n"
        f"📦 Collections: {collections_status}"
    )

def main():
    global df
    df = load_excel_on_startup()
    
    # Create application
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("listexcel", listexcel))
    app.add_handler(CommandHandler("reload", reload))
    app.add_handler(CommandHandler("logout", logout))
    app.add_handler(CommandHandler("profile", profile))
    app.add_handler(CommandHandler("userinfo", userinfo))
    app.add_handler(CommandHandler("feedback", feedback))
    app.add_handler(CommandHandler("name", search_name))
    app.add_handler(CommandHandler("email", search_email))
    app.add_handler(CommandHandler("phone", search_phone))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("addaccess", addaccess))
    app.add_handler(CommandHandler("block", block))
    app.add_handler(CommandHandler("unblock", unblock))
    app.add_handler(CommandHandler("logs", logs))
    app.add_handler(CommandHandler("analytics", analytics))
    app.add_handler(CommandHandler("replyfeedback", replyfeedback))
    app.add_handler(CommandHandler("exportusers", exportusers))
    app.add_handler(CommandHandler("health", health_check))
    app.add_handler(MessageHandler(DOCUMENT_FILTER, handle_document))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_error_handler(error_handler)

    # Start the bot
    logger.info("🤖 sniper's Bot running...")
    
    if USE_WEBHOOK:
        # Webhook mode for production
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}"
        )
    else:
        # Polling mode for development
        app.run_polling()

if __name__ == "__main__":
    main()
