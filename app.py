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
from flask import Flask, request, Response
import threading

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
    WEBHOOK_URL = get_env_var('WEBHOOK_URL')  # e.g., https://your-app.onrender.com
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
    
