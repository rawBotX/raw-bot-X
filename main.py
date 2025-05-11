"""
raw-bot-X
Version: 0.1.0
"""

import sys
import nest_asyncio
nest_asyncio.apply()
import pickle
import time
import random
import re
import requests
import asyncio
import platform
import os
import subprocess
import json
import html
import base64
import logging
import traceback
from telegram.constants import ParseMode
from urllib.parse import urlparse
from collections import deque
from datetime import datetime, timezone, timedelta

try:
    from zoneinfo import ZoneInfo
    print("INFO: Using 'zoneinfo' (Python 3.9+) for timezones.")
except ImportError:
    try:
        import pytz
        print("INFO: 'zoneinfo' not found, using 'pytz' as fallback.")
        # Define a pytz-based ZoneInfo class for compatibility
        class ZoneInfo(pytz.tzinfo.BaseTzInfo):
            def __init__(self, zone):
                self.zone = zone
                self._pytz_zone = pytz.timezone(zone)
            def utcoffset(self, dt): return self._pytz_zone.utcoffset(dt)
            def dst(self, dt): return self._pytz_zone.dst(dt)
            def tzname(self, dt): return self._pytz_zone.tzname(dt)
            def __reduce__(self): return (self.__class__, (self.zone,))

    except ImportError:
        print("ERROR: Neither 'zoneinfo' nor 'pytz' found.")
        # Define a simple fixed offset fallback if neither is available
        class FixedOffsetZone(timezone):
             def __init__(self, offset_hours=2, name="UTC+02:00_Fallback"):
                 self._offset = timedelta(hours=offset_hours)
                 self._name = name
                 super().__init__(self._offset, self._name)
             def __reduce__(self):
                 return (self.__class__, (self._offset.total_seconds() / 3600, self._name))
        # Make ZoneInfo point to the fallback
        ZoneInfo = lambda tz_name: FixedOffsetZone()

# --- Timezone Configuration ---
DEFAULT_FALLBACK_TIMEZONE_STR = "Europe/Berlin" # Used if .env and system TZ fails
USER_CONFIGURED_TIMEZONE = None # Will be set by load_user_timezone
USER_TIMEZONE_STR = "" # Will be set to the name of the timezone being used

def get_system_timezone_name():
    """Attempts to get the system's IANA timezone name."""
    try:
        # datetime.now().astimezone().tzname() can be unreliable for IANA names.
        # It often returns abbreviations (PST, CET) or fixed offsets.
        # A more robust method for system timezone is using the 'tzlocal' library,
        # but we aim for a solution without new mandatory dependencies.

        # On Linux, try reading /etc/timezone or /etc/localtime symlink
        if platform.system() == "Linux":
            try:
                with open("/etc/timezone", "r") as f:
                    tz_name = f.read().strip()
                    ZoneInfo(tz_name) # Validate it's a known IANA name
                    return tz_name
            except Exception:
                pass # File not found or invalid content
            try:
                # /etc/localtime is often a symlink to /usr/share/zoneinfo/Region/City
                localtime_path = os.path.realpath("/etc/localtime")
                if localtime_path.startswith("/usr/share/zoneinfo/"):
                    # Extract like "Region/City"
                    tz_name = localtime_path[len("/usr/share/zoneinfo/"):]
                    ZoneInfo(tz_name) # Validate
                    return tz_name
            except Exception:
                pass # Symlink not as expected or invalid
        
        # If not Linux or above methods failed, try a more general approach
        # This is less reliable for getting a direct IANA name usable by ZoneInfo
        # but can give a hint.
        system_tz_offset_name = datetime.now().astimezone().tzname()
        if system_tz_offset_name:
            try:
                # Check if the system name is directly usable by ZoneInfo
                ZoneInfo(system_tz_offset_name)
                return system_tz_offset_name
            except:
                # If not directly usable, it's likely an abbreviation or offset.
                # We can't reliably convert this to an IANA name without more tools.
                print(f"DEBUG: System tzname() returned '{system_tz_offset_name}', which is not a direct IANA name for ZoneInfo.")
                pass

    except Exception as e_sys_detect:
        print(f"DEBUG: Error during system timezone detection: {e_sys_detect}")
    return None

def load_user_timezone():
    """Loads the timezone in order of priority: .env, system, fallback."""
    global USER_CONFIGURED_TIMEZONE, USER_TIMEZONE_STR, DEFAULT_FALLBACK_TIMEZONE_STR

    env_timezone_str = os.getenv("TIMEZONE")

    if env_timezone_str:
        print(f"INFO: 'TIMEZONE={env_timezone_str}' found in .env. Attempting to load...")
        try:
            USER_CONFIGURED_TIMEZONE = ZoneInfo(env_timezone_str)
            USER_TIMEZONE_STR = env_timezone_str
            print(f"INFO: Successfully loaded timezone from .env: '{USER_TIMEZONE_STR}'")
            return
        except Exception as e_env_tz:
            print(f"WARNING: Could not load timezone '{env_timezone_str}' from .env: {e_env_tz}.")
            print(f"         Ensure it's a valid IANA TZ Database Name (e.g., 'America/New_York', 'Europe/Paris').")
            print(f"         See: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones")
            print(f"         Will try to use system timezone or fallback.")
            # Fall through to try system timezone
    else:
        print("INFO: 'TIMEZONE' not set in .env. Attempting to use system timezone.")

    # Try to get system timezone
    system_tz_name = get_system_timezone_name()
    if system_tz_name:
        print(f"INFO: Detected system timezone as '{system_tz_name}'. Attempting to load...")
        try:
            USER_CONFIGURED_TIMEZONE = ZoneInfo(system_tz_name)
            USER_TIMEZONE_STR = system_tz_name
            print(f"INFO: Successfully loaded system timezone: '{USER_TIMEZONE_STR}'")
            print(f"      To override, set 'TIMEZONE' in your config.env file.")
            return
        except Exception as e_sys_tz:
            print(f"WARNING: Could not load detected system timezone '{system_tz_name}': {e_sys_tz}.")
            print(f"         Falling back to default: '{DEFAULT_FALLBACK_TIMEZONE_STR}'.")
            # Fall through to default fallback
    else:
        print(f"INFO: Could not determine a usable system timezone. Falling back to default: '{DEFAULT_FALLBACK_TIMEZONE_STR}'.")
        print(f"      You can set 'TIMEZONE' in config.env for a specific zone (e.g., 'America/New_York').")
        print(f"      See: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones")


    # Fallback to default (e.g., "Europe/Berlin")
    try:
        USER_CONFIGURED_TIMEZONE = ZoneInfo(DEFAULT_FALLBACK_TIMEZONE_STR)
        USER_TIMEZONE_STR = DEFAULT_FALLBACK_TIMEZONE_STR
        print(f"INFO: Successfully loaded default fallback timezone: '{USER_TIMEZONE_STR}'")
    except Exception as e_fb_tz:
        print(f"ERROR: Could not load default fallback timezone '{DEFAULT_FALLBACK_TIMEZONE_STR}': {e_fb_tz}.")
        print(f"       Using a fixed UTC offset or pure UTC as last resort.")
        # Last resort: Use the FixedOffsetZone (if zoneinfo/pytz are missing) or UTC
        try:
            USER_CONFIGURED_TIMEZONE = ZoneInfo(None) # This will use FixedOffsetZone if zoneinfo/pytz are missing
            
            # Determine the name of the fallback timezone object
            if hasattr(USER_CONFIGURED_TIMEZONE, '_name') and USER_CONFIGURED_TIMEZONE._name: # Our FixedOffsetZone
                USER_TIMEZONE_STR = USER_CONFIGURED_TIMEZONE._name
            elif hasattr(USER_CONFIGURED_TIMEZONE, 'zone') and USER_CONFIGURED_TIMEZONE.zone: # pytz wrapper for ZoneInfo
                USER_TIMEZONE_STR = USER_CONFIGURED_TIMEZONE.zone
            elif isinstance(USER_CONFIGURED_TIMEZONE, timezone) and USER_CONFIGURED_TIMEZONE.utcoffset(None) == timedelta(0):
                USER_TIMEZONE_STR = "UTC"
            else: # Fallback for other tzinfo objects or if name is not descriptive
                offset = USER_CONFIGURED_TIMEZONE.utcoffset(datetime.now())
                if offset is not None:
                    offset_hours = offset.total_seconds() / 3600
                    USER_TIMEZONE_STR = f"UTC{offset_hours:+03.0f}:00_Fallback"
                else: # Should not happen with our FixedOffsetZone or UTC
                    USER_TIMEZONE_STR = "Unknown_Offset_Fallback"
            print(f"INFO: Using '{USER_TIMEZONE_STR}' as final fallback timezone.")
        except Exception as e_final_fb:
            USER_CONFIGURED_TIMEZONE = timezone.utc # Absolute last resort
            USER_TIMEZONE_STR = "UTC"
            print(f"CRITICAL ERROR: All timezone loading failed catastrophically: {e_final_fb}. Using UTC as absolute fallback.")

load_user_timezone()
# --- End Timezone Configuration ---

from bs4 import BeautifulSoup

from bs4 import BeautifulSoup
# Note: datetime, timezone, timedelta already imported above
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from telegram import InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler
# Note: ParseMode already imported above
from telegram import Update
import telegram
from telegram.ext import ApplicationBuilder, MessageHandler, filters, CommandHandler, ContextTypes
from dotenv import load_dotenv
import functools


ascii_art = [
r"                                                           ",
r"                                                           ",
r"                                                           ",
r"             \\\\\\\\\                    ////             ",
r"               \\\   \\\                ////               ",
r"                 \\\   \\\         _  ////                 ",
r"                   \\\   \\\      | | //         _         ",
r"          ____   ____  _ _ _  ___ | | _    ___  | |_       ",
r"         / ___) / _  || | | |(___)| || \  / _ \ |  _)      ",
r"        | |    ( ( | || | | |     | |_) )| |_| || |__      ",
r"        |_|     \_||_| \____|     |____/  \___/  \___)     ",
r"                                                           ",
r"                       ////    \\\   \\\                   ",
r"                     ////        \\\   \\\                 ",
r"                   ////            \\\   \\\               ",
r"                 ////                \\\   \\\             ",
r"               ////                    \\\\\\\\\           ",
r"                                                           ",
r"                                                           ",
r"                                                           "
]

# --- Admin Configuration ---
ADMINS_FILE = "admins.json"
# Load the initial admin ID from the .env file (important for the first start!)
# Add a line ADMIN_USER_ID=<your_telegram_user_id> to your config.env
# You can find your ID e.g. with @userinfobot in Telegram.
INITIAL_ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
admin_user_ids = set() # Will be loaded on startup
# --- End Admin Configuration ---


# Configure logging (optional, but recommended)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO # Your global level remains INFO
)
# Replace 'your_bot_module_name' if necessary with the name of your script/module
logger = logging.getLogger(__name__) # Use standard Python logger

# Set the logging level for httpx higher to suppress INFO messages
logging.getLogger("httpx").setLevel(logging.WARNING)

# Configuration
SETTINGS_FILE = "settings.json" # For persistence
KEYWORDS_FILE = "keywords.json"
# Load keywords from file or use default values
try:
    with open(KEYWORDS_FILE, 'r') as f:
        KEYWORDS = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    KEYWORDS = ["token", "meme", "coin"]
    # Create the file with default values
    with open(KEYWORDS_FILE, 'w') as f:
        json.dump(KEYWORDS, f)

TOKEN_PATTERN = r"(?<![/\"'=&?])(?:\b[A-Za-z0-9]{32,}\b)(?![/\"'&?])"
AUTH_CODE = None
WAITING_FOR_AUTH = False

# Load .env file
try:
    # Optional: Use explicit path relative to the script
    # script_dir = os.path.dirname(__file__)
    # dotenv_path = os.path.join(script_dir, 'config.env')
    # print(f"DEBUG: Trying to load .env from: {dotenv_path}")
    # loaded = load_dotenv(dotenv_path=dotenv_path, verbose=True)

    # Or stick to the relative path to the working directory
    config_path_load = os.path.abspath("config.env") # Get path for the message
    print(f"DEBUG: Trying to load .env from: {config_path_load}")
    loaded = load_dotenv("config.env", verbose=True) # verbose=True for more info

    print(f"DEBUG: load_dotenv successful? {loaded}")
    if not loaded:
         print("WARNING: load_dotenv reports that the file was not loaded or was empty.")
except Exception as e_dotenv:
    print(f"FATAL: Error executing load_dotenv: {e_dotenv}")
    import traceback
    traceback.print_exc()

# Check directly afterwards what os.getenv delivers:
check_admin_id = os.getenv("ADMIN_USER_ID")
# This print statement must be on its OWN line:
print(f"DEBUG: os.getenv('ADMIN_USER_ID') after load_dotenv: '{check_admin_id}'")

# The original assignments follow AFTERWARDS:
DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN")
TEST_BOT_TOKEN = os.getenv("BOT_TEST_TOKEN") #  Load test token
CHANNEL_ID = os.getenv("CHANNEL_ID")
# --- Admin Configuration --- (These lines remain here)
ADMINS_FILE = "admins.json"
INITIAL_ADMIN_USER_ID = os.getenv("ADMIN_USER_ID") # This assignment is correct here
admin_user_ids = set() # Will be loaded on startup
# --- End Admin Configuration ---


DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN")
TEST_BOT_TOKEN = os.getenv("BOT_TEST_TOKEN") #  Load test token
CHANNEL_ID = os.getenv("CHANNEL_ID")

#  Global variable for the *currently* used bot token
ACTIVE_BOT_TOKEN = None # Will be set in the main block

# === Dynamic Account Creation ===
ACCOUNTS = []
account_index = 1
while True:
    email = os.getenv(f"ACCOUNT_{account_index}_EMAIL")
    if not email:
        break

    password = os.getenv(f"ACCOUNT_{account_index}_PASSWORD")
    username = os.getenv(f"ACCOUNT_{account_index}_USERNAME")
    # --- CHANGED: Cookie file handling ---
    cookies_file_env = os.getenv(f"ACCOUNT_{account_index}_COOKIES")
    # Fallback to default name if not in .env or empty
    if not cookies_file_env:
        safe_username_for_file = re.sub(r'[\\/*?:"<>|]', "_", username) if username else f"account_{account_index}"
        cookies_file = f"{safe_username_for_file}_cookies.cookies.json" # Default filename with .cookies.json
        print(f"WARNING: ACCOUNT_{account_index}_COOKIES not found/empty in .env. Using default: {cookies_file}")
    else:
        cookies_file = cookies_file_env
        # Ensure the extension is correct (optional, but good for consistency)
        if not cookies_file.endswith(".cookies.json"):
             print(f"WARNING: Cookie file '{cookies_file}' for account {account_index} does not end with '.cookies.json'. Using recommended extension.")
             # Optional: Force extension (could rename existing files)
             # cookies_file = os.path.splitext(cookies_file)[0] + ".cookies.json"
    # --- END CHANGE ---

    ACCOUNTS.append({
        "email": email,
        "password": password,
        "username": username,
        "cookies_file": cookies_file, # Use the (potentially corrected) variable
    })
    print(f"INFO: Account {account_index} ({username or email}) loaded. Cookie file: {cookies_file}")
    account_index += 1

if not ACCOUNTS:
    print("ERROR: No account data found in config.env! Please define at least ACCOUNT_1_EMAIL etc.")
    import sys
    sys.exit(1)
# === End Dynamic Account Creation ===

# ===> NEW/CHANGED: Constants and variables for follow functions <===
FOLLOW_LIST_TEMPLATE = "add_contacts_{}.txt"     # Template for account follow lists
FOLLOWER_BACKUP_TEMPLATE = "follower_backup_{}.txt" # Template for account backups
GLOBAL_FOLLOWED_FILE = "global_followed_users.txt" # Central list of all followed users

# Global sets for quick access (loaded/updated during startup/backup)
global_followed_users_set = set()
# The account-specific list is now loaded dynamically
current_account_usernames_to_follow = [] # Loaded on startup/switch

last_follow_attempt_time = time.time()
is_periodic_follow_active = True # Control flag for auto-follow
# ------------------------------------------------------------------------------------

# Global variables
is_backup_running = False
cancel_backup_flag = False
is_sync_running = False
cancel_sync_flag = False
PROCESSED_TWEETS_MAXLEN = 200 # Choose a suitable size
processed_tweets = deque(maxlen=PROCESSED_TWEETS_MAXLEN)
current_account = 0
driver = None
application = None
last_like_time = time.time()
last_driver_restart_time = time.time()
# Counter for login attempts
login_attempts = 0
# New variables for pause mechanism
is_scraping_paused = False
pause_event = asyncio.Event()
pause_event.set()  # Not paused by default
is_schedule_pause = False  # Flag to distinguish if pause comes from scheduler
# Variable to track first run for optimized post processing
first_run = True
# Search settings:
search_keywords_enabled = True # Default: Enabled
search_ca_enabled = True       # Default: Enabled
search_tickers_enabled = True  # Default: Enabled (existing)

# Maximum post age in minutes to be considered recent
max_tweet_age_minutes = 15 # Default value

# Headless mode: True to run Chrome without GUI
is_headless_enabled = False # Default: Disabled

# Cloudflare Check Globals
WAITING_FOR_CLOUDFLARE_CONFIRMATION = False
CLOUDFLARE_ACCOUNT_INDEX = None
cloudflare_solved_event = asyncio.Event()

# Debug Pause Event after Password - REMOVED
# manual_login_debug_continue_event = asyncio.Event()


# ===> Auto-Follow Modes & settings <===
auto_follow_mode = "off" # Possible values: "off", "slow", "fast"
auto_follow_interval_minutes = [15, 30] # [min, max] for Slow Mode
is_fast_follow_running = False # Flag for the fast follow task
cancel_fast_follow_flag = False # Flag to cancel the fast follow task
# ===> END Auto-Follow Modes <===

# ===> Following Database <===
FOLLOWING_DB_FILE = "following_database.json"
following_database = {} # Loaded on startup
is_db_scrape_running = False # Flag for concurrency
cancel_db_scrape_flag = False # Flag for cancellation
# ===> END Following Database <===

is_any_scheduled_browser_task_running = False # True if Sync or FollowList schedule is active
is_scheduled_follow_list_running = False    # True if process_follow_list_schedule_logic is active
cancel_scheduled_follow_list_flag = False # To cancel the scheduled follow list task


# ===> Scrape Queue (for headless restart) <===
SCRAPE_QUEUE_FILE = "scrape_queue.txt"

def add_username_to_scrape_queue(username):
    """Appends a username to the scrape queue file."""
    try:
        # Ensure username is clean (no @, strip whitespace)
        clean_username = username.strip().lstrip('@')
        if not re.match(r'^[A-Za-z0-9_]{1,15}$', clean_username):
            logger.warning(f"[Scrape Queue] Invalid username format '{username}' - not adding.")
            return False
        with open(SCRAPE_QUEUE_FILE, 'a', encoding='utf-8') as f:
            f.write(f"{clean_username}\n")
        logger.info(f"[Scrape Queue] Added @{clean_username} to queue file.")
        return True
    except Exception as e:
        logger.error(f"[Scrape Queue] Error adding @{username} to queue file: {e}", exc_info=True)
        return False

def read_and_clear_scrape_queue():
    """Reads all usernames from the queue file and clears it."""
    usernames = []
    if os.path.exists(SCRAPE_QUEUE_FILE):
        try:
            with open(SCRAPE_QUEUE_FILE, 'r', encoding='utf-8') as f:
                # Read, strip whitespace, remove '@' just in case, filter empty lines and duplicates
                raw_lines = [line.strip().lstrip('@') for line in f if line.strip()]
                # Keep order but remove duplicates
                seen = set()
                usernames = [u for u in raw_lines if not (u in seen or seen.add(u))]

            # Clear the file after reading successfully
            with open(SCRAPE_QUEUE_FILE, 'w', encoding='utf-8') as f:
                f.write("") # Write empty string to clear
            logger.info(f"[Scrape Queue] Read {len(usernames)} unique usernames and cleared queue file.")
        except Exception as e:
            logger.error(f"[Scrape Queue] Error reading/clearing queue file: {e}", exc_info=True)
            # Attempt to clear anyway if reading failed but file exists
            try:
                with open(SCRAPE_QUEUE_FILE, 'w', encoding='utf-8') as f: f.write("")
            except: pass
    return usernames
# ===> END Scrape Queue <===

# Post counting variables
POSTS_COUNT_FILE = "posts_count.json"
# Schedule variables
SCHEDULE_FILE = "schedule.json"
schedule_enabled = False
schedule_pause_start = "00:00"  # Default start time in 24-hour format
schedule_pause_end = "00:00"    # Default end time in 24-hour format

# --- New Schedules ---
# For Sync Followers
schedule_sync_enabled = False
schedule_sync_start_time = "03:00"  # Default start time for sync
schedule_sync_end_time = "03:30"    # Default end time for sync
last_sync_schedule_run_date = None # Stores date of last run

# For Follow List Processing
schedule_follow_list_enabled = False
schedule_follow_list_start_time = "04:00" # Default start time for follow list
schedule_follow_list_end_time = "04:30"   # Default end time for follow list
last_follow_list_schedule_run_date = None # Stores date of last run
# --- End New Schedules ---
posts_count = {
    "found": {
        "today": 0,
        "yesterday": 0,
        "day_before_yesterday": 0, # Translated key
        "total": 0
    },
    "scanned": {
        "today": 0,
        "yesterday": 0,
        "day_before_yesterday": 0, # Translated key
        "total": 0
    },
    "weekdays": {
        "Monday": {"count": 0, "days": 0},
        "Tuesday": {"count": 0, "days": 0},
        "Wednesday": {"count": 0, "days": 0},
        "Thursday": {"count": 0, "days": 0},
        "Friday": {"count": 0, "days": 0},
        "Saturday": {"count": 0, "days": 0},
        "Sunday": {"count": 0, "days": 0}
    }
}
last_count_date = None  # To track date changes
start_time = datetime.now()  # To track uptime

action_queue = asyncio.Queue()

# ===>  Rating System <===
RATINGS_FILE = "ratings.json"
ratings_data = {} # Loaded on startup
# ===> END Rating System <===

# Stores post URLs for the last messages
last_tweet_urls = {}

rate_limit_patterns = [
    '//span[contains(text(), "unlock more posts by subscribing")]',
    '//span[contains(text(), "Subscribe to Premium")]',
    '//span[contains(text(), "rate limit")]',
    '//div[contains(text(), "Something went wrong")]'
]

def display_ascii_animation(art_lines, delay_min=0.05, delay_max=0.1):
    """Prints ASCII art line by line with delay and flush."""
    print("\n" * 2) # Blank lines before
    for line in art_lines:
        print(line)
        sys.stdout.flush() # <<<--- Flush buffer
        time.sleep(random.uniform(delay_min, delay_max))

def load_settings():
    """Loads settings from the file, including scraping, auto-follow, search toggles, headless mode, and max post age."""
    global is_scraping_paused, pause_event
    global auto_follow_mode, auto_follow_interval_minutes
    global search_keywords_enabled, search_ca_enabled, search_tickers_enabled
    global is_headless_enabled
    global max_tweet_age_minutes

    # --- Define default values ---
    default_scraping_paused = True
    default_autofollow_mode = "off"
    default_autofollow_interval = [15, 30]
    default_search_keywords_enabled = True
    default_search_ca_enabled = True
    default_search_tickers_enabled = True
    default_headless_enabled = False
    default_max_tweet_age = 15

    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                settings = json.load(f)
                is_scraping_paused = settings.get("is_scraping_paused", default_scraping_paused)
                auto_follow_mode = settings.get("auto_follow_mode", default_autofollow_mode)
                loaded_interval = settings.get("auto_follow_interval_minutes", default_autofollow_interval)
                if isinstance(loaded_interval, list) and len(loaded_interval) == 2 and all(isinstance(x, int) for x in loaded_interval):
                    auto_follow_interval_minutes = loaded_interval
                else:
                    print(f"WARNING: Invalid auto_follow_interval_minutes in {SETTINGS_FILE}. Using default: {default_autofollow_interval}")
                    auto_follow_interval_minutes = default_autofollow_interval

                search_keywords_enabled = settings.get("search_keywords_enabled", default_search_keywords_enabled)
                search_ca_enabled = settings.get("search_ca_enabled", default_search_ca_enabled)
                search_tickers_enabled = settings.get("search_tickers_enabled", default_search_tickers_enabled)
                is_headless_enabled = settings.get("is_headless_enabled", default_headless_enabled)
                loaded_max_age = settings.get("max_tweet_age_minutes", default_max_tweet_age)
                if isinstance(loaded_max_age, int) and loaded_max_age >= 1:
                    max_tweet_age_minutes = loaded_max_age
                else:
                    print(f"WARNING: Invalid max_tweet_age_minutes ('{loaded_max_age}') in {SETTINGS_FILE}. Using default: {default_max_tweet_age}")
                    max_tweet_age_minutes = default_max_tweet_age

                print(f"Settings loaded:")
                print(f"  - Scraping: {'PAUSED' if is_scraping_paused else 'ACTIVE'}")
                print(f"  - Auto-Follow Mode: {auto_follow_mode.upper()}")
                if auto_follow_mode == "slow":
                    print(f"  - Auto-Follow Interval: {auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]} Min")
                print(f"  - Keyword Search: {'ENABLED' if search_keywords_enabled else 'DISABLED'}")
                print(f"  - CA Search: {'ENABLED' if search_ca_enabled else 'DISABLED'}")
                print(f"  - Ticker Search: {'ENABLED' if search_tickers_enabled else 'DISABLED'}")
                print(f"  - Headless Mode: {'ENABLED' if is_headless_enabled else 'DISABLED'}")
                print(f"  - Max post Age: {max_tweet_age_minutes} minutes")

        else:
            print("No settings file found, setting default values and creating file...")
            is_scraping_paused = default_scraping_paused
            auto_follow_mode = default_autofollow_mode
            auto_follow_interval_minutes = default_autofollow_interval
            search_keywords_enabled = default_search_keywords_enabled
            search_ca_enabled = default_search_ca_enabled
            search_tickers_enabled = default_search_tickers_enabled
            is_headless_enabled = default_headless_enabled
            max_tweet_age_minutes = default_max_tweet_age
            save_settings()
            print(f"Default settings file '{SETTINGS_FILE}' has been created.")

    except (json.JSONDecodeError, Exception) as e:
        print(f"Error loading settings ({type(e).__name__}): {e}. Using default values.")
        is_scraping_paused = default_scraping_paused
        auto_follow_mode = default_autofollow_mode
        auto_follow_interval_minutes = default_autofollow_interval
        search_keywords_enabled = default_search_keywords_enabled
        search_ca_enabled = default_search_ca_enabled
        search_tickers_enabled = default_search_tickers_enabled
        is_headless_enabled = default_headless_enabled
        max_tweet_age_minutes = default_max_tweet_age

    # --- IMPORTANT: Set asyncio.Event based on loaded status ---
    if is_scraping_paused:
        pause_event.clear() # Paused
    else:
        pause_event.set()   # Running

def save_settings():
    """Saves current settings to the file, incl. scraping, auto-follow, search toggles, headless mode, and max post age."""
    global is_scraping_paused
    global auto_follow_mode, auto_follow_interval_minutes
    global search_keywords_enabled, search_ca_enabled, search_tickers_enabled
    global is_headless_enabled
    global max_tweet_age_minutes
    try:
        settings = {
            "is_scraping_paused": is_scraping_paused,
            "auto_follow_mode": auto_follow_mode,
            "auto_follow_interval_minutes": auto_follow_interval_minutes,
            "search_keywords_enabled": search_keywords_enabled,
            "search_ca_enabled": search_ca_enabled,
            "search_tickers_enabled": search_tickers_enabled,
            "is_headless_enabled": is_headless_enabled,
            "max_tweet_age_minutes": max_tweet_age_minutes
        }
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=4)
        # print("Settings saved.") # Optional logging
    except Exception as e:
        print(f"Error saving settings: {e}")

def load_admins():
    """Loads admin user IDs from the file."""
    global admin_user_ids
    try:
        if os.path.exists(ADMINS_FILE):
            with open(ADMINS_FILE, 'r') as f:
                data = json.load(f)
                # Ensure it is a list of integers
                loaded_ids = data.get("admin_user_ids", [])
                admin_user_ids = {int(uid) for uid in loaded_ids if isinstance(uid, (int, str)) and str(uid).isdigit()}
                print(f"Admins loaded: {len(admin_user_ids)} User IDs.")
        else:
            print(f"No {ADMINS_FILE} found.")
            # --- Initial Admin Setup ---
            if INITIAL_ADMIN_USER_ID and INITIAL_ADMIN_USER_ID.isdigit():
                print(f"Adding initial admin from .env: {INITIAL_ADMIN_USER_ID}")
                admin_user_ids = {int(INITIAL_ADMIN_USER_ID)}
                save_admins() # Save the file with the initial admin
            else:
                print("WARNING: No admins file and no valid INITIAL_ADMIN_USER_ID found in .env!")
                admin_user_ids = set()
            # --- End Initial Admin Setup ---

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        print(f"ERROR loading or processing {ADMINS_FILE}: {e}. Resetting admin list.")
        admin_user_ids = set()
        # Optional: Try adding initial admin again
        if INITIAL_ADMIN_USER_ID and INITIAL_ADMIN_USER_ID.isdigit():
             admin_user_ids = {int(INITIAL_ADMIN_USER_ID)}
             save_admins()
    except Exception as e:
        print(f"Unexpected error loading admins: {e}")
        admin_user_ids = set()

def save_admins():
    """Saves the current admin list to the file."""
    global admin_user_ids
    try:
        # Convert the set to a list for JSON storage
        data = {"admin_user_ids": sorted(list(admin_user_ids))}
        with open(ADMINS_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        # print("Admin list saved.") # Optional
    except Exception as e:
        print(f"Error saving admin list: {e}")

def is_user_admin(user_id: int) -> bool:
    """Checks if a given user ID is in the admin list."""
    global admin_user_ids
    # Ensure the list is loaded (although it's loaded on startup)
    if not admin_user_ids and os.path.exists(ADMINS_FILE):
        load_admins() # Reload if it's empty for some reason
    return user_id in admin_user_ids

def add_admin_command_handler(application, command, callback):
    """
    Registers a command handler that checks if the user is an admin
    before executing the callback.
    """
    @functools.wraps(callback)
    async def admin_check_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.message or not update.message.from_user:
            logger.warning(f"Admin check failed for command '{command}': No user object.")
            return # Cannot check

        user_id = update.message.from_user.id
        if is_user_admin(user_id):
            # User is admin, execute the original function
            # Important: The original function manages its own pause/resume
            await callback(update, context, *args, **kwargs)
        else:
            # User is not admin, send error message
            logger.warning(f"Unauthorized access to command '{command}' by user {user_id}.")
            await update.message.reply_text("❌ Access denied. You are not an admin.")
            # No automatic resume_scraping here, as we don't know
            # if the original command would have paused. The bot remains in the
            # current state (running/paused).

    # Register the wrapper instead of the original callback
    application.add_handler(CommandHandler(command, admin_check_wrapper))

def load_ratings():
    """Loads the rating data from the file."""
    global ratings_data
    try:
        if os.path.exists(RATINGS_FILE):
            with open(RATINGS_FILE, 'r') as f:
                ratings_data = json.load(f)
                print(f"Rating data loaded for {len(ratings_data)} sources.")
        else:
            ratings_data = {}
            print(f"No rating file found ({RATINGS_FILE}), starting with an empty database and creating the file.")
            save_ratings() # Create the file with empty data
    except json.JSONDecodeError:
        print(f"ERROR: {RATINGS_FILE} is corrupt. Starting with an empty database and creating a new file.")
        ratings_data = {}
        save_ratings() # Create a new, empty ratings file
    except Exception as e:
        print(f"Error loading rating data: {e}. Initializing empty and attempting to save.")
        ratings_data = {}
        save_ratings() # Attempt to create a new, empty ratings file

def save_ratings():
    """Saves the current rating data to the file."""
    global ratings_data
    try:
        with open(RATINGS_FILE, 'w') as f:
            json.dump(ratings_data, f, indent=4)
        # print("Rating data saved.")
    except Exception as e:
        print(f"Error saving rating data: {e}")

def load_following_database():
    """Loads the following database from the file."""
    global following_database
    try:
        if os.path.exists(FOLLOWING_DB_FILE):
            with open(FOLLOWING_DB_FILE, 'r', encoding='utf-8') as f:
                following_database = json.load(f)
                print(f"Following database loaded ({len(following_database)} entries).")
        else:
            following_database = {}
            print(f"No following database file found ({FOLLOWING_DB_FILE}), starting with an empty database and creating the file.")
            save_following_database() # Create the file with empty data
    except json.JSONDecodeError:
        print(f"ERROR: {FOLLOWING_DB_FILE} is corrupt. Starting with an empty database and creating a new file.")
        following_database = {}
        save_following_database() # Create a new, empty database file
    except Exception as e:
        print(f"Error loading the following database: {e}. Initializing empty and attempting to save.")
        following_database = {}
        save_following_database() # Attempt to create a new, empty database file

def save_following_database():
    """Saves the current following database to the file."""
    global following_database
    try:
        with open(FOLLOWING_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(following_database, f, indent=2) # indent=2 for some readability
        # print("Following database saved.")
    except Exception as e:
        print(f"Error saving the following database: {e}")

def load_set_from_file(filepath):
    """Loads lines from a file into a set."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                # Remove @ and empty lines
                return {line.strip().lstrip('@') for line in f if line.strip()}
        else:
            return set()
    except Exception as e:
        print(f"Error loading set from {filepath}: {e}")
        return set()

def save_set_to_file(data_set, filepath):
    """Saves a set to a file, one line per element."""
    try:
        # Sort for consistent files
        sorted_list = sorted(list(data_set))
        with open(filepath, 'w') as f:
            for item in sorted_list:
                f.write(f"{item}\n") # Write without @
    except Exception as e:
        print(f"Error saving set to {filepath}: {e}")

def add_to_set_file(data_set, filepath):
    """Adds elements to a file representing a set (reads, adds, writes)."""
    try:
        existing_set = load_set_from_file(filepath)
        initial_size = len(existing_set)
        updated_set = existing_set.union(data_set) # Add new elements
        if len(updated_set) > initial_size: # Only write if something has changed
             save_set_to_file(updated_set, filepath)
             # print(f"File {filepath} updated with {len(updated_set) - initial_size} new entries.")
    except Exception as e:
         print(f"Error adding to set file {filepath}: {e}")

def get_current_account_username():
    """Returns the username of the currently active account."""
    global current_account, ACCOUNTS
    if 0 <= current_account < len(ACCOUNTS):
        # Ensure the key exists and is not None
        username = ACCOUNTS[current_account].get("username")
        return username if username else None # Returns None if key is missing or value is None
    return None

def get_current_follow_list_path():
    """Returns the file path for the follow list of the current account."""
    username = get_current_account_username()
    if username:
        # Replace invalid characters for filenames if necessary (although usernames should be safe)
        safe_username = re.sub(r'[\\/*?:"<>|]', "_", username)
        return FOLLOW_LIST_TEMPLATE.format(safe_username)
    return None

def get_current_backup_file_path():
    """Returns the file path for the backup file of the current account."""
    username = get_current_account_username()
    if username:
        safe_username = re.sub(r'[\\/*?:"<>|]', "_", username)
        return FOLLOWER_BACKUP_TEMPLATE.format(safe_username)
    return None

def ensure_data_files_exist():
    """
    Checks for the existence of data files that are not automatically
    created by their load functions and creates them empty if they don't exist.
    This is primarily for account-specific lists and the global list.
    """
    print("INFO: Ensuring all necessary data files exist...")

    # 1. Global Followed Users File
    if not os.path.exists(GLOBAL_FOLLOWED_FILE):
        print(f"INFO: Global followed users file '{GLOBAL_FOLLOWED_FILE}' not found. Creating empty file.")
        save_set_to_file(set(), GLOBAL_FOLLOWED_FILE)

    # 2. Scrape Queue File
    if not os.path.exists(SCRAPE_QUEUE_FILE):
        print(f"INFO: Scrape queue file '{SCRAPE_QUEUE_FILE}' not found. Creating empty file.")
        try:
            with open(SCRAPE_QUEUE_FILE, 'w', encoding='utf-8') as f:
                f.write("") # Create empty file
        except Exception as e:
            print(f"ERROR: Could not create empty scrape queue file '{SCRAPE_QUEUE_FILE}': {e}")

    # 3. Account-specific files
    if not ACCOUNTS:
        print("WARNING: No accounts configured. Skipping creation of account-specific files.")
        return

    for i, account_info in enumerate(ACCOUNTS):
        acc_username = account_info.get("username")
        # Use a generic identifier if username is missing, for file path generation
        safe_username_for_file = None
        if acc_username:
            safe_username_for_file = re.sub(r'[\\/*?:"<>|]', "_", acc_username)
        else:
            # Fallback if username is not set for the account in ACCOUNTS
            # This might happen if .env is incomplete but ACCOUNTS list was still populated
            safe_username_for_file = f"account_{i+1}_unknown"
            print(f"WARNING: Username for account index {i} is missing. Using generic filename part '{safe_username_for_file}'.")


        # a. Account Follow List (add_contacts_*.txt)
        # We need to use the logic from get_current_follow_list_path but for *any* account
        # This is a bit tricky as get_current_follow_list_path depends on current_account
        # Let's construct it directly here.
        follow_list_filename = FOLLOW_LIST_TEMPLATE.format(safe_username_for_file)
        if not os.path.exists(follow_list_filename):
            print(f"INFO: Account follow list '{follow_list_filename}' for '{acc_username or f'Account {i+1}'}' not found. Creating empty file.")
            save_set_to_file(set(), follow_list_filename)

        # b. Account Follower Backup (follower_backup_*.txt)
        # Similar direct construction for backup file path
        backup_filename = FOLLOWER_BACKUP_TEMPLATE.format(safe_username_for_file)
        if not os.path.exists(backup_filename):
            print(f"INFO: Account follower backup '{backup_filename}' for '{acc_username or f'Account {i+1}'}' not found. Creating empty file.")
            save_set_to_file(set(), backup_filename)

    print("INFO: Data file existence check complete.")

def create_driver():
    options = webdriver.ChromeOptions()
    global is_headless_enabled # Access the global setting
    # Enhanced anti-detection settings
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--start-maximized')
    options.add_argument('--force-device-scale-factor=0.25') # Setzt den Skalierungsfaktor auf 25%

    # Additional anti-detection measures
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)

    # Memory optimizations for Raspberry Pi
    options.add_argument('--disable-dev-tools')
    options.add_argument('--no-zygote')
    options.add_argument('--single-process')
    options.add_argument('--disable-features=VizDisplayCompositor')

    # Raspberry Pi specific options
    is_raspberry_pi = os.path.exists('/usr/bin/chromium-browser')
    if is_raspberry_pi:
        options.add_argument('--disable-gpu')
        # Headless is now controlled by the global setting, not just RPi detection
        # options.add_argument('--headless')

    # Add headless argument based on the global setting
    if is_headless_enabled:
        print("INFO: Headless mode is ENABLED. Adding --headless argument.")
        options.add_argument('--headless')
    else:
        print("INFO: Headless mode is DISABLED.")

    # User Agent - use a more recent user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')

    # Check if we are on Raspberry Pi or another system
    if is_raspberry_pi:
        options.binary_location = '/usr/bin/chromium-browser'
    else:
        options.binary_location = '/usr/bin/chromium'

    service = Service(executable_path='/usr/bin/chromedriver')

    try:
        driver = webdriver.Chrome(service=service, options=options)

        # Execute CDP commands to make detection harder
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": random.choice(user_agents)
        })

        # Mask WebDriver presence
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # Der JavaScript Zoom wurde entfernt, da er unzuverlässig war.
        # Der Zoom wird jetzt über '--force-device-scale-factor' gesetzt (siehe oben).

        return driver
    except Exception as e:
        print(f"Error creating driver: {e}")
        raise

async def initialize():
    """Initializes the WebDriver, logs in, and switches to the following tab."""
    # The Telegram application is now initialized centrally in run().
    global driver, application, current_account # 'application' is only referenced here, not recreated
    try:
        print("Initializing WebDriver...")
        driver = create_driver()
        print("WebDriver initialized. Starting login...")
        login_success = await login()
        if login_success:
            print("Login successful. Switching to 'Following' tab...")
            await switch_to_following_tab()
            print("'Following' tab reached.")
        else:
            print("WARNING: Login failed during initialization.")

        # Ensure the global 'application' from run() is available
        if application is None:
             print("ERROR: Telegram application was not correctly initialized in run().")
             raise RuntimeError("Telegram application not initialized in run()")
        else:
             print("Telegram application is initialized and ready.")

    except Exception as e:
        # Print a more specific error message
        print(f"ERROR DURING INITIALIZATION (initialize function): {e}")
        import traceback
        traceback.print_exc() # Print the full traceback for more details
        raise # Re-raise the error so the script might stop

async def switch_to_following_tab():
    """Checks for ad relevance popup and ensures we're on the Following tab."""
    try:
        # ---  Popup Check ---
        # XPath looking for a button containing a span with the specific text
        popup_button_xpath = "//button[.//span[contains(text(), 'Keep less relevant ads')]]"
        try:
            print("Checking for 'Keep less relevant ads' popup...")
            # Wait only briefly (e.g., 5 seconds), as the popup should appear quickly if present
            popup_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, popup_button_xpath))
            )
            print("Popup found, clicking 'Keep less relevant ads'...")
            popup_button.click()
            await asyncio.sleep(random.uniform(1, 2)) # Short pause after the click
            print("Popup button clicked.")
        except (TimeoutException, NoSuchElementException):
            # This is the normal case when the popup is not present
            print("No 'Keep less relevant ads' popup found.")
            pass
        except Exception as popup_err:
            # Log error handling the popup, but continue
            print(f"WARNING: Error handling the 'Keep ads' popup: {popup_err}")
        # --- END Popup Check ---

        # --- Existing logic for switching tabs ---
        print("Attempting to switch to the 'Following' tab...")
        # The original XPath for the "Following" tab
        following_tab_button_xpath = "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div/nav/div/div[2]/div/div[2]/a"
        following_tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, following_tab_button_xpath))
        )
        # A short pause before clicking can sometimes help, especially after UI changes
        await asyncio.sleep(random.uniform(0.5, 1.5))
        following_tab_button.click()
        print("'Following' tab clicked.")
        await asyncio.sleep(random.uniform(2, 4)) # Keep waiting time after the click

    except (TimeoutException, NoSuchElementException):
         # Error if the "Following" tab itself is not found
         print("WARNING: Could not find or click the 'Following' tab button.")
         pass # Ignore and continue, maybe already on it
    except Exception as e:
        # General error in this function
        print(f"Error in switch_to_following_tab: {e}")

async def login():
    """Main login method that tries different login approaches in sequence"""
    global current_account, login_attempts
    try:
        account = ACCOUNTS[current_account]

        # Explicitly navigate to the login page first
        driver.get("https://x.com/login")
        await asyncio.sleep(3)

        # If we're already logged in (redirected to home), return success
        if "home" in driver.current_url:
            await send_telegram_message(f"✅ Already logged in as Account {current_account+1}")
            login_attempts = 0
            return True

        # First, try to login with cookies
        if await cookie_login():
            login_attempts = 0
            return True

        # If cookie login fails, try manual login
        await send_telegram_message(f"🔑 Starting login for Account {current_account+1}...")
        result = await manual_login()

        if result:
            login_attempts = 0
            return True
        else:
            login_attempts += 1
            if login_attempts >= 3:
                await send_telegram_message("⚠️ Multiple login attempts failed. Waiting 15 minutes before trying again...")
                await asyncio.sleep(900)  # Wait 15 minutes
                login_attempts = 0

            await switch_account()
            return False

    except Exception as e:
        await send_telegram_message(f"❌ Login error: {str(e)}")
        login_attempts += 1
        await switch_account()
        return False

async def cookie_login():
    """Try to login using saved cookies (JSON format)."""
    global current_account, driver
    account = ACCOUNTS[current_account]
    cookie_filepath = account.get('cookies_file') # Safe access

    if not cookie_filepath:
        print("ERROR: No cookie file path found for the current account.")
        return False

    try:
        # Navigate to X before setting cookies
        driver.get("https://x.com")
        await asyncio.sleep(2)

        # Load and set cookies from JSON
        try:
            print(f"Trying to load cookies from JSON file: {cookie_filepath}")
            # --- CHANGED: Load JSON in text mode ---
            with open(cookie_filepath, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            # --- END CHANGE ---
            print(f"{len(cookies)} cookies loaded.")

            # Clear existing cookies
            driver.delete_all_cookies()

            # Add cookies
            added_count = 0
            for cookie in cookies:
                # --- CHANGED: SameSite handling remains, but no more Pickle-specific things ---
                if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
                    cookie['sameSite'] = 'Lax'
                # Remove 'expiry' if it's not an integer (sometimes float from old pickles?)
                if 'expiry' in cookie and not isinstance(cookie['expiry'], int):
                    del cookie['expiry']

                try:
                    driver.add_cookie(cookie)
                    added_count += 1
                except Exception as cookie_error:
                    # Log more details about the faulty cookie
                    print(f"WARNING: Error adding cookie '{cookie.get('name', 'N/A')}': {cookie_error}")
                    continue
            print(f"{added_count}/{len(cookies)} cookies successfully added.")
            # --- END CHANGE ---

            # Refresh and check if logged in
            print("Refreshing page after setting cookies...")
            driver.refresh()
            await asyncio.sleep(random.uniform(3, 5)) # Wait a bit longer after refresh

            if "home" in driver.current_url:
                print("Login via cookies successful.")
                # Navigate to Following timeline (as before)
                driver.get("https://x.com/home")
                await asyncio.sleep(2)
                try:
                    initial_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH,
                        "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div/nav/div/div[2]/div/div[2]/a"))
                    )
                    time.sleep(random.uniform(2, 4))
                    initial_button.click()
                except:
                    pass # Ignore errors here if not necessary
                await asyncio.sleep(2)
                await send_telegram_message("✅ Login via cookies successful!")
                return True
            else:
                print(f"WARNING: Login via cookies failed (URL after refresh: {driver.current_url}). Trying manual login.")
                return False

        except FileNotFoundError:
            print(f"Cookie file '{cookie_filepath}' not found. Trying manual login.")
            return False
        # --- CHANGED: Add JSONDecodeError ---
        except json.JSONDecodeError as json_err:
            print(f"ERROR: Cookie file '{cookie_filepath}' is corrupt or not a valid JSON file: {json_err}")
            print("Trying manual login.")
            # Optional: Delete the corrupt file
            # try: os.remove(cookie_filepath)
            # except OSError as e: print(f"Could not delete corrupt cookie file: {e}")
            return False
        # --- END CHANGE ---

    except Exception as e:
        print(f"Unexpected error in cookie login: {e}")
        logger.error("Unexpected error in cookie_login", exc_info=True)
        return False

async def manual_login():
    """Perform manual login using email/username and password"""
    global current_account, driver, AUTH_CODE
    account = ACCOUNTS[current_account]
    try:
        # Go to the login page
        driver.get("https://x.com/login")
        await asyncio.sleep(random.uniform(4, 6))

        # Username/email input
        if check_element_exists(By.NAME, "text"):
            username_input = driver.find_element(By.NAME, "text")
            await type_like_human(username_input, account.get("email", ""))
            username_input.send_keys(Keys.RETURN)
            await asyncio.sleep(random.uniform(2, 4))
            print("Entered username/email")

        # Additional username step if needed
        if check_element_exists(By.CSS_SELECTOR, '[data-testid="ocfEnterTextTextInput"]'):
            username_input = driver.find_element(By.CSS_SELECTOR, '[data-testid="ocfEnterTextTextInput"]')
            await type_like_human(username_input, account.get("username", ""))
            username_input.send_keys(Keys.RETURN)
            await asyncio.sleep(random.uniform(2, 4))
            print("Entered additional username")

        # Password input
        if check_element_exists(By.NAME, "password"):
            password_input = driver.find_element(By.NAME, "password")
            await type_like_human(password_input, account.get("password", ""))
            password_input.send_keys(Keys.RETURN)
            await asyncio.sleep(random.uniform(4, 6))
            print("Entered password")

        # === DEBUG PAUSE REMOVED ===

        # === Cloudflare Check ===
        cloudflare_check_needed = False
        print("Performing Cloudflare checks...") # Log start of check
        try:
            # --- Check indicators in order of reliability ---
            # 1. Hidden Inputs (Very Reliable)
            if check_element_exists(By.CSS_SELECTOR, 'input[name="cf-turnstile-response"]', timeout=1):
                print("Cloudflare indicator: Found cf-turnstile-response input.")
                cloudflare_check_needed = True
            elif check_element_exists(By.CSS_SELECTOR, 'input[name="cf_challenge_response"]', timeout=1):
                print("Cloudflare indicator: Found cf_challenge_response input.")
                cloudflare_check_needed = True
            # 2. Ray ID (Very Reliable)
            elif check_element_exists(By.CSS_SELECTOR, 'div.ray-id', timeout=1):
                print("Cloudflare indicator: Found div.ray-id.")
                cloudflare_check_needed = True
            # 3. Specific ID 'verifying' (Good, but might be hidden)
            elif check_element_exists(By.ID, 'verifying', timeout=1):
                print("Cloudflare indicator: Found div#verifying.")
                cloudflare_check_needed = True
            # 4. Footer Link (Medium Reliability)
            elif check_element_exists(By.XPATH, '//div[@id="footer-text"]//a[contains(@href, "cloudflare.com")]', timeout=1):
                print("Cloudflare indicator: Found footer link to cloudflare.com.")
                cloudflare_check_needed = True
            # 5. Text Indicators (Medium Reliability - Keep as fallback)
            elif check_element_exists(By.XPATH, '//span[contains(text(), "Verify you are human")]', timeout=1):
                 print("Cloudflare indicator: Found text 'Verify you are human'.")
                 cloudflare_check_needed = True
            # 6. IFrame (Medium Reliability - Keep as fallback)
            elif check_element_exists(By.XPATH, '//iframe[contains(@title, "Cloudflare") or contains(@title, "challenge")]', timeout=1):
                 print("Cloudflare indicator: Found challenge iframe.")
                 cloudflare_check_needed = True

        except Exception as cf_check_err:
            # Log errors during the check itself, but don't necessarily stop the login
            print(f"Warning: Error during Cloudflare check execution: {cf_check_err}")

        # --- If Cloudflare was detected, wait for user confirmation ---
        if cloudflare_check_needed:
            global WAITING_FOR_CLOUDFLARE_CONFIRMATION, CLOUDFLARE_ACCOUNT_INDEX, cloudflare_solved_event
            WAITING_FOR_CLOUDFLARE_CONFIRMATION = True
            CLOUDFLARE_ACCOUNT_INDEX = current_account
            cloudflare_solved_event.clear()

            # Send message to Telegram - ** CHANGED INSTRUCTIONS **
            account_display = ACCOUNTS[current_account].get("username", f"Account {current_account+1}")
            message_text = (f"🚨 **Manual Login Required (Cloudflare)** for {account_display}!\n\n"
                            f"Cloudflare detected. Please **log in manually**.\n\n"
                            f"Click the button below **ONLY AFTER** you are successfully logged into X or restart the bot.")
            # ** CHANGED BUTTON TEXT **
            keyboard = [[InlineKeyboardButton("✅ I have logged in manually", callback_data=f"cloudflare_solved:{current_account}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Use the global send_telegram_message function
            await send_telegram_message(message_text, reply_markup=reply_markup)
            print(f"Cloudflare detected for Account {current_account+1}. Waiting for user to log in manually and confirm via Telegram button...")

            # Wait for the user to click the button
            await cloudflare_solved_event.wait()

            # Reset flags after confirmation
            WAITING_FOR_CLOUDFLARE_CONFIRMATION = False
            CLOUDFLARE_ACCOUNT_INDEX = None
            print(f"Manual login confirmation received for Account {current_account+1}. Continuing login verification...")
            # Refresh page after user confirms manual login
            try:
                driver.refresh()
                print("Page refreshed after manual login confirmation.")
                await asyncio.sleep(random.uniform(3, 5)) # Wait longer after refresh
            except Exception as refresh_err:
                print(f"Warning: Error refreshing page after manual login confirmation: {refresh_err}")
        # === End Cloudflare Check ===

        # Handle 2FA if needed (Only if Cloudflare wasn't detected or was solved via manual login)
        # Note: 2FA might appear *after* manual login if triggered by Cloudflare resolution
        if not cloudflare_check_needed and check_element_exists(By.CSS_SELECTOR, '[data-testid="ocfEnterTextTextInput"]'):
            await handle_2fa()

        # Handle account unlock if needed (Only if Cloudflare wasn't detected or was solved)
        if not cloudflare_check_needed and check_element_exists(By.XPATH, "//div[contains(text(), 'Your account has been locked')]"):
            await handle_account_unlock()

        # Verify successful login
        await asyncio.sleep(3)
        if "home" in driver.current_url:
            cookie_filepath = account.get('cookies_file')
            if cookie_filepath:
                # --- CHANGED: Save cookies as JSON ---
                try:
                    print(f"Saving cookies as JSON to: {cookie_filepath}")
                    cookies_to_save = driver.get_cookies()
                    with open(cookie_filepath, "w", encoding='utf-8') as file:
                        json.dump(cookies_to_save, file, indent=4) # indent=4 for readability
                    print(f"{len(cookies_to_save)} cookies saved.")
                except Exception as save_err:
                    print(f"ERROR saving cookies to '{cookie_filepath}': {save_err}")
                    logger.error(f"Failed to save cookies to {cookie_filepath}", exc_info=True)
                # --- END CHANGE ---
            else:
                 print("WARNING: No cookie file path found to save for this account.")

            await send_telegram_message(f"✅ Login for Account {current_account+1} successful!")
            return True
        else:
            await send_telegram_message(f"❌ Login for Account {current_account+1} failed!")
            return False

    except Exception as e:
        await send_telegram_message(f"❌ Manual login failed: {str(e)}")
        return False

async def handle_2fa():
    """Handle two-factor authentication during login"""
    global AUTH_CODE, WAITING_FOR_AUTH
    try:
        auth_input = driver.find_element(By.CSS_SELECTOR, '[data-testid="ocfEnterTextTextInput"]')

        # Request code from Telegram user
        WAITING_FOR_AUTH = True
        await send_telegram_message(f"🔐 2FA code required for Account {current_account+1}")

        # Wait for the auth code
        code = await wait_for_auth_code()
        WAITING_FOR_AUTH = False

        if code:
            await type_like_human(auth_input, code)
            auth_input.send_keys(Keys.RETURN)
            await asyncio.sleep(4)
            return True
        return False
    except Exception as e:
        WAITING_FOR_AUTH = False
        print(f"Error handling 2FA: {e}")
        return False

async def wait_for_auth_code():
    """Wait for the user to send an auth code via Telegram"""
    global AUTH_CODE, WAITING_FOR_AUTH
    logger.info("[Auth Wait] Starting to wait for auth code...") # Log start
    for i in range(300):  # 5 minutes timeout
        # Log the check in each iteration
        logger.debug(f"[Auth Wait] Loop {i+1}/300: Checking AUTH_CODE (current value: {'Set' if AUTH_CODE else 'None'})...")
        if AUTH_CODE:
            code = AUTH_CODE
            AUTH_CODE = None  # Reset code
            logger.info(f"[Auth Wait] Auth code '{code}' received!") # Log success with code
            return code
        await asyncio.sleep(1)
    # Reached only if the loop finishes without finding the code
    logger.warning("[Auth Wait] Timeout after 300 seconds while waiting for authentication code.") # Log timeout
    await send_telegram_message("⏰ Timeout while waiting for authentication code")
    return None

async def handle_account_unlock():
    """Handle the account unlock process if the account is locked"""
    global AUTH_CODE, WAITING_FOR_AUTH
    try:
        await send_telegram_message("⚠️ Account is locked. Starting unlock process...")

        # Click "Start" button if available
        if check_element_exists(By.XPATH, "//input[@value='Start']"):
            driver.find_element(By.XPATH, "//input[@value='Start']").click()
            await asyncio.sleep(2)

        # Click "Send email" button if available
        if check_element_exists(By.XPATH, "//input[@value='Send email']"):
            driver.find_element(By.XPATH, "//input[@value='Send email']").click()
            await asyncio.sleep(2)

        # Enter verification code if needed
        if check_element_exists(By.NAME, "token"):
            WAITING_FOR_AUTH = True
            await send_telegram_message("🔑 Please enter the code from the email") # Translated
            code = await wait_for_auth_code()
            WAITING_FOR_AUTH = False

            if code:
                code_input = driver.find_element(By.NAME, "token")
                await type_like_human(code_input, code)
                driver.find_element(By.XPATH, "//input[@value='Verify']").click()
                await asyncio.sleep(2)

        # Click "Continue to X" button if available
        if check_element_exists(By.XPATH, "//input[@value='Continue to X']"):
            driver.find_element(By.XPATH, "//input[@value='Continue to X']").click()
            await asyncio.sleep(2)

        return True
    except Exception as e:
        WAITING_FOR_AUTH = False
        await send_telegram_message(f"❌ Account unlock error: {str(e)}")
        return False

async def type_like_human(element, text):
    """Simulate human-like typing with random delays between keystrokes"""
    for char in text:
        element.send_keys(char)
        await asyncio.sleep(random.uniform(0.05, 0.2))

def check_element_exists(by, value, timeout=5):
    """Check if an element exists on the page"""
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return True
    except:
        return False

async def check_rate_limit():
    """Check if we've hit a rate limit based on various patterns"""
    # First, check the internet connection
    try:
        # Quick ping test (5-second timeout)
        response = requests.get("https://api.x.com/ping", timeout=5)
    except:
        # Try again with another service
        try:
            response = requests.get("https://www.google.com", timeout=5)
        except Exception as e:
            print(f"Internet connection might be interrupted: {e}")
            # Treat as a rate limit since we can't fetch data
            return True

    # Then perform the original rate limit check
    try:
        for pattern in rate_limit_patterns:
            try:
                # SIGNIFICANTLY shorter timeout
                element = WebDriverWait(driver, 0.5).until(
                    EC.presence_of_element_located((By.XPATH, pattern))
                )
                if element:
                    await handle_rate_limit()
                    return True
            except (NoSuchElementException, TimeoutException):
                continue # Element not found quickly enough -> continue
        return False
    except Exception as e:
        print(f"Error checking rate limit: {e}")
        return False

async def handle_rate_limit():
    """Handle rate limit by simply logging and refreshing the page without switching accounts."""
    await asyncio.sleep(random.uniform(4, 6))

async def switch_account():
    """Switch to the next available account or reuse current if only one account"""
    global current_account, driver

    if len(ACCOUNTS) <= 1:
        await send_telegram_message("⚠️ Only one account available, trying again with the same account")
        await logout()
    else:
        await logout()
        current_account = (current_account + 1) % len(ACCOUNTS)

    # Restart the driver
    if driver:
        driver.quit()
    driver = create_driver()

    # Login with the new account
    await login()

async def logout():
    """Logout from the current account"""
    try:
        # First, ensure we are on an X page
        if not "x.com" in driver.current_url:
            driver.get("https://x.com/home")
            await asyncio.sleep(3)

        # Try to logout via UI
        if check_element_exists(By.CSS_SELECTOR, 'div[data-testid="SideNav_AccountSwitcher_Button"]', timeout=3):
            account_button = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="SideNav_AccountSwitcher_Button"]')
            account_button.click()
            await asyncio.sleep(random.uniform(1, 2))

            if check_element_exists(By.CSS_SELECTOR, 'a[data-testid="logout"]', timeout=3):
                logout_button = driver.find_element(By.CSS_SELECTOR, 'a[data-testid="logout"]')
                logout_button.click()
                await asyncio.sleep(random.uniform(1, 2))

                if check_element_exists(By.CSS_SELECTOR, 'div[data-testid="confirmationSheetConfirm"]', timeout=3):
                    confirm_button = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="confirmationSheetConfirm"]')
                    confirm_button.click()
                    await asyncio.sleep(random.uniform(2, 3))
                    print("Successfully logged out via UI")

                    # Explicitly navigate to the login page after logout
                    driver.get("https://x.com/login")
                    await asyncio.sleep(2)
                    return True
    except Exception as e:
        print(f"Logout failed via UI: {e}")

    # Fallback: clear all cookies and storage
    try:
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear();")
        driver.execute_script("window.sessionStorage.clear();")
        print("Cleared cookies and storage as fallback")

        # Explicitly navigate to the login page after clearing cookies
        driver.get("https://x.com/login")
        await asyncio.sleep(2)
        return True
    except Exception as e:
        print(f"Error clearing cookies and storage: {e}")
        return False

def parse_follower_count(count_str: str) -> int:
    """
    Converts follower count strings (e.g., "9.6M", "862K", "12345", "2,4m", "23.83k") into integers.
    Returns 0 on errors or invalid format.
    """
    if not isinstance(count_str, str):
        return 0

    # 1. Preprocessing: Lowercase, remove ALL commas, strip whitespace
    # Commas are ALWAYS treated as thousand separators and removed.
    # The period is interpreted as a decimal separator.
    processed_str = count_str.lower().strip().replace(',', '')
    if not processed_str:
        return 0

    # 2. Suffix Handling
    multiplier = 1
    num_part = processed_str
    if processed_str.endswith('m'):
        multiplier = 1_000_000
        num_part = processed_str[:-1].strip() # Remove 'm' and possibly preceding space
    elif processed_str.endswith('k'):
        multiplier = 1_000
        num_part = processed_str[:-1].strip() # Remove 'k' and possibly preceding space

    # 3. Number Parsing
    try:
        # Check if more than one period is present (invalid)
        if num_part.count('.') > 1:
            return 0

        # Convert to float to handle decimal places (e.g., "2.4", "23.83")
        num_float = float(num_part)

        # Calculate the final value and convert to int
        final_value = int(num_float * multiplier)
        return final_value
    except ValueError:
        # Error converting to float (e.g., "abc", empty string after suffix removal)
        return 0
    except Exception as e:
        # Other unexpected errors
        print(f"Debug parse_follower_count: Unexpected error with '{count_str}': {e}")
        return 0

async def follow_user(username):
    """Follow a user on X based on their username"""
    try:
        # Navigate to user's profile
        driver.get(f"https://x.com/{username}")
        await asyncio.sleep(random.uniform(1.5, 3))

        # Check if already following
        unfollow_button_xpaths = [
            "//button[@aria-label='Following @" + username + "']",
            "//button[contains(@aria-label, 'Following @" + username + "')]",
            "//button[starts-with(@aria-label, 'Following @')]",
            "//div[@role='button' and @aria-label='Following @" + username + "']",
            "//div[@role='button' and contains(@aria-label, 'Following @')]"
        ]

        # Check if already following
        for xpath in unfollow_button_xpaths:
            try:
                unfollow_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, xpath))
                )
                if unfollow_button:
                    await send_telegram_message(f"ℹ️ You are already following @{username}") # Translated
                    # Navigate back to the following timeline
                    driver.get("https://x.com/home")
                    await asyncio.sleep(random.uniform(2, 3))
                    await switch_to_following_tab()
                    return "already_following"
            except:
                continue

        # If not following, look for follow button
        follow_button_xpaths = [
            "//button[@aria-label='Follow @" + username + "']",
            "//button[contains(@aria-label, 'Follow @" + username + "')]",
            "//button[starts-with(@aria-label, 'Follow @')]",
            "//div[@role='button' and @aria-label='Follow @" + username + "']",
            "//div[@role='button' and contains(@aria-label, 'Follow @')]"
        ]

        follow_button = None
        for xpath in follow_button_xpaths:
            try:
                follow_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                break
            except:
                continue

        if follow_button:
            try:
                # --- Attempt 1: Scroll into view and use JavaScript click ---
                print(f"    Scrolling follow button for @{username} into view...")
                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", follow_button)
                await asyncio.sleep(random.uniform(0.5, 1.0)) # Short pause after scroll

                print(f"    Attempting JavaScript click on follow button for @{username}...")
                driver.execute_script("arguments[0].click();", follow_button)
                print(f"    JavaScript click successful for @{username}.")

                await asyncio.sleep(random.uniform(2, 3)) # Wait for action to register
                await send_telegram_message(f"✅ Successfully followed @{username}")

                # Navigate back to the following timeline
                driver.get("https://x.com/home")
                await asyncio.sleep(random.uniform(1, 2))
                await switch_to_following_tab()
                return True

            except Exception as click_err:
                print(f"    WARNING: JavaScript click failed for @{username}: {click_err}")
                print(f"    Attempting fallback standard click for @{username}...")
                # --- Attempt 2: Fallback to standard click (might still fail) ---
                try:
                    # Ensure it's clickable again after potential JS errors
                    follow_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable(follow_button) # Re-check clickability
                    )
                    follow_button.click()
                    await asyncio.sleep(random.uniform(1.5, 2))
                    await send_telegram_message(f"✅ Successfully followed @{username} (Fallback Click)")

                    # Navigate back
                    driver.get("https://x.com/home")
                    await asyncio.sleep(random.uniform(1, 2))
                    await switch_to_following_tab()
                    return True

                except Exception as fallback_click_err:
                    # If both clicks fail, report the original error type but mention fallback failure
                    print(f"    ERROR: Both JavaScript and standard click failed for @{username}. Fallback error: {fallback_click_err}")
                    # Send the original error type to Telegram for better diagnosis
                    error_type = type(click_err).__name__ # Get name of the initial error
                    await send_telegram_message(f"❌ Could not click follow button for @{username} ({error_type}).")
                    # Navigate back even on failure
                    driver.get("https://x.com/home")
                    await asyncio.sleep(random.uniform(1, 2))
                    await switch_to_following_tab()
                    return False
        else:
            await send_telegram_message(f"❌ Could not find follow button for @{username}")
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(1, 2))
            await switch_to_following_tab()
            return False

    except Exception as e:
        await send_telegram_message(f"❌ Error while trying to follow @{username}: {str(e)}")
        return False

async def unfollow_user(username):
    """Unfollow a user on X based on their username"""
    try:
        print(f"Starting unfollow process for @{username}")
        # Navigate to user's profile
        driver.get(f"https://x.com/{username}")
        await asyncio.sleep(random.uniform(2, 4))

        # If we're here, check for various "unfollow" or "currently following" buttons
        # Priority 1: Standard "Following" button (text-based or with specific aria-label)
        # This indicates we are clearly in a "following" state.
        following_state_button_xpaths = [
            "//button[@data-testid='user-follow-button']//span[text()='Following']", # TestID with "Following" text
            "//button[@aria-label='Following @" + username + "']",
            "//button[contains(@aria-label, 'Following @" + username + "')]",
            "//button[starts-with(@aria-label, 'Following @')]",
            "//div[@role='button' and @aria-label='Following @" + username + "']",
            "//div[@role='button' and contains(@aria-label, 'Following @')]"
        ]

        # Priority 2: Alternative "Unfollow" button (often with an icon, aria-label "Unfollow @username")
        # This button might appear directly if the UI decides to show it instead of "Following".
        # It still means we are currently following them, and clicking it initiates unfollow.
        alternative_unfollow_button_xpaths = [
            # Specific to the button you provided: aria-label="Unfollow @username" and contains an SVG
            "//button[@aria-label='Unfollow @" + username + "' and .//svg]",
            "//div[@role='button' and @aria-label='Unfollow @" + username + "' and .//svg]"
        ]

        # Combine the lists, giving priority to the "Following" state buttons
        unfollow_button_xpath_candidates = following_state_button_xpaths + alternative_unfollow_button_xpaths

        unfollow_button = None
        for xpath in unfollow_button_xpath_candidates: # Use the new combined list
            try:
                unfollow_button = WebDriverWait(driver, 2).until( # Slightly shorter timeout per attempt
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                print(f"Found a clickable 'currently following' or 'unfollow initiation' button using XPath: {xpath}")
                break # Found a suitable button
            except:
                continue # Try next XPath

        if unfollow_button:
            print(f"Found initial 'following' or 'unfollow initiation' button for @{username}, clicking it...")
            await asyncio.sleep(random.uniform(0.5, 1.5)) # Shorter sleep before click
            unfollow_button.click()
            await asyncio.sleep(random.uniform(1.5, 2.5)) # Wait for popup/dropdown to appear

            # Handle confirmation dialog - trying different types
            print("Looking for confirmation dialog or dropdown menu item...")
            confirmation_found = False

            # --- PRIORITY 1: Standard Confirmation Dialog (larger popup) ---
            standard_dialog_xpaths = [
                '//button[@data-testid="confirmationSheetConfirm"]',          # Most reliable for standard dialog
                '//div[@role="dialog"]//span[text()="Unfollow"]/ancestor::button[1]', # Text-based in dialog
                '//div[@role="dialog"]//button[1]' # Fallback: first button in any dialog (use with caution)
            ]
            # Less reliable / more specific XPaths for standard dialog (can be added if needed)
            # xpath1 = '//*[@id="layers"]/div[2]/div/div/div/div/div/div[2]/div[2]/div[2]/button[1]'
            # xpath2 = '/html/body/div[1]/div/div/div[1]/div[2]/div/div/div/div/div/div[2]/div[2]/div[2]/button[1]'
            # css_selector = "#layers > div:nth-child(2) > div > div > div > div > div > div.css-175oi2r... > button:nth-child(1)"


            for i, xpath in enumerate(standard_dialog_xpaths):
                try:
                    confirm_button = WebDriverWait(driver, 1.5).until( # Short wait for each attempt
                        EC.element_to_be_clickable((By.XPATH, xpath))
                    )
                    if confirm_button:
                        print(f"Found standard confirmation button (Attempt {i+1} with XPath: {xpath}), clicking it...")
                        confirm_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2)) # Wait for action
                        break # Exit loop once found and clicked
                except Exception as e_std_dialog:
                    # print(f"Debug: Standard dialog attempt {i+1} with '{xpath}' failed: {e_std_dialog}")
                    pass # Continue to next XPath or next approach

            # --- PRIORITY 2: Dropdown Menu Confirmation (if standard dialog not found) ---
            if not confirmation_found:
                print("Standard confirmation dialog not found, checking for dropdown menu item...")
                try:
                    # XPath based on your provided HTML for the dropdown item:
                    # It looks for a div with role="menuitem" that contains a span with the exact text "Unfollow @username"
                    # The username variable needs to be correctly substituted.
                    dropdown_item_xpath = f'//div[@data-testid="Dropdown"]//div[@role="menuitem" and .//span[text()="Unfollow @{username}"]]'
                    
                    dropdown_confirm_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, dropdown_item_xpath))
                    )
                    if dropdown_confirm_button:
                        print(f"Found 'Unfollow @{username}' in dropdown menu, clicking it...")
                        dropdown_confirm_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2)) # Wait for action
                except Exception as e_dropdown:
                    print(f"Could not find or click 'Unfollow @{username}' in dropdown menu: {e_dropdown}")

            if not confirmation_found:
                print("WARNING: Could not find or click any confirmation button (standard dialog or dropdown). Unfollow might not have completed.")
            else:
                print("Confirmation step processed.")

            print("Sending success message") # This line was already there
            try:
                await send_telegram_message(f"✅ Successfully unfollowed @{username}")
            except Exception as msg_err:
                print(f"Error sending success message: {msg_err}")

            # Navigate back to the following timeline
            print("Navigating back to home timeline")
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return True
        else:
            # Not following this user
            print(f"Not following @{username} - no unfollow button found")
            await send_telegram_message(f"ℹ️ You are not following @{username}") # Translated

            # Navigate back to home
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return "not_following"

    except Exception as e:
        print(f"Exception in unfollow_user: {e}")
        try:
            await send_telegram_message(f"❌ Error while trying to unfollow @{username}: {str(e)}")
        except Exception as msg_err:
            print(f"Error sending error message: {msg_err}")
        return False

async def backup_followers_logic(update: Update):
    """Scrapes the 'Following' list, saves it account-specifically
    and updates the global followed list. (With cancellation option)"""
    global driver, current_account, is_scraping_paused, pause_event, ACCOUNTS
    global global_followed_users_set
    global is_backup_running, cancel_backup_flag # Import flags

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if is_backup_running:
        await update.message.reply_text("⚠️ A backup process is already running.")
        # IMPORTANT: Do not continue, but also do not pause/resume here
        return
    if not account_username or not backup_filepath:
        await update.message.reply_text("❌ Error: Account username or backup path could not be determined.")
        return

    # ===== Task Start Marker =====
    is_backup_running = True
    cancel_backup_flag = False # Ensure flag is reset
    # ================================

    print(f"[Backup] Starting follower backup for @{account_username} -> {backup_filepath}...")
    await update.message.reply_text(f"⏳ Starting follower backup for @{account_username}...\n"
                                     f"   To cancel: `/cancelbackup`") # Info about cancel command

    await pause_scraping() # Pause main scraping

    found_followers = set()
    navigation_successful = False
    last_found_count = -1
    cancelled_early = False # Flag for cancellation message

    try: # Main try block
        following_url = f"https://x.com/{account_username}/following"
        print(f"[Backup] Navigating to: {following_url}")
        driver.get(following_url)
        await asyncio.sleep(random.uniform(8, 12))
        user_cell_button_xpath = '//button[@data-testid="UserCell"]'
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.XPATH, user_cell_button_xpath)))
        print("[Backup] Found initial UserCell buttons.")

        scroll_attempts_without_new_followers = 0
        max_scroll_attempts_without_new_followers = 3

        while scroll_attempts_without_new_followers < max_scroll_attempts_without_new_followers:
            # ===== Cancellation Check =====
            if cancel_backup_flag:
                print("[Backup] Cancellation signal received.")
                cancelled_early = True
                await update.message.reply_text("🟡 Backup is being cancelled...")
                break # Exit loop
            # =========================

            initial_follower_count_in_loop = len(found_followers)
            await asyncio.sleep(0.5)

            # User extraction... (as before)
            try:
                user_cells = driver.find_elements(By.XPATH, user_cell_button_xpath)
                # ... (Rest of the extraction logic) ...
                relative_link_xpath = ".//a[contains(@href, '/')]"
                for cell_button in user_cells:
                    # ===== Cancellation Check (more granular) =====
                    if cancel_backup_flag: break
                    # ==========================================
                    try:
                        link_element = cell_button.find_element(By.XPATH, relative_link_xpath)
                        href = link_element.get_attribute('href')
                        if href:
                             parts = href.split('/')
                             if len(parts) > 0:
                                 username = parts[-1].strip()
                                 if username and re.match(r'^[A-Za-z0-9_]{1,15}$', username):
                                     found_followers.add(username)
                    except NoSuchElementException: pass
                    except Exception as cell_err: print(f"  [Backup] Warning: Extraction error: {cell_err}")
                if cancel_backup_flag: break # Also check after the inner loop
            except Exception as find_err: print(f"[Backup] Warning: Search error: {find_err}")

            current_follower_count_in_loop = len(found_followers)
            newly_found_in_loop = current_follower_count_in_loop - initial_follower_count_in_loop

            current_scroll_pos = driver.execute_script("return window.pageYOffset;")
            total_scroll_height = driver.execute_script("return document.body.scrollHeight;")
            print(f"[Backup] Scroll-Pos: {current_scroll_pos}/{total_scroll_height}, Found={current_follower_count_in_loop} (+{newly_found_in_loop} iteration), Failed attempts={scroll_attempts_without_new_followers}")

            if current_follower_count_in_loop == last_found_count:
                 scroll_attempts_without_new_followers += 1
                 print(f"[Backup] No *new* unique users. Attempt {scroll_attempts_without_new_followers}/{max_scroll_attempts_without_new_followers}.")
            else:
                 scroll_attempts_without_new_followers = 0
                 print(f"[Backup] New unique users found ({last_found_count} -> {current_follower_count_in_loop}). Resetting failed attempts.")

            last_found_count = current_follower_count_in_loop

            if scroll_attempts_without_new_followers >= max_scroll_attempts_without_new_followers:
                print(f"[Backup] Stopping scrolling: {max_scroll_attempts_without_new_followers} attempts without new users.")
                break

            # ===== Cancellation Check =====
            if cancel_backup_flag:
                print("[Backup] Cancellation signal received before scrolling.")
                cancelled_early = True
                await update.message.reply_text("🟡 Backup is being cancelled...")
                break
            # =========================

            await asyncio.sleep(0.5)
            # ===> CHANGED: Increased scroll multiplier due to zoom <===
            driver.execute_script("window.scrollBy(0, window.innerHeight * 1.5);")
            # ===> END CHANGE <===
            wait_time = random.uniform(2.0, 3.5) # Keep wait time the same for now
            print(f"[Backup] Waiting {wait_time:.1f} seconds for loading...")
            await asyncio.sleep(wait_time)
            # End of the while loop

        # After the loop (normal or cancelled)
        if cancelled_early:
             print(f"[Backup] Process cancelled. {len(found_followers)} users found until then (will not be saved).")
             # IMPORTANT: On cancellation, we save NOTHING to avoid inconsistent backups.
             await update.message.reply_text(f"🛑 Backup cancelled. No file was saved/updated.")
        else:
            print(f"[Backup] Scrolling completed. Found {len(found_followers)} unique users in total.")
            # Save results (only if not cancelled)
            if found_followers:
                # Save ONLY the account-specific backup
                save_set_to_file(found_followers, backup_filepath)
                logger.info(f"[Backup] Account backup for @{account_username} saved to {os.path.basename(backup_filepath)} ({len(found_followers)} users).")
                # Change the success message - NO global update anymore
                success_message = (f"✅ Follower backup for @{account_username} ({len(found_followers)} users) "
                                   f"completed and saved to `{os.path.basename(backup_filepath)}`.\n"
                                   f"(Global list was NOT changed.)")
                await update.message.reply_text(success_message)
            else:
                # Empty backup file if nothing was found
                save_set_to_file(set(), backup_filepath)
                await update.message.reply_text(f"ℹ️ No followers found for @{account_username} or backup file `{os.path.basename(backup_filepath)}` was emptied.")
                logger.info(f"[Backup] No followers found for @{account_username} or backup file cleared.")

    except TimeoutException:
         await update.message.reply_text("❌ Error: Loading the follower list for backup failed (Timeout).")
         print("[Backup] TimeoutException while waiting for UserCells.")
    except Exception as e:
        error_message = f"💥 Critical error during follower backup: {e}"
        await update.message.reply_text(error_message)
        print(error_message)
        import traceback
        traceback.print_exc()

    finally: # ===== IMPORTANT FINALLY BLOCK =====
        print("[Backup] Finally block reached.")
        # Return to the main timeline
        print("[Backup] Attempting to return to the main timeline (/home)...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(4, 6))
            await switch_to_following_tab()
            print("[Backup] Back on /home 'Following' tab.")
            navigation_successful = True
        except Exception as nav_err:
            error_msg = f"⚠️ Error returning to the main timeline after backup: {nav_err}."
            print(error_msg)
            try: await update.message.reply_text(error_msg)
            except: pass

        # Resume main scraping
        print("[Backup] Resuming main scraping.")
        await resume_scraping()

        # ===== Task End Marker =====
        is_backup_running = False
        cancel_backup_flag = False # Ensure flag is false for the next run
        print("[Backup] Status flags reset.")
        # =============================


async def scrape_target_following(update: Update, target_username: str):
    """
    Scrapes the 'Following' list of *any* X user, extracts follower counts
    and updates the `following_database`. (With cancellation option)
    This is the restored working version. Manages the is_db_scrape_running flag.
    """
    global driver, is_scraping_paused, pause_event, following_database
    global is_db_scrape_running, cancel_db_scrape_flag # Import flags

    # Clean the target username
    target_username = target_username.strip().lstrip('@')
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', target_username):
        logger.error(f"[DB Scrape Internal] Invalid target username received: {target_username}")
    if update and hasattr(update, 'message') and update.message:
        await update.message.reply_text(f"❌ Invalid target username: {target_username}")
        return # End early

    # Check if a scrape is already running (important for standalone calls)
    if is_db_scrape_running:
        logger.warning(f"[DB Scrape @{target_username}] Attempted to start while another scrape is running.")
        if update and hasattr(update, 'message') and update.message:
            await update.message.reply_text("⚠️ A database scrape process is already running.")
        return

    # ===== Task Start Marker =====
    is_db_scrape_running = True
    cancel_db_scrape_flag = False # Reset flag for this specific run
    # ================================

    print(f"[DB Scrape @{target_username}] Starting scrape...")
    # Message is sent by the calling command/task if update exists

    await pause_scraping() # Pause main scraping

    processed_in_this_scrape = set()
    users_added_or_updated = 0
    last_found_count = -1
    cancelled_early = False
    navigation_successful = False
    db_changed = False # Flag to know if saving is needed

    # === Helper function for sending status messages ===
    async def _send_scrape_status(text):
        """Sends status either as reply or to main channel."""
        prefix = f"[DB Scrape @{target_username}] "
        if update and hasattr(update, 'message') and update.message:
            try:
                await update.message.reply_text(text)
            except Exception as e:
                logger.error(f"{prefix}Failed to send reply to user: {e}")
                await send_telegram_message(f"{prefix}{text}") # Fallback
        else:
            # If update is None (e.g., called from queue), send to main channel
            await send_telegram_message(f"{prefix}{text}")
    # === End Helper function ===

    try: # Main try block for the entire function
        following_url = f"https://x.com/{target_username}/following"
        print(f"[DB Scrape @{target_username}] Navigating to: {following_url}")
        driver.get(following_url)
        await asyncio.sleep(random.uniform(5, 8)) # Longer wait for external profiles

        # --- Check for private/non-existent profiles ---
        try:
            error_indicators = [
                '//span[contains(text(), "These posts are protected")]',
                '//span[contains(text(), "This account doesn’t exist")]',
                '//span[contains(text(), "Hmm...this page doesn’t exist.")]'
            ]
            error_found = False
            for indicator in error_indicators:
                try:
                    WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, indicator)))
                    error_text = driver.find_element(By.XPATH, indicator).text
                    await _send_scrape_status(f"❌ Error accessing @{target_username}/following: {error_text}")
                    print(f"[DB Scrape @{target_username}] Profile Error: {error_text}")
                    error_found = True
                    break
                except (TimeoutException, NoSuchElementException):
                    continue
            if error_found:
                raise Exception("Profile inaccessible")

        except Exception as profile_check_err:
            if "Profile inaccessible" not in str(profile_check_err):
                 print(f"[DB Scrape @{target_username}] Unexpected error during profile check: {profile_check_err}")
            raise # Re-raise the error to exit the try block

        # --- End Check ---

        user_cell_button_xpath = '//button[@data-testid="UserCell"]'
        # Wait for the first appearance of UserCells
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, user_cell_button_xpath)))
        print(f"[DB Scrape @{target_username}] Found initial UserCell buttons.")

        scroll_attempts_without_new = 0
        max_scroll_attempts_without_new = 5

        # --- Start of the main scroll loop ---
        while scroll_attempts_without_new < max_scroll_attempts_without_new:
            if cancel_db_scrape_flag:
                print(f"[DB Scrape @{target_username}] Cancellation signal received.")
                cancelled_early = True
                await _send_scrape_status("🟡 Database scrape is being cancelled...")
                break

            initial_processed_count = len(processed_in_this_scrape)

            # --- User extraction and processing per scroll view ---
            try: # Try block for finding cells in this view
                user_cells = driver.find_elements(By.XPATH, user_cell_button_xpath)
                print(f"[DB Scrape @{target_username}] Found {len(user_cells)} UserCells in this view.")

                # --- Iterate safely over the found cells ---
                for cell_index, cell_button in enumerate(user_cells):
                    # --- Define variables with default values at the start of EACH iteration ---
                    scraped_username = None
                    follower_count = 0
                    bio_text = "" # Important: Initialize here

                    try: # --- Comprehensive try block for processing a single cell ---
                        if cancel_db_scrape_flag: break # Early cancellation check

                        # 1. Extract username
                        try:
                            relative_link_xpath = ".//a[contains(@href, '/') and not(contains(@href, '/photo'))]"
                            link_element = WebDriverWait(cell_button, 2).until( # Short wait per cell
                                EC.presence_of_element_located((By.XPATH, relative_link_xpath))
                            )
                            href = link_element.get_attribute('href')
                            if href:
                                parts = href.split('/')
                                potential_username = parts[-1].strip()
                                if potential_username and re.match(r'^[A-Za-z0-9_]{1,15}$', potential_username):
                                    scraped_username = potential_username
                                else:
                                    continue # Next cell
                            else:
                                continue # Next cell
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            continue # Next cell
                        except Exception as user_err:
                            print(f"  [DB Scrape @{target_username}] Error (Cell {cell_index}) during username extraction: {user_err}")
                            continue # Next cell

                        # Check: Skip if no username or already processed
                        if not scraped_username or scraped_username in processed_in_this_scrape:
                            continue

                        # 2. Extract bio text from UserCell (CORRECT POSITION)
                        try:
                            bio_div_xpath = './div/div[2]/div[2]'
                            # Short wait, bio is not always there
                            bio_div = WebDriverWait(cell_button, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, bio_div_xpath))
                            )
                            bio_text = bio_div.get_attribute('textContent').strip()
                        except (TimeoutException, NoSuchElementException):
                            pass # No bio is okay
                        except Exception as bio_err:
                            print(f"  [DB Scrape @{target_username}] Error (Cell {cell_index}) extracting bio for @{scraped_username}: {bio_err}")
                        # --- END Bio Extraction ---

                        # 3. Extract follower count (Hover)
                        hover_target_element = None
                        hover_card = None
                        try:
                            hover_target_xpath = './div/div[2]/div[1]/div[1]/div/div[1]/a'
                            try:
                                hover_target_element = cell_button.find_element(By.XPATH, hover_target_xpath)
                            except NoSuchElementException:
                                pass # Continue without follower count

                            if hover_target_element:
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", hover_target_element)
                                await asyncio.sleep(random.uniform(0.4, 0.8))
                                driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));", hover_target_element)
                                wait_for_card_render = random.uniform(1.8, 2.8)
                                await asyncio.sleep(wait_for_card_render)
                                hover_card_xpath = '//div[@data-testid="HoverCard"]'
                                hover_card = WebDriverWait(driver, 6).until(EC.visibility_of_element_located((By.XPATH, hover_card_xpath)))

                                # --- Inner try for HoverCard interaction ---
                                try:
                                    follower_link_xpath = './/a[contains(@href, "/followers") or contains(@href, "/verified_followers")]'
                                    follower_link = WebDriverWait(hover_card, 4).until(EC.presence_of_element_located((By.XPATH, follower_link_xpath)))
                                    possible_text_xpaths = ['./span[1]/span', './span[1]', './/span[contains(text(),"Followers")]/preceding-sibling::span/span', './/span/span', '.']
                                    follower_text = ""
                                    found_text = False
                                    for i, text_xpath in enumerate(possible_text_xpaths):
                                        try:
                                            await asyncio.sleep(0.1)
                                            follower_text_element = follower_link.find_element(By.XPATH, text_xpath)
                                            extracted_text = follower_text_element.get_attribute('textContent')
                                            if extracted_text:
                                                 follower_text = extracted_text.strip()
                                                 if follower_text:
                                                     found_text = True
                                                     break
                                        except (NoSuchElementException, StaleElementReferenceException): continue
                                        except Exception as e_xpath: print(f"  [DB Scrape @{target_username}] Follower text attempt {i+1} ('{text_xpath}') - Unexpected error: {e_xpath}")
                                    if found_text:
                                        follower_count = parse_follower_count(follower_text)
                                        print(f"  [DB Scrape @{target_username}] @{scraped_username} - Parsed followers: {follower_count} (Raw: '{follower_text}')")
                                except TimeoutException as te_inner: print(f"  [DB Scrape @{target_username}] Warning (Cell {cell_index}): Timeout *inside* HoverCard @{scraped_username}. {te_inner}")
                                except (NoSuchElementException, StaleElementReferenceException) as e_inner: print(f"  [DB Scrape @{target_username}] Warning (Cell {cell_index}): Element not found *inside* HoverCard @{scraped_username}. {e_inner}")
                                except Exception as inner_err: print(f"  [DB Scrape @{target_username}] Unexpected error *inside* HoverCard @{scraped_username}: {inner_err}"); logger.warning(f"Unexpected error inside HoverCard processing for {scraped_username}", exc_info=True)
                                finally:
                                    # --- Close HoverCard by scrolling ---
                                    try:
                                        driver.execute_script("window.scrollBy(0, 1);") # Minimal scroll to close
                                        await asyncio.sleep(random.uniform(0.2, 0.4))
                                    except Exception as close_err: print(f"  [DB Scrape @{target_username}] Warning (Cell {cell_index}): Error closing HoverCard for @{scraped_username}: {close_err}")
                        except TimeoutException as te_outer: print(f"  [DB Scrape @{target_username}] Warning (Cell {cell_index}): Timeout waiting for HoverCard for @{scraped_username}. {te_outer}")
                        except (NoSuchElementException, StaleElementReferenceException) as e_outer: print(f"  [DB Scrape @{target_username}] Warning (Cell {cell_index}): Element not found during hover setup for @{scraped_username}. {e_outer}")
                        except Exception as hover_err: print(f"  [DB Scrape @{target_username}] Unexpected error during hover setup for @{scraped_username}: {hover_err}"); logger.warning(f"Unexpected JS hover setup/trigger error for {scraped_username}", exc_info=True)
                        # --- End Follower Count Extraction ---

                        # 4. Update database (with bio)
                        now_iso = datetime.now(timezone.utc).isoformat()
                        if scraped_username in following_database:
                            following_database[scraped_username]["seen_count"] += 1
                            if follower_count > 0 or "follower_count" not in following_database[scraped_username]:
                                following_database[scraped_username]["follower_count"] = follower_count
                            following_database[scraped_username]["bio"] = bio_text
                            following_database[scraped_username]["last_updated"] = now_iso
                            db_changed = True
                            users_added_or_updated += 1
                        else:
                            following_database[scraped_username] = {
                                "follower_count": follower_count,
                                "seen_count": 1,
                                "bio": bio_text,
                                "last_updated": now_iso
                            }
                            db_changed = True
                            users_added_or_updated += 1
                        # --- End Database Update ---

                        # Mark as processed
                        processed_in_this_scrape.add(scraped_username)

                    except StaleElementReferenceException:
                        print(f"  [DB Scrape @{target_username}] Error (Cell {cell_index}): Stale Element Reference. Skipping this cell.")
                        continue # Go to the next cell
                    except Exception as cell_processing_error:
                        print(f"  [DB Scrape @{target_username}] Unexpected error processing cell {cell_index} (User: {'@'+scraped_username if scraped_username else 'Unknown'}): {cell_processing_error}")
                        logger.warning(f"Unexpected error processing cell {cell_index}", exc_info=True)
                        continue # Go to the next cell
                    # --- END of the comprehensive try block for a cell ---

                # --- End of the for loop over user_cells ---
                if cancel_db_scrape_flag: break # Exit outer loop if cancelled

            except Exception as find_err:
                 # Error finding user_cells initially
                 print(f"[DB Scrape @{target_username}] Critical error finding UserCells: {find_err}")
                 break # Abort the outer while loop

            # --- After processing cells in this view ---
            current_processed_count = len(processed_in_this_scrape)
            newly_processed_in_loop = current_processed_count - initial_processed_count

            current_scroll_pos = driver.execute_script("return window.pageYOffset;")
            total_scroll_height = driver.execute_script("return document.body.scrollHeight;")
            print(f"[DB Scrape @{target_username}] Scroll-Pos: {int(current_scroll_pos)}/{int(total_scroll_height)}, Processed={current_processed_count} (+{newly_processed_in_loop} iteration), DB Updates={users_added_or_updated}, Failed attempts={scroll_attempts_without_new}")

            if current_processed_count == last_found_count:
                scroll_attempts_without_new += 1
            else:
                scroll_attempts_without_new = 0

            last_found_count = current_processed_count

            if scroll_attempts_without_new >= max_scroll_attempts_without_new:
                print(f"[DB Scrape @{target_username}] Stopping scrolling: {max_scroll_attempts_without_new} attempts without new users.")
                break

            if cancel_db_scrape_flag: break

            # Scroll for the next round (THIS IS THE ORIGINAL WORKING SCROLL)
            driver.execute_script("window.scrollBy(0, window.innerHeight * 1.5);")
            wait_time = random.uniform(1.5, 2.5) # Keep wait time the same for now
            await asyncio.sleep(wait_time)
            # --- End of the while loop ---

        # --- After the scroll loop (normal or cancelled) ---
        if cancelled_early:
            print(f"[DB Scrape @{target_username}] Process cancelled. {len(processed_in_this_scrape)} users processed until then.")
            if db_changed:
                print("[DB Scrape] Saving database changes made so far...")
                save_following_database()
                await _send_scrape_status(f"🟡 Scrape cancelled. {users_added_or_updated} database updates were saved.")
            else:
                await _send_scrape_status(f"🛑 Scrape cancelled. No database changes made.")
        else:
            print(f"[DB Scrape @{target_username}] Scrolling completed. Processed {len(processed_in_this_scrape)} unique users in total.")
            if db_changed:
                print("[DB Scrape] Saving final database changes...")
                save_following_database()
                await _send_scrape_status(f"✅ Scrape for @{target_username} completed. {users_added_or_updated} database updates performed ({len(following_database)} total).")
            else:
                await _send_scrape_status(f"✅ Scrape for @{target_username} completed. No new updates for the database.")

    except Exception as e:
        # Error handling for the outer try block
        if "Profile inaccessible" in str(e):
             pass # Message was already sent
        else:
            error_message = f"💥 Critical error during DB scrape for @{target_username}: {e}"
            await _send_scrape_status(error_message)
            print(error_message)
            logger.error(f"Critical error during DB scrape for @{target_username}: {e}", exc_info=True)
            # Save DB anyway if changes were made
            if db_changed:
                print("[DB Scrape] Saving database despite error...")
                save_following_database()

    finally: # ===== IMPORTANT FINALLY BLOCK =====
        print(f"[DB Scrape @{target_username}] Finally block reached.")
        # Return to the main timeline
        print(f"[DB Scrape @{target_username}] Attempting to return to the main timeline (/home)...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(4, 6))
            await switch_to_following_tab()
            print(f"[DB Scrape @{target_username}] Back on /home 'Following' tab.")
            navigation_successful = True
        except Exception as nav_err:
            error_msg = f"⚠️ Error returning to the main timeline after DB scrape: {nav_err}."
            print(error_msg)
            await _send_scrape_status(error_msg)

        # Resume main scraping
        print(f"[DB Scrape @{target_username}] Resuming main scraping.")
        await resume_scraping()

        # ===== Task End Marker =====
        # This function now manages the flag for its own execution
        is_db_scrape_running = False
        cancel_db_scrape_flag = False # Reset here as well for safety
        print(f"[DB Scrape @{target_username}] Status flags reset.")
        # =============================

async def recover_followers_logic(update: Update):
    """Reads the account-specific backup and adds users to the
    account-specific follow list (checks against global followed list)."""
    global current_account_usernames_to_follow, global_followed_users_set
    global is_scraping_paused, pause_event # Access for pause/resume

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()
    follow_list_filepath = get_current_follow_list_path()

    if not account_username or not backup_filepath or not follow_list_filepath:
        await update.message.reply_text("❌ Error: Account info/file paths could not be determined.")
        return # Task finished

    # Message is sent by the button handler
    # await update.message.reply_text(f"⏳ Starting recovery for @{account_username} from `{os.path.basename(backup_filepath)}`...")
    print(f"[Recover] Starting recovery for @{account_username} from {backup_filepath}...")

    # This function runs as a task and must manage pause/resume itself.
    await pause_scraping() # Pause main scraping

    try:
        # 1. Read backup file
        backup_users = load_set_from_file(backup_filepath)
        if not backup_users:
            await update.message.reply_text(f"ℹ️ Backup file `{os.path.basename(backup_filepath)}` is empty or does not exist. No recovery possible.")
            # No 'return' here, continue to finally for resume
        else:
            await update.message.reply_text(f"Found: {len(backup_users)} users in the backup file.")

            # 2. Load current follow list for this account (from memory)
            current_follow_list_set = set(current_account_usernames_to_follow)
            print(f"[Recover] Currently {len(current_follow_list_set)} users in the follow list for @{account_username}.")

            # 3. Determine users to add
            users_to_add = set()
            already_followed_globally_count = 0
            already_in_list_count = 0

            for user in backup_users:
                if user in global_followed_users_set:
                    already_followed_globally_count += 1
                elif user in current_follow_list_set:
                    already_in_list_count += 1
                else:
                    users_to_add.add(user)

            if not users_to_add:
                 feedback = f"ℹ️ No new users found to add to the list of @{account_username} from the backup."
                 if already_in_list_count > 0: feedback += f"\n{already_in_list_count} users were already in the list."
                 if already_followed_globally_count > 0: feedback += f"\n{already_followed_globally_count} users are already followed globally."
                 await update.message.reply_text(feedback)
                 # No 'return', continue to finally
            else:
                await update.message.reply_text(f"💾 Adding {len(users_to_add)} new users to the follow list of @{account_username}...")

                # 4. Create combined list, update global variable, and save
                updated_list = list(current_follow_list_set.union(users_to_add))
                current_account_usernames_to_follow = updated_list # Update global variable (list)
                save_current_account_follow_list() # Saves the list under the account path

                await update.message.reply_text(f"✅ Recovery for @{account_username} completed! List now contains {len(current_account_usernames_to_follow)} users.")
                if already_in_list_count > 0: await update.message.reply_text(f"ℹ️ {already_in_list_count} users were already in the list.")
                if already_followed_globally_count > 0: await update.message.reply_text(f"ℹ️ {already_followed_globally_count} users are already followed globally and were not added.")

    except Exception as e:
        print(f"[Recover] Error in the recovery process: {e}")
        try:
            await update.message.reply_text(f"❌ An error occurred during recovery: {e}")
        except: pass # Ignore errors during sending
    finally:
        # Resume main scraping
        print("[Recover] Resuming scraping after recovery.")
        await resume_scraping() # Resume at the end of the task

async def like_tweet(tweet_url):
    """Like a post on X"""
    try:
        print(f"Navigating to post URL: {tweet_url}")
        # Save current URL to return later
        current_url = driver.current_url

        # Navigate to the post URL
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 5))

        # DO NOT scroll on the page - important to avoid click issues
        driver.execute_script("window.scrollTo(0, 0);")
        await asyncio.sleep(1)

        print("Searching for like button...")
        # Find and click the like button - try multiple approaches
        like_button = None
        button_selectors = [
            '//button[@data-testid="like"]',
            '//div[@data-testid="like"]',
            '//div[@role="button" and @data-testid="like"]',
            '//button[contains(@aria-label, "Like")]',
            '//div[contains(@aria-label, "Like")]'
        ]

        for selector in button_selectors:
            try:
                print(f"Trying selector: {selector}")
                like_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if like_button:
                    break
            except:
                continue

        if not like_button:
            print("No like button found")
            # Return to original page
            driver.get(current_url)
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return False

        print("Like button found, trying JS click...")
        # Try JavaScript click instead of normal click
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", like_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", like_button)
        except Exception as e:
            print(f"JS click failed: {e}, trying normal click")
            try:
                like_button.click()
            except Exception as click_error:
                print(f"Normal click also failed: {click_error}")
                # Return to original page
                driver.get(current_url)
                await asyncio.sleep(random.uniform(2, 3))
                await switch_to_following_tab()
                return False

        await asyncio.sleep(random.uniform(2, 3))
        print(f"Successfully liked tweet: {tweet_url}")

        # Return to original page
        driver.get(current_url)
        await asyncio.sleep(random.uniform(2, 3))
        await switch_to_following_tab()

        return True
    except Exception as e:
        print(f"Error liking tweet: {e}")
        # Still try to get back to the timeline
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
        except:
            pass
        return False

def load_current_account_follow_list():
    """Loads the follow list for the current account."""
    global current_account_usernames_to_follow # Updates the global list
    filepath = get_current_follow_list_path()
    account_username = get_current_account_username() or "Unknown"

    if filepath:
        loaded_set = load_set_from_file(filepath)
        current_account_usernames_to_follow = list(loaded_set) # Convert to list for random.choice
        print(f"Follow list for account @{account_username} loaded ({os.path.basename(filepath)}): {len(current_account_usernames_to_follow)} names")
    else:
        print(f"Could not load follow list for account @{account_username}: Path could not be created.")
        current_account_usernames_to_follow = [] # Set to empty list

def save_current_account_follow_list():
    """Saves the follow list for the current account."""
    global current_account_usernames_to_follow
    filepath = get_current_follow_list_path()
    account_username = get_current_account_username() or "Unknown"

    if filepath:
        # Ensure we save a set without duplicates
        save_set_to_file(set(current_account_usernames_to_follow), filepath)
        # print(f"Follow list for account @{account_username} saved ({os.path.basename(filepath)})")
    else:
        print(f"Could not save follow list for account @{account_username}: Path could not be created.")

async def repost_tweet(tweet_url):
    """Repost a post on X"""
    try:
        print(f"Navigating to post URL for repost: {tweet_url}")
        # Save current URL to return later
        current_url = driver.current_url

        # Navigate to the post URL
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 5))

        # DO NOT scroll on the page - important to avoid click issues
        driver.execute_script("window.scrollTo(0, 0);")
        await asyncio.sleep(1)

        print("Searching for repost button...")
        # Find and click the repost button - try multiple approaches
        repost_button = None
        button_selectors = [
            '//button[@data-testid="retweet"]',
            '//div[@data-testid="retweet"]',
            '//div[@role="button" and @data-testid="retweet"]',
            '//button[contains(@aria-label, "Repost")]',
            '//div[contains(@aria-label, "Repost")]'
        ]

        for selector in button_selectors:
            try:
                print(f"Trying repost selector: {selector}")
                repost_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if repost_button:
                    break
            except:
                continue

        if not repost_button:
            print("No repost button found")
            # Return to original page
            driver.get(current_url)
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return False

        print("Repost button found, trying JS click...")
        # Try JavaScript click instead of normal click
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", repost_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", repost_button)
        except Exception as e:
            print(f"JS click failed: {e}, trying normal click")
            try:
                repost_button.click()
            except Exception as click_error:
                print(f"Normal click also failed: {click_error}")
                # Return to original page
                driver.get(current_url)
                await asyncio.sleep(random.uniform(2, 3))
                await switch_to_following_tab()
                return False

        # Wait for the menu to open
        await asyncio.sleep(random.uniform(2, 3))

        print("Searching for confirmation button...")
        # Find and click the confirm repost option - try multiple approaches
        confirm_selectors = [
            '//div[@data-testid="retweetConfirm"]',
            '//span[text()="Repost"]/ancestor::div[@role="menuitem"]',
            '//div[@role="menuitem" and contains(., "Repost")]',
            '//div[contains(@data-testid, "retweet") and @role="menuitem"]'
        ]

        confirm_button = None
        for selector in confirm_selectors:
            try:
                print(f"Trying confirmation selector: {selector}")
                confirm_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if confirm_button:
                    break
            except:
                continue

        if not confirm_button:
            print("No confirmation button found")
            # Escape from menu by clicking elsewhere
            try:
                driver.execute_script("document.body.click();")
            except:
                pass
            # Return to original page
            driver.get(current_url)
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return False

        print("Confirmation button found, trying JS click...")
        # Try JavaScript click for the confirmation button
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", confirm_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", confirm_button)
        except Exception as e:
            print(f"JS click failed: {e}, trying normal click")
            try:
                confirm_button.click()
            except Exception as click_error:
                print(f"Normal click also failed: {click_error}")
                # Return to original page
                driver.get(current_url)
                await asyncio.sleep(random.uniform(2, 3))
                await switch_to_following_tab()
                return False

        await asyncio.sleep(random.uniform(2, 3))
        print(f"Successfully reposted tweet: {tweet_url}")

        # Return to original page
        driver.get(current_url)
        await asyncio.sleep(random.uniform(2, 3))
        await switch_to_following_tab()

        return True
    except Exception as e:
        print(f"Error reposting tweet: {e}")
        # Still try to get back to the timeline
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
        except:
            pass
        return False

async def get_full_tweet_text(tweet_url):
    """Navigates to a post URL, scrapes its full text content, and navigates back."""
    global driver
    try:
        print(f"Navigating to {tweet_url} to get full text...")
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 6)) # Allow time for page load

        # Wait for the main post text element to be present
        tweet_text_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//article[@data-testid="tweet"]//div[@data-testid="tweetText"]'))
        )

        full_text = tweet_text_element.text
        print(f"Successfully scraped full text (length: {len(full_text)}).")

        # --- Navigate back to the main timeline ---
        print("Navigating back to home timeline after getting full text...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 4)) # Wait for home to load
            await switch_to_following_tab() # Ensure we are on the 'Following' tab
            print("Successfully navigated back to home 'Following' tab.")
        except Exception as nav_err:
            print(f"WARNING: Error returning to the main timeline after get_full_text: {nav_err}")
            # Try to recover, but proceed anyway
            try:
                driver.get("https://x.com/home") # Second attempt
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except: pass # Ignore further errors here
        # --- End Navigation Back ---

        return full_text

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"Error finding post text element for {tweet_url}: {e}")
        # --- Navigate back even on error ---
        print("Navigating back to home timeline after finding error in get_full_text...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 4))
            await switch_to_following_tab()
            print("Successfully navigated back to home 'Following' tab after finding error.")
        except Exception as nav_err:
            print(f"WARNING: Error returning to the main timeline after error in get_full_text: {nav_err}")
            # Try to recover, but proceed anyway
            try:
                driver.get("https://x.com/home") # Second attempt
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except: pass # Ignore further errors here
        # --- End Navigation Back ---
        return None # Indicate failure

    except Exception as e:
        print(f"Unexpected error scraping full text for {tweet_url}: {e}")
        logger.error(f"Unexpected error scraping full text for {tweet_url}", exc_info=True)
        # --- Navigate back even on unexpected error ---
        print("Navigating back to home timeline after unexpected error in get_full_text...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 4))
            await switch_to_following_tab()
            print("Successfully navigated back to home 'Following' tab after unexpected error.")
        except Exception as nav_err:
            print(f"WARNING: Error returning to the main timeline after unexpected error in get_full_text: {nav_err}")
            # Try to recover, but proceed anyway
            try:
                driver.get("https://x.com/home") # Second attempt
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except: pass # Ignore further errors here
        # --- End Navigation Back ---
        return None # Indicate failure

def load_schedule():
    """Load schedule settings from file"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    global schedule_sync_enabled, schedule_sync_start_time, schedule_sync_end_time, last_sync_schedule_run_date
    global schedule_follow_list_enabled, schedule_follow_list_start_time, schedule_follow_list_end_time, last_follow_list_schedule_run_date

    # Defaults are already set in global variables.
    # These local defaults are for the data.get() calls if keys are missing.
    # They should reflect the initial global defaults.
    _default_schedule_enabled_local = False
    _default_pause_start_local = "00:00"
    _default_pause_end_local = "00:00"
    _default_sync_enabled_local = False
    _default_sync_start_time_local = "03:00" # Match global default
    _default_sync_end_time_local = "03:30"   # Match global default
    _default_follow_list_enabled_local = False
    _default_follow_list_start_time_local = "04:00" # Match global default
    _default_follow_list_end_time_local = "04:30"   # Match global default

    try:
        if os.path.exists(SCHEDULE_FILE):
            with open(SCHEDULE_FILE, 'r') as f:
                data = json.load(f)
                # Load values from file, using current global values as fallback if key is missing
                # This ensures that if a key is removed from the file, the bot doesn't revert to hardcoded defaults here,
                # but keeps its current (potentially user-set via command) global value.
                schedule_enabled = data.get("enabled", schedule_enabled)
                schedule_pause_start = data.get("pause_start", schedule_pause_start)
                schedule_pause_end = data.get("pause_end", schedule_pause_end)

                schedule_sync_enabled = data.get("schedule_sync_enabled", schedule_sync_enabled)
                schedule_sync_start_time = data.get("schedule_sync_start_time", schedule_sync_start_time)
                schedule_sync_end_time = data.get("schedule_sync_end_time", schedule_sync_end_time)
                schedule_follow_list_enabled = data.get("schedule_follow_list_enabled", schedule_follow_list_enabled)
                schedule_follow_list_start_time = data.get("schedule_follow_list_start_time", schedule_follow_list_start_time)
                schedule_follow_list_end_time = data.get("schedule_follow_list_end_time", schedule_follow_list_end_time)
                last_sync_date_str = data.get("last_sync_schedule_run_date")
                if last_sync_date_str:
                    try: last_sync_schedule_run_date = datetime.strptime(last_sync_date_str, "%Y-%m-%d").date()
                    except ValueError: last_sync_schedule_run_date = None
                else: last_sync_schedule_run_date = None

                last_follow_date_str = data.get("last_follow_list_schedule_run_date")
                if last_follow_date_str:
                    try: last_follow_list_schedule_run_date = datetime.strptime(last_follow_date_str, "%Y-%m-%d").date()
                    except ValueError: last_follow_list_schedule_run_date = None
                else: last_follow_list_schedule_run_date = None
            print("Schedule settings loaded.")
            print(f"  DEBUG LOAD: schedule_sync_start_time='{schedule_sync_start_time}', schedule_sync_end_time='{schedule_sync_end_time}'")
            print(f"  DEBUG LOAD: schedule_follow_list_start_time='{schedule_follow_list_start_time}', schedule_follow_list_end_time='{schedule_follow_list_end_time}'")
        else:
            print(f"No schedule file found ({SCHEDULE_FILE}), using initial default settings and creating the file.")
            # Globals already hold their initial defaults. We just need to ensure last run dates are None.
            last_sync_schedule_run_date = None
            last_follow_list_schedule_run_date = None
            save_schedule() # Create the file with current global (default) settings
            print(f"  DEBUG LOAD (New File Created with Globals): schedule_sync_start_time='{schedule_sync_start_time}', schedule_sync_end_time='{schedule_sync_end_time}'")
            print(f"  DEBUG LOAD (New File Created with Globals): schedule_follow_list_start_time='{schedule_follow_list_start_time}', schedule_follow_list_end_time='{schedule_follow_list_end_time}'")
            last_sync_schedule_run_date = None
            last_follow_list_schedule_run_date = None
            save_schedule() # Create the file with default settings
            print(f"  DEBUG LOAD (New File): schedule_sync_start_time='{schedule_sync_start_time}', schedule_sync_end_time='{schedule_sync_end_time}'")
            print(f"  DEBUG LOAD (New File): schedule_follow_list_start_time='{schedule_follow_list_start_time}', schedule_follow_list_end_time='{schedule_follow_list_end_time}'")
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error loading schedule settings: {e}. Using current global values (likely defaults) and attempting to create/update the file.")
        # Globals retain their current values. We just need to ensure last run dates are None if file is corrupt.
        last_sync_schedule_run_date = None
        last_follow_list_schedule_run_date = None
        save_schedule() # Attempt to create/update the file with current global values
        print(f"  DEBUG LOAD (Exception, Saved Globals): schedule_sync_start_time='{schedule_sync_start_time}', schedule_sync_end_time='{schedule_sync_end_time}'")
        print(f"  DEBUG LOAD (Exception, Saved Globals): schedule_follow_list_start_time='{schedule_follow_list_start_time}', schedule_follow_list_end_time='{schedule_follow_list_end_time}'")

def save_schedule():
    """Save schedule settings to file"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    global schedule_sync_enabled, schedule_sync_start_time, schedule_sync_end_time, last_sync_schedule_run_date
    global schedule_follow_list_enabled, schedule_follow_list_start_time, schedule_follow_list_end_time, last_follow_list_schedule_run_date
    try:
        data = {
            "enabled": schedule_enabled,
            "pause_start": schedule_pause_start,
            "pause_end": schedule_pause_end,

            "schedule_sync_enabled": schedule_sync_enabled,
            "schedule_sync_start_time": schedule_sync_start_time,
            "schedule_sync_end_time": schedule_sync_end_time,
            "last_sync_schedule_run_date": last_sync_schedule_run_date.strftime("%Y-%m-%d") if last_sync_schedule_run_date else None,

            "schedule_follow_list_enabled": schedule_follow_list_enabled,
            "schedule_follow_list_start_time": schedule_follow_list_start_time,
            "schedule_follow_list_end_time": schedule_follow_list_end_time,
            "last_follow_list_schedule_run_date": last_follow_list_schedule_run_date.strftime("%Y-%m-%d") if last_follow_list_schedule_run_date else None,
        }
        with open(SCHEDULE_FILE, 'w') as f:
            json.dump(data, f, indent=4) # Added indent for readability
    except Exception as e:
        print(f"Error saving schedule settings: {e}")

def check_schedule():
    """
    Checks if the current time is within the scheduled pause period.
    Uses timezones for accuracy.
    Returns:
        True: If the bot is running but should pause now.
        "resume": If the bot is paused due to the schedule and should resume now.
        False: If no state change is required due to the schedule.
    """
    global schedule_enabled, schedule_pause_start, schedule_pause_end, is_scraping_paused, is_schedule_pause

    if not schedule_enabled:
        return False

    try:
        # Use the globally configured timezone
        local_tz = USER_CONFIGURED_TIMEZONE
        if local_tz is None: # Should not happen
            print("CRITICAL ERROR: USER_CONFIGURED_TIMEZONE is None in check_schedule. Defaulting to UTC.")
            local_tz = timezone.utc
        
        now_local = datetime.now(local_tz)
        today_local = now_local.date()

        start_naive = datetime.strptime(f"{today_local} {schedule_pause_start}", "%Y-%m-%d %H:%M")
        end_naive = datetime.strptime(f"{today_local} {schedule_pause_end}", "%Y-%m-%d %H:%M")
        start_dt = start_naive.replace(tzinfo=local_tz)
        end_dt = end_naive.replace(tzinfo=local_tz) # End time for TODAY

        is_in_pause_period = False
        # Check if the period crosses midnight
        if end_dt <= start_dt:  # Overnight case (e.g., 22:00 - 09:00)
            # Pause active if:
            # 1) After the start time on the *same* day (e.g., now 23:00, start 22:00)
            # OR
            # 2) Before the end time on the *next* day (e.g., now 08:00, end 09:00) - here we use end_dt from TODAY for comparison
            if now_local >= start_dt or now_local < end_dt:
                 is_in_pause_period = True
        else:  # Same day case (e.g., 10:00 - 17:00)
            # Pause active if between start (incl.) and end (excl.)
            if start_dt <= now_local < end_dt:
                is_in_pause_period = True

    except ValueError:
        print(f"ERROR: Invalid time format in schedule ({schedule_pause_start}-{schedule_pause_end}). Schedule will be ignored.")
        return False
    except Exception as e:
        print(f"ERROR during schedule check: {e}")
        return False

    # --- Decision logic ---
    if is_in_pause_period:
        if not is_scraping_paused:
            return True
        else:
            return False
    else: # Outside pause period
        if is_scraping_paused and is_schedule_pause:
            return "resume"
        else:
            return False

# New functions for Pause/Resume
async def pause_scraping():
    """Pauses post scraping"""
    global is_scraping_paused, driver
    is_scraping_paused = True
    pause_event.clear()
    print("Pausing scraping...")

    # Stop active scrolling operations
    try:
        driver.execute_script("window.stop();")
    except:
        pass

    # Wait briefly to ensure ongoing operations complete
    await asyncio.sleep(1)
    print("Scraping is now paused")
    save_settings() # Save the new pause status

async def resume_scraping():
    """Resumes post scraping"""
    global is_scraping_paused
    is_scraping_paused = False
    pause_event.set()
    print("Resuming scraping")
    save_settings() # Save the new running status

async def scrape_following_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /scrapefollowing <username1> [username2...].
    Queues usernames if headless, otherwise processes them sequentially.
    """
    global is_db_scrape_running, is_headless_enabled, driver, cancel_db_scrape_flag

    # --- Argument Parsing for Multiple Usernames ---
    if not context.args:
        await update.message.reply_text("❌ Please provide at least one X username.\nFormat: `/scrapefollowing <user1>` [user2] ...", parse_mode=ParseMode.MARKDOWN)
        return # Wrapper handles resume

    target_usernames = []
    invalid_usernames = []
    input_text = " ".join(context.args)
    potential_usernames = {name.strip().lstrip('@') for name in re.split(r'[,\s]+', input_text) if name.strip()}

    for username in potential_usernames:
        if re.match(r'^[A-Za-z0-9_]{1,15}$', username):
            target_usernames.append(username)
        else:
            invalid_usernames.append(username)

    if not target_usernames:
        await update.message.reply_text(f"❌ No valid usernames provided. Invalid: {', '.join(invalid_usernames)}" if invalid_usernames else "❌ No usernames provided.")
        return # Wrapper handles resume

    if invalid_usernames:
        await update.message.reply_text(f"⚠️ Invalid usernames skipped: {', '.join(invalid_usernames)}")
    # --- End Argument Parsing ---

    # Check if a scrape is already running (still needed here)
    # Use a short wait/retry loop in case the flag is briefly True from a previous run ending
    for _ in range(3): # Try up to 3 times
        if is_db_scrape_running:
            logger.warning("[Scrape Command] DB scrape seems to be running, waiting briefly...")
            await asyncio.sleep(2)
        else:
            break # Flag is False, proceed
    else: # Loop finished without break
        await update.message.reply_text("⚠️ A database scrape process is still running after waiting. Please wait or use `/canceldbscrape`.")
        return # Wrapper handles resume


    # --- Headless Mode Handling ---
    original_headless_state = is_headless_enabled
    needs_headless_restore = False
    restart_success = True # Assume success initially

    if original_headless_state:
        logger.warning(f"User {update.message.from_user.id} initiated /scrapefollowing with headless ON.")
        # Add all valid usernames to the queue
        queued_count = 0
        failed_queue_count = 0
        for username in target_usernames:
            if add_username_to_scrape_queue(username):
                queued_count += 1
            else:
                failed_queue_count += 1

        queue_msg = f"{queued_count} username(s) added to the processing queue."
        if failed_queue_count > 0:
            queue_msg += f" ({failed_queue_count} failed to queue - check logs)."

        keyboard = [[
            InlineKeyboardButton("✅ Yes, disable Headless & Restart", callback_data=f"headless_scrape:yes"),
            InlineKeyboardButton("❌ No, keep Headless (Queue Only)", callback_data=f"headless_scrape:no")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            f"👻 **Headless Mode Active** 👻\n\n"
            f"Scraping following lists requires interaction and is unreliable in headless mode.\n\n"
            f"{queue_msg}\n\n"
            f"Do you want to disable headless mode now?\n"
            f"(This requires a **bot restart**. Queued scrapes will start automatically after restart if headless is disabled).",
            reply_markup=reply_markup
        )
        # Do NOT start scraping. Wrapper handles resume.
        return
    # --- End Headless Mode Check ---

    # --- Normal Sequential Execution (Headless OFF) ---
    # Start the sequential processing in a background task
    # so the command handler can return quickly.
    await update.message.reply_text(f"✅ Starting sequential database scrape for {len(target_usernames)} users in the background...\n"
                                     f"   Users: {', '.join(target_usernames)}\n"
                                     f"   To cancel: `/canceldbscrape`")

    asyncio.create_task(process_multiple_scrapes_sequentially(update, target_usernames))

    # No resume_scraping here, the task runs independently. The wrapper doesn't resume either.


async def process_multiple_scrapes_sequentially(update: Update, usernames: list):
    """
    Task to process multiple scrape requests one after another.
    Calls scrape_target_following for each username.
    """
    global is_db_scrape_running, cancel_db_scrape_flag

    processed_count = 0
    cancelled = False
    # Reset flag before starting loop
    cancel_db_scrape_flag = False

    logger.info(f"[Multi Scrape Task] Starting sequential processing for {len(usernames)} users.")

    try:
        for i, username in enumerate(usernames):
            # Check cancellation flag before starting each user
            if cancel_db_scrape_flag:
                cancelled = True
                await update.message.reply_text(f"🟡 Scrape sequence cancelled before processing @{username}.")
                logger.warning(f"[Multi Scrape Task] Cancellation requested before processing @{username}.")
                break

            # Check if another scrape is somehow running (safety check)
            if is_db_scrape_running:
                 logger.warning(f"[Multi Scrape Task] is_db_scrape_running is True before calling for @{username}. Waiting...")
                 await update.message.reply_text(f"⏳ Waiting for previous scrape to finish before starting @{username}...")
                 while is_db_scrape_running:
                     await asyncio.sleep(5)
                     if cancel_db_scrape_flag: # Check again during wait
                         cancelled = True
                         break
                 if cancelled:
                     await update.message.reply_text(f"🟡 Scrape sequence cancelled while waiting for @{username}.")
                     break
                 logger.info(f"[Multi Scrape Task] Previous scrape finished. Proceeding with @{username}.")


            await update.message.reply_text(f"⏳ Processing user {i+1}/{len(usernames)}: @{username}...")
            logger.info(f"[Multi Scrape Task] Calling scrape_target_following for @{username} ({i+1}/{len(usernames)}).")

            # Call the core scraping logic for a single user
            # scrape_target_following now manages is_db_scrape_running and pause/resume
            try:
                # Pass the update object
                await scrape_target_following(update, username)
                # scrape_target_following will set is_db_scrape_running to False in its finally block

                # Check flag again *after* the call completes
                if cancel_db_scrape_flag:
                    cancelled = True
                    logger.warning(f"[Multi Scrape Task] Cancellation detected after processing @{username}.")
                    break
            except Exception as single_scrape_err:
                logger.error(f"[Multi Scrape Task] Error during scrape_target_following call for @{username}: {single_scrape_err}", exc_info=True)
                await update.message.reply_text(f"❌ Error scraping @{username}. See logs. Continuing...")
                # Ensure flags are reset if sub-task failed badly
                is_db_scrape_running = False
                if is_scraping_paused: await resume_scraping()

            processed_count += 1
            # Optional short delay between users
            if not cancelled: # Don't wait if cancelled
                logger.debug(f"[Multi Scrape Task] Waiting before next user...")
                await asyncio.sleep(random.uniform(5, 10))

    except Exception as loop_err:
        logger.error(f"[Multi Scrape Task] Unexpected error in processing loop: {loop_err}", exc_info=True)
        await update.message.reply_text(f"💥 Critical error during multi-scrape task. Check logs.")
    finally:
        # Final cleanup and status message
        is_db_scrape_running = False # Ensure flag is reset finally
        cancel_db_scrape_flag = False
        logger.info(f"[Multi Scrape Task] Sequence finished. Processed Attempts: {processed_count}, Cancelled: {cancelled}")

        summary_msg = f"🏁 Multi-Scrape Sequence Finished 🏁\n"
        summary_msg += f"   - Total Users Requested: {len(usernames)}\n"
        summary_msg += f"   - Processed Attempts: {processed_count}\n"
        if cancelled:
            summary_msg += f"   - Status: Cancelled 🛑\n"
        else:
            summary_msg += f"   - Status: Completed ✅\n"

        await update.message.reply_text(summary_msg)

        # Ensure main scraping is resumed if it was left paused
        if is_scraping_paused:
            logger.warning("[Multi Scrape Task] Scraping was paused at the end. Resuming.")
            await resume_scraping()

async def add_from_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /addfromdb with flexible filters. Adds users from the
    following_database to the follow list of the *current* account.

    Syntax: /addfromdb [followers:NUM] [seen:NUM] [keywords:WORD1 WORD2...]
    At least one criterion must be specified.
    """
    global following_database, current_account_usernames_to_follow, global_followed_users_set
    # is_scraping_paused is handled by the admin wrapper

    # --- Initialize filter variables ---
    min_followers = -1 # -1 means: not specified/active
    min_seen = 0      # 0 means: not specified/active (default was 1, changing here)
    keywords = []
    criteria_count = 0 # Counts how many criteria were specified

    # --- Parse arguments ---
    current_keyword_parsing_mode = None # To handle multi-word keyword values
    if context.args:
        for arg in context.args:
            arg_lower = arg.lower()

            # Check for followers (short 'f:' or long 'followers:')
            if arg_lower.startswith("followers:") or arg_lower.startswith("f:"):
                try:
                    key_name = "followers" if arg_lower.startswith("followers:") else "f"
                    value_str = arg.split(":", 1)[1]
                    if not value_str:
                        await update.message.reply_text(f"❌ Missing value for '{key_name}:'.")
                        return
                    min_followers = parse_follower_count(value_str)
                    criteria_count += 1
                    current_keyword_parsing_mode = None # Reset keyword mode
                except IndexError: # Should be caught by "not value_str"
                    await update.message.reply_text(f"❌ Missing value for '{key_name}:'.")
                    return
                # parse_follower_count handles internal errors and returns 0

            # Check for seen (short 's:' or long 'seen:')
            elif arg_lower.startswith("seen:") or arg_lower.startswith("s:"):
                try:
                    key_name = "seen" if arg_lower.startswith("seen:") else "s"
                    value_str = arg.split(":", 1)[1]
                    if not value_str:
                        await update.message.reply_text(f"❌ Missing value for '{key_name}:'.")
                        return
                    min_seen = int(value_str)
                    if min_seen < 1:
                        await update.message.reply_text(f"❌ Value for '{key_name}:' must be >= 1.")
                        return
                    criteria_count += 1
                    current_keyword_parsing_mode = None # Reset keyword mode
                except (ValueError, IndexError):
                    await update.message.reply_text(f"❌ Invalid value for '{key_name}:'. Please provide a number >= 1.")
                    return

            # Check for keywords (short 'k:' or long 'keywords:')
            elif arg_lower.startswith("keywords:") or arg_lower.startswith("k:"):
                try:
                    value_part = arg.split(":", 1)[1]
                    # Split by comma OR space, remove empty entries
                    found_kws = [kw.strip() for kw in re.split(r'[,\s]+', value_part) if kw.strip()]
                    if found_kws:
                        keywords.extend(found_kws)
                        criteria_count += 1 # Count criteria only if keywords are actually provided
                    current_keyword_parsing_mode = "keywords" # Enter keyword mode for subsequent args
                except IndexError: # No value after 'keywords:' or 'k:'
                    current_keyword_parsing_mode = "keywords" # Still enter mode, expect keywords in next arg

            elif current_keyword_parsing_mode == "keywords":
                # Subsequent arguments are treated as keywords if in keyword mode
                found_kws = [kw.strip() for kw in re.split(r'[,\s]+', arg) if kw.strip()]
                if found_kws:
                    keywords.extend(found_kws)
                    if criteria_count == 0 or not keywords: # Ensure criteria_count is incremented if this is the first time keywords are added
                        criteria_count +=1
                # Stay in keyword mode for more keywords unless a new criterion starts
            else:
                await update.message.reply_text(f"❓ Unknown argument or missing key: '{arg}'.\nUse `followers:`(or `f:`), `seen:`(or `s:`), or `keywords:`(or `k:`).")
                return

    # --- CORRECTED: Final cleanup of keywords ---
    # Ensure all keywords are lowercase and unique
    if keywords:
        keywords = sorted(list(set(kw.lower() for kw in keywords if kw))) # Unique, lowercase, sorted

    # --- Check if at least one criterion was specified ---
    if criteria_count == 0:
        await update.message.reply_text(
            "❌ Please specify at least one filter criterion.\n"
            "Syntax: `/addfromdb [followers:NUM] [seen:NUM] [keywords:WORD1 WORD2...]`\n"
            "Examples:\n"
            "`/addfromdb followers:100000`\n"
            "`/addfromdb keywords:crypto nft`\n"
            "`/addfromdb seen:3 keywords:developer`\n"
            "`/addfromdb followers:5000 seen:2 keywords:web3`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # --- Get account info ---
    account_username = get_current_account_username()
    current_follow_list_path = get_current_follow_list_path()

    if not account_username or not current_follow_list_path:
        await update.message.reply_text("❌ Error: Active account username/list path not found.")
        return

    if not following_database:
        await update.message.reply_text("ℹ️ The following database is empty. Run `/scrapefollowing` first.")
        return

    # --- Filter users from DB based on *active* criteria ---
    qualified_users = set()
    print(f"[AddFromDB] Filter: followers>={min_followers}, seen>={min_seen}, keywords={keywords}") # Debug
    for username, data in following_database.items():
        # Qualified by default, set to False if criteria not met
        is_qualified = True

        # 1. Follower check (only if criterion is active)
        if min_followers != -1:
            f_count = data.get("follower_count", -1) # -1 if not present
            if not isinstance(f_count, int) or f_count < min_followers:
                is_qualified = False
                # print(f"  - @{username} disqualified (Followers: {f_count} < {min_followers})") # Debug

        # 2. Seen check (only if criterion is active and still qualified)
        if is_qualified and min_seen != 0:
            s_count = data.get("seen_count", 0)
            if not isinstance(s_count, int) or s_count < min_seen:
                is_qualified = False
                # print(f"  - @{username} disqualified (Seen: {s_count} < {min_seen})") # Debug

        # 3. Keyword check (only if criterion is active and still qualified)
        if is_qualified and keywords:
            bio_lower = data.get("bio", "").lower()
            # Check if *all* specified keywords are present in the bio
            if not all(kw in bio_lower for kw in keywords):
                is_qualified = False
                # print(f"  - @{username} disqualified (Keywords not all found in bio)") # Debug

        # If still qualified after all active checks, add
        if is_qualified:
            qualified_users.add(username)
            # print(f"  + @{username} qualified!") # Debug

    if not qualified_users:
        criteria_str = []
        if min_followers != -1: criteria_str.append(f"F>={min_followers}")
        if min_seen != 0: criteria_str.append(f"S>={min_seen}")
        if keywords: criteria_str.append(f"KW='{' '.join(keywords)}'")
        await update.message.reply_text(f"ℹ️ No users in the database meet the criteria ({', '.join(criteria_str)}).")
        return

    # --- Filter against global and current list (as before) ---
    added_to_current_account = set()
    already_followed_globally = set()
    already_in_current_list = set()

    current_list_set = set(current_account_usernames_to_follow)

    for username in qualified_users:
        if username in global_followed_users_set:
            already_followed_globally.add(username)
        elif username in current_list_set:
            already_in_current_list.add(username)
        else:
            added_to_current_account.add(username)

    # --- Build result message ---
    criteria_summary = []
    if min_followers != -1: criteria_summary.append(f"Followers ≥ {min_followers}")
    if min_seen != 0: criteria_summary.append(f"Seen ≥ {min_seen}")
    if keywords: criteria_summary.append(f"Keywords: '{' '.join(keywords)}'")
    response = f"📊 Filter Result ({', '.join(criteria_summary)}):\n"
    response += f"- {len(qualified_users)} users qualified.\n"

    if added_to_current_account:
        current_account_usernames_to_follow.extend(list(added_to_current_account))
        save_current_account_follow_list()
        response += f"✅ {len(added_to_current_account)} users added to the list of @{account_username}.\n"

    if already_in_current_list:
         response += f"ℹ️ {len(already_in_current_list)} of them were already in the list of @{account_username}.\n"
    if already_followed_globally:
        response += f"🚫 {len(already_followed_globally)} of them are already followed globally.\n"

    if not added_to_current_account and not already_in_current_list and not already_followed_globally and qualified_users:
         response += "ℹ️ All qualified users are already followed globally or in the list.\n"

    await update.message.reply_text(response.strip())
    # resume_scraping is done by the admin wrapper

async def cancel_db_scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Requests cancellation of the ongoing database scrape process."""
    global is_db_scrape_running, cancel_db_scrape_flag
    if is_db_scrape_running:
        cancel_db_scrape_flag = True
        await update.message.reply_text("🟡 Cancellation of database scrape requested. It might take a moment...")
        print("[Cancel] DB Scrape cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ No database scrape process is currently running.")
    # No resume/pause here, this command only affects the flag

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries with improved logging and robustness."""
    global current_account_usernames_to_follow
    global search_keywords_enabled, search_ca_enabled, search_tickers_enabled # Add these here
    global schedule_enabled, schedule_sync_enabled, schedule_follow_list_enabled # For other toggles
    global is_headless_enabled # For headless toggle
    global auto_follow_mode, auto_follow_interval_minutes # For autofollow mode buttons
    query = update.callback_query
    # === 1. Answer IMMEDIATELY ===
    try:
        await query.answer()
        logger.debug(f"CallbackQuery answered for data: {query.data}")
    except Exception as answer_err:
        logger.error(f"FATAL: Failed to answer CallbackQuery for data {query.data}: {answer_err}", exc_info=True)
        return

    # === 2. Main logic with comprehensive Try-Except ===
    try:
        if not query.data:
            logger.warning("Received CallbackQuery with no data.")
            return

        parts = query.data.split(":", 1)
        action_type = parts[0]
        logger.info(f"Button Click Received: Type='{action_type}', Data='{query.data}'")
        emulated_update = type('obj', (object,), {'message': query.message})

        # ===== SYNC CALLBACKS =====
        if action_type == "sync":
            logger.info(f"Processing sync callback: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
            if len(parts) < 2:
                 logger.warning("Invalid sync callback format received.")
                 await query.edit_message_text("❌ Invalid sync callback format.")
                 return

            # Split action and optional username
            action_parts = parts[1].split(":", 1)
            action = action_parts[0]
            # Extract the username *always*, if present (for consistency check)
            target_username_from_callback = action_parts[1] if len(action_parts) > 1 else None

            current_active_username = get_current_account_username()

            # --- Consistency check: Is the account still the same? ---
            # Perform this check for all actions that have a target_username
            if target_username_from_callback and target_username_from_callback != current_active_username:
                logger.warning(f"Sync target mismatch: Button was for @{target_username_from_callback}, but @{current_active_username} is now active.")
                await query.edit_message_text(f"❌ Error: Button was for @{target_username_from_callback}, but @{current_active_username} is now active. Please run /syncfollows again.")
                return

            # --- Process actions ---
            # Admin check is already done by handle_callback_query
            if action == "create_backup": # Offered only in the "No Backup" case
                await query.edit_message_text("✅ Backup for the current account is starting in the background...")
                # Create an emulated update instance for backup_followers_logic
                emulated_update_for_backup = type('obj', (object,), {'message': query.message})
                asyncio.create_task(backup_followers_logic(emulated_update_for_backup))
                logger.info("Follower backup task started via sync callback (create_backup option).")

            elif action == "proceed": # This is the case "Backup missing/empty, user wants to add"
                await query.edit_message_text(f"✅ Sync (add only) for @{current_active_username} is starting in the background...")
                backup_filepath = get_current_backup_file_path()
                # Load the global list fresh before the task starts
                global_set_for_task = load_set_from_file(GLOBAL_FOLLOWED_FILE)
                # Create an emulated update instance for sync_followers_logic
                emulated_update_for_sync = type('obj', (object,), {'message': query.message})
                asyncio.create_task(sync_followers_logic(emulated_update_for_sync, current_active_username, backup_filepath, global_set_for_task))
                logger.info("Follower sync task started via sync callback (proceed - no backup case).")

            elif action == "proceed_sync": # This is the case "Backup exists, user confirms sync"
                await query.edit_message_text(f"✅ Sync for @{current_active_username} is starting in the background...")
                backup_filepath = get_current_backup_file_path()
                # Load the global list fresh before the task starts
                global_set_for_task = load_set_from_file(GLOBAL_FOLLOWED_FILE)
                # Create an emulated update instance for sync_followers_logic
                emulated_update_for_sync = type('obj', (object,), {'message': query.message})
                asyncio.create_task(sync_followers_logic(emulated_update_for_sync, current_active_username, backup_filepath, global_set_for_task))
                logger.info("Follower sync task started via sync callback (proceed_sync - normal case).")

            elif action == "cancel_sync": # This is the cancel handler (used for both cases)
                username_display = f" for @{target_username_from_callback}" if target_username_from_callback else ""
                await query.edit_message_text(f"❌ Synchronization{username_display} cancelled.")
                logger.info(f"Sync cancelled by user (cancel_sync callback for {target_username_from_callback or 'N/A'}).")
                await resume_scraping()
            else:
                logger.warning(f"Unknown sync action received: {action}")
                await query.edit_message_text(f"❌ Unknown sync action: {action}")
                await resume_scraping()

            return # Sync Callbacks don't need resume here

        # ===== CLEAR FOLLOW LIST CALLBACKS =====
        elif action_type == "confirm_clear_follow_list":
             logger.info("Processing confirm_clear_follow_list callback.")
             # Admin check is already done by handle_callback_query

             if len(parts) < 2:
                  logger.warning("Invalid clear_follow_list callback format.")
                  await query.edit_message_text("❌ Invalid clear callback format.")
                  return
             target_username = parts[1]
             current_active_username = get_current_account_username()
             if target_username == current_active_username:
                 current_account_usernames_to_follow = []
                 save_current_account_follow_list()
                 filepath = get_current_follow_list_path()
                 filename = os.path.basename(filepath) if filepath else "N/A"
                 await query.edit_message_text(f"🗑️ Follow list for @{current_active_username} (`{filename}`) has been cleared.")
                 logger.info(f"Follow list for @{current_active_username} cleared via button.")
                 await resume_scraping()
             else:
                  logger.warning(f"Clear list target mismatch: Button for {target_username}, active is {current_active_username}")
                  await query.edit_message_text(f"❌ Error: Button was for account @{target_username}, but @{current_active_username} is active.")
                  await resume_scraping()
             return # Quick action, no resume

        elif action_type == "cancel_clear_follow_list":
            logger.info("Processing cancel_clear_follow_list callback.")
            await query.edit_message_text("❌ Clearing the follow list cancelled.")
            await resume_scraping()
            return # Quick action, no resume

        # ===== BUILD GLOBAL FROM BACKUPS CALLBACKS =====
        elif action_type == "confirm_build_global":
            logger.info("Processing confirm_build_global callback.")
            try:
                combined_set = set()
                for i, account_info in enumerate(ACCOUNTS):
                    acc_username = account_info.get("username")
                    if not acc_username: continue
                    safe_username = re.sub(r'[\\/*?:"<>|]', "_", acc_username)
                    backup_filepath = FOLLOWER_BACKUP_TEMPLATE.format(safe_username)
                    if os.path.exists(backup_filepath):
                        backup_set = load_set_from_file(backup_filepath)
                        combined_set.update(backup_set)
                current_global_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
                final_global_set = current_global_set.union(combined_set)
                global global_followed_users_set
                save_set_to_file(final_global_set, GLOBAL_FOLLOWED_FILE)
                global_followed_users_set = final_global_set
                logger.info(f"Global follower list updated from all backups. New global count: {len(global_followed_users_set)}")
                await query.edit_message_text(f"✅ Global follower list successfully updated ({len(final_global_set)} users).")
            except Exception as e:
                 logger.error(f"Error during build_global process: {e}", exc_info=True)
                 await query.edit_message_text(f"❌ Error updating the global list: {e}")
            return # Quick action, no resume

        elif action_type == "cancel_build_global":
            logger.info("Processing cancel_build_global callback.")
            await query.edit_message_text("❌ Update of the global list cancelled.")
            return # Quick action, no resume

        # ===== HELP CALLBACKS =====
        elif action_type == "help":
            if len(parts) < 2:
                 logger.warning("Invalid help callback format.")
                 await query.edit_message_text("❌ Invalid help callback format.")
                 return # No resume here, this is a quick action
            payload = parts[1] 
            logger.info(f"Processing help payload: {payload}")

            # --- Tasks ---
            if payload == "backup_followers":
                 await query.message.reply_text("✅ Follower backup is starting in the background...")
                 asyncio.create_task(backup_followers_logic(emulated_update))
                 logger.info("Follower backup task started via help callback.")
                 return 
            elif payload == "sync_follows": # This is for MANUAL sync
                 await query.message.reply_text("✅ Starting check for follower synchronization...")
                 await sync_followers_command(emulated_update, None) # context is None
                 logger.info("sync_followers_command called via help callback.")
                 return 

            # --- NEUER HANDLER: Toggle Pause/Resume ---
            elif payload == "toggle_pause_resume":
                logger.info("Processing toggle_pause_resume help payload.")
                if is_scraping_paused:
                    await resume_command(emulated_update, None)
                else:
                    await pause_command(emulated_update, None)
                try: 
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_pause_resume":
                                    new_button_text = ("RUNNING 🟢" if not is_scraping_paused else
                                                       ("SCHEDULE PAUSED 🟡" if is_schedule_pause else
                                                        "PAUSED 🟡"))
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_pause_resume"))
                                else: new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating pause/resume button: {e}")
                return

            # --- NEUER HANDLER: Toggle Main Schedule ---
            elif payload == "toggle_main_schedule":
                logger.info("Processing toggle_main_schedule help payload.")
                if schedule_enabled:
                    await schedule_off_command(emulated_update, None)
                else:
                    await schedule_on_command(emulated_update, None)
                try: 
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_main_schedule":
                                    new_button_text = f"⏰ Main Sched. {'🟢' if schedule_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_main_schedule"))
                                else: new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating main schedule button: {e}")
                return

            # # --- ANGEPASSTER HANDLER: Mode Full ---
            # elif payload == "mode_full":
            #     logger.info("Processing mode_full help payload.")
            #     await mode_full_command(emulated_update, None) 
            #     try: 
            #         original_markup = query.message.reply_markup
            #         if original_markup:
            #             new_keyboard = []
            #             for row in original_markup.inline_keyboard:
            #                 new_row = []
            #                 for button_in_row in row:
            #                     if button_in_row.callback_data == "help:mode_full":
            #                         new_row.append(InlineKeyboardButton(f"🔍 FULL {'🟢' if search_mode == 'full' else '⚪'}", callback_data="help:mode_full"))
            #                     elif button_in_row.callback_data == "help:mode_ca": 
            #                         new_row.append(InlineKeyboardButton(f"🔍 CA {'🟢' if search_mode == 'ca_only' else '⚪'}", callback_data="help:mode_ca"))
            #                     else: new_row.append(button_in_row)
            #                 new_keyboard.append(new_row)
            #             await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
            #     except Exception as e: logger.error(f"Error updating mode buttons after mode_full: {e}")
            #     return

            # # --- ANGEPASSTER HANDLER: Mode CA ---
            # elif payload == "mode_ca":
            #     logger.info("Processing mode_ca help payload.")
            #     await mode_ca_command(emulated_update, None) 
            #     try: 
            #         original_markup = query.message.reply_markup
            #         if original_markup:
            #             new_keyboard = []
            #             for row in original_markup.inline_keyboard:
            #                 new_row = []
            #                 for button_in_row in row:
            #                     if button_in_row.callback_data == "help:mode_ca":
            #                         new_row.append(InlineKeyboardButton(f"🔍 CA {'🟢' if search_mode == 'ca_only' else '⚪'}", callback_data="help:mode_ca"))
            #                     elif button_in_row.callback_data == "help:mode_full": 
            #                         new_row.append(InlineKeyboardButton(f"🔍 FULL {'🟢' if search_mode == 'full' else '⚪'}", callback_data="help:mode_full"))
            #                     else: new_row.append(button_in_row)
            #                 new_keyboard.append(new_row)
            #             await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
            #     except Exception as e: logger.error(f"Error updating mode buttons after mode_ca: {e}")
            #     return
            
            # --- Andere direkte Befehlsaufrufe (stelle sicher, dass jeder mit 'return' endet) ---
            elif payload == "stats": await stats_command(emulated_update, None); return
            elif payload == "ping": await ping_command(emulated_update, None); return
            elif payload == "keywords": await keywords_command(emulated_update, None); return
            elif payload == "account": await account_command(emulated_update, None); return
            elif payload == "schedule": await schedule_command(emulated_update, None); return
            elif payload == "mode": await mode_command(emulated_update, None); return
            elif payload == "help": await help_command(emulated_update, None); return
            elif payload == "show_rates": await show_ratings_command(emulated_update, None); return
            elif payload == "build_global": await build_global_from_backups_command(emulated_update, None); return
            elif payload == "status": await status_command(emulated_update, None); return
            elif payload == "autofollow_status": await autofollow_status_command(emulated_update, None); return
            elif payload == "cancel_fast_follow": await cancel_fast_follow_command(emulated_update, None); return
            elif payload == "autofollow_mode_off":
                mock_context = type('obj', (object,), {'args': ["off"]})
                await autofollow_mode_command(emulated_update, mock_context); return
            elif payload == "autofollow_mode_slow":
                mock_context = type('obj', (object,), {'args': ["slow"]})
                await autofollow_mode_command(emulated_update, mock_context); return
            elif payload == "autofollow_mode_fast":
                mock_context = type('obj', (object,), {'args': ["fast"]})
                await autofollow_mode_command(emulated_update, mock_context); return
            elif payload == "show_all_schedules": 
                await show_detailed_schedules_command(emulated_update, None); return

            # --- "Prepare" Payloads (send only text) ---
            elif payload == "prepare_addusers":
                 logger.info("Processing prepare_addusers help payload.")
                 await query.message.reply_text("Copy, add usernames:\n\n`/addusers `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_addkeyword":
                 logger.info("Processing prepare_addkeyword help payload.")
                 await query.message.reply_text("Copy, add keywords (comma-separated):\n\n`/addkeyword `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_removekeyword":
                 logger.info("Processing prepare_removekeyword help payload.")
                 await query.message.reply_text("Copy, add keywords (comma-separated):\n\n`/removekeyword `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_follow":
                 logger.info("Processing prepare_follow help payload.")
                 await query.message.reply_text("Copy and add the username:\n\n`/follow `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_unfollow":
                 logger.info("Processing prepare_unfollow help payload.")
                 await query.message.reply_text("Copy and add the username:\n\n`/unfollow `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_like":
                 logger.info("Processing prepare_like help payload.")
                 await query.message.reply_text("Copy and add the post URL:\n\n`/like `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_repost":
                 logger.info("Processing prepare_repost help payload.")
                 await query.message.reply_text("Copy and add the post URL:\n\n`/repost `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_switchaccount":
                 logger.info("Processing prepare_switchaccount help payload.")
                 await query.message.reply_text("Copy and add the account number:\n\n`/switchaccount `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_scheduletime": 
                 logger.info("Processing prepare_scheduletime help payload.")
                 await query.message.reply_text("Copy and add the time range (HH:MM-HH:MM):\n\n`/scheduletime `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_schedule_sync_time": 
                 logger.info("Processing prepare_schedule_sync_time help payload.")
                 await query.message.reply_text(f"Copy and add time window (HH:MM-HH:MM) for Scheduled Sync:\n\n`/schedulesynctime {schedule_sync_start_time}-{schedule_sync_end_time}`", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_schedule_follow_list_time": 
                 logger.info("Processing prepare_schedule_follow_list_time help payload.")
                 await query.message.reply_text(f"Copy and add time window (HH:MM-HH:MM) for Scheduled Follow List:\n\n`/schedulefollowlisttime {schedule_follow_list_start_time}-{schedule_follow_list_end_time}`", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "set_schedule": # Alias, behalten für Abwärtskompatibilität falls Buttons noch existieren
                 logger.info("Processing set_schedule help payload (now prepare_scheduletime).")
                 await query.message.reply_text("Copy and add the time range (HH:MM-HH:MM):\n\n`/scheduletime `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_autofollow_interval":
                 logger.info("Processing prepare_autofollow_interval help payload.")
                 current_interval = f"{auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]}"
                 await query.message.reply_text(f"Copy, change Min/Max (minutes):\n\n`/autofollowinterval {current_interval}`", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return

            # --- Bestehende Toggle Payloads ---
            elif payload == "toggle_schedule_sync": 
                global schedule_sync_enabled
                schedule_sync_enabled = not schedule_sync_enabled
                save_schedule()
                status_text = "ENABLED 🟢" if schedule_sync_enabled else "DISABLED 🔴"
                logger.info(f"Scheduled Sync toggled to {status_text} via help button by user {query.from_user.id}")
                await query.answer(f"Scheduled Sync: {status_text}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_schedule_sync":
                                    new_button_text = f"🔄 Sched. Sync {'🟢' if schedule_sync_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_schedule_sync"))
                                else: new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for Sched. Sync: {e}")
                return 

            elif payload == "toggle_schedule_follow_list": 
                global schedule_follow_list_enabled
                schedule_follow_list_enabled = not schedule_follow_list_enabled
                save_schedule()
                status_text = "ENABLED 🟢" if schedule_follow_list_enabled else "DISABLED 🔴"
                logger.info(f"Scheduled Follow List toggled to {status_text} via help button by user {query.from_user.id}")
                await query.answer(f"Scheduled Follow List: {status_text}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_schedule_follow_list":
                                    new_button_text = f"🚶‍♂️‍➡️ Sched. Follow {'🟢' if schedule_follow_list_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_schedule_follow_list"))
                                else: new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for Sched. Follow: {e}")
                return 

            elif payload == "toggle_keywords":
                # global search_keywords_enabled # REMOVE THIS LINE
                logger.info("Processing toggle_keywords help payload.")
                search_keywords_enabled = not search_keywords_enabled
                save_settings()
                status_text = "ENABLED 🟢" if search_keywords_enabled else "DISABLED 🔴"
                logger.info(f"Keyword search toggled to {status_text} via help button by user {query.from_user.id}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_keywords":
                                    new_button_text = f"🔑 Keywords {'🟢' if search_keywords_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_keywords"))
                                elif button.callback_data == "help:toggle_ca": # Keep other buttons in row updated
                                    new_button_text = f"📝 CA {'🟢' if search_ca_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_ca"))
                                elif button.callback_data == "help:toggle_tickers": # Keep other buttons in row updated
                                    new_button_text = f"💲 Ticker {'🟢' if search_tickers_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_tickers"))
                                else:
                                    new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for keyword toggle: {e}")
                await query.answer(f"Keyword search: {status_text}")
                return

            elif payload == "toggle_ca":
                # global search_ca_enabled # REMOVE THIS LINE
                logger.info("Processing toggle_ca help payload.")
                search_ca_enabled = not search_ca_enabled
                save_settings()
                status_text = "ENABLED 🟢" if search_ca_enabled else "DISABLED 🔴"
                logger.info(f"CA search toggled to {status_text} via help button by user {query.from_user.id}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_ca":
                                    new_button_text = f"📝 CA {'🟢' if search_ca_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_ca"))
                                elif button.callback_data == "help:toggle_keywords": # Keep other buttons in row updated
                                    new_button_text = f"🔑 Keywords {'🟢' if search_keywords_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_keywords"))
                                elif button.callback_data == "help:toggle_tickers": # Keep other buttons in row updated
                                    new_button_text = f"💲 Ticker {'🟢' if search_tickers_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_tickers"))
                                else:
                                    new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for CA toggle: {e}")
                await query.answer(f"CA search: {status_text}")
                return

            elif payload == "toggle_tickers":
                logger.info("Processing toggle_tickers help payload.")
                #global search_tickers_enabled
                search_tickers_enabled = not search_tickers_enabled
                save_settings()
                status_text = "ENABLED 🟢" if search_tickers_enabled else "DISABLED 🔴"
                logger.info(f"Ticker search toggled to {status_text} via help button by user {query.from_user.id}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_tickers":
                                    new_button_text = f"💲 Ticker {'🟢' if search_tickers_enabled else '🔴'}" # Emoji geändert
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_tickers"))
                                else: new_row.append(button) 
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for ticker toggle: {e}")
                await query.answer(f"Ticker search: {status_text}") 
                return

            elif payload == "toggle_headless":
                logger.info("Processing toggle_headless help payload.")
                global is_headless_enabled
                is_headless_enabled = not is_headless_enabled
                save_settings()
                status_text = "ENABLED 🟢" if is_headless_enabled else "DISABLED 🔴"
                logger.info(f"Headless mode toggled to {status_text} via help button by user {query.from_user.id}")
                try:
                    original_markup = query.message.reply_markup
                    if original_markup:
                        new_keyboard = []
                        for row in original_markup.inline_keyboard:
                            new_row = []
                            for button in row:
                                if button.callback_data == "help:toggle_headless":
                                    new_button_text = f"👻 Headless {'🟢' if is_headless_enabled else '🔴'}"
                                    new_row.append(InlineKeyboardButton(new_button_text, callback_data="help:toggle_headless"))
                                else: new_row.append(button)
                            new_keyboard.append(new_row)
                        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                except Exception as e: logger.error(f"Error updating help button for headless toggle: {e}")
                await query.answer(f"Headless mode: {status_text}") 
                await query.message.reply_text(f"✅ Headless mode is now {status_text}. Restarting WebDriver...")
                await restart_driver_and_login(query)
                return

            elif payload == "cancel_action":
                 logger.info("Processing cancel_action.")
                 try: await query.edit_message_text("✅ Action cancelled.")
                 except Exception as e: logger.error(f"Error editing message on cancel_action: {e}", exc_info=True)
                 await resume_scraping(); return

            else:
                 logger.warning(f"Unknown help payload received: {payload}")
                 await query.message.reply_text(f"❌ Unknown help action: {payload}")
                 await resume_scraping(); return
            # --- End Help Payloads ---

        # ===== LIKE/REPOST CALLBACKS (Queueing with full markup info) =====
        elif action_type in ["like", "repost"]:
             if len(parts) < 2 or not parts[1].isdigit():
                  logger.warning(f"Invalid {action_type} callback format: {query.data}")
                  try: await query.answer(f"❌ Invalid format.", show_alert=True)
                  except: pass
                  return

             tweet_id = parts[1]
             action_description = f"{action_type} for post {tweet_id}"
             logger.info(f"Queueing action: {action_description}")

             # --- Immediate Feedback (change only the clicked button) ---
             original_markup = query.message.reply_markup
             new_keyboard = []
             original_button_data = [] # To store button data for the queue
             try:
                 await query.answer(f"⏳ {action_type.capitalize()} queued...")
                 if original_markup:
                     for row_idx, row in enumerate(original_markup.inline_keyboard):
                         new_row = []
                         original_button_data.append([]) # New row for queue data
                         for button in row:
                             original_button_data[row_idx].append({'text': button.text, 'callback_data': button.callback_data}) # Store original data
                             if button.callback_data == query.data:
                                 new_row.append(InlineKeyboardButton(f"{action_type.capitalize()} (⏳)", callback_data="noop_processing"))
                             else:
                                 new_row.append(button)
                         new_keyboard.append(new_row)

                 if new_keyboard:
                     await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                     logger.debug(f"Edited specific button for {action_description} to show queued state.")
                 else:
                     logger.warning(f"Could not reconstruct keyboard for {action_description}")

             except telegram.error.BadRequest as e:
                 if "message is not modified" in str(e).lower(): pass
                 else: logger.error(f"BadRequest editing reply markup for {action_description}: {e}", exc_info=True)
             except Exception as edit_err:
                 logger.error(f"Failed to edit reply markup for {action_description}: {edit_err}", exc_info=True)
             # --- End Feedback ---

             # --- Put action into queue (with original button data) ---
             try:
                 await action_queue.put((action_type, {
                     'tweet_id': tweet_id,
                     'chat_id': query.message.chat_id,
                     'message_id': query.message.message_id,
                     'original_callback_data': query.data, # The clicked button
                     'original_keyboard_data': original_button_data # The structure of all buttons
                 }))
                 logger.info(f"Action {action_description} successfully added to queue with keyboard data.")
             except Exception as q_err:
                 logger.error(f"Failed to put action {action_description} into queue: {q_err}", exc_info=True)
                 try: await query.message.reply_text(f"❌ Error queuing action '{action_type}'.")
                 except: pass
             return # Handler is finished here

        # ===== RATING CALLBACKS (Quick action) =====
        elif action_type == "rate":
            logger.debug(f"Processing rate action: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
            if len(parts) < 2:
                 logger.warning("Invalid rating callback format.")
                 await query.answer("❌ Error: Invalid rating format.", show_alert=True)
                 return
            try:
                sub_parts = parts[1].split(":", 2)
                if len(sub_parts) != 3:
                    logger.warning(f"Invalid rating format details: {parts[1]}")
                    await query.answer("❌ Error: Invalid rating format (details).", show_alert=True)
                    return
                rating_value_str, source_key, encoded_name = sub_parts
                rating_value = int(rating_value_str)
                try: decoded_name = base64.urlsafe_b64decode(encoded_name).decode()
                except Exception as dec_err: logger.error(f"Failed to decode name '{encoded_name}': {dec_err}", exc_info=True); decoded_name = source_key
                if not (1 <= rating_value <= 5):
                    logger.warning(f"Invalid rating value received: {rating_value}")
                    await query.answer("❌ Invalid value (1-5).", show_alert=True)
                    return

                global ratings_data; entry_needs_update = False
                if source_key not in ratings_data:
                    ratings_data[source_key] = {"name": decoded_name, "ratings": {str(i): 0 for i in range(1, 6)}}; entry_needs_update = True
                    logger.debug(f"Initialized new rating entry for {source_key} with name '{decoded_name}'")
                elif "ratings" not in ratings_data[source_key] or not isinstance(ratings_data[source_key].get("ratings"), dict):
                     ratings_data[source_key] = {"name": decoded_name, "ratings": {str(i): 0 for i in range(1, 6)}}; entry_needs_update = True
                     logger.warning(f"Fixed rating structure for {source_key}, using name '{decoded_name}'")
                elif ratings_data[source_key].get("name") != decoded_name:
                     ratings_data[source_key]["name"] = decoded_name; entry_needs_update = True
                     logger.info(f"Updated name for {source_key} to '{decoded_name}'")

                rating_key_str = str(rating_value)
                current_count = ratings_data[source_key]["ratings"].get(rating_key_str, 0)
                ratings_data[source_key]["ratings"][rating_key_str] = current_count + 1
                entry_needs_update = True
                if entry_needs_update: save_ratings()
                logger.info(f"Rating saved: {source_key} -> {rating_value} stars")

                try: # Optional: Remove buttons
                    original_markup = query.message.reply_markup; new_keyboard = []
                    if original_markup:
                        for row in original_markup.inline_keyboard:
                            is_rating_header = any(b.callback_data == "rate_noop" for b in row)
                            is_rating_buttons = any(b.callback_data.startswith("rate:") for b in row)
                            if not is_rating_header and not is_rating_buttons: new_keyboard.append(row)
                    if new_keyboard: await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                    else: await query.edit_message_reply_markup(reply_markup=None)
                except Exception as edit_err: logger.warning(f"Could not edit reply markup after rating: {edit_err}")
            except ValueError:
                logger.warning(f"Invalid rating value format: {parts[1]}", exc_info=True)
                await query.answer("❌ Error: Invalid rating value.", show_alert=True)
            except Exception as rate_err:
                logger.error(f"Error processing rating for data {query.data}: {rate_err}", exc_info=True)
                await query.answer("❌ Error saving.", show_alert=True)
            return # Quick action, no resume

        elif action_type == "rate_noop":
             logger.debug("Ignoring click on rate_noop button.")
             return # Quick action, no resume


        # ===== FULL TEXT CALLBACK (Queueing with full markup info) =====
        elif action_type == "full":
            if len(parts) < 2 or not parts[1].isdigit():
                logger.warning(f"Invalid full_text callback format: {query.data}")
                try: await query.answer("❌ Error: Invalid format.", show_alert=True)
                except: pass
                return

            tweet_id = parts[1]
            action_description = f"Get full text for {tweet_id}"
            logger.info(f"Queueing action: {action_description}")

            # --- Immediate Feedback (change only the clicked button) ---
            original_markup = query.message.reply_markup
            new_keyboard = []
            original_button_data = [] # To store button data for the queue
            try:
                await query.answer("⏳ Full Text queued...")
                if original_markup:
                    for row_idx, row in enumerate(original_markup.inline_keyboard):
                        new_row = []
                        original_button_data.append([]) # New row for queue data
                        for button in row:
                            original_button_data[row_idx].append({'text': button.text, 'callback_data': button.callback_data}) # Store original data
                            if button.callback_data == query.data:
                                new_row.append(InlineKeyboardButton("Loading Text (⏳)", callback_data="noop_processing"))
                            else:
                                new_row.append(button)
                        new_keyboard.append(new_row)

                if new_keyboard:
                    await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                    logger.debug(f"Edited specific button for {action_description} to show queued state.")
                else:
                     logger.warning(f"Could not reconstruct keyboard for {action_description}")

            except telegram.error.BadRequest as e:
                 if "message is not modified" in str(e).lower(): pass
                 else: logger.error(f"BadRequest editing reply markup for {action_description}: {e}", exc_info=True)
            except Exception as edit_err:
                logger.error(f"Failed to edit reply markup for {action_description}: {edit_err}", exc_info=True)
            # --- End Feedback ---

            # --- Put action into queue (with original button data) ---
            try:
                await action_queue.put((action_type, {
                    'tweet_id': tweet_id,
                    'chat_id': query.message.chat_id,
                    'message_id': query.message.message_id,
                    'original_callback_data': query.data, # The clicked button
                    'original_keyboard_data': original_button_data # The structure of all buttons
                }))
                logger.info(f"Action {action_description} successfully added to queue with keyboard data.")
            except Exception as q_err:
                logger.error(f"Failed to put action {action_description} into queue: {q_err}", exc_info=True)
                try: await query.message.reply_text(f"❌ Error queuing action 'Full Text'.")
                except: pass
            return # Handler is finished here


        # ===== FULL TEXT CALLBACK (Long-running action - DEPRECATED, use queueing above) =====
        # This block is kept for reference but should ideally be removed if queueing works well.
        elif action_type == "full_text":
            logger.warning("Received deprecated 'full_text' callback. Use 'full' for queueing.")
            await query.answer("ℹ️ This action is deprecated.", show_alert=True)
            return # End deprecated action

        # ===== STATUS CALLBACKS =====
        elif action_type == "status":
            logger.info(f"Processing status callback: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
            if len(parts) < 2:
                 logger.warning("Invalid status callback format received.")
                 await query.edit_message_text("❌ Invalid status callback format.")
                 return # Status Callbacks don't need resume here

            action = parts[1]
            if action == "show_follow_list":
                # Show the current follow list
                account_username = get_current_account_username() or "Unknown"
                filepath = get_current_follow_list_path()
                filename = os.path.basename(filepath) if filepath else "N/A"

                list_content = current_account_usernames_to_follow
                if not list_content:
                    await query.message.reply_text(f"ℹ️ The follow list for @{account_username} (`{filename}`) is currently empty.")
                else:
                    # Prepare the list for display
                    max_users_to_show = 100 # Limit to avoid overloading messages
                    output_text = f"📝 **Follow List for @{account_username} (`{filename}`)** ({len(list_content)} Users):\n\n"
                    output_text += "\n".join([f"- `{user}`" for user in list_content[:max_users_to_show]]) # Markdown code formatting

                    if len(list_content) > max_users_to_show:
                        output_text += f"\n\n*... and {len(list_content) - max_users_to_show} more users.*"

                    # Send as a new message (do not edit the status message)
                    try:
                        await query.message.reply_text(output_text, parse_mode=ParseMode.MARKDOWN)
                    except telegram.error.BadRequest as e:
                         if "message is too long" in str(e).lower():
                              await query.message.reply_text(f"❌ Error: The follow list is too long ({len(list_content)} users) to display.")
                         else: raise e # Re-raise other errors
                    except Exception as e:
                         logger.error(f"Error sending the follow list: {e}", exc_info=True)
                         await query.message.reply_text("❌ Error displaying the list.")
                # No resume_scraping here, as it's a quick action
                return
            else:
                logger.warning(f"Unknown status action received: {action}")
                await query.edit_message_text(f"❌ Unknown status action: {action}")
            return # Status Callbacks don't need resume here

        # ===== UNKNOWN ACTION =====
        else:
             logger.warning(f"Unknown button action type received: {action_type}")
             await query.message.reply_text(f"❌ Unknown button action: {action_type}")
             await resume_scraping() # Resume just in case
             return

    # === 3. General Fallback Error Handler ===
    except Exception as e:
        logger.error(f"Unhandled error in button_callback_handler for data '{query.data}': {e}", exc_info=True)
        try: await query.message.reply_text(text=f"❌ Unexpected error during button action. See logs for details.")
        except Exception as send_error: logger.error(f"Failed to send error message to user after unhandled exception: {send_error}")
        if is_scraping_paused:
             logger.warning("Attempting to resume scraping after unhandled exception in button handler.")
             await resume_scraping()

def admin_required(func):
    """
    Decorator that checks if the executing user is an admin.
    Works for CommandHandler.
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.message or not update.message.from_user:
            logger.warning(f"Admin check failed for function {func.__name__}: No user object.")
            return # Cannot check

        user_id = update.message.from_user.id
        if is_user_admin(user_id):
            # User is admin, execute the original function
            return await func(update, context, *args, **kwargs)
        else:
            # User is not admin, send error message
            logger.warning(f"Unauthorized access to '{func.__name__}' by user {user_id}.")
            await update.message.reply_text("❌ Access denied. You are not an admin.")
            # Important: Resume scraping if the command would have paused
            if 'pause_scraping' in func.__code__.co_names or 'resume_scraping' in func.__code__.co_names:
                 if is_scraping_paused: # Only if it was paused
                      print(f"Admin Check: Resuming scraping after denied access to {func.__name__}")
                      await resume_scraping()
            return # Do not execute the function
    return wrapper

# --- Keywords ---
async def keywords_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the current keywords (Handler for /keywords)."""
    await pause_scraping() # Pause for the duration of the command
    global KEYWORDS
    keywords_text = "\n".join([f"- {keyword}" for keyword in KEYWORDS])
    await update.message.reply_text(f"🔑 Current Keywords:\n{keywords_text}")
    await resume_scraping() # Resume after the command

async def add_keyword_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Adds one or more keywords (Handler for /addkeyword)."""
    await pause_scraping()
    global KEYWORDS
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Please provide the keywords after the command (comma-separated).\n\n"
            "Format: `/addkeyword <word1,word2...>`\n\n"
            "Copy this and add your keywords:\n"
            "`/addkeyword `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    keyword_text = " ".join(context.args)
    keywords_to_add = [k.strip().lower() for k in keyword_text.split(',') if k.strip()] # Enforce lowercase
    added = []
    already_exists = []

    for keyword in keywords_to_add:
        if keyword not in KEYWORDS:
            KEYWORDS.append(keyword)
            added.append(keyword)
        else:
            already_exists.append(keyword)

    if added: # Only save if something was added
        await save_keywords()

    response = ""
    if added:
        response += f"✅ {len(added)} keywords added: {', '.join(added)}\n"
    if already_exists:
        response += f"⚠️ {len(already_exists)} keywords already exist: {', '.join(already_exists)}"

    await update.message.reply_text(response.strip() if response else "No new keywords found to add.")
    # Show the updated list (internally calls resume_scraping)
    await keywords_command(update, context)
    # No resume_scraping here, as keywords_command does it

async def remove_keyword_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes one or more keywords (Handler for /removekeyword)."""
    await pause_scraping()
    global KEYWORDS
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Please provide the keywords after the command (comma-separated).\n\n"
            "Format: `/removekeyword <word1,word2...>`\n\n"
            "Copy this and add your keywords:\n"
            "`/removekeyword `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    keyword_text = " ".join(context.args)
    keywords_to_remove = [k.strip().lower() for k in keyword_text.split(',') if k.strip()] # Lowercase
    removed = []
    not_found = []

    changed = False
    for keyword in keywords_to_remove:
        if keyword in KEYWORDS:
            KEYWORDS.remove(keyword)
            removed.append(keyword)
            changed = True
        else:
            not_found.append(keyword)

    if changed: # Only save if something was removed
        await save_keywords()

    response = ""
    if removed:
        response += f"🗑️ {len(removed)} keywords removed: {', '.join(removed)}\n"
    if not_found:
        response += f"⚠️ {len(not_found)} keywords not found: {', '.join(not_found)}"

    await update.message.reply_text(response.strip() if response else "No keywords found to remove.")
    # Show the updated list (internally calls resume_scraping)
    await keywords_command(update, context)
    # No resume_scraping here

# --- Follow / Unfollow ---
async def follow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Follows a user (Handler for /follow)."""
    await pause_scraping()
    # --- CHANGED: Check argument count ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Please provide exactly ONE X username after the command.\n\n"
            "Format: `/follow <username>` (with or without @)\n\n"
            "Copy this and add the username:\n"
            "`/follow `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping() # Resume before returning due to invalid arguments
        return
    # --- END CHANGE ---

    username = context.args[0].lstrip('@').strip()
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', username):
        await update.message.reply_text("❌ Invalid username format.") # Added emoji
        await resume_scraping()
        return




    # --- Logic moved from process_follow_request here ---
    global global_followed_users_set
    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if not account_username or not backup_filepath:
         await update.message.reply_text("❌ Error: Active account cannot be determined for follow update.")
         await resume_scraping()
         return

    await update.message.reply_text(f"⏳ Trying to follow @{username} with account @{account_username}...")
    result = await follow_user(username) # Perform the follow attempt

    if result is True:
        # Success message is now sent by follow_user()
        print(f"Manual follow successful: @{username}")
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
            print(f"@{username} added to global followed list.")
        add_to_set_file({username}, backup_filepath)
        print(f"@{username} added to account backup ({os.path.basename(backup_filepath)}).")
    elif result == "already_following":
        # Message is sent by follow_user()
        print(f"Manual follow: @{username} was already followed.")
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
        add_to_set_file({username}, backup_filepath)
    else: # Error case (result is False)
        # Error message is now sent by follow_user()
        print(f"Manual follow failed: @{username}")
    # --- End Logic ---
    await resume_scraping() # Resume scraping at the end of the command

async def unfollow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unfollows a user, removes them from the global list and current backup."""
    await pause_scraping()
    # --- CHANGED: Check argument count ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Please provide exactly ONE X username after the command.\n\n"
            "Format: `/unfollow <username>` (with or without @)\n\n"
            "Copy this and add the username:\n"
            "`/unfollow `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return
    # --- END CHANGE ---

    username_to_unfollow = context.args[0].lstrip('@').strip()
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', username_to_unfollow):
        await update.message.reply_text("❌ Invalid username format.") # Added emoji
        await resume_scraping()
        return

    # Access global variable
    global global_followed_users_set

    await update.message.reply_text(f"🔍 Trying to unfollow @{username_to_unfollow}...")
    result = await unfollow_user(username_to_unfollow)

    removed_from_global = False
    removed_from_backup = False
    current_backup_path = get_current_backup_file_path()
    current_account_username = get_current_account_username() or "Unknown"

    response_message = ""

    # --- Logic based on Selenium result AND list consistency ---
    if result == "not_following" or result is True:
        # Selenium was successful OR the account wasn't following anymore anyway.
        # NOW clean up the lists.
        if result is True:
             response_message = f"✅ Successfully unfollowed @{username_to_unfollow}!"
        else: # result == "not_following"
             response_message = f"ℹ️ Account @{current_account_username} is not (or no longer) following @{username_to_unfollow}."

        # Remove from global list (if still there)
        if username_to_unfollow in global_followed_users_set:
            global_followed_users_set.discard(username_to_unfollow)
            save_set_to_file(global_followed_users_set, GLOBAL_FOLLOWED_FILE)
            removed_from_global = True
            logger.info(f"User @{username_to_unfollow} removed from global list.")
        else:
            logger.debug(f"User @{username_to_unfollow} was already not in global list.")

        # Remove from current backup (if still there)
        if current_backup_path:
            backup_set = load_set_from_file(current_backup_path)
            if username_to_unfollow in backup_set:
                backup_set.discard(username_to_unfollow)
                save_set_to_file(backup_set, current_backup_path)
                removed_from_backup = True
                logger.info(f"User @{username_to_unfollow} removed from backup for @{current_account_username}.")
            else:
                logger.debug(f"User @{username_to_unfollow} was already not in backup for @{current_account_username}.")

    else: # Error during unfollow (result is False)
        response_message = f"❌ Could not unfollow @{username_to_unfollow} (Selenium error). Lists were NOT changed."
        logger.warning(f"Selenium failed to unfollow @{username_to_unfollow}. Lists remain unchanged.")

    # Add additional info about list changes to the main message
    if removed_from_global:
        response_message += f"\n🗑️ @{username_to_unfollow} removed from global list."
    if removed_from_backup:
        response_message += f"\n🗑️ @{username_to_unfollow} removed from backup for @{current_account_username}."

    await update.message.reply_text(response_message)
    await resume_scraping()

# --- Like / Repost ---
async def like_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Likes a post via URL (Handler for /like)."""
    await pause_scraping()
    # --- CHANGED: Argument count and URL validation ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Please provide exactly ONE post URL after the command.\n\n"
            "Format: `/like <tweet_url>`\n\n"
            "Copy this and add the URL:\n"
            "`/like `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    input_url = context.args[0].strip()
    parsed_url = None
    try:
        # Try to parse the URL
        parsed_url = urlparse(input_url)
        # Check scheme and domain
        if not parsed_url.scheme in ['http', 'https'] or not (parsed_url.netloc.endswith('x.com') or parsed_url.netloc.endswith('twitter.com')):
            raise ValueError("Not a valid X domain")
        # Check if the path looks like a post (optional, but recommended)
        # Example: /<username>/status/<id>
        if not re.match(r'^/[A-Za-z0-9_]+/status/\d+$', parsed_url.path):
             # Also allow /i/status/<id>
             if not re.match(r'^/i/status/\d+$', parsed_url.path):
                  raise ValueError("URL does not seem to be a post link")
        # Reconstruct the URL for consistency (optional)
        tweet_url = parsed_url.geturl()

    except ValueError as e:
        await update.message.reply_text(f"❌ Invalid or unsupported URL: {e}.\nPlease provide a complete X.com post URL.")
        await resume_scraping()
        return
    except Exception as e: # Other parsing errors
         await update.message.reply_text(f"❌ Error processing the URL: {e}")
         await resume_scraping()
         return
    # --- END CHANGE ---

    # --- Existing logic ---
    await update.message.reply_text(f"🔍 Trying to like tweet: {tweet_url}")
    try:
        result = await like_tweet(tweet_url)
        if result: await update.message.reply_text(f"✅ post successfully liked!")
        else: await update.message.reply_text(f"❌ Could not like tweet")
    except Exception as e:
        await update.message.reply_text(f"❌ Error liking: {str(e)[:100]}")
    # --- End Existing Logic ---
    await resume_scraping()

async def repost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reposts a post via URL (Handler for /repost)."""
    await pause_scraping()
    # --- CHANGED: Argument count and URL validation ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Please provide exactly ONE post URL after the command.\n\n"
            "Format: `/repost <tweet_url>`\n\n"
            "Copy this and add the URL:\n"
            "`/repost `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    input_url = context.args[0].strip()
    parsed_url = None
    try:
        # Try to parse the URL
        parsed_url = urlparse(input_url)
        # Check scheme and domain
        if not parsed_url.scheme in ['http', 'https'] or not (parsed_url.netloc.endswith('x.com') or parsed_url.netloc.endswith('twitter.com')):
            raise ValueError("Not a valid X/Twitter domain")
        # Check if the path looks like a tweet
        if not re.match(r'^/[A-Za-z0-9_]+/status/\d+$', parsed_url.path):
             if not re.match(r'^/i/status/\d+$', parsed_url.path):
                  raise ValueError("URL does not seem to be a post link")
        tweet_url = parsed_url.geturl()

    except ValueError as e:
        await update.message.reply_text(f"❌ Invalid or unsupported URL: {e}.\nPlease provide a complete X.com post URL.")
        await resume_scraping()
        return
    except Exception as e: # Other parsing errors
         await update.message.reply_text(f"❌ Error processing the URL: {e}")
         await resume_scraping()
         return
    # --- END CHANGE ---

    # --- Existing logic ---
    await update.message.reply_text(f"🔍 Trying to repost tweet: {tweet_url}")
    try:
        result = await repost_tweet(tweet_url)
        if result: await update.message.reply_text(f"✅ post successfully reposted!")
        else: await update.message.reply_text(f"❌ Could not repost tweet")
    except Exception as e:
        await update.message.reply_text(f"❌ Error reposting: {str(e)[:100]}")
    # --- End Existing Logic ---
    await resume_scraping()

# --- Account / Reboot / Shutdown / Help / Stats / Ping ---
async def account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the current account (Handler for /account)."""
    await pause_scraping()
    await update.message.reply_text(f"🥷 Current Account: {current_account+1} (@{get_current_account_username() or 'N/A'})")
    await resume_scraping()

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the help message (Handler for /help)."""
    # await pause_scraping() # REMOVED - show_help_message handles its own needs
    # The show_help_message function needs to be adapted to show the new syntax
    await show_help_message(update) # show_help_message handles resume itself

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays statistics (Handler for /stats)."""
    await pause_scraping()
    await show_post_counts(update) # show_post_counts handles resume itself

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Replies with Pong (Handler for /ping)."""
    await pause_scraping()
    await update.message.reply_text(f"🏓 Pong!")
    await resume_scraping()

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays a comprehensive operational status of the bot."""
    # --- Global declarations must be at the very top of the function scope ---
    global is_scraping_paused, is_schedule_pause, search_mode, schedule_enabled, \
           schedule_pause_start, schedule_pause_end, is_periodic_follow_active, \
           current_account, ACCOUNTS, global_followed_users_set, \
           current_account_usernames_to_follow, GLOBAL_FOLLOWED_FILE, \
           auto_follow_mode, auto_follow_interval_minutes, is_fast_follow_running, \
           search_tickers_enabled, is_headless_enabled, USER_TIMEZONE_STR # Added USER_TIMEZONE_STR

    # --- Store initial state AFTER global declarations ---
    initial_pause_state = is_scraping_paused 

    # --- Gather information ---


    # 1. Running status
    if is_scraping_paused:
        running_status = "🟡 PAUSED (Schedule)" if is_schedule_pause else "🟡 PAUSED (Manual)"
    else:
        running_status = "🟢 RUNNING"

    # 2. Current account
    current_username = get_current_account_username() or "N/A"
    account_info = f"Acc {current_account+1} (@{current_username})"

    # 4. Schedule
    schedule_status = "🟢" if schedule_enabled else "🔴"
    schedule_details = f"{schedule_status} ({schedule_pause_start} - {schedule_pause_end})"

    # 5. Global follow list info
    global_list_count = len(global_followed_users_set) # In-memory is usually current enough for status
    global_list_mod_time_str = "N/A"
    try:
        if os.path.exists(GLOBAL_FOLLOWED_FILE):
            mod_timestamp = os.path.getmtime(GLOBAL_FOLLOWED_FILE)
            try:
                local_tz = ZoneInfo("Europe/Berlin")
            except Exception:
                local_tz = timezone(timedelta(hours=2)) # Fallback
            mod_datetime_local = datetime.fromtimestamp(mod_timestamp, tz=local_tz)
            # Only date and time for compactness
            global_list_mod_time_str = mod_datetime_local.strftime('%Y-%m-%d %H:%M')
        else:
             global_list_count = 0 # If file doesn't exist
    except Exception as e:
        logger.error(f"Error getting global list info for status: {e}")
        global_list_mod_time_str = "Error"
    global_list_info = f"{global_list_count} Users (As of: {global_list_mod_time_str})"

    # 6. Auto-Follow Status (for current account)
    autofollow_stat = auto_follow_mode.upper() # Start with mode name
    if auto_follow_mode == "slow":
        autofollow_stat = f"SLOW ({auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]} min) ▶️"
    elif auto_follow_mode == "fast":
        autofollow_stat = "FAST 🚀"
        if is_fast_follow_running: # Check if the task is running
            autofollow_stat += " (Running...)"
    elif auto_follow_mode == "off":
        autofollow_stat = "OFF ⏸️"
    # Fallback, shouldn't happen
    else:
        autofollow_stat = f"Unknown ({auto_follow_mode}) ❓"

    # 7. Current Account Follow List Info + Preview
    current_list_path = get_current_follow_list_path()
    current_list_filename = os.path.basename(current_list_path) if current_list_path else "N/A"
    current_list_count = len(current_account_usernames_to_follow)
    current_list_preview = ""
    if current_list_count > 0:
        max_preview = 30
        # Take the first users from the list for the preview
        preview_list = current_account_usernames_to_follow[:max_preview]
        # Format each user as code
        current_list_preview = "\n".join([f"    - `{user}`" for user in preview_list])
        if current_list_count > max_preview:
            current_list_preview += f"\n    ... and {current_list_count - max_preview} more."
    else:
        current_list_preview = "    (List is empty)"
    # Info line for the list
    current_list_info = f"{current_list_count} Users in {current_list_filename}"

    # 7. New Schedules Status
    sync_sched_disp = f"Sync: {'🟢 ON' if schedule_sync_enabled else '🔴 OFF'} at {schedule_sync_start_time}-{schedule_sync_end_time}"
    fl_sched_disp = f"FollowList: {'🟢 ON' if schedule_follow_list_enabled else '🔴 OFF'} at {schedule_follow_list_start_time}-{schedule_follow_list_end_time}"
    new_schedules_info = f"{sync_sched_disp}\n   └ {fl_sched_disp}"

    # --- Build message ---
    ticker_status_text = "ENABLED 🟢" if search_tickers_enabled else "DISABLED 🔴"
    headless_status_text = "ENABLED 🟢" if is_headless_enabled else "DISABLED 🔴"
    status_message = (
        f"📊 **Bot Overall Status** 📊\n\n"
        f"{'▶️' if not is_scraping_paused else ('⏸️⏰' if is_schedule_pause else '⏸️🟡')} **Operation:** {running_status}\n"
        f"🥷 **Active Account:** {account_info}\n"
        f"🔑 **Keyword 🔎:** {'ENABLED 🟢' if search_keywords_enabled else 'DISABLED 🔴'}\n"
        f"📝 **CA 🔎:** {'ENABLED 🟢' if search_ca_enabled else 'DISABLED 🔴'}\n"
        f"💲 **Ticker 🔎:** {ticker_status_text}\n"
        f"⏰ **Main Schedule:** {schedule_details}\n"
        f"👻 **Headless Mode:** {headless_status_text}\n" 
        f"🌍 **Global Follow List:** {global_list_info}\n"
        f"🤖 **Auto-Follow (Curr. Acc):** {autofollow_stat}\n"
        f"🗓️ **Other Schedules:**\n   └ {new_schedules_info}\n" 
        f"📝 **Follow List (Curr. Acc):** {current_list_info}\n"
        f"{current_list_preview}"
    )

    # --- Send message ---
    # We remove the old button as the list is now displayed directly.
    await update.message.reply_text(status_message, parse_mode=ParseMode.MARKDOWN)

# --- Mode ---
async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the current search mode (Handler for /mode)."""
    await pause_scraping()
    global search_mode
    mode_text = "Keywords" if search_mode == "full" else "CA Only"
    await update.message.reply_text(f"🔍 Search mode: {mode_text}")
    await resume_scraping()

async def mode_full_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the search mode to CA + Keywords (Handler for /modefull)."""
    await pause_scraping()
    global search_mode
    if search_mode != "full": # Only save if something changes
        search_mode = "full"
        save_settings() # Save the setting
        await update.message.reply_text("✅ Search mode set to Keywords")
    else:
        await update.message.reply_text("ℹ️ Search mode is already Keywords.")
    await resume_scraping()

async def mode_ca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the search mode to CA only (Handler for /modeca)."""
    await pause_scraping()
    global search_mode
    if search_mode != "ca_only": # Only save if something changes
        search_mode = "ca_only"
        save_settings() # Save the setting
        await update.message.reply_text("✅ Search mode set to CA Only")
    else:
        await update.message.reply_text("ℹ️ Search mode is already CA Only.")
    await resume_scraping()

# --- Pause / Resume ---
async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pauses scraping (Handler for /pause)."""
    # No pause_scraping() here, as we want to pause!
    global is_schedule_pause
    await update.message.reply_text(f"⏸️ Pausing scraping...")
    is_schedule_pause = False  # Manual pause
    await pause_scraping() # The actual pause action
    await update.message.reply_text(f"⏸️ Scraping has been paused! Use `/resume` to continue.")
    # NO resume_scraping()!

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resumes scraping (Handler for /resume)."""
    # No pause_scraping() here
    await update.message.reply_text(f"▶️ Resuming scraping...")
    await resume_scraping() # The actual resume action
    await update.message.reply_text(f"▶️ Scraping is running again!")

# --- Schedule ---
async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the schedule settings (Handler for /schedule)."""
    await pause_scraping()
    await show_schedule(update) # show_schedule handles resume itself

async def schedule_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Activates the schedule (Handler for /scheduleon)."""
    await pause_scraping()
    await set_schedule_enabled(update, True) # set_schedule_enabled handles resume itself

async def schedule_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deactivates the schedule (Handler for /scheduleoff)."""
    await pause_scraping()
    await set_schedule_enabled(update, False) # set_schedule_enabled handles resume itself

async def schedule_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the schedule times (Handler for /scheduletime)."""
    await pause_scraping()
    # --- CHANGED: Check argument count (exactly 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Please provide exactly ONE time range after the command.\n\n"
            "Format: `/scheduletime HH:MM-HH:MM` (24h)\n\n"
            "Copy this and add the time range:\n"
            "`/scheduletime `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return
    # --- END CHANGE ---
    time_str = context.args[0].strip()
    # set_schedule_time checks the format and handles resume
    await set_schedule_time(update, time_str) # set_schedule_time contains the format check

# --- Switch Account ---
async def switch_account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Switches the account (Handler for /switchaccount)."""
    await pause_scraping()
    # --- CHANGED: Check argument count (0 or 1) ---
    if len(context.args) > 1:
         await update.message.reply_text("❌ Too many arguments. Please provide optionally ONE account number.\nFormat: `/switchaccount [number]`")
         await resume_scraping()
         return
    # --- END CHANGE ---

    account_num_str = context.args[0] if context.args else None
    account_num_idx = None # Index (0-based)
    if account_num_str:
        try:
            req_num = int(account_num_str)
            if 1 <= req_num <= len(ACCOUNTS):
                account_num_idx = req_num - 1 # To 0-based index
            else:
                await update.message.reply_text(f"❌ Invalid account number. Available: 1-{len(ACCOUNTS)}")
                await resume_scraping()
                return
        except ValueError:
            await update.message.reply_text("❌ Invalid account number (must be a number).") # Clearer message
            await resume_scraping()
            return
    # If no number was provided (account_num_idx is None),
    # switch_account_request will automatically switch to the next one.

    # switch_account_request handles pause/resume and login etc. internally
    await switch_account_request(update, account_num_idx) # Pass the index or None
    # No resume_scraping here, as switch_account_request handles it


async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming Telegram messages that are NOT commands."""
    global AUTH_CODE, WAITING_FOR_AUTH, is_scraping_paused, pause_event

    if update.message is None or not update.message.text:
        logger.debug("[Telegram Handler] Received update without message text. Ignoring.")
        return

    message_text = update.message.text.strip()
    user_id = update.message.from_user.id # Get user ID for logging
    logger.info(f"[Telegram Handler] Received message from {user_id}: '{message_text[:50]}...'") # Log reception
    logger.info(f"[Telegram Handler] Current WAITING_FOR_AUTH state: {WAITING_FOR_AUTH}") # Log flag state

    # Auth code handling remains the main task here
    if WAITING_FOR_AUTH:
        # +++ ADMIN CHECK FOR 2FA +++
        if not is_user_admin(user_id):
            logger.warning(f"[Telegram Handler] Non-admin user {user_id} attempted to send 2FA code ('{message_text}'). Ignoring.")
            await update.message.reply_text("❌ Access denied. Only admins can provide 2FA codes.")
            # Do NOT set AUTH_CODE, do NOT proceed with 2FA logic for this user.
            return # Stop processing this message further
        # +++ END ADMIN CHECK FOR 2FA +++

        # Now that we know it's an admin, proceed with format check
        logger.debug(f"[Telegram Handler] Admin {user_id} sent message '{message_text}'. Checking if it matches 2FA format (Length: {len(message_text)}, Alnum: {message_text.isalnum()})...")
        if 6 <= len(message_text) <= 10 and message_text.isalnum():
            logger.info(f"[Telegram Handler] Message matches 2FA format. Setting AUTH_CODE.") # Log match
            AUTH_CODE = message_text
            # IMPORTANT: DO NOT pause/resume here, as the login process is running
            await update.message.reply_text("✅ Auth code received! Processing...")
            logger.info(f"[Telegram Handler] AUTH_CODE set for user {user_id}.") # Log success
            return # Important: End the function here
        else:
            # Log why it didn't match if waiting
            logger.warning(f"[Telegram Handler] Message from {user_id} ('{message_text}') received while waiting, but did NOT match 2FA format. Ignoring for auth.")
            # Send help to the user
            await update.message.reply_text("⚠️ Invalid format for 2FA code. Please send *only* the 6-10 digit code (numbers/letters).")
            # Do not return, so other logic (if any) can still run, but AUTH_CODE is not set.

    # --- Optional: Handling replies for quick liking/reposting ---
    # ... (optional code, if you re-insert it) ...
    # --- End Optional: Replies ---

    # If the message was not an auth code (or WAITING_FOR_AUTH was false)
    # and no other logic matched:
    if not WAITING_FOR_AUTH: # Only log if we are *not* waiting for code
         logger.debug(f"[Telegram Handler] Ignoring non-command message from {user_id} as not waiting for auth.")

    # IMPORTANT: No general pause/resume here.
    pass # Do nothing further for normal text messages


def load_posts_count():
    """Load post counts from file"""
    global posts_count, last_count_date

    # Define the default structure including ads_total
    default_counts = {
        "found": {"today": 0, "yesterday": 0, "day_before_yesterday": 0, "total": 0}, # Translated key
        "scanned": {"today": 0, "yesterday": 0, "day_before_yesterday": 0, "total": 0}, # Translated key
        "ads_total": 0, # Default value 0
        "weekdays": {
            "Monday": {"count": 0, "days": 0}, "Tuesday": {"count": 0, "days": 0},
            "Wednesday": {"count": 0, "days": 0}, "Thursday": {"count": 0, "days": 0},
            "Friday": {"count": 0, "days": 0}, "Saturday": {"count": 0, "days": 0},
            "Sunday": {"count": 0, "days": 0}
        }
    }

    try:
        if os.path.exists(POSTS_COUNT_FILE):
            with open(POSTS_COUNT_FILE, 'r') as f:
                data = json.load(f)
                loaded_counts_data = data.get("counts", {})

                # Important: Merge the loaded data with the default structure
                posts_count = default_counts.copy() # Start with a fresh default structure

                # Iterate over categories found in the loaded data
                for category_loaded, values_loaded in loaded_counts_data.items():
                    if category_loaded in posts_count: # If category exists in our default structure
                        if isinstance(values_loaded, dict): # For 'found', 'scanned', 'weekdays'
                            if category_loaded in ["found", "scanned"]:
                                for key_loaded, value_loaded in values_loaded.items():
                                    # Translate old key 'vorgestern'
                                    target_key = "day_before_yesterday" if key_loaded == "vorgestern" else key_loaded
                                    # Ensure the target_key is valid for the category in default_counts
                                    if target_key in posts_count[category_loaded]:
                                        posts_count[category_loaded][target_key] = value_loaded
                                    # else: print(f"Warning: Skipped unknown key '{key_loaded}' (->'{target_key}') in category '{category_loaded}' during count load.")
                            elif category_loaded == "weekdays":
                                # For weekdays, directly update if the day exists
                                for day_loaded, day_data_loaded in values_loaded.items():
                                    if day_loaded in posts_count["weekdays"]:
                                        posts_count["weekdays"][day_loaded] = day_data_loaded
                                    # else: print(f"Warning: Skipped unknown weekday '{day_loaded}' during count load.")
                        elif category_loaded == "ads_total":
                            posts_count["ads_total"] = values_loaded if isinstance(values_loaded, int) else 0
                    # else: print(f"Warning: Skipped unknown category '{category_loaded}' during count load.")

                # Ensure ads_total exists, even if it was missing in the file
                if "ads_total" not in posts_count:
                    posts_count["ads_total"] = 0

                last_count_date_str = data.get("last_date")
                if last_count_date_str:
                    try:
                         last_count_date = datetime.strptime(last_count_date_str, "%Y-%m-%d").date()
                    except ValueError:
                         print(f"WARNING: Invalid date '{last_count_date_str}' in {POSTS_COUNT_FILE}. Setting to today.")
                         last_count_date = datetime.now().date()
                else:
                    last_count_date = datetime.now().date()
        else:
            posts_count = default_counts.copy()
            last_count_date = datetime.now().date()
            print(f"No {POSTS_COUNT_FILE} found, using default counters and creating the file.")
            save_posts_count() # Create the file with default counts

    except json.JSONDecodeError:
         print(f"ERROR: {POSTS_COUNT_FILE} is corrupt (JSONDecodeError). Using default counters and creating a new file.")
         posts_count = default_counts.copy()
         last_count_date = datetime.now().date()
         save_posts_count() # Create a new file with default counts
    except Exception as e:
        print(f"Error loading {POSTS_COUNT_FILE}: {e}. Using default counters and attempting to create the file.")
        posts_count = default_counts.copy()
        last_count_date = datetime.now().date()
        save_posts_count() # Attempt to create the file with default counts

    # Double check that ads_total exists
    if "ads_total" not in posts_count:
        posts_count["ads_total"] = 0
    # Remove the old 'ads' structure if it still exists (from old runs)
    if "ads" in posts_count:
         del posts_count["ads"]
    # Remove the old 'vorgestern' key if it still exists after potential merge
    if "vorgestern" in posts_count.get("found", {}):
        del posts_count["found"]["vorgestern"]
    if "vorgestern" in posts_count.get("scanned", {}):
        del posts_count["scanned"]["vorgestern"]


def save_posts_count():
    """Save post counts to file"""
    global posts_count, last_count_date
    try:
        # Create a copy to save, ensuring the old "ads" structure is gone
        data_to_save = posts_count.copy()
        if "ads" in data_to_save: # Remove old structure if present
             del data_to_save["ads"]
        # Remove 'vorgestern' if present before saving
        if "vorgestern" in data_to_save.get("found", {}):
            del data_to_save["found"]["vorgestern"]
        if "vorgestern" in data_to_save.get("scanned", {}):
            del data_to_save["scanned"]["vorgestern"]

        data = {
            "counts": data_to_save, # Save the cleaned copy
            "last_date": last_count_date.strftime("%Y-%m-%d") if last_count_date else None
        }
        with open(POSTS_COUNT_FILE, 'w') as f:
            json.dump(data, f, indent=4) # indent=4 for better file readability
    except Exception as e:
        print(f"Error saving posts count: {e}")

def check_rotate_counts():
    """Check if the date has changed and rotate counts if needed"""
    global posts_count, last_count_date
    current_date = datetime.now().date()

    if last_count_date is None:
        last_count_date = current_date
        return

    # If date has changed
    if current_date > last_count_date:
        # Update weekday stats for the previous day
        weekday = last_count_date.strftime("%A")  # Get day name (Monday, Tuesday, etc.)
        posts_count["weekdays"][weekday]["count"] += posts_count["found"]["today"]
        posts_count["weekdays"][weekday]["days"] += 1

        days_diff = (current_date - last_count_date).days

        if days_diff == 1:
            # Shift by one day for both found and scanned
            for category in ["found", "scanned"]:
                posts_count[category]["day_before_yesterday"] = posts_count[category]["yesterday"] # Use translated key
                posts_count[category]["yesterday"] = posts_count[category]["today"]
                posts_count[category]["today"] = 0
        elif days_diff > 1:
            # More than one day passed, reset older counts
            for category in ["found", "scanned"]:
                posts_count[category]["day_before_yesterday"] = 0 # Use translated key
                posts_count[category]["yesterday"] = 0
                posts_count[category]["today"] = 0

        last_count_date = current_date
        save_posts_count()  # Save after rotation

def increment_scanned_count():
    """Increment the count of scanned posts"""
    global posts_count
    check_rotate_counts()  # Check if we need to rotate counts first
    posts_count["scanned"]["today"] += 1
    posts_count["scanned"]["total"] += 1

    # Save every 50 posts to avoid too frequent writes
    if posts_count["scanned"]["today"] % 50 == 0:
        save_posts_count()

def increment_found_count():
    """Increment the count of found/relevant posts"""
    global posts_count
    check_rotate_counts()  # Check if we need to rotate counts first
    posts_count["found"]["today"] += 1
    posts_count["found"]["total"] += 1

    # Save every 10 posts to avoid too frequent writes
    if posts_count["found"]["today"] % 10 == 0:
        save_posts_count()

def get_uptime():
    """Calculate and format the uptime"""
    global start_time
    uptime = datetime.now() - start_time
    days = uptime.days
    hours, remainder = divmod(uptime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    else:
        return f"{minutes}m {seconds}s"

async def show_post_counts(update: Update):
    """Show the current post counts to the user"""
    global posts_count
    check_rotate_counts()  # Ensure counts are up to date

    # Get the total number of ads safely using .get()
    total_ads = posts_count.get("ads_total", 0)

    # --- CALCULATE WEEKDAY AVERAGES FIRST ---
    weekday_averages = {}
    weekdays_data = posts_count.get("weekdays", {})
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
         data = weekdays_data.get(day, {"count": 0, "days": 0})
         if data["days"] > 0:
             weekday_averages[day] = round(data["count"] / data["days"], 1)
         else:
             weekday_averages[day] = 0
    # --- END CALCULATION ---


    # --- CREATE THE ENTIRE MESSAGE NOW IN ONE BLOCK ---
    message = (
        "📊 Post Statistics 📊\n\n"
        f"⏱️ Uptime: {get_uptime()}\n\n"
        "💪🏻 Found Posts:\n" # You can mix normal strings and f-strings
        f"Today: {posts_count.get('found', {}).get('today', 0)} posts\n"
        f"Yesterday: {posts_count.get('found', {}).get('yesterday', 0)} posts\n"
        f"Day before yesterday: {posts_count.get('found', {}).get('day_before_yesterday', 0)} posts\n" # Use translated key
        f"Total: {posts_count.get('found', {}).get('total', 0)} posts\n\n"

        "🔎 Scanned Posts:\n"
        f"Today: {posts_count.get('scanned', {}).get('today', 0)} posts\n"
        f"Yesterday: {posts_count.get('scanned', {}).get('yesterday', 0)} posts\n"
        f"Day before yesterday: {posts_count.get('scanned', {}).get('day_before_yesterday', 0)} posts\n" # Use translated key
        f"Total: {posts_count.get('scanned', {}).get('total', 0)} posts\n\n"

        f"📢 Ads (Total): {total_ads}\n\n"

        "📅 Average Posts by Weekday:\n"
        # Insert the weekday strings directly here
        f"Mon: {weekday_averages['Monday']}\n"
        f"Tue: {weekday_averages['Tuesday']}\n"
        f"Wed: {weekday_averages['Wednesday']}\n"
        f"Thu: {weekday_averages['Thursday']}\n"
        f"Fri: {weekday_averages['Friday']}\n"
        f"Sat: {weekday_averages['Saturday']}\n"
        f"Sun: {weekday_averages['Sunday']}"
    ) # <<--- Ensure THIS parenthesis closes the one opened after 'message = ('

    # The message += (...) block is completely removed

    await update.message.reply_text(message)
    await resume_scraping()

async def show_help_message(update: Update):
    """Displays the help message (adapted for /commands)."""
    # Create keyboard markup with buttons for common commands (Buttons remain the same)
    separator_button = InlineKeyboardButton(" ", callback_data="noop_separator")
    global search_tickers_enabled, is_headless_enabled # Need both for button text
    keyboard = [
        [ # Combined Pause/Resume Button - Shows current status
            InlineKeyboardButton(
                ("RUNNING 🟢" if not is_scraping_paused else 
                 ("SCHEDULE PAUSED 🟡" if is_schedule_pause else 
                  "PAUSED 🟡")),
                callback_data="help:toggle_pause_resume"
            )
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="help:status")
        ],
        [
            InlineKeyboardButton(f"🔑 Words {'🟢' if search_keywords_enabled else '🔴'}", callback_data="help:toggle_keywords"),
            InlineKeyboardButton(f"📝 CA {'🟢' if search_ca_enabled else '🔴'}", callback_data="help:toggle_ca"),
            InlineKeyboardButton(f"💲 Ticker {'🟢' if search_tickers_enabled else '🔴'}", callback_data="help:toggle_tickers")
        ],
        [separator_button],
        [ # All Schedules Status
            InlineKeyboardButton("🗓️ All Schedules", callback_data="help:show_all_schedules")
        ],
        [ # Main Schedule Control
            InlineKeyboardButton(
                f"⏰ Main Sched. {'🟢' if schedule_enabled else '🔴'}", # Toggle On/Off
                callback_data="help:toggle_main_schedule"
            ),
            InlineKeyboardButton("⏰ Set Main Time", callback_data="help:prepare_scheduletime") # Keep Set Time
        ],
        [ # Scheduled Sync
            InlineKeyboardButton(f"🔄 Sched. Sync {'🟢' if schedule_sync_enabled else '🔴'}", callback_data="help:toggle_schedule_sync"),
            InlineKeyboardButton("🔄 Set Sync Time (fast)", callback_data="help:prepare_schedule_sync_time")
        ],
        [ # Scheduled Follow List
            InlineKeyboardButton(f"🚶‍♂️‍➡️ Sched. Follow {'🟢' if schedule_follow_list_enabled else '🔴'}", callback_data="help:toggle_schedule_follow_list"),
            InlineKeyboardButton("🚶‍♂️‍➡️ Set Follow Time (fast)", callback_data="help:prepare_schedule_follow_list_time")
        ],
        [separator_button],
        [ # Mode & Account
            InlineKeyboardButton("🥷 Account Info", callback_data="help:account"),
            InlineKeyboardButton("🥷 Switch Acc 1️⃣🔜2️⃣", callback_data="help:prepare_switchaccount")
        ],
        [separator_button],
        [ # Keywords
            InlineKeyboardButton("🔑 Keywords", callback_data="help:keywords"),
            InlineKeyboardButton("🔑➕ Add", callback_data="help:prepare_addkeyword"),
            InlineKeyboardButton("🔑➖ Remove", callback_data="help:prepare_removekeyword")
        ],
        [separator_button],
        [ # Follow / Unfollow
             InlineKeyboardButton("🚶‍♂️‍➡️➕ Follow", callback_data="help:prepare_follow"),
             InlineKeyboardButton("🚶‍♂️‍➡️➖ Unfollow", callback_data="help:prepare_unfollow"),
             InlineKeyboardButton("🚶‍♂️‍➡️➕ Add2List", callback_data="help:prepare_addusers")
        ],
        [separator_button],
        [ # Auto-Follow Control
            InlineKeyboardButton("AF Mode OFF", callback_data="help:autofollow_mode_off"),
            InlineKeyboardButton("AF Mode SLOW", callback_data="help:autofollow_mode_slow"),
            InlineKeyboardButton("AF Mode FAST", callback_data="help:autofollow_mode_fast")
        ],
        [
            InlineKeyboardButton("AF Set Interval", callback_data="help:prepare_autofollow_interval"),
            InlineKeyboardButton("AF Status", callback_data="help:autofollow_status"),
            InlineKeyboardButton("AF Cancel FAST", callback_data="help:cancel_fast_follow")
        ],
        [separator_button],
        [ # Backup / Sync / Build
            InlineKeyboardButton("💾 Backup", callback_data="help:backup_followers"),
            InlineKeyboardButton("🔄 Sync", callback_data="help:sync_follows"), # This is manual sync
            InlineKeyboardButton("🏗️ Build Global", callback_data="help:build_global")
        ],
        [separator_button],
        [ # Stats / Rates
            InlineKeyboardButton("📊 Stats", callback_data="help:stats"),
            InlineKeyboardButton("⭐️ Rates", callback_data="help:show_rates"),
            InlineKeyboardButton("🌍 Global Info", callback_data="help:global_info"),
            InlineKeyboardButton("🏓 Ping", callback_data="help:ping")
        ],
        [separator_button],
        [
            InlineKeyboardButton(f"👻 Headless {'🟢' if is_headless_enabled else '🔴'}", callback_data="help:toggle_headless")
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)


    global current_account, max_tweet_age_minutes, schedule_pause_start, schedule_pause_end, auto_follow_interval_minutes,schedule_sync_start_time, schedule_sync_end_time, schedule_follow_list_start_time, schedule_follow_list_end_time

    next_account_display_val = (current_account + 1) % len(ACCOUNTS) + 1 if ACCOUNTS and len(ACCOUNTS) > 0 else 1

    await update.message.reply_text(
        "🆘 `/help` - Show menu 🆘\n"
        " \n"
        "🚶‍♂️‍➡️    Follow / Unfollow    🚶‍♂️\n"
        "   <code>/follow username</code>\n"
        "   <code>/unfollow username</code>\n"
        "   <code>/addusers @user1</code> user2 ...  - \n"
        "      └ Adds to follow list.\n"
        "   <code>/autofollowmode off</code> slow fast\n"
        f"   <code>/autofollowinterval {auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]}</code> - interval (Slow Mode)\n"
        "   /cancelfastfollow\n"
        "   /autofollowstatus\n"
        "   /clearfollowlist\n"
        "  \n"
        "👍    Like / Repost    🔄\n"
        "   <code>/like tweet_url</code>\n"
        "   <code>/repost tweet_url</code>\n"
        "  \n"
        "🔑    Keywords    🔑\n"
        "   /keywords  - Shows list\n"
        "   <code>/addkeyword word1</code>,word2...\n"
        "   <code>/removekeyword word1</code>,word2...\n"
        "  \n"
        "🥷    Accounts    🥷\n"
        "   /account  - Show active account\n"
        f"   <code>/switchaccount {next_account_display_val}</code> \n"
        "      └ Switches to acc [nmbr]\n"
        "  \n"
        "🔍    Search Mode    🔍\n"
        "   /mode  - current search mode\n"
        "   /modefull  - Sets mode to CA + Keywords\n"
        "   /modeca  - Sets mode to CA Only\n"
        "   /searchtickers\n"
        "      └ scan for $Tickers on|off\n"
        f"   <code>/setmaxage {max_tweet_age_minutes}</code>\n"
        "      └ Sets max post age(min) (default: 15)\n"
        "  \n"
        "⏯️    Control    ⏯️\n"
        "   /pause\n"
        "   /resume\n"
        "  \n"
        "🗓️    All Schedules    🗓️\n"
        "   /allschedules - Shows status of all schedules\n\n"
        "   ▫️ Main Bot Pause Schedule:\n"
        "     /schedule  - current main schedule\n"
        "     /scheduleon | /scheduleoff\n"
        f"     <code>/scheduletime {schedule_pause_start}-{schedule_pause_end}</code>\n\n"
        "   ▫️ Scheduled Sync:\n"
        "     /schedulesync on|off\n"
        f"     <code>/schedulesynctime {schedule_sync_start_time}-{schedule_sync_end_time}</code>\n\n"        "   ▫️ Scheduled Follow List:\n"
        "     /schedulefollowlist on|off\n"
        f"     <code>/schedulefollowlisttime {schedule_follow_list_start_time}-{schedule_follow_list_end_time}</code>\n"
        "  \n"        "  \n"
        "📊    Statistics & Status    📊\n"
        "   /status\n\n"
        "   /stats\n"
        "   /rates  - collected ratings\n"
        "   /globallistinfo  - global follower list\n"
        "   /ping  - test\n"
        "  \n"
        "💾  Following DB & Management  💾\n"
        "   <code>/scrapefollowing username</code>\n"
        "         └ Scans following of [username] & saves to DB\n"
        "   <code>/addfromdb </code> f:50k s:3 k:WORD1 WORD2..\n"
        "         └ Adds from DB. Filters:\n"
        "           f[ollowers]: Min. follower count (e.g., `f:10k` or `followers:10000`)\n"
        "           s[een]: Min. times seen in scans (e.g., `s:3`)\n"
        "           k[eywords]: Keywords in bio (e.g., `k:btc eth` or `keywords:solana nft`)\n"
        "   /backupfollowers  - Saves snapshot of the active account\n"
        "   /syncfollows  - Synchronizes active account with global list\n"
        "   /buildglobalfrombackups \n"
        "         └ Adds users from all backups to the global list\n"
        "   /cancelbackup ,  /cancelsync ,  /canceldbscrape \n"
        "         └ Cancels running processes\n"
        "  \n"
        "   /toggleheadless \n"
        "         └ Toggles Headless mode ON/OFF (Restart required!)\n",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML # Wichtig, damit <code> funktioniert
    )
    # Important: This function must call resume at the end, as the calling handler paused
    # await resume_scraping()

async def add_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Adds usernames to the current account's list, checks against global followed list."""
    global current_account_usernames_to_follow, global_followed_users_set
    # is_scraping_paused, pause_event are no longer needed directly here

    account_username = get_current_account_username()
    current_follow_list_path = get_current_follow_list_path()

    if not account_username or not current_follow_list_path:
        await update.message.reply_text("❌ Error: Active account username/list path not found.")
        await resume_scraping() # Important: Resume, as the main handler paused
        return

    # Simulate arguments
    if hasattr(context, 'args'): args = context.args
    elif isinstance(context, list): args = context
    else: args = []

    if not args:
        await update.message.reply_text(f"Please provide one or more X usernames.\ne.g.: `addusers user1 @user2,user3`")
        await resume_scraping() # Important: Resume
        return

    input_text = " ".join(args)
    # Validate usernames during parsing
    potential_usernames = {name.strip().lstrip('@') for name in re.split(r'[,\s]+', input_text)
                           if name.strip() and re.match(r'^[A-Za-z0-9_]{1,15}$', name.strip().lstrip('@'))}

    if not potential_usernames:
         await update.message.reply_text("ℹ️ No valid usernames found in input.")
         await resume_scraping() # Important: Resume
         return

    # Optionally add to global add queue (if implemented)
    # add_to_set_file(potential_usernames, GLOBAL_ADD_QUEUE_FILE)

    added_to_current_account = set()
    already_followed_globally = set()
    already_in_current_list = set()

    # Use the global set in memory for the current list
    current_list_set = set(current_account_usernames_to_follow)

    for username in potential_usernames:
        if username in global_followed_users_set:
            already_followed_globally.add(username)
        elif username in current_list_set:
            already_in_current_list.add(username)
        else:
            added_to_current_account.add(username)

    # Update the in-memory list and save it
    response = ""
    if added_to_current_account:
        current_account_usernames_to_follow.extend(list(added_to_current_account)) # Add to the list
        save_current_account_follow_list() # Save the updated list
        response += f"✅ {len(added_to_current_account)} users added to the list of @{account_username}: {', '.join(sorted(list(added_to_current_account)))}\n"

    if already_in_current_list:
         response += f"ℹ️ {len(already_in_current_list)} users were already in the list of @{account_username}.\n"
    if already_followed_globally:
        response += f"🚫 {len(already_followed_globally)} users are already followed globally and were not added to the list of @{account_username}: {', '.join(sorted(list(already_followed_globally)))}"

    if not response: # Fallback
         response = "ℹ️ No changes made to the follow list."

    await update.message.reply_text(response.strip())
    await resume_scraping() # Important: Resume at the end


# eventually delete process_follow_request
async def process_follow_request(update: Update, username: str):
    """Process follow requests: Follows user and updates lists on success."""
    global global_followed_users_set # Access global set

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if not account_username or not backup_filepath:
         await update.message.reply_text("❌ Error: Active account cannot be determined for follow update.")
         await resume_scraping() # Resume, as main handler paused
         return

    # Send message before calling follow_user
    await update.message.reply_text(f"⏳ Trying to follow @{username} with account @{account_username}...")
    # The main handler has already paused. follow_user navigates away.

    result = await follow_user(username) # Perform the follow attempt
                                         # follow_user navigates back to /home at the end

    if result is True: # Only on *successful new* follow
        await update.message.reply_text(f"✅ Successfully followed @{username}!")
        print(f"Manual follow successful: @{username}")
        # Update global list (memory & file)
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE) # Correct global filename
            print(f"@{username} added to global followed list.")
        # Update account backup (file)
        add_to_set_file({username}, backup_filepath)
        print(f"@{username} added to account backup ({os.path.basename(backup_filepath)}).")

    elif result == "already_following":
        await update.message.reply_text(f"ℹ️ Account @{account_username} is already following @{username}.")
        print(f"Manual follow: @{username} was already followed.")
        # Ensure consistency (add if missing)
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE) # Correct global filename
        # Ensure it's also in the account backup
        add_to_set_file({username}, backup_filepath)

    else: # Error
        await update.message.reply_text(f"❌ Could not follow @{username}.")
        print(f"Manual follow failed: @{username}")

    await resume_scraping() # Resume at the end of the handler

async def process_unfollow_request(update: Update, username: str):
    """Process unfollow requests centrally"""
    await update.message.reply_text(f"🔍 Trying to unfollow @{username}...")
    result = await unfollow_user(username)
    if result == "not_following":
        await update.message.reply_text(f"ℹ️ You are not following @{username}")
    elif result:
        await update.message.reply_text(f"✅ Successfully unfollowed @{username}!")
    else:
        await update.message.reply_text(f"❌ Could not unfollow @{username}")
    await resume_scraping()

async def process_like_request(update: Update, tweet_url: str):
    """Process like requests as text command"""
    await pause_scraping()
    await update.message.reply_text(f"🔍 Trying to like post: {tweet_url}")

    # Check URL format
    if not (tweet_url.startswith("http://") or tweet_url.startswith("https://")):
        tweet_url = "https://x.com" + ("/" if not tweet_url.startswith("/") else "") + tweet_url

    # Ensure it's an X/Twitter URL
    if not ("x.com" in tweet_url or "twitter.com" in tweet_url):
        await update.message.reply_text("❌ Invalid post URL. Please provide an X.com URL.")
        await resume_scraping()
        return

    try:
        result = await like_tweet(tweet_url)
        if result:
            await update.message.reply_text(f"✅ post successfully liked!")
        else:
            await update.message.reply_text(f"❌ Could not like post")
    except Exception as e:
        await update.message.reply_text(f"❌ Error liking: {str(e)[:100]}")

    await resume_scraping()

async def process_repost_request(update: Update, tweet_url: str):
    """Process repost requests as text command"""
    await pause_scraping()
    await update.message.reply_text(f"🔍 Trying to repost post: {tweet_url}")

    # Check URL format
    if not (tweet_url.startswith("http://") or tweet_url.startswith("https://")):
        tweet_url = "https://x.com" + ("/" if not tweet_url.startswith("/") else "") + tweet_url

    # Ensure it's an X/Twitter URL
    if not ("x.com" in tweet_url or "twitter.com" in tweet_url):
        await update.message.reply_text("❌ Invalid post URL. Please provide an X.com or Twitter.com URL.")
        await resume_scraping()
        return

    try:
        result = await repost_tweet(tweet_url)
        if result:
            await update.message.reply_text(f"✅ post successfully reposted!")
        else:
            await update.message.reply_text(f"❌ Could not repost post")
    except Exception as e:
        await update.message.reply_text(f"❌ Error reposting: {str(e)[:100]}")

    await resume_scraping()
# ===========================================================
# NEW TELEGRAM COMMANDS FOR AUTO-FOLLOW CONTROL
# ===========================================================

# Add this function:

async def backup_followers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for the /backupfollowers command. Starts the backup logic as a task."""
    global is_backup_running # Check if a backup is already running

    # IMPORTANT: This handler does NOT pause scraping itself.
    # backup_followers_logic is started as a task and manages pause/resume internally.

    if is_backup_running:
        await update.message.reply_text("⚠️ A backup process is already running. Please wait or use `/cancelbackup`.")
        return # Do not proceed if already active

    # Send message that the task is starting
    await update.message.reply_text("✅ Follower backup is starting in the background...")

    # Start the actual logic as a background task
    # Pass the 'update' object that backup_followers_logic expects
    asyncio.create_task(backup_followers_logic(update))

    # No resume_scraping here, the task runs independently and manages itself.

async def autofollow_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the status of the automatic follow function for the current account."""
    global auto_follow_mode, auto_follow_interval_minutes, current_account_usernames_to_follow
    global is_fast_follow_running # Check if Fast task is running

    mode_display = auto_follow_mode.upper()
    if auto_follow_mode == "fast" and is_fast_follow_running:
        mode_display += " (Running...)"
    elif auto_follow_mode == "off":
        mode_display = "OFF ⏸️"
    elif auto_follow_mode == "slow":
        mode_display = f"SLOW ({auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]} min) ▶️"

    account_username = get_current_account_username() or "Unknown"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)

    await update.message.reply_text(f"🤖 Status Auto-Follow for @{account_username}:\n"
                                     f"   Mode: {mode_display}\n"
                                     f"   Users in `{filename}`: {count}")
    # IMPORTANT: Main handler paused, resume here
    await resume_scraping()

async def clear_follow_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks for confirmation to clear the *current* account's follow list."""
    global current_account_usernames_to_follow
    account_username = get_current_account_username() or "Unknown"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)

    if count == 0:
        await update.message.reply_text(f"ℹ️ The follow list for @{account_username} (`{filename}`) is already empty.")
        await resume_scraping() # Resume
        return

    keyboard = [[
        # The payload now contains the account name for safety
        InlineKeyboardButton(f"✅ Yes, clear list for @{account_username}", callback_data=f"confirm_clear_follow_list:{account_username}"),
        InlineKeyboardButton("❌ No, cancel", callback_data="cancel_clear_follow_list")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ Are you sure you want to delete the follow list for account @{account_username} (`{filename}`)? "
        f"It currently contains {count} users.",
        reply_markup=reply_markup
    )
    # No resume_scraping here, wait for button response or timeout



async def sync_followers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Checks if a sync is necessary (adding OR removing),
    asks for confirmation if needed, and then starts the sync task.
    """
    await pause_scraping() # Pause for the check

    account_username = get_current_account_username()
    if not account_username:
        await update.message.reply_text("❌ Error: Active account cannot be determined.")
        await resume_scraping()
        return

    backup_filepath = get_current_backup_file_path()
    if not backup_filepath:
        await update.message.reply_text("❌ Error: Backup file path could not be determined.")
        await resume_scraping()
        return

    # 1. Load global list and account backup
    global_all_followed_users_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
    account_backup_set = load_set_from_file(backup_filepath)
    backup_exists_and_not_empty = bool(account_backup_set)

    # 2. Determine BOTH differences
    users_to_add = global_all_followed_users_set - account_backup_set
    total_to_add = len(users_to_add)

    users_to_remove = account_backup_set - global_all_followed_users_set
    total_to_remove = len(users_to_remove)

    # 3. Check if *any* action is needed
    sync_needed = total_to_add > 0 or total_to_remove > 0

    if not sync_needed:
        await update.message.reply_text(f"✅ Account @{account_username} is already synchronized with the global list.")
        await resume_scraping()
        return

    # 4. Time estimation (Consider both actions, roughly)
    # (Estimation is less critical, can be simplified or based only on adds)
    estimated_seconds_per_user = 30
    total_estimated_seconds = (total_to_add + total_to_remove) * estimated_seconds_per_user
    estimated_time_str = "a few minutes" # Simplified estimate
    if total_estimated_seconds > 0:
        minutes, seconds = divmod(total_estimated_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        temp_str = ""
        if hours > 0: temp_str += f"{hours}h"
        if minutes > 0: temp_str += f" {minutes}m"
        if not temp_str or hours == 0:
             if seconds > 0: temp_str += f" {seconds}s"
        if temp_str: estimated_time_str = f"~{temp_str.strip()}"


    # 5. Decision based on backup status and whether sync is needed
    if backup_exists_and_not_empty:
        # Case B: Backup exists - Normal Sync -> Request confirmation
        message = (f"ℹ️ **Sync Preview for @{account_username}**\n\n"
                   f"   - Users in backup: {len(account_backup_set)}\n"
                   f"   - Users globally: {len(global_all_followed_users_set)}\n"
                   f"   - ➡️ Actions: *+{total_to_add} Users / -{total_to_remove} Users*\n" # Show both numbers
                   f"   - ⏱️ Estimated duration: *{estimated_time_str}*\n\n"
                   f"Do you want to start this sync now?")

        # Add Yes/No buttons
        keyboard = [[
            # Important: Embed account username in callback data!
            InlineKeyboardButton(f"✅ Yes, start sync", callback_data=f"sync:proceed_sync:{account_username}"),
            InlineKeyboardButton("❌ No, cancel", callback_data=f"sync:cancel_sync:{account_username}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        # DO NOT start the task HERE anymore! That happens in the button handler.
        # No resume_scraping here, wait for button response.

    else:
        # Case A: Backup missing or empty - Request special confirmation
        if total_to_add == 0:
             # If no backup AND nothing to add
             await update.message.reply_text(f"ℹ️ Backup for @{account_username} missing/empty and no users found to add from global list. No sync needed.")
             await resume_scraping()
             return

        # Message for Case A
        message = (f"⚠️ **Attention: Sync for @{account_username}**\n\n"
                   f"The backup file `{os.path.basename(backup_filepath)}` is missing or empty.\n\n"
                   f"A sync would now attempt to follow *{total_to_add}* users from the global list.\n"
                   f"(Users to remove cannot be determined).\n"
                   f"Estimated duration (adding only): *{estimated_time_str}*\n\n"
                   f"How do you want to proceed?")

        # Buttons for Case A
        keyboard = [[
            # Option 1: Create backup
            InlineKeyboardButton("💾 Create Backup & Cancel", callback_data=f"sync:create_backup:{account_username}"),
        ],[
            # Option 2: Start adding only
            InlineKeyboardButton(f"▶️ Yes, add {total_to_add} users", callback_data=f"sync:proceed:{account_username}"),
            # Option 3: Cancel
            InlineKeyboardButton("❌ No, Cancel", callback_data=f"sync:cancel_sync:{account_username}") # Use cancel_sync
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        # Do not resume, wait for button response or timeout

async def sync_followers_logic(update: Update, account_username: str, backup_filepath: str, global_set_for_sync: set):
    """
    Performs the synchronization for the given account.
    Adds users who are followed globally but not in the backup.
    Removes users who are in the backup but no longer followed globally.
    (With cancellation option)
    """
    # Access global variables/flags
    global driver, is_scraping_paused, pause_event
    global is_sync_running, cancel_sync_flag
    global global_followed_users_set # Access global set (only read here, not modified)
    global is_any_scheduled_browser_task_running # New global flag

    # Determine if this is a scheduled run (update is None)
    is_scheduled_run = update is None

    # Prevent starting if another scheduled browser task is running (only for scheduled runs)
    if is_scheduled_run and is_any_scheduled_browser_task_running:
        logger.warning(f"[Sync @{account_username}] Scheduled run skipped: another scheduled browser task is active.")
        # Silently skip for scheduler, or send a message if manually triggered with update=None (less likely)
        return

    if is_sync_running: # Check if this specific sync task type is already running
        if update and hasattr(update, 'message') and update.message: # Manual trigger
            await update.message.reply_text("⚠️ A sync process (manual or scheduled) is already running.")
        else: # Scheduled trigger, but another sync (maybe manual) is already on
            logger.warning(f"[Sync @{account_username}] Attempted to start scheduled sync, but another sync is already running.")
        return

    main_loop_was_initially_paused = is_scraping_paused

    # ===== Task Start Marker =====
    # Set specific flag first
    is_sync_running = True
    cancel_sync_flag = False # Reset for this run
    # Then set the general flag if this is a scheduled run that uses the browser
    if is_scheduled_run:
        is_any_scheduled_browser_task_running = True
    # ================================

    logger.info(f"[Sync @{account_username}] Starting synchronization process...")
    # Send message only if manually triggered (update object exists)
    if update and hasattr(update, 'message') and update.message:
        await update.message.reply_text(f"⏳ Starting sync for @{account_username}...\n"
                                         f"   To cancel: `/cancelsync`")
    # else: # If called by scheduler, a message is already sent by the run() loop

    # Pause main scraping only if it was running
    if not main_loop_was_initially_paused:
        await pause_scraping()

    # --- Initialize counters ---
    users_followed_in_sync = 0
    users_already_followed_checked = 0
    users_failed_to_follow = 0
    users_unfollowed_in_sync = 0
    users_already_unfollowed_checked = 0
    users_failed_to_unfollow = 0
    users_processed_add_count = 0
    users_processed_remove_count = 0
    # --- End counters ---

    navigation_successful = False
    cancelled_early = False
    backup_modified = False # Flag to know if the backup needs to be saved

    try: # Main try block
        # Load current data (global set is already current in memory)
        # Load the backup of *this* account
        account_backup_set = load_set_from_file(backup_filepath)
        initial_backup_size = len(account_backup_set)

        # --- Calculate differences ---
        # Users to be added (global but not in backup)
        # Use the passed set!
        users_to_add = global_set_for_sync - account_backup_set
        total_to_add = len(users_to_add)

        # Users to be removed (in backup but no longer global)
        # Use the passed set!
        users_to_remove = account_backup_set - global_set_for_sync
        total_to_remove = len(users_to_remove)
        # --- End differences ---

        logger.info(f"[Sync @{account_username}] Global (passed): {len(global_set_for_sync)} | Backup (Start): {initial_backup_size} | To Add: {total_to_add} | To Remove: {total_to_remove}")

        if not users_to_add and not users_to_remove:
            msg_sync_already_done = f"✅ Account @{account_username} is already synchronized."
            if update and hasattr(update, 'message') and update.message:
                await update.message.reply_text(msg_sync_already_done)
            else:
                await send_telegram_message(msg_sync_already_done)
            # Jump directly to finally
        else:
            msg_sync_starting_details = f"⏳ Synchronizing @{account_username}: +{total_to_add} Users / -{total_to_remove} Users..."
            if update and hasattr(update, 'message') and update.message:
                await update.message.reply_text(msg_sync_starting_details)
            else:
                await send_telegram_message(msg_sync_starting_details)

            # === PHASE 1: Adding ===
            if users_to_add:
                logger.info(f"[Sync @{account_username}] Starting ADD phase ({total_to_add} users)...")
                user_list_to_add = list(users_to_add)
                random.shuffle(user_list_to_add)

                for i, username in enumerate(user_list_to_add):
                    users_processed_add_count = i + 1
                    if cancel_sync_flag: cancelled_early = True; break # Cancellation check

                    logger.debug(f"[Sync @{account_username} ADD] Attempt {i+1}/{total_to_add}: Following @{username}...")
                    wait_follow = random.uniform(4, 7)
                    logger.debug(f"    -> Waiting {wait_follow:.1f}s before next attempt")
                    await asyncio.sleep(wait_follow)

                    if cancel_sync_flag: cancelled_early = True; break # Cancellation check after wait

                    follow_result = await follow_user(username)

                    if follow_result is True:
                        logger.debug(f"  -> Success!")
                        users_followed_in_sync += 1
                        account_backup_set.add(username) # Add to the in-memory set
                        backup_modified = True
                    elif follow_result == "already_following":
                        logger.debug(f"  -> Already following (consistency check).")
                        users_already_followed_checked += 1
                        if username not in account_backup_set: # Add if it was missing
                            account_backup_set.add(username)
                            backup_modified = True
                    else: # Error
                        logger.warning(f"  -> Failed to follow @{username}!")
                        users_failed_to_follow += 1

                    # Report progress (optional)
                    if not cancelled_early and ((i + 1) % 10 == 0 or (i + 1) == total_to_add):
                         progress_msg = f"[Sync @{account_username} ADD] Progress: {i+1}/{total_to_add} attempted..."
                         await send_telegram_message(progress_msg) # Use your send function

                if cancelled_early:
                    logger.warning("[Sync] Cancellation signal received during ADD phase.")
                    msg_sync_cancelled_adding = "🟡 Sync is being cancelled (during adding)..."
                    if update and hasattr(update, 'message') and update.message:
                        await update.message.reply_text(msg_sync_cancelled_adding)
                    else:
                        await send_telegram_message(msg_sync_cancelled_adding)
                    # Jump out of sync logic (finally will be executed)


            # === PHASE 2: Removing (only if not cancelled) ===
            if not cancelled_early and users_to_remove:
                logger.info(f"[Sync @{account_username}] Starting REMOVE phase ({total_to_remove} users)...")
                user_list_to_remove = list(users_to_remove)
                random.shuffle(user_list_to_remove)

                for i, username in enumerate(user_list_to_remove):
                    users_processed_remove_count = i + 1
                    if cancel_sync_flag: cancelled_early = True; break # Cancellation check

                    logger.debug(f"[Sync @{account_username} REMOVE] Attempt {i+1}/{total_to_remove}: Checking/Unfollowing @{username} (as it's not in global list)...")
                    wait_unfollow = random.uniform(4, 7)
                    logger.debug(f"    -> Waiting {wait_unfollow:.1f}s before next attempt")
                    await asyncio.sleep(wait_unfollow)

                    if cancel_sync_flag: cancelled_early = True; break # Cancellation check after wait

                    # --- Try to unfollow via Selenium ---
                    unfollow_result = await unfollow_user(username)
                    selenium_unfollowed = False # Track if Selenium was successful

                    if unfollow_result is True:
                        logger.debug(f"  -> Successfully unfollowed @{username} via Selenium.")
                        users_unfollowed_in_sync += 1
                        selenium_unfollowed = True
                    elif unfollow_result == "not_following":
                        logger.debug(f"  -> Account @{account_username} was not following @{username} (Selenium check).")
                        users_already_unfollowed_checked += 1
                        selenium_unfollowed = True # Treat as success for list cleanup
                    else: # Error
                        logger.warning(f"  -> Failed to unfollow @{username} via Selenium! (Will still remove from backup)")
                        users_failed_to_unfollow += 1
                        # selenium_unfollowed remains False

                    # --- Update the backup set ALWAYS if the user is in users_to_remove ---
                    # The user should be removed from this account's backup because they are no longer global.
                    if username in account_backup_set:
                        logger.info(f"  -> Removing @{username} from in-memory backup set for @{account_username} (due to global list difference).")
                        account_backup_set.discard(username)
                        backup_modified = True # Mark that the backup was changed
                    else:
                         # Shouldn't happen if users_to_remove was calculated correctly, but log for safety
                         logger.debug(f"  -> @{username} was already not in the in-memory backup set for @{account_username}.")

                    # Report progress (optional)
                    if not cancelled_early and ((i + 1) % 10 == 0 or (i + 1) == total_to_remove):
                         progress_msg = f"[Sync @{account_username} REMOVE] Progress: {i+1}/{total_to_remove} checked/attempted..."
                         await send_telegram_message(progress_msg) # Use your send function

                if cancelled_early:
                    logger.warning("[Sync] Cancellation signal received during REMOVE phase.")
                    msg_sync_cancelled_removing = "🟡 Sync is being cancelled (during removing)..."
                    if update and hasattr(update, 'message') and update.message:
                        await update.message.reply_text(msg_sync_cancelled_removing)
                    else:
                        await send_telegram_message(msg_sync_cancelled_removing)
                    # Jump out of sync logic (finally will be executed)

            # === After both phases (or cancellation) ===
            if cancelled_early:
                logger.info(f"[Sync @{account_username}] Process cancelled after {users_processed_add_count}/{total_to_add} adds and {users_processed_remove_count}/{total_to_remove} removes attempted.")
                summary = (f"🛑 Sync for @{account_username} cancelled!\n"
                        f"------------------------------------\n"
                        f" Additions attempted: {users_processed_add_count}/{total_to_add}\n"
                        f"   - Successful: {users_followed_in_sync}\n"
                        f"   - Already followed: {users_already_followed_checked}\n"
                        f"   - Errors: {users_failed_to_follow}\n"
                        f" Removals attempted: {users_processed_remove_count}/{total_to_remove}\n"
                        f"   - Successful: {users_unfollowed_in_sync}\n"
                        f"   - Not followed: {users_already_unfollowed_checked}\n"
                        f"   - Errors: {users_failed_to_unfollow}\n"
                        f"------------------------------------\n"
                        f" Changes to backup were saved up to cancellation.")
                if update and hasattr(update, 'message') and update.message:
                 await update.message.reply_text(summary)
                else:
                 await send_telegram_message(summary)
            else:
                # Normal end - report final result
                final_backup_size = len(account_backup_set)
                summary = (f"✅ Sync for @{account_username} completed:\n"
                           f"------------------------------------\n"
                           f" Globally followed (Base): {len(global_set_for_sync)}\n"
                           f" In Backup (Start): {initial_backup_size}\n"
                           f" In Backup (End): {final_backup_size}\n"
                           f"------------------------------------\n"
                           f" Adding (+{total_to_add}):\n"
                           f"   - Successfully followed: {users_followed_in_sync}\n"
                           f"   - Already followed (Check): {users_already_followed_checked}\n"
                           f"   - Errors following: {users_failed_to_follow}\n"
                           f" Removing (-{total_to_remove}):\n"
                           f"   - Successfully unfollowed: {users_unfollowed_in_sync}\n"
                           f"   - Not followed (Check): {users_already_unfollowed_checked}\n"
                           f"   - Errors unfollowing: {users_failed_to_unfollow}\n"
                           f"------------------------------------")
                if update and hasattr(update, 'message') and update.message:
                    await update.message.reply_text(summary)
                else:
                    await send_telegram_message(summary)
                    logger.info(f"[Sync @{account_username}] Synchronization completed.")

                # Update schedule date if called by scheduler (update is None) and not cancelled
                if update is None and not cancelled_early: # Check if it's a scheduled run
                    global last_sync_schedule_run_date, schedule_sync_enabled, USER_CONFIGURED_TIMEZONE # Ensure globals
                    if schedule_sync_enabled: # Only if still enabled
                        try:
                            local_tz_for_date = USER_CONFIGURED_TIMEZONE if USER_CONFIGURED_TIMEZONE else timezone.utc
                            today_for_sched_update = datetime.now(local_tz_for_date).date()
                            last_sync_schedule_run_date = today_for_sched_update
                            save_schedule()
                            logger.info(f"[Sync @{account_username}] Scheduled run completed. Updated last_sync_schedule_run_date to {today_for_sched_update}.")
                        except Exception as e_save_sched:
                            logger.error(f"[Sync @{account_username}] Error updating schedule after successful run: {e_save_sched}")

            # === Save backup (only if modified) ===
            # Save the updated backup set *after* all operations
            if backup_modified:
                logger.info(f"[Sync @{account_username}] Saving updated backup file: {backup_filepath}")
                save_set_to_file(account_backup_set, backup_filepath)
            else:
                 logger.info(f"[Sync @{account_username}] Backup file was not modified.")


    except Exception as e:
        error_message = f"💥 Critical error during synchronization for @{account_username}: {e}"
        await update.message.reply_text(error_message)
        logger.error(f"Critical error during sync for @{account_username}: {e}", exc_info=True)

    finally: # ===== IMPORTANT FINALLY BLOCK =====
        logger.debug(f"[Sync @{account_username}] Entering finally block.")
        # Return to the main timeline
        logger.debug(f"[Sync @{account_username}] Attempting to navigate back to home timeline...")
        try:
            if driver and "x.com" in driver.current_url and driver.current_url != "https://x.com/home":
                 logger.debug("Navigating to x.com/home")
                 driver.get("https://x.com/home")
                 await asyncio.sleep(random.uniform(3, 5))
            await switch_to_following_tab() # Ensures we are on the Following Tab
            logger.debug("[Sync] Successfully navigated back to home 'Following' tab.")
            navigation_successful = True # Not really used, but for info
        except Exception as nav_err:
            logger.error(f"[Sync] Error navigating back to home timeline: {nav_err}", exc_info=True)

        # --- Restore original pause state ---
        if not main_loop_was_initially_paused: # Only resume if we paused it
            await resume_scraping()
        logger.info(f"[Sync @{account_username}] Main scraping state restored (was_paused={main_loop_was_initially_paused}).")
        # ---

        # ===== Task End Marker =====
        is_sync_running = False
        cancel_sync_flag = False # Ensure flag is false for the next run
        if is_scheduled_run: # Clear the global flag only if this task set it
            is_any_scheduled_browser_task_running = False
        logger.info(f"[Sync @{account_username}] Status flags reset (is_any_scheduled_browser_task_running: {is_any_scheduled_browser_task_running}).")
        # =============================

async def fast_follow_logic(update: Update):
    """
    Executes the "Fast Follow" mode: Follows all users from the
    current account list sequentially with a short delay.
    (With cancellation option)
    """
    global driver, is_scraping_paused, pause_event
    global current_account_usernames_to_follow, global_followed_users_set
    global is_fast_follow_running, cancel_fast_follow_flag # Flags for this task

    account_username = get_current_account_username()
    if not account_username:
        logger.error("[Fast Follow] Cannot determine account username. Aborting.")
        # No update object here by default, so no reply_text
        return

    if is_fast_follow_running:
        logger.warning(f"[Fast Follow @{account_username}] Task is already running.")
        if update: await update.message.reply_text("⚠️ A Fast-Follow process is already running.")
        return

    # ===== Task Start Marker =====
    is_fast_follow_running = True
    cancel_fast_follow_flag = False
    # ================================

    logger.info(f"[Fast Follow @{account_username}] Starting fast follow process...")
    start_message = (f"🚀 Starting Fast-Follow for @{account_username}...\n"
                     f"   Users in list: {len(current_account_usernames_to_follow)}\n"
                     f"   To cancel: `/cancelfastfollow`")
    # Send message only if an update object was passed (e.g., from manual start)
    if update: await update.message.reply_text(start_message)
    else: await send_telegram_message(start_message) # Send to channel if started automatically

    await pause_scraping() # Pause main scraping

    # --- Initialize counters ---
    users_followed_in_task = 0
    users_already_followed_checked = 0
    users_failed_to_follow = 0
    users_processed_count = 0
    # --- End counters ---

    navigation_successful = False
    cancelled_early = False
    list_modified = False # Flag to know if the list needs to be saved

    # Copy the list to process it safely
    list_to_process = current_account_usernames_to_follow[:]
    total_to_process = len(list_to_process)

    try: # Main try block
        if not list_to_process:
            logger.info(f"[Fast Follow @{account_username}] List is empty. Nothing to do.")
            if update: await update.message.reply_text(f"ℹ️ Follow list for @{account_username} is empty.")
            # Jump directly to finally
        else:
            logger.info(f"[Fast Follow @{account_username}] Processing {total_to_process} users...")

            for i, username in enumerate(list_to_process):
                users_processed_count = i + 1
                if cancel_fast_follow_flag: cancelled_early = True; break # Cancellation check

                logger.debug(f"[Fast Follow @{account_username}] Attempt {i+1}/{total_to_process}: Following @{username}...")
                wait_follow = random.uniform(4, 7) # Keep a small pause
                logger.debug(f"    -> Waiting {wait_follow:.1f}s before next attempt")
                await asyncio.sleep(wait_follow)

                if cancel_fast_follow_flag: cancelled_early = True; break # Cancellation check after wait

                follow_result = await follow_user(username)

                if follow_result is True:
                    logger.debug(f"  -> Success!")
                    users_followed_in_task += 1
                    # Remove from the *global* list and mark for saving
                    if username in current_account_usernames_to_follow:
                        current_account_usernames_to_follow.remove(username)
                        list_modified = True
                    # Add to global list and backup
                    if username not in global_followed_users_set:
                        global_followed_users_set.add(username)
                        add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
                    backup_filepath = get_current_backup_file_path()
                    if backup_filepath: add_to_set_file({username}, backup_filepath)

                elif follow_result == "already_following":
                    logger.debug(f"  -> Already following.")
                    users_already_followed_checked += 1
                    # Remove from the *global* list and mark for saving
                    if username in current_account_usernames_to_follow:
                        current_account_usernames_to_follow.remove(username)
                        list_modified = True
                    # Ensure consistency (global/backup)
                    if username not in global_followed_users_set:
                        global_followed_users_set.add(username)
                        add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
                    backup_filepath = get_current_backup_file_path()
                    if backup_filepath: add_to_set_file({username}, backup_filepath)

                else: # Error
                    logger.warning(f"  -> Failed to follow @{username}! Remains in list.")
                    users_failed_to_follow += 1

                # Report progress (optional, e.g., every 10 users)
                if not cancelled_early and ((i + 1) % 10 == 0 or (i + 1) == total_to_process):
                     progress_msg = f"[Fast Follow @{account_username}] Progress: {i+1}/{total_to_process} attempted..."
                     await send_telegram_message(progress_msg) # Send to channel

            # === After the loop (or cancellation) ===
            if list_modified:
                logger.info(f"[Fast Follow @{account_username}] Saving updated follow list...")
                save_current_account_follow_list() # Save the modified list

            if cancelled_early:
                 logger.info(f"[Fast Follow @{account_username}] Process cancelled after {users_processed_count}/{total_to_process} attempts.")
                 summary = (f"🛑 Fast-Follow for @{account_username} cancelled!\n"
                            f"------------------------------------\n"
                            f" Follows attempted: {users_processed_count}/{total_to_process}\n"
                            f"   - Successful: {users_followed_in_task}\n"
                            f"   - Already followed: {users_already_followed_checked}\n"
                            f"   - Errors: {users_failed_to_follow}\n"
                            f"------------------------------------\n"
                            f" Changes to the list were saved up to cancellation.")
                 if update: await update.message.reply_text(summary)
                 else: await send_telegram_message(summary)
            else:
                # Normal end - report final result
                final_list_size = len(current_account_usernames_to_follow)
                summary = (f"✅ Fast-Follow for @{account_username} completed:\n"
                           f"------------------------------------\n"
                           f" Processed: {total_to_process}\n"
                           f"   - Successfully followed: {users_followed_in_task}\n"
                           f"   - Already followed (Check): {users_already_followed_checked}\n"
                           f"   - Errors following: {users_failed_to_follow}\n"
                           f"------------------------------------\n"
                           f" Remaining users in list: {final_list_size}")
                if update: await update.message.reply_text(summary)
                else: await send_telegram_message(summary)
                logger.info(f"[Fast Follow @{account_username}] Process completed.")

    except Exception as e:
        error_message = f"💥 Critical error during Fast-Follow for @{account_username}: {e}"
        if update: await update.message.reply_text(error_message)
        else: await send_telegram_message(error_message)
        logger.error(f"Critical error during fast follow for @{account_username}: {e}", exc_info=True)
        # Save list anyway if changes were made
        if list_modified:
            logger.info(f"[Fast Follow @{account_username}] Saving list state despite error...")
            save_current_account_follow_list()

    finally: # ===== IMPORTANT FINALLY BLOCK =====
        logger.debug(f"[Fast Follow @{account_username}] Entering finally block.")
        # Return to the main timeline
        logger.debug(f"[Fast Follow @{account_username}] Attempting to navigate back to home timeline...")
        try:
            if driver and "x.com" in driver.current_url and driver.current_url != "https://x.com/home":
                 logger.debug("Navigating to x.com/home")
                 driver.get("https://x.com/home")
                 await asyncio.sleep(random.uniform(3, 5))
            await switch_to_following_tab()
            logger.debug("[Fast Follow] Successfully navigated back to home 'Following' tab.")
            navigation_successful = True
        except Exception as nav_err:
            logger.error(f"[Fast Follow] Error navigating back to home timeline: {nav_err}", exc_info=True)

        # Resume main scraping
        logger.info(f"[Fast Follow @{account_username}] Resuming main scraping process.")
        await resume_scraping()

        # ===== Task End Marker =====
        is_fast_follow_running = False
        cancel_fast_follow_flag = False
        logger.info("[Fast Follow] Status flags reset.")
        # =============================

        # If the task finished and the mode is still "fast", set it to "off"
        global auto_follow_mode
        if auto_follow_mode == "fast":
            logger.info("[Fast Follow] Task finished, setting auto_follow_mode to 'off'.")
            auto_follow_mode = "off"
            save_settings()
            await send_telegram_message(f"ℹ️ Fast-Follow for @{account_username} completed. Mode set to 'OFF'.")

async def process_follow_list_schedule_logic(update: Update = None):
    """
    Scheduled task to process the current account's follow list.
    Follows users sequentially with a short delay.
    Manages pause/resume of main scraping internally.
    """
    global driver, is_scraping_paused, pause_event
    global current_account_usernames_to_follow, global_followed_users_set
    global is_any_scheduled_browser_task_running, is_scheduled_follow_list_running, cancel_scheduled_follow_list_flag

    account_username = get_current_account_username()
    if not account_username:
        logger.error("[Sched FollowList] Cannot determine account username. Aborting.")
        return

    # Prevent starting if another scheduled browser task is running
    if is_any_scheduled_browser_task_running:
        logger.warning(f"[Sched FollowList @{account_username}] Skipped: another scheduled browser task is active.")
        # Silently skip for scheduler. If manually triggered (update exists), inform.
        if update: await update.message.reply_text("⚠️ Cannot start: another scheduled browser task is active.")
        return
    
    if is_scheduled_follow_list_running: # Check if this specific task type is already running
        logger.warning(f"[Sched FollowList @{account_username}] Skipped: A Follow List task is already running.")
        if update: await update.message.reply_text("⚠️ Cannot start: A Follow List task is already running.")
        return

    # ===== Task Start Marker =====
    # Set specific flag first
    is_scheduled_follow_list_running = True
    cancel_scheduled_follow_list_flag = False # Reset for this run
    # Then set the general flag (this task always uses the browser and is scheduled)
    is_any_scheduled_browser_task_running = True
    # ================================

    logger.info(f"[Sched FollowList @{account_username}] Starting scheduled follow list processing...")
    start_message = (f"⏰ Starting Scheduled Follow List Processing for @{account_username}...\n"
                     f"   Users in list: {len(current_account_usernames_to_follow)}\n"
                     f"   To cancel (if running long): `/canceldbscrape` (uses same flag for now, or implement specific cancel)") # Temporary cancel note
    if update: await update.message.reply_text(start_message)
    else: await send_telegram_message(start_message)

    main_loop_was_initially_paused = is_scraping_paused
    if not main_loop_was_initially_paused:
        await pause_scraping()

    users_followed_in_task = 0
    users_already_followed_checked = 0
    users_failed_to_follow = 0
    list_modified = False

    list_to_process = current_account_usernames_to_follow[:] # Process a copy
    total_to_process = len(list_to_process)

    try:
        if not list_to_process:
            logger.info(f"[Sched FollowList @{account_username}] List is empty. Nothing to do.")
            if update: await update.message.reply_text(f"ℹ️ Follow list for @{account_username} is empty.")
            else: await send_telegram_message(f"ℹ️ Follow list for @{account_username} is empty for scheduled run.")
        else:
            logger.info(f"[Sched FollowList @{account_username}] Processing {total_to_process} users...")

            for i, username in enumerate(list_to_process):
                if cancel_scheduled_follow_list_flag:
                    logger.info(f"[Sched FollowList @{account_username}] Cancellation requested. Stopping.")
                    if update: await update.message.reply_text("🟡 Follow List processing cancelled.")
                    else: await send_telegram_message(f"🟡 Scheduled Follow List for @{account_username} cancelled.")
                    break # Exit the loop
                logger.debug(f"[Sched FollowList @{account_username}] Attempt {i+1}/{total_to_process}: Following @{username}...")
                wait_follow = random.uniform(4, 7)
                logger.debug(f"    -> Waiting {wait_follow:.1f}s")
                await asyncio.sleep(wait_follow)

                follow_result = await follow_user(username) # follow_user navigates back to /home

                if follow_result is True:
                    logger.debug(f"  -> Success!")
                    users_followed_in_task += 1
                    if username in current_account_usernames_to_follow:
                        current_account_usernames_to_follow.remove(username)
                        list_modified = True
                    if username not in global_followed_users_set:
                        global_followed_users_set.add(username)
                        add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
                    backup_filepath = get_current_backup_file_path()
                    if backup_filepath: add_to_set_file({username}, backup_filepath)

                elif follow_result == "already_following":
                    logger.debug(f"  -> Already following.")
                    users_already_followed_checked += 1
                    if username in current_account_usernames_to_follow:
                        current_account_usernames_to_follow.remove(username)
                        list_modified = True
                    if username not in global_followed_users_set:
                        global_followed_users_set.add(username)
                        add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
                    backup_filepath = get_current_backup_file_path()
                    if backup_filepath: add_to_set_file({username}, backup_filepath)
                else: # Error
                    logger.warning(f"  -> Failed to follow @{username}! Remains in list.")
                    users_failed_to_follow += 1

                if (i + 1) % 10 == 0 or (i + 1) == total_to_process:
                     progress_msg = f"[Sched FollowList @{account_username}] Progress: {i+1}/{total_to_process} attempted..."
                     await send_telegram_message(progress_msg)

            if list_modified:
                logger.info(f"[Sched FollowList @{account_username}] Saving updated follow list...")
                save_current_account_follow_list()

            final_list_size = len(current_account_usernames_to_follow)
            summary = (f"✅ Scheduled Follow List Processing for @{account_username} completed:\n"
                       f"------------------------------------\n"
                       f" Processed: {total_to_process}\n"
                       f"   - Successfully followed: {users_followed_in_task}\n"
                       f"   - Already followed: {users_already_followed_checked}\n"
                       f"   - Errors: {users_failed_to_follow}\n"
                       f"------------------------------------\n"
                       f" Remaining users in list: {final_list_size}")
            if update: await update.message.reply_text(summary)
            else: await send_telegram_message(summary)
            logger.info(f"[Sched FollowList @{account_username}] Process completed.")

            # Update schedule date if called by scheduler (update is None)
            if update is None: # Indicates a scheduled run
                global last_follow_list_schedule_run_date, schedule_follow_list_enabled, USER_CONFIGURED_TIMEZONE # Ensure globals
                if schedule_follow_list_enabled: # Only if still enabled
                    try:
                        local_tz_for_date = USER_CONFIGURED_TIMEZONE if USER_CONFIGURED_TIMEZONE else timezone.utc
                        today_for_sched_update = datetime.now(local_tz_for_date).date()
                        last_follow_list_schedule_run_date = today_for_sched_update
                        save_schedule()
                        logger.info(f"[Sched FollowList @{account_username}] Scheduled run completed. Updated last_follow_list_schedule_run_date to {today_for_sched_update}.")
                    except Exception as e_save_sched:
                        logger.error(f"[Sched FollowList @{account_username}] Error updating schedule after successful run: {e_save_sched}")

    except Exception as e:
        error_message = f"💥 Critical error during Scheduled Follow List for @{account_username}: {e}"
        if update: await update.message.reply_text(error_message)
        else: await send_telegram_message(error_message)
        logger.error(f"Critical error during scheduled follow list for @{account_username}: {e}", exc_info=True)
        if list_modified:
            logger.info(f"[Sched FollowList @{account_username}] Saving list state despite error...")
            save_current_account_follow_list()
    finally:
        logger.debug(f"[Sched FollowList @{account_username}] Entering finally block.")
        try:
            if driver and "x.com" in driver.current_url and driver.current_url != "https://x.com/home":
                 driver.get("https://x.com/home")
                 await asyncio.sleep(random.uniform(3, 5))
            await switch_to_following_tab()
        except Exception as nav_err:
            logger.error(f"[Sched FollowList] Error navigating back to home timeline: {nav_err}", exc_info=True)

        # --- Restore original pause state ---
        if not main_loop_was_initially_paused: # Only resume if we paused it
            await resume_scraping()
        logger.info(f"[Sched FollowList @{account_username}] Main scraping state restored (was_paused={main_loop_was_initially_paused}).")
        
        # ===== Task End Marker =====
        is_scheduled_follow_list_running = False
        is_any_scheduled_browser_task_running = False # This task was the one using it
        cancel_scheduled_follow_list_flag = False # Reset for next potential run
        logger.info(f"[Sched FollowList @{account_username}] Status flags reset.")
        # =============================

async def cancel_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Requests cancellation of the ongoing backup process."""
    global is_backup_running, cancel_backup_flag
    if is_backup_running:
        cancel_backup_flag = True
        await update.message.reply_text("🟡 Cancellation of backup requested. It might take a moment for the process to stop.")
        print("[Cancel] Backup cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ No backup process is currently running.")
    # No resume/pause here, this command only affects the flag

async def cancel_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Requests cancellation of the ongoing sync process."""
    global is_sync_running, cancel_sync_flag
    if is_sync_running:
        cancel_sync_flag = True
        await update.message.reply_text("🟡 Cancellation of sync requested. It might take a moment for the process to stop.")
        print("[Cancel] Sync cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ No sync process is currently running.")
    # No resume/pause here, this command only affects the flag

async def global_list_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays information about the global follower list."""
    await pause_scraping() # Pause for the duration of the command

    file_path = GLOBAL_FOLLOWED_FILE
    response_message = f"ℹ️ Status of the global list (`{os.path.basename(file_path)}`):\n"

    try:
        if os.path.exists(file_path):
            # Get last modification time
            mod_timestamp = os.path.getmtime(file_path)
            # Get timezone
            local_tz = USER_CONFIGURED_TIMEZONE
            if local_tz is None: local_tz = timezone.utc # Fallback
            
            mod_datetime_local = datetime.fromtimestamp(mod_timestamp, tz=local_tz)
            mod_time_str = mod_datetime_local.strftime('%Y-%m-%d %H:%M:%S %Z%z')

            # Get number of entries (by reading the file)
            current_global_set = load_set_from_file(file_path)
            user_count = len(current_global_set)

            response_message += f"  - Last modified: {mod_time_str}\n"
            response_message += f"  - User count: {user_count}"
        else:
            response_message += "  - File does not exist yet."

    except Exception as e:
        logger.error(f"Error getting info for {file_path}: {e}", exc_info=True)
        response_message += f"\n❌ Error retrieving file information: {e}"

    await update.message.reply_text(response_message)
    await resume_scraping() # Resume after the command

async def build_global_from_backups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Updates the global follower list by merging the contents of ALL
    existing account backups (Union).
    Existing global entries are NOT deleted.
    """
    global global_followed_users_set # Access to update

    combined_set = set()
    missing_backups = []
    processed_accounts = 0

    await update.message.reply_text("⏳ Reading all existing account backups...")

    # Iterate through all configured accounts
    for i, account_info in enumerate(ACCOUNTS):
        acc_num = i + 1
        acc_username = account_info.get("username")
        if not acc_username:
            logger.warning(f"Skipping account {acc_num}: No username configured.")
            continue

        safe_username = re.sub(r'[\\/*?:"<>|]', "_", acc_username)
        backup_filepath = FOLLOWER_BACKUP_TEMPLATE.format(safe_username)

        if os.path.exists(backup_filepath):
            logger.info(f"Reading backup for @{acc_username} from {os.path.basename(backup_filepath)}...")
            backup_set = load_set_from_file(backup_filepath)
            combined_set.update(backup_set) # Add users to the total set (Union)
            processed_accounts += 1
            logger.debug(f"  -> Added {len(backup_set)} users. Combined set size: {len(combined_set)}")
        else:
            logger.warning(f"Backup file not found for account @{acc_username}. Skipping.")
            missing_backups.append(f"@{acc_username}")

    if processed_accounts == 0:
        await update.message.reply_text("❌ No backup files found. Please run `/backupfollowers` for at least one account first.")
        return

    # Load the *current* global set to see how many are new
    current_global_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
    newly_added_count = len(combined_set - current_global_set)
    final_global_set = current_global_set.union(combined_set) # Combine existing global with all backups

    # Build the confirmation message
    confirmation_message = (
        f"ℹ️ Backup merge completed ({processed_accounts} accounts read).\n"
        f"   - Total users found (from backups): {len(combined_set)}\n"
        f"   - Current global list: {len(current_global_set)} users\n"
        f"   - New users to add: {newly_added_count}\n"
        f"   - Final global list will contain {len(final_global_set)} users.\n\n"
    )
    if missing_backups:
        confirmation_message += f"⚠️ Missing backups: {', '.join(missing_backups)}\n\n"

    confirmation_message += (
        f"Do you want to update the global list (`{GLOBAL_FOLLOWED_FILE}`) with these {len(final_global_set)} users now? "
        f"(Only users will be added, none removed)."
    )

    # Ask for confirmation
    keyboard = [[
        InlineKeyboardButton(f"✅ Yes, update global list", callback_data=f"confirm_build_global"),
        InlineKeyboardButton("❌ No, cancel", callback_data="cancel_build_global")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(confirmation_message, reply_markup=reply_markup)
    # No resume here, wait for button

async def init_global_from_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Initializes/Overwrites the global follower list from the backup
    of a specific or the current account.
    """
    global global_followed_users_set # Access to reload

    target_account_index = -1
    target_account_username = None
    backup_filepath = None

    # Determine the target account
    if context.args:
        try:
            account_num = int(context.args[0])
            target_account_index = account_num - 1
            if not (0 <= target_account_index < len(ACCOUNTS)):
                await update.message.reply_text(f"❌ Invalid account number. Available: 1-{len(ACCOUNTS)}")
                return
            target_account_username = ACCOUNTS[target_account_index].get("username")
            # Get backup path for the target account
            safe_username = re.sub(r'[\\/*?:"<>|]', "_", target_account_username) if target_account_username else None
            if safe_username:
                backup_filepath = FOLLOWER_BACKUP_TEMPLATE.format(safe_username)
            else:
                 await update.message.reply_text(f"❌ Could not find username for account {account_num}.")
                 return
        except ValueError:
            await update.message.reply_text("❌ Please provide a valid account number (e.g., `/initglobalfrombackup 1`).")
            return
        except Exception as e:
             await update.message.reply_text(f"❌ Error determining target account: {e}")
             return
    else:
        # If no number provided, take the current account
        target_account_index = current_account
        target_account_username = get_current_account_username()
        backup_filepath = get_current_backup_file_path()
        if not target_account_username or not backup_filepath:
             await update.message.reply_text("❌ Could not determine current account or backup path.")
             return

    # Check if the backup file exists
    if not backup_filepath or not os.path.exists(backup_filepath):
        await update.message.reply_text(f"❌ Backup file for account @{target_account_username} (`{os.path.basename(backup_filepath or '')}`) not found. Please run `/backupfollowers` for this account first.")
        return

    # Ask for confirmation
    keyboard = [[
        InlineKeyboardButton(f"✅ Yes, overwrite global list", callback_data=f"confirm_init_global:{target_account_index}"),
        InlineKeyboardButton("❌ No, cancel", callback_data="cancel_init_global")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ **Attention!**\n"
        f"This will overwrite the entire global follower list (`{GLOBAL_FOLLOWED_FILE}`) "
        f"with the content of the backup from account {target_account_index + 1} (@{target_account_username}).\n\n"
        f"All previous entries in the global list will be lost.\n"
        f"Proceed?",
        reply_markup=reply_markup
    )
    # No resume here, wait for button

async def autofollow_pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pauses the automatic processing of the follow list."""
    global is_periodic_follow_active
    is_periodic_follow_active = False
    await update.message.reply_text("⏸️ Automatic following from the account list has been paused.")
    print("[Auto-Follow] Paused via Telegram command.")
    # No resume_scraping needed, as the control only affects starting the follow process

async def autofollow_resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resumes the automatic processing of the follow list."""
    global is_periodic_follow_active
    is_periodic_follow_active = True
    await update.message.reply_text("▶️ Automatic following from the account list has been resumed.")
    print("[Auto-Follow] Resumed via Telegram command.")
    # No resume_scraping needed

async def autofollow_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the status of the automatic follow function for the current account."""
    global is_periodic_follow_active, current_account_usernames_to_follow
    status = "ACTIVE ▶️" if is_periodic_follow_active else "PAUSED ⏸️"
    account_username = get_current_account_username() or "Unknown"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)
    await update.message.reply_text(f"🤖 Status Auto-Follow for @{account_username}: {status}\n"
                                     f"📝 Users in `{filename}`: {count}")
    # IMPORTANT: Main handler paused, resume here
    await resume_scraping()

async def clear_follow_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Asks for confirmation to clear the *current* account's follow list."""
    global current_account_usernames_to_follow
    account_username = get_current_account_username() or "Unknown"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)

    if count == 0:
        await update.message.reply_text(f"ℹ️ The follow list for @{account_username} (`{filename}`) is already empty.")
        await resume_scraping() # Resume
        return

    keyboard = [[
        # The payload now contains the account name for safety
        InlineKeyboardButton(f"✅ Yes, clear list for @{account_username}", callback_data=f"confirm_clear_follow_list:{account_username}"),
        InlineKeyboardButton("❌ No, cancel", callback_data="cancel_clear_follow_list")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ Are you sure you want to delete the follow list for account @{account_username} (`{filename}`)? "
        f"It currently contains {count} users.",
        reply_markup=reply_markup
    )
    # No resume_scraping here, wait for button response or timeout

async def show_mode(update: Update):
    """Displays the current search mode"""
    global search_mode
    mode_text = "Keywords" if search_mode == "full" else "CA Only"
    await update.message.reply_text(f"🔍 Search mode: {mode_text}")
    await resume_scraping()

async def set_mode_full(update: Update):
    """Sets the search mode to CA + Keywords"""
    global search_mode
    search_mode = "full"
    await update.message.reply_text("✅ Search mode set to CA + Keywords")
    await resume_scraping()

async def set_mode_ca_only(update: Update):
    """Sets the search mode to CA only"""
    global search_mode
    search_mode = "ca_only"
    await update.message.reply_text("✅ Search mode set to CA Only")
    await resume_scraping()

async def ping_pong_request(update: Update):
    """Process ping requests centrally"""
    await update.message.reply_text(f"🏓 Pong!")
    await resume_scraping()

async def pause_request(update: Update):
    """Pauses scraping"""
    global is_schedule_pause
    await update.message.reply_text(f"⏸️ Pausing scraping...")
    is_schedule_pause = False  # Manual pause, not from scheduler
    await pause_scraping()
    await update.message.reply_text(f"⏸️ Scraping has been paused! Use 'resume' to continue.")
    # DO NOT call resume_scraping()!

async def resume_request(update: Update):

    """Resumes scraping"""
    await update.message.reply_text(f"▶️ Resuming scraping...")
    await resume_scraping()
    await update.message.reply_text(f"▶️ Scraping is running again!")

async def show_schedule(update: Update):
    """Show the current schedule settings"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    status = "ENABLED ✅" if schedule_enabled else "DISABLED ❌"

    # Use the globally configured timezone
    local_tz = USER_CONFIGURED_TIMEZONE
    if local_tz is None: local_tz = timezone.utc # Fallback
    
    # Get current time in local timezone
    now_local = datetime.now(local_tz)
    current_time_str = now_local.strftime("%H:%M") # String for display
    tz_name_display = USER_TIMEZONE_STR # Display the configured (or fallen-back-to) name

    message = (
        f"📅 Schedule: {status}\n"
        f"⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}\n"
        f"🕒 Current bot time: {current_time_str} ({tz_name_display})\n\n"
    )

    # Add schedule status
    if schedule_enabled:
        # Check if we're currently in the pause period
        global is_schedule_pause
        is_schedule_pause = True # Assume paused if schedule is on and we are checking
        # now_local is already defined above and is timezone-aware
        today_local = now_local.date()
        current_dt = now_local # Use the already timezone-aware datetime
        start_time = datetime.strptime(f"{today_local} {schedule_pause_start}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
        end_time = datetime.strptime(f"{today_local} {schedule_pause_end}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)

        # Handle overnight periods
        if end_time < start_time:
            # If current time is after start OR before end (on the next day conceptually)
            if current_dt >= start_time or current_dt < end_time:
                in_pause = True
            else:
                in_pause = False
        else:
            # Same day period
            if start_time <= current_dt < end_time:
                 in_pause = True
            else:
                 in_pause = False

        if in_pause:
            next_event = f"Resume at {schedule_pause_end}"
            status_str = "⏸️ PAUSED"
        else:
            is_schedule_pause = False # Not actually paused by schedule right now
            if current_dt < start_time:
                next_event = f"Pause at {schedule_pause_start}"
            else:
                # Next pause is tomorrow
                next_event = f"Pause tomorrow at {schedule_pause_start}"
            status_str = "▶️ RUNNING"

        message += f"Status: {status_str}\nNext event: {next_event}"

    await update.message.reply_text(message)
    await resume_scraping()

async def show_schedule_set_command(update: Update):
    """Shows a prepared command for setting the schedule"""
    global schedule_pause_start, schedule_pause_end

    # Create the command with the current time range
    command = f"schedule time {schedule_pause_start}-{schedule_pause_end}"

    # Send the message
    await update.message.reply_text(
        f"{command}"
    )
    await resume_scraping()

async def set_schedule_enabled(update: Update, enabled: bool):
    """Enable or disable the schedule"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end, is_schedule_pause
    schedule_enabled = enabled
    save_schedule()

    if enabled:
        # Use the globally configured timezone
        local_tz = USER_CONFIGURED_TIMEZONE
        if local_tz is None: local_tz = timezone.utc # Fallback

        # Get current time in local timezone
        now_local = datetime.now(local_tz)
        current_time_str = now_local.strftime("%H:%M") # String for display
        today_local = now_local.date()

        # Create datetime objects for comparison (all timezone-aware)
        current_dt = now_local # Already tz-aware
        start_time = datetime.strptime(f"{today_local} {schedule_pause_start}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
        end_time = datetime.strptime(f"{today_local} {schedule_pause_end}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)

        # Handle overnight periods
        in_pause = False
        if end_time <= start_time: # Overnight
            if current_dt >= start_time or current_dt < end_time:
                in_pause = True
        else: # Same day
            if start_time <= current_dt < end_time:
                in_pause = True

        # Check if we're currently in the pause period
        if in_pause:
            status_msg = (
                f"✅ Schedule ENABLED\n"
                f"⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}\n"
                f"⚠️ Current time ({current_time_str}) is within pause period!\n"
                f"⏸️ Bot will pause now until {schedule_pause_end}"
            )
            # Trigger pause immediately
            is_schedule_pause = True # Mark as schedule pause
            await pause_scraping() # Pause now
        else:
            # Calculate time until next pause
            if current_dt < start_time:
                time_diff = start_time - current_dt
                hours, remainder = divmod(time_diff.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                time_until = f"{hours}h {minutes}m"
                next_pause = f"Today at {schedule_pause_start}"
            else:
                # Next pause is tomorrow
                tomorrow_start = start_time + timedelta(days=1)
                time_diff = tomorrow_start - current_dt
                hours, remainder = divmod(time_diff.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                time_until = f"{hours}h {minutes}m"
                next_pause = f"Tomorrow at {schedule_pause_start}"

            status_msg = (
                f"✅ Schedule ENABLED\n"
                f"⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}\n"
                f"▶️ Bot will continue running until {next_pause}\n"
                f"⏱️ Time until next pause: {time_until}"
            )
            # Ensure bot is running if schedule is enabled but outside pause time
            if is_scraping_paused and is_schedule_pause:
                 await resume_scraping()
            is_schedule_pause = False # Not paused by schedule
    else:
        status_msg = f"❌ Schedule DISABLED\n⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}"
        # If schedule is disabled, ensure bot is running (unless manually paused)
        if is_scraping_paused and is_schedule_pause:
             await resume_scraping()
        is_schedule_pause = False # No longer paused by schedule

    await update.message.reply_text(status_msg)
    # Resume scraping if it wasn't paused by the schedule logic above
    if not is_scraping_paused:
        await resume_scraping()


async def set_schedule_time(update: Update, time_str: str):
    """Set the schedule pause time period"""
    global schedule_pause_start, schedule_pause_end

    # Split the time range and remove any spaces
    time_parts = [t.strip() for t in time_str.split('-')]

    if len(time_parts) != 2:
        await update.message.reply_text("❌ Invalid time format. Please use HH:MM-HH:MM (24-hour format)")
        await resume_scraping()
        return

    start_time = time_parts[0].strip()
    end_time = time_parts[1].strip()

    # Validate time formats (HH:MM)
    if not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', start_time) or not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', end_time):
        await update.message.reply_text("❌ Invalid time format. Please use HH:MM-HH:MM (24-hour format)")
        await resume_scraping()
        return

    schedule_pause_start = start_time
    schedule_pause_end = end_time
    save_schedule()
    await update.message.reply_text(f"✅ Schedule pause period set to {start_time} - {end_time}")
    await resume_scraping()

async def schedule_sync_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggles the scheduled sync ON or OFF."""
    global schedule_sync_enabled
    schedule_sync_enabled = not schedule_sync_enabled
    save_schedule()
    status = "ENABLED 🟢" if schedule_sync_enabled else "DISABLED 🔴"
    await update.message.reply_text(f"⏰ Scheduled Sync is now {status}.")
    logger.info(f"Scheduled Sync toggled to {status} by user {update.message.from_user.id}")
    await resume_scraping() # Command handler should resume

async def schedule_sync_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the time window for scheduled sync."""
    global schedule_sync_start_time, schedule_sync_end_time
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            f"ℹ️ Please provide the time window in HH:MM-HH:MM format.\n"
            f"Current Sync Window: {schedule_sync_start_time}-{schedule_sync_end_time}\n\n"
            f"Format: `/schedulesynctime HH:MM-HH:MM`\n\n"
            f"Example: `/schedulesynctime {schedule_sync_start_time}-{schedule_sync_end_time}`",
            parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    time_range_str = context.args[0].strip()
    time_parts = [t.strip() for t in time_range_str.split('-')]

    if len(time_parts) != 2:
        await update.message.reply_text("❌ Invalid time format. Please use HH:MM-HH:MM.")
        await resume_scraping()
        return

    start_str, end_str = time_parts[0], time_parts[1]

    if not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', start_str) or \
       not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', end_str):
        await update.message.reply_text("❌ Invalid time format in window. Please use HH:MM for both start and end.")
        await resume_scraping()
        return

    # Optional: Check if end_time is after start_time if they are on the same day.
    # For simplicity, we'll allow overnight windows like 23:00-01:00.
    # The run loop logic will handle this.

    # Check for overlap with Follow List schedule if it's enabled
    if schedule_follow_list_enabled:
        # Define a helper function for checking overlap (can be defined globally or locally if preferred)
        def _check_overlap(s1_str, e1_str, s2_str, e2_str):
            try:
                s1 = datetime.strptime(s1_str, "%H:%M").time()
                e1 = datetime.strptime(e1_str, "%H:%M").time()
                s2 = datetime.strptime(s2_str, "%H:%M").time()
                e2 = datetime.strptime(e2_str, "%H:%M").time()

                # Convert to minutes from midnight for easier comparison
                s1_mins = s1.hour * 60 + s1.minute
                e1_mins = e1.hour * 60 + e1.minute
                s2_mins = s2.hour * 60 + s2.minute
                e2_mins = e2.hour * 60 + e2.minute

                # Create lists of (start, end) intervals, handling overnight
                intervals1 = []
                if e1_mins <= s1_mins: # Overnight for interval 1
                    intervals1.append((s1_mins, 24 * 60)) # s1 to midnight
                    intervals1.append((0, e1_mins))       # midnight to e1
                else:
                    intervals1.append((s1_mins, e1_mins))

                intervals2 = []
                if e2_mins <= s2_mins: # Overnight for interval 2
                    intervals2.append((s2_mins, 24 * 60))
                    intervals2.append((0, e2_mins))
                else:
                    intervals2.append((s2_mins, e2_mins))

                for i1_s, i1_e in intervals1:
                    for i2_s, i2_e in intervals2:
                        # Check for overlap: max_start < min_end
                        if max(i1_s, i2_s) < min(i1_e, i2_e):
                            return True # Overlap found
                return False # No overlap
            except ValueError: # Should not happen if times are validated
                return False 

        if _check_overlap(start_str, end_str, schedule_follow_list_start_time, schedule_follow_list_end_time):
            await update.message.reply_text(
                f"❌ Overlap Detected! The new Sync window ({start_str}-{end_str}) "
                f"overlaps with the enabled Follow List window ({schedule_follow_list_start_time}-{schedule_follow_list_end_time}).\n"
                f"Please adjust the times or disable one of the schedules."
            )
            await resume_scraping()
            return

    global last_sync_schedule_run_date # Add this global
    schedule_sync_start_time = start_str
    schedule_sync_end_time = end_str
    
    # If the schedule is enabled and times are changing, reset the last run date
    # to allow it to run again today if the new window is met.
    if schedule_sync_enabled:
        last_sync_schedule_run_date = None # Resetting to None forces re-evaluation
        logger.info(f"Scheduled Sync time changed. Resetting last_sync_schedule_run_date to allow re-trigger today if new window is met.")
        
    save_schedule()
    await update.message.reply_text(f"✅ Scheduled Sync window set to {schedule_sync_start_time}-{schedule_sync_end_time}.")
    logger.info(f"Scheduled Sync window set to {schedule_sync_start_time}-{schedule_sync_end_time} by user {update.message.from_user.id}")
    await resume_scraping()

async def schedule_follow_list_toggle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggles the scheduled follow list processing ON or OFF."""
    global schedule_follow_list_enabled
    schedule_follow_list_enabled = not schedule_follow_list_enabled
    save_schedule()
    status = "ENABLED 🟢" if schedule_follow_list_enabled else "DISABLED 🔴"
    await update.message.reply_text(f"🚶‍♂️‍➡️ Scheduled Follow List Processing is now {status}.")
    logger.info(f"Scheduled Follow List Processing toggled to {status} by user {update.message.from_user.id}")
    await resume_scraping()

async def schedule_follow_list_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the time window for scheduled follow list processing."""
    global schedule_follow_list_start_time, schedule_follow_list_end_time
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            f"ℹ️ Please provide the time window in HH:MM-HH:MM format.\n"
            f"Current Follow List Window: {schedule_follow_list_start_time}-{schedule_follow_list_end_time}\n\n"
            f"Format: `/schedulefollowlisttime HH:MM-HH:MM`\n\n"
            f"Example: `/schedulefollowlisttime {schedule_follow_list_start_time}-{schedule_follow_list_end_time}`",
            parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    time_range_str = context.args[0].strip()
    time_parts = [t.strip() for t in time_range_str.split('-')]

    if len(time_parts) != 2:
        await update.message.reply_text("❌ Invalid time format. Please use HH:MM-HH:MM.")
        await resume_scraping()
        return

    start_str, end_str = time_parts[0], time_parts[1]

    if not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', start_str) or \
       not re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', end_str):
        await update.message.reply_text("❌ Invalid time format in window. Please use HH:MM for both start and end.")
        await resume_scraping()
        return

    # Check for overlap with Sync schedule if it's enabled
    if schedule_sync_enabled:
        # Define a helper function for checking overlap (can be defined globally or locally if preferred)
        def _check_overlap(s1_str, e1_str, s2_str, e2_str):
            try:
                s1 = datetime.strptime(s1_str, "%H:%M").time()
                e1 = datetime.strptime(e1_str, "%H:%M").time()
                s2 = datetime.strptime(s2_str, "%H:%M").time()
                e2 = datetime.strptime(e2_str, "%H:%M").time()

                s1_mins = s1.hour * 60 + s1.minute
                e1_mins = e1.hour * 60 + e1.minute
                s2_mins = s2.hour * 60 + s2.minute
                e2_mins = e2.hour * 60 + e2.minute

                intervals1 = []
                if e1_mins <= s1_mins: 
                    intervals1.append((s1_mins, 24 * 60))
                    intervals1.append((0, e1_mins))      
                else:
                    intervals1.append((s1_mins, e1_mins))

                intervals2 = []
                if e2_mins <= s2_mins: 
                    intervals2.append((s2_mins, 24 * 60))
                    intervals2.append((0, e2_mins))
                else:
                    intervals2.append((s2_mins, e2_mins))

                for i1_s, i1_e in intervals1:
                    for i2_s, i2_e in intervals2:
                        if max(i1_s, i2_s) < min(i1_e, i2_e):
                            return True 
                return False 
            except ValueError: 
                return False

        if _check_overlap(start_str, end_str, schedule_sync_start_time, schedule_sync_end_time):
            await update.message.reply_text(
                f"❌ Overlap Detected! The new Follow List window ({start_str}-{end_str}) "
                f"overlaps with the enabled Sync window ({schedule_sync_start_time}-{schedule_sync_end_time}).\n"
                f"Please adjust the times or disable one of the schedules."
            )
            await resume_scraping()
            return

    global last_follow_list_schedule_run_date # Add this global
    schedule_follow_list_start_time = start_str
    schedule_follow_list_end_time = end_str

    # If the schedule is enabled and times are changing, reset the last run date
    if schedule_follow_list_enabled:
        last_follow_list_schedule_run_date = None # Resetting to None forces re-evaluation
        logger.info(f"Scheduled Follow List time changed. Resetting last_follow_list_schedule_run_date to allow re-trigger today if new window is met.")

    save_schedule()
    await update.message.reply_text(f"✅ Scheduled Follow List Processing window set to {schedule_follow_list_start_time}-{schedule_follow_list_end_time}.")
    logger.info(f"Scheduled Follow List Processing window set to {schedule_follow_list_start_time}-{schedule_follow_list_end_time} by user {update.message.from_user.id}")
    await resume_scraping()

async def show_detailed_schedules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the status of all schedules."""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    global schedule_sync_enabled, schedule_sync_time, last_sync_schedule_run_date
    global schedule_follow_list_enabled, schedule_follow_list_time, last_follow_list_schedule_run_date
    global USER_CONFIGURED_TIMEZONE, USER_TIMEZONE_STR

    local_tz = USER_CONFIGURED_TIMEZONE if USER_CONFIGURED_TIMEZONE else timezone.utc
    now_local = datetime.now(local_tz)
    today_date_local = now_local.date()

    main_sched_status = "ENABLED 🟢" if schedule_enabled else "DISABLED 🔴"
    sync_sched_status = "ENABLED 🟢" if schedule_sync_enabled else "DISABLED 🔴"
    follow_list_sched_status = "ENABLED 🟢" if schedule_follow_list_enabled else "DISABLED 🔴"

    main_sched_next_run = ""
    if schedule_enabled:
        # This logic is simplified from your check_schedule, focusing on next action
        start_dt_naive = datetime.strptime(schedule_pause_start, "%H:%M").time()
        end_dt_naive = datetime.strptime(schedule_pause_end, "%H:%M").time()
        current_time_naive = now_local.time()
        if end_dt_naive <= start_dt_naive: # Overnight
            if start_dt_naive <= current_time_naive or current_time_naive < end_dt_naive: # Currently in pause
                main_sched_next_run = f"(Active Pause until {schedule_pause_end})"
            else: # Not in pause, next pause is start_dt
                main_sched_next_run = f"(Next Pause at {schedule_pause_start})"
        else: # Same day
            if start_dt_naive <= current_time_naive < end_dt_naive: # Currently in pause
                main_sched_next_run = f"(Active Pause until {schedule_pause_end})"
            else: # Not in pause
                if current_time_naive < start_dt_naive:
                    main_sched_next_run = f"(Next Pause at {schedule_pause_start})"
                else: # Pause for today passed, next is tomorrow
                    main_sched_next_run = f"(Next Pause tomorrow at {schedule_pause_start})"


    sync_run_status_text = ""
    if schedule_sync_enabled:
        start_sync_t = datetime.strptime(schedule_sync_start_time, "%H:%M").time()
        end_sync_t = datetime.strptime(schedule_sync_end_time, "%H:%M").time()
        current_t = now_local.time()
        ran_today_sync = last_sync_schedule_run_date == today_date_local

        if end_sync_t <= start_sync_t: # Overnight
            if (start_sync_t <= current_t or current_t < end_sync_t) and not ran_today_sync:
                sync_run_status_text = "(Active Window - Pending)"
            elif ran_today_sync:
                sync_run_status_text = "(Ran Today)"
            else:
                sync_run_status_text = "(Scheduled)"
        else: # Same day
            if start_sync_t <= current_t < end_sync_t and not ran_today_sync:
                sync_run_status_text = "(Active Window - Pending)"
            elif ran_today_sync:
                sync_run_status_text = "(Ran Today)"
            else:
                sync_run_status_text = "(Scheduled)"
    
    follow_list_run_status_text = ""
    if schedule_follow_list_enabled:
        start_fl_t = datetime.strptime(schedule_follow_list_start_time, "%H:%M").time()
        end_fl_t = datetime.strptime(schedule_follow_list_end_time, "%H:%M").time()
        current_t = now_local.time()
        ran_today_fl = last_follow_list_schedule_run_date == today_date_local

        if end_fl_t <= start_fl_t: # Overnight
            if (start_fl_t <= current_t or current_t < end_fl_t) and not ran_today_fl:
                follow_list_run_status_text = "(Active Window - Pending)"
            elif ran_today_fl:
                follow_list_run_status_text = "(Ran Today)"
            else:
                follow_list_run_status_text = "(Scheduled)"
        else: # Same day
            if start_fl_t <= current_t < end_fl_t and not ran_today_fl:
                follow_list_run_status_text = "(Active Window - Pending)"
            elif ran_today_fl:
                follow_list_run_status_text = "(Ran Today)"
            else:
                follow_list_run_status_text = "(Scheduled)"

    message = (
        f"🗓️ **All Schedule Statuses** ({USER_TIMEZONE_STR}) 🗓️\n\n"
        f"⏸️ **Main Bot Pause Schedule:** {main_sched_status}\n"
        f"   └ Time: {schedule_pause_start} - {schedule_pause_end} {main_sched_next_run}\n\n"
        f"🔄 **Scheduled Sync:** {sync_sched_status}\n"
        f"   └ Window: {schedule_sync_start_time} - {schedule_sync_end_time} {sync_run_status_text}\n\n"
        f"🚶‍♂️‍➡️ **Scheduled Follow List:** {follow_list_sched_status}\n"
        f"   └ Window: {schedule_follow_list_start_time} - {schedule_follow_list_end_time} {follow_list_run_status_text}\n\n"
        f"🕒 Current Bot Time: {now_local.strftime('%H:%M:%S')}"
    )
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)
    await resume_scraping()

async def switch_account_request(update: Update, account_num=None):
    """Manually switches account, pauses auto-follow, and loads new follow list."""
    global current_account, driver, is_periodic_follow_active, current_account_usernames_to_follow

    old_account_index = current_account # Store index

    # Determine new index
    new_account_index = -1 # Invalid start value
    if account_num is not None:
         if 0 <= account_num < len(ACCOUNTS):
              new_account_index = account_num
         else:
              await update.message.reply_text(f"❌ Invalid account number. Available: 1-{len(ACCOUNTS)}")
              await resume_scraping() # Resume, as the main handler paused
              return # Important: Abort here
    else: # If no number given, switch to the next
         new_account_index = (old_account_index + 1) % len(ACCOUNTS)

    # Only proceed if the account actually changes
    if new_account_index == old_account_index:
         await update.message.reply_text(f"ℹ️ Already on account {old_account_index+1}.")
         await resume_scraping() # Resume
         return

    # ===>  Pause auto-follow <===
    is_periodic_follow_active = False
    print("[Auto-Follow] Paused due to account switch.")

    old_account_username = ACCOUNTS[old_account_index].get("username", f"Index {old_account_index}")
    # ===> IMPORTANT: Update global index *before* calling get_current_account_username <===
    current_account = new_account_index # Update index for the rest of the script
    new_account_username = get_current_account_username() or f"Index {current_account}" # Get new username

    await update.message.reply_text(f"🔄 Switching from account @{old_account_username} to @{new_account_username}...\n"
                                     f"⏸️ Automatic following has been paused.")

    try:
        # IMPORTANT: Logout should be robust, even if current_account is already new
        await logout()
        if driver:
             try: driver.quit()
             except: pass # Ignore errors during closing
        driver = create_driver() # New driver for new account

        # Explicitly navigate to the login page
        driver.get("https://x.com/login")
        await asyncio.sleep(3)

        # Login with the new account (login() now uses the updated global `current_account`)
        result = await login()

        if result:
            await update.message.reply_text(f"✅ Successfully switched to account @{new_account_username}!")
            # ===>  Load the follow list for the NEW account <===
            load_current_account_follow_list() # Loads the list for the now active account
            # Navigate to timeline after successful login
            try:
                driver.get("https://x.com/home")
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except Exception as nav_home_err:
                 print(f"Warning: Could not navigate to /home after login: {nav_home_err}")
        else:
            await update.message.reply_text(f"❌ Login with account @{new_account_username} failed!")
            # Still load the (presumably empty) list for the new account
            load_current_account_follow_list()

    except Exception as e:
        await update.message.reply_text(f"❌ Error during account switch: {str(e)}")
        # Still try to load the list to have a defined state
        load_current_account_follow_list()

    # IMPORTANT: Scraping was paused by the main handler, resume here
    await resume_scraping()

async def account_request(update: Update):
    """Displays the current account"""
    await update.message.reply_text(f"🥷 Current Account: {current_account+1}")
    await resume_scraping()

async def reboot_request(update: Update):
    """Process reboot requests centrally"""
    await update.message.reply_text(f"♻️ R E B O O T")
    try:
        # Save counts before rebooting
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system reboot command
        os.system("sudo reboot")
    except Exception as reboot_error:
        await send_telegram_message(f"❌ Error during reboot: {str(reboot_error)}")
    await resume_scraping()

async def shutdown_request(update: Update):
    """Process shutdown requests centrally"""
    await update.message.reply_text(f"😴 SHUTDOWN")
    try:
        # Save counts before shutdown
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system shutdown command
        os.system("sudo shutdown now")
    except Exception as shutdown_error:
        await send_telegram_message(f"❌ Error during shutdown: {str(shutdown_error)}")
    await resume_scraping()

async def show_keywords(update: Update):
    """Displays the current keywords"""
    global KEYWORDS
    keywords_text = "\n".join([f"- {keyword}" for keyword in KEYWORDS])
    await update.message.reply_text(f"🔑 Current Keywords:\n{keywords_text}")
    await resume_scraping()

async def save_keywords():
    """Saves the keywords to a file"""
    with open(KEYWORDS_FILE, 'w') as f:
        json.dump(KEYWORDS, f)

async def add_keyword(update: Update, keyword_text: str):
    """Adds one or more comma-separated keywords"""
    global KEYWORDS
    keywords_to_add = [k.strip() for k in keyword_text.split(',') if k.strip()]
    added = []
    already_exists = []

    for keyword in keywords_to_add:
        if keyword in KEYWORDS:
            already_exists.append(keyword)
        else:
            KEYWORDS.append(keyword)
            added.append(keyword)

    await save_keywords()

    response = ""
    if added:
        response += f"✅ {len(added)} keywords added: {', '.join(added)}\n"
    if already_exists:
        response += f"⚠️ {len(already_exists)} keywords already exist: {', '.join(already_exists)}"

    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def remove_keyword(update: Update, keyword_text: str):
    """Removes one or more comma-separated keywords from the list"""
    global KEYWORDS
    keywords_to_remove = [k.strip() for k in keyword_text.split(',') if k.strip()]
    removed = []
    not_found = []

    for keyword in keywords_to_remove:
        if keyword in KEYWORDS:
            KEYWORDS.remove(keyword)
            removed.append(keyword)
        else:
            not_found.append(keyword)

    await save_keywords()

    response = ""
    if removed:
        response += f"🗑️ {len(removed)} keywords removed: {', '.join(removed)}\n"
    if not_found:
        response += f"⚠️ {len(not_found)} keywords not found: {', '.join(not_found)}"

    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def _send_long_message(application, chat_id, text, reply_markup, tweet_url):
    """
    Sends text messages, splitting them if necessary (> 4096 chars)
    and handles errors. Adds buttons only at the end.
    """
    global last_tweet_urls # Access global variable for post URLs

    message_limit = 4096
    try:
        if len(text) > message_limit:
            print(f"Message too long ({len(text)} > {message_limit}), splitting...")
            chunks = [text[i:i+message_limit] for i in range(0, len(text), message_limit)]
            message_sent = None # To track the last message
            for i, chunk in enumerate(chunks):
                # Add buttons only to the last chunk
                current_reply_markup = reply_markup if i == len(chunks) - 1 else None
                message_sent = await application.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=current_reply_markup
                )
                await asyncio.sleep(0.5) # Avoid rate limits

            # Store post URL if buttons were on the last message
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url

        else:
            # Message is short enough, send in one go
            message_sent = await application.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
             # Store post URL if buttons were present
            if reply_markup and tweet_url:
                last_tweet_urls[chat_id] = tweet_url

    except Exception as send_error:
        print(f"Error sending Telegram message (Text): {send_error}")
        # Fallback 1: Try sending without HTML parsing
        try:
            plain_text_fallback = html.unescape(text) # Try to remove HTML entities for plain text
            await application.bot.send_message(
                chat_id=chat_id,
                text=plain_text_fallback, # Send plain text
                parse_mode=None,
                disable_web_page_preview=True,
                reply_markup=reply_markup # Keep buttons if possible
            )
            print("Message sent successfully without HTML parsing.")
            # Store post URL here too if buttons were present
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url
        except Exception as plain_error:
            print(f"Sending without HTML parsing also failed: {plain_error}")
            # Fallback 2: Send a very short, simple message
            try:
                # Extract the first part of the original text as a hint
                error_indicator_text = text.split('\n')[0] # First line as a clue
                simple_text = error_indicator_text[:200] + "... [Error sending]"
                await application.bot.send_message(chat_id=chat_id, text=simple_text)
            except Exception as final_error:
                print(f"Final attempt to send simple error message failed: {final_error}")

async def send_telegram_message(text, images=None, tweet_url=None, reply_markup=None):
    """
    Sends a message to Telegram.
    If images are present AND the text is > 1024 characters long,
    a text message with a 🖼️ emoji is sent instead.
    """
    global application, last_tweet_urls # Access global variables

    try:
        if application is None:
            print("Warning: Telegram application is not yet initialized.")
            return

        # Prepare the base text (without emoji initially)
        full_text = f"{text}\n"
        text_length = len(full_text)

        # --- Button Logic ---
        # The reply markup is now passed directly from process_posts
        # if rating or like/repost buttons are needed.
        # We simply use the `reply_markup` we receive.
        final_reply_markup = reply_markup
        # --- End Button Logic ---

        await asyncio.sleep(0.5) # Reduce potential conflicts

        caption_limit = 1024
        image_emoji = "🖼️" # Emoji indicating images were present

        # Check if images should be sent
        if images:
            # Check length for caption
            if text_length <= caption_limit:
                # Case 1: Images present, text short enough -> Send with send_photo
                try:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=images[0], # Send only the first image
                        caption=full_text, # Entire text fits
                        parse_mode=ParseMode.HTML,
                        reply_markup=final_reply_markup
                    )
                    # Store post URL since image was sent and buttons might be present
                    if tweet_url:
                        last_tweet_urls[CHANNEL_ID] = tweet_url
                except Exception as send_photo_error:
                    print(f"Error sending photo (despite fitting length): {send_photo_error}")
                    # Fallback: Try sending as text message with emoji
                    modified_text = f"{image_emoji} {full_text}"
                    await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

            else:
                # Case 2: Images present, text too long for caption -> Send as text with emoji
                print(f"Message too long for caption ({text_length} > {caption_limit}), sending as text with emoji.")
                modified_text = f"{image_emoji} {full_text}" # Prepend emoji
                await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

        else:
            # Case 3: No images -> Send as normal text message
            await _send_long_message(application, CHANNEL_ID, full_text, final_reply_markup, tweet_url)

    except Exception as e:
        print(f"Unexpected error in send_telegram_message: {e}")

def detect_chain(contract):
    """Detect which blockchain a contract address belongs to"""
    if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', contract):
        return 'solana'
    elif re.match(r'^0x[a-fA-F0-9]{40}$', contract):
        return 'bsc'
    return 'unknown'

def get_contract_links(contract, chain):
    """Generate links for exploring a contract on various platforms"""
    links = [f""]

    if chain == 'solana':
            links.extend([
                f"<a href=\"https://neo.bullx.io/terminal?chainId=1399811149&address={contract}\">Bull✖️</a>\n",

                f"  <a href=\"https://rugcheck.xyz/tokens/{contract}#search\">RugCheck 🕵️‍♂️</a>\n",
                f"    <a href=\"https://dexscreener.com/solana/{contract}\">Dex Screener 🦅</a>\n",
                f"  <a href=\"https://pump.fun/coin/{contract}\">pumpfun 💊</a>\n",
                f"<a href=\"https://solscan.io/token/{contract}\">Solscan 📡</a>\n"
            ])
    elif chain == 'bsc':
        links.extend([
            f"<a href=\"https://gmgn.ai/bsc/token/sMF2eWcC_{contract}\">GMGN 🦖</a>\n",
            f"  <a href=\"https://four.meme/token/{contract}\">FOUR meme 🥦</a>\n",
            f"   <a href=\"https://dexscreener.com/bsc/{contract}\">Dex Screener 🦅</a>\n",
            f"  <a href=\"https://pancakeswap.finance/?outputCurrency={contract}&chainId=56&inputCurrency=BNB\">PancageSwap 🥞</a>\n",
            f"<a href=\"https://bscscan.com/address/{contract}\">BSC Scan 📡</a>\n"
        ])

    return ''.join(links)

def format_time(datetime_str):
    global max_tweet_age_minutes, USER_CONFIGURED_TIMEZONE, USER_TIMEZONE_STR # Access global settings
    is_recent = False # Default: Not recent
    formatted_string = "📅 Time invalid"

    # Use the globally configured timezone
    local_tz = USER_CONFIGURED_TIMEZONE
    if local_tz is None: # Should not happen if load_user_timezone() was called
        print("CRITICAL ERROR: USER_CONFIGURED_TIMEZONE is None in format_time. Defaulting to UTC.")
        local_tz = timezone.utc

    try:
        # Parse the UTC time from the post
        tweet_time_utc = datetime.fromisoformat(datetime_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
        # Convert to the user's local timezone
        tweet_time_local = tweet_time_utc.astimezone(local_tz)
        # Get the current time in the user's local timezone
        current_time_local = datetime.now(local_tz)
        # Calculate the difference
        time_diff = current_time_local - tweet_time_local
        seconds_ago = time_diff.total_seconds()

        # Determine if the post is recent (using configured max age)
        max_age_seconds = max_tweet_age_minutes * 60
        if 0 <= seconds_ago < max_age_seconds:
            is_recent = True
        # else: # Optional: Debug log if not recent
            # print(f"DEBUG format_time: Post IS NOT recent. Age: {seconds_ago/60:.1f} min (Max: {max_tweet_age_minutes} min)")

        # Determine date format based on the user's timezone string
        if USER_TIMEZONE_STR.startswith("Europe/"):
            # Format: DD.MM.YY HH:MM
            base_date_format = "%d.%m.%y"
        elif USER_TIMEZONE_STR.startswith("America/") or USER_TIMEZONE_STR.startswith("US/"):
            # Format: MM/DD/YY HH:MM
            base_date_format = "%m/%d/%y"
        else:
            # Default/Fallback format: YYYY-MM-DD HH:MM
            base_date_format = "%d.%m.%y"

        # current_display_format wird hier nicht mehr direkt für die Ausgabe verwendet,
        # aber base_date_format wird noch für den Datumsteil gebraucht.
        
        time_str_part = tweet_time_local.strftime('%H:%M')
        date_str_part = tweet_time_local.strftime(base_date_format)
        relative_str_part = ""

        if seconds_ago < 0:
            relative_str_part = "(Future?)"
        elif seconds_ago < 180: # Under 3 minutes
            relative_str_part = f"({int(seconds_ago)}s)"
        elif seconds_ago < 3600: # Under 1 hour
            minutes_ago = int(seconds_ago // 60)
            relative_str_part = f"({minutes_ago}m)"
        elif seconds_ago < 86400: # Under 1 day
            hours_ago = int(seconds_ago // 3600)
            relative_str_part = f"({hours_ago}h)"
        else: # Older than 1 day
            days_ago = int(seconds_ago // 86400)
            relative_str_part = f"({days_ago}d)"
        
        formatted_string = f"📅 {time_str_part}  {relative_str_part} {date_str_part}"
        
        # The explicit timezone name is no longer appended here.


    except ValueError:
        # Error parsing the datetime string
        pass # Keep default "invalid" message
    except Exception as e:
        print(f"ERROR in format_time for '{datetime_str}': {e}")
        # Keep default "invalid" message

    return formatted_string, is_recent


def format_token_info(tweet_text):
    """
    Extracts and formats Tickers ($) and Contract Addresses (CA)
    from a post text. Filters out pure currency amounts and amounts
    with K/M/B/T suffixes from tickers. Ticker extraction is conditional.
    """
    global search_tickers_enabled # Access the global setting

    # --- Ticker ($) Extraction and Cleanup (Conditional) ---
    ticker_section = "" # Initialize ticker_section to empty string

    # Check if ticker search should be performed
    if search_tickers_enabled:
        all_potential_tickers = [word for word in tweet_text.split() if word.startswith("$")]
        tickers = []
        punctuation_to_strip = '.,;:!?()&"\'+-/' # Remove punctuation at the end

        # Regex to recognize pure numeric amounts (optional with .,) and those with K/M/B/T
        currency_pattern = r"^[0-9][0-9,.]*([KkMmBbTt])?$"

        # Loop for processing potential tickers
        for potential_ticker in all_potential_tickers:
            cleaned = potential_ticker.rstrip(punctuation_to_strip)
            if len(cleaned) <= 1: continue
            value_part = cleaned[1:]
            if re.fullmatch(currency_pattern, value_part, re.IGNORECASE): continue
            tickers.append(cleaned)

        # Assign to ticker_section only if tickers were found
        if tickers:
            unique_tickers = sorted(list(set(tickers)))
            ticker_section = "\n💲 " + "".join(f"<code>{html.escape(ticker)}</code> " for ticker in unique_tickers).strip()

    # --- Contract Address (CA) Extraction and Formatting ---
    try:
        ca_matches = re.findall(TOKEN_PATTERN, tweet_text)
    except NameError:
        print("ERROR: TOKEN_PATTERN is not defined!")
        ca_matches = []

    filtered_ca_matches = []
    seen_tokens = set()

    for match in ca_matches:
        try:
            chain = detect_chain(match)
            if chain != 'unknown' and match not in seen_tokens:
                filtered_ca_matches.append(match)
                seen_tokens.add(match)
        except NameError:
            # Handle case where detect_chain might be missing during development/error
            if match not in seen_tokens:
                seen_tokens.add(match) # Still track to avoid duplicates if pattern matches
            continue # Skip chain/link processing if detect_chain fails

    contract_section = ""
    if filtered_ca_matches:
        contract_section += "\n📝 " # A blank line before
        for contract in filtered_ca_matches:
            try:
                chain = detect_chain(contract)
            except NameError:
                chain = "unknown" # Fallback if detect_chain is missing

            contract_section += f"<code>{html.escape(contract)}</code>\n"
            contract_section += f"🧬 {chain.upper()}\n"

            try:
                links_html = get_contract_links(contract, chain)
                if links_html:
                    contract_section += "\n" + links_html
            except NameError:
                # Function get_contract_links might be missing
                pass

            contract_section += "\n" # Additional blank line

    contract_section = contract_section.strip()

    # Return ticker section, contract section, and a flag indicating if tickers were found (and enabled)
    ticker_found_flag = bool(ticker_section) # True if ticker_section is not empty
    return ticker_section, contract_section, ticker_found_flag

# def format_token_info(post_text):
#     """
#     Extracts and formats Tickers ($) and Contract Addresses (CA)
#     from a post text. Filters out pure currency amounts and amounts
#     with K/M/B/T suffixes from tickers.
#     """

#     # --- Ticker ($) Extraction and Cleanup ---
#     all_potential_tickers = [word for word in post_text.split() if word.startswith("$")]
#     tickers = []
#     punctuation_to_strip = '.,;:!?()&"\'+-/' # Remove punctuation at the end

#     # Regex to recognize pure numeric amounts (optional with .,) and those with K/M/B/T
#     currency_pattern = r"^[0-9][0-9,.]*([KkMmBbTt])?$"

#     for potential_ticker in all_potential_tickers:
#         cleaned = potential_ticker.rstrip(punctuation_to_strip)
#         if len(cleaned) <= 1: continue
#         value_part = cleaned[1:]
#         if re.fullmatch(currency_pattern, value_part, re.IGNORECASE): continue
#         tickers.append(cleaned)

#     ticker_section = ""
#     if tickers:
#         unique_tickers = sorted(list(set(tickers)))
#         ticker_section = "\n💲 " + "".join(f"<code>{html.escape(ticker)}</code> " for ticker in unique_tickers).strip()
#     # --- End Conditional Ticker Logic ---

#     # --- Contract Address (CA) Extraction and Formatting ---
#     try: ca_matches = re.findall(TOKEN_PATTERN, post_text)
#     except NameError: print("ERROR: TOKEN_PATTERN is not defined!"); ca_matches = []

#     filtered_ca_matches = []; seen_tokens = set()
#     for match in ca_matches:
#         try: chain = detect_chain(match)
#         except NameError: chain = "unknown"
#         if chain != 'unknown' and match not in seen_tokens:
#             filtered_ca_matches.append(match); seen_tokens.add(match)
#         elif match not in seen_tokens: seen_tokens.add(match) # Add unknown CAs to seen to avoid duplicates

#     contract_section = ""
#     if filtered_ca_matches:
#         contract_section += "\n📝 "
#         for contract in filtered_ca_matches:
#             try: chain = detect_chain(contract)
#             except NameError: chain = "unknown"
#             contract_section += f"<code>{html.escape(contract)}</code>\n"
#             contract_section += f"🧬 {chain.upper()}\n"
#             try: links_html = get_contract_links(contract, chain)
#             except NameError: links_html = ""
#             if links_html: contract_section += "\n" + links_html
#             contract_section += "\n"
#     contract_section = contract_section.strip()

#     return ticker_section, contract_section

async def process_tweets():
    """
    Process posts in the timeline. Optimized to only search when necessary,
    scrolls down, and processes posts *immediately* upon finding them
    to avoid StaleElementReferenceExceptions. INCLUDES ENHANCED DEBUG LOGGING FOR AUTHOR EXTRACTION.
    """
    global driver, is_scraping_paused, first_run, processed_tweets, KEYWORDS, TOKEN_PATTERN, search_mode, ratings_data

    if is_scraping_paused: return

    try:
        button_tweet_count = await check_new_tweets_button()
        should_search_and_process = first_run or button_tweet_count > 0

        if not should_search_and_process:
            return

        print("Searching and processing new posts (with scrolling)...") # Translated
        target_new_button_tweets = button_tweet_count
        processed_in_this_round = set()
        newly_processed_count = 0
        #  Counter for posts processed since the button click
        processed_since_button_click = 0
        max_scroll_attempts = 20 # Keep safety limit
        scroll_attempt = 0
        consecutive_scrolls_without_new = 0
        max_consecutive_scrolls_without_new = 3 # Keep fallback limit
        target_met_flag = False # Flag to break outer loop when target is met

        # The loop runs until the target is reached OR fallbacks trigger
        while scroll_attempt < max_scroll_attempts:
            # The primary break condition is checked *inside* the loop
            scroll_attempt += 1
            found_in_this_scroll = 0
            current_containers = []
            try:
                current_containers = driver.find_elements(By.XPATH, '//article[@data-testid="tweet"]')
                if not current_containers:
                    print(f"Scroll attempt {scroll_attempt}/{max_scroll_attempts}: No post containers found.") # Translated
                    await asyncio.sleep(1)
                    continue
            except Exception as e_find:
                 print(f"Error finding post containers (Scroll loop {scroll_attempt}): {e_find}") # Translated
                 break

            print(f"Scroll attempt {scroll_attempt}/{max_scroll_attempts}: {len(current_containers)} containers found. Processing new ones...") # Translated

            for container in current_containers:
                tweet_id = None
                tweet_url = None
                # === 1. Extract ID and URL ===
                try:
                    link_element = WebDriverWait(container, 0.5).until(
                        EC.presence_of_element_located((By.XPATH, './/a[contains(@href, "/status/")]'))
                    )
                    tweet_url = link_element.get_attribute('href')
                    tweet_id_match = re.search(r'/status/(\d+)', tweet_url)
                    if tweet_id_match:
                        tweet_id = tweet_id_match.group(1)
                    else:
                        continue
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                    continue
                except Exception as e_id_extract:
                    print(f"WARNING: Unexpected error extracting ID/URL: {e_id_extract}") # Translated
                    continue

                # === 2. Check if already processed ===
                if tweet_id in processed_tweets or tweet_id in processed_in_this_round:
                    continue


                # === TARGET COUNTING AND CHECK ===
                # This is a NEW post for this round. Increment counter if target exists.
                if target_new_button_tweets > 0:
                    processed_since_button_click += 1
                    print(f"    (Target Counter: {processed_since_button_click}/{target_new_button_tweets})") # Optional debug log

                    # Check if target is met *before* processing details
                    if processed_since_button_click > target_new_button_tweets:
                         # This handles cases where the button count was slightly off
                         # or we already processed enough in previous iterations of the *outer* loop.
                         print(f"Target of {target_new_button_tweets} already met or exceeded. Setting flag and breaking inner loop.")
                         target_met_flag = True
                         break # Exit the inner 'for' loop

                    # If exactly the target number is reached now, process this one last post
                    # and then set the flag to break the outer loop afterwards.
                    if processed_since_button_click == target_new_button_tweets:
                        print(f"Target of {target_new_button_tweets} will be met after processing this post.")
                        # We don't break here yet, process this post first.
                        # The flag will be checked after the inner loop.
                        target_met_flag = True # Set flag to break outer loop *after* this inner loop finishes

                # === 3. Process post IMMEDIATELY ===
                print(f"  -> Processing new post: {tweet_id}") # Translated
                increment_scanned_count()

                process_success = False # Will be set to True later if processing succeeds
                try:
                    # --- Ad Check ---
                    is_ad = False
                    try:
                        # Search directly, without waiting
                        ad_indicators = container.find_elements(By.XPATH, './/span[text()="Ad" or text()="Anzeige"]') # Keep "Anzeige" for German UI? Or remove? Let's remove for consistency.
                        # ad_indicators = container.find_elements(By.XPATH, './/span[text()="Ad"]') # English only
                        # If the list is not empty, an indicator was found
                        if ad_indicators:
                            is_ad = True
                    except (NoSuchElementException, StaleElementReferenceException): pass # Catch errors during search itself

                    if is_ad:
                        print(f"    post {tweet_id} is an ad -> skipping") # Translated
                        increment_ad_total_count()
                        processed_in_this_round.add(tweet_id)
                        processed_tweets.append(tweet_id)
                        found_in_this_scroll += 1
                        continue

                    # --- Repost Check ---
                    is_repost = False; repost_text = ""
                    try:
                        # Search directly, without waiting
                        sc_elements = container.find_elements(By.XPATH, './/span[@data-testid="socialContext"]')
                        # If the list is not empty, the element was found
                        if sc_elements:
                            repost_text = sc_elements[0].text.strip() # Take the text of the first found element
                            is_repost = bool(repost_text) # Check if text is present
                            if is_repost: print(f"    Repost found: {repost_text}") # Translated
                    except (NoSuchElementException, StaleElementReferenceException): pass # Catch errors during search itself

                    # --- Time ---
                    datetime_str = ""; time_str = "📅 Time Unknown"; tweet_is_recent = False # Translated
                    try:
                        te = WebDriverWait(container, 0.3).until(EC.presence_of_element_located((By.XPATH, './/time[@datetime]'))) # <-- Timeout reduced
                        datetime_str = te.get_attribute('datetime')
                        if datetime_str: time_str, tweet_is_recent = format_time(datetime_str)
                    except (TimeoutException, NoSuchElementException, StaleElementReferenceException): pass

                    # --- Skip if older than 15 minutes (strict check) ---
                    # This check now uses the updated 'is_recent' which is True only if < max_tweet_age_minutes
                    if not tweet_is_recent:
                        # Use the global variable in the log message
                        print(f"    post {tweet_id} skipped: Too old ({time_str} > {max_tweet_age_minutes} min)")
                        # Mark as processed so we don't check it again in this round
                        processed_in_this_round.add(tweet_id)
                        processed_tweets.append(tweet_id)
                        # IMPORTANT: Do not increment processed_since_button_click here
                        # because this post doesn't count towards the button's target.
                        found_in_this_scroll += 1 # Still counts as found in this scroll view for the scroll logic
                        continue # Skip to the next container immediately


                    # --- Variables for both authors (reposter and original) ---
                    author_name = "Unknown"  # Original author
                    author_handle = "@unknown"  # Original author
                    reposter_name = None  # Reposter (only for reposts)
                    reposter_handle = None  # Reposter (only for reposts)


                    # --- Reposter Extraction (only if is_repost is True) ---
                    if is_repost:
                        try:
                            # Extract reposter info from the social context
                            sc_element = WebDriverWait(container, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, './/span[@data-testid="socialContext"]'))
                            )

                            # Case where we can find the link directly
                            try:
                                # The social context link contains the reposter
                                reposter_link = sc_element.find_element(By.XPATH, './/a[contains(@href, "/")]')
                                reposter_href = reposter_link.get_attribute('href')

                                # Extract the handle from the href
                                if reposter_href:
                                    raw_handle = reposter_href.split('/')[-1]
                                    if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                        reposter_handle = "@" + raw_handle
                                    else:
                                        reposter_handle = "@unknown" # Translated

                                # Extract the name from the link text
                                full_text = reposter_link.text.strip()
                                if " reposted" in full_text.lower():
                                    reposter_name = full_text.lower().split(" reposted")[0].strip()
                                else:
                                    reposter_name = full_text.strip()

                                print(f"    Reposter extracted (direct): Name='{reposter_name}', Handle='{reposter_handle}'") # Translated
                            except (NoSuchElementException, StaleElementReferenceException):
                                print("    DEBUG REPOST: Direct reposter link not found. Trying alternative method.") # Translated

                                # Alternative: Extract the text and try to get the name
                                full_context_text = sc_element.text.strip()
                                if " reposted" in full_context_text.lower():
                                    reposter_name = full_context_text.lower().split(" reposted")[0].strip()
                                else:
                                    reposter_name = "Unknown" # Translated

                                # Try another alternative XPath for the link
                                try:
                                    alt_reposter_link = container.find_element(By.XPATH,
                                        './/div[1]/div/div/div/div/div[2]/div/div/div/a[contains(@href, "/")]')
                                    alt_href = alt_reposter_link.get_attribute('href')
                                    if alt_href:
                                        raw_handle = alt_href.split('/')[-1]
                                        if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                            reposter_handle = "@" + raw_handle
                                            print(f"    Reposter Handle found with alternative method: {reposter_handle}") # Translated
                                except (NoSuchElementException, StaleElementReferenceException):
                                    print("    DEBUG REPOST: Alternative reposter link also not found.") # Translated
                                    reposter_handle = "@unknown" # Translated

                                print(f"    Reposter extracted (alternative): Name='{reposter_name}', Handle='{reposter_handle}'") # Translated

                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            print("    WARNING: Could not find socialContext for repost.") # Translated
                            reposter_name = "Unknown" # Translated
                            reposter_handle = "@unknown" # Translated
                        except Exception as e_repost_context:
                            print(f"    WARNING: Error extracting repost context: {e_repost_context}") # Translated
                            reposter_name = "Unknown" # Translated
                            reposter_handle = "@unknown" # Translated
                    # --- Original Author Extraction (for every post) ---
                    try:
                        # Extract original author (User-Name) - always present
                        try:
                            nc_id = "User-Name"
                            nc = WebDriverWait(container, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, f'.//div[@data-testid="{nc_id}"]'))
                            )

                            # Extract author link (Handle) from User-Name
                            try:
                                user_link = nc.find_element(By.XPATH, './/a[contains(@href, "/")]')
                                user_href = user_link.get_attribute('href')
                                if user_href:
                                    raw_handle = user_href.split('/')[-1]
                                    if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                        author_handle = "@" + raw_handle
                            except (NoSuchElementException, StaleElementReferenceException):
                                print("    WARNING: Could not find original author link.") # Translated

                            # Extract name from the first span (or fallback to whole text)
                            try:
                                name_span = nc.find_element(By.XPATH, './/span[1]')
                                temp_name = name_span.text.strip()
                                author_name = temp_name if temp_name and not temp_name.startswith('@') else nc.text.strip()
                            except (NoSuchElementException, StaleElementReferenceException):
                                author_name = nc.text.strip()

                            # Remove handle from name if present
                            if author_handle != "@unknown" and author_handle in author_name: # Translated
                                author_name = author_name.replace(author_handle, '').strip()

                            # Fallback for empty name
                            if not author_name or author_name == author_handle:
                                author_name = f"Unknown ({author_handle})" # Translated

                            print(f"    Original author extracted: {author_name} ({author_handle})") # Translated
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            print("    WARNING: Could not find original author.") # Translated
                            author_name = f"Unknown ({author_handle})" # Translated
                    except Exception as e_auth:
                        print(f"    WARNING: Author extraction error: {e_auth}") # Translated
                        author_name = f"Unknown ({author_handle})" # Translated

                    # --- Content ---
                    tweet_content = "[Content not found]" # Translated
                    try:
                        tt = WebDriverWait(container, 1).until(EC.presence_of_element_located((By.XPATH, './/div[@data-testid="tweetText"]')))
                        tweet_content = tt.text
                    except (TimeoutException, StaleElementReferenceException): print(f"    WARNING: Content not found for {tweet_id}.") # Translated

                    # --- Images ---
                    image_urls = []
                    try:
                        pds = container.find_elements(By.XPATH, './/div[@data-testid="tweetPhoto"]')
                        for div in pds:
                            try:
                                imgs = div.find_elements(By.XPATH, './/img[@alt="Image"]')
                                for img in imgs:
                                    try: src = img.get_attribute('src'); image_urls.append(src) if src and 'profile_images' not in src and 'emoji' not in src else None
                                    except StaleElementReferenceException: pass
                            except StaleElementReferenceException: pass
                        image_urls = list(set(image_urls))
                    except StaleElementReferenceException: pass

                    # --- Relevance Check and Sending ---
                    # format_token_info now returns: ticker_section, contract_section, ticker_actually_found_and_enabled
                    # ticker_found from format_token_info already considers if search_tickers_enabled is True
                    ticker_section, contract_section, ticker_found_and_enabled = format_token_info(tweet_content)
                    
                    contains_keyword_match = any(keyword.lower() in tweet_content.lower() for keyword in KEYWORDS)
                    contains_ca_match = bool(contract_section) # True if a CA was found and formatted

                    is_relevant = False
                    reasons = []

                    if search_keywords_enabled and contains_keyword_match:
                        is_relevant = True
                        # Add specific found keywords to reasons
                        found_kws_in_post = [kw for kw in KEYWORDS if kw.lower() in tweet_content.lower()]
                        if found_kws_in_post: reasons.extend(found_kws_in_post)
                    
                    if search_ca_enabled and contains_ca_match:
                        is_relevant = True
                        reasons.append("CA") # Generic "CA" reason if CA search is on and a CA is present
                        
                    # ticker_found_and_enabled is already true if search_tickers_enabled AND a ticker was found
                    if ticker_found_and_enabled: 
                        is_relevant = True
                        reasons.append("Ticker")

                    if is_relevant:
                        # reasons list is already built above
                        
                        reasons = sorted(list(set(reasons))) # Final list of reasons for display
                        
                        # Log the reasons
                        if reasons:
                            print(f"    post {tweet_id} relevant due to: {', '.join(sorted(list(set(reasons))))}")
                        else:
                            # This case should ideally not happen if is_relevant is True,
                            # but as a fallback or for debugging:
                            print(f"    post {tweet_id} marked relevant, but no specific reason (CA/Keyword/Ticker) captured for logging. Check logic.")
                        
                        increment_found_count()

                        # --- Build message (corrected version) ---
                        handle_for_command = author_handle.lstrip('@')

                        # --- Get rating info ---
                        rating_display = ""
                        if author_handle in ratings_data:
                            rating_info = ratings_data[author_handle].get("ratings", {})
                            if isinstance(rating_info, dict):
                                total_ratings = 0
                                weighted_sum = 0
                                for star_str, count in rating_info.items():
                                    try:
                                        star = int(star_str)
                                        if 1 <= star <= 5:
                                            total_ratings += count
                                            weighted_sum += star * count
                                    except ValueError: continue
                                if total_ratings > 0:
                                    average_rating = weighted_sum / total_ratings
                                    rating_display = f" {average_rating:.1f}⭐({total_ratings})" # Space at the beginning
                        # --- End rating info ---

                        user_info_line = f"👤 <b><a href=\"https://x.com/{html.escape(author_handle.lstrip('@'))}\">{html.escape(author_name)}</a></b> (<code><i>{html.escape(author_handle)}</i></code>){rating_display}" # Rating appended here
                        message_parts = [user_info_line]

                        # --- Build message (for repost) ---
                        if is_repost:
                            # Use reposter_name and reposter_handle for repost info
                            reposter_handle_for_command = reposter_handle.lstrip('@') if reposter_handle and reposter_handle != "@unknown" else "unknown" # Translated
                            repost_info = f"🔄 <b><a href=\"https://x.com/{html.escape(reposter_handle_for_command.lstrip('@'))}\">{html.escape(reposter_name or 'Unknown')}</a></b> (<code><i>{html.escape(reposter_handle or '@unknown')}</i></code>) reposted" # Translated
                            message_parts.append(repost_info)

                        message_parts.append(f"<blockquote>{html.escape(tweet_content)}</blockquote>")
                        message_parts.append(f"<b>{time_str}</b>")
                        message_parts.append(f"🌐 <a href='{tweet_url}'>Post Link</a>") # Translated
                        if ticker_section: message_parts.append(ticker_section)
                        if contract_section: message_parts.append(contract_section)
                        if reasons: message_parts.append(f"💎 {', '.join(sorted(list(set(reasons))))} 💎")
                        final_message = "\n".join(message_parts)
                        # --- End build message ---

                        # --- Check for "Show more" ---
                        show_more_present = False
                        try:
                            # Search for the "Show more" link/span *within* the postText div
                            tweet_text_div = container.find_element(By.XPATH, './/div[@data-testid="tweetText"]')
                            # Check various possible texts/elements for "Show more"
                            WebDriverWait(tweet_text_div, 0.2).until(
                                EC.presence_of_element_located((By.XPATH, './/span[text()="Show more"] | .//a[contains(@href, "/status/") and contains(text(), "Show more")] | .//button[contains(., "Show more")]'))
                            )
                            show_more_present = True
                            print(f"    'Show more' indicator found for post {tweet_id}") # Translated
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            pass # No "Show more" found
                        except Exception as e_show_more:
                            print(f"    WARNING: Error checking for 'Show more': {e_show_more}") # Translated

                        show_full_text_needed = show_more_present
                        # --- End Check for "Show more" ---

                        # --- Build Markup ---
                        final_reply_markup = None
                        combined_keyboard = []  # Initialize this once, at the beginning

                        # Rating Buttons (only if CA search enabled AND CA present)
                        if search_ca_enabled and contains_ca_match:
                            source_key = author_handle
                            try:
                                author_name_str = str(author_name) if author_name else source_key
                                encoded_name = base64.urlsafe_b64encode(author_name_str.encode()).decode()
                            except Exception as enc_err:
                                logger.warning(f"Name encoding failed for {source_key}: {enc_err}. Using key.")
                                encoded_name = base64.urlsafe_b64encode(source_key.encode()).decode()
                            # Rating Buttons
                            rating_buttons_row = [InlineKeyboardButton(str(i)+"⭐", callback_data=f"rate:{i}:{source_key}:{encoded_name}") for i in range(1, 6)]
                            combined_keyboard.append(rating_buttons_row)

                        # Like/Repost/FullText Buttons
                        action_buttons = []
                        if tweet_id:  # tweet_id should always be present
                            action_buttons.append(InlineKeyboardButton("👍 Like", callback_data=f"like:{tweet_id}"))
                            action_buttons.append(InlineKeyboardButton("🔄 Repost", callback_data=f"repost:{tweet_id}"))

                            # Add "Show Full Text" button if needed
                            if show_full_text_needed:
                                print(f"    Adding 'Show Full Text' button forpost{tweet_id}") # Translated
                                action_buttons.append(InlineKeyboardButton("📄 Full Text", callback_data=f"full:{tweet_id}")) # Translated

                        # Important: Add the action buttons anyway if they exist
                        if action_buttons:
                            combined_keyboard.append(action_buttons)

                        if combined_keyboard:
                            final_reply_markup = InlineKeyboardMarkup(combined_keyboard)
                        # --- End Build Markup ---

                        # --- Update names in DB (Original Author) ---
                        # This block is empty in your code. If you had logic here,
                        # it needs to be correctly indented. If it should remain empty,
                        # 'pass' is good practice.
                        if search_ca_enabled and contains_ca_match:
                            pass # Add 'pass' if the block is intentionally empty

                        # --- Send (ALWAYS if is_relevant) ---
                        # Correctly indented, one level deeper than 'if is_relevant:',
                        # but at the same level as 'if contains_token:'
                        # +++ DEBUG LOGGING (before sending) +++
                        logger.debug(f"post {tweet_id}: final_reply_markup before sending: {'Set' if final_reply_markup else 'None'}") # Translated
                        if final_reply_markup:
                            # Try to safely output the structure
                            try:
                                keyboard_repr = repr(final_reply_markup.inline_keyboard)
                                logger.debug(f"post {tweet_id}: Keyboard structure: {keyboard_repr[:500]}...") # Shortened for readability
                            except Exception as log_e:
                                logger.error(f"Error logging keyboard structure: {log_e}") # Translated
                        # +++ END DEBUG LOGGING +++

                        # The original send line follows here:
                        await send_telegram_message(final_message, image_urls, tweet_url, reply_markup=final_reply_markup)
                        process_success = True # Belongs to sending

                    else: # Belongs to 'if is_relevant:'
                        print(f"    post {tweet_id} skipped (no keywords/token)") # Translated
                        process_success = True # Belongs to skipping

                except StaleElementReferenceException:
                    print(f"    WARNING: Stale Element during detail processing of {tweet_id}. Skipping.") # Translated
                    process_success = False
                except Exception as e_process:
                    print(f"    !!!!!!!! ERROR during detail processing of post {tweet_id} !!!!!!!! : {e_process}") # Translated
                    logger.error(f"Error processing details for post {tweet_id}", exc_info=True)
                    process_success = False

                # === 4. Mark as processed ===
                processed_in_this_round.add(tweet_id)
                processed_tweets.append(tweet_id)
                found_in_this_scroll += 1
                if process_success:
                    newly_processed_count += 1

                # === TARGET CHECK ===
                # Increment the counter *only* if the post was identified as new in this round
                # (i.e., not skipped by the initial ID check)
                # AND check if the target has been reached.
                # This check happens *after* processing/skipping the current tweet.
                if target_new_button_tweets > 0: # Only check if a target exists (button was clicked)
                    # Increment counter for tweets processed towards the target
                    # Note: We increment 'processed_since_button_click' here, AFTER potentially skipping old/ad tweets
                    # This ensures we count towards the target only potentially relevant new tweets.
                    # If you want to count *every* single new post encountered, move this increment
                    # right after the "if tweet_id in processed_tweets..." check.
                    # Let's stick to counting potentially relevant ones for now:
                    # processed_since_button_click += 1 # Moved this counter logic earlier

                    if processed_since_button_click >= target_new_button_tweets:
                        print(f"Target of {target_new_button_tweets} new posts reached after processing {tweet_id}. Stopping inner loop.")
                        break # Exit the inner 'for container...' loop immediately

                print("    ____________________________________")

            # --- End of loop over current containers ---


            # --- Check if the target was met inside the inner loop ---
            if target_met_flag:
                print("Target met flag is True. Breaking outer scroll loop.")
                break # Exit the outer 'while' loop

            # --- Check fallback break condition (no new posts in viewport) ---
            if found_in_this_scroll == 0:
                consecutive_scrolls_without_new += 1
                print(f"Scroll attempt {scroll_attempt}: No *new* tweets found in this round (Streak: {consecutive_scrolls_without_new}/{max_consecutive_scrolls_without_new}).") # Translated
                if consecutive_scrolls_without_new >= max_consecutive_scrolls_without_new:
                    print("Stopping scrolling (Fallback), as no new posts were found in the visible area.") # Translated
                    break # Exit loop
            else:
                consecutive_scrolls_without_new = 0 # Reset if new ones were found

            # --- Check fallback break condition (maximum scroll attempts) ---
            if scroll_attempt >= max_scroll_attempts:
                print(f"Maximum number of {max_scroll_attempts} scroll attempts reached. Stopping scrolling.") # Translated
                break # Exit loop

            # --- Only scroll if not already broken ---
            print(f"Scrolling down for attempt {scroll_attempt + 1}...") # Translated
            try:
                # ===> CHANGED: Increased scroll multiplier due to zoom <===
                driver.execute_script("window.scrollBy(0, window.innerHeight * 1.5);")
                # ===> END CHANGE <===
                await asyncio.sleep(random.uniform(0.2, 0.5)) # Keep the pause after scrolling
            except Exception as scroll_err:
                print(f"Error during scrolling: {scroll_err}. Breaking scroll loop.") # Translated
                break
        # --- End of while scroll loop ---

        if first_run and newly_processed_count > 0:
            first_run = False
            print("First scan round completed, switching to optimized mode") # Translated

        print(f"Processing round completed. {processed_since_button_click} posts processed since button click (target was {target_new_button_tweets}). {newly_processed_count} posts successfully processed/skipped in total.") # Translated

    except Exception as e_outer:
        print(f"!!!!!!!! CRITICAL ERROR in process_posts !!!!!!!! : {e_outer}") # Translated
        logger.error("Unhandled exception in process_posts", exc_info=True)

async def process_full_text_request(query, tweet_id):
    try:
        # Notify user that we're processing
        await query.answer("Fetching full text...")

        # Generate the post URL from the ID
        tweet_url = f"https://twitter.com/i/status/{tweet_id}"

        # Navigate to the post with your existing driver
        driver.get(tweet_url)
        await asyncio.sleep(3)  # Wait for page to load

        # Find the post text
        tweet_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//div[@data-testid="tweetText"]'))
        )

        # Check if there's a "show more" button and click it
        try:
            show_more_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, './/button[@data-testid="tweet-text-show-more-link"]'))
            )
            show_more_button.click()
            await asyncio.sleep(1)  # Give time for expansion
        except (TimeoutException, NoSuchElementException):
            pass  # No show more button, or already expanded
        
        if result:
            await update.message.reply_text(f"✅ Successfully switched to account @{new_account_username}!")
            # ===>  Load the follow list for the NEW account <===
            load_current_account_follow_list() # Loads the list for the now active account
            # Navigate to timeline after successful login
            try:
                driver.get("https://x.com/home")
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except Exception as nav_home_err:
                 print(f"Warning: Could not navigate to /home after login: {nav_home_err}")
        else:
            await update.message.reply_text(f"❌ Login with account @{new_account_username} failed!")
            # Still load the (presumably empty) list for the new account
            load_current_account_follow_list()

    except Exception as e:
        await update.message.reply_text(f"❌ Error during account switch: {str(e)}")
        # Still try to load the list to have a defined state
        load_current_account_follow_list()

    # IMPORTANT: Scraping was paused by the main handler, resume here
    await resume_scraping()

async def account_request(update: Update):
    """Displays the current account"""
    await update.message.reply_text(f"🥷 Current Account: {current_account+1}")
    await resume_scraping()

async def reboot_request(update: Update):
    """Process reboot requests centrally"""
    await update.message.reply_text(f"♻️ R E B O O T")
    try:
        # Save counts before rebooting
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system reboot command
        os.system("sudo reboot")
    except Exception as reboot_error:
        await send_telegram_message(f"❌ Error during reboot: {str(reboot_error)}")
    await resume_scraping()

async def shutdown_request(update: Update):
    """Process shutdown requests centrally"""
    await update.message.reply_text(f"😴 SHUTDOWN")
    try:
        # Save counts before shutdown
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system shutdown command
        os.system("sudo shutdown now")
    except Exception as shutdown_error:
        await send_telegram_message(f"❌ Error during shutdown: {str(shutdown_error)}")
    await resume_scraping()

async def show_keywords(update: Update):
    """Displays the current keywords"""
    global KEYWORDS
    keywords_text = "\n".join([f"- {keyword}" for keyword in KEYWORDS])
    await update.message.reply_text(f"🔑 Current Keywords:\n{keywords_text}")
    await resume_scraping()

async def save_keywords():
    """Saves the keywords to a file"""
    with open(KEYWORDS_FILE, 'w') as f:
        json.dump(KEYWORDS, f)

async def add_keyword(update: Update, keyword_text: str):
    """Adds one or more comma-separated keywords"""
    global KEYWORDS
    keywords_to_add = [k.strip() for k in keyword_text.split(',') if k.strip()]
    added = []
    already_exists = []

    for keyword in keywords_to_add:
        if keyword in KEYWORDS:
            already_exists.append(keyword)
        else:
            KEYWORDS.append(keyword)
            added.append(keyword)

    await save_keywords()

    response = ""
    if added:
        response += f"✅ {len(added)} keywords added: {', '.join(added)}\n"
    if already_exists:
        response += f"⚠️ {len(already_exists)} keywords already exist: {', '.join(already_exists)}"

    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def remove_keyword(update: Update, keyword_text: str):
    """Removes one or more comma-separated keywords from the list"""
    global KEYWORDS
    keywords_to_remove = [k.strip() for k in keyword_text.split(',') if k.strip()]
    removed = []
    not_found = []

    for keyword in keywords_to_remove:
        if keyword in KEYWORDS:
            KEYWORDS.remove(keyword)
            removed.append(keyword)
        else:
            not_found.append(keyword)

    await save_keywords()

    response = ""
    if removed:
        response += f"🗑️ {len(removed)} keywords removed: {', '.join(removed)}\n"
    if not_found:
        response += f"⚠️ {len(not_found)} keywords not found: {', '.join(not_found)}"

    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def _send_long_message(application, chat_id, text, reply_markup, tweet_url):
    """
    Sends text messages, splitting them if necessary (> 4096 chars)
    and handles errors. Adds buttons only at the end.
    """
    global last_tweet_urls # Access global variable for post URLs

    message_limit = 4096
    try:
        if len(text) > message_limit:
            print(f"Message too long ({len(text)} > {message_limit}), splitting...")
            chunks = [text[i:i+message_limit] for i in range(0, len(text), message_limit)]
            message_sent = None # To track the last message
            for i, chunk in enumerate(chunks):
                # Add buttons only to the last chunk
                current_reply_markup = reply_markup if i == len(chunks) - 1 else None
                message_sent = await application.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=current_reply_markup
                )
                await asyncio.sleep(0.5) # Avoid rate limits

            # Store post URL if buttons were on the last message
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url

        else:
            # Message is short enough, send in one go
            message_sent = await application.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
             # Store post URL if buttons were present
            if reply_markup and tweet_url:
                last_tweet_urls[chat_id] = tweet_url

    except Exception as send_error:
        print(f"Error sending Telegram message (Text): {send_error}")
        # Fallback 1: Try sending without HTML parsing
        try:
            plain_text_fallback = html.unescape(text) # Try to remove HTML entities for plain text
            await application.bot.send_message(
                chat_id=chat_id,
                text=plain_text_fallback, # Send plain text
                parse_mode=None,
                disable_web_page_preview=True,
                reply_markup=reply_markup # Keep buttons if possible
            )
            print("Message sent successfully without HTML parsing.")
            # Store post URL here too if buttons were present
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url
        except Exception as plain_error:
            print(f"Sending without HTML parsing also failed: {plain_error}")
            # Fallback 2: Send a very short, simple message
            try:
                # Extract the first part of the original text as a hint
                error_indicator_text = text.split('\n')[0] # First line as a clue
                simple_text = error_indicator_text[:200] + "... [Error sending]"
                await application.bot.send_message(chat_id=chat_id, text=simple_text)
            except Exception as final_error:
                print(f"Final attempt to send simple error message failed: {final_error}")

async def send_telegram_message(text, images=None, tweet_url=None, reply_markup=None):
    """
    Sends a message to Telegram.
    If images are present AND the text is > 1024 characters long,
    a text message with a 🖼️ emoji is sent instead.
    """
    global application, last_tweet_urls # Access global variables

    try:
        if application is None:
            print("Warning: Telegram application is not yet initialized.")
            return

        # Prepare the base text (without emoji initially)
        full_text = f"{text}\n"
        text_length = len(full_text)

        # --- Button Logic ---
        # The reply markup is now passed directly from process_posts
        # if rating or like/repost buttons are needed.
        # We simply use the `reply_markup` we receive.
        final_reply_markup = reply_markup
        # --- End Button Logic ---

        await asyncio.sleep(0.5) # Reduce potential conflicts

        caption_limit = 1024
        image_emoji = "🖼️" # Emoji indicating images were present

        # Check if images should be sent
        if images:
            # Check length for caption
            if text_length <= caption_limit:
                # Case 1: Images present, text short enough -> Send with send_photo
                try:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=images[0], # Send only the first image
                        caption=full_text, # Entire text fits
                        parse_mode=ParseMode.HTML,
                        reply_markup=final_reply_markup
                    )
                    # Store post URL since image was sent and buttons might be present
                    if tweet_url:
                        last_tweet_urls[CHANNEL_ID] = tweet_url
                except Exception as send_photo_error:
                    print(f"Error sending photo (despite fitting length): {send_photo_error}")
                    # Fallback: Try sending as text message with emoji
                    modified_text = f"{image_emoji} {full_text}"
                    await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

            else:
                # Case 2: Images present, text too long for caption -> Send as text with emoji
                print(f"Message too long for caption ({text_length} > {caption_limit}), sending as text with emoji.")
                modified_text = f"{image_emoji} {full_text}" # Prepend emoji
                await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

        else:
            # Case 3: No images -> Send as normal text message
            await _send_long_message(application, CHANNEL_ID, full_text, final_reply_markup, tweet_url)

    except Exception as e:
        print(f"Unexpected error in send_telegram_message: {e}")

def detect_chain(contract):
    """Detect which blockchain a contract address belongs to"""
    if re.match(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$', contract):
        return 'solana'
    elif re.match(r'^0x[a-fA-F0-9]{40}$', contract):
        return 'bsc'
    return 'unknown'

def get_contract_links(contract, chain):
    """Generate links for exploring a contract on various platforms"""
    links = [f""]

    if chain == 'solana':
            links.extend([
                f"<a href=\"https://neo.bullx.io/terminal?chainId=1399811149&address={contract}\">Bull✖️</a>\n",

                f"  <a href=\"https://rugcheck.xyz/tokens/{contract}#search\">RugCheck 🕵️‍♂️</a>\n",
                f"    <a href=\"https://dexscreener.com/solana/{contract}\">Dex Screener 🦅</a>\n",
                f"  <a href=\"https://pump.fun/coin/{contract}\">pumpfun 💊</a>\n",
                f"<a href=\"https://solscan.io/token/{contract}\">Solscan 📡</a>\n"
            ])
    elif chain == 'bsc':
        links.extend([
            f"<a href=\"https://gmgn.ai/bsc/token/sMF2eWcC_{contract}\">GMGN 🦖</a>\n",
            f"  <a href=\"https://four.meme/token/{contract}\">FOUR meme 🥦</a>\n",
            f"   <a href=\"https://dexscreener.com/bsc/{contract}\">Dex Screener 🦅</a>\n",
            f"  <a href=\"https://pancakeswap.finance/?outputCurrency={contract}&chainId=56&inputCurrency=BNB\">PancageSwap 🥞</a>\n",
            f"<a href=\"https://bscscan.com/address/{contract}\">BSC Scan 📡</a>\n"
        ])

    return ''.join(links)

# def format_time(datetime_str):
#     is_recent = False # Default: Not recent
#     formatted_string = "📅 Time invalid"
#     try:
#         # Attempt to use the configured timezone
#         local_tz = ZoneInfo("Europe/Berlin")
#     except Exception:
#         # Fallback to a fixed offset if timezone info is unavailable
#         print("WARNING: Could not load 'Europe/Berlin' timezone. Using UTC+2 fallback.")
#         local_tz = timezone(timedelta(hours=2), "UTC+02:00_Fallback")

#     try:
#         # Parse the UTC time from the tweet
#         tweet_time_utc = datetime.fromisoformat(datetime_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
#         # Convert to the local timezone
#         tweet_time_local = tweet_time_utc.astimezone(local_tz)
#         # Get the current time in the same local timezone
#         current_time_local = datetime.now(local_tz)
#         # Calculate the difference
#         time_diff = current_time_local - tweet_time_local
#         seconds_ago = time_diff.total_seconds()

#         # --- Determine if the post is recent (within 15 minutes) ---
#         # 900 seconds = 15 minutes
#         if 0 <= seconds_ago < 900:
#             is_recent = True
#         # --- End recent check ---

#         # --- Format the display string based on age ---
#         if seconds_ago < 0:
#             # post is from the future (clock skew?)
#             formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} (Future?)"
#         elif seconds_ago < 3600: # Under 1 hour (includes the <15 min case)
#             minutes_ago = int(seconds_ago // 60)
#             formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({minutes_ago} min)"
#         elif seconds_ago < 86400: # Under 1 day (but older than 1 hour)
#             hours_ago = int(seconds_ago // 3600)
#             formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({hours_ago} hrs)"
#         else: # Older than 1 day
#             formatted_string = f"📅 {tweet_time_local.strftime('%d.%m.%y %H:%M')}"
#         # --- End string formatting ---

#     except ValueError:
#         # Error parsing the datetime string, keep default "invalid" message
#         pass
#     except Exception as e:
#         # Log other unexpected errors during time formatting
#         print(f"ERROR in format_time: {e}")
#         # Keep default "invalid" message

#     return formatted_string, is_recent


async def process_full_text_request(query, context, tweet_url): # Added context
    """Processes the request to get full post text and update the message."""
    try:
        # Notify user that we're processing
        await query.answer("Fetching full text...")

        # --- Pause Scraping ---
        await pause_scraping()

        # --- Get Full Text ---
        full_text = await get_full_tweet_text(tweet_url) # This function now handles navigation back

        if full_text is None:
            await query.answer("Could not load full text. Please try again.")
            await resume_scraping() # Resume on failure
            return

        # --- Update the original message ---
        original_message = query.message
        original_text_html = original_message.text_html # Use HTML for reliable parsing

        # +++ Ensure original_text_html is a string +++
        if not isinstance(original_text_html, str):
            logger.error(f"Original message text_html is not a string (type: {type(original_text_html)}) for message ID {original_message.message_id}. Cannot process full text update inline.")
            await query.answer("ℹ️ Original message format error. Sending text separately.", show_alert=False)
            try:
                escaped_full_text_fallback = html.escape(full_text)
                await query.message.reply_text(f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text_fallback}</blockquote>\n\n🔥 FULL TEXT", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as reply_err:
                logger.error(f"Failed to send full text as reply fallback (TypeError case): {reply_err}")
                await query.answer("❌ Error sending full text.", show_alert=True)
            await resume_scraping() # Resume after handling
            return
        # --- End String Check ---

        # --- Replace blockquote content ---
        blockquote_pattern = r"<blockquote>.*?</blockquote>"
        escaped_full_text = html.escape(full_text)
        new_blockquote = f"<blockquote>{escaped_full_text}</blockquote>"
        new_message_text, num_replacements = re.subn(blockquote_pattern, new_blockquote, original_text_html, count=1, flags=re.DOTALL)

        if num_replacements > 0:
            # Add the marker
            new_message_text += "\n\n🔥 FULL TEXT"
            try:
                await context.bot.edit_message_text(
                    chat_id=original_message.chat_id,
                    message_id=original_message.message_id,
                    text=new_message_text,
                    reply_markup=original_message.reply_markup, # Keep original buttons
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
                await query.answer("Full text loaded!")
            except telegram.error.BadRequest as edit_error:
                 if "Message is not modified" in str(edit_error):
                     await query.answer("ℹ️ Text is already complete.", show_alert=False)
                 else:
                     logger.error(f"BadRequest editing message {original_message.message_id} for full text: {edit_error}", exc_info=True)
                     await query.answer("❌ Error updating message.", show_alert=True)
            except Exception as edit_error:
                logger.error(f"Unexpected error editing message {original_message.message_id} for full text: {edit_error}", exc_info=True)
                await query.answer("❌ Error updating message.", show_alert=True)
        else:
            # Fallback if blockquote wasn't found for replacement
            logger.warning(f"Could not find blockquote to replace in message {original_message.message_id}. Sending full text as reply.")
            await query.answer("ℹ️ Sending full text as reply.", show_alert=False)
            try:
                await query.message.reply_text(f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text}</blockquote>\n\n🔥 FULL TEXT", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as reply_err:
                logger.error(f"Failed to send full text as reply fallback (no blockquote): {reply_err}")
                await query.answer("❌ Error sending full text.", show_alert=True)

    except Exception as e:
        print(f"Error processing full text request: {e}")
        logger.error("Error processing full text request", exc_info=True)
        try: await query.answer("Could not load full text. Please try again.")
        except: pass # Ignore answer error if query already answered
    finally:
        # --- Resume Scraping ---
        await resume_scraping()


async def handle_callback_query(update, context):
    """Handles all button clicks."""
    global is_headless_enabled
    query = update.callback_query
    data = query.data

    # Answer the query immediately to remove the "loading" state
    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Failed to answer callback query for data {data}: {e}")
        # If answering fails, we might not be able to proceed gracefully.
        return

    # +++ ADMIN CHECK FOR ALL CALLBACK QUERIES +++
    if not query.from_user:
        logger.warning(f"Callback query from unknown user for data: {data}")
        return # Cannot check admin status

    user_id = query.from_user.id
    if not is_user_admin(user_id):
        logger.warning(f"Unauthorized callback query access by non-admin user {user_id} for data: {data}")
        try:
            # Inform the user via an alert on the button press
            await query.answer("❌ Access Denied. Admin only.", show_alert=True)
        except Exception as e_ans:
            logger.error(f"Failed to send 'Access Denied' answer for callback query to non-admin {user_id}: {e_ans}")
        # Optionally, edit the message to indicate denial if possible and appropriate.
        # However, editing might be intrusive or fail if the message is old.
        # For now, just the alert is sufficient.
        return # Stop further processing for non-admins
    # +++ END ADMIN CHECK +++

    logger.info(f"Callback received from admin {user_id}: {data}") # Log now confirms admin

    # Split data safely
    parts = data.split(":", 1)
    action_type = parts[0] if parts else None

    if action_type == "like":
        if len(parts) > 1:
            tweet_id = parts[1]
            # Queue the like action
            await action_queue.put(('like', {'tweet_id': tweet_id, 'chat_id': query.message.chat_id, 'message_id': query.message.message_id, 'original_callback_data': data, 'original_keyboard_data': query.message.reply_markup.to_dict().get('inline_keyboard') if query.message.reply_markup else None}))
            # ... (code to edit button text to "Like (⏳)") ...
        else: logger.warning(f"Invalid like callback format: {data}")

    elif action_type == "repost":
        if len(parts) > 1:
            tweet_id = parts[1]
            # Queue the repost action
            await action_queue.put(('repost', {'tweet_id': tweet_id, 'chat_id': query.message.chat_id, 'message_id': query.message.message_id, 'original_callback_data': data, 'original_keyboard_data': query.message.reply_markup.to_dict().get('inline_keyboard') if query.message.reply_markup else None}))
            # ... (code to edit button text to "Repost (⏳)") ...
        else: logger.warning(f"Invalid repost callback format: {data}")

    elif action_type == "full":
        if len(parts) > 1:
            tweet_id = parts[1]
            # Queue the full text action
            await action_queue.put(('full', {'tweet_id': tweet_id, 'chat_id': query.message.chat_id, 'message_id': query.message.message_id, 'original_callback_data': data, 'original_keyboard_data': query.message.reply_markup.to_dict().get('inline_keyboard') if query.message.reply_markup else None}))
            # ... (code to edit button text to "Full Text (⏳)") ...
        else: logger.warning(f"Invalid full callback format: {data}")

    elif action_type == "rate":
        # Extract the rating data
        rate_parts = data.split(":", 3) # Expect rate:value:key:name
        if len(rate_parts) != 4:
            logger.warning(f"Invalid rating format: {data}")
            return

        rating_value = rate_parts[1]
        source_key = rate_parts[2]
        encoded_name = rate_parts[3]

        try:
            rating_int = int(rating_value)
            if not 1 <= rating_int <= 5:
                logger.warning(f"Invalid rating value: {rating_int}")
                return

            try:
                author_name = base64.urlsafe_b64decode(encoded_name.encode()).decode()
            except:
                author_name = source_key

            global ratings_data # Ensure access
            if source_key not in ratings_data:
                 ratings_data[source_key] = {"name": author_name, "ratings": {str(i): 0 for i in range(1, 6)}}
            elif "ratings" not in ratings_data[source_key] or not isinstance(ratings_data[source_key].get("ratings"), dict):
                 ratings_data[source_key]["ratings"] = {str(i): 0 for i in range(1, 6)}
            ratings_data[source_key]["name"] = author_name

            rating_key_str = str(rating_int)
            ratings_data[source_key]["ratings"][rating_key_str] = ratings_data[source_key]["ratings"].get(rating_key_str, 0) + 1
            save_ratings()

            total_ratings = 0; weighted_sum = 0
            for star_str, count in ratings_data[source_key].get("ratings", {}).items():
                try:
                    star = int(star_str)
                    if 1 <= star <= 5: total_ratings += count; weighted_sum += star * count
                except ValueError: continue

            if total_ratings > 0:
                average_rating = weighted_sum / total_ratings
                rating_display = f"{average_rating:.1f}⭐({total_ratings})"
                logger.info(f"Rating recorded for {author_name}: {rating_int} stars. New avg: {rating_display}")
            else:
                logger.info(f"Rating recorded for {author_name}: {rating_int} stars.")

            # --- Remove rating buttons ---
            try:
                original_markup = query.message.reply_markup; new_keyboard = []
                if original_markup:
                    for row in original_markup.inline_keyboard:
                        is_rating_header = any(b.callback_data == "rate_noop" for b in row)
                        is_rating_buttons = any(b.callback_data.startswith("rate:") for b in row)
                        if not is_rating_header and not is_rating_buttons: new_keyboard.append(row)
                if new_keyboard: await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                else: await query.edit_message_reply_markup(reply_markup=None)
            except Exception as edit_err: logger.warning(f"Could not edit reply markup after rating: {edit_err}")

        except Exception as e:
            print(f"Error processing rating: {e}")
            logger.error("Error processing rating", exc_info=True)

    elif action_type == "cloudflare_solved":
        logger.info(f"Processing cloudflare_solved callback: {data}")
        try:
            if len(parts) > 1:
                clicked_account_index_str = parts[1]
                clicked_account_index = int(clicked_account_index_str)

                global WAITING_FOR_CLOUDFLARE_CONFIRMATION, CLOUDFLARE_ACCOUNT_INDEX, cloudflare_solved_event
                if WAITING_FOR_CLOUDFLARE_CONFIRMATION and CLOUDFLARE_ACCOUNT_INDEX == clicked_account_index:
                    cloudflare_solved_event.set()
                    await query.answer("Confirmation received. Resuming login check...")
                    await query.edit_message_text(text=query.message.text + "\n\n✅ Confirmation received.", reply_markup=None)
                    logger.info(f"Cloudflare confirmation processed for account index {clicked_account_index}.")
                elif CLOUDFLARE_ACCOUNT_INDEX != clicked_account_index:
                    await query.answer("⚠️ This confirmation is for a different account's Cloudflare check.", show_alert=True)
                    logger.warning(f"Cloudflare confirmation received for wrong account index ({clicked_account_index} vs {CLOUDFLARE_ACCOUNT_INDEX}).")
                else:
                    await query.answer("ℹ️ Not currently waiting for this confirmation.", show_alert=True)
                    logger.warning(f"Received unexpected Cloudflare confirmation: {data} (Waiting: {WAITING_FOR_CLOUDFLARE_CONFIRMATION}, Waiting Index: {CLOUDFLARE_ACCOUNT_INDEX})")
                    try: await query.edit_message_reply_markup(reply_markup=None)
                    except: pass
            else:
                logger.error(f"Missing account index in cloudflare_solved callback data '{data}'")
                await query.answer("❌ Error processing confirmation (invalid data).", show_alert=True)

        except (IndexError, ValueError) as parse_err:
            logger.error(f"Error parsing cloudflare_solved callback data '{data}': {parse_err}")
            await query.answer("❌ Error processing confirmation.", show_alert=True)
        except Exception as e:
            logger.error(f"Error handling cloudflare_solved callback '{data}': {e}", exc_info=True)
            await query.answer("❌ Error processing confirmation.", show_alert=True)

    elif action_type == "headless_follow":
        logger.info(f"Processing headless_follow callback: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
        try:
            if len(parts) < 2: raise ValueError("Missing data after headless_follow:")
            decision_parts = parts[1].split(":", 1)
            decision = decision_parts[0]
            target_username = decision_parts[1] if len(decision_parts) > 1 else None

            if decision == "yes":
                # Admin check is already done by handle_callback_query

                global is_headless_enabled
                is_headless_enabled = False
                save_settings()
                logger.info(f"Headless mode disabled by user {query.from_user.id} via follow prompt.")
                follow_command_text = f"/follow @{target_username}" if target_username else "/follow <username>"
                await query.edit_message_text(
                    f"✅ Headless mode has been disabled.\n\n"
                    f"‼️ **Please restart the bot now.**\n\n"
                    f"After restarting, use the command `{follow_command_text}` again.",
                    parse_mode=ParseMode.MARKDOWN
                )

            elif decision == "no":
                logger.info(f"User {query.from_user.id} cancelled follow action due to headless mode.")
                await query.edit_message_text("❌ Follow action cancelled. Headless mode remains active.")
                await resume_scraping()
            else:
                logger.warning(f"Unknown decision in headless_follow callback: {decision}")
                await query.edit_message_text("❌ Unknown action.")
                await resume_scraping()

        except ValueError as ve:
            logger.error(f"Error parsing headless_follow callback data ({parts[1] if len(parts) > 1 else 'N/A'}): {ve}", exc_info=True)
            await query.edit_message_text("❌ Error processing request (invalid data format).")
            await resume_scraping()
        except Exception as e:
            logger.error(f"Error processing headless_follow callback: {e}", exc_info=True)
            await query.edit_message_text("❌ Error processing request.")
            await resume_scraping()

    elif action_type == "headless_scrape":
        logger.info(f"Processing headless_scrape callback: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
        try:
            if len(parts) < 2: raise ValueError("Missing data after headless_scrape:")
            decision = parts[1] # Should be 'yes' or 'no'

            if decision == "yes":
                # Admin check is already done by handle_callback_query

                is_headless_enabled = False
                save_settings()
                logger.info(f"Headless mode disabled by user {query.from_user.id} via scrape prompt.")
                await query.edit_message_text(
                    f"✅ Headless mode has been disabled.\n\n"
                    f"‼️ **Restarting WebDriver now...**\n\n"
                    f"Queued scrapes will start automatically after login if the queue is not empty."
                )
                # Call the restart helper function
                await restart_driver_and_login(query)
                # No resume here, restart_driver_and_login handles it

            elif decision == "no":
                logger.info(f"User {query.from_user.id} chose to keep headless mode active for scraping (queued).")
                await query.edit_message_text("✅ Headless mode remains active. Scrape requests remain queued.")
                await resume_scraping() # Resume scraping, as the command handler paused
            else:
                logger.warning(f"Unknown decision in headless_scrape callback: {decision}")
                await query.edit_message_text("❌ Unknown action.")
                await resume_scraping() # Resume scraping

        except ValueError as ve:
            logger.error(f"Error parsing headless_scrape callback data ({parts[1] if len(parts) > 1 else 'N/A'}): {ve}", exc_info=True)
            await query.edit_message_text("❌ Error processing request (invalid data format).")
            await resume_scraping() # Resume scraping
        except Exception as e:
            logger.error(f"Error processing headless_scrape callback: {e}", exc_info=True)
            await query.edit_message_text("❌ Error processing request.")
            await resume_scraping() # Resume scraping


    elif action_type == "noop_processing":
        pass # Do nothing for the placeholder button

    elif action_type == "rate_noop":
        pass # Do nothing for the rating header
    else:
        # Handle other callbacks (sync, help, etc.) - Call the main handler
        # Ensure button_callback_handler exists and is correctly defined elsewhere
        # This assumes button_callback_handler handles the rest
        await button_callback_handler(update, context)


async def check_new_tweets_button():
    """
    Checks for the 'Show new tweets' button, clicks it, logs the count found
    on the button with structure, and returns that count.
    Returns 0 if the button is not found or no count could be extracted.
    """
    global driver # Access the global WebDriver
    num_new_tweets_on_button = 0 # Default value
    try:
        # Wait briefly for the button
        button = WebDriverWait(driver, 2).until(
             EC.presence_of_element_located((By.XPATH, '//button[.//span[contains(text(), "Show") and contains(text(), "post")]]'))
        )

        # Try to extract the count for logging *before* clicking
        try:
            span_element = button.find_element(By.XPATH, './/span[contains(text(), "Show")]')
            button_text = span_element.text.strip()
            match = re.search(r'(\d+)', button_text)
            if match:
                num_new_tweets_on_button = int(match.group(1))
        except Exception as e_extract:
             # print(f"INFO: Could not extract number from button text: {e_extract}") # Optional
             num_new_tweets_on_button = 1 # Conservative assumption


        # Click immediately if button found
        button.click()

        # === STRUCTURED LOGGING ===
        print("\n############################") # Blank line before + Header
        print(f"New Tweets button clicked (approx. {num_new_tweets_on_button} tweets)") # Translated
        print("############################\n") # Header + Blank line after
        # === END STRUCTURED LOGGING ===

        # Short wait for new tweets to load
        await asyncio.sleep(random.uniform(1.5, 2.5))

        return num_new_tweets_on_button

    except (TimeoutException, NoSuchElementException):
        # No new tweets button found, this is normal
        return 0 # Return 0 if no button found
    except Exception as e:
        print(f"Error checking/clicking the 'New Tweets' button: {e}") # Translated
        return 0 # Return 0 on error

async def autofollow_mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the Auto-Follow mode (off, slow, fast)."""
    global auto_follow_mode, cancel_fast_follow_flag, is_fast_follow_running

    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Please specify mode: `/autofollowmode <off|slow|fast>`", parse_mode=ParseMode.MARKDOWN)
        await resume_scraping()
        return

    new_mode = context.args[0].lower()

    if new_mode not in ["off", "slow", "fast"]:
        await update.message.reply_text("❌ Invalid mode. Choose: `off`, `slow`, or `fast`.", parse_mode=ParseMode.MARKDOWN)
        await resume_scraping()
        return

    current_mode = auto_follow_mode
    if new_mode == current_mode:
        await update.message.reply_text(f"ℹ️ Auto-Follow mode is already '{current_mode.upper()}'.")
        await resume_scraping()
        return

    # If switching from/to Fast, cancel running task if necessary
    if (current_mode == "fast" and new_mode != "fast") or \
       (current_mode != "fast" and new_mode == "fast"):
        if is_fast_follow_running:
            print("[Mode Change] Cancelling running Fast-Follow task due to mode change.")
            cancel_fast_follow_flag = True
            await update.message.reply_text("⚠️ Running Fast-Follow task is being cancelled...")
            # Give a short pause for the task to react
            await asyncio.sleep(2)

    auto_follow_mode = new_mode
    save_settings()
    await update.message.reply_text(f"✅ Auto-Follow mode set to '{new_mode.upper()}'.")
    #  If mode is set to SLOW, directly show the interval command
    if new_mode == "slow":
        # Ensure the global variable is available here
        global auto_follow_interval_minutes
        current_interval = f"{auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]}"
        await update.message.reply_text(
            f"Copy, change Min/Max (minutes):\n\n`/autofollowinterval {current_interval}`",
            parse_mode=ParseMode.MARKDOWN
        )
    logger.info(f"Auto-Follow mode set to '{new_mode}' by user {update.message.from_user.id}")

    # If set to Fast, give a message (task starts in the next loop)
    if new_mode == "fast":
         account_username = get_current_account_username() or "Unknown"
         list_count = len(current_account_usernames_to_follow)
         if list_count > 0:
             await update.message.reply_text(f"🚀 Fast-Follow for @{account_username} will start on the next cycle ({list_count} users).")
         else:
             await update.message.reply_text(f"ℹ️ Fast-Follow activated, but list for @{account_username} is empty.")


    await resume_scraping()

async def search_tickers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggles the Ticker search functionality."""
    global search_tickers_enabled
    # is_scraping_paused is handled by the admin wrapper

    # Toggle the setting
    search_tickers_enabled = not search_tickers_enabled
    save_settings() # Save the new state

    # Send confirmation message
    status_text = "ENABLED 🟢" if search_tickers_enabled else "DISABLED 🔴"
    await update.message.reply_text(f"✅ Ticker search is now {status_text}")
    logger.info(f"Ticker search toggled to {status_text} by user {update.message.from_user.id}")

    # resume_scraping is handled by the admin wrapper

async def autofollow_interval_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the interval for Slow Mode (min-max minutes)."""
    global auto_follow_interval_minutes

    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Please specify interval: `/autofollowinterval <min>-<max>` (minutes)", parse_mode=ParseMode.MARKDOWN)
        await resume_scraping()
        return

    interval_str = context.args[0]
    try:
        parts = interval_str.split('-')
        if len(parts) != 2: raise ValueError("Format must be min-max")

        min_val = int(parts[0].strip())
        max_val = int(parts[1].strip())

        if not (1 <= min_val <= 1440 and 1 <= max_val <= 1440): # Max 1 day
            raise ValueError("Values must be between 1 and 1440")
        if min_val > max_val:
            raise ValueError("Minimum value cannot be greater than maximum value")

        auto_follow_interval_minutes = [min_val, max_val]
        save_settings()
        await update.message.reply_text(f"✅ Slow-Mode interval set to {min_val}-{max_val} minutes.")
        logger.info(f"Auto-Follow slow interval set to {min_val}-{max_val} min by user {update.message.from_user.id}")

    except ValueError as e:
        await update.message.reply_text(f"❌ Invalid interval: {e}. Format: `<min>-<max>` (e.g., `5-15`)")
    except Exception as e:
        await update.message.reply_text(f"❌ Error setting interval: {e}")
        logger.error(f"Error setting interval '{interval_str}': {e}", exc_info=True)

    await resume_scraping()

async def cancel_fast_follow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Requests cancellation of the ongoing Fast-Follow process."""
    global is_fast_follow_running, cancel_fast_follow_flag
    if is_fast_follow_running:
        cancel_fast_follow_flag = True
        await update.message.reply_text("🟡 Cancellation of Fast-Follow task requested. It might take a moment...")
        print("[Cancel] Fast-Follow cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ No Fast-Follow task is currently running.")
    # No resume/pause here, this command only affects the flag

def increment_ad_total_count():
    """Increment the total count of found ads"""
    global posts_count
    # check_rotate_counts() is NOT needed here
    # Ensure ads_total exists before accessing it
    if "ads_total" not in posts_count:
        posts_count["ads_total"] = 0
    posts_count["ads_total"] += 1 # Increment by 1

    # Save e.g., every 50 ads to avoid writing constantly
    # Check safely if the key exists
    if posts_count.get("ads_total", 0) % 50 == 0:
        save_posts_count()


async def check_and_process_queue(application):
    """
    Checks the action queue and processes ONE item if the bot is running.
    Attempts to update the original message's button state.
    Returns True if an action was processed, False otherwise.
    Manages pause/resume internally for the action duration.
    """
    global is_scraping_paused, action_queue, CHANNEL_ID

    if not is_scraping_paused and not action_queue.empty():
        logger.info("[Queue Check] Action queue has items. Processing one...")
        await pause_scraping()
        action_processed = False
        action_type = "unknown"
        action_data = {}
        success = False # Track action success
        result_message_for_log = "" # For logging

        try:
            action_type, action_data = await action_queue.get()
            logger.info(f"[Action Queue] Processing: {action_type} with data: {action_data}")

            tweet_id = action_data.get('tweet_id')
            chat_id = action_data.get('chat_id')
            message_id = action_data.get('message_id')
            original_callback_data = action_data.get('original_callback_data')
            original_keyboard_data = action_data.get('original_keyboard_data') # Get original button structure
            tweet_url = f"https://x.com/i/status/{tweet_id}" if tweet_id else None

            # --- Execute the action ---
            if action_type == "like" and tweet_url:
                success = await like_tweet(tweet_url)
                result_message_for_log = f"Like {'succeeded' if success else 'failed'} for {tweet_id}"
            elif action_type == "repost" and tweet_url:
                success = await repost_tweet(tweet_url)
                result_message_for_log = f"Repost {'succeeded' if success else 'failed'} for {tweet_id}"
            elif action_type == "full" and tweet_url and chat_id and message_id:
                full_text = await get_full_tweet_text(tweet_url)
                if full_text:
                    escaped_full_text = html.escape(full_text)
                    try:
                        # Send full text as reply
                        await application.bot.send_message(
                            chat_id=chat_id,
                            text=f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text}</blockquote>\n\n🔥 FULL TEXT",
                            parse_mode=ParseMode.HTML,
                            reply_to_message_id=message_id,
                            disable_web_page_preview=True
                        )
                        result_message_for_log = f"Full text sent for {tweet_id}"
                        success = True # Mark as success for button update
                    except Exception as send_err:
                         logger.error(f"Failed to send full text reply from queue: {send_err}")
                         result_message_for_log = f"Failed to send full text for {tweet_id}"
                         success = False
                else:
                    result_message_for_log = f"Could not retrieve full text for {tweet_id}"
                    success = False
            else:
                logger.warning(f"Unknown action type in queue: {action_type}")
                result_message_for_log = f"Unknown action '{action_type}'"
                success = False

            logger.info(f"[Action Queue] Result: {result_message_for_log}")

            # --- Try to update the original message's button ---
            if chat_id and message_id and original_callback_data and original_keyboard_data:
                try:
                    new_keyboard_markup = None
                    updated_keyboard = []
                    for row_data in original_keyboard_data:
                        new_row = []
                        for button_data in row_data:
                            # Check if this is the button that was originally clicked
                            if button_data['callback_data'] == original_callback_data:
                                new_text = button_data['text'] # Default to original text
                                if action_type == "like":
                                    new_text = "Like ✅" if success else "👍 Like" # Update text based on success
                                elif action_type == "repost":
                                    new_text = "Repost ✅" if success else "🔄 Repost"
                                elif action_type == "full":
                                    # For full text, revert the button text
                                    new_text = "✅ Full Text" if success else "📄 Full Text"
                                # Create the updated button (keep original callback data for potential re-click)
                                new_row.append(InlineKeyboardButton(new_text, callback_data=button_data['callback_data']))
                            else:
                                # Keep other buttons as they were
                                new_row.append(InlineKeyboardButton(button_data['text'], callback_data=button_data['callback_data']))
                        updated_keyboard.append(new_row)

                    if updated_keyboard:
                        new_keyboard_markup = InlineKeyboardMarkup(updated_keyboard)

                    # Edit the original message with the updated keyboard
                    await application.bot.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=new_keyboard_markup
                    )
                    logger.info(f"Successfully updated buttons for message {message_id} after {action_type} ({'Success' if success else 'Failure'}).")

                except telegram.error.BadRequest as e:
                    if "message is not modified" not in str(e).lower() and "message to edit not found" not in str(e).lower():
                        logger.warning(f"Could not update buttons for message {message_id} (BadRequest): {e}")
                except Exception as update_err:
                    logger.warning(f"Could not update buttons for message {message_id}: {update_err}")
            else:
                 logger.warning(f"Missing data to update original message buttons for action {action_type}.")


            action_queue.task_done()
            action_processed = True

        except asyncio.CancelledError:
            logger.warning("[Action Queue] Task cancelled during processing.")
            raise
        except Exception as e:
            logger.error(f"Error processing action {action_type} from queue: {e}", exc_info=True)
            if not action_queue.empty():
                try: action_queue.task_done()
                except ValueError: pass
            try: await application.bot.send_message(CHANNEL_ID, f"❌ Critical error processing action '{action_type}' from queue.")
            except: pass
        finally:
            await resume_scraping()
            await asyncio.sleep(1)

        return action_processed
    else:
        return False

async def run():
    """Main loop with correct state handling for pause/resume."""
    global application, global_followed_users_set, is_scraping_paused, is_schedule_pause, pause_event
    global last_follow_attempt_time, current_account_usernames_to_follow, is_periodic_follow_active
    global schedule_pause_start, schedule_pause_end # Access for messages
    global search_mode, current_account # For start message
    global schedule_sync_enabled, schedule_sync_start_time, schedule_sync_end_time, last_sync_schedule_run_date
    global schedule_follow_list_enabled, schedule_follow_list_start_time, schedule_follow_list_end_time, last_follow_list_schedule_run_date
    global cancel_sync_flag, cancel_scheduled_follow_list_flag # Declare these as global

    network_error_count = 0
    last_error_time = time.time()

    try:
        # --- Ensure all base data files exist (call this early) ---
        ensure_data_files_exist() # New function call

        # --- Initialization (Telegram Bot, Settings, Lists etc.) ---
        print("Initializing Telegram Bot...")
        global ACTIVE_BOT_TOKEN
        if not ACTIVE_BOT_TOKEN: print("ERROR: No active bot token!"); return
        application = ApplicationBuilder().token(ACTIVE_BOT_TOKEN).connect_timeout(30).read_timeout(30).build()
        # ... Add handlers ...
          # --- Register Command Handlers (ALL via the Admin Helper) ---
        # Syntax: add_admin_command_handler(application, "command_name", function_name)

        # Existing / Commands
        add_admin_command_handler(application, "addusers", add_users_command)
        add_admin_command_handler(application, "autofollowmode", autofollow_mode_command)
        add_admin_command_handler(application, "autofollowinterval", autofollow_interval_command)
        add_admin_command_handler(application, "cancelfastfollow", cancel_fast_follow_command)
        add_admin_command_handler(application, "autofollowstatus", autofollow_status_command) # Maybe leave status public?
        add_admin_command_handler(application, "clearfollowlist", clear_follow_list_command)
        add_admin_command_handler(application, "syncfollows", sync_followers_command)
        add_admin_command_handler(application, "buildglobalfrombackups", build_global_from_backups_command)
        add_admin_command_handler(application, "globallistinfo", global_list_info_command) # Maybe leave info public?
        add_admin_command_handler(application, "initglobalfrombackup", init_global_from_backup_command)
        add_admin_command_handler(application, "cancelbackup", cancel_backup_command)
        add_admin_command_handler(application, "cancelsync", cancel_sync_command)
        add_admin_command_handler(application, "rates", show_ratings_command) # Maybe leave ratings public?
        add_admin_command_handler(application, "backupfollowers", backup_followers_command)
        add_admin_command_handler(application, "setmaxage", set_max_age_command)

        # Following Database Commands
        add_admin_command_handler(application, "scrapefollowing", scrape_following_command)
        add_admin_command_handler(application, "addfromdb", add_from_db_command)
        add_admin_command_handler(application, "canceldbscrape", cancel_db_scrape_command)

        # Converted Commands
        add_admin_command_handler(application, "keywords", keywords_command) # Maybe leave list public?
        add_admin_command_handler(application, "addkeyword", add_keyword_command)
        add_admin_command_handler(application, "removekeyword", remove_keyword_command)
        add_admin_command_handler(application, "follow", follow_command)
        add_admin_command_handler(application, "unfollow", unfollow_command)
        add_admin_command_handler(application, "like", like_command)
        add_admin_command_handler(application, "repost", repost_command)
        add_admin_command_handler(application, "account", account_command) # Maybe leave account public?
        add_admin_command_handler(application, "help", help_command)
        add_admin_command_handler(application, "status", status_command)
        add_admin_command_handler(application, "stats", stats_command) # Maybe leave stats public?
        add_admin_command_handler(application, "count", stats_command) # Alias for stats
        add_admin_command_handler(application, "ping", ping_command)
        #add_admin_command_handler(application, "mode", mode_command) # Maybe leave mode public?
        #add_admin_command_handler(application, "modefull", mode_full_command)
        #add_admin_command_handler(application, "modeca", mode_ca_command)
        add_admin_command_handler(application, "pause", pause_command)
        add_admin_command_handler(application, "resume", resume_command)
        add_admin_command_handler(application, "schedule", schedule_command) # Maybe leave schedule public?
        add_admin_command_handler(application, "scheduleon", schedule_on_command)
        add_admin_command_handler(application, "scheduleoff", schedule_off_command)
        add_admin_command_handler(application, "scheduletime", schedule_time_command)
        add_admin_command_handler(application, "switchaccount", switch_account_command)
        add_admin_command_handler(application, "schedulesync", schedule_sync_toggle_command) # Toggles on/off
        add_admin_command_handler(application, "schedulesynctime", schedule_sync_time_command)
        add_admin_command_handler(application, "schedulefollowlist", schedule_follow_list_toggle_command) # Toggles on/off
        add_admin_command_handler(application, "schedulefollowlisttime", schedule_follow_list_time_command)
        add_admin_command_handler(application, "allschedules", show_detailed_schedules_command) # Shows status of all

        # Admin Management Commands (now also registered via the helper)
        # IMPORTANT: The functions themselves NO LONGER need the @admin_required decorator!
        add_admin_command_handler(application, "addadmin", add_admin_command)
        add_admin_command_handler(application, "removeadmin", remove_admin_command)
        add_admin_command_handler(application, "listadmins", list_admins_command)
        # Ticker Search Toggle Command
        add_admin_command_handler(application, "searchtickers", search_tickers_command)
        # Headless Mode Toggle Command
        add_admin_command_handler(application, "toggleheadless", toggle_headless_command)


        # Callback Handler for Buttons
        application.add_handler(CallbackQueryHandler(handle_callback_query)) # Use the new unified handler

        # Message Handler ONLY for Non-Commands (e.g., Auth Code)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_telegram_message))

        max_init_retries = 3
        init_retry_delay = 10 # seconds
        initialized_successfully = False
        for attempt in range(max_init_retries):
            try:
                print(f"Attempting Telegram initialization (Attempt {attempt + 1}/{max_init_retries})...")
                await application.initialize()
                print("Telegram initialization successful.")
                initialized_successfully = True
                break # Success, exit loop
            except telegram.error.TimedOut as e:
                print(f"WARNING: Timeout during Telegram initialization (Attempt {attempt + 1}): {e}")
                if attempt < max_init_retries - 1:
                    print(f"Waiting {init_retry_delay} seconds before next attempt...")
                    await asyncio.sleep(init_retry_delay)
                else:
                    print("ERROR: Maximum initialization attempts reached. Aborting.")
                    # raise RuntimeError("Could not initialize Telegram Bot after multiple attempts.") from e
            except Exception as e:
                # Catch other errors during initialization
                print(f"ERROR during Telegram initialization (Attempt {attempt + 1}): {e}")
                raise RuntimeError(f"Unexpected error during Telegram initialization: {e}") from e

        if not initialized_successfully:
            # End the script if initialization ultimately fails
            print("ERROR: Telegram could not be initialized. Script will exit.")
            return # Exits the run() function cleanly
        # --- End Robust Initialization ---

        print("Loading settings, counters, and lists...")
        load_settings(); load_posts_count(); load_schedule(); load_ratings()
        load_following_database() # Load Following DB
        load_admins() # Load Admin list
        global_followed_users_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
        print(f"{len(global_followed_users_set)} users loaded globally.")
        load_current_account_follow_list()
        print("Starting Telegram polling...")
        try: # Skip old updates
            updates = await application.bot.get_updates(offset=-1, limit=1)
            if updates: await application.bot.get_updates(offset=updates[-1].update_id + 1)
            print("Skipped old Telegram updates.")
        except Exception as e: print(f"Error skipping old updates: {e}")
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True, timeout=30) # etc.
        print("Initializing WebDriver and X Login...")
        await initialize()

        # --- Load initial bot status from settings and adjust via schedule if needed ---
        # is_scraping_paused and is_periodic_follow_active are already set in load_settings()
        # pause_event is also set in load_settings()

        is_schedule_pause = False # Schedule pause is a runtime status, not persistent

        # Check if the schedule would force a pause *now*
        initial_schedule_check = check_schedule()
        if initial_schedule_check is True:
            # If the schedule wants a pause AND the bot is currently running (according to settings)
            if not is_scraping_paused:
                print("INFO: Start time is within the scheduled pause period (Schedule active). Overriding loaded status -> PAUSED.")
                is_scraping_paused = True
                is_schedule_pause = True
                pause_event.clear()
                # No save_settings() here, as this is just the initial state
            else:
                # Bot is already paused (manually or from last run), schedule also wants pause
                print("INFO: Start time is within the scheduled pause period, bot is already paused (according to settings).")
                # Set is_schedule_pause if the reason is now the schedule
                is_schedule_pause = True # Mark that the *current* pause is (also) due to schedule
        # The case initial_schedule_check == "resume" is not handled here,
        # as the bot starts paused by default or the saved state applies.
        # The main loop will handle the resume case correctly.

        running_status = "⏸️ PAUSED 🟡 (Schedule)" if is_scraping_paused and is_schedule_pause else ("⏸️ PAUSED 🟡 (Manual)" if is_scraping_paused else "▶️ RUNNING 🟢")
        schedule_status = "ON 🟢" if schedule_enabled else "OFF ❌"
        current_username_welcome = get_current_account_username() or "N/A"
        autofollow_mode_display = auto_follow_mode.upper()
        if auto_follow_mode == "slow":
            autofollow_mode_display += f" ({auto_follow_interval_minutes[0]}-{auto_follow_interval_minutes[1]} min)"
        elif auto_follow_mode == "off":
             autofollow_mode_display = "OFF ⏸️"
        ticker_status_welcome = "ON 🟢" if search_tickers_enabled else "OFF 🔴"
        headless_status_welcome = "ON 🟢" if is_headless_enabled else "OFF 🔴"
        welcome_message = (
            f"🤖 raw-bot-X 🚀 START\n"
            f"👉 Acc {current_account+1} (@{current_username_welcome})\n\n"
            f"📊 ▫️STATUS▫️\n"
            f"{running_status}\n"
            f"🔑 Keyword 🔎: {'ON 🟢' if search_keywords_enabled else 'OFF 🔴'}\n"
            f"📝 CA 🔎: {'ON 🟢' if search_ca_enabled else 'OFF 🔴'}\n"
            f"💲 Ticker 🔎: {ticker_status_welcome}\n"
            f"👻 Headless Mode: {headless_status_welcome}\n" # New line for Headless status
            f"⏰ Schedule: {schedule_status} ({schedule_pause_start} - {schedule_pause_end})\n"
            f"🏃🏼‍♂️‍➡️ Auto-Follow: {autofollow_mode_display}\n"
            f"🌍 Timezone: {USER_TIMEZONE_STR}\n"
        )


        keyboard = [[InlineKeyboardButton("ℹ️ Show Help", callback_data="help:help")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_telegram_message(welcome_message, reply_markup=reply_markup)
        print("Start message sent.")

        # --- Initial Queue Check on Startup ---
        if not is_headless_enabled: # Only check if not starting in headless
            queued_usernames = read_and_clear_scrape_queue()
            if queued_usernames:
                logger.info(f"Found {len(queued_usernames)} usernames in scrape queue on startup. Starting tasks...")
                await send_telegram_message(f"▶️ Bot started. Starting {len(queued_usernames)} queued scrape task(s)...")
                # Use asyncio.create_task to run scrapes concurrently after startup
                tasks = []
                processed_for_tasks = set()
                for username in queued_usernames:
                     if username not in processed_for_tasks:
                        logger.info(f"  -> Creating scrape task for @{username}")
                        # Pass None for update, task will use send_telegram_message
                        task = asyncio.create_task(scrape_target_following(None, username))
                        tasks.append(task)
                        processed_for_tasks.add(username)
                        await asyncio.sleep(1) # Stagger task creation slightly
                logger.info(f"Launched {len(tasks)} scrape tasks from startup queue.")
                # Don't wait for completion here, let them run in background
        elif is_headless_enabled:
            logger.info("Bot starting in headless mode. Scrape queue will not be processed automatically on startup.")
            # Optionally check if queue has items and inform user
            if os.path.exists(SCRAPE_QUEUE_FILE) and os.path.getsize(SCRAPE_QUEUE_FILE) > 0:
                 await send_telegram_message("ℹ️ Bot started in headless mode. There are usernames in the scrape queue. Disable headless mode and restart to process them.")
        # --- End Initial Queue Check ---


        # --- Main Loop ---
        print("Starting main loop...")
        while True:
            try:
                # --- New Scheduled Task Checks (Sync & Follow List) ---
                # These run regardless of the main bot's pause state.
                # They will internally manage pausing/resuming the main scraping if they use the browser.
                if USER_CONFIGURED_TIMEZONE: # Ensure timezone is loaded
                    now_local_dt_for_sched = datetime.now(USER_CONFIGURED_TIMEZONE)
                    today_date_for_sched = now_local_dt_for_sched.date()
                    current_time_str_for_sched = now_local_dt_for_sched.strftime("%H:%M")

                    # Check Scheduled Sync
                    if schedule_sync_enabled and \
                       (last_sync_schedule_run_date is None or last_sync_schedule_run_date != today_date_for_sched):
                        
                        start_sync_dt_obj = datetime.strptime(schedule_sync_start_time, "%H:%M").time()
                        end_sync_dt_obj = datetime.strptime(schedule_sync_end_time, "%H:%M").time()
                        
                        is_within_sync_window = False
                        current_time_obj = now_local_dt_for_sched.time()

                        if end_sync_dt_obj <= start_sync_dt_obj: # Overnight window
                            if current_time_obj >= start_sync_dt_obj or current_time_obj < end_sync_dt_obj:
                                is_within_sync_window = True
                        else: # Same day window
                            if start_sync_dt_obj <= current_time_obj < end_sync_dt_obj:
                                is_within_sync_window = True
                        
                        if is_within_sync_window:
                            logger.debug(f"[Scheduler Check SYNC] Within window. Last run: {last_sync_schedule_run_date}, Today: {today_date_for_sched}")
                            active_account_username = get_current_account_username() or "N/A"
                            task_blocked = False
                            # Check if another TYPE of scheduled task is running, or if THIS task type is already running.
                            if is_scheduled_follow_list_running: # Check if the *other* type is running
                                logger.warning(f"[Scheduler] Scheduled Sync for @{active_account_username} BLOCKED: Scheduled Follow List task is active.")
                                await send_telegram_message(f"⚠️ Scheduled Sync for @{active_account_username} skipped: Follow List task is active.")
                                task_blocked = True
                            elif is_sync_running: # Check if an instance of THIS task type is already running
                                logger.warning(f"[Scheduler] Scheduled Sync for @{active_account_username} BLOCKED: Another Sync task is already running.")
                                await send_telegram_message(f"⚠️ Scheduled Sync for @{active_account_username} skipped: another sync task is already running.")
                                task_blocked = True
                            
                            if task_blocked:
                                # If blocked, but it's the first time today we've hit this window,
                                # mark it as "attempted" for today to prevent re-logging/re-messaging.
                                last_sync_schedule_run_date = today_date_for_sched 
                                save_schedule()
                                logger.info(f"[Scheduler] Updated last_sync_schedule_run_date to {today_date_for_sched} because task was blocked but due.")
                            else:
                                # Conditions met to actually start the task
                                logger.info(f"[Scheduler] Current time is within Scheduled Sync window ({schedule_sync_start_time}-{schedule_sync_end_time}). Starting task for @{active_account_username}.")
                                await send_telegram_message(f"⏰ Starting Scheduled Sync for @{active_account_username} (Window: {schedule_sync_start_time}-{schedule_sync_end_time})...")
                                asyncio.create_task(sync_followers_logic(
                                    update=None, 
                                    account_username=active_account_username,
                                    backup_filepath=get_current_backup_file_path(),
                                    global_set_for_sync=load_set_from_file(GLOBAL_FOLLOWED_FILE)
                                ))
                                # The sync_followers_logic itself will update last_sync_schedule_run_date upon *successful completion*.
                                # If it fails or is cancelled, it won't update, allowing a retry on a subsequent day if the window is hit again.

                    # Check Scheduled Follow List Processing
                    if schedule_follow_list_enabled and \
                       (last_follow_list_schedule_run_date is None or last_follow_list_schedule_run_date != today_date_for_sched):

                        start_fl_dt_obj = datetime.strptime(schedule_follow_list_start_time, "%H:%M").time()
                        end_fl_dt_obj = datetime.strptime(schedule_follow_list_end_time, "%H:%M").time()
                        current_time_obj = now_local_dt_for_sched.time()

                        is_within_fl_window = False
                        if end_fl_dt_obj <= start_fl_dt_obj: # Overnight window
                            if current_time_obj >= start_fl_dt_obj or current_time_obj < end_fl_dt_obj:
                                is_within_fl_window = True
                        else: # Same day window
                            if start_fl_dt_obj <= current_time_obj < end_fl_dt_obj:
                                is_within_fl_window = True

                        if is_within_fl_window:
                            logger.debug(f"[Scheduler Check FOLLOW_LIST] Within window. Last run: {last_follow_list_schedule_run_date}, Today: {today_date_for_sched}")
                            active_account_username = get_current_account_username() or "N/A"
                            task_blocked = False
                            # Check if another TYPE of scheduled task is running, or if THIS task type is already running.
                            if is_sync_running: # Check if the *other* type is running
                                logger.warning(f"[Scheduler] Scheduled Follow List for @{active_account_username} BLOCKED: Scheduled Sync task is active.")
                                await send_telegram_message(f"⚠️ Scheduled Follow List for @{active_account_username} skipped: Sync task is active.")
                                task_blocked = True
                            elif is_scheduled_follow_list_running: # Check if an instance of THIS task type is already running
                                logger.warning(f"[Scheduler] Scheduled Follow List for @{active_account_username} BLOCKED: Another Follow List task is already running.")
                                await send_telegram_message(f"⚠️ Scheduled Follow List for @{active_account_username} skipped: another Follow List task is already running.")
                                task_blocked = True
                            
                            if task_blocked:
                                last_follow_list_schedule_run_date = today_date_for_sched
                                save_schedule()
                                logger.info(f"[Scheduler] Updated last_follow_list_schedule_run_date to {today_date_for_sched} because task was blocked but due.")
                            else:
                                logger.info(f"[Scheduler] Current time is within Scheduled Follow List window ({schedule_follow_list_start_time}-{schedule_follow_list_end_time}). Starting task for @{active_account_username}.")
                                await send_telegram_message(f"⏰ Starting Scheduled Follow List Processing for @{active_account_username} (Window: {schedule_follow_list_start_time}-{schedule_follow_list_end_time})...")
                                asyncio.create_task(process_follow_list_schedule_logic(None))
                                # The process_follow_list_schedule_logic itself will update last_follow_list_schedule_run_date upon *successful completion*.
                else:
                    logger.warning("[Scheduler] USER_CONFIGURED_TIMEZONE not set, cannot run new scheduled tasks.")
                # --- End New Scheduled Task Checks ---

                # --- Check and Stop Overdue Scheduled Tasks ---
                if USER_CONFIGURED_TIMEZONE: # Ensure timezone is loaded
                    now_for_stop_check = datetime.now(USER_CONFIGURED_TIMEZONE)
                    
                    # Check Sync Task
                    if is_sync_running and schedule_sync_enabled and not cancel_sync_flag:
                        # Determine the date the current/last sync task was supposed to run or start
                        # If last_sync_schedule_run_date is today, it means it started today or is due today.
                        # If it's yesterday, it might be an overnight task that started yesterday.
                        task_run_date_for_end_check = last_sync_schedule_run_date if last_sync_schedule_run_date else now_for_stop_check.date()

                        s_time_obj = datetime.strptime(schedule_sync_start_time, "%H:%M").time()
                        e_time_obj = datetime.strptime(schedule_sync_end_time, "%H:%M").time()
                        
                        # Determine the actual datetime the task should have ended
                        task_end_dt_on_schedule = datetime.combine(task_run_date_for_end_check, e_time_obj, tzinfo=USER_CONFIGURED_TIMEZONE)
                        if e_time_obj <= s_time_obj: # Overnight task, so end time is on the next day relative to task_run_date_for_end_check
                            task_end_dt_on_schedule += timedelta(days=1)
                        
                        if now_for_stop_check > task_end_dt_on_schedule:
                            logger.warning(f"[Scheduler] Sync task for @{get_current_account_username()} is running past its scheduled end time ({schedule_sync_end_time}). Requesting cancellation.")
                            await send_telegram_message(f"⏰ Scheduled Sync task for @{get_current_account_username()} is running past its end time ({schedule_sync_end_time}). Attempting to stop.")
                            cancel_sync_flag = True # Signal the task to stop

                    # Check Follow List Task
                    if is_scheduled_follow_list_running and schedule_follow_list_enabled and not cancel_scheduled_follow_list_flag:
                        task_run_date_fl_for_end_check = last_follow_list_schedule_run_date if last_follow_list_schedule_run_date else now_for_stop_check.date()

                        s_time_obj_fl = datetime.strptime(schedule_follow_list_start_time, "%H:%M").time()
                        e_time_obj_fl = datetime.strptime(schedule_follow_list_end_time, "%H:%M").time()

                        task_end_dt_fl_on_schedule = datetime.combine(task_run_date_fl_for_end_check, e_time_obj_fl, tzinfo=USER_CONFIGURED_TIMEZONE)
                        if e_time_obj_fl <= s_time_obj_fl: # Overnight
                            task_end_dt_fl_on_schedule += timedelta(days=1)

                        if now_for_stop_check > task_end_dt_fl_on_schedule:
                            logger.warning(f"[Scheduler] Follow List task for @{get_current_account_username()} running past scheduled end time ({schedule_follow_list_end_time}). Requesting cancellation.")
                            await send_telegram_message(f"⏰ Scheduled Follow List task for @{get_current_account_username()} running past end time ({schedule_follow_list_end_time}). Attempting to stop.")
                            cancel_scheduled_follow_list_flag = True
                # --- End Check and Stop Overdue ---

                # 1. Schedule Check (for main bot pause/resume)
                schedule_action = check_schedule()
                if schedule_action == "resume":
                    if is_scraping_paused and is_schedule_pause:
                        print("[Run Loop] Schedule ended pause. Resuming...")
                        await resume_scraping()
                        is_schedule_pause = False
                        await send_telegram_message("▶️ Scheduled pause ended, operation resumed.")
                elif schedule_action is True:
                    if not is_scraping_paused:
                        print("[Run Loop] Schedule starting pause...")
                        # --- Message about pause start (logic for time calculation remains) ---
                        try:
                            local_tz = USER_CONFIGURED_TIMEZONE # Use global
                            if local_tz is None: local_tz = timezone.utc # Fallback
                            
                            now_local = datetime.now(local_tz)
                            today_local = now_local.date()
                            start_dt = datetime.strptime(f"{today_local} {schedule_pause_start}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
                            end_dt = datetime.strptime(f"{today_local} {schedule_pause_end}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
                            next_end_dt = end_dt
                            is_overnight = end_dt <= start_dt
                            # Determine the *next* end time correctly
                            # Determine the *next* end time correctly
                            if is_overnight: # If overnight
                                if now_local >= end_dt: # Use now_local
                                     next_end_dt = end_dt + timedelta(days=1) # Take tomorrow's
                            elif now_local >= end_dt: # Use now_local
                                 next_end_dt = end_dt + timedelta(days=1) # Take tomorrow's

                            remaining_time = next_end_dt - now_local
                            remaining_seconds = max(0, remaining_time.total_seconds())
                            total_minutes = int(remaining_seconds // 60)
                            hours = total_minutes // 60
                            minutes = total_minutes % 60
                            remaining_str = f"{hours}h {minutes}m"
                            if hours == 0 and minutes == 0 and remaining_seconds > 0: remaining_str = "< 1m"
                            message = (
                                f"⏰ Scheduled pause activated\n"
                                f"⏸️ Pausing from {schedule_pause_start} to {schedule_pause_end}\n"
                                f"▶️ Resuming in ~{remaining_str} (at {next_end_dt.strftime('%H:%M')})"
                            )
                            await send_telegram_message(message)
                        except Exception as msg_err:
                            print(f"ERROR sending pause start message: {msg_err}")
                        # --- End message ---
                        is_schedule_pause = True
                        await pause_scraping()

                # 2. Check if we are *now* paused (important!)
                if is_scraping_paused:
                    # print("[Run Loop] Paused. Waiting 5s.") # Shorter wait in paused state
                    await asyncio.sleep(5)
                    continue # To the next loop iteration

                # 3. If we are here, the bot is NOT paused

                # ---  Periodic WebDriver Restart ---
                global last_driver_restart_time, driver # Access globals
                restart_interval_seconds = 4 * 60 * 60 # 4 hours

                if time.time() - last_driver_restart_time > restart_interval_seconds:
                    print(f"INFO: {restart_interval_seconds / 3600:.1f} hours passed since last driver restart. Restarting...")
                    await send_telegram_message("🔄 Starting scheduled WebDriver restart for memory release...")
                    await pause_scraping() # Pause main scraping
                    login_ok_after_restart = False
                    try:
                        # Close old driver safely
                        if driver:
                            print("Closing old WebDriver...")
                            try:
                                driver.quit()
                            except Exception as quit_err:
                                print(f"WARNING: Error closing old driver (possibly already closed): {quit_err}")
                        driver = None # Explicitly set to None

                        # Create new driver
                        print("Creating new WebDriver...")
                        driver = create_driver() # Your function to create the driver

                        # Log in again
                        print("Attempting login again with current account...")
                        if await login(): # login() uses global current_account
                            print("Login after WebDriver restart successful.")
                            await switch_to_following_tab() # Important: Switch to Following tab
                            await send_telegram_message("✅ WebDriver restart and login successful.")
                            last_driver_restart_time = time.time() # Update timestamp ONLY on success
                            login_ok_after_restart = True
                        else:
                            print("ERROR: Login after WebDriver restart failed!")
                            await send_telegram_message("❌ ERROR: Login after WebDriver restart failed! Trying again at the next interval.")
                            # DO NOT update timestamp, so it tries again soon
                    except Exception as restart_err:
                        print(f"ERROR during WebDriver restart/login: {restart_err}")
                        logger.error("Exception during WebDriver restart/login", exc_info=True)
                        await send_telegram_message(f"❌ Critical error during WebDriver restart: {str(restart_err)[:200]}")
                        # Set driver to None if creation failed
                        if 'driver' in locals() and driver is None:
                             pass # Is already None
                        elif 'driver' not in locals():
                             pass # Was never assigned
                        else: # Driver exists, but login failed or similar
                             try:
                                 driver.quit()
                             except: pass
                             driver = None


                    await resume_scraping() # Resume scraping

                    # Important: After a restart attempt (successful or not)
                    # jump directly to the next loop iteration to re-evaluate the state.
                    print("Continuing main loop after restart attempt...")
                    await asyncio.sleep(5) # Short pause after the whole process
                    continue # Jump to the start of the while loop

                # --- END WebDriver Restart ---

                # ---  Periodic Auto-Follow Check (Mode-based) ---
                if auto_follow_mode == "slow":
                    # --- Slow Mode Logic ---
                    min_sec = auto_follow_interval_minutes[0] * 60
                    max_sec = auto_follow_interval_minutes[1] * 60
                    # Ensure min <= max
                    if min_sec > max_sec: min_sec = max_sec
                    follow_interval = random.uniform(min_sec, max_sec)

                    if current_account_usernames_to_follow and (time.time() - last_follow_attempt_time > follow_interval):
                        username_to_try = random.choice(current_account_usernames_to_follow)
                        current_account_username_log = get_current_account_username() or "Unknown"
                        print(f"[Auto-Follow SLOW @{current_account_username_log}] Starting attempt for: @{username_to_try} (Interval: {follow_interval:.0f}s)")
                        await pause_scraping() # Pause for the follow attempt
                        follow_result = None
                        try:
                            follow_result = await follow_user(username_to_try)
                            if follow_result is True or follow_result == "already_following":
                                print(f"[Auto-Follow SLOW @{current_account_username_log}] Success/Already followed @{username_to_try}. Removing from list.")
                                if username_to_try in current_account_usernames_to_follow:
                                     current_account_usernames_to_follow.remove(username_to_try)
                                     save_current_account_follow_list()
                                else: print(f"Warning: @{username_to_try} no longer found in list."); save_current_account_follow_list()
                                # Update global/backup
                                if username_to_try not in global_followed_users_set:
                                     global_followed_users_set.add(username_to_try); add_to_set_file({username_to_try}, GLOBAL_FOLLOWED_FILE); print(f"@{username_to_try} added to global list.")
                                backup_filepath = get_current_backup_file_path();
                                if backup_filepath: add_to_set_file({username_to_try}, backup_filepath)
                            else: print(f"[Auto-Follow SLOW @{current_account_username_log}] Error with @{username_to_try}. Remains in list.")
                        except Exception as follow_err: print(f"[Auto-Follow SLOW @{current_account_username_log}] Critical error with @{username_to_try}: {follow_err}")
                        finally:
                            last_follow_attempt_time = time.time() # Update timestamp
                            await resume_scraping() # Resume scraping
                            await asyncio.sleep(random.uniform(3, 5)) # Short pause after attempt
                        # After a follow attempt (successful or not), jump directly to the next loop iteration
                        continue

                elif auto_follow_mode == "fast":
                    # --- Fast Mode Logic ---
                    # Check if the task should be started (only if not already running AND list is not empty)
                    if not is_fast_follow_running and current_account_usernames_to_follow:
                        current_account_username_log = get_current_account_username() or "Unknown"
                        logger.info(f"[Auto-Follow FAST @{current_account_username_log}] Mode is 'fast' and task is not running. Starting Fast-Follow task...")
                        # Start the task in the background. It manages pause/resume itself.
                        # No 'update' object needed, as started automatically.
                        asyncio.create_task(fast_follow_logic(None))
                        # No 'continue' here, the main loop continues while the task works.
                        # The task sets the mode to 'off' at the end.
                    # If task is already running or list is empty, nothing happens here.

                # --- End Auto-Follow Check ---

                # --- Queue Check 1: Before post Processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action
                # --- Scroll to Top before processing ---
                try:
                    #print("[Run Loop] Scrolling to top...") # Optional Debug
                    driver.execute_script("window.scrollTo(0, 0);")
                    await asyncio.sleep(random.uniform(0.5, 1.0)) # Short wait after scroll up
                except Exception as scroll_err:
                    print(f"Error scrolling to top: {scroll_err}")
                # --- End Scroll to Top ---

                # --- Queue Check 1.5: After scrolling top, before processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action
                # --- Main Scraping Logic ---
                await process_tweets()

                # --- Queue Check 2: After post Processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action

                # --- Rate Limit Check ---
                await check_rate_limit()

                # --- Queue Check 3: After Rate Limit Check ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action

                # Decide if scrolling happens in this iteration (ONLY if not paused)
                if pause_event.is_set(): # Check if the pause event is NOT set (i.e., bot is running)
                    if random.random() < 1: # Scrolls in 80% of cases WHEN RUNNING
                        try:
                            # Random scroll distance
                            scroll_percentage = random.uniform(0.9, 3.5)
                            scroll_command = f"window.scrollBy(0, window.innerHeight * {scroll_percentage});"
                            print(f"[Run Loop] Scrolling down by {scroll_percentage:.2f} * viewport height...") # Console output
                            driver.execute_script(scroll_command)

                            # Random wait time after scrolling
                            wait_after_scroll = random.uniform(0.2, 0.7)
                            await asyncio.sleep(wait_after_scroll)
                        except Exception as scroll_err:
                            print(f"Error scrolling in run loop: {scroll_err}")
                    else:
                        # print("[Run Loop] Skipping scroll this iteration.") # Optional: Keep or remove log
                        await asyncio.sleep(random.uniform(0.5, 1.5)) # Still wait even if not scrolling
                else:
                    # If paused, just wait briefly instead of scrolling
                    # print("[Run Loop] Paused, skipping scroll.") # Optional log
                    await asyncio.sleep(1.0) # Wait 1 second if paused

                # --- Queue Check 4: After Scrolling / Pause Wait ---
                # Queue check should happen regardless of scroll
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action

                # --- Short pause at the end of the loop ---
                await asyncio.sleep(random.uniform(0.1, 0.3))

            except Exception as e:
                # ... (Your error handling as before) ...
                print(f"!! ERROR in main loop: {e} !!")
                import traceback
                traceback.print_exc()
                # Network error logic etc.
                current_time = time.time()
                if isinstance(e, (requests.exceptions.ConnectionError, TimeoutException, OSError)): # OSError for DNS etc.
                    network_error_count += 1
                    print(f"Network error detected ({network_error_count}). Waiting longer...")
                    await asyncio.sleep(60 * network_error_count) # Longer pause for repeated errors
                    if network_error_count > 3 and (current_time - last_error_time) < 600: # More than 3 errors in 10 min
                         print("ERROR: Too many network errors in a short time. Bot will stop. Please restart manually.")
                         await send_telegram_message("🚨 Too many network errors in a short time. Bot will stop. Please restart manually.")
                         # Exit the script cleanly instead of trying to restart
                         await cleanup()
                         sys.exit(1) # Exits the script with error code
                    last_error_time = current_time
                else:
                    network_error_count = 0 # Reset for other errors
                    await asyncio.sleep(3) # Standard pause for other errors
                last_error_time = current_time # Remember time of last error

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Stopping bot...")
    except Exception as e:
        print(f"\n!! CRITICAL ERROR outside main loop: {e} !!")
        import traceback
        traceback.print_exc()
    finally:
        print("Executing cleanup...")
        await cleanup()
        # ... (Rest of your cleanup for Telegram) ...
        if application and application.running:
            print("Stopping Telegram..."); await application.updater.stop(); await application.stop(); await application.shutdown(); print("Telegram stopped.")
        print("Cleanup completed. Script finished.")

async def show_ratings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the collected ratings, including Top 3."""
    global ratings_data
    load_ratings() # Ensure the latest data is loaded

    if not ratings_data:
        await update.message.reply_text("📊 No ratings available yet.")
        await resume_scraping() # Resume after command
        return

    source_averages = []
    # Calculate average for all sources first
    for source_key, data in ratings_data.items():
        # Ensure 'ratings' exists and is a dictionary
        rating_counts = data.get("ratings", {})
        if not isinstance(rating_counts, dict):
            print(f"WARNING: Invalid rating data for {source_key}, skipping.")
            continue

        # Get the name, fall back to the key if not present
        display_name = data.get("name", source_key)

        total_ratings = 0
        weighted_sum = 0
        for star_str, count in rating_counts.items():
            try:
                star = int(star_str)
                if 1 <= star <= 5:
                    total_ratings += count
                    weighted_sum += star * count
            except ValueError:
                continue # Ignore invalid keys

        if total_ratings > 0:
            average = weighted_sum / total_ratings
            source_averages.append({
                "key": source_key,
                "name": display_name,
                "average": average,
                "total_ratings": total_ratings,
                "counts": rating_counts # Keep counts for detail view
            })
        else:
             # Optional: Add sources without ratings if desired (not relevant for Top 3 here)
             pass

    # Sort by average (descending)
    sorted_averages = sorted(source_averages, key=lambda item: item["average"], reverse=True)

    output_messages = []
    current_message = ""

    # ===  Top 3 Section ===
    top_3_output = "🏆 <b>Top 3 Rated Sources</b> 🏆\n"
    top_3_output += "     ⚜️⚜️⚜️\n"
    if not sorted_averages:
        top_3_output += "<i>(No sources with ratings yet)</i>\n"
    else:
        # Define medal emojis
        medals = ["🥇", "🥈", "🥉"]
        for i, item in enumerate(sorted_averages[:3]):
            # Get the corresponding medal, default to empty string if index out of bounds (shouldn't happen with [:3])
            medal = medals[i] if i < len(medals) else ""
            # Show Name and Handle (Key) with medal
            top_3_output += (f"{medal} {html.escape(item['name'])} ({html.escape(item['key'])}) "
                             f"~ {item['average']:.2f} ⭐ ({item['total_ratings']} Ratings)\n")
    top_3_output += "     ⚜️⚜️⚜️\n"
    current_message += top_3_output
    # === END Top 3 Section ===

    current_message += "\n📊 <b>All Ratings (Detail):</b>\n" # Heading for details

    # Add details for all sources (sorted by key for consistency)
    # Sort the original data by key
    all_sorted_sources = sorted(ratings_data.items())

    for source_key, data in all_sorted_sources:
        # Get the data again or use the already calculated ones if available
        # Here we get them again to ensure all are displayed
        display_name = data.get("name", source_key)
        rating_counts = data.get("ratings", {})
        if not isinstance(rating_counts, dict): continue # Skip invalid

        source_output = f"\n<b>{html.escape(display_name)} ({html.escape(source_key)})</b>\n"

        total_ratings = 0
        weighted_sum = 0
        details = ""
        for star in range(1, 6):
            star_str = str(star)
            count = rating_counts.get(star_str, 0)
            details += f"{star} ⭐ - {count}\n"
            total_ratings += count
            weighted_sum += star * count

        if total_ratings > 0:
            average = weighted_sum / total_ratings
            avg_str = f"⭐ ~ {average:.2f}"
        else:
            avg_str = "⭐ ~ N/A"

        source_output += details + avg_str

        # Check if the message gets too long
        if len(current_message) + len(source_output) > 4000: # Leave some buffer
            output_messages.append(current_message)
            current_message = source_output.lstrip() # New message
        else:
            current_message += source_output

    output_messages.append(current_message) # Add the last message

    # Send the messages
    for msg in output_messages:
        if msg.strip(): # Only send if the message is not empty
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            time.sleep(0.5) # Small pause

    await resume_scraping() # Resume after command

# --- Admin Management Commands ---
async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Adds a new admin (admins only)."""
    global admin_user_ids
    # --- CHANGED: Check argument count (exactly 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Please provide exactly ONE Telegram User ID after the command.\nFormat: `/addadmin <user_id>`")
        return
    # --- END CHANGE ---

    try:
        new_admin_id = int(context.args[0])
        if new_admin_id in admin_user_ids:
            await update.message.reply_text(f"ℹ️ User ID {new_admin_id} is already an admin.")
        else:
            admin_user_ids.add(new_admin_id)
            save_admins()
            await update.message.reply_text(f"✅ User ID {new_admin_id} successfully added as admin.")
            logger.info(f"Admin {update.message.from_user.id} added new admin {new_admin_id}")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID. Please provide a number.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error adding admin: {e}")
        logger.error(f"Error adding admin {context.args[0]}: {e}", exc_info=True)

async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes an admin (admins only)."""
    global admin_user_ids
    current_user_id = update.message.from_user.id

    # --- CHANGED: Check argument count (exactly 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Please provide exactly ONE Telegram User ID after the command.\nFormat: `/removeadmin <user_id>`")
        return
    # --- END CHANGE ---

    try:
        admin_id_to_remove = int(context.args[0])

        # Safety check: Prevent removing the last admin
        if len(admin_user_ids) <= 1 and admin_id_to_remove in admin_user_ids:
             await update.message.reply_text("⚠️ Action not allowed: This is the last admin.")
             return

        # Optional: Prevent self-removal (can also be allowed)
        # if admin_id_to_remove == current_user_id:
        #     await update.message.reply_text("⚠️ You cannot remove yourself.")
        #     return

        if admin_id_to_remove in admin_user_ids:
            admin_user_ids.remove(admin_id_to_remove)
            save_admins()
            await update.message.reply_text(f"🗑️ User ID {admin_id_to_remove} successfully removed as admin.")
            logger.info(f"Admin {current_user_id} removed admin {admin_id_to_remove}")
        else:
            await update.message.reply_text(f"ℹ️ User ID {admin_id_to_remove} was not found in the admin list.")
    except ValueError:
        await update.message.reply_text("❌ Invalid User ID. Please provide a number.")
    except Exception as e:
        await update.message.reply_text(f"❌ Error removing admin: {e}")
        logger.error(f"Error removing admin {context.args[0]}: {e}", exc_info=True)

async def list_admins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists all current admin User IDs (admins only)."""
    global admin_user_ids
    if not admin_user_ids:
        await update.message.reply_text("ℹ️ No admins are currently defined.")
        return

    admin_list_str = "\n".join([f"- `{uid}`" for uid in sorted(list(admin_user_ids))])
    await update.message.reply_text(f"👑 Current Admin User IDs:\n{admin_list_str}", parse_mode=ParseMode.MARKDOWN)

# --- End Admin Management Commands ---

async def set_max_age_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sets the maximum age (in minutes) for tweets to be processed."""
    global max_tweet_age_minutes
    # pause/resume is handled by the admin wrapper

    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            f"ℹ️ Please provide the maximum age in minutes.\n"
            f"Current: {max_tweet_age_minutes} min\n\n"
            f"Format: `/setmaxage <minutes>`\n\n"
            f"Example: `/setmaxage {max_tweet_age_minutes}`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    try:
        new_age = int(context.args[0])
        if new_age >= 1: # Must be at least 1 minute
            max_tweet_age_minutes = new_age
            save_settings()
            await update.message.reply_text(f"✅ Maximum post age set to {new_age} minutes.")
            logger.info(f"Maximum post age set to {new_age} minutes by user {update.message.from_user.id}")
        else:
            await update.message.reply_text("❌ Age must be at least 1 minute.")
    except ValueError:
        await update.message.reply_text("❌ Invalid input. Please provide a whole number (minutes).")
    except Exception as e:
        await update.message.reply_text(f"❌ Error setting max age: {e}")
        logger.error(f"Error setting max post age to '{context.args[0]}': {e}", exc_info=True)

# --- Headless Mode Toggle Command ---
async def toggle_headless_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggles the Headless mode ON/OFF (requires restart)."""
    global is_headless_enabled
    # is_scraping_paused is handled by the admin wrapper

    # Toggle the setting
    is_headless_enabled = not is_headless_enabled
    save_settings() # Save the new state

    # Send confirmation message
    status_text = "ENABLED 🟢" if is_headless_enabled else "DISABLED 🔴"
    await update.message.reply_text(f"✅ Headless mode toggled to {status_text}. Restarting WebDriver...")
    logger.info(f"Headless mode toggled to {status_text} by user {update.message.from_user.id} via command. Triggering driver restart.")

    # Call the restart helper function
    await restart_driver_and_login(update)

    # resume_scraping is handled by the admin wrapper AND the helper function
# --- End Headless Mode Toggle Command ---


# --- Helper Function for Driver Restart ---
async def restart_driver_and_login(update_or_query):
    """Quits the current driver, creates a new one based on current settings, and logs in."""
    global driver, last_driver_restart_time, application # Need application for sending messages if update_or_query is None

    # Determine how to send messages
    message_sender = None
    if hasattr(update_or_query, 'message') and update_or_query.message:
        message_sender = update_or_query.message # From CommandHandler update or CallbackQuery
    elif application: # Fallback for internal calls without update context
        message_sender = application.bot
    else:
        logger.error("Cannot send message in restart_driver_and_login: No update/query or application.")
        # Proceed with restart attempt anyway, but without user feedback

    async def send_msg(text):
        if message_sender:
            try:
                if hasattr(message_sender, 'reply_text'):
                    await message_sender.reply_text(text)
                elif hasattr(message_sender, 'send_message') and CHANNEL_ID: # If it's the bot object
                    await message_sender.send_message(chat_id=CHANNEL_ID, text=text)
                else:
                    logger.warning(f"Could not send restart message: {text}")
            except Exception as send_err:
                logger.error(f"Error sending message during driver restart: {send_err}")
        else:
            print(f"INFO (No Sender): {text}") # Log to console if no sender

    await send_msg("🔄 Restarting WebDriver due to settings change...")
    await pause_scraping() # Pause main scraping

    login_ok_after_restart = False
    try:
        # Close old driver safely
        if driver:
            print("Closing old WebDriver...")
            await asyncio.sleep(1.0) # Kleine Pause VOR dem Schließen
            try:
                driver.quit()
                await asyncio.sleep(1.5) # Etwas längere Pause NACH dem Schließen
            except Exception as quit_err:
                print(f"WARNING: Error closing old driver (possibly already closed or timed out): {quit_err}")
                # Auch wenn quit() fehlschlägt, versuchen wir Prozesse zu beenden

        # --- Force kill lingering processes (Linux focused) ---
        # Dies hilft, wenn driver.quit() hängt oder nicht alle Prozesse beendet
        print("Attempting to forcefully kill lingering browser/driver processes...")
        try:
            # Befehle zum Beenden von Chrome/Chromium und Chromedriver
            # -f matcht den vollen Pfad/Argumente, SIGTERM (15) ist Standard (graceful)
            kill_commands = [
                ['pkill', '-f', 'chrome'],
                ['pkill', '-f', 'chromium'],
                ['pkill', '-f', 'chromedriver']
            ]
            for cmd in kill_commands:
                try:
                    # Kurzer Timeout für jeden Kill-Befehl
                    subprocess.run(cmd, timeout=5, check=False, capture_output=True)
                    await asyncio.sleep(0.3) # Kurze Pause zwischen Kills
                except FileNotFoundError:
                    print(f"WARNING: Command '{cmd[0]}' not found. Skipping kill.")
                    break # Wenn pkill nicht da ist, brauchen wir nicht weiterzumachen
                except subprocess.TimeoutExpired:
                    print(f"WARNING: Command '{' '.join(cmd)}' timed out.")
                except Exception as run_err:
                     print(f"WARNING: Error running kill command '{' '.join(cmd)}': {run_err}")

            print("Process kill commands executed (attempted).")
            await asyncio.sleep(1.5) # Längere Pause nach den Kill-Versuchen
        except Exception as kill_err:
            print(f"WARNING: Error during force kill block: {kill_err}")
        # --- End force kill ---

        driver = None # Explizit auf None setzen, falls quit() fehlschlug oder für Klarheit

        # Create new driver (will read the new headless setting)
        print("Creating new WebDriver...")
        await asyncio.sleep(2.0) # Zusätzliche Pause VOR dem Erstellen des neuen Drivers
        driver = create_driver()

        # Log in again
        print("Attempting login with current account...")
        if await login(): # login() uses global current_account
            print("Login after WebDriver restart successful.")
            await switch_to_following_tab() # Important: Switch to Following tab
            await send_msg("✅ WebDriver restart and login successful. New settings are active.")
            last_driver_restart_time = time.time() # Update timestamp
            login_ok_after_restart = True
        else:
            print("ERROR: Login after WebDriver restart failed!")
            await send_msg("❌ ERROR: Login after WebDriver restart failed! Please check credentials or try switching accounts.")
            # Driver might exist but login failed
            if driver:
                try: driver.quit()
                except: pass
                driver = None

    except Exception as restart_err:
        print(f"ERROR during WebDriver restart/login: {restart_err}")
        logger.error("Exception during WebDriver restart/login", exc_info=True)
        await send_msg(f"❌ Critical error during WebDriver restart: {str(restart_err)[:200]}")
        # Ensure driver is None if creation/login failed critically
        if driver:
            try: driver.quit()
            except: pass
        driver = None
    finally:
        # --- Start Queued Scrapes (only if login succeeded and headless is OFF) ---
        if login_ok_after_restart and not is_headless_enabled:
            queued_usernames = read_and_clear_scrape_queue()
            if queued_usernames:
                logger.info(f"Found {len(queued_usernames)} usernames in scrape queue after restart. Starting tasks...")
                await send_msg(f"✅ Headless mode disabled. Starting {len(queued_usernames)} queued scrape task(s)...")
                # Use asyncio.create_task to run scrapes concurrently after restart
                tasks = []
                processed_for_tasks = set()
                for username in queued_usernames:
                     if username not in processed_for_tasks:
                        logger.info(f"  -> Creating scrape task for @{username}")
                        # Pass None for update, task will use send_telegram_message
                        # Run scrape_target_following directly as a task
                        task = asyncio.create_task(scrape_target_following(None, username))
                        tasks.append(task)
                        processed_for_tasks.add(username)
                        await asyncio.sleep(1) # Stagger task creation slightly
                # Optional: Wait for these initial tasks if needed, or let them run in background
                # await asyncio.gather(*tasks)
                logger.info(f"Launched {len(tasks)} scrape tasks from queue.")
            else:
                logger.info("Scrape queue is empty after restart.")
        elif login_ok_after_restart and is_headless_enabled:
             logger.info("Headless mode is still enabled after restart, not processing scrape queue.")
        elif not login_ok_after_restart:
             logger.warning("Login failed after restart, not processing scrape queue.")
        # --- End Start Queued Scrapes ---

        await resume_scraping() # Resume scraping regardless of outcome
        await asyncio.sleep(2) # Short pause after the whole process

    return login_ok_after_restart
# --- End Helper Function ---

# --- Your cleanup() function ---
async def cleanup():
    """Clean up resources on exit"""
    global driver
    print("Attempting to close WebDriver...")
    if driver:
        try:
            driver.quit()
            print("WebDriver closed successfully.")
            driver = None
        except Exception as e:
            print(f"Error closing WebDriver: {e}")
    else:
        print("No active WebDriver found to close.")


# ... (Rest of your script: __main__ block etc.) ...
# Ensure the __main__ block calls `asyncio.run(run())`
if __name__ == '__main__':
    # ... (Your argument parsing code to set ACTIVE_BOT_TOKEN and current_account) ...
    # Default values
    account_index_to_use = 0
    bot_token_to_use = DEFAULT_BOT_TOKEN
    bot_identifier = "Default"

    # === ASCII Art Animation at Start (AFTER nest_asyncio) ===
    try:
        # Ensure the function and variable are defined above
        display_ascii_animation(ascii_art)
    except NameError:
        print("WARNING: Could not display ASCII art (function/variable not found).")
    # =====================================

    # Argument Parsing (as provided by you)
    num_args = len(sys.argv)
    valid_args = True
    if num_args == 1:
        print("")
        print("")
        print("No argument provided. Starting with default bot and Account 1.")
    elif num_args == 2:
        arg1 = sys.argv[1]
        if arg1.lower() == "test":
            print("")
            print("")
            print("Argument 'test' recognized. Starting with test bot and Account 1.")
            bot_token_to_use = TEST_BOT_TOKEN
            bot_identifier = "Test"
            if not bot_token_to_use: print("ERROR: TEST_BOT_TOKEN not in config.env!"); valid_args = False
        else:
            try:
                req_acc_num = int(arg1); req_idx = req_acc_num - 1
                if 0 <= req_idx < len(ACCOUNTS):
                    print(f"Argument '{arg1}' recognized as account no. Starting default bot, Account {req_acc_num}.")
                    account_index_to_use = req_idx
                else: print(f"Warning: Invalid account no. '{req_acc_num}'. Available: 1-{len(ACCOUNTS)}."); valid_args = False
            except ValueError: print(f"Warning: Invalid argument '{arg1}'. Expected: Account no. or 'test'."); valid_args = False
        if not valid_args: print("Using default bot and Account 1 as fallback."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default"; valid_args = True
    elif num_args == 3:
        arg1, arg2 = sys.argv[1], sys.argv[2]
        if arg1.lower() == "test": bot_token_to_use = TEST_BOT_TOKEN; bot_identifier = "Test";
        elif arg1.lower() == "default": bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default";
        else: print(f"Warning: Invalid bot ID '{arg1}'. Expected: 'test'/'default'."); valid_args = False;
        if valid_args:
             try:
                 req_acc_num = int(arg2); req_idx = req_acc_num - 1
                 if 0 <= req_idx < len(ACCOUNTS): account_index_to_use = req_idx
                 else: print(f"Warning: Invalid account no. '{req_acc_num}'. Available: 1-{len(ACCOUNTS)}."); valid_args = False
             except ValueError: print(f"Warning: Invalid account no. '{arg2}'."); valid_args = False
        if not valid_args: print("Invalid arguments. Starting default bot, Account 1."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default"; valid_args = True
    else: print("Warning: Too many arguments. Starting default bot, Account 1."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default";

    # Final check and set globals
    if not bot_token_to_use: print("ERROR: No valid bot token (Default/Test). Check config.env!"); sys.exit(1)
    if not (0 <= account_index_to_use < len(ACCOUNTS)): print(f"ERROR: Internal error - Account index {account_index_to_use} invalid."); sys.exit(1)

    ACTIVE_BOT_TOKEN = bot_token_to_use
    current_account = account_index_to_use

    print(f"\n\n\n---> Starting script with Bot: '{bot_identifier}', Account: {account_index_to_use + 1} <---\n")

    # Start script
    try:
        # Get or create event loop and run run()
        asyncio.run(run()) # Preferred method in Python 3.7+
    except KeyboardInterrupt:
        print("Bot stopped due to KeyboardInterrupt...")
    except RuntimeError as e:
         if "Cannot run the event loop while another loop is running" in str(e) or "This event loop is already running" in str(e):
              print("Error: Event loop conflict. Trying nest_asyncio (already applied). Script might be unstable.")
         else:
             print(f"Unexpected runtime error in __main__: {e}")
             import traceback
             traceback.print_exc()
    except Exception as e:
        print(f"Unexpected error in __main__: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Script execution finished.")