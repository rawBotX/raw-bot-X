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
    #print("INFO: Verwende 'zoneinfo' (Python 3.9+) für Zeitzonen.")
except ImportError:
    try:
        import pytz
        print("INFO: 'zoneinfo' nicht gefunden, verwende 'pytz' als Fallback.")
        # Erstelle eine Wrapper-Klasse oder Funktion, die pytz wie zoneinfo verwendet
        # (Diese Klasse ist optional, aber macht den Code konsistenter)
        class ZoneInfo(pytz.tzinfo.BaseTzInfo):
            def __init__(self, zone):
                self.zone = zone
                self._pytz_zone = pytz.timezone(zone)
            def utcoffset(self, dt): return self._pytz_zone.utcoffset(dt)
            def dst(self, dt): return self._pytz_zone.dst(dt)
            def tzname(self, dt): return self._pytz_zone.tzname(dt)
            def __reduce__(self): return (self.__class__, (self.zone,))

    except ImportError:
        #print("\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("FEHLER: Weder 'zoneinfo' noch 'pytz' gefunden.")
        # ... (Restlicher Fehlerblock und Fallback wie in deinem Original) ...
        class FixedOffsetZone(timezone): # Dein Fallback
             def __init__(self, offset_hours=2, name="UTC+02:00_Fallback"):
                 self._offset = timedelta(hours=offset_hours)
                 self._name = name
                 super().__init__(self._offset, self._name)
             def __reduce__(self):
                 return (self.__class__, (self._offset.total_seconds() / 3600, self._name))
        ZoneInfo = lambda tz_name: FixedOffsetZone()
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from telegram import InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler
from telegram.constants import ParseMode
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
# Lade die initiale Admin-ID aus der .env-Datei (wichtig für den ersten Start!)
# Füge eine Zeile ADMIN_USER_ID=<deine_telegram_user_id> zu deiner config.env hinzu
# Du kannst deine ID z.B. mit @userinfobot in Telegram herausfinden.
INITIAL_ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
admin_user_ids = set() # Wird beim Start geladen
# --- End Admin Configuration ---


# 2. Füge Funktionen zum Laden und Speichern der Admin-Liste hinzu
#    Platziere diese z.B. nach den save/load_settings Funktionen


# Konfiguriere Logging (optional, aber empfohlen)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO # Dein globales Level bleibt INFO
)
# Ersetze 'your_bot_module_name' ggf. durch den Namen deines Skripts/Moduls
logger = logging.getLogger(__name__) # Standard Python Logger verwenden

# Setze das Logging-Level für httpx höher, um INFO-Meldungen zu unterdrücken
logging.getLogger("httpx").setLevel(logging.WARNING)

# Configuration
SETTINGS_FILE = "settings.json" # Für Persistenz
KEYWORDS_FILE = "keywords.json"
# Lade Keywords aus Datei oder verwende Standardwerte
try:
    with open(KEYWORDS_FILE, 'r') as f:
        KEYWORDS = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    KEYWORDS = ["token", "meme", "coin"]
    # Erstelle die Datei mit Standardwerten
    with open(KEYWORDS_FILE, 'w') as f:
        json.dump(KEYWORDS, f)

TOKEN_PATTERN = r"(?<![/\"'=&?])(?:\b[A-Za-z0-9]{32,}\b)(?![/\"'&?])"
#BOTTOM_BORDER = ""
AUTH_CODE = None
WAITING_FOR_AUTH = False

# .env-Datei laden
try:
    # Optional: Expliziten Pfad relativ zum Skript verwenden
    # script_dir = os.path.dirname(__file__)
    # dotenv_path = os.path.join(script_dir, 'config.env')
    # print(f"DEBUG: Versuche .env zu laden von: {dotenv_path}")
    # loaded = load_dotenv(dotenv_path=dotenv_path, verbose=True)

    # Oder bleibe beim relativen Pfad zum Arbeitsverzeichnis
    config_path_load = os.path.abspath("config.env") # Pfad für die Meldung holen
    print(f"DEBUG: Versuche .env zu laden von: {config_path_load}")
    loaded = load_dotenv("config.env", verbose=True) # verbose=True für mehr Infos

    print(f"DEBUG: load_dotenv erfolgreich? {loaded}")
    if not loaded:
         print("WARNUNG: load_dotenv meldet, dass die Datei nicht geladen wurde oder leer war.")
except Exception as e_dotenv:
    print(f"FATAL: Fehler beim Ausführen von load_dotenv: {e_dotenv}")
    import traceback
    traceback.print_exc()

# Direkt danach prüfen, was os.getenv liefert:
check_admin_id = os.getenv("ADMIN_USER_ID")
# Diese print-Anweisung muss in einer EIGENEN Zeile stehen:
print(f"DEBUG: os.getenv('ADMIN_USER_ID') nach load_dotenv: '{check_admin_id}'")

# Die ursprünglichen Zuweisungen folgen DANACH:
DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN")
TEST_BOT_TOKEN = os.getenv("BOT_TEST_TOKEN") # NEU: Test-Token laden
CHANNEL_ID = os.getenv("CHANNEL_ID")
# --- Admin Configuration --- (Diese Zeilen bleiben hier)
ADMINS_FILE = "admins.json"
INITIAL_ADMIN_USER_ID = os.getenv("ADMIN_USER_ID") # Diese Zuweisung ist korrekt hier
admin_user_ids = set() # Wird beim Start geladen
# --- End Admin Configuration ---

# [END - Fix Debug Print Syntax & Check load_dotenv]

DEFAULT_BOT_TOKEN = os.getenv("BOT_TOKEN")
TEST_BOT_TOKEN = os.getenv("BOT_TEST_TOKEN") # NEU: Test-Token laden
CHANNEL_ID = os.getenv("CHANNEL_ID")

# NEU: Globale Variable für den *aktuell* zu verwendenden Bot-Token
ACTIVE_BOT_TOKEN = None # Wird im main-Block gesetzt

# === Dynamische Account-Erstellung ===
ACCOUNTS = []
account_index = 1
while True:
    email = os.getenv(f"ACCOUNT_{account_index}_EMAIL")
    if not email:
        break

    password = os.getenv(f"ACCOUNT_{account_index}_PASSWORD")
    username = os.getenv(f"ACCOUNT_{account_index}_USERNAME")
    # --- GEÄNDERT: Cookie-Datei-Handling ---
    cookies_file_env = os.getenv(f"ACCOUNT_{account_index}_COOKIES")
    # Fallback auf Standardnamen, wenn nicht in .env oder leer
    if not cookies_file_env:
        safe_username_for_file = re.sub(r'[\\/*?:"<>|]', "_", username) if username else f"account_{account_index}"
        cookies_file = f"{safe_username_for_file}_cookies.cookies.json" # Standard-Dateiname mit .cookies.json
        print(f"WARNUNG: ACCOUNT_{account_index}_COOKIES nicht in .env gefunden/leer. Verwende Standard: {cookies_file}")
    else:
        cookies_file = cookies_file_env
        # Stelle sicher, dass die Endung korrekt ist (optional, aber gut für Konsistenz)
        if not cookies_file.endswith(".cookies.json"):
             print(f"WARNUNG: Cookie-Datei '{cookies_file}' für Account {account_index} endet nicht auf '.cookies.json'. Empfohlene Endung verwenden.")
             # Optional: Endung erzwingen (könnte bestehende Dateien umbenennen)
             # cookies_file = os.path.splitext(cookies_file)[0] + ".cookies.json"
    # --- ENDE ÄNDERUNG ---

    ACCOUNTS.append({
        "email": email,
        "password": password,
        "username": username,
        "cookies_file": cookies_file, # Verwende die (ggf. korrigierte) Variable
    })
    print(f"INFO: Account {account_index} ({username or email}) geladen. Cookie-Datei: {cookies_file}")
    account_index += 1

if not ACCOUNTS:
    print("FEHLER: Keine Account-Daten in config.env gefunden! Bitte mindestens ACCOUNT_1_EMAIL etc. definieren.")
    import sys
    sys.exit(1)
# === Ende Dynamische Account-Erstellung ===

# ===> NEU/GEÄNDERT: Konstanten und Variablen für Follow-Funktionen <===
FOLLOW_LIST_TEMPLATE = "add_contacts_{}.txt"     # Template für Account-Follow-Listen
FOLLOWER_BACKUP_TEMPLATE = "follower_backup_{}.txt" # Template für Account-Backups
GLOBAL_FOLLOWED_FILE = "global_followed_users.txt" # Zentrale Liste aller gefolgten User

# Globale Sets für schnellen Zugriff (werden beim Start/Backup geladen/aktualisiert)
global_followed_users_set = set()
# Die account-spezifische Liste wird jetzt dynamisch geladen
current_account_usernames_to_follow = [] # Wird beim Start/Wechsel geladen

last_follow_attempt_time = time.time()
is_periodic_follow_active = True # Steuerungs-Flag für Auto-Follow
# ------------------------------------------------------------------------------------

# Global variables
dnd_mode_enabled = False
is_backup_running = False
cancel_backup_flag = False
is_sync_running = False
cancel_sync_flag = False
PROCESSED_TWEETS_MAXLEN = 200 # Wähle eine passende Größe
processed_tweets = deque(maxlen=PROCESSED_TWEETS_MAXLEN)
current_account = 0
driver = None
application = None
last_like_time = time.time()
last_driver_restart_time = time.time() 
# Counter für Login-Versuche
login_attempts = 0
# Neue Variablen für Pause-Mechanismus
is_scraping_paused = False
pause_event = asyncio.Event()
pause_event.set()  # Standardmäßig nicht pausiert
is_schedule_pause = False  # Flag um zu unterscheiden, ob Pause vom Scheduler kommt
# Variable to track first run for optimized tweet processing
first_run = True
# Suchmodus: "full" für CA + Keywords, "ca_only" für nur CA
search_mode = "full"

# ===> Following Database <===
FOLLOWING_DB_FILE = "following_database.json"
following_database = {} # Wird beim Start geladen
is_db_scrape_running = False # Flag für Nebenläufigkeit
cancel_db_scrape_flag = False # Flag zum Abbrechen
# ===> END Following Database <===

# Post counting variables
POSTS_COUNT_FILE = "posts_count.json"
# Schedule variables
SCHEDULE_FILE = "schedule.json"
schedule_enabled = False
schedule_pause_start = "00:00"  # Default start time in 24-hour format
schedule_pause_end = "00:00"    # Default end time in 24-hour format
posts_count = {
    "found": {
        "today": 0,
        "yesterday": 0,
        "vorgestern": 0,
        "total": 0
    },
    "scanned": {
        "today": 0,
        "yesterday": 0,
        "vorgestern": 0,
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

# ===> NEU: Rating-System <===
RATINGS_FILE = "ratings.json"
ratings_data = {} # Wird beim Start geladen
# ===> ENDE Rating-System <===

# Speichert Tweet-URLs für die letzten Nachrichten
last_tweet_urls = {}

rate_limit_patterns = [
    '//span[contains(text(), "unlock more posts by subscribing")]',
    '//span[contains(text(), "Subscribe to Premium")]',
    '//span[contains(text(), "rate limit")]',
    '//div[contains(text(), "Something went wrong")]'
]

def display_ascii_animation(art_lines, delay_min=0.05, delay_max=0.1):
    """Gibt ASCII-Art Zeile für Zeile mit Verzögerung und Flush aus."""
    print("\n" * 2) # Leerzeilen davor
    for line in art_lines:
        print(line)
        sys.stdout.flush() # <<<--- Puffer leeren
        time.sleep(random.uniform(delay_min, delay_max))

def load_settings():
    """Lädt Einstellungen aus der Datei, inklusive Scraping- und Auto-Follow-Status."""
    global dnd_mode_enabled, search_mode, is_scraping_paused, is_periodic_follow_active, pause_event

    # --- Standardwerte definieren ---
    default_dnd = False
    default_search_mode = "full"
    default_scraping_paused = True  # Standard: PAUSIERT
    default_autofollow_active = False # Standard: AUS

    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f:
                settings = json.load(f)
                dnd_mode_enabled = settings.get("dnd_mode_enabled", default_dnd)
                search_mode = settings.get("search_mode", default_search_mode)
                is_scraping_paused = settings.get("is_scraping_paused", default_scraping_paused)
                is_periodic_follow_active = settings.get("is_periodic_follow_active", default_autofollow_active)
                print(f"Einstellungen geladen:")
                print(f"  - DND-Modus: {'AN' if dnd_mode_enabled else 'AUS'}")
                print(f"  - Suchmodus: {search_mode}")
                print(f"  - Scraping: {'PAUSIERT' if is_scraping_paused else 'AKTIV'}")
                print(f"  - Auto-Follow: {'AKTIV' if is_periodic_follow_active else 'AUS'}")
        else:
            print("Keine Einstellungsdatei gefunden, setze Standardwerte und erstelle Datei...")
            dnd_mode_enabled = default_dnd
            search_mode = default_search_mode
            is_scraping_paused = default_scraping_paused
            is_periodic_follow_active = default_autofollow_active
            # Speichere die Standardwerte sofort, um die Datei zu erstellen
            save_settings() # save_settings muss die neuen Keys kennen!
            print(f"Standard-Einstellungsdatei '{SETTINGS_FILE}' wurde erstellt.")

    except (json.JSONDecodeError, Exception) as e:
        print(f"Fehler beim Laden der Einstellungen ({type(e).__name__}): {e}. Verwende Standardwerte.")
        dnd_mode_enabled = default_dnd
        search_mode = default_search_mode
        is_scraping_paused = default_scraping_paused
        is_periodic_follow_active = default_autofollow_active

    # --- WICHTIG: asyncio.Event basierend auf geladenem Status setzen ---
    if is_scraping_paused:
        pause_event.clear() # Pausiert
    else:
        pause_event.set()   # Läuft

def save_settings():
    """Speichert aktuelle Einstellungen in die Datei, inkl. Scraping/Auto-Follow."""
    global dnd_mode_enabled, search_mode, is_scraping_paused, is_periodic_follow_active
    try:
        settings = {
            "dnd_mode_enabled": dnd_mode_enabled,
            "search_mode": search_mode,
            "is_scraping_paused": is_scraping_paused,           # Hinzugefügt
            "is_periodic_follow_active": is_periodic_follow_active # Hinzugefügt
        }
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(settings, f, indent=4)
        # print("Einstellungen gespeichert.") # Optionales Logging
    except Exception as e:
        print(f"Fehler beim Speichern der Einstellungen: {e}")

def load_admins():
    """Lädt Admin-User-IDs aus der Datei."""
    global admin_user_ids
    try:
        if os.path.exists(ADMINS_FILE):
            with open(ADMINS_FILE, 'r') as f:
                data = json.load(f)
                # Stelle sicher, dass es eine Liste von Integers ist
                loaded_ids = data.get("admin_user_ids", [])
                admin_user_ids = {int(uid) for uid in loaded_ids if isinstance(uid, (int, str)) and str(uid).isdigit()}
                print(f"Admins geladen: {len(admin_user_ids)} User IDs.")
        else:
            print(f"Keine {ADMINS_FILE} gefunden.")
            # --- Initialer Admin Setup ---
            if INITIAL_ADMIN_USER_ID and INITIAL_ADMIN_USER_ID.isdigit():
                print(f"Füge initialen Admin aus .env hinzu: {INITIAL_ADMIN_USER_ID}")
                admin_user_ids = {int(INITIAL_ADMIN_USER_ID)}
                save_admins() # Speichere die Datei mit dem initialen Admin
            else:
                print("WARNUNG: Keine Admins-Datei und keine gültige INITIAL_ADMIN_USER_ID in .env gefunden!")
                admin_user_ids = set()
            # --- Ende Initialer Admin Setup ---

    except (json.JSONDecodeError, ValueError, TypeError) as e:
        print(f"FEHLER beim Laden oder Verarbeiten von {ADMINS_FILE}: {e}. Setze Admin-Liste zurück.")
        admin_user_ids = set()
        # Optional: Versuche erneut, initialen Admin hinzuzufügen
        if INITIAL_ADMIN_USER_ID and INITIAL_ADMIN_USER_ID.isdigit():
             admin_user_ids = {int(INITIAL_ADMIN_USER_ID)}
             save_admins()
    except Exception as e:
        print(f"Unerwarteter Fehler beim Laden von Admins: {e}")
        admin_user_ids = set()

def save_admins():
    """Speichert die aktuelle Admin-Liste in die Datei."""
    global admin_user_ids
    try:
        # Konvertiere das Set zu einer Liste für JSON-Speicherung
        data = {"admin_user_ids": sorted(list(admin_user_ids))}
        with open(ADMINS_FILE, 'w') as f:
            json.dump(data, f, indent=4)
        # print("Admin-Liste gespeichert.") # Optional
    except Exception as e:
        print(f"Fehler beim Speichern der Admin-Liste: {e}")

def is_user_admin(user_id: int) -> bool:
    """Prüft, ob eine gegebene User-ID in der Admin-Liste ist."""
    global admin_user_ids
    # Stelle sicher, dass die Liste geladen ist (obwohl sie beim Start geladen wird)
    if not admin_user_ids and os.path.exists(ADMINS_FILE):
        load_admins() # Lade neu, falls sie aus irgendeinem Grund leer ist
    return user_id in admin_user_ids

def add_admin_command_handler(application, command, callback):
    """
    Registriert einen Command Handler, der vor der Ausführung des Callbacks
    prüft, ob der Benutzer ein Admin ist.
    """
    @functools.wraps(callback)
    async def admin_check_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.message or not update.message.from_user:
            logger.warning(f"Admin check fehlgeschlagen für Befehl '{command}': Kein User-Objekt.")
            return # Kann nicht prüfen

        user_id = update.message.from_user.id
        if is_user_admin(user_id):
            # User ist Admin, führe die Originalfunktion aus
            # Wichtig: Die Originalfunktion managed ihr eigenes pause/resume
            await callback(update, context, *args, **kwargs)
        else:
            # User ist kein Admin, sende Fehlermeldung
            logger.warning(f"Nicht autorisierter Zugriff auf Befehl '{command}' durch User {user_id}.")
            await update.message.reply_text("❌ Zugriff verweigert. Du bist kein Admin.")
            # Kein automatisches resume_scraping hier, da wir nicht wissen,
            # ob der ursprüngliche Befehl pausiert hätte. Der Bot bleibt im
            # aktuellen Zustand (running/paused).

    # Registriere den Wrapper anstelle des originalen Callbacks
    application.add_handler(CommandHandler(command, admin_check_wrapper))

def load_ratings():
    """Lädt die Rating-Daten aus der Datei."""
    global ratings_data
    try:
        if os.path.exists(RATINGS_FILE):
            with open(RATINGS_FILE, 'r') as f:
                ratings_data = json.load(f)
                print(f"Rating-Daten geladen für {len(ratings_data)} Quellen.")
        else:
            ratings_data = {}
            print("Keine Rating-Datei gefunden, starte mit leerer Datenbank.")
    except json.JSONDecodeError:
        print(f"FEHLER: {RATINGS_FILE} ist korrupt. Starte mit leerer Datenbank.")
        ratings_data = {}
    except Exception as e:
        print(f"Fehler beim Laden der Rating-Daten: {e}")
        ratings_data = {}

def save_ratings():
    """Speichert die aktuellen Rating-Daten in die Datei."""
    global ratings_data
    try:
        with open(RATINGS_FILE, 'w') as f:
            json.dump(ratings_data, f, indent=4)
        # print("Rating-Daten gespeichert.") # Optional
    except Exception as e:
        print(f"Fehler beim Speichern der Rating-Daten: {e}")

def load_following_database():
    """Lädt die Following-Datenbank aus der Datei."""
    global following_database
    try:
        if os.path.exists(FOLLOWING_DB_FILE):
            with open(FOLLOWING_DB_FILE, 'r', encoding='utf-8') as f:
                following_database = json.load(f)
                print(f"Following-Datenbank geladen ({len(following_database)} Einträge).")
        else:
            following_database = {}
            print("Keine Following-Datenbank-Datei gefunden, starte mit leerer Datenbank.")
    except json.JSONDecodeError:
        print(f"FEHLER: {FOLLOWING_DB_FILE} ist korrupt. Starte mit leerer Datenbank.")
        following_database = {}
    except Exception as e:
        print(f"Fehler beim Laden der Following-Datenbank: {e}")
        following_database = {}

def save_following_database():
    """Speichert die aktuelle Following-Datenbank in die Datei."""
    global following_database
    try:
        with open(FOLLOWING_DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(following_database, f, indent=2) # indent=2 für etwas Lesbarkeit
        # print("Following-Datenbank gespeichert.") # Optional
    except Exception as e:
        print(f"Fehler beim Speichern der Following-Datenbank: {e}")

def load_set_from_file(filepath):
    """Lädt Zeilen aus einer Datei in ein Set."""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                # Entferne @ und leere Zeilen
                return {line.strip().lstrip('@') for line in f if line.strip()}
        else:
            return set()
    except Exception as e:
        print(f"Fehler beim Laden von Set aus {filepath}: {e}")
        return set()

def save_set_to_file(data_set, filepath):
    """Speichert ein Set in eine Datei, eine Zeile pro Element."""
    try:
        # Sortieren für konsistente Dateien
        sorted_list = sorted(list(data_set))
        with open(filepath, 'w') as f:
            for item in sorted_list:
                f.write(f"{item}\n") # Schreibe ohne @
    except Exception as e:
        print(f"Fehler beim Speichern von Set in {filepath}: {e}")

def add_to_set_file(data_set, filepath):
    """Fügt Elemente zu einer Datei hinzu, die ein Set repräsentiert (liest, fügt hinzu, schreibt)."""
    try:
        existing_set = load_set_from_file(filepath)
        initial_size = len(existing_set)
        updated_set = existing_set.union(data_set) # Füge neue Elemente hinzu
        if len(updated_set) > initial_size: # Nur schreiben, wenn sich was geändert hat
             save_set_to_file(updated_set, filepath)
             # print(f"Datei {filepath} aktualisiert mit {len(updated_set) - initial_size} neuen Einträgen.") # Optionales Logging
    except Exception as e:
         print(f"Fehler beim Hinzufügen zu Set-Datei {filepath}: {e}")

def get_current_account_username():
    """Gibt den Usernamen des aktuell aktiven Accounts zurück."""
    global current_account, ACCOUNTS
    if 0 <= current_account < len(ACCOUNTS):
        # Stelle sicher, dass der Key existiert und nicht None ist
        username = ACCOUNTS[current_account].get("username")
        return username if username else None # Gibt None zurück, wenn Key fehlt oder Wert None ist
    return None

def get_current_follow_list_path():
    """Gibt den Dateipfad für die Follow-Liste des aktuellen Accounts zurück."""
    username = get_current_account_username()
    if username:
        # Ersetze ungültige Zeichen für Dateinamen, falls nötig (obwohl Usernames sicher sein sollten)
        safe_username = re.sub(r'[\\/*?:"<>|]', "_", username)
        return FOLLOW_LIST_TEMPLATE.format(safe_username)
    return None

def get_current_backup_file_path():
    """Gibt den Dateipfad für die Backup-Datei des aktuellen Accounts zurück."""
    username = get_current_account_username()
    if username:
        safe_username = re.sub(r'[\\/*?:"<>|]', "_", username)
        return FOLLOWER_BACKUP_TEMPLATE.format(safe_username)
    return None

def create_driver():  
    options = webdriver.ChromeOptions()
    # Enhanced anti-detection settings
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--start-maximized')
    
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
        options.add_argument('--headless')

    # User Agent - use a more recent user agent
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    options.add_argument(f'user-agent={random.choice(user_agents)}')

    # Prüfen, ob wir auf dem Raspberry Pi oder einem anderen System sind
    
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
        
        return driver
    except Exception as e:
        print(f"Error creating driver: {e}")
        raise

async def initialize():
    """Initializes the WebDriver, logs in, and switches to the following tab."""
    # Die Telegram-Anwendung wird jetzt zentral in run() initialisiert.
    global driver, application, current_account # 'application' wird hier nur referenziert, nicht neu erstellt
    try:
        print("Initialisiere WebDriver...")
        driver = create_driver()
        print("WebDriver initialisiert. Starte Login...")
        login_success = await login()
        if login_success:
            print("Login erfolgreich. Wechsle zum 'Following'-Tab...")
            await switch_to_following_tab()
            print("'Following'-Tab erreicht.")
        else:
            print("WARNUNG: Login während der Initialisierung fehlgeschlagen.")
            # Hier könnte man überlegen, ob man abbricht oder weitermacht

        # Stelle sicher, dass die globale 'application' aus run() verfügbar ist
        if application is None:
             print("FEHLER: Telegram-Anwendung wurde nicht korrekt in run() initialisiert.")
             # Hier könnte ein schwerwiegender Fehler ausgelöst werden, da der Bot nicht funktionieren wird
             raise RuntimeError("Telegram application not initialized in run()")
        else:
             print("Telegram-Anwendung ist initialisiert und bereit.")

    except Exception as e:
        # Gib eine spezifischere Fehlermeldung aus
        print(f"FEHLER BEIM INITIALISIEREN (initialize Funktion): {e}")
        import traceback
        traceback.print_exc() # Drucke den vollen Traceback für mehr Details
        raise # Den Fehler weitergeben, damit das Skript ggf. stoppt                                                                                      

async def switch_to_following_tab():
    """Checks for ad relevance popup and ensures we're on the Following tab."""
    try:
        # --- NEU: Popup Check ---
        # XPath, der nach einem Button sucht, der einen Span mit dem spezifischen Text enthält
        popup_button_xpath = "//button[.//span[contains(text(), 'Keep less relevant ads')]]"
        try:
            print("Prüfe auf 'Keep less relevant ads' Popup...")
            # Warte nur kurz (z.B. 5 Sekunden), da das Popup schnell erscheinen sollte, wenn es da ist
            popup_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, popup_button_xpath))
            )
            print("Popup gefunden, klicke 'Keep less relevant ads'...")
            popup_button.click()
            await asyncio.sleep(random.uniform(1, 2)) # Kurze Pause nach dem Klick
            print("Popup-Button geklickt.")
        except (TimeoutException, NoSuchElementException):
            # Das ist der Normalfall, wenn das Popup nicht da ist
            print("Kein 'Keep less relevant ads' Popup gefunden.")
            pass
        except Exception as popup_err:
            # Fehler beim Behandeln des Popups loggen, aber weitermachen
            print(f"WARNUNG: Fehler beim Behandeln des 'Keep ads' Popups: {popup_err}")
        # --- ENDE Popup Check ---

        # --- Bestehende Logik zum Wechseln des Tabs ---
        print("Versuche zum 'Following'-Tab zu wechseln...")
        # Der ursprüngliche XPath für den "Following"-Tab
        following_tab_button_xpath = "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[1]/div[1]/div/nav/div/div[2]/div/div[2]/a"
        following_tab_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, following_tab_button_xpath))
        )
        # Kleine Pause vor dem Klick kann manchmal helfen, besonders nach UI-Änderungen
        await asyncio.sleep(random.uniform(0.5, 1.5))
        following_tab_button.click()
        print("'Following'-Tab geklickt.")
        await asyncio.sleep(random.uniform(2, 4)) # Wartezeit nach dem Klick beibehalten

    except (TimeoutException, NoSuchElementException):
         # Fehler, wenn der "Following"-Tab selbst nicht gefunden wird
         print("WARNUNG: Konnte den 'Following'-Tab-Button nicht finden oder klicken.")
         pass # Ignorieren und weitermachen, vielleicht ist man schon drauf
    except Exception as e:
        # Allgemeiner Fehler in dieser Funktion
        print(f"Fehler in switch_to_following_tab: {e}")
    
async def login():
    """Main login method that tries different login approaches in sequence"""
    global current_account, login_attempts
    try:
        account = ACCOUNTS[current_account]
        
        # Zuerst explizit zur Login-Seite navigieren
        driver.get("https://x.com/login")
        await asyncio.sleep(3)
        
        # If we're already logged in (redirected to home), return success
        if "home" in driver.current_url:
            await send_telegram_message(f"✅ Bereits eingeloggt als Account {current_account+1}")
            login_attempts = 0
            return True
        
        # First, try to login with cookies
        if await cookie_login():
            login_attempts = 0
            return True
            
        # If cookie login fails, try manual login
        await send_telegram_message(f"🔑 Starte Login für Account {current_account+1}...")
        result = await manual_login()
        
        if result:
            login_attempts = 0
            return True
        else:
            login_attempts += 1
            if login_attempts >= 3:
                await send_telegram_message("⚠️ Mehrere Login-Versuche fehlgeschlagen. Warte 15 Minuten vor erneutem Versuch...")
                await asyncio.sleep(900)  # 15 Minuten warten
                login_attempts = 0
            
            await switch_account()
            return False

    except Exception as e:
        await send_telegram_message(f"❌ Login-Fehler: {str(e)}")
        login_attempts += 1
        await switch_account()
        return False

async def cookie_login():
    """Try to login using saved cookies (JSON format)."""
    global current_account, driver
    account = ACCOUNTS[current_account]
    cookie_filepath = account.get('cookies_file') # Sicherer Zugriff

    if not cookie_filepath:
        print("FEHLER: Kein Cookie-Dateipfad für den aktuellen Account gefunden.")
        return False

    try:
        # Navigate to X before setting cookies
        driver.get("https://x.com")
        await asyncio.sleep(2)

        # Load and set cookies from JSON
        try:
            print(f"Versuche Cookies aus JSON-Datei zu laden: {cookie_filepath}")
            # --- GEÄNDERT: JSON laden im Textmodus ---
            with open(cookie_filepath, 'r', encoding='utf-8') as f:
                cookies = json.load(f)
            # --- ENDE ÄNDERUNG ---
            print(f"{len(cookies)} Cookies geladen.")

            # Clear existing cookies
            driver.delete_all_cookies()

            # Add cookies
            added_count = 0
            for cookie in cookies:
                # --- GEÄNDERT: SameSite Handling bleibt, aber keine Pickle-spezifischen Dinge mehr ---
                if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
                    # print(f"DEBUG: Adjusting sameSite for cookie {cookie.get('name')}: {cookie['sameSite']} -> Lax") # Optional Debug
                    cookie['sameSite'] = 'Lax'
                # Entferne 'expiry' wenn es kein Integer ist (manchmal float von alten Pickles?)
                if 'expiry' in cookie and not isinstance(cookie['expiry'], int):
                    # print(f"DEBUG: Removing invalid expiry type for cookie {cookie.get('name')}: {type(cookie['expiry'])}") # Optional Debug
                    del cookie['expiry']

                try:
                    driver.add_cookie(cookie)
                    added_count += 1
                except Exception as cookie_error:
                    # Logge mehr Details zum fehlerhaften Cookie
                    print(f"WARNUNG: Fehler beim Hinzufügen des Cookies '{cookie.get('name', 'N/A')}': {cookie_error}")
                    # print(f"DEBUG: Fehlerhaftes Cookie-Dict: {cookie}") # Optional: Zeige das problematische Cookie
                    continue
            print(f"{added_count}/{len(cookies)} Cookies erfolgreich hinzugefügt.")
            # --- ENDE ÄNDERUNG ---

            # Refresh and check if logged in
            print("Aktualisiere Seite nach Cookie-Setzung...")
            driver.refresh()
            await asyncio.sleep(random.uniform(3, 5)) # Etwas länger warten nach Refresh

            if "home" in driver.current_url:
                print("Login via Cookies erfolgreich.")
                # Navigate to Following timeline (wie zuvor)
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
                    pass # Ignoriere Fehler hier, falls nicht nötig
                await asyncio.sleep(2)
                await send_telegram_message("✅ Login via Cookies erfolgreich!")
                return True
            else:
                print(f"WARNUNG: Login via Cookies fehlgeschlagen (URL nach Refresh: {driver.current_url}). Versuche manuellen Login.")
                return False

        except FileNotFoundError:
            print(f"Cookie-Datei '{cookie_filepath}' nicht gefunden. Versuche manuellen Login.")
            return False
        # --- GEÄNDERT: JSONDecodeError hinzufügen ---
        except json.JSONDecodeError as json_err:
            print(f"FEHLER: Cookie-Datei '{cookie_filepath}' ist korrupt oder keine gültige JSON-Datei: {json_err}")
            print("Versuche manuellen Login.")
            # Optional: Lösche die korrupte Datei
            # try: os.remove(cookie_filepath)
            # except OSError as e: print(f"Konnte korrupte Cookie-Datei nicht löschen: {e}")
            return False
        # --- ENDE ÄNDERUNG ---

    except Exception as e:
        print(f"Unerwarteter Fehler im Cookie-Login: {e}")
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
        
        # Handle 2FA if needed
        if check_element_exists(By.CSS_SELECTOR, '[data-testid="ocfEnterTextTextInput"]'):
            await handle_2fa()
        
        # Handle account unlock if needed
        if check_element_exists(By.XPATH, "//div[contains(text(), 'Your account has been locked')]"):
            await handle_account_unlock()
        
        # Verify successful login
        await asyncio.sleep(3)
        if "home" in driver.current_url:
            cookie_filepath = account.get('cookies_file')
            if cookie_filepath:
                # --- GEÄNDERT: Cookies als JSON speichern ---
                try:
                    print(f"Speichere Cookies als JSON in: {cookie_filepath}")
                    cookies_to_save = driver.get_cookies()
                    with open(cookie_filepath, "w", encoding='utf-8') as file:
                        json.dump(cookies_to_save, file, indent=4) # indent=4 für Lesbarkeit
                    print(f"{len(cookies_to_save)} Cookies gespeichert.")
                except Exception as save_err:
                    print(f"FEHLER beim Speichern der Cookies in '{cookie_filepath}': {save_err}")
                    logger.error(f"Failed to save cookies to {cookie_filepath}", exc_info=True)
                # --- ENDE ÄNDERUNG ---
            else:
                 print("WARNUNG: Kein Cookie-Dateipfad zum Speichern für diesen Account gefunden.")

            await send_telegram_message(f"✅ Login für Account {current_account+1} erfolgreich!")
            return True
        else:
            await send_telegram_message(f"❌ Login für Account {current_account+1} fehlgeschlagen!")
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
        await send_telegram_message(f"🔐 2FA Code benötigt für Account {current_account+1}")
        
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
        # Logge den Check in jedem Durchlauf
        logger.debug(f"[Auth Wait] Loop {i+1}/300: Checking AUTH_CODE (current value: {'Set' if AUTH_CODE else 'None'})...")
        if AUTH_CODE:
            code = AUTH_CODE
            AUTH_CODE = None  # Reset code
            logger.info(f"[Auth Wait] Auth code '{code}' received!") # Log success mit Code
            return code
        await asyncio.sleep(1) # Wichtig: await hier lassen!
    # Wird nur erreicht, wenn die Schleife ohne Fund durchläuft
    logger.warning("[Auth Wait] Timeout after 300 seconds while waiting for authentication code.") # Log timeout
    await send_telegram_message("⏰ Timeout while waiting for authentication code")
    return None

async def handle_account_unlock():
    """Handle the account unlock process if the account is locked"""
    global AUTH_CODE, WAITING_FOR_AUTH
    try:
        await send_telegram_message("⚠️ Account ist gesperrt. Starte Entsperrungsprozess...")

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
            await send_telegram_message("🔑 Bitte Code aus der E-Mail eingeben")
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
    # Überprüfe zuerst die Internetverbindung
    try:
        # Schneller Ping-Test (Timeout 5 Sekunden)
        response = requests.get("https://api.x.com/ping", timeout=5)
    except:
        # Versuche es noch einmal mit einem anderen Service
        try:
            response = requests.get("https://www.google.com", timeout=5)
        except Exception as e:
            print(f"Internetverbindung möglicherweise unterbrochen: {e}")
            # Behandle wie einen Rate-Limit, da wir keine Daten abrufen können
            return True
    
    # Führe dann die ursprüngliche Rate-Limit-Prüfung durch
    try:
        for pattern in rate_limit_patterns:
            try:
                element = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, pattern))
                )
                if element:
                    await handle_rate_limit()
                    return True
            except (NoSuchElementException, TimeoutException):
                continue
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
        await send_telegram_message("⚠️ Nur ein Account verfügbar, versuche erneut mit demselben Account")
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
        # Zuerst sicherstellen, dass wir auf einer X-Seite sind
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
                    
                    # Explizit zur Login-Seite navigieren nach Logout
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
        
        # Explizit zur Login-Seite navigieren nach dem Leeren der Cookies
        driver.get("https://x.com/login")
        await asyncio.sleep(2)
        return True
    except Exception as e:
        print(f"Error clearing cookies and storage: {e}")
        return False

def parse_follower_count(count_str: str) -> int:
    """
    Konvertiert Follower-Zahl-Strings (z.B. "9.6M", "862K", "12345", "2,4m", "23.83k") in Integer.
    Gibt 0 zurück bei Fehlern oder ungültigem Format.
    """
    if not isinstance(count_str, str):
        return 0

    # 1. Preprocessing: Lowercase, remove ALL commas, strip whitespace
    # Kommas werden IMMER als Tausendertrennzeichen behandelt und entfernt.
    # Der Punkt wird als Dezimaltrennzeichen interpretiert.
    processed_str = count_str.lower().strip().replace(',', '')
    if not processed_str:
        return 0

    # 2. Suffix Handling
    multiplier = 1
    num_part = processed_str
    if processed_str.endswith('m'):
        multiplier = 1_000_000
        num_part = processed_str[:-1].strip() # Entferne 'm' und evtl. Leerzeichen davor
    elif processed_str.endswith('k'):
        multiplier = 1_000
        num_part = processed_str[:-1].strip() # Entferne 'k' und evtl. Leerzeichen davor

    # 3. Number Parsing
    try:
        # Prüfe, ob mehr als ein Punkt vorhanden ist (ungültig)
        if num_part.count('.') > 1:
            # print(f"Debug parse_follower_count: Ungültiges Format (mehrere Punkte): '{num_part}'")
            return 0

        # Konvertiere zu float, um Dezimalstellen (z.B. "2.4", "23.83") zu behandeln
        num_float = float(num_part)

        # Berechne den Endwert und konvertiere zu int
        final_value = int(num_float * multiplier)
        return final_value
    except ValueError:
        # Fehler bei der Konvertierung zu float (z.B. "abc", leere Zeichenkette nach Suffix-Entfernung)
        # print(f"Debug parse_follower_count: Konnte '{num_part}' nicht in Zahl umwandeln.")
        return 0
    except Exception as e:
        # Andere unerwartete Fehler
        print(f"Debug parse_follower_count: Unerwarteter Fehler bei '{count_str}': {e}")
        return 0

async def follow_user(username):
    """Follow a user on X based on their username"""
    try:
        # Navigate to user's profile
        driver.get(f"https://x.com/{username}")
        await asyncio.sleep(random.uniform(3, 5))
        
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
                    await send_telegram_message(f"ℹ️ Du folgst @{username} bereits")
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
                follow_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                break
            except:
                continue
                
        if follow_button:
            await asyncio.sleep(random.uniform(1, 2))
            follow_button.click()
            await asyncio.sleep(random.uniform(2, 3))
            await send_telegram_message(f"✅ Successfully followed @{username}")
            
            # Navigate back to the following timeline
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return True
        else:
            await send_telegram_message(f"❌ Could not find follow button for @{username}")
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 3))
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
        await asyncio.sleep(random.uniform(3, 5))

        # If we're here, check for unfollow button
        unfollow_button_xpath = [
            "//button[@aria-label='Following @" + username + "']",
            "//button[contains(@aria-label, 'Following @" + username + "')]",
            "//button[starts-with(@aria-label, 'Following @')]",
            "//div[@role='button' and @aria-label='Following @" + username + "']",
            "//div[@role='button' and contains(@aria-label, 'Following @')]"
        ]
        
        unfollow_button = None
        for xpath in unfollow_button_xpath:
            try:
                unfollow_button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                print(f"Found unfollow button using XPath: {xpath}")
                break
            except:
                continue
                
        if unfollow_button:
            print(f"Found unfollow button for @{username}, clicking it")
            await asyncio.sleep(random.uniform(1, 2))
            unfollow_button.click()
            await asyncio.sleep(random.uniform(2, 3))
            
            # Handle confirmation dialog - using multiple approaches
            print("Looking for confirmation dialog")
            confirmation_found = False
            
            # Approach 1: Using data-testid
            try:
                confirm_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-testid="confirmationSheetConfirm"]'))
                )
                if confirm_button:
                    print("Found confirmation button by data-testid, clicking it")
                    confirm_button.click()
                    confirmation_found = True
                    await asyncio.sleep(random.uniform(1, 2))
            except Exception as e:
                print(f"Could not find confirmation by data-testid: {e}")
            
            # Approach 2: Using text content
            if not confirmation_found:
                try:
                    unfollow_text_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, '//span[text()="Unfollow"]/ancestor::button'))
                    )
                    if unfollow_text_button:
                        print("Found confirmation button by text 'Unfollow', clicking it")
                        unfollow_text_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2))
                except Exception as e2:
                    print(f"Could not find confirmation by text: {e2}")
            
            # Approach 3: Using the CSS selector you provided
            if not confirmation_found:
                try:
                    css_selector = "#layers > div:nth-child(2) > div > div > div > div > div > div.css-175oi2r.r-1ny4l3l.r-18u37iz.r-1pi2tsx.r-1777fci.r-1xcajam.r-ipm5af.r-1kihuf0.r-xr3zp9.r-1awozwy.r-1pjcn9w.r-9dcw1g > div.css-175oi2r.r-14lw9ot.r-pm9dpa.r-1rnoaur.r-1867qdf.r-z6ln5t.r-494qqr.r-f8sm7e.r-13qz1uu.r-1ye8kvj > div.css-175oi2r.r-eqz5dr.r-1hc659g.r-7lkd7n.r-11c0sde.r-13qz1uu > button:nth-child(1)"
                    confirm_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector))
                    )
                    if confirm_button:
                        print("Found confirmation button by complex CSS selector, clicking it")
                        confirm_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2))
                except Exception as e3:
                    print(f"Could not find confirmation by CSS selector: {e3}")
            
            # Approach 4: Using the XPath you provided
            if not confirmation_found:
                try:
                    xpath1 = '//*[@id="layers"]/div[2]/div/div/div/div/div/div[2]/div[2]/div[2]/button[1]'
                    confirm_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, xpath1))
                    )
                    if confirm_button:
                        print("Found confirmation button by XPath 1, clicking it")
                        confirm_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2))
                except Exception as e4:
                    print(f"Could not find confirmation by XPath 1: {e4}")
            
            # Approach 5: Using the full XPath you provided
            if not confirmation_found:
                try:
                    xpath2 = '/html/body/div[1]/div/div/div[1]/div[2]/div/div/div/div/div/div[2]/div[2]/div[2]/button[1]'
                    confirm_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, xpath2))
                    )
                    if confirm_button:
                        print("Found confirmation button by XPath 2, clicking it")
                        confirm_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2))
                except Exception as e5:
                    print(f"Could not find confirmation by XPath 2: {e5}")
            
            # Approach 6: More general approach - first button in the dialog
            if not confirmation_found:
                try:
                    general_button = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.XPATH, '//div[@role="dialog"]//button[1]'))
                    )
                    if general_button:
                        print("Found first button in dialog, clicking it")
                        general_button.click()
                        confirmation_found = True
                        await asyncio.sleep(random.uniform(1, 2))
                except Exception as e6:
                    print(f"Could not find first button in dialog: {e6}")
            
            if not confirmation_found:
                print("WARNING: Could not find any confirmation button")
            
            print("Sending success message")
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
            await send_telegram_message(f"ℹ️ Du folgst @{username} nicht")
            
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
    """Scrapt die 'Following'-Liste, speichert sie account-spezifisch
    und aktualisiert die globale Followed-Liste. (Mit Abbruchmöglichkeit)"""
    global driver, current_account, is_scraping_paused, pause_event, ACCOUNTS
    global global_followed_users_set
    global is_backup_running, cancel_backup_flag # Flags importieren

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if is_backup_running:
        await update.message.reply_text("⚠️ Ein Backup-Prozess läuft bereits.")
        # WICHTIG: Nicht fortsetzen, aber auch nicht pausieren/resumen hier
        return
    if not account_username or not backup_filepath:
        await update.message.reply_text("❌ Fehler: Account-Username oder Backup-Pfad konnte nicht ermittelt werden.")
        return

    # ===== Task Start Markierung =====
    is_backup_running = True
    cancel_backup_flag = False # Sicherstellen, dass Flag zurückgesetzt ist
    # ================================

    print(f"[Backup] Starte Follower-Backup für @{account_username} -> {backup_filepath}...")
    await update.message.reply_text(f"⏳ Starte Follower-Backup für @{account_username}...\n"
                                     f"   Zum Abbrechen: `/cancelbackup`") # Info über Abbruchbefehl

    await pause_scraping() # Pausiere Haupt-Scraping

    found_followers = set()
    navigation_successful = False
    last_found_count = -1
    cancelled_early = False # Flag für Abbruchmeldung

    try: # Haupt-Try-Block
        following_url = f"https://x.com/{account_username}/following"
        print(f"[Backup] Navigiere zu: {following_url}")
        driver.get(following_url)
        await asyncio.sleep(random.uniform(8, 12))
        user_cell_button_xpath = '//button[@data-testid="UserCell"]'
        WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.XPATH, user_cell_button_xpath)))
        print("[Backup] Erste UserCell-Buttons gefunden.")

        scroll_attempts_without_new_followers = 0
        max_scroll_attempts_without_new_followers = 3

        while scroll_attempts_without_new_followers < max_scroll_attempts_without_new_followers:
            # ===== Abbruchprüfung =====
            if cancel_backup_flag:
                print("[Backup] Abbruchsignal empfangen.")
                cancelled_early = True
                await update.message.reply_text("🟡 Backup wird abgebrochen...")
                break # Schleife verlassen
            # =========================

            initial_follower_count_in_loop = len(found_followers)
            #wait_time = random.uniform(6.0, 9.0)
            await asyncio.sleep(0.5)

            # User-Extraktion... (wie zuvor)
            try:
                user_cells = driver.find_elements(By.XPATH, user_cell_button_xpath)
                # ... (Rest der Extraktionslogik) ...
                relative_link_xpath = ".//a[contains(@href, '/')]"
                for cell_button in user_cells:
                    # ===== Abbruchprüfung (feingranularer) =====
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
                    except Exception as cell_err: print(f"  [Backup] Warnung: Extraktionsfehler: {cell_err}")
                if cancel_backup_flag: break # Auch nach der inneren Schleife prüfen
            except Exception as find_err: print(f"[Backup] Warnung: Suchfehler: {find_err}")


            current_follower_count_in_loop = len(found_followers)
            newly_found_in_loop = current_follower_count_in_loop - initial_follower_count_in_loop

            current_scroll_pos = driver.execute_script("return window.pageYOffset;")
            total_scroll_height = driver.execute_script("return document.body.scrollHeight;")
            print(f"[Backup] Scroll-Pos: {current_scroll_pos}/{total_scroll_height}, Gefunden={current_follower_count_in_loop} (+{newly_found_in_loop} Iteration), Fehlversuche={scroll_attempts_without_new_followers}")

            if current_follower_count_in_loop == last_found_count:
                 scroll_attempts_without_new_followers += 1
                 print(f"[Backup] Keine *neuen* einzigartigen User. Versuch {scroll_attempts_without_new_followers}/{max_scroll_attempts_without_new_followers}.")
            else:
                 scroll_attempts_without_new_followers = 0
                 print(f"[Backup] Neue einzigartige User gefunden ({last_found_count} -> {current_follower_count_in_loop}). Setze Fehlversuche zurück.")

            last_found_count = current_follower_count_in_loop

            if scroll_attempts_without_new_followers >= max_scroll_attempts_without_new_followers:
                print(f"[Backup] Stoppe Scrollen: {max_scroll_attempts_without_new_followers} Versuche ohne neue User.")
                break

            # ===== Abbruchprüfung =====
            if cancel_backup_flag:
                print("[Backup] Abbruchsignal empfangen vor dem Scrollen.")
                cancelled_early = True
                await update.message.reply_text("🟡 Backup wird abgebrochen...")
                break
            # =========================

            await asyncio.sleep(0.5)
            driver.execute_script("window.scrollBy(0, window.innerHeight);")
            wait_time = random.uniform(6.0, 9.0)
            print(f"[Backup] Warte {wait_time:.1f} Sekunden auf das Laden...")
            await asyncio.sleep(wait_time)
            # Ende der while-Schleife

        # Nach der Schleife (normal oder abgebrochen)
        if cancelled_early:
             print(f"[Backup] Prozess abgebrochen. {len(found_followers)} User bis dahin gefunden (werden nicht gespeichert).")
             # WICHTIG: Bei Abbruch speichern wir NICHTS, um inkonsistente Backups zu vermeiden.
             await update.message.reply_text(f"🛑 Backup abgebrochen. Es wurde keine Datei gespeichert/aktualisiert.")
        else:
            print(f"[Backup] Scrollen abgeschlossen. Insgesamt {len(found_followers)} einzigartige User gefunden.")
            # Ergebnisse speichern (nur wenn nicht abgebrochen)
            if found_followers:
                # Speichere NUR das account-spezifische Backup
                save_set_to_file(found_followers, backup_filepath)
                logger.info(f"[Backup] Account backup for @{account_username} saved to {os.path.basename(backup_filepath)} ({len(found_followers)} users).")
                # Ändere die Erfolgsnachricht - KEINE globale Aktualisierung mehr
                success_message = (f"✅ Follower-Backup für @{account_username} ({len(found_followers)} User) "
                                   f"abgeschlossen und gespeichert in `{os.path.basename(backup_filepath)}`.\n"
                                   f"(Globale Liste wurde NICHT geändert.)")
                await update.message.reply_text(success_message)
            else:
                # Leere Backup-Datei, wenn nichts gefunden wurde
                save_set_to_file(set(), backup_filepath)
                await update.message.reply_text(f"ℹ️ Keine Follower für @{account_username} gefunden oder Backup-Datei `{os.path.basename(backup_filepath)}` wurde geleert.")
                logger.info(f"[Backup] No followers found for @{account_username} or backup file cleared.")

    except TimeoutException:
         await update.message.reply_text("❌ Fehler: Laden der Follower-Liste für Backup fehlgeschlagen (Timeout).")
         print("[Backup] TimeoutException beim Warten auf UserCells.")
    except Exception as e:
        error_message = f"💥 Schwerwiegender Fehler während des Follower-Backups: {e}"
        await update.message.reply_text(error_message)
        print(error_message)
        import traceback
        traceback.print_exc()

    finally: # ===== WICHTIGER FINALLY BLOCK =====
        print("[Backup] Finally Block erreicht.")
        # Rückkehr zur Haupt-Timeline
        print("[Backup] Versuche zur Haupt-Timeline (/home) zurückzukehren...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(4, 6))
            await switch_to_following_tab()
            print("[Backup] Zurück auf /home 'Following'-Tab.")
            navigation_successful = True
        except Exception as nav_err:
            error_msg = f"⚠️ Fehler bei Rückkehr zur Haupt-Timeline nach Backup: {nav_err}."
            print(error_msg)
            try: await update.message.reply_text(error_msg)
            except: pass

        # Haupt-Scraping fortsetzen
        print("[Backup] Setze Haupt-Scraping fort.")
        await resume_scraping()

        # ===== Task Ende Markierung =====
        is_backup_running = False
        cancel_backup_flag = False # Sicherstellen, dass Flag für nächsten Lauf false ist
        print("[Backup] Status-Flags zurückgesetzt.")
        # =============================


async def scrape_target_following(update: Update, target_username: str):
    """
    Scrapt die 'Following'-Liste eines *beliebigen* X-Users, extrahiert Follower-Zahlen
    und aktualisiert die `following_database`. (Mit Abbruchmöglichkeit)
    """
    global driver, is_scraping_paused, pause_event, following_database
    global is_db_scrape_running, cancel_db_scrape_flag # Flags importieren

    # Bereinige den Ziel-Usernamen
    target_username = target_username.strip().lstrip('@')
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', target_username):
        await update.message.reply_text(f"❌ Ungültiger Ziel-Username: {target_username}")
        return # Beende frühzeitig

    if is_db_scrape_running:
        await update.message.reply_text("⚠️ Ein Datenbank-Scrape-Prozess läuft bereits.")
        return

    # ===== Task Start Markierung =====
    is_db_scrape_running = True
    cancel_db_scrape_flag = False
    # ================================

    print(f"[DB Scrape] Starte Scrape der Following-Liste von @{target_username}...")
    await update.message.reply_text(f"⏳ Starte Scrape für @{target_username}...\n"
                                     f"   Zum Abbrechen: `/canceldbscrape`")

    await pause_scraping() # Pausiere Haupt-Scraping

    processed_in_this_scrape = set()
    users_added_or_updated = 0
    last_found_count = -1
    cancelled_early = False
    navigation_successful = False
    db_changed = False # Flag, um zu wissen, ob gespeichert werden muss

    try: # Haupt-Try-Block für die gesamte Funktion
        following_url = f"https://x.com/{target_username}/following"
        print(f"[DB Scrape] Navigiere zu: {following_url}")
        driver.get(following_url)
        await asyncio.sleep(random.uniform(5, 8)) # Längere Wartezeit für externe Profile

        # --- Check für private/nicht existierende Profile ---
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
                    await update.message.reply_text(f"❌ Fehler beim Zugriff auf @{target_username}/following: {error_text}")
                    print(f"[DB Scrape] Fehler: {error_text}")
                    error_found = True
                    break
                except (TimeoutException, NoSuchElementException):
                    continue
            if error_found:
                raise Exception("Profile inaccessible")

        except Exception as profile_check_err:
            if "Profile inaccessible" not in str(profile_check_err):
                 print(f"[DB Scrape] Unerwarteter Fehler bei Profilprüfung: {profile_check_err}")
            raise # Gibt den Fehler weiter, um den try-Block zu verlassen

        # --- Ende Check ---

        user_cell_button_xpath = '//button[@data-testid="UserCell"]'
        # Warte auf das erste Erscheinen der UserCells
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, user_cell_button_xpath)))
        print("[DB Scrape] Erste UserCell-Buttons gefunden.")

        scroll_attempts_without_new = 0
        max_scroll_attempts_without_new = 5

        # --- Beginn der Haupt-Scroll-Schleife ---
        while scroll_attempts_without_new < max_scroll_attempts_without_new:
            if cancel_db_scrape_flag:
                print("[DB Scrape] Abbruchsignal empfangen.")
                cancelled_early = True
                await update.message.reply_text("🟡 Datenbank-Scrape wird abgebrochen...")
                break

            initial_processed_count = len(processed_in_this_scrape)

            # --- User-Extraktion und Verarbeitung pro Scroll-Ansicht ---
            try: # Try-Block für das Finden der Zellen in dieser Ansicht
                user_cells = driver.find_elements(By.XPATH, user_cell_button_xpath)
                print(f"[DB Scrape] {len(user_cells)} UserCells in dieser Ansicht gefunden.")

                # --- Iteriere sicher über die gefundenen Zellen ---
                for cell_index, cell_button in enumerate(user_cells):
                    # --- Definiere Variablen mit Standardwerten zu Beginn JEDER Iteration ---
                    scraped_username = None
                    follower_count = 0
                    bio_text = "" # Wichtig: Hier initialisieren

                    try: # --- Umfassender Try-Block für die Verarbeitung einer einzelnen Zelle ---
                        if cancel_db_scrape_flag: break # Früher Abbruchcheck

                        # 1. Username extrahieren
                        try:
                            relative_link_xpath = ".//a[contains(@href, '/') and not(contains(@href, '/photo'))]"
                            link_element = WebDriverWait(cell_button, 2).until( # Kurzer Wait pro Zelle
                                EC.presence_of_element_located((By.XPATH, relative_link_xpath))
                            )
                            href = link_element.get_attribute('href')
                            if href:
                                parts = href.split('/')
                                potential_username = parts[-1].strip()
                                if potential_username and re.match(r'^[A-Za-z0-9_]{1,15}$', potential_username):
                                    scraped_username = potential_username
                                else:
                                    # print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Ungültiger Username aus href '{href}'.")
                                    continue # Nächste Zelle
                            else:
                                # print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Leerer href für Username-Link.")
                                continue # Nächste Zelle
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            # print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Link für Username nicht gefunden.")
                            continue # Nächste Zelle
                        except Exception as user_err:
                            print(f"  [DB Scrape] Fehler (Zelle {cell_index}) bei Username-Extraktion: {user_err}")
                            continue # Nächste Zelle

                        # Prüfung: Überspringen, wenn kein Username oder schon verarbeitet
                        if not scraped_username or scraped_username in processed_in_this_scrape:
                            continue

                        # 2. Bio-Text aus UserCell extrahieren (KORREKTE POSITION)
                        try:
                            bio_div_xpath = './div/div[2]/div[2]'
                            # Kurzer Wait, Bio ist nicht immer da
                            bio_div = WebDriverWait(cell_button, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, bio_div_xpath))
                            )
                            bio_text = bio_div.get_attribute('textContent').strip()
                        except (TimeoutException, NoSuchElementException):
                            pass # Keine Bio ist ok
                        except Exception as bio_err:
                            print(f"  [DB Scrape] Fehler (Zelle {cell_index}) beim Extrahieren der Bio für @{scraped_username}: {bio_err}")
                        # --- ENDE Bio-Extraktion ---

                        # 3. Follower-Zahl extrahieren (Hover)
                        hover_target_element = None
                        hover_card = None
                        try:
                            hover_target_xpath = './div/div[2]/div[1]/div[1]/div/div[1]/a'
                            try:
                                hover_target_element = cell_button.find_element(By.XPATH, hover_target_xpath)
                            except NoSuchElementException:
                                # print(f"  [DB Scrape] FEHLER (Zelle {cell_index}): Konnte Hover-Ziel-Link für @{scraped_username} nicht finden.")
                                pass # Mache weiter ohne Follower-Zahl

                            if hover_target_element:
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});", hover_target_element)
                                await asyncio.sleep(random.uniform(0.4, 0.8))
                                driver.execute_script("arguments[0].dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));", hover_target_element)
                                wait_for_card_render = random.uniform(1.8, 2.8)
                                await asyncio.sleep(wait_for_card_render)
                                hover_card_xpath = '//div[@data-testid="HoverCard"]'
                                hover_card = WebDriverWait(driver, 6).until(EC.visibility_of_element_located((By.XPATH, hover_card_xpath)))

                                # --- Innerer Try für HoverCard-Interaktion ---
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
                                                     # print(f"  [DB Scrape] Follower-Text Versuch {i+1} ('{text_xpath}') ERFOLG: '{follower_text}'") # Debug
                                                     found_text = True
                                                     break
                                        except (NoSuchElementException, StaleElementReferenceException): continue
                                        except Exception as e_xpath: print(f"  [DB Scrape] Follower-Text Versuch {i+1} ('{text_xpath}') - Unerwarteter Fehler: {e_xpath}")
                                    if found_text:
                                        # print(f"  [DB Scrape] Versuche Parsing für Text: '{follower_text}'") # Debug
                                        follower_count = parse_follower_count(follower_text)
                                        print(f"  [DB Scrape] @{scraped_username} - Follower geparsed: {follower_count} (Raw: '{follower_text}')") # Debug
                                    # else: # Kein else nötig, follower_count bleibt 0
                                    #    print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Konnte Follower-Text für @{scraped_username} nicht extrahieren.")
                                except TimeoutException as te_inner: print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Timeout *innerhalb* HoverCard @{scraped_username}. {te_inner}")
                                except (NoSuchElementException, StaleElementReferenceException) as e_inner: print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Element nicht gefunden *innerhalb* HoverCard @{scraped_username}. {e_inner}")
                                except Exception as inner_err: print(f"  [DB Scrape] Unerwarteter Fehler *innerhalb* HoverCard @{scraped_username}: {inner_err}"); logger.warning(f"Unexpected error inside HoverCard processing for {scraped_username}", exc_info=True)
                                finally:
                                    # --- HoverCard durch Scrollen schließen ---
                                    try:
                                        driver.execute_script("window.scrollBy(0, 1);")
                                        await asyncio.sleep(random.uniform(0.2, 0.4))
                                    except Exception as close_err: print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Fehler beim Schließen der HoverCard für @{scraped_username}: {close_err}")
                        except TimeoutException as te_outer: print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Timeout beim Warten auf HoverCard für @{scraped_username}. {te_outer}")
                        except (NoSuchElementException, StaleElementReferenceException) as e_outer: print(f"  [DB Scrape] Warnung (Zelle {cell_index}): Element nicht gefunden beim Hover-Setup für @{scraped_username}. {e_outer}")
                        except Exception as hover_err: print(f"  [DB Scrape] Unerwarteter Fehler beim Hover-Setup für @{scraped_username}: {hover_err}"); logger.warning(f"Unexpected JS hover setup/trigger error for {scraped_username}", exc_info=True)
                        # --- Ende Follower-Zahl Extraktion ---

                        # 4. Datenbank aktualisieren (mit Bio)
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
                        # --- Ende Datenbank Update ---

                        # Als verarbeitet markieren
                        processed_in_this_scrape.add(scraped_username)

                    except StaleElementReferenceException:
                        print(f"  [DB Scrape] Fehler (Zelle {cell_index}): Stale Element Reference. Überspringe diese Zelle.")
                        continue # Gehe zur nächsten Zelle
                    except Exception as cell_processing_error:
                        print(f"  [DB Scrape] Unerwarteter Fehler bei der Verarbeitung von Zelle {cell_index} (User: {'@'+scraped_username if scraped_username else 'Unbekannt'}): {cell_processing_error}")
                        logger.warning(f"Unexpected error processing cell {cell_index}", exc_info=True)
                        continue # Gehe zur nächsten Zelle
                    # --- ENDE des umfassenden Try-Blocks für eine Zelle ---

                # --- Ende der for-Schleife über user_cells ---
                if cancel_db_scrape_flag: break # Äußere Schleife verlassen, wenn abgebrochen

            except Exception as find_err:
                 # Fehler beim ursprünglichen Finden von user_cells
                 print(f"[DB Scrape] Kritischer Fehler beim Finden von UserCells: {find_err}")
                 break # Breche die äußere while-Schleife ab

            # --- Nach Verarbeitung der Zellen in dieser Ansicht ---
            current_processed_count = len(processed_in_this_scrape)
            newly_processed_in_loop = current_processed_count - initial_processed_count

            current_scroll_pos = driver.execute_script("return window.pageYOffset;")
            total_scroll_height = driver.execute_script("return document.body.scrollHeight;")
            print(f"[DB Scrape] Scroll-Pos: {int(current_scroll_pos)}/{int(total_scroll_height)}, Verarbeitet={current_processed_count} (+{newly_processed_in_loop} Iteration), DB Updates={users_added_or_updated}, Fehlversuche={scroll_attempts_without_new}")

            if current_processed_count == last_found_count:
                scroll_attempts_without_new += 1
            else:
                scroll_attempts_without_new = 0

            last_found_count = current_processed_count

            if scroll_attempts_without_new >= max_scroll_attempts_without_new:
                print(f"[DB Scrape] Stoppe Scrollen: {max_scroll_attempts_without_new} Versuche ohne neue User.")
                break

            if cancel_db_scrape_flag: break

            # Scrollen für nächste Runde
            driver.execute_script("window.scrollBy(0, window.innerHeight * 0.9);")
            wait_time = random.uniform(2.0, 4.0)
            await asyncio.sleep(wait_time)
            # --- Ende der while-Schleife ---

        # --- Nach der Scroll-Schleife (normal oder abgebrochen) ---
        if cancelled_early:
            print(f"[DB Scrape] Prozess abgebrochen. {len(processed_in_this_scrape)} User bis dahin verarbeitet.")
            if db_changed:
                print("[DB Scrape] Speichere bisherige Datenbankänderungen...")
                save_following_database()
                await update.message.reply_text(f"🟡 Scrape abgebrochen. {users_added_or_updated} Datenbank-Updates wurden gespeichert.")
            else:
                await update.message.reply_text(f"🛑 Scrape abgebrochen. Keine Datenbankänderungen vorgenommen.")
        else:
            print(f"[DB Scrape] Scrollen abgeschlossen. Insgesamt {len(processed_in_this_scrape)} einzigartige User verarbeitet.")
            if db_changed:
                print("[DB Scrape] Speichere finale Datenbankänderungen...")
                save_following_database()
                await update.message.reply_text(f"✅ Scrape für @{target_username} abgeschlossen. {users_added_or_updated} Datenbank-Updates durchgeführt ({len(following_database)} gesamt).")
            else:
                await update.message.reply_text(f"✅ Scrape für @{target_username} abgeschlossen. Keine neuen Updates für die Datenbank.")

    except Exception as e:
        # Fehlerbehandlung für den äußeren Try-Block
        if "Profile inaccessible" in str(e):
             pass # Nachricht wurde bereits gesendet
        else:
            error_message = f"💥 Schwerwiegender Fehler während des DB-Scrapes für @{target_username}: {e}"
            await update.message.reply_text(error_message)
            print(error_message)
            logger.error(f"Critical error during DB scrape for @{target_username}: {e}", exc_info=True)
            # Speichere DB trotzdem, falls Änderungen gemacht wurden
            if db_changed:
                print("[DB Scrape] Speichere Datenbank trotz Fehler...")
                save_following_database()

    finally: # ===== WICHTIGER FINALLY BLOCK =====
        print("[DB Scrape] Finally Block erreicht.")
        # Rückkehr zur Haupt-Timeline
        print("[DB Scrape] Versuche zur Haupt-Timeline (/home) zurückzukehren...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(4, 6))
            await switch_to_following_tab()
            print("[DB Scrape] Zurück auf /home 'Following'-Tab.")
            navigation_successful = True
        except Exception as nav_err:
            error_msg = f"⚠️ Fehler bei Rückkehr zur Haupt-Timeline nach DB-Scrape: {nav_err}."
            print(error_msg)
            try: await update.message.reply_text(error_msg)
            except: pass

        # Haupt-Scraping fortsetzen
        print("[DB Scrape] Setze Haupt-Scraping fort.")
        await resume_scraping()

        # ===== Task Ende Markierung =====
        is_db_scrape_running = False
        cancel_db_scrape_flag = False
        print("[DB Scrape] Status-Flags zurückgesetzt.")
        # =============================

async def recover_followers_logic(update: Update):
    """Liest das account-spezifische Backup und fügt User zur
    account-spezifischen Follow-Liste hinzu (prüft gegen globale Followed-Liste)."""
    global current_account_usernames_to_follow, global_followed_users_set
    global is_scraping_paused, pause_event # Zugriff für pause/resume

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()
    follow_list_filepath = get_current_follow_list_path()

    if not account_username or not backup_filepath or not follow_list_filepath:
        await update.message.reply_text("❌ Fehler: Account-Infos/Dateipfade konnten nicht ermittelt werden.")
        return # Task beendet

    # Nachricht wird vom Button-Handler gesendet
    # await update.message.reply_text(f"⏳ Starte Wiederherstellung für @{account_username} aus `{os.path.basename(backup_filepath)}`...")
    print(f"[Recover] Starte Wiederherstellung für @{account_username} aus {backup_filepath}...")

    # Diese Funktion läuft als Task und muss pause/resume selbst managen.
    await pause_scraping() # Pausiere Haupt-Scraping

    try:
        # 1. Backup-Datei lesen
        backup_users = load_set_from_file(backup_filepath)
        if not backup_users:
            await update.message.reply_text(f"ℹ️ Backup-Datei `{os.path.basename(backup_filepath)}` ist leer oder nicht vorhanden. Keine Wiederherstellung möglich.")
            # Kein 'return' hier, weiter zu finally für resume
        else:
            await update.message.reply_text(f"Gefunden: {len(backup_users)} User in der Backup-Datei.")

            # 2. Aktuelle Follow-Liste für diesen Account laden (aus Speicher)
            current_follow_list_set = set(current_account_usernames_to_follow)
            print(f"[Recover] Aktuell {len(current_follow_list_set)} User in der Follow-Liste für @{account_username}.")

            # 3. User zum Hinzufügen bestimmen
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
                 feedback = f"ℹ️ Keine neuen User zum Hinzufügen zur Liste von @{account_username} aus dem Backup gefunden."
                 if already_in_list_count > 0: feedback += f"\n{already_in_list_count} User waren bereits in der Liste."
                 if already_followed_globally_count > 0: feedback += f"\n{already_followed_globally_count} User werden bereits global gefolgt."
                 await update.message.reply_text(feedback)
                 # Kein 'return', weiter zu finally
            else:
                await update.message.reply_text(f"💾 Füge {len(users_to_add)} neue User zur Follow-Liste von @{account_username} hinzu...")

                # 4. Kombinierte Liste erstellen, globale Variable aktualisieren und speichern
                updated_list = list(current_follow_list_set.union(users_to_add))
                current_account_usernames_to_follow = updated_list # Globale Variable (Liste) aktualisieren
                save_current_account_follow_list() # Speichert die Liste unter dem Account-Pfad

                await update.message.reply_text(f"✅ Wiederherstellung für @{account_username} abgeschlossen! Liste enthält jetzt {len(current_account_usernames_to_follow)} User.")
                if already_in_list_count > 0: await update.message.reply_text(f"ℹ️ {already_in_list_count} User waren bereits in der Liste.")
                if already_followed_globally_count > 0: await update.message.reply_text(f"ℹ️ {already_followed_globally_count} User werden bereits global gefolgt und wurden nicht hinzugefügt.")

    except Exception as e:
        print(f"[Recover] Fehler im Wiederherstellungs-Prozess: {e}")
        try:
            await update.message.reply_text(f"❌ Ein Fehler ist während der Wiederherstellung aufgetreten: {e}")
        except: pass # Fehler beim Senden ignorieren
    finally:
        # Haupt-Scraping fortsetzen
        print("[Recover] Setze Scraping nach Wiederherstellung fort.")
        await resume_scraping() # Resume am Ende des Tasks

async def like_tweet(tweet_url):
    """Like a tweet on X"""
    try:
        print(f"Navigiere zu Tweet URL: {tweet_url}")
        # Save current URL to return later
        current_url = driver.current_url
        
        # Navigate to the tweet URL
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 5))
        
        # Scrolle NICHT auf der Seite - wichtig um Klick-Probleme zu vermeiden
        driver.execute_script("window.scrollTo(0, 0);")
        await asyncio.sleep(1)
        
        print("Suche Like-Button...")
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
                print(f"Versuche Selektor: {selector}")
                like_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if like_button:
                    break
            except:
                continue
                
        if not like_button:
            print("Kein Like-Button gefunden")
            # Return to original page
            driver.get(current_url)
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return False
            
        print("Like-Button gefunden, versuche JS-Klick...")
        # Versuche JavaScript-Klick statt normalen Klick
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", like_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", like_button)
        except Exception as e:
            print(f"JS-Klick fehlgeschlagen: {e}, versuche normalen Klick")
            try:
                like_button.click()
            except Exception as click_error:
                print(f"Auch normaler Klick fehlgeschlagen: {click_error}")
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
    """Lädt die Follow-Liste für den aktuellen Account."""
    global current_account_usernames_to_follow # Aktualisiert die globale Liste
    filepath = get_current_follow_list_path()
    account_username = get_current_account_username() or "Unbekannt"

    if filepath:
        loaded_set = load_set_from_file(filepath)
        current_account_usernames_to_follow = list(loaded_set) # Konvertiere zu Liste für random.choice
        print(f"Follow-Liste für Account @{account_username} geladen ({os.path.basename(filepath)}): {len(current_account_usernames_to_follow)} Namen")
    else:
        print(f"Konnte Follow-Liste für Account @{account_username} nicht laden: Pfad konnte nicht erstellt werden.")
        current_account_usernames_to_follow = [] # Setze auf leere Liste

def save_current_account_follow_list():
    """Speichert die Follow-Liste für den aktuellen Account."""
    global current_account_usernames_to_follow
    filepath = get_current_follow_list_path()
    account_username = get_current_account_username() or "Unbekannt"

    if filepath:
        # Stelle sicher, dass wir ein Set ohne Duplikate speichern
        save_set_to_file(set(current_account_usernames_to_follow), filepath)
        # print(f"Follow-Liste für Account @{account_username} gespeichert ({os.path.basename(filepath)})") # Optional
    else:
        print(f"Konnte Follow-Liste für Account @{account_username} nicht speichern: Pfad konnte nicht erstellt werden.")

async def repost_tweet(tweet_url):
    """Repost a tweet on X"""
    try:
        print(f"Navigiere zu Tweet URL für Repost: {tweet_url}")
        # Save current URL to return later
        current_url = driver.current_url
        
        # Navigate to the tweet URL
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 5))
        
        # Scrolle NICHT auf der Seite - wichtig um Klick-Probleme zu vermeiden
        driver.execute_script("window.scrollTo(0, 0);")
        await asyncio.sleep(1)
        
        print("Suche Repost-Button...")
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
                print(f"Versuche Repost-Selektor: {selector}")
                repost_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if repost_button:
                    break
            except:
                continue
                
        if not repost_button:
            print("Kein Repost-Button gefunden")
            # Return to original page
            driver.get(current_url)
            await asyncio.sleep(random.uniform(2, 3))
            await switch_to_following_tab()
            return False
            
        print("Repost-Button gefunden, versuche JS-Klick...")
        # Versuche JavaScript-Klick statt normalen Klick
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", repost_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", repost_button)
        except Exception as e:
            print(f"JS-Klick fehlgeschlagen: {e}, versuche normalen Klick")
            try:
                repost_button.click()
            except Exception as click_error:
                print(f"Auch normaler Klick fehlgeschlagen: {click_error}")
                # Return to original page
                driver.get(current_url)
                await asyncio.sleep(random.uniform(2, 3))
                await switch_to_following_tab()
                return False
        
        # Wait for the menu to open
        await asyncio.sleep(random.uniform(2, 3))
        
        print("Suche Bestätigungs-Button...")
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
                print(f"Versuche Bestätigungs-Selektor: {selector}")
                confirm_button = WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                if confirm_button:
                    break
            except:
                continue
                
        if not confirm_button:
            print("Kein Bestätigungs-Button gefunden")
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
        
        print("Bestätigungs-Button gefunden, versuche JS-Klick...")
        # Versuche JavaScript-Klick für den Bestätigungs-Button
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", confirm_button)
            await asyncio.sleep(1)
            driver.execute_script("arguments[0].click();", confirm_button)
        except Exception as e:
            print(f"JS-Klick fehlgeschlagen: {e}, versuche normalen Klick")
            try:
                confirm_button.click()
            except Exception as click_error:
                print(f"Auch normaler Klick fehlgeschlagen: {click_error}")
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
    """Navigates to a tweet URL, scrapes its full text content, and navigates back."""
    global driver
    try:
        print(f"Navigating to {tweet_url} to get full text...")
        # Save current URL to potentially return later if needed, although not strictly necessary here
        # current_url = driver.current_url
        driver.get(tweet_url)
        await asyncio.sleep(random.uniform(3, 6)) # Allow time for page load

        # Wait for the main tweet text element to be present
        tweet_text_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//article[@data-testid="tweet"]//div[@data-testid="tweetText"]'))
            # Using a more specific path within the article to target the main tweet
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
            print(f"WARNUNG: Fehler bei der Rückkehr zur Haupt-Timeline nach get_full_text: {nav_err}")
            # Try to recover, but proceed anyway
            try:
                driver.get("https://x.com/home") # Second attempt
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except: pass # Ignore further errors here
        # --- End Navigation Back ---

        return full_text

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"Error finding tweet text element for {tweet_url}: {e}")
        # --- Navigate back even on error ---
        print("Navigating back to home timeline after finding error in get_full_text...")
        try:
            driver.get("https://x.com/home")
            await asyncio.sleep(random.uniform(2, 4))
            await switch_to_following_tab()
            print("Successfully navigated back to home 'Following' tab after finding error.")
        except Exception as nav_err:
            print(f"WARNUNG: Fehler bei der Rückkehr zur Haupt-Timeline nach Fehler in get_full_text: {nav_err}")
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
            print(f"WARNUNG: Fehler bei der Rückkehr zur Haupt-Timeline nach unerwartetem Fehler in get_full_text: {nav_err}")
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
    try:
        if os.path.exists(SCHEDULE_FILE):
            with open(SCHEDULE_FILE, 'r') as f:
                data = json.load(f)
                schedule_enabled = data.get("enabled", False)
                schedule_pause_start = data.get("pause_start", "00:00")
                schedule_pause_end = data.get("pause_end", "00:00")
    except Exception as e:
        print(f"Error loading schedule settings: {e}")
        schedule_enabled = False
        schedule_pause_start = "00:00"
        schedule_pause_end = "00:00"

def save_schedule():
    """Save schedule settings to file"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    try:
        data = {
            "enabled": schedule_enabled,
            "pause_start": schedule_pause_start,
            "pause_end": schedule_pause_end
        }
        with open(SCHEDULE_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving schedule settings: {e}")

def check_schedule():
    """
    Prüft, ob die aktuelle Zeit innerhalb des geplanten Pausenzeitraums liegt.
    Verwendet Zeitzonen für Genauigkeit.
    Returns:
        True: Wenn der Bot läuft, aber jetzt pausieren sollte.
        "resume": Wenn der Bot wegen des Zeitplans pausiert ist und jetzt fortgesetzt werden soll.
        False: Wenn keine Zustandsänderung aufgrund des Zeitplans erforderlich ist.
    """
    global schedule_enabled, schedule_pause_start, schedule_pause_end, is_scraping_paused, is_schedule_pause

    if not schedule_enabled:
        return False

    try:
        try:
            local_tz = ZoneInfo("Europe/Berlin")
        except Exception:
            print("WARNUNG: Konnte Zeitzone 'Europe/Berlin' nicht laden, verwende Fallback.")
            local_tz = ZoneInfo(None) # Nutzt den Fallback

        now_local = datetime.now(local_tz)
        today_local = now_local.date()

        start_naive = datetime.strptime(f"{today_local} {schedule_pause_start}", "%Y-%m-%d %H:%M")
        end_naive = datetime.strptime(f"{today_local} {schedule_pause_end}", "%Y-%m-%d %H:%M")
        start_dt = start_naive.replace(tzinfo=local_tz)
        end_dt = end_naive.replace(tzinfo=local_tz) # Endzeit für HEUTE

        # print(f"DEBUG check_schedule: Now={now_local}, Start={start_dt}, End={end_dt}") # DEBUG

        is_in_pause_period = False
        # Prüfen, ob der Zeitraum über Mitternacht geht
        if end_dt <= start_dt:  # Overnight case (e.g., 22:00 - 09:00)
            # Pause aktiv, wenn:
            # 1) Nach der Startzeit am *selben* Tag (z.B. jetzt 23:00, start 22:00)
            # ODER
            # 2) Vor der Endzeit am *nächsten* Tag (z.B. jetzt 08:00, ende 09:00) - hier verwenden wir end_dt von HEUTE für den Vergleich
            if now_local >= start_dt or now_local < end_dt:
                 is_in_pause_period = True
                 # print(f"DEBUG check_schedule: In overnight period.") # DEBUG
            # else:
                 # print(f"DEBUG check_schedule: Outside overnight period.") # DEBUG
        else:  # Same day case (e.g., 10:00 - 17:00)
            # Pause aktiv, wenn zwischen Start (inkl.) und Ende (exkl.)
            if start_dt <= now_local < end_dt:
                is_in_pause_period = True
                # print(f"DEBUG check_schedule: In same day period.") # DEBUG
            # else:
                 # print(f"DEBUG check_schedule: Outside same day period.") # DEBUG

    except ValueError:
        print(f"FEHLER: Ungültiges Zeitformat im Schedule ({schedule_pause_start}-{schedule_pause_end}). Schedule wird ignoriert.")
        return False
    except Exception as e:
        print(f"FEHLER bei der Schedule-Prüfung: {e}")
        return False

    # --- Entscheidungslogik ---
    # print(f"DEBUG check_schedule: Final check: is_in_pause={is_in_pause_period}, is_scraping_paused={is_scraping_paused}, is_schedule_pause={is_schedule_pause}") # DEBUG
    if is_in_pause_period:
        if not is_scraping_paused:
            # print("DEBUG check_schedule: Returning True (Start Pause)") # DEBUG
            return True
        else:
            # print("DEBUG check_schedule: Returning False (already paused)") # DEBUG
            return False
    else: # Außerhalb der Pause
        if is_scraping_paused and is_schedule_pause:
            # print("DEBUG check_schedule: Returning 'resume'") # DEBUG
            return "resume"
        else:
            # print("DEBUG check_schedule: Returning False (running or manual pause)") # DEBUG
            return False

# Neue Funktionen für Pause/Resume
async def pause_scraping():
    """Pausiert das Tweet-Scraping"""
    global is_scraping_paused, driver
    is_scraping_paused = True
    pause_event.clear()
    print("Scraping wird pausiert...")
    
    # Stoppt aktive Scrolling-Operationen
    try:
        driver.execute_script("window.stop();")
    except:
        pass
    
    # Warte kurz, um sicherzustellen, dass laufende Operationen abgeschlossen werden
    await asyncio.sleep(1)
    print("Scraping ist jetzt pausiert")
    save_settings() # Speichere den neuen Pausenstatus

async def resume_scraping():
    """Setzt das Tweet-Scraping fort"""
    global is_scraping_paused
    is_scraping_paused = False
    pause_event.set()
    print("Scraping fortgesetzt")
    save_settings() # Speichere den neuen Laufstatus

async def scrape_following_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler für /scrapefollowing <username>. Startet den Scrape-Prozess für
    die Following-Liste des Ziel-Users als Hintergrund-Task.
    """
    global is_db_scrape_running # Prüfen, ob bereits ein Scrape läuft

    # Argumentprüfung
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Bitte gib genau EINEN X-Usernamen nach dem Befehl an.\nFormat: `/scrapefollowing <username>`")
        # Kein resume nötig, da der Admin-Wrapper das macht
        return

    target_username = context.args[0].strip().lstrip('@')
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', target_username):
        await update.message.reply_text(f"❌ Ungültiger Ziel-Username: {target_username}")
        return

    # Prüfen, ob bereits ein Scrape läuft
    if is_db_scrape_running:
        await update.message.reply_text("⚠️ Ein Datenbank-Scrape-Prozess läuft bereits. Bitte warte oder verwende `/canceldbscrape`.")
        return

    # Nachricht senden, dass der Task gestartet wird
    await update.message.reply_text(f"✅ Datenbank-Scrape für @{target_username} wird im Hintergrund gestartet...")

    # Starte die eigentliche Logik als Hintergrund-Task
    asyncio.create_task(scrape_target_following(update, target_username))

    # Kein resume_scraping hier, der Task läuft unabhängig und managed das selbst.

async def add_from_db_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler für /addfromdb mit flexiblen Filtern. Fügt User aus der
    following_database zur Follow-Liste des *aktuellen* Accounts hinzu.

    Syntax: /addfromdb [followers:NUM] [seen:NUM] [keywords:WORT1 WORT2...]
    Mindestens ein Kriterium muss angegeben werden.
    """
    global following_database, current_account_usernames_to_follow, global_followed_users_set
    # is_scraping_paused wird vom Admin-Wrapper gehandhabt

    # --- Filter-Variablen initialisieren ---
    min_followers = -1 # -1 bedeutet: nicht angegeben/aktiv
    min_seen = 0      # 0 bedeutet: nicht angegeben/aktiv (Standard war 1, ändern wir hier)
    keywords = []
    criteria_count = 0 # Zählt, wie viele Kriterien angegeben wurden

    # --- Argumente parsen ---
    current_keyword = None
    keyword_args = []
    if context.args:
        for arg in context.args:
            arg_lower = arg.lower()
            if arg_lower.startswith("followers:"):
                try:
                    # --- KORRIGIERT: Verwende die neue Parsing-Funktion ---
                    follower_str = arg.split(":", 1)[1]
                    if not follower_str: # Prüfen, ob nach ":" etwas kommt
                        raise IndexError("Leerer Wert nach followers:")

                    min_followers = parse_follower_count(follower_str)
                    # parse_follower_count gibt 0 bei Fehler zurück, was als gültiger Mindestwert behandelt wird.
                    # Eine explizite Prüfung auf < 0 ist nicht mehr nötig.

                    criteria_count += 1
                    current_keyword = None # Beende Keyword-Sammlung
                except IndexError:
                    await update.message.reply_text(f"❌ Fehlender Wert für 'followers:'.")
                    return
                # ValueError wird jetzt von parse_follower_count intern behandelt.
            elif arg_lower.startswith("seen:"):
                try:
                    min_seen = int(arg.split(":", 1)[1])
                    if min_seen < 1: raise ValueError("Seen muss >= 1 sein")
                    criteria_count += 1
                    current_keyword = None # Beende Keyword-Sammlung
                except (ValueError, IndexError):
                    await update.message.reply_text(f"❌ Ungültiger Wert für 'seen:'. Bitte eine Zahl >= 1 angeben.")
                    return
            elif arg_lower.startswith("keywords:"):
                criteria_count += 1
                current_keyword = "keywords" # Markiere, dass wir Keywords erwarten
                # --- KORRIGIERT: Verarbeite Keywords direkt ---
                value_part = arg.split(":", 1)[1] # Hole alles nach 'keywords:'
                # Teile nach Komma ODER Leerzeichen, entferne leere Einträge
                found_kws = [kw.strip() for kw in re.split(r'[,\s]+', value_part) if kw.strip()]
                if found_kws:
                    keywords.extend(found_kws) # Füge die gefundenen Keywords direkt hinzu
                else:
                    # Wenn nach 'keywords:' nichts oder nur Leerzeichen/Kommas kommen
                    pass # Warte auf nachfolgende Argumente
            elif current_keyword == "keywords":
                # --- KORRIGIERT: Behandle nachfolgende Argumente als einzelne Keywords ---
                # Teile auch hier nach Komma/Leerzeichen, falls mehrere in einem Arg stehen
                found_kws = [kw.strip() for kw in re.split(r'[,\s]+', arg) if kw.strip()]
                keywords.extend(found_kws)
                # Bleibe im Keyword-Modus, falls weitere Argumente kommen
            else:
                # Argument gehört zu keinem bekannten Schlüssel
                await update.message.reply_text(f"❓ Unbekanntes Argument oder fehlender Schlüssel: '{arg}'.\nVerwende `followers:`, `seen:` oder `keywords:`.")
                return

    # --- KORRIGIERT: Finale Bereinigung der Keywords ---
    # Stelle sicher, dass alle Keywords klein geschrieben und einzigartig sind
    if keywords:
        keywords = sorted(list(set(kw.lower() for kw in keywords if kw))) # Eindeutig, Kleinschreibung, sortiert

    # Entferne die alte Verarbeitung von keyword_args am Ende:
    # if keyword_args:
    #    keywords = " ".join(keyword_args).lower().split() # Diese Zeile wird entfernt/ersetzt

    # --- Prüfen, ob mindestens ein Kriterium angegeben wurde ---
    if criteria_count == 0:
        await update.message.reply_text(
            "❌ Bitte mindestens ein Filterkriterium angeben.\n"
            "Syntax: `/addfromdb [followers:NUM] [seen:NUM] [keywords:WORT1 WORT2...]`\n"
            "Beispiele:\n"
            "`/addfromdb followers:100000`\n"
            "`/addfromdb keywords:crypto nft`\n"
            "`/addfromdb seen:3 keywords:developer`\n"
            "`/addfromdb followers:5000 seen:2 keywords:web3`",
            parse_mode=ParseMode.MARKDOWN 
        )
        return

    # --- Account-Infos holen ---
    account_username = get_current_account_username()
    current_follow_list_path = get_current_follow_list_path()

    if not account_username or not current_follow_list_path:
        await update.message.reply_text("❌ Fehler: Aktiver Account-Username/Listenpfad nicht gefunden.")
        return

    if not following_database:
        await update.message.reply_text("ℹ️ Die Following-Datenbank ist leer. Führe zuerst `/scrapefollowing` aus.")
        return

    # --- User aus DB filtern basierend auf den *aktiven* Kriterien ---
    qualified_users = set()
    print(f"[AddFromDB] Filter: followers>={min_followers}, seen>={min_seen}, keywords={keywords}") # Debug
    for username, data in following_database.items():
        # Standardmäßig qualifiziert, wird bei Nichterfüllung auf False gesetzt
        is_qualified = True

        # 1. Follower-Check (nur wenn Kriterium aktiv)
        if min_followers != -1:
            f_count = data.get("follower_count", -1) # -1 wenn nicht vorhanden
            if not isinstance(f_count, int) or f_count < min_followers:
                is_qualified = False
                # print(f"  - @{username} disqualifiziert (Follower: {f_count} < {min_followers})") # Debug

        # 2. Seen-Check (nur wenn Kriterium aktiv und noch qualifiziert)
        if is_qualified and min_seen != 0:
            s_count = data.get("seen_count", 0)
            if not isinstance(s_count, int) or s_count < min_seen:
                is_qualified = False
                # print(f"  - @{username} disqualifiziert (Seen: {s_count} < {min_seen})") # Debug

        # 3. Keyword-Check (nur wenn Kriterium aktiv und noch qualifiziert)
        if is_qualified and keywords:
            bio_lower = data.get("bio", "").lower()
            # Prüfe, ob *alle* angegebenen Keywords in der Bio vorkommen
            if not all(kw in bio_lower for kw in keywords):
                is_qualified = False
                # print(f"  - @{username} disqualifiziert (Keywords nicht alle in Bio gefunden)") # Debug

        # Wenn nach allen aktiven Checks immer noch qualifiziert, hinzufügen
        if is_qualified:
            qualified_users.add(username)
            # print(f"  + @{username} qualifiziert!") # Debug

    if not qualified_users:
        criteria_str = []
        if min_followers != -1: criteria_str.append(f"F>={min_followers}")
        if min_seen != 0: criteria_str.append(f"S>={min_seen}")
        if keywords: criteria_str.append(f"KW='{' '.join(keywords)}'")
        await update.message.reply_text(f"ℹ️ Keine User in der Datenbank erfüllen die Kriterien ({', '.join(criteria_str)}).")
        return

    # --- Filtern gegen globale und aktuelle Liste (wie vorher) ---
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

    # --- Ergebnisnachricht bauen ---
    criteria_summary = []
    if min_followers != -1: criteria_summary.append(f"Followers ≥ {min_followers}")
    if min_seen != 0: criteria_summary.append(f"Seen ≥ {min_seen}")
    if keywords: criteria_summary.append(f"Keywords: '{' '.join(keywords)}'")
    response = f"📊 Filter-Ergebnis ({', '.join(criteria_summary)}):\n"
    response += f"- {len(qualified_users)} User qualifiziert.\n"

    if added_to_current_account:
        current_account_usernames_to_follow.extend(list(added_to_current_account))
        save_current_account_follow_list()
        response += f"✅ {len(added_to_current_account)} User zur Liste von @{account_username} hinzugefügt.\n"

    if already_in_current_list:
         response += f"ℹ️ {len(already_in_current_list)} davon waren bereits in der Liste von @{account_username}.\n"
    if already_followed_globally:
        response += f"🚫 {len(already_followed_globally)} davon werden bereits global gefolgt.\n"

    if not added_to_current_account and not already_in_current_list and not already_followed_globally and qualified_users:
         response += "ℹ️ Alle qualifizierten User sind bereits global gefolgt oder in der Liste.\n"

    await update.message.reply_text(response.strip())
    # resume_scraping wird vom Admin-Wrapper gemacht

async def cancel_db_scrape_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fordert den Abbruch des laufenden Datenbank-Scrape-Prozesses an."""
    global is_db_scrape_running, cancel_db_scrape_flag
    if is_db_scrape_running:
        cancel_db_scrape_flag = True
        await update.message.reply_text("🟡 Abbruch des Datenbank-Scrapes angefordert. Es kann einen Moment dauern...")
        print("[Cancel] DB Scrape cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ Aktuell läuft kein Datenbank-Scrape-Prozess.")
    # Kein resume/pause hier, dieser Befehl beeinflusst nur das Flag

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle callback queries with improved logging and robustness."""
    global current_account_usernames_to_follow
    query = update.callback_query
    # === 1. SOFORT antworten ===
    try:
        await query.answer()
        logger.debug(f"CallbackQuery answered for data: {query.data}")
    except Exception as answer_err:
        logger.error(f"FATAL: Failed to answer CallbackQuery for data {query.data}: {answer_err}", exc_info=True)
        return

    # === 2. Hauptlogik mit umfassendem Try-Except ===
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
                 await query.edit_message_text("❌ Ungültiges Sync-Callback-Format.")
                 return

            # Teile Aktion und optionalen Username
            action_parts = parts[1].split(":", 1)
            action = action_parts[0]
            # Extrahiere den Username *immer*, wenn vorhanden (für Konsistenzprüfung)
            target_username_from_callback = action_parts[1] if len(action_parts) > 1 else None

            current_active_username = get_current_account_username()

            # --- Konsistenzprüfung: Ist der Account noch derselbe? ---
            # Mache diese Prüfung für alle Aktionen, die einen target_username haben
            if target_username_from_callback and target_username_from_callback != current_active_username:
                logger.warning(f"Sync target mismatch: Button was for @{target_username_from_callback}, but @{current_active_username} is now active.")
                await query.edit_message_text(f"❌ Fehler: Button war für @{target_username_from_callback}, aber @{current_active_username} ist jetzt aktiv. Bitte /syncfollows erneut ausführen.")
                return

            # --- Aktionen verarbeiten ---
            if action == "create_backup": # Wird nur im Fall "Kein Backup" angeboten
                # Admin Check nicht nötig für Backup-Erstellung selbst
                await query.edit_message_text("✅ Backup für den aktuellen Account wird im Hintergrund gestartet...")
                # Erstelle eine emulierte Update-Instanz für backup_followers_logic
                emulated_update_for_backup = type('obj', (object,), {'message': query.message})
                asyncio.create_task(backup_followers_logic(emulated_update_for_backup))
                logger.info("Follower backup task started via sync callback (create_backup option).")

            elif action == "proceed": # Dies ist der Fall "Backup fehlt/leer, User will hinzufügen"
                # +++ Admin Check HIER +++
                if not is_user_admin(query.from_user.id):
                    logger.warning(f"User {query.from_user.id} tried to proceed sync (no backup) without admin rights.")
                    await query.answer("❌ Zugriff verweigert (Admin benötigt).", show_alert=True)
                    try: await query.edit_message_text("❌ Aktion abgebrochen (Keine Admin-Rechte).")
                    except: pass
                    return # Abbrechen
                # --- Ende Admin Check ---
                await query.edit_message_text(f"✅ Sync (nur Hinzufügen) für @{current_active_username} wird im Hintergrund gestartet...")
                backup_filepath = get_current_backup_file_path()
                # Lade die globale Liste frisch, bevor der Task startet
                global_set_for_task = load_set_from_file(GLOBAL_FOLLOWED_FILE)
                # Erstelle eine emulierte Update-Instanz für sync_followers_logic
                emulated_update_for_sync = type('obj', (object,), {'message': query.message})
                asyncio.create_task(sync_followers_logic(emulated_update_for_sync, current_active_username, backup_filepath, global_set_for_task))
                logger.info("Follower sync task started via sync callback (proceed - no backup case).")

            elif action == "proceed_sync": # Dies ist der Fall "Backup existiert, User bestätigt Sync"
                # +++ Admin Check HIER +++
                if not is_user_admin(query.from_user.id):
                    logger.warning(f"User {query.from_user.id} tried to proceed sync (normal) without admin rights.")
                    await query.answer("❌ Zugriff verweigert (Admin benötigt).", show_alert=True)
                    try: await query.edit_message_text("❌ Aktion abgebrochen (Keine Admin-Rechte).")
                    except: pass
                    return # Abbrechen
                # --- Ende Admin Check ---
                await query.edit_message_text(f"✅ Sync für @{current_active_username} wird im Hintergrund gestartet...")
                backup_filepath = get_current_backup_file_path()
                # Lade die globale Liste frisch, bevor der Task startet
                global_set_for_task = load_set_from_file(GLOBAL_FOLLOWED_FILE)
                # Erstelle eine emulierte Update-Instanz für sync_followers_logic
                emulated_update_for_sync = type('obj', (object,), {'message': query.message})
                asyncio.create_task(sync_followers_logic(emulated_update_for_sync, current_active_username, backup_filepath, global_set_for_task))
                logger.info("Follower sync task started via sync callback (proceed_sync - normal case).")

            elif action == "cancel_sync": # Dies ist der Cancel-Handler (wird für beide Fälle verwendet)
                username_display = f" für @{target_username_from_callback}" if target_username_from_callback else ""
                await query.edit_message_text(f"❌ Synchronisation{username_display} abgebrochen.")
                logger.info(f"Sync cancelled by user (cancel_sync callback for {target_username_from_callback or 'N/A'}).")
                await resume_scraping()
            else:
                logger.warning(f"Unknown sync action received: {action}")
                await query.edit_message_text(f"❌ Unbekannte Sync-Aktion: {action}")
                await resume_scraping()

            return # Sync Callbacks brauchen kein resume hier

        # ===== CLEAR FOLLOW LIST CALLBACKS =====
        elif action_type == "confirm_clear_follow_list":
             logger.info("Processing confirm_clear_follow_list callback.")
              # +++ Admin Check HIER +++
             if not is_user_admin(query.from_user.id):
                 logger.warning(f"User {query.from_user.id} tried to confirm clear list without admin rights.")
                 await query.answer("❌ Zugriff verweigert (Admin benötigt).", show_alert=True)
                 try: await query.edit_message_text("❌ Aktion abgebrochen (Keine Admin-Rechte).")
                 except: pass
                 return # Abbrechen
             # --- Ende Admin Check ---
   
             if len(parts) < 2:
                  logger.warning("Invalid clear_follow_list callback format.")
                  await query.edit_message_text("❌ Ungültiges Clear-Callback-Format.")
                  return
             target_username = parts[1]
             current_active_username = get_current_account_username()
             if target_username == current_active_username:
                 current_account_usernames_to_follow = []
                 save_current_account_follow_list()
                 filepath = get_current_follow_list_path()
                 filename = os.path.basename(filepath) if filepath else "N/A"
                 await query.edit_message_text(f"🗑️ Follow-Liste für @{current_active_username} (`{filename}`) wurde geleert.")
                 logger.info(f"Follow list for @{current_active_username} cleared via button.")
                 await resume_scraping()
             else:
                  logger.warning(f"Clear list target mismatch: Button for {target_username}, active is {current_active_username}")
                  await query.edit_message_text(f"❌ Fehler: Button war für Account @{target_username}, aber @{current_active_username} ist aktiv.")
                  await resume_scraping()
             return # Schnelle Aktion, kein resume

        elif action_type == "cancel_clear_follow_list":
            logger.info("Processing cancel_clear_follow_list callback.")
            await query.edit_message_text("❌ Löschen der Follow-Liste abgebrochen.")
            await resume_scraping()
            return # Schnelle Aktion, kein resume

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
                await query.edit_message_text(f"✅ Globale Follower-Liste erfolgreich aktualisiert ({len(final_global_set)} User).")
            except Exception as e:
                 logger.error(f"Error during build_global process: {e}", exc_info=True)
                 await query.edit_message_text(f"❌ Fehler beim Aktualisieren der globalen Liste: {e}")
            return # Schnelle Aktion, kein resume

        elif action_type == "cancel_build_global":
            logger.info("Processing cancel_build_global callback.")
            await query.edit_message_text("❌ Aktualisierung der globalen Liste abgebrochen.")
            return # Schnelle Aktion, kein resume

        # ===== HELP CALLBACKS =====
        elif action_type == "help":
            if len(parts) < 2:
                 logger.warning("Invalid help callback format.")
                 await query.edit_message_text("❌ Ungültiges Help-Callback-Format.")
                 return
            payload = parts[1] # Payload wird HIER definiert
            logger.info(f"Processing help payload: {payload}")

            # --- Tasks ---
            if payload == "backup_followers":
                 await query.message.reply_text("✅ Follower-Backup wird im Hintergrund gestartet...")
                 asyncio.create_task(backup_followers_logic(emulated_update))
                 logger.info("Follower backup task started via help callback.")
                 return # Task läuft, kein resume hier
            elif payload == "sync_follows":
                 await query.message.reply_text("✅ Starte Prüfung für Follower-Synchronisation...")
                 await sync_followers_command(emulated_update, None)
                 logger.info("sync_followers_command called via help callback.")
                 return # sync_followers_command managed resume/task

            # --- Direkte Command-Aufrufe (managen ihr eigenes pause/resume) ---
            elif payload in ["pause", "resume", "mode_full", "mode_ca", "stats", "ping", "keywords", "account", "schedule_on", "schedule_off", "schedule", "mode", "help", "show_rates", "build_global", "global_info", "status"]: 
                 logger.debug(f"Calling command handler for help payload: {payload}")
                 if payload == "pause": await pause_command(emulated_update, None)
                 elif payload == "resume": await resume_command(emulated_update, None)
                 elif payload == "mode_full": await mode_full_command(emulated_update, None)
                 elif payload == "mode_ca": await mode_ca_command(emulated_update, None)
                 elif payload == "stats": await stats_command(emulated_update, None)
                 elif payload == "ping": await ping_command(emulated_update, None)
                 elif payload == "keywords": await keywords_command(emulated_update, None)
                 elif payload == "account": await account_command(emulated_update, None)
                 elif payload == "schedule_on": await schedule_on_command(emulated_update, None)
                 elif payload == "schedule_off": await schedule_off_command(emulated_update, None)
                 elif payload == "schedule": await schedule_command(emulated_update, None)
                 # elif payload == "set_schedule": await show_schedule_set_command(emulated_update) # show_schedule_set_command braucht kein context
                 elif payload == "mode": await mode_command(emulated_update, None)
                 elif payload == "help": await help_command(emulated_update, None)
                 elif payload == "show_rates": await show_ratings_command(emulated_update, None)
                 elif payload == "build_global": await build_global_from_backups_command(emulated_update, None) # Aufruf für Button
                 elif payload == "status": await status_command(emulated_update, None) 
                 return # Die aufgerufenen Commands machen resume

            # --- "Prepare" Payloads (senden nur Text) ---
            elif payload == "prepare_addusers":
                 logger.info("Processing prepare_addusers help payload.")
                 await query.message.reply_text("Kopiere, füge Usernamen hinzu:\n\n`/addusers `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_addkeyword":
                 logger.info("Processing prepare_addkeyword help payload.")
                 await query.message.reply_text("Kopiere, füge Keywords hinzu (Komma-getrennt):\n\n`/addkeyword `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_removekeyword":
                 logger.info("Processing prepare_removekeyword help payload.")
                 await query.message.reply_text("Kopiere, füge Keywords hinzu (Komma-getrennt):\n\n`/removekeyword `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_follow":
                 logger.info("Processing prepare_follow help payload.")
                 await query.message.reply_text("Kopiere und füge den Usernamen hinzu:\n\n`/follow `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_unfollow":
                 logger.info("Processing prepare_unfollow help payload.")
                 await query.message.reply_text("Kopiere und füge den Usernamen hinzu:\n\n`/unfollow `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_like":
                 logger.info("Processing prepare_like help payload.")
                 await query.message.reply_text("Kopiere und füge die Tweet-URL hinzu:\n\n`/like `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_repost":
                 logger.info("Processing prepare_repost help payload.")
                 await query.message.reply_text("Kopiere und füge die Tweet-URL hinzu:\n\n`/repost `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_switchaccount":
                 logger.info("Processing prepare_switchaccount help payload.")
                 await query.message.reply_text("Kopiere und füge die Account-Nummer hinzu:\n\n`/switchaccount `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "prepare_scheduletime":
                 logger.info("Processing prepare_scheduletime help payload.")
                 await query.message.reply_text("Kopiere und füge den Zeitbereich hinzu (HH:MM-HH:MM):\n\n`/scheduletime `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
            elif payload == "set_schedule": # Früher show_schedule_set_command
                 logger.info("Processing set_schedule help payload (now prepare_scheduletime).")
                 await query.message.reply_text("Kopiere und füge den Zeitbereich hinzu (HH:MM-HH:MM):\n\n`/scheduletime `", parse_mode=ParseMode.MARKDOWN)
                 await resume_scraping(); return
   
            elif payload == "cancel_action":
                 logger.info("Processing cancel_action.")
                 try:
                     # Verwende query.edit_message_text, um die Nachricht des Buttons zu bearbeiten
                     await query.edit_message_text("✅ Aktion abgebrochen.")
                 except telegram.error.BadRequest as e:
                     # Fehler abfangen, wenn die Nachricht nicht bearbeitet werden kann
                     # (z.B. weil sie zu alt ist oder bereits entfernt wurde)
                     if "message to edit not found" in str(e).lower() or "message can't be edited" in str(e).lower():
                         logger.warning(f"Could not edit message on cancel_action (likely old): {e}")
                         # Optional: Sende eine neue Nachricht als Bestätigung, wenn Bearbeiten fehlschlägt
                         # await query.message.reply_text("Aktion abgebrochen.")
                     else:
                         # Andere BadRequests loggen
                         logger.error(f"BadRequest editing message on cancel_action: {e}", exc_info=True)
                 except Exception as e:
                     # Andere unerwartete Fehler loggen
                     logger.error(f"Error editing message on cancel_action: {e}", exc_info=True)

                 # Scraping fortsetzen, auch wenn das Bearbeiten der Nachricht fehlschlägt
                 await resume_scraping() # Fortsetzen nach Abbruch
                 return
            else:
                 logger.warning(f"Unknown help payload received: {payload}")
                 await query.message.reply_text(f"❌ Unbekannte Hilfsaktion: {payload}")
                 await resume_scraping() # Fortsetzen bei unbekannter Aktion
                 return
            # --- Ende Help Payloads ---

        # ===== LIKE/REPOST CALLBACKS (Queueing with full markup info) =====
        elif action_type in ["like", "repost"]:
             if len(parts) < 2 or not parts[1].isdigit():
                  logger.warning(f"Invalid {action_type} callback format: {query.data}")
                  try: await query.answer(f"❌ Ungültiges Format.", show_alert=True)
                  except: pass
                  return

             tweet_id = parts[1]
             action_description = f"{action_type} für Tweet {tweet_id}"
             logger.info(f"Queueing action: {action_description}")

             # --- Sofortiges Feedback (nur den geklickten Button ändern) ---
             original_markup = query.message.reply_markup
             new_keyboard = []
             original_button_data = [] # Zum Speichern der Button-Daten für die Queue
             try:
                 await query.answer(f"⏳ {action_type.capitalize()} eingereiht...")
                 if original_markup:
                     for row_idx, row in enumerate(original_markup.inline_keyboard):
                         new_row = []
                         original_button_data.append([]) # Neue Reihe für Queue-Daten
                         for button in row:
                             original_button_data[row_idx].append({'text': button.text, 'callback_data': button.callback_data}) # Speichere Originaldaten
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
             # --- Ende Feedback ---

             # --- Aktion in Queue legen (mit originalen Button-Daten) ---
             try:
                 await action_queue.put((action_type, {
                     'tweet_id': tweet_id,
                     'chat_id': query.message.chat_id,
                     'message_id': query.message.message_id,
                     'original_callback_data': query.data, # Der geklickte Button
                     'original_keyboard_data': original_button_data # Die Struktur aller Buttons
                 }))
                 logger.info(f"Action {action_description} successfully added to queue with keyboard data.")
             except Exception as q_err:
                 logger.error(f"Failed to put action {action_description} into queue: {q_err}", exc_info=True)
                 try: await query.message.reply_text(f"❌ Fehler beim Einreihen der Aktion '{action_type}'.")
                 except: pass
             return # Handler ist hier fertig

        # ===== RATING CALLBACKS (Schnelle Aktion) =====
        elif action_type == "rate":
            logger.debug(f"Processing rate action: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
            if len(parts) < 2:
                 logger.warning("Invalid rating callback format.")
                 await query.answer("❌ Fehler: Ungültiges Rating-Format.", show_alert=True)
                 return
            try:
                sub_parts = parts[1].split(":", 2)
                if len(sub_parts) != 3:
                    logger.warning(f"Invalid rating format details: {parts[1]}")
                    await query.answer("❌ Fehler: Ungültiges Rating-Format (Details).", show_alert=True)
                    return
                rating_value_str, source_key, encoded_name = sub_parts
                rating_value = int(rating_value_str)
                try: decoded_name = base64.urlsafe_b64decode(encoded_name).decode()
                except Exception as dec_err: logger.error(f"Failed to decode name '{encoded_name}': {dec_err}", exc_info=True); decoded_name = source_key
                if not (1 <= rating_value <= 5):
                    logger.warning(f"Invalid rating value received: {rating_value}")
                    await query.answer("❌ Ungültiger Wert (1-5).", show_alert=True)
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

                try: # Optional: Buttons entfernen
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
                await query.answer("❌ Fehler: Ungültiger Rating-Wert.", show_alert=True)
            except Exception as rate_err:
                logger.error(f"Error processing rating for data {query.data}: {rate_err}", exc_info=True)
                await query.answer("❌ Fehler beim Speichern.", show_alert=True)
            return # Schnelle Aktion, kein resume

        elif action_type == "rate_noop":
             logger.debug("Ignoring click on rate_noop button.")
             return # Schnelle Aktion, kein resume


        # ===== FULL TEXT CALLBACK (Queueing with full markup info) =====
        elif action_type == "full":
            if len(parts) < 2 or not parts[1].isdigit():
                logger.warning(f"Invalid full_text callback format: {query.data}")
                try: await query.answer("❌ Fehler: Ungültiges Format.", show_alert=True)
                except: pass
                return

            tweet_id = parts[1]
            action_description = f"Get full text for {tweet_id}"
            logger.info(f"Queueing action: {action_description}")

            # --- Sofortiges Feedback (nur den geklickten Button ändern) ---
            original_markup = query.message.reply_markup
            new_keyboard = []
            original_button_data = [] # Zum Speichern der Button-Daten für die Queue
            try:
                await query.answer("⏳ Full Text eingereiht...")
                if original_markup:
                    for row_idx, row in enumerate(original_markup.inline_keyboard):
                        new_row = []
                        original_button_data.append([]) # Neue Reihe für Queue-Daten
                        for button in row:
                            original_button_data[row_idx].append({'text': button.text, 'callback_data': button.callback_data}) # Speichere Originaldaten
                            if button.callback_data == query.data:
                                new_row.append(InlineKeyboardButton("Lade Text (⏳)", callback_data="noop_processing"))
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
            # --- Ende Feedback ---

            # --- Aktion in Queue legen (mit originalen Button-Daten) ---
            try:
                await action_queue.put((action_type, {
                    'tweet_id': tweet_id,
                    'chat_id': query.message.chat_id,
                    'message_id': query.message.message_id,
                    'original_callback_data': query.data, # Der geklickte Button
                    'original_keyboard_data': original_button_data # Die Struktur aller Buttons
                }))
                logger.info(f"Action {action_description} successfully added to queue with keyboard data.")
            except Exception as q_err:
                logger.error(f"Failed to put action {action_description} into queue: {q_err}", exc_info=True)
                try: await query.message.reply_text(f"❌ Fehler beim Einreihen der Aktion 'Full Text'.")
                except: pass
            return # Handler ist hier fertig


    # ===== FULL TEXT CALLBACK (Langlaufende Aktion) =====
        elif action_type == "full_text":
            if len(parts) < 2:
                logger.warning("Invalid full_text callback format.")
                await query.answer("❌ Fehler: Ungültiges Format.", show_alert=True)
                return # Wichtig: Hier beenden bei Fehler
            # Verwende die URL direkt aus dem Callback
            tweet_url = parts[1]
            action_description = f"Get full text for {tweet_url}"
            # status_message = None # Nicht mehr benötigt für Edit
            logger.info(f"Starting long action: {action_description}")

            try:
                # Inform user (optional, can be quick)
                await query.answer("⏳ Hole vollen Text...")
            except Exception as answer_err:
                 logger.warning(f"Could not answer query for full_text: {answer_err}")


            logger.debug(f"Pausing scraping for action: {action_description}")
            await pause_scraping()
            full_tweet_text = None; error_detail = ""
            original_message = query.message # Get the original message object

            try:
                full_tweet_text = await get_full_tweet_text(tweet_url) # Holt Text UND navigiert zurück

                if full_tweet_text is not None:
                    logger.info(f"Action '{action_description}' completed. Full text length: {len(full_tweet_text)}")

                    # --- Reconstruct the message AND check for changes ---
                    original_text_html = original_message.text_html

                    # +++ NEUE PRÜFUNG: Sicherstellen, dass original_text_html ein String ist +++
                    if not isinstance(original_text_html, str):
                        logger.error(f"Original message text_html is not a string (type: {type(original_text_html)}) for message ID {original_message.message_id}. Cannot process full text update inline.")
                        # Informiere den User und sende den Text als neue Nachricht
                        await query.answer("ℹ️ Originalnachricht hat unerwartetes Format. Sende Text separat.", show_alert=False)
                        try:
                            escaped_full_text_fallback = html.escape(full_tweet_text)
                            await query.message.reply_text(f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text_fallback}</blockquote>\n\nFULL TEXT", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                        except Exception as reply_err:
                            logger.error(f"Failed to send full text as reply fallback (TypeError case): {reply_err}")
                            await query.answer("❌ Fehler beim Senden des vollen Texts.", show_alert=True)
                        # Wichtig: Scraping fortsetzen und Callback beenden
                        logger.debug(f"Resuming scraping after handling TypeError for full_text.")
                        await resume_scraping()
                        return # Beende die Verarbeitung dieses Callbacks hier
                    # --- ENDE NEUE PRÜFUNG ---

                    # Wenn wir hier sind, ist original_text_html ein String
                    blockquote_pattern = r"<blockquote>.*?</blockquote>"
                    escaped_full_text = html.escape(full_tweet_text)
                    new_blockquote = f"<blockquote>{escaped_full_text}</blockquote>"

                    # Find original blockquote content for comparison
                    original_blockquote_match = re.search(blockquote_pattern, original_text_html, flags=re.DOTALL)
                    text_needs_update = False # Flag to track if editing is necessary
                    new_message_text = original_text_html # Default to original text

                    if original_blockquote_match:
                        # Extract content between <blockquote> and </blockquote>
                        original_inner_html = original_blockquote_match.group(0)[len("<blockquote>"):-len("</blockquote>")]
                        # Unescape HTML entities to compare with the raw scraped text
                        original_inner_text = html.unescape(original_inner_html)

                        # Compare unescaped original text with the newly scraped text (strip whitespace)
                        if original_inner_text.strip() != full_tweet_text.strip():
                            logger.info(f"Text for {tweet_url} differs. Preparing update.")
                            text_needs_update = True
                            # Perform the replacement using regex
                            new_message_text, num_replacements = re.subn(blockquote_pattern, new_blockquote, original_text_html, count=1, flags=re.DOTALL)
                            if num_replacements == 0:
                                # Should not happen if original_blockquote_match succeeded, but safety check
                                logger.error(f"Blockquote found but replacement failed for {tweet_url}. This is unexpected.")
                                text_needs_update = False # Prevent faulty edit attempt
                        else:
                            logger.info(f"Full text for {tweet_url} is the same as the current text. No edit needed.")
                            await query.answer("ℹ️ Text ist bereits vollständig.", show_alert=False) # Inform user briefly
                    else:
                        # Original blockquote not found - cannot compare or replace inline.
                        # We will send the full text as a new message later.
                        logger.warning(f"Could not find blockquote in original message for {tweet_url}. Will send as new message.")
                        text_needs_update = False # Cannot edit inline

                    # --- Edit the original message ONLY if text changed and blockquote was found/replaced ---
                    if text_needs_update:
                        try:
                            # Add the marker before sending the edited message
                            new_message_text += "\n\n🔥 FULL TEXT"
                            await query.edit_message_text(
                                text=new_message_text, # Now includes the marker
                                parse_mode=ParseMode.HTML,
                                disable_web_page_preview=True,
                                reply_markup=original_message.reply_markup # Keep original buttons!
                            )
                            logger.info(f"Successfully updated message {original_message.message_id} with full text.")
                        except telegram.error.BadRequest as edit_error:
                            if "Message is not modified" in str(edit_error):
                                logger.warning(f"Edit failed for {tweet_url} because message was not modified (comparison might have failed).")
                                await query.answer("ℹ️ Text ist bereits vollständig.", show_alert=False)
                            else:
                                logger.error(f"BadRequest editing message {original_message.message_id}: {edit_error}", exc_info=True)
                                # Fallback: Send a new message
                                try:
                                    await query.message.reply_text(f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text}</blockquote>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                                except Exception as reply_err: logger.error(f"Failed to send full text as reply fallback (BadRequest): {reply_err}")
                        except Exception as edit_error:
                             logger.error(f"Unexpected error editing message {original_message.message_id}: {edit_error}", exc_info=True)
                             await query.answer("❌ Fehler beim Aktualisieren.", show_alert=True)
                    elif not original_blockquote_match and full_tweet_text is not None:
                         # Blockquote not found case: Send as new message
                         logger.info(f"Sending full text for {tweet_url} as a new message (blockquote not found).")
                         try:
                             await query.message.reply_text(f"<b>Full Text for <a href='{tweet_url}'>this post</a>:</b>\n<blockquote>{escaped_full_text}</blockquote>", parse_mode=ParseMode.HTML, disable_web_page_preview=True)
                         except Exception as reply_err:
                             logger.error(f"Failed to send full text as reply (no blockquote case): {reply_err}")
                             await query.answer("❌ Fehler beim Anzeigen des vollen Texts.", show_alert=True)
                    # else: If text was the same, we already sent the query.answer

                else:
                    # Scraping failed (full_tweet_text is None)
                    logger.error(f"Failed to get full text for {tweet_url}")
                    await query.answer("❌ Fehler beim Abrufen des vollen Texts.", show_alert=True)

            except Exception as action_error:
                logger.error(f"Error during action '{action_description}': {action_error}", exc_info=True)
                error_detail = f": {str(action_error)[:100]}"
                try: await query.answer(f"❌ Fehler{error_detail}", show_alert=True)
                except: pass # Ignore if answer fails
            finally:
                logger.debug(f"Resuming scraping after action: {action_description}")
                await resume_scraping() # Wichtig: Immer fortsetzen
            return # End of full_text handler

        # ===== STATUS CALLBACKS =====
        elif action_type == "status":
            logger.info(f"Processing status callback: {parts[1] if len(parts) > 1 else 'Invalid Format'}")
            if len(parts) < 2:
                 logger.warning("Invalid status callback format received.")
                 await query.edit_message_text("❌ Ungültiges Status-Callback-Format.")
                 return # Status Callbacks brauchen kein resume hier

            action = parts[1]
            if action == "show_follow_list":
                # Zeige die aktuelle Follow-Liste an
                account_username = get_current_account_username() or "Unbekannt"
                filepath = get_current_follow_list_path()
                filename = os.path.basename(filepath) if filepath else "N/A"

                list_content = current_account_usernames_to_follow
                if not list_content:
                    await query.message.reply_text(f"ℹ️ Die Follow-Liste für @{account_username} (`{filename}`) ist derzeit leer.")
                else:
                    # Bereite die Liste für die Anzeige vor
                    max_users_to_show = 100 # Limit, um Nachrichten nicht zu überladen
                    output_text = f"📝 **Follow-Liste für @{account_username} (`{filename}`)** ({len(list_content)} User):\n\n"
                    output_text += "\n".join([f"- `{user}`" for user in list_content[:max_users_to_show]]) # Markdown Code-Formatierung

                    if len(list_content) > max_users_to_show:
                        output_text += f"\n\n*... und {len(list_content) - max_users_to_show} weitere User.*"

                    # Sende als neue Nachricht (nicht die Statusnachricht bearbeiten)
                    try:
                        await query.message.reply_text(output_text, parse_mode=ParseMode.MARKDOWN)
                    except telegram.error.BadRequest as e:
                         if "message is too long" in str(e).lower():
                              await query.message.reply_text(f"❌ Fehler: Die Follow-Liste ist zu lang ({len(list_content)} User), um sie anzuzeigen.")
                         else: raise e # Anderen Fehler weitergeben
                    except Exception as e:
                         logger.error(f"Fehler beim Senden der Follow-Liste: {e}", exc_info=True)
                         await query.message.reply_text("❌ Fehler beim Anzeigen der Liste.")
                # Kein resume_scraping hier, da es eine schnelle Aktion ist
                return
            else:
                logger.warning(f"Unknown status action received: {action}")
                await query.edit_message_text(f"❌ Unbekannte Status-Aktion: {action}")
            return # Status Callbacks brauchen kein resume hier

        # ===== UNBEKANNTE AKTION =====
        else:
             logger.warning(f"Unknown button action type received: {action_type}")
             await query.message.reply_text(f"❌ Unbekannte Button-Aktion: {action_type}")
             await resume_scraping() # Im Zweifel fortsetzen
             return

    # === 3. Allgemeiner Fallback-Fehlerhandler ===
    except Exception as e:
        logger.error(f"Unhandled error in button_callback_handler for data '{query.data}': {e}", exc_info=True)
        try: await query.message.reply_text(text=f"❌ Unerwarteter Fehler bei Button-Aktion. Details siehe Log.")
        except Exception as send_error: logger.error(f"Failed to send error message to user after unhandled exception: {send_error}")
        if is_scraping_paused:
             logger.warning("Attempting to resume scraping after unhandled exception in button handler.")
             await resume_scraping()

def admin_required(func):
    """
    Decorator, der prüft, ob der ausführende User ein Admin ist.
    Funktioniert für CommandHandler.
    """
    @functools.wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if not update.message or not update.message.from_user:
            logger.warning(f"Admin check fehlgeschlagen für Funktion {func.__name__}: Kein User-Objekt.")
            return # Kann nicht prüfen

        user_id = update.message.from_user.id
        if is_user_admin(user_id):
            # User ist Admin, führe die Originalfunktion aus
            return await func(update, context, *args, **kwargs)
        else:
            # User ist kein Admin, sende Fehlermeldung
            logger.warning(f"Nicht autorisierter Zugriff auf '{func.__name__}' durch User {user_id}.")
            await update.message.reply_text("❌ Zugriff verweigert. Du bist kein Admin.")
            # Wichtig: Hier ggf. Scraping fortsetzen, wenn der Befehl pausiert hätte
            if 'pause_scraping' in func.__code__.co_names or 'resume_scraping' in func.__code__.co_names:
                 if is_scraping_paused: # Nur wenn es pausiert wurde
                      print(f"Admin Check: Setze Scraping fort nach verweigertem Zugriff auf {func.__name__}")
                      await resume_scraping()
            return # Funktion nicht ausführen
    return wrapper

# --- Keywords ---
async def keywords_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt die aktuellen Keywords an (Handler für /keywords)."""
    await pause_scraping() # Pause für die Dauer des Befehls
    global KEYWORDS
    keywords_text = "\n".join([f"- {keyword}" for keyword in KEYWORDS])
    await update.message.reply_text(f"🔑 Aktuelle Keywords:\n{keywords_text}")
    await resume_scraping() # Fortsetzen nach dem Befehl

async def add_keyword_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fügt ein oder mehrere Keywords hinzu (Handler für /addkeyword)."""
    await pause_scraping()
    global KEYWORDS
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Bitte gib die Keywords nach dem Befehl an (durch Komma getrennt).\n\n"
            "Format: `/addkeyword <wort1,wort2...>`\n\n"
            "Kopiere dies und füge deine Keywords hinzu:\n"
            "`/addkeyword `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    keyword_text = " ".join(context.args)
    keywords_to_add = [k.strip().lower() for k in keyword_text.split(',') if k.strip()] # Kleinschreibung erzwingen
    added = []
    already_exists = []

    for keyword in keywords_to_add:
        if keyword not in KEYWORDS:
            KEYWORDS.append(keyword)
            added.append(keyword)
        else:
            already_exists.append(keyword)

    if added: # Nur speichern, wenn etwas hinzugefügt wurde
        await save_keywords()

    response = ""
    if added:
        response += f"✅ {len(added)} Keywords hinzugefügt: {', '.join(added)}\n"
    if already_exists:
        response += f"⚠️ {len(already_exists)} Keywords existieren bereits: {', '.join(already_exists)}"

    await update.message.reply_text(response.strip() if response else "Keine neuen Keywords zum Hinzufügen gefunden.")
    # Zeige die aktualisierte Liste (ruft intern resume_scraping auf)
    await keywords_command(update, context)
    # Kein resume_scraping hier, da keywords_command es macht

async def remove_keyword_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entfernt ein oder mehrere Keywords (Handler für /removekeyword)."""
    await pause_scraping()
    global KEYWORDS
    if not context.args:
        await update.message.reply_text(
            "ℹ️ Bitte gib die Keywords nach dem Befehl an (durch Komma getrennt).\n\n"
            "Format: `/removekeyword <wort1,wort2...>`\n\n"
            "Kopiere dies und füge deine Keywords hinzu:\n"
            "`/removekeyword `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    keyword_text = " ".join(context.args)
    keywords_to_remove = [k.strip().lower() for k in keyword_text.split(',') if k.strip()] # Kleinschreibung
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

    if changed: # Nur speichern, wenn etwas entfernt wurde
        await save_keywords()

    response = ""
    if removed:
        response += f"🗑️ {len(removed)} Keywords entfernt: {', '.join(removed)}\n"
    if not_found:
        response += f"⚠️ {len(not_found)} Keywords nicht gefunden: {', '.join(not_found)}"

    await update.message.reply_text(response.strip() if response else "Keine Keywords zum Entfernen gefunden.")
    # Zeige die aktualisierte Liste (ruft intern resume_scraping auf)
    await keywords_command(update, context)
    # Kein resume_scraping hier

# --- Follow / Unfollow ---
async def follow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Folgt einem User (Handler für /follow)."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl prüfen ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Bitte gib genau EINEN X-Usernamen nach dem Befehl an.\n\n"
            "Format: `/follow <username>` (mit oder ohne @)\n\n"
            "Kopiere dies und füge den Usernamen hinzu:\n"
            "`/follow `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return
    # --- ENDE ÄNDERUNG ---

    username = context.args[0].lstrip('@').strip()
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', username):
        await update.message.reply_text("❌ Ungültiger Username-Format.") # Emoji hinzugefügt
        await resume_scraping()
        return

    # --- Logik aus process_follow_request hierher verschoben ---
    global global_followed_users_set
    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if not account_username or not backup_filepath:
         await update.message.reply_text("❌ Fehler: Aktiver Account kann nicht ermittelt werden für Follow-Aktualisierung.")
         await resume_scraping()
         return

    await update.message.reply_text(f"⏳ Versuche @{username} mit Account @{account_username} zu folgen...")
    result = await follow_user(username) # Führe den Follow-Versuch durch

    if result is True:
        # Erfolgsmeldung wird jetzt von follow_user() gesendet
        print(f"Manueller Follow erfolgreich: @{username}")
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
            print(f"@{username} zur globalen Followed-Liste hinzugefügt.")
        add_to_set_file({username}, backup_filepath)
        print(f"@{username} zum Account-Backup ({os.path.basename(backup_filepath)}) hinzugefügt.")
    elif result == "already_following":
        await update.message.reply_text(f"ℹ️ Account @{account_username} folgt @{username} bereits.")
        print(f"Manueller Follow: @{username} wurde bereits gefolgt.")
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE)
        add_to_set_file({username}, backup_filepath)
    else: # Fehlerfall (result is False)
        # Fehlermeldung wird jetzt von follow_user() gesendet
        print(f"Manueller Follow fehlgeschlagen: @{username}")
    # --- Ende Logik ---
    await resume_scraping()

async def unfollow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entfolgt einem User, entfernt ihn aus globaler Liste und aktuellem Backup."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl prüfen ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Bitte gib genau EINEN X-Usernamen nach dem Befehl an.\n\n"
            "Format: `/unfollow <username>` (mit oder ohne @)\n\n"
            "Kopiere dies und füge den Usernamen hinzu:\n"
            "`/unfollow `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return
    # --- ENDE ÄNDERUNG ---

    username_to_unfollow = context.args[0].lstrip('@').strip()
    if not re.match(r'^[A-Za-z0-9_]{1,15}$', username_to_unfollow):
        await update.message.reply_text("❌ Ungültiger Username-Format.") # Emoji hinzugefügt
        await resume_scraping()
        return

    # Zugriff auf globale Variable
    global global_followed_users_set

    await update.message.reply_text(f"🔍 Versuche @{username_to_unfollow} zu entfolgen...")
    result = await unfollow_user(username_to_unfollow)

    removed_from_global = False
    removed_from_backup = False
    current_backup_path = get_current_backup_file_path()
    current_account_username = get_current_account_username() or "Unbekannt"

    response_message = ""

    # --- Logik basierend auf Selenium-Ergebnis UND Listen-Konsistenz ---
    if result == "not_following" or result is True:
        # Selenium war erfolgreich ODER der Account folgte eh nicht mehr.
        # JETZT die Listen bereinigen.
        if result is True:
             response_message = f"✅ Erfolgreich @{username_to_unfollow} entfolgt!"
        else: # result == "not_following"
             response_message = f"ℹ️ Account @{current_account_username} folgt @{username_to_unfollow} nicht (mehr)."

        # Aus globaler Liste entfernen (falls noch drin)
        if username_to_unfollow in global_followed_users_set:
            global_followed_users_set.discard(username_to_unfollow)
            save_set_to_file(global_followed_users_set, GLOBAL_FOLLOWED_FILE)
            removed_from_global = True
            logger.info(f"User @{username_to_unfollow} removed from global list.")
        else:
            logger.debug(f"User @{username_to_unfollow} was already not in global list.")

        # Aus aktuellem Backup entfernen (falls noch drin)
        if current_backup_path:
            backup_set = load_set_from_file(current_backup_path)
            if username_to_unfollow in backup_set:
                backup_set.discard(username_to_unfollow)
                save_set_to_file(backup_set, current_backup_path)
                removed_from_backup = True
                logger.info(f"User @{username_to_unfollow} removed from backup for @{current_account_username}.")
            else:
                logger.debug(f"User @{username_to_unfollow} was already not in backup for @{current_account_username}.")

    else: # Fehler beim Unfollow (result is False)
        response_message = f"❌ Konnte @{username_to_unfollow} nicht entfolgen (Selenium-Fehler). Listen wurden NICHT geändert."
        logger.warning(f"Selenium failed to unfollow @{username_to_unfollow}. Lists remain unchanged.")

    # Zusätzliche Info über Listen-Änderungen zur Hauptnachricht hinzufügen
    if removed_from_global:
        response_message += f"\n🗑️ @{username_to_unfollow} aus globaler Liste entfernt."
    if removed_from_backup:
        response_message += f"\n🗑️ @{username_to_unfollow} aus Backup für @{current_account_username} entfernt."

    await update.message.reply_text(response_message)
    await resume_scraping()

# --- Like / Repost ---
async def like_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Liked einen Tweet per URL (Handler für /like)."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl und URL-Validierung ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Bitte gib genau EINE Tweet-URL nach dem Befehl an.\n\n"
            "Format: `/like <tweet_url>`\n\n"
            "Kopiere dies und füge die URL hinzu:\n"
            "`/like `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    input_url = context.args[0].strip()
    parsed_url = None
    try:
        # Versuche die URL zu parsen
        parsed_url = urlparse(input_url)
        # Prüfe Schema und Domain
        if not parsed_url.scheme in ['http', 'https'] or not (parsed_url.netloc.endswith('x.com') or parsed_url.netloc.endswith('twitter.com')):
            raise ValueError("Keine gültige X/Twitter Domain")
        # Prüfe, ob der Pfad einem Tweet ähnelt (optional, aber empfohlen)
        # Beispiel: /<username>/status/<id>
        if not re.match(r'^/[A-Za-z0-9_]+/status/\d+$', parsed_url.path):
             # Erlaube auch /i/status/<id>
             if not re.match(r'^/i/status/\d+$', parsed_url.path):
                  raise ValueError("URL scheint kein Tweet-Link zu sein")
        # Rekonstruiere die URL für Konsistenz (optional)
        tweet_url = parsed_url.geturl()

    except ValueError as e:
        await update.message.reply_text(f"❌ Ungültige oder nicht unterstützte URL: {e}.\nBitte gib eine vollständige X.com/Twitter.com Tweet-URL an.")
        await resume_scraping()
        return
    except Exception as e: # Andere Parsing-Fehler
         await update.message.reply_text(f"❌ Fehler beim Verarbeiten der URL: {e}")
         await resume_scraping()
         return
    # --- ENDE ÄNDERUNG ---

    # --- Bestehende Logik ---
    await update.message.reply_text(f"🔍 Versuche Tweet zu liken: {tweet_url}")
    try:
        result = await like_tweet(tweet_url)
        if result: await update.message.reply_text(f"✅ Tweet erfolgreich geliked!")
        else: await update.message.reply_text(f"❌ Konnte Tweet nicht liken")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Liken: {str(e)[:100]}")
    # --- Ende Bestehende Logik ---
    await resume_scraping()

async def repost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Repostet einen Tweet per URL (Handler für /repost)."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl und URL-Validierung ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Bitte gib genau EINE Tweet-URL nach dem Befehl an.\n\n"
            "Format: `/repost <tweet_url>`\n\n"
            "Kopiere dies und füge die URL hinzu:\n"
            "`/repost `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return

    input_url = context.args[0].strip()
    parsed_url = None
    try:
        # Versuche die URL zu parsen
        parsed_url = urlparse(input_url)
        # Prüfe Schema und Domain
        if not parsed_url.scheme in ['http', 'https'] or not (parsed_url.netloc.endswith('x.com') or parsed_url.netloc.endswith('twitter.com')):
            raise ValueError("Keine gültige X/Twitter Domain")
        # Prüfe, ob der Pfad einem Tweet ähnelt
        if not re.match(r'^/[A-Za-z0-9_]+/status/\d+$', parsed_url.path):
             if not re.match(r'^/i/status/\d+$', parsed_url.path):
                  raise ValueError("URL scheint kein Tweet-Link zu sein")
        tweet_url = parsed_url.geturl()

    except ValueError as e:
        await update.message.reply_text(f"❌ Ungültige oder nicht unterstützte URL: {e}.\nBitte gib eine vollständige X.com/Twitter.com Tweet-URL an.")
        await resume_scraping()
        return
    except Exception as e: # Andere Parsing-Fehler
         await update.message.reply_text(f"❌ Fehler beim Verarbeiten der URL: {e}")
         await resume_scraping()
         return
    # --- ENDE ÄNDERUNG ---

    # --- Bestehende Logik ---
    await update.message.reply_text(f"🔍 Versuche Tweet zu reposten: {tweet_url}")
    try:
        result = await repost_tweet(tweet_url)
        if result: await update.message.reply_text(f"✅ Tweet erfolgreich repostet!")
        else: await update.message.reply_text(f"❌ Konnte Tweet nicht reposten")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Reposten: {str(e)[:100]}")
    # --- Ende Bestehende Logik ---
    await resume_scraping()

# --- Account / Reboot / Shutdown / Help / Stats / Ping ---
async def account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt den aktuellen Account an (Handler für /account)."""
    await pause_scraping()
    await update.message.reply_text(f"🥷 Aktueller Account: {current_account+1} (@{get_current_account_username() or 'N/A'})")
    await resume_scraping()

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt die Hilfemeldung an (Handler für /help)."""
    await pause_scraping()
    # Die Funktion show_help_message muss angepasst werden, um die neue Syntax zu zeigen
    await show_help_message(update) # show_help_message macht selbst resume

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt Statistiken an (Handler für /stats)."""
    await pause_scraping()
    await show_post_counts(update) # show_post_counts macht selbst resume

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Antwortet mit Pong (Handler für /ping)."""
    await pause_scraping()
    await update.message.reply_text(f"🏓 Pong!")
    await resume_scraping()

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt einen umfassenden Betriebsstatus des Bots an."""

    # --- Globale Variablen sammeln ---
    global is_scraping_paused, is_schedule_pause, search_mode, schedule_enabled, \
           schedule_pause_start, schedule_pause_end, is_periodic_follow_active, \
           current_account, ACCOUNTS, global_followed_users_set, \
           current_account_usernames_to_follow, GLOBAL_FOLLOWED_FILE

    # --- Informationen sammeln ---

    # 1. Laufstatus
    if is_scraping_paused:
        running_status = "🟡 PAUSED (Schedule)" if is_schedule_pause else "🟡 PAUSED (Manual)"
    else:
        running_status = "🟢 RUNNING"

    # 2. Aktueller Account
    current_username = get_current_account_username() or "N/A"
    account_info = f"Acc {current_account+1} (@{current_username})"

    # 3. Suchmodus
    mode_text = "Full (CA + Keywords)" if search_mode == "full" else "CA ONLY"

    # 4. Zeitplan
    schedule_status = "🟢" if schedule_enabled else "🔴"
    schedule_details = f"{schedule_status} ({schedule_pause_start} - {schedule_pause_end})"

    # 5. Globale Follow-Liste Info
    global_list_count = len(global_followed_users_set) # In-Memory ist meist aktuell genug für Status
    global_list_mod_time_str = "N/A"
    try:
        if os.path.exists(GLOBAL_FOLLOWED_FILE):
            mod_timestamp = os.path.getmtime(GLOBAL_FOLLOWED_FILE)
            try:
                local_tz = ZoneInfo("Europe/Berlin")
            except Exception:
                local_tz = timezone(timedelta(hours=2)) # Fallback
            mod_datetime_local = datetime.fromtimestamp(mod_timestamp, tz=local_tz)
            # Nur Datum und Uhrzeit für Kompaktheit
            global_list_mod_time_str = mod_datetime_local.strftime('%Y-%m-%d %H:%M')
        else:
             global_list_count = 0 # Wenn Datei nicht existiert
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Global-List-Infos für Status: {e}")
        global_list_mod_time_str = "Fehler"
    global_list_info = f"{global_list_count} User (Stand: {global_list_mod_time_str})"

    # 6. Auto-Follow Status (für aktuellen Account)
    autofollow_stat = "🟢" if is_periodic_follow_active else "🟡"

    # 7. Aktuelle Account Follow-Liste Info + Vorschau
    current_list_path = get_current_follow_list_path()
    current_list_filename = os.path.basename(current_list_path) if current_list_path else "N/A"
    current_list_count = len(current_account_usernames_to_follow)
    current_list_preview = ""
    if current_list_count > 0:
        max_preview = 30
        # Nimm die ersten User aus der Liste für die Vorschau
        preview_list = current_account_usernames_to_follow[:max_preview]
        # Formatiere jeden User als Code
        current_list_preview = "\n".join([f"    - `{user}`" for user in preview_list])
        if current_list_count > max_preview:
            current_list_preview += f"\n    ... und {current_list_count - max_preview} weitere."
    else:
        current_list_preview = "    (Liste ist leer)"
    # Info-Zeile für die Liste
    current_list_info = f"{current_list_count} User in `{current_list_filename}`"

    # --- Nachricht zusammenbauen ---
    status_message = (
        f"📊 **Bot Gesamtstatus** 📊\n\n"
        f"{'▶️' if not is_scraping_paused else ('⏸️⏰' if is_schedule_pause else '⏸️🟡')} **Betrieb:** {running_status}\n"
        f"🥷 **Aktiver Account:** {account_info}\n"
        f"🔍 **Suchmodus:** {mode_text}\n"
        f"⏰ **Zeitplan:** {schedule_details}\n"
        f"🌍 **Globale Follow-Liste:** {global_list_info}\n"
        f"🤖 **Auto-Follow (Akt. Acc):** {autofollow_stat}\n"
        f"📝 **Follow-Liste (Akt. Acc):** {current_list_info}\n"
        f"{current_list_preview}"
    )

    # --- Nachricht senden ---
    # Wir entfernen den alten Button, da die Liste jetzt direkt angezeigt wird.
    await update.message.reply_text(status_message, parse_mode=ParseMode.MARKDOWN)

# --- Mode ---
async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt den aktuellen Suchmodus an (Handler für /mode)."""
    await pause_scraping()
    global search_mode
    mode_text = "CA + Keywords" if search_mode == "full" else "Nur CA"
    await update.message.reply_text(f"🔍 Search mode: {mode_text}")
    await resume_scraping()

async def mode_full_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt den Suchmodus auf CA + Keywords (Handler für /modefull)."""
    await pause_scraping()
    global search_mode
    if search_mode != "full": # Nur speichern, wenn sich was ändert
        search_mode = "full"
        save_settings() # Speichere die Einstellung # <<< KORRIGIERT: await entfernt
        await update.message.reply_text("✅ Suchmodus auf CA + Keywords gesetzt")
    else:
        await update.message.reply_text("ℹ️ Suchmodus ist bereits CA + Keywords.")
    await resume_scraping()

async def mode_ca_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt den Suchmodus auf nur CA (Handler für /modeca)."""
    await pause_scraping()
    global search_mode
    if search_mode != "ca_only": # Nur speichern, wenn sich was ändert
        search_mode = "ca_only"
        save_settings() # Speichere die Einstellung
        await update.message.reply_text("✅ Suchmodus auf Nur CA gesetzt")
    else:
        await update.message.reply_text("ℹ️ Suchmodus ist bereits Nur CA.")
    await resume_scraping()

# --- Pause / Resume ---
async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pausiert das Scraping (Handler für /pause)."""
    # Kein pause_scraping() hier, da wir ja pausieren wollen!
    global is_schedule_pause
    await update.message.reply_text(f"⏸️ Scraping wird pausiert...")
    is_schedule_pause = False  # Manuelle Pause
    await pause_scraping() # Die eigentliche Pause-Aktion
    await update.message.reply_text(f"⏸️ Scraping wurde pausiert! Verwende `/resume` zum Fortsetzen.")
    # KEIN resume_scraping()!

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt das Scraping fort (Handler für /resume)."""
    # Kein pause_scraping() hier
    await update.message.reply_text(f"▶️ Scraping wird fortgesetzt...")
    await resume_scraping() # Die eigentliche Resume-Aktion
    await update.message.reply_text(f"▶️ Scraping läuft wieder!")

# --- Schedule ---
async def schedule_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt die Schedule-Einstellungen an (Handler für /schedule)."""
    await pause_scraping()
    await show_schedule(update) # show_schedule macht selbst resume

async def schedule_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Aktiviert den Schedule (Handler für /scheduleon)."""
    await pause_scraping()
    await set_schedule_enabled(update, True) # set_schedule_enabled macht selbst resume

async def schedule_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deaktiviert den Schedule (Handler für /scheduleoff)."""
    await pause_scraping()
    await set_schedule_enabled(update, False) # set_schedule_enabled macht selbst resume

async def schedule_time_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt die Schedule-Zeiten (Handler für /scheduletime)."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl prüfen (genau 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "ℹ️ Bitte gib genau EINEN Zeitbereich nach dem Befehl an.\n\n"
            "Format: `/scheduletime HH:MM-HH:MM` (24h)\n\n"
            "Kopiere dies und füge den Zeitbereich hinzu:\n"
            "`/scheduletime `"
            , parse_mode=ParseMode.MARKDOWN
        )
        await resume_scraping()
        return
    # --- ENDE ÄNDERUNG ---
    time_str = context.args[0].strip()
    # set_schedule_time prüft das Format und macht resume
    await set_schedule_time(update, time_str) # set_schedule_time enthält die Formatprüfung

# --- Switch Account ---
async def switch_account_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wechselt den Account (Handler für /switchaccount)."""
    await pause_scraping()
    # --- GEÄNDERT: Argumentanzahl prüfen (0 oder 1) ---
    if len(context.args) > 1:
         await update.message.reply_text("❌ Zu viele Argumente. Bitte gib optional EINE Account-Nummer an.\nFormat: `/switchaccount [nummer]`")
         await resume_scraping()
         return
    # --- ENDE ÄNDERUNG ---

    account_num_str = context.args[0] if context.args else None
    account_num_idx = None # Index (0-basiert)
    if account_num_str:
        try:
            req_num = int(account_num_str)
            if 1 <= req_num <= len(ACCOUNTS):
                account_num_idx = req_num - 1 # Zu 0-basiertem Index
            else:
                await update.message.reply_text(f"❌ Ungültige Account-Nummer. Verfügbar: 1-{len(ACCOUNTS)}")
                await resume_scraping()
                return
        except ValueError:
            await update.message.reply_text("❌ Ungültige Account-Nummer (muss eine Zahl sein).") # Klarere Meldung
            await resume_scraping()
            return
    # Wenn keine Nummer angegeben wurde (account_num_idx ist None),
    # wird switch_account_request automatisch zum nächsten wechseln.

    # switch_account_request macht intern pause/resume und login etc.
    await switch_account_request(update, account_num_idx) # Übergebe den Index oder None
    # Kein resume_scraping hier, da switch_account_request es behandelt


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

    # Auth-Code Handling bleibt die Hauptaufgabe hier
    if WAITING_FOR_AUTH:
        logger.debug(f"[Telegram Handler] Checking if message '{message_text}' matches 2FA format (Length: {len(message_text)}, Alnum: {message_text.isalnum()})...")
        if 6 <= len(message_text) <= 10 and message_text.isalnum():
            logger.info(f"[Telegram Handler] Message matches 2FA format. Setting AUTH_CODE.") # Log match
            AUTH_CODE = message_text
            # WICHTIG: Hier NICHT pausieren/resumen, da der Login-Prozess läuft
            await update.message.reply_text("✅ Auth-Code empfangen! Wird verarbeitet...")
            logger.info(f"[Telegram Handler] AUTH_CODE set for user {user_id}.") # Log success
            return # Wichtig: Beende die Funktion hier
        else:
            # Log why it didn't match if waiting
            logger.warning(f"[Telegram Handler] Message from {user_id} ('{message_text}') received while waiting, but did NOT match 2FA format. Ignoring for auth.")
            # Sende eine Hilfestellung an den User
            await update.message.reply_text("⚠️ Ungültiges Format für 2FA-Code. Bitte sende *nur* den 6-10 stelligen Code (Zahlen/Buchstaben).")
            # Nicht returnen, damit ggf. andere Logik (falls vorhanden) noch greift, aber AUTH_CODE wird nicht gesetzt.

    # --- Optional: Behandlung von Antworten für schnelles Liken/Reposten ---
    # ... (optionaler Code, falls du ihn wieder einfügst) ...
    # --- Ende Optional: Antworten ---

    # Wenn die Nachricht kein Auth-Code war (oder WAITING_FOR_AUTH false war)
    # und keine andere Logik gegriffen hat:
    if not WAITING_FOR_AUTH: # Nur loggen, wenn wir *nicht* auf Code warten
         logger.debug(f"[Telegram Handler] Ignoring non-command message from {user_id} as not waiting for auth.")

    # WICHTIG: Kein pauschales pause/resume hier.
    pass # Tue nichts weiter für normale Textnachrichten


    # Wenn die Nachricht kein Auth-Code und kein bekannter Befehl ist,
    # könnte man hier eine kurze Info senden oder einfach nichts tun.
    # Wichtig: NICHTS tun ist besser, um den Chat nicht zuzuspammen.
    # print(f"Ignoriere normale Nachricht: {message_text[:50]}...") # Optional: Logging

    # WICHTIG: Da diese Funktion jetzt nur noch selten aufgerufen wird (nur für Auth-Code oder normale Nachrichten),
    # sollte sie NICHT mehr pauschal pausieren/resumen. Das machen die CommandHandler.
    pass # Tue nichts für normale Textnachrichten

def load_posts_count():
    """Load post counts from file"""
    global posts_count, last_count_date

    # Definiere die Standardstruktur inklusive ads_total
    default_counts = {
        "found": {"today": 0, "yesterday": 0, "vorgestern": 0, "total": 0},
        "scanned": {"today": 0, "yesterday": 0, "vorgestern": 0, "total": 0},
        "ads_total": 0, # Standardwert 0
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

                # Wichtig: Merge die geladenen Daten mit der Default-Struktur
                posts_count = default_counts.copy()
                for category, values in loaded_counts_data.items():
                    if category in posts_count:
                        if isinstance(values, dict):
                             # Updates für 'found', 'scanned', 'weekdays'
                             for key, value in values.items():
                                 if key in posts_count[category]:
                                     posts_count[category][key] = value
                        elif category == "ads_total": # Speziell für ads_total
                            posts_count["ads_total"] = values if isinstance(values, int) else 0
                    else:
                        # Füge neue Kategorie hinzu (sollte nicht passieren, aber sicher ist sicher)
                        posts_count[category] = values

                # Stelle sicher, dass ads_total existiert, auch wenn es in der Datei fehlte
                if "ads_total" not in posts_count:
                    posts_count["ads_total"] = 0

                last_count_date_str = data.get("last_date")
                if last_count_date_str:
                    try:
                         last_count_date = datetime.strptime(last_count_date_str, "%Y-%m-%d").date()
                    except ValueError:
                         print(f"WARNUNG: Ungültiges Datum '{last_count_date_str}' in {POSTS_COUNT_FILE}. Setze auf heute.")
                         last_count_date = datetime.now().date()
                else:
                    last_count_date = datetime.now().date()
        else:
            posts_count = default_counts.copy()
            last_count_date = datetime.now().date()
            print(f"Keine {POSTS_COUNT_FILE} gefunden, verwende Standardzähler.")

    except json.JSONDecodeError:
         print(f"FEHLER: {POSTS_COUNT_FILE} ist korrupt (JSONDecodeError). Verwende Standardzähler.")
         posts_count = default_counts.copy()
         last_count_date = datetime.now().date()
    except Exception as e:
        print(f"Fehler beim Laden von {POSTS_COUNT_FILE}: {e}")
        posts_count = default_counts.copy()
        last_count_date = datetime.now().date()

    # Doppelte Sicherstellung, dass ads_total existiert
    if "ads_total" not in posts_count:
        posts_count["ads_total"] = 0
    # Entferne die alte 'ads'-Struktur falls sie noch existiert (aus alten Läufen)
    if "ads" in posts_count:
         del posts_count["ads"]

def save_posts_count():
    """Save post counts to file"""
    global posts_count, last_count_date
    try:
        # Erstelle eine Kopie zum Speichern, um sicherzustellen, dass die alte "ads" Struktur weg ist
        data_to_save = posts_count.copy()
        if "ads" in data_to_save: # Entferne alte Struktur falls vorhanden
             del data_to_save["ads"]

        data = {
            "counts": data_to_save, # Speichere die bereinigte Kopie
            "last_date": last_count_date.strftime("%Y-%m-%d") if last_count_date else None
        }
        with open(POSTS_COUNT_FILE, 'w') as f:
            json.dump(data, f, indent=4) # indent=4 für bessere Lesbarkeit der Datei
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
                posts_count[category]["vorgestern"] = posts_count[category]["yesterday"]
                posts_count[category]["yesterday"] = posts_count[category]["today"]
                posts_count[category]["today"] = 0
        elif days_diff > 1:
            # More than one day passed, reset older counts
            for category in ["found", "scanned"]:
                posts_count[category]["vorgestern"] = 0
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

    # Hole die Gesamtzahl der Ads sicher mit .get()
    total_ads = posts_count.get("ads_total", 0)

    # --- BERECHNE WEEKDAY AVERAGES ZUERST ---
    weekday_averages = {}
    weekdays_data = posts_count.get("weekdays", {})
    for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
         data = weekdays_data.get(day, {"count": 0, "days": 0})
         if data["days"] > 0:
             weekday_averages[day] = round(data["count"] / data["days"], 1)
         else:
             weekday_averages[day] = 0
    # --- ENDE BERECHNUNG ---


    # --- ERSTELLE DIE GESAMTE NACHRICHT JETZT IN EINEM BLOCK ---
    message = (
        "📊 Post Statistics 📊\n\n"
        f"⏱️ Uptime: {get_uptime()}\n\n"
        "💪🏻 Found Posts:\n" # Du kannst normale Strings und f-Strings mischen
        f"Today: {posts_count.get('found', {}).get('today', 0)} posts\n"
        f"Yesterday: {posts_count.get('found', {}).get('yesterday', 0)} posts\n"
        f"Day before yesterday: {posts_count.get('found', {}).get('vorgestern', 0)} posts\n"
        f"Total: {posts_count.get('found', {}).get('total', 0)} posts\n\n"

        "🔎 Scanned Posts:\n"
        f"Today: {posts_count.get('scanned', {}).get('today', 0)} posts\n"
        f"Yesterday: {posts_count.get('scanned', {}).get('yesterday', 0)} posts\n"
        f"Day before yesterday: {posts_count.get('scanned', {}).get('vorgestern', 0)} posts\n"
        f"Total: {posts_count.get('scanned', {}).get('total', 0)} posts\n\n"

        f"📢 Ads (Total): {total_ads}\n\n"

        "📅 Average Posts by Weekday:\n"
        # Füge die Weekday-Strings hier direkt ein
        f"Mon: {weekday_averages['Monday']}\n"
        f"Tue: {weekday_averages['Tuesday']}\n"
        f"Wed: {weekday_averages['Wednesday']}\n"
        f"Thu: {weekday_averages['Thursday']}\n"
        f"Fri: {weekday_averages['Friday']}\n"
        f"Sat: {weekday_averages['Saturday']}\n"
        f"Sun: {weekday_averages['Sunday']}"
    ) # <<--- Stelle sicher, dass DIESE Klammer die nach 'message = (' öffnende Klammer schließt

    # Der message += (...) Block wird komplett entfernt

    await update.message.reply_text(message)
    await resume_scraping()

async def show_help_message(update: Update):
    """Zeigt die Hilfemeldung an (angepasst für /commands)."""
    # Create keyboard markup with buttons for common commands (Buttons bleiben gleich)
    separator_button = InlineKeyboardButton(" ", callback_data="noop_separator")
    keyboard = [
        # ... (Keyboard-Definition bleibt unverändert) ...
         [
            InlineKeyboardButton("⏸️ Pause", callback_data="help:pause"),
            InlineKeyboardButton("▶️ Resume", callback_data="help:resume")
        ],
        [
            InlineKeyboardButton("📊 Status", callback_data="help:status")
        ],
        [
            InlineKeyboardButton("🔍 Show Mode", callback_data="help:mode"),
            InlineKeyboardButton("🔍 Mode FULL", callback_data="help:mode_full"),
            InlineKeyboardButton("🔍 Mode CA", callback_data="help:mode_ca")
        ],
        [separator_button],
        [
            InlineKeyboardButton("⏰ Schedule", callback_data="help:schedule"),
            InlineKeyboardButton("⏰ Set Time", callback_data="help:prepare_scheduletime")
        ],
        [
            InlineKeyboardButton("⏰ Schedule ON", callback_data="help:schedule_on"),
            InlineKeyboardButton("⏰ Schedule OFF", callback_data="help:schedule_off")
        ],
        [separator_button],
        [ # Modus & Account
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
        [ # Like / Repost
             InlineKeyboardButton("👍 Like", callback_data="help:prepare_like"),
             InlineKeyboardButton("🔄 Repost", callback_data="help:prepare_repost")
        ],
        [separator_button],
        [ # Backup / Sync / Build
            InlineKeyboardButton("💾 Backup", callback_data="help:backup_followers"),
            InlineKeyboardButton("🔄 Sync", callback_data="help:sync_follows"),
            InlineKeyboardButton("🏗️ Build Global", callback_data="help:build_global") # Neuer Befehl braucht auch Button
        ],
        [separator_button],
        [ # Stats / Rates
            InlineKeyboardButton("📊 Stats", callback_data="help:stats"),
            InlineKeyboardButton("⭐️ Rates", callback_data="help:show_rates"),
            InlineKeyboardButton("🌍 Global Info", callback_data="help:global_info"), # Neuer Befehl braucht auch Button
            InlineKeyboardButton("🏓 Ping", callback_data="help:ping")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔺 🆘 `/help` - Show menu 🆘\n"
        " \n"
        "🔸 🚶‍♂️‍➡️    Follow / Unfollow    🚶‍♂️\n"
        "   /follow <username>  - Folgt einem User\n" # Geändert
        "   /unfollow <username>  - Entfolgt einem User\n" # Geändert
        "   /addusers <@user1 user2 ...>  - \n"
        "         └ Fügt User zur Follow-Liste hinzu welche nach und nach abgearbeitet wird\n" # Geändert
        "   /autofollowpause ,  /autofollowresume  - Steuert Auto-Follow\n"
        "   /autofollowstatus  - Zeigt Status und Listenlänge\n"
        "   /clearfollowlist  - Leert die Follow-Liste\n"
        "  \n"
        "🔻 👍    Like / Repost    🔄\n"
        "   /like <tweet_url>  - Liked einen Tweet\n" # Geändert
        "   /repost <tweet_url>  - Repostet einen Tweet\n" # Geändert
        "  \n"
        "▫️ 🔑    Keywords    🔑\n"
        "   /keywords  - Zeigt die Keyword-Liste\n"
        "   /addkeyword <wort1,wort2...>  - Fügt Keywords hinzu\n" # Geändert
        "   /removekeyword <wort1,wort2...>  - Entfernt Keywords\n" # Geändert
        "  \n"
        "🔸 🥷    Accounts    🥷\n"
        "   /account  - Zeigt den aktiven Account\n"
        "   /switchaccount [nummer] \n" # Eckige Klammern sind OK
        "         └ Wechselt zum Account [nummer]\n" # Geändert
        "  \n"
        "🔺 🔍    Suchmodus    🔍\n"
        "   /mode  - Zeigt den aktuellen Suchmodus\n"
        "   /modefull  - Setzt Modus auf CA + Keywords\n"
        "   /modeca  - Setzt Modus auf Nur CA\n"
        "  \n"
        "🔹 ⏯️    Steuerung    ⏯️\n"
        "   /pause  - Pausiert das Tweet-Suchen ⏸️\n"
        "   /resume  - Setzt das Suchen fort ▶️\n"
        "  \n"
        "🔸 ⏰    Zeitplan (Schedule)    ⏰\n"
        "   /schedule  - Zeigt den aktuellen Zeitplan\n"
        "   /scheduleon  - Aktiviert den Zeitplan\n"
        "   /scheduleoff  - Deaktiviert den Zeitplan\n"
        "   /scheduletime <HH:MM-HH:MM>  - Setzt die Pausenzeit\n" # Geändert
        "  \n"
        "▫️ 📊    Statistiken & Status    📊\n"
        "   /status  - Zeigt den aktuellen Betriebsstatus 📊\n\n"        
        "   /stats  oder  /count  - Zeigt Post-Statistiken 📈\n"
        "   /rates  - Zeigt die gesammelten Quellen-Ratings ⭐️\n"
        "   /globallistinfo  - Zeigt Status der globalen Follower-Liste 🌍\n"
        "   /ping  - Prüft, ob der Bot antwortet 🏓\n"
        "  \n"
        "🔸 💾  Following DB & Management  💾\n"
        "   /scrapefollowing <username>\n"
        "         └ Scannt Following von <username> & speichert in DB\n"
        "   /addfromdb [f:NUM] [s:NUM] [k:WORT..]\n"
        "         └ Fügt User aus DB hinzu (Filter: f=followers, s=seen, k=keywords)\n"
        "   /backupfollowers  - Speichert Snapshot des aktiven Accounts\n"
        "   /syncfollows  - Synchronisiert aktiven Account mit globaler Liste\n"
        "   /buildglobalfrombackups \n"
        "         └ Fügt User aus allen Backups zur globalen Liste hinzu\n"
        "❌  /cancelbackup ,  /cancelsync ,  /canceldbscrape \n"
        "         └ Bricht laufende Prozesse ab\n",
        reply_markup=reply_markup
    )
    # Wichtig: Diese Funktion muss am Ende resume aufrufen, da der aufrufende Handler pausiert hat
    await resume_scraping()

async def add_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fügt Usernamen zur Liste des aktuellen Accounts hinzu, prüft gegen globale Followed-Liste."""
    global current_account_usernames_to_follow, global_followed_users_set
    # is_scraping_paused, pause_event werden hier nicht mehr direkt benötigt

    account_username = get_current_account_username()
    current_follow_list_path = get_current_follow_list_path()

    if not account_username or not current_follow_list_path:
        await update.message.reply_text("❌ Fehler: Aktiver Account-Username/Listenpfad nicht gefunden.")
        await resume_scraping() # Wichtig: Fortsetzen, da der Haupt-Handler pausiert hat
        return

    # Simuliere Argumente
    if hasattr(context, 'args'): args = context.args
    elif isinstance(context, list): args = context
    else: args = []

    if not args:
        await update.message.reply_text(f"Bitte gib einen oder mehrere X-Usernamen an.\nz.B.: `addusers user1 @user2,user3`")
        await resume_scraping() # Wichtig: Fortsetzen
        return

    input_text = " ".join(args)
    # Validiere Usernamen beim Parsen
    potential_usernames = {name.strip().lstrip('@') for name in re.split(r'[,\s]+', input_text)
                           if name.strip() and re.match(r'^[A-Za-z0-9_]{1,15}$', name.strip().lstrip('@'))}

    if not potential_usernames:
         await update.message.reply_text("ℹ️ Keine gültigen Usernamen im Input gefunden.")
         await resume_scraping() # Wichtig: Fortsetzen
         return

    # Füge optional zur globalen Add-Queue hinzu (falls implementiert)
    # add_to_set_file(potential_usernames, GLOBAL_ADD_QUEUE_FILE)

    added_to_current_account = set()
    already_followed_globally = set()
    already_in_current_list = set()

    # Verwende das globale Set im Speicher für die aktuelle Liste
    current_list_set = set(current_account_usernames_to_follow)

    for username in potential_usernames:
        if username in global_followed_users_set:
            already_followed_globally.add(username)
        elif username in current_list_set:
            already_in_current_list.add(username)
        else:
            added_to_current_account.add(username)

    # Aktualisiere die In-Memory-Liste und speichere sie
    response = ""
    if added_to_current_account:
        current_account_usernames_to_follow.extend(list(added_to_current_account)) # Füge zur Liste hinzu
        save_current_account_follow_list() # Speichere die aktualisierte Liste
        response += f"✅ {len(added_to_current_account)} User zur Liste von @{account_username} hinzugefügt: {', '.join(sorted(list(added_to_current_account)))}\n"

    if already_in_current_list:
         response += f"ℹ️ {len(already_in_current_list)} User waren bereits in der Liste von @{account_username}.\n"
    if already_followed_globally:
        response += f"🚫 {len(already_followed_globally)} User werden bereits global gefolgt und wurden nicht zur Liste von @{account_username} hinzugefügt: {', '.join(sorted(list(already_followed_globally)))}"

    if not response: # Fallback
         response = "ℹ️ Keine Änderungen an der Follow-Liste vorgenommen."

    await update.message.reply_text(response.strip())
    await resume_scraping() # Wichtig: Am Ende fortsetzen


# eventually delete process_follow_request
async def process_follow_request(update: Update, username: str):
    """Verarbeite Follow-Requests: Folgt User und aktualisiert Listen bei Erfolg."""
    global global_followed_users_set # Zugriff auf globales Set

    account_username = get_current_account_username()
    backup_filepath = get_current_backup_file_path()

    if not account_username or not backup_filepath:
         await update.message.reply_text("❌ Fehler: Aktiver Account kann nicht ermittelt werden für Follow-Aktualisierung.")
         await resume_scraping() # Fortsetzen, da Haupt-Handler pausiert hat
         return

    # Nachricht senden, bevor follow_user aufgerufen wird
    await update.message.reply_text(f"⏳ Versuche @{username} mit Account @{account_username} zu folgen...")
    # Der Haupt-Handler hat bereits pausiert. follow_user navigiert weg.

    result = await follow_user(username) # Führe den Follow-Versuch durch
                                         # follow_user navigiert am Ende zurück zu /home

    if result is True: # Nur bei *erfolgreichem neuen* Follow
        await update.message.reply_text(f"✅ Erfolgreich @{username} gefolgt!")
        print(f"Manueller Follow erfolgreich: @{username}")
        # Update globale Liste (Speicher & Datei)
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE) # Korrekter globaler Dateiname
            print(f"@{username} zur globalen Followed-Liste hinzugefügt.")
        # Update Account-Backup (Datei)
        add_to_set_file({username}, backup_filepath)
        print(f"@{username} zum Account-Backup ({os.path.basename(backup_filepath)}) hinzugefügt.")

    elif result == "already_following":
        await update.message.reply_text(f"ℹ️ Account @{account_username} folgt @{username} bereits.")
        print(f"Manueller Follow: @{username} wurde bereits gefolgt.")
        # Stelle Konsistenz sicher (füge hinzu falls fehlt)
        if username not in global_followed_users_set:
            global_followed_users_set.add(username)
            add_to_set_file({username}, GLOBAL_FOLLOWED_FILE) # Korrekter globaler Dateiname
        # Stelle sicher, dass es auch im Account-Backup ist
        add_to_set_file({username}, backup_filepath)

    else: # Fehler
        await update.message.reply_text(f"❌ Konnte @{username} nicht folgen.")
        print(f"Manueller Follow fehlgeschlagen: @{username}")

    await resume_scraping() # Am Ende des Handlers fortsetzen

async def process_unfollow_request(update: Update, username: str):
    """Verarbeite Unfollow-Requests zentralisiert"""
    await update.message.reply_text(f"🔍 Versuche @{username} zu entfolgen...")
    result = await unfollow_user(username)
    if result == "not_following":
        await update.message.reply_text(f"ℹ️ Du folgst @{username} nicht")
    elif result:
        await update.message.reply_text(f"✅ Erfolgreich @{username} entfolgt!")
    else:
        await update.message.reply_text(f"❌ Konnte @{username} nicht entfolgen")
    await resume_scraping()

async def process_like_request(update: Update, tweet_url: str):
    """Verarbeite Like-Requests als Textbefehl"""
    await pause_scraping()
    await update.message.reply_text(f"🔍 Versuche Tweet zu liken: {tweet_url}")
    
    # Überprüfe URL-Format
    if not (tweet_url.startswith("http://") or tweet_url.startswith("https://")):
        tweet_url = "https://x.com" + ("/" if not tweet_url.startswith("/") else "") + tweet_url
    
    # Stelle sicher, dass es eine X/Twitter-URL ist
    if not ("x.com" in tweet_url or "twitter.com" in tweet_url):
        await update.message.reply_text("❌ Ungültige Tweet-URL. Bitte gib eine X.com oder Twitter.com URL an.")
        await resume_scraping()
        return
    
    try:
        result = await like_tweet(tweet_url)
        if result:
            await update.message.reply_text(f"✅ Tweet erfolgreich geliked!")
        else:
            await update.message.reply_text(f"❌ Konnte Tweet nicht liken")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Liken: {str(e)[:100]}")
    
    await resume_scraping()

async def process_repost_request(update: Update, tweet_url: str):
    """Verarbeite Repost-Requests als Textbefehl"""
    await pause_scraping()
    await update.message.reply_text(f"🔍 Versuche Tweet zu reposten: {tweet_url}")
    
    # Überprüfe URL-Format
    if not (tweet_url.startswith("http://") or tweet_url.startswith("https://")):
        tweet_url = "https://x.com" + ("/" if not tweet_url.startswith("/") else "") + tweet_url
    
    # Stelle sicher, dass es eine X/Twitter-URL ist
    if not ("x.com" in tweet_url or "twitter.com" in tweet_url):
        await update.message.reply_text("❌ Ungültige Tweet-URL. Bitte gib eine X.com oder Twitter.com URL an.")
        await resume_scraping()
        return
    
    try:
        result = await repost_tweet(tweet_url)
        if result:
            await update.message.reply_text(f"✅ Tweet erfolgreich repostet!")
        else:
            await update.message.reply_text(f"❌ Konnte Tweet nicht reposten")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Reposten: {str(e)[:100]}")
    
    await resume_scraping()
# ===========================================================
# NEUE TELEGRAM BEFEHLE FÜR AUTO-FOLLOW STEUERUNG
# ===========================================================

# Füge diese Funktion hinzu:

async def backup_followers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler für den /backupfollowers Befehl. Startet die Backup-Logik als Task."""
    global is_backup_running # Prüfen, ob bereits ein Backup läuft

    # WICHTIG: Dieser Handler pausiert NICHT selbst das Scraping.
    # backup_followers_logic wird als Task gestartet und managed Pause/Resume intern.

    if is_backup_running:
        await update.message.reply_text("⚠️ Ein Backup-Prozess läuft bereits. Bitte warte oder verwende `/cancelbackup`.")
        return # Nicht fortfahren, wenn bereits aktiv

    # Nachricht senden, dass der Task gestartet wird
    await update.message.reply_text("✅ Follower-Backup wird im Hintergrund gestartet...")

    # Starte die eigentliche Logik als Hintergrund-Task
    # Übergib das 'update'-Objekt, das backup_followers_logic erwartet
    asyncio.create_task(backup_followers_logic(update))

    # Kein resume_scraping hier, der Task läuft unabhängig und managed das selbst.

async def autofollow_pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pausiert die automatische Abarbeitung der Follow-Liste."""
    global is_periodic_follow_active
    is_periodic_follow_active = False
    await update.message.reply_text("⏸️ Automatisches Folgen aus der Account-Liste wurde pausiert.")
    print("[Auto-Follow] Pausiert via Telegram-Befehl.")
    save_settings() 
    # Kein resume_scraping nötig, da die Steuerung nur das Starten des Follow-Prozesses betrifft

async def autofollow_resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt die automatische Abarbeitung der Follow-Liste fort."""
    global is_periodic_follow_active
    is_periodic_follow_active = True
    await update.message.reply_text("▶️ Automatisches Folgen aus der Account-Liste wurde fortgesetzt.")
    print("[Auto-Follow] Fortgesetzt via Telegram-Befehl.")
    save_settings() 
    # Kein resume_scraping nötig

async def autofollow_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt den Status der automatischen Follow-Funktion für den aktuellen Account an."""
    global is_periodic_follow_active, current_account_usernames_to_follow
    status = "AKTIV ▶️" if is_periodic_follow_active else "PAUSIERT ⏸️"
    account_username = get_current_account_username() or "Unbekannt"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)
    await update.message.reply_text(f"🤖 Status Auto-Follow für @{account_username}: {status}\n"
                                     f"📝 User in `{filename}`: {count}")
    # WICHTIG: Haupt-Handler hat pausiert, hier fortsetzen
    await resume_scraping()

async def clear_follow_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fragt nach Bestätigung zum Leeren der *aktuellen* Account-Follow-Liste."""
    global current_account_usernames_to_follow
    account_username = get_current_account_username() or "Unbekannt"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)

    if count == 0:
        await update.message.reply_text(f"ℹ️ Die Follow-Liste für @{account_username} (`{filename}`) ist bereits leer.")
        await resume_scraping() # Fortsetzen
        return

    keyboard = [[
        # Der Payload enthält jetzt den Account-Namen zur Sicherheit
        InlineKeyboardButton(f"✅ Ja, Liste für @{account_username} leeren", callback_data=f"confirm_clear_follow_list:{account_username}"),
        InlineKeyboardButton("❌ Nein, abbrechen", callback_data="cancel_clear_follow_list")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ Bist du sicher, dass du die Follow-Liste für Account @{account_username} (`{filename}`) löschen möchtest? "
        f"Aktuell sind {count} User enthalten.",
        reply_markup=reply_markup
    )
    # Kein resume_scraping hier, warten auf Button-Antwort oder Timeout



async def sync_followers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Prüft, ob ein Sync notwendig ist (Hinzufügen ODER Entfernen),
    fragt ggf. nach Bestätigung und startet dann den Sync-Task.
    """
    await pause_scraping() # Pause für die Prüfung

    account_username = get_current_account_username()
    if not account_username:
        await update.message.reply_text("❌ Fehler: Aktiver Account kann nicht ermittelt werden.")
        await resume_scraping()
        return

    backup_filepath = get_current_backup_file_path()
    if not backup_filepath:
        await update.message.reply_text("❌ Fehler: Backup-Dateipfad konnte nicht ermittelt werden.")
        await resume_scraping()
        return

    # 1. Lade globale Liste und Account-Backup
    global_all_followed_users_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
    account_backup_set = load_set_from_file(backup_filepath)
    backup_exists_and_not_empty = bool(account_backup_set)

    # 2. Ermittle BEIDE Differenzen
    users_to_add = global_all_followed_users_set - account_backup_set
    total_to_add = len(users_to_add)

    users_to_remove = account_backup_set - global_all_followed_users_set
    total_to_remove = len(users_to_remove)

    # 3. Prüfe, ob *überhaupt* eine Aktion nötig ist
    sync_needed = total_to_add > 0 or total_to_remove > 0

    if not sync_needed:
        await update.message.reply_text(f"✅ Account @{account_username} ist bereits synchron mit der globalen Liste.")
        await resume_scraping()
        return

    # 4. Zeitabschätzung (Berücksichtige beide Aktionen, grob)
    # (Die Abschätzung ist weniger kritisch, kann vereinfacht werden oder nur auf Adds basieren)
    estimated_seconds_per_user = 30
    total_estimated_seconds = (total_to_add + total_to_remove) * estimated_seconds_per_user
    estimated_time_str = "wenigen Minuten" # Vereinfachte Schätzung
    if total_estimated_seconds > 0:
        minutes, seconds = divmod(total_estimated_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        temp_str = ""
        if hours > 0: temp_str += f"{hours}h"
        if minutes > 0: temp_str += f" {minutes}m"
        if not temp_str or hours == 0:
             if seconds > 0: temp_str += f" {seconds}s"
        if temp_str: estimated_time_str = f"~{temp_str.strip()}"


    # 5. Entscheidung basierend auf Backup-Status und ob Sync nötig ist
    if backup_exists_and_not_empty:
        # Fall B: Backup existiert - Normaler Sync -> Bestätigung anfordern
        message = (f"ℹ️ **Sync-Vorschau für @{account_username}**\n\n"
                   f"   - User im Backup: {len(account_backup_set)}\n"
                   f"   - User global: {len(global_all_followed_users_set)}\n"
                   f"   - ➡️ Aktionen: *+{total_to_add} User / -{total_to_remove} User*\n" # Zeige beide Zahlen
                   f"   - ⏱️ Geschätzte Dauer: *{estimated_time_str}*\n\n"
                   f"Möchtest du diesen Sync jetzt starten?")

        # Buttons für Ja/Nein hinzufügen
        keyboard = [[
            # Wichtig: Account-Username in Callback-Daten einbetten!
            InlineKeyboardButton(f"✅ Ja, Sync starten", callback_data=f"sync:proceed_sync:{account_username}"),
            InlineKeyboardButton("❌ Nein, abbrechen", callback_data=f"sync:cancel_sync:{account_username}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        # Starte den Task HIER NICHT MEHR! Das passiert im Button-Handler.
        # Kein resume_scraping hier, warten auf Button-Antwort.

    else:
        # Fall A: Backup fehlt oder ist leer - Spezielle Bestätigung anfordern
        if total_to_add == 0:
             # Wenn kein Backup da ist UND nichts hinzuzufügen wäre
             await update.message.reply_text(f"ℹ️ Backup für @{account_username} fehlt/leer und keine User zum Hinzufügen aus globaler Liste gefunden. Kein Sync nötig.")
             await resume_scraping()
             return

        # Nachricht für Fall A
        message = (f"⚠️ **Achtung: Sync für @{account_username}**\n\n"
                   f"Die Backup-Datei `{os.path.basename(backup_filepath)}` fehlt oder ist leer.\n\n"
                   f"Ein Sync würde jetzt versuchen, *{total_to_add}* User aus der globalen Liste zu folgen.\n"
                   f"(User zum Entfernen können nicht bestimmt werden).\n"
                   f"Geschätzte Dauer (nur Hinzufügen): *{estimated_time_str}*\n\n"
                   f"Wie möchtest du fortfahren?")

        # Buttons für Fall A
        keyboard = [[
            # Option 1: Backup erstellen
            InlineKeyboardButton("💾 Backup erstellen & Abbrechen", callback_data=f"sync:create_backup:{account_username}"),
        ],[
            # Option 2: Nur Hinzufügen starten
            InlineKeyboardButton(f"▶️ Ja, {total_to_add} User hinzufügen", callback_data=f"sync:proceed:{account_username}"),
            # Option 3: Abbrechen
            InlineKeyboardButton("❌ Nein, Abbrechen", callback_data=f"sync:cancel_sync:{account_username}") # Verwende cancel_sync
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        # await send_telegram_message(text=message, reply_markup=reply_markup) # send_telegram_message ist für den Kanal, hier direkt antworten
        await update.message.reply_text(text=message, parse_mode=ParseMode.MARKDOWN, reply_markup=reply_markup)
        # Nicht fortsetzen, warten auf Button-Antwort oder Timeout

async def sync_followers_logic(update: Update, account_username: str, backup_filepath: str, global_set_for_sync: set):
    """
    Führt die Synchronisation für den gegebenen Account durch.
    Fügt User hinzu, die global gefolgt, aber nicht im Backup sind.
    Entfernt User, die im Backup sind, aber nicht mehr global gefolgt werden.
    (Mit Abbruchmöglichkeit)
    """
    # Zugriff auf globale Variablen/Flags
    global driver, is_scraping_paused, pause_event
    global is_sync_running, cancel_sync_flag
    # Zugriff auf globales Set (wird hier nur gelesen, nicht geändert)
    global global_followed_users_set

    if is_sync_running:
        await update.message.reply_text("⚠️ Ein Sync-Prozess läuft bereits.")
        return

    # ===== Task Start Markierung =====
    is_sync_running = True
    cancel_sync_flag = False
    # ================================

    logger.info(f"[Sync @{account_username}] Starting synchronization process...")
    await update.message.reply_text(f"⏳ Starte Sync für @{account_username}...\n"
                                     f"   Zum Abbrechen: `/cancelsync`")

    await pause_scraping() # Pausiere Haupt-Scraping für die Dauer des Syncs

    # --- Initialisiere Zähler ---
    users_followed_in_sync = 0
    users_already_followed_checked = 0
    users_failed_to_follow = 0
    users_unfollowed_in_sync = 0
    users_already_unfollowed_checked = 0
    users_failed_to_unfollow = 0
    users_processed_add_count = 0
    users_processed_remove_count = 0
    # --- Ende Zähler ---

    navigation_successful = False
    cancelled_early = False
    backup_modified = False # Flag, um zu wissen, ob das Backup gespeichert werden muss

    try: # Haupt-Try-Block
        # Lade aktuelle Daten (globales Set ist bereits im Speicher aktuell)
        # Lade das Backup *dieses* Accounts
        account_backup_set = load_set_from_file(backup_filepath)
        initial_backup_size = len(account_backup_set)

        # --- Berechne Differenzen ---
        # User, die hinzugefügt werden sollen (global aber nicht im Backup)
        # Verwende das übergebene Set!
        users_to_add = global_set_for_sync - account_backup_set
        total_to_add = len(users_to_add)

        # User, die entfernt werden sollen (im Backup aber nicht mehr global)
        # Verwende das übergebene Set!
        users_to_remove = account_backup_set - global_set_for_sync
        total_to_remove = len(users_to_remove)
        # --- Ende Differenzen ---

        logger.info(f"[Sync @{account_username}] Global (passed): {len(global_set_for_sync)} | Backup (Start): {initial_backup_size} | To Add: {total_to_add} | To Remove: {total_to_remove}")

        if not users_to_add and not users_to_remove:
            await update.message.reply_text(f"✅ Account @{account_username} ist bereits synchron.")
            # Springe direkt zu finally
        else:
            await update.message.reply_text(f"⏳ Synchronisiere @{account_username}: +{total_to_add} User / -{total_to_remove} User...")

            # === PHASE 1: Hinzufügen ===
            if users_to_add:
                logger.info(f"[Sync @{account_username}] Starting ADD phase ({total_to_add} users)...")
                user_list_to_add = list(users_to_add)
                random.shuffle(user_list_to_add)

                for i, username in enumerate(user_list_to_add):
                    users_processed_add_count = i + 1
                    if cancel_sync_flag: cancelled_early = True; break # Abbruchprüfung

                    logger.debug(f"[Sync @{account_username} ADD] Attempt {i+1}/{total_to_add}: Following @{username}...")
                    wait_follow = random.uniform(4, 7)
                    logger.debug(f"    -> Waiting {wait_follow:.1f}s before next attempt")
                    await asyncio.sleep(wait_follow)

                    if cancel_sync_flag: cancelled_early = True; break # Abbruchprüfung nach Wartezeit

                    follow_result = await follow_user(username)

                    if follow_result is True:
                        logger.debug(f"  -> Success!")
                        users_followed_in_sync += 1
                        account_backup_set.add(username) # Füge zum In-Memory-Set hinzu
                        backup_modified = True
                    elif follow_result == "already_following":
                        logger.debug(f"  -> Already following (consistency check).")
                        users_already_followed_checked += 1
                        if username not in account_backup_set: # Füge hinzu, falls es fehlte
                            account_backup_set.add(username)
                            backup_modified = True
                    else: # Fehler
                        logger.warning(f"  -> Failed to follow @{username}!")
                        users_failed_to_follow += 1

                    # Fortschritt melden (optional)
                    if not cancelled_early and ((i + 1) % 10 == 0 or (i + 1) == total_to_add):
                         progress_msg = f"[Sync @{account_username} ADD] Progress: {i+1}/{total_to_add} attempted..."
                         await send_telegram_message(progress_msg) # Verwende deine Sendefunktion

                if cancelled_early:
                    logger.warning("[Sync] Cancellation signal received during ADD phase.")
                    await update.message.reply_text("🟡 Sync wird abgebrochen (während Hinzufügen)...")
                    # Springe aus der Sync-Logik (finally wird ausgeführt)


            # === PHASE 2: Entfernen (nur wenn nicht abgebrochen) ===
            if not cancelled_early and users_to_remove:
                logger.info(f"[Sync @{account_username}] Starting REMOVE phase ({total_to_remove} users)...")
                user_list_to_remove = list(users_to_remove)
                random.shuffle(user_list_to_remove)

                for i, username in enumerate(user_list_to_remove):
                    users_processed_remove_count = i + 1
                    if cancel_sync_flag: cancelled_early = True; break # Abbruchprüfung

                    logger.debug(f"[Sync @{account_username} REMOVE] Attempt {i+1}/{total_to_remove}: Checking/Unfollowing @{username} (as it's not in global list)...")
                    wait_unfollow = random.uniform(4, 7)
                    logger.debug(f"    -> Waiting {wait_unfollow:.1f}s before next attempt")
                    await asyncio.sleep(wait_unfollow)

                    if cancel_sync_flag: cancelled_early = True; break # Abbruchprüfung nach Wartezeit

                    # --- Versuche, via Selenium zu entfolgen ---
                    unfollow_result = await unfollow_user(username)
                    selenium_unfollowed = False # Verfolgen, ob Selenium erfolgreich war

                    if unfollow_result is True:
                        logger.debug(f"  -> Successfully unfollowed @{username} via Selenium.")
                        users_unfollowed_in_sync += 1
                        selenium_unfollowed = True
                    elif unfollow_result == "not_following":
                        logger.debug(f"  -> Account @{account_username} was not following @{username} (Selenium check).")
                        users_already_unfollowed_checked += 1
                        selenium_unfollowed = True # Behandle als Erfolg für die Listenbereinigung
                    else: # Fehler
                        logger.warning(f"  -> Failed to unfollow @{username} via Selenium! (Will still remove from backup)")
                        users_failed_to_unfollow += 1
                        # selenium_unfollowed bleibt False

                    # --- Aktualisiere das Backup-Set IMMER, wenn der User in users_to_remove ist ---
                    # Der User soll aus dem Backup dieses Accounts entfernt werden, da er nicht mehr global ist.
                    if username in account_backup_set:
                        logger.info(f"  -> Removing @{username} from in-memory backup set for @{account_username} (due to global list difference).")
                        account_backup_set.discard(username)
                        backup_modified = True # Markiere, dass das Backup geändert wurde
                    else:
                         # Sollte nicht passieren, wenn users_to_remove korrekt berechnet wurde, aber zur Sicherheit loggen
                         logger.debug(f"  -> @{username} was already not in the in-memory backup set for @{account_username}.")

                    # Fortschritt melden (optional)
                    if not cancelled_early and ((i + 1) % 10 == 0 or (i + 1) == total_to_remove):
                         progress_msg = f"[Sync @{account_username} REMOVE] Progress: {i+1}/{total_to_remove} checked/attempted..."
                         await send_telegram_message(progress_msg) # Verwende deine Sendefunktion

                if cancelled_early:
                    logger.warning("[Sync] Cancellation signal received during REMOVE phase.")
                    await update.message.reply_text("🟡 Sync wird abgebrochen (während Entfernen)...")
                    # Springe aus der Sync-Logik (finally wird ausgeführt)

            # === Nach beiden Phasen (oder Abbruch) ===
            if cancelled_early:
                 logger.info(f"[Sync @{account_username}] Process cancelled after {users_processed_add_count}/{total_to_add} adds and {users_processed_remove_count}/{total_to_remove} removes attempted.")
                 summary = (f"🛑 Sync für @{account_username} abgebrochen!\n"
                            f"------------------------------------\n"
                            f" Versuchte Hinzufügungen: {users_processed_add_count}/{total_to_add}\n"
                            f"   - Erfolgreich: {users_followed_in_sync}\n"
                            f"   - Bereits gefolgt: {users_already_followed_checked}\n"
                            f"   - Fehler: {users_failed_to_follow}\n"
                            f" Versuchte Entfernungen: {users_processed_remove_count}/{total_to_remove}\n"
                            f"   - Erfolgreich: {users_unfollowed_in_sync}\n"
                            f"   - Nicht gefolgt: {users_already_unfollowed_checked}\n"
                            f"   - Fehler: {users_failed_to_unfollow}\n"
                            f"------------------------------------\n"
                            f" Änderungen am Backup wurden bis zum Abbruch gespeichert.")
                 await update.message.reply_text(summary)
            else:
                # Normales Ende - Endergebnis melden
                final_backup_size = len(account_backup_set)
                summary = (f"✅ Sync für @{account_username} abgeschlossen:\n"
                           f"------------------------------------\n"
                           f" Global gefolgt (Basis): {len(global_set_for_sync)}\n"
                           f" Im Backup (Start): {initial_backup_size}\n"
                           f" Im Backup (Ende): {final_backup_size}\n"
                           f"------------------------------------\n"
                           f" Hinzufügen (+{total_to_add}):\n"
                           f"   - Erfolgreich gefolgt: {users_followed_in_sync}\n"
                           f"   - Bereits gefolgt (Check): {users_already_followed_checked}\n"
                           f"   - Fehler beim Folgen: {users_failed_to_follow}\n"
                           f" Entfernen (-{total_to_remove}):\n"
                           f"   - Erfolgreich entfolgt: {users_unfollowed_in_sync}\n"
                           f"   - Nicht gefolgt (Check): {users_already_unfollowed_checked}\n"
                           f"   - Fehler beim Entfolgen: {users_failed_to_unfollow}\n"
                           f"------------------------------------")
                await update.message.reply_text(summary)
                logger.info(f"[Sync @{account_username}] Synchronization completed.")

            # === Backup speichern (nur wenn geändert und nicht abgebrochen) ===
            # Speichere das aktualisierte Backup-Set *nach* allen Operationen
            if backup_modified:
                logger.info(f"[Sync @{account_username}] Saving updated backup file: {backup_filepath}")
                save_set_to_file(account_backup_set, backup_filepath)
            else:
                 logger.info(f"[Sync @{account_username}] Backup file was not modified.")


    except Exception as e:
        error_message = f"💥 Schwerwiegender Fehler während der Synchronisation für @{account_username}: {e}"
        await update.message.reply_text(error_message)
        logger.error(f"Critical error during sync for @{account_username}: {e}", exc_info=True)
        # import traceback # Nicht mehr nötig bei logger.error mit exc_info=True
        # traceback.print_exc()

    finally: # ===== WICHTIGER FINALLY BLOCK =====
        logger.debug(f"[Sync @{account_username}] Entering finally block.")
        # Rückkehr zur Haupt-Timeline
        logger.debug(f"[Sync @{account_username}] Attempting to navigate back to home timeline...")
        try:
            if driver and "x.com" in driver.current_url and driver.current_url != "https://x.com/home":
                 logger.debug("Navigating to x.com/home")
                 driver.get("https://x.com/home")
                 await asyncio.sleep(random.uniform(3, 5))
            await switch_to_following_tab() # Stellt sicher, dass wir auf dem Following Tab sind
            logger.debug("[Sync] Successfully navigated back to home 'Following' tab.")
            navigation_successful = True # Nicht wirklich verwendet, aber zur Info
        except Exception as nav_err:
            logger.error(f"[Sync] Error navigating back to home timeline: {nav_err}", exc_info=True)

        # Haupt-Scraping fortsetzen
        logger.info(f"[Sync @{account_username}] Resuming main scraping process.")
        await resume_scraping()

        # ===== Task Ende Markierung =====
        is_sync_running = False
        cancel_sync_flag = False # Sicherstellen, dass Flag für nächsten Lauf false ist
        logger.info("[Sync] Status flags reset.")
        # =============================

async def cancel_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fordert den Abbruch des laufenden Backup-Prozesses an."""
    global is_backup_running, cancel_backup_flag
    if is_backup_running:
        cancel_backup_flag = True
        await update.message.reply_text("🟡 Abbruch des Backups angefordert. Es kann einen Moment dauern, bis der Prozess stoppt.")
        print("[Cancel] Backup cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ Aktuell läuft kein Backup-Prozess.")
    # Kein resume/pause hier, dieser Befehl beeinflusst nur das Flag

async def cancel_sync_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fordert den Abbruch des laufenden Sync-Prozesses an."""
    global is_sync_running, cancel_sync_flag
    if is_sync_running:
        cancel_sync_flag = True
        await update.message.reply_text("🟡 Abbruch des Syncs angefordert. Es kann einen Moment dauern, bis der Prozess stoppt.")
        print("[Cancel] Sync cancellation requested.")
    else:
        await update.message.reply_text("ℹ️ Aktuell läuft kein Sync-Prozess.")
    # Kein resume/pause hier, dieser Befehl beeinflusst nur das Flag

async def global_list_info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt Informationen zur globalen Follower-Liste an."""
    await pause_scraping() # Pause für die Dauer des Befehls

    file_path = GLOBAL_FOLLOWED_FILE
    response_message = f"ℹ️ Status der globalen Liste (`{os.path.basename(file_path)}`):\n"

    try:
        if os.path.exists(file_path):
            # Letzte Änderung holen
            mod_timestamp = os.path.getmtime(file_path)
            # Zeitzone holen (wie in format_time)
            try:
                local_tz = ZoneInfo("Europe/Berlin")
            except Exception:
                local_tz = timezone(timedelta(hours=2)) # Fallback
            mod_datetime_local = datetime.fromtimestamp(mod_timestamp, tz=local_tz)
            mod_time_str = mod_datetime_local.strftime('%Y-%m-%d %H:%M:%S %Z%z')

            # Anzahl der Einträge holen (durch Lesen der Datei)
            current_global_set = load_set_from_file(file_path)
            user_count = len(current_global_set)

            response_message += f"  - Letzte Änderung: {mod_time_str}\n"
            response_message += f"  - Anzahl User: {user_count}"
        else:
            response_message += "  - Datei existiert noch nicht."

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Infos für {file_path}: {e}", exc_info=True)
        response_message += f"\n❌ Fehler beim Abrufen der Datei-Informationen: {e}"

    await update.message.reply_text(response_message)
    await resume_scraping() # Fortsetzen nach dem Befehl

async def build_global_from_backups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Aktualisiert die globale Follower-Liste, indem die Inhalte ALLER
    vorhandenen Account-Backups zusammengeführt werden (Union).
    Bestehende globale Einträge werden NICHT gelöscht.
    """
    global global_followed_users_set # Zugriff zum Aktualisieren

    combined_set = set()
    missing_backups = []
    processed_accounts = 0

    await update.message.reply_text("⏳ Lese alle vorhandenen Account-Backups...")

    # Iteriere durch alle konfigurierten Accounts
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
            combined_set.update(backup_set) # Füge User zum Gesamtset hinzu (Union)
            processed_accounts += 1
            logger.debug(f"  -> Added {len(backup_set)} users. Combined set size: {len(combined_set)}")
        else:
            logger.warning(f"Backup file not found for account @{acc_username}. Skipping.")
            missing_backups.append(f"@{acc_username}")

    if processed_accounts == 0:
        await update.message.reply_text("❌ Keine Backup-Dateien gefunden. Bitte zuerst `/backupfollowers` für mindestens einen Account ausführen.")
        return

    # Lade das *aktuelle* globale Set, um zu sehen, wie viele neu hinzukommen
    current_global_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
    newly_added_count = len(combined_set - current_global_set)
    final_global_set = current_global_set.union(combined_set) # Kombiniere bestehendes Global mit allen Backups

    # Baue die Bestätigungsnachricht
    confirmation_message = (
        f"ℹ️ Zusammenführung der Backups abgeschlossen ({processed_accounts} Accounts gelesen).\n"
        f"   - Gesamt gefundene User (aus Backups): {len(combined_set)}\n"
        f"   - Aktuelle globale Liste: {len(current_global_set)} User\n"
        f"   - Neue User zum Hinzufügen: {newly_added_count}\n"
        f"   - Finale globale Liste wird {len(final_global_set)} User enthalten.\n\n"
    )
    if missing_backups:
        confirmation_message += f"⚠️ Fehlende Backups: {', '.join(missing_backups)}\n\n"

    confirmation_message += (
        f"Möchtest du die globale Liste (`{GLOBAL_FOLLOWED_FILE}`) jetzt mit diesen {len(final_global_set)} Usern aktualisieren? "
        f"(Es werden nur User hinzugefügt, keine entfernt)."
    )

    # Frage nach Bestätigung
    keyboard = [[
        InlineKeyboardButton(f"✅ Ja, globale Liste aktualisieren", callback_data=f"confirm_build_global"),
        InlineKeyboardButton("❌ Nein, abbrechen", callback_data="cancel_build_global")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(confirmation_message, reply_markup=reply_markup)
    # Kein resume hier, warte auf Button

async def init_global_from_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Initialisiert/Überschreibt die globale Follower-Liste aus dem Backup
    eines spezifischen oder des aktuellen Accounts.
    """
    global global_followed_users_set # Zugriff zum Neuladen

    target_account_index = -1
    target_account_username = None
    backup_filepath = None

    # Bestimme den Ziel-Account
    if context.args:
        try:
            account_num = int(context.args[0])
            target_account_index = account_num - 1
            if not (0 <= target_account_index < len(ACCOUNTS)):
                await update.message.reply_text(f"❌ Ungültige Account-Nummer. Verfügbar: 1-{len(ACCOUNTS)}")
                return
            target_account_username = ACCOUNTS[target_account_index].get("username")
            # Backup-Pfad für den Ziel-Account holen
            safe_username = re.sub(r'[\\/*?:"<>|]', "_", target_account_username) if target_account_username else None
            if safe_username:
                backup_filepath = FOLLOWER_BACKUP_TEMPLATE.format(safe_username)
            else:
                 await update.message.reply_text(f"❌ Konnte Username für Account {account_num} nicht finden.")
                 return
        except ValueError:
            await update.message.reply_text("❌ Bitte eine gültige Account-Nummer angeben (z.B. `/initglobalfrombackup 1`).")
            return
        except Exception as e:
             await update.message.reply_text(f"❌ Fehler beim Ermitteln des Ziel-Accounts: {e}")
             return
    else:
        # Wenn keine Nummer angegeben, nimm den aktuellen Account
        target_account_index = current_account
        target_account_username = get_current_account_username()
        backup_filepath = get_current_backup_file_path()
        if not target_account_username or not backup_filepath:
             await update.message.reply_text("❌ Konnte aktuellen Account oder Backup-Pfad nicht ermitteln.")
             return

    # Prüfe, ob die Backup-Datei existiert
    if not backup_filepath or not os.path.exists(backup_filepath):
        await update.message.reply_text(f"❌ Backup-Datei für Account @{target_account_username} (`{os.path.basename(backup_filepath or '')}`) nicht gefunden. Bitte zuerst `/backupfollowers` für diesen Account ausführen.")
        return

    # Frage nach Bestätigung
    keyboard = [[
        InlineKeyboardButton(f"✅ Ja, globale Liste überschreiben", callback_data=f"confirm_init_global:{target_account_index}"),
        InlineKeyboardButton("❌ Nein, abbrechen", callback_data="cancel_init_global")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ **Achtung!**\n"
        f"Dies überschreibt die gesamte globale Follower-Liste (`{GLOBAL_FOLLOWED_FILE}`) "
        f"mit dem Inhalt des Backups von Account {target_account_index + 1} (@{target_account_username}).\n\n"
        f"Alle bisherigen Einträge in der globalen Liste gehen verloren.\n"
        f"Fortfahren?",
        reply_markup=reply_markup
    )
    # Kein resume hier, warte auf Button

async def autofollow_pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Pausiert die automatische Abarbeitung der Follow-Liste."""
    global is_periodic_follow_active
    is_periodic_follow_active = False
    await update.message.reply_text("⏸️ Automatisches Folgen aus der Account-Liste wurde pausiert.")
    print("[Auto-Follow] Pausiert via Telegram-Befehl.")
    # Kein resume_scraping nötig, da die Steuerung nur das Starten des Follow-Prozesses betrifft

async def autofollow_resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Setzt die automatische Abarbeitung der Follow-Liste fort."""
    global is_periodic_follow_active
    is_periodic_follow_active = True
    await update.message.reply_text("▶️ Automatisches Folgen aus der Account-Liste wurde fortgesetzt.")
    print("[Auto-Follow] Fortgesetzt via Telegram-Befehl.")
    # Kein resume_scraping nötig

async def autofollow_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt den Status der automatischen Follow-Funktion für den aktuellen Account an."""
    global is_periodic_follow_active, current_account_usernames_to_follow
    status = "AKTIV ▶️" if is_periodic_follow_active else "PAUSIERT ⏸️"
    account_username = get_current_account_username() or "Unbekannt"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)
    await update.message.reply_text(f"🤖 Status Auto-Follow für @{account_username}: {status}\n"
                                     f"📝 User in `{filename}`: {count}")
    # WICHTIG: Haupt-Handler hat pausiert, hier fortsetzen
    await resume_scraping()

async def clear_follow_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fragt nach Bestätigung zum Leeren der *aktuellen* Account-Follow-Liste."""
    global current_account_usernames_to_follow
    account_username = get_current_account_username() or "Unbekannt"
    filepath = get_current_follow_list_path()
    filename = os.path.basename(filepath) if filepath else "N/A"
    count = len(current_account_usernames_to_follow)

    if count == 0:
        await update.message.reply_text(f"ℹ️ Die Follow-Liste für @{account_username} (`{filename}`) ist bereits leer.")
        await resume_scraping() # Fortsetzen
        return

    keyboard = [[
        # Der Payload enthält jetzt den Account-Namen zur Sicherheit
        InlineKeyboardButton(f"✅ Ja, Liste für @{account_username} leeren", callback_data=f"confirm_clear_follow_list:{account_username}"),
        InlineKeyboardButton("❌ Nein, abbrechen", callback_data="cancel_clear_follow_list")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        f"⚠️ Bist du sicher, dass du die Follow-Liste für Account @{account_username} (`{filename}`) löschen möchtest? "
        f"Aktuell sind {count} User enthalten.",
        reply_markup=reply_markup
    )
    # Kein resume_scraping hier, warten auf Button-Antwort oder Timeout

async def show_mode(update: Update):
    """Zeigt den aktuellen Suchmodus an"""
    global search_mode
    mode_text = "CA + Keywords" if search_mode == "full" else "Nur CA"
    await update.message.reply_text(f"🔍 Search mode: {mode_text}")
    await resume_scraping()

async def set_mode_full(update: Update):
    """Setzt den Suchmodus auf CA + Keywords"""
    global search_mode
    search_mode = "full"
    await update.message.reply_text("✅ Suchmodus auf CA + Keywords gesetzt")
    await resume_scraping()

async def set_mode_ca_only(update: Update):
    """Setzt den Suchmodus auf nur CA"""
    global search_mode
    search_mode = "ca_only"
    await update.message.reply_text("✅ Suchmodus auf Nur CA gesetzt")
    await resume_scraping()

async def ping_pong_request(update: Update):
    """Verarbeite Ping-Requests zentralisiert"""
    await update.message.reply_text(f"🏓 Pong!")
    await resume_scraping()

async def pause_request(update: Update):
    """Pausiert das Scraping"""
    global is_schedule_pause
    await update.message.reply_text(f"⏸️ Scraping wird pausiert...")
    is_schedule_pause = False  # Manuelle Pause, nicht vom Scheduler
    await pause_scraping()
    await update.message.reply_text(f"⏸️ Scraping wurde pausiert! Verwende 'resume' zum Fortsetzen.")
    # NICHT resume_scraping() aufrufen!

async def resume_request(update: Update):

    """Setzt das Scraping fort"""
    await update.message.reply_text(f"▶️ Scraping wird fortgesetzt...")
    await resume_scraping()
    await update.message.reply_text(f"▶️ Scraping läuft wieder!")

async def show_schedule(update: Update):
    """Show the current schedule settings"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    status = "ENABLED ✅" if schedule_enabled else "DISABLED ❌"
    
    # Get current time to help debug timezone issues
    current_time = datetime.now().strftime("%H:%M")
    
    message = (
        f"📅 Schedule: {status}\n"
        f"⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}\n"
        f"🕒 Current system time: {current_time}\n\n"
    )
    
    # Add schedule status
    if schedule_enabled:
        # Check if we're currently in the pause period
        global is_schedule_pause
        is_schedule_pause = True
        now = datetime.now()
        today = now.date()
        current_dt = datetime.strptime(f"{today} {current_time}", "%Y-%m-%d %H:%M")
        start_time = datetime.strptime(f"{today} {schedule_pause_start}", "%Y-%m-%d %H:%M")
        end_time = datetime.strptime(f"{today} {schedule_pause_end}", "%Y-%m-%d %H:%M")
        
        # Handle overnight periods
        if end_time < start_time:
            end_time = end_time + timedelta(days=1)
        
        if start_time <= current_dt <= end_time:
            next_event = f"Resume at {schedule_pause_end}"
            status_str = "⏸️ PAUSED"
        else:
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
    """Zeigt einen vorbereiteten Befehl zum Einstellen des Zeitplans"""
    global schedule_pause_start, schedule_pause_end
    
    # Erstelle den Befehl mit aktuellem Zeitbereich
    command = f"schedule time {schedule_pause_start}-{schedule_pause_end}"
    
    # Sende die Nachricht
    await update.message.reply_text(
        f"{command}"
    )
    await resume_scraping()

async def set_schedule_enabled(update: Update, enabled: bool):
    """Enable or disable the schedule"""
    global schedule_enabled, schedule_pause_start, schedule_pause_end
    schedule_enabled = enabled
    save_schedule()
    
    if enabled:
        # Get current time to help with scheduling info
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        today = now.date()
        
        # Create datetime objects for comparison
        current_dt = datetime.strptime(f"{today} {current_time}", "%Y-%m-%d %H:%M")
        start_time = datetime.strptime(f"{today} {schedule_pause_start}", "%Y-%m-%d %H:%M")
        end_time = datetime.strptime(f"{today} {schedule_pause_end}", "%Y-%m-%d %H:%M")
        
        # Handle overnight periods
        if end_time < start_time:
            end_time = end_time + timedelta(days=1)
        
        # Check if we're currently in the pause period
        if start_time <= current_dt <= end_time:
            status_msg = (
                f"✅ Schedule ENABLED\n"
                f"⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}\n"
                f"⚠️ Current time ({current_time}) is within pause period!\n"
                f"⏸️ Bot will pause now until {schedule_pause_end}"
            )
            # Trigger pause immediately
            asyncio.create_task(pause_scraping())
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
    else:
        status_msg = f"❌ Schedule DISABLED\n⏰ Pause period: {schedule_pause_start} - {schedule_pause_end}"
    
    await update.message.reply_text(status_msg)
    await resume_scraping()

async def set_schedule_time(update: Update, time_str: str):
    """Set the schedule pause time period"""
    global schedule_pause_start, schedule_pause_end
    
    # Split the time range and remove any spaces
    time_parts = [t.strip() for t in time_str.replace('-', ' - ').split('-')]
    
    if len(time_parts) != 2:
        await update.message.reply_text("❌ Invalid time format. Please use HH:MM-HH:MM or HH:MM - HH:MM (24-hour format)")
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

async def switch_account_request(update: Update, account_num=None):
    """Wechselt manuell Account, pausiert Auto-Follow und lädt neue Follow-Liste."""
    global current_account, driver, is_periodic_follow_active, current_account_usernames_to_follow

    old_account_index = current_account # Index speichern

    # Bestimme neuen Index
    new_account_index = -1 # Ungültiger Startwert
    if account_num is not None:
         if 0 <= account_num < len(ACCOUNTS):
              new_account_index = account_num
         else:
              await update.message.reply_text(f"❌ Ungültige Account-Nummer. Verfügbar: 1-{len(ACCOUNTS)}")
              await resume_scraping() # Fortsetzen, da der Haupt-Handler pausiert hat
              return # Wichtig: Hier abbrechen
    else: # Wenn keine Nummer gegeben, zum nächsten wechseln
         new_account_index = (old_account_index + 1) % len(ACCOUNTS)

    # Nur weitermachen, wenn sich der Account tatsächlich ändert
    if new_account_index == old_account_index:
         await update.message.reply_text(f"ℹ️ Bereits auf Account {old_account_index+1}.")
         await resume_scraping() # Fortsetzen
         return

    # ===> NEU: Auto-Follow pausieren <===
    is_periodic_follow_active = False
    print("[Auto-Follow] Pausiert wegen Account-Wechsel.")

    old_account_username = ACCOUNTS[old_account_index].get("username", f"Index {old_account_index}")
    # ===> WICHTIG: Update globalen Index *bevor* get_current_account_username aufgerufen wird <===
    current_account = new_account_index # Index für den Rest des Skripts aktualisieren
    new_account_username = get_current_account_username() or f"Index {current_account}" # Hole neuen Usernamen

    await update.message.reply_text(f"🔄 Wechsle von Account @{old_account_username} zu @{new_account_username}...\n"
                                     f"⏸️ Automatisches Folgen wurde pausiert.")

    try:
        # WICHTIG: Logout sollte robust sein, auch wenn current_account schon neu ist
        await logout()
        if driver:
             try: driver.quit()
             except: pass # Fehler beim Schließen ignorieren
        driver = create_driver() # Neuer Driver für neuen Account

        # Explizit zur Login-Seite navigieren
        driver.get("https://x.com/login")
        await asyncio.sleep(3)

        # Login with the new account (login() verwendet jetzt den aktualisierten globalen `current_account`)
        result = await login()

        if result:
            await update.message.reply_text(f"✅ Erfolgreich zu Account @{new_account_username} gewechselt!")
            # ===> NEU: Lade die Follow-Liste für den NEUEN Account <===
            load_current_account_follow_list() # Lädt die Liste für den jetzt aktiven Account
            # Nach erfolgreichem Login zur Timeline navigieren
            try:
                driver.get("https://x.com/home")
                await asyncio.sleep(2)
                await switch_to_following_tab()
            except Exception as nav_home_err:
                 print(f"Warnung: Konnte nach Login nicht zu /home navigieren: {nav_home_err}")
        else:
            await update.message.reply_text(f"❌ Login mit Account @{new_account_username} fehlgeschlagen!")
            # Lade trotzdem die (vermutlich leere) Liste für den neuen Account
            load_current_account_follow_list()

    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Account-Wechsel: {str(e)}")
        # Versuche trotzdem, die Liste zu laden, um einen definierten Zustand zu haben
        load_current_account_follow_list()

    # WICHTIG: Scraping wurde vom Haupt-Handler pausiert, hier fortsetzen
    await resume_scraping()

async def account_request(update: Update):
    """Zeigt die aktuellen Keywords an"""
    await update.message.reply_text(f"🥷 Aktueller Account: {current_account+1}")
    await resume_scraping()

async def reboot_request(update: Update):
    """Verarbeite Reboot-Requests zentralisiert"""
    await update.message.reply_text(f"♻️ R E B O O T")
    try:
        # Save counts before rebooting
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system reboot command
        os.system("sudo reboot")
    except Exception as reboot_error:
        await send_telegram_message(f"❌ Fehler beim Neustart: {str(reboot_error)}")
    await resume_scraping()

async def shutdown_request(update: Update):
    """Verarbeite Shutdown-Requests zentralisiert"""
    await update.message.reply_text(f"😴 SHUTDOWN")
    try:
        # Save counts before shutdown
        save_posts_count()
        # Wait a moment to ensure the message is sent
        await asyncio.sleep(5)
        # Execute system shutdown command
        os.system("sudo shutdown now")
    except Exception as shutdown_error:
        await send_telegram_message(f"❌ Fehler beim Herunterfahren: {str(shutdown_error)}")
    await resume_scraping()

async def show_keywords(update: Update):
    """Zeigt die aktuellen Keywords an"""
    global KEYWORDS
    keywords_text = "\n".join([f"- {keyword}" for keyword in KEYWORDS])
    await update.message.reply_text(f"🔑 Aktuelle Keywords:\n{keywords_text}")
    await resume_scraping()

async def save_keywords():
    """Speichert die Keywords in einer Datei"""
    with open(KEYWORDS_FILE, 'w') as f:
        json.dump(KEYWORDS, f)

async def add_keyword(update: Update, keyword_text: str):
    """Fügt ein oder mehrere durch Komma getrennte Keywords hinzu"""
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
        response += f"✅ {len(added)} Keywords hinzugefügt: {', '.join(added)}\n"
    if already_exists:
        response += f"⚠️ {len(already_exists)} Keywords existieren bereits: {', '.join(already_exists)}"
    
    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def remove_keyword(update: Update, keyword_text: str):
    """Entfernt ein oder mehrere durch Komma getrennte Keywords aus der Liste"""
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
        response += f"🗑️ {len(removed)} Keywords entfernt: {', '.join(removed)}\n"
    if not_found:
        response += f"⚠️ {len(not_found)} Keywords nicht gefunden: {', '.join(not_found)}"
    
    await update.message.reply_text(response.strip())
    await show_keywords(update)
    await resume_scraping()

async def _send_long_message(application, chat_id, text, reply_markup, tweet_url):
    """
    Sendet Textnachrichten, teilt sie bei Bedarf auf (> 4096 Zeichen)
    und behandelt Fehler. Fügt Buttons nur am Ende hinzu.
    """
    global last_tweet_urls # Zugriff auf globale Variable für Tweet-URLs

    message_limit = 4096
    try:
        if len(text) > message_limit:
            print(f"Nachricht zu lang ({len(text)} > {message_limit}), teile auf...")
            chunks = [text[i:i+message_limit] for i in range(0, len(text), message_limit)]
            message_sent = None # Um die letzte Nachricht zu verfolgen
            for i, chunk in enumerate(chunks):
                # Füge Buttons nur zum letzten Chunk hinzu
                current_reply_markup = reply_markup if i == len(chunks) - 1 else None
                message_sent = await application.bot.send_message(
                    chat_id=chat_id,
                    text=chunk,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=current_reply_markup
                )
                await asyncio.sleep(0.5) # Rate-Limit vermeiden

            # Speichere Tweet-URL, wenn Buttons an der letzten Nachricht waren
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url

        else:
            # Nachricht ist kurz genug, sende in einem Rutsch
            message_sent = await application.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
             # Speichere Tweet-URL, wenn Buttons vorhanden waren
            if reply_markup and tweet_url:
                last_tweet_urls[chat_id] = tweet_url

    except Exception as send_error:
        print(f"Fehler beim Senden der Telegram-Nachricht (Text): {send_error}")
        # Fallback 1: Ohne HTML-Parsing versuchen
        try:
            plain_text_fallback = html.unescape(text) # Versuche, HTML-Entitäten für reinen Text zu entfernen
            await application.bot.send_message(
                chat_id=chat_id,
                text=plain_text_fallback, # Reinen Text senden
                parse_mode=None,
                disable_web_page_preview=True,
                reply_markup=reply_markup # Buttons beibehalten, falls möglich
            )
            print("Nachricht erfolgreich ohne HTML-Parsing gesendet.")
            # Speichere Tweet-URL auch hier, wenn Buttons vorhanden waren
            if reply_markup and tweet_url:
                 last_tweet_urls[chat_id] = tweet_url
        except Exception as plain_error:
            print(f"Senden ohne HTML-Parsing ebenfalls fehlgeschlagen: {plain_error}")
            # Fallback 2: Sehr kurze, einfache Nachricht senden
            try:
                # Extrahiere den ersten Teil des ursprünglichen Textes als Hinweis
                error_indicator_text = text.split('\n')[0] # Erste Zeile als Anhaltspunkt
                simple_text = error_indicator_text[:200] + "... [Fehler beim Senden]"
                await application.bot.send_message(chat_id=chat_id, text=simple_text)
            except Exception as final_error:
                print(f"Finaler Versuch, einfache Fehlermeldung zu senden, fehlgeschlagen: {final_error}")

async def send_telegram_message(text, images=None, tweet_url=None, reply_markup=None):
    """
    Sendet eine Nachricht an Telegram.
    Wenn Bilder vorhanden sind UND der Text > 1024 Zeichen lang ist,
    wird stattdessen eine Textnachricht mit einem 🖼️ Emoji gesendet.
    """
    global application, last_tweet_urls # Zugriff auf globale Variablen

    try:
        if application is None:
            print("Warnung: Telegram-Anwendung ist noch nicht initialisiert.")
            return

        # Bereite den Basistext vor (ohne Emoji erstmal)
        full_text = f"{text}\n"
        text_length = len(full_text)

        # --- Button Logik ---
        # Das Reply Markup wird jetzt direkt von process_tweets übergeben,
        # wenn Rating- oder Like/Repost-Buttons benötigt werden.
        # Wir verwenden einfach das `reply_markup`, das wir erhalten.
        final_reply_markup = reply_markup
        # --- Ende Button Logik ---

        await asyncio.sleep(0.5) # Reduziere mögliche Konflikte

        caption_limit = 1024
        image_emoji = "🖼️" # Emoji, das anzeigt, dass Bilder vorhanden waren

        # Prüfe, ob Bilder gesendet werden sollen
        if images:
            # Prüfe Länge für Caption
            if text_length <= caption_limit:
                # Fall 1: Bilder vorhanden, Text kurz genug -> Sende mit send_photo
                try:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=images[0], # Sende nur das erste Bild
                        caption=full_text, # Gesamter Text passt
                        parse_mode=ParseMode.HTML,
                        reply_markup=final_reply_markup
                    )
                    # Speichere Tweet-URL, da Bild gesendet wurde und Buttons evtl. da sind
                    if tweet_url:
                        last_tweet_urls[CHANNEL_ID] = tweet_url
                except Exception as send_photo_error:
                    print(f"Fehler beim Senden von Foto (trotz passender Länge): {send_photo_error}")
                    # Fallback: Versuche als Textnachricht mit Emoji zu senden
                    modified_text = f"{image_emoji} {full_text}"
                    await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

            else:
                # Fall 2: Bilder vorhanden, Text zu lang für Caption -> Sende als Text mit Emoji
                print(f"Nachricht zu lang für Caption ({text_length} > {caption_limit}), sende als Text mit Emoji.")
                modified_text = f"{image_emoji} {full_text}" # Emoji voranstellen
                await _send_long_message(application, CHANNEL_ID, modified_text, final_reply_markup, tweet_url)

        else:
            # Fall 3: Keine Bilder -> Sende als normale Textnachricht
            await _send_long_message(application, CHANNEL_ID, full_text, final_reply_markup, tweet_url)

    except Exception as e:
        print(f"Unerwarteter Fehler in send_telegram_message: {e}")

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
                #f"<a href=\"https://axiom.trade/meme/{contract}\">AXIOM</a>\n", (funktioniert derzeit nicht)
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
    is_recent = False
    formatted_string = "📅 Zeit ungültig"
    try:
        local_tz = ZoneInfo("Europe/Berlin")
    except Exception:
        local_tz = timezone(timedelta(hours=2)) # Fallback UTC+2

    try:
        tweet_time_utc = datetime.fromisoformat(datetime_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)
        tweet_time_local = tweet_time_utc.astimezone(local_tz)
        current_time_local = datetime.now(local_tz)
        time_diff = current_time_local - tweet_time_local

        minutes_ago = int(time_diff.total_seconds() // 60)
        hours_ago = int(time_diff.total_seconds() // 3600)

        if time_diff.total_seconds() < 0:
             formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')}"
             is_recent = False
        elif time_diff.total_seconds() < 3600: # Unter 1h
            formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({minutes_ago} min)"
            is_recent = True
        elif time_diff.total_seconds() < 86400: # Unter 1 Tag
             formatted_string = f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({hours_ago} Std.)"
             is_recent = True
        else: # Älter
            formatted_string = f"📅 {tweet_time_local.strftime('%d.%m.%y %H:%M')}"
            is_recent = False
    except ValueError:
        pass # Fehler beim Parsen, Standardwerte bleiben

    return formatted_string, is_recent

# def format_time(datetime_str):
#     """Format a tweet's timestamp into a readable string, considering DST."""
#     try:
#         # Zeitzone definieren (z.B. für Deutschland/Mitteleuropa)
#         # Ersetze "Europe/Berlin" ggf. durch deine korrekte IANA-Zeitzone
#         local_tz = ZoneInfo("Europe/Berlin")
#     except Exception as tz_error:
#         print(f"Fehler beim Laden der Zeitzone 'Europe/Berlin': {tz_error}")
#         # Fallback zu einer festen Zeitzone (wahrscheinlich falsch bei DST)
#         local_tz = timezone(timedelta(hours=1)) # Fallback CET

#     try:
#         # Tweet-Zeit ist in UTC ('Z')
#         tweet_time_utc = datetime.fromisoformat(datetime_str.replace('Z', '+00:00')).replace(tzinfo=timezone.utc)

#         # Konvertiere Tweet-Zeit in die lokale Zeitzone
#         tweet_time_local = tweet_time_utc.astimezone(local_tz)

#         # Hole die aktuelle Zeit ebenfalls in der lokalen Zeitzone
#         current_time_local = datetime.now(local_tz)

#         # Berechne die Differenz
#         time_diff = current_time_local - tweet_time_local
#         minutes_ago = int(time_diff.total_seconds() // 60)
#         hours_ago = int(time_diff.total_seconds() // 3600)

#         # Korrigierte strftime Formate: %d (Tag), %m (Monat), %y (Jahr zweistellig)
#         # Kein %MM oder %dd
#         if time_diff.total_seconds() < 0:
#              # Tweet liegt in der Zukunft (Uhrenproblem?) - gib absolute Zeit an
#              return f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')}"
#         elif time_diff.total_seconds() < 3600: # Weniger als 1 Stunde
#             return f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({minutes_ago} min)"
#         elif time_diff.total_seconds() < 86400: # Weniger als 1 Tag
#              return f"📅 {tweet_time_local.strftime('%H:%M %d.%m.%y')} ({hours_ago} Std.)" # Geändert zu Std.
#         else: # Älter als 1 Tag
#             return f"📅 {tweet_time_local.strftime('%d.%m.%y %H:%M')}" # Gib nur Datum und Uhrzeit

#     except ValueError as e:
#         print(f"Fehler beim Parsen/Formatieren der Zeit '{datetime_str}': {e}")
#         return "📅 Zeit ungültig" # Fallback-String

def format_token_info(tweet_text):
    """
    Extrahiert und formatiert Ticker ($) und Contract Addresses (CA)
    aus einem Tweet-Text. Filtert reine Währungsbeträge und Beträge
    mit K/M/B/T-Suffixen aus den Tickern heraus.
    """

    # --- Ticker ($) Extraktion und Bereinigung ---
    all_potential_tickers = [word for word in tweet_text.split() if word.startswith("$")]
    tickers = []
    punctuation_to_strip = '.,;:!?()&"\'+-/' # Satzzeichen am Ende entfernen

    # Regex, um reine Zahlenbeträge (optional mit .,) und solche mit K/M/B/T zu erkennen
    # ^ = Anfang des Strings (nach dem '$')
    # [0-9] = Muss mit einer Ziffer beginnen
    # [0-9,.]* = Kann weitere Ziffern, Kommas oder Punkte enthalten
    # ([KkMmBbTt])? = Kann optional mit K, M, B oder T enden (case-insensitive)
    # $ = Ende des Strings
    currency_pattern = r"^[0-9][0-9,.]*([KkMmBbTt])?$"

    for potential_ticker in all_potential_tickers:
        # Schritt 1: Allgemeine Satzzeichen am Ende entfernen
        cleaned = potential_ticker.rstrip(punctuation_to_strip)

        # Schritt 2: Prüfen, ob es ein gültiger Ticker ist (nicht nur '$')
        if len(cleaned) <= 1:
            continue # Nur '$' oder leer nach dem Stripping -> überspringen

        # Schritt 3: Den Teil nach dem '$' extrahieren
        value_part = cleaned[1:]

        # Schritt 4: Prüfen, ob der value_part einem reinen Währungs-/Zahlenbetrag entspricht
        # (z.B. "100", "123", "1,000", "123.45", "110T", "300B", "1.5k", "5M")
        if re.fullmatch(currency_pattern, value_part, re.IGNORECASE):
            continue # Dies ist ein reiner Betrag -> überspringen

        # Schritt 5: Wenn keine der obigen Filterbedingungen zutrifft, ist es ein gültiger Ticker
        tickers.append(cleaned)

    ticker_section = ""
    if tickers:
        # Duplikate entfernen und alphabetisch sortieren
        unique_tickers = sorted(list(set(tickers)))
        ticker_section = "\n💲 " + "".join(f"<code>{html.escape(ticker)}</code> " for ticker in unique_tickers).strip()

    # --- Contract Address (CA) Extraktion und Formatierung ---
    # (Dieser Teil bleibt unverändert, wie im Original angefragt)

    # Finde alle potenziellen CA-Matches basierend auf dem globalen Pattern
    try:
        ca_matches = re.findall(TOKEN_PATTERN, tweet_text)
    except NameError:
        print("FEHLER: TOKEN_PATTERN ist nicht definiert!")
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
             if match not in seen_tokens:
                 seen_tokens.add(match)
             continue

    contract_section = ""
    if filtered_ca_matches:
        contract_section += "\n📝 " # Eine Leerzeile davor
        for contract in filtered_ca_matches:
            try:
                chain = detect_chain(contract)
            except NameError:
                chain = "unknown" # Fallback, wenn detect_chain fehlt

            contract_section += f"<code>{html.escape(contract)}</code>\n"

            contract_section += f"🧬 {chain.upper()}\n"

            try:
                links_html = get_contract_links(contract, chain)
                if links_html:
                     contract_section += "\n" + links_html
            except NameError:
                pass

            contract_section += "\n" # Zusätzliche Leerzeile

    contract_section = contract_section.strip()

    # Gib beide Sektionen zurück
    return ticker_section, contract_section

# def is_tweet_recent(time_str):
#     """Check if a tweet is recent (within the last 15 minutes)"""
#     tweet_time = datetime.fromisoformat(time_str.replace('Z', '+01:00'))
#     current_time = datetime.now(timezone(timedelta(hours=1)))
#     return (current_time - tweet_time) <= timedelta(minutes=15)

async def process_tweets():
    """
    Process tweets in the timeline. Optimized to only search when necessary,
    scrolls down, and processes tweets *immediately* upon finding them
    to avoid StaleElementReferenceExceptions. INCLUDES ENHANCED DEBUG LOGGING FOR AUTHOR EXTRACTION.
    """
    global driver, is_scraping_paused, first_run, processed_tweets, KEYWORDS, TOKEN_PATTERN, search_mode, ratings_data

    if is_scraping_paused: return

    try:
        button_tweet_count = await check_new_tweets_button()
        should_search_and_process = first_run or button_tweet_count > 0

        if not should_search_and_process:
            return

        print("Suche und verarbeite neue Tweets (mit Scrollen)...")
        processed_in_this_round = set()
        newly_processed_count = 0
        max_scroll_attempts = 10
        scroll_attempt = 0
        target_to_find_or_process = button_tweet_count if button_tweet_count > 0 else (5 if first_run else 0)
        consecutive_scrolls_without_new = 0
        max_consecutive_scrolls_without_new = 3

        while scroll_attempt < max_scroll_attempts and newly_processed_count < target_to_find_or_process:
            scroll_attempt += 1
            found_in_this_scroll = 0
            current_containers = []
            try:
                current_containers = driver.find_elements(By.XPATH, '//article[@data-testid="tweet"]')
                if not current_containers:
                    print(f"Scroll-Versuch {scroll_attempt}/{max_scroll_attempts}: Keine Tweet-Container gefunden.")
                    await asyncio.sleep(1)
                    continue
            except Exception as e_find:
                 print(f"Fehler beim Finden von Tweet-Containern (Scroll-Loop {scroll_attempt}): {e_find}")
                 break

            print(f"Scroll-Versuch {scroll_attempt}/{max_scroll_attempts}: {len(current_containers)} Container gefunden. Verarbeite neue...")

            for container in current_containers:
                tweet_id = None
                tweet_url = None
                # === 1. ID und URL extrahieren ===
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
                    print(f"WARNUNG: Unerwarteter Fehler beim Extrahieren der ID/URL: {e_id_extract}")
                    continue

                # === 2. Prüfen, ob schon verarbeitet ===
                if tweet_id in processed_tweets or tweet_id in processed_in_this_round:
                    continue

                # === 3. Tweet SOFORT verarbeiten ===
                print(f"  -> Verarbeite neuen Tweet: {tweet_id}")
                increment_scanned_count()
                process_success = False
                try:
                    # --- Ad Check ---
                    is_ad = False
                    try:
                        WebDriverWait(container, 0.2).until(EC.presence_of_element_located((By.XPATH, './/span[text()="Ad" or text()="Anzeige"]')))
                        is_ad = True
                    except (TimeoutException, NoSuchElementException, StaleElementReferenceException): pass

                    if is_ad:
                        print(f"    Tweet {tweet_id} ist Werbung -> überspringe")
                        increment_ad_total_count()
                        processed_in_this_round.add(tweet_id)
                        processed_tweets.append(tweet_id)
                        found_in_this_scroll += 1
                        continue

                    # --- Repost Check ---
                    is_repost = False; repost_text = ""
                    try:
                        sc = WebDriverWait(container, 0.2).until(EC.presence_of_element_located((By.XPATH, './/span[@data-testid="socialContext"]')))
                        repost_text = sc.text.strip(); is_repost = bool(repost_text)
                        if is_repost: print(f"    Repost gefunden: {repost_text}")
                    except (TimeoutException, NoSuchElementException, StaleElementReferenceException): pass

                    # --- Zeit ---
                    datetime_str = ""; time_str = "📅 Zeit Unbekannt"; tweet_is_recent = False
                    try:
                        te = WebDriverWait(container, 0.5).until(EC.presence_of_element_located((By.XPATH, './/time[@datetime]')))
                        datetime_str = te.get_attribute('datetime')
                        if datetime_str: time_str, tweet_is_recent = format_time(datetime_str)
                    except (TimeoutException, NoSuchElementException, StaleElementReferenceException): pass

                    # --- Überspringen wenn alt & kein Repost ---
                    if not is_repost and not tweet_is_recent:
                        print(f"    Tweet {tweet_id} zu alt ({time_str}) & kein Repost -> überspringe")
                        processed_in_this_round.add(tweet_id); processed_tweets.append(tweet_id)
                        found_in_this_scroll += 1
                        continue

                    # --- Variablen für beide Autoren (Reposter und Original) ---
                    author_name = "Unbekannt"  # Original-Autor
                    author_handle = "@unbekannt"  # Original-Autor
                    reposter_name = None  # Reposter (nur bei Reposts)
                    reposter_handle = None  # Reposter (nur bei Reposts)


                    # --- Reposter-Extraktion (nur wenn is_repost True ist) ---
                    if is_repost:
                        try:
                            # Reposter Infos aus dem Social Context extrahieren
                            sc_element = WebDriverWait(container, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, './/span[@data-testid="socialContext"]'))
                            )
                            
                            # Für den Fall, dass wir den Link direkt finden können
                            try:
                                # Der Social Context Link enthält den Reposter
                                reposter_link = sc_element.find_element(By.XPATH, './/a[contains(@href, "/")]')
                                reposter_href = reposter_link.get_attribute('href')
                                
                                # Extrahiere den Handle aus dem href
                                if reposter_href:
                                    raw_handle = reposter_href.split('/')[-1]
                                    if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                        reposter_handle = "@" + raw_handle
                                    else:
                                        reposter_handle = "@unbekannt"
                                
                                # Extrahiere den Namen aus dem Text des Links
                                full_text = reposter_link.text.strip()
                                if " reposted" in full_text.lower():
                                    reposter_name = full_text.lower().split(" reposted")[0].strip()
                                else:
                                    reposter_name = full_text.strip()
                                    
                                print(f"    Reposter extrahiert (direct): Name='{reposter_name}', Handle='{reposter_handle}'")
                            except (NoSuchElementException, StaleElementReferenceException):
                                print("    DEBUG REPOST: Direkter Reposter-Link nicht gefunden. Versuche alternative Methode.")
                                
                                # Alternative: Extrahiere den Text und versuche den Namen zu bekommen
                                full_context_text = sc_element.text.strip()
                                if " reposted" in full_context_text.lower():
                                    reposter_name = full_context_text.lower().split(" reposted")[0].strip()
                                else:
                                    reposter_name = "Unbekannt"
                                    
                                # Versuche noch einen alternativen XPath für den Link
                                try:
                                    alt_reposter_link = container.find_element(By.XPATH, 
                                        './/div[1]/div/div/div/div/div[2]/div/div/div/a[contains(@href, "/")]')
                                    alt_href = alt_reposter_link.get_attribute('href')
                                    if alt_href:
                                        raw_handle = alt_href.split('/')[-1]
                                        if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                            reposter_handle = "@" + raw_handle
                                            print(f"    Reposter Handle mit alternativer Methode gefunden: {reposter_handle}")
                                except (NoSuchElementException, StaleElementReferenceException):
                                    print("    DEBUG REPOST: Auch alternativer Reposter-Link nicht gefunden.")
                                    reposter_handle = "@unbekannt"
                                    
                                print(f"    Reposter extrahiert (alternative): Name='{reposter_name}', Handle='{reposter_handle}'")
                                
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            print("    WARNUNG: Konnte socialContext für Repost nicht finden.")
                            reposter_name = "Unbekannt"
                            reposter_handle = "@unbekannt"
                        except Exception as e_repost_context:
                            print(f"    WARNUNG: Fehler beim Extrahieren des Repost-Kontexts: {e_repost_context}")
                            reposter_name = "Unbekannt"
                            reposter_handle = "@unbekannt"
                    # --- Original-Autor-Extraktion (für jeden Tweet) ---
                    try:
                        # Original-Autor (User-Name) extrahieren - immer vorhanden
                        try:
                            nc_id = "User-Name"
                            nc = WebDriverWait(container, 0.5).until(
                                EC.presence_of_element_located((By.XPATH, f'.//div[@data-testid="{nc_id}"]'))
                            )
                            
                            # Autor-Link (Handle) aus User-Name extrahieren
                            try:
                                user_link = nc.find_element(By.XPATH, './/a[contains(@href, "/")]')
                                user_href = user_link.get_attribute('href')
                                if user_href:
                                    raw_handle = user_href.split('/')[-1]
                                    if re.match(r'^[A-Za-z0-9_]{1,15}$', raw_handle):
                                        author_handle = "@" + raw_handle
                            except (NoSuchElementException, StaleElementReferenceException):
                                print("    WARNUNG: Konnte Original-Autor-Link nicht finden.")
                            
                            # Name aus dem ersten Span extrahieren (oder fallback auf ganzen Text)
                            try:
                                name_span = nc.find_element(By.XPATH, './/span[1]')
                                temp_name = name_span.text.strip()
                                author_name = temp_name if temp_name and not temp_name.startswith('@') else nc.text.strip()
                            except (NoSuchElementException, StaleElementReferenceException):
                                author_name = nc.text.strip()
                                
                            # Handle aus Name entfernen falls vorhanden
                            if author_handle != "@unbekannt" and author_handle in author_name:
                                author_name = author_name.replace(author_handle, '').strip()
                                
                            # Fallback für leeren Namen
                            if not author_name or author_name == author_handle:
                                author_name = f"Unbekannt ({author_handle})"
                                
                            print(f"    Original-Autor extrahiert: {author_name} ({author_handle})")
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            print("    WARNUNG: Konnte Original-Autor nicht finden.")
                            author_name = f"Unbekannt ({author_handle})"
                    except Exception as e_auth:
                        print(f"    WARNUNG: Autor-Extraktionsfehler: {e_auth}")
                        author_name = f"Unbekannt ({author_handle})"

                    # --- Inhalt ---
                    tweet_content = "[Inhalt nicht gefunden]"
                    try:
                        tt = WebDriverWait(container, 1).until(EC.presence_of_element_located((By.XPATH, './/div[@data-testid="tweetText"]')))
                        tweet_content = tt.text
                    except (TimeoutException, StaleElementReferenceException): print(f"    WARNUNG: Inhalt nicht gefunden für {tweet_id}.")

                    # --- Bilder ---
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

                    # --- Relevanzprüfung und Senden ---
                    ticker_section, contract_section = format_token_info(tweet_content)
                    contains_keyword = any(keyword.lower() in tweet_content.lower() for keyword in KEYWORDS)
                    contains_token = bool(contract_section)
                    is_relevant = (search_mode == "full" and (contains_keyword or contains_token)) or (search_mode != "full" and contains_token)

                    if is_relevant:
                        reasons = []
                        if contains_token: reasons.append("CA")
                        if search_mode == "full" and contains_keyword:
                            found_kws = [kw for kw in KEYWORDS if kw.lower() in tweet_content.lower()]
                            if found_kws: reasons.extend(found_kws)
                        print(f"    Tweet {tweet_id} relevant wegen: {', '.join(sorted(list(set(reasons))))}")
                        increment_found_count()

                        # --- Nachricht bauen (korrigierte Version) ---
                        handle_for_command = author_handle.lstrip('@')

                        # --- Rating Info holen ---
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
                                    rating_display = f" {average_rating:.1f}⭐({total_ratings})" # Leerzeichen am Anfang
                        # --- Ende Rating Info ---

                        user_info_line = f"👤 <b><a href=\"https://x.com/{html.escape(author_handle.lstrip('@'))}\">{html.escape(author_name)}</a></b> (<code><i>{html.escape(author_handle)}</i></code>){rating_display}" # Rating hier angefügt
                        message_parts = [user_info_line]
                        
                        # --- Nachricht bauen (für Repost) ---
                        if is_repost:
                            # Verwende reposter_name und reposter_handle für Repost-Info
                            reposter_handle_for_command = reposter_handle.lstrip('@') if reposter_handle and reposter_handle != "@unbekannt" else "unbekannt"
                            repost_info = f"🔄 <b><a href=\"https://x.com/{html.escape(reposter_handle_for_command.lstrip('@'))}\">{html.escape(reposter_name or 'Unbekannt')}</a></b> (<code><i>{html.escape(reposter_handle or '@unbekannt')}</i></code>) reposted"
                            message_parts.append(repost_info)
                            
                        message_parts.append(f"<blockquote>{html.escape(tweet_content)}</blockquote>")
                        message_parts.append(f"<b>{time_str}</b>")
                        message_parts.append(f"🌐 <a href='{tweet_url}'>Post-Link</a>")
                        if ticker_section: message_parts.append(ticker_section)
                        if contract_section: message_parts.append(contract_section)
                        if reasons: message_parts.append(f"💎 {', '.join(sorted(list(set(reasons))))} 💎")
                        final_message = "\n".join(message_parts)
                        # --- Ende Nachricht bauen ---

                        # --- Check for "Show more" ---
                        show_more_present = False
                        if not is_repost:
                            try:
                                WebDriverWait(container, 0.3).until(EC.presence_of_element_located((By.XPATH, './/button[@data-testid="tweet-text-show-more-link"]')))
                                show_more_present = True
                            except (TimeoutException, NoSuchElementException, StaleElementReferenceException): pass
                            except Exception as e_button: print(f"    WARNUNG: Fehler beim Prüfen auf 'Show more' Button: {e_button}")

                        if show_more_present or (is_repost and tweet_content and len(tweet_content) > 140):  # Check in both normal and repost cases
                            print(f"    'Show more' button found for tweet {tweet_id}, will add 'Show Full Text' button")
                            show_full_text_needed = True
                        else:
                            show_full_text_needed = False                        

                        # Rating-Buttons (nur wenn Token/CA vorhanden)
                        if contains_token:
                            source_key = author_handle
                            try:
                                author_name_str = str(author_name) if author_name else source_key
                                encoded_name = base64.urlsafe_b64encode(author_name_str.encode()).decode()
                            except Exception as enc_err:
                                logger.warning(f"Name encoding failed for {source_key}: {enc_err}. Using key.")
                                encoded_name = base64.urlsafe_b64encode(source_key.encode()).decode()
                            # Rating-Buttons
                            rating_buttons_row = [ InlineKeyboardButton(str(i)+"⭐", callback_data=f"rate:{i}:{source_key}:{encoded_name}") for i in range(1, 6) ]
                            combined_keyboard.append(rating_buttons_row)

                        # --- Markup bauen ---
                        final_reply_markup = None
                        combined_keyboard = []  # Initialize this once, at the beginning

                        # Rating-Buttons (nur wenn Token/CA vorhanden)
                        if contains_token:
                            source_key = author_handle
                            try:
                                author_name_str = str(author_name) if author_name else source_key
                                encoded_name = base64.urlsafe_b64encode(author_name_str.encode()).decode()
                            except Exception as enc_err:
                                logger.warning(f"Name encoding failed for {source_key}: {enc_err}. Using key.")
                                encoded_name = base64.urlsafe_b64encode(source_key.encode()).decode()
                            # Rating-Buttons
                            rating_buttons_row = [InlineKeyboardButton(str(i)+"⭐", callback_data=f"rate:{i}:{source_key}:{encoded_name}") for i in range(1, 6)]
                            combined_keyboard.append(rating_buttons_row)

                        # Like/Repost/FullText Buttons
                        action_buttons = []
                        if tweet_id:  # tweet_id sollte immer vorhanden sein
                            action_buttons.append(InlineKeyboardButton("👍 Like", callback_data=f"like:{tweet_id}"))
                            action_buttons.append(InlineKeyboardButton("🔄 Repost", callback_data=f"repost:{tweet_id}"))

                            # Füge "Show Full Text" Button hinzu, wenn nötig
                            if show_more_present or (is_repost and tweet_content and len(tweet_content) > 140):
                                print(f"    Füge 'Show Full Text' Button für Tweet {tweet_id} hinzu")
                                action_buttons.append(InlineKeyboardButton("📄 Full Text", callback_data=f"full:{tweet_id}"))

                        # Wichtig: Füge die Action-Buttons auf jeden Fall hinzu, wenn sie existieren
                        if action_buttons:
                            combined_keyboard.append(action_buttons)

                        if combined_keyboard:
                            final_reply_markup = InlineKeyboardMarkup(combined_keyboard)
                        # --- Ende Markup bauen ---

# --- Check for "Show more" ---
                        show_more_present = False
                        try:
                            # Suche nach dem "Show more" Link/Span *innerhalb* des tweetText-Divs
                            tweet_text_div = container.find_element(By.XPATH, './/div[@data-testid="tweetText"]')
                            # Verschiedene mögliche Texte/Elemente für "Show more" prüfen
                            WebDriverWait(tweet_text_div, 0.2).until(
                                EC.presence_of_element_located((By.XPATH, './/span[text()="Show more"] | .//a[contains(@href, "/status/") and contains(text(), "Show more")] | .//button[contains(., "Show more")]'))
                            )
                            show_more_present = True
                            print(f"    'Show more' Indikator gefunden für Tweet {tweet_id}")
                        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
                            pass # Kein "Show more" gefunden
                        except Exception as e_show_more:
                            print(f"    WARNUNG: Fehler beim Prüfen auf 'Show more': {e_show_more}")

                        show_full_text_needed = show_more_present
                        # --- Ende Check for "Show more" ---

                        # # --- Markup bauen ---
                        # final_reply_markup = None
                        # combined_keyboard = []

                        # # Rating-Buttons (nur wenn Token/CA vorhanden)
                        # if contains_token:
                        #     source_key = author_handle
                        #     try:
                        #         author_name_str = str(author_name) if author_name else source_key
                        #         encoded_name = base64.urlsafe_b64encode(author_name_str.encode()).decode()
                        #     except Exception as enc_err:
                        #         logger.warning(f"Name encoding failed for {source_key}: {enc_err}. Using key.")
                        #         encoded_name = base64.urlsafe_b64encode(source_key.encode()).decode()
                        #     # Rating-Header
                        #     combined_keyboard.append([InlineKeyboardButton(f"Rate {html.escape(author_name_str)}:", callback_data="rate_noop")])
                        #     # Rating-Buttons
                        #     rating_buttons_row = [ InlineKeyboardButton(str(i)+"⭐", callback_data=f"rate:{i}:{source_key}:{encoded_name}") for i in range(1, 6) ]
                        #     combined_keyboard.append(rating_buttons_row)

                        # # Like/Repost/FullText Buttons
                        # action_buttons = []
                        # if tweet_id: # tweet_id sollte immer vorhanden sein
                        #     action_buttons.append(InlineKeyboardButton("👍 Like", callback_data=f"like:{tweet_id}"))
                        #     action_buttons.append(InlineKeyboardButton("🔄 Repost", callback_data=f"repost:{tweet_id}"))

                        #     # Füge "Show Full Text" Button hinzu, wenn nötig
                        #     if show_full_text_needed:
                        #         # Verwende die TWEET_ID, um das Längenlimit einzuhalten! Kürzerer Prefix 'full'.
                        #         action_buttons.append(InlineKeyboardButton("📄 Show Full Text", callback_data=f"full:{tweet_id}"))

                        # if action_buttons:
                        #     combined_keyboard.append(action_buttons)

                        # if combined_keyboard:
                        #     final_reply_markup = InlineKeyboardMarkup(combined_keyboard)
                        # # --- Ende Markup bauen ---

                        # --- Namen in DB aktualisieren (Original-Autor) ---
                        # Dieser Block ist bei dir leer. Falls du hier Logik hattest,
                        # muss sie korrekt eingerückt sein. Wenn er leer bleiben soll,
                        # ist 'pass' eine gute Praxis.
                        if contains_token:
                            pass # Füge 'pass' hinzu, wenn der Block absichtlich leer ist

                        # --- Senden (IMMER wenn is_relevant) ---
                        # Korrekt eingerückt, eine Ebene tiefer als 'if is_relevant:',
                        # aber auf gleicher Ebene wie 'if contains_token:'
                        # +++ DEBUG LOGGING (vor dem Senden) +++
                        logger.debug(f"Tweet {tweet_id}: final_reply_markup vor Senden: {'Gesetzt' if final_reply_markup else 'None'}")
                        if final_reply_markup:
                            # Versuche, die Struktur sicher auszugeben
                            try:
                                keyboard_repr = repr(final_reply_markup.inline_keyboard)
                                logger.debug(f"Tweet {tweet_id}: Keyboard Struktur: {keyboard_repr[:500]}...") # Gekürzt für Lesbarkeit
                            except Exception as log_e:
                                logger.error(f"Fehler beim Loggen der Keyboard-Struktur: {log_e}")
                        # +++ ENDE DEBUG LOGGING +++

                        # Die ursprüngliche Sendezeile folgt hier:
                        await send_telegram_message(final_message, image_urls, tweet_url, reply_markup=final_reply_markup)
                        process_success = True # Gehört zum Senden

                    else: # Gehört zu 'if is_relevant:'
                        print(f"    Tweet {tweet_id} ohne Keywords/Token übersprungen")
                        process_success = True # Gehört zum Überspringen

                except StaleElementReferenceException:
                    print(f"    WARNUNG: Stale Element während der Detailverarbeitung von {tweet_id}. Überspringe.")
                    process_success = False
                except Exception as e_process:
                    print(f"    !!!!!!!! FEHLER bei Detailverarbeitung von Tweet {tweet_id} !!!!!!!! : {e_process}")
                    logger.error(f"Error processing details for tweet {tweet_id}", exc_info=True)
                    process_success = False

                # === 4. Markiere als verarbeitet ===
                processed_in_this_round.add(tweet_id)
                processed_tweets.append(tweet_id)
                found_in_this_scroll += 1
                if process_success:
                    newly_processed_count += 1

                print("    ____________________________________")

            # --- Ende der Schleife über aktuelle Container ---

            if found_in_this_scroll == 0:
                consecutive_scrolls_without_new += 1
                print(f"Scroll-Versuch {scroll_attempt}: Keine *neuen* Tweets in dieser Runde gefunden (Serie: {consecutive_scrolls_without_new}/{max_consecutive_scrolls_without_new}).")
                if consecutive_scrolls_without_new >= max_consecutive_scrolls_without_new:
                    print("Stoppe Scrollen frühzeitig, da keine neuen Tweets mehr gefunden wurden.")
                    break
            else:
                consecutive_scrolls_without_new = 0

            if newly_processed_count >= target_to_find_or_process:
                print(f"Ziel von {target_to_find_or_process} verarbeiteten Tweets erreicht.")
                break
            if scroll_attempt >= max_scroll_attempts:
                print(f"Maximale Anzahl von {max_scroll_attempts} Scroll-Versuchen erreicht.")
                break

            print(f"Scrolle nach unten für Versuch {scroll_attempt + 1}...")
            try:
                driver.execute_script("window.scrollBy(0, window.innerHeight * 0.8);")
                await asyncio.sleep(random.uniform(1.0, 2.0))
            except Exception as scroll_err:
                print(f"Fehler beim Scrollen: {scroll_err}. Breche Scroll-Loop ab.")
                break
        # --- Ende der while Scroll-Schleife ---

        if first_run and newly_processed_count > 0:
            first_run = False
            print("Erste Scan-Runde abgeschlossen, wechsle zu optimiertem Modus")

        print(f"Verarbeitungsrunde abgeschlossen. {newly_processed_count} Tweets neu verarbeitet.")

    except Exception as e_outer:
        print(f"!!!!!!!! SCHWERER FEHLER in process_tweets !!!!!!!! : {e_outer}")
        logger.error("Unhandled exception in process_tweets", exc_info=True)

async def process_full_text_request(query, tweet_id):
    try:
        # Notify user that we're processing
        await query.answer("Fetching full text...")
        
        # Generate the tweet URL from the ID
        tweet_url = f"https://twitter.com/i/status/{tweet_id}"
        
        # Navigate to the tweet with your existing driver
        driver.get(tweet_url)
        await asyncio.sleep(3)  # Wait for page to load
        
        # Find the tweet text
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
        
        # Get the full text now
        full_text = tweet_element.text
        
        # Update the original message with the full text
        original_message = query.message
        original_text = original_message.text
        
        # Replace the truncated text with the full text
        new_text = ""
        lines = original_text.split('\n')
        blockquote_start_index = -1
        blockquote_end_index = -1
        
        for i, line in enumerate(lines):
            if line.startswith('"') and blockquote_start_index == -1:
                blockquote_start_index = i
            elif blockquote_start_index != -1 and blockquote_end_index == -1 and (line.endswith('"') or i == len(lines) - 1 or line.startswith('🕒') or line.startswith('📅')):
                blockquote_end_index = i if line.endswith('"') else i - 1
        
        if blockquote_start_index != -1 and blockquote_end_index != -1:
            # Replace the blockquote content
            new_lines = lines[:blockquote_start_index]
            new_lines.append(f'"{full_text}"')
            new_lines.extend(lines[blockquote_end_index + 1:])
            new_text = '\n'.join(new_lines)
        else:
            # Fallback if we can't find the blockquote
            new_text = original_text + "\n\nFull text:\n" + full_text
        
        # Update the message
        await context.bot.edit_message_text(
            chat_id=original_message.chat_id,
            message_id=original_message.message_id,
            text=new_text,
            reply_markup=original_message.reply_markup,
            parse_mode='HTML'  # Use HTML if your original message uses HTML
        )
        
        await query.answer("Full text loaded!")
        
    except Exception as e:
        print(f"Error processing full text request: {e}")
        logger.error("Error processing full text request", exc_info=True)
        await query.answer("Could not load full text. Please try again.")

async def handle_callback_query(update, context):
    query = update.callback_query
    data = query.data
    
    if data.startswith("like:"):
        tweet_id = data.split(":", 1)[1]
        tweet_url = f"https://x.com/i/status/{tweet_id}"
        # Your like handling code
        
    elif data.startswith("repost:"):
        tweet_id = data.split(":", 1)[1]
        tweet_url = f"https://x.com/i/status/{tweet_id}"
        # Your repost handling code
        
    elif data.startswith("full:"):
        tweet_id = data.split(":", 1)[1]
        tweet_url = f"https://x.com/i/status/{tweet_id}"
        await process_full_text_request(query, tweet_url)
        
    elif data.startswith("rate:"):
        # Extract the rating data
        parts = data.split(":", 3)
        if len(parts) != 4:
            await query.answer("Invalid rating format")
            return
            
        rating_value = parts[1]
        source_key = parts[2]
        encoded_name = parts[3]
        
        try:
            # Convert rating to integer
            rating_int = int(rating_value)
            if not 1 <= rating_int <= 5:
                await query.answer("Invalid rating value")
                return
                
            # Decode the name
            try:
                author_name = base64.urlsafe_b64decode(encoded_name.encode()).decode()
            except:
                author_name = source_key
            
            # Update the ratings in your ratings_data dictionary
            if source_key in ratings_data:
                if "ratings" not in ratings_data[source_key]:
                    ratings_data[source_key]["ratings"] = {str(i): 0 for i in range(1, 6)}
                
                # Increment the rating count for this value
                if str(rating_int) in ratings_data[source_key]["ratings"]:
                    ratings_data[source_key]["ratings"][str(rating_int)] += 1
                else:
                    ratings_data[source_key]["ratings"][str(rating_int)] = 1
                    
                # Save the updated ratings
                save_ratings()
                
                # Calculate the new average rating
                total_ratings = 0
                weighted_sum = 0
                for star_str, count in ratings_data[source_key].get("ratings", {}).items():
                    try:
                        star = int(star_str)
                        if 1 <= star <= 5:
                            total_ratings += count
                            weighted_sum += star * count
                    except ValueError:
                        continue
                        
                if total_ratings > 0:
                    average_rating = weighted_sum / total_ratings
                    rating_display = f"{average_rating:.1f}⭐({total_ratings})"
                    await query.answer(f"You rated {author_name} {rating_int}⭐. New average: {rating_display}")
                else:
                    await query.answer(f"You rated {author_name} {rating_int}⭐")
            else:
                # Create a new entry if it doesn't exist
                ratings_data[source_key] = {
                    "name": author_name,
                    "ratings": {str(i): 0 for i in range(1, 6)}
                }
                ratings_data[source_key]["ratings"][str(rating_int)] = 1
                save_ratings()
                await query.answer(f"You rated {author_name} {rating_int}⭐")
                
        except Exception as e:
            print(f"Error processing rating: {e}")
            logger.error("Error processing rating", exc_info=True)
            await query.answer("Rating failed. Please try again.")

async def check_new_tweets_button():
    """
    Checks for the 'Show new tweets' button, clicks it, logs the count found
    on the button with structure, and returns that count.
    Returns 0 if the button is not found or no count could be extracted.
    """
    global driver # Zugriff auf den globalen WebDriver
    num_new_tweets_on_button = 0 # Standardwert
    try:
        # Warte kurz auf den Button
        button = WebDriverWait(driver, 2).until(
             EC.presence_of_element_located((By.XPATH, '//button[.//span[contains(text(), "Show") and contains(text(), "post")]]'))
        )

        # Versuche, die Anzahl für Logging zu extrahieren *bevor* dem Klick
        try:
            span_element = button.find_element(By.XPATH, './/span[contains(text(), "Show")]')
            button_text = span_element.text.strip()
            match = re.search(r'(\d+)', button_text)
            if match:
                num_new_tweets_on_button = int(match.group(1))
        except Exception as e_extract:
             # print(f"INFO: Konnte Zahl nicht aus Button-Text extrahieren: {e_extract}") # Optional
             num_new_tweets_on_button = 1 # Konservative Annahme


        # Sofort klicken, wenn Button gefunden
        button.click()

        # === STRUKTURIERTES LOGGING ===
        print("\n############################") # Leerzeile davor + Header
        print(f"Neue Tweets-Button geklickt (ca. {num_new_tweets_on_button} Tweets)")
        print("############################\n") # Header + Leerzeile danach
        # === ENDE STRUKTURIERTES LOGGING ===

        # Kurze Wartezeit, damit die neuen Tweets laden können
        await asyncio.sleep(random.uniform(1.5, 2.5))

        return num_new_tweets_on_button

    except (TimeoutException, NoSuchElementException):
        # Kein neuer Tweets Button gefunden, das ist normal
        return 0 # Gib 0 zurück, wenn kein Button gefunden wurde
    except Exception as e:
        print(f"Fehler beim Prüfen/Klicken des 'Neue Tweets'-Buttons: {e}")
        return 0 # Gib 0 im Fehlerfall zurück

# async def check_new_tweets_button():
#     try:
#         # Kürzerer Timeout für schnelleres Finden des Buttons
#         button = WebDriverWait(driver, 1).until(
#             EC.presence_of_element_located((By.XPATH, 
#             "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[5]/section/div/div/div[1]/div[1]/button"))
#         )
        
#         # Sofort klicken wenn Button gefunden
#         button.click()
#         print("Neue Tweets-Button sofort geklickt")
        
#         # Minimal warten nach dem Klick - nur 1 Sekunde
#         time.sleep(1)
        
#         # Versuche Anzahl für Logging zu extrahieren
#         try:
#             span_element = button.find_element(By.XPATH, "./div/div/span")
#             button_text = span_element.text.strip()
#             match = re.search(r'Show (\d+)', button_text)
#             if match:
#                 num_new_tweets = int(match.group(1))
#                 print(f"Ca. {num_new_tweets} neue Tweets geladen")
#                 return num_new_tweets
#         except:
#             pass
        
#         return 1  # Nur einen neuen Tweet annehmen
        
#     except Exception as e:
#         # Kein neuer Tweets Button gefunden, das ist normal
#         return 0

def increment_ad_total_count():
    """Increment the total count of found ads"""
    global posts_count
    # check_rotate_counts() ist hier NICHT nötig
    # Sicherstellen, dass ads_total existiert, bevor wir darauf zugreifen
    if "ads_total" not in posts_count:
        posts_count["ads_total"] = 0
    posts_count["ads_total"] += 1 # Erhöhe um 1

    # Speichere z.B. alle 50 Ads, um nicht ständig zu schreiben
    # Prüfe sicherheitshalber, ob der Key existiert
    if posts_count.get("ads_total", 0) % 50 == 0:
        save_posts_count()

# async def check_new_tweets_button():
#     """
#     Checks for the 'Show new tweets' button, clicks it, logs the count found
#     on the button, and returns that count.
#     Returns 0 if the button is not found or no count could be extracted.
#     """
#     num_new_tweets_on_button = 0 # Standardwert
#     try:
#         # Warte kurz auf den Button
#         button = WebDriverWait(driver, 2).until( # Leicht erhöhte Wartezeit auf 2s
#             EC.presence_of_element_located((By.XPATH,
#             # Robusterer XPath, der auf den Button selbst zielt
#             '//button[.//span[contains(text(), "Show") and contains(text(), "post")]]'
#             # Alternativ, wenn der alte zuverlässig war:
#             # "/html/body/div[1]/div/div/div[2]/main/div/div/div/div/div/div[5]/section/div/div/div[1]/div[1]/button"
#             ))
#         )

#         # Versuche, die Anzahl für Logging zu extrahieren *bevor* dem Klick
#         try:
#             # Suche nach dem inneren Span, der die Zahl enthält
#             span_element = button.find_element(By.XPATH, './/span[contains(text(), "Show")]')
#             button_text = span_element.text.strip()
#             # Regex, um die Zahl zu finden (robuster gegen Textänderungen)
#             match = re.search(r'(\d+)', button_text)
#             if match:
#                 num_new_tweets_on_button = int(match.group(1))
#         except Exception as e_extract:
#              print(f"INFO: Konnte Zahl nicht aus Button-Text extrahieren: {e_extract}")
#              # Setze auf 1, da der Button da ist, aber die Zahl fehlt (konservative Annahme)
#              num_new_tweets_on_button = 1


#         # Sofort klicken, wenn Button gefunden
#         button.click()
#         # Gib die Meldung direkt nach dem Klick aus
#         print(f"Neue Tweets-Button geklickt (ca. {num_new_tweets_on_button} Tweets)")

#         # Kurze Wartezeit, damit die neuen Tweets laden können
#         await asyncio.sleep(random.uniform(1.5, 2.5)) # Etwas länger warten

#         return num_new_tweets_on_button

#     except (TimeoutException, NoSuchElementException):
#         # Kein neuer Tweets Button gefunden, das ist normal
#         return 0 # Gib 0 zurück, wenn kein Button gefunden wurde
#     except Exception as e:
#         print(f"Fehler beim Prüfen/Klicken des 'Neue Tweets'-Buttons: {e}")
#         return 0 # Gib 0 im Fehlerfall zurück

# async def cleanup():
#     """Clean up resources when shutting down"""
#     global driver
#     if driver:
#         try:
#             driver.quit()
#         except Exception as e:
#             print(f"Error quitting driver: {e}")



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
                            text=f"<b>Full Text für <a href='{tweet_url}'>diesen Post</a>:</b>\n<blockquote>{escaped_full_text}</blockquote>\n\n🔥 FULL TEXT",
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
                                    # For full text, we might just remove the button or revert it
                                    # Reverting it:
                                    new_text = "✅ Full Text" if success else "📄 Full Text" # Revert to original
                                    # Or remove it (more complex, requires filtering the row)
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
            try: await application.bot.send_message(CHANNEL_ID, f"❌ Kritischer Fehler bei Verarbeitung von Aktion '{action_type}' aus der Queue.")
            except: pass
        finally:
            await resume_scraping()
            await asyncio.sleep(1)

        return action_processed
    else:
        return False

async def run():
    """Hauptschleife mit korrekter Zustandsbehandlung für Pause/Resume."""
    global application, global_followed_users_set, is_scraping_paused, is_schedule_pause, pause_event
    global last_follow_attempt_time, current_account_usernames_to_follow, is_periodic_follow_active
    global schedule_pause_start, schedule_pause_end # Zugriff für Nachrichten
    global search_mode, current_account # Für Startnachricht

    network_error_count = 0
    last_error_time = time.time()

    try:
        # --- Initialisierung (Telegram Bot, Einstellungen, Listen etc.) ---
        # ... (Dein Initialisierungscode wie vorher) ...
        print("Initialisiere Telegram Bot...")
        global ACTIVE_BOT_TOKEN
        if not ACTIVE_BOT_TOKEN: print("FEHLER: Kein aktiver Bot-Token!"); return
        application = ApplicationBuilder().token(ACTIVE_BOT_TOKEN).connect_timeout(30).read_timeout(30).build()
        # ... Handler hinzufügen ...
          # --- Command Handler registrieren (ALLE über den Admin Helper) ---
        # Syntax: add_admin_command_handler(application, "befehlsname", funktionsname)

        # Bestehende / Befehle
        add_admin_command_handler(application, "addusers", add_users_command)
        add_admin_command_handler(application, "autofollowpause", autofollow_pause_command)
        add_admin_command_handler(application, "autofollowresume", autofollow_resume_command)
        add_admin_command_handler(application, "autofollowstatus", autofollow_status_command) # Status evtl. öffentlich lassen?
        add_admin_command_handler(application, "clearfollowlist", clear_follow_list_command)
        add_admin_command_handler(application, "syncfollows", sync_followers_command)
        add_admin_command_handler(application, "buildglobalfrombackups", build_global_from_backups_command)
        add_admin_command_handler(application, "globallistinfo", global_list_info_command) # Info evtl. öffentlich?
        add_admin_command_handler(application, "initglobalfrombackup", init_global_from_backup_command)
        add_admin_command_handler(application, "cancelbackup", cancel_backup_command)
        add_admin_command_handler(application, "cancelsync", cancel_sync_command)
        add_admin_command_handler(application, "rates", show_ratings_command) # Ratings evtl. öffentlich?
        add_admin_command_handler(application, "backupfollowers", backup_followers_command)

        # Following Database Commands
        add_admin_command_handler(application, "scrapefollowing", scrape_following_command)
        add_admin_command_handler(application, "addfromdb", add_from_db_command)
        add_admin_command_handler(application, "canceldbscrape", cancel_db_scrape_command)

        # Umgewandelte Befehle
        add_admin_command_handler(application, "keywords", keywords_command) # Liste zeigen evtl. öffentlich?
        add_admin_command_handler(application, "addkeyword", add_keyword_command)
        add_admin_command_handler(application, "removekeyword", remove_keyword_command)
        add_admin_command_handler(application, "follow", follow_command)
        add_admin_command_handler(application, "unfollow", unfollow_command)
        add_admin_command_handler(application, "like", like_command)
        add_admin_command_handler(application, "repost", repost_command)
        add_admin_command_handler(application, "account", account_command) # Account zeigen evtl. öffentlich?
        add_admin_command_handler(application, "help", help_command) # Hilfe IMMER öffentlich lassen!
        add_admin_command_handler(application, "status", status_command)
        add_admin_command_handler(application, "stats", stats_command) # Stats evtl. öffentlich?
        add_admin_command_handler(application, "count", stats_command) # Alias für stats
        add_admin_command_handler(application, "ping", ping_command) # Ping IMMER öffentlich lassen!
        add_admin_command_handler(application, "mode", mode_command) # Modus zeigen evtl. öffentlich?
        add_admin_command_handler(application, "modefull", mode_full_command)
        add_admin_command_handler(application, "modeca", mode_ca_command)
        add_admin_command_handler(application, "pause", pause_command)
        add_admin_command_handler(application, "resume", resume_command)
        add_admin_command_handler(application, "schedule", schedule_command) # Schedule zeigen evtl. öffentlich?
        add_admin_command_handler(application, "scheduleon", schedule_on_command)
        add_admin_command_handler(application, "scheduleoff", schedule_off_command)
        add_admin_command_handler(application, "scheduletime", schedule_time_command)
        add_admin_command_handler(application, "switchaccount", switch_account_command)

        # Admin Management Commands (jetzt auch über den Helper registriert)
        # WICHTIG: Die Funktionen selbst brauchen KEINEN @admin_required Decorator mehr!
        add_admin_command_handler(application, "addadmin", add_admin_command)
        add_admin_command_handler(application, "removeadmin", remove_admin_command)
        add_admin_command_handler(application, "listadmins", list_admins_command)


        # Callback Handler für Buttons
        application.add_handler(CallbackQueryHandler(button_callback_handler))

        # Message Handler NUR für Nicht-Befehle (z.B. Auth-Code)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_telegram_message))

        max_init_retries = 3
        init_retry_delay = 10 # Sekunden
        initialized_successfully = False
        for attempt in range(max_init_retries):
            try:
                print(f"Versuche Telegram-Initialisierung (Versuch {attempt + 1}/{max_init_retries})...")
                # Setze Timeouts direkt hier oder im Builder (siehe Option 1)
                # application = ApplicationBuilder()...connect_timeout(20).read_timeout(20).build() # Beispiel
                await application.initialize()
                print("Telegram-Initialisierung erfolgreich.")
                initialized_successfully = True
                break # Erfolg, Schleife verlassen
            except telegram.error.TimedOut as e:
                print(f"WARNUNG: Timeout bei Telegram-Initialisierung (Versuch {attempt + 1}): {e}")
                if attempt < max_init_retries - 1:
                    print(f"Warte {init_retry_delay} Sekunden vor nächstem Versuch...")
                    await asyncio.sleep(init_retry_delay)
                else:
                    print("FEHLER: Maximale Initialisierungsversuche erreicht. Breche ab.")
                    # Hier könntest du entscheiden, das Skript ganz zu beenden:
                    # raise RuntimeError("Konnte Telegram Bot nach mehreren Versuchen nicht initialisieren.") from e
            except Exception as e:
                # Andere Fehler während der Initialisierung abfangen
                print(f"FEHLER bei Telegram-Initialisierung (Versuch {attempt + 1}): {e}")
                # Hier solltest du wahrscheinlich abbrechen
                raise RuntimeError(f"Unerwarteter Fehler bei Telegram-Initialisierung: {e}") from e

        if not initialized_successfully:
            # Beende das Skript, wenn die Initialisierung endgültig fehlschlägt
            print("FEHLER: Telegram konnte nicht initialisiert werden. Skript wird beendet.")
            # Optional: Sende eine letzte Meldung über einen anderen Weg, falls möglich
            return # Beendet die run() Funktion sauber
        # --- Ende Robuste Initialisierung ---

        print("Lade Einstellungen, Zähler und Listen...")
        load_settings(); load_posts_count(); load_schedule(); load_ratings()
        load_following_database() # Lade Following-DB
        load_admins() # Lade Admin-Liste
        global_followed_users_set = load_set_from_file(GLOBAL_FOLLOWED_FILE)
        print(f"{len(global_followed_users_set)} User global geladen.")
        load_current_account_follow_list()
        print("Telegram Polling starten...")
        try: # Alte Updates überspringen
            updates = await application.bot.get_updates(offset=-1, limit=1)
            if updates: await application.bot.get_updates(offset=updates[-1].update_id + 1)
            print("Alte Telegram-Updates übersprungen.")
        except Exception as e: print(f"Fehler beim Überspringen alter Updates: {e}")
        await application.start()
        await application.updater.start_polling(drop_pending_updates=True, timeout=30) # etc.
        print("WebDriver und X Login initialisieren...")
        await initialize()

        # --- Initialen Bot-Status aus Settings laden und ggf. durch Schedule anpassen ---
        # is_scraping_paused und is_periodic_follow_active werden bereits in load_settings() gesetzt
        # pause_event wird ebenfalls in load_settings() gesetzt

        is_schedule_pause = False # Schedule-Pause ist ein Laufzeit-Status, nicht persistent

        # Prüfe, ob der Schedule *jetzt* eine Pause erzwingen würde
        initial_schedule_check = check_schedule()
        if initial_schedule_check is True:
            # Wenn der Schedule eine Pause will UND der Bot aktuell läuft (laut Settings)
            if not is_scraping_paused:
                print("INFO: Startzeit liegt im geplanten Pausenzeitraum (Schedule aktiv). Überschreibe geladenen Status -> PAUSIERT.")
                is_scraping_paused = True
                is_schedule_pause = True
                pause_event.clear()
                # Kein save_settings() hier, da dies nur der initiale Zustand ist
            else:
                # Bot ist bereits pausiert (manuell oder durch letzten Lauf), Schedule will auch Pause
                print("INFO: Startzeit liegt im geplanten Pausenzeitraum, Bot ist bereits pausiert (laut Settings).")
                # Setze is_schedule_pause, falls der Grund jetzt der Schedule ist
                is_schedule_pause = True # Markieren, dass die *aktuelle* Pause (auch) wegen Schedule ist
        # Der Fall initial_schedule_check == "resume" wird hier nicht behandelt,
        # da der Bot standardmäßig pausiert startet oder der gespeicherte Zustand gilt.
        # Die Hauptschleife wird den Resume-Fall korrekt handhaben.

        running_status = "⏸️ PAUSED 🟡 (Schedule)" if is_scraping_paused and is_schedule_pause else ("⏸️ PAUSED 🟡 (Manual)" if is_scraping_paused else "▶️ RUNNING 🟢")
        mode_text = "Full 💯 (CA + Keywords)" if search_mode == "full" else "📝 CA ONLY" 
        schedule_status = "ON ✅" if schedule_enabled else "OFF ❌"
        current_username_welcome = get_current_account_username() or "N/A"
        autofollow_stat = "ACTIVE" if is_periodic_follow_active else "PAUSED"
        welcome_message = (
            f"🤖 raw-bot-X 🚀 START\n"
            f"👉 Acc {current_account+1} (@{current_username_welcome})\n\n"
            f"📊 ▫️STATUS▫️\n"
            f"{running_status}\n"
            f"🔍 Search mode: {mode_text}\n"
            f"⏰ Schedule: {schedule_status} ({schedule_pause_start} - {schedule_pause_end})\n"
            f"🏃🏼‍♂️‍➡️ Auto-Follow: {autofollow_stat}\n"
        )


        #         running_status = "▶️ RUNNING 🟢" # Standardwert

        # # Initialen Status prüfen (check_schedule verwenden)
        # initial_check_result = check_schedule()
        # if initial_check_result is True:
        #     print("INFO: Startzeit liegt im geplanten Pausenzeitraum. Setze initialen Status auf 'Pausiert'.")
        #     is_scraping_paused = True
        #     is_schedule_pause = True
        #     pause_event.clear()
        #     running_status = "⏸️ PAUSED 💤" # Status überschreiben
        # mode_text = "CA + Keywords" if search_mode == "full" else "CA only"
        # schedule_status = "ON ✅" if schedule_enabled else "OFF ❌"
        # running_status = "⏸️ PAUSED 🟡" if is_scraping_paused else "▶️ RUNNING 🟢"
        # current_username_welcome = get_current_account_username() or "N/A" # Sicherstellen, dass es einen Wert gibt
        # autofollow_stat = "ACTIVE ▶️" if is_periodic_follow_active else "PAUSED ⏸️" # NEU
        # welcome_message = (
        #     f"🤖 X|B0T S T A R T E D 🚀 👉 account {current_account+1} (@{current_username_welcome})\n\n" # Username hinzugefügt
        #     f"📊 ▫️STATUS▫️\n"
        #     f"🔍 Search mode: {mode_text}\n"
        #     f"⏰ Schedule: {schedule_status}\n"
        #     f"⏰ ↘️ Time: {schedule_pause_start} - {schedule_pause_end}\n"
        #     f"🏃🏼‍♂️‍➡️ Auto-Follow: {autofollow_stat}\n"
        #     f"Status: {running_status}"
        # )







        keyboard = [[InlineKeyboardButton("ℹ️ Hilfe anzeigen", callback_data="help:help")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_telegram_message(welcome_message, reply_markup=reply_markup)
        print("Startnachricht gesendet.")

        # --- Hauptschleife ---
        print("Starte Hauptschleife...")
        while True:
            try:
                # 1. Schedule-Prüfung (wie zuvor)
                schedule_action = check_schedule()
                if schedule_action == "resume":
                    if is_scraping_paused and is_schedule_pause:
                        print("[Run Loop] Zeitplan beendet Pause. Setze fort...")
                        await resume_scraping()
                        is_schedule_pause = False
                        await send_telegram_message("▶️ Geplante Pause beendet, Betrieb fortgesetzt.")
                elif schedule_action is True:
                    if not is_scraping_paused:
                        print("[Run Loop] Zeitplan startet Pause...")
                        # --- Nachricht über Pausenstart (Logik zur Zeitberechnung bleibt) ---
                        try:
                            try: local_tz = ZoneInfo("Europe/Berlin")
                            except: local_tz = ZoneInfo(None) # Fallback
                            now = datetime.now(local_tz); today = now.date()
                            start_dt = datetime.strptime(f"{today} {schedule_pause_start}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
                            end_dt = datetime.strptime(f"{today} {schedule_pause_end}", "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
                            next_end_dt = end_dt
                            is_overnight = end_dt <= start_dt
                            # Bestimme die *nächste* Endzeit korrekt
                            if is_overnight: # Wenn über Mitternacht
                                if now >= end_dt: # Wenn die heutige Endzeit schon vorbei ist
                                     next_end_dt = end_dt + timedelta(days=1) # Nimm die von morgen
                            elif now >= end_dt: # Wenn gleicher Tag, aber Endzeit schon vorbei
                                 next_end_dt = end_dt + timedelta(days=1) # Nimm die von morgen

                            remaining_time = next_end_dt - now
                            remaining_seconds = max(0, remaining_time.total_seconds())
                            total_minutes = int(remaining_seconds // 60)
                            hours = total_minutes // 60
                            minutes = total_minutes % 60
                            remaining_str = f"{hours}h {minutes}m"
                            if hours == 0 and minutes == 0 and remaining_seconds > 0: remaining_str = "< 1m"
                            message = (
                                f"⏰ Geplante Pause aktiviert\n"
                                f"⏸️ Pausiere von {schedule_pause_start} bis {schedule_pause_end}\n"
                                f"▶️ Fortsetzung in ~{remaining_str} (um {next_end_dt.strftime('%H:%M')})"
                            )
                            await send_telegram_message(message)
                        except Exception as msg_err:
                            print(f"FEHLER beim Senden der Pausen-Startnachricht: {msg_err}")
                        # --- Ende Nachricht ---
                        is_schedule_pause = True
                        await pause_scraping()

                # 2. Prüfen ob wir *jetzt* pausiert sind (wichtig!)
                if is_scraping_paused:
                    # print("[Run Loop] Pausiert. Warte 5s.") # Kürzere Wartezeit im Pausenzustand
                    await asyncio.sleep(5)
                    continue # Zum nächsten Schleifendurchlauf

                # 3. Wenn wir hier sind, ist der Bot NICHT pausiert

                # --- NEU: Periodischer WebDriver-Neustart ---
                global last_driver_restart_time, driver # Zugriff auf Globals
                restart_interval_seconds = 4 * 60 * 60 # 4 Stunden

                if time.time() - last_driver_restart_time > restart_interval_seconds:
                    print(f"INFO: {restart_interval_seconds / 3600:.1f} Stunden seit letztem Driver-Neustart vergangen. Starte neu...")
                    await send_telegram_message("🔄 Starte geplanten WebDriver-Neustart zur Speicherfreigabe...")
                    await pause_scraping() # Pausiere das Haupt-Scraping
                    login_ok_after_restart = False
                    try:
                        # Alten Driver sicher schließen
                        if driver:
                            print("Schließe alten WebDriver...")
                            try:
                                driver.quit()
                            except Exception as quit_err:
                                print(f"WARNUNG: Fehler beim Schließen des alten Drivers (möglicherweise schon geschlossen): {quit_err}")
                        driver = None # Explizit auf None setzen

                        # Neuen Driver erstellen
                        print("Erstelle neuen WebDriver...")
                        driver = create_driver() # Deine Funktion zum Erstellen des Drivers

                        # Erneut einloggen
                        print("Versuche erneuten Login mit aktuellem Account...")
                        if await login(): # login() verwendet global current_account
                            print("Login nach WebDriver-Neustart erfolgreich.")
                            await switch_to_following_tab() # Wichtig: Zum Following-Tab wechseln
                            await send_telegram_message("✅ WebDriver-Neustart und Login erfolgreich.")
                            last_driver_restart_time = time.time() # Zeitstempel NUR bei Erfolg aktualisieren
                            login_ok_after_restart = True
                        else:
                            print("FEHLER: Login nach WebDriver-Neustart fehlgeschlagen!")
                            await send_telegram_message("❌ FEHLER: Login nach WebDriver-Neustart fehlgeschlagen! Versuche beim nächsten Intervall erneut.")
                            # Zeitstempel NICHT aktualisieren, damit es bald wieder versucht wird
                    except Exception as restart_err:
                        print(f"FEHLER während des WebDriver-Neustarts/Logins: {restart_err}")
                        logger.error("Exception during WebDriver restart/login", exc_info=True)
                        await send_telegram_message(f"❌ Kritischer Fehler während WebDriver-Neustart: {str(restart_err)[:200]}")
                        # Driver ggf. auf None setzen, falls Erstellung fehlschlug
                        if 'driver' in locals() and driver is None:
                             pass # Ist schon None
                        elif 'driver' not in locals():
                             pass # Wurde nie zugewiesen
                        else: # Driver existiert, aber Login schlug fehl o.ä.
                             try:
                                 driver.quit()
                             except: pass
                             driver = None


                    await resume_scraping() # Scraping fortsetzen

                    # Wichtig: Nach einem Neustart-Versuch (egal ob erfolgreich)
                    # direkt zum nächsten Loop-Durchlauf springen, um den Zustand neu zu bewerten.
                    print("Setze Hauptschleife nach Neustart-Versuch fort...")
                    await asyncio.sleep(5) # Kurze Pause nach dem ganzen Prozess
                    continue # Springe zum Anfang der while-Schleife

                # --- ENDE WebDriver-Neustart ---

                # --- Periodischer Follow Check (wie zuvor) ---
                follow_interval = random.uniform(900, 1800) # 15-30 Minuten
                if is_periodic_follow_active and current_account_usernames_to_follow and (time.time() - last_follow_attempt_time > follow_interval):
                    if current_account_usernames_to_follow: # Nur wenn Liste nicht leer
                        username_to_try = random.choice(current_account_usernames_to_follow)
                        current_account_username_log = get_current_account_username() or "Unbekannt"
                        print(f"[Auto-Follow @{current_account_username_log}] Starte Versuch für: @{username_to_try}")
                        await pause_scraping() # Pausiere für den Follow-Versuch
                        follow_result = None
                        try:
                            follow_result = await follow_user(username_to_try)
                            if follow_result is True or follow_result == "already_following":
                                print(f"[Auto-Follow @{current_account_username_log}] Erfolg/Bereits gefolgt @{username_to_try}. Entferne aus Liste.")
                                if username_to_try in current_account_usernames_to_follow:
                                     current_account_usernames_to_follow.remove(username_to_try)
                                     save_current_account_follow_list()
                                else: print(f"Warnung: @{username_to_try} nicht mehr in Liste gefunden."); save_current_account_follow_list()
                                if username_to_try not in global_followed_users_set:
                                     global_followed_users_set.add(username_to_try); add_to_set_file({username_to_try}, GLOBAL_FOLLOWED_FILE); print(f"@{username_to_try} zur globalen Liste hinzugefügt.")
                                backup_filepath = get_current_backup_file_path();
                                if backup_filepath: add_to_set_file({username_to_try}, backup_filepath)
                            else: print(f"[Auto-Follow @{current_account_username_log}] Fehler bei @{username_to_try}. Bleibt in Liste.")
                        except Exception as follow_err: print(f"[Auto-Follow @{current_account_username_log}] Schwerer Fehler bei @{username_to_try}: {follow_err}")
                        finally:
                            last_follow_attempt_time = time.time() # Zeitstempel aktualisieren
                            await resume_scraping() # Scraping fortsetzen
                            await asyncio.sleep(random.uniform(3, 5)) # Kurze Pause nach Versuch

                    # Nach einem Follow-Versuch (erfolgreich oder nicht), direkt zum nächsten Loop-Durchlauf
                    continue

                # --- Queue Check 1: Vor dem Tweet Processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Starte nächsten Loop-Durchlauf sofort nach Button-Aktion
                # --- Scroll to Top before processing ---
                try:
                    #print("[Run Loop] Scrolling to top...") # Optional Debug
                    driver.execute_script("window.scrollTo(0, 0);")
                    await asyncio.sleep(random.uniform(0.5, 1.0)) # Short wait after scroll up
                except Exception as scroll_err:
                    print(f"Fehler beim Scrollen nach oben: {scroll_err}")
                # --- End Scroll to Top ---

                # --- Queue Check 1.5: After scrolling top, before processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Start next loop iteration immediately after button action
                # --- Haupt-Scraping-Logik ---
                await process_tweets()

                # --- Queue Check 2: Nach dem Tweet Processing ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Starte nächsten Loop-Durchlauf sofort nach Button-Aktion

                # --- Rate Limit Check ---
                await check_rate_limit()

                # --- Queue Check 3: Nach Rate Limit Check ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Starte nächsten Loop-Durchlauf sofort nach Button-Aktion

                # --- Integrierte Scroll-Logik (ersetzt perform_scroll_cycle) ---
                # Entscheide, ob in dieser Iteration gescrollt wird (z.B. 80% Wahrscheinlichkeit)
                if random.random() < 0.8: # Scrollt in 80% der Fälle
                    try:
                        # Zufällige Scroll-Distanz (z.B. 60% bis 110% der Fensterhöhe)
                        scroll_percentage = random.uniform(0.6, 1.1)
                        scroll_command = f"window.scrollBy(0, window.innerHeight * {scroll_percentage});"
                        # Optional: Debug-Log für die Scroll-Distanz
                        # logger.debug(f"[Run Loop] Scrolling down by {scroll_percentage:.2f} * viewport height...")
                        print(f"[Run Loop] Scrolling down by {scroll_percentage:.2f} * viewport height...") # Konsolenausgabe
                        driver.execute_script(scroll_command)

                        # Zufällige Wartezeit nach dem Scrollen (größerer Bereich)
                        wait_after_scroll = random.uniform(0.8, 2.8)
                        await asyncio.sleep(wait_after_scroll)
                    except Exception as scroll_err:
                        print(f"Fehler beim Scrollen in run loop: {scroll_err}")
                else:
                    # Optional: Loggen, wenn Scrollen übersprungen wird
                    # logger.debug("[Run Loop] Skipping scroll this iteration.")
                    print("[Run Loop] Skipping scroll this iteration.")
                    # Optional: Kleine alternative Wartezeit, wenn nicht gescrollt wird
                    await asyncio.sleep(random.uniform(0.5, 1.5))

                # --- Queue Check 4: Nach dem Scrollen ---
                action_was_processed = await check_and_process_queue(application)
                if action_was_processed:
                    continue # Starte nächsten Loop-Durchlauf sofort nach Button-Aktion

                # --- Kurze Pause am Ende des Loops ---
                await asyncio.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                # ... (Deine Fehlerbehandlung wie vorher) ...
                print(f"!! FEHLER in Hauptschleife: {e} !!")
                import traceback
                traceback.print_exc()
                # Netzwerkfehler-Logik etc.
                current_time = time.time()
                if isinstance(e, (requests.exceptions.ConnectionError, TimeoutException, OSError)): # OSError für DNS etc.
                    network_error_count += 1
                    print(f"Netzwerkfehler erkannt ({network_error_count}). Warte länger...")
                    await asyncio.sleep(60 * network_error_count) # Längere Pause bei wiederholten Fehlern
                    if network_error_count > 3 and (current_time - last_error_time) < 600: # Mehr als 3 Fehler in 10 Min
                         print("FEHLER: Zu viele Netzwerkfehler in kurzer Zeit. Bot wird gestoppt. Bitte manuell neu starten.")
                         await send_telegram_message("🚨 Zu viele Netzwerkfehler in kurzer Zeit. Bot wird gestoppt. Bitte manuell neu starten.")
                         # Beende das Skript sauber, statt Neustart zu versuchen
                         await cleanup()
                         sys.exit(1) # Beendet das Skript mit Fehlercode
                    last_error_time = current_time
                else:
                    network_error_count = 0 # Reset bei anderen Fehlern
                    await asyncio.sleep(15) # Standardpause bei anderen Fehlern
                last_error_time = current_time # Zeit des letzten Fehlers merken

            except Exception as e:
                # ... (Deine Fehlerbehandlung wie vorher) ...
                print(f"!! FEHLER in Hauptschleife: {e} !!")
                import traceback
                traceback.print_exc()
                # ... (Netzwerkfehler-Logik etc.) ...
                await asyncio.sleep(15)

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt empfangen. Beende Bot...")
    except Exception as e:
        print(f"\n!! KRITISCHER FEHLER außerhalb der Hauptschleife: {e} !!")
        import traceback
        traceback.print_exc()
    finally:
        print("Führe Cleanup aus...")
        await cleanup()
        # ... (Restlicher Cleanup für Telegram) ...
        if application and application.running:
            print("Stoppe Telegram..."); await application.updater.stop(); await application.stop(); await application.shutdown(); print("Telegram gestoppt.")
        print("Cleanup abgeschlossen. Skript beendet.")

async def show_ratings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Zeigt die gesammelten Ratings an, inklusive Top 3."""
    global ratings_data
    load_ratings() # Stelle sicher, dass die neuesten Daten geladen sind

    if not ratings_data:
        await update.message.reply_text("📊 Noch keine Ratings vorhanden.")
        await resume_scraping() # Fortsetzen nach Befehl
        return

    source_averages = []
    # Berechne Durchschnitt für alle Quellen zuerst
    for source_key, data in ratings_data.items():
        # Stelle sicher, dass 'ratings' existiert und ein Dictionary ist
        rating_counts = data.get("ratings", {})
        if not isinstance(rating_counts, dict):
            print(f"WARNUNG: Ungültige Rating-Daten für {source_key}, überspringe.")
            continue

        # Hole den Namen, falle auf den Key zurück, wenn nicht vorhanden
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
                continue # Ignoriere ungültige Schlüssel

        if total_ratings > 0:
            average = weighted_sum / total_ratings
            source_averages.append({
                "key": source_key,
                "name": display_name,
                "average": average,
                "total_ratings": total_ratings,
                "counts": rating_counts # Behalte die Zählungen für die Detailansicht
            })
        else:
             # Optional: Quellen ohne Ratings hinzufügen, wenn gewünscht (hier nicht für Top 3 relevant)
             pass

    # Sortiere nach Durchschnitt (absteigend)
    sorted_averages = sorted(source_averages, key=lambda item: item["average"], reverse=True)

    output_messages = []
    current_message = ""

    # === NEU: Top 3 Abschnitt ===
    top_3_output = "🏆 <b>Top 3 Rated Sources</b> 🏆\n"
    top_3_output += "     ⚜️⚜️⚜️\n"
    if not sorted_averages:
        top_3_output += "<i>(Noch keine Quellen mit Ratings)</i>\n"
    else:
        # Define medal emojis
        medals = ["🥇", "🥈", "🥉"]
        for i, item in enumerate(sorted_averages[:3]):
            # Get the corresponding medal, default to empty string if index out of bounds (shouldn't happen with [:3])
            medal = medals[i] if i < len(medals) else ""
            # Zeige Name und Handle (Key) with medal
            top_3_output += (f"{medal} {html.escape(item['name'])} ({html.escape(item['key'])}) "
                             f"~ {item['average']:.2f} ⭐ ({item['total_ratings']} Ratings)\n")
    top_3_output += "     ⚜️⚜️⚜️\n"
    current_message += top_3_output
    # === ENDE Top 3 Abschnitt ===

    current_message += "\n📊 <b>Alle Ratings (Detail):</b>\n" # Überschrift für Details

    # Füge Details für alle Quellen hinzu (sortiert nach Key für Konsistenz)
    # Sortiere die ursprünglichen Daten nach Key
    all_sorted_sources = sorted(ratings_data.items())

    for source_key, data in all_sorted_sources:
        # Hole die Daten erneut oder verwende die bereits berechneten, falls verfügbar
        # Hier holen wir sie neu, um sicherzustellen, dass alle angezeigt werden
        display_name = data.get("name", source_key)
        rating_counts = data.get("ratings", {})
        if not isinstance(rating_counts, dict): continue # Überspringe ungültige

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

        # Prüfen, ob die Nachricht zu lang wird
        if len(current_message) + len(source_output) > 4000: # Etwas Puffer lassen
            output_messages.append(current_message)
            current_message = source_output.lstrip() # Neue Nachricht
        else:
            current_message += source_output

    output_messages.append(current_message) # Füge die letzte Nachricht hinzu

    # Sende die Nachrichten
    for msg in output_messages:
        if msg.strip(): # Nur senden, wenn die Nachricht nicht leer ist
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            time.sleep(0.5) # Kleine Pause

    await resume_scraping() # Fortsetzen nach Befehl

# --- Admin Management Commands ---
async def add_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fügt einen neuen Admin hinzu (nur für Admins)."""
    global admin_user_ids
    # --- GEÄNDERT: Argumentanzahl prüfen (genau 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Bitte gib genau EINE Telegram User ID nach dem Befehl an.\nFormat: `/addadmin <user_id>`")
        return
    # --- ENDE ÄNDERUNG ---

    try:
        new_admin_id = int(context.args[0])
        if new_admin_id in admin_user_ids:
            await update.message.reply_text(f"ℹ️ User ID {new_admin_id} ist bereits ein Admin.")
        else:
            admin_user_ids.add(new_admin_id)
            save_admins()
            await update.message.reply_text(f"✅ User ID {new_admin_id} wurde erfolgreich als Admin hinzugefügt.")
            logger.info(f"Admin {update.message.from_user.id} added new admin {new_admin_id}")
    except ValueError:
        await update.message.reply_text("❌ Ungültige User ID. Bitte gib eine Zahl an.")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Hinzufügen des Admins: {e}")
        logger.error(f"Error adding admin {context.args[0]}: {e}", exc_info=True)

async def remove_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entfernt einen Admin (nur für Admins)."""
    global admin_user_ids
    current_user_id = update.message.from_user.id

    # --- GEÄNDERT: Argumentanzahl prüfen (genau 1) ---
    if not context.args or len(context.args) != 1:
        await update.message.reply_text("❌ Bitte gib genau EINE Telegram User ID nach dem Befehl an.\nFormat: `/removeadmin <user_id>`")
        return
    # --- ENDE ÄNDERUNG ---

    try:
        admin_id_to_remove = int(context.args[0])

        # Sicherheitsabfrage: Verhindere das Entfernen des letzten Admins
        if len(admin_user_ids) <= 1 and admin_id_to_remove in admin_user_ids:
             await update.message.reply_text("⚠️ Aktion nicht erlaubt: Dies ist der letzte Admin.")
             return

        # Optional: Verhindere Selbstentfernung (kann man auch erlauben)
        # if admin_id_to_remove == current_user_id:
        #     await update.message.reply_text("⚠️ Du kannst dich nicht selbst entfernen.")
        #     return

        if admin_id_to_remove in admin_user_ids:
            admin_user_ids.remove(admin_id_to_remove)
            save_admins()
            await update.message.reply_text(f"🗑️ User ID {admin_id_to_remove} wurde erfolgreich als Admin entfernt.")
            logger.info(f"Admin {current_user_id} removed admin {admin_id_to_remove}")
        else:
            await update.message.reply_text(f"ℹ️ User ID {admin_id_to_remove} wurde nicht in der Admin-Liste gefunden.")
    except ValueError:
        await update.message.reply_text("❌ Ungültige User ID. Bitte gib eine Zahl an.")
    except Exception as e:
        await update.message.reply_text(f"❌ Fehler beim Entfernen des Admins: {e}")
        logger.error(f"Error removing admin {context.args[0]}: {e}", exc_info=True)

async def list_admins_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listet alle aktuellen Admin User IDs auf (nur für Admins)."""
    global admin_user_ids
    if not admin_user_ids:
        await update.message.reply_text("ℹ️ Aktuell sind keine Admins definiert.")
        return

    admin_list_str = "\n".join([f"- `{uid}`" for uid in sorted(list(admin_user_ids))])
    await update.message.reply_text(f"👑 Aktuelle Admin User IDs:\n{admin_list_str}", parse_mode=ParseMode.MARKDOWN)

# --- End Admin Management Commands ---

# --- Deine Funktion cleanup() ---
async def cleanup():
    """Ressourcen beim Beenden aufräumen"""
    global driver
    print("Versuche WebDriver zu schließen...")
    if driver:
        try:
            driver.quit()
            print("WebDriver erfolgreich geschlossen.")
            driver = None
        except Exception as e:
            print(f"Fehler beim Schließen des WebDrivers: {e}")
    else:
        print("Kein aktiver WebDriver zum Schließen gefunden.")


# ... (Rest deines Skripts: __main__ Block etc.) ...
# Stelle sicher, dass der __main__ Block `asyncio.run(run())` aufruft
if __name__ == '__main__':
    # ... (Dein Argument-Parsing-Code zum Setzen von ACTIVE_BOT_TOKEN und current_account) ...
    # Standardwerte
    account_index_to_use = 0
    bot_token_to_use = DEFAULT_BOT_TOKEN
    bot_identifier = "Default"

    # === ASCII Art Animation beim Start (NACH nest_asyncio) ===
    try:
        # Stelle sicher, dass die Funktion und die Variable oben definiert sind
        display_ascii_animation(ascii_art)
    except NameError:
        print("WARNUNG: ASCII Art konnte nicht angezeigt werden (Funktion/Variable nicht gefunden).")
    # =====================================

    # Argument Parsing (wie von dir bereitgestellt)
    num_args = len(sys.argv)
    valid_args = True
    if num_args == 1:
        print("")
        print("")
        print("Kein Argument angegeben. Starte mit Standard-Bot und Account 1.")
    elif num_args == 2:
        arg1 = sys.argv[1]
        if arg1.lower() == "test":
            print("")
            print("")
            print("Argument 'test' erkannt. Starte mit Test-Bot und Account 1.")
            bot_token_to_use = TEST_BOT_TOKEN
            bot_identifier = "Test"
            if not bot_token_to_use: print("FEHLER: TEST_BOT_TOKEN nicht in config.env!"); valid_args = False
        else:
            try:
                req_acc_num = int(arg1); req_idx = req_acc_num - 1
                if 0 <= req_idx < len(ACCOUNTS):
                    print(f"Argument '{arg1}' als Account-Nr. erkannt. Starte Standard-Bot, Account {req_acc_num}.")
                    account_index_to_use = req_idx
                else: print(f"Warnung: Ungültige Account-Nr. '{req_acc_num}'. Verfügbar: 1-{len(ACCOUNTS)}."); valid_args = False
            except ValueError: print(f"Warnung: Ungültiges Argument '{arg1}'. Erwartet: Account-Nr. oder 'test'."); valid_args = False
        if not valid_args: print("Verwende Standard-Bot und Account 1 als Fallback."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default"; valid_args = True
    elif num_args == 3:
        arg1, arg2 = sys.argv[1], sys.argv[2]
        if arg1.lower() == "test": bot_token_to_use = TEST_BOT_TOKEN; bot_identifier = "Test";
        elif arg1.lower() == "default": bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default";
        else: print(f"Warnung: Ungültiger Bot-ID '{arg1}'. Erwartet: 'test'/'default'."); valid_args = False;
        if valid_args:
             try:
                 req_acc_num = int(arg2); req_idx = req_acc_num - 1
                 if 0 <= req_idx < len(ACCOUNTS): account_index_to_use = req_idx
                 else: print(f"Warnung: Ungültige Account-Nr. '{req_acc_num}'. Verfügbar: 1-{len(ACCOUNTS)}."); valid_args = False
             except ValueError: print(f"Warnung: Ungültige Account-Nr. '{arg2}'."); valid_args = False
        if not valid_args: print("Argumente ungültig. Starte Standard-Bot, Account 1."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default"; valid_args = True
    else: print("Warnung: Zu viele Argumente. Starte Standard-Bot, Account 1."); account_index_to_use = 0; bot_token_to_use = DEFAULT_BOT_TOKEN; bot_identifier = "Default";

    # Finale Prüfung und Globals setzen
    if not bot_token_to_use: print("FEHLER: Kein gültiger Bot-Token (Standard/Test). config.env prüfen!"); sys.exit(1)
    if not (0 <= account_index_to_use < len(ACCOUNTS)): print(f"FEHLER: Interner Fehler - Account-Index {account_index_to_use} ungültig."); sys.exit(1)

    ACTIVE_BOT_TOKEN = bot_token_to_use
    current_account = account_index_to_use

    print(f"\n\n\n---> Starte Skript mit Bot: '{bot_identifier}', Account: {account_index_to_use + 1} <---\n")

    # Skript starten
    try:
        # Event Loop holen oder erstellen und run() ausführen
        # loop = asyncio.get_event_loop() # Kann manchmal Probleme machen
        # loop.run_until_complete(run())
        asyncio.run(run()) # Bevorzugte Methode in Python 3.7+
    except KeyboardInterrupt:
        print("Bot wird wegen KeyboardInterrupt gestoppt...")
    except RuntimeError as e:
         if "Cannot run the event loop while another loop is running" in str(e) or "This event loop is already running" in str(e):
              print("Fehler: Event-Loop-Konflikt. Versuche nest_asyncio (bereits angewendet). Skript könnte instabil sein.")
              # Versuche, die run-Funktion manuell in der bestehenden Schleife auszuführen (Experimentell!)
              # loop = asyncio.get_event_loop()
              # loop.create_task(run()) # Nicht blockierend, könnte zu Problemen führen
         else:
             print(f"Unerwarteter Laufzeitfehler im __main__: {e}")
             import traceback
             traceback.print_exc()
    except Exception as e:
        print(f"Unerwarteter Fehler im __main__: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("Skriptausführung beendet.")
    



# if __name__ == '__main__':
#     # Löse das Problem mit verschiedenen Event Loops
#     import nest_asyncio
#     nest_asyncio.apply()
#     import asyncio # Stelle sicher, dass asyncio hier importiert wird
#     import sys     # Import für sys.argv hinzufügen/sicherstellen
#     # os und load_dotenv sind oben schon importiert

#     display_ascii_animation(ascii_art)

#     # --- Standardwerte ---
#     account_index_to_use = 0
#     bot_token_to_use = DEFAULT_BOT_TOKEN
#     bot_identifier = "Default" # Für Logging-Zwecke

#     # --- Argument Parsing ---
#     num_args = len(sys.argv)
#     valid_args = True

#     if num_args == 1:
#         # Keine Argumente: Standard-Bot, Account 1
#         print("Kein Argument angegeben. Starte mit Standard-Bot und Account 1.")
#         bot_token_to_use = DEFAULT_BOT_TOKEN
#         account_index_to_use = 0
#         bot_identifier = "Default"

#     elif num_args == 2:
#         # Ein Argument: Entweder Account-Nummer (für Default-Bot) oder Bot-Identifier 'test' (für Account 1)
#         arg1 = sys.argv[1]
#         if arg1.lower() == "test":
#             print("Argument 'test' erkannt. Starte mit Test-Bot und Account 1.")
#             bot_token_to_use = TEST_BOT_TOKEN
#             account_index_to_use = 0
#             bot_identifier = "Test"
#             if not bot_token_to_use:
#                  print("FEHLER: TEST_BOT_TOKEN nicht in config.env gefunden!")
#                  valid_args = False
#         else:
#             # Versuche, als Account-Nummer für Default-Bot zu interpretieren
#             try:
#                 requested_account_num = int(arg1)
#                 requested_index = requested_account_num - 1
#                 if 0 <= requested_index < len(ACCOUNTS):
#                     print(f"Argument '{arg1}' als Account-Nummer erkannt. Starte mit Standard-Bot und Account {requested_account_num}.")
#                     bot_token_to_use = DEFAULT_BOT_TOKEN
#                     account_index_to_use = requested_index
#                     bot_identifier = "Default"
#                 else:
#                     print(f"Warnung: Ungültige Account-Nummer '{requested_account_num}'. Verfügbar: 1-{len(ACCOUNTS)}.")
#                     valid_args = False
#             except ValueError:
#                 print(f"Warnung: Ungültiges Argument '{arg1}'. Erwartet: Account-Nummer oder 'test'.")
#                 valid_args = False

#         if not valid_args:
#             print("Verwende Standard-Bot und Account 1 als Fallback.")
#             bot_token_to_use = DEFAULT_BOT_TOKEN
#             account_index_to_use = 0
#             bot_identifier = "Default"
#             valid_args = True # Setze zurück, damit das Skript startet

#     elif num_args == 3:
#         # Zwei Argumente: Bot-Identifier und Account-Nummer
#         arg1 = sys.argv[1]
#         arg2 = sys.argv[2]

#         # Bot auswählen
#         if arg1.lower() == "test":
#             bot_token_to_use = TEST_BOT_TOKEN
#             bot_identifier = "Test"
#             if not bot_token_to_use:
#                  print("FEHLER: TEST_BOT_TOKEN nicht in config.env gefunden!")
#                  valid_args = False
#         elif arg1.lower() == "default": # Explizit Default erlauben
#              bot_token_to_use = DEFAULT_BOT_TOKEN
#              bot_identifier = "Default"
#         else:
#             print(f"Warnung: Ungültiger Bot-Identifier '{arg1}'. Erwartet: 'test' oder 'default'.")
#             valid_args = False

#         # Account auswählen
#         if valid_args: # Nur parsen, wenn Bot gültig war
#             try:
#                 requested_account_num = int(arg2)
#                 requested_index = requested_account_num - 1
#                 if 0 <= requested_index < len(ACCOUNTS):
#                     account_index_to_use = requested_index
#                 else:
#                     print(f"Warnung: Ungültige Account-Nummer '{requested_account_num}'. Verfügbar: 1-{len(ACCOUNTS)}.")
#                     valid_args = False
#             except ValueError:
#                 print(f"Warnung: Ungültige Account-Nummer '{arg2}'.")
#                 valid_args = False

#         if not valid_args:
#             print("Argumente ungültig. Starte mit Standard-Bot und Account 1 als Fallback.")
#             bot_token_to_use = DEFAULT_BOT_TOKEN
#             account_index_to_use = 0
#             bot_identifier = "Default"
#             valid_args = True # Setze zurück

#     else: # Zu viele Argumente
#         print("Warnung: Zu viele Argumente angegeben.")
#         print("Verwende Standard-Bot und Account 1 als Fallback.")
#         bot_token_to_use = DEFAULT_BOT_TOKEN
#         account_index_to_use = 0
#         bot_identifier = "Default"

#     # --- Finale Prüfung und Globals setzen ---
#     if not bot_token_to_use:
#         print(f"FEHLER: Kein gültiger Bot-Token ausgewählt (Standard oder Test). Bitte config.env prüfen.")
#         sys.exit(1)

#     if not (0 <= account_index_to_use < len(ACCOUNTS)):
#          print(f"FEHLER: Interner Fehler - Account-Index {account_index_to_use} ungültig.")
#          sys.exit(1)

#     # Setze die globalen Variablen, die von run() verwendet werden
#     ACTIVE_BOT_TOKEN = bot_token_to_use
#     current_account = account_index_to_use # Diese globale Variable wird bereits verwendet

#     # --- Skript starten ---
#     print(f"\n---> Starte Skript mit Bot: '{bot_identifier}', Account: {account_index_to_use + 1} <---\n")
#     try:
#         asyncio.run(run())
#     except KeyboardInterrupt:
#         print("Bot wird wegen KeyboardInterrupt gestoppt...")
#     except RuntimeError as e:
#         if "This event loop is already running" in str(e):
#             print("Fehler: Event-Loop-Konflikt. Bitte das Skript neu starten.")
#         else:
#              print(f"Unerwarteter Laufzeitfehler: {e}")
#     except Exception as e:
#         print(f"Unerwarteter Fehler im Hauptprogramm: {e}")