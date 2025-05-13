# raw-bot-X

A Telegram-based account and post manager for X, primarily developed for monitoring and interacting with memecoin-related content. Built entirely using Claude 3.7 Sonnet and Gemini 2.5 Pro Preview (05-06).

> ⚠️ **WARNING:** Use this bot at your own risk. Some actions could violate X's Terms of Service and could result in account bans. Always test with new or disposable accounts.

---

## ⚙️ Features

### 🔐 Login
- Automatic login using email/password/username and cookies.(`config.env`)
- Authentication codes can be sent via Telegram.
- Automatic account switch on failure (not fully tested).
- Supports long-term usage with automated Selenium driver restarts.

### 🔍 Post Scanning
Scans your timeline for:
- Custom keywords
- Contract addresses (Solana, BNB)
- Ticker symbols

**Scan modes** (combinable):
- Ticker
- Contract adress
- Keywords

Sends results to your Telegram group with:
- Post imagen (only 1)
- Post time & time since posting
- Direct link to the post
- Post text
- Ticker & CA copy buttons
- Quick links: BullX, Rugcheck, Dexscreener, Pumpfun, Solscan
- Basic Account ratings (only with CA) (top 3 shown)
- Like & repost buttons
- Scan trigger info (e.g., triggered keywords/CA/ticker)

### ➕ Add Followers
- Add via Telegram commands: `/follow [username]`, `/unfollow [username]`
- Restore from backups
- Sync from other accounts (global follow list)
- Scan follow list from another account (only ~50 Accounts showing): `/scrapefollowing [username]`
- Stores scraped users (with follower count, bio) in Database
- Add from Database to followlist using filters:
  - `/addfromdb followers:5000 seen:2 keywords:web3,CEO,facebook` (seen=how often this account has been seen in scanned accounts)
  - `/addfromdb followers:30k keywords:meme`
  - `/addfromdb followers:1M`

### 🤖 Auto Follow
- Slow mode: follows users at intervals (adjustable)
- Fast mode: immediate list execution (fast means not "fast", it's very slow ~2-3 acc/min but in future will give the opportunity to do it over night like scheudle. Faster would just look like suspicious')
- Toggle modes on/off

### ⏱️ Schedule
- Pause/resume bot at defined times
- `/schedule 22:00-12:00`

### 📊 Statistics
- Uptime
- Posts found (today & total)
- Posts scanned (today & total)
- Total ads found (not working now)
- Average posts by weekday

### 💻 Headless Mode
- Runs browser in background (Selenium), recommend for slow PI
- Some features disabled in this mode
- Toggle on/off

### 🧠 Keywords
- Add single or comma-separated list via:
  - `/addkeyword keyword1, keyword2, keyword3`

---

## 🚀 Installation

### 1. Choose a target folder and navigate into it
```bash
mkdir -p ~/bots  # or any path you prefer
cd ~/bots
```
### 2. Clone the repository
```bash
git clone https://github.com/rawBotX/raw-bot-X.git
cd raw-bot-X
```

### 3. Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### `requirements.txt` includes:
- nest-asyncio  
- requests  
- pytz  
- beautifulsoup4  
- selenium  
- python-telegram-bot  
- python-dotenv  

---

## 🛠️ Configuration

### Create a `config.env` in the root directory:
```ini
# Bot Configuration
BOT_TOKEN=YourBotToken
BOT_USERNAME=YourBotUsername
CHANNEL_ID=YourTelegramChannelID

# Admin (mandatory for bot interaction)
ADMIN_USER_ID=YourAdminUserID

# Account 1
ACCOUNT_1_EMAIL=web3@mail.net
ACCOUNT_1_PASSWORD=yourPassword1
ACCOUNT_1_USERNAME=account1username
ACCOUNT_1_COOKIES=x_0.cookies.json

# Account 2
ACCOUNT_2_EMAIL=trench@mail.net
ACCOUNT_2_PASSWORD=yourPassword2
ACCOUNT_2_USERNAME=account2username
ACCOUNT_2_COOKIES=x_1.cookies.json

# Add more accounts as needed...
```

### How to get Telegram credentials:
- `BOT_TOKEN` & `BOT_USERNAME`: [@BotFather](https://t.me/botfather) → `/newbot`
- `CHANNEL_ID`: Add bot to group/channel → send message → use [@getidsbot](https://t.me/getidsbot)
- `ADMIN_USER_ID`: Send `/start` to your bot → use [@userinfobot](https://t.me/userinfobot)

---

### Optional: Telegram Bot Commands (via BotFather)
If you want your bot to offer built-in command suggestions in Telegram (autocomplete), you can set them via @BotFather:

Open BotFather

Select your bot → Edit Bot → Edit Commands

Paste the following command list:
```bash
help - Show help menu and command list
ping - Check if the bot is responding
stats - Show post statistics (alias: /count)
status - Show current operational status
account - Show active account info
keywords - Show current keywords
mode - Show current search mode
schedule - Show current schedule status
rates - Show collected source ratings
globallistinfo - Show status of the global follower list
autofollowstatus - Show auto-follow status for current account

pause - Pause post searching
resume - Resume post searching
modefull - Set search mode to CA + Keywords
modeca - Set search mode to CA Only
searchtickers - Toggle searching for $Tickers ON/OFF
setmaxage - Set max post age (e.g., /setmaxage 30)
toggleheadless - Toggle headless mode ON/OFF (requires restart)

scheduleon - Activate the schedule
scheduleoff - Deactivate the schedule
scheduletime - Set schedule pause (e.g., /scheduletime 22:00-08:00)

addkeyword - Add keywords (e.g., /addkeyword btc,eth)
removekeyword - Remove keywords (e.g., /removekeyword sol)

follow - Follow a user (e.g., /follow elonmusk)
unfollow - Unfollow a user (e.g., /unfollow vitalikbuterin)
addusers - Add users to list (e.g., /addusers user1 @user2)
autofollowmode - Set auto-follow (e.g., /autofollowmode slow)
autofollowinterval - Set slow auto-follow interval (e.g., /autofollowinterval 10-20)
cancelfastfollow - Cancel running Fast-Follow task
clearfollowlist - Clear current account's follow list

like - Like a post by URL (e.g., /like <tweet_url>)
repost - Repost a post by URL (e.g., /repost <tweet_url>)

switchaccount - Switch X account (e.g., /switchaccount 2)

backupfollowers - Backup current account's 'following' list
syncfollows - Sync current account's 'following' with global list
buildglobalfrombackups - Update global list from all account backups
initglobalfrombackup - Overwrite global list from one account's backup (e.g., /initglobalfrombackup 1)
cancelbackup - Cancel ongoing follower backup
cancelsync - Cancel ongoing follower sync

scrapefollowing - Scan 'following' of a user (e.g., /scrapefollowing someuser)
addfromdb - Add from DB (e.g., /addfromdb f:10k s:2 k:dev)
canceldbscrape - Cancel ongoing database scrape

addadmin - Add bot admin (e.g., /addadmin 123456789)
removeadmin - Remove bot admin (e.g., /removeadmin 987654321)
listadmins - List current bot admins
```

## 🔐 File Permissions (Linux)
Run this after you’ve started the bot for the first time.
Make sure you're in the bot’s project directory (raw-bot-X/):
```bash
chmod 600 config.env
chmod 600 admins.json keywords.json settings.json ratings.json schedule.json posts_count.json following_database.json
chmod 600 *.cookies.json
chmod 600 *.txt global_followed_users.txt
```

---

## 🧪 Supported Platforms

- Raspberry Pi (tested on Pi 4, 2GB, Pi OS Lite + LXQt, headless mode)
- Linux (Debian/Ubuntu)

---

## 📦 Version

Current version: **v0.1.1**

See all releases and changelogs here: [Releases](https://github.com/rawBotX/raw-bot-X/releases)

---

## 🪪 License

This project is open source under the MIT License:

```text
MIT License

Copyright (c) 2025 rawBotX

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
...
```

Full license text is in [LICENSE](LICENSE)

---

## 📚 Used Libraries & Licenses

This project uses the following open-source Python libraries:

| Library                | License Type             | License URL                                                                 |
|------------------------|--------------------------|-----------------------------------------------------------------------------|
| selenium               | Apache License 2.0       | https://www.apache.org/licenses/LICENSE-2.0                                 |
| python-telegram-bot    | LGPLv3                   | https://www.gnu.org/licenses/lgpl-3.0.html                                  |
| python-dotenv          | BSD License              | https://opensource.org/licenses/BSD-3-Clause                                |
| nest-asyncio           | MIT License              | https://opensource.org/licenses/MIT                                         |
| requests               | Apache License 2.0       | https://www.apache.org/licenses/LICENSE-2.0                                 |
| pytz                   | MIT License              | https://opensource.org/licenses/MIT                                         |
| beautifulsoup4         | MIT License              | https://opensource.org/licenses/MIT                                         |

**Note on LGPLv3:**  
The library `python-telegram-bot` is used as-is and not modified.  
LGPLv3 allows usage in open or closed source projects as long as the library remains replaceable and dynamically linked.  
For details: https://www.gnu.org/licenses/lgpl-3.0.html

---

**Disclaimer:**  
This project was generated entirely with the help of AI tools (Claude 3.7 Sonnet & Gemini 2.5 Pro).  
It is provided "as-is" under the MIT license. Users may modify and distribute it at their own risk.


## 🤝 Contributing

Pull requests are welcome. Please open an issue first to discuss changes.

---

## 📫 Contact

For questions, suggestions or issues, open a GitHub Issue or contact via Telegram bot channel.


