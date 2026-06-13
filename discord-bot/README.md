MEM Store Bot — New Version
Discord bot + web dashboard for MEM Store server.

Setup
Copy .env.example to .env and fill in your values
Install dependencies: pip install -r requirements.txt
Run both processes (or use Procfile with gunicorn):
python bot.py               # Discord bot + WebSocket server
python dashboard/app.py     # Web dashboard (port 5001)

Environment Variables
See .env.example for the full list.

Required:

DISCORD_TOKEN — Bot token from Discord Developer Portal
MONGODB_URI — MongoDB Atlas connection string (mongodb+srv://...)
GUILD_ID — Your Discord server ID
DASHBOARD_PASSWORD — Login password for the dashboard
Features
Discord Bot
Ticket System — Sell / Buy / Partner tickets with open, close, claim
Seller Ratings — 👍 / 👎 after ticket closes, updates leaderboard
Welcome System — Auto-welcome on join, goodbye on leave
AutoMod — Bad words, anti-spam, anti-links, anti-mention
Self Roles — Language (English/Arabic) + Game roles via buttons
Moderation — /warn, /ban, /kick, /timeout, /unwarn, /warnings, /blacklist
Voice Relay — /join <channel> to join any voice channel + browser PTT
Embeds — /embed to send custom embeds
Orders — /order to log completed transactions
Feedback — /feedback to rate sellers
Voice Relay (NEW)
/join <channel> — choose any voice channel from a dropdown (no need to be in voice!)
/leave — disconnect the bot
/voice_status — check current voice session
Browser Push-to-Talk via dashboard
Full call history saved to MongoDB
Dashboard
Home — Live stats overview
Analytics — Log breakdown, ticket types, top sellers
Tickets — Manage all tickets, configure embed
Marketplace — Seller leaderboard + orders + feedback
Moderation — Warnings, bans, blacklist management
Logs — Filterable server activity logs (incl. voice type)
Roles — Add/remove language and game roles
Welcome — Customize welcome/leave messages
Security — Toggle anti-spam, anti-links, bad words list
Embeds — Build and send Discord embeds
Settings — Channel IDs, toggle systems
Voice Relay — PTT interface + live connection info + call history
MongoDB Collections
tickets — All ticket records
leaderboard — Seller ratings
warnings — User warnings
logs — All server events
members — Member join records
blacklist — Blacklisted users
voice_calls — Voice session history
config — Bot configuration
saved_embeds — Saved embed templates
