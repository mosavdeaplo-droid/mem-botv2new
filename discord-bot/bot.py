import discord
from discord.ext import commands
from discord.ui import View, Button
from discord import Option, PermissionOverwrite
from pymongo import MongoClient
from datetime import datetime, timezone
from collections import defaultdict
import asyncio
import os
import io
import re
import aiohttp
import queue
import json
import websockets
import websockets.exceptions

# ═══════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════
GIF_URL  = os.getenv("GIF_URL", "")
LOGO_URL = os.getenv("LOGO_URL", "")

EMBED_COLOR  = 0x1a2332
FOOTER_TEXT  = "Powered by MEM Development | Deaplo"
SERVER_NAME  = "MEM Store"
GUILD_ID     = int(os.getenv("GUILD_ID", "1504256091872301116"))

# ── Channels ──
TICKET_CHANNEL_ID      = int(os.getenv("TICKET_CHANNEL_ID",      "1505921799978881105"))
ORDER_CHANNEL_ID       = int(os.getenv("ORDER_CHANNEL_ID",       "1504265744719020122"))
WELCOME_CHANNEL_ID     = int(os.getenv("WELCOME_CHANNEL_ID",     "1505922477753368740"))
LOG_CHANNEL_ID         = int(os.getenv("LOG_CHANNEL_ID",         "1505308788780040222"))
LEADERBOARD_CHANNEL_ID = int(os.getenv("LEADERBOARD_CHANNEL_ID", "1505922561903562812"))
LINKS_ALLOWED_CHANNEL  = int(os.getenv("LINKS_ALLOWED_CHANNEL",  "1504271389656486049"))
SELF_ROLES_CHANNEL_ID  = int(os.getenv("SELF_ROLES_CHANNEL_ID",  "1506220242626543697"))
FEEDBACK_CHANNEL_ID    = int(os.getenv("FEEDBACK_CHANNEL_ID",    "1506926493798764635"))

# ── Categories ──
TICKET_CATEGORY_ID = int(os.getenv("TICKET_CATEGORY_ID", "1505922835359596644"))

# ── Roles ──
STAFF_ROLE_ID    = int(os.getenv("STAFF_ROLE_ID",    "1504374917360128040"))
MEMBER_ROLE_ID   = int(os.getenv("MEMBER_ROLE_ID",   "1504383155921092808"))
SECURITY_ROLE_ID = int(os.getenv("SECURITY_ROLE_ID", "1505133078111191142"))
ARC_ROLE_ID      = int(os.getenv("ARC_ROLE_ID",      "1506219518567911566"))

LANGUAGE_ROLES = {
    "English": int(os.getenv("ROLE_ENGLISH", "1506219132037763092")),
    "Arabic":  int(os.getenv("ROLE_ARABIC",  "1506219366939885669")),
}
GAME_ROLES = {
    "ARC Raiders": int(os.getenv("ROLE_ARC",       "1506219518567911566")),
    "PUBG Mobile": int(os.getenv("ROLE_PUBG_MOB",  "1506219627246649455")),
    "PUBG Steam":  int(os.getenv("ROLE_PUBG_STEAM","1506219763171463209")),
}

# ── Bad Words Filter ──
BAD_WORDS = [
    "fuck", "shit", "bitch", "asshole", "bastard", "cunt", "damn", "dick",
    "pussy", "nigga", "nigger", "faggot", "retard", "whore", "slut",
    "كس", "زب", "طيز", "منيوك", "شرموط", "عرص", "خول", "متناك",
    "كلب", "حمار", "زنيك", "نيك", "ابن الشرموطة", "ابن الكلب",
    "يلعن", "العن", "لعنة", "قحبة", "وسخ",
]

# ── Anti Spam ──
spam_tracker = defaultdict(list)
SPAM_LIMIT   = 5
SPAM_WINDOW  = 5

# ── WebSocket Port ──
WS_PORT = int(os.getenv("WS_PORT", "8765"))

# ─────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────
mongo_client = MongoClient(os.getenv("MONGODB_URI"))
db = mongo_client["mem_store"]

# ─────────────────────────────────────────
#  VOICE RELAY
# ─────────────────────────────────────────
audio_queue: queue.Queue = queue.Queue(maxsize=500)
ws_clients: set = set()
voice_session: dict = {}   # {guild_id: {"channel": name, "channel_id": id, "start": iso_str}}


class MicAudioSource(discord.AudioSource):
    FRAME_SIZE = 3840  # 20ms @ 48kHz stereo 16-bit

    def __init__(self):
        self.buffer = b''
        self.silence = b'\x00' * self.FRAME_SIZE

    def read(self) -> bytes:
        while len(self.buffer) < self.FRAME_SIZE:
            try:
                self.buffer += audio_queue.get_nowait()
            except queue.Empty:
                return self.silence
        frame = self.buffer[:self.FRAME_SIZE]
        self.buffer = self.buffer[self.FRAME_SIZE:]
        return frame

    def is_opus(self) -> bool:
        return False


async def _ws_broadcast(data: dict):
    if ws_clients:
        msg = json.dumps(data)
        await asyncio.gather(*[c.send(msg) for c in list(ws_clients)],
                             return_exceptions=True)


async def ws_handler(websocket):
    ws_clients.add(websocket)
    if bot.is_ready():
        await websocket.send(json.dumps({"type": "bot_ready", "user": str(bot.user)}))
    for guild in bot.guilds:
        if guild.voice_client and guild.voice_client.is_connected():
            sess = voice_session.get(guild.id, {})
            await websocket.send(json.dumps({
                "type": "joined",
                "channel": guild.voice_client.channel.name,
                "channel_id": guild.voice_client.channel.id,
                "start": sess.get("start", ""),
                "members": [m.name for m in guild.voice_client.channel.members if not m.bot]
            }))
            break
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                try:
                    audio_queue.put_nowait(message)
                except queue.Full:
                    try:
                        audio_queue.get_nowait()
                        audio_queue.put_nowait(message)
                    except queue.Empty:
                        pass
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        ws_clients.discard(websocket)

# ─────────────────────────────────────────
#  BOT SETUP
# ─────────────────────────────────────────
intents = discord.Intents.all()
bot = discord.Bot(intents=intents)

# ─────────────────────────────────────────
#  HELPER: SEND LOG
# ─────────────────────────────────────────
async def send_log(guild, title: str, description: str, color: int = EMBED_COLOR, fields: list = None):
    channel = guild.get_channel(LOG_CHANNEL_ID)
    if not channel:
        return
    embed = discord.Embed(
        title=title,
        description=description,
        color=color,
        timestamp=datetime.now(timezone.utc)
    )
    if fields:
        for name, value, inline in fields:
            embed.add_field(name=name, value=value, inline=inline)
    embed.set_footer(text=FOOTER_TEXT)
    await channel.send(embed=embed)
    try:
        log_entry = {
            "guild_id":    guild.id,
            "title":       title,
            "description": description,
            "fields":      [{"name": f[0], "value": f[1]} for f in (fields or [])],
            "timestamp":   datetime.now(timezone.utc),
            "color":       color,
        }
        if any(x in title for x in ["Ban", "Kick", "Warn", "Timeout"]):
            log_entry["type"] = "moderation"
        elif "Ticket" in title:
            log_entry["type"] = "ticket"
        elif any(x in title for x in ["Member", "Join", "Left"]):
            log_entry["type"] = "member"
        elif any(x in title for x in ["Message", "Edit", "Delete"]):
            log_entry["type"] = "message"
        elif "Order" in title:
            log_entry["type"] = "order"
        elif any(x in title for x in ["Spam", "Link", "Bad Word"]):
            log_entry["type"] = "automod"
        elif any(x in title for x in ["Voice", "Joined Voice", "Left Voice"]):
            log_entry["type"] = "voice"
        else:
            log_entry["type"] = "general"
        db["logs"].insert_one(log_entry)
    except Exception:
        pass

# ─────────────────────────────────────────
#  HELPER: SECURITY CHECK
# ─────────────────────────────────────────
def has_security_role():
    async def predicate(ctx):
        role = ctx.guild.get_role(SECURITY_ROLE_ID)
        if role not in ctx.author.roles:
            await ctx.respond("❌ You don't have permission to use this command.", ephemeral=True)
            return False
        return True
    return commands.check(predicate)

# ─────────────────────────────────────────
#  HELPER: DISCORD API CALL
# ─────────────────────────────────────────
async def discord_api(method: str, endpoint: str, data: dict = None):
    url     = f"https://discord.com/api/v10{endpoint}"
    headers = {
        "Authorization": f"Bot {os.getenv('DISCORD_TOKEN')}",
        "Content-Type":  "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, json=data, headers=headers) as r:
            if r.status in (200, 201, 204):
                try:
                    return await r.json()
                except Exception:
                    return {}
            return None

# ═══════════════════════════════════════════════════════════════
#  ██  TICKETS SYSTEM
# ═══════════════════════════════════════════════════════════════

class TicketView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Sell", emoji="💰", style=discord.ButtonStyle.secondary, custom_id="ticket_sell")
    async def sell_button(self, button: Button, interaction: discord.Interaction):
        await open_ticket(interaction, ticket_type="Sell")

    @discord.ui.button(label="Buy", emoji="🛒", style=discord.ButtonStyle.success, custom_id="ticket_buy")
    async def buy_button(self, button: Button, interaction: discord.Interaction):
        await open_ticket(interaction, ticket_type="Buy")

    @discord.ui.button(label="Partner", emoji="🤝", style=discord.ButtonStyle.danger, custom_id="ticket_partner")
    async def partner_button(self, button: Button, interaction: discord.Interaction):
        await open_ticket(interaction, ticket_type="Partner")


class TicketControlView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Close Ticket", emoji="🔒", style=discord.ButtonStyle.danger, custom_id="ticket_close")
    async def close_button(self, button: Button, interaction: discord.Interaction):
        await close_ticket(interaction)

    @discord.ui.button(label="Claim", emoji="✋", style=discord.ButtonStyle.primary, custom_id="ticket_claim")
    async def claim_button(self, button: Button, interaction: discord.Interaction):
        await claim_ticket(interaction)


class TicketRatingView(View):
    def __init__(self, seller_id: int):
        super().__init__(timeout=120)
        self.seller_id = seller_id

    @discord.ui.button(label="👍 Like", style=discord.ButtonStyle.success, custom_id="rating_like")
    async def like_button(self, button: Button, interaction: discord.Interaction):
        await submit_rating(interaction, self.seller_id, is_positive=True)
        self.stop()

    @discord.ui.button(label="👎 Dislike", style=discord.ButtonStyle.danger, custom_id="rating_dislike")
    async def dislike_button(self, button: Button, interaction: discord.Interaction):
        await submit_rating(interaction, self.seller_id, is_positive=False)
        self.stop()


async def open_ticket(interaction: discord.Interaction, ticket_type: str):
    guild  = interaction.guild
    member = interaction.user
    existing = discord.utils.get(
        guild.text_channels,
        name=f"ticket-{member.name.lower().replace(' ', '-')}"
    )
    if existing:
        await interaction.response.send_message(
            f"❌ You already have an open ticket: {existing.mention}", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    category   = discord.utils.get(guild.categories, id=TICKET_CATEGORY_ID)
    staff_role = guild.get_role(STAFF_ROLE_ID)
    overwrites = {
        guild.default_role: PermissionOverwrite(view_channel=False, send_messages=False),
        member:             PermissionOverwrite(view_channel=True, send_messages=True),
    }
    if staff_role:
        overwrites[staff_role] = PermissionOverwrite(view_channel=True, send_messages=True)
    channel = await guild.create_text_channel(
        name=f"ticket-{member.name.lower().replace(' ', '-')}",
        category=category,
        overwrites=overwrites,
    )
    ticket_data = {
        "guild_id":  guild.id,
        "channel_id":channel.id,
        "user_id":   member.id,
        "type":      ticket_type,
        "status":    "open",
        "opened_at": datetime.now(timezone.utc),
        "seller_id": None,
    }
    db["tickets"].insert_one(ticket_data)
    color_map = {"Sell": 0xF1C40F, "Buy": 0x2ECC71, "Partner": 0x5865F2}
    embed = discord.Embed(
        title=f"{'💰' if ticket_type=='Sell' else '🛒' if ticket_type=='Buy' else '🤝'} {ticket_type} Ticket",
        description=(
            f"Welcome {member.mention}!\n\n"
            f"**Type:** {ticket_type}\n"
            f"**Opened:** <t:{int(datetime.now(timezone.utc).timestamp())}:R>\n\n"
            "Please describe your request and wait for a staff member."
        ),
        color=color_map.get(ticket_type, EMBED_COLOR),
    )
    embed.set_footer(text=FOOTER_TEXT)
    if LOGO_URL:
        embed.set_thumbnail(url=LOGO_URL)
    await channel.send(embed=embed, view=TicketControlView())
    await interaction.followup.send(f"✅ Ticket opened: {channel.mention}", ephemeral=True)
    await send_log(guild, f"🎫 Ticket Opened — {ticket_type}",
                   f"**User:** {member.mention}\n**Channel:** {channel.mention}\n**Type:** {ticket_type}",
                   color=color_map.get(ticket_type, EMBED_COLOR))


async def close_ticket(interaction: discord.Interaction):
    channel = interaction.channel
    guild   = interaction.guild
    ticket  = db["tickets"].find_one({"channel_id": channel.id, "status": "open"})
    if not ticket:
        await interaction.response.send_message("❌ This is not an open ticket.", ephemeral=True)
        return
    await interaction.response.defer()
    seller_id = ticket.get("seller_id")
    user_id   = ticket.get("user_id")
    db["tickets"].update_one({"channel_id": channel.id}, {"$set": {"status": "closed", "closed_at": datetime.now(timezone.utc)}})
    await send_log(guild, "🔒 Ticket Closed",
                   f"**Channel:** {channel.name}\n**Type:** {ticket.get('type','N/A')}",
                   color=0xE74C3C)
    if seller_id and user_id:
        user = guild.get_member(user_id)
        if user:
            try:
                rating_embed = discord.Embed(
                    title="⭐ Rate Your Experience",
                    description=f"Please rate your transaction in **{channel.name}**.",
                    color=EMBED_COLOR,
                )
                rating_embed.set_footer(text=FOOTER_TEXT)
                await user.send(embed=rating_embed, view=TicketRatingView(seller_id))
            except Exception:
                pass
    await channel.delete()


async def claim_ticket(interaction: discord.Interaction):
    channel = interaction.channel
    guild   = interaction.guild
    member  = interaction.user
    staff_role = guild.get_role(STAFF_ROLE_ID)
    if not staff_role or staff_role not in member.roles:
        await interaction.response.send_message("❌ Only staff can claim tickets.", ephemeral=True)
        return
    ticket = db["tickets"].find_one({"channel_id": channel.id})
    if not ticket:
        await interaction.response.send_message("❌ Ticket not found.", ephemeral=True)
        return
    db["tickets"].update_one({"channel_id": channel.id}, {"$set": {"seller_id": member.id}})
    await interaction.response.send_message(f"✅ Ticket claimed by {member.mention}!")
    await send_log(guild, "✋ Ticket Claimed",
                   f"**Channel:** {channel.name}\n**Claimed by:** {member.mention}")


async def submit_rating(interaction: discord.Interaction, seller_id: int, is_positive: bool):
    db["leaderboard"].update_one(
        {"seller_id": seller_id},
        {"$inc": {"total": 1, "positive": 1 if is_positive else 0}},
        upsert=True,
    )
    emoji = "👍" if is_positive else "👎"
    await interaction.response.send_message(
        f"{emoji} Rating submitted!", ephemeral=True
    )
    guild = interaction.guild
    if guild:
        seller = guild.get_member(seller_id)
        seller_name = seller.mention if seller else f"<@{seller_id}>"
        await send_log(
            guild,
            f"⭐ Rating — {'Positive' if is_positive else 'Negative'}",
            f"**Seller:** {seller_name}\n**By:** {interaction.user.mention}\n**Type:** {'👍 Positive' if is_positive else '👎 Negative'}",
            color=0x2ECC71 if is_positive else 0xE74C3C,
        )
    # update leaderboard embed
    await update_leaderboard(guild)


async def update_leaderboard(guild):
    if not guild:
        return
    channel = guild.get_channel(LEADERBOARD_CHANNEL_ID)
    if not channel:
        return
    sellers = list(db["leaderboard"].find().sort("positive", -1).limit(10))
    desc = ""
    medals = ["🥇", "🥈", "🥉"]
    for i, s in enumerate(sellers):
        total    = s.get("total", 0)
        positive = s.get("positive", 0)
        ratio    = round((positive / total) * 100) if total > 0 else 0
        medal    = medals[i] if i < 3 else f"**#{i+1}**"
        member   = guild.get_member(s["seller_id"])
        name     = member.mention if member else f"<@{s['seller_id']}>"
        desc    += f"{medal} {name} — 👍 {positive}/{total} ({ratio}%)\n"
    embed = discord.Embed(
        title="🏆 Seller Leaderboard",
        description=desc or "No data yet.",
        color=0xF1C40F,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=FOOTER_TEXT)
    saved = db["config"].find_one({"key": "leaderboard_message"})
    if saved:
        try:
            msg = await channel.fetch_message(saved["message_id"])
            await msg.edit(embed=embed)
            return
        except Exception:
            pass
    msg = await channel.send(embed=embed)
    db["config"].update_one(
        {"key": "leaderboard_message"},
        {"$set": {"message_id": msg.id}},
        upsert=True,
    )

# ═══════════════════════════════════════════════════════════════
#  ██  SELF ROLES SYSTEM
# ═══════════════════════════════════════════════════════════════

class LanguageRoleView(View):
    def __init__(self):
        super().__init__(timeout=None)
        for label, role_id in LANGUAGE_ROLES.items():
            self.add_item(LanguageRoleButton(label=label, role_id=role_id))


class LanguageRoleButton(Button):
    def __init__(self, label: str, role_id: int):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, custom_id=f"lang_{role_id}")
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        member = interaction.user
        guild  = interaction.guild
        role   = guild.get_role(self.role_id)
        if not role:
            await interaction.response.send_message("❌ Role not found.", ephemeral=True)
            return
        if role in member.roles:
            await member.remove_roles(role)
            await interaction.response.send_message(f"✅ Removed **{role.name}**", ephemeral=True)
        else:
            await member.add_roles(role)
            await interaction.response.send_message(f"✅ Added **{role.name}**", ephemeral=True)


class GameRoleView(View):
    def __init__(self):
        super().__init__(timeout=None)
        styles = [discord.ButtonStyle.primary, discord.ButtonStyle.success, discord.ButtonStyle.secondary]
        for i, (label, role_id) in enumerate(GAME_ROLES.items()):
            self.add_item(GameRoleButton(label=label, role_id=role_id, style=styles[i % len(styles)]))


class GameRoleButton(Button):
    def __init__(self, label: str, role_id: int, style):
        super().__init__(label=label, style=style, custom_id=f"game_{role_id}")
        self.role_id = role_id

    async def callback(self, interaction: discord.Interaction):
        member = interaction.user
        guild  = interaction.guild
        role   = guild.get_role(self.role_id)
        if not role:
            await interaction.response.send_message("❌ Role not found.", ephemeral=True)
            return
        if role in member.roles:
            await member.remove_roles(role)
            await interaction.response.send_message(f"✅ Removed **{role.name}**", ephemeral=True)
        else:
            await member.add_roles(role)
            await interaction.response.send_message(f"✅ Added **{role.name}**", ephemeral=True)

# ═══════════════════════════════════════════════════════════════
#  ██  EVENTS
# ═══════════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} ({bot.user.id})")
    bot.add_view(TicketView())
    bot.add_view(TicketControlView())
    bot.add_view(LanguageRoleView())
    bot.add_view(GameRoleView())
    asyncio.ensure_future(start_ws_server())


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    cfg   = db["config"].find_one({"key": "welcome_config"}) or {}
    # Auto-assign member role
    role_id = cfg.get("member_role") or MEMBER_ROLE_ID
    role = guild.get_role(int(role_id)) if role_id else None
    if role:
        try:
            await member.add_roles(role)
        except Exception:
            pass
    # Welcome message
    ch_id = cfg.get("welcome_channel") or WELCOME_CHANNEL_ID
    channel = guild.get_channel(int(ch_id)) if ch_id else None
    if channel:
        msg = cfg.get("message", "We hope you have a great time.")
        count = guild.member_count
        embed = discord.Embed(
            title=f"👋 Welcome to {guild.name}!",
            description=(
                f"{member.mention} | <t:{int(datetime.now(timezone.utc).timestamp())}:R>\n\n"
                f"• Welcome **{member.name}**\n"
                f"• Our family now consists of **{count} Members**\n"
                f"• {msg}"
            ),
            color=0x2ECC71,
            timestamp=datetime.now(timezone.utc),
        )
        if LOGO_URL:
            embed.set_thumbnail(url=LOGO_URL)
        embed.set_footer(text=FOOTER_TEXT)
        await channel.send(embed=embed)
    await send_log(guild, "👋 Member Joined",
                   f"**User:** {member.mention}\n**Account:** <t:{int(member.created_at.timestamp())}:R>",
                   color=0x2ECC71)
    db["members"].update_one({"user_id": member.id}, {"$set": {"user_id": member.id, "joined_at": datetime.now(timezone.utc)}}, upsert=True)


@bot.event
async def on_member_remove(member: discord.Member):
    guild = member.guild
    cfg   = db["config"].find_one({"key": "welcome_config"}) or {}
    ch_id = cfg.get("welcome_channel") or WELCOME_CHANNEL_ID
    channel = guild.get_channel(int(ch_id)) if ch_id else None
    if channel:
        leave_msg = cfg.get("leave_message", "Goodbye!")
        embed = discord.Embed(
            title=f"👋 {member.name} Left",
            description=leave_msg,
            color=0xE74C3C,
            timestamp=datetime.now(timezone.utc),
        )
        embed.set_footer(text=FOOTER_TEXT)
        await channel.send(embed=embed)
    await send_log(guild, "🚪 Member Left",
                   f"**User:** {member.name}\n**ID:** {member.id}",
                   color=0xE74C3C)


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    guild  = message.guild
    author = message.author
    content = message.content.lower()

    # ── Bad words ──
    bw_cfg = db["config"].find_one({"key": "bad_words"})
    bad_list = bw_cfg.get("words", BAD_WORDS) if bw_cfg else BAD_WORDS
    for word in bad_list:
        if word.lower() in content:
            try:
                await message.delete()
            except Exception:
                pass
            await message.channel.send(f"⚠️ {author.mention} Watch your language!", delete_after=5)
            await send_log(guild, "🤬 Bad Word Detected",
                           f"**User:** {author.mention}\n**Word:** ||{word}||\n**Channel:** {message.channel.mention}",
                           color=0xE74C3C)
            return

    # ── Anti-Spam ──
    sec_cfg = db["config"].find_one({"key": "security_config"}) or {}
    if sec_cfg.get("anti_spam", True):
        spam_limit  = sec_cfg.get("spam_limit", SPAM_LIMIT)
        spam_window = sec_cfg.get("spam_window", SPAM_WINDOW)
        now = datetime.now(timezone.utc).timestamp()
        tracker = spam_tracker[author.id]
        tracker.append(now)
        spam_tracker[author.id] = [t for t in tracker if now - t < spam_window]
        if len(spam_tracker[author.id]) >= spam_limit:
            spam_tracker[author.id] = []
            try:
                await author.timeout(discord.utils.utcnow().__class__.now(timezone.utc).replace(
                    second=0, microsecond=0), reason="Spam detected")
            except Exception:
                pass
            await message.channel.send(f"⚠️ {author.mention} has been timed out for spamming!", delete_after=5)
            await send_log(guild, "🔇 Anti-Spam Triggered",
                           f"**User:** {author.mention}\n**Channel:** {message.channel.mention}",
                           color=0xE74C3C)
            return

    # ── Anti-Links ──
    if sec_cfg.get("anti_links", True):
        if message.channel.id != LINKS_ALLOWED_CHANNEL:
            url_pattern = re.compile(r"https?://|discord\.gg/|\.com|\.net|\.org|\.gg", re.IGNORECASE)
            if url_pattern.search(message.content):
                try:
                    await message.delete()
                except Exception:
                    pass
                await message.channel.send(f"🔗 {author.mention} Links are not allowed here!", delete_after=5)
                await send_log(guild, "🔗 Link Blocked",
                               f"**User:** {author.mention}\n**Channel:** {message.channel.mention}",
                               color=0xE67E22)
                return

    # ── Anti-Mention Spam ──
    mention_limit = sec_cfg.get("mention_limit", 5)
    if sec_cfg.get("anti_mention", True) and len(message.mentions) >= mention_limit:
        try:
            await message.delete()
        except Exception:
            pass
        await message.channel.send(f"⚠️ {author.mention} Too many mentions!", delete_after=5)
        await send_log(guild, "📢 Mention Spam Blocked",
                       f"**User:** {author.mention}\n**Mentions:** {len(message.mentions)}",
                       color=0xE74C3C)
        return


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot:
        return
    if before.content == after.content:
        return
    await send_log(before.guild, "✏️ Message Edited",
                   f"**User:** {before.author.mention}\n**Channel:** {before.channel.mention}",
                   color=0x3498DB,
                   fields=[
                       ("Before", before.content[:512] or "(empty)", False),
                       ("After", after.content[:512] or "(empty)", False),
                   ])


@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot:
        return
    await send_log(message.guild, "🗑️ Message Deleted",
                   f"**User:** {message.author.mention}\n**Channel:** {message.channel.mention}\n**Content:** {message.content[:512] or '(empty)'}",
                   color=0xE74C3C)


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
    if member.bot:
        return
    guild = member.guild
    if before.channel is None and after.channel is not None:
        await send_log(guild, "🔊 Member Joined Voice",
                       f"**User:** {member.mention}\n**Channel:** {after.channel.name}",
                       color=0x2ECC71)
    elif before.channel is not None and after.channel is None:
        await send_log(guild, "🔇 Member Left Voice",
                       f"**User:** {member.mention}\n**Channel:** {before.channel.name}",
                       color=0xE74C3C)
    elif before.channel != after.channel:
        await send_log(guild, "🔀 Member Switched Voice",
                       f"**User:** {member.mention}\n**From:** {before.channel.name} → **To:** {after.channel.name}",
                       color=0x3498DB)

# ═══════════════════════════════════════════════════════════════
#  ██  SLASH COMMANDS
# ═══════════════════════════════════════════════════════════════

# ── Setup Commands ──
@bot.slash_command(guild_ids=[GUILD_ID], name="setup_tickets", description="Send the ticket panel")
@has_security_role()
async def setup_tickets(ctx):
    channel = ctx.guild.get_channel(TICKET_CHANNEL_ID)
    if not channel:
        await ctx.respond("❌ Ticket channel not found.", ephemeral=True)
        return
    cfg = db["config"].find_one({"key": "ticket_config"}) or {}
    embed = discord.Embed(
        title=cfg.get("title", "🎫 MEM Store | Ticket Center"),
        description=cfg.get("description",
            "Open a ticket to buy, sell, or partner with us.\n\n"
            "💰 **Sell** — List your items\n"
            "🛒 **Buy** — Purchase items\n"
            "🤝 **Partner** — Business inquiries"
        ),
        color=EMBED_COLOR,
    )
    if LOGO_URL:
        embed.set_thumbnail(url=LOGO_URL)
    if GIF_URL:
        embed.set_image(url=GIF_URL)
    embed.set_footer(text=cfg.get("footer", FOOTER_TEXT))
    await channel.send(embed=embed, view=TicketView())
    await ctx.respond("✅ Ticket panel sent!", ephemeral=True)


@bot.slash_command(guild_ids=[GUILD_ID], name="setup_roles", description="Send the self-roles panel")
@has_security_role()
async def setup_roles(ctx):
    channel = ctx.guild.get_channel(SELF_ROLES_CHANNEL_ID)
    if not channel:
        await ctx.respond("❌ Self roles channel not found.", ephemeral=True)
        return
    lang_embed = discord.Embed(
        title="🌍 Language Roles",
        description="Choose your preferred language:",
        color=EMBED_COLOR,
    )
    lang_embed.set_footer(text=FOOTER_TEXT)
    await channel.send(embed=lang_embed, view=LanguageRoleView())
    game_embed = discord.Embed(
        title="🎮 Game Roles",
        description="Choose the games you play:",
        color=EMBED_COLOR,
    )
    game_embed.set_footer(text=FOOTER_TEXT)
    await channel.send(embed=game_embed, view=GameRoleView())
    await ctx.respond("✅ Self roles panel sent!", ephemeral=True)


# ── Moderation Commands ──
@bot.slash_command(guild_ids=[GUILD_ID], name="warn", description="Warn a member")
@has_security_role()
async def warn(ctx,
               member: Option(discord.Member, "Member to warn"),
               reason: Option(str, "Reason", default="No reason provided")):
    db["warnings"].insert_one({
        "guild_id":  ctx.guild.id,
        "user_id":   member.id,
        "by":        ctx.author.id,
        "reason":    reason,
        "timestamp": datetime.now(timezone.utc),
    })
    warns = db["warnings"].count_documents({"guild_id": ctx.guild.id, "user_id": member.id})
    embed = discord.Embed(
        title="⚠️ Warning Issued",
        description=f"**User:** {member.mention}\n**Reason:** {reason}\n**Total Warnings:** {warns}",
        color=0xF1C40F,
    )
    embed.set_footer(text=FOOTER_TEXT)
    await ctx.respond(embed=embed)
    await send_log(ctx.guild, "⚠️ Member Warned",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Reason:** {reason}\n**Total:** {warns}",
                   color=0xF1C40F)
    try:
        await member.send(f"⚠️ You were warned in **{ctx.guild.name}**\nReason: {reason}")
    except Exception:
        pass


@bot.slash_command(guild_ids=[GUILD_ID], name="ban", description="Ban a member")
@has_security_role()
async def ban(ctx,
              member: Option(discord.Member, "Member to ban"),
              reason: Option(str, "Reason", default="No reason provided")):
    try:
        await member.send(f"🔨 You were banned from **{ctx.guild.name}**\nReason: {reason}")
    except Exception:
        pass
    await member.ban(reason=reason)
    await ctx.respond(f"✅ **{member.name}** has been banned.")
    await send_log(ctx.guild, "🔨 Member Banned",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Reason:** {reason}",
                   color=0xE74C3C)


@bot.slash_command(guild_ids=[GUILD_ID], name="kick", description="Kick a member")
@has_security_role()
async def kick(ctx,
               member: Option(discord.Member, "Member to kick"),
               reason: Option(str, "Reason", default="No reason provided")):
    try:
        await member.send(f"👢 You were kicked from **{ctx.guild.name}**\nReason: {reason}")
    except Exception:
        pass
    await member.kick(reason=reason)
    await ctx.respond(f"✅ **{member.name}** has been kicked.")
    await send_log(ctx.guild, "👢 Member Kicked",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Reason:** {reason}",
                   color=0xE67E22)


@bot.slash_command(guild_ids=[GUILD_ID], name="timeout", description="Timeout a member")
@has_security_role()
async def timeout_cmd(ctx,
                      member: Option(discord.Member, "Member to timeout"),
                      minutes: Option(int, "Duration in minutes", default=10),
                      reason: Option(str, "Reason", default="No reason provided")):
    until = discord.utils.utcnow() + discord.utils.utcnow().__class__.now(timezone.utc).__class__(
        year=discord.utils.utcnow().year,
        month=discord.utils.utcnow().month,
        day=discord.utils.utcnow().day,
        hour=discord.utils.utcnow().hour,
        minute=discord.utils.utcnow().minute + minutes,
        tzinfo=timezone.utc
    ).utcoffset() or discord.utils.utcnow()
    import datetime as dt
    until = discord.utils.utcnow() + dt.timedelta(minutes=minutes)
    await member.timeout(until, reason=reason)
    await ctx.respond(f"✅ **{member.name}** timed out for {minutes} minutes.")
    await send_log(ctx.guild, "⏰ Member Timed Out",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Duration:** {minutes}m\n**Reason:** {reason}",
                   color=0xF39C12)


@bot.slash_command(guild_ids=[GUILD_ID], name="unwarn", description="Remove warnings from a member")
@has_security_role()
async def unwarn(ctx,
                 member: Option(discord.Member, "Member to clear warnings for")):
    result = db["warnings"].delete_many({"guild_id": ctx.guild.id, "user_id": member.id})
    await ctx.respond(f"✅ Cleared **{result.deleted_count}** warning(s) for {member.mention}.")
    await send_log(ctx.guild, "✅ Warnings Cleared",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Count:** {result.deleted_count}",
                   color=0x2ECC71)


@bot.slash_command(guild_ids=[GUILD_ID], name="warnings", description="Check warnings for a member")
async def check_warnings(ctx,
                         member: Option(discord.Member, "Member to check")):
    warns = list(db["warnings"].find({"guild_id": ctx.guild.id, "user_id": member.id}))
    if not warns:
        await ctx.respond(f"✅ {member.mention} has no warnings.", ephemeral=True)
        return
    desc = "\n".join([f"• {w.get('reason', 'No reason')} — <t:{int(w['timestamp'].timestamp())}:R>" for w in warns[:10]])
    embed = discord.Embed(
        title=f"⚠️ Warnings for {member.name}",
        description=desc,
        color=0xF1C40F,
    )
    embed.set_footer(text=f"Total: {len(warns)} warning(s)")
    await ctx.respond(embed=embed, ephemeral=True)


@bot.slash_command(guild_ids=[GUILD_ID], name="blacklist", description="Blacklist a user")
@has_security_role()
async def blacklist(ctx,
                    member: Option(discord.Member, "Member to blacklist"),
                    reason: Option(str, "Reason", default="No reason provided")):
    db["blacklist"].update_one(
        {"user_id": member.id},
        {"$set": {"user_id": member.id, "reason": reason, "added_at": datetime.now(timezone.utc), "by": ctx.author.id}},
        upsert=True,
    )
    await ctx.respond(f"🚫 **{member.name}** has been blacklisted.")
    await send_log(ctx.guild, "🚫 User Blacklisted",
                   f"**User:** {member.mention}\n**By:** {ctx.author.mention}\n**Reason:** {reason}",
                   color=0xE74C3C)


# ── Feedback Command ──
@bot.slash_command(guild_ids=[GUILD_ID], name="feedback", description="Submit feedback about a seller")
async def feedback(ctx,
                   seller: Option(discord.Member, "Seller to rate"),
                   message: Option(str, "Your feedback message")):
    channel = ctx.guild.get_channel(FEEDBACK_CHANNEL_ID)
    embed = discord.Embed(
        title="💬 New Feedback",
        description=f"**Seller:** {seller.mention}\n**By:** {ctx.author.mention}\n\n{message}",
        color=EMBED_COLOR,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=FOOTER_TEXT)
    if channel:
        await channel.send(embed=embed)
    await ctx.respond("✅ Feedback submitted!", ephemeral=True)
    await send_log(ctx.guild, "💬 Feedback Submitted",
                   f"**Seller:** {seller.mention}\n**By:** {ctx.author.mention}\n**Message:** {message[:200]}",
                   color=0x3498DB)


# ── Order Command ──
@bot.slash_command(guild_ids=[GUILD_ID], name="order", description="Log a completed order")
@has_security_role()
async def order(ctx,
                buyer:  Option(discord.Member, "Buyer"),
                item:   Option(str, "Item/description"),
                amount: Option(str, "Amount/price")):
    channel = ctx.guild.get_channel(ORDER_CHANNEL_ID)
    embed = discord.Embed(
        title="📦 Order Completed",
        description=(
            f"**Buyer:** {buyer.mention}\n"
            f"**Seller:** {ctx.author.mention}\n"
            f"**Item:** {item}\n"
            f"**Amount:** {amount}"
        ),
        color=0x2ECC71,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=FOOTER_TEXT)
    if channel:
        await channel.send(embed=embed)
    await ctx.respond("✅ Order logged!", ephemeral=True)
    await send_log(ctx.guild, "📦 Order Logged",
                   f"**Buyer:** {buyer.mention}\n**Seller:** {ctx.author.mention}\n**Item:** {item}\n**Amount:** {amount}",
                   color=0x2ECC71)


# ═══════════════════════════════════════════════════════════════
#  ██  VOICE RELAY COMMANDS (ENHANCED)
# ═══════════════════════════════════════════════════════════════

@bot.slash_command(guild_ids=[GUILD_ID], name="join", description="Make the bot join a voice channel")
@has_security_role()
async def join_voice(ctx,
                     channel: Option(discord.VoiceChannel, "Select the voice channel to join", required=True)):
    guild = ctx.guild
    # Disconnect from existing session if any
    if guild.voice_client and guild.voice_client.is_connected():
        old_channel = guild.voice_client.channel.name
        # Save call history
        sess = voice_session.get(guild.id, {})
        if sess.get("start"):
            start_dt = datetime.fromisoformat(sess["start"])
            end_dt   = datetime.now(timezone.utc)
            duration_secs = int((end_dt - start_dt).total_seconds())
            db["voice_calls"].insert_one({
                "guild_id":    guild.id,
                "channel":     old_channel,
                "channel_id":  sess.get("channel_id"),
                "started_by":  sess.get("started_by"),
                "start":       start_dt,
                "end":         end_dt,
                "duration":    duration_secs,
            })
        await guild.voice_client.disconnect()
    try:
        vc = await channel.connect()
    except Exception as e:
        await ctx.respond(f"❌ Failed to join channel: {e}", ephemeral=True)
        return
    source = MicAudioSource()
    vc.play(source, after=lambda e: None)
    start_time = datetime.now(timezone.utc).isoformat()
    voice_session[guild.id] = {
        "channel":    channel.name,
        "channel_id": channel.id,
        "start":      start_time,
        "started_by": ctx.author.id,
        "started_by_name": ctx.author.name,
    }
    await _ws_broadcast({
        "type":       "joined",
        "channel":    channel.name,
        "channel_id": channel.id,
        "start":      start_time,
        "started_by": ctx.author.name,
        "members":    [m.name for m in channel.members if not m.bot],
    })
    embed = discord.Embed(
        title="🎙️ Voice Relay Active",
        description=(
            f"**Channel:** {channel.mention}\n"
            f"**Started by:** {ctx.author.mention}\n\n"
            "The bot is now in the voice channel.\n"
            "Use `/leave` to disconnect.\n"
            "Use the **Dashboard → Voice Relay** to speak."
        ),
        color=0x2ECC71,
        timestamp=datetime.now(timezone.utc),
    )
    embed.set_footer(text=FOOTER_TEXT)
    await ctx.respond(embed=embed)
    await send_log(guild, "🎙️ Voice Relay Started",
                   f"**Channel:** {channel.name}\n**By:** {ctx.author.mention}",
                   color=0x2ECC71)


@bot.slash_command(guild_ids=[GUILD_ID], name="leave", description="Disconnect the bot from voice")
@has_security_role()
async def leave_voice(ctx):
    guild = ctx.guild
    if not guild.voice_client or not guild.voice_client.is_connected():
        await ctx.respond("❌ Not in a voice channel.", ephemeral=True)
        return
    channel_name = guild.voice_client.channel.name
    sess = voice_session.get(guild.id, {})
    if sess.get("start"):
        start_dt = datetime.fromisoformat(sess["start"])
        end_dt   = datetime.now(timezone.utc)
        duration_secs = int((end_dt - start_dt).total_seconds())
        db["voice_calls"].insert_one({
            "guild_id":    guild.id,
            "channel":     channel_name,
            "channel_id":  sess.get("channel_id"),
            "started_by":  sess.get("started_by"),
            "started_by_name": sess.get("started_by_name", "Unknown"),
            "start":       start_dt,
            "end":         end_dt,
            "duration":    duration_secs,
        })
    await guild.voice_client.disconnect()
    voice_session.pop(guild.id, None)
    await _ws_broadcast({"type": "left", "channel": channel_name})
    await ctx.respond(f"✅ Disconnected from **{channel_name}**.")
    await send_log(guild, "🔇 Voice Relay Ended",
                   f"**Channel:** {channel_name}\n**By:** {ctx.author.mention}",
                   color=0xE74C3C)


@bot.slash_command(guild_ids=[GUILD_ID], name="voice_status", description="Check voice relay status")
async def voice_status(ctx):
    guild = ctx.guild
    if not guild.voice_client or not guild.voice_client.is_connected():
        await ctx.respond("🔇 Bot is not in any voice channel.", ephemeral=True)
        return
    sess    = voice_session.get(guild.id, {})
    channel = guild.voice_client.channel
    members = [m.mention for m in channel.members if not m.bot]
    start_ts = datetime.fromisoformat(sess["start"]).timestamp() if sess.get("start") else 0
    embed = discord.Embed(
        title="🎙️ Voice Relay Status",
        description=(
            f"**Channel:** {channel.mention}\n"
            f"**Started:** <t:{int(start_ts)}:R>\n"
            f"**Members:** {', '.join(members) or 'None'}"
        ),
        color=0x2ECC71,
    )
    embed.set_footer(text=FOOTER_TEXT)
    await ctx.respond(embed=embed, ephemeral=True)


# ── Embed Command ──
@bot.slash_command(guild_ids=[GUILD_ID], name="embed", description="Send a custom embed to a channel")
@has_security_role()
async def send_embed(ctx,
                     channel: Option(discord.TextChannel, "Target channel"),
                     title:   Option(str, "Embed title", default=""),
                     description: Option(str, "Embed description", default=""),
                     color:   Option(str, "Hex color (e.g. #1a2332)", default="#1a2332")):
    try:
        color_int = int(color.strip("#"), 16)
    except Exception:
        color_int = EMBED_COLOR
    embed = discord.Embed(title=title, description=description, color=color_int, timestamp=datetime.now(timezone.utc))
    embed.set_footer(text=FOOTER_TEXT)
    await channel.send(embed=embed)
    await ctx.respond(f"✅ Embed sent to {channel.mention}", ephemeral=True)


# ═══════════════════════════════════════════════════════════════
#  ██  WEBSOCKET SERVER
# ═══════════════════════════════════════════════════════════════

async def start_ws_server():
    print(f"🔌 WebSocket server starting on port {WS_PORT}")
    async with websockets.serve(ws_handler, "0.0.0.0", WS_PORT):
        await asyncio.Future()


# ═══════════════════════════════════════════════════════════════
#  ██  RUN
# ═══════════════════════════════════════════════════════════════

bot.run(os.getenv("DISCORD_TOKEN"))
