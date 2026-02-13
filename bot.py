import asyncio
import discord
import os
import logging
import aiosqlite
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "messages.db")
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))

_raw_guild_ids = os.getenv("GUILD_IDS", "").strip()
GUILD_IDS = (
    {int(gid) for gid in _raw_guild_ids.split(",") if gid.strip()}
    if _raw_guild_ids
    else None
)

MIRROR_CHANNEL_ID = int(os.getenv("MIRROR_CHANNEL_ID", "0"))
_raw_mirror_ids = os.getenv("MIRROR_SOURCE_IDS", "").strip()
MIRROR_SOURCE_IDS = (
    {int(cid) for cid in _raw_mirror_ids.split(",") if cid.strip()}
    if _raw_mirror_ids
    else set()
)

client = discord.Client()

os.makedirs("media", exist_ok=True)

DISCORD_MAX_LEN = 2000

# Module-level state (initialized once in on_ready)
db = None
db_lock = None
log_queue = None
api_semaphore = None
_log_worker_task = None


async def send_long_message(channel, text, files=None):
    """Send a message, splitting into multiple if over 2000 chars."""
    async with api_semaphore:
        if len(text) <= DISCORD_MAX_LEN:
            await channel.send(text, files=files)
            return

        chunks = []
        while text:
            if len(text) <= DISCORD_MAX_LEN:
                chunks.append(text)
                break
            split_at = text.rfind("\n", 0, DISCORD_MAX_LEN)
            if split_at == -1:
                split_at = DISCORD_MAX_LEN
            chunks.append(text[:split_at])
            text = text[split_at:].lstrip("\n")

        for i, chunk in enumerate(chunks):
            if i == 0 and files:
                await channel.send(chunk, files=files)
            else:
                await channel.send(chunk)


async def log_worker():
    """Background worker that sends log messages at ~2/sec."""
    while True:
        text, files = await log_queue.get()
        try:
            log_channel = client.get_channel(LOG_CHANNEL_ID)
            if log_channel:
                await send_long_message(log_channel, text, files=files)
        except Exception:
            log.exception("log_worker failed to send message")
        finally:
            log_queue.task_done()
        await asyncio.sleep(0.5)


@client.event
async def on_ready():
    global db, db_lock, log_queue, api_semaphore, _log_worker_task

    if db is None:
        db = await aiosqlite.connect(DB_PATH)
        await db.execute("""CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY,
            author TEXT,
            channel TEXT,
            content TEXT,
            attachments TEXT,
            created_at TEXT
        )""")
        await db.commit()
        db_lock = asyncio.Lock()
        log_queue = asyncio.Queue()
        api_semaphore = asyncio.Semaphore(10)
        _log_worker_task = asyncio.create_task(log_worker())

    log.info("Logged in as %s", client.user)


def _guild_allowed(guild):
    if GUILD_IDS is None or guild is None:
        return True
    return guild.id in GUILD_IDS


@client.event
async def on_message(message):
    if message.author.id == client.user.id:
        return
    if not _guild_allowed(message.guild):
        return

    attachment_names = (
        ",".join(a.filename for a in message.attachments)
        if message.attachments
        else ""
    )

    try:
        async with db_lock:
            await db.execute(
                "INSERT OR REPLACE INTO messages "
                "(message_id, author, channel, content, attachments, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    message.id,
                    str(message.author),
                    str(message.channel),
                    message.content,
                    attachment_names,
                    str(message.created_at),
                ),
            )
            await db.commit()
    except Exception:
        log.exception("Failed to save message %s to database", message.id)

    for attachment in message.attachments:
        try:
            file_name = f"media/{message.id}_{attachment.filename}"
            await attachment.save(file_name)
        except Exception:
            log.exception("Failed to save attachment %s", attachment.filename)

    if MIRROR_CHANNEL_ID and message.channel.id in MIRROR_SOURCE_IDS:
        mirror_ch = client.get_channel(MIRROR_CHANNEL_ID)
        if mirror_ch:
            header = f"**[#{message.channel.name}]** {message.author} ({message.created_at.strftime('%Y-%m-%d %H:%M')}):"
            mirror_text = f"{header}\n{message.content}" if message.content else header

            mirror_files = []
            for attachment in message.attachments:
                path = f"media/{message.id}_{attachment.filename}"
                if os.path.exists(path):
                    mirror_files.append(discord.File(path, filename=attachment.filename))

            try:
                await send_long_message(mirror_ch, mirror_text, files=mirror_files or None)
            except Exception:
                log.exception("Failed to mirror message %s", message.id)


@client.event
async def on_message_delete(message):
    if not _guild_allowed(message.guild):
        return
    try:
        async with db_lock:
            async with db.execute(
                "SELECT author, channel, content, attachments, created_at "
                "FROM messages WHERE message_id = ?",
                (message.id,),
            ) as cursor:
                row = await cursor.fetchone()

        if row:
            author, channel, content, attachments, created_at = row

            guild_name = message.guild.name if message.guild else "DM"
            log_text = (
                f"**Message Deleted**\n"
                f"**Server:** {guild_name}\n"
                f"**Author:** {author}\n"
                f"**Channel:** {channel}\n"
                f"**Content:** {content or '*empty*'}\n"
                f"**Original timestamp:** {created_at}"
            )

            files = []
            if attachments:
                for filename in attachments.split(","):
                    path = f"media/{message.id}_{filename}"
                    if os.path.exists(path):
                        files.append(discord.File(path, filename=filename))

            await log_queue.put((log_text, files))

            async with db_lock:
                await db.execute(
                    "DELETE FROM messages WHERE message_id = ?", (message.id,)
                )
                await db.commit()
    except Exception:
        log.exception("Failed to handle message deletion for %s", message.id)


@client.event
async def on_bulk_message_delete(messages):
    if not messages:
        return
    if not _guild_allowed(messages[0].guild):
        return

    guild_name = messages[0].guild.name if messages[0].guild else "DM"
    channel = str(messages[0].channel)
    msg_ids = [m.id for m in messages]

    try:
        placeholders = ",".join("?" * len(msg_ids))
        async with db_lock:
            async with db.execute(
                f"SELECT message_id, author, content, attachments, created_at "
                f"FROM messages WHERE message_id IN ({placeholders})",
                msg_ids,
            ) as cursor:
                rows = {r[0]: r[1:] for r in await cursor.fetchall()}

        lines = [
            f"**Bulk Delete — {len(messages)} messages**",
            f"**Server:** {guild_name}",
            f"**Channel:** {channel}",
            "",
        ]

        all_files = []
        for msg in messages:
            if msg.id in rows:
                author, content, attachments, created_at = rows[msg.id]
            else:
                author = str(msg.author) if msg.author else "Unknown"
                content = msg.content or ""
                attachments = ""
                created_at = str(msg.created_at) if msg.created_at else "?"

            lines.append(f"[{created_at}] {author}: {content or '*empty*'}")

            if attachments:
                for filename in attachments.split(","):
                    path = f"media/{msg.id}_{filename}"
                    if os.path.exists(path):
                        all_files.append(discord.File(path, filename=filename))

        await log_queue.put(("\n".join(lines), all_files or None))

        if rows:
            async with db_lock:
                await db.execute(
                    f"DELETE FROM messages WHERE message_id IN ({placeholders})",
                    msg_ids,
                )
                await db.commit()
    except Exception:
        log.exception("Failed to handle bulk message deletion")


@client.event
async def on_message_edit(before, after):
    if not _guild_allowed(before.guild):
        return
    try:
        async with db_lock:
            async with db.execute(
                "SELECT content FROM messages WHERE message_id = ?",
                (before.id,),
            ) as cursor:
                row = await cursor.fetchone()

        old_content = row[0] if row else str(before.content)

        if old_content == after.content:
            return

        guild_name = after.guild.name if after.guild else "DM"
        log_text = (
            f"**Message Edited**\n"
            f"**Server:** {guild_name}\n"
            f"**Author:** {after.author}\n"
            f"**Channel:** {after.channel}\n"
            f"**Before:** {old_content or '*empty*'}\n"
            f"**After:** {after.content or '*empty*'}\n"
            f"{after.jump_url}"
        )

        await log_queue.put((log_text, None))

        async with db_lock:
            await db.execute(
                "UPDATE messages SET content = ? WHERE message_id = ?",
                (after.content, after.id),
            )
            await db.commit()
    except Exception:
        log.exception("Failed to handle message edit for %s", before.id)


client.run(os.getenv("DISCORD_TOKEN"))
