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

DB_PATH = "messages.db"
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "0"))

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


@client.event
async def on_message(message):
    if message.author.id == client.user.id:
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


@client.event
async def on_message_delete(message):
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

            log_text = (
                f"**Message Deleted**\n"
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
async def on_message_edit(before, after):
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

        log_text = (
            f"**Message Edited**\n"
            f"**Author:** {after.author}\n"
            f"**Channel:** {after.channel}\n"
            f"**Before:** {old_content or '*empty*'}\n"
            f"**After:** {after.content or '*empty*'}"
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
