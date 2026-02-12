import discord
import os
import logging
import aiosqlite
from datetime import datetime
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


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY,
            author TEXT,
            channel TEXT,
            content TEXT,
            attachments TEXT,
            created_at TEXT
        )""")
        await db.commit()


@client.event
async def on_ready():
    await init_db()
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
        async with aiosqlite.connect(DB_PATH) as db:
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
        async with aiosqlite.connect(DB_PATH) as db:
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

                log_channel = client.get_channel(LOG_CHANNEL_ID)
                if log_channel:
                    await log_channel.send(log_text, files=files)

                await db.execute(
                    "DELETE FROM messages WHERE message_id = ?", (message.id,)
                )
                await db.commit()
    except Exception:
        log.exception("Failed to handle message deletion for %s", message.id)


@client.event
async def on_message_edit(before, after):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
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

            log_channel = client.get_channel(LOG_CHANNEL_ID)
            if log_channel:
                await log_channel.send(log_text)

            await db.execute(
                "UPDATE messages SET content = ? WHERE message_id = ?",
                (after.content, after.id),
            )
            await db.commit()
    except Exception:
        log.exception("Failed to handle message edit for %s", before.id)


client.run(os.getenv("DISCORD_TOKEN"))
