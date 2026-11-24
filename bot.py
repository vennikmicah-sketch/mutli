"""
DropBot — Discord file dropper + gen/stock bot
Railway-ready.  Set BOT_TOKEN env var before running.
"""

import asyncio
import json
import os
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BOT_TOKEN       = os.environ.get("BOT_TOKEN", "")
LOG_CHANNEL_ID  = int(os.environ.get("LOG_CHANNEL_ID", "0"))
PREFIX          = os.environ.get("PREFIX", "!")
ADMIN_IDS_RAW   = os.environ.get("ADMIN_IDS", "")
ADMIN_IDS       = set(int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip())

STATE_FILE  = "state.json"   # persists queue / stock across restarts

# ─────────────────────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────────────────────

# drop_queue: list of {lines, channel_id, cooldown_s, label, next_drop, loop}
drop_queue: list[dict] = []

# stock: {service: [lines]}
stock: dict[str, list[str]] = defaultdict(list)

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"drop_queue": drop_queue, "stock": dict(stock)}, f, indent=2)
    except Exception as e:
        print(f"[STATE] save error: {e}")

def load_state() -> None:
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data.get("drop_queue", []):
            drop_queue.append(item)
        for svc, lines in data.get("stock", {}).items():
            stock[svc] = lines
        print(f"[STATE] loaded {len(drop_queue)} queued drops, "
              f"{sum(len(v) for v in stock.values())} stock lines")
    except Exception as e:
        print(f"[STATE] load error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# BOT SETUP
# ─────────────────────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_admin(ctx: commands.Context) -> bool:
    if ctx.author.id in ADMIN_IDS:
        return True
    if ctx.guild and ctx.author.guild_permissions.administrator:
        return True
    return False

def fmt_time(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def parse_cooldown(raw: str) -> int:
    """Parse '30s', '5m', '2h', or bare integer seconds."""
    raw = raw.strip().lower()
    if raw.endswith("h"):
        return int(raw[:-1]) * 3600
    if raw.endswith("m"):
        return int(raw[:-1]) * 60
    if raw.endswith("s"):
        return int(raw[:-1])
    return int(raw)

def chunk_lines(lines: list[str], max_chars: int = 1900) -> list[str]:
    """Split lines into Discord-safe chunks."""
    chunks, cur = [], []
    cur_len = 0
    for line in lines:
        if cur_len + len(line) + 1 > max_chars and cur:
            chunks.append("\n".join(cur))
            cur, cur_len = [], 0
        cur.append(line)
        cur_len += len(line) + 1
    if cur:
        chunks.append("\n".join(cur))
    return chunks

async def log(msg: str) -> None:
    print(f"[LOG] {msg}")
    if LOG_CHANNEL_ID:
        try:
            ch = bot.get_channel(LOG_CHANNEL_ID)
            if ch:
                await ch.send(f"```{msg}```")
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# DROP WORKER
# ─────────────────────────────────────────────────────────────────────────────

@tasks.loop(seconds=5)
async def drop_worker():
    now = time.time()
    for item in list(drop_queue):
        if item.get("paused"):
            continue
        if now < item.get("next_drop", 0):
            continue
        if not item["lines"]:
            if item.get("loop") and item.get("original_lines"):
                item["lines"] = list(item["original_lines"])
            else:
                drop_queue.remove(item)
                save_state()
                await log(f"Drop finished: **{item.get('label', 'unnamed')}**")
                continue

        line = item["lines"].pop(0)
        item["next_drop"] = now + item["cooldown_s"]
        save_state()

        channel = bot.get_channel(item["channel_id"])
        if channel is None:
            await log(f"[DROP] channel {item['channel_id']} not found — skipping")
            continue
        try:
            await channel.send(f"```\n{line}\n```")
        except discord.Forbidden:
            await log(f"[DROP] no permission in channel {item['channel_id']}")
        except Exception as e:
            await log(f"[DROP] error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# EVENTS
# ─────────────────────────────────────────────────────────────────────────────

@bot.event
async def on_ready():
    load_state()
    drop_worker.start()
    print(f"[READY] logged in as {bot.user} (ID {bot.user.id})")
    print(f"[READY] {len(drop_queue)} drop jobs loaded")
    await log(f"DropBot online — {bot.user}")

# ─────────────────────────────────────────────────────────────────────────────
# ── DROP COMMANDS ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

@bot.command(name="load")
async def cmd_load(ctx, channel: discord.TextChannel, cooldown_raw: str, *, label: str = ""):
    """
    !load #channel 1h [label]
    Attach a .txt file to load lines for dropping into that channel.
    """
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if not ctx.message.attachments:
        return await ctx.send("❌ Attach a `.txt` file with the lines to drop.")

    att = ctx.message.attachments[0]
    if not att.filename.endswith(".txt"):
        return await ctx.send("❌ Only `.txt` files supported.")

    try:
        cooldown_s = parse_cooldown(cooldown_raw)
    except ValueError:
        return await ctx.send("❌ Bad cooldown — use `30s`, `5m`, `2h`, or plain seconds.")

    raw = await att.read()
    lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
    if not lines:
        return await ctx.send("❌ File is empty.")

    item = {
        "label":          label or att.filename,
        "lines":          lines,
        "original_lines": list(lines),
        "channel_id":     channel.id,
        "cooldown_s":     cooldown_s,
        "next_drop":      time.time(),
        "loop":           False,
        "paused":         False,
    }
    drop_queue.append(item)
    save_state()

    await ctx.send(
        f"✅ Loaded **{len(lines)}** lines → {channel.mention}\n"
        f"⏱ Cooldown: `{fmt_time(cooldown_s)}`\n"
        f"📋 Label: `{item['label']}`\n"
        f"🔢 Queue position: `{len(drop_queue)}`"
    )

@bot.command(name="loadloop")
async def cmd_loadloop(ctx, channel: discord.TextChannel, cooldown_raw: str, *, label: str = ""):
    """
    !loadloop #channel 1h [label]
    Same as !load but loops forever when lines run out.
    """
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if not ctx.message.attachments:
        return await ctx.send("❌ Attach a `.txt` file.")

    att = ctx.message.attachments[0]
    if not att.filename.endswith(".txt"):
        return await ctx.send("❌ Only `.txt` files supported.")

    try:
        cooldown_s = parse_cooldown(cooldown_raw)
    except ValueError:
        return await ctx.send("❌ Bad cooldown.")

    raw = await att.read()
    lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
    if not lines:
        return await ctx.send("❌ File is empty.")

    item = {
        "label":          label or att.filename,
        "lines":          list(lines),
        "original_lines": list(lines),
        "channel_id":     channel.id,
        "cooldown_s":     cooldown_s,
        "next_drop":      time.time(),
        "loop":           True,
        "paused":         False,
    }
    drop_queue.append(item)
    save_state()

    await ctx.send(
        f"🔁 Loaded **{len(lines)}** lines (loop) → {channel.mention}\n"
        f"⏱ Cooldown: `{fmt_time(cooldown_s)}`\n"
        f"📋 Label: `{item['label']}`"
    )

@bot.command(name="queue")
async def cmd_queue(ctx):
    """!queue — show all active drop jobs."""
    if not drop_queue:
        return await ctx.send("📭 Drop queue is empty.")

    now = time.time()
    lines = ["```", "ID  Label                     Channel        Left   CD       Status"]
    lines.append("─" * 70)
    for i, item in enumerate(drop_queue):
        remaining_s = max(0, item.get("next_drop", now) - now)
        status = "⏸ PAUSED" if item.get("paused") else f"⏱ {fmt_time(remaining_s)}"
        loop_tag = "🔁" if item.get("loop") else "  "
        label = item.get("label", "")[:24].ljust(24)
        ch_id = str(item["channel_id"])[-6:]
        left = str(len(item["lines"])).rjust(5)
        cd = fmt_time(item["cooldown_s"]).rjust(7)
        lines.append(f"{str(i).rjust(2)}  {loop_tag} {label}  #{ch_id}  {left}  {cd}  {status}")
    lines.append("```")
    await ctx.send("\n".join(lines))

@bot.command(name="pause")
async def cmd_pause(ctx, job_id: int = None):
    """!pause [id] — pause a drop job (or all if no id)."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if job_id is None:
        for item in drop_queue:
            item["paused"] = True
        save_state()
        return await ctx.send(f"⏸ Paused all {len(drop_queue)} drop jobs.")
    if 0 <= job_id < len(drop_queue):
        drop_queue[job_id]["paused"] = True
        save_state()
        await ctx.send(f"⏸ Paused job `{job_id}` — `{drop_queue[job_id].get('label', '')}`")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="resume")
async def cmd_resume(ctx, job_id: int = None):
    """!resume [id] — resume a paused drop job (or all)."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if job_id is None:
        for item in drop_queue:
            item["paused"] = False
        save_state()
        return await ctx.send(f"▶️ Resumed all {len(drop_queue)} drop jobs.")
    if 0 <= job_id < len(drop_queue):
        drop_queue[job_id]["paused"] = False
        save_state()
        await ctx.send(f"▶️ Resumed job `{job_id}` — `{drop_queue[job_id].get('label', '')}`")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="remove")
async def cmd_remove(ctx, job_id: int):
    """!remove <id> — remove a drop job from the queue."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if 0 <= job_id < len(drop_queue):
        item = drop_queue.pop(job_id)
        save_state()
        await ctx.send(f"🗑️ Removed job `{job_id}` — `{item.get('label', '')}`")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="clearqueue")
async def cmd_clearqueue(ctx):
    """!clearqueue — remove ALL drop jobs."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    count = len(drop_queue)
    drop_queue.clear()
    save_state()
    await ctx.send(f"🗑️ Cleared {count} drop jobs.")

@bot.command(name="setcooldown")
async def cmd_setcooldown(ctx, job_id: int, cooldown_raw: str):
    """!setcooldown <id> <cooldown> — change cooldown of a running job."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    try:
        cooldown_s = parse_cooldown(cooldown_raw)
    except ValueError:
        return await ctx.send("❌ Bad cooldown.")
    if 0 <= job_id < len(drop_queue):
        drop_queue[job_id]["cooldown_s"] = cooldown_s
        save_state()
        await ctx.send(f"⏱ Job `{job_id}` cooldown set to `{fmt_time(cooldown_s)}`")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="setchannel")
async def cmd_setchannel(ctx, job_id: int, channel: discord.TextChannel):
    """!setchannel <id> #channel — move a job to a different channel."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if 0 <= job_id < len(drop_queue):
        drop_queue[job_id]["channel_id"] = channel.id
        save_state()
        await ctx.send(f"📌 Job `{job_id}` now drops into {channel.mention}")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="peek")
async def cmd_peek(ctx, job_id: int, count: int = 10):
    """!peek <id> [count] — preview the next N lines queued for a job."""
    if 0 <= job_id < len(drop_queue):
        item = drop_queue[job_id]
        lines = item["lines"][:count]
        if not lines:
            return await ctx.send("📭 No lines left in this job.")
        txt = "\n".join(lines)
        await ctx.send(f"**Next {len(lines)} lines for job `{job_id}` (`{item.get('label','')}`):**\n```\n{txt[:1800]}\n```")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="dropadd")
async def cmd_dropadd(ctx, job_id: int, *, lines_raw: str = ""):
    """
    !dropadd <id> [lines]
    Add more lines to an existing job.  Lines in message body, one per line.
    Or attach a .txt file.
    """
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if 0 <= job_id < len(drop_queue):
        new_lines = []
        if ctx.message.attachments:
            raw = await ctx.message.attachments[0].read()
            new_lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
        elif lines_raw:
            new_lines = [l.strip() for l in lines_raw.splitlines() if l.strip()]
        if not new_lines:
            return await ctx.send("❌ No lines provided.")
        drop_queue[job_id]["lines"].extend(new_lines)
        save_state()
        await ctx.send(f"➕ Added **{len(new_lines)}** lines to job `{job_id}`. Total left: `{len(drop_queue[job_id]['lines'])}`")
    else:
        await ctx.send("❌ Invalid job ID.")

@bot.command(name="dropsend")
async def cmd_dropsend(ctx, channel: discord.TextChannel, *, content: str):
    """!dropsend #channel <text> — instantly send something to a channel."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    await channel.send(content)
    await ctx.message.add_reaction("✅")

# ─────────────────────────────────────────────────────────────────────────────
# ── GEN / STOCK COMMANDS ──────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

@bot.command(name="addstock")
async def cmd_addstock(ctx, service: str = ""):
    """
    !addstock <service>
    Attach a .txt file to add lines to that service's stock.
    """
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if not service:
        return await ctx.send("❌ Usage: `!addstock <service>` — attach .txt file.")
    if not ctx.message.attachments:
        return await ctx.send("❌ Attach a `.txt` file with accounts.")
    att = ctx.message.attachments[0]
    raw = await att.read()
    lines = [l.strip() for l in raw.decode("utf-8", errors="replace").splitlines() if l.strip()]
    if not lines:
        return await ctx.send("❌ File is empty.")
    service = service.lower()
    stock[service].extend(lines)
    save_state()
    await ctx.send(f"📦 Added **{len(lines)}** lines to `{service}`. Total stock: `{len(stock[service])}`")

@bot.command(name="gen")
async def cmd_gen(ctx, service: str = ""):
    """!gen <service> — generate one account from stock."""
    if not service:
        return await ctx.send("❌ Usage: `!gen <service>`")
    service = service.lower()
    if not stock.get(service):
        return await ctx.send(f"❌ No stock for `{service}`.")
    line = stock[service].pop(0)
    save_state()
    try:
        await ctx.message.delete()
    except Exception:
        pass
    try:
        await ctx.author.send(
            f"🎁 **{service.upper()}** — generated for you\n```\n{line}\n```"
        )
        await ctx.send(f"✅ Sent `{service}` account to your DMs! (`{len(stock[service])}` left)", delete_after=10)
    except discord.Forbidden:
        await ctx.send(f"🎁 **{service.upper()}**\n```\n{line}\n```", delete_after=30)

@bot.command(name="genall")
async def cmd_genall(ctx):
    """!genall — show all services with available stock."""
    if not any(stock.values()):
        return await ctx.send("📭 No stock available.")
    lines = ["**📦 Available Services:**"]
    for svc in sorted(stock):
        if stock[svc]:
            lines.append(f"• `{svc}` — `{len(stock[svc])}` accounts")
    await ctx.send("\n".join(lines))

@bot.command(name="stock")
async def cmd_stock(ctx):
    """!stock — show stock counts per service."""
    if not any(stock.values()):
        return await ctx.send("📭 Stock is empty.")
    lines = ["```", "Service              Count"]
    lines.append("─" * 32)
    for svc in sorted(stock):
        lines.append(f"{svc.ljust(20)} {len(stock[svc])}")
    lines.append("```")
    await ctx.send("\n".join(lines))

@bot.command(name="clearstock")
async def cmd_clearstock(ctx, service: str = ""):
    """!clearstock [service] — clear stock for one service or all."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if service:
        service = service.lower()
        stock[service] = []
        save_state()
        await ctx.send(f"🗑️ Cleared stock for `{service}`.")
    else:
        for k in stock:
            stock[k] = []
        save_state()
        await ctx.send("🗑️ All stock cleared.")

@bot.command(name="stockdump")
async def cmd_stockdump(ctx, service: str = ""):
    """!stockdump <service> — DM yourself all remaining stock for a service."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    service = service.lower()
    if not stock.get(service):
        return await ctx.send(f"❌ No stock for `{service}`.")
    chunks = chunk_lines(stock[service])
    try:
        for chunk in chunks:
            await ctx.author.send(f"**{service.upper()} stock dump:**\n```\n{chunk}\n```")
        await ctx.send(f"📬 Sent {len(stock[service])} lines to your DMs.")
    except discord.Forbidden:
        await ctx.send("❌ Cannot DM you. Enable DMs from server members.")

@bot.command(name="stockexport")
async def cmd_stockexport(ctx, service: str = ""):
    """!stockexport <service> — export stock as a .txt file attachment."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    service = service.lower()
    if not stock.get(service):
        return await ctx.send(f"❌ No stock for `{service}`.")
    content = "\n".join(stock[service]).encode("utf-8")
    await ctx.send(
        f"📎 `{service}` stock — {len(stock[service])} lines",
        file=discord.File(fp=__import__("io").BytesIO(content), filename=f"{service}_stock.txt")
    )

@bot.command(name="stockadd")
async def cmd_stockadd(ctx, service: str = "", *, lines_raw: str = ""):
    """!stockadd <service> <line1> | <line2> | ... — add lines inline."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    if not service:
        return await ctx.send("❌ Usage: `!stockadd <service> line1 | line2 | ...`")
    service = service.lower()
    lines = [l.strip() for l in lines_raw.split("|") if l.strip()]
    if not lines:
        return await ctx.send("❌ No lines provided.")
    stock[service].extend(lines)
    save_state()
    await ctx.send(f"➕ Added **{len(lines)}** lines to `{service}`. Total: `{len(stock[service])}`")

# ─────────────────────────────────────────────────────────────────────────────
# ── UTILITY COMMANDS ─────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────

@bot.command(name="status")
async def cmd_status(ctx):
    """!status — show bot status overview."""
    total_lines = sum(len(item["lines"]) for item in drop_queue)
    total_stock = sum(len(v) for v in stock.values())
    active_jobs = sum(1 for item in drop_queue if not item.get("paused"))
    paused_jobs = len(drop_queue) - active_jobs
    embed = discord.Embed(title="📊 DropBot Status", color=0x2b2d31)
    embed.add_field(name="Drop Jobs", value=f"Active: `{active_jobs}` | Paused: `{paused_jobs}`", inline=False)
    embed.add_field(name="Lines Queued", value=f"`{total_lines}`", inline=True)
    embed.add_field(name="Stock Lines", value=f"`{total_stock}`", inline=True)
    embed.add_field(name="Services", value=f"`{len([s for s in stock if stock[s]])}`", inline=True)
    embed.set_footer(text=f"DropBot • {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    await ctx.send(embed=embed)

@bot.command(name="uptime")
async def cmd_uptime(ctx):
    """!uptime — show how long the bot has been running."""
    now = time.time()
    up = int(now - bot._start_time)
    await ctx.send(f"⏳ Uptime: `{fmt_time(up)}`")

@bot.command(name="ping")
async def cmd_ping(ctx):
    """!ping — check latency."""
    await ctx.send(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")

@bot.command(name="say")
async def cmd_say(ctx, channel: discord.TextChannel, *, msg: str):
    """!say #channel <message> — make the bot say something."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    await channel.send(msg)
    await ctx.message.add_reaction("✅")

@bot.command(name="announce")
async def cmd_announce(ctx, channel: discord.TextChannel, *, msg: str):
    """!announce #channel <message> — send an embed announcement."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    embed = discord.Embed(description=msg, color=0x5865F2)
    embed.set_footer(text=f"Announcement by {ctx.author.display_name}")
    await channel.send(embed=embed)
    await ctx.message.add_reaction("✅")

@bot.command(name="purge")
async def cmd_purge(ctx, amount: int = 10):
    """!purge [amount] — delete last N messages in this channel."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    deleted = await ctx.channel.purge(limit=amount + 1)
    await ctx.send(f"🗑️ Deleted `{len(deleted) - 1}` messages.", delete_after=5)

@bot.command(name="listservices")
async def cmd_listservices(ctx):
    """!listservices — list all services with stock."""
    services = [s for s in sorted(stock) if stock[s]]
    if not services:
        return await ctx.send("📭 No services loaded.")
    await ctx.send("**🗂️ Services with stock:**\n" + ", ".join(f"`{s}`" for s in services))

@bot.command(name="reload")
async def cmd_reload(ctx):
    """!reload — reload state from disk."""
    if not is_admin(ctx):
        return await ctx.send("❌ Admins only.")
    drop_queue.clear()
    for k in list(stock.keys()):
        del stock[k]
    load_state()
    await ctx.send("🔄 State reloaded from disk.")

@bot.command(name="help")
async def cmd_help(ctx):
    """!help — show all commands."""
    embed = discord.Embed(title="📋 DropBot Commands", color=0x5865F2)

    drop_cmds = (
        "`!load #ch <cd> [label]` — load txt file to drop (attach file)\n"
        "`!loadloop #ch <cd> [label]` — same but loops forever\n"
        "`!queue` — show all drop jobs\n"
        "`!peek <id> [n]` — preview next N lines of a job\n"
        "`!pause [id]` — pause job (or all)\n"
        "`!resume [id]` — resume job (or all)\n"
        "`!remove <id>` — delete a drop job\n"
        "`!clearqueue` — delete all drop jobs\n"
        "`!setcooldown <id> <cd>` — change job cooldown\n"
        "`!setchannel <id> #ch` — move job to new channel\n"
        "`!dropadd <id> [lines]` — add more lines to a job\n"
        "`!dropsend #ch <text>` — instantly send to channel\n"
    )
    stock_cmds = (
        "`!addstock <svc>` — load stock from txt file (attach)\n"
        "`!stockadd <svc> l1|l2|...` — add lines inline\n"
        "`!gen <svc>` — generate one account\n"
        "`!genall` — list all services + counts\n"
        "`!stock` — view stock counts\n"
        "`!clearstock [svc]` — clear stock\n"
        "`!stockdump <svc>` — DM yourself full stock\n"
        "`!stockexport <svc>` — export stock as .txt\n"
    )
    util_cmds = (
        "`!status` — bot overview\n"
        "`!uptime` — uptime\n"
        "`!ping` — latency\n"
        "`!say #ch <msg>` — make bot say something\n"
        "`!announce #ch <msg>` — embed announcement\n"
        "`!purge [n]` — delete messages\n"
        "`!listservices` — list loaded services\n"
        "`!reload` — reload state from disk\n"
    )
    cooldown_fmt = (
        "Cooldown format: `30s` / `5m` / `2h` / bare seconds\n"
        "Multiple jobs on different channels all drop independently."
    )
    embed.add_field(name="🗂️ Drop", value=drop_cmds, inline=False)
    embed.add_field(name="📦 Gen / Stock", value=stock_cmds, inline=False)
    embed.add_field(name="🔧 Utility", value=util_cmds, inline=False)
    embed.add_field(name="ℹ️ Cooldown Format", value=cooldown_fmt, inline=False)
    await ctx.send(embed=embed)

# ─────────────────────────────────────────────────────────────────────────────
# STARTUP
# ─────────────────────────────────────────────────────────────────────────────

@bot.event
async def on_connect():
    bot._start_time = time.time()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env var not set.")

bot.run(BOT_TOKEN)
