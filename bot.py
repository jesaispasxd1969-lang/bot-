import json
import os
import random
import sqlite3
import threading
import unicodedata
import urllib.error
import urllib.request
import math
import statistics
import asyncio
import io
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple
from itertools import combinations

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

# Requis pour la génération d'images style Koya
try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("[ERREUR] La librairie Pillow n'est pas installée. Tapez 'pip install Pillow' dans le terminal.")

# ===================== CONFIG =====================
load_dotenv()

TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = os.getenv("DISCORD_GUILD_ID")
DB_PATH = os.getenv("PP_DB_PATH", "pp_bot.sqlite3")

VERIFY_CHANNEL_NAME = os.getenv("VERIFY_CHANNEL_NAME", "verification")
PREP_CHANNEL_NAMES = [
    name.strip()
    for name in os.getenv(
        "PREP_CHANNEL_NAMES",
        "Préparation 1,Préparation 2,Préparation 3,Préparation 4",
    ).split(",")
    if name.strip()
]

VERIFY_CHANNEL_ALIASES = [
    name.strip()
    for name in os.getenv(
        "VERIFY_CHANNEL_ALIASES",
        f"{VERIFY_CHANNEL_NAME},vérification,verification-rank,verif",
    ).split(",")
    if name.strip()
]

# Nouvelles catégories et salons
HOME_CATEGORY_NAME = os.getenv("HOME_CATEGORY_NAME", "⛩️ ・ GRAND PORTAIL ・ ⛩️")
ARTISANS_CATEGORY_NAME = os.getenv("ARTISANS_CATEGORY_NAME", "🏯 ・ QUARTIER DES ARTISANS ・ 🏯")
RANK_CATEGORY_NAME = os.getenv("RANK_CATEGORY_NAME", "🎭 ・ PARTIE PERSO ・ 🎭")
RANK_CHANNEL_NAME = os.getenv("RANK_CHANNEL_NAME", "🎭・choisi-ton-rank")
PARTY_CATEGORY_NAME = os.getenv("PARTY_CATEGORY_NAME", "PARTIE PERSO")
ORGA_TEXT_CHANNEL_NAME = os.getenv("ORGA_TEXT_CHANNEL_NAME", "orga-pp")
WELCOME_CHANNEL_NAME = os.getenv("WELCOME_CHANNEL_NAME", "├🏮・kaminarimon")

# Config Tickets
TICKET_CATEGORY_NAME = os.getenv("TICKET_CATEGORY_NAME", "🏮 ・ TICKETS D'ASAKUSA ・ 🏮")
TICKET_CHANNEL_NAME = os.getenv("TICKET_CHANNEL_NAME", "├🎟️・créer-un-ticket")
METSUKE_ROLE_ID = 1460123520905380117

CUSTOM_VOICE_CATEGORY_ID = int(os.getenv("CUSTOM_VOICE_CATEGORY_ID", "0"))
CUSTOM_VOICE_CATEGORY_NAME = os.getenv("CUSTOM_VOICE_CATEGORY_NAME", ARTISANS_CATEGORY_NAME)
CUSTOM_VOICE_DEFAULT_LIMIT = int(os.getenv("CUSTOM_VOICE_DEFAULT_LIMIT", "0"))

CREATE_VOICE_TRIGGER_NAME = os.getenv("CREATE_VOICE_TRIGGER_NAME", "🔊・Créer un salon")
CREATE_VOICE_TRIGGER_ALIASES = [
    name.strip()
    for name in os.getenv(
        "CREATE_VOICE_TRIGGER_ALIASES",
        f"{CREATE_VOICE_TRIGGER_NAME},creer un salon,+ creer un salon,+ 🔊・Créer un salon",
    ).split(",")
    if name.strip()
]

NON_VERIFIED_ROLE = os.getenv("NON_VERIFIED_ROLE", "Non vérifié")
MEMBER_ROLE = os.getenv("MEMBER_ROLE", "Membre")
ORGA_ROLE = os.getenv("ORGA_ROLE", "Orga PP")
TEAM_ATTACK_ROLE = os.getenv("TEAM_ATTACK_ROLE", "Équipe Attaque")
TEAM_DEFENSE_ROLE = os.getenv("TEAM_DEFENSE_ROLE", "Équipe Défense")
PLAYER_ROLE = os.getenv("PLAYER_ROLE", "🌸・Pèlerin")

VALORANT_MAPS = [
    "Ascent",
    "Bind",
    "Haven",
    "Split",
    "Lotus",
    "Sunset",
    "Icebox",
    "Breeze",
    "Pearl",
    "Fracture",
    "Abyss",
    "Corrode",
    "Summit",
]

VOTE_THRESHOLD_ACCEPT = 5
VOTE_THRESHOLD_REJECT = 5

INTENTS = discord.Intents.default()
INTENTS.guilds = True
INTENTS.members = True
INTENTS.voice_states = True
INTENTS.messages = True
INTENTS.message_content = False
INTENTS.invites = True

RANK_OPTIONS: List[Tuple[str, int]] = [
    ("Fer 1", 100), ("Fer 2", 110), ("Fer 3", 120),
    ("Bronze 1", 200), ("Bronze 2", 210), ("Bronze 3", 220),
    ("Argent 1", 300), ("Argent 2", 310), ("Argent 3", 320),
    ("Or 1", 400), ("Or 2", 410), ("Or 3", 420),
    ("Platine 1", 500), ("Platine 2", 510), ("Platine 3", 520),
    ("Diamant 1", 600), ("Diamant 2", 610), ("Diamant 3", 620),
    ("Ascendant 1", 700), ("Ascendant 2", 710), ("Ascendant 3", 720),
    ("Immortal 1", 800), ("Immortal 2", 810), ("Immortal 3", 820),
    ("Radiant", 900),
]
RANK_VALUE_BY_NAME = {name: value for name, value in RANK_OPTIONS}

RANK_EMOJI_BY_NAME = {
    "Fer 1": "Iron_1_Rank",
    "Fer 2": "Iron_2_Rank",
    "Fer 3": "Iron_3_Rank",
    "Bronze 1": "Bronze_1_Rank",
    "Bronze 2": "Bronze_2_Rank",
    "Bronze 3": "Bronze_3_Rank",
    "Argent 1": "Silver_1_Rank",
    "Argent 2": "Silver_2_Rank",
    "Argent 3": "Silver_3_Rank",
    "Or 1": "Gold_1_Rank",
    "Or 2": "Gold_2_Rank",
    "Or 3": "Gold_3_Rank",
    "Platine 1": "Platinum_1_Rank",
    "Platine 2": "Platinum_2_Rank",
    "Platine 3": "Platinum_3_Rank",
    "Diamant 1": "Diamond_1_Rank",
    "Diamant 2": "Diamond_2_Rank",
    "Diamant 3": "Diamond_3_Rank",
    "Ascendant 1": "Ascendant_1_Rank",
    "Ascendant 2": "Ascendant_2_Rank",
    "Ascendant 3": "Ascendant_3_Rank",
    "Immortal 1": "Immortal_1_Rank",
    "Immortal 2": "Immortal_2_Rank",
    "Immortal 3": "Immortal_3_Rank",
    "Radiant": "Radiant_Rank",
}

RANK_TIER_EMOJI = {
    "Fer": "⚫",
    "Bronze": "🥉",
    "Argent": "🥈",
    "Or": "🥇",
    "Platine": "🔷",
    "Diamant": "💎",
    "Ascendant": "🟢",
    "Immortal": "👑",
    "Radiant": "🌟",
}

MAP_IMAGE: Dict[str, str] = {
    "Haven":    "https://media.valorant-api.com/maps/2bee0dc9-4ffe-519b-1cbd-7fbe763a6047/splash.png",
    "Corrode":  "https://media.valorant-api.com/maps/1c18ab1f-420d-0d8b-71d0-77ad3c439115/splash.png",
    "Icebox":   "https://media.valorant-api.com/maps/e2ad5c54-4114-a870-9641-8ea21279579a/splash.png",
    "Pearl":    "https://media.valorant-api.com/maps/fd267378-4d1d-484f-ff52-77821ed10dc2/splash.png",
    "Sunset":   "https://media.valorant-api.com/maps/92584fbe-486a-b1b2-9faa-39b0f486b498/splash.png",
    "Lotus":    "https://media.valorant-api.com/maps/2fe4ed3a-450a-948b-6d6b-e89a78e680a9/splash.png",
    "Abyss":    "https://media.valorant-api.com/maps/224b0a95-48b9-f703-1bd8-67aca101a61f/splash.png",
    "Breeze":   "https://media.valorant-api.com/maps/2fb9a4fd-47b8-4e7d-a969-74b4046ebd53/splash.png",
    "Ascent":   "https://media.valorant-api.com/maps/7eaecc1b-4337-bbf6-6ab9-04b8f06b3319/splash.png",
    "Split":    "https://media.valorant-api.com/maps/d960549e-485c-e861-8d71-aa9d1aed12a2/splash.png",
    "Fracture": "https://media.valorant-api.com/maps/b529448b-4d60-346e-e89e-00a4c527a405/splash.png",
    "Bind":     "https://media.valorant-api.com/maps/2c9d57ec-4431-9c5e-2939-8f9ef6dd5cba/splash.png",
    "Summit":   "https://cdn.discordapp.com/attachments/1460123533828030699/1531838845157638164/1200px-Loading_Screen_Summit.png?ex=6a6aab98&is=6a695a18&hm=5cea97a069046a65a7b5cd94f4dcd2b84bffc1e3d925b912f966f46dafd127ed&",
}

# Ordre d'arrivée dans chaque vocal Préparation.
JOIN_SEQUENCE = 0
PREP_JOIN_ORDER: Dict[int, Dict[int, int]] = {}

def next_join_sequence() -> int:
    global JOIN_SEQUENCE
    JOIN_SEQUENCE += 1
    return JOIN_SEQUENCE

def slug(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    for sep in ["・", "|", "—", "-", "•", "·", "_", "/"]:
        text = text.replace(sep, " ")
    return " ".join(text.lower().split())

# ===================== DATABASE =====================
class Database:
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init_schema()

    def init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                rank_name TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS active_matches (
                prep_channel_id INTEGER PRIMARY KEY,
                started_by_id INTEGER NOT NULL,
                ui_message_id INTEGER NOT NULL,
                party_code TEXT NOT NULL,
                map_name TEXT NOT NULL,
                attack_ids TEXT NOT NULL,
                defense_ids TEXT NOT NULL,
                map_yes INTEGER NOT NULL DEFAULT 0,
                map_no INTEGER NOT NULL DEFAULT 0,
                map_locked INTEGER NOT NULL DEFAULT 0,
                map_voters TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        existing_columns = {row[1] for row in cur.execute("PRAGMA table_info(active_matches)").fetchall()}
        migrations = {
            "map_yes": "ALTER TABLE active_matches ADD COLUMN map_yes INTEGER NOT NULL DEFAULT 0",
            "map_no": "ALTER TABLE active_matches ADD COLUMN map_no INTEGER NOT NULL DEFAULT 0",
            "map_locked": "ALTER TABLE active_matches ADD COLUMN map_locked INTEGER NOT NULL DEFAULT 0",
            "map_voters": "ALTER TABLE active_matches ADD COLUMN map_voters TEXT NOT NULL DEFAULT '{}'",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                cur.execute(statement)

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_voice_rooms (
                channel_id INTEGER PRIMARY KEY,
                owner_id INTEGER NOT NULL
            )
            """
        )
        self.conn.commit()

    def upsert_player_rank(self, user_id: int, rank_name: str) -> None:
        self.conn.execute(
            """
            INSERT INTO players (user_id, rank_name)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET rank_name = excluded.rank_name
            """,
            (user_id, rank_name),
        )
        self.conn.commit()

    def get_player_rank(self, user_id: int) -> Optional[str]:
        row = self.conn.execute(
            "SELECT rank_name FROM players WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return row[0] if row else None

    def register_custom_voice(self, channel_id: int, owner_id: int) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO custom_voice_rooms (channel_id, owner_id) VALUES (?, ?)",
            (channel_id, owner_id),
        )
        self.conn.commit()

    def get_custom_voice_owner(self, channel_id: int) -> Optional[int]:
        row = self.conn.execute(
            "SELECT owner_id FROM custom_voice_rooms WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return int(row[0]) if row else None

    def delete_custom_voice(self, channel_id: int) -> None:
        self.conn.execute("DELETE FROM custom_voice_rooms WHERE channel_id = ?", (channel_id,))
        self.conn.commit()

    def save_active_match(
        self,
        prep_channel_id: int,
        started_by_id: int,
        ui_message_id: int,
        party_code: str,
        map_name: str,
        attack_ids: List[int],
        defense_ids: List[int],
        map_yes: int,
        map_no: int,
        map_locked: bool,
        map_voters: Dict[str, str],
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO active_matches (
                prep_channel_id, started_by_id, ui_message_id, party_code,
                map_name, attack_ids, defense_ids, map_yes, map_no, map_locked, map_voters
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prep_channel_id,
                started_by_id,
                ui_message_id,
                party_code,
                map_name,
                json.dumps(attack_ids),
                json.dumps(defense_ids),
                map_yes,
                map_no,
                int(map_locked),
                json.dumps(map_voters),
            ),
        )
        self.conn.commit()

    def get_active_match(self, prep_channel_id: int) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM active_matches WHERE prep_channel_id = ?",
            (prep_channel_id,),
        ).fetchone()

    def delete_active_match(self, prep_channel_id: int) -> None:
        self.conn.execute(
            "DELETE FROM active_matches WHERE prep_channel_id = ?",
            (prep_channel_id,),
        )
        self.conn.commit()


db = Database(DB_PATH)


@dataclass
class MatchState:
    prep_channel_id: int
    started_by_id: int
    ui_message_id: int
    party_code: str
    map_name: str
    attack_ids: List[int]
    defense_ids: List[int]
    map_yes: int
    map_no: int
    map_locked: bool
    map_voters: Dict[str, str]

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MatchState":
        raw_voters = row["map_voters"] if "map_voters" in row.keys() else "{}"
        return cls(
            prep_channel_id=row["prep_channel_id"],
            started_by_id=row["started_by_id"],
            ui_message_id=row["ui_message_id"],
            party_code=row["party_code"],
            map_name=row["map_name"],
            attack_ids=json.loads(row["attack_ids"]),
            defense_ids=json.loads(row["defense_ids"]),
            map_yes=row["map_yes"] if "map_yes" in row.keys() else 0,
            map_no=row["map_no"] if "map_no" in row.keys() else 0,
            map_locked=bool(row["map_locked"]) if "map_locked" in row.keys() else False,
            map_voters=json.loads(raw_voters or "{}"),
        )


# ===================== IMAGE GENERATION =====================
def get_text_dimensions(text, font, draw):
    # Compatibilité entre les versions anciennes et récentes de Pillow
    try:
        bbox = draw.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1]
    except AttributeError:
        return font.getsize(text)

async def generate_welcome_card(member: discord.Member) -> io.BytesIO:
    # 1. Téléchargement de la police Montserrat-Black
    font_path_title = "Montserrat-Black.ttf"
    if not os.path.exists(font_path_title):
        try:
            urllib.request.urlretrieve("https://raw.githubusercontent.com/google/fonts/main/ofl/montserrat/Montserrat-Black.ttf", font_path_title)
        except Exception as e:
            print(f"[WARN] Téléchargement police échoué : {e}")

    # 2. Arrière-plan
    bg_url = "https://cdn.discordapp.com/attachments/1460123533828030699/1533549541972902030/a0e0ef14cf5902013f6c12e94e79e45f.png?ex=6a70e4ce&is=6a6f934e&hm=3c2e90efd79c22aff07b073e636d21525ef46595a6d82f2df5bf57d2505527e7&"
    try:
        req = urllib.request.Request(bg_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            bg_bytes = response.read()
        bg = Image.open(io.BytesIO(bg_bytes)).convert("RGBA")
        bg = bg.resize((800, 400)) # Format bannière large
    except Exception:
        # Fallback si l'image ne charge pas
        bg = Image.new("RGBA", (800, 400), (20, 22, 28, 255))
        
    draw = ImageDraw.Draw(bg)

    # 3. Avatar Processing (Image très grande, parfaitement centrée)
    # Taille de l'avatar augmentée à 240x240
    avatar_size = 240
    avatar_bytes = await member.display_avatar.replace(size=256, format="png").read()
    avatar = Image.open(io.BytesIO(avatar_bytes)).convert("RGBA")
    avatar = avatar.resize((avatar_size, avatar_size))

    mask = Image.new("L", (avatar_size, avatar_size), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.ellipse((0, 0, avatar_size, avatar_size), fill=255)
    
    circular_avatar = Image.new("RGBA", (avatar_size, avatar_size))
    circular_avatar.paste(avatar, (0, 0), mask)

    # Création du contour (Border rouge, +16px plus grand que l'avatar)
    border_size = avatar_size + 16
    border_mask = Image.new("RGBA", (border_size, border_size), (0, 0, 0, 0))
    border_draw = ImageDraw.Draw(border_mask)
    border_draw.ellipse((0, 0, border_size, border_size), fill=(231, 76, 60, 255))
    
    # Calcul du centrage parfait sur le canvas de 800x400
    # Le centre de l'avatar est abaissé légèrement pour laisser de la place au gros texte
    avatar_x = (800 - avatar_size) // 2
    avatar_y = 135
    
    border_x = (800 - border_size) // 2
    border_y = avatar_y - 8
    
    bg.paste(border_mask, (border_x, border_y), border_mask)
    bg.paste(circular_avatar, (avatar_x, avatar_y), circular_avatar)

    # 4. Texte BIENVENUE PSEUDO (Géant, au-dessus de l'avatar)
    text_title = f"BIENVENUE {member.display_name.upper()} !"
    
    title_size = 110 # Taille gigantesque
    if os.path.exists(font_path_title):
        font_title = ImageFont.truetype(font_path_title, title_size)
    else:
        font_title = ImageFont.load_default()

    # Redimensionnement dynamique pour ne pas dépasser les 800px de large
    title_width, title_height = get_text_dimensions(text_title, font_title, draw)
    while title_width > 770 and title_size > 30:
        title_size -= 4
        font_title = ImageFont.truetype(font_path_title, title_size) if os.path.exists(font_path_title) else font_title
        title_width, title_height = get_text_dimensions(text_title, font_title, draw)

    title_x = (800 - title_width) / 2
    title_y = 20 # Tout en haut

    # Épaisseur du contour adaptatif
    outline_width = max(3, title_size // 15)

    def draw_text_with_outline(x, y, text, font, text_color, outline_color, out_w):
        for adj_x in range(-out_w, out_w + 1):
            for adj_y in range(-out_w, out_w + 1):
                if adj_x == 0 and adj_y == 0:
                    continue
                draw.text((x + adj_x, y + adj_y), text, font=font, fill=outline_color)
        draw.text((x, y), text, font=font, fill=text_color)

    # Contour noir énorme, texte blanc
    draw_text_with_outline(title_x, title_y, text_title, font_title, (255, 255, 255), (0, 0, 0), outline_width)

    buffer = io.BytesIO()
    bg.convert("RGB").save(buffer, format="PNG")
    buffer.seek(0)
    return buffer


# ===================== HELPERS =====================
def tier_emoji(rank_name: str) -> str:
    tier = rank_name.split()[0]
    return RANK_TIER_EMOJI.get(tier, "🎯")


def rank_select_emoji(guild: Optional[discord.Guild], rank_name: str):
    if guild is not None:
        emoji_name = RANK_EMOJI_BY_NAME.get(rank_name)
        if emoji_name:
            custom_emoji = discord.utils.get(guild.emojis, name=emoji_name)
            if custom_emoji is not None:
                return custom_emoji
    return tier_emoji(rank_name)


def rank_value_for_member(member: discord.Member) -> int:
    stored = db.get_player_rank(member.id)
    best = 0
    if stored and stored in RANK_VALUE_BY_NAME:
        best = RANK_VALUE_BY_NAME[stored]
    for role in member.roles:
        if role.name in RANK_VALUE_BY_NAME:
            best = max(best, RANK_VALUE_BY_NAME[role.name])
    return best


def is_prep_voice(channel: Optional[discord.abc.GuildChannel]) -> bool:
    return isinstance(channel, discord.VoiceChannel) and slug(channel.name) in {slug(n) for n in PREP_CHANNEL_NAMES}


def find_category(guild: discord.Guild, name: str) -> Optional[discord.CategoryChannel]:
    return discord.utils.find(
        lambda c: isinstance(c, discord.CategoryChannel) and slug(c.name) == slug(name),
        guild.categories,
    )


def find_text_channel(guild: discord.Guild, aliases: List[str], *, category: Optional[discord.CategoryChannel] = None) -> Optional[discord.TextChannel]:
    wanted = {slug(name) for name in aliases if name}
    channels = category.text_channels if category is not None else guild.text_channels
    return discord.utils.find(
        lambda c: isinstance(c, discord.TextChannel) and slug(c.name) in wanted,
        channels,
    )


def get_verify_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    aliases = [VERIFY_CHANNEL_NAME, *VERIFY_CHANNEL_ALIASES]
    return find_text_channel(guild, aliases)


def get_rank_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    category = find_category(guild, RANK_CATEGORY_NAME)
    return find_text_channel(guild, [RANK_CHANNEL_NAME, "choisi-ton-rank"], category=category) or find_text_channel(guild, [RANK_CHANNEL_NAME])


def get_welcome_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    home_category = find_category(guild, HOME_CATEGORY_NAME)
    aliases = [WELCOME_CHANNEL_NAME, "kaminarimon", "bienvenue", "welcome"]
    return find_text_channel(guild, aliases, category=home_category) or find_text_channel(guild, aliases)


def is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def has_orga_access(member: discord.Member) -> bool:
    return is_admin(member) or any(r.name == ORGA_ROLE for r in member.roles)


def is_verified_member(member: discord.Member) -> bool:
    return any(r.name == PLAYER_ROLE for r in member.roles) or has_orga_access(member)


def is_custom_voice(channel: Optional[discord.abc.GuildChannel]) -> bool:
    return isinstance(channel, discord.VoiceChannel) and db.get_custom_voice_owner(channel.id) is not None


def is_create_voice_trigger(channel: Optional[discord.abc.GuildChannel]) -> bool:
    if not isinstance(channel, discord.VoiceChannel): return False
    return slug(channel.name) in {slug(n) for n in CREATE_VOICE_TRIGGER_ALIASES}


def custom_voice_locked(channel: discord.VoiceChannel) -> bool:
    player_role = discord.utils.get(channel.guild.roles, name=PLAYER_ROLE)
    if player_role is None:
        return False
    overwrite = channel.overwrites_for(player_role)
    return overwrite.connect is False


def can_manage_custom_voice(member: discord.Member, channel: Optional[discord.VoiceChannel]) -> bool:
    if not isinstance(channel, discord.VoiceChannel):
        return False
    owner_id = db.get_custom_voice_owner(channel.id)
    return owner_id is not None and (member.id == owner_id or has_orga_access(member))


async def ensure_role(guild: discord.Guild, role_name: str, *, color: Optional[discord.Color] = None) -> discord.Role:
    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        role = await guild.create_role(name=role_name, color=color or discord.Color.default(), reason="PP setup")
    return role


async def ensure_core_roles(guild: discord.Guild) -> Dict[str, discord.Role]:
    player_role = await ensure_role(guild, PLAYER_ROLE)
    roles = {
        "non_verified": await ensure_role(guild, NON_VERIFIED_ROLE),
        "member": player_role,
        "orga": await ensure_role(guild, ORGA_ROLE),
        "attack": await ensure_role(guild, TEAM_ATTACK_ROLE),
        "defense": await ensure_role(guild, TEAM_DEFENSE_ROLE),
        "player": player_role,
    }
    if MEMBER_ROLE != PLAYER_ROLE:
        await ensure_role(guild, MEMBER_ROLE)
    for rank_name, _ in RANK_OPTIONS:
        await ensure_role(guild, rank_name)
    return roles

async def sync_existing_membership_roles(guild: discord.Guild) -> None:
    roles = await ensure_core_roles(guild)
    verified_role = roles["player"]
    non_verified_role = roles["non_verified"]

    for member in guild.members:
        if member.bot:
            continue
        if non_verified_role in member.roles:
            continue
        if verified_role in member.roles:
            continue
        try:
            await member.add_roles(verified_role, reason="PP setup membership sync")
        except discord.Forbidden:
            pass

async def _safe_set_permissions(channel: discord.abc.GuildChannel, target: discord.abc.Snowflake, **kwargs) -> None:
    try:
        await channel.set_permissions(target, reason="PP access setup", **kwargs)
    except discord.Forbidden:
        pass


async def _configure_text_channel(
    channel: discord.TextChannel,
    *,
    default_role: discord.Role,
    non_verified: discord.Role,
    member: discord.Role,
    orga: discord.Role,
    member_can_write: bool,
    visible_to_member: bool = True,
    visible_to_orga: bool = True,
) -> None:
    await _safe_set_permissions(channel, default_role, view_channel=False, send_messages=False, add_reactions=False)
    await _safe_set_permissions(channel, non_verified, view_channel=False, send_messages=False, add_reactions=False)
    await _safe_set_permissions(
        channel,
        member,
        view_channel=visible_to_member,
        send_messages=member_can_write if visible_to_member else False,
        add_reactions=member_can_write if visible_to_member else False,
        read_message_history=visible_to_member,
        use_application_commands=visible_to_member,
    )
    await _safe_set_permissions(
        channel,
        orga,
        view_channel=visible_to_orga,
        send_messages=visible_to_orga,
        add_reactions=visible_to_orga,
        read_message_history=visible_to_orga,
        use_application_commands=visible_to_orga,
        manage_messages=visible_to_orga,
    )


async def _configure_voice_channel(
    channel: discord.VoiceChannel,
    *,
    default_role: discord.Role,
    non_verified: discord.Role,
    member: discord.Role,
    orga: discord.Role,
    member_can_connect: bool = True,
    orga_can_connect: bool = True,
) -> None:
    await _safe_set_permissions(channel, default_role, view_channel=False, connect=False, send_messages=False)
    await _safe_set_permissions(channel, non_verified, view_channel=False, connect=False, send_messages=False)
    await _safe_set_permissions(
        channel,
        member,
        view_channel=True,
        connect=member_can_connect,
        speak=member_can_connect,
        stream=member_can_connect,
        use_voice_activation=member_can_connect,
        send_messages=True,
        read_message_history=True,
        use_application_commands=True,
    )
    await _safe_set_permissions(
        channel,
        orga,
        view_channel=True,
        connect=orga_can_connect,
        speak=orga_can_connect,
        stream=orga_can_connect,
        use_voice_activation=orga_can_connect,
        send_messages=True,
        read_message_history=True,
        use_application_commands=True,
        move_members=True,
        mute_members=True,
        deafen_members=True,
    )


async def set_verification_permissions(guild: discord.Guild) -> None:
    roles = await ensure_core_roles(guild)
    await sync_existing_membership_roles(guild)

    default_role = guild.default_role
    non_verified = roles["non_verified"]
    member = roles["member"]
    orga = roles["orga"]

    try:
        perms = non_verified.permissions
        perms.update(read_messages=False, send_messages=False, connect=False)
        await non_verified.edit(permissions=perms, reason="PP Setup: Default block for non verified")
    except discord.Forbidden:
        pass

    verify_channel = get_verify_channel(guild)
    rank_channel = get_rank_channel(guild)
    party_category = find_category(guild, PARTY_CATEGORY_NAME)
    orga_channel = find_text_channel(guild, [ORGA_TEXT_CHANNEL_NAME, "orga pp"], category=party_category)

    # Configuration du salon de vérification (Anti-Robot)
    if verify_channel is not None:
        await _safe_set_permissions(verify_channel, default_role, view_channel=False, send_messages=False, add_reactions=False)
        await _safe_set_permissions(verify_channel, member, view_channel=False, send_messages=False, add_reactions=False)
        await _safe_set_permissions(
            verify_channel,
            non_verified,
            view_channel=True,
            send_messages=False,
            add_reactions=False,
            read_message_history=True,
            use_application_commands=True,
        )
        await _safe_set_permissions(
            verify_channel,
            orga,
            view_channel=True,
            send_messages=True,
            add_reactions=True,
            read_message_history=True,
            use_application_commands=True,
            manage_messages=True,
        )

    # Configuration du salon de choix de rank
    if rank_channel is not None:
        await _safe_set_permissions(rank_channel, default_role, view_channel=False, send_messages=False, add_reactions=False)
        await _safe_set_permissions(rank_channel, non_verified, view_channel=False, send_messages=False, add_reactions=False)
        await _safe_set_permissions(
            rank_channel,
            member,
            view_channel=True,
            send_messages=False,
            add_reactions=False,
            read_message_history=True,
            use_application_commands=True,
        )
        await _safe_set_permissions(
            rank_channel,
            orga,
            view_channel=True,
            send_messages=True,
            add_reactions=True,
            read_message_history=True,
            use_application_commands=True,
            manage_messages=True,
        )

    if orga_channel is not None:
        await _configure_text_channel(
            orga_channel,
            default_role=default_role,
            non_verified=non_verified,
            member=member,
            orga=orga,
            member_can_write=False,
            visible_to_member=False,
            visible_to_orga=True,
        )

    for channel_name in PREP_CHANNEL_NAMES:
        prep = discord.utils.find(
            lambda c: isinstance(c, discord.VoiceChannel) and slug(c.name) == slug(channel_name),
            guild.channels,
        )
        if prep is not None:
            await _configure_voice_channel(
                prep,
                default_role=default_role,
                non_verified=non_verified,
                member=member,
                orga=orga,
            )


async def set_custom_voice_permissions(channel: discord.VoiceChannel, *, owner: discord.Member, locked: bool = False) -> None:
    roles = await ensure_core_roles(channel.guild)
    player = roles["player"]
    orga = roles["orga"]
    await _safe_set_permissions(channel, channel.guild.default_role, view_channel=False, connect=False, send_messages=False)
    await _safe_set_permissions(channel, roles["non_verified"], view_channel=False, connect=False, send_messages=False)
    await _safe_set_permissions(
        channel,
        player,
        view_channel=True,
        connect=not locked,
        speak=True,
        stream=True,
        use_voice_activation=True,
        send_messages=True,
        read_message_history=True,
        use_application_commands=True,
    )
    await _safe_set_permissions(
        channel,
        orga,
        view_channel=True,
        connect=True,
        speak=True,
        stream=True,
        use_voice_activation=True,
        send_messages=True,
        read_message_history=True,
        use_application_commands=True,
        move_members=True,
        manage_channels=True,
        mute_members=True,
        deafen_members=True,
    )
    await _safe_set_permissions(
        channel,
        owner,
        view_channel=True,
        connect=True,
        speak=True,
        stream=True,
        use_voice_activation=True,
        send_messages=True,
        read_message_history=True,
        use_application_commands=True,
        move_members=True,
        manage_channels=True,
        priority_speaker=True,
    )


async def create_custom_voice_channel(guild: discord.Guild, owner: discord.Member, name: str, user_limit: int = 0) -> discord.VoiceChannel:
    category = guild.get_channel(CUSTOM_VOICE_CATEGORY_ID)
    if not isinstance(category, discord.CategoryChannel):
        category = find_category(guild, CUSTOM_VOICE_CATEGORY_NAME) or find_category(guild, ARTISANS_CATEGORY_NAME)
    channel = await guild.create_voice_channel(name=name, category=category, user_limit=max(0, min(99, user_limit)))
    db.register_custom_voice(channel.id, owner.id)
    await set_custom_voice_permissions(channel, owner=owner, locked=False)
    try:
        await owner.move_to(channel)
    except (discord.Forbidden, discord.HTTPException):
        pass
    return channel


async def cleanup_custom_voice_if_empty(channel: discord.VoiceChannel) -> None:
    if is_custom_voice(channel) and len(channel.members) == 0:
        db.delete_custom_voice(channel.id)
        try:
            await channel.delete(reason="Temporary custom voice empty")
        except (discord.Forbidden, discord.HTTPException):
            pass


async def _build_custom_voice_panel_embed(channel: discord.VoiceChannel) -> discord.Embed:
    owner_id = db.get_custom_voice_owner(channel.id)
    owner = channel.guild.get_member(owner_id) if owner_id else None
    embed = discord.Embed(
        title=f"🎤 {channel.name}",
        description=(
            "Bienvenue dans ton salon privé.\n"
            "Utilise les boutons ci-dessous pour **verrouiller**, **renommer**, "
            "**changer les slots** ou **expulser** quelqu’un de la voc."
        ),
        color=discord.Color.dark_gold(),
    )
    embed.add_field(name="Propriétaire", value=owner.mention if owner else "Inconnu", inline=True)
    embed.add_field(name="État", value="🔒 Verrouillé" if custom_voice_locked(channel) else "🔓 Ouvert", inline=True)
    embed.add_field(name="Slots", value=str(channel.user_limit) if channel.user_limit else "∞", inline=True)
    embed.set_footer(text="Réservé au propriétaire du salon, Orga PP ou admin.")
    return embed


async def ensure_custom_voice_panel(channel: discord.VoiceChannel) -> None:
    try:
        async for msg in channel.history(limit=30):
            if msg.author == channel.guild.me and msg.components:
                return
    except (discord.Forbidden, discord.HTTPException):
        return

    try:
        msg = await channel.send(embed=await _build_custom_voice_panel_embed(channel), view=CustomVoiceControlView())
        try:
            await msg.pin()
        except (discord.Forbidden, discord.HTTPException):
            pass
    except (discord.Forbidden, discord.HTTPException):
        pass


async def refresh_custom_voice_panel(channel: discord.VoiceChannel) -> None:
    try:
        async for msg in channel.history(limit=30):
            if msg.author == channel.guild.me and msg.components:
                await msg.edit(embed=await _build_custom_voice_panel_embed(channel), view=CustomVoiceControlView())
                return
    except (discord.Forbidden, discord.HTTPException):
        return


async def apply_rank(member: discord.Member, rank_name: str) -> None:
    roles = await ensure_core_roles(member.guild)
    rank_role = discord.utils.get(member.guild.roles, name=rank_name)
    if rank_role is None:
        rank_role = await ensure_role(member.guild, rank_name)

    to_remove = [r for r in member.roles if r.name in RANK_VALUE_BY_NAME or r == roles["non_verified"]]
    if to_remove:
        try:
            await member.remove_roles(*to_remove, reason="PP rank verification")
        except discord.Forbidden:
            pass

    add_roles = [rank_role, roles["player"]]
    missing = [r for r in add_roles if r not in member.roles]
    if missing:
        try:
            await member.add_roles(*missing, reason="PP rank verification")
        except discord.Forbidden:
            pass

    db.upsert_player_rank(member.id, rank_name)


async def clear_team_roles(guild: discord.Guild, members: Optional[List[discord.Member]] = None) -> None:
    attack_role = discord.utils.get(guild.roles, name=TEAM_ATTACK_ROLE)
    defense_role = discord.utils.get(guild.roles, name=TEAM_DEFENSE_ROLE)
    if attack_role is None or defense_role is None:
        return

    targets = members or list(guild.members)
    for member in targets:
        to_remove = [r for r in (attack_role, defense_role) if r in member.roles]
        if to_remove:
            try:
                await member.remove_roles(*to_remove, reason="PP team reset")
            except discord.Forbidden:
                pass


async def apply_team_roles(guild: discord.Guild, attack: List[discord.Member], defense: List[discord.Member]) -> None:
    attack_role = discord.utils.get(guild.roles, name=TEAM_ATTACK_ROLE)
    defense_role = discord.utils.get(guild.roles, name=TEAM_DEFENSE_ROLE)
    if attack_role is None or defense_role is None:
        return

    await clear_team_roles(guild, attack + defense)
    for member in attack:
        try:
            await member.add_roles(attack_role, reason="PP teams")
        except discord.Forbidden:
            pass
    for member in defense:
        try:
            await member.add_roles(defense_role, reason="PP teams")
        except discord.Forbidden:
            pass


async def seed_existing_prep_members(guilds: List[discord.Guild]) -> None:
    for guild in guilds:
        for channel in guild.voice_channels:
            if not is_prep_voice(channel):
                continue
            for member in channel.members:
                if member.bot:
                    continue
                remember_member_in_prep(channel, member)


def remember_member_in_prep(channel: discord.VoiceChannel, member: discord.Member) -> None:
    PREP_JOIN_ORDER.setdefault(channel.id, {})[member.id] = PREP_JOIN_ORDER.get(channel.id, {}).get(member.id, next_join_sequence())


def forget_member_from_prep(channel: discord.VoiceChannel, member: discord.Member) -> None:
    PREP_JOIN_ORDER.get(channel.id, {}).pop(member.id, None)


def ordered_prep_members(channel: discord.VoiceChannel) -> List[discord.Member]:
    order_map = PREP_JOIN_ORDER.setdefault(channel.id, {})
    members = [m for m in channel.members if not m.bot]
    for member in members:
        if member.id not in order_map:
            order_map[member.id] = next_join_sequence()
    return sorted(members, key=lambda m: (order_map.get(m.id, 10**12), m.display_name.lower()))


def _effective_player_skill(member: discord.Member) -> float:
    raw = float(max(1, rank_value_for_member(member)))
    return (raw ** 1.12) + (22.0 * math.log1p(raw)) + (8.0 * math.sqrt(raw))


def _team_balance_cost(team_a: List[discord.Member], team_b: List[discord.Member]) -> float:
    skills_a = sorted((_effective_player_skill(m) for m in team_a), reverse=True)
    skills_b = sorted((_effective_player_skill(m) for m in team_b), reverse=True)

    sum_a, sum_b = sum(skills_a), sum(skills_b)
    mean_a, mean_b = statistics.fmean(skills_a), statistics.fmean(skills_b)
    stdev_a = statistics.pstdev(skills_a) if len(skills_a) > 1 else 0.0
    stdev_b = statistics.pstdev(skills_b) if len(skills_b) > 1 else 0.0

    top2_a, top2_b = sum(skills_a[:2]), sum(skills_b[:2])
    bot2_a, bot2_b = sum(skills_a[-2:]), sum(skills_b[-2:])
    median_a, median_b = statistics.median(skills_a), statistics.median(skills_b)

    return (
        abs(sum_a - sum_b)
        + 0.65 * abs(mean_a - mean_b)
        + 0.40 * abs(stdev_a - stdev_b)
        + 0.55 * abs(top2_a - top2_b)
        + 0.35 * abs(bot2_a - bot2_b)
        + 0.25 * abs(median_a - median_b)
    )


def split_balanced_teams(members: List[discord.Member]) -> Tuple[List[discord.Member], List[discord.Member]]:
    if len(members) != 10:
        scored = sorted(members, key=rank_value_for_member, reverse=True)
        midpoint = len(scored) // 2
        return scored[:midpoint], scored[midpoint:]

    indexed = list(enumerate(members))
    best_attack: List[discord.Member] = []
    best_defense: List[discord.Member] = []
    best_cost = float('inf')
    best_raw_gap = float('inf')

    for combo in combinations(indexed, 5):
        attack_indices = {idx for idx, _ in combo}
        attack = [member for idx, member in indexed if idx in attack_indices]
        defense = [member for idx, member in indexed if idx not in attack_indices]

        cost = _team_balance_cost(attack, defense)
        raw_gap = abs(sum(rank_value_for_member(m) for m in attack) - sum(rank_value_for_member(m) for m in defense))

        if cost < best_cost - 1e-9 or (abs(cost - best_cost) <= 1e-9 and raw_gap < best_raw_gap):
            best_cost = cost
            best_raw_gap = raw_gap
            best_attack = attack
            best_defense = defense

    return best_attack, best_defense


def get_associated_team_channels(prep_channel: discord.VoiceChannel) -> Tuple[Optional[discord.VoiceChannel], Optional[discord.VoiceChannel]]:
    category = prep_channel.category
    if category is None:
        return None, None

    voices = sorted(category.voice_channels, key=lambda c: c.position)
    try:
        prep_index = next(i for i, vc in enumerate(voices) if vc.id == prep_channel.id)
    except StopIteration:
        return None, None

    next_prep_index = len(voices)
    for i in range(prep_index + 1, len(voices)):
        if is_prep_voice(voices[i]):
            next_prep_index = i
            break

    attack = None
    defense = None
    for vc in voices[prep_index + 1:next_prep_index]:
        name = slug(vc.name)
        if "attaque" in name or "atk" in name or name.endswith("att"):
            attack = vc
        if "defense" in name or "def" in name:
            defense = vc
    return attack, defense


async def move_teams_if_possible(prep_channel: discord.VoiceChannel, attack: List[discord.Member], defense: List[discord.Member]) -> None:
    attack_vc, defense_vc = get_associated_team_channels(prep_channel)
    if attack_vc is None or defense_vc is None:
        return

    for member in attack:
        if member.voice and member.voice.channel and member.voice.channel.id == prep_channel.id:
            try:
                await member.move_to(attack_vc, reason="PP move attack")
            except discord.Forbidden:
                pass

    for member in defense:
        if member.voice and member.voice.channel and member.voice.channel.id == prep_channel.id:
            try:
                await member.move_to(defense_vc, reason="PP move defense")
            except discord.Forbidden:
                pass


def pick_map(exclude: Optional[str] = None) -> str:
    pool = [m for m in VALORANT_MAPS if m != exclude]
    return random.choice(pool or VALORANT_MAPS)


def map_image_url(map_name: str) -> Optional[str]:
    return MAP_IMAGE.get(map_name)


def load_match_state(prep_channel_id: int) -> Optional[MatchState]:
    row = db.get_active_match(prep_channel_id)
    return MatchState.from_row(row) if row else None


def is_match_controller(member: discord.Member, state: MatchState) -> bool:
    if member.guild_permissions.administrator:
        return True
    if member.id == state.started_by_id:
        return True
    return any(role.name == ORGA_ROLE for role in member.roles)


def format_mentions(members: List[discord.Member]) -> str:
    return "\n".join(member.mention for member in members) if members else "—"


def persist_match_state(state: MatchState) -> None:
    db.save_active_match(
        prep_channel_id=state.prep_channel_id,
        started_by_id=state.started_by_id,
        ui_message_id=state.ui_message_id,
        party_code=state.party_code,
        map_name=state.map_name,
        attack_ids=state.attack_ids,
        defense_ids=state.defense_ids,
        map_yes=state.map_yes,
        map_no=state.map_no,
        map_locked=state.map_locked,
        map_voters=state.map_voters,
    )


def build_match_embeds(guild: discord.Guild, state: MatchState) -> List[discord.Embed]:
    prep_channel = guild.get_channel(state.prep_channel_id)
    prep_name = prep_channel.name if isinstance(prep_channel, discord.VoiceChannel) else "Préparation"
    current_members = ordered_prep_members(prep_channel) if isinstance(prep_channel, discord.VoiceChannel) else []
    selected_members = current_members[:10]
    waiting_members = current_members[10:]

    status_line = "✅ Map acceptée" if state.map_locked else "🗳️ Vote map ouvert"
    if state.attack_ids and state.defense_ids:
        status_line = "🚀 PP lancée"

    header = discord.Embed(
        title=f"🗺️ Roulette map — {prep_name}",
        description=(
            f"**Party code :** `{state.party_code}`\n"
            f"**Map proposée :** **{state.map_name}**"
        ),
        color=discord.Color.green() if state.map_locked else discord.Color.blurple(),
    )

    image_url = map_image_url(state.map_name)
    if image_url:
        header.set_image(url=image_url)
    else:
        header.add_field(name="🖼️ Image de map", value="Image indisponible pour cette map.", inline=False)

    details = discord.Embed(
        description=(
            f"**Votes** — ✅ Oui: **{state.map_yes}/{VOTE_THRESHOLD_ACCEPT}** • ❌ Non: **{state.map_no}/{VOTE_THRESHOLD_REJECT}**\n"
            f"*(1 vote par personne)*\n\n"
            f"{status_line}"
        ),
        color=header.color,
    )

    details.add_field(
        name="👥 Joueurs détectés dans la voc",
        value=(
            f"**{len(current_members)}** joueur(s) présent(s).\n"
            f"La PP prend les **10 premiers arrivés** s'il y a plus de 10 joueurs."
        ),
        inline=False,
    )

    if selected_members:
        details.add_field(name="🎮 Top 10 pris en compte", value=format_mentions(selected_members), inline=False)
    if waiting_members:
        details.add_field(name="⏳ Hors top 10", value=format_mentions(waiting_members), inline=False)

    if state.attack_ids and state.defense_ids:
        attack_members = [guild.get_member(user_id) for user_id in state.attack_ids]
        defense_members = [guild.get_member(user_id) for user_id in state.defense_ids]
        attack_members = [m for m in attack_members if m is not None]
        defense_members = [m for m in defense_members if m is not None]
        details.add_field(name="⚔️ Attaque", value=format_mentions(attack_members), inline=True)
        details.add_field(name="🛡️ Défense", value=format_mentions(defense_members), inline=True)

    details.set_footer(text="Vote map • Lancer la PP • Annuler")
    return [header, details]


async def refresh_match_message(guild: discord.Guild, prep_channel_id: int) -> None:
    state = load_match_state(prep_channel_id)
    if state is None:
        return
    prep_channel = guild.get_channel(prep_channel_id)
    if not isinstance(prep_channel, discord.VoiceChannel):
        return
    try:
        message = await prep_channel.fetch_message(state.ui_message_id)
    except (discord.NotFound, discord.Forbidden, discord.HTTPException):
        return
    try:
        await message.edit(embeds=build_match_embeds(guild, state), view=PPMatchView())
    except (discord.Forbidden, discord.HTTPException):
        pass


# ===================== UI: TICKETS =====================
class TicketStaffView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🗑️ Supprimer le ticket", style=discord.ButtonStyle.danger, custom_id="ticket:delete")
    async def delete_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return
        # Vérifie si le membre a les permissions Orga PP ou le rôle Metsuke
        if not has_orga_access(interaction.user) and not any(r.id == METSUKE_ROLE_ID for r in interaction.user.roles):
            return await interaction.response.send_message("Seul le staff peut supprimer définitivement le ticket.", ephemeral=True)
        
        await interaction.response.send_message("🗑️ Suppression du ticket dans 5 secondes...")
        await asyncio.sleep(5)
        try:
            if isinstance(interaction.channel, discord.TextChannel):
                await interaction.channel.delete(reason=f"Ticket supprimé par {interaction.user.display_name}")
        except discord.HTTPException:
            pass


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎟️ Créer un ticket", style=discord.ButtonStyle.primary, custom_id="ticket:create")
    async def create_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
            
        guild = interaction.guild
        user = interaction.user
        
        ticket_name = f"ticket-{slug(user.display_name).replace(' ', '-')}"
        existing_channel = discord.utils.get(guild.text_channels, name=ticket_name)
        
        if existing_channel:
            return await interaction.response.send_message(f"Tu as déjà un ticket ouvert : {existing_channel.mention}", ephemeral=True)

        category = find_category(guild, TICKET_CATEGORY_NAME)
        if not category:
            return await interaction.response.send_message("La catégorie de tickets est introuvable. Demande à un administrateur de refaire le /setup_pp.", ephemeral=True)

        metsuke_role = guild.get_role(METSUKE_ROLE_ID)
        if not metsuke_role:
            return await interaction.response.send_message(f"Erreur : Le rôle Metsuke ({METSUKE_ROLE_ID}) n'a pas été trouvé. Demande à un admin.", ephemeral=True)

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, attach_files=True),
            metsuke_role: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True)
        }

        ticket_channel = await guild.create_text_channel(
            name=ticket_name,
            category=category,
            overwrites=overwrites,
            topic=f"Ticket de {user.id}"
        )

        await interaction.response.send_message(f"✅ Ticket créé avec succès : {ticket_channel.mention}", ephemeral=True)

        embed = discord.Embed(
            title="🎟️ Ticket Ouvert",
            description=(
                f"Bienvenue {user.mention} !\n"
                f"Un membre du staff ({metsuke_role.mention}) va te répondre sous peu.\n\n"
                "Merci d'expliquer ta demande en détail (Recrutement Staff, Preuve pour le rôle Radiant, ou autre problème)."
            ),
            color=discord.Color.gold()
        )
        await ticket_channel.send(content=f"{user.mention} {metsuke_role.mention}", embed=embed, view=TicketActiveView())


class TicketActiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔒 Fermer le ticket", style=discord.ButtonStyle.danger, custom_id="ticket:close")
    async def close_ticket(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
            
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await interaction.response.send_message("🔒 Le ticket a été fermé. Il est maintenant masqué pour toi.", ephemeral=True)
        
        # Retire la permission de voir le salon à l'utilisateur
        try:
            await interaction.channel.set_permissions(interaction.user, view_channel=False)
        except discord.HTTPException:
            pass

        embed = discord.Embed(
            title="🔒 Ticket fermé",
            description=f"Le ticket a été fermé par {interaction.user.mention}.\nLe staff peut désormais consulter les logs ou le supprimer définitivement.",
            color=discord.Color.dark_gray()
        )
        # Permet au staff de supprimer définitivement
        await interaction.channel.send(embed=embed, view=TicketStaffView())


# ===================== UI: CAPTCHA / VERIFICATION =====================
class CaptchaView(discord.ui.View):
    def __init__(self, guild: Optional[discord.Guild] = None):
        super().__init__(timeout=None)
        
    @discord.ui.button(label="✅ Je ne suis pas un robot", style=discord.ButtonStyle.success, custom_id="pp:verify:captcha")
    async def captcha_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        
        roles = await ensure_core_roles(interaction.guild)
        
        # Retire le rôle non vérifié et ajoute le rôle joueur (Pèlerin)
        try:
            if roles["non_verified"] in interaction.user.roles:
                await interaction.user.remove_roles(roles["non_verified"], reason="Captcha validé")
            if roles["player"] not in interaction.user.roles:
                await interaction.user.add_roles(roles["player"], reason="Captcha validé")
        except discord.Forbidden:
            return await interaction.response.send_message("Erreur de permissions pour t'attribuer le rôle.", ephemeral=True)
            
        await interaction.response.send_message(
            f"✅ **Vérification réussie !** Tu as obtenu le rôle {roles['player'].mention}.\n"
            f"N'oublie pas de te rendre dans le salon **{RANK_CHANNEL_NAME}** pour choisir ton grade.",
            ephemeral=True
        )


class RankSelect(discord.ui.Select):
    def __init__(self, guild: Optional[discord.Guild] = None):
        options = [
            discord.SelectOption(
                label=name,
                value=name,
                emoji=rank_select_emoji(guild, name),
                description=f"Attribue le rôle {name}",
            )
            for name, _ in RANK_OPTIONS if name != "Radiant"
        ]
        super().__init__(
            placeholder="Choisis ton rank Valorant",
            min_values=1,
            max_values=1,
            options=options,
            custom_id="pp:verify:rank",
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)

        chosen_rank = self.values[0]
        await apply_rank(interaction.user, chosen_rank)
        await interaction.response.send_message(
            f"✅ Rank enregistré : **{chosen_rank}**. Ton profil est à jour.",
            ephemeral=True,
        )


class VerificationView(discord.ui.View):
    def __init__(self, guild: Optional[discord.Guild] = None):
        super().__init__(timeout=None)
        self.add_item(RankSelect(guild))


# ===================== UI: /pp MATCH =====================
class PPStartModal(discord.ui.Modal, title="Lancer une partie perso"):
    party_code = discord.ui.TextInput(
        label="Party code Valorant",
        placeholder="Ex: ABCD-EFGH-IJKL",
        required=True,
        max_length=64,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)

        prep_channel = interaction.user.voice.channel if interaction.user.voice else None
        if not is_prep_voice(prep_channel):
            return await interaction.response.send_message(
                "Tu dois être connecté dans **Préparation 1-4** pour utiliser `/pp`.",
                ephemeral=True,
            )

        if load_match_state(prep_channel.id) is not None:
            return await interaction.response.send_message(
                f"Une partie est déjà active dans **{prep_channel.name}**. Termine-la ou utilise `/pp_cleanup`.",
                ephemeral=True,
            )

        state = MatchState(
            prep_channel_id=prep_channel.id,
            started_by_id=interaction.user.id,
            ui_message_id=0,
            party_code=str(self.party_code.value).strip(),
            map_name=pick_map(),
            attack_ids=[],
            defense_ids=[],
            map_yes=0,
            map_no=0,
            map_locked=False,
            map_voters={},
        )

        ui_message = await prep_channel.send(embeds=build_match_embeds(interaction.guild, state), view=PPMatchView())
        state.ui_message_id = ui_message.id
        persist_match_state(state)

        count = len(ordered_prep_members(prep_channel))
        await interaction.response.send_message(
            (
                f"✅ Partie créée dans le chat de **{prep_channel.name}**.\n"
                f"Map + vote dispo tout de suite. Équipes auto seulement à **10 joueurs** minimum.\n"
                f"Joueurs actuellement détectés : **{count}**."
            ),
            ephemeral=True,
        )


class PPMatchView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _resolve(self, interaction: discord.Interaction) -> Tuple[Optional[discord.VoiceChannel], Optional[MatchState]]:
        channel = interaction.channel
        if not isinstance(channel, discord.VoiceChannel):
            await interaction.response.send_message("Ce panneau doit être utilisé dans le chat d'un vocal Préparation.", ephemeral=True)
            return None, None

        state = load_match_state(channel.id)
        if state is None:
            await interaction.response.send_message("Aucune partie active pour ce vocal.", ephemeral=True)
            return None, None

        if interaction.message and interaction.message.id != state.ui_message_id:
            await interaction.response.send_message("Ce panneau est obsolète. Utilise le plus récent.", ephemeral=True)
            return None, None

        return channel, state

    @discord.ui.button(label="✅ Oui", style=discord.ButtonStyle.success, custom_id="pp:match:yes", row=0)
    async def vote_yes(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if state.map_locked:
            return await interaction.response.send_message("La map est déjà acceptée.", ephemeral=True)

        voter_key = str(interaction.user.id)
        if voter_key in state.map_voters:
            return await interaction.response.send_message("Tu as déjà voté pour cette map.", ephemeral=True)

        state.map_voters[voter_key] = "yes"
        state.map_yes += 1
        if state.map_yes >= VOTE_THRESHOLD_ACCEPT:
            state.map_locked = True
        persist_match_state(state)
        await interaction.response.edit_message(embeds=build_match_embeds(interaction.guild, state), view=self)

    @discord.ui.button(label="❌ Non", style=discord.ButtonStyle.danger, custom_id="pp:match:no", row=0)
    async def vote_no(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if state.map_locked:
            return await interaction.response.send_message("La map est déjà acceptée.", ephemeral=True)

        voter_key = str(interaction.user.id)
        if voter_key in state.map_voters:
            return await interaction.response.send_message("Tu as déjà voté pour cette map.", ephemeral=True)

        state.map_voters[voter_key] = "no"
        state.map_no += 1
        note = None
        if state.map_no >= VOTE_THRESHOLD_REJECT:
            old_map = state.map_name
            state.map_name = pick_map(exclude=old_map)
            state.map_yes = 0
            state.map_no = 0
            state.map_locked = False
            state.map_voters = {}
            note = "❌ 5 votes non atteints : nouvelle map proposée."

        persist_match_state(state)
        await interaction.response.edit_message(embeds=build_match_embeds(interaction.guild, state), view=self)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    @discord.ui.button(label="🎲 Relancer (Orga)", style=discord.ButtonStyle.secondary, custom_id="pp:match:reroll", row=0)
    async def reroll(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)

        state.map_name = pick_map(exclude=state.map_name)
        state.map_yes = 0
        state.map_no = 0
        state.map_locked = False
        state.map_voters = {}
        persist_match_state(state)
        await interaction.response.edit_message(embeds=build_match_embeds(interaction.guild, state), view=self)

    @discord.ui.button(label="🚀 Lancer la PP", style=discord.ButtonStyle.primary, custom_id="pp:match:launch", row=1)
    async def launch(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)
        if state.attack_ids or state.defense_ids:
            return await interaction.response.send_message("La PP est déjà lancée pour ce vocal.", ephemeral=True)

        current_members = ordered_prep_members(prep_channel)
        if len(current_members) < 10:
            await interaction.response.edit_message(embeds=build_match_embeds(interaction.guild, state), view=self)
            return await interaction.followup.send(
                f"Il faut **10 joueurs** pour lancer la PP. Actuellement : **{len(current_members)}/10**.",
                ephemeral=True,
            )

        selected_members = current_members[:10]
        waiting_members = current_members[10:]
        attack, defense = split_balanced_teams(selected_members)
        await apply_team_roles(interaction.guild, attack, defense)
        await move_teams_if_possible(prep_channel, attack, defense)

        state.attack_ids = [member.id for member in attack]
        state.defense_ids = [member.id for member in defense]
        persist_match_state(state)
        await interaction.response.edit_message(embeds=build_match_embeds(interaction.guild, state), view=self)

        if waiting_members:
            await interaction.followup.send(
                "✅ PP lancée avec les **10 premiers arrivés**. Hors top 10 : " + ", ".join(member.display_name for member in waiting_members),
                ephemeral=True,
            )

    @discord.ui.button(label="❌ Annuler", style=discord.ButtonStyle.danger, custom_id="pp:match:cancel", row=1)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)

        db.delete_active_match(prep_channel.id)
        members = [m for m in interaction.guild.members if m.id in state.attack_ids + state.defense_ids]
        await clear_team_roles(interaction.guild, members)
        await interaction.response.edit_message(content="❌ Partie annulée.", embed=None, view=None)
        try:
            await prep_channel.send("❌ La partie active a été annulée.")
        except (discord.Forbidden, discord.HTTPException):
            pass


class CustomVoiceRenameModal(discord.ui.Modal, title="Renommer le salon privé"):
    new_name = discord.ui.TextInput(label="Nouveau nom", max_length=100)

    def __init__(self, channel_id: int):
        super().__init__()
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        channel = interaction.guild.get_channel(self.channel_id) if interaction.guild else None
        if not isinstance(channel, discord.VoiceChannel) or not can_manage_custom_voice(interaction.user, channel):
            return await interaction.response.send_message("Tu ne peux pas gérer ce salon.", ephemeral=True)
        name = str(self.new_name.value).strip()
        if len(name) < 2:
            return await interaction.response.send_message("Nom trop court.", ephemeral=True)
        await channel.edit(name=name, reason="Custom voice rename via UI")
        await refresh_custom_voice_panel(channel)
        await interaction.response.send_message(f"✏️ Salon renommé en **{name}**.", ephemeral=True)


class CustomVoiceLimitModal(discord.ui.Modal, title="Changer la limite de slots"):
    slots = discord.ui.TextInput(label="Nombre de places (0 = illimité)", max_length=2, placeholder="0-99")

    def __init__(self, channel_id: int):
        super().__init__()
        self.channel_id = channel_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        channel = interaction.guild.get_channel(self.channel_id) if interaction.guild else None
        if not isinstance(channel, discord.VoiceChannel) or not can_manage_custom_voice(interaction.user, channel):
            return await interaction.response.send_message("Tu ne peux pas gérer ce salon.", ephemeral=True)
        try:
            limit = max(0, min(99, int(str(self.slots.value).strip())))
        except ValueError:
            return await interaction.response.send_message("Entre un nombre valide entre 0 et 99.", ephemeral=True)
        await channel.edit(user_limit=limit, reason="Custom voice limit via UI")
        await refresh_custom_voice_panel(channel)
        shown = str(limit) if limit else "∞"
        await interaction.response.send_message(f"👥 Limite mise à **{shown}**.", ephemeral=True)


class CustomVoiceKickSelect(discord.ui.Select):
    def __init__(self, channel: discord.VoiceChannel, requester_id: int):
        self.channel_id = channel.id
        self.requester_id = requester_id
        options = [
            discord.SelectOption(label=m.display_name[:100], value=str(m.id))
            for m in channel.members[:25]
            if not m.bot and m.id != requester_id
        ]
        super().__init__(placeholder="Choisis qui expulser", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        if interaction.user.id != self.requester_id:
            return await interaction.response.send_message("Cette sélection ne t’est pas destinée.", ephemeral=True)
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        channel = interaction.guild.get_channel(self.channel_id) if interaction.guild else None
        if not isinstance(channel, discord.VoiceChannel) or not can_manage_custom_voice(interaction.user, channel):
            return await interaction.response.send_message("Tu ne peux pas gérer ce salon.", ephemeral=True)
        member = interaction.guild.get_member(int(self.values[0])) if interaction.guild else None
        if member is None or not member.voice or member.voice.channel.id != channel.id:
            return await interaction.response.send_message("Ce membre n'est plus dans la voc.", ephemeral=True)
        try:
            await member.move_to(None, reason=f"Disconnected from private voice by {interaction.user}")
        except (discord.Forbidden, discord.HTTPException):
            return await interaction.response.send_message("Impossible de déconnecter ce membre.", ephemeral=True)
        await refresh_custom_voice_panel(channel)
        await interaction.response.send_message(f"⛔ {member.mention} a été déconnecté.", ephemeral=True)


class CustomVoiceKickView(discord.ui.View):
    def __init__(self, channel: discord.VoiceChannel, requester_id: int):
        super().__init__(timeout=60)
        self.add_item(CustomVoiceKickSelect(channel, requester_id))


class CustomVoiceControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _resolve(self, interaction: discord.Interaction) -> Optional[discord.VoiceChannel]:
        channel = interaction.channel
        if not isinstance(channel, discord.VoiceChannel) or not is_custom_voice(channel):
            await interaction.response.send_message("Ce panneau doit être utilisé dans le chat d’une voc privée.", ephemeral=True)
            return None
        if not isinstance(interaction.user, discord.Member) or not can_manage_custom_voice(interaction.user, channel):
            await interaction.response.send_message("Réservé au propriétaire du salon, Orga PP ou admin.", ephemeral=True)
            return None
        return channel

    @discord.ui.button(label="🔒 Lock", style=discord.ButtonStyle.secondary, custom_id="cvoice:lock")
    async def lock_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        channel = await self._resolve(interaction)
        if channel is None:
            return
        owner_id = db.get_custom_voice_owner(channel.id)
        owner = interaction.guild.get_member(owner_id) if owner_id else interaction.user
        await set_custom_voice_permissions(channel, owner=owner, locked=True)
        await refresh_custom_voice_panel(channel)
        await interaction.response.send_message("🔒 Salon verrouillé.", ephemeral=True)

    @discord.ui.button(label="🔓 Unlock", style=discord.ButtonStyle.success, custom_id="cvoice:unlock")
    async def unlock_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        channel = await self._resolve(interaction)
        if channel is None:
            return
        owner_id = db.get_custom_voice_owner(channel.id)
        owner = interaction.guild.get_member(owner_id) if owner_id else interaction.user
        await set_custom_voice_permissions(channel, owner=owner, locked=False)
        await refresh_custom_voice_panel(channel)
        await interaction.response.send_message("🔓 Salon ouvert.", ephemeral=True)

    @discord.ui.button(label="✏️ Rename", style=discord.ButtonStyle.primary, custom_id="cvoice:rename")
    async def rename_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        channel = await self._resolve(interaction)
        if channel is None:
            return
        await interaction.response.send_modal(CustomVoiceRenameModal(channel.id))

    @discord.ui.button(label="👥 Slots", style=discord.ButtonStyle.primary, custom_id="cvoice:slots")
    async def slots_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        channel = await self._resolve(interaction)
        if channel is None:
            return
        await interaction.response.send_modal(CustomVoiceLimitModal(channel.id))

    @discord.ui.button(label="⛔ Expulser", style=discord.ButtonStyle.danger, custom_id="cvoice:kick")
    async def kick_btn(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        channel = await self._resolve(interaction)
        if channel is None:
            return
        eligible = [m for m in channel.members if not m.bot and m.id != interaction.user.id]
        if not eligible:
            return await interaction.response.send_message("Personne à expulser dans cette voc.", ephemeral=True)
        await interaction.response.send_message("Choisis un membre à déconnecter :", view=CustomVoiceKickView(channel, interaction.user.id), ephemeral=True)


# ===================== BOT =====================
class PPBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)
        self.invites_cache = {}

    async def setup_hook(self) -> None:
        self.add_view(CaptchaView())
        self.add_view(VerificationView())
        self.add_view(PPMatchView())
        self.add_view(CustomVoiceControlView())
        self.add_view(TicketPanelView())
        self.add_view(TicketActiveView())
        self.add_view(TicketStaffView())
        if GUILD_ID:
            guild = discord.Object(id=int(GUILD_ID))
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        else:
            await self.tree.sync()


bot = PPBot()


# ===================== EVENTS =====================
@bot.event
async def on_ready() -> None:
    await seed_existing_prep_members(bot.guilds)
    
    # Cache invitations pour le tracker
    for guild in bot.guilds:
        try:
            bot.invites_cache[guild.id] = await guild.invites()
        except discord.Forbidden:
            pass

    print(f"[OK] Connecté en tant que {bot.user} ({bot.user.id})")


@bot.event
async def on_invite_create(invite: discord.Invite) -> None:
    try:
        bot.invites_cache[invite.guild.id] = await invite.guild.invites()
    except discord.Forbidden:
        pass


@bot.event
async def on_invite_delete(invite: discord.Invite) -> None:
    try:
        bot.invites_cache[invite.guild.id] = await invite.guild.invites()
    except discord.Forbidden:
        pass


@bot.event
async def on_member_join(member: discord.Member) -> None:
    roles = await ensure_core_roles(member.guild)
    try:
        await member.add_roles(roles["non_verified"], reason="PP new member verification")
    except discord.Forbidden:
        pass

    # ================= TRACKER INVITATION =================
    inviter_mention = "/asak"
    try:
        old_invites = bot.invites_cache.get(member.guild.id, [])
        new_invites = await member.guild.invites()
        for invite in new_invites:
            for old_invite in old_invites:
                if invite.code == old_invite.code and invite.uses > old_invite.uses:
                    if invite.inviter:
                        inviter_mention = invite.inviter.mention
                    break
        bot.invites_cache[member.guild.id] = new_invites
    except discord.Forbidden:
        pass

    welcome_channel = get_welcome_channel(member.guild)
    if welcome_channel is not None:
        msg_content = (
            f"⛩️ Bienvenue dans les ruelles d'Asakusa, {member.mention} !\n"
            f"Tu as été invité(e) par **{inviter_mention}**."
        )
        try:
            # Génération de l'image personnalisée
            img_buffer = await generate_welcome_card(member)
            file = discord.File(fp=img_buffer, filename="welcome.png")
            await welcome_channel.send(content=msg_content, file=file)
        except Exception as e:
            # Fallback en cas d'erreur de la librairie d'image
            embed = discord.Embed(
                title="⛩️ Bienvenue à Asakusa",
                description=(
                    f"{member.mention}, les portes du temple s'ouvrent devant toi.\n"
                    f"Tu as été invité(e) par **{inviter_mention}**.\n\n"
                    f"Passe d'abord par **{VERIFY_CHANNEL_NAME}** pour prouver que tu n'es pas un robot."
                ),
                color=discord.Color.gold(),
            )
            embed.set_footer(text="Une fois vérifié, n'oublie pas de choisir ton rang !")
            try:
                await welcome_channel.send(content=member.mention, embed=embed)
            except (discord.Forbidden, discord.HTTPException):
                pass


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState) -> None:
    if member.bot:
        return

    if isinstance(before.channel, discord.VoiceChannel):
        if is_prep_voice(before.channel) and (not after.channel or after.channel.id != before.channel.id):
            forget_member_from_prep(before.channel, member)
            if load_match_state(before.channel.id) is not None:
                await refresh_match_message(member.guild, before.channel.id)
        if is_custom_voice(before.channel) and (not after.channel or after.channel.id != before.channel.id):
            await refresh_custom_voice_panel(before.channel)
        if not after.channel or after.channel.id != before.channel.id:
            await cleanup_custom_voice_if_empty(before.channel)

    if isinstance(after.channel, discord.VoiceChannel):
        if is_create_voice_trigger(after.channel) and (not before.channel or before.channel.id != after.channel.id):
            if not is_verified_member(member):
                try:
                    await member.move_to(None, reason="Verification required before creating custom voice")
                except (discord.Forbidden, discord.HTTPException):
                    pass
                return
            created = await create_custom_voice_channel(
                member.guild,
                member,
                f"🎤 Salon de {member.display_name}",
                CUSTOM_VOICE_DEFAULT_LIMIT,
            )
            return

        if is_prep_voice(after.channel) and (not before.channel or before.channel.id != after.channel.id):
            remember_member_in_prep(after.channel, member)
            if load_match_state(after.channel.id) is not None:
                await refresh_match_message(member.guild, after.channel.id)
        if is_custom_voice(after.channel) and (not before.channel or before.channel.id != after.channel.id):
            await ensure_custom_voice_panel(after.channel)
            await refresh_custom_voice_panel(after.channel)

# ===================== COMMANDS =====================
@bot.tree.command(name="setup_pp", description="Configure les rôles, permissions et panneaux PP sur les salons de la catégorie PP.")
@app_commands.guild_only()
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_pp(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
        return await interaction.response.send_message("Commande réservée aux admins du serveur.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)

    roles = await ensure_core_roles(guild)
    await set_verification_permissions(guild)

    verify_channel = get_verify_channel(guild)
    rank_channel = get_rank_channel(guild)
    missing: List[str] = []
    
    if verify_channel is None:
        missing.append(f"#{VERIFY_CHANNEL_NAME} (ou alias de vérification)")
    if rank_channel is None:
        missing.append(f"#{RANK_CHANNEL_NAME} (ou alias pour le rank)")
        
    for name in PREP_CHANNEL_NAMES:
        found = discord.utils.find(
            lambda c: isinstance(c, discord.VoiceChannel) and slug(c.name) == slug(name),
            guild.channels,
        )
        if found is None:
            missing.append(name)

    # Déploiement du message Captcha
    if verify_channel is not None:
        should_post = True
        async for msg in verify_channel.history(limit=20):
            if msg.author == guild.me and msg.components:
                should_post = False
                break
        if should_post:
            embed = discord.Embed(
                title="🛡️ Vérification de sécurité",
                description="Bienvenue à Asakusa ! Avant de pouvoir entrer et discuter, prouve que tu n'es pas un robot en cliquant sur le bouton ci-dessous.",
                color=discord.Color.green(),
            )
            await verify_channel.send(embed=embed, view=CaptchaView(guild))

    # Déploiement du message de choix de Rank
    if rank_channel is not None:
        should_post = True
        async for msg in rank_channel.history(limit=20):
            if msg.author == guild.me and msg.components:
                should_post = False
                break
        if should_post:
            embed = discord.Embed(
                title="🎭 Choix du Rank Valorant",
                description="Choisis ton **Peak Elo Valorant des 5 derniers actes** pour mettre à jour ton profil.\nLe salon est en **lecture seule** : tout se fait via le menu.",
                color=discord.Color.blurple(),
            )
            await rank_channel.send(embed=embed, view=VerificationView(guild))


    # === CRÉATION ET CONFIGURATION DES TICKETS ===
    ticket_category = find_category(guild, TICKET_CATEGORY_NAME)
    if not ticket_category:
        ticket_category = await guild.create_category(TICKET_CATEGORY_NAME)

    ticket_channel = find_text_channel(guild, [TICKET_CHANNEL_NAME], category=ticket_category)
    if not ticket_channel:
        ticket_channel = await guild.create_text_channel(
            name=TICKET_CHANNEL_NAME,
            category=ticket_category
        )

    await _safe_set_permissions(ticket_channel, guild.default_role, view_channel=False)
    await _safe_set_permissions(ticket_channel, roles["non_verified"], view_channel=False)
    await _safe_set_permissions(
        ticket_channel,
        roles["member"],
        view_channel=True, send_messages=False, add_reactions=False, read_message_history=True
    )
    await _safe_set_permissions(
        ticket_channel,
        roles["orga"],
        view_channel=True, send_messages=True, add_reactions=True, read_message_history=True, manage_messages=True
    )
    
    metsuke_role = guild.get_role(METSUKE_ROLE_ID)
    if metsuke_role:
        await _safe_set_permissions(
            ticket_channel,
            metsuke_role,
            view_channel=True, send_messages=True, add_reactions=True, read_message_history=True, manage_messages=True
        )

    should_post_ticket = True
    async for msg in ticket_channel.history(limit=20):
        if msg.author == guild.me and msg.components:
            should_post_ticket = False
            break

    if should_post_ticket:
        embed = discord.Embed(
            title="🎟️ Assistance & Requêtes",
            description=(
                "Bienvenue au comptoir d'assistance d'Asakusa !\n\n"
                "Clique sur le bouton ci-dessous pour ouvrir un ticket privé avec le staff.\n\n"
                "**Utilise ce système pour :**\n"
                "• 📝 Demander à être recruté dans le staff.\n"
                "• 🌟 Demander l'attribution du rôle **Radiant** (merci de fournir des preuves in-game).\n"
                "• ❓ Toute autre question, problème ou signalement."
            ),
            color=discord.Color.red()
        )
        await ticket_channel.send(embed=embed, view=TicketPanelView())

    text = "✅ Setup de la catégorie PP terminé.\n• Les autres salons du serveur ont été laissés indépendants.\n"
    if missing:
        text += "⚠️ Salons introuvables : " + ", ".join(missing)
    else:
        text += "Tous les salons requis ont été configurés avec succès."
    await interaction.followup.send(text, ephemeral=True)


@bot.tree.command(name="pp", description="Lance une partie perso depuis ton vocal Préparation.")
@app_commands.guild_only()
async def pp(interaction: discord.Interaction) -> None:
    if not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
    if not has_orga_access(interaction.user):
        return await interaction.response.send_message("Commande réservée aux **Orga PP** et admins.", ephemeral=True)
    prep_channel = interaction.user.voice.channel if interaction.user.voice else None
    if not is_prep_voice(prep_channel):
        return await interaction.response.send_message(
            "Tu dois être connecté dans **Préparation 1, 2, 3 ou 4** pour lancer `/pp`.",
            ephemeral=True,
        )
    if load_match_state(prep_channel.id) is not None:
        return await interaction.response.send_message(
            f"Une partie est déjà active dans **{prep_channel.name}**. Termine-la avec les boutons du panneau ou `/pp_cleanup`.",
            ephemeral=True,
        )
    await interaction.response.send_modal(PPStartModal())


@bot.tree.command(name="pp_cleanup", description="Retire les rôles d'équipe et ferme la partie active du vocal où tu es.")
@app_commands.guild_only()
async def pp_cleanup(interaction: discord.Interaction) -> None:
    if not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
    if not has_orga_access(interaction.user):
        return await interaction.response.send_message("Commande réservée aux **Orga PP** et admins.", ephemeral=True)

    prep_channel = interaction.user.voice.channel if interaction.user.voice else None
    if not is_prep_voice(prep_channel):
        return await interaction.response.send_message("Connecte-toi dans un vocal Préparation.", ephemeral=True)

    state = load_match_state(prep_channel.id)
    if state is None:
        return await interaction.response.send_message("Aucune partie active dans ce vocal.", ephemeral=True)
    if not is_match_controller(interaction.user, state):
        return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)

    members = [m for m in interaction.guild.members if m.id in state.attack_ids + state.defense_ids]
    await clear_team_roles(interaction.guild, members)
    db.delete_active_match(prep_channel.id)
    await interaction.response.send_message("✅ Partie active nettoyée.", ephemeral=True)


# ===================== RENDER WEB HEALTH SERVER =====================
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("PORT", os.getenv("WEB_PORT", "10000")))


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health", "/healthz"):
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            body = b"not found"
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def log_message(self, format, *args):
        return


def start_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((WEB_HOST, WEB_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[HTTP] Health server listening on http://{WEB_HOST}:{WEB_PORT}")
    return server


# ===================== RUN =====================
def main() -> None:
    if not TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN manquant dans le .env")
    start_health_server()
    bot.run(TOKEN)


if __name__ == "__main__":
    main()
