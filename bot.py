import json
import os
import random
import sqlite3
import threading
import unicodedata
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv

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

NON_VERIFIED_ROLE = os.getenv("NON_VERIFIED_ROLE", "Non vérifié")
MEMBER_ROLE = os.getenv("MEMBER_ROLE", "Membre")
ORGA_ROLE = os.getenv("ORGA_ROLE", "Orga PP")
TEAM_ATTACK_ROLE = os.getenv("TEAM_ATTACK_ROLE", "Équipe Attaque")
TEAM_DEFENSE_ROLE = os.getenv("TEAM_DEFENSE_ROLE", "Équipe Défense")
PLAYER_ROLE = os.getenv("PLAYER_ROLE", "Joueur")

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
]

INTENTS = discord.Intents.default()
INTENTS.guilds = True
INTENTS.members = True
INTENTS.voice_states = True
INTENTS.messages = True
INTENTS.message_content = False

# 25 options max sur un menu déroulant Discord.
RANK_OPTIONS: List[Tuple[str, int]] = [
    ("Fer 1", 100), ("Fer 2", 110), ("Fer 3", 120),
    ("Bronze 1", 200), ("Bronze 2", 210), ("Bronze 3", 220),
    ("Argent 1", 300), ("Argent 2", 310), ("Argent 3", 320),
    ("Or 1", 400), ("Or 2", 410), ("Or 3", 420),
    ("Platine 1", 500), ("Platine 2", 510), ("Platine 3", 520),
    ("Diamant 1", 600), ("Diamant 2", 610), ("Diamant 3", 620),
    ("Ascendant 1", 700), ("Ascendant 2", 710), ("Ascendant 3", 720),
    ("Immortal 1", 800), ("Immortal 2", 810), ("Immortal 3", 820),
]
RANK_VALUE_BY_NAME = {name: value for name, value in RANK_OPTIONS}

RANK_TIER_EMOJI = {
    "Fer": "⚫",
    "Bronze": "🥉",
    "Argent": "🥈",
    "Or": "🥇",
    "Platine": "🔷",
    "Diamant": "💎",
    "Ascendant": "🟢",
    "Immortal": "👑",
}

MAP_IMAGE = {
    "Haven": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt1f6b8f00f50f1d2d/5eb26f538c3b8b4d13a56656/Haven_2.jpg",
    "Corrode": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt2b5d5f8bde7975ed/66882f7d0ea0db6b7c80a8d3/Corrode_Splash.jpg",
    "Icebox": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blteae6cfc448f942f4/5ed5667f6f8c0d0fe64f2d2d/Icebox_2.jpg",
    "Pearl": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt6d0ea3f8be4f7fdb/62a0a6dc83b0814f921e7f65/PearlLoadingScreen.jpg",
    "Sunset": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt5f1ea0fdf75ebf2d/64d46d72e3a2f21845f6e4da/Sunset_Loading_Screen.jpg",
    "Lotus": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/bltc9716a9d8a8a14ce/63ca98c9f8f6c46a2a23e3da/Lotus_2.jpg",
    "Abyss": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt58d627eb8f9da5be/665605c2629f6f0cb0d8f98f/Abyss_Splash.jpg",
    "Breeze": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt7a8e8ed30d1dc022/608b02f93f6c7c14ac24d434/Breeze_2.jpg",
    "Ascent": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blta26f5bf0b5f86579/5eb7fa55fdaf4a4b4840c83d/Ascent_2.jpg",
    "Split": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/blt643d6e03da4c4c57/5eb7fa25d8c0d4168f4b8f53/Split_2.jpg",
    "Fracture": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/bltd5f9ce7bf65d7c44/61787db31b09f3266d6f294e/Fracture_2.jpg",
    "Bind": "https://images.contentstack.io/v3/assets/bltb6530b271fddd0b1/bltfd633927429c40f4/5eb26f6b8c3b8b4d13a56657/Bind_2.jpg",
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
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

    def save_active_match(
        self,
        prep_channel_id: int,
        started_by_id: int,
        ui_message_id: int,
        party_code: str,
        map_name: str,
        attack_ids: List[int],
        defense_ids: List[int],
    ) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO active_matches (
                prep_channel_id, started_by_id, ui_message_id, party_code,
                map_name, attack_ids, defense_ids
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                prep_channel_id,
                started_by_id,
                ui_message_id,
                party_code,
                map_name,
                json.dumps(attack_ids),
                json.dumps(defense_ids),
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

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "MatchState":
        return cls(
            prep_channel_id=row["prep_channel_id"],
            started_by_id=row["started_by_id"],
            ui_message_id=row["ui_message_id"],
            party_code=row["party_code"],
            map_name=row["map_name"],
            attack_ids=json.loads(row["attack_ids"]),
            defense_ids=json.loads(row["defense_ids"]),
        )


# ===================== HELPERS =====================
def tier_emoji(rank_name: str) -> str:
    tier = rank_name.split()[0]
    return RANK_TIER_EMOJI.get(tier, "🎯")


def rank_value_for_member(member: discord.Member) -> int:
    stored = db.get_player_rank(member.id)
    if stored and stored in RANK_VALUE_BY_NAME:
        return RANK_VALUE_BY_NAME[stored]
    for role in member.roles:
        if role.name in RANK_VALUE_BY_NAME:
            return RANK_VALUE_BY_NAME[role.name]
    return 0


def is_prep_voice(channel: Optional[discord.abc.GuildChannel]) -> bool:
    return isinstance(channel, discord.VoiceChannel) and slug(channel.name) in {slug(n) for n in PREP_CHANNEL_NAMES}


def get_verify_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    return discord.utils.find(
        lambda c: isinstance(c, discord.TextChannel) and slug(c.name) == slug(VERIFY_CHANNEL_NAME),
        guild.channels,
    )


async def ensure_role(guild: discord.Guild, role_name: str, *, color: Optional[discord.Color] = None) -> discord.Role:
    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        role = await guild.create_role(name=role_name, color=color or discord.Color.default(), reason="PP setup")
    return role


async def ensure_core_roles(guild: discord.Guild) -> Dict[str, discord.Role]:
    roles = {
        "non_verified": await ensure_role(guild, NON_VERIFIED_ROLE),
        "member": await ensure_role(guild, MEMBER_ROLE),
        "orga": await ensure_role(guild, ORGA_ROLE),
        "attack": await ensure_role(guild, TEAM_ATTACK_ROLE),
        "defense": await ensure_role(guild, TEAM_DEFENSE_ROLE),
        "player": await ensure_role(guild, PLAYER_ROLE),
    }
    for rank_name, _ in RANK_OPTIONS:
        await ensure_role(guild, rank_name)
    return roles


async def set_verification_permissions(guild: discord.Guild) -> None:
    roles = await ensure_core_roles(guild)
    non_verified = roles["non_verified"]
    member = roles["member"]
    verify_channel = get_verify_channel(guild)

    for channel in guild.channels:
        if channel == verify_channel:
            continue
        try:
            await channel.set_permissions(non_verified, view_channel=False, reason="PP verification lock")
        except discord.Forbidden:
            pass

    if verify_channel is not None:
        try:
            await verify_channel.set_permissions(
                guild.default_role,
                view_channel=False,
                send_messages=False,
                add_reactions=False,
                reason="Verification hidden by default",
            )
            await verify_channel.set_permissions(
                member,
                view_channel=False,
                send_messages=False,
                add_reactions=False,
                reason="Verification hidden after validation",
            )
            await verify_channel.set_permissions(
                non_verified,
                view_channel=True,
                send_messages=False,
                add_reactions=False,
                read_message_history=True,
                use_application_commands=True,
                reason="Verification visible only for new members",
            )
        except discord.Forbidden:
            pass

    for channel_name in PREP_CHANNEL_NAMES:
        prep = discord.utils.find(
            lambda c: isinstance(c, discord.VoiceChannel) and slug(c.name) == slug(channel_name),
            guild.channels,
        )
        if prep is None:
            continue
        try:
            await prep.set_permissions(non_verified, view_channel=False, reason="Hide prep from new members")
        except discord.Forbidden:
            pass


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

    add_roles = [rank_role, roles["member"], roles["player"]]
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


def split_balanced_teams(members: List[discord.Member]) -> Tuple[List[discord.Member], List[discord.Member]]:
    scored = sorted(members, key=rank_value_for_member, reverse=True)
    attack: List[discord.Member] = []
    defense: List[discord.Member] = []
    score_attack = 0
    score_defense = 0

    for member in scored:
        value = rank_value_for_member(member)
        if len(attack) >= 5:
            defense.append(member)
            score_defense += value
        elif len(defense) >= 5:
            attack.append(member)
            score_attack += value
        elif score_attack <= score_defense:
            attack.append(member)
            score_attack += value
        else:
            defense.append(member)
            score_defense += value

    return attack, defense


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


def build_match_embed(guild: discord.Guild, state: MatchState) -> discord.Embed:
    prep_channel = guild.get_channel(state.prep_channel_id)
    prep_name = prep_channel.name if isinstance(prep_channel, discord.VoiceChannel) else "Préparation"
    current_members = ordered_prep_members(prep_channel) if isinstance(prep_channel, discord.VoiceChannel) else []
    selected_members = current_members[:10]
    waiting_members = current_members[10:]

    embed = discord.Embed(
        title=f"Partie perso — {prep_name}",
        description=f"**Party code :** `{state.party_code}`\n**Map :** **{state.map_name}**",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="👥 Joueurs dans la voc",
        value=(
            f"**{len(current_members)}/10**\n"
            f"Les **10 premiers arrivés** sont pris pour les équipes s'il y a plus de 10 joueurs."
        ),
        inline=False,
    )

    if state.attack_ids and state.defense_ids:
        attack_members = [guild.get_member(user_id) for user_id in state.attack_ids]
        defense_members = [guild.get_member(user_id) for user_id in state.defense_ids]
        attack_members = [m for m in attack_members if m is not None]
        defense_members = [m for m in defense_members if m is not None]
        embed.add_field(name="⚔️ Attaque", value=format_mentions(attack_members), inline=True)
        embed.add_field(name="🛡️ Défense", value=format_mentions(defense_members), inline=True)
    else:
        embed.add_field(
            name="⚖️ Équipes",
            value="Pas encore générées. Le vote map et le party code sont déjà actifs.\nLe bouton **Générer équipes** marche à partir de **10 joueurs**.",
            inline=False,
        )

    if selected_members:
        embed.add_field(name="🎮 Joueurs pris en compte", value=format_mentions(selected_members), inline=False)
    if waiting_members:
        embed.add_field(name="⏳ En attente / hors top 10", value=format_mentions(waiting_members), inline=False)

    image_url = map_image_url(state.map_name)
    if image_url:
        embed.set_image(url=image_url)
    embed.set_footer(text="Boutons : reroll map • générer équipes • victoire attaque • victoire défense • annuler")
    return embed


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
        await message.edit(embed=build_match_embed(guild, state), view=PPMatchView())
    except (discord.Forbidden, discord.HTTPException):
        pass


# ===================== UI: VERIFICATION =====================
class RankSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(
                label=name,
                value=name,
                emoji=tier_emoji(name),
                description=f"Attribue le rôle {name}",
            )
            for name, _ in RANK_OPTIONS
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
            f"✅ Rank enregistré : **{chosen_rank}**. Tu as maintenant accès au serveur.",
            ephemeral=True,
        )


class VerificationView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(RankSelect())


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
        )

        ui_message = await prep_channel.send(embed=build_match_embed(interaction.guild, state), view=PPMatchView())
        state.ui_message_id = ui_message.id
        db.save_active_match(
            prep_channel_id=state.prep_channel_id,
            started_by_id=state.started_by_id,
            ui_message_id=state.ui_message_id,
            party_code=state.party_code,
            map_name=state.map_name,
            attack_ids=state.attack_ids,
            defense_ids=state.defense_ids,
        )

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

    @discord.ui.button(label="🎲 Reroll map", style=discord.ButtonStyle.secondary, custom_id="pp:match:reroll", row=0)
    async def reroll(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)

        state.map_name = pick_map(exclude=state.map_name)
        db.save_active_match(
            prep_channel_id=state.prep_channel_id,
            started_by_id=state.started_by_id,
            ui_message_id=state.ui_message_id,
            party_code=state.party_code,
            map_name=state.map_name,
            attack_ids=state.attack_ids,
            defense_ids=state.defense_ids,
        )
        await interaction.response.edit_message(embed=build_match_embed(interaction.guild, state), view=self)

    @discord.ui.button(label="⚖️ Générer équipes", style=discord.ButtonStyle.primary, custom_id="pp:match:generate", row=0)
    async def generate(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)

        current_members = ordered_prep_members(prep_channel)
        if len(current_members) < 10:
            await interaction.response.edit_message(embed=build_match_embed(interaction.guild, state), view=self)
            return await interaction.followup.send(
                f"Il faut **10 joueurs** pour générer les équipes. Actuellement : **{len(current_members)}/10**.",
                ephemeral=True,
            )

        selected_members = current_members[:10]
        waiting_members = current_members[10:]
        attack, defense = split_balanced_teams(selected_members)
        await apply_team_roles(interaction.guild, attack, defense)
        await move_teams_if_possible(prep_channel, attack, defense)

        state.attack_ids = [member.id for member in attack]
        state.defense_ids = [member.id for member in defense]
        db.save_active_match(
            prep_channel_id=state.prep_channel_id,
            started_by_id=state.started_by_id,
            ui_message_id=state.ui_message_id,
            party_code=state.party_code,
            map_name=state.map_name,
            attack_ids=state.attack_ids,
            defense_ids=state.defense_ids,
        )
        await interaction.response.edit_message(embed=build_match_embed(interaction.guild, state), view=self)
        if waiting_members:
            await interaction.followup.send(
                f"✅ Équipes générées avec les **10 premiers**. Joueurs non pris en compte : {', '.join(member.display_name for member in waiting_members)}",
                ephemeral=True,
            )

    @discord.ui.button(label="⚔️ Victoire attaque", style=discord.ButtonStyle.success, custom_id="pp:match:attack_win", row=1)
    async def attack_win(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._finish_match(interaction, winners_are_attack=True)

    @discord.ui.button(label="🛡️ Victoire défense", style=discord.ButtonStyle.success, custom_id="pp:match:defense_win", row=1)
    async def defense_win(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self._finish_match(interaction, winners_are_attack=False)

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

    async def _finish_match(self, interaction: discord.Interaction, *, winners_are_attack: bool) -> None:
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
        prep_channel, state = await self._resolve(interaction)
        if prep_channel is None or state is None:
            return
        if not is_match_controller(interaction.user, state):
            return await interaction.response.send_message("Réservé au créateur de la partie, Orga PP ou admin.", ephemeral=True)
        if not state.attack_ids or not state.defense_ids:
            return await interaction.response.send_message("Les équipes ne sont pas encore générées.", ephemeral=True)

        db.delete_active_match(prep_channel.id)
        members = [m for m in interaction.guild.members if m.id in state.attack_ids + state.defense_ids]
        await clear_team_roles(interaction.guild, members)

        winning_side = "Attaque" if winners_are_attack else "Défense"
        await interaction.response.edit_message(
            content=f"🏁 Match terminé — **{winning_side}** gagne.",
            embed=None,
            view=None,
        )
        try:
            await prep_channel.send(f"🏁 Partie terminée. **{winning_side}** gagne.")
        except (discord.Forbidden, discord.HTTPException):
            pass


# ===================== BOT =====================
class PPBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)

    async def setup_hook(self) -> None:
        self.add_view(VerificationView())
        self.add_view(PPMatchView())
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
    print(f"[OK] Connecté en tant que {bot.user} ({bot.user.id})")


@bot.event
async def on_member_join(member: discord.Member) -> None:
    roles = await ensure_core_roles(member.guild)
    try:
        await member.add_roles(roles["non_verified"], reason="PP new member verification")
    except discord.Forbidden:
        pass


@bot.event
async def on_voice_state_update(member: discord.Member, before: discord.VoiceState, after: discord.VoiceState) -> None:
    if member.bot:
        return

    if isinstance(before.channel, discord.VoiceChannel) and is_prep_voice(before.channel):
        if not after.channel or after.channel.id != before.channel.id:
            forget_member_from_prep(before.channel, member)
            if load_match_state(before.channel.id) is not None:
                await refresh_match_message(member.guild, before.channel.id)

    if isinstance(after.channel, discord.VoiceChannel) and is_prep_voice(after.channel):
        if not before.channel or before.channel.id != after.channel.id:
            remember_member_in_prep(after.channel, member)
            if load_match_state(after.channel.id) is not None:
                await refresh_match_message(member.guild, after.channel.id)


# ===================== COMMANDS =====================
@bot.tree.command(name="setup_pp", description="Configure les rôles, permissions et panneaux PP sur les salons existants.")
@app_commands.guild_only()
@app_commands.checks.has_permissions(manage_guild=True)
async def setup_pp(interaction: discord.Interaction) -> None:
    guild = interaction.guild
    await interaction.response.defer(ephemeral=True, thinking=True)

    await ensure_core_roles(guild)
    await set_verification_permissions(guild)

    verify_channel = get_verify_channel(guild)
    missing: List[str] = []
    if verify_channel is None:
        missing.append(f"#{VERIFY_CHANNEL_NAME}")
    for name in PREP_CHANNEL_NAMES:
        found = discord.utils.find(
            lambda c: isinstance(c, discord.VoiceChannel) and slug(c.name) == slug(name),
            guild.channels,
        )
        if found is None:
            missing.append(name)

    if verify_channel is not None:
        should_post = True
        async for msg in verify_channel.history(limit=20):
            if msg.author == guild.me and msg.components:
                should_post = False
                break
        if should_post:
            embed = discord.Embed(
                title="Vérification Valorant",
                description="Choisis ton **rank Valorant** pour débloquer l'accès au serveur.\nLe salon est en **lecture seule** : tout se fait via le menu.",
                color=discord.Color.blurple(),
            )
            await verify_channel.send(embed=embed, view=VerificationView())

    text = "✅ Setup terminé.\n"
    if missing:
        text += "⚠️ Salons introuvables : " + ", ".join(missing)
    else:
        text += "Tous les salons requis ont été trouvés."
    await interaction.followup.send(text, ephemeral=True)


@bot.tree.command(name="pp", description="Lance une partie perso depuis ton vocal Préparation.")
@app_commands.guild_only()
async def pp(interaction: discord.Interaction) -> None:
    if not isinstance(interaction.user, discord.Member):
        return await interaction.response.send_message("Interaction invalide.", ephemeral=True)
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
