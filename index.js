const {
  Client,
  GatewayIntentBits,
  REST,
  Routes,
  SlashCommandBuilder,
  EmbedBuilder,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  StringSelectMenuBuilder,
  StringSelectMenuOptionBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle
} = require("discord.js");
const fetch = require("node-fetch");
const FormData = require("form-data");
const mm = require("music-metadata");
const crypto = require("crypto");

const FILE_COUNT = 5;
const DEFAULT_PREFIX = ".";
const MAX_EMBED_FIELD = 1024;
const MAX_EMBED_DESC = 4096;
const PENDING_TTL = 300_000;
const POLL_INTERVAL = 3000;
const POLL_MAX = 20;
const CF_KV_KEY = "botdata";
const C = { ok: 0x57f287, err: 0xed4245, info: 0x5865f2, warn: 0xfee75c };

const REQUIRED_ENV = ["DISCORD_TOKEN", "OWNER_ID"];
const CF_ENV = ["CF_ACCOUNT_ID", "CF_KV_NAMESPACE_ID", "CF_API_TOKEN"];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`[FATAL] Missing required env var: ${key}`);
    process.exit(1);
  }
}
for (const key of CF_ENV) {
  if (!process.env[key]) {
    console.warn(`[WARN] Missing env var: ${key} — data will not persist across restarts`);
  }
}

const pending = new Map();
const pendingTimers = new Map();

let dataCache = null;
let dataCacheTTL = 0;
const CACHE_DURATION = 10_000;

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
});

function cfKvUrl() {
  return `https://api.cloudflare.com/client/v4/accounts/${process.env.CF_ACCOUNT_ID}/storage/kv/namespaces/${process.env.CF_KV_NAMESPACE_ID}/values/${CF_KV_KEY}`;
}

function cfHeaders(extra = {}) {
  return { "Authorization": `Bearer ${process.env.CF_API_TOKEN}`, ...extra };
}

async function loadData(bust = false) {
  if (!bust && dataCache && Date.now() < dataCacheTTL) return structuredClone(dataCache);
  const defaults = {
    whitelist: [],
    universeId: null,
    staffRole: "Staff Team",
    prefixes: {},
    mainDatastore: "MainData_v2"
  };
  if (!process.env.CF_ACCOUNT_ID || !process.env.CF_KV_NAMESPACE_ID || !process.env.CF_API_TOKEN)
    return defaults;
  try {
    const res = await fetch(cfKvUrl(), { headers: cfHeaders() });
    if (res.status === 404) return defaults;
    if (!res.ok) {
      console.error(`[loadData] CF KV fetch failed: ${res.status}`);
      return defaults;
    }
    const record = await res.json();
    dataCache = { ...defaults, ...record };
    dataCacheTTL = Date.now() + CACHE_DURATION;
    return structuredClone(dataCache);
  } catch (e) {
    console.error("[loadData] error:", e.message);
    return defaults;
  }
}

async function saveData(d) {
  if (!process.env.CF_ACCOUNT_ID || !process.env.CF_KV_NAMESPACE_ID || !process.env.CF_API_TOKEN) {
    console.error("[saveData] CF env vars not set — cannot persist data");
    return false;
  }
  try {
    const res = await fetch(cfKvUrl(), {
      method: "PUT",
      headers: cfHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(d)
    });
    if (!res.ok) {
      console.error("[saveData] failed:", res.status, await res.text());
      return false;
    }
    dataCache = structuredClone(d);
    dataCacheTTL = Date.now() + CACHE_DURATION;
    return true;
  } catch (e) {
    console.error("[saveData] error:", e.message);
    return false;
  }
}

function isOwner(userId) {
  return userId === process.env.OWNER_ID;
}

async function hasStaff(member) {
  if (!member) return false;
  const d = await loadData();
  return member.roles.cache.some((r) => r.name === d.staffRole);
}

async function isWhitelisted(userId) {
  const d = await loadData();
  return d.whitelist.includes(userId);
}

async function getPrefix(guildId) {
  const d = await loadData();
  return d.prefixes?.[guildId] || DEFAULT_PREFIX;
}

function md5b64(str) {
  return crypto.createHash("md5").update(str).digest("base64");
}

async function getRobloxUser(username) {
  if (!username || typeof username !== "string" || username.length > 20) return null;
  try {
    const res = await fetch("https://users.roblox.com/v1/usernames/users", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ usernames: [username], excludeBannedUsers: false })
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data?.data?.[0] || null;
  } catch {
    return null;
  }
}

async function getRobloxUserById(userId) {
  if (!userId || !/^\d+$/.test(String(userId))) return null;
  try {
    const res = await fetch(`https://users.roblox.com/v1/users/${userId}`);
    if (!res.ok) return null;
    const data = await res.json();
    return data?.id ? data : null;
  } catch {
    return null;
  }
}

async function resolveRobloxId(input) {
  if (!input || typeof input !== "string") return null;
  const clean = input.trim().slice(0, 50);
  if (/^\d+$/.test(clean)) {
    const u = await getRobloxUserById(clean);
    return u ? { id: String(u.id), name: u.name, displayName: u.displayName } : null;
  }
  const u = await getRobloxUser(clean);
  return u ? { id: String(u.id), name: u.name, displayName: u.displayName } : null;
}

async function pollOperation(opPath) {
  for (let i = 0; i < POLL_MAX; i++) {
    await new Promise((r) => setTimeout(r, POLL_INTERVAL));
    try {
      const res = await fetch(`https://apis.roblox.com/assets/v1/${opPath}`, {
        headers: { "x-api-key": process.env.ROBLOX_API_KEY }
      });
      if (!res.ok) continue;
      const data = await res.json();
      if (data.done) return data.response?.assetId;
    } catch {
      continue;
    }
  }
  return null;
}

function fmtDuration(secs) {
  const m = Math.floor(secs / 60);
  const s = Math.round(secs % 60);
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

async function uploadFile(file) {
  const fileRes = await fetch(file.url);
  if (!fileRes.ok) throw new Error("failed to download file");
  const buf = Buffer.from(await fileRes.arrayBuffer());
  if (buf.length > 20 * 1024 * 1024) throw new Error("file too large (>20MB)");
  const meta = await mm.parseBuffer(buf, { mimeType: "audio/mpeg" });
  const duration = fmtDuration(meta.format.duration || 0);
  const form = new FormData();
  form.append(
    "request",
    JSON.stringify({
      assetType: "Audio",
      displayName: file.name.replace(/\.mp3$/i, "").slice(0, 50),
      description: "",
      creationContext: { creator: { userId: process.env.ROBLOX_USER_ID } }
    }),
    { contentType: "application/json" }
  );
  form.append("fileContent", buf, { filename: file.name, contentType: "audio/mpeg" });
  const res = await fetch("https://apis.roblox.com/assets/v1/assets", {
    method: "POST",
    headers: { "x-api-key": process.env.ROBLOX_API_KEY, ...form.getHeaders() },
    body: form
  });
  const data = await res.json();
  if (!data.path) throw new Error(JSON.stringify(data));
  const assetId = await pollOperation(data.path);
  if (!assetId) throw new Error("poll timed out");
  return { name: file.name.replace(/\.mp3$/i, ""), assetId, duration };
}

function dsBase(uid) {
  return `https://apis.roblox.com/datastores/v1/universes/${uid}/standard-datastores`;
}

function dsH(extra = {}) {
  return { "x-api-key": process.env.ROBLOX_BAN_API_KEY, ...extra };
}

async function dsListStores(uid) {
  const res = await fetch(`${dsBase(uid)}?limit=50`, { headers: dsH() });
  if (!res.ok) throw new Error(`dsListStores: ${res.status}`);
  return res.json();
}

async function dsListEntries(uid, store) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries?datastoreName=${encodeURIComponent(store)}&limit=25`,
    { headers: dsH() }
  );
  if (!res.ok) throw new Error(`dsListEntries: ${res.status}`);
  return res.json();
}

async function dsGetEntry(uid, store, key) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`,
    { headers: dsH() }
  );
  return { status: res.status, body: await res.text() };
}

async function dsSetEntry(uid, store, key, value) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`,
    {
      method: "POST",
      headers: dsH({ "Content-Type": "application/json", "content-md5": md5b64(value) }),
      body: value
    }
  );
  return { status: res.status, ok: res.ok };
}

async function dsDeleteEntry(uid, store, key) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`,
    { method: "DELETE", headers: dsH() }
  );
  return { status: res.status, ok: res.ok };
}

async function dsIncrementEntry(uid, store, key, amount) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries/entry/increment?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&incrementBy=${amount}`,
    { method: "POST", headers: dsH() }
  );
  return { status: res.status, body: await res.text(), ok: res.ok };
}

async function dsListVersions(uid, store, key) {
  const res = await fetch(
    `${dsBase(uid)}/datastore/entries/entry/versions?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&limit=10`,
    { headers: dsH() }
  );
  if (!res.ok) throw new Error(`dsListVersions: ${res.status}`);
  return res.json();
}

async function getPlayerData(uid, userId, dsName) {
  const { status, body } = await dsGetEntry(uid, dsName, `Player_${userId}`);
  if (status !== 200) return null;
  try {
    return JSON.parse(body);
  } catch {
    return null;
  }
}

async function setPlayerData(uid, userId, pdata, dsName) {
  return dsSetEntry(uid, dsName, `Player_${userId}`, JSON.stringify(pdata));
}

function emb(color, desc, title) {
  const e = new EmbedBuilder().setColor(color).setDescription(desc.slice(0, MAX_EMBED_DESC));
  if (title) e.setTitle(title.slice(0, 256));
  return { embeds: [e] };
}

function confirmRow(token) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`confirm_${token}`).setLabel("Confirm").setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId(`cancel_${token}`).setLabel("Cancel").setStyle(ButtonStyle.Secondary)
  );
}

function mkToken() {
  return crypto.randomBytes(6).toString("hex");
}

function setPending(token, data) {
  if (pendingTimers.has(token)) clearTimeout(pendingTimers.get(token));
  pending.set(token, data);
  const timer = setTimeout(() => {
    pending.delete(token);
    pendingTimers.delete(token);
  }, PENDING_TTL);
  pendingTimers.set(token, timer);
}

function getPending(token) {
  return pending.get(token) || null;
}

function consumePending(token) {
  const data = pending.get(token);
  if (!data) return null;
  pending.delete(token);
  if (pendingTimers.has(token)) {
    clearTimeout(pendingTimers.get(token));
    pendingTimers.delete(token);
  }
  return data;
}

function makeSaveSelect(saves, field, token) {
  const keys = Object.keys(saves);
  if (!keys.length) return null;
  const opts = keys
    .sort((a, b) => {
      const na = parseInt(a.replace(/\D/g, ""));
      const nb = parseInt(b.replace(/\D/g, ""));
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return a.localeCompare(b);
    })
    .slice(0, 25)
    .map((slot) =>
      new StringSelectMenuOptionBuilder()
        .setLabel(slot)
        .setValue(slot)
        .setDescription(String(saves[slot]).slice(0, 100))
    );
  return new ActionRowBuilder().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId(`${field}_select_${token}`)
      .setPlaceholder("Select a slot to edit...")
      .addOptions(opts)
  );
}

function fmtSaves(saves) {
  return Object.entries(saves)
    .sort(([a], [b]) => {
      const na = parseInt(a.replace(/\D/g, ""));
      const nb = parseInt(b.replace(/\D/g, ""));
      if (!isNaN(na) && !isNaN(nb)) return na - nb;
      return a.localeCompare(b);
    })
    .map(([k, v]) => `\`${k}\`: ${v}`)
    .join("\n");
}

function trunc(s, max = MAX_EMBED_FIELD - 4) {
  if (!s) return "none";
  return s.length > max ? s.slice(0, max) + "..." : s;
}

async function requireUniverse() {
  const d = await loadData();
  return d.universeId ? d : null;
}

async function safeReply(target, content) {
  try {
    return await target.reply(typeof content === "string" ? emb(C.info, content) : content);
  } catch (e) {
    console.error("safeReply failed:", e.message);
    return null;
  }
}

async function safeEdit(msg, content) {
  try {
    return await msg.edit(typeof content === "string" ? emb(C.info, content) : content);
  } catch (e) {
    console.error("safeEdit failed:", e.message);
    return null;
  }
}

async function safeInteractionReply(interaction, content, ephemeral = false) {
  try {
    if (interaction.replied || interaction.deferred) return await interaction.editReply(content);
    return await interaction.reply({ ...content, ephemeral });
  } catch (e) {
    console.error("safeInteractionReply failed:", e.message);
    return null;
  }
}

async function safeInteractionUpdate(interaction, content) {
  try {
    if (interaction.replied || interaction.deferred) return await interaction.editReply(content);
    return await interaction.update(content);
  } catch (e) {
    console.error("safeInteractionUpdate failed:", e.message);
    return null;
  }
}

const uploadSlash = new SlashCommandBuilder()
  .setName("upload")
  .setDescription("Upload up to 5 MP3s to Roblox");
for (let i = 1; i <= FILE_COUNT; i++) {
  uploadSlash.addAttachmentOption((o) =>
    o.setName(`file${i}`).setDescription(`MP3 file ${i}`).setRequired(i === 1)
  );
}

const whitelistSlash = new SlashCommandBuilder()
  .setName("whitelist")
  .setDescription("Manage upload whitelist")
  .addSubcommand((s) =>
    s.setName("add").setDescription("Whitelist a user")
      .addUserOption((o) => o.setName("user").setDescription("Discord user").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("remove").setDescription("Remove a user")
      .addUserOption((o) => o.setName("user").setDescription("Discord user").setRequired(true))
  )
  .addSubcommand((s) => s.setName("list").setDescription("Show whitelisted users"));

const setupSlash = new SlashCommandBuilder()
  .setName("setup")
  .setDescription("Configure bot settings")
  .addSubcommand((s) =>
    s.setName("universe").setDescription("Set universe ID")
      .addStringOption((o) => o.setName("id").setDescription("Universe ID").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("staffrole").setDescription("Set staff role")
      .addStringOption((o) => o.setName("name").setDescription("Exact role name").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("prefix").setDescription("Set prefix")
      .addStringOption((o) => o.setName("prefix").setDescription("New prefix").setRequired(true))
  )
  .addSubcommand((s) => s.setName("show").setDescription("Show current settings"));

const dsSlash = new SlashCommandBuilder()
  .setName("ds")
  .setDescription("Manage Roblox datastores")
  .addSubcommand((s) => s.setName("list").setDescription("List all datastores"))
  .addSubcommand((s) =>
    s.setName("entries").setDescription("List entries")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("get").setDescription("Get entry")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
      .addStringOption((o) => o.setName("key").setDescription("Entry key").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("set").setDescription("Set entry")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
      .addStringOption((o) => o.setName("key").setDescription("Entry key").setRequired(true))
      .addStringOption((o) => o.setName("value").setDescription("Value").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("delete").setDescription("Delete entry")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
      .addStringOption((o) => o.setName("key").setDescription("Entry key").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("increment").setDescription("Increment entry")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
      .addStringOption((o) => o.setName("key").setDescription("Entry key").setRequired(true))
      .addNumberOption((o) => o.setName("amount").setDescription("Amount").setRequired(true))
  )
  .addSubcommand((s) =>
    s.setName("versions").setDescription("List versions")
      .addStringOption((o) => o.setName("store").setDescription("Datastore name").setRequired(true))
      .addStringOption((o) => o.setName("key").setDescription("Entry key").setRequired(true))
  );

const banSlash = new SlashCommandBuilder()
  .setName("rban")
  .setDescription("Ban a Roblox user")
  .addStringOption((o) => o.setName("username").setDescription("Roblox username or user ID").setRequired(true))
  .addStringOption((o) => o.setName("reason").setDescription("Ban reason"));

const unbanSlash = new SlashCommandBuilder()
  .setName("runban")
  .setDescription("Unban a Roblox user")
  .addStringOption((o) => o.setName("username").setDescription("Roblox username or user ID").setRequired(true));

const slashCommands = [uploadSlash, whitelistSlash, setupSlash, dsSlash, banSlash, unbanSlash];

client.once("ready", async () => {
  try {
    const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN);
    await rest.put(Routes.applicationCommands(client.user.id), {
      body: slashCommands.map((c) => c.toJSON())
    });
    console.log(`ready as ${client.user.tag}`);
  } catch (e) {
    console.error("failed to register commands:", e);
  }
});

const prefixHandlers = {
  async setmainds(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (!args[0]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}setmainds <datastore name>\``));
    const name = args[0].slice(0, 100);
    const d = await loadData(true);
    d.mainDatastore = name;
    const ok = await saveData(d);
    if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
    return safeReply(msg, emb(C.ok, `main datastore set to \`${name}\``));
  },

  async getstats(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (!args[0]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}getstats <username/userid>\``));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(args[0]);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const pdata = await getPlayerData(d.universeId, user.id, dsName);
      if (!pdata) return safeEdit(status, emb(C.err, `no data for **${user.name}**.`));
      const data = pdata.Data;
      if (!data) return safeEdit(status, emb(C.err, "player data has no Data field."));
      const quest = data.quest || {};
      const trollSavesStr = trunc(fmtSaves(data.troll_saves || {}));
      const itemSavesStr = trunc(fmtSaves(data.item_saves || {}));
      const questStr = `**${quest.Name || "none"}**\nGoal: \`${quest.Goal ?? "?"}\` | Progress: \`${quest.Progress ?? "?"}\` | Reward: \`${quest.Reward || "?"}\``;
      const embed = new EmbedBuilder()
        .setColor(C.info)
        .setTitle(`Stats — ${user.displayName} (${user.name})`)
        .addFields(
          { name: "💰 Coins", value: `\`${(data.coins || 0).toLocaleString()}\``, inline: true },
          { name: "🎭 Stand", value: `\`${data.troll_stand || "none"}\``, inline: true },
          { name: "🔢 Type", value: `\`${data.type_i ?? "?"}\``, inline: true },
          { name: "📋 Quest", value: questStr },
          { name: `🎭 Troll Saves (${Object.keys(data.troll_saves || {}).length})`, value: trollSavesStr },
          { name: `🎒 Item Saves (${Object.keys(data.item_saves || {}).length})`, value: itemSavesStr }
        )
        .setFooter({ text: `Player_${user.id} | ${dsName}` });
      await safeEdit(status, { embeds: [embed] });
    } catch (e) {
      console.error("getstats error:", e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async settroll(msg, args, prefix) {
    return prefixHandlers._setSaveSlots(msg, args, prefix, "troll_saves", "Troll Saves", "settroll");
  },

  async setitem(msg, args, prefix) {
    return prefixHandlers._setSaveSlots(msg, args, prefix, "item_saves", "Item Saves", "setitem");
  },

  async _setSaveSlots(msg, args, prefix, field, label, cmdName) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (!args[0]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}${cmdName} <username/userid>\``));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(args[0]);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const pdata = await getPlayerData(d.universeId, user.id, dsName);
      if (!pdata) return safeEdit(status, emb(C.err, `no data for **${user.name}**.`));
      const saves = pdata.Data?.[field] || {};
      if (!Object.keys(saves).length)
        return safeEdit(status, emb(C.warn, `**${user.name}** has no ${label.toLowerCase()}.`));
      const token = mkToken();
      setPending(token, {
        type: `${field}_select`,
        uid: d.universeId,
        dsName,
        userId: user.id,
        username: user.name,
        displayName: user.displayName,
        pdata,
        field,
        initiator: msg.author.id
      });
      const preview = fmtSaves(saves);
      const selectRow = makeSaveSelect(saves, field, token);
      await safeEdit(status, {
        embeds: [
          new EmbedBuilder()
            .setColor(C.info)
            .setTitle(`${label} — ${user.displayName} (${user.name})`)
            .setDescription(preview.length > 2000 ? preview.slice(0, 2000) + "..." : preview || "empty")
            .setFooter({ text: "Select a slot below to edit" })
        ],
        components: selectRow ? [selectRow] : []
      });
    } catch (e) {
      console.error(`${cmdName} error:`, e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async setcoins(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (!args[0] || !args[1])
      return safeReply(msg, emb(C.warn, `usage: \`${prefix}setcoins <username/userid> <amount>\``));
    const amount = parseInt(args[1]);
    if (isNaN(amount) || amount < 0 || amount > Number.MAX_SAFE_INTEGER)
      return safeReply(msg, emb(C.err, "invalid amount."));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(args[0]);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const pdata = await getPlayerData(d.universeId, user.id, dsName);
      if (!pdata) return safeEdit(status, emb(C.err, `no data for **${user.name}**.`));
      const oldCoins = pdata.Data?.coins || 0;
      const newPdata = structuredClone(pdata);
      newPdata.Data.coins = amount;
      const token = mkToken();
      setPending(token, {
        type: "generic_confirm",
        uid: d.universeId,
        dsName,
        userId: user.id,
        newPdata,
        initiator: msg.author.id
      });
      await safeEdit(status, {
        ...emb(C.warn, [
          `**Player:** ${user.displayName} (\`${user.name}\`)`,
          `**Coins:** \`${oldCoins.toLocaleString()}\` → \`${amount.toLocaleString()}\``
        ].join("\n"), "Confirm Set Coins"),
        components: [confirmRow(token)]
      });
    } catch (e) {
      console.error("setcoins error:", e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async setstand(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    const stand = args.slice(1).join(" ");
    if (!args[0] || !stand)
      return safeReply(msg, emb(C.warn, `usage: \`${prefix}setstand <username/userid> <stand name>\``));
    if (stand.length > 200) return safeReply(msg, emb(C.err, "stand name too long (max 200)."));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(args[0]);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const pdata = await getPlayerData(d.universeId, user.id, dsName);
      if (!pdata) return safeEdit(status, emb(C.err, `no data for **${user.name}**.`));
      const oldStand = pdata.Data?.troll_stand || "none";
      const newPdata = structuredClone(pdata);
      newPdata.Data.troll_stand = stand;
      const token = mkToken();
      setPending(token, {
        type: "generic_confirm",
        uid: d.universeId,
        dsName,
        userId: user.id,
        newPdata,
        initiator: msg.author.id
      });
      await safeEdit(status, {
        ...emb(C.warn, [
          `**Player:** ${user.displayName} (\`${user.name}\`)`,
          `**Stand:** \`${oldStand}\` → \`${stand}\``
        ].join("\n"), "Confirm Set Stand"),
        components: [confirmRow(token)]
      });
    } catch (e) {
      console.error("setstand error:", e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async setquest(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (args.length < 5)
      return safeReply(msg, emb(C.warn, `usage: \`${prefix}setquest <user> <name> <goal> <progress> <reward>\` (use _ for spaces)`));
    const [input, rawName, goalStr, progressStr, ...rewardParts] = args;
    const questName = rawName.replace(/_/g, " ").slice(0, 200);
    const goal = parseInt(goalStr);
    const progress = parseInt(progressStr);
    const reward = rewardParts.join(" ").replace(/_/g, " ").slice(0, 200);
    if (isNaN(goal) || isNaN(progress)) return safeReply(msg, emb(C.err, "goal and progress must be numbers."));
    if (goal < 0 || progress < 0) return safeReply(msg, emb(C.err, "goal and progress must be non-negative."));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(input);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const pdata = await getPlayerData(d.universeId, user.id, dsName);
      if (!pdata) return safeEdit(status, emb(C.err, `no data for **${user.name}**.`));
      const newPdata = structuredClone(pdata);
      newPdata.Data.quest = { Name: questName, Goal: goal, Progress: progress, Reward: reward };
      const token = mkToken();
      setPending(token, {
        type: "generic_confirm",
        uid: d.universeId,
        dsName,
        userId: user.id,
        newPdata,
        initiator: msg.author.id
      });
      await safeEdit(status, {
        ...emb(C.warn, [
          `**Player:** ${user.displayName} (\`${user.name}\`)`,
          `**Name:** ${questName}`,
          `**Goal:** ${goal} | **Progress:** ${progress}`,
          `**Reward:** ${reward}`
        ].join("\n"), "Confirm Set Quest"),
        components: [confirmRow(token)]
      });
    } catch (e) {
      console.error("setquest error:", e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async viewraw(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    if (!args[0]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}viewraw <username/userid>\``));
    const status = await safeReply(msg, emb(C.info, "fetching..."));
    if (!status) return;
    try {
      const d = await requireUniverse();
      if (!d) return safeEdit(status, emb(C.err, "universe ID not set."));
      const dsName = d.mainDatastore || "MainData_v2";
      const user = await resolveRobloxId(args[0]);
      if (!user) return safeEdit(status, emb(C.err, "user not found."));
      const { status: httpStatus, body } = await dsGetEntry(d.universeId, dsName, `Player_${user.id}`);
      if (httpStatus !== 200) return safeEdit(status, emb(C.err, `no data found (${httpStatus}).`));
      const preview = body.length > 1800 ? body.slice(0, 1800) + "\n...(truncated)" : body;
      await safeEdit(status, emb(C.info, `\`\`\`json\n${preview}\n\`\`\``, `Raw — Player_${user.id}`));
    } catch (e) {
      console.error("viewraw error:", e);
      await safeEdit(status, emb(C.err, `error: ${e.message}`));
    }
  },

  async setprefix(msg, args, prefix) {
    if (!await hasStaff(msg.member)) return safeReply(msg, emb(C.err, "staff only."));
    const np = args[0];
    if (!np) return safeReply(msg, emb(C.warn, `usage: \`${prefix}setprefix <new prefix>\``));
    if (np.length > 5) return safeReply(msg, emb(C.err, "prefix too long (max 5 chars)."));
    const d = await loadData(true);
    if (!d.prefixes) d.prefixes = {};
    d.prefixes[msg.guild.id] = np;
    const ok = await saveData(d);
    if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
    return safeReply(msg, emb(C.ok, `prefix set to \`${np}\``));
  },

  async upload(msg, args, prefix) {
    if (!await isWhitelisted(msg.author.id) && !await hasStaff(msg.member))
      return safeReply(msg, emb(C.err, "you're not whitelisted."));
    const attachments = [...msg.attachments.values()].filter((a) => a.name?.toLowerCase().endsWith(".mp3"));
    if (!attachments.length) return safeReply(msg, emb(C.err, "attach at least one mp3 file."));
    if (attachments.length > FILE_COUNT) return safeReply(msg, emb(C.err, `max ${FILE_COUNT} files at a time.`));
    const status = await safeReply(msg, emb(C.info, `uploading ${attachments.length} file(s)...`));
    if (!status) return;
    const lines = [];
    for (const file of attachments) {
      try {
        const { name, assetId, duration } = await uploadFile(file);
        lines.push(`**${name}** — \`${assetId}\` — ${duration}`);
      } catch (e) {
        lines.push(`**${file.name}** — failed: ${e.message}`);
      }
    }
    return safeEdit(status, emb(C.ok, lines.join("\n"), "Upload Results"));
  },

  async whitelist(msg, args, prefix) {
    if (!isOwner(msg.author.id)) return safeReply(msg, emb(C.err, "owner only."));
    const sub = args[0];
    const d = await loadData(true);
    if (sub === "add") {
      const mentioned = msg.mentions.users.first();
      if (!mentioned) return safeReply(msg, emb(C.warn, `usage: \`${prefix}whitelist add @user\``));
      if (d.whitelist.includes(mentioned.id))
        return safeReply(msg, emb(C.warn, `${mentioned.username} is already whitelisted.`));
      d.whitelist.push(mentioned.id);
      const ok = await saveData(d);
      if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
      return safeReply(msg, emb(C.ok, `**${mentioned.username}** added to whitelist.`));
    }
    if (sub === "remove") {
      const mentioned = msg.mentions.users.first();
      if (!mentioned) return safeReply(msg, emb(C.warn, `usage: \`${prefix}whitelist remove @user\``));
      if (!d.whitelist.includes(mentioned.id))
        return safeReply(msg, emb(C.warn, `${mentioned.username} isn't whitelisted.`));
      d.whitelist = d.whitelist.filter((id) => id !== mentioned.id);
      const ok = await saveData(d);
      if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
      return safeReply(msg, emb(C.ok, `**${mentioned.username}** removed from whitelist.`));
    }
    if (sub === "list") {
      if (!d.whitelist.length) return safeReply(msg, emb(C.info, "no users whitelisted."));
      return safeReply(msg, emb(C.info, d.whitelist.map((id) => `<@${id}>`).join("\n"), "Whitelisted Users"));
    }
    return safeReply(msg, emb(C.warn, `usage: \`${prefix}whitelist add/remove/list\``));
  },

  async setup(msg, args, prefix) {
    if (!await hasStaff(msg.member)) return safeReply(msg, emb(C.err, "staff only."));
    const sub = args[0];
    const d = await loadData(true);
    if (sub === "universe") {
      if (!args[1]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}setup universe <id>\``));
      if (!/^\d+$/.test(args[1])) return safeReply(msg, emb(C.err, "universe ID must be numeric."));
      d.universeId = args[1];
      const ok = await saveData(d);
      if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
      return safeReply(msg, emb(C.ok, `universe ID set to \`${d.universeId}\``));
    }
    if (sub === "staffrole") {
      const roleName = args.slice(1).join(" ");
      if (!roleName) return safeReply(msg, emb(C.warn, `usage: \`${prefix}setup staffrole <role name>\``));
      if (roleName.length > 100) return safeReply(msg, emb(C.err, "role name too long (max 100)."));
      d.staffRole = roleName;
      const ok = await saveData(d);
      if (!ok) return safeReply(msg, emb(C.err, "failed to save. check CF env vars."));
      return safeReply(msg, emb(C.ok, `staff role set to **${d.staffRole}**`));
    }
    if (sub === "show") {
      return safeReply(msg, emb(C.info, [
        `**Universe ID:** \`${d.universeId || "not set"}\``,
        `**Staff Role:** ${d.staffRole || "Staff Team"}`,
        `**Main Datastore:** \`${d.mainDatastore || "MainData_v2"}\``,
        `**Prefix:** \`${prefix}\``,
        `**Whitelisted Users:** ${d.whitelist.length}`
      ].join("\n"), "Bot Settings"));
    }
    return safeReply(msg, emb(C.warn, `usage: \`${prefix}setup universe/staffrole/show\``));
  },

  async rban(msg, args, prefix) {
    if (!await hasStaff(msg.member)) return safeReply(msg, emb(C.err, "staff only."));
    const input = args[0];
    if (!input) return safeReply(msg, emb(C.warn, `usage: \`${prefix}rban <username or userid> [reason]\``));
    const reason = args.slice(1).join(" ").slice(0, 500) || "No reason provided";
    const d = await loadData();
    const gameId = d.universeId;
    if (!gameId) return safeReply(msg, emb(C.err, "universe ID not set."));
    const status = await safeReply(msg, emb(C.info, "looking up user..."));
    if (!status) return;
    const user = await resolveRobloxId(input);
    if (!user) return safeEdit(status, emb(C.err, `no Roblox user found for **${input}**.`));
    const token = mkToken();
    setPending(token, {
      type: "ban",
      userId: user.id,
      username: user.name,
      displayName: user.displayName,
      reason,
      gameId,
      by: msg.author.username,
      initiator: msg.author.id
    });
    return safeEdit(status, {
      ...emb(C.warn, [
        `**Roblox User:** ${user.displayName} (\`${user.name}\`)`,
        `**User ID:** \`${user.id}\``,
        `**Reason:** ${reason}`
      ].join("\n"), "Confirm Ban"),
      components: [confirmRow(token)]
    });
  },

  async runban(msg, args, prefix) {
    if (!await hasStaff(msg.member)) return safeReply(msg, emb(C.err, "staff only."));
    const input = args[0];
    if (!input) return safeReply(msg, emb(C.warn, `usage: \`${prefix}runban <username or userid>\``));
    const d = await loadData();
    const gameId = d.universeId;
    if (!gameId) return safeReply(msg, emb(C.err, "universe ID not set."));
    const status = await safeReply(msg, emb(C.info, "looking up user..."));
    if (!status) return;
    const user = await resolveRobloxId(input);
    if (!user) return safeEdit(status, emb(C.err, `no Roblox user found for **${input}**.`));
    const token = mkToken();
    setPending(token, {
      type: "unban",
      userId: user.id,
      username: user.name,
      displayName: user.displayName,
      gameId,
      by: msg.author.username,
      initiator: msg.author.id
    });
    return safeEdit(status, {
      ...emb(C.warn, [
        `**Roblox User:** ${user.displayName} (\`${user.name}\`)`,
        `**User ID:** \`${user.id}\``
      ].join("\n"), "Confirm Unban"),
      components: [confirmRow(token)]
    });
  },

  async ds(msg, args, prefix) {
    if (!await hasStaff(msg.member) && !await isWhitelisted(msg.author.id))
      return safeReply(msg, emb(C.err, "you're not whitelisted."));
    const sub = args[0];
    const d = await loadData();
    const uid = d.universeId;
    if (!uid) return safeReply(msg, emb(C.err, "universe ID not set."));
    const store = args[1];
    const key = args[2];
    if (sub === "list") {
      try {
        const data = await dsListStores(uid);
        const stores = data?.datastores?.map((s) => `\`${s.name}\``).join("\n");
        return safeReply(msg, emb(stores ? C.info : C.warn, stores || "no datastores found.", stores ? "Datastores" : null));
      } catch (e) {
        return safeReply(msg, emb(C.err, `error: ${e.message}`));
      }
    }
    if (sub === "entries") {
      if (!store) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds entries <store>\``));
      try {
        const data = await dsListEntries(uid, store);
        const keys = data?.keys?.map((k) => `\`${k.key}\``).join("\n");
        return safeReply(msg, emb(keys ? C.info : C.warn, keys || "no entries found.", keys ? `${store} Entries` : null));
      } catch (e) {
        return safeReply(msg, emb(C.err, `error: ${e.message}`));
      }
    }
    if (sub === "get") {
      if (!store || !key) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds get <store> <key>\``));
      const { status, body } = await dsGetEntry(uid, store, key);
      if (status !== 200) return safeReply(msg, emb(C.err, `failed (${status})`));
      const preview = body.length > 1800 ? body.slice(0, 1800) + "..." : body;
      return safeReply(msg, emb(C.info, `\`\`\`json\n${preview}\n\`\`\``, `${store} / ${key}`));
    }
    if (sub === "set") {
      if (!store || !key || !args[3]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds set <store> <key> <value>\``));
      let value = args.slice(3).join(" ");
      try { JSON.parse(value); } catch { value = JSON.stringify(value); }
      const { status, ok } = await dsSetEntry(uid, store, key, value);
      return safeReply(msg, emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** updated.` : `failed (${status})`));
    }
    if (sub === "delete") {
      if (!store || !key) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds delete <store> <key>\``));
      const token = mkToken();
      setPending(token, { type: "dsdel", uid, store, key, initiator: msg.author.id });
      return safeReply(msg, {
        ...emb(C.warn, `delete **${store} / ${key}**?`, "Confirm Delete"),
        components: [confirmRow(token)]
      });
    }
    if (sub === "increment") {
      if (!store || !key || !args[3]) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds increment <store> <key> <amount>\``));
      const amount = parseFloat(args[3]);
      if (isNaN(amount)) return safeReply(msg, emb(C.err, "amount must be a number."));
      const { status, body, ok } = await dsIncrementEntry(uid, store, key, amount);
      return safeReply(msg, emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** new value: \`${body}\`` : `failed (${status})`));
    }
    if (sub === "versions") {
      if (!store || !key) return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds versions <store> <key>\``));
      try {
        const data = await dsListVersions(uid, store, key);
        const versions = data?.versions?.map((v) => `\`${v.version}\` — ${new Date(v.createdTime).toLocaleString()}`).join("\n");
        return safeReply(msg, emb(versions ? C.info : C.warn, versions || "no versions found.", versions ? `${store} / ${key} Versions` : null));
      } catch (e) {
        return safeReply(msg, emb(C.err, `error: ${e.message}`));
      }
    }
    return safeReply(msg, emb(C.warn, `usage: \`${prefix}ds list/entries/get/set/delete/increment/versions\``));
  }
};

client.on("messageCreate", async (msg) => {
  if (msg.author.bot || !msg.guild) return;
  const prefix = await getPrefix(msg.guild.id);
  if (!msg.content.startsWith(prefix)) return;
  const args = msg.content.slice(prefix.length).trim().split(/\s+/);
  const cmd = args.shift()?.toLowerCase();
  if (!cmd) return;
  const handler = prefixHandlers[cmd];
  if (!handler || cmd.startsWith("_")) return;
  try {
    await handler(msg, args, prefix);
  } catch (e) {
    console.error(`unhandled error in ${cmd}:`, e);
    safeReply(msg, emb(C.err, "an unexpected error occurred.")).catch(() => {});
  }
});

async function handleSlashUpload(interaction) {
  if (!await isWhitelisted(interaction.user.id) && !await hasStaff(interaction.member))
    return safeInteractionReply(interaction, emb(C.err, "you're not whitelisted."), true);
  const files = [];
  for (let i = 1; i <= FILE_COUNT; i++) {
    const f = interaction.options.getAttachment(`file${i}`);
    if (f) files.push(f);
  }
  const mp3s = files.filter((f) => f.name?.toLowerCase().endsWith(".mp3"));
  if (!mp3s.length) return safeInteractionReply(interaction, emb(C.err, "attach at least one mp3 file."), true);
  await interaction.deferReply();
  const lines = [];
  for (const file of mp3s) {
    try {
      const { name, assetId, duration } = await uploadFile(file);
      lines.push(`**${name}** — \`${assetId}\` — ${duration}`);
    } catch (e) {
      lines.push(`**${file.name}** — failed: ${e.message}`);
    }
  }
  return interaction.editReply(emb(C.ok, lines.join("\n"), "Upload Results"));
}

async function handleSlashWhitelist(interaction) {
  if (!isOwner(interaction.user.id))
    return safeInteractionReply(interaction, emb(C.err, "owner only."), true);
  const sub = interaction.options.getSubcommand();
  const d = await loadData(true);
  if (sub === "add") {
    const user = interaction.options.getUser("user");
    if (d.whitelist.includes(user.id))
      return safeInteractionReply(interaction, emb(C.warn, `${user.username} is already whitelisted.`), true);
    d.whitelist.push(user.id);
    const ok = await saveData(d);
    if (!ok) return safeInteractionReply(interaction, emb(C.err, "failed to save. check CF env vars."), true);
    return safeInteractionReply(interaction, emb(C.ok, `**${user.username}** added to whitelist.`));
  }
  if (sub === "remove") {
    const user = interaction.options.getUser("user");
    if (!d.whitelist.includes(user.id))
      return safeInteractionReply(interaction, emb(C.warn, `${user.username} isn't whitelisted.`), true);
    d.whitelist = d.whitelist.filter((id) => id !== user.id);
    const ok = await saveData(d);
    if (!ok) return safeInteractionReply(interaction, emb(C.err, "failed to save. check CF env vars."), true);
    return safeInteractionReply(interaction, emb(C.ok, `**${user.username}** removed from whitelist.`));
  }
  if (sub === "list") {
    if (!d.whitelist.length) return safeInteractionReply(interaction, emb(C.info, "no users whitelisted."));
    return safeInteractionReply(interaction, emb(C.info, d.whitelist.map((id) => `<@${id}>`).join("\n"), "Whitelisted Users"));
  }
}

async function handleSlashSetup(interaction) {
  if (!await hasStaff(interaction.member))
    return safeInteractionReply(interaction, emb(C.err, "staff only."), true);
  const sub = interaction.options.getSubcommand();
  const d = await loadData(true);
  if (sub === "universe") {
    const id = interaction.options.getString("id");
    if (!/^\d+$/.test(id)) return safeInteractionReply(interaction, emb(C.err, "universe ID must be numeric."), true);
    d.universeId = id;
    const ok = await saveData(d);
    if (!ok) return safeInteractionReply(interaction, emb(C.err, "failed to save. check CF env vars."), true);
    return safeInteractionReply(interaction, emb(C.ok, `universe ID set to \`${id}\``));
  }
  if (sub === "staffrole") {
    const name = interaction.options.getString("name").slice(0, 100);
    d.staffRole = name;
    const ok = await saveData(d);
    if (!ok) return safeInteractionReply(interaction, emb(C.err, "failed to save. check CF env vars."), true);
    return safeInteractionReply(interaction, emb(C.ok, `staff role set to **${name}**`));
  }
  if (sub === "prefix") {
    const np = interaction.options.getString("prefix").slice(0, 5);
    if (!d.prefixes) d.prefixes = {};
    d.prefixes[interaction.guildId] = np;
    const ok = await saveData(d);
    if (!ok) return safeInteractionReply(interaction, emb(C.err, "failed to save. check CF env vars."), true);
    return safeInteractionReply(interaction, emb(C.ok, `prefix set to \`${np}\``));
  }
  if (sub === "show") {
    const prefix = d.prefixes?.[interaction.guildId] || DEFAULT_PREFIX;
    return safeInteractionReply(interaction, emb(C.info, [
      `**Universe ID:** \`${d.universeId || "not set"}\``,
      `**Staff Role:** ${d.staffRole || "Staff Team"}`,
      `**Main Datastore:** \`${d.mainDatastore || "MainData_v2"}\``,
      `**Prefix:** \`${prefix}\``,
      `**Whitelisted Users:** ${d.whitelist.length}`
    ].join("\n"), "Bot Settings"));
  }
}

async function handleSlashDs(interaction) {
  if (!await hasStaff(interaction.member) && !await isWhitelisted(interaction.user.id))
    return safeInteractionReply(interaction, emb(C.err, "you're not whitelisted."), true);
  const d = await loadData();
  const uid = d.universeId;
  if (!uid) return safeInteractionReply(interaction, emb(C.err, "universe ID not set."), true);
  const sub = interaction.options.getSubcommand();
  await interaction.deferReply();
  try {
    if (sub === "list") {
      const data = await dsListStores(uid);
      const stores = data?.datastores?.map((s) => `\`${s.name}\``).join("\n");
      return interaction.editReply(emb(stores ? C.info : C.warn, stores || "no datastores found.", stores ? "Datastores" : null));
    }
    const store = interaction.options.getString("store");
    const key = interaction.options.getString("key");
    if (sub === "entries") {
      const data = await dsListEntries(uid, store);
      const keys = data?.keys?.map((k) => `\`${k.key}\``).join("\n");
      return interaction.editReply(emb(keys ? C.info : C.warn, keys || "no entries found.", keys ? `${store} Entries` : null));
    }
    if (sub === "get") {
      const { status, body } = await dsGetEntry(uid, store, key);
      if (status !== 200) return interaction.editReply(emb(C.err, `failed (${status})`));
      const preview = body.length > 1800 ? body.slice(0, 1800) + "..." : body;
      return interaction.editReply(emb(C.info, `\`\`\`json\n${preview}\n\`\`\``, `${store} / ${key}`));
    }
    if (sub === "set") {
      let value = interaction.options.getString("value");
      try { JSON.parse(value); } catch { value = JSON.stringify(value); }
      const { status, ok } = await dsSetEntry(uid, store, key, value);
      return interaction.editReply(emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** updated.` : `failed (${status})`));
    }
    if (sub === "delete") {
      const token = mkToken();
      setPending(token, { type: "dsdel", uid, store, key, initiator: interaction.user.id });
      return interaction.editReply({
        ...emb(C.warn, `delete **${store} / ${key}**?`, "Confirm Delete"),
        components: [confirmRow(token)]
      });
    }
    if (sub === "increment") {
      const amount = interaction.options.getNumber("amount");
      const { status, body, ok } = await dsIncrementEntry(uid, store, key, amount);
      return interaction.editReply(emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** new value: \`${body}\`` : `failed (${status})`));
    }
    if (sub === "versions") {
      const data = await dsListVersions(uid, store, key);
      const versions = data?.versions?.map((v) => `\`${v.version}\` — ${new Date(v.createdTime).toLocaleString()}`).join("\n");
      return interaction.editReply(emb(versions ? C.info : C.warn, versions || "no versions found.", versions ? `${store} / ${key} Versions` : null));
    }
  } catch (e) {
    console.error("ds slash error:", e);
    return interaction.editReply(emb(C.err, `error: ${e.message}`));
  }
}

async function handleSlashBan(interaction) {
  if (!await hasStaff(interaction.member))
    return safeInteractionReply(interaction, emb(C.err, "staff only."), true);
  const input = interaction.options.getString("username");
  const reason = interaction.options.getString("reason")?.slice(0, 500) || "No reason provided";
  const d = await loadData();
  const gameId = d.universeId;
  if (!gameId) return safeInteractionReply(interaction, emb(C.err, "universe ID not set."), true);
  await interaction.deferReply();
  const user = await resolveRobloxId(input);
  if (!user) return interaction.editReply(emb(C.err, `no Roblox user found for **${input}**.`));
  const token = mkToken();
  setPending(token, {
    type: "ban",
    userId: user.id,
    username: user.name,
    displayName: user.displayName,
    reason,
    gameId,
    by: interaction.user.username,
    initiator: interaction.user.id
  });
  return interaction.editReply({
    ...emb(C.warn, [
      `**Roblox User:** ${user.displayName} (\`${user.name}\`)`,
      `**User ID:** \`${user.id}\``,
      `**Reason:** ${reason}`
    ].join("\n"), "Confirm Ban"),
    components: [confirmRow(token)]
  });
}

async function handleSlashUnban(interaction) {
  if (!await hasStaff(interaction.member))
    return safeInteractionReply(interaction, emb(C.err, "staff only."), true);
  const input = interaction.options.getString("username");
  const d = await loadData();
  const gameId = d.universeId;
  if (!gameId) return safeInteractionReply(interaction, emb(C.err, "universe ID not set."), true);
  await interaction.deferReply();
  const user = await resolveRobloxId(input);
  if (!user) return interaction.editReply(emb(C.err, `no Roblox user found for **${input}**.`));
  const token = mkToken();
  setPending(token, {
    type: "unban",
    userId: user.id,
    username: user.name,
    displayName: user.displayName,
    gameId,
    by: interaction.user.username,
    initiator: interaction.user.id
  });
  return interaction.editReply({
    ...emb(C.warn, [
      `**Roblox User:** ${user.displayName} (\`${user.name}\`)`,
      `**User ID:** \`${user.id}\``
    ].join("\n"), "Confirm Unban"),
    components: [confirmRow(token)]
  });
}

const slashHandlers = {
  upload: handleSlashUpload,
  whitelist: handleSlashWhitelist,
  setup: handleSlashSetup,
  ds: handleSlashDs,
  rban: handleSlashBan,
  runban: handleSlashUnban
};

client.on("interactionCreate", async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      const handler = slashHandlers[interaction.commandName];
      if (handler) return await handler(interaction);
      return;
    }

    if (interaction.isStringSelectMenu()) {
      const cidx = interaction.customId.indexOf("_select_");
      if (cidx === -1) return;
      const field = interaction.customId.slice(0, cidx);
      const token = interaction.customId.slice(cidx + 8);
      const pend = getPending(token);
      if (!pend) return safeInteractionReply(interaction, emb(C.err, "this action has expired."), true);
      if (pend.initiator && pend.initiator !== interaction.user.id)
        return safeInteractionReply(interaction, emb(C.err, "you didn't initiate this action."), true);
      const slot = interaction.values[0];
      const saves = pend.pdata?.Data?.[field];
      if (!saves || !(slot in saves)) return safeInteractionReply(interaction, emb(C.err, "invalid slot."), true);
      const currentVal = String(saves[slot] ?? "");
      pend.slot = slot;
      setPending(token, pend);
      const modal = new ModalBuilder().setCustomId(`save_modal_${token}`).setTitle(`Edit ${slot}`);
      modal.addComponents(
        new ActionRowBuilder().addComponents(
          new TextInputBuilder()
            .setCustomId("new_value")
            .setLabel(`New value for ${slot}`)
            .setStyle(TextInputStyle.Short)
            .setValue(currentVal.slice(0, 4000))
            .setRequired(true)
            .setMaxLength(200)
        )
      );
      return interaction.showModal(modal);
    }

    if (interaction.isModalSubmit()) {
      if (!interaction.customId.startsWith("save_modal_")) return;
      const token = interaction.customId.slice(11);
      const pend = consumePending(token);
      if (!pend) return safeInteractionReply(interaction, emb(C.err, "this action has expired."), true);
      if (pend.initiator && pend.initiator !== interaction.user.id)
        return safeInteractionReply(interaction, emb(C.err, "you didn't initiate this action."), true);
      const newVal = interaction.fields.getTextInputValue("new_value");
      if (!newVal || !newVal.trim()) return safeInteractionReply(interaction, emb(C.err, "value cannot be empty."), true);
      const { slot, field, uid, dsName, userId, username, displayName, pdata } = pend;
      if (!slot || !field || !uid || !dsName || !userId || !pdata)
        return safeInteractionReply(interaction, emb(C.err, "incomplete pending data."), true);
      const newPdata = structuredClone(pdata);
      newPdata.Data[field][slot] = newVal;
      const confirmToken = mkToken();
      const label = field === "troll_saves" ? "Troll Save" : "Item Save";
      setPending(confirmToken, { type: "generic_confirm", uid, dsName, userId, newPdata, initiator: pend.initiator });
      return safeInteractionReply(interaction, {
        ...emb(C.warn, [
          `**Player:** ${displayName} (\`${username}\`)`,
          `**Slot:** \`${slot}\``,
          `**New Value:** ${newVal}`
        ].join("\n"), `Confirm Edit ${label}`),
        components: [confirmRow(confirmToken)]
      });
    }

    if (interaction.isButton()) {
      const parts = interaction.customId.split("_");
      const btnAction = parts[0];
      const btnToken = parts[parts.length - 1];
      if (btnAction !== "confirm" && btnAction !== "cancel") return;
      const peeked = getPending(btnToken);
      if (!peeked) return safeInteractionReply(interaction, emb(C.err, "this action has expired."), true);
      if (peeked.initiator && peeked.initiator !== interaction.user.id)
        return safeInteractionReply(interaction, emb(C.err, "you didn't initiate this action."), true);
      const data = consumePending(btnToken);
      if (!data) return safeInteractionReply(interaction, emb(C.err, "this action has expired."), true);
      if (btnAction === "cancel")
        return safeInteractionUpdate(interaction, { ...emb(C.info, "action cancelled."), components: [] });
      if (data.type === "generic_confirm") {
        if (!data.uid || !data.userId || !data.newPdata || !data.dsName)
          return safeInteractionUpdate(interaction, { ...emb(C.err, "incomplete data."), components: [] });
        const { ok, status } = await setPlayerData(data.uid, data.userId, data.newPdata, data.dsName);
        return safeInteractionUpdate(interaction, {
          ...emb(ok ? C.ok : C.err, ok ? "data updated successfully." : `failed (${status})`),
          components: []
        });
      }
      if (data.type === "dsdel") {
        if (!data.uid || !data.store || !data.key)
          return safeInteractionUpdate(interaction, { ...emb(C.err, "incomplete data."), components: [] });
        const { status, ok } = await dsDeleteEntry(data.uid, data.store, data.key);
        return safeInteractionUpdate(interaction, {
          ...emb(ok ? C.ok : C.err, ok ? `**${data.store} / ${data.key}** deleted.` : `failed (${status})`),
          components: []
        });
      }
      if (data.type === "ban") {
        if (!data.gameId || !data.userId)
          return safeInteractionUpdate(interaction, { ...emb(C.err, "incomplete data."), components: [] });
        try {
          const res = await fetch(
            `https://apis.roblox.com/cloud/v2/universes/${data.gameId}/user-restrictions/${data.userId}`,
            {
              method: "PATCH",
              headers: { "x-api-key": process.env.ROBLOX_API_KEY, "Content-Type": "application/json" },
              body: JSON.stringify({
                gameJoinRestriction: { active: true, privateReason: data.reason, displayReason: data.reason }
              })
            }
          );
          const result = await res.json();
          const ok = res.ok || result?.gameJoinRestriction;
          return safeInteractionUpdate(interaction, {
            ...emb(ok ? C.ok : C.err, ok
              ? `**${data.displayName}** (\`${data.username}\`) banned.\n**Reason:** ${data.reason}`
              : `failed: ${JSON.stringify(result).slice(0, 1000)}`),
            components: []
          });
        } catch (e) {
          return safeInteractionUpdate(interaction, { ...emb(C.err, `ban request failed: ${e.message}`), components: [] });
        }
      }
      if (data.type === "unban") {
        if (!data.gameId || !data.userId)
          return safeInteractionUpdate(interaction, { ...emb(C.err, "incomplete data."), components: [] });
        try {
          const res = await fetch(
            `https://apis.roblox.com/cloud/v2/universes/${data.gameId}/user-restrictions/${data.userId}`,
            {
              method: "PATCH",
              headers: { "x-api-key": process.env.ROBLOX_API_KEY, "Content-Type": "application/json" },
              body: JSON.stringify({ gameJoinRestriction: { active: false } })
            }
          );
          const result = await res.json();
          const ok = res.ok || result?.gameJoinRestriction;
          return safeInteractionUpdate(interaction, {
            ...emb(ok ? C.ok : C.err, ok
              ? `**${data.displayName}** (\`${data.username}\`) unbanned.`
              : `failed: ${JSON.stringify(result).slice(0, 1000)}`),
            components: []
          });
        } catch (e) {
          return safeInteractionUpdate(interaction, { ...emb(C.err, `unban request failed: ${e.message}`), components: [] });
        }
      }
      return safeInteractionUpdate(interaction, { ...emb(C.err, "unknown action type."), components: [] });
    }
  } catch (e) {
    console.error("interaction handler error:", e);
    safeInteractionReply(interaction, emb(C.err, "an unexpected error occurred."), true).catch(() => {});
  }
});

process.on("unhandledRejection", (e) => console.error("unhandled rejection:", e));

client.login(process.env.DISCORD_TOKEN);
