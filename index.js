const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder, EmbedBuilder, ButtonBuilder, ButtonStyle, ActionRowBuilder, StringSelectMenuBuilder, StringSelectMenuOptionBuilder, ModalBuilder, TextInputBuilder, TextInputStyle } = require("discord.js")
const fetch = require("node-fetch")
const FormData = require("form-data")
const mm = require("music-metadata")
const crypto = require("crypto")

const FILE_COUNT = 5
const DEFAULT_PREFIX = "."
const C = { ok: 0x57F287, err: 0xED4245, info: 0x5865F2, warn: 0xFEE75C }
const pending = new Map()

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
})

async function loadData() {
  const defaults = { whitelist: [], universeId: null, staffRole: "Staff Team", prefixes: {} }
  try {
    const res = await fetch(`https://api.jsonbin.io/v3/b/${process.env.JSONBIN_ID}/latest`, {
      headers: { "X-Master-Key": process.env.JSONBIN_KEY }
    })
    const data = await res.json()
    return { ...defaults, ...data.record }
  } catch { return defaults }
}

async function saveData(d) {
  const res = await fetch(`https://api.jsonbin.io/v3/b/${process.env.JSONBIN_ID}`, {
    method: "PUT",
    headers: { "X-Master-Key": process.env.JSONBIN_KEY, "Content-Type": "application/json" },
    body: JSON.stringify(d)
  })
  if (!res.ok) console.error("saveData failed:", res.status, await res.text())
}

async function hasStaff(member) {
  const d = await loadData()
  return member.roles.cache.some(r => r.name === d.staffRole)
}

async function isWhitelisted(userId) {
  const d = await loadData()
  return d.whitelist.includes(userId)
}

async function getPrefix(guildId) {
  const d = await loadData()
  return d.prefixes?.[guildId] || DEFAULT_PREFIX
}

function md5b64(str) {
  return crypto.createHash("md5").update(str).digest("base64")
}

async function getRobloxUser(username) {
  const res = await fetch("https://users.roblox.com/v1/usernames/users", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ usernames: [username], excludeBannedUsers: false })
  })
  const data = await res.json()
  return data?.data?.[0] || null
}

async function pollOperation(opPath) {
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 3000))
    const res = await fetch(`https://apis.roblox.com/assets/v1/${opPath}`, {
      headers: { "x-api-key": process.env.ROBLOX_API_KEY }
    })
    const data = await res.json()
    if (data.done) return data.response?.assetId
  }
  return null
}

function fmtDuration(secs) {
  const m = Math.floor(secs / 60), s = Math.round(secs % 60)
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

async function uploadFile(file) {
  const fileRes = await fetch(file.url)
  const buf = await fileRes.buffer()
  const meta = await mm.parseBuffer(buf, { mimeType: "audio/mpeg" })
  const duration = fmtDuration(meta.format.duration || 0)
  const form = new FormData()
  form.append("request", JSON.stringify({
    assetType: "Audio",
    displayName: file.name.replace(".mp3", ""),
    description: "",
    creationContext: { creator: { userId: process.env.ROBLOX_USER_ID } }
  }), { contentType: "application/json" })
  form.append("fileContent", buf, { filename: file.name, contentType: "audio/mpeg" })
  const res = await fetch("https://apis.roblox.com/assets/v1/assets", {
    method: "POST",
    headers: { "x-api-key": process.env.ROBLOX_API_KEY, ...form.getHeaders() },
    body: form
  })
  const data = await res.json()
  if (!data.path) throw new Error(JSON.stringify(data))
  const assetId = await pollOperation(data.path)
  return { name: file.name.replace(".mp3", ""), assetId, duration }
}

function dsBase(uid) { return `https://apis.roblox.com/datastores/v1/universes/${uid}/standard-datastores` }
function dsH(extra = {}) { return { "x-api-key": process.env.ROBLOX_BAN_API_KEY, ...extra } }
async function dsListStores(uid) { return (await fetch(`${dsBase(uid)}?limit=50`, { headers: dsH() })).json() }
async function dsListEntries(uid, store) { return (await fetch(`${dsBase(uid)}/datastore/entries?datastoreName=${encodeURIComponent(store)}&limit=25`, { headers: dsH() })).json() }
async function dsGetEntry(uid, store, key) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, { headers: dsH() })
  return { status: res.status, body: await res.text() }
}
async function dsSetEntry(uid, store, key, value) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, {
    method: "POST", headers: dsH({ "Content-Type": "application/json", "content-md5": md5b64(value) }), body: value
  })
  return { status: res.status, ok: res.ok }
}
async function dsDeleteEntry(uid, store, key) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, {
    method: "DELETE", headers: dsH()
  })
  return { status: res.status, ok: res.ok }
}
async function dsIncrementEntry(uid, store, key, amount) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry/increment?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&incrementBy=${amount}`, {
    method: "POST", headers: dsH()
  })
  return { status: res.status, body: await res.text(), ok: res.ok }
}
async function dsListVersions(uid, store, key) {
  return (await fetch(`${dsBase(uid)}/datastore/entries/entry/versions?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&limit=10`, { headers: dsH() })).json()
}

function emb(color, desc, title) {
  const e = new EmbedBuilder().setColor(color).setDescription(desc)
  if (title) e.setTitle(title)
  return { embeds: [e] }
}

async function getRobloxUserById(userId) {
  const res = await fetch(`https://users.roblox.com/v1/users/${userId}`)
  const data = await res.json()
  return data?.id ? data : null
}

async function resolveRobloxId(input) {
  if (/^\d+$/.test(input)) {
    const u = await getRobloxUserById(input)
    return u ? { id: String(u.id), name: u.name, displayName: u.displayName } : { id: input, name: input, displayName: input }
  }
  const u = await getRobloxUser(input)
  return u ? { id: String(u.id), name: u.name, displayName: u.displayName } : null
}

async function getPlayerData(uid, userId, dsName) {
  const { status, body } = await dsGetEntry(uid, dsName, `Player_${userId}`)
  if (status !== 200) return null
  try { return JSON.parse(body) } catch { return null }
}

async function setPlayerData(uid, userId, pdata, dsName) {
  return dsSetEntry(uid, dsName, `Player_${userId}`, JSON.stringify(pdata))
}

function makeSaveSelect(saves, field, token) {
  const opts = Object.keys(saves)
    .sort((a, b) => parseInt(a.slice(4)) - parseInt(b.slice(4)))
    .slice(0, 25)
    .map(slot => new StringSelectMenuOptionBuilder()
      .setLabel(slot)
      .setValue(slot)
      .setDescription(String(saves[slot]).slice(0, 100))
    )
  return new ActionRowBuilder().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId(`${field}_select_${token}`)
      .setPlaceholder("Select a slot to edit...")
      .addOptions(opts)
  )
}

function fmtSaves(saves) {
  return Object.entries(saves)
    .sort(([a], [b]) => parseInt(a.slice(4)) - parseInt(b.slice(4)))
    .map(([k, v]) => `\`${k}\`: ${v}`)
    .join("\n")
}

function confirmRow(token) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`confirm_${token}`).setLabel("Confirm").setStyle(ButtonStyle.Danger),
    new ButtonBuilder().setCustomId(`cancel_${token}`).setLabel("Cancel").setStyle(ButtonStyle.Secondary)
  )
}

function mkToken() { return Math.random().toString(36).slice(2, 8) }

function setPending(token, data, ttl = 60000) {
  pending.set(token, data)
  setTimeout(() => pending.delete(token), ttl)
}

const uploadCmd = new SlashCommandBuilder().setName("upload").setDescription("Upload up to 5 MP3s to Roblox")
for (let i = 1; i <= FILE_COUNT; i++) uploadCmd.addAttachmentOption(o => o.setName(`file${i}`).setDescription(`MP3 file ${i}`).setRequired(i === 1))

const whitelistCmd = new SlashCommandBuilder().setName("whitelist").setDescription("Manage upload whitelist")
  .addSubcommand(s => s.setName("add").setDescription("Whitelist a user").addUserOption(o => o.setName("user").setDescription("Discord user").setRequired(true)))
  .addSubcommand(s => s.setName("remove").setDescription("Remove a user").addUserOption(o => o.setName("user").setDescription("Discord user").setRequired(true)))
  .addSubcommand(s => s.setName("list").setDescription("Show whitelisted users"))

const setupCmd = new SlashCommandBuilder().setName("setup").setDescription("Configure bot settings")
  .addSubcommand(s => s.setName("universe").setDescription("Set universe ID").addStringOption(o => o.setName("id").setDescription("Universe ID").setRequired(true)))
  .addSubcommand(s => s.setName("staffrole").setDescription("Set staff role").addStringOption(o => o.setName("name").setDescription("Exact role name").setRequired(true)))
  .addSubcommand(s => s.setName("prefix").setDescription("Set prefix").addStringOption(o => o.setName("prefix").setDescription("New prefix").setRequired(true)))
  .addSubcommand(s => s.setName("show").setDescription("Show current settings"))

const dsCmd = new SlashCommandBuilder().setName("ds").setDescription("Manage Roblox datastores")
  .addSubcommand(s => s.setName("list").setDescription("List all datastores"))
  .addSubcommand(s => s.setName("entries").setDescription("List entries").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)))
  .addSubcommand(s => s.setName("get").setDescription("Get entry").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)).addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))
  .addSubcommand(s => s.setName("set").setDescription("Set entry").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)).addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)).addStringOption(o => o.setName("value").setDescription("Value").setRequired(true)))
  .addSubcommand(s => s.setName("delete").setDescription("Delete entry").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)).addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))
  .addSubcommand(s => s.setName("increment").setDescription("Increment entry").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)).addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)).addNumberOption(o => o.setName("amount").setDescription("Amount").setRequired(true)))
  .addSubcommand(s => s.setName("versions").setDescription("List versions").addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)).addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))

const banCmd = new SlashCommandBuilder().setName("rban").setDescription("Ban a Roblox user")
  .addStringOption(o => o.setName("username").setDescription("Roblox username").setRequired(true))
  .addStringOption(o => o.setName("reason").setDescription("Ban reason"))

const unbanCmd = new SlashCommandBuilder().setName("runban").setDescription("Unban a Roblox user")
  .addStringOption(o => o.setName("username").setDescription("Roblox username").setRequired(true))

client.once("clientReady", async () => {
  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN)
  await rest.put(Routes.applicationCommands(client.user.id), {
    body: [uploadCmd, whitelistCmd, setupCmd, dsCmd, banCmd, unbanCmd].map(c => c.toJSON())
  })
  console.log("ready")
})

client.on("messageCreate", async msg => {
  if (msg.author.bot || !msg.guild) return
  const prefix = await getPrefix(msg.guild.id)
  if (!msg.content.startsWith(prefix)) return
  const args = msg.content.slice(prefix.length).trim().split(/\s+/)
  const cmd = args.shift().toLowerCase()
  const reply = t => msg.reply(typeof t === "string" ? emb(C.info, t) : t)

  if (cmd === "setmainds") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (!args[0]) return reply(emb(C.warn, `usage: \`${prefix}setmainds <datastore name>\``))
    const d = await loadData()
    d.mainDatastore = args[0]
    await saveData(d)
    return reply(emb(C.ok, `main datastore set to \`${args[0]}\``))
  }

  if (cmd === "getstats") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (!args[0]) return reply(emb(C.warn, `usage: \`${prefix}getstats <username/userid>\``))
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(args[0])
    if (!user) return status.edit(emb(C.err, "user not found."))
    const pdata = await getPlayerData(uid, user.id, dsName)
    if (!pdata) return status.edit(emb(C.err, `no data for **${user.name}**.`))
    const data = pdata.Data
    const quest = data.quest || {}
    const trunc = s => s.length > 1020 ? s.slice(0, 1020) + "..." : (s || "none")
    return status.edit({ embeds: [new EmbedBuilder()
      .setColor(C.info)
      .setTitle(`Stats — ${user.displayName} (${user.name})`)
      .addFields(
        { name: "💰 Coins", value: `\`${(data.coins || 0).toLocaleString()}\``, inline: true },
        { name: "🎭 Stand", value: `\`${data.troll_stand || "none"}\``, inline: true },
        { name: "🔢 Type", value: `\`${data.type_i ?? "?"}\``, inline: true },
        { name: "📋 Quest", value: `**${quest.Name || "none"}**\nGoal: \`${quest.Goal}\` | Progress: \`${quest.Progress}\` | Reward: \`${quest.Reward || "?"}\`` },
        { name: `🎭 Troll Saves (${Object.keys(data.troll_saves || {}).length})`, value: trunc(fmtSaves(data.troll_saves || {})) },
        { name: `🎒 Item Saves (${Object.keys(data.item_saves || {}).length})`, value: trunc(fmtSaves(data.item_saves || {})) }
      )
      .setFooter({ text: `Player_${user.id} | ${dsName}` })
    ] })
  }

  if (cmd === "settroll" || cmd === "setitem") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (!args[0]) return reply(emb(C.warn, `usage: \`${prefix}${cmd} <username/userid>\``))
    const field = cmd === "settroll" ? "troll_saves" : "item_saves"
    const label = cmd === "settroll" ? "Troll Saves" : "Item Saves"
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(args[0])
    if (!user) return status.edit(emb(C.err, "user not found."))
    const pdata = await getPlayerData(uid, user.id, dsName)
    if (!pdata) return status.edit(emb(C.err, `no data for **${user.name}**.`))
    const saves = pdata.Data[field] || {}
    const token = mkToken()
    setPending(token, { type: `${field}_select`, uid, dsName, userId: user.id, username: user.name, displayName: user.displayName, pdata, field })
    const preview = fmtSaves(saves)
    return status.edit({
      embeds: [new EmbedBuilder()
        .setColor(C.info)
        .setTitle(`${label} — ${user.displayName} (${user.name})`)
        .setDescription(preview.length > 2000 ? preview.slice(0, 2000) + "..." : preview || "empty")
        .setFooter({ text: "Select a slot below to edit" })
      ],
      components: [makeSaveSelect(saves, field, token)]
    })
  }

  if (cmd === "setcoins") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (!args[0] || !args[1]) return reply(emb(C.warn, `usage: \`${prefix}setcoins <username/userid> <amount>\``))
    const amount = parseInt(args[1])
    if (isNaN(amount) || amount < 0) return reply(emb(C.err, "invalid amount."))
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(args[0])
    if (!user) return status.edit(emb(C.err, "user not found."))
    const pdata = await getPlayerData(uid, user.id, dsName)
    if (!pdata) return status.edit(emb(C.err, `no data for **${user.name}**.`))
    const oldCoins = pdata.Data.coins || 0
    const newPdata = JSON.parse(JSON.stringify(pdata))
    newPdata.Data.coins = amount
    const token = mkToken()
    setPending(token, { type: "generic_confirm", uid, dsName, userId: user.id, newPdata })
    return status.edit({
      ...emb(C.warn, [`**Player:** ${user.displayName} (\`${user.name}\`)`, `**Coins:** \`${oldCoins.toLocaleString()}\` → \`${amount.toLocaleString()}\``].join("\n"), "Confirm Set Coins"),
      components: [confirmRow(token)]
    })
  }

  if (cmd === "setstand") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    const stand = args.slice(1).join(" ")
    if (!args[0] || !stand) return reply(emb(C.warn, `usage: \`${prefix}setstand <username/userid> <stand name>\``))
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(args[0])
    if (!user) return status.edit(emb(C.err, "user not found."))
    const pdata = await getPlayerData(uid, user.id, dsName)
    if (!pdata) return status.edit(emb(C.err, `no data for **${user.name}**.`))
    const oldStand = pdata.Data.troll_stand
    const newPdata = JSON.parse(JSON.stringify(pdata))
    newPdata.Data.troll_stand = stand
    const token = mkToken()
    setPending(token, { type: "generic_confirm", uid, dsName, userId: user.id, newPdata })
    return status.edit({
      ...emb(C.warn, [`**Player:** ${user.displayName} (\`${user.name}\`)`, `**Stand:** \`${oldStand}\` → \`${stand}\``].join("\n"), "Confirm Set Stand"),
      components: [confirmRow(token)]
    })
  }

  if (cmd === "setquest") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (args.length < 5) return reply(emb(C.warn, `usage: \`${prefix}setquest <user> <name> <goal> <progress> <reward>\` (use _ for spaces in name/reward)`))
    const [input, rawName, goalStr, progressStr, ...rewardParts] = args
    const questName = rawName.replace(/_/g, " ")
    const goal = parseInt(goalStr)
    const progress = parseInt(progressStr)
    const reward = rewardParts.join(" ").replace(/_/g, " ")
    if (isNaN(goal) || isNaN(progress)) return reply(emb(C.err, "goal and progress must be numbers."))
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(input)
    if (!user) return status.edit(emb(C.err, "user not found."))
    const pdata = await getPlayerData(uid, user.id, dsName)
    if (!pdata) return status.edit(emb(C.err, `no data for **${user.name}**.`))
    const newPdata = JSON.parse(JSON.stringify(pdata))
    newPdata.Data.quest = { Name: questName, Goal: goal, Progress: progress, Reward: reward }
    const token = mkToken()
    setPending(token, { type: "generic_confirm", uid, dsName, userId: user.id, newPdata })
    return status.edit({
      ...emb(C.warn, [
        `**Player:** ${user.displayName} (\`${user.name}\`)`,
        `**Name:** ${questName}`,
        `**Goal:** ${goal} | **Progress:** ${progress}`,
        `**Reward:** ${reward}`
      ].join("\n"), "Confirm Set Quest"),
      components: [confirmRow(token)]
    })
  }

  if (cmd === "viewraw") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    if (!args[0]) return reply(emb(C.warn, `usage: \`${prefix}viewraw <username/userid>\``))
    const status = await reply(emb(C.info, "fetching..."))
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return status.edit(emb(C.err, "universe ID not set."))
    const dsName = d.mainDatastore || "MainData_v2"
    const user = await resolveRobloxId(args[0])
    if (!user) return status.edit(emb(C.err, "user not found."))
    const { status: httpStatus, body } = await dsGetEntry(uid, dsName, `Player_${user.id}`)
    if (httpStatus !== 200) return status.edit(emb(C.err, `no data found (${httpStatus}).`))
    const preview = body.length > 1800 ? body.slice(0, 1800) + "\n...(truncated)" : body
    return status.edit(emb(C.info, `\`\`\`json\n${preview}\n\`\`\``, `Raw — Player_${user.id}`))
        }

  if (cmd === "setprefix") {
    if (!await hasStaff(msg.member)) return reply(emb(C.err, "staff only."))
    const np = args[0]
    if (!np) return reply(emb(C.warn, `usage: \`${prefix}setprefix <new prefix>\``))
    const d = await loadData()
    if (!d.prefixes) d.prefixes = {}
    d.prefixes[msg.guild.id] = np
    await saveData(d)
    return reply(emb(C.ok, `prefix set to \`${np}\``))
  }

  if (cmd === "upload") {
    if (!await isWhitelisted(msg.author.id) && !await hasStaff(msg.member)) return reply(emb(C.err, "you're not whitelisted."))
    const attachments = [...msg.attachments.values()].filter(a => a.name?.endsWith(".mp3"))
    if (!attachments.length) return reply(emb(C.err, "attach at least one mp3 file."))
    const status = await reply(emb(C.info, `uploading ${attachments.length} file(s)...`))
    const lines = []
    for (const file of attachments) {
      try {
        const { name, assetId, duration } = await uploadFile(file)
        lines.push(`**${name}** — \`${assetId}\` — ${duration}`)
      } catch (e) {
        lines.push(`**${file.name}** — failed: ${e.message}`)
      }
    }
    return status.edit(emb(C.ok, lines.join("\n"), "Upload Results"))
  }

  if (cmd === "whitelist") {
    if (msg.author.id !== process.env.OWNER_ID) return reply(emb(C.err, "owner only."))
    const sub = args[0]
    const d = await loadData()
    if (sub === "add") {
      const mentioned = msg.mentions.users.first()
      if (!mentioned) return reply(emb(C.warn, `usage: \`${prefix}whitelist add @user\``))
      if (d.whitelist.includes(mentioned.id)) return reply(emb(C.warn, `${mentioned.username} is already whitelisted.`))
      d.whitelist.push(mentioned.id)
      await saveData(d)
      return reply(emb(C.ok, `**${mentioned.username}** added to whitelist.`))
    }
    if (sub === "remove") {
      const mentioned = msg.mentions.users.first()
      if (!mentioned) return reply(emb(C.warn, `usage: \`${prefix}whitelist remove @user\``))
      if (!d.whitelist.includes(mentioned.id)) return reply(emb(C.warn, `${mentioned.username} isn't whitelisted.`))
      d.whitelist = d.whitelist.filter(id => id !== mentioned.id)
      await saveData(d)
      return reply(emb(C.ok, `**${mentioned.username}** removed from whitelist.`))
    }
    if (sub === "list") {
      if (!d.whitelist.length) return reply(emb(C.info, "no users whitelisted."))
      return reply(emb(C.info, d.whitelist.map(id => `<@${id}>`).join("\n"), "Whitelisted Users"))
    }
    return reply(emb(C.warn, `usage: \`${prefix}whitelist add/remove/list\``))
  }

  if (cmd === "setup") {
    if (!await hasStaff(msg.member)) return reply(emb(C.err, "staff only."))
    const sub = args[0]
    const d = await loadData()
    if (sub === "universe") {
      if (!args[1]) return reply(emb(C.warn, `usage: \`${prefix}setup universe <id>\``))
      d.universeId = args[1]
      await saveData(d)
      return reply(emb(C.ok, `universe ID set to \`${d.universeId}\``))
    }
    if (sub === "staffrole") {
      const roleName = args.slice(1).join(" ")
      if (!roleName) return reply(emb(C.warn, `usage: \`${prefix}setup staffrole <role name>\``))
      d.staffRole = roleName
      await saveData(d)
      return reply(emb(C.ok, `staff role set to **${d.staffRole}**`))
    }
    if (sub === "show") {
      return reply(emb(C.info, [
        `**Universe ID:** \`${d.universeId || "not set"}\``,
        `**Staff Role:** ${d.staffRole || "Staff Team"}`,
        `**Prefix:** \`${prefix}\``,
        `**Whitelisted Users:** ${d.whitelist.length}`
      ].join("\n"), "Bot Settings"))
    }
    return reply(emb(C.warn, `usage: \`${prefix}setup universe/staffrole/show\``))
  }

  if (cmd === "rban") {
    if (!await hasStaff(msg.member)) return reply(emb(C.err, "staff only."))
    const username = args[0]
    if (!username) return reply(emb(C.warn, `usage: \`${prefix}rban <username> [reason]\``))
    const reason = args.slice(1).join(" ") || "No reason provided"
    const d = await loadData()
    const gameId = d.universeId || "5254578111"
    const status = await reply(emb(C.info, "looking up user..."))
    const user = await getRobloxUser(username)
    if (!user) return status.edit(emb(C.err, `no Roblox user found for **${username}**.`))
    const token = mkToken()
    setPending(token, { type: "ban", userId: user.id, username, displayName: user.displayName, reason, gameId, by: msg.author.username })
    return status.edit({
      ...emb(C.warn, [`**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``, `**Reason:** ${reason}`].join("\n"), "Confirm Ban"),
      components: [confirmRow(token)]
    })
  }

  if (cmd === "runban") {
    if (!await hasStaff(msg.member)) return reply(emb(C.err, "staff only."))
    const username = args[0]
    if (!username) return reply(emb(C.warn, `usage: \`${prefix}runban <username>\``))
    const d = await loadData()
    const gameId = d.universeId || "5254578111"
    const status = await reply(emb(C.info, "looking up user..."))
    const user = await getRobloxUser(username)
    if (!user) return status.edit(emb(C.err, `no Roblox user found for **${username}**.`))
    const token = mkToken()
    setPending(token, { type: "unban", userId: user.id, username, displayName: user.displayName, gameId, by: msg.author.username })
    return status.edit({
      ...emb(C.warn, [`**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``].join("\n"), "Confirm Unban"),
      components: [confirmRow(token)]
    })
  }

  if (cmd === "ds") {
    if (!await hasStaff(msg.member) && !await isWhitelisted(msg.author.id)) return reply(emb(C.err, "you're not whitelisted."))
    const sub = args[0]
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return reply(emb(C.err, "universe ID not set. run `setup universe` first."))
    const store = args[1], key = args[2]
    if (sub === "list") {
      const data = await dsListStores(uid)
      const stores = data?.datastores?.map(s => `\`${s.name}\``).join("\n")
      return reply(emb(stores ? C.info : C.warn, stores || "no datastores found.", stores ? "Datastores" : null))
    }
    if (sub === "entries") {
      if (!store) return reply(emb(C.warn, `usage: \`${prefix}ds entries <store>\``))
      const data = await dsListEntries(uid, store)
      const keys = data?.keys?.map(k => `\`${k.key}\``).join("\n")
      return reply(emb(keys ? C.info : C.warn, keys || "no entries found.", keys ? `${store} Entries` : null))
    }
    if (sub === "get") {
      if (!store || !key) return reply(emb(C.warn, `usage: \`${prefix}ds get <store> <key>\``))
      const { status, body } = await dsGetEntry(uid, store, key)
      if (status !== 200) return reply(emb(C.err, `failed (${status})`))
      const preview = body.length > 1800 ? body.slice(0, 1800) + "..." : body
      return reply(emb(C.info, `\`\`\`json\n${preview}\n\`\`\``, `${store} / ${key}`))
    }
    if (sub === "set") {
      if (!store || !key || !args[3]) return reply(emb(C.warn, `usage: \`${prefix}ds set <store> <key> <value>\``))
      let value = args.slice(3).join(" ")
      try { JSON.parse(value) } catch { value = JSON.stringify(value) }
      const { status, ok } = await dsSetEntry(uid, store, key, value)
      return reply(emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** updated.` : `failed (${status})`))
    }
    if (sub === "delete") {
      if (!store || !key) return reply(emb(C.warn, `usage: \`${prefix}ds delete <store> <key>\``))
      const token = mkToken()
      setPending(token, { type: "dsdel", uid, store, key })
      return reply({ ...emb(C.warn, `delete **${store} / ${key}**?`, "Confirm Delete"), components: [confirmRow(token)] })
    }
    if (sub === "increment") {
      if (!store || !key || !args[3]) return reply(emb(C.warn, `usage: \`${prefix}ds increment <store> <key> <amount>\``))
      const amount = parseFloat(args[3])
      if (isNaN(amount)) return reply(emb(C.err, "amount must be a number."))
      const { status, body, ok } = await dsIncrementEntry(uid, store, key, amount)
      return reply(emb(ok ? C.ok : C.err, ok ? `**${store} / ${key}** new value: \`${body}\`` : `failed (${status})`))
    }
    if (sub === "versions") {
      if (!store || !key) return reply(emb(C.warn, `usage: \`${prefix}ds versions <store> <key>\``))
      const data = await dsListVersions(uid, store, key)
      const versions = data?.versions?.map(v => `\`${v.version}\` — ${new Date(v.createdTime).toLocaleString()}`).join("\n")
      return reply(emb(versions ? C.info : C.warn, versions || "no versions found.", versions ? `${store} / ${key} Versions` : null))
    }
    return reply(emb(C.warn, `usage: \`${prefix}ds list/entries/get/set/delete/increment/versions\``))
  }
})

client.on("interactionCreate", async interaction => {
  if (interaction.isStringSelectMenu()) {
    const cidx = interaction.customId.indexOf("_select_")
    if (cidx === -1) return
    const field = interaction.customId.slice(0, cidx)
    const token = interaction.customId.slice(cidx + 8)
    const pend = pending.get(token)
    if (!pend) return interaction.reply({ ...emb(C.err, "this action has expired."), ephemeral: true })
    const slot = interaction.values[0]
    const currentVal = String(pend.pdata.Data[field][slot] ?? "")
    pend.slot = slot
    pending.set(token, pend)
    const modal = new ModalBuilder()
      .setCustomId(`save_modal_${token}`)
      .setTitle(`Edit ${slot}`)
    modal.addComponents(new ActionRowBuilder().addComponents(
      new TextInputBuilder()
        .setCustomId("new_value")
        .setLabel(`New value for ${slot}`)
        .setStyle(TextInputStyle.Short)
        .setValue(currentVal.slice(0, 4000))
        .setRequired(true)
        .setMaxLength(200)
    ))
    return interaction.showModal(modal)
  }

  if (interaction.isModalSubmit()) {
    if (!interaction.customId.startsWith("save_modal_")) return
    const token = interaction.customId.slice(11)
    const pend = pending.get(token)
    if (!pend) return interaction.reply({ ...emb(C.err, "this action has expired."), ephemeral: true })
    pending.delete(token)
    const newVal = interaction.fields.getTextInputValue("new_value")
    const { slot, field, uid, dsName, userId, username, displayName, pdata } = pend
    const newPdata = JSON.parse(JSON.stringify(pdata))
    newPdata.Data[field][slot] = newVal
    const confirmToken = mkToken()
    const label = field === "troll_saves" ? "Troll Save" : "Item Save"
    setPending(confirmToken, { type: "generic_confirm", uid, dsName, userId, newPdata })
    return interaction.reply({
      ...emb(C.warn, [
        `**Player:** ${displayName} (\`${username}\`)`,
        `**Slot:** \`${slot}\``,
        `**New Value:** ${newVal}`
      ].join("\n"), `Confirm Edit ${label}`),
      components: [confirmRow(confirmToken)]
    })
  }

  if (interaction.isButton()) {
    const parts = interaction.customId.split("_")
    const btnAction = parts[0]
    const btnToken = parts[parts.length - 1]
    const data = pending.get(btnToken)
    if (!data) return interaction.reply({ ...emb(C.err, "this action has expired."), ephemeral: true })
    pending.delete(btnToken)
    if (btnAction === "cancel") return interaction.update({ ...emb(C.info, "action cancelled."), components: [] })

    if (data.type === "generic_confirm") {
      const { ok, status } = await setPlayerData(data.uid, data.userId, data.newPdata, data.dsName)
      return interaction.update({ ...emb(ok ? C.ok : C.err, ok ? "data updated successfully." : `failed (${status})`), components: [] })
    }

    if (data.type === "dsdel") {
      const { status, ok } = await dsDeleteEntry(data.uid, data.store, data.key)
      return interaction.update({ ...emb(ok ? C.ok : C.err, ok ? `**${data.store} / ${data.key}** deleted.` : `failed (${status})`), components: [] })
    }

    if (data.type === "ban") {
      const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${data.gameId}/user-restrictions/${data.userId}`, {
        method: "PATCH",
        headers: { "x-api-key": process.env.ROBLOX_API_KEY, "Content-Type": "application/json" },
        body: JSON.stringify({ gameJoinRestriction: { active: true, privateReason: data.reason, displayReason: data.reason } })
      })
      const result = await res.json()
      const ok = res.ok || result?.gameJoinRestriction
      return interaction.update({ ...emb(ok ? C.ok : C.err, ok ? `**${data.displayName}** (\`${data.username}\`) banned.\n**Reason:** ${data.reason}` : `failed: ${JSON.stringify(result)}`), components: [] })
    }

    if (data.type === "unban") {
      const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${data.gameId}/user-restrictions/${data.userId}`, {
        method: "PATCH",
        headers: { "x-api-key": process.env.ROBLOX_API_KEY, "Content-Type": "application/json" },
        body: JSON.stringify({ gameJoinRestriction: { active: false } })
      })
      const result = await res.json()
      const ok = res.ok || result?.gameJoinRestriction
      return interaction.update({ ...emb(ok ? C.ok : C.err, ok ? `**${data.displayName}** (\`${data.username}\`) unbanned.` : `failed: ${JSON.stringify(result)}`), components: [] })
    }
  }
})

client.login(process.env.DISCORD_TOKEN)
