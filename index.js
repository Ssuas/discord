const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require("discord.js")
const fetch = require("node-fetch")
const FormData = require("form-data")
const mm = require("music-metadata")
const crypto = require("crypto")

const FILE_COUNT = 5
const DEFAULT_PREFIX = "."

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent
  ]
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
  await fetch(`https://api.jsonbin.io/v3/b/${process.env.JSONBIN_ID}`, {
    method: "PUT",
    headers: { "X-Master-Key": process.env.JSONBIN_KEY, "Content-Type": "application/json" },
    body: JSON.stringify(d)
  })
}

async function hasStaffMember(member) {
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
  const m = Math.floor(secs / 60)
  const s = Math.round(secs % 60)
  return m > 0 ? `${m}m ${s}s` : `${s}s`
}

async function uploadFile(file) {
  const fileRes = await fetch(file.url)
  const buf = await fileRes.buffer()
  const meta = await mm.parseBuffer(buf, "audio/mpeg")
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

function dsBase(universeId) {
  return `https://apis.roblox.com/datastores/v1/universes/${universeId}/standard-datastores`
}

function dsH(extra = {}) {
  return { "x-api-key": process.env.ROBLOX_BAN_API_KEY, ...extra }
}

async function dsListStores(uid) {
  const res = await fetch(`${dsBase(uid)}?limit=50`, { headers: dsH() })
  return res.json()
}

async function dsListEntries(uid, store) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries?datastoreName=${encodeURIComponent(store)}&limit=25`, { headers: dsH() })
  return res.json()
}

async function dsGetEntry(uid, store, key) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, { headers: dsH() })
  return { status: res.status, body: await res.text() }
}

async function dsSetEntry(uid, store, key, value) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, {
    method: "POST",
    headers: dsH({ "Content-Type": "application/json", "content-md5": md5b64(value) }),
    body: value
  })
  return { status: res.status, ok: res.ok }
}

async function dsDeleteEntry(uid, store, key) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}`, {
    method: "DELETE",
    headers: dsH()
  })
  return { status: res.status, ok: res.ok }
}

async function dsIncrementEntry(uid, store, key, amount) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry/increment?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&incrementBy=${amount}`, {
    method: "POST",
    headers: dsH()
  })
  return { status: res.status, body: await res.text(), ok: res.ok }
}

async function dsListVersions(uid, store, key) {
  const res = await fetch(`${dsBase(uid)}/datastore/entries/entry/versions?datastoreName=${encodeURIComponent(store)}&entryKey=${encodeURIComponent(key)}&limit=10`, { headers: dsH() })
  return res.json()
}

const uploadCmd = new SlashCommandBuilder()
  .setName("upload")
  .setDescription("Upload up to 5 MP3s to Roblox")
for (let i = 1; i <= FILE_COUNT; i++) {
  uploadCmd.addAttachmentOption(o =>
    o.setName(`file${i}`).setDescription(`MP3 file ${i}`).setRequired(i === 1)
  )
}

const whitelistCmd = new SlashCommandBuilder()
  .setName("whitelist")
  .setDescription("Manage upload whitelist")
  .addSubcommand(s => s.setName("add").setDescription("Whitelist a user")
    .addUserOption(o => o.setName("user").setDescription("Discord user").setRequired(true)))
  .addSubcommand(s => s.setName("remove").setDescription("Remove a user from whitelist")
    .addUserOption(o => o.setName("user").setDescription("Discord user").setRequired(true)))
  .addSubcommand(s => s.setName("list").setDescription("Show all whitelisted users"))

const setupCmd = new SlashCommandBuilder()
  .setName("setup")
  .setDescription("Configure bot settings")
  .addSubcommand(s => s.setName("universe").setDescription("Set the Roblox universe ID")
    .addStringOption(o => o.setName("id").setDescription("Universe ID").setRequired(true)))
  .addSubcommand(s => s.setName("staffrole").setDescription("Set the staff role name")
    .addStringOption(o => o.setName("name").setDescription("Exact role name").setRequired(true)))
  .addSubcommand(s => s.setName("prefix").setDescription("Set the bot prefix for this server")
    .addStringOption(o => o.setName("prefix").setDescription("New prefix").setRequired(true)))
  .addSubcommand(s => s.setName("show").setDescription("Show current settings"))

const dsCmd = new SlashCommandBuilder()
  .setName("ds")
  .setDescription("Manage Roblox datastores")
  .addSubcommand(s => s.setName("list").setDescription("List all datastores"))
  .addSubcommand(s => s.setName("entries").setDescription("List entries in a datastore")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true)))
  .addSubcommand(s => s.setName("get").setDescription("Get a datastore entry")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true))
    .addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))
  .addSubcommand(s => s.setName("set").setDescription("Set a datastore entry")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true))
    .addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true))
    .addStringOption(o => o.setName("value").setDescription("Value").setRequired(true)))
  .addSubcommand(s => s.setName("delete").setDescription("Delete a datastore entry")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true))
    .addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))
  .addSubcommand(s => s.setName("increment").setDescription("Increment a numeric entry")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true))
    .addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true))
    .addNumberOption(o => o.setName("amount").setDescription("Amount").setRequired(true)))
  .addSubcommand(s => s.setName("versions").setDescription("List versions of an entry")
    .addStringOption(o => o.setName("store").setDescription("Datastore name").setRequired(true))
    .addStringOption(o => o.setName("key").setDescription("Entry key").setRequired(true)))

const banCmd = new SlashCommandBuilder()
  .setName("rban")
  .setDescription("Ban a Roblox user")
  .addStringOption(o => o.setName("username").setDescription("Roblox username").setRequired(true))
  .addStringOption(o => o.setName("reason").setDescription("Ban reason"))

const unbanCmd = new SlashCommandBuilder()
  .setName("runban")
  .setDescription("Unban a Roblox user")
  .addStringOption(o => o.setName("username").setDescription("Roblox username").setRequired(true))

client.once("ready", async () => {
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

  async function reply(text) { return msg.reply(text) }

  if (cmd === "setprefix") {
    if (!await hasStaffMember(msg.member)) return reply("staff only.")
    const newPrefix = args[0]
    if (!newPrefix) return reply(`usage: \`${prefix}setprefix <new prefix>\``)
    const d = await loadData()
    if (!d.prefixes) d.prefixes = {}
    d.prefixes[msg.guild.id] = newPrefix
    await saveData(d)
    return reply(`✅ prefix set to \`${newPrefix}\``)
  }

  if (cmd === "upload") {
    if (!await isWhitelisted(msg.author.id) && !await hasStaffMember(msg.member)) return reply("you're not whitelisted.")
    const attachments = [...msg.attachments.values()].filter(a => a.name?.endsWith(".mp3"))
    if (!attachments.length) return reply("attach at least one mp3 file.")
    const status = await reply(`uploading ${attachments.length} file(s)...`)
    const lines = []
    for (const file of attachments) {
      try {
        const { name, assetId, duration } = await uploadFile(file)
        lines.push(`**${name}** — \`${assetId}\` — ${duration}`)
      } catch (e) {
        lines.push(`**${file.name}** — failed: ${e.message}`)
      }
    }
    return status.edit(lines.join("\n"))
  }

  if (cmd === "whitelist") {
    if (msg.author.id !== process.env.OWNER_ID) return reply("owner only.")
    const sub = args[0]
    const d = await loadData()
    if (sub === "add") {
      const mentioned = msg.mentions.users.first()
      if (!mentioned) return reply(`usage: \`${prefix}whitelist add @user\``)
      if (d.whitelist.includes(mentioned.id)) return reply(`${mentioned.username} is already whitelisted.`)
      d.whitelist.push(mentioned.id)
      await saveData(d)
      return reply(`✅ **${mentioned.username}** added to whitelist.`)
    }
    if (sub === "remove") {
      const mentioned = msg.mentions.users.first()
      if (!mentioned) return reply(`usage: \`${prefix}whitelist remove @user\``)
      if (!d.whitelist.includes(mentioned.id)) return reply(`${mentioned.username} isn't whitelisted.`)
      d.whitelist = d.whitelist.filter(id => id !== mentioned.id)
      await saveData(d)
      return reply(`✅ **${mentioned.username}** removed from whitelist.`)
    }
    if (sub === "list") {
      if (!d.whitelist.length) return reply("no users whitelisted.")
      return reply(`**Whitelisted Users:**\n${d.whitelist.map(id => `<@${id}>`).join("\n")}`)
    }
    return reply(`usage: \`${prefix}whitelist add/remove/list\``)
  }

  if (cmd === "setup") {
    if (!await hasStaffMember(msg.member)) return reply("staff only.")
    const sub = args[0]
    const d = await loadData()
    if (sub === "universe") {
      if (!args[1]) return reply(`usage: \`${prefix}setup universe <id>\``)
      d.universeId = args[1]
      await saveData(d)
      return reply(`✅ universe ID set to \`${d.universeId}\``)
    }
    if (sub === "staffrole") {
      const roleName = args.slice(1).join(" ")
      if (!roleName) return reply(`usage: \`${prefix}setup staffrole <role name>\``)
      d.staffRole = roleName
      await saveData(d)
      return reply(`✅ staff role set to **${d.staffRole}**`)
    }
    if (sub === "show") {
      return reply([
        `**Bot Settings**`,
        `Universe ID: \`${d.universeId || "not set"}\``,
        `Staff Role: **${d.staffRole || "Staff Team"}**`,
        `Prefix: \`${prefix}\``,
        `Whitelisted Users: ${d.whitelist.length}`
      ].join("\n"))
    }
    return reply(`usage: \`${prefix}setup universe/staffrole/show\``)
  }

  if (cmd === "rban") {
    if (!await hasStaffMember(msg.member)) return reply("staff only.")
    const username = args[0]
    if (!username) return reply(`usage: \`${prefix}rban <username> [reason]\``)
    const reason = args.slice(1).join(" ") || "No reason provided"
    const d = await loadData()
    const gameId = d.universeId || "5254578111"
    const status = await reply("looking up user...")
    const user = await getRobloxUser(username)
    if (!user) return status.edit(`❌ no Roblox user found for **${username}**.`)
    const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${gameId}/user-restrictions/${user.id}?updateMask=gameJoinRestriction`, {
      method: "PATCH",
      headers: { "x-api-key": process.env.ROBLOX_BAN_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ gameJoinRestriction: { active: true, privateReason: reason, displayReason: "You have been banned from this experience." } })
    })
    if (!res.ok) return status.edit(`❌ ban failed. status: \`${res.status}\``)
    return status.edit([`🔨 **Player Banned**`, `**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``, `**Reason:** ${reason}`, `**Banned By:** ${msg.author.username}`].join("\n"))
  }

  if (cmd === "runban") {
    if (!await hasStaffMember(msg.member)) return reply("staff only.")
    const username = args[0]
    if (!username) return reply(`usage: \`${prefix}runban <username>\``)
    const d = await loadData()
    const gameId = d.universeId || "5254578111"
    const status = await reply("looking up user...")
    const user = await getRobloxUser(username)
    if (!user) return status.edit(`❌ no Roblox user found for **${username}**.`)
    const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${gameId}/user-restrictions/${user.id}?updateMask=gameJoinRestriction`, {
      method: "PATCH",
      headers: { "x-api-key": process.env.ROBLOX_BAN_API_KEY, "Content-Type": "application/json" },
      body: JSON.stringify({ gameJoinRestriction: { active: false, privateReason: "", displayReason: "", excludeAltAccounts: false } })
    })
    if (!res.ok) return status.edit(`❌ unban failed. status: \`${res.status}\``)
    return status.edit([`✅ **Player Unbanned**`, `**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``, `**Unbanned By:** ${msg.author.username}`].join("\n"))
  }

  if (cmd === "ds") {
    if (!await hasStaffMember(msg.member) && !await isWhitelisted(msg.author.id)) return reply("you're not whitelisted.")
    const sub = args[0]
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return reply("universe ID not set. run `setup universe` first.")
    const store = args[1]
    const key = args[2]
    if (sub === "list") {
      const data = await dsListStores(uid)
      const stores = data?.datastores?.map(s => `\`${s.name}\``).join("\n")
      return reply(stores ? `**Datastores:**\n${stores}` : "no datastores found.")
    }
    if (sub === "entries") {
      if (!store) return reply(`usage: \`${prefix}ds entries <store>\``)
      const data = await dsListEntries(uid, store)
      const keys = data?.keys?.map(k => `\`${k.key}\``).join("\n")
      return reply(keys ? `**${store} entries:**\n${keys}` : "no entries found.")
    }
    if (sub === "get") {
      if (!store || !key) return reply(`usage: \`${prefix}ds get <store> <key>\``)
      const { status, body } = await dsGetEntry(uid, store, key)
      if (status !== 200) return reply(`❌ failed (${status})`)
      const preview = body.length > 1800 ? body.slice(0, 1800) + "..." : body
      return reply(`**${store} / ${key}:**\n\`\`\`json\n${preview}\n\`\`\``)
    }
    if (sub === "set") {
      if (!store || !key || !args[3]) return reply(`usage: \`${prefix}ds set <store> <key> <value>\``)
      let value = args.slice(3).join(" ")
      try { JSON.parse(value) } catch { value = JSON.stringify(value) }
      const { status, ok } = await dsSetEntry(uid, store, key, value)
      return reply(ok ? `✅ **${store} / ${key}** updated.` : `❌ failed (${status})`)
    }
    if (sub === "delete") {
      if (!store || !key) return reply(`usage: \`${prefix}ds delete <store> <key>\``)
      const { status, ok } = await dsDeleteEntry(uid, store, key)
      return reply(ok ? `✅ **${store} / ${key}** deleted.` : `❌ failed (${status})`)
    }
    if (sub === "increment") {
      if (!store || !key || !args[3]) return reply(`usage: \`${prefix}ds increment <store> <key> <amount>\``)
      const amount = parseFloat(args[3])
      if (isNaN(amount)) return reply("amount must be a number.")
      const { status, body, ok } = await dsIncrementEntry(uid, store, key, amount)
      return reply(ok ? `✅ **${store} / ${key}** new value: \`${body}\`` : `❌ failed (${status})`)
    }
    if (sub === "versions") {
      if (!store || !key) return reply(`usage: \`${prefix}ds versions <store> <key>\``)
      const data = await dsListVersions(uid, store, key)
      const versions = data?.versions?.map(v => `\`${v.version}\` — ${new Date(v.createdTime).toLocaleString()}`).join("\n")
      return reply(versions ? `**${store} / ${key} versions:**\n${versions}` : "no versions found.")
    }
    return reply(`usage: \`${prefix}ds list/entries/get/set/delete/increment/versions\``)
  }
})

client.on("interactionCreate", async interaction => {
  if (!interaction.isChatInputCommand()) return
  const cmd = interaction.commandName
  const sub = interaction.options.getSubcommand(false)

  if (cmd === "upload") {
    if (!await isWhitelisted(interaction.user.id) && !await hasStaffMember(interaction.member)) {
      return interaction.reply({ content: "you're not whitelisted.", ephemeral: true })
    }
    const files = []
    for (let i = 1; i <= FILE_COUNT; i++) {
      const f = interaction.options.getAttachment(`file${i}`)
      if (f) files.push(f)
    }
    if (files.some(f => !f.name.endsWith(".mp3"))) return interaction.reply({ content: "mp3 files only", ephemeral: true })
    await interaction.reply(`uploading ${files.length} file(s)...`)
    const lines = []
    for (const file of files) {
      try {
        const { name, assetId, duration } = await uploadFile(file)
        lines.push(`**${name}** — \`${assetId}\` — ${duration}`)
      } catch (e) {
        lines.push(`**${file.name}** — failed: ${e.message}`)
      }
    }
    return interaction.editReply(lines.join("\n"))
  }

  if (cmd === "whitelist") {
    if (interaction.user.id !== process.env.OWNER_ID) return interaction.reply({ content: "owner only.", ephemeral: true })
    const d = await loadData()
    if (sub === "add") {
      const user = interaction.options.getUser("user")
      if (d.whitelist.includes(user.id)) return interaction.reply({ content: `${user.username} is already whitelisted.`, ephemeral: true })
      d.whitelist.push(user.id)
      await saveData(d)
      return interaction.reply(`✅ **${user.username}** added to whitelist.`)
    }
    if (sub === "remove") {
      const user = interaction.options.getUser("user")
      if (!d.whitelist.includes(user.id)) return interaction.reply({ content: `${user.username} isn't whitelisted.`, ephemeral: true })
      d.whitelist = d.whitelist.filter(id => id !== user.id)
      await saveData(d)
      return interaction.reply(`✅ **${user.username}** removed from whitelist.`)
    }
    if (sub === "list") {
      if (!d.whitelist.length) return interaction.reply("no users whitelisted.")
      return interaction.reply(`**Whitelisted Users:**\n${d.whitelist.map(id => `<@${id}>`).join("\n")}`)
    }
  }

  if (cmd === "setup") {
    if (!await hasStaffMember(interaction.member)) return interaction.reply({ content: "staff only.", ephemeral: true })
    const d = await loadData()
    if (sub === "universe") {
      d.universeId = interaction.options.getString("id")
      await saveData(d)
      return interaction.reply(`✅ universe ID set to \`${d.universeId}\``)
    }
    if (sub === "staffrole") {
      d.staffRole = interaction.options.getString("name")
      await saveData(d)
      return interaction.reply(`✅ staff role set to **${d.staffRole}**`)
    }
    if (sub === "prefix") {
      const newPrefix = interaction.options.getString("prefix")
      if (!d.prefixes) d.prefixes = {}
      d.prefixes[interaction.guild.id] = newPrefix
      await saveData(d)
      return interaction.reply(`✅ prefix set to \`${newPrefix}\``)
    }
    if (sub === "show") {
      const prefix = await getPrefix(interaction.guild.id)
      return interaction.reply([
        `**Bot Settings**`,
        `Universe ID: \`${d.universeId || "not set"}\``,
        `Staff Role: **${d.staffRole || "Staff Team"}**`,
        `Prefix: \`${prefix}\``,
        `Whitelisted Users: ${d.whitelist.length}`
      ].join("\n"))
    }
  }

  if (cmd === "ds") {
    if (!await hasStaffMember(interaction.member) && !await isWhitelisted(interaction.user.id)) return interaction.reply({ content: "you're not whitelisted.", ephemeral: true })
    const d = await loadData()
    const uid = d.universeId
    if (!uid) return interaction.reply({ content: "universe ID not set. run /setup universe first.", ephemeral: true })
    await interaction.deferReply()
    if (sub === "list") {
      const data = await dsListStores(uid)
      const stores = data?.datastores?.map(s => `\`${s.name}\``).join("\n")
      return interaction.editReply(stores ? `**Datastores:**\n${stores}` : "no datastores found.")
    }
    if (sub === "entries") {
      const store = interaction.options.getString("store")
      const data = await dsListEntries(uid, store)
      const keys = data?.keys?.map(k => `\`${k.key}\``).join("\n")
      return interaction.editReply(keys ? `**${store} entries (up to 25):**\n${keys}` : "no entries found.")
    }
    if (sub === "get") {
      const store = interaction.options.getString("store")
      const key = interaction.options.getString("key")
      const { status, body } = await dsGetEntry(uid, store, key)
      if (status !== 200) return interaction.editReply(`❌ failed (${status})`)
      const preview = body.length > 1800 ? body.slice(0, 1800) + "..." : body
      return interaction.editReply(`**${store} / ${key}:**\n\`\`\`json\n${preview}\n\`\`\``)
    }
    if (sub === "set") {
      const store = interaction.options.getString("store")
      const key = interaction.options.getString("key")
      let value = interaction.options.getString("value")
      try { JSON.parse(value) } catch { value = JSON.stringify(value) }
      const { status, ok } = await dsSetEntry(uid, store, key, value)
      return interaction.editReply(ok ? `✅ **${store} / ${key}** updated.` : `❌ failed (${status})`)
    }
    if (sub === "delete") {
      const store = interaction.options.getString("store")
      const key = interaction.options.getString("key")
      const { status, ok } = await dsDeleteEntry(uid, store, key)
      return interaction.editReply(ok ? `✅ **${store} / ${key}** deleted.` : `❌ failed (${status})`)
    }
    if (sub === "increment") {
      const store = interaction.options.getString("store")
      const key = interaction.options.getString("key")
      const amount = interaction.options.getNumber("amount")
      const { status, body, ok } = await dsIncrementEntry(uid, store, key, amount)
      return interaction.editReply(ok ? `✅ **${store} / ${key}** new value: \`${body}\`` : `❌ failed (${status})`)
    }
    if (sub === "versions") {
      const store = interaction.options.getString("store")
      const key = interaction.options.getString("key")
      const data = await dsListVersions(uid, store, key)
      const versions = data?.versions?.map(v => `\`${v.version}\` — ${new Date(v.createdTime).toLocaleString()}`).join("\n")
      return interaction.editReply(versions ? `**${store} / ${key} versions:**\n${versions}` : "no versions found.")
    }
  }

  if (cmd === "rban" || cmd === "runban") {
    if (!await hasStaffMember(interaction.member)) return interaction.reply({ content: "staff only.", ephemeral: true })
    const username = interaction.options.getString("username")
    const reason = interaction.options.getString("reason") || "No reason provided"
    const d = await loadData()
    const gameId = d.universeId || "5254578111"
    await interaction.reply("looking up user...")
    const user = await getRobloxUser(username)
    if (!user) return interaction.editReply(`❌ no Roblox user found for **${username}**.`)
    if (cmd === "rban") {
      const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${gameId}/user-restrictions/${user.id}?updateMask=gameJoinRestriction`, {
        method: "PATCH",
        headers: { "x-api-key": process.env.ROBLOX_BAN_API_KEY, "Content-Type": "application/json" },
        body: JSON.stringify({ gameJoinRestriction: { active: true, privateReason: reason, displayReason: "You have been banned from this experience." } })
      })
      if (!res.ok) return interaction.editReply(`❌ ban failed. status: \`${res.status}\``)
      return interaction.editReply([`🔨 **Player Banned**`, `**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``, `**Reason:** ${reason}`, `**Banned By:** ${interaction.user.username}`].join("\n"))
    }
    if (cmd === "runban") {
      const res = await fetch(`https://apis.roblox.com/cloud/v2/universes/${gameId}/user-restrictions/${user.id}?updateMask=gameJoinRestriction`, {
        method: "PATCH",
        headers: { "x-api-key": process.env.ROBLOX_BAN_API_KEY, "Content-Type": "application/json" },
        body: JSON.stringify({ gameJoinRestriction: { active: false, privateReason: "", displayReason: "", excludeAltAccounts: false } })
      })
      if (!res.ok) return interaction.editReply(`❌ unban failed. status: \`${res.status}\``)
      return interaction.editReply([`✅ **Player Unbanned**`, `**Roblox User:** ${user.displayName} (\`${username}\`)`, `**User ID:** \`${user.id}\``, `**Unbanned By:** ${interaction.user.username}`].join("\n"))
    }
  }
})

client.login(process.env.DISCORD_TOKEN)
