const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require("discord.js")
const fetch = require("node-fetch")
const FormData = require("form-data")
const mm = require("music-metadata")

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
})

const FILE_COUNT = 5

const cmd = new SlashCommandBuilder()
  .setName("upload")
  .setDescription("Upload up to 5 MP3s to Roblox")

for (let i = 1; i <= FILE_COUNT; i++) {
  cmd.addAttachmentOption(o =>
    o.setName(`file${i}`).setDescription(`MP3 file ${i}`).setRequired(i === 1)
  )
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

client.once("ready", async () => {
  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN)
  await rest.put(Routes.applicationCommands(client.user.id), { body: [cmd.toJSON()] })
  console.log("ready")
})

client.on("interactionCreate", async interaction => {
  if (!interaction.isChatInputCommand() || interaction.commandName !== "upload") return

  const files = []
  for (let i = 1; i <= FILE_COUNT; i++) {
    const f = interaction.options.getAttachment(`file${i}`)
    if (f) files.push(f)
  }

  const invalid = files.filter(f => !f.name.endsWith(".mp3"))
  if (invalid.length) return interaction.reply({ content: "mp3 files only", ephemeral: true })

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

  interaction.editReply(lines.join("\n"))
})

client.login(process.env.DISCORD_TOKEN)
