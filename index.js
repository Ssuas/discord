const { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } = require("discord.js")
const fetch = require("node-fetch")
const FormData = require("form-data")

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
})

const commands = [
  new SlashCommandBuilder()
    .setName("upload")
    .setDescription("Upload an MP3 to Roblox")
    .addAttachmentOption(o => o.setName("file").setDescription("MP3 file").setRequired(true))
].map(c => c.toJSON())

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

client.once("ready", async () => {
  const rest = new REST({ version: "10" }).setToken(process.env.DISCORD_TOKEN)
  await rest.put(Routes.applicationCommands(client.user.id), { body: commands })
  console.log("ready")
})

client.on("interactionCreate", async interaction => {
  if (!interaction.isChatInputCommand()) return
  if (interaction.commandName !== "upload") return

  const file = interaction.options.getAttachment("file")
  if (!file.name.endsWith(".mp3")) return interaction.reply({ content: "mp3 only", ephemeral: true })

  await interaction.reply("uploading to roblox...")

  try {
    const fileRes = await fetch(file.url)
    const buf = await fileRes.buffer()

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
    const opPath = data.path
    if (!opPath) return interaction.editReply(`failed: ${JSON.stringify(data)}`)

    await interaction.editReply("processing...")
    const assetId = await pollOperation(opPath)

    if (assetId) {
      interaction.editReply(`**${file.name.replace(".mp3", "")}**\nasset id: \`${assetId}\``)
    } else {
      interaction.editReply("timed out, check creator hub")
    }
  } catch (e) {
    interaction.editReply(`error: ${e.message}`)
  }
})

client.login(process.env.DISCORD_TOKEN)
