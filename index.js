const { Client, GatewayIntentBits } = require("discord.js")
const fetch = require("node-fetch")
const FormData = require("form-data")

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
})

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

client.on("messageCreate", async msg => {
  if (msg.author.bot) return
  const mp3 = msg.attachments.find(a => a.name?.endsWith(".mp3"))
  if (!mp3) return

  const status = await msg.reply("uploading to roblox...")

  try {
    const fileRes = await fetch(mp3.url)
    const buf = await fileRes.buffer()

    const form = new FormData()
    form.append("request", JSON.stringify({
      assetType: "Audio",
      displayName: mp3.name.replace(".mp3", ""),
      description: "",
      creationContext: { creator: { userId: process.env.ROBLOX_USER_ID } }
    }), { contentType: "application/json" })
    form.append("fileContent", buf, { filename: mp3.name, contentType: "audio/mpeg" })

    const res = await fetch("https://apis.roblox.com/assets/v1/assets", {
      method: "POST",
      headers: { "x-api-key": process.env.ROBLOX_API_KEY, ...form.getHeaders() },
      body: form
    })
    const data = await res.json()

    const opPath = data.path
    if (!opPath) return status.edit(`failed: ${JSON.stringify(data)}`)

    await status.edit("processing...")
    const assetId = await pollOperation(opPath)

    if (assetId) {
      status.edit(`done! asset id: \`${assetId}\``)
    } else {
      status.edit("upload timed out, check creator hub")
    }
  } catch (e) {
    status.edit(`error: ${e.message}`)
  }
})

client.login(process.env.DISCORD_TOKEN)
