import { Client, TextChannel } from "discord.js-selfbot-v13";
import sqlite from "bun:sqlite";

const db = new sqlite("database.sqlite");
const client = new Client();

// Drop and recreate the 'messages' table on every run
db.run(`DROP TABLE IF EXISTS messages`);
db.run(`
  CREATE TABLE messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    authorId TEXT,
    author TEXT,
    content TEXT,
    messageId TEXT UNIQUE,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP 
  )
`);

let dumpChannel: TextChannel | null = null;
const targetChannelId = "1157345461598965800";
const dumpChannelId = "1292662478546669651";
const messagesPerFetch = 100;
const delayBetweenFetches = 100;

client.on("ready", async () => {
  console.log(`${client.user!.username} is ready!`);

  try {
    dumpChannel = (await client.channels.fetch(dumpChannelId)) as TextChannel;
    const channel = (await client.channels.fetch(
      targetChannelId
    )) as TextChannel;
    console.log(`Found target channel: ${channel.name}`);

    await fetchAndStoreMessages(channel);
  } catch (error) {
    console.error("Error during initialization:", error);
    // Handle initialization errors appropriately (e.g., exit process)
  }
});

async function fetchAndStoreMessages(channel: TextChannel) {
  let lastMessageId: string | undefined = undefined;
  let totalMessagesFetched = 0;

  while (true) {
    const start = new Date().getTime()
    console.log(
      `Fetching messages... (lastMessageId: ${lastMessageId || "N/A"})`
    );

    try {
      const options: {limit: number, before?: string} = lastMessageId
        ? { limit: messagesPerFetch, before: lastMessageId }
        : { limit: messagesPerFetch };

      const messages = await channel.messages.fetch(options);
      console.log(`Fetched ${messages.size} messages.`);

      if (messages.size === 0) {
        console.log("All messages fetched and stored!");
        break;
      }

      const insertStmt = db.prepare(
        "INSERT INTO messages (authorId, author, content, messageId) VALUES (?, ?, ?, ?)"
      );

      for (const message of messages.values()) {
        try {
          insertStmt.run(message.author.id, message.author.username, message.content, message.id);
          totalMessagesFetched++;
        } catch (dbError) {
          console.error(`Error inserting message ${message.id}:`, dbError);
        }
      }

      lastMessageId = messages.last()?.id;
      console.log(`Total messages fetched: ${totalMessagesFetched}`);

      const end = new Date().getTime()
      console.log(`time taken: ${Math.floor(end-start)}ms`)
      
      await new Promise((resolve) => setTimeout(resolve, delayBetweenFetches)); 
    } catch (error) {
      console.error("Error fetching or storing messages:", error); 
    }
  }
}


// async function sendChunks(message: any) {
//   try {
//     if (typeof message !== "string") message = JSON.stringify(message, null, 4);
//     const chunks = message.match(/.{1,1900}/gms) || [];
//     for (const chunk of chunks) {
//       await dumpChannel?.send("```\n" + chunk + "```");
//     }
//   } catch (err) {
//     console.error("Error sending chunks:", err);
//   }
// }

client.login(process.env.discord);
