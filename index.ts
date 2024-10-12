import {
  Client,
  Collection,
  Guild,
  Message,
  TextChannel,
  type NonThreadGuildBasedChannel,
} from "discord.js-selfbot-v13";
import sqlite, { Statement } from "bun:sqlite";

const db = new sqlite("database.sqlite");
const client = new Client();

const messagesPerChannel = 500;
const messagesPerFetch = 100;
const maxConcurrentTasks = 5;
const delayBetweenFetches = 100;
const MAX_RETRIES = 3;
const BASE_DELAY = 100;

let activeChannelFetches = 0;
const channelQueue: TextChannel[] = [];

client.on("ready", async () => {
  console.log(`${client.user!.username} is ready!`);

  try {
    const tables = db
      .query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
      )
      .all() as any;
    for (const table of tables) {
      db.run(`DROP TABLE IF EXISTS "${table.name}"`);
      console.log(`Dropped table: ${table.name}`);
    }

    listServersAndChannels();

    for (const guild of client.guilds.cache.values()) {
      const channels = await fetchChannelsWithRetry(guild);
      for (const channel of channels.values()) {
        if (channel?.type === "GUILD_TEXT") {
          channelQueue.push(channel as TextChannel);
        }
      }
    }

    for (let i = 0; i < maxConcurrentTasks && i < channelQueue.length; i++) {
      processNextChannel();
    }
  } catch (error) {
    console.error("Error during initialization:", error);
  }
});

async function listServersAndChannels() {
  console.log("Servers and Channels:");
  for (const guild of client.guilds.cache.values()) {
    console.log(`- ${guild.name} (ID: ${guild.id})`);
    const channels = await fetchChannelsWithRetry(guild);
    for (const channel of channels.values()) {
      if (channel) {
        console.log(
          `    - #${channel.name} (${guild.name}) (ID: ${channel.id}, Type: ${channel.type})`
        );
      }
    }
  }
}

async function fetchChannelsWithRetry(
  guild: Guild
): Promise<Collection<string, NonThreadGuildBasedChannel | null>> {
  let retries = 0;
  while (retries <= MAX_RETRIES) {
    try {
      return await guild.channels.fetch();
    } catch (error) {
      retries++;
      const delay = BASE_DELAY * retries * 2;
      console.warn(
        `Error fetching channels, retrying in ${delay / 1000}s...`,
        error
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  throw new Error(`Failed to fetch channels after ${MAX_RETRIES} retries.`);
}

async function fetchAndStoreChannelMessages(channel: TextChannel) {
  const tableName = `${channel.guild.name}: ${channel.name} - ${channel.guild.id} ${channel.id}`;
  db.run(`
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      authorId TEXT,
      author TEXT,
      content TEXT,
      messageId TEXT UNIQUE,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  let lastMessageId: string | undefined = undefined;
  let consecutiveErrors = 0;
  let messagesFetched = 0;

  while (
    consecutiveErrors < MAX_RETRIES &&
    messagesFetched < messagesPerChannel
  ) {
    try {
      const options: { limit: number; before?: string } = lastMessageId
        ? { limit: messagesPerFetch, before: lastMessageId }
        : { limit: messagesPerFetch };

      const messages = await channel.messages.fetch(options);
      console.log(`Fetched ${messages.size} messages from #${channel.name} (${channel.guild.name}).`);

      if (messages.size === 0) {
        break;
      }

      messagesFetched += messages.size;

      const insertStmt = db.prepare(
        `INSERT INTO "${tableName}" (authorId, author, content, messageId) VALUES (?, ?, ?, ?)`
      );
      for (const message of messages.values()) {
        await insertMessageWithRetry(message, insertStmt);
      }

      lastMessageId = messages.last()?.id;
      consecutiveErrors = 0;

      if (messages.size < messagesPerFetch) break;
      await new Promise((resolve) => setTimeout(resolve, delayBetweenFetches));
    } catch (error) {
      consecutiveErrors++;
      console.error(`Error fetching messages from #${channel.name} (${channel.guild.name}):`, error);
      if (consecutiveErrors < MAX_RETRIES) {
        const delay = BASE_DELAY * consecutiveErrors * 2;
        console.log(`Retrying in ${delay / 1000} seconds...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      } else {
        console.warn(`Skipping #${channel.name} (${channel.guild.name}) after exceeding max retries.`);
      }
    }
  }
  
  activeChannelFetches--;
  console.log(`${messagesFetched} messages from #${channel.name} (${channel.guild.name}) in ${channel.guild.name} fetched and stored. active channel fetches: ${activeChannelFetches}`);
  processNextChannel();
}

async function insertMessageWithRetry(message: Message, insertStmt: Statement) {
  let retries = 0;
  while (retries <= MAX_RETRIES) {
    try {
      insertStmt.run(
        message.author.id,
        message.author.username,
        message.content,
        message.id
      );
      return;
    } catch (dbError) {
      retries++;
      const delay = BASE_DELAY * retries;
      console.warn(
        `Error inserting message, retrying in ${delay / 1000}s...`,
        dbError
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
  console.error(
    `Failed to insert message ${message.id} after ${MAX_RETRIES} retries.`
  );
}

function processNextChannel() {
  if (channelQueue.length > 0 && activeChannelFetches < maxConcurrentTasks) {
    activeChannelFetches++;
    fetchAndStoreChannelMessages(channelQueue.shift()!);
  }
}

client.login(process.env.discord);