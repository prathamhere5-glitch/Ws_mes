import express from "express";
import { createServer as createViteServer } from "vite";
import { Telegraf, Context, Markup } from "telegraf";
import makeWASocket, { 
  useMultiFileAuthState, 
  DisconnectReason, 
  fetchLatestBaileysVersion,
  WASocket
} from "@whiskeysockets/baileys";
import pino from "pino";
import { Boom } from "@hapi/boom";
import fs from "fs";
import path from "path";
import { DateTime } from "luxon";
import cron from "node-cron";
import dotenv from "dotenv";
import pg from "pg";

dotenv.config();

const { Pool } = pg;
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || "postgresql://postgres:Pratham1234%23@db.rayyrbgatdsvouizpjvs.supabase.co:5432/postgres",
  ssl: { rejectUnauthorized: false }
});

const app = express();
const PORT = 3000;

// --- State Management ---
interface UserSession {
  id: string; // phone number
  socket: WASocket | null;
  status: "connected" | "connecting" | "disconnected";
}

interface ForceSubChannel {
  id: string | number;
  type: "normal" | "join_request";
}

interface AppState {
  sessions: Map<string, UserSession>;
  delay: number; // in seconds
  isP2PActive: boolean;
  isGroupActive: boolean;
  manuallyStoppedP2P: boolean;
  scheduledTime: string | null; // HH:mm format or HH:mm-HH:mm range
  lastChatId: number | null;
  userLanguages: Map<number, string>;
  totalMessagesSent: number;
  authorizedUsers: Set<number>;
  forceSubChannels: ForceSubChannel[]; // array of channel objects
  users: Map<number, { id: number, username?: string, first_name: string, last_name?: string }>;
  accessMode: "public" | "private";
  pendingAddSub?: { id: string | number };
  appUrl: string | null;
  botEnabled: boolean;
  userAccounts: Map<number, string[]>; // userId -> phoneNumbers[]
}

const state: AppState = {
  sessions: new Map(),
  delay: 250, // default 200-300 range
  isP2PActive: false,
  isGroupActive: false,
  manuallyStoppedP2P: false,
  scheduledTime: null,
  lastChatId: null,
  userLanguages: new Map(),
  totalMessagesSent: 0,
  authorizedUsers: new Set(),
  forceSubChannels: [],
  users: new Map(),
  accessMode: "private",
  appUrl: process.env.APP_URL || null,
  botEnabled: true,
  userAccounts: new Map(),
};

async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bot_state (
        id SERIAL PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS wa_sessions (
        phone TEXT PRIMARY KEY,
        creds JSONB NOT NULL
      );
    `);
    console.log("Database tables initialized.");
  } catch (err) {
    console.error("Failed to initialize database:", err);
  }
}

async function saveStateToDB() {
  try {
    const data = {
      delay: state.delay,
      scheduledTime: state.scheduledTime,
      lastChatId: state.lastChatId,
      totalMessagesSent: state.totalMessagesSent,
      authorizedUsers: Array.from(state.authorizedUsers),
      forceSubChannels: state.forceSubChannels,
      userLanguages: Array.from(state.userLanguages.entries()),
      users: Array.from(state.users.entries()),
      accessMode: state.accessMode,
      appUrl: state.appUrl,
      botEnabled: state.botEnabled,
      userAccounts: Array.from(state.userAccounts.entries())
    };
    await pool.query("INSERT INTO bot_state (id, data) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET data = $1", [data]);
    console.log("State saved to database.");
  } catch (err) {
    console.error("Failed to save state to DB:", err);
  }
}

async function loadStateFromDB(bot?: Telegraf<Context>) {
  try {
    const res = await pool.query("SELECT data FROM bot_state WHERE id = 1");
    if (res.rows.length > 0) {
      const data = res.rows[0].data;
      state.delay = data.delay || 250;
      state.scheduledTime = data.scheduledTime || null;
      state.lastChatId = data.lastChatId || null;
      state.totalMessagesSent = data.totalMessagesSent || 0;
      state.authorizedUsers = new Set(data.authorizedUsers || []);
      state.forceSubChannels = data.forceSubChannels || [];
      state.userLanguages = new Map(data.userLanguages || []);
      state.users = new Map(data.users || []);
      state.accessMode = data.accessMode || "private";
      state.appUrl = data.appUrl || null;
      state.botEnabled = data.botEnabled !== undefined ? data.botEnabled : true;
      state.userAccounts = new Map(data.userAccounts || []);
      console.log("State loaded from database.");

      // Auto-reconnect existing accounts
      if (bot) {
        for (const [userId, phoneNumbers] of state.userAccounts) {
          for (const phone of phoneNumbers) {
            console.log(`Auto-reconnecting account ${phone} for user ${userId}...`);
            connectToWhatsApp(phone, bot, userId, false); // false = not a new connection
          }
        }
      }
    }
  } catch (err) {
    console.error("Failed to load state from DB:", err);
  }
}

const ADMIN_ID = process.env.ADMIN_ID ? parseInt(process.env.ADMIN_ID) : null;

const TRANSLATIONS: Record<string, any> = {
  en: {
    welcome: "👋 Welcome to WhatsApp Bot Manager!\n\nWhat does this bot do?\nThis bot allows you to link multiple WhatsApp accounts and automate conversations between them. It mimics human behavior to keep your accounts active and safe.\n\nBenefits:\n✅ Anti-Ban Protection: Uses randomized delays.\n✅ Multi-Account: Link as many accounts as you need.\n✅ Scheduling: Set a time and let the bot work.\n✅ 24/7 Activity: Keeps your accounts active.\n\nDeveloped by @indiawsagent",
    choose_lang: "Please choose your language:",
    menu_add: "➕ Add Account",
    menu_list: "📋 List Accounts",
    menu_schedule: "⏰ Schedule",
    menu_delay: "⏳ Set Delay",
    menu_start: "🚀 Start Messaging",
    menu_p2p: "👤 Person to Person",
    menu_group: "👥 Group Messaging",
    menu_stop: "🛑 Stop",
    menu_logout: "🚪 Logout Account",
    menu_status: "📊 Status",
    menu_change_lang: "🌐 Change Language",
    menu_admin: "🛠 Admin Panel",
    prompt_phone: "Please send the phone number you want to link (with country code, e.g., 919876543210):",
    prompt_delay: "Current delay: {delay}s\n\nSend a number to change the delay (e.g., 250):",
    prompt_schedule: "Current schedule: {schedule} (IST)\n\nSend time in HH:mm format (e.g., 14:30) or a range HH:mm-HH:mm (e.g., 05:00-18:00) to schedule:",
    back: "🔙 Back to Menu",
    status_text: "📊 Bot Status\n\n✅ Total Messages Sent: {total}\n🔗 Active Sessions: {sessions}\n⏳ Current Delay: {delay}s\n⏰ Scheduled: {schedule} (IST)",
    unauthorized: "⚠️ You are not authorized to use this bot. Please contact the admin.",
    contact_admin: "👤 Contact Admin",
    force_sub: "⚠️ You must subscribe to our channels to use this bot:\n{channels}\n\nAfter subscribing, click /start again.",
  },
  hi: {
    welcome: "👋 व्हाट्सएप बॉट मैनेजर में आपका स्वागत है!\n\nयह बॉट क्या करता है?\nयह बॉट आपको कई व्हाट्सएप खातों को जोड़ने और उनके बीच बातचीत को स्वचालित करने की अनुमति देता है। यह आपके खातों को सक्रिय और सुरक्षित रखने के लिए मानवीय व्यवहार की नकल करता है।\n\nलाभ:\n✅ एंटी-बैन सुरक्षा: यादृच्छिक देरी का उपयोग करता है।\n✅ मल्टी-अकाउंट: जितने चाहें उतने खाते जोड़ें।\n✅ शेड्यूलिंग: समय निर्धारित करें और बॉट को काम करने दें।\n✅ 24/7 गतिविधि: आपके खातों को सक्रिय रखता है।\n\nDeveloped by @indiawsagent",
    choose_lang: "कृपया अपनी भाषा चुनें:",
    menu_add: "➕ खाता जोड़ें",
    menu_list: "📋 खातों की सूची",
    menu_schedule: "⏰ शेड्यूल",
    menu_delay: "⏳ विलंब सेट करें",
    menu_start: "🚀 संदेश भेजना शुरू करें",
    menu_p2p: "👤 व्यक्ति से व्यक्ति",
    menu_group: "👥 ग्रुप मैसेजिंग",
    menu_stop: "🛑 रोकें",
    menu_logout: "🚪 खाता लॉगआउट करें",
    menu_status: "📊 स्थिति",
    menu_change_lang: "🌐 भाषा बदलें",
    menu_admin: "🛠 एडमिन पैनल",
    prompt_phone: "कृपया वह फ़ोन नंबर भेजें जिसे आप लिंक करना चाहते हैं (देश कोड के साथ, जैसे, 919876543210):",
    prompt_delay: "वर्तमान विलंब: {delay}s\n\nविलंब बदलने के लिए एक नंबर भेजें (जैसे, 250):",
    prompt_schedule: "वर्तमान शेड्यूल: {schedule} (IST)\n\nशेड्यूल करने के लिए HH:mm प्रारूप (जैसे, 14:30) या HH:mm-HH:mm रेंज (जैसे, 05:00-18:00) में समय भेजें:",
    back: "🔙 मेनू पर वापस जाएं",
    status_text: "📊 बॉट स्थिति\n\n✅ कुल भेजे गए संदेश: {total}\n🔗 सक्रिय सत्र: {sessions}\n⏳ वर्तमान विलंब: {delay}s\n⏰ शेड्यूल: {schedule} (IST)",
    unauthorized: "⚠️ आप इस बॉट का उपयोग करने के लिए अधिकृत नहीं हैं। कृपया एडमिन से संपर्क करें।",
    contact_admin: "👤 एडमिन से संपर्क करें",
    force_sub: "⚠️ इस बॉट का उपयोग करने के लिए आपको हमारे चैनलों की सदस्यता लेनी होगी:\n{channels}\n\nसदस्यता लेने के बाद, फिर से /start पर क्लिक करें।",
  },
  id: {
    welcome: "👋 Selamat datang di Pengelola Bot WhatsApp!\n\nApa yang dilakukan bot ini?\nBot ini memungkinkan Anda menautkan beberapa akun WhatsApp dan mengotomatiskan percakapan di antara mereka. Ini meniru perilaku manusia agar akun Anda tetap aktif dan aman.\n\nManfaat:\n✅ Perlindungan Anti-Ban: Menggunakan jeda acak.\n✅ Multi-Akun: Tautkan akun sebanyak yang Anda butuhkan.\n✅ Penjadwalan: Atur waktu dan biarkan bot bekerja.\n✅ Aktivitas 24/7: Menjaga akun Anda tetap aktif.\n\nDeveloped by @indiawsagent",
    choose_lang: "Silakan pilih bahasa Anda:",
    menu_add: "➕ Tambah Akun",
    menu_list: "📋 Daftar Akun",
    menu_schedule: "⏰ Jadwal",
    menu_delay: "⏳ Atur Jeda",
    menu_start: "🚀 Mulai Mengirim Pesan",
    menu_p2p: "👤 Orang ke Orang",
    menu_group: "👥 Pesan Grup",
    menu_stop: "🛑 Berhenti",
    menu_logout: "🚪 Keluar Akun",
    menu_status: "📊 Status",
    menu_change_lang: "🌐 Ubah Bahasa",
    menu_admin: "🛠 Panel Admin",
    prompt_phone: "Silakan kirim nomor telepon yang ingin Anda tautkan (dengan kode negara, misal, 919876543210):",
    prompt_delay: "Jeda saat ini: {delay}s\n\nKirim angka untuk mengubah jeda (misal, 250):",
    prompt_schedule: "Jadwal saat ini: {schedule} (WIB)\n\nKirim waktu dalam format HH:mm (misal, 14:30) atau rentang HH:mm-HH:mm (misal, 05:00-18:00) untuk menjadwalkan:",
    back: "🔙 Kembali ke Menu",
    status_text: "📊 Status Bot\n\n✅ Total Pesan Terkirim: {total}\n🔗 Sesi Aktif: {sessions}\n⏳ Jeda Saat Ini: {delay}s\n⏰ Jadwal: {schedule} (WIB)",
    unauthorized: "⚠️ Anda tidak berwenang menggunakan bot ini. Silakan hubungi admin.",
    contact_admin: "👤 Hubungi Admin",
    force_sub: "⚠️ Anda harus berlangganan saluran kami untuk menggunakan bot ini:\n{channels}\n\nSetelah berlangganan, klik /start lagi.",
  },
  zh: {
    welcome: "👋 欢迎使用 WhatsApp 机器人管理器！\n\n这个机器人是做什么的？\n此机器人允许您链接多个 WhatsApp 账号并自动执行它们之间的对话。它模仿人类行为以确保您的账号活跃且安全。\n\n优势：\n✅ 防封号保护：使用随机延迟。\n✅ 多账号支持：根据需要链接多个账号。\n✅ 定时任务：设置时间，让机器人为您工作。\n✅ 24/7 活跃：保持您的账号活跃。\n\nDeveloped by @indiawsagent",
    choose_lang: "请选择您的语言：",
    menu_add: "➕ 添加账号",
    menu_list: "📋 账号列表",
    menu_schedule: "⏰ 定时任务",
    menu_delay: "⏳ 设置延迟",
    menu_start: "🚀 开始发送消息",
    menu_p2p: "👤 个人对个人",
    menu_group: "👥 群组消息",
    menu_stop: "🛑 停止",
    menu_logout: "🚪 退出账号",
    menu_status: "📊 状态",
    menu_change_lang: "🌐 更改语言",
    menu_admin: "🛠 管理面板",
    prompt_phone: "请发送您要链接的电话号码（带国家代码，例如 919876543210）：",
    prompt_delay: "当前延迟：{delay}s\n\n发送数字以更改延迟（例如 250）：",
    prompt_schedule: "当前计划：{schedule} (北京时间)\n\n发送 HH:mm 格式的时间（例如 14:30）或时间范围 HH:mm-HH:mm（例如 05:00-18:00）进行预约：",
    back: "🔙 返回菜单",
    status_text: "📊 机器人状态\n\n✅ 已发送消息总数: {total}\n🔗 活动会话: {sessions}\n⏳ 当前延迟: {delay}s\n⏰ 计划: {schedule} (北京时间)",
    unauthorized: "⚠️ 您未被授权使用此机器人。请联系管理员。",
    contact_admin: "👤 联系管理员",
    force_sub: "⚠️ 您必须订阅我们的频道才能使用此机器人：\n{channels}\n\n订阅后，请再次点击 /start。",
  }
};

const SESSIONS_DIR = path.join(process.cwd(), "sessions");
if (!fs.existsSync(SESSIONS_DIR)) {
  fs.mkdirSync(SESSIONS_DIR);
}

// --- WhatsApp Logic ---

async function connectToWhatsApp(phoneNumber: string, bot: Telegraf<Context>, chatId: number, isNew: boolean = false) {
  const sessionPath = path.join(SESSIONS_DIR, phoneNumber);
  
  // Only delete session folder if it's a brand new connection request
  if (isNew && fs.existsSync(sessionPath)) {
    fs.rmSync(sessionPath, { recursive: true, force: true });
  }

  const { state: authState, saveCreds } = await useMultiFileAuthState(sessionPath);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    printQRInTerminal: false,
    auth: authState,
    logger: pino({ level: "silent" }),
    browser: ["Ubuntu", "Chrome", "20.0.04"],
  });

  state.sessions.set(phoneNumber, { id: phoneNumber, socket: sock, status: "connecting" });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;
    
    if (connection === "close") {
      const shouldReconnect = (lastDisconnect?.error as Boom)?.output?.statusCode !== DisconnectReason.loggedOut;
      console.log(`Connection closed for ${phoneNumber}. Reconnecting: ${shouldReconnect}`);
      
      state.sessions.set(phoneNumber, { id: phoneNumber, socket: null, status: "disconnected" });
      
      if (shouldReconnect) {
        // Reconnect WITHOUT deleting session folder
        connectToWhatsApp(phoneNumber, bot, chatId, false);
      } else {
        bot.telegram.sendMessage(chatId, `❌ Account ${phoneNumber} logged out.`);
        // Remove from user accounts
        const userAccs = state.userAccounts.get(chatId) || [];
        state.userAccounts.set(chatId, userAccs.filter(p => p !== phoneNumber));
        saveStateToDB();
        
        // Clean up session folder on explicit logout
        if (fs.existsSync(sessionPath)) {
          fs.rmSync(sessionPath, { recursive: true, force: true });
        }
      }
    } else if (connection === "open") {
      console.log("Opened connection for", phoneNumber);
      state.sessions.set(phoneNumber, { id: phoneNumber, socket: sock, status: "connected" });
      
      // Only notify if it was a user-initiated connection
      if (isNew) {
        bot.telegram.sendMessage(chatId, `✅ WhatsApp account ${phoneNumber} linked successfully!`);
      }
      
      // Add to user accounts if not already there
      const userAccs = state.userAccounts.get(chatId) || [];
      if (!userAccs.includes(phoneNumber)) {
        userAccs.push(phoneNumber);
        state.userAccounts.set(chatId, userAccs);
        saveStateToDB();
      }
    }
  });

  // Handle pairing code ONLY if not registered and it's a new connection
  if (isNew && !sock.authState.creds.registered) {
    setTimeout(async () => {
      try {
        // Double check registration status before requesting
        if (!sock.authState.creds.registered) {
          const code = await sock.requestPairingCode(phoneNumber);
          bot.telegram.sendMessage(chatId, `🔢 Your pairing code for ${phoneNumber} is: \`${code}\`\n\nEnter this code on your WhatsApp Web (Link with phone number).`, { parse_mode: "Markdown" });
        }
      } catch (err) {
        console.error("Error requesting pairing code:", err);
        bot.telegram.sendMessage(chatId, "❌ Failed to generate pairing code. Please try again.");
      }
    }, 5000);
  }

  return sock;
}

// --- Messaging Logic ---

const RANDOM_MESSAGES = [
  "Hey, how are you doing? 😊", "Just checking in! 👋", "Did you see that news today? 📰", "Hope you're having a great day. 🌟",
  "What's the plan for the weekend? 🗓️", "I'm thinking of starting a new project. 💡", "The weather is quite nice today, isn't it? ☀️",
  "Have you finished that book yet? 📚", "Let's catch up soon! ☕", "I'll be busy for a while, talk later. ⏳",
  "What's for lunch today? 🍕", "I just saw a really funny video. 😂", "Do you have any recommendations for a good movie? 🎬",
  "I'm feeling a bit tired today. 😴", "Just finished a long workout! 💪", "Are you coming to the party on Friday? 🎉",
  "I need to buy some new clothes. 🛍️", "Have you ever been to that new cafe downtown? ☕", "I'm so excited for the concert! 🎸",
  "What's your favorite type of music? 🎶", "I'm learning to play the guitar. 🎸", "Do you like to travel? ✈️",
  "I'm planning a trip to Japan next year. 🇯🇵", "What's the best place you've ever visited? 🌍", "I love cooking new recipes. 🍳",
  "Do you have any pets? 🐶", "I have a dog named Max. 🐕", "What's your favorite hobby? 🎨", "I enjoy hiking in the mountains. 🏔️",
  "Have you seen the latest episode of that show? 📺", "I'm a big fan of sci-fi movies. 🚀", "What's your favorite book? 📖",
  "I'm currently reading a mystery novel. 🔍", "Do you like to play video games? 🎮", "I'm playing a lot of Minecraft lately. 🧱",
  "What's your favorite sport? ⚽", "I love playing soccer. ⚽", "Have you ever tried skydiving? 🪂",
  "I'm a bit nervous about my presentation tomorrow. 😟", "Good luck with your exam! 🍀", "I'm so happy for you! ✨",
  "Congratulations on your new job! 🎊", "I'm sorry to hear that. 😔", "I hope you feel better soon. 💊",
  "Let me know if you need anything. 🤝", "I'm always here for you. ❤️", "You're a great friend! 👫",
  "Thanks for everything! 🙏", "I really appreciate your help. 🙌", "I'll talk to you later. 👋", "Have a wonderful evening! 🌙",
  // Warming / Group Growth Messages
  "We should add some more people to this group! 📈",
  "I'm thinking of inviting some international friends here. 🌍",
  "Does anyone know the best way to grow our community? 🚀",
  "Welcome to all the new members! 👋",
  "Let's keep the conversation active to keep the group healthy. ✨",
  "I love how diverse this group is getting. 🌟",
  "Adding members from different countries would be awesome. 🗺️",
  "What are the group rules for adding new people? 📋",
  "I'm going to share this group link with some friends. 🔗",
  "Let's make this the best group on WhatsApp! 🏆"
];

const EMOJIS = ["😊", "😂", "🤣", "😍", "🙌", "👍", "🔥", "✨", "🎉", "👋", "🤔", "😎", "💯", "🚀", "💡"];

async function sendHumanizedMessage(sender: UserSession, jid: string, message: string) {
  try {
    // Typing jitter
    await sender.socket!.sendPresenceUpdate('composing', jid);
    const typingTime = Math.floor(Math.random() * 3000) + 2000;
    await new Promise(resolve => setTimeout(resolve, typingTime));
    await sender.socket!.sendPresenceUpdate('paused', jid);

    // Randomly decide to send an image (10% chance) for warming
    if (Math.random() > 0.9) {
      const randomId = Math.floor(Math.random() * 1000);
      await sender.socket!.sendMessage(jid, { 
        image: { url: `https://picsum.photos/seed/${randomId}/800/600` },
        caption: "Check this out! 📸"
      });
      state.totalMessagesSent++;
      return true;
    }

    // Randomly decide to send an audio/voice message (5% chance)
    if (Math.random() > 0.95) {
      await sender.socket!.sendMessage(jid, {
        audio: { url: "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3" },
        mimetype: 'audio/mp4',
        ptt: true // Send as a Push-to-Talk voice message
      });
      state.totalMessagesSent++;
      return true;
    }

    // Randomly add an extra emoji
    const finalMessage = Math.random() > 0.7 ? `${message} ${EMOJIS[Math.floor(Math.random() * EMOJIS.length)]}` : message;
    
    await sender.socket!.sendMessage(jid, { text: finalMessage });
    state.totalMessagesSent++;
    return true;
  } catch (err) {
    console.error(`Failed to send message from ${sender.id}:`, err);
    return false;
  }
}

async function runAutomatedMessaging(bot: Telegraf<Context>, chatId: number) {
  if (state.isP2PActive) return;
  
  const userPhoneNumbers = state.userAccounts.get(chatId) || [];
  const connectedSessions = userPhoneNumbers
    .map(p => state.sessions.get(p))
    .filter(s => s && s.status === "connected" && s.socket) as UserSession[];

  if (connectedSessions.length < 2) {
    bot.telegram.sendMessage(chatId, "⚠️ You need at least 2 connected accounts to start messaging.");
    return;
  }

  state.isP2PActive = true;
  bot.telegram.sendMessage(chatId, `🚀 Starting simultaneous automated messaging session for ${connectedSessions.length} accounts...`);

  let totalSessionMessages = 0;
  let breakThreshold = Math.floor(Math.random() * 11) + 15; // 15 to 25
  
  const intervals = Math.floor(Math.random() * 11) + 10; // 10 to 20 intervals

  for (let i = 0; i < intervals; i++) {
    if (!state.isP2PActive) break;

    // Coffee Break Check
    if (totalSessionMessages >= breakThreshold) {
      bot.telegram.sendMessage(chatId, "☕ Taking a 10-minute coffee break to stay human...");
      // Check for stop every second during break
      for (let s = 0; s < 600; s++) {
        if (!state.isP2PActive) break;
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      if (!state.isP2PActive) break;
      totalSessionMessages = 0;
      breakThreshold = Math.floor(Math.random() * 11) + 15;
    }

    // Form pairs based on logic
    const pairs: [UserSession, UserSession][] = [];
    if (connectedSessions.length % 2 === 0) {
      // Even: A <-> B, C <-> D
      for (let j = 0; j < connectedSessions.length; j += 2) {
        pairs.push([connectedSessions[j], connectedSessions[j+1]]);
        pairs.push([connectedSessions[j+1], connectedSessions[j]]);
      }
    } else {
      // Odd: A -> B, B -> C, C -> A, A -> C, C -> B, B -> A
      const n = connectedSessions.length;
      for (let j = 0; j < n; j++) {
        pairs.push([connectedSessions[j], connectedSessions[(j + 1) % n]]);
        pairs.push([connectedSessions[(j + 1) % n], connectedSessions[j]]);
      }
    }

    // Send messages in pairs simultaneously
    await Promise.all(pairs.map(async ([sender, receiver]) => {
      if (!state.isP2PActive) return;
      const message = RANDOM_MESSAGES[Math.floor(Math.random() * RANDOM_MESSAGES.length)];
      const jid = `${receiver.id}@s.whatsapp.net`;
      if (await sendHumanizedMessage(sender, jid, message)) {
        totalSessionMessages++;
      }
    }));

    if (!state.isP2PActive) break;

    // Wait for main delay with jitter
    const jitteredDelay = state.delay + (Math.random() * 20 - 10);
    const delayMs = Math.max(10, jitteredDelay) * 1000;
    // Check for stop during delay
    const checkInterval = 1000;
    for (let d = 0; d < delayMs; d += checkInterval) {
      if (!state.isP2PActive) break;
      await new Promise(resolve => setTimeout(resolve, Math.min(checkInterval, delayMs - d)));
    }
  }

  state.isP2PActive = false;
  bot.telegram.sendMessage(chatId, "🏁 Automated messaging session completed.");
}

async function runGroupMessaging(bot: Telegraf<Context>, chatId: number, senderPhones: string[], groupJids: string[]) {
  if (state.isGroupActive) {
    return bot.telegram.sendMessage(chatId, "⚠️ A group messaging session is already active. Please stop it first.");
  }
  
  const senders = senderPhones
    .map(p => state.sessions.get(p))
    .filter(s => s && s.status === "connected" && s.socket) as UserSession[];

  if (senders.length === 0) {
    return bot.telegram.sendMessage(chatId, "❌ No connected sender accounts found.");
  }

  state.isGroupActive = true;
  bot.telegram.sendMessage(chatId, `🚀 Starting group messaging with ${senders.length} accounts on ${groupJids.length} groups...\n\n📈 Limits: 1-4 messages per burst, 10m break every 30 messages per group.`);

  const groupMessageCounts = new Map<string, number>();
  const groupBreakEndTimes = new Map<string, number>();

  for (let i = 0; i < 100; i++) { // Run for more rounds since we have per-group breaks
    if (!state.isGroupActive) break;

    // Process all groups simultaneously
    const groupPromises = groupJids.map(async (groupJid, index) => {
      if (!state.isGroupActive) return;

      // Staggered start for each group to look more natural (0-3 seconds)
      const staggerTime = index * (Math.random() * 300 + 100);
      for (let s = 0; s < staggerTime; s += 100) {
        if (!state.isGroupActive) return;
        await new Promise(resolve => setTimeout(resolve, Math.min(100, staggerTime - s)));
      }

      if (!state.isGroupActive) return;

      // Check if this group is on a break
      const breakEnd = groupBreakEndTimes.get(groupJid) || 0;
      if (Date.now() < breakEnd) {
        return; // Skip this group for now
      }

      // Determine how many messages to send in this burst (1 to 4)
      const burstSize = Math.floor(Math.random() * 4) + 1;
      
      for (let b = 0; b < burstSize; b++) {
        if (!state.isGroupActive) break;

        // Pick a random sender for this message
        const sender = senders[Math.floor(Math.random() * senders.length)];
        const message = RANDOM_MESSAGES[Math.floor(Math.random() * RANDOM_MESSAGES.length)];
        
        if (await sendHumanizedMessage(sender, groupJid, message)) {
          // Increment group counter
          const currentCount = (groupMessageCounts.get(groupJid) || 0) + 1;
          groupMessageCounts.set(groupJid, currentCount);

          // Check for 30 message limit for this group
          if (currentCount >= 30) {
            const breakTime = 10 * 60 * 1000; // 10 minutes
            groupBreakEndTimes.set(groupJid, Date.now() + breakTime);
            groupMessageCounts.set(groupJid, 0); // Reset counter
            bot.telegram.sendMessage(chatId, `☕ Group *${groupJid.split('@')[0]}* reached 30 messages. Taking a 10-minute break for this group.`, { parse_mode: "Markdown" });
            break; // Stop burst for this group
          }
        }
        
        if (!state.isGroupActive) break;

        // Small delay between messages in the same burst (human-like)
        const burstDelay = (Math.random() * 3 + 2) * 1000;
        for (let d = 0; d < burstDelay; d += 500) {
          if (!state.isGroupActive) break;
          await new Promise(resolve => setTimeout(resolve, Math.min(500, burstDelay - d)));
        }
      }
    });

    // Wait for all group bursts in this round to complete
    await Promise.all(groupPromises);

    if (!state.isGroupActive) break;

    // Main delay between rounds
    const jitteredDelay = state.delay + (Math.random() * 20 - 10);
    const delayMs = Math.max(10, jitteredDelay) * 1000;
    for (let d = 0; d < delayMs; d += 1000) {
      if (!state.isGroupActive) break;
      await new Promise(resolve => setTimeout(resolve, Math.min(1000, delayMs - d)));
    }
  }

  state.isGroupActive = false;
  bot.telegram.sendMessage(chatId, "🏁 Group messaging session completed.");
}

// --- Telegram Bot ---

// --- Selection State ---
const userSelectionState = new Map<number, {
  selectedAccounts: string[];
  selectedGroups: string[];
  availableGroups: any[];
  currentPage: number;
}>();

let bot: Telegraf<Context>;
const botToken = process.env.TELEGRAM_BOT_TOKEN;
if (!botToken) {
  console.error("TELEGRAM_BOT_TOKEN is missing!");
} else {
  bot = new Telegraf(botToken);

  const getLang = (chatId: number) => state.userLanguages.get(chatId) || "en";
  const t = (chatId: number, key: string, params: Record<string, any> = {}) => {
    const lang = getLang(chatId);
    let text = TRANSLATIONS[lang][key] || TRANSLATIONS["en"][key] || key;
    for (const [k, v] of Object.entries(params)) {
      text = text.replace(`{${k}}`, v);
    }
    return text;
  };

  const getMainMenu = (chatId: number) => {
    const buttons = [
      [Markup.button.callback(t(chatId, "menu_add"), "add_account"), Markup.button.callback(t(chatId, "menu_list"), "list_accounts")],
      [Markup.button.callback(t(chatId, "menu_schedule"), "schedule"), Markup.button.callback(t(chatId, "menu_delay"), "set_delay")],
      [Markup.button.callback(t(chatId, "menu_start"), "start_messaging"), Markup.button.callback(t(chatId, "menu_stop"), "stop_messaging")],
      [Markup.button.callback(t(chatId, "menu_status"), "status"), Markup.button.callback(t(chatId, "menu_change_lang"), "change_lang")],
      [Markup.button.callback(t(chatId, "menu_logout"), "logout_menu")]
    ];

    if (chatId === ADMIN_ID) {
      buttons.push([Markup.button.callback(t(chatId, "menu_admin"), "admin_panel")]);
    }

    return Markup.inlineKeyboard(buttons);
  };

  const getGroupSelectionKeyboard = (userId: number) => {
    const selection = userSelectionState.get(userId);
    if (!selection) return null;

    const pageSize = 10;
    const startIdx = selection.currentPage * pageSize;
    const endIdx = startIdx + pageSize;
    const currentGroups = selection.availableGroups.slice(startIdx, endIdx);

    const buttons = currentGroups.map(g => {
      const isSelected = selection.selectedGroups.includes(g.id);
      return [Markup.button.callback(`${isSelected ? "✅ " : ""}${g.subject}`, `toggle_group_${g.id}`)];
    });

    const paginationButtons = [];
    if (selection.currentPage > 0) {
      paginationButtons.push(Markup.button.callback("⬅️ Previous", "group_prev_page"));
    }
    if (endIdx < selection.availableGroups.length) {
      paginationButtons.push(Markup.button.callback("➡️ Next", "group_next_page"));
    }
    if (paginationButtons.length > 0) {
      buttons.push(paginationButtons);
    }

    buttons.push([Markup.button.callback("🚀 Start Messaging", "group_start_final")]);
    buttons.push([Markup.button.callback(t(userId, "back"), "start_group_select_sender")]);

    return Markup.inlineKeyboard(buttons);
  };

  const checkForceSub = async (ctx: Context) => {
    if (state.forceSubChannels.length === 0) return true;
    for (const channel of state.forceSubChannels) {
      try {
        const member = await ctx.telegram.getChatMember(channel.id, ctx.from!.id);
        // For both normal and join_request, we check if they are currently a member
        // In Telegram, if a user has a pending join request, their status is still 'left'
        // So they must be approved to pass the check.
        if (member.status === "left" || member.status === "kicked") return false;
      } catch (err) {
        console.error(`Error checking sub for ${channel.id}:`, err);
        // If bot is not admin or channel not found, we might want to skip or fail.
        // Usually, failing is safer for force sub.
        return false;
      }
    }
    return true;
  };

  const isAuthorized = (userId: number) => {
    if (userId === ADMIN_ID) return true;
    if (!state.botEnabled) return false;
    if (state.accessMode === "public") return true;
    return state.authorizedUsers.has(userId);
  };

  bot.start(async (ctx) => {
    const userId = ctx.from!.id;
    state.lastChatId = ctx.chat.id;

    // User Join Notification
    if (!state.users.has(userId)) {
      state.users.set(userId, {
        id: userId,
        username: ctx.from!.username,
        first_name: ctx.from!.first_name,
        last_name: ctx.from!.last_name
      });

      if (ADMIN_ID) {
        const username = ctx.from!.username ? `@${ctx.from!.username.replace(/_/g, "\\_")}` : "No Username";
        const name = `${ctx.from!.first_name} ${ctx.from!.last_name || ""}`.trim();
        bot.telegram.sendMessage(ADMIN_ID, `🆕 *New User Joined*\n\n👤 Name: ${name}\n🆔 ID: \`${userId}\`\n🔗 Username: ${username}`, { parse_mode: "Markdown" });
      }
    }

    if (!isAuthorized(userId)) {
      const adminUser = ADMIN_ID ? state.users.get(ADMIN_ID) : null;
      const adminUsername = adminUser?.username || "indiawsagent";
      return ctx.reply(t(userId, "unauthorized"), Markup.inlineKeyboard([
        [Markup.button.url(t(userId, "contact_admin"), `https://t.me/${adminUsername}`)]
      ]));
    }

    if (!state.botEnabled && userId !== ADMIN_ID) {
      return ctx.reply("⚠️ The bot is currently under maintenance or disabled by admin. Please try again later.");
    }

    const isSubscribed = await checkForceSub(ctx);
    if (!isSubscribed) {
      const channels = state.forceSubChannels.map(c => `${c.id} (${c.type})`).join("\n");
      return ctx.reply(t(userId, "force_sub", { channels }));
    }

    if (!state.userLanguages.has(userId)) {
      return ctx.reply("Please choose your language / कृपया अपनी भाषा चुनें / Silakan pilih bahasa Anda / 请选择您的语言：", Markup.inlineKeyboard([
        [Markup.button.callback("English 🇺🇸", "lang_en"), Markup.button.callback("हिन्दी 🇮🇳", "lang_hi")],
        [Markup.button.callback("Indonesian 🇮🇩", "lang_id"), Markup.button.callback("Chinese 🇨🇳", "lang_zh")]
      ]));
    }

    ctx.reply(t(userId, "welcome"), getMainMenu(userId));
  });

  bot.action(/^lang_(.+)$/, (ctx) => {
    const lang = ctx.match[1];
    state.userLanguages.set(ctx.from!.id, lang);
    ctx.answerCbQuery();
    ctx.editMessageText(t(ctx.from!.id, "welcome"), getMainMenu(ctx.from!.id));
  });

  bot.action("main_menu", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText(t(ctx.from!.id, "welcome"), getMainMenu(ctx.from!.id));
  });

  bot.action("add_account", (ctx) => {
    const userId = ctx.from!.id;
    const userAccs = state.userAccounts.get(userId) || [];
    const limit = state.authorizedUsers.has(userId) || userId === ADMIN_ID ? 20 : 4;
    
    if (userAccs.length >= limit) {
      return ctx.answerCbQuery(`❌ You have reached your limit of ${limit} accounts.`);
    }
    
    ctx.answerCbQuery();
    ctx.reply(t(ctx.from!.id, "prompt_phone"), Markup.inlineKeyboard([[Markup.button.callback(t(ctx.from!.id, "back"), "main_menu")]]));
  });

  bot.action("list_accounts", (ctx) => {
    ctx.answerCbQuery();
    const userId = ctx.from!.id;
    const userPhoneNumbers = state.userAccounts.get(userId) || [];
    const sessions = userPhoneNumbers.map(p => state.sessions.get(p)).filter(Boolean) as UserSession[];
    
    if (sessions.length === 0) {
      return ctx.editMessageText("No accounts linked yet.", getMainMenu(ctx.from!.id));
    }
    const list = sessions.map(s => `${s.status === "connected" ? "✅" : "❌"} ${s.id} (${s.status})`).join("\n");
    ctx.editMessageText(`Linked Accounts (${sessions.length}/${state.authorizedUsers.has(userId) || userId === ADMIN_ID ? 20 : 4}):\n\n${list}`, getMainMenu(ctx.from!.id));
  });

  bot.action("set_delay", (ctx) => {
    ctx.answerCbQuery();
    ctx.reply(t(ctx.from!.id, "prompt_delay", { delay: state.delay }), Markup.inlineKeyboard([[Markup.button.callback(t(ctx.from!.id, "back"), "main_menu")]]));
  });

  bot.action("schedule", (ctx) => {
    const userId = ctx.from!.id;
    ctx.answerCbQuery();
    ctx.reply(t(userId, "prompt_schedule", { schedule: state.scheduledTime || "None" }), Markup.inlineKeyboard([
      [Markup.button.callback("❌ Clear Schedule", "clear_schedule")],
      [Markup.button.callback(t(userId, "back"), "main_menu")]
    ]));
  });

  bot.action("clear_schedule", (ctx) => {
    const userId = ctx.from!.id;
    state.scheduledTime = null;
    state.manuallyStoppedP2P = false;
    saveStateToDB();
    ctx.answerCbQuery("✅ Schedule cleared.");
    ctx.editMessageText("✅ *Schedule Cleared*\n\nAutomatic messaging is now disabled.", {
      parse_mode: "Markdown",
      ...getMainMenu(userId)
    });
  });

  bot.action("start_messaging", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText("🚀 *Choose Messaging Mode*", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([
        [Markup.button.callback(t(ctx.from!.id, "menu_p2p"), "start_p2p")],
        [Markup.button.callback(t(ctx.from!.id, "menu_group"), "start_group_select_sender")],
        [Markup.button.callback(t(ctx.from!.id, "back"), "main_menu")]
      ])
    });
  });

  bot.action("start_p2p", (ctx) => {
    ctx.answerCbQuery();
    state.manuallyStoppedP2P = false;
    runAutomatedMessaging(bot, ctx.from!.id);
  });

  bot.action("start_group_select_sender", (ctx) => {
    const userId = ctx.from!.id;
    const userPhoneNumbers = state.userAccounts.get(userId) || [];
    const connectedSessions = userPhoneNumbers.map(p => state.sessions.get(p)).filter(s => s && s.status === "connected") as UserSession[];
    
    if (connectedSessions.length === 0) {
      return ctx.answerCbQuery("❌ No connected accounts found.");
    }

    // Initialize selection state
    userSelectionState.set(userId, {
      selectedAccounts: [],
      selectedGroups: [],
      availableGroups: [],
      currentPage: 0
    });

    const selection = userSelectionState.get(userId)!;
    const buttons = connectedSessions.map(s => {
      const isSelected = selection.selectedAccounts.includes(s.id);
      return [Markup.button.callback(`${isSelected ? "✅ " : ""}${s.id}`, `toggle_acc_${s.id}`)];
    });
    
    buttons.push([Markup.button.callback("➡️ Next (Select Groups)", "group_accounts_done")]);
    buttons.push([Markup.button.callback(t(userId, "back"), "start_messaging")]);
    
    ctx.answerCbQuery();
    ctx.editMessageText("👥 *Select Accounts for Group Messaging*\nChoose one or more accounts. Messages will be sent from all selected accounts to common groups.", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard(buttons)
    });
  });

  bot.action(/^toggle_acc_(.+)$/, (ctx) => {
    const userId = ctx.from!.id;
    const phone = ctx.match[1];
    const selection = userSelectionState.get(userId);
    if (!selection) return ctx.answerCbQuery("Session expired. Please restart.");

    if (selection.selectedAccounts.includes(phone)) {
      selection.selectedAccounts = selection.selectedAccounts.filter(p => p !== phone);
    } else {
      selection.selectedAccounts.push(phone);
    }

    const userPhoneNumbers = state.userAccounts.get(userId) || [];
    const connectedSessions = userPhoneNumbers.map(p => state.sessions.get(p)).filter(s => s && s.status === "connected") as UserSession[];
    
    const buttons = connectedSessions.map(s => {
      const isSelected = selection.selectedAccounts.includes(s.id);
      return [Markup.button.callback(`${isSelected ? "✅ " : ""}${s.id}`, `toggle_acc_${s.id}`)];
    });
    
    buttons.push([Markup.button.callback("➡️ Next (Select Groups)", "group_accounts_done")]);
    buttons.push([Markup.button.callback(t(userId, "back"), "start_messaging")]);

    ctx.answerCbQuery();
    ctx.editMessageReplyMarkup(Markup.inlineKeyboard(buttons).reply_markup);
  });

  bot.action("group_accounts_done", async (ctx) => {
    const userId = ctx.from!.id;
    const selection = userSelectionState.get(userId);
    if (!selection || selection.selectedAccounts.length === 0) {
      return ctx.answerCbQuery("❌ Please select at least one account.");
    }

    ctx.answerCbQuery();
    ctx.editMessageText("🔍 Fetching common open groups... Please wait.");

    try {
      const allAccountGroups: Map<string, any>[] = [];
      
      for (const phone of selection.selectedAccounts) {
        const session = state.sessions.get(phone);
        if (session && session.socket) {
          const groups = await session.socket.groupFetchAllParticipating();
          allAccountGroups.push(new Map(Object.entries(groups)));
        }
      }

      if (allAccountGroups.length === 0) {
        return ctx.editMessageText("❌ Could not fetch groups for selected accounts.", getMainMenu(userId));
      }

      // Find common groups
      let commonGroupIds = Array.from(allAccountGroups[0].keys());
      for (let i = 1; i < allAccountGroups.length; i++) {
        commonGroupIds = commonGroupIds.filter(id => allAccountGroups[i].has(id));
      }

      if (commonGroupIds.length === 0) {
        return ctx.editMessageText("❌ No common groups found between selected accounts.", getMainMenu(userId));
      }

      // Filter for "open" groups (where we can send messages)
      const commonGroups: any[] = [];
      const firstAccGroups = allAccountGroups[0];
      
      for (const id of commonGroupIds) {
        const group = firstAccGroups.get(id);
        // Check if group is open (not announce: true, or if we are admin)
        // Note: Baileys groupMetadata has 'announce' property
        // We also check if we are not banned (if we are in the list, we are not banned)
        
        // Simple check: if announce is false, everyone can send. 
        // If announce is true, only admins can send.
        // For simplicity and safety, we'll show groups where announce is false.
        if (!group.announce) {
          commonGroups.push(group);
        }
      }

      if (commonGroups.length === 0) {
        return ctx.editMessageText("❌ No common 'open' groups found (where all accounts can send messages).", getMainMenu(userId));
      }

      selection.availableGroups = commonGroups;
      selection.selectedGroups = [];
      selection.currentPage = 0;

      const keyboard = getGroupSelectionKeyboard(userId);
      if (!keyboard) return ctx.editMessageText("❌ Error generating keyboard.", getMainMenu(userId));

      ctx.editMessageText(`👥 *Select Common Groups (${commonGroups.length} found)*\nOnly groups where all selected accounts can send messages are shown.`, {
        parse_mode: "Markdown",
        ...keyboard
      });
    } catch (err) {
      console.error("Error fetching common groups:", err);
      ctx.editMessageText("❌ Error fetching groups: " + (err as Error).message, getMainMenu(userId));
    }
  });

  bot.action(/^toggle_group_(.+)$/, (ctx) => {
    const userId = ctx.from!.id;
    const groupId = ctx.match[1];
    const selection = userSelectionState.get(userId);
    if (!selection) return ctx.answerCbQuery("Session expired.");

    if (selection.selectedGroups.includes(groupId)) {
      selection.selectedGroups = selection.selectedGroups.filter(id => id !== groupId);
    } else {
      selection.selectedGroups.push(groupId);
    }

    const keyboard = getGroupSelectionKeyboard(userId);
    if (!keyboard) return ctx.answerCbQuery("Error generating keyboard.");

    ctx.answerCbQuery();
    ctx.editMessageReplyMarkup(keyboard.reply_markup);
  });

  bot.action("group_next_page", (ctx) => {
    const userId = ctx.from!.id;
    const selection = userSelectionState.get(userId);
    if (!selection) return ctx.answerCbQuery("Session expired.");

    selection.currentPage++;
    const keyboard = getGroupSelectionKeyboard(userId);
    if (!keyboard) return ctx.answerCbQuery("Error generating keyboard.");

    ctx.answerCbQuery();
    ctx.editMessageReplyMarkup(keyboard.reply_markup);
  });

  bot.action("group_prev_page", (ctx) => {
    const userId = ctx.from!.id;
    const selection = userSelectionState.get(userId);
    if (!selection) return ctx.answerCbQuery("Session expired.");

    if (selection.currentPage > 0) {
      selection.currentPage--;
    }
    const keyboard = getGroupSelectionKeyboard(userId);
    if (!keyboard) return ctx.answerCbQuery("Error generating keyboard.");

    ctx.answerCbQuery();
    ctx.editMessageReplyMarkup(keyboard.reply_markup);
  });

  bot.action("group_start_final", (ctx) => {
    const userId = ctx.from!.id;
    const selection = userSelectionState.get(userId);
    if (!selection || selection.selectedGroups.length === 0) {
      return ctx.answerCbQuery("❌ Please select at least one group.");
    }
    
    if (state.isGroupActive) {
      return ctx.answerCbQuery("⚠️ A group session is already active.");
    }

    ctx.answerCbQuery("🚀 Starting session...");
    ctx.editMessageText("✅ *Group Messaging Session Started*\n\nYour accounts are now warming up the selected groups. You will receive updates here.", { parse_mode: "Markdown" });
    
    runGroupMessaging(bot, userId, selection.selectedAccounts, selection.selectedGroups);
    // Don't delete state immediately to avoid race conditions if user clicks again
    setTimeout(() => userSelectionState.delete(userId), 5000);
  });

  bot.action("stop_messaging", (ctx) => {
    const userId = ctx.from!.id;
    ctx.answerCbQuery();
    
    const buttons = [];
    if (state.isP2PActive) buttons.push([Markup.button.callback("🛑 Stop P2P Messaging", "stop_p2p")]);
    if (state.isGroupActive) buttons.push([Markup.button.callback("🛑 Stop Group Messaging", "stop_group")]);
    
    if (buttons.length === 0) {
      return ctx.reply("ℹ️ No active messaging sessions to stop.");
    }
    
    buttons.push([Markup.button.callback("🛑 Stop All", "stop_all")]);
    buttons.push([Markup.button.callback(t(userId, "back"), "start_messaging")]);
    
    ctx.reply("❓ Which messaging session would you like to stop?", Markup.inlineKeyboard(buttons));
  });

  bot.action("stop_p2p", (ctx) => {
    ctx.answerCbQuery();
    state.isP2PActive = false;
    state.manuallyStoppedP2P = true;
    ctx.reply("🛑 Stopping P2P automated messaging...");
  });

  bot.action("stop_group", (ctx) => {
    ctx.answerCbQuery();
    state.isGroupActive = false;
    ctx.reply("🛑 Stopping Group automated messaging...");
  });

  bot.action("stop_all", (ctx) => {
    ctx.answerCbQuery();
    state.isP2PActive = false;
    state.isGroupActive = false;
    state.manuallyStoppedP2P = true;
    ctx.reply("🛑 Stopping all automated messaging...");
  });

  bot.action("logout_menu", (ctx) => {
    ctx.answerCbQuery();
    const userId = ctx.from!.id;
    const userPhoneNumbers = state.userAccounts.get(userId) || [];
    const sessions = userPhoneNumbers.map(p => state.sessions.get(p)).filter(Boolean) as UserSession[];
    
    if (sessions.length === 0) return ctx.editMessageText("No accounts to logout.", getMainMenu(ctx.from!.id));
    
    const buttons = sessions.map(s => [Markup.button.callback(`Logout ${s.id}`, `logout_${s.id}`)]);
    buttons.push([Markup.button.callback(t(ctx.from!.id, "back"), "main_menu")]);
    ctx.editMessageText("Select account to logout:", Markup.inlineKeyboard(buttons));
  });

  bot.action("change_lang", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText("Please choose your language / कृपया अपनी भाषा चुनें / Silakan pilih bahasa Anda / 请选择您的语言：", Markup.inlineKeyboard([
      [Markup.button.callback("English 🇺🇸", "lang_en"), Markup.button.callback("हिन्दी 🇮🇳", "lang_hi")],
      [Markup.button.callback("Indonesian 🇮🇩", "lang_id"), Markup.button.callback("Chinese 🇨🇳", "lang_zh")]
    ]));
  });

  bot.action("status", (ctx) => {
    ctx.answerCbQuery();
    const userId = ctx.chat!.id;
    const text = t(userId, "status_text", {
      total: state.totalMessagesSent,
      sessions: Array.from(state.sessions.values()).filter(s => s.status === "connected").length,
      delay: state.delay,
      schedule: state.scheduledTime || "None"
    });
    ctx.editMessageText(text, { parse_mode: "Markdown", ...getMainMenu(userId) });
  });

  bot.action("admin_panel", (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return ctx.answerCbQuery("Unauthorized");
    ctx.answerCbQuery();
    ctx.editMessageText(`🛠 *Admin Panel*\n\nCurrent Access Mode: ${state.accessMode.toUpperCase()}\nBot Status: ${state.botEnabled ? "✅ ON" : "🛑 OFF"}\nApp URL: ${state.appUrl || "Not Set"}\n\nManage users, broadcasts, and force subscription settings.`, {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([
        [Markup.button.callback("📊 Stats", "admin_stats"), Markup.button.callback("📢 Broadcast", "admin_broadcast")],
        [Markup.button.callback("👤 Manage Access", "admin_access"), Markup.button.callback("📢 Force Sub", "admin_forcesub")],
        [Markup.button.callback(`🔓 Mode: ${state.accessMode === "public" ? "PUBLIC" : "PRIVATE"}`, "toggle_mode")],
        [Markup.button.callback(`${state.botEnabled ? "🛑 Turn OFF" : "✅ Turn ON"}`, "toggle_bot")],
        [Markup.button.callback("🔗 Set App URL", "admin_seturl")],
        [Markup.button.callback("💾 Backup", "admin_backup"), Markup.button.callback("📂 Restore", "admin_restore")],
        [Markup.button.callback("🔙 Back", "main_menu")]
      ])
    });
  });

  bot.action("admin_seturl", (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return ctx.answerCbQuery("Unauthorized");
    ctx.answerCbQuery();
    ctx.editMessageText("🔗 *Set App URL*\n\nPlease send your App URL (e.g., `https://your-app.run.app`) to keep the bot active 24/7.\n\nSend `seturl <url>` to update.", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.action("toggle_bot", async (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return ctx.answerCbQuery("Unauthorized");
    state.botEnabled = !state.botEnabled;
    saveStateToDB();
    ctx.answerCbQuery(`Bot turned ${state.botEnabled ? "ON" : "OFF"}`);
    
    const statusMsg = state.botEnabled ? "✅ *Bot is now ONLINE*" : "🛑 *Bot is now OFFLINE for maintenance*";
    
    // Broadcast to all users
    for (const [uid] of state.users) {
      try {
        await bot.telegram.sendMessage(uid, statusMsg, { parse_mode: "Markdown" });
      } catch (err) {
        console.error(`Failed to notify ${uid} about bot status change:`, err);
      }
    }
    
    // Refresh admin panel
    ctx.editMessageText(`🛠 *Admin Panel*\n\nCurrent Access Mode: ${state.accessMode.toUpperCase()}\nBot Status: ${state.botEnabled ? "✅ ON" : "🛑 OFF"}\nApp URL: ${state.appUrl || "Not Set"}\n\nManage users, broadcasts, and force subscription settings.`, {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([
        [Markup.button.callback("📊 Stats", "admin_stats"), Markup.button.callback("📢 Broadcast", "admin_broadcast")],
        [Markup.button.callback("👤 Manage Access", "admin_access"), Markup.button.callback("📢 Force Sub", "admin_forcesub")],
        [Markup.button.callback(`🔓 Mode: ${state.accessMode === "public" ? "PUBLIC" : "PRIVATE"}`, "toggle_mode")],
        [Markup.button.callback(`${state.botEnabled ? "🛑 Turn OFF" : "✅ Turn ON"}`, "toggle_bot")],
        [Markup.button.callback("🔗 Set App URL", "admin_seturl")],
        [Markup.button.callback("💾 Backup", "admin_backup"), Markup.button.callback("📂 Restore", "admin_restore")],
        [Markup.button.callback("🔙 Back", "main_menu")]
      ])
    });
  });

  bot.action("toggle_mode", (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return ctx.answerCbQuery("Unauthorized");
    state.accessMode = state.accessMode === "public" ? "private" : "public";
    saveStateToDB();
    ctx.answerCbQuery(`Access mode set to ${state.accessMode}`);
    // Refresh admin panel
    ctx.editMessageText(`🛠 *Admin Panel*\n\nCurrent Access Mode: ${state.accessMode.toUpperCase()}\nBot Status: ${state.botEnabled ? "✅ ON" : "🛑 OFF"}\nApp URL: ${state.appUrl || "Not Set"}\n\nManage users, broadcasts, and force subscription settings.`, {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([
        [Markup.button.callback("📊 Stats", "admin_stats"), Markup.button.callback("📢 Broadcast", "admin_broadcast")],
        [Markup.button.callback("👤 Manage Access", "admin_access"), Markup.button.callback("📢 Force Sub", "admin_forcesub")],
        [Markup.button.callback(`🔓 Mode: ${state.accessMode === "public" ? "PUBLIC" : "PRIVATE"}`, "toggle_mode")],
        [Markup.button.callback(`${state.botEnabled ? "🛑 Turn OFF" : "✅ Turn ON"}`, "toggle_bot")],
        [Markup.button.callback("🔗 Set App URL", "admin_seturl")],
        [Markup.button.callback("💾 Backup", "admin_backup"), Markup.button.callback("📂 Restore", "admin_restore")],
        [Markup.button.callback("🔙 Back", "main_menu")]
      ])
    });
  });

  bot.action("admin_stats", async (ctx) => {
    ctx.answerCbQuery();
    const totalUsers = state.users.size;
    const authUsers = state.authorizedUsers.size;
    const activeSessions = Array.from(state.sessions.values()).filter(s => s.status === "connected").length;
    
    let dbStatus = "❌ Disconnected";
    try {
      const res = await pool.query("SELECT 1");
      if (res.rows.length > 0) dbStatus = "✅ Connected";
    } catch (e) {}

    ctx.editMessageText(`📊 Admin Statistics\n\n👤 Total Users: ${totalUsers}\n✅ Authorized Users: ${authUsers}\n🔗 Active WhatsApp Sessions: ${activeSessions}\n✉️ Total Messages Sent: ${state.totalMessagesSent}\n🔓 Access Mode: ${state.accessMode.toUpperCase()}\n🔗 App URL: ${state.appUrl || "Not Set"}\n🗄 DB Status: ${dbStatus}`, {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.action("admin_backup", async (ctx) => {
    ctx.answerCbQuery("Creating backup...");
    try {
      const AdmZip = (await import("adm-zip")).default;
      const zip = new AdmZip();
      
      // Add sessions folder
      if (fs.existsSync(SESSIONS_DIR)) {
        zip.addLocalFolder(SESSIONS_DIR, "sessions");
      }
      
      // Add state data (excluding sockets)
      const backupData = {
        totalMessagesSent: state.totalMessagesSent,
        authorizedUsers: Array.from(state.authorizedUsers),
        forceSubChannels: state.forceSubChannels,
        userLanguages: Array.from(state.userLanguages.entries()),
        users: Array.from(state.users.entries()),
        delay: state.delay,
        scheduledTime: state.scheduledTime,
        accessMode: state.accessMode,
        appUrl: state.appUrl,
        botEnabled: state.botEnabled
      };
      zip.addFile("state.json", Buffer.from(JSON.stringify(backupData, null, 2)));
      
      const buffer = zip.toBuffer();
      const filename = `backup_${DateTime.now().toFormat("yyyyMMdd_HHmmss")}.zip`;
      
      await ctx.replyWithDocument({ source: buffer, filename }, { caption: "✅ Bot Backup Created Successfully" });
    } catch (err) {
      console.error("Backup failed:", err);
      ctx.reply("❌ Backup failed: " + (err as Error).message);
    }
  });

  bot.action("admin_restore", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText("📂 *Restore Data*\n\nPlease upload the backup ZIP file to restore all data and sessions.", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.on("document", async (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return;
    
    const doc = ctx.message.document;
    if (!doc.file_name?.endsWith(".zip")) {
      return ctx.reply("❌ Please upload a valid ZIP backup file.");
    }

    ctx.reply("⏳ Restoring data... This may take a moment.");
    
    try {
      const fileLink = await ctx.telegram.getFileLink(doc.file_id);
      const response = await fetch(fileLink.href);
      const buffer = Buffer.from(await response.arrayBuffer());
      
      const AdmZip = (await import("adm-zip")).default;
      const zip = new AdmZip(buffer);
      
      // Extract sessions
      zip.extractEntryTo("sessions/", SESSIONS_DIR, false, true);
      
      // Restore state
      const stateEntry = zip.getEntry("state.json");
      if (stateEntry) {
        const data = JSON.parse(stateEntry.getData().toString("utf8"));
        state.totalMessagesSent = data.totalMessagesSent || 0;
        state.authorizedUsers = new Set(data.authorizedUsers || []);
        state.forceSubChannels = data.forceSubChannels || [];
        state.userLanguages = new Map(data.userLanguages || []);
        state.users = new Map(data.users || []);
        state.delay = data.delay || 250;
        state.scheduledTime = data.scheduledTime || null;
        state.accessMode = data.accessMode || "private";
        state.appUrl = data.appUrl || null;
        
        ctx.reply("✅ State restored. Re-initializing WhatsApp sessions...");
        
        // Re-initialize sessions
        const sessionFolders = fs.readdirSync(SESSIONS_DIR);
        for (const phone of sessionFolders) {
          if (fs.statSync(path.join(SESSIONS_DIR, phone)).isDirectory()) {
            connectToWhatsApp(phone, bot, ctx.chat.id);
          }
        }
        
        ctx.reply("🎉 Restore complete! All accounts are being reconnected.");
      } else {
        ctx.reply("❌ Invalid backup: state.json missing.");
      }
    } catch (err) {
      console.error("Restore failed:", err);
      ctx.reply("❌ Restore failed: " + (err as Error).message);
    }
  });

  bot.action("admin_access", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText("👤 *Manage Access*\n\nSend `grant <userId>` to authorize or `revoke <userId>` to remove access.", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.action("admin_broadcast", (ctx) => {
    ctx.answerCbQuery();
    ctx.editMessageText("📢 *Broadcast*\n\nSend `broadcast <message>` to send a message to all users.", {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.action("admin_forcesub", (ctx) => {
    ctx.answerCbQuery();
    const list = state.forceSubChannels.map(c => `🔹 ${c.id} [${c.type.toUpperCase()}]`).join("\n") || "None";
    ctx.editMessageText(`📢 *Force Subscription*\n\nCurrent Channels:\n${list}\n\nSend \`addsub <id>\` to add or \`remsub <id>\` to remove.`, {
      parse_mode: "Markdown",
      ...Markup.inlineKeyboard([[Markup.button.callback("🔙 Back", "admin_panel")]])
    });
  });

  bot.action(/^addsub_type_(.+)$/, (ctx) => {
    if (ctx.from!.id !== ADMIN_ID) return ctx.answerCbQuery("Unauthorized");
    const type = ctx.match[1] as "normal" | "join_request";
    if (state.pendingAddSub) {
      state.forceSubChannels.push({ id: state.pendingAddSub.id, type });
      saveStateToDB();
      ctx.answerCbQuery(`Added ${state.pendingAddSub.id} as ${type}`);
      ctx.editMessageText(`✅ Channel ${state.pendingAddSub.id} added to force sub as *${type.toUpperCase()}*.`, { parse_mode: "Markdown" });
      delete state.pendingAddSub;
    } else {
      ctx.answerCbQuery("No pending channel addition.");
    }
  });

  bot.on("text", async (ctx, next) => {
    const userId = ctx.from!.id;
    const text = ctx.message.text;

    // Admin Commands
      if (userId === ADMIN_ID) {
        if (text.startsWith("grant ")) {
          const targetId = parseInt(text.split(" ")[1]);
          if (!isNaN(targetId)) {
            state.authorizedUsers.add(targetId);
            saveStateToDB();
            return ctx.reply(`✅ User ${targetId} granted access.`);
          }
        } else if (text.startsWith("revoke ")) {
          const targetId = parseInt(text.split(" ")[1]);
          if (!isNaN(targetId)) {
            state.authorizedUsers.delete(targetId);
            saveStateToDB();
            return ctx.reply(`✅ User ${targetId} access revoked.`);
          }
        } else if (text.startsWith("broadcast ")) {
          const msg = text.substring(10);
          let count = 0;
          for (const [uid] of state.users) {
            try {
              await bot.telegram.sendMessage(uid, `📢 *Broadcast Message*\n\n${msg}`, { parse_mode: "Markdown" });
              count++;
            } catch (err) {
              console.error(`Failed to broadcast to ${uid}:`, err);
            }
          }
          return ctx.reply(`✅ Broadcast sent to ${count} users.`);
        } else if (text.startsWith("seturl ")) {
          const url = text.split(" ")[1];
          if (url && (url.startsWith("http://") || url.startsWith("https://"))) {
            state.appUrl = url;
            saveStateToDB();
            return ctx.reply(`✅ App URL updated to: ${url}\n\nSelf-pinging mechanism is now active to keep the bot 24/7.`);
          } else {
            return ctx.reply("❌ Invalid URL. Please include http:// or https://");
          }
        } else if (text.startsWith("addsub ")) {
          const channel = text.split(" ")[1];
          if (channel) {
            const channelId = channel.startsWith("@") ? channel : (isNaN(parseInt(channel)) ? channel : parseInt(channel));
            
            // Verify if bot is admin in the channel
            try {
              const botMember = await ctx.telegram.getChatMember(channelId, ctx.botInfo.id);
              if (botMember.status !== "administrator" && botMember.status !== "creator") {
                return ctx.reply("❌ Bot must be an administrator in the channel to add it to force sub.");
              }
            } catch (err) {
              return ctx.reply(`❌ Could not verify channel ${channelId}. Make sure the bot is added to the channel as admin.\nError: ${(err as Error).message}`);
            }

            state.pendingAddSub = { id: channelId };
            return ctx.reply(`❓ Select the type for channel ${channelId}:`, Markup.inlineKeyboard([
              [Markup.button.callback("Normal", "addsub_type_normal"), Markup.button.callback("Join Request", "addsub_type_join_request")]
            ]));
          }
        } else if (text.startsWith("remsub ")) {
          const channel = text.split(" ")[1];
          const channelId = channel.startsWith("@") ? channel : (isNaN(parseInt(channel)) ? channel : parseInt(channel));
          state.forceSubChannels = state.forceSubChannels.filter(c => c.id !== channelId);
          saveStateToDB();
          return ctx.reply(`✅ Channel ${channelId} removed from force sub.`);
        }
      }

    if (!isAuthorized(userId)) return;

    if (/^\d{10,15}$/.test(text)) {
      ctx.reply(`⏳ Initiating connection for ${text}...`);
      await connectToWhatsApp(text, bot, ctx.chat.id, true); // true for new connection
    } else if (/^\d+$/.test(text) && parseInt(text) >= 10) {
      state.delay = parseInt(text);
      saveStateToDB();
      ctx.reply(`✅ Delay set to ${state.delay} seconds.`, getMainMenu(ctx.chat.id));
    } else if (/^\d{2}:\d{2}$/.test(text) || /^\d{2}:\d{2}-\d{2}:\d{2}$/.test(text)) {
      state.scheduledTime = text;
      saveStateToDB();
      const lang = getLang(ctx.chat.id);
      let tz = "IST";
      if (lang === "id") tz = "WIB";
      if (lang === "zh") tz = "Beijing Time";
      ctx.reply(`✅ Messaging scheduled for ${text} (${tz}) every day.`, getMainMenu(ctx.chat.id));
    } else {
      return next();
    }
  });

  bot.action(/^logout_(.+)$/, async (ctx) => {
    const phone = ctx.match[1];
    const session = state.sessions.get(phone);
    if (session && session.socket) {
      try { await session.socket.logout(); } catch (e) {}
      state.sessions.delete(phone);
      const sessionPath = path.join(SESSIONS_DIR, phone);
      if (fs.existsSync(sessionPath)) {
        fs.rmSync(sessionPath, { recursive: true, force: true });
      }
      // Remove from user accounts
      const userId = ctx.from!.id;
      const userAccs = state.userAccounts.get(userId) || [];
      state.userAccounts.set(userId, userAccs.filter(p => p !== phone));
      saveStateToDB();
      
      ctx.answerCbQuery(`Logged out ${phone}`);
      ctx.editMessageText(`✅ Account ${phone} has been logged out and removed.`);
    }
  });

  bot.catch((err: any, ctx: Context) => {
    console.error(`Telegraf Error for ${ctx.updateType}:`, err);
    if (err.message && err.message.includes("409: Conflict")) {
      console.warn("⚠️ Conflict detected. Another instance might be running. Retrying in 5s...");
      setTimeout(() => bot.launch().catch(e => console.error("Retry failed:", e)), 5000);
    }
  });

  // Proper shutdown handlers
  process.once('SIGINT', () => {
    console.log("Shutting down bot...");
    bot.stop('SIGINT');
  });
  process.once('SIGTERM', () => {
    console.log("Shutting down bot...");
    bot.stop('SIGTERM');
  });

  // Delay launch slightly to avoid 409 Conflict on rapid restarts
  setTimeout(() => {
    bot.launch()
      .then(() => console.log("Telegram bot launched!"))
      .catch(err => {
        console.error("Failed to launch bot:", err);
        if (err.message && err.message.includes("409: Conflict")) {
          console.warn("⚠️ Conflict detected on launch. Retrying in 10s...");
          setTimeout(() => bot.launch().catch(e => console.error("Retry failed:", e)), 10000);
        }
      });
  }, 2000);

  // --- Cron Job for Scheduling ---
  let lastRunMinute = "";

  cron.schedule("* * * * *", async () => {
    // Keep-alive log
    const nowIST = DateTime.now().setZone("Asia/Kolkata");
    const currentTimeIST = nowIST.toFormat("HH:mm:ss");
    console.log(`[${currentTimeIST}] Bot Heartbeat: Active`);

    // Self-ping mechanism to keep the bot active 24/7
    if (state.appUrl) {
      try {
        const pingUrl = `${state.appUrl.replace(/\/$/, "")}/api/status`;
        await Promise.all([
          fetch(pingUrl),
          fetch(state.appUrl)
        ]);
      } catch (err) {
        console.error("Self-ping failed:", (err as Error).message);
      }
    }

    if (!state.scheduledTime) return;
    
    // Determine timezone based on last user's language or default to IST
    const lang = state.lastChatId ? state.userLanguages.get(state.lastChatId) || "en" : "en";
    let timezone = "Asia/Kolkata";
    if (lang === "id") timezone = "Asia/Jakarta";
    if (lang === "zh") timezone = "Asia/Shanghai";

    const nowTZ = DateTime.now().setZone(timezone);
    const currentTime = nowTZ.toFormat("HH:mm");
    const currentDate = nowTZ.toFormat("yyyy-MM-dd");
    const currentMinuteKey = `${currentDate}_${currentTime}`;

    // Prevent multiple starts in the same minute
    if (lastRunMinute === currentMinuteKey) return;
    
    if (state.scheduledTime.includes("-")) {
      // Range mode: HH:mm-HH:mm
      const [start, end] = state.scheduledTime.split("-");
      if (currentTime >= start && currentTime <= end) {
        if (!state.isP2PActive && !state.manuallyStoppedP2P) {
          console.log(`Scheduled range reached (${start}-${end}), starting messaging...`);
          if (state.lastChatId) {
            lastRunMinute = currentMinuteKey;
            runAutomatedMessaging(bot, state.lastChatId);
          }
        }
      } else {
        // Outside range: reset manually stopped flag for next window
        state.manuallyStoppedP2P = false;
        if (state.isP2PActive) {
          console.log(`Out of scheduled range (${start}-${end}), stopping messaging...`);
          state.isP2PActive = false;
        }
      }
    } else {
      // Single time mode: HH:mm
      if (currentTime === state.scheduledTime) {
        if (!state.isP2PActive && !state.manuallyStoppedP2P) {
          console.log(`Scheduled time reached (${state.scheduledTime}), starting messaging...`);
          if (state.lastChatId) {
            lastRunMinute = currentMinuteKey;
            runAutomatedMessaging(bot, state.lastChatId);
          }
        }
      } else {
        // Different time: reset manually stopped flag for next day
        state.manuallyStoppedP2P = false;
      }
    }
  });
}

// --- Express & Vite ---

async function startServer() {
  await initDB();
  await loadStateFromDB(bot);

  // Middleware
  app.use(express.json());

  // API routes (Registered BEFORE Vite middleware)
  app.get("/api/status", (req, res) => {
    res.json({
      sessions: Array.from(state.sessions.values()).map(s => ({ id: s.id, status: s.status })),
      delay: state.delay,
      isP2PActive: state.isP2PActive,
      isGroupActive: state.isGroupActive,
      scheduledTime: state.scheduledTime,
      totalMessagesSent: state.totalMessagesSent,
      dbStatus: "connected"
    });
  });

  app.get("/api/health", (req, res) => {
    res.json({ status: "ok" });
  });

  // Removed Vite/Frontend logic for Bot-Only deployment

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}

startServer();
