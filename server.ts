import express from "express";
import { createServer as createViteServer } from "vite";
import { Telegraf, Markup, Context } from "telegraf";
import makeWASocket, { 
    DisconnectReason, 
    fetchLatestBaileysVersion,
    makeCacheableSignalKeyStore,
    WA_DEFAULT_EPHEMERAL,
    isJidBroadcast,
    proto,
    initAuthCreds,
    BufferJSON
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import Database from "better-sqlite3";
import { createDB, DB } from "./database.js";
import cron from "node-cron";
import fs from "fs";
import path from "path";
import dns from "dns";

// Prefer IPv4 but allow IPv6 if needed
if (typeof dns.setDefaultResultOrder === 'function') {
    dns.setDefaultResultOrder('ipv4first');
}

const logger = pino({ level: 'debug' });

// --- Configuration & Initialization ---
const PORT = process.env.PORT || 3000;
const OWNER_ID = process.env.OWNER_ID || "6729390752";
const OWNER_USERNAME = process.env.OWNER_USERNAME || "indiawsagent";
const botToken = process.env.TELEGRAM_BOT_TOKEN;
// Use a more stable path for data
const DATA_DIR = process.env.DATA_PATH || path.resolve(process.cwd(), "data");
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
}

const SESSIONS_DIR = path.resolve(DATA_DIR, "sessions");
if (!fs.existsSync(SESSIONS_DIR)) {
    fs.mkdirSync(SESSIONS_DIR, { recursive: true });
}

// Migration: Move old sessions to new SESSIONS_DIR if they exist
const oldSessionsDir = path.resolve(process.cwd(), "sessions");
if (fs.existsSync(oldSessionsDir) && fs.readdirSync(oldSessionsDir).length > 0) {
    console.log(`[Startup] Migrating sessions from ${oldSessionsDir} to ${SESSIONS_DIR}`);
    try {
        const files = fs.readdirSync(oldSessionsDir);
        for (const file of files) {
            const oldPath = path.join(oldSessionsDir, file);
            const newPath = path.join(SESSIONS_DIR, file);
            if (!fs.existsSync(newPath)) {
                fs.renameSync(oldPath, newPath);
            }
        }
    } catch (e) {
        console.error("[Startup] Session migration error:", e);
    }
}

const oldDbPath = path.resolve(process.cwd(), "bot_data.db");
const dbPath = path.resolve(DATA_DIR, "bot_data.db");

// Migration: Move old DB to new DATA_DIR if it exists
if (fs.existsSync(oldDbPath) && !fs.existsSync(dbPath)) {
    console.log(`[Startup] Migrating database from ${oldDbPath} to ${dbPath}`);
    try {
        fs.renameSync(oldDbPath, dbPath);
    } catch (e) {
        console.error("[Startup] DB migration error:", e);
    }
}

console.log(`[Startup] Database path: ${dbPath}`);
let db: DB = createDB(dbPath);
let isFreshDb = false;

async function initDatabase() {
    // Verify database integrity
    try {
        const userCount = await db.prepare("SELECT COUNT(*) as count FROM sqlite_master WHERE type='table' AND name='users'").get() as any;
        if (userCount && userCount.count > 0) {
            const totalUsers = await db.prepare("SELECT COUNT(*) as count FROM users").get() as any;
            console.log(`[Startup] Database connected. Found ${totalUsers?.count || 0} existing users.`);
        } else {
            console.log("[Startup] Database is fresh. Creating tables...");
            isFreshDb = true;
        }
    } catch (e) {
        console.error("[Startup] Database check error:", e);
    }

    // --- Database Setup ---
    await db.exec(`
      CREATE TABLE IF NOT EXISTS users (
        telegram_id TEXT PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        language TEXT,
        delay_seconds INTEGER DEFAULT 250,
        is_running INTEGER DEFAULT 0,
        is_authorized INTEGER DEFAULT 0,
        is_admin INTEGER DEFAULT 0,
        referred_by TEXT,
        referral_count INTEGER DEFAULT 0,
        sent_count INTEGER DEFAULT 0,
        access_expiry TEXT,
        current_step INTEGER DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS whatsapp_sessions (
        id SERIAL PRIMARY KEY,
        telegram_id TEXT,
        phone_number TEXT,
        session_id TEXT UNIQUE,
        is_connected INTEGER DEFAULT 0,
        is_new_link INTEGER DEFAULT 1
      );

      CREATE TABLE IF NOT EXISTS schedules (
        telegram_id TEXT PRIMARY KEY,
        cron_time TEXT,
        is_active INTEGER DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS whatsapp_auth (
        session_id TEXT,
        key_id TEXT,
        data TEXT,
        PRIMARY KEY (session_id, key_id)
      );

      CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
      );

      CREATE TABLE IF NOT EXISTS force_sub_channels (
        channel_id TEXT PRIMARY KEY,
        invite_link TEXT,
        channel_name TEXT
      );
    `);

    // Initialize default settings
    const initSettings = db.prepare("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)");
    await initSettings.run("is_public", "0");
    await initSettings.run("bot_active", "1");

    // Migration for whatsapp_auth to support multi-file auth state
    try {
        const authTableInfo = await db.prepare("PRAGMA table_info(whatsapp_auth)").all() as any[];
        const hasKeyId = authTableInfo.some(col => col.name === 'key_id');
        if (!hasKeyId && authTableInfo.length > 0) {
            console.log("Migration: Updating whatsapp_auth table...");
            await db.exec("DROP TABLE whatsapp_auth");
            await db.exec(`
                CREATE TABLE whatsapp_auth (
                    session_id TEXT,
                    key_id TEXT,
                    data TEXT,
                    PRIMARY KEY (session_id, key_id)
                )
            `);
        }
    } catch (e) {
        // Table might not exist yet, which is fine
    }

    // Ensure owner is admin and authorized
    if (OWNER_ID) {
        await db.prepare("INSERT OR IGNORE INTO users (telegram_id, username, is_authorized, is_admin) VALUES (?, ?, 1, 1)")
            .run(OWNER_ID, OWNER_USERNAME);
        await db.prepare("UPDATE users SET is_authorized = 1, is_admin = 1 WHERE telegram_id = ?")
            .run(OWNER_ID);
    }

    // Migration: Add columns if they don't exist
    try {
        const tableInfo = await db.prepare("PRAGMA table_info(users)").all() as any[];
        
        const hasDelayColumn = tableInfo.some(col => col.name === 'delay_seconds');
        if (!hasDelayColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN delay_seconds INTEGER DEFAULT 250");
            console.log("Migration: Added delay_seconds column to users table");
        }

        const hasRunningColumn = tableInfo.some(col => col.name === 'is_running');
        if (!hasRunningColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN is_running INTEGER DEFAULT 0");
            console.log("Migration: Added is_running column to users table");
        }

        const hasStepColumn = tableInfo.some(col => col.name === 'current_step');
        if (!hasStepColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN current_step INTEGER DEFAULT 0");
            console.log("Migration: Added current_step column to users table");
        }

        const hasAuthColumn = tableInfo.some(col => col.name === 'is_authorized');
        if (!hasAuthColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN is_authorized INTEGER DEFAULT 0");
            console.log("Migration: Added is_authorized column to users table");
        }

        const hasAdminColumn = tableInfo.some(col => col.name === 'is_admin');
        if (!hasAdminColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0");
            console.log("Migration: Added is_admin column to users table");
        }

        const hasReferredByColumn = tableInfo.some(col => col.name === 'referred_by');
        if (!hasReferredByColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN referred_by TEXT");
            console.log("Migration: Added referred_by column to users table");
        }

        const hasReferralCountColumn = tableInfo.some(col => col.name === 'referral_count');
        if (!hasReferralCountColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN referral_count INTEGER DEFAULT 0");
            console.log("Migration: Added referral_count column to users table");
        }

        const hasUsernameColumn = tableInfo.some(col => col.name === 'username');
        if (!hasUsernameColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN username TEXT");
            console.log("Migration: Added username column to users table");
        }

        const hasFirstNameColumn = tableInfo.some(col => col.name === 'first_name');
        if (!hasFirstNameColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN first_name TEXT");
            console.log("Migration: Added first_name column to users table");
        }

        const hasAccessExpiryColumn = tableInfo.some(col => col.name === 'access_expiry');
        if (!hasAccessExpiryColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN access_expiry TEXT");
            console.log("Migration: Added access_expiry column to users table");
        }

        const hasSentCountColumn = tableInfo.some(col => col.name === 'sent_count');
        if (!hasSentCountColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN sent_count INTEGER DEFAULT 0");
            console.log("Migration: Added sent_count column to users table");
        }

        const hasLanguageColumn = tableInfo.some(col => col.name === 'language');
        if (!hasLanguageColumn) {
            await db.exec("ALTER TABLE users ADD COLUMN language TEXT DEFAULT 'en'");
            console.log("Migration: Added language column to users table");
        }

        const sessionTableInfo = await db.prepare("PRAGMA table_info(whatsapp_sessions)").all() as any[];
        const hasNewLinkColumn = sessionTableInfo.some(col => col.name === 'is_new_link');
        if (!hasNewLinkColumn) {
            await db.exec("ALTER TABLE whatsapp_sessions ADD COLUMN is_new_link INTEGER DEFAULT 1");
            console.log("Migration: Added is_new_link column to whatsapp_sessions table");
        }
    } catch (e) {
        console.error("Migration error:", e);
    }
}


// --- Translations ---
const translations: any = {
    en: {
        welcome: "👋 *Welcome to WhatsApp Linker!* \n\nThis bot helps you keep your WhatsApp accounts active and safe from bans by creating a natural messaging loop between your own accounts. \n\n🚀 *Benefits:*\n• Creates strong account history\n• Reduces ban risk significantly\n• Add international users after 1 week\n• Fully automated 24/7 operation",
        choose_lang: "Please choose your language:",
        main_menu: "👋 Welcome back! Use the buttons below to manage your accounts.",
        add_acc: "📱 Add Account",
        list_acc: "📋 List Accounts",
        start_msg: "🚀 Start Messaging",
        stop_msg: "🛑 Stop",
        schedule: "📅 Schedule",
        delay: "⏳ Set Delay",
        status: "📊 Status",
        referral: "👥 Referral Program",
        logout: "🚪 Logout All",
        admin: "🔑 Admin Panel",
        lang_btn: "🌐 Change Language",
        force_sub: "⚠️ *Join our channels to continue using the bot!*",
        verify_btn: "✅ Verify Subscription",
        contact_owner: "👤 Contact Owner",
        ref_link: "👥 My Referral Link",
        claim_btn: "🎁 Claim 15 Days Access",
        access_denied: "🚫 *Access Denied*\n\nYou do not have access. Refer 10 users or contact admin.",
        pairing_instruction: "Please send the phone number (with country code, e.g., 919876543210):",
        delay_instruction: "Enter delay in seconds (Recommended: 200-300s):",
        bot_disabled: "⚠️ *Bot Disabled*\n\nThe bot is currently disabled by the owner for maintenance.",
        acc_limit_reached: "❌ You have reached the account limit of {limit} accounts.",
        pairing_fresh: "⏳ Requesting a fresh pairing code for {phoneNumber}...",
        delay_updated: "✅ Delay updated to {delay} seconds. This will take effect from the next message.",
        min_delay: "❌ Delay too short! Minimum 10 seconds for safety.",
        acc_deleted: "🗑️ Account {phoneNumber} has been deleted and session cleared.",
        all_logged_out: "👋 All accounts logged out and sessions cleared.",
        messaging_started: "🚀 Automated messaging started!\n\n⏱️ Current Delay: {delay}s (+ random jitter)\n🛡️ Anti-ban: Enabled (Typing simulation & randomized timing)",
        messaging_stopped: "🛑 Your messaging (manual and scheduled) has been stopped. Your sessions remain active.",
        need_2_acc: "❌ You need at least 2 connected accounts for two-way messaging.",
        back_to_menu_msg: "👋 Welcome back to the main menu!",
        schedule_set: "✅ Schedule set: *{range}* (IST).\n\nBot will now follow this schedule automatically.",
        schedule_instruction: "📅 *Set Messaging Schedule (IST)*\n\nPlease enter the time range in 24-hour format (e.g., `09:00-18:00`).\n\nThe bot will automatically start messaging during this period and stop outside of it.",
        back_btn: "🔙 Back",
        acc_disconnected: "⚠️ *Account Disconnected:* {phoneNumber}\n\nThis account has been logged out or the session has expired.",
        acc_connected: "✅ WhatsApp account {phoneNumber} connected successfully!",
        pairing_code_msg: "🔑 *Pairing Code for {phoneNumber}:*\n\n👉 `{code}`\n\n*⚠️ IMPORTANT:* You will NOT receive a push notification. You must enter this code manually:\n\n1️⃣ Open WhatsApp on your phone\n2️⃣ Go to *Settings* > *Linked Devices*\n3️⃣ Tap *Link a Device*\n4️⃣ Tap *Link with phone number instead*\n5️⃣ Enter the code: `{code}`",
        rate_limit: "❌ *Rate Limit:* WhatsApp is blocking pairing requests for {phoneNumber} temporarily. Please try again in 24 hours.",
        pairing_failed: "❌ Failed to get pairing code for {phoneNumber}: {error}",
        access_expired: "⚠️ *Access Expired*\n\nYour access to this bot has expired. Please contact the owner or refer 10 users to get 15 days of access.",
        choose_lang_title: "🌐 *Choose Your Language / अपनी भाषा चुनें / Pilih Bahasa Anda / 选择您的语言:*\n\nPlease select your preferred language to continue.",
        lang_updated: "✅ *Language Updated!*\n\n{welcome}",
        cleaning_up: "Cleaning up...",
        preparing_backup: "📦 Preparing database backup...",
        db_not_found: "❌ Database file not found.",
        restore_error_no_file: "❌ *Restore Error:* No pending restore file found. Please send the .db file again.",
        downloading_backup: "🔄 *Downloading backup...*",
        restore_failed_invalid: "❌ *Restore Failed:* The uploaded file is not a valid bot database or is corrupted.",
        restore_warning_0_users: "⚠️ *Warning:* The backup you uploaded contains *0 users*. Restoring this will wipe your current data. If you are sure, please send the file again and confirm.",
        restore_success: "✅ *Database Restored Successfully!*\n\nRestored `{count}` users.\n\n🔄 *Reloading sessions...*",
        bot_reloaded: "✨ *Bot Reloaded Successfully!*\n\nAll data has been restored and sessions are reconnecting.",
        restore_failed: "❌ *Restore Failed:* {error}",
        restore_cancelled: "❌ Restore cancelled.",
        db_restore_detected: "⚠️ *Database Restore Detected*\n\nAre you sure you want to replace the current database with this backup? This will:\n1️⃣ Replace all user data\n2️⃣ Restore all WhatsApp sessions\n3️⃣ Restart the bot\n\n*Proceed?*",
        doc_received: "❓ Received document: `{name}` ({type}).\n\nIf this is a database backup, please ensure it ends with `.db`.",
        doc_error: "❌ Error processing document: {error}",
        db_restore_required: "⚠️ *Database Restore Required*\n\nIt seems your bot's data was lost due to an environment reset. To restore your accounts and users:\n\n1️⃣ Send your `.db` backup file to this bot.\n2️⃣ Tap 'Confirm Restore' when prompted.\n\n_If you don't have a backup, you must start fresh._",
        help_msg: "📖 *Help & Commands*\n\n• /start - Show main menu\n• /id - Show your Telegram ID\n• /ping - Check bot status",
        user_not_found: "User not found.",
        referral_program_title: "👥 *Referral Program*\n\nRefer 10 users to get 15 days of access!\n\nYour Referrals: `{count}`",
        claim_success: "🎉 *Success!*\n\nYou have claimed 15 days of access using 10 referrals.\n\nNew Expiry: `{expiry}`\nRemaining Referrals: `{count}`",
        claim_failed: "❌ *Claim Failed*\n\nYou need at least 10 referrals to claim access. You currently have `{count}` referrals.",
        no_acc_linked: "No accounts linked yet.",
        acc_status_msg: "📱 *Account:* {phoneNumber}\n*Status:* {status}",
        broadcast_started: "📢 *Starting Broadcast...*\n\nTarget: `{count}` users.",
        broadcast_completed: "✅ *Broadcast Completed*\n\n🟢 Success: `{success}`\n🔴 Failed: `{fail}`",
        manual_restore_started: "🔄 *Starting Manual Restoration...*\n\nRestoring all WhatsApp sessions from database and filesystem.",
        manual_restore_complete: "✅ *Restoration Complete!*",
        manual_restore_failed: "❌ *Restoration Failed:* {error}",
        joined_channels: "👋 Welcome! You have successfully joined the required channels.",
        admin_restore_btn: "⚠️ RESTORE DATABASE",
        admin_panel_btn: "🔑 Admin Panel",
        bot_status_title: "📊 *Your Bot Status*",
        user_label: "👤 *User:*",
        bot_state_label: "🤖 *Bot State:*",
        access_label: "🔑 *Access:*",
        stats_label: "📈 *Statistics:*",
        msgs_sent_label: "• Messages Sent:",
        referrals_label: "• Referrals:",
        wa_accounts_label: "📱 *WhatsApp Accounts:*",
        acc_list_label: "*Account List:*",
        running_status: "🟢 Running",
        stopped_status: "🔴 Stopped",
        access_expiry_label: "• Access Expiry:",
        ref_link_label: "🔗 *Your Referral Link:*",
        no_active_access: "No active access",
        referral_milestone_msg: "🎉 *Congratulations!*\n\nYou have referred 10 users. Your access has been extended by 15 days.\n\nNew Expiry: `{expiry}`",
    },
    hi: {
        welcome: "👋 *WhatsApp Linker में आपका स्वागत है!* \n\nयह बॉट आपके WhatsApp खातों को सक्रिय रखने और आपके अपने खातों के बीच एक प्राकृतिक मैसेजिंग लूप बनाकर प्रतिबंधों से सुरक्षित रखने में मदद करता है। \n\n🚀 *लाभ:*\n• मजबूत खाता इतिहास बनाता है\n• प्रतिबंध जोखिम को काफी कम करता है\n• 1 सप्ताह के बाद अंतर्राष्ट्रीय उपयोगकर्ताओं को जोड़ें\n• पूरी तरह से स्वचालित 24/7 संचालन",
        choose_lang: "कृपया अपनी भाषा चुनें:",
        main_menu: "👋 वापस स्वागत है! अपने खातों को प्रबंधित करने के लिए नीचे दिए गए बटनों का उपयोग करें।",
        add_acc: "📱 खाता जोड़ें",
        list_acc: "📋 खातों की सूची",
        start_msg: "🚀 मैसेजिंग शुरू करें",
        stop_msg: "🛑 रोकें",
        schedule: "📅 शेड्यूल",
        delay: "⏳ देरी सेट करें",
        status: "📊 स्थिति",
        referral: "👥 रेफरल प्रोग्राम",
        logout: "🚪 सभी लॉगआउट करें",
        admin: "🔑 एडमिन पैनल",
        lang_btn: "🌐 भाषा बदलें",
        force_sub: "⚠️ *जारी रखने के लिए हमारे चैनलों से जुड़ें!*",
        verify_btn: "✅ सब्सक्रिप्शन सत्यापित करें",
        contact_owner: "👤 मालिक से संपर्क करें",
        ref_link: "👥 मेरा रेफरल लिंक",
        claim_btn: "🎁 15 दिन का एक्सेस प्राप्त करें",
        access_denied: "🚫 *एक्सेस अस्वीकार*\n\nआपके पास एक्सेस नहीं है। 10 उपयोगकर्ताओं को रेफर करें या एडमिन से संपर्क करें।",
        pairing_instruction: "कृपया फोन नंबर भेजें (देश कोड के साथ, जैसे 919876543210):",
        delay_instruction: "सेकंड में देरी दर्ज करें (अनुशंसित: 200-300s):",
        bot_disabled: "⚠️ *बॉट अक्षम*\n\nबॉट वर्तमान में रखरखाव के लिए मालिक द्वारा अक्षम किया गया है।",
        acc_limit_reached: "❌ आपने {limit} खातों की सीमा पूरी कर ली है।",
        pairing_fresh: "{phoneNumber} के लिए नया पेयरिंग कोड अनुरोध किया जा रहा है...",
        delay_updated: "✅ देरी को {delay} सेकंड में अपडेट किया गया। यह अगले संदेश से प्रभावी होगा।",
        min_delay: "❌ देरी बहुत कम है! सुरक्षा के लिए न्यूनतम 10 सेकंड।",
        acc_deleted: "🗑️ खाता {phoneNumber} हटा दिया गया है और सत्र साफ़ कर दिया गया है।",
        all_logged_out: "👋 सभी खाते लॉग आउट हो गए और सत्र साफ़ हो गए।",
        messaging_started: "🚀 स्वचालित मैसेजिंग शुरू!\n\n⏱️ वर्तमान देरी: {delay}s (+ रैंडम जिटर)\n🛡️ एंटी-बैन: सक्षम (टाइपिंग सिमुलेशन और रैंडमाइज्ड टाइमिंग)",
        messaging_stopped: "🛑 आपकी मैसेजिंग (मैनुअल और शेड्यूल) रोक दी गई है। आपके सत्र सक्रिय रहेंगे।",
        need_2_acc: "❌ आपको टू-वे मैसेजिंग के लिए कम से कम 2 कनेक्टेड खातों की आवश्यकता है।",
        back_to_menu_msg: "👋 मुख्य मेनू में आपका स्वागत है!",
        schedule_set: "✅ शेड्यूल सेट: *{range}* (IST).\n\nबॉट अब इस शेड्यूल का पालन करेगा।",
        schedule_instruction: "📅 *मैसेजिंग शेड्यूल सेट करें (IST)*\n\nकृपया 24-घंटे के प्रारूप में समय सीमा दर्ज करें (जैसे, `09:00-18:00`)।",
        back_btn: "🔙 वापस",
        acc_disconnected: "⚠️ *खाता डिस्कनेक्ट हुआ:* {phoneNumber}\n\nयह खाता लॉग आउट हो गया है या सत्र समाप्त हो गया है।",
        acc_connected: "✅ WhatsApp खाता {phoneNumber} सफलतापूर्वक कनेक्ट हो गया!",
        pairing_code_msg: "🔑 *{phoneNumber} के लिए पेयरिंग कोड:*\n\n👉 `{code}`\n\n*⚠️ महत्वपूर्ण:* आपको पुश नोटिफिकेशन नहीं मिलेगा। आपको यह कोड मैन्युअल रूप से दर्ज करना होगा।",
        rate_limit: "❌ *रेट लिमिट:* WhatsApp {phoneNumber} के लिए पेयरing अनुरोधों को अस्थायी रूप से ब्लॉक कर रहा है।",
        pairing_failed: "❌ {phoneNumber} के लिए पेयरिंग कोड प्राप्त करने में विफल: {error}",
        access_expired: "⚠️ *एक्सेस समाप्त*\n\nआपका एक्सेस समाप्त हो गया है।",
        choose_lang_title: "🌐 *अपनी भाषा चुनें:*",
        lang_updated: "✅ *भाषा अपडेट की गई!*\n\n{welcome}",
        cleaning_up: "सफाई हो रही है...",
        preparing_backup: "📦 डेटाबेस बैकअप तैयार किया जा रहा है...",
        db_not_found: "❌ डेटाबेस फ़ाइल नहीं मिली।",
        restore_error_no_file: "❌ *रीस्टोर त्रुटि:* कोई लंबित फ़ाइल नहीं मिली।",
        downloading_backup: "🔄 *बैकअप डाउनलोड हो रहा है...*",
        restore_failed_invalid: "❌ *रीस्टोर विफल:* फ़ाइल अमान्य है।",
        restore_warning_0_users: "⚠️ *चेतावनी:* बैकअप में 0 उपयोगकर्ता हैं।",
        restore_success: "✅ *डेटाबेस सफलतापूर्वक रीस्टोर किया गया!*\n\n{count} उपयोगकर्ता रीस्टोर किए गए।",
        bot_reloaded: "✨ *बॉट सफलतापूर्वक रीलोड हुआ!*",
        restore_failed: "❌ *रीस्टोर विफल:* {error}",
        restore_cancelled: "❌ रीस्टोर रद्द कर दिया गया।",
        db_restore_detected: "⚠️ *डेटाबेस रीस्टोर का पता चला*",
        doc_received: "❓ दस्तावेज़ प्राप्त हुआ: `{name}`",
        doc_error: "❌ दस्तावेज़ त्रुटि: {error}",
        db_restore_required: "⚠️ *डेटाबेस रीस्टोर आवश्यक है*",
        help_msg: "📖 *सहायता और कमांड*",
        user_not_found: "उपयोगकर्ता नहीं मिला।",
        referral_program_title: "👥 *रेफरल प्रोग्राम*\n\nआपके रेफरल: `{count}`",
        claim_success: "🎉 *सफलता!*\n\nनया एक्सपायरी: `{expiry}`",
        claim_failed: "❌ *दावा विफल*\n\nआपके पास `{count}` रेफरल हैं।",
        no_acc_linked: "अभी तक कोई खाता लिंक नहीं किया गया है।",
        acc_status_msg: "📱 *खाता:* {phoneNumber}\n*स्थिति:* {status}",
        broadcast_started: "📢 *ब्रॉडकास्ट शुरू...*",
        broadcast_completed: "✅ *ब्रॉडकास्ट पूरा हुआ*",
        manual_restore_started: "🔄 *मैनुअल रीस्टोर शुरू...*",
        manual_restore_complete: "✅ *रीस्टोर पूरा हुआ!*",
        manual_restore_failed: "❌ *रीस्टोर विफल:* {error}",
        joined_channels: "👋 स्वागत है! आप आवश्यक चैनलों में शामिल हो गए हैं।",
        admin_restore_btn: "⚠️ डेटाबेस रीस्टोर करें",
        admin_panel_btn: "🔑 एडमिन पैनल",
        bot_status_title: "📊 *आपकी बॉट स्थिति*",
        user_label: "👤 *उपयोगकर्ता:*",
        bot_state_label: "🤖 *बॉट स्थिति:*",
        access_label: "🔑 *एक्सेस:*",
        stats_label: "📈 *सांख्यिकी:*",
        msgs_sent_label: "• भेजे गए संदेश:",
        referrals_label: "• रेफरल:",
        wa_accounts_label: "📱 *WhatsApp खाते:*",
        acc_list_label: "*खाता सूची:*",
        running_status: "🟢 चल रहा है",
        stopped_status: "🔴 रुका हुआ",
        access_expiry_label: "• एक्सेस समाप्ति:",
        ref_link_label: "🔗 *आपका रेफरल लिंक:*",
        no_active_access: "कोई सक्रिय एक्सेस नहीं",
        referral_milestone_msg: "🎉 *बधाई हो!*\n\nआपने 10 उपयोगकर्ताओं को रेफर किया है। आपका एक्सेस 15 दिनों के लिए बढ़ा दिया गया है।\n\nनई समाप्ति: `{expiry}`",
    },
    id: {
        welcome: "👋 *Selamat datang di WhatsApp Linker!* \n\nBot ini membantu Anda menjaga akun WhatsApp tetap aktif dan aman dari pemblokiran dengan membuat loop pesan alami antar akun Anda sendiri. \n\n🚀 *Manfaat:*\n• Membuat riwayat akun yang kuat\n• Mengurangi risiko pemblokiran secara signifikan\n• Tambahkan pengguna internasional setelah 1 minggu\n• Operasi 24/7 otomatis penuh",
        choose_lang: "Silakan pilih bahasa Anda:",
        main_menu: "👋 Selamat datang kembali! Gunakan tombol di bawah untuk mengelola akun Anda.",
        add_acc: "📱 Tambah Akun",
        list_acc: "📋 Daftar Akun",
        start_msg: "🚀 Mulai Pesan",
        stop_msg: "🛑 Berhenti",
        schedule: "📅 Jadwal",
        delay: "⏳ Atur Jeda",
        status: "📊 Status",
        referral: "👥 Program Referal",
        logout: "🚪 Keluar Semua",
        admin: "🔑 Panel Admin",
        lang_btn: "🌐 Ubah Bahasa",
        force_sub: "⚠️ *Bergabunglah dengan saluran kami untuk melanjutkan!*",
        verify_btn: "✅ Verifikasi Langganan",
        contact_owner: "👤 Hubungi Pemilik",
        ref_link: "👥 Link Referal Saya",
        claim_btn: "🎁 Klaim Akses 15 Hari",
        access_denied: "🚫 *Akses Ditolak*\n\nAnda tidak memiliki akses. Referensikan 10 pengguna atau hubungi admin.",
        pairing_instruction: "Silakan kirim nomor telepon (dengan kode negara, misal 628123456789):",
        delay_instruction: "Masukkan jeda dalam detik (Disarankan: 200-300 detik):",
        bot_disabled: "⚠️ *Bot Dinonaktifkan*\n\nBot saat ini dinonaktifkan oleh pemilik untuk pemeliharaan.",
        acc_limit_reached: "❌ Anda telah mencapai batas akun sebanyak {limit} akun.",
        pairing_fresh: "⏳ Meminta kode pairing baru untuk {phoneNumber}...",
        delay_updated: "✅ Jeda diperbarui menjadi {delay} detik. Ini akan berlaku mulai pesan berikutnya.",
        min_delay: "❌ Jeda terlalu singkat! Minimal 10 detik untuk keamanan.",
        acc_deleted: "🗑️ Akun {phoneNumber} telah dihapus dan sesi dibersihkan.",
        all_logged_out: "👋 Semua akun telah keluar dan sesi dibersihkan.",
        messaging_started: "🚀 Pesan otomatis dimulai!\n\n⏱️ Jeda Saat Ini: {delay}s (+ jitter acak)\n🛡️ Anti-ban: Aktif (Simulasi pengetikan & waktu acak)",
        messaging_stopped: "🛑 Pesan Anda (manual dan terjadwal) telah dihentikan. Sesi Anda tetap aktif.",
        need_2_acc: "❌ Anda memerlukan setidaknya 2 akun yang terhubung untuk pesan dua arah.",
        back_to_menu_msg: "👋 Selamat datang kembali di menu utama!",
        schedule_set: "✅ Jadwal diatur: *{range}* (WIB).\n\nBot akan mengikuti jadwal ini secara otomatis.",
        schedule_instruction: "📅 *Atur Jadwal Pesan (WIB)*\n\nSilakan masukkan rentang waktu dalam format 24 jam (misal, `09:00-18:00`).",
        back_btn: "🔙 Kembali",
        acc_disconnected: "⚠️ *Akun Terputus:* {phoneNumber}\n\nAkun ini telah keluar atau sesi telah kedaluwarsa.",
        acc_connected: "✅ Akun WhatsApp {phoneNumber} berhasil terhubung!",
        pairing_code_msg: "🔑 *Kode Pairing untuk {phoneNumber}:*\n\n👉 `{code}`\n\n*⚠️ PENTING:* Anda TIDAK akan menerima notifikasi push. Anda harus memasukkan kode ini secara manual.",
        rate_limit: "❌ *Batas Kecepatan:* WhatsApp memblokir permintaan pairing untuk {phoneNumber} sementara.",
        pairing_failed: "❌ Gagal mendapatkan kode pairing untuk {phoneNumber}: {error}",
        access_expired: "⚠️ *Akses Kedaluwarsa*\n\nAkses Anda telah berakhir.",
        choose_lang_title: "🌐 *Pilih Bahasa Anda:*",
        lang_updated: "✅ *Bahasa Diperbarui!*\n\n{welcome}",
        cleaning_up: "Membersihkan...",
        preparing_backup: "📦 Menyiapkan cadangan database...",
        db_not_found: "❌ File database tidak ditemukan.",
        restore_error_no_file: "❌ *Kesalahan Restore:* Tidak ada file tertunda.",
        downloading_backup: "🔄 *Mengunduh cadangan...*",
        restore_failed_invalid: "❌ *Restore Gagal:* File tidak valid.",
        restore_warning_0_users: "⚠️ *Peringatan:* Cadangan berisi 0 pengguna.",
        restore_success: "✅ *Database Berhasil Dipulihkan!*\n\n{count} pengguna dipulihkan.",
        bot_reloaded: "✨ *Bot Berhasil Dimuat Ulang!*",
        restore_failed: "❌ *Restore Gagal:* {error}",
        restore_cancelled: "❌ Restore dibatalkan.",
        db_restore_detected: "⚠️ *Restore Database Terdeteksi*",
        doc_received: "❓ Dokumen diterima: `{name}`",
        doc_error: "❌ Kesalahan dokumen: {error}",
        db_restore_required: "⚠️ *Restore Database Diperlukan*",
        help_msg: "📖 *Bantuan & Perintah*",
        user_not_found: "Pengguna tidak ditemukan.",
        referral_program_title: "👥 *Program Referal*\n\nReferal Anda: `{count}`",
        claim_success: "🎉 *Berhasil!*\n\nKedaluwarsa Baru: `{expiry}`",
        claim_failed: "❌ *Klaim Gagal*\n\nAnda memiliki `{count}` referal.",
        no_acc_linked: "Belum ada akun yang ditautkan.",
        acc_status_msg: "📱 *Akun:* {phoneNumber}\n*Status:* {status}",
        broadcast_started: "📢 *Memulai Siaran...*",
        broadcast_completed: "✅ *Siaran Selesai*",
        manual_restore_started: "🔄 *Memulai Pemulihan Manual...*",
        manual_restore_complete: "✅ *Pemulihan Selesai!*",
        manual_restore_failed: "❌ *Pemulihan Gagal:* {error}",
        joined_channels: "👋 Selamat datang! Anda telah berhasil bergabung dengan saluran yang diperlukan.",
        admin_restore_btn: "⚠️ PULIHKAN DATABASE",
        admin_panel_btn: "🔑 Panel Admin",
        bot_status_title: "📊 *Status Bot Anda*",
        user_label: "👤 *Pengguna:*",
        bot_state_label: "🤖 *Status Bot:*",
        access_label: "🔑 *Akses:*",
        stats_label: "📈 *Statistik:*",
        msgs_sent_label: "• Pesan Terkirim:",
        referrals_label: "• Referal:",
        wa_accounts_label: "📱 *Akun WhatsApp:*",
        acc_list_label: "*Daftar Akun:*",
        running_status: "🟢 Berjalan",
        stopped_status: "🔴 Berhenti",
        access_expiry_label: "• Kedaluwarsa Akses:",
        ref_link_label: "🔗 *Link Referal Anda:*",
        no_active_access: "Tidak ada akses aktif",
        referral_milestone_msg: "🎉 *Selamat!*\n\nAnda telah mereferensikan 10 pengguna. Akses Anda telah diperpanjang selama 15 hari.\n\nKedaluwarsa Baru: `{expiry}`",
    },
    zh: {
        welcome: "👋 *欢迎使用 WhatsApp Linker!* \n\n此机器人通过在您自己的账户之间创建自然的消息循环，帮助您保持 WhatsApp 账户活跃并防止被封禁。 \n\n🚀 *优势:*\n• 创建强大的账户历史记录\n• 显著降低封禁风险\n• 1周后可添加国际用户\n• 全天候 24/7 自动运行",
        choose_lang: "请选择您的语言：",
        main_menu: "👋 欢迎回来！使用下面的按钮管理您的账户。",
        add_acc: "📱 添加账户",
        list_acc: "📋 账户列表",
        start_msg: "🚀 开始发送",
        stop_msg: "🛑 停止",
        schedule: "📅 定时",
        delay: "⏳ 设置延迟",
        status: "📊 状态",
        referral: "👥 推荐计划",
        logout: "🚪 全部退出",
        admin: "🔑 管理面板",
        lang_btn: "🌐 更改语言",
        force_sub: "⚠️ *请加入我们的频道以继续使用！*",
        verify_btn: "✅ 验证订阅",
        contact_owner: "👤 联系所有者",
        ref_link: "👥 我的推荐链接",
        claim_btn: "🎁 领取 15 天访问权限",
        access_denied: "🚫 *访问被拒绝*\n\n您没有访问权限。推荐 10 位用户或联系管理员。",
        pairing_instruction: "请发送电话号码（带国家代码，例如 8613812345678）：",
        delay_instruction: "输入延迟秒数（建议：200-300秒）：",
        bot_disabled: "⚠️ *机器人已禁用*\n\n机器人目前已被所有者禁用以进行维护。",
        acc_limit_reached: "❌ 您已达到 {limit} 个账户的限制。",
        pairing_fresh: "⏳ 正在为 {phoneNumber} 请求新的配对码...",
        delay_updated: "✅ 延迟已更新为 {delay} 秒。这将从下一条消息开始生效。",
        min_delay: "❌ 延迟太短！为了安全，至少需要 10 秒。",
        acc_deleted: "账户 {phoneNumber} 已删除，会话已清除。",
        all_logged_out: "👋 所有账户已退出，会话已清除。",
        messaging_started: "🚀 自动消息已开始！\n\n⏱️ 当前延迟：{delay}秒（+ 随机抖动）\n🛡️ 反封禁：已启用（打字模拟和随机时间）",
        messaging_stopped: "🛑 您的消息发送（手动和定时）已停止。您的会话保持活跃。",
        need_2_acc: "❌ 您至少需要 2 个已连接的账户才能进行双向消息发送。",
        back_to_menu_msg: "👋 欢迎回到主菜单！",
        schedule_set: "✅ 定时设置成功：*{range}* (CST)。\n\n机器人现在将自动遵循此定时。",
        schedule_instruction: "📅 *设置消息定时 (CST)*\n\n请输入 24 小时格式的时间范围（例如，`09:00-18:00`）。",
        back_btn: "🔙 返回",
        acc_disconnected: "⚠️ *账户已断开：* {phoneNumber}\n\n此账户已退出或会话已过期。",
        acc_connected: "✅ WhatsApp 账户 {phoneNumber} 连接成功！",
        pairing_code_msg: "🔑 *{phoneNumber} 的配对码：*\n\n👉 `{code}`\n\n*⚠️ 重要提示：* 您不会收到推送通知。您必须手动输入此代码。",
        rate_limit: "❌ *速率限制：* WhatsApp 暂时阻止了 {phoneNumber} 的配对请求。",
        pairing_failed: "❌ 无法获取 {phoneNumber} 的配对码：{error}",
        access_expired: "⚠️ *访问过期*\n\n您的访问权限已过期。",
        choose_lang_title: "🌐 *选择您的语言：*",
        lang_updated: "✅ *语言已更新！*\n\n{welcome}",
        cleaning_up: "正在清理...",
        preparing_backup: "📦 正在准备数据库备份...",
        db_not_found: "❌ 找不到数据库文件。",
        restore_error_no_file: "❌ *恢复错误：* 找不到待处理文件。",
        downloading_backup: "🔄 *正在下载备份...*",
        restore_failed_invalid: "❌ *恢复失败：* 文件无效。",
        restore_warning_0_users: "⚠️ *警告：* 备份包含 0 个用户。",
        restore_success: "✅ *数据库恢复成功！*\n\n已恢复 {count} 个用户。",
        bot_reloaded: "✨ *机器人重载成功！*",
        restore_failed: "❌ *恢复失败：* {error}",
        restore_cancelled: "❌ 恢复已取消。",
        db_restore_detected: "⚠️ *检测到数据库恢复*",
        doc_received: "❓ 收到文档：`{name}`",
        doc_error: "❌ 文档错误：{error}",
        db_restore_required: "⚠️ *需要恢复数据库*",
        help_msg: "📖 *帮助与命令*",
        user_not_found: "找不到用户。",
        referral_program_title: "👥 *推荐计划*\n\n您的推荐数：`{count}`",
        claim_success: "🎉 *成功！*\n\n新过期时间：`{expiry}`",
        claim_failed: "❌ *领取失败*\n\n您目前有 `{count}` 个推荐。",
        no_acc_linked: "尚未绑定账户。",
        acc_status_msg: "📱 *账户：* {phoneNumber}\n*状态：* {status}",
        broadcast_started: "📢 *开始广播...*",
        broadcast_completed: "✅ *广播完成*",
        manual_restore_started: "🔄 *开始手动恢复...*",
        manual_restore_complete: "✅ *恢复完成！*",
        manual_restore_failed: "❌ *恢复失败：* {error}",
        joined_channels: "👋 欢迎！您已成功加入所需频道。",
        admin_restore_btn: "⚠️ 恢复数据库",
        admin_panel_btn: "🔑 管理面板",
        bot_status_title: "📊 *您的机器人状态*",
        user_label: "👤 *用户:*",
        bot_state_label: "🤖 *机器人状态:*",
        access_label: "🔑 *访问权限:*",
        stats_label: "📈 *统计数据:*",
        msgs_sent_label: "• 已发送消息:",
        referrals_label: "• 推荐数:",
        wa_accounts_label: "📱 *WhatsApp 账户:*",
        acc_list_label: "*账户列表:*",
        running_status: "🟢 运行中",
        stopped_status: "🔴 已停止",
        access_expiry_label: "• 访问过期时间:",
        ref_link_label: "🔗 *您的推荐链接:*",
        no_active_access: "无活跃访问权限",
        referral_milestone_msg: "🎉 *恭喜！*\n\n您已推荐 10 位用户。您的访问权限已延长 15 天。\n\n新过期时间：`{expiry}`",
    }
};

const randomMessages: any = {
    en: [
        "Hello, how are you?", "Just checking in!", "Hope you're having a great day.", "What's up?", "Nice to chat with you.", "Have a good one!", "Talk to you later.", "Everything okay?", "Good morning!", "Good evening!",
        "Hey, how are you doing today?",
        "Just checking in, hope everything is well!",
        "What's the plan for the weekend?",
        "Have you seen that new movie everyone is talking about?",
        "I was just thinking about our last conversation.",
        "Do you have any recommendations for a good book?",
        "The weather has been quite nice lately, hasn't it?",
        "I'm looking for some new music, any suggestions?",
        "Hope you're having a productive week!",
        "It's been a while, let's catch up soon.",
        "Did you hear the news today?",
        "I'm trying out a new recipe tonight, wish me luck!",
        "What's your favorite way to relax after a long day?",
        "I just saw something that reminded me of you.",
        "Are you still working on that project we discussed?",
        "I'm planning a trip, any travel tips?",
        "Just wanted to say hi and wish you a great day!",
        "How's your family doing?",
        "I'm thinking of starting a new hobby, any ideas?",
        "That was a great meeting we had earlier.",
        "I'm so glad we're in touch.",
        "Let me know if you need anything.",
        "Talk to you later!",
        "Take care!",
        "Best regards,",
        "Cheers!",
        "See you soon.",
        "Looking forward to it.",
        "Have a wonderful evening.",
        "Good morning! Hope you slept well.",
        "How was your day?",
        "Any exciting news?",
        "I'm here if you want to chat.",
        "Stay safe and healthy!",
        "Sending you positive vibes.",
        "You've got this!",
        "Keep up the great work.",
        "I'm proud of you.",
        "Thanks for being such a good friend.",
        "I appreciate your help.",
        "Let's grab coffee sometime.",
        "Dinner next week?",
        "Can't wait to see you.",
        "Missing our chats.",
        "Thinking of you.",
        "Hope this message finds you well.",
        "Just a quick note to say hello.",
        "Wishing you all the best.",
        "Have a fantastic day ahead!"
    ],
    hi: ["नमस्ते, आप कैसे हैं?", "बस हाल-चाल पूछ रहा था!", "आशा है कि आपका दिन अच्छा बीत रहा होगा।", "क्या चल रहा है?", "आपसे बात करके अच्छा लगा।", "आपका दिन शुभ हो!", "बाद में बात करते हैं।", "सब ठीक है?", "शुभ प्रभात!", "शुभ संध्या!"],
    id: ["Halo, apa kabar?", "Hanya menyapa!", "Semoga hari Anda menyenangkan.", "Ada apa?", "Senang mengobrol dengan Anda.", "Semoga harimu menyenangkan!", "Sampai nanti.", "Semuanya baik-baik saja?", "Selamat pagi!", "Selamat malam!"],
    zh: ["你好，你好吗？", "只是来看看！", "希望你今天过得愉快。", "最近怎么样？", "很高兴能和你聊天。", "祝你过得愉快！", "晚点再聊。", "一切都好吗？", "早上好！", "晚上好！"]
};
const sessions = new Map<string, any>();
const connectingSessions = new Set<string>();

/**
 * Custom authentication state that stores data in the database.
 * This ensures sessions persist across ephemeral filesystem wipes on Render.
 */
async function useDbAuthState(sessionId: string) {
    const writeData = async (data: any, key: string) => {
        const json = JSON.stringify(data, BufferJSON.replacer);
        await db.prepare("INSERT OR REPLACE INTO whatsapp_auth (session_id, key_id, data) VALUES (?, ?, ?)").run(sessionId, key, json);
    };

    const readData = async (key: string) => {
        const row = await db.prepare("SELECT data FROM whatsapp_auth WHERE session_id = ? AND key_id = ?").get(sessionId, key) as any;
        return row ? JSON.parse(row.data, BufferJSON.reviver) : null;
    };

    const removeData = async (key: string) => {
        await db.prepare("DELETE FROM whatsapp_auth WHERE session_id = ? AND key_id = ?").run(sessionId, key);
    };

    const creds = await readData('creds') || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type: string, ids: string[]) => {
                    const data: { [key: string]: any } = {};
                    await Promise.all(
                        ids.map(async (id) => {
                            let value = await readData(`${type}-${id}`);
                            if (type === 'app-state-sync-key' && value) {
                                value = proto.Message.AppStateSyncKeyData.fromObject(value);
                            }
                            data[id] = value;
                        })
                    );
                    return data;
                },
                set: async (data: any) => {
                    for (const type in data) {
                        for (const id in data[type]) {
                            const value = data[type][id];
                            const key = `${type}-${id}`;
                            if (value) {
                                await writeData(value, key);
                            } else {
                                await removeData(key);
                            }
                        }
                    }
                }
            }
        },
        saveCreds: () => writeData(creds, 'creds')
    };
}

async function connectToWhatsApp(telegramId: string, phoneNumber: string, bot: Telegraf<Context>) {
    const cleanNumber = phoneNumber.replace(/\D/g, '');
    const sessionId = `session_${telegramId}_${cleanNumber}`;
    
    if (connectingSessions.has(sessionId)) {
        console.log(`[Session ${sessionId}] Connection already in progress, skipping...`);
        return sessions.get(sessionId);
    }
    
    connectingSessions.add(sessionId);
    
    try {
        // Use database-backed auth state for persistence
        const { state, saveCreds } = await useDbAuthState(sessionId);
        const { version } = await fetchLatestBaileysVersion();
        console.log(`Connecting session ${sessionId} using WA v${version.join('.')}`);

        const sock = makeWASocket({
            version,
            printQRInTerminal: false,
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, logger),
            },
            logger,
            browser: ["Windows", "Chrome", "122.0.6261.112"],
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: 60000,
            keepAliveIntervalMs: 30000,
            generateHighQualityLinkPreview: false,
            syncFullHistory: false,
            markOnlineOnConnect: true,
            shouldIgnoreJid: (jid) => isJidBroadcast(jid),
        });

        sessions.set(sessionId, sock);

        sock.ev.on('creds.update', async () => {
            await saveCreds();
        });

        // Anti-ban: Automatically read incoming messages
        sock.ev.on('messages.upsert', async (m) => {
            if (m.type === 'notify') {
                for (const msg of m.messages) {
                    if (!msg.key.fromMe && msg.key.remoteJid) {
                        try {
                            // Random delay before reading (2-5 seconds)
                            setTimeout(async () => {
                                try {
                                    // Check if socket is still connected
                                    if (sessions.has(sessionId)) {
                                        await sock.readMessages([msg.key]);
                                    }
                                } catch (e) {
                                    console.error(`[Session ${sessionId}] Error reading message:`, e.message);
                                }
                            }, 2000 + Math.random() * 3000);
                        } catch (e) {}
                    }
                }
            }
        });

        let pairingCodeRequested = false;

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect } = update;
            const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
            const lang = user?.language || 'en';
            const t = translations[lang] || translations.en;
            
            if (connection === 'close') {
                const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut && statusCode !== 401;
                
                console.log(`Connection closed for ${phoneNumber}. Status: ${statusCode}. Reconnecting: ${shouldReconnect}`);
                
                // Mark as disconnected in DB for status view
                await db.prepare("UPDATE whatsapp_sessions SET is_connected = 0 WHERE session_id = ?").run(sessionId);

                if (shouldReconnect) {
                    setTimeout(() => connectToWhatsApp(telegramId, phoneNumber, bot), 5000);
                } else {
                    // Permanent disconnect (Logout or Banned)
                    await db.prepare("DELETE FROM whatsapp_sessions WHERE session_id = ?").run(sessionId);
                    await db.prepare("DELETE FROM whatsapp_auth WHERE session_id = ?").run(sessionId);
                    sessions.delete(sessionId);
                    
                    const authPath = path.join(SESSIONS_DIR, sessionId);
                    if (fs.existsSync(authPath)) {
                        fs.rmSync(authPath, { recursive: true, force: true });
                    }

                    await bot.telegram.sendMessage(telegramId, t.acc_disconnected.replace('{phoneNumber}', phoneNumber), { parse_mode: 'Markdown' });
                }
            } else if (connection === 'open') {
                console.log(`Opened connection for ${phoneNumber}`);
                const session = await db.prepare("SELECT is_new_link FROM whatsapp_sessions WHERE session_id = ?").get(sessionId) as any;
                if (session && session.is_new_link === 1) {
                    await bot.telegram.sendMessage(telegramId, t.acc_connected.replace('{phoneNumber}', phoneNumber));
                    await db.prepare("UPDATE whatsapp_sessions SET is_new_link = 0 WHERE session_id = ?").run(sessionId);
                }
                await db.prepare("UPDATE whatsapp_sessions SET is_connected = 1 WHERE session_id = ?").run(sessionId);
            }
        });

        // Handle pairing code if not connected
        if (!sock.authState.creds.registered && !pairingCodeRequested) {
            pairingCodeRequested = true;
            
            const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
            const lang = user?.language || 'en';
            const t = translations[lang] || translations.en;

            // Wait for a bit to ensure socket is ready
            setTimeout(async () => {
                try {
                    // Double check registration status and session validity
                    if (sock.authState.creds.registered || sessions.get(sessionId) !== sock) return;

                    const num = phoneNumber.replace(/\D/g, '');
                    console.log(`[Session ${sessionId}] Requesting pairing code for ${num}...`);
                    
                    const code = await sock.requestPairingCode(num);
                    
                    await bot.telegram.sendMessage(telegramId, 
                        t.pairing_code_msg.replace('{phoneNumber}', phoneNumber).replace(/{code}/g, code), 
                        { parse_mode: 'Markdown' }
                    );
                } catch (err: any) {
                    console.error(`[Session ${sessionId}] Error requesting pairing code:`, err.message || err);
                    pairingCodeRequested = false; 
                    
                    // If it's a rate limit error or similar, inform the user
                    const errorMsg = err.message || 'Unknown error';
                    if (errorMsg.includes('rate-overlimit')) {
                        await bot.telegram.sendMessage(telegramId, t.rate_limit.replace('{phoneNumber}', phoneNumber));
                    } else {
                        await bot.telegram.sendMessage(telegramId, t.pairing_failed.replace('{phoneNumber}', phoneNumber).replace('{error}', errorMsg));
                    }
                }
            }, 6000); // 6s is usually enough for the socket to be ready for pairing
        }

        return sock;
    } finally {
        connectingSessions.delete(sessionId);
    }
}

/**
 * Reconnects all WhatsApp sessions stored in the database.
 * This ensures sessions persist across server restarts.
 */
async function reconnectAllSessions(bot: Telegraf<Context>) {
    try {
        const sessionsInDb = await db.prepare("SELECT * FROM whatsapp_sessions").all() as any[];
        console.log(`[Startup] Found ${sessionsInDb.length} sessions in database.`);
        
        if (sessionsInDb.length === 0) {
            console.log("[Startup] No WhatsApp sessions to reconnect.");
            return;
        }
        
        console.log(`🔄 [Startup] Reconnecting ${sessionsInDb.length} WhatsApp sessions...`);
        for (const session of sessionsInDb) {
            try {
                const authPath = path.join(SESSIONS_DIR, session.session_id);
                const exists = fs.existsSync(authPath);
                console.log(`[Startup] Reconnecting ${session.phone_number} (${session.session_id}). Auth path exists: ${exists}`);
                
                // We don't want to trigger "New connection" messages on restart
                await db.prepare("UPDATE whatsapp_sessions SET is_new_link = 0 WHERE session_id = ?").run(session.session_id);
                
                await connectToWhatsApp(session.telegram_id, session.phone_number, bot);
                // Increased delay between reconnections to 10s to avoid WhatsApp spam detection
                await new Promise(resolve => setTimeout(resolve, 10000));
            } catch (err) {
                console.error(`❌ [Startup] Failed to reconnect session ${session.session_id}:`, err);
            }
        }
        console.log("✅ [Startup] All existing sessions have been queued for reconnection.");
    } catch (err) {
        console.error("❌ [Startup] Error during session reconnection:", err);
    }
}

// --- Telegram Bot Logic ---
// botToken is already declared at the top level
const bot = new Telegraf(botToken || "DUMMY_TOKEN");

// Global error handler
bot.catch((err: any, ctx) => {
    console.error(`Ooops, encountered an error for ${ctx.updateType}`, err.message || err);
});

// Helper to check force sub
async function checkForceSub(ctx: Context, telegramId: string): Promise<boolean> {
    const channels = await db.prepare("SELECT * FROM force_sub_channels").all() as any[];
    if (channels.length === 0) return true;

    for (const channel of channels) {
        try {
            const member = await ctx.telegram.getChatMember(channel.channel_id, Number(telegramId));
            if (['left', 'kicked', 'restricted'].includes(member.status)) {
                return false;
            }
        } catch (e: any) {
            console.error(`[ForceSub] Error checking channel ${channel.channel_id}:`, e.message);
            // If we can't verify (e.g. bot not admin), we must NOT return true
            // as that would be "automatically verifying".
            return false;
        }
    }
    return true;
}

// Middleware to ensure user exists in DB and check authorization
bot.use(async (ctx, next) => {
    if (ctx.from) {
        const telegramId = ctx.from.id.toString();
        
        // Admin Debug Logging
        if (telegramId === OWNER_ID && ctx.message) {
            console.log(`[Admin Debug] Message Type: ${Object.keys(ctx.message).join(', ')}`);
            if ('document' in ctx.message) {
                console.log(`[Admin Debug] Document: ${ctx.message.document.file_name} (${ctx.message.document.mime_type})`);
            }
        }
        
        let user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
        
        if (!user) {
            // Check if this is the owner
            const isAdmin = (telegramId === OWNER_ID) ? 1 : 0;
            const isAuth = (telegramId === OWNER_ID) ? 1 : 0;
            
            await db.prepare("INSERT INTO users (telegram_id, is_admin, is_authorized) VALUES (?, ?, ?)").run(telegramId, isAdmin, isAuth);
            user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId);
        } else if (telegramId === OWNER_ID && !user.is_admin) {
            // Ensure owner is always admin
            await db.prepare("UPDATE users SET is_admin = 1, is_authorized = 1 WHERE telegram_id = ?").run(telegramId);
            user.is_admin = 1;
            user.is_authorized = 1;
        }

        // Handle referral in middleware to capture it early
        if (ctx.message && 'text' in ctx.message && ctx.message.text.startsWith('/start ')) {
            const startPayload = ctx.message.text.split(' ')[1];
            if (startPayload && startPayload.startsWith('ref_') && user && !user.referred_by && startPayload.replace('ref_', '') !== telegramId) {
                const referrerId = startPayload.replace('ref_', '');
                const referrer = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(referrerId) as any;
                
                if (referrer) {
                    await db.prepare("UPDATE users SET referred_by = ? WHERE telegram_id = ?").run(referrerId, telegramId);
                    const newCount = (referrer.referral_count || 0) + 1;
                    await db.prepare("UPDATE users SET referral_count = ? WHERE telegram_id = ?").run(newCount, referrerId);
                    
                    if (newCount % 10 === 0) {
                        let currentExpiry = referrer.access_expiry ? new Date(referrer.access_expiry) : new Date();
                        if (currentExpiry < new Date()) currentExpiry = new Date();
                        currentExpiry.setDate(currentExpiry.getDate() + 15);
                        const expiryStr = currentExpiry.toISOString();
                        await db.prepare("UPDATE users SET is_authorized = 1, access_expiry = ? WHERE telegram_id = ?").run(expiryStr, referrerId);
                        try {
                            const referrerLang = referrer.language || 'en';
                            const rt = translations[referrerLang] || translations.en;
                            await ctx.telegram.sendMessage(referrerId, rt.referral_milestone_msg.replace('{expiry}', expiryStr.split('T')[0]), { parse_mode: 'Markdown' });
                        } catch (e) {}
                    }
                }
            }
        }

        const lang = user.language || 'en';
        const t = translations[lang] || translations.en;

        // Bot Active Check
        const botActiveSetting = await db.prepare("SELECT value FROM settings WHERE key = 'bot_active'").get() as any;
        const isBotActive = botActiveSetting?.value === "1";
        
        if (!isBotActive && !user.is_admin) {
            return ctx.reply(t.bot_disabled, { parse_mode: 'Markdown' });
        }

        // Allow admins to use everything
        if (user.is_admin) return next();

        // 1. Language selection check (only if not already selecting language)
        if (!user.language && !(ctx.callbackQuery && 'data' in ctx.callbackQuery && ctx.callbackQuery.data.startsWith('set_lang_'))) {
            return showLanguageSelection(ctx);
        }

        // 2. Force Sub Check
        const isSubbed = await checkForceSub(ctx, telegramId);
        if (!isSubbed) {
            const channels = await db.prepare("SELECT * FROM force_sub_channels").all() as any[];
            const buttons = [];
            
            for (const c of channels) {
                buttons.push([Markup.button.url(`📢 Join ${c.channel_name || 'Channel'}`, c.invite_link)]);
            }
            buttons.push([Markup.button.callback(t.verify_btn, 'verify_sub')]);

            return ctx.reply(t.force_sub, {
                parse_mode: 'Markdown',
                ...Markup.inlineKeyboard(buttons)
            });
        }

        // 3. Public/Private Mode Check
        const isPublicSetting = await db.prepare("SELECT value FROM settings WHERE key = 'is_public'").get() as any;
        const isPublic = isPublicSetting?.value === "1";

        // Check for access expiry
        if (user.is_authorized && user.access_expiry) {
            const expiry = new Date(user.access_expiry);
            if (expiry < new Date()) {
                await db.prepare("UPDATE users SET is_authorized = 0, access_expiry = NULL WHERE telegram_id = ?").run(telegramId);
                user.is_authorized = 0;
                user.access_expiry = null;
                ctx.reply("⚠️ *Access Expired*\n\nYour access to this bot has expired. Please contact the owner or refer 10 users to get 15 days of access.", { parse_mode: 'Markdown' });
            }
        }

        if (!isPublic && !user.is_authorized) {
            // Allow /start and basic actions for unauthorized users
            if (ctx.message && 'text' in ctx.message) {
                const text = ctx.message.text;
                if (text === '/start') return next();
            }
            
            if (ctx.callbackQuery && 'data' in ctx.callbackQuery) {
                const data = ctx.callbackQuery.data;
                const allowedActions = ['claim_access', 'show_referral', 'referral_program', 'show_status', 'set_lang_en', 'set_lang_hi', 'set_lang_id', 'set_lang_zh', 'change_language'];
                if (allowedActions.some(a => data.startsWith(a))) return next();
            }
            
            return ctx.reply(t.access_denied + `\n\nYour Telegram ID: \`${telegramId}\`\nReferrals: \`${user.referral_count || 0}/10\``, {
                parse_mode: 'Markdown',
                ...Markup.inlineKeyboard([
                    [Markup.button.url(t.contact_owner, `https://t.me/${OWNER_USERNAME.replace('@', '')}`)],
                    [Markup.button.callback(t.ref_link, 'show_referral')],
                    [Markup.button.callback(t.claim_btn, 'claim_access')]
                ])
            });
        }
    }
    return next();
});

function showLanguageSelection(ctx: Context) {
    // We use English as default for the selection message itself since we don't know the language yet
    const t = translations.en;
    const text = t.choose_lang_title;
    const keyboard = Markup.inlineKeyboard([
        [Markup.button.callback('English 🇺🇸', 'set_lang_en'), Markup.button.callback('Hindi 🇮🇳', 'set_lang_hi')],
        [Markup.button.callback('Indonesian 🇮🇩', 'set_lang_id'), Markup.button.callback('Chinese 🇨🇳', 'set_lang_zh')]
    ]);

    if (ctx.callbackQuery) {
        return ctx.editMessageText(text, { parse_mode: 'Markdown', ...keyboard });
    } else {
        return ctx.reply(text, { parse_mode: 'Markdown', ...keyboard });
    }
}

bot.action(/^set_lang_(.+)$/, async (ctx) => {
    const lang = ctx.match[1];
    const telegramId = ctx.from!.id.toString();
    await db.prepare("UPDATE users SET language = ? WHERE telegram_id = ?").run(lang, telegramId);
    
    const t = translations[lang] || translations.en;
    await ctx.answerCbQuery(`Language set to ${lang.toUpperCase()}`);
    await ctx.editMessageText(t.lang_updated.replace('{welcome}', t.welcome), getMainMenu(telegramId, lang));
});

bot.action('change_language', (ctx) => {
    showLanguageSelection(ctx);
    ctx.answerCbQuery();
});

const getMainMenu = (telegramId: string, lang: string = 'en') => {
    const t = translations[lang] || translations.en;
    const buttons = [
        [Markup.button.callback(t.add_acc, 'add_account'), Markup.button.callback(t.list_acc, 'list_accounts')],
        [Markup.button.callback(t.start_msg, 'start_messaging'), Markup.button.callback(t.stop_msg, 'stop_messaging')],
        [Markup.button.callback(t.schedule, 'set_schedule'), Markup.button.callback(t.delay, 'set_delay')],
        [Markup.button.callback(t.status, 'show_status'), Markup.button.callback(t.referral, 'referral_program')],
        [Markup.button.callback(t.lang_btn, 'change_language')],
        [Markup.button.callback(t.logout, 'logout_all')]
    ];
    if (telegramId === OWNER_ID) {
        if (isFreshDb) {
            buttons.unshift([Markup.button.callback(t.admin_restore_btn, 'admin_restore_info')]);
        }
        buttons.push([Markup.button.callback(t.admin_panel_btn, 'admin_panel')]);
    }
    return Markup.inlineKeyboard(buttons);
};

bot.start(async (ctx) => {
    const telegramId = ctx.from.id.toString();
    const username = ctx.from.username || null;
    const firstName = ctx.from.first_name || 'User';
    
    let user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    
    if (!user) {
        const isAdmin = (telegramId === OWNER_ID) ? 1 : 0;
        const isAuth = (telegramId === OWNER_ID) ? 1 : 0;
        await db.prepare("INSERT INTO users (telegram_id, username, first_name, is_admin, is_authorized) VALUES (?, ?, ?, ?, ?)").run(telegramId, username, firstName, isAdmin, isAuth);
        user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId);
    } else {
        await db.prepare("UPDATE users SET username = ?, first_name = ? WHERE telegram_id = ?").run(username, firstName, telegramId);
    }

    // Notify owner about new user
    if (telegramId !== OWNER_ID) {
        try {
            const escapedUsername = username ? username.replace(/_/g, '\\_').replace(/\*/g, '\\*') : 'N/A';
            const escapedFirstName = firstName.replace(/_/g, '\\_').replace(/\*/g, '\\*');
            const notifyMsg = `🆕 *New User Alert*\n\n` +
                             `👤 Name: ${escapedFirstName}\n` +
                             `🆔 ID: \`${telegramId}\`\n` +
                             `🔗 Username: @${escapedUsername}`;
            await ctx.telegram.sendMessage(OWNER_ID, notifyMsg, { parse_mode: 'Markdown' });
        } catch (e) {
            console.error("Failed to notify owner about new user:", e);
        }
    }

    const lang = user.language;
    if (!lang) {
        return showLanguageSelection(ctx);
    }

    const t = translations[lang] || translations.en;

    await ctx.reply(t.cleaning_up, Markup.removeKeyboard());
    ctx.reply(t.welcome, getMainMenu(telegramId, lang));
});

bot.command('link', async (ctx) => {
    const telegramId = ctx.from.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;

    const text = (ctx.message as any).text;
    const parts = text.split(' ');
    if (parts.length < 2) {
        return ctx.reply("❌ *Usage:* `/link <phone_number>`\nExample: `/link 919876543210`", { parse_mode: 'Markdown' });
    }

    const phoneNumber = parts[1].replace(/\D/g, '');
    if (phoneNumber.length < 10) {
        return ctx.reply("❌ *Invalid phone number.* Please include country code.");
    }

    const sessionId = `session_${telegramId}_${phoneNumber}`;
    const existing = await db.prepare("SELECT * FROM whatsapp_sessions WHERE session_id = ?").get(sessionId);
    
    if (!existing) {
        await db.prepare("INSERT INTO whatsapp_sessions (telegram_id, phone_number, session_id, is_new_link) VALUES (?, ?, ?, 1)").run(telegramId, phoneNumber, sessionId);
    } else {
        await db.prepare("UPDATE whatsapp_sessions SET is_new_link = 1 WHERE session_id = ?").run(sessionId);
    }

    await ctx.reply(`🔄 *Linking ${phoneNumber}...*\n\nI will now generate a pairing code. Please wait...`, { parse_mode: 'Markdown' });
    connectToWhatsApp(telegramId, phoneNumber, bot);
});

bot.command('accounts', async (ctx) => {
    const telegramId = ctx.from.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;

    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ?").all(telegramId) as any[];
    if (accounts.length === 0) {
        return ctx.reply("📭 *No accounts linked.* Use `/link <phone_number>` to add one.", { parse_mode: 'Markdown' });
    }

    let msg = "📱 *Your Linked Accounts:*\n\n";
    accounts.forEach((acc, i) => {
        const status = acc.is_connected ? "✅ Connected" : "❌ Disconnected";
        msg += `${i + 1}. *${acc.phone_number}* - ${status}\n`;
    });

    ctx.reply(msg, { parse_mode: 'Markdown' });
});

bot.command('backup', async (ctx) => {
    const adminId = ctx.from?.id.toString();
    if (adminId !== OWNER_ID) return;
    
    await ctx.reply("📦 Preparing database backup...");
    if (fs.existsSync(dbPath)) {
        await ctx.replyWithDocument({ source: dbPath, filename: `bot_data_backup_${Date.now()}.db` }, {
            caption: "📦 *Database Backup*\n\nKeep this file safe. To restore, simply send this file back to the bot.",
            parse_mode: 'Markdown'
        });
    } else {
        await ctx.reply("❌ Database file not found.");
    }
});

bot.action('confirm_db_restore', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        const fileIdSetting = await db.prepare("SELECT value FROM settings WHERE key = 'pending_restore_file_id'").get() as any;
        const fileId = fileIdSetting?.value;

        if (!fileId) {
            return ctx.reply("❌ *Restore Error:* No pending restore file found. Please send the .db file again.");
        }

        await ctx.answerCbQuery("🔄 Restoring...");
        await ctx.editMessageText("🔄 *Downloading backup...*", { parse_mode: 'Markdown' });

        const fileLink = await ctx.telegram.getFileLink(fileId);
        const response = await fetch(fileLink.href);
        const buffer = await response.arrayBuffer();
        const dbBuffer = Buffer.from(buffer);

        // Validation: Try to open the uploaded buffer as a temporary database to verify it
        const tempDbPath = path.join(DATA_DIR, `temp_restore_${Date.now()}.db`);
        fs.writeFileSync(tempDbPath, dbBuffer);
        
        let backupUserCount = 0;
        let isValid = false;
        try {
            const tempDb = new Database(tempDbPath);
            const countResult = tempDb.prepare("SELECT COUNT(*) as count FROM users").get() as any;
            backupUserCount = countResult?.count || 0;
            tempDb.close();
            isValid = true;
            console.log(`[Admin] Backup validation successful. Found ${backupUserCount} users.`);
        } catch (err) {
            console.error("[Admin] Backup validation failed:", err);
        } finally {
            if (fs.existsSync(tempDbPath)) fs.unlinkSync(tempDbPath);
        }

        if (!isValid) {
            return ctx.reply("❌ *Restore Failed:* The uploaded file is not a valid bot database or is corrupted.");
        }

        if (backupUserCount === 0) {
            return ctx.reply("⚠️ *Warning:* The backup you uploaded contains *0 users*. Restoring this will wipe your current data. If you are sure, please send the file again and confirm.");
        }

        await ctx.editMessageText(`✅ *Database Restored Successfully!*\n\nRestored \`${backupUserCount}\` users.\n\n🔄 *Reloading sessions...*`, { parse_mode: 'Markdown' });

        // Close current DB connection
        console.log("[Admin] Closing current database connection...");
        db.close();
        
        // Small delay to ensure file handles are released
        await new Promise(resolve => setTimeout(resolve, 1000));
        
        // Replace the database file
        console.log(`[Admin] Overwriting ${dbPath} with backup...`);
        fs.writeFileSync(dbPath, dbBuffer);
        
        // Re-open the database
        console.log("[Admin] Re-opening database...");
        db = new Database(dbPath);
        isFreshDb = false; // Reset flag after successful restore
        
        // Re-initialize sessions map and reconnect
        console.log("[Admin] Reconnecting all sessions from new database...");
        
        // Close all existing WhatsApp connections first
        for (const [sid, sock] of sessions.entries()) {
            try {
                sock.ev.removeAllListeners('connection.update');
                sock.end();
            } catch (e) {}
        }
        sessions.clear();
        
        // Reconnect everything
        await reconnectAllSessions(bot);
        
        await ctx.reply("✨ *Bot Reloaded Successfully!*\n\nAll data has been restored and sessions are reconnecting.");
        ctx.answerCbQuery();
    } catch (err: any) {
        console.error("DB Restore Error:", err);
        ctx.reply(`❌ *Restore Failed:* ${err.message}`);
        
        // Re-open DB if it was closed but restore failed
        try {
            db = new Database(dbPath);
        } catch (e) {}
    }
});

bot.action('cancel_restore', (ctx) => {
    ctx.editMessageText("❌ Restore cancelled.");
    ctx.answerCbQuery();
});

// Move document handler here for higher priority
bot.on('document', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        const doc = ctx.message.document;
        console.log(`[Admin] Document received: Name=${doc.file_name}, Mime=${doc.mime_type}, Size=${doc.file_size}`);

        // More inclusive check for database files
        const isDbFile = doc.file_name?.toLowerCase().endsWith('.db') || 
                         doc.mime_type === 'application/x-sqlite3' || 
                         doc.mime_type === 'application/octet-stream' && doc.file_name?.includes('bot_data');

        if (isDbFile) {
            // Store file_id in DB to avoid BUTTON_DATA_INVALID (callback_data limit is 64 bytes)
            await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)").run('pending_restore_file_id', doc.file_id);

            await ctx.reply("⚠️ *Database Restore Detected*\n\nAre you sure you want to replace the current database with this backup? This will:\n1️⃣ Replace all user data\n2️⃣ Restore all WhatsApp sessions\n3️⃣ Restart the bot\n\n*Proceed?*", {
                parse_mode: 'Markdown',
                ...Markup.inlineKeyboard([
                    [Markup.button.callback('✅ Confirm Restore', 'confirm_db_restore')],
                    [Markup.button.callback('❌ Cancel', 'cancel_restore')]
                ])
            });
        } else {
            await ctx.reply(`❓ Received document: \`${doc.file_name}\` (${doc.mime_type}).\n\nIf this is a database backup, please ensure it ends with \`.db\`.`, { parse_mode: 'Markdown' });
        }
    } catch (err: any) {
        console.error("Document Handler Error:", err);
        await ctx.reply(`❌ Error processing document: ${err.message}`);
    }
});

bot.action('admin_restore_info', (ctx) => {
    const adminId = ctx.from?.id.toString();
    if (adminId !== OWNER_ID) return;
    
    ctx.reply("⚠️ *Database Restore Required*\n\nIt seems your bot's data was lost due to an environment reset. To restore your accounts and users:\n\n1️⃣ Send your `.db` backup file to this bot.\n2️⃣ Tap 'Confirm Restore' when prompted.\n\n_If you don't have a backup, you must start fresh._", { parse_mode: 'Markdown' });
    ctx.answerCbQuery();
});

bot.action('referral_program', (ctx) => {
    showReferralInfo(ctx);
    ctx.answerCbQuery();
});

bot.action('show_status', (ctx) => {
    showStatus(ctx);
    ctx.answerCbQuery();
});

bot.command('status', (ctx) => {
    showStatus(ctx);
});

bot.command('help', (ctx) => {
    ctx.reply(`📖 *Help & Commands*\n\n` +
              `*General Commands:*\n` +
              `• /start - Open main menu\n` +
              `• /status - View your bot status\n` +
              `• /help - Show this help message\n` +
              `• /id - Get your Telegram ID\n` +
              `• /ping - Check if bot is online\n\n` +
              `*How to use:*\n` +
              `1. Link at least 2 WhatsApp accounts using *📱 Add Account*.\n` +
              `2. Set a safe delay (e.g., 250s) using *⏳ Set Delay*.\n` +
              `3. Click *🚀 Start Messaging* to begin the anti-ban loop.\n` +
              `4. Use *📅 Schedule* to automate messaging during specific hours.`, { parse_mode: 'Markdown' });
});

async function showStatus(ctx: Context) {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ?").all(telegramId) as any[];
    
    if (!user) return ctx.reply(t.user_not_found);

    const activeAccounts = accounts.filter(acc => acc.is_connected === 1).length;
    const totalAccounts = accounts.length;
    const accessExpiry = user.access_expiry ? user.access_expiry.split('T')[0] : 'Lifetime';
    const isRunning = user.is_running === 1 ? t.running_status : t.stopped_status;

    let msg = `${t.bot_status_title}\n\n` +
              `${t.user_label} \`${telegramId}\`\n` +
              `${t.bot_state_label} ${isRunning}\n` +
              `${t.access_label} \`${accessExpiry}\`\n\n` +
              `${t.stats_label}\n` +
              `${t.msgs_sent_label} \`${user.sent_count || 0}\`\n` +
              `${t.referrals_label} \`${user.referral_count || 0}\`\n\n` +
              `${t.wa_accounts_label} \`${activeAccounts}/${totalAccounts} Active\`\n`;

    if (accounts.length > 0) {
        msg += `\n${t.acc_list_label}\n`;
        accounts.forEach(acc => {
            const status = acc.is_connected === 1 ? "✅" : "❌";
            msg += `${status} \`${acc.phone_number}\`\n`;
        });
    } else {
        msg += `\n_${t.no_acc_linked}_`;
    }

    const keyboard = Markup.inlineKeyboard([
        [Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]
    ]);

    if (ctx.callbackQuery) {
        ctx.editMessageText(msg, { parse_mode: 'Markdown', ...keyboard });
    } else {
        ctx.reply(msg, { parse_mode: 'Markdown', ...keyboard });
    }
}

bot.action('show_referral', (ctx) => {
    showReferralInfo(ctx);
    ctx.answerCbQuery();
});

async function showReferralInfo(ctx: Context) {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    const botUsername = ctx.botInfo.username;
    const refLink = `https://t.me/${botUsername}?start=ref_${telegramId}`;
    
    const referralCount = user?.referral_count || 0;
    const accessExpiry = user?.access_expiry ? user.access_expiry.split('T')[0] : t.no_active_access;
    
    const msg = t.referral_program_title.replace('{count}', referralCount.toString()) + `\n\n` +
              `${t.stats_label}\n` +
              `${t.referrals_label} \`${referralCount}\`\n` +
              `${t.access_expiry_label} \`${accessExpiry}\`\n\n` +
              `${t.ref_link_label}\n\`${refLink}\``;

    const keyboard = Markup.inlineKeyboard([
        [Markup.button.callback('🎁 Claim 15 Days Access', 'claim_access')],
        [Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]
    ]);

    if (ctx.callbackQuery) {
        ctx.editMessageText(msg, { parse_mode: 'Markdown', ...keyboard });
    } else {
        ctx.reply(msg, { parse_mode: 'Markdown', ...keyboard });
    }
}

bot.action('claim_access', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    
    if (!user) return ctx.answerCbQuery(t.user_not_found);
    
    if (user.referral_count >= 10) {
        // Deduct 10 referrals and grant 15 days
        const newCount = user.referral_count - 10;
        
        let currentExpiry = user.access_expiry ? new Date(user.access_expiry) : new Date();
        if (currentExpiry < new Date()) currentExpiry = new Date();
        
        currentExpiry.setDate(currentExpiry.getDate() + 15);
        const expiryStr = currentExpiry.toISOString();
        
        await db.prepare("UPDATE users SET is_authorized = 1, access_expiry = ?, referral_count = ? WHERE telegram_id = ?")
            .run(expiryStr, newCount, telegramId);
            
        await ctx.reply(t.claim_success.replace('{expiry}', expiryStr.split('T')[0]).replace('{count}', newCount.toString()), { parse_mode: 'Markdown' });
        ctx.answerCbQuery("Access granted!");
    } else {
        await ctx.reply(t.claim_failed.replace('{count}', (user.referral_count || 0).toString()), { parse_mode: 'Markdown' });
        ctx.answerCbQuery("Not enough referrals.");
    }
});

bot.command('ping', (ctx) => ctx.reply('🏓 Pong! Bot is active and running.'));
bot.command('id', (ctx) => ctx.reply(`Your Telegram ID is: \`${ctx.from.id}\``, { parse_mode: 'Markdown' }));

bot.action('add_account', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    ctx.editMessageText(t.pairing_instruction, { 
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]])
    });
    ctx.answerCbQuery();
});

bot.action('set_delay', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    ctx.editMessageText(t.delay_instruction, { 
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]])
    });
    ctx.answerCbQuery();
});

bot.on('text', async (ctx, next) => {
    const text = ctx.message.text.trim();
    const telegramId = ctx.from.id.toString();

    // Check if it's a delay setting (1-4 digits)
    if (/^\d{1,4}$/.test(text)) {
        const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
        const lang = user?.language || 'en';
        const t = translations[lang] || translations.en;
        const delay = parseInt(text);
        const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]);
        
        if (delay < 10) return ctx.reply(t.min_delay, keyboard);
        
        await db.prepare("UPDATE users SET delay_seconds = ? WHERE telegram_id = ?").run(delay, telegramId);
        return ctx.reply(t.delay_updated.replace('{delay}', delay.toString()), keyboard);
    }

    // Check if it's a phone number (10-15 digits)
    if (/^(\+?\d{10,15})$/.test(text)) {
        const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
        const lang = user.language || 'en';
        const t = translations[lang] || translations.en;

        const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ?").all(telegramId) as any[];
        const limit = user.is_authorized || user.is_admin ? 20 : 4;

        if (accounts.length >= limit) {
            return ctx.reply(t.acc_limit_reached.replace('{limit}', limit.toString()));
        }

        const phoneNumber = text;
        const cleanNumber = phoneNumber.replace(/\D/g, '');
        const sessionId = `session_${telegramId}_${cleanNumber}`;
        
        const authPath = path.join(SESSIONS_DIR, sessionId);
        if (fs.existsSync(authPath)) {
            fs.rmSync(authPath, { recursive: true, force: true });
        }

        const existing = await db.prepare("SELECT * FROM whatsapp_sessions WHERE session_id = ?").get(sessionId);
        if (!existing) {
            await db.prepare("INSERT INTO whatsapp_sessions (telegram_id, phone_number, session_id, is_new_link) VALUES (?, ?, ?, 1)").run(telegramId, phoneNumber, sessionId);
        } else {
            await db.prepare("UPDATE whatsapp_sessions SET is_new_link = 1 WHERE session_id = ?").run(sessionId);
        }
        
        ctx.reply(t.pairing_fresh.replace('{phoneNumber}', phoneNumber));
        await connectToWhatsApp(telegramId, phoneNumber, bot);
        return;
    }

    return next();
});

bot.action('list_accounts', async (ctx) => {
    await showAccountList(ctx);
    ctx.answerCbQuery();
});

async function showAccountList(ctx: Context) {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    
    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ?").all(telegramId) as any[];
    if (accounts.length === 0) {
        const msg = t.no_acc_linked;
        const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]);
        if (ctx.callbackQuery) {
            return ctx.editMessageText(msg, keyboard);
        } else {
            return ctx.reply(msg, keyboard);
        }
    }
    
    let msg = `📱 *Your Linked Accounts*\n\n`;
    const buttons: any[] = [];
    
    accounts.forEach((acc) => {
        const status = acc.is_connected ? '✅ Connected' : '❌ Disconnected';
        msg += `• \`${acc.phone_number}\` - ${status}\n`;
        buttons.push([Markup.button.callback(`🗑️ Delete ${acc.phone_number}`, `delete_${acc.session_id}`)]);
    });
    
    buttons.push([Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]);
    
    const keyboard = Markup.inlineKeyboard(buttons);
    
    if (ctx.callbackQuery) {
        ctx.editMessageText(msg, { parse_mode: 'Markdown', ...keyboard });
    } else {
        ctx.reply(msg, { parse_mode: 'Markdown', ...keyboard });
    }
}

bot.action('start_messaging', async (ctx) => {
    await startMessaging(ctx);
    ctx.answerCbQuery();
});

async function startMessaging(ctx: Context) {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ? AND is_connected = 1").all(telegramId) as any[];
    
    if (accounts.length < 2) {
        const msg = t.need_2_acc;
        if (ctx.callbackQuery) {
            return ctx.editMessageText(msg);
        } else {
            return ctx.reply(msg);
        }
    }
    
    await db.prepare("UPDATE users SET is_running = 1 WHERE telegram_id = ?").run(telegramId);
    const successMsg = t.messaging_started.replace('{delay}', user.delay_seconds.toString());
    const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]);
    
    if (ctx.callbackQuery) {
        ctx.editMessageText(successMsg, keyboard);
    } else {
        ctx.reply(successMsg, keyboard);
    }
    
    startMessagingLoop(telegramId);
}

bot.action('stop_messaging', async (ctx) => {
    await stopMessaging(ctx);
    ctx.answerCbQuery();
});

async function stopMessaging(ctx: Context) {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    await db.prepare("UPDATE users SET is_running = 0 WHERE telegram_id = ?").run(telegramId);
    await db.prepare("UPDATE schedules SET is_active = 0 WHERE telegram_id = ?").run(telegramId);
    
    const msg = t.messaging_stopped;
    const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]);
    
    if (ctx.callbackQuery) {
        ctx.editMessageText(msg, keyboard);
    } else {
        ctx.reply(msg, keyboard);
    }
}

bot.action('set_schedule', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    ctx.editMessageText(t.schedule_instruction, { 
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]])
    });
    ctx.answerCbQuery();
});

bot.hears(/^(\d{1,2}:\d{2}-\d{1,2}:\d{2})$/, async (ctx) => {
    const range = ctx.message.text;
    const telegramId = ctx.from.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    
    await db.prepare("INSERT OR REPLACE INTO schedules (telegram_id, cron_time, is_active) VALUES (?, ?, 1)").run(telegramId, range);
    ctx.reply(t.schedule_set.replace('{range}', range), { parse_mode: 'Markdown' });
    
    // Trigger the loop check immediately
    startMessagingLoop(telegramId);
});

// --- Admin Commands ---
bot.command('admin', async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;
    await showAdminPanel(ctx);
});

bot.hears(/^access grant (\d+)(?:\s+(\d+))?$/, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const targetId = ctx.match[1];
    const days = ctx.match[2] ? parseInt(ctx.match[2]) : null;
    
    let expiryStr = null;
    if (days) {
        const expiry = new Date();
        expiry.setDate(expiry.getDate() + days);
        expiryStr = expiry.toISOString();
    }

    await db.prepare("UPDATE users SET is_authorized = 1, access_expiry = ? WHERE telegram_id = ?").run(expiryStr, targetId);
    
    let msg = `✅ Access granted to user ID: \`${targetId}\``;
    if (days) {
        msg += ` for *${days} days* (Expires: \`${expiryStr?.split('T')[0]}\`)`;
    } else {
        msg += ` (Lifetime access)`;
    }
    
    ctx.reply(msg, { parse_mode: 'Markdown' });
    
    try {
        bot.telegram.sendMessage(targetId, `🎉 *Access Granted!*\n\nYou now have access to use this bot.\n\n${days ? `Expiry: \`${expiryStr?.split('T')[0]}\`` : 'Access Type: Lifetime'}\n\nSend /start to begin.`, { parse_mode: 'Markdown' });
    } catch (e) {}
});

bot.hears(/^access remove (\d+)$/, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const targetId = ctx.match[1];
    await db.prepare("UPDATE users SET is_authorized = 0 WHERE telegram_id = ?").run(targetId);
    ctx.reply(`❌ Access removed from user ID: ${targetId}`);
    bot.telegram.sendMessage(targetId, "🚫 Your access has been revoked by the admin.").catch(() => {});
});

bot.hears('access list', async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const authorizedUsers = await db.prepare("SELECT * FROM users WHERE is_authorized = 1").all() as any[];
    if (authorizedUsers.length === 0) {
        return ctx.reply("No authorized users found.");
    }

    let list = "👥 *Authorized Users:*\n\n";
    authorizedUsers.forEach(u => {
        list += `• ID: \`${u.telegram_id}\` ${u.is_admin ? '(Admin)' : ''}\n`;
    });
    ctx.reply(list, { parse_mode: 'Markdown' });
});

bot.hears(/^broadcast\s+(.+)$/s, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const message = ctx.match[1];
    const users = await db.prepare("SELECT telegram_id FROM users").all() as any[];
    
    ctx.reply(`📢 *Starting Broadcast...*\n\nTarget: \`${users.length}\` users.`, { parse_mode: 'Markdown' });
    
    let success = 0;
    let fail = 0;
    
    for (const u of users) {
        try {
            await bot.telegram.sendMessage(u.telegram_id, message, { parse_mode: 'Markdown' });
            success++;
        } catch (e) {
            fail++;
        }
        // Small delay to avoid hitting Telegram rate limits
        await new Promise(resolve => setTimeout(resolve, 50));
    }
    
    ctx.reply(`✅ *Broadcast Completed*\n\n🟢 Success: \`${success}\`\n🔴 Failed: \`${fail}\``, { parse_mode: 'Markdown' });
});

bot.action(/^delete_(.+)$/, async (ctx) => {
    const sessionId = ctx.match[1];
    const telegramId = ctx.from!.id.toString();
    
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;

    const acc = await db.prepare("SELECT * FROM whatsapp_sessions WHERE session_id = ? AND telegram_id = ?").get(sessionId, telegramId) as any;
    if (!acc) return ctx.answerCbQuery("Account not found.");

    const sock = sessions.get(sessionId);
    if (sock) {
        try { await sock.logout(); } catch (e) {}
        sessions.delete(sessionId);
    }

    const authPath = path.join(SESSIONS_DIR, sessionId);
    if (fs.existsSync(authPath)) {
        fs.rmSync(authPath, { recursive: true, force: true });
    }

    await db.prepare("DELETE FROM whatsapp_sessions WHERE session_id = ?").run(sessionId);
    await db.prepare("DELETE FROM whatsapp_auth WHERE session_id = ?").run(sessionId);
    
    await ctx.editMessageText(t.acc_deleted.replace('{phoneNumber}', acc.phone_number), Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]));
    ctx.answerCbQuery();
});

bot.action('back_to_menu', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;
    
    ctx.editMessageText(t.back_to_menu_msg, getMainMenu(telegramId, lang));
    ctx.answerCbQuery();
});

bot.action('admin_stats', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        const users = await db.prepare("SELECT * FROM users").all() as any[];
        const totalUsers = users.length;
        const authorizedUsers = users.filter(u => u.is_authorized === 1).length;
        const runningUsers = users.filter(u => u.is_running === 1).length;
        const sessionsCount = await db.prepare("SELECT COUNT(*) as count FROM whatsapp_sessions").get() as any;

        const dbSize = fs.existsSync(dbPath) ? (fs.statSync(dbPath).size / 1024).toFixed(2) : "0";

        let msg = `📊 *Bot Statistics*\n\n` +
                  `👥 Total Users: \`${totalUsers}\`\n` +
                  `🔑 Authorized: \`${authorizedUsers}\`\n` +
                  `🚀 Running: \`${runningUsers}\`\n` +
                  `📱 WA Sessions: \`${sessionsCount.count}\`\n` +
                  `📦 DB Size: \`${dbSize} KB\`\n\n` +
                  `*Recent Users:*\n`;

        const lastUsers = users.slice(-10).reverse();
        lastUsers.forEach(u => {
            const status = u.is_running === 1 ? "🟢" : "🔴";
            const auth = u.is_authorized === 1 ? "🔑" : "🚫";
            const escapedUsername = u.username ? u.username.replace(/_/g, '\\_').replace(/\*/g, '\\*') : 'N/A';
            msg += `${status}${auth} \`${u.telegram_id}\` (@${escapedUsername})\n`;
        });

        ctx.editMessageText(msg, {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard([
                [Markup.button.callback('📥 Backup DB', 'admin_backup'), Markup.button.callback('🔄 Restore Sessions', 'admin_restore')],
                [Markup.button.callback('🔙 Back to Admin', 'admin_panel')]
            ])
        });
        ctx.answerCbQuery();
    } catch (err: any) {
        console.error("Admin Stats Error:", err);
        ctx.answerCbQuery("❌ Error fetching stats");
    }
});

bot.action('admin_users', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        const users = await db.prepare("SELECT * FROM users ORDER BY is_authorized DESC, telegram_id ASC LIMIT 20").all() as any[];
        
        let msg = `👥 *User Management (Last 20)*\n\n`;
        const buttons = [];

        for (const u of users) {
            const status = u.is_authorized ? "✅" : "🚫";
            const admin = u.is_admin ? "⭐" : "";
            const escapedUsername = u.username ? u.username.replace(/_/g, '\\_').replace(/\*/g, '\\*') : 'N/A';
            msg += `${status}${admin} \`${u.telegram_id}\` (@${escapedUsername})\n`;
            
            // Add toggle button for each user (except owner)
            if (u.telegram_id !== OWNER_ID) {
                buttons.push([Markup.button.callback(`${u.is_authorized ? '🚫 Revoke' : '✅ Grant'} ${u.telegram_id}`, `toggle_auth_${u.telegram_id}`)]);
            }
        }

        buttons.push([Markup.button.callback('🔙 Back to Admin', 'admin_panel')]);

        ctx.editMessageText(msg, {
            parse_mode: 'Markdown',
            ...Markup.inlineKeyboard(buttons)
        });
        ctx.answerCbQuery();
    } catch (err: any) {
        console.error("Admin Users Error:", err);
        ctx.answerCbQuery("❌ Error fetching users");
    }
});

bot.action(/^toggle_auth_(.+)$/, async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        const targetId = ctx.match[1];
        const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(targetId) as any;
        
        if (!user) return ctx.answerCbQuery("User not found");

        const newState = user.is_authorized ? 0 : 1;
        await db.prepare("UPDATE users SET is_authorized = ? WHERE telegram_id = ?").run(newState, targetId);

        ctx.answerCbQuery(`User ${targetId} is now ${newState ? 'Authorized' : 'Unauthorized'}`);
        
        // Refresh the user list
        const users = await db.prepare("SELECT * FROM users ORDER BY is_authorized DESC, telegram_id ASC LIMIT 20").all() as any[];
        let msg = `👥 *User Management (Last 20)*\n\n`;
        const buttons = [];
        for (const u of users) {
            const status = u.is_authorized ? "✅" : "🚫";
            const admin = u.is_admin ? "⭐" : "";
            const escapedUsername = u.username ? u.username.replace(/_/g, '\\_').replace(/\*/g, '\\*') : 'N/A';
            msg += `${status}${admin} \`${u.telegram_id}\` (@${escapedUsername})\n`;
            if (u.telegram_id !== OWNER_ID) {
                buttons.push([Markup.button.callback(`${u.is_authorized ? '🚫 Revoke' : '✅ Grant'} ${u.telegram_id}`, `toggle_auth_${u.telegram_id}`)]);
            }
        }
        buttons.push([Markup.button.callback('🔙 Back to Admin', 'admin_panel')]);
        ctx.editMessageText(msg, { parse_mode: 'Markdown', ...Markup.inlineKeyboard(buttons) });

        // Notify user
        try {
            await bot.telegram.sendMessage(targetId, newState ? "🎉 *Access Granted!*\n\nYou now have access to use this bot." : "🚫 *Access Revoked*\n\nYour access has been revoked by the admin.", { parse_mode: 'Markdown' });
        } catch (e) {}

    } catch (err: any) {
        console.error("Toggle Auth Error:", err);
        ctx.answerCbQuery("❌ Error updating user");
    }
});

bot.action('admin_backup', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        await ctx.answerCbQuery("📦 Preparing backup...");
        
        if (fs.existsSync(dbPath)) {
            await ctx.replyWithDocument({ source: dbPath, filename: `bot_data_backup_${Date.now()}.db` }, {
                caption: "📦 *Database Backup*\n\nKeep this file safe. You can restore it by sending it back to the bot if data is lost.",
                parse_mode: 'Markdown'
            });
        } else {
            await ctx.reply("❌ Database file not found.");
        }
    } catch (err: any) {
        console.error("Admin Backup Error:", err);
        ctx.reply(`❌ *Backup Failed:* ${err.message}`);
    }
});

bot.action('admin_restore', async (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;

        await ctx.answerCbQuery("🔄 Starting Restoration...");
        const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Admin', 'admin_panel')]]);
        await ctx.editMessageText("🔄 *Starting Manual Restoration...*\n\nRestoring all WhatsApp sessions from database.", { parse_mode: 'Markdown', ...keyboard });
        await reconnectAllSessions(bot);
        await ctx.editMessageText("✅ *Restoration Complete!*", keyboard);
    } catch (err: any) {
        console.error("Admin Restore Error:", err);
        const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Admin', 'admin_panel')]]);
        await ctx.editMessageText(`❌ *Restoration Failed:* ${err.message}`, keyboard);
    }
});

bot.action('admin_panel', (ctx) => {
    try {
        const adminId = ctx.from?.id.toString();
        if (adminId !== OWNER_ID) return;
        
        showAdminPanel(ctx);
        ctx.answerCbQuery();
    } catch (err: any) {
        console.error("Admin Panel Action Error:", err);
        ctx.answerCbQuery("❌ Error opening panel");
    }
});

async function showAdminPanel(ctx: Context) {
    try {
        const adminId = ctx.from?.id.toString();
        if (!adminId || adminId !== OWNER_ID) return;

        const isPublicSetting = await db.prepare("SELECT value FROM settings WHERE key = 'is_public'").get() as any;
        const isPublic = isPublicSetting?.value === "1";

        const botActiveSetting = await db.prepare("SELECT value FROM settings WHERE key = 'bot_active'").get() as any;
        const isBotActive = botActiveSetting?.value === "1";

        const text = `🔑 *Admin Control Panel*\n\n` +
                    `🤖 Bot Status: ${isBotActive ? '✅ *ONLINE*' : '🛑 *OFFLINE*'}\n` +
                    `🌍 Bot Mode: ${isPublic ? '🔓 *PUBLIC*' : '🔒 *PRIVATE*'}\n\n` +
                    `Use the buttons below to manage the bot.`;

        const keyboard = Markup.inlineKeyboard([
            [Markup.button.callback(isBotActive ? '🛑 Turn Bot OFF' : '✅ Turn Bot ON', 'toggle_bot_active')],
            [Markup.button.callback(isPublic ? '🔒 Switch to PRIVATE' : '🔓 Switch to PUBLIC', 'toggle_bot_mode')],
            [Markup.button.callback('📊 Bot Stats', 'admin_stats'), Markup.button.callback('📢 Broadcast', 'admin_broadcast')],
            [Markup.button.callback('📢 Force Sub', 'admin_force_sub'), Markup.button.callback('📦 Backup', 'admin_backup')],
            [Markup.button.callback('🔄 Restore Sessions', 'admin_restore')],
            [Markup.button.callback('👥 Manage Users', 'admin_users'), Markup.button.callback('🌐 Show App URL', 'admin_show_url')],
            [Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]
        ]);

        if (ctx.callbackQuery) {
            ctx.editMessageText(text, { parse_mode: 'Markdown', ...keyboard });
        } else {
            ctx.reply(text, { parse_mode: 'Markdown', ...keyboard });
        }
    } catch (err: any) {
        console.error("showAdminPanel Error:", err);
        ctx.reply(`❌ *Error loading Admin Panel:* ${err.message}`);
    }
}

bot.action('admin_show_url', (ctx) => {
    const adminId = ctx.from?.id.toString();
    if (adminId !== OWNER_ID) return;

    const appUrl = process.env.APP_URL || "NOT SET";
    ctx.reply(`🌐 *Current App URL:*\n\n\`${appUrl}\`\n\n_Set this in Render Environment Variables as APP_URL to enable self-pings._`, { parse_mode: 'Markdown' });
    ctx.answerCbQuery();
});

bot.action('toggle_bot_active', async (ctx) => {
    const adminId = ctx.from!.id.toString();
    if (adminId !== OWNER_ID) return;

    const botActiveSetting = await db.prepare("SELECT value FROM settings WHERE key = 'bot_active'").get() as any;
    const newState = botActiveSetting?.value === "1" ? "0" : "1";
    await db.prepare("UPDATE settings SET value = ? WHERE key = 'bot_active'").run(newState);

    const msg = newState === "1" ? "✅ *Bot Enabled by Owner*" : "🛑 *Bot Disabled by Owner*";
    
    // Broadcast to all users in background to avoid blocking the admin panel
    const users = await db.prepare("SELECT telegram_id FROM users").all() as any[];
    
    // Use a separate async function for broadcast
    const broadcastStatus = async () => {
        for (const u of users) {
            try {
                await bot.telegram.sendMessage(u.telegram_id, msg, { parse_mode: 'Markdown' });
                // Small delay between messages
                await new Promise(resolve => setTimeout(resolve, 50));
            } catch (e) {}
        }
    };
    
    broadcastStatus(); // Run in background

    ctx.answerCbQuery(`Bot is now ${newState === "1" ? 'ON' : 'OFF'}`);
    await showAdminPanel(ctx);
});

bot.action('toggle_bot_mode', async (ctx) => {
    const adminId = ctx.from!.id.toString();
    if (adminId !== OWNER_ID) return;

    const isPublicSetting = await db.prepare("SELECT value FROM settings WHERE key = 'is_public'").get() as any;
    const newState = isPublicSetting?.value === "1" ? "0" : "1";
    await db.prepare("UPDATE settings SET value = ? WHERE key = 'is_public'").run(newState);

    // If switching to PRIVATE, stop all unauthorized users
    if (newState === "0") {
        const unauthorizedUsers = await db.prepare("SELECT telegram_id FROM users WHERE is_authorized = 0 AND is_admin = 0 AND is_running = 1").all() as any[];
        console.log(`[Admin] Switching to PRIVATE. Stopping ${unauthorizedUsers.length} unauthorized users.`);
        for (const u of unauthorizedUsers) {
            await db.prepare("UPDATE users SET is_running = 0 WHERE telegram_id = ?").run(u.telegram_id);
            if (activeTimeouts.has(u.telegram_id)) {
                clearTimeout(activeTimeouts.get(u.telegram_id));
                activeTimeouts.delete(u.telegram_id);
            }
            try {
                await bot.telegram.sendMessage(u.telegram_id, "🔒 *Bot Mode Changed to PRIVATE*\n\nYour messaging loop has been stopped because the bot is now in private mode. Please contact the owner for access.", { parse_mode: 'Markdown' });
            } catch (e) {}
        }
    }

    ctx.answerCbQuery(`Mode changed to ${newState === "1" ? 'PUBLIC' : 'PRIVATE'}`);
    await showAdminPanel(ctx);
});

bot.action('admin_broadcast', (ctx) => {
    const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Admin', 'admin_panel')]]);
    ctx.editMessageText("📢 *Broadcast Message*\n\nTo broadcast a message to all users, use the following command format:\n\n`broadcast Your message here`\n\nExample:\n`broadcast Hello everyone! New update available.`", { parse_mode: 'Markdown', ...keyboard });
    ctx.answerCbQuery();
});

bot.action('admin_force_sub', (ctx) => {
    ctx.editMessageText("📢 *Force Subscription Settings*\n\nUse commands to manage channels:\n`add channel [ID]`\n`del channel [ID]`\n`channel list`", {
        parse_mode: 'Markdown',
        ...Markup.inlineKeyboard([[Markup.button.callback('🔙 Back', 'admin_panel')]])
    });
    ctx.answerCbQuery();
});

// --- Force Sub Channel Commands ---
bot.hears(/^add channel\s+(-?\d+)$/i, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channelId = ctx.match[1];

    try {
        // Check if bot is admin and get chat info
        const chat = await ctx.telegram.getChat(channelId) as any;
        const admins = await ctx.telegram.getChatAdministrators(channelId);
        const botId = ctx.botInfo.id;
        const isBotAdmin = admins.some(admin => admin.user.id === botId);

        if (!isBotAdmin) {
            return ctx.reply("❌ *Error:* Bot must be an admin in the channel to add it for force subscription.", { parse_mode: 'Markdown' });
        }

        const channelName = chat.title || "Channel";
        let inviteLink = chat.invite_link;

        if (!inviteLink) {
            inviteLink = await ctx.telegram.exportChatInviteLink(channelId);
        }

        await db.prepare("INSERT OR REPLACE INTO force_sub_channels (channel_id, channel_name, invite_link) VALUES (?, ?, ?)")
            .run(channelId, channelName, inviteLink);

        ctx.reply(`✅ *Channel Added Successfully*\n\nName: \`${channelName}\`\nID: \`${channelId}\`\nLink: ${inviteLink}`, { parse_mode: 'Markdown' });
    } catch (err: any) {
        console.error("Add Channel Error:", err);
        if (err.message.includes("chat not found")) {
            ctx.reply("❌ *Error:* Channel not found. Make sure the ID is correct and the bot is added to the channel.");
        } else if (err.message.includes("bot was kicked") || err.message.includes("member list is inaccessible")) {
            ctx.reply("❌ *Error:* Bot must be an admin for adding in force sub.");
        } else {
            ctx.reply(`❌ *Error adding channel:* ${err.message}`);
        }
    }
});

bot.hears(/^del channel\s+(-?\d+)$/i, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channelId = ctx.match[1];
    await db.prepare("DELETE FROM force_sub_channels WHERE channel_id = ?").run(channelId);
    ctx.reply(`✅ *Channel Deleted*\n\nID: \`${channelId}\``, { parse_mode: 'Markdown' });
});

bot.hears('channel list', async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channels = await db.prepare("SELECT * FROM force_sub_channels").all() as any[];
    if (channels.length === 0) {
        return ctx.reply("No force sub channels found.");
    }

    let list = "📢 *Force Sub Channels:*\n\n";
    channels.forEach(c => {
        list += `• \`${c.channel_name}\` (ID: \`${c.channel_id}\`)\nLink: ${c.invite_link}\n\n`;
    });
    ctx.reply(list, { parse_mode: 'Markdown' });
});

// --- Bot Mode Commands ---
bot.hears(/^\/restore$/i, async (ctx) => {
    const adminId = ctx.from!.id.toString();
    if (adminId !== OWNER_ID) return;

    await ctx.reply("🔄 *Starting Manual Restoration...*\n\nRestoring all WhatsApp sessions from database and filesystem.", { parse_mode: 'Markdown' });
    await reconnectAllSessions(bot);
    await ctx.reply("✅ *Restoration Complete!*");
});

bot.hears(/^set url\s+(https?:\/\/.+)$/i, async (ctx) => {
    const adminId = ctx.from!.id.toString();
    if (adminId !== OWNER_ID) return;

    const url = ctx.match[1].replace(/\/$/, '');
    await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('app_url', ?)").run(url);
    ctx.reply(`✅ *APP URL Updated*\n\nURL: \`${url}\`\n\nThe bot will now use this for keep-alive pings.`, { parse_mode: 'Markdown' });
});

bot.hears(/^bot mode (public|private)$/i, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const mode = ctx.match[1].toLowerCase();
    const value = mode === "public" ? "1" : "0";
    await db.prepare("UPDATE settings SET value = ? WHERE key = 'is_public'").run(value);
    ctx.reply(`✅ Bot mode set to: *${mode.toUpperCase()}*`, { parse_mode: 'Markdown' });
});

// --- Force Sub Channel Commands ---
bot.hears(/^channel add (-?\d+)$/, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channelId = ctx.match[1];
    try {
        const chat = await ctx.telegram.getChat(channelId);
        if (chat.type !== 'channel' && chat.type !== 'supergroup') {
            return ctx.reply("❌ That ID does not belong to a channel or supergroup.");
        }

        // Try to get or create an invite link
        let inviteLink = '';
        try {
            inviteLink = chat.invite_link || await ctx.telegram.exportChatInviteLink(channelId);
        } catch (e) {
            return ctx.reply("❌ Bot must be an admin in the channel to manage force sub.");
        }

        await db.prepare("INSERT OR REPLACE INTO force_sub_channels (channel_id, invite_link, channel_name) VALUES (?, ?, ?)")
            .run(channelId, inviteLink, chat.title);
        
        ctx.reply(`✅ Channel added: *${chat.title}*\nID: \`${channelId}\`\nLink: ${inviteLink}`, { parse_mode: 'Markdown' });
    } catch (err: any) {
        ctx.reply(`❌ Error: ${err.message || 'Could not find channel'}`);
    }
});

bot.hears(/^channel remove (-?\d+)$/, async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channelId = ctx.match[1];
    await db.prepare("DELETE FROM force_sub_channels WHERE channel_id = ?").run(channelId);
    ctx.reply(`✅ Channel removed: \`${channelId}\``, { parse_mode: 'Markdown' });
});

bot.hears('channel list', async (ctx) => {
    const adminId = ctx.from.id.toString();
    if (adminId !== OWNER_ID) return;

    const channels = await db.prepare("SELECT * FROM force_sub_channels").all() as any[];
    if (channels.length === 0) return ctx.reply("No force sub channels added.");

    let list = "📢 *Force Sub Channels:*\n\n";
    channels.forEach(c => {
        list += `• *${c.channel_name}*\n  ID: \`${c.channel_id}\`\n  Link: ${c.invite_link}\n\n`;
    });
    ctx.reply(list, { parse_mode: 'Markdown' });
});

bot.action('verify_sub', async (ctx) => {
    try {
        const telegramId = ctx.from?.id.toString();
        if (!telegramId) return;
        
        const isSubbed = await checkForceSub(ctx, telegramId);
        if (isSubbed) {
            await ctx.answerCbQuery("✅ Verification successful!");
            try { await ctx.deleteMessage(); } catch (e) {}
            
            const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
            if (!user?.language) {
                return showLanguageSelection(ctx);
            }
            
            const lang = user.language;
            const t = translations[lang] || translations.en;
            return ctx.reply(t.welcome, getMainMenu(telegramId, lang));
        } else {
            await ctx.answerCbQuery("❌ You haven't joined all channels yet!", { show_alert: true });
        }
    } catch (err: any) {
        console.error("Verify Sub Error:", err);
        ctx.answerCbQuery("❌ Error during verification");
    }
});

bot.action('logout_all', async (ctx) => {
    const telegramId = ctx.from!.id.toString();
    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    const lang = user?.language || 'en';
    const t = translations[lang] || translations.en;

    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ?").all(telegramId) as any[];
    
    for (const acc of accounts) {
        const sock = sessions.get(acc.session_id);
        if (sock) {
            try { await sock.logout(); } catch (e) {}
            sessions.delete(acc.session_id);
        }
        const authPath = path.join(SESSIONS_DIR, acc.session_id);
        if (fs.existsSync(authPath)) {
            fs.rmSync(authPath, { recursive: true, force: true });
        }
        await db.prepare("DELETE FROM whatsapp_auth WHERE session_id = ?").run(acc.session_id);
    }
    
    await db.prepare("DELETE FROM whatsapp_sessions WHERE telegram_id = ?").run(telegramId);
    const msg = t.all_logged_out;
    const keyboard = Markup.inlineKeyboard([[Markup.button.callback('🔙 Back to Menu', 'back_to_menu')]]);
    ctx.editMessageText(msg, keyboard);
    ctx.answerCbQuery();
});

// --- Messaging Logic ---


function isWithinTimeRange(rangeStr: string, language: string = 'en'): boolean {
    if (!rangeStr) return true;
    try {
        const [start, end] = rangeStr.split('-');
        
        // Get current time
        const now = new Date();
        
        // Timezone Offsets
        let offset = 0;
        if (language === 'en' || language === 'hi') {
            offset = 5.5 * 60 * 60 * 1000; // IST (UTC+5:30)
        } else if (language === 'id') {
            offset = 7 * 60 * 60 * 1000;   // WIB (UTC+7)
        } else if (language === 'zh') {
            offset = 8 * 60 * 60 * 1000;   // CST (UTC+8)
        } else {
            offset = 0; // Default to UTC
        }
        
        // Calculate target time using UTC methods to be server-independent
        const targetDate = new Date(now.getTime() + (now.getTimezoneOffset() * 60000) + offset);
        const currentMinutes = targetDate.getUTCHours() * 60 + targetDate.getUTCMinutes();
        
        const [startH, startM] = start.split(':').map(Number);
        const [endH, endM] = end.split(':').map(Number);
        
        const startMinutes = startH * 60 + startM;
        const endMinutes = endH * 60 + endM;
        
        if (startMinutes <= endMinutes) {
            return currentMinutes >= startMinutes && currentMinutes <= endMinutes;
        } else {
            // Overlap midnight
            return currentMinutes >= startMinutes || currentMinutes <= endMinutes;
        }
    } catch (e) {
        console.error("isWithinTimeRange Error:", e);
        return true;
    }
}

const activeTimeouts = new Map<string, NodeJS.Timeout>();

async function sendBetweenAccounts(sender: any, receiver: any, telegramId: string, step: number, user: any) {
    const senderSock = sessions.get(sender.session_id);
    if (!senderSock) return 0;

    const lang = user.language || 'en';
    const msgs = randomMessages[lang] || randomMessages.en;
    
    // Send 1 to 4 random messages
    const count = 1 + Math.floor(Math.random() * 4);
    
    try {
        const jid = receiver.phone_number.replace(/\D/g, '') + '@s.whatsapp.net';
        
        for (let i = 0; i < count; i++) {
            const msg = msgs[Math.floor(Math.random() * msgs.length)];
            
            // Anti-ban: Simulate human behavior
            if (Math.random() > 0.7) {
                try {
                    await senderSock.onWhatsApp(jid);
                    await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 3000));
                } catch (e) {}
            }

            await senderSock.sendPresenceUpdate('composing', jid);
            // Random typing delay between 3-7 seconds
            await new Promise(resolve => setTimeout(resolve, 3000 + Math.random() * 4000));
            await senderSock.sendPresenceUpdate('paused', jid);

            await senderSock.sendMessage(jid, { text: msg });
            
            // Anti-ban: Simulate reading the message back (sometimes)
            if (Math.random() > 0.4) {
                setTimeout(async () => {
                    try {
                        await senderSock.readMessages([{ remoteJid: jid, id: undefined }]);
                    } catch (e) {}
                }, 3000 + Math.random() * 5000);
            }
            
            // Small delay between multiple messages
            if (i < count - 1) {
                await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 3000));
            }
        }

        console.log(`[User ${telegramId}][Step ${step}] Sent ${count} messages from ${sender.phone_number} to ${receiver.phone_number}`);
        return count;
    } catch (err) {
        console.error(`Error sending from ${sender.phone_number}:`, err);
        return 0;
    }
}

async function startMessagingLoop(telegramId: string) {
    // Clear any existing timeout for this user to prevent duplicate loops
    if (activeTimeouts.has(telegramId)) {
        clearTimeout(activeTimeouts.get(telegramId));
        activeTimeouts.delete(telegramId);
    }

    const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId) as any;
    if (!user) return;

    // Check if bot is active
    const botActiveSetting = await db.prepare("SELECT value FROM settings WHERE key = 'bot_active'").get() as any;
    if (botActiveSetting?.value !== "1" && !user.is_admin) {
        console.log(`[Messaging] Bot is disabled. Stopping loop for ${telegramId}`);
        await db.prepare("UPDATE users SET is_running = 0 WHERE telegram_id = ?").run(telegramId);
        return;
    }

    // Check if user is still authorized (if mode is private)
    const isPublicSetting = await db.prepare("SELECT value FROM settings WHERE key = 'is_public'").get() as any;
    if (isPublicSetting?.value !== "1" && !user.is_authorized && !user.is_admin) {
        console.log(`[Messaging] User ${telegramId} is no longer authorized. Stopping loop.`);
        await db.prepare("UPDATE users SET is_running = 0 WHERE telegram_id = ?").run(telegramId);
        return;
    }

    const schedule = await db.prepare("SELECT * FROM schedules WHERE telegram_id = ? AND is_active = 1").get(telegramId) as any;
    const range = schedule ? schedule.cron_time : null;

    // Logic: Run if manually started OR if scheduled and within range
    const isManual = user.is_running === 1;
    const isScheduledActive = schedule && isWithinTimeRange(range, user.language);

    if (!isManual && !isScheduledActive) {
        if (schedule) {
            console.log(`[User ${telegramId}] Outside schedule range (${range}). Waiting...`);
            // Check again in 5 minutes if scheduled but outside range
            const timeout = setTimeout(() => startMessagingLoop(telegramId), 300000);
            activeTimeouts.set(telegramId, timeout);
        }
        return;
    }

    const accounts = await db.prepare("SELECT * FROM whatsapp_sessions WHERE telegram_id = ? AND is_connected = 1").all(telegramId) as any[];
    if (accounts.length < 2) {
        console.log(`[User ${telegramId}] Less than 2 connected accounts. Stopping loop.`);
        await db.prepare("UPDATE users SET is_running = 0 WHERE telegram_id = ?").run(telegramId);
        return;
    }

    const step = user.current_step || 0;
    const N = accounts.length;
    const pairs: { sender: any, receiver: any }[] = [];

    // --- Pair-Based Messaging Implementation ---
    // If N is even, we form N/2 pairs.
    // Step Even: Acc1 -> Acc2, Acc3 -> Acc4...
    // Step Odd:  Acc2 -> Acc1, Acc4 -> Acc3...
    // This ensures "vice versa after delay" as requested.
    
    if (N % 2 === 0) {
        for (let i = 0; i < N; i += 2) {
            const acc1 = accounts[i];
            const acc2 = accounts[i + 1];
            if (step % 2 === 0) {
                pairs.push({ sender: acc1, receiver: acc2 });
            } else {
                pairs.push({ sender: acc2, receiver: acc1 });
            }
        }
    } else {
        // For odd number of accounts, we use a modified ring to ensure everyone is included
        // but only one direction per account per step.
        const shift = (step % (N - 1)) + 1;
        for (let i = 0; i < N; i++) {
            const sender = accounts[i];
            const receiver = accounts[(i + shift) % N];
            pairs.push({ sender, receiver });
        }
    }

    // Run all pairs in parallel (Simultaneously per delay)
    const results = await Promise.all(pairs.map(p => sendBetweenAccounts(p.sender, p.receiver, telegramId, step, user)));
    const totalSent = results.reduce((a, b) => a + b, 0);

    // Update step and sent_count in DB
    await db.prepare("UPDATE users SET current_step = ?, sent_count = (sent_count + ?) WHERE telegram_id = ?").run(step + 1, totalSent, telegramId);

    // Anti-ban: Add random jitter (±20% of the base delay)
    const baseDelay = user.delay_seconds || 250;
    
    // Long Break Logic: Every 15-25 steps, take a 10-20 minute break
    const shouldTakeLongBreak = step > 0 && step % (15 + Math.floor(Math.random() * 10)) === 0;
    
    let finalDelay: number;
    if (shouldTakeLongBreak) {
        const breakMinutes = 10 + Math.floor(Math.random() * 10);
        console.log(`[User ${telegramId}] ☕ Taking a long break for ${breakMinutes} minutes...`);
        finalDelay = breakMinutes * 60 * 1000;
    } else {
        const jitter = (Math.random() * 0.4 - 0.2) * baseDelay; 
        finalDelay = Math.max(10, (baseDelay + jitter)) * 1000;
    }
    
    console.log(`[User ${telegramId}] Next message in ${Math.round(finalDelay/1000)}s`);
    const nextTimeout = setTimeout(() => startMessagingLoop(telegramId), finalDelay);
    activeTimeouts.set(telegramId, nextTimeout);
}

// --- Cron Jobs for Schedules ---
// Check every 5 minutes for scheduled tasks that should start
cron.schedule('*/5 * * * *', async () => {
    const activeSchedules = await db.prepare("SELECT * FROM schedules WHERE is_active = 1").all() as any[];
    for (const sched of activeSchedules) {
        const user = await db.prepare("SELECT language FROM users WHERE telegram_id = ?").get(sched.telegram_id) as any;
        if (isWithinTimeRange(sched.cron_time, user?.language)) {
            // Only start if not already running to avoid resetting the loop
            if (!activeTimeouts.has(sched.telegram_id)) {
                console.log(`[Schedule] Starting loop for user ${sched.telegram_id} within range ${sched.cron_time}`);
                startMessagingLoop(sched.telegram_id);
            }
        }
    }
});

// --- Express Server Setup ---
async function startServer() {
    const app = express();
    
    // Request logging middleware for debugging
    app.use((req, res, next) => {
        if (!req.path.startsWith('/@') && !req.path.includes('node_modules')) {
            console.log(`[Server] ${req.method} ${req.path}`);
        }
        next();
    });

    app.use(express.json());

    app.get("/api/status", async (req, res) => {
        try {
            const stats = {
                users: await db.prepare("SELECT COUNT(*) as count FROM users").get() as any || { count: 0 },
                sessions: await db.prepare("SELECT COUNT(*) as count FROM whatsapp_sessions").get() as any || { count: 0 },
                connected: await db.prepare("SELECT COUNT(*) as count FROM whatsapp_sessions WHERE is_connected = 1").get() as any || { count: 0 },
            };
            res.json(stats);
        } catch (err: any) {
            console.error("API Status Error:", err);
            res.status(500).json({ error: "Database error", details: err.message });
        }
    });

    app.get("/api/health", (req, res) => {
        res.json({ status: "ok", uptime: process.uptime() });
    });

    // WhatsApp add-account API
    app.post("/api/add-account", express.json(), async (req, res) => {
        const { telegramId, phoneNumber } = req.body;
        if (!telegramId || !phoneNumber) {
            return res.status(400).json({ error: "Missing telegramId or phoneNumber" });
        }

        try {
            const cleanNumber = phoneNumber.replace(/\D/g, '');
            const sessionId = `session_${telegramId}_${cleanNumber}`;
            
            const existing = await db.prepare("SELECT * FROM whatsapp_sessions WHERE session_id = ?").get(sessionId);
            if (!existing) {
                await db.prepare("INSERT INTO whatsapp_sessions (telegram_id, phone_number, session_id, is_new_link) VALUES (?, ?, ?, 1)").run(telegramId, phoneNumber, sessionId);
            } else {
                await db.prepare("UPDATE whatsapp_sessions SET is_new_link = 1 WHERE session_id = ?").run(sessionId);
            }

            // Ensure user exists
            const user = await db.prepare("SELECT * FROM users WHERE telegram_id = ?").get(telegramId.toString());
            if (!user) {
                await db.prepare("INSERT INTO users (telegram_id) VALUES (?)").run(telegramId.toString());
            }

            connectToWhatsApp(telegramId.toString(), phoneNumber, bot);
            res.json({ success: true });
        } catch (err: any) {
            res.status(500).json({ error: err.message });
        }
    });

    // Health check routes
    app.get("/api/health", (req, res) => {
        res.json({ status: "ok", database: db ? "initialized" : "missing" });
    });
    app.get("/health", (req, res) => res.send("OK"));

    if (process.env.NODE_ENV !== "production") {
        const vite = await createViteServer({
            server: { middlewareMode: true },
            appType: "spa",
        });
        app.use(vite.middlewares);
    } else {
        const distPath = path.resolve(process.cwd(), "dist");
        if (fs.existsSync(distPath)) {
            app.use(express.static(distPath));
            // Catch-all for SPA routing in production
            app.get("*", (req, res, next) => {
                if (req.path.startsWith('/api')) return next();
                res.sendFile(path.join(distPath, "index.html"));
            });
        } else {
            console.warn("⚠️ [Production] 'dist' folder not found. Static files will not be served.");
        }
    }

    // Initialize Database BEFORE starting the server
    try {
        await initDatabase();
    } catch (dbErr) {
        console.error("[Startup] CRITICAL: Database initialization failed:", dbErr);
    }

    app.listen(Number(PORT), "0.0.0.0", async () => {
        console.log(`Server running on http://localhost:${PORT}`);
        console.log(`[Startup] APP_URL: ${process.env.APP_URL || 'NOT SET'}`);
        
        // Reconnect existing WhatsApp sessions on startup
        await reconnectAllSessions(bot);

        if (botToken && botToken !== "DUMMY_TOKEN") {
            console.log(`[Telegram] Bot token found: ${botToken.substring(0, 5)}...${botToken.substring(botToken.length - 4)}`);
            const startBot = async (retries = 5, delay = 5000) => {
                for (let i = 0; i < retries; i++) {
                    try {
                        console.log(`[Telegram] Attempting to start bot (Attempt ${i + 1})...`);
                        
                        // Clear any existing webhooks
                        await bot.telegram.deleteWebhook({ drop_pending_updates: true });
                        
                        // Wait for Telegram servers to sync
                        await new Promise(resolve => setTimeout(resolve, delay));
                        
                        await bot.launch({
                            allowedUpdates: ['message', 'callback_query', 'chat_member'],
                            dropPendingUpdates: true
                        });
                        
                        console.log("✅ Telegram Bot is now ONLINE (Polling)");
                        
                        // Notify owner
                        try {
                            await bot.telegram.sendMessage(OWNER_ID, "🚀 *Bot Started Successfully!*\n\nAll systems are online and WhatsApp sessions are being reconnected.", { parse_mode: 'Markdown' });
                        } catch (e) {}

                        // Notify all users about restart (as requested)
                        try {
                            const users = await db.prepare("SELECT telegram_id FROM users").all() as any[];
                            console.log(`[Startup] Notifying ${users.length} users about restart...`);
                            for (const u of users) {
                                try {
                                    await bot.telegram.sendMessage(u.telegram_id, "🔄 *Bot Restarted*\n\nThe bot has been updated or restarted. If your accounts show as disconnected, please re-link them to ensure 24/7 operation.", { parse_mode: 'Markdown' });
                                    // Small delay to avoid Telegram flood limits
                                    await new Promise(resolve => setTimeout(resolve, 50));
                                } catch (e) {}
                            }
                        } catch (e) {
                            console.error("[Startup] Error notifying users:", e);
                        }

                        return;
                    } catch (err: any) {
                        if (err.message.includes('409: Conflict')) {
                            console.warn(`⚠️ [Telegram] Conflict detected (Attempt ${i + 1}). Another instance might be closing. Retrying in ${delay/1000}s...`);
                            // Stop the bot instance if it was partially started
                            try { bot.stop(); } catch (e) {}
                            await new Promise(resolve => setTimeout(resolve, delay));
                            // Increase delay for next attempt
                            delay += 5000;
                        } else if (err.message.includes('401: Unauthorized')) {
                            console.error("❌ [Telegram] CRITICAL ERROR: 401 Unauthorized. Your TELEGRAM_BOT_TOKEN is invalid or has expired.");
                            console.error("👉 Please check your environment variables and ensure you have provided a valid token from @BotFather.");
                            return;
                        } else {
                            console.error("❌ [Telegram] Failed to launch bot:", err.message);
                            return; // Non-conflict error, stop retrying
                        }
                    }
                }
                console.error("❌ [Telegram] Could not start bot after multiple attempts.");
            };

            startBot();
        } else {
            console.warn("⚠️ [Telegram] Bot token is MISSING or DUMMY. Bot will not start.");
            console.warn("👉 Please set TELEGRAM_BOT_TOKEN in your environment variables.");
        }
    });
}

// Graceful shutdown
const shutdown = (signal: string) => {
    console.log(`Received ${signal}. Shutting down bot...`);
    bot.stop(signal);
    process.exit(0);
};

process.once('SIGINT', () => shutdown('SIGINT'));
process.once('SIGTERM', () => shutdown('SIGTERM'));
process.once('SIGUSR2', () => shutdown('SIGUSR2')); // For nodemon/tsx restarts

startServer();

// Aggressive Keep-Alive to prevent deep sleep (Optional)
cron.schedule('*/5 * * * *', async () => {
    let appUrl = process.env.APP_URL;
    
    // Skip if no APP_URL is provided (e.g. Background Worker deployment)
    if (!appUrl || !appUrl.startsWith('http')) {
        return;
    }

    try {
        const statusUrl = `${appUrl.replace(/\/$/, '')}/api/status`;
        console.log(`[Keep-Alive] Pinging self: ${statusUrl}`);
        const response = await fetch(statusUrl, {
            headers: { 'User-Agent': 'Bot-Keep-Alive' }
        });
        if (response.ok) {
            console.log(`[Keep-Alive] Self-ping successful: ${response.status}`);
        } else {
            console.warn(`[Keep-Alive] Self-ping returned non-OK status: ${response.status}`);
        }
    } catch (err: any) {
        console.error(`[Keep-Alive] Self-ping failed:`, err.message);
    }
});
