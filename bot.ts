import { Bot, InputFile } from "grammy";
import { Database } from "bun:sqlite";

const BOT_TOKEN = process.env.BOT_TOKEN;
const DB_PATH = process.env.DB_PATH || "./blackouts.db";

const bot = new Bot(BOT_TOKEN!);

const db = new Database(DB_PATH);

db.run(`
    CREATE TABLE IF NOT EXISTS blackout_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        group_no TEXT NOT NULL,
        data JSONB NOT NULL
    )
`);

db.run(`
    CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        group_no TEXT NOT NULL,
        UNIQUE(chat_id, group_no)
    )
`);

db.run(`
    CREATE TABLE IF NOT EXISTS sent_notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        group_no TEXT NOT NULL,
        notification_type TEXT NOT NULL,
        hour INTEGER NOT NULL,
        date TEXT NOT NULL,
        UNIQUE(chat_id, group_no, notification_type, hour, date)
    )
`);

db.run(`CREATE INDEX IF NOT EXISTS idx_timestamp_group ON blackout_data(timestamp, group_no)`);

console.log(`Database initialized at ${DB_PATH}`);

const existingGroups: Set<string> = new Set(["11", "12", "21", "22", "31", "32", "41", "42", "51", "52", "61", "62"]);

const userAgents = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
];

function getRandomUserAgent(): string {
    return userAgents[Math.floor(Math.random() * userAgents.length)]!;
}

async function getTokenAndCookies() {
    const response = await fetch("https://kiroe.com.ua/electricity-blackout", {
        method: "GET",
        headers: {
            "User-Agent": getRandomUserAgent(),
        },
    });
    console.log("Response status:", response.status);
    
    const setCookieHeader = response.headers.get("set-cookie");
    const cookies = setCookieHeader ? setCookieHeader.split(", ") : [];
    
    console.log("Cookies:", cookies);
    
    const body = await response.text();
    
    const tokenRegex = /token: "(.+?)"/;
    const tokenMatch = body.match(tokenRegex);
    const token = tokenMatch ? tokenMatch[1] : null;
    
    console.log("Token:", token);

    if (!token) {
        throw new Error("Token not found");
    }

    return { token, cookies };
}

const mapToJson = (map: Map<any, any>) => {
    return JSON.stringify(Object.fromEntries(map));
};

async function fetchGroupsMap() {
    try {
        let { token, cookies } = await getTokenAndCookies();

        const formData = new FormData();
        formData.append("token", token);
        formData.append("ajax", "1");
        
        let curId = 100800;
        let groupsToFind: Set<string> = new Set(existingGroups);
        let groupsAtIds: Map<string, number> = new Map();
        
        while (groupsToFind.size > 0) {
            const cityResponse = await fetch("https://kiroe.com.ua/electricity-blackout/websearch/v3/" + curId, {
                method: "POST",
                headers: {
                    "User-Agent": getRandomUserAgent(),
                    "Cookie": cookies.map(cookie => cookie.split(";")[0]).join("; "),
                },
                body: formData,
            });
            
            console.log(`City Response for ID ${curId} status:`, cityResponse.status);
            const cityData = await cityResponse.json();
            if (cityData.data && cityData.data.length > 0) {

                const groupNo = cityData.data[0].GroupNo;
                if (groupsToFind.has(groupNo)) {
                    console.log(`Found data for GroupNo ${groupNo}`);
                    groupsToFind.delete(groupNo);
                    groupsAtIds.set(groupNo, curId);
                }

                for (const groupNo_ of groupsToFind.keys()) {
                    console.log(`- Still looking for GroupNo ${groupNo_}`);
                }
            }
            else {
                console.log(`No data found for ID ${curId}. Updating token and cookies.`);

                const { token: newToken, cookies: newCookies } = await getTokenAndCookies();
                token = newToken;
                cookies = newCookies;

                await new Promise(resolve => setTimeout(resolve, 2000 + Math.random() * 2000));
            }

            curId = Math.floor(Math.random() * 200000) + 1;

            await new Promise(resolve => setTimeout(resolve, 800 + Math.random() * 400));
        }
        
        return { groupsAtIds };
        
    } catch (error) {
        console.error("Error in fetchData:", error);
        throw error;
    }
}

async function fetchAllData(forceRefresh: boolean = false) {
    const dataFile = Bun.file("groups_data.json");
    const dataExists = await dataFile.exists();

    if (!forceRefresh && dataExists) {
        const cachedContent = await dataFile.text();
        if (cachedContent && cachedContent.trim().length > 0) {
            try {
                const cachedData = JSON.parse(cachedContent);
                const savedTime = cachedData.timestamp;
                const now = Date.now();
                const fiveMinutes = 5 * 60 * 1000;

                if (savedTime && (now - savedTime) < fiveMinutes) {
                    console.log(`Using cached data from ${new Date(savedTime).toISOString()}`);
                    const groupsAtIds = new Map<string, number>(Object.entries(cachedData.groupsAtIds) as [string, number][]);
                    const groupsData = new Map<string, Object>(Object.entries(cachedData.groupsData) as [string, Object][]);
                    return { groupsAtIds, groupsData };
                } else {
                    console.log(`Cached data is older than 5 minutes (${new Date(savedTime).toISOString()}), refetching...`);
                }
            } catch (parseError) {
                console.log("Failed to parse cached data, refetching...");
            }
        }
    }

    while (true) {
        let groupsData: Map<string, Object> = new Map();

        const file = Bun.file("groups_map.json");
        const exists = await file.exists();

        if (!exists) {
            const { groupsAtIds } = await fetchGroupsMap();
            const jsonContent = mapToJson(groupsAtIds);
            await Bun.write("groups_map.json", jsonContent);
        }

        const content = await Bun.file("groups_map.json").text();
        
        if (!content || content.trim().length === 0) {
            throw new Error("groups_map.json is empty");
        }
        
        let parsedData;
        try {
            parsedData = JSON.parse(content);
        } catch (parseError) {
            throw new Error(`Failed to parse groups_map.json: ${parseError}`);
        }
        
        const groupsAtIds = new Map<string, number>(Object.entries(parsedData) as [string, number][]);
        
        console.log("Loaded groupsAtIds from file:", groupsAtIds);    

        const { token, cookies } = await getTokenAndCookies();

        const formData = new FormData();
        formData.append("token", token);
        formData.append("ajax", "1");

        for (const [groupNo, id] of groupsAtIds.entries()) {
            console.log(`GroupNo ${groupNo} is at ID ${id}`);

            const cityResponse = await fetch("https://kiroe.com.ua/electricity-blackout/websearch/v3/" + id, {
                method: "POST",
                headers: {
                    "User-Agent": getRandomUserAgent(),
                    "Cookie": cookies.map(cookie => cookie.split(";")[0]).join("; "),
                },
                body: formData,
            });

            const cityData = await cityResponse.json();

            if (!cityData.data || cityData.data.length === 0) {
                console.log(`Data for GroupNo ${groupNo} at ID ${id} is missing or invalid. Refetching all data.`);
                throw new Error("Data missing");
            }

            if (cityData.data[0].GroupNo !== groupNo) {
                console.log(`Data mismatch for GroupNo ${groupNo} at ID ${id}. Refetching all data.`);
                await Bun.file("groups_map.json").delete();
                continue;
            }

            console.log(`Got valid data for GroupNo ${groupNo} at ID ${id}.`);
            groupsData.set(groupNo, cityData.data[0]);
        }

        const timestamp = Date.now();
        const dataToSave = {
            timestamp,
            groupsAtIds: Object.fromEntries(groupsAtIds),
            groupsData: Object.fromEntries(groupsData)
        };
        await Bun.write("groups_data.json", JSON.stringify(dataToSave, null, 2));
        console.log(`Saved fresh data at ${new Date(timestamp).toISOString()}`);

        const insertStmt = db.prepare("INSERT INTO blackout_data (timestamp, group_no, data) VALUES (?, ?, jsonb(?))");
        
        for (const [groupNo, data] of groupsData.entries()) {
            const jsonData = JSON.stringify(data);
            insertStmt.run(timestamp, groupNo, jsonData);
            console.log(`Stored data for GroupNo ${groupNo} in database`);
        }
        
        insertStmt.finalize();

        return { groupsAtIds, groupsData }
    }
}

interface Schedule {
    DayId: number;
    DayNo: number;
    DayName: string;
    IsToday: number;
    H01: number;
    H02: number;
    H03: number;
    H04: number;
    H05: number;
    H06: number;
    H07: number;
    H08: number;
    H09: number;
    H10: number;
    H11: number;
    H12: number;
    H13: number;
    H14: number;
    H15: number;
    H16: number;
    H17: number;
    H18: number;
    H19: number;
    H20: number;
    H21: number;
    H22: number;
    H23: number;
    H24: number;
}

// interface GroupData {
//     SearchDate: string;
//     SearchID: number;
//     SearchCnt: number;
//     SearchAdress: string;
//     GroupNo: string;
//     BlackOutMsg: string;
//     SearchMsg: string;
//     SheduleShow: number;
//     SheduleTitle: string;
//     Shedule: Array<Schedule>;
//     AdditionalInfoText: string;
//     FooterText: string;
// }

async function renderScheduleImage(schedule: Array<Schedule>, groupNo: string, searchDate: string) {
    let canvas = require("canvas");
    let Canvas = canvas.Canvas;
    
    const cellWidth = 35;
    const cellHeight = 40;
    const headerHeight = 60;
    const dayNameWidth = 120;
    const padding = 10;
    
    const canvasWidth = dayNameWidth + (24 * cellWidth) + (padding * 2);
    const canvasHeight = headerHeight + (schedule.length * cellHeight) + (padding * 2) + 50;
    
    let canvasObj = new Canvas(canvasWidth, canvasHeight);
    let ctx2d = canvasObj.getContext("2d");

    // Background
    ctx2d.fillStyle = "#1a1a1a";
    ctx2d.fillRect(0, 0, canvasWidth, canvasHeight);

    // Title
    ctx2d.fillStyle = "#ffffff";
    ctx2d.font = "bold 24px Arial";
    ctx2d.textAlign = "center";
    ctx2d.fillText(`Group ${groupNo[0]}.${groupNo.slice(1)} - ${searchDate}`, canvasWidth / 2, 35);

    const startX = padding;
    const startY = headerHeight + padding;

    // Draw hour headers
    ctx2d.font = "bold 14px Arial";
    ctx2d.textAlign = "center";
    ctx2d.fillStyle = "#cccccc";
    
    for (let h = 1; h <= 24; h++) {
        const x = startX + dayNameWidth + ((h - 1) * cellWidth) + (cellWidth / 2);
        ctx2d.fillText(h.toString(), x, startY - 10);
    }

    // Draw schedule rows
    for (let i = 0; i < schedule.length; i++) {
        const day = schedule[i];
        if (!day) continue;
        
        const rowY = startY + (i * cellHeight);

        // Day name background
        if (day.IsToday === 1) {
            ctx2d.fillStyle = "#2d5f2d";
        } else {
            ctx2d.fillStyle = "#2a2a2a";
        }
        ctx2d.fillRect(startX, rowY, dayNameWidth, cellHeight);

        // Day name text
        ctx2d.fillStyle = "#ffffff";
        ctx2d.font = day.IsToday === 1 ? "bold 14px Arial" : "14px Arial";
        ctx2d.textAlign = "left";
        ctx2d.fillText(day.DayName, startX + 10, rowY + (cellHeight / 2) + 5);

        // Draw hour cells
        for (let h = 1; h <= 24; h++) {
            const hourKey = `H${h.toString().padStart(2, '0')}`;
            const hourValue = (day as any)[hourKey];
            const cellX = startX + dayNameWidth + ((h - 1) * cellWidth);

            // Cell background based on status
            if (hourValue === 0) {
                // OFF - Red
                ctx2d.fillStyle = "#8b0000";
            } else if (hourValue === 1) {
                // ON - Green
                ctx2d.fillStyle = "#006400";
            } else if (hourValue === 2) {
                // Maybe - Yellow
                ctx2d.fillStyle = "#8b8b00";
            } else {
                // Unknown - Gray
                ctx2d.fillStyle = "#3a3a3a";
            }
            
            ctx2d.fillRect(cellX, rowY, cellWidth, cellHeight);

            // Cell border
            ctx2d.strokeStyle = "#1a1a1a";
            ctx2d.lineWidth = 1;
            ctx2d.strokeRect(cellX, rowY, cellWidth, cellHeight);
        }

        // Row border
        ctx2d.strokeStyle = "#444444";
        ctx2d.lineWidth = 1;
        ctx2d.strokeRect(startX, rowY, dayNameWidth, cellHeight);
    }

    // Legend
    const legendY = startY + (schedule.length * cellHeight) + 20;
    ctx2d.font = "12px Arial";
    ctx2d.textAlign = "left";
    
    // ON
    ctx2d.fillStyle = "#006400";
    ctx2d.fillRect(startX, legendY, 20, 20);
    ctx2d.fillStyle = "#ffffff";
    ctx2d.fillText("ON (Light)", startX + 30, legendY + 15);
    
    // OFF
    ctx2d.fillStyle = "#8b0000";
    ctx2d.fillRect(startX + 150, legendY, 20, 20);
    ctx2d.fillStyle = "#ffffff";
    ctx2d.fillText("OFF (Blackout)", startX + 180, legendY + 15);
    
    // Maybe
    ctx2d.fillStyle = "#8b8b00";
    ctx2d.fillRect(startX + 330, legendY, 20, 20);
    ctx2d.fillStyle = "#ffffff";
    ctx2d.fillText("Possible", startX + 360, legendY + 15);

    const buffer = canvasObj.toBuffer("image/png");
    return buffer;
}

bot.command("start", (ctx) => ctx.reply("Welcome! Up and running."));

bot.command("fetch", async (ctx) => {
    try {
        const group = ctx.match.trim().split(".").join("");
        const chatId = ctx.chat?.id;

        if (!chatId) {
            await ctx.reply("Unable to identify chat.");
            return;
        }

        console.log("Fetch command invoked with group:", group);
        
        let { groupsAtIds, groupsData } = await fetchAllData();
        
        if (!group || group.length === 0) {
            const subs = db.prepare("SELECT group_no FROM subscriptions WHERE chat_id = ?").all(chatId) as Array<{ group_no: string }>;
            
            if (subs.length === 0) {
                const formatGroup = (k: string) => k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k;
                const availableGroups = Array.from(groupsData.keys()).map(formatGroup).join(", ");
                await ctx.reply(`You have no active subscriptions. Available groups: ${availableGroups}\n\nUse /subscribe <group> to subscribe.`);
                return;
            }

            for (const sub of subs) {
                const subGroupNo = sub.group_no;
                let groupData = groupsData.get(subGroupNo);
                
                if (!groupData) {
                    await ctx.reply(`No data found for GroupNo ${subGroupNo[0]}.${subGroupNo.slice(1)}`);
                    continue;
                }

                const schedule: Array<Schedule> = (groupData as any).Shedule;
                const searchDate: string = (groupData as any).SearchDate;

                if (!schedule || !Array.isArray(schedule)) {
                    await ctx.reply(`Invalid schedule data for GroupNo ${subGroupNo[0]}.${subGroupNo.slice(1)}`);
                    continue;
                }

                const buffer = await renderScheduleImage(schedule, subGroupNo, searchDate);
                await ctx.replyWithPhoto(new InputFile(buffer), { caption: `Data for GroupNo ${subGroupNo[0]}.${subGroupNo.slice(1)}` });
            }
            
            return;
        }

        let groupData = groupsData.get(group);
        if (!groupData) {
            const formatGroup = (k: string) => k.includes(".") ? k : (k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k);
            const availableGroups = Array.from(groupsData.keys()).map(formatGroup).join(", ");
            await ctx.reply(`No data found for GroupNo ${group}. Available groups: ${availableGroups}`);
            return;
        }

        const schedule: Array<Schedule> = (groupData as any).Shedule;
        const searchDate: string = (groupData as any).SearchDate;

        if (!schedule || !Array.isArray(schedule)) {
            await ctx.reply(`Invalid schedule data for GroupNo ${group}`);
            return;
        }

        const buffer = await renderScheduleImage(schedule, group, searchDate);

        await ctx.replyWithPhoto(new InputFile(buffer), { caption: `Visual data for GroupNo ${group}` });
    }
    catch (error) {
        console.error("Error fetching data:", error);
        await ctx.reply("Failed to fetch data.");
    }
});

bot.command("subscribe", async (ctx) => {
    try {
        const group = ctx.match.trim().split(".").join("");
        const chatId = ctx.chat?.id;

        if (!chatId) {
            await ctx.reply("Unable to identify chat.");
            return;
        }

        if (!group || group.length === 0) {
            await ctx.reply("Please specify a group number. Usage: /subscribe <group> (e.g., /subscribe 21 or /subscribe 2.1)");
            return;
        }

        let { groupsData } = await fetchAllData();
        if (!groupsData.has(group)) {
            const formatGroup = (k: string) => k.includes(".") ? k : (k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k);
            const availableGroups = Array.from(groupsData.keys()).map(formatGroup).join(", ");
            await ctx.reply(`Invalid group ${group}. Available groups: ${availableGroups}`);
            return;
        }

        const insertStmt = db.prepare("INSERT OR IGNORE INTO subscriptions (chat_id, group_no) VALUES (?, ?)");
        const result = insertStmt.run(chatId, group);
        insertStmt.finalize();

        if (result.changes > 0) {
            await ctx.reply(`Successfully subscribed to group ${group[0]}.${group.slice(1)}`);
        } else {
            await ctx.reply(`You are already subscribed to group ${group[0]}.${group.slice(1)}`);
        }

        const subs = db.prepare("SELECT group_no FROM subscriptions WHERE chat_id = ?").all(chatId) as Array<{ group_no: string }>;
        const formatGroup = (k: string) => k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k;
        const subsList = subs.map(s => formatGroup(s.group_no)).join(", ");
        await ctx.reply(`Your subscriptions: ${subsList}`);
    } catch (error) {
        console.error("Error subscribing:", error);
        await ctx.reply("Failed to subscribe.");
    }
});

bot.command("unsubscribe", async (ctx) => {
    try {
        const group = ctx.match.trim().split(".").join("");
        const chatId = ctx.chat?.id;

        if (!chatId) {
            await ctx.reply("Unable to identify chat.");
            return;
        }

        if (!group || group.length === 0) {
            await ctx.reply("Please specify a group number. Usage: /unsubscribe <group> (e.g., /unsubscribe 21 or /unsubscribe 2.1)");
            return;
        }

        const deleteStmt = db.prepare("DELETE FROM subscriptions WHERE chat_id = ? AND group_no = ?");
        const result = deleteStmt.run(chatId, group);
        deleteStmt.finalize();

        if (result.changes > 0) {
            await ctx.reply(`Successfully unsubscribed from group ${group[0]}.${group.slice(1)}`);
        } else {
            await ctx.reply(`You were not subscribed to group ${group[0]}.${group.slice(1)}`);
        }

        const subs = db.prepare("SELECT group_no FROM subscriptions WHERE chat_id = ?").all(chatId) as Array<{ group_no: string }>;
        if (subs.length > 0) {
            const formatGroup = (k: string) => k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k;
            const subsList = subs.map(s => formatGroup(s.group_no)).join(", ");
            await ctx.reply(`Your remaining subscriptions: ${subsList}`);
        } else {
            await ctx.reply(`You have no active subscriptions.`);
        }
    } catch (error) {
        console.error("Error unsubscribing:", error);
        await ctx.reply("Failed to unsubscribe.");
    }
});

bot.command("settings", async (ctx) => {
    try {
        const chatId = ctx.chat?.id;

        if (!chatId) {
            await ctx.reply("Unable to identify chat.");
            return;
        }

        const subs = db.prepare("SELECT group_no FROM subscriptions WHERE chat_id = ?").all(chatId) as Array<{ group_no: string }>;
        
        if (subs.length === 0) {
            await ctx.reply("You have no active subscriptions.\n\nUse /subscribe <group> to subscribe to blackout updates.");
            return;
        }

        const formatGroup = (k: string) => k.length >= 2 ? `${k[0]}.${k.slice(1)}` : k;
        const subsList = subs.map(s => formatGroup(s.group_no)).join(", ");
        await ctx.reply(`Your active subscriptions:\n${subsList}\n\nUse /subscribe <group> to add more or /unsubscribe <group> to remove.`);
    } catch (error) {
        console.error("Error getting settings:", error);
        await ctx.reply("Failed to retrieve settings.");
    }
});

bot.on("message", async (ctx) => 
    {
        try {
            console.log(ctx.message);

        }
        catch (error) {
            console.error("Error handling message:", error);
        }
    }

);

bot.catch((err) => {
    console.error("Error in bot:", err);
});

await bot.api.setMyCommands([
    { command: "start", description: "Start the bot" },
    { command: "subscribe", description: "Subscribe to updates for a specific group" },
    { command: "unsubscribe", description: "Unsubscribe from updates for a specific group" },
    { command: "settings", description: "Show settings" },
    { command: "fetch", description: "Fetch latest blackout data for a group" },
]);

async function checkAndNotifyBlackouts() {
    try {
        console.log(`[${new Date().toISOString()}] Checking for upcoming blackouts...`);
        
        const { groupsData } = await fetchAllData();
        const now = new Date();
        const currentHour = now.getHours() + 1; // H01 = 0:00-1:00, H02 = 1:00-2:00, etc
        const currentMinute = now.getMinutes();
        const currentDate = now.toISOString().split('T')[0]!; // YYYY-MM-DD
        
        if (currentMinute < 50 || currentMinute > 55) {
            console.log(`[${new Date().toISOString()}] Not in notification window (current minute: ${currentMinute})`);
            return;
        }
        
        const nextHour = currentHour + 1;
        if (nextHour > 24) {
            console.log(`[${new Date().toISOString()}] Next hour is beyond 24 (would be ${nextHour}), skipping`);
            return;
        }
        
        const nextHourKey = `H${nextHour.toString().padStart(2, '0')}`;
        
        const allSubs = db.prepare("SELECT DISTINCT chat_id, group_no FROM subscriptions").all() as Array<{ chat_id: number, group_no: string }>;
        
        for (const sub of allSubs) {
            const groupData = groupsData.get(sub.group_no);
            if (!groupData) continue;
            
            const schedule: Array<Schedule> = (groupData as any).Shedule;
            if (!schedule || !Array.isArray(schedule)) continue;
            
            const todaySchedule = schedule.find(day => day.IsToday === 1);
            if (!todaySchedule) continue;
            
            const currentHourValue = (todaySchedule as any)[`H${currentHour.toString().padStart(2, '0')}`];
            const nextHourValue = (todaySchedule as any)[nextHourKey];
            
            if (currentHourValue === 1 && nextHourValue === 0) {
                const checkStmt = db.prepare("SELECT id FROM sent_notifications WHERE chat_id = ? AND group_no = ? AND notification_type = ? AND hour = ? AND date = ?");
                const existing = checkStmt.get(sub.chat_id, sub.group_no, 'blackout', nextHour, currentDate);
                checkStmt.finalize();
                
                if (existing) {
                    console.log(`[${new Date().toISOString()}] Already sent blackout notification to chat ${sub.chat_id} for group ${sub.group_no} at hour ${nextHour}`);
                    continue;
                }
                
                const minutesUntilBlackout = 60 - currentMinute;
                const message = `BLACKOUT ALERT for group ${sub.group_no[0]}.${sub.group_no.slice(1)}\n\n` +
                    `Electricity will be turned OFF in approximately ${minutesUntilBlackout} minutes (at ${nextHour}:00).\n\n` +
                    `Please prepare your devices!`;
                
                try {
                    await bot.api.sendMessage(sub.chat_id, message);
                    console.log(`[${new Date().toISOString()}] Sent blackout notification to chat ${sub.chat_id} for group ${sub.group_no}`);
                    
                    const insertStmt = db.prepare("INSERT OR IGNORE INTO sent_notifications (chat_id, group_no, notification_type, hour, date) VALUES (?, ?, ?, ?, ?)");
                    insertStmt.run(sub.chat_id, sub.group_no, 'blackout', nextHour, currentDate);
                    insertStmt.finalize();
                } catch (sendError) {
                    console.error(`[${new Date().toISOString()}] Failed to send notification to chat ${sub.chat_id}:`, sendError);
                }
            }
            else if (currentHourValue === 0 && nextHourValue === 1) {
                const checkStmt = db.prepare("SELECT id FROM sent_notifications WHERE chat_id = ? AND group_no = ? AND notification_type = ? AND hour = ? AND date = ?");
                const existing = checkStmt.get(sub.chat_id, sub.group_no, 'restoration', nextHour, currentDate);
                checkStmt.finalize();
                
                if (existing) {
                    console.log(`[${new Date().toISOString()}] Already sent restoration notification to chat ${sub.chat_id} for group ${sub.group_no} at hour ${nextHour}`);
                    continue;
                }
                
                const minutesUntilRestoration = 60 - currentMinute;
                const message = `POWER RESTORATION for group ${sub.group_no[0]}.${sub.group_no.slice(1)}\n\n` +
                    `Electricity will be turned ON in approximately ${minutesUntilRestoration} minutes (at ${nextHour}:00).`;
                
                try {
                    await bot.api.sendMessage(sub.chat_id, message);
                    console.log(`[${new Date().toISOString()}] Sent restoration notification to chat ${sub.chat_id} for group ${sub.group_no}`);
                    
                    const insertStmt = db.prepare("INSERT OR IGNORE INTO sent_notifications (chat_id, group_no, notification_type, hour, date) VALUES (?, ?, ?, ?, ?)");
                    insertStmt.run(sub.chat_id, sub.group_no, 'restoration', nextHour, currentDate);
                    insertStmt.finalize();
                } catch (sendError) {
                    console.error(`[${new Date().toISOString()}] Failed to send notification to chat ${sub.chat_id}:`, sendError);
                }
            }
        }
        
        console.log(`[${new Date().toISOString()}] Finished checking for upcoming blackouts`);
    } catch (error) {
        console.error(`[${new Date().toISOString()}] Error checking blackouts:`, error);
    }
}

async function backgroundFetchJob() {
    console.log("Starting background fetch job...");
    
    while (true) {
        try {
            console.log(`[${new Date().toISOString()}] Running background fetch...`);
            await fetchAllData(true);
            console.log(`[${new Date().toISOString()}] Background fetch completed successfully`);
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error in background fetch:`, error);
        }
        
        await new Promise(resolve => setTimeout(resolve, 5 * 60 * 1000));
    }
}

async function blackoutNotificationJob() {
    console.log("Starting blackout notification job...");
    
    while (true) {
        try {
            await checkAndNotifyBlackouts();
        } catch (error) {
            console.error(`[${new Date().toISOString()}] Error in notification job:`, error);
        }
        
        // Check every minute
        await new Promise(resolve => setTimeout(resolve, 60 * 1000));
    }
}

backgroundFetchJob().catch(err => {
    console.error("Background fetch job crashed:", err);
});

blackoutNotificationJob().catch(err => {
    console.error("Blackout notification job crashed:", err);
});

bot.start();