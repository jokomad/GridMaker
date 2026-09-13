const express = require("express");
const cors = require("cors");
const crypto = require("crypto");
const path = require("path");
const fs = require("fs");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 8000;

// Configuration from Environment Variables (e.g. Northflank)
const BYBIT_API_KEY = process.env.BYBIT_API_KEY || "";
const BYBIT_API_SECRET = process.env.BYBIT_API_SECRET || "";
const IS_TESTNET = process.env.BYBIT_TESTNET === "true" || process.env.BYBIT_TESTNET === "1";

const BYBIT_BASE_URL = IS_TESTNET
    ? "https://api-testnet.bybit.com"
    : "https://api.bybit.com";

app.use(cors());
app.use(express.json());

const { MongoClient } = require("mongodb");

// Persistent local storage files for tracking started bots and autotrader state (fallback if no MongoDB)
const BOTS_STORAGE_FILE = path.join(__dirname, "active_bots.json");
const AUTOTRADER_STORAGE_FILE = path.join(__dirname, "autotrader_state.json");

// MongoDB Configuration (optional, e.g. Northflank MongoDB addon)
const MONGO_URI = process.env.MONGODB_URI || process.env.MONGO_URL || "";
let mongoDb = null;
let botsCollection = null;
let autotraderCollection = null;

// Autotrader In-Memory State
let autotraderState = {
    running: false,
    status: "IDLE",
    investment: 50,
    symbols: [],
    initialFundingBalance: null,
    sessionStartFundingBalance: null,
    realizedProfitTotal: 0,
    startedAt: null,
    lastUpdate: null
};

function loadStoredAutotraderState() {
    try {
        if (fs.existsSync(AUTOTRADER_STORAGE_FILE)) {
            const data = fs.readFileSync(AUTOTRADER_STORAGE_FILE, "utf-8");
            return JSON.parse(data);
        }
    } catch (e) {
        console.error("Error reading stored autotrader state:", e);
    }
    return autotraderState;
}

async function saveStoredAutotraderState(state) {
    autotraderState = { ...autotraderState, ...state, lastUpdate: Date.now() };

    try {
        fs.writeFileSync(AUTOTRADER_STORAGE_FILE, JSON.stringify(autotraderState, null, 2), "utf-8");
    } catch (e) {
        // Ignored in read-only environments
    }

    if (autotraderCollection) {
        try {
            await autotraderCollection.updateOne(
                { _id: "current_autotrader_state" },
                { $set: autotraderState },
                { upsert: true }
            );
        } catch (dbErr) {
            console.warn(`[MONGODB] Error saving autotrader state:`, dbErr.message);
        }
    }
}

async function initMongo() {
    if (!MONGO_URI) {
        console.log(`ℹ️ [STORAGE] No MONGODB_URI configured. Using local/JSON storage.`);
        return;
    }
    try {
        const client = new MongoClient(MONGO_URI);
        await client.connect();
        mongoDb = client.db(process.env.MONGO_DB_NAME || "gridmaker");
        botsCollection = mongoDb.collection("bots");
        autotraderCollection = mongoDb.collection("autotrader");
        console.log(`🍃 [MONGODB] Connected successfully to MongoDB! Persistent storage active.`);

        // Sync stored bots from MongoDB into memory
        const docs = await botsCollection.find({}).toArray();
        if (docs && docs.length > 0) {
            activeBots = docs.map(d => {
                const { _id, ...bot } = d;
                return bot;
            });
            console.log(`🍃 [MONGODB] Loaded ${activeBots.length} bots from MongoDB.`);
        }

        // Sync autotrader state from MongoDB
        const atDoc = await autotraderCollection.findOne({ _id: "current_autotrader_state" });
        if (atDoc) {
            const { _id, ...savedState } = atDoc;
            autotraderState = { ...autotraderState, ...savedState };
            console.log(`🍃 [MONGODB] Loaded persistent Autotrader state (Running: ${autotraderState.running}, Symbols: ${autotraderState.symbols.join(",") || "none"}).`);
        }
    } catch (err) {
        console.warn(`⚠️ [MONGODB] Failed to connect to MongoDB: ${err.message}. Falling back to file storage.`);
    }
}

function loadStoredBots() {
    try {
        if (fs.existsSync(BOTS_STORAGE_FILE)) {
            const data = fs.readFileSync(BOTS_STORAGE_FILE, "utf-8");
            return JSON.parse(data);
        }
    } catch (e) {
        console.error("Error reading stored bots:", e);
    }
    return [];
}

async function saveStoredBots(bots) {
    try {
        fs.writeFileSync(BOTS_STORAGE_FILE, JSON.stringify(bots, null, 2), "utf-8");
    } catch (e) {
        // Ignored in read-only environments
    }

    if (botsCollection) {
        try {
            for (const bot of bots) {
                await botsCollection.updateOne(
                    { id: bot.id },
                    { $set: bot },
                    { upsert: true }
                );
            }
        } catch (dbErr) {
            console.warn(`[MONGODB] Error saving bot:`, dbErr.message);
        }
    }
}

let activeBots = loadStoredBots();
let autotraderStateLoaded = loadStoredAutotraderState();
autotraderState = { ...autotraderState, ...autotraderStateLoaded };
initMongo().catch(console.error);

// -------------------------------------------------------------
// BYBIT SERVER TIME SYNCHRONIZATION
// -------------------------------------------------------------
let serverTimeOffset = 0; // difference between Bybit server time and local time in ms
let lastSyncTime = 0;

/**
 * Synchronize clock with Bybit server time
 */
async function syncServerTime() {
    try {
        const start = Date.now();
        const res = await fetch(`${BYBIT_BASE_URL}/v5/market/time`);
        const data = await res.json();
        const end = Date.now();

        if (data && data.time) {
            const bybitServerTime = Number(data.time);
            const latency = (end - start) / 2;
            const estimatedLocal = start + latency;
            serverTimeOffset = Math.round(bybitServerTime - estimatedLocal);
            lastSyncTime = Date.now();
            console.log(`[TIME SYNC] Synchronized with Bybit server clock. Offset: ${serverTimeOffset > 0 ? '+' : ''}${serverTimeOffset}ms`);
        }
    } catch (e) {
        console.warn(`[TIME SYNC] Failed to sync time with Bybit:`, e.message);
    }
}

/**
 * Get synchronized Bybit timestamp
 */
function getBybitTimestamp() {
    // Re-sync if older than 5 minutes
    if (Date.now() - lastSyncTime > 5 * 60 * 1000) {
        syncServerTime().catch(() => { });
    }
    return String(Date.now() + serverTimeOffset);
}

// Initial sync immediately and periodically every 5 minutes
syncServerTime();
setInterval(syncServerTime, 5 * 60 * 1000);

/**
 * Generate Bybit V5 HMAC-SHA256 signature
 */
function generateSignature(timestamp, apiKey, recvWindow, payloadStr, secret) {
    const message = timestamp + apiKey + recvWindow + payloadStr;
    return crypto.createHmac("sha256", secret).update(message).digest("hex");
}

/**
 * Send signed Bybit V5 API request with synchronized timestamp
 */
async function bybitSignedRequest(method, endpoint, params = {}) {
    if (!BYBIT_API_KEY || !BYBIT_API_SECRET) {
        throw new Error("Bybit API Key and Secret are not configured in Northflank environment variables.");
    }

    const timestamp = getBybitTimestamp();
    const recvWindow = "10000"; // 10s window to comfortably absorb network variance

    let url = BYBIT_BASE_URL + endpoint;
    let bodyStr = "";

    if (method === "GET") {
        const queryParams = new URLSearchParams(params).toString();
        if (queryParams) {
            url += "?" + queryParams;
            bodyStr = queryParams;
        }
    } else {
        bodyStr = JSON.stringify(params);
    }

    const signature = generateSignature(timestamp, BYBIT_API_KEY, recvWindow, bodyStr, BYBIT_API_SECRET);

    const headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-SIGN": signature,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recvWindow,
        "Content-Type": "application/json"
    };

    const fetchOptions = {
        method,
        headers
    };

    if (method !== "GET" && bodyStr) {
        fetchOptions.body = bodyStr;
    }

    const response = await fetch(url, fetchOptions);
    const data = await response.json();

    return { httpStatus: response.status, data };
}

// -------------------------------------------------------------
// API ENDPOINTS
// -------------------------------------------------------------

/**
 * Health check & status endpoint
 */
app.get("/api/health", (req, res) => {
    res.json({
        status: "ok",
        hasApiKey: !!BYBIT_API_KEY,
        isTestnet: IS_TESTNET,
        hasMongo: !!botsCollection,
        activeBotsCount: activeBots.filter(b => b.status === "RUNNING").length
    });
});

/**
 * Create Neutral Grid Bot
 * Parameters expected: symbol, lowerPrice, upperPrice, gridCount, leverage (default 10), investment (default 50)
 */
app.post("/api/bot/create", async (req, res) => {
    try {
        const {
            symbol,
            lowerPrice,
            upperPrice,
            stopLossPrice,
            stopLossRatio,
            gridCount,
            leverage = 10,
            investment = 50,
            currentPrice
        } = req.body;

        if (!symbol || !lowerPrice || !upperPrice || !gridCount) {
            return res.status(400).json({
                success: false,
                message: "Missing required parameters: symbol, lowerPrice, upperPrice, or gridCount."
            });
        }

        const invAmount = Number(investment) || 50;
        const lev = Number(leverage) || 10;
        const grids = parseInt(gridCount, 10);
        const lower = Number(lowerPrice);
        const upper = Number(upperPrice);
        const slPrice = stopLossPrice ? Number(stopLossPrice) : null;
        const slRatio = stopLossRatio ? Number(stopLossRatio) : (slPrice ? null : 0.25);

        if (!BYBIT_API_KEY || !BYBIT_API_SECRET) {
            return res.status(400).json({
                success: false,
                message: "Bybit API Key or Secret is missing in environment variables."
            });
        }

        console.log(`[BOT CREATE] Launching Neutral Grid Bot for ${symbol}: Lower=${lower}, Upper=${upper}, Grids=${grids}, Lev=${lev}x, Invest=${invAmount} USDT, SL Ratio=${slRatio}, SL Price=${slPrice}`);

        // Bybit V5 Futures Grid Bot payload:
        // direction: 3 (Neutral), grid_mode: 1 (Neutral), grid_type: 1 (Arithmetic)
        // TP/SL documentation: https://bybit-exchange.github.io/docs/v5/bot/futures-grid/create
        // tp_sl_type: 1 = Both TP and SL by percentage
        // stop_loss_per: Stop-loss percentage value (e.g. "25" means 25%, "0.25" means 0.25%)
        const fgridPayload = {
            symbol: symbol,
            direction: 3,
            leverage: String(lev),
            min_price: String(lower),
            max_price: String(upper),
            cell_number: String(grids),
            total_investment: String(invAmount),
            grid_mode: 1,
            grid_type: 1
        };

        if (slRatio) {
            fgridPayload.tp_sl_type = 1;
            // If passed as decimal ratio like 0.25, convert to whole percentage "25"
            const slPercentValue = slRatio <= 1 ? (slRatio * 100) : slRatio;
            fgridPayload.stop_loss_per = String(slPercentValue);
        } else if (slPrice) {
            fgridPayload.tp_sl_type = 2;
            fgridPayload.stop_loss_price = String(slPrice);
        }

        const result = await bybitSignedRequest("POST", "/v5/fgridbot/create", fgridPayload);
        const bybitData = result.data;

        console.log(`[BOT CREATE] Bybit response:`, JSON.stringify(bybitData));

        if (!bybitData || bybitData.retCode !== 0) {
            const errorMsg = bybitData ? (bybitData.retMsg || JSON.stringify(bybitData)) : "No response from Bybit";
            return res.status(400).json({
                success: false,
                message: `Bybit error: ${errorMsg}`,
                bybitData: bybitData
            });
        }

        const botId = bybitData.result?.bot_id;

        if (!botId || botId === "0") {
            const debugMsg = bybitData.result?.debug_msg || bybitData.result?.ban_reason_text || "Failed to initialize bot";
            return res.status(400).json({
                success: false,
                message: `Bybit rejected bot: ${debugMsg}`,
                bybitData: bybitData
            });
        }

        // Fetch live bot initial state directly from Bybit
        let liveDetail = null;
        try {
            const detailRes = await bybitSignedRequest("POST", "/v5/fgridbot/detail", { bot_id: String(botId) });
            liveDetail = detailRes.data?.result?.detail;
        } catch (detailErr) {
            console.warn("Could not immediately fetch initial bot detail:", detailErr.message);
        }

        const newBot = {
            id: String(botId),
            symbol: symbol,
            direction: "Neutral",
            leverage: lev,
            lowerPrice: lower,
            upperPrice: upper,
            stopLossRatio: slRatio,
            stopLossPrice: slPrice,
            gridCount: grids,
            investment: invAmount,
            entryPrice: Number(liveDetail?.entry_price) || Number(currentPrice) || (lower + upper) / 2,
            currentPrice: Number(liveDetail?.last_price) || Number(currentPrice) || (lower + upper) / 2,
            status: "RUNNING",
            pnl: Number(liveDetail?.pnl) || 0,
            pnlPercent: Number(liveDetail?.pnl_per) || 0,
            realizedPnl: Number(liveDetail?.realised_pnl) || 0,
            unrealizedPnl: Number(liveDetail?.unrealised_pnl) || 0,
            arbitrageNum: Number(liveDetail?.arbitrage_num) || 0,
            startTime: Date.now(),
            bybitBotId: String(botId)
        };

        // Remove any existing stopped or previous entry for this symbol
        activeBots = activeBots.filter(b => b.symbol !== symbol || b.status !== "RUNNING");
        activeBots.unshift(newBot);
        saveStoredBots(activeBots);

        res.json({
            success: true,
            bot: newBot,
            message: `Neutral Grid Bot for ${symbol} launched on Bybit! (Bot ID: ${botId})`
        });

    } catch (error) {
        console.error("Error creating grid bot:", error);
        res.status(500).json({
            success: false,
            message: error.message || "Failed to create grid bot."
        });
    }
});

/**
 * List all active grid bots with current PnL
 */
app.get("/api/bot/list", async (req, res) => {
    try {
        // If client provides known bot IDs (e.g. from browser localStorage when Northflank restarts fresh)
        const clientBotIdsParam = req.query.knownIds;
        if (clientBotIdsParam && typeof clientBotIdsParam === "string") {
            const clientIds = clientBotIdsParam.split(",").map(s => s.trim()).filter(Boolean);
            for (const cId of clientIds) {
                if (!activeBots.some(b => b.id === cId)) {
                    // Temporarily add to activeBots to fetch live details from Bybit
                    activeBots.push({ id: cId, status: "RUNNING", symbol: "SYNC", startTime: Date.now() });
                }
            }
        }

        const runningBots = activeBots.filter(b => b.status === "RUNNING");

        // Sync live status and real PnL directly from Bybit
        if (BYBIT_API_KEY && BYBIT_API_SECRET && runningBots.length > 0) {
            for (const bot of runningBots) {
                if (bot.id && !bot.id.startsWith("bot_")) {
                    try {
                        const detailRes = await bybitSignedRequest("POST", "/v5/fgridbot/detail", { bot_id: String(bot.id) });
                        const detail = detailRes.data?.result?.detail;

                        if (detail) {
                            if (detail.status === "FUTURE_GRID_STATUS_TERMINATED" || detail.status === "FUTURE_GRID_STATUS_CLOSED") {
                                bot.status = "STOPPED";
                            }
                            bot.symbol = detail.symbol || bot.symbol;
                            bot.direction = "Neutral";
                            bot.leverage = Number(detail.leverage) || bot.leverage || 10;
                            bot.lowerPrice = Number(detail.min_price) || bot.lowerPrice;
                            bot.upperPrice = Number(detail.max_price) || bot.upperPrice;
                            bot.gridCount = Number(detail.cell_number) || bot.gridCount;
                            bot.investment = Number(detail.total_investment) || bot.investment;
                            bot.entryPrice = Number(detail.entry_price) || bot.entryPrice;
                            bot.currentPrice = Number(detail.last_price) || bot.currentPrice;
                            bot.pnl = Number(detail.pnl) || 0;
                            bot.pnlPercent = Number(detail.pnl_per) || 0;
                            bot.realizedPnl = Number(detail.realised_pnl) || 0;
                            bot.unrealizedPnl = Number(detail.unrealised_pnl) || 0;
                            bot.arbitrageNum = Number(detail.arbitrage_num) || 0;
                            if (detail.stop_loss_per) {
                                bot.stopLossRatio = Number(detail.stop_loss_per);
                            }
                            if (detail.stop_loss_price) {
                                bot.stopLossPrice = Number(detail.stop_loss_price);
                            }
                            if (detail.tp_sl_type) {
                                bot.tpSlType = detail.tp_sl_type;
                            }
                            if (detail.create_time) {
                                bot.startTime = Number(detail.create_time);
                            }
                        }
                    } catch (syncErr) {
                        console.warn(`Could not sync detail for bot ${bot.id}:`, syncErr.message);
                    }
                }
            }
        }

        // Cap stopped bots in memory & database to latest 30 to prevent unbounded memory growth over months of recycling
        const runningList = activeBots.filter(b => b.status === "RUNNING");
        const stoppedList = activeBots.filter(b => b.status === "STOPPED").slice(0, 30);
        activeBots = [...runningList, ...stoppedList];

        saveStoredBots(activeBots);

        const currentRunning = runningList;
        const totalInvestment = currentRunning.reduce((sum, b) => sum + (Number(b.investment) || 0), 0);
        const totalPnl = currentRunning.reduce((sum, b) => sum + (Number(b.pnl) || 0), 0);
        const totalPnlPercent = totalInvestment > 0 ? (totalPnl / totalInvestment) * 100 : 0;

        res.json({
            success: true,
            bots: currentRunning,
            history: activeBots.filter(b => b.status === "STOPPED").slice(0, 10),
            summary: {
                totalActiveBots: currentRunning.length,
                totalInvestment: Number(totalInvestment.toFixed(2)),
                totalPnl: Number(totalPnl.toFixed(2)),
                totalPnlPercent: Number(totalPnlPercent.toFixed(2))
            }
        });

    } catch (error) {
        console.error("Error listing bots:", error);
        res.status(500).json({
            success: false,
            message: error.message
        });
    }
});

/**
 * Stop single Grid Bot on Bybit
 */
app.post("/api/bot/stop", async (req, res) => {
    try {
        const { botId, symbol } = req.body;
        const bot = activeBots.find(b => (botId && b.id === botId) || (symbol && b.symbol === symbol && b.status === "RUNNING"));

        if (!bot) {
            return res.status(404).json({ success: false, message: "Bot not found or already stopped." });
        }

        if (BYBIT_API_KEY && BYBIT_API_SECRET && bot.id && !bot.id.startsWith("bot_")) {
            console.log(`[BOT STOP] Closing bot ${bot.id} (${bot.symbol}) on Bybit...`);
            try {
                const closeRes = await bybitSignedRequest("POST", "/v5/fgridbot/close", {
                    bot_id: String(bot.id),
                    close_type: 1
                });
                console.log(`[BOT STOP] Bybit close response:`, JSON.stringify(closeRes.data));
            } catch (err) {
                console.warn(`Could not close bot ${bot.id} via Bybit API:`, err.message);
            }
        }

        bot.status = "STOPPED";
        bot.stoppedAt = Date.now();
        saveStoredBots(activeBots);

        res.json({
            success: true,
            message: `Grid Bot for ${bot.symbol} has been stopped on Bybit.`
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

/**
 * Query Funding Account USDT Balance
 */
app.get("/api/account/funding-balance", async (req, res) => {
    try {
        if (!BYBIT_API_KEY || !BYBIT_API_SECRET) {
            return res.json({
                success: true,
                walletBalance: 0,
                transferBalance: 0,
                isMock: true,
                message: "No API keys configured"
            });
        }

        const queryParams = {
            accountType: "FUND",
            coin: "USDT"
        };

        const result = await bybitSignedRequest("GET", "/v5/asset/transfer/query-account-coin-balance", queryParams);

        if (result.httpStatus === 200 && result.data && result.data.retCode === 0) {
            const bal = (result.data.result && result.data.result.balance) || {};
            const walletBalance = parseFloat(bal.walletBalance || bal.transferBalance || "0") || 0;
            const transferBalance = parseFloat(bal.transferBalance || bal.walletBalance || "0") || 0;

            return res.json({
                success: true,
                walletBalance,
                transferBalance,
                raw: bal
            });
        } else {
            return res.status(500).json({
                success: false,
                message: result.data ? result.data.retMsg : "Failed to query funding balance",
                raw: result.data
            });
        }
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

/**
 * Stop All Active Grid Bots
 */
app.post("/api/bot/stop-all", async (req, res) => {
    try {
        const runningBots = activeBots.filter(b => b.status === "RUNNING");

        if (runningBots.length === 0) {
            return res.json({ success: true, message: "No running bots to stop.", stoppedCount: 0 });
        }

        for (const bot of runningBots) {
            bot.status = "STOPPED";
            bot.stoppedAt = Date.now();

            if (BYBIT_API_KEY && BYBIT_API_SECRET && bot.id && !bot.id.startsWith("bot_")) {
                try {
                    await bybitSignedRequest("POST", "/v5/fgridbot/close", {
                        bot_id: String(bot.id),
                        close_type: 1
                    });
                } catch (e) {
                    console.warn(`Failed to close bot ${bot.id}:`, e.message);
                }
            }
        }

        saveStoredBots(activeBots);

        res.json({
            success: true,
            stoppedCount: runningBots.length,
            message: `All ${runningBots.length} active grid bots have been stopped on Bybit.`
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

/**
 * Get Persistent Autotrader State (from MongoDB / local storage)
 */
app.get("/api/autotrader/state", (req, res) => {
    res.json({
        success: true,
        state: autotraderState
    });
});

/**
 * Save / Update Persistent Autotrader State (saves to MongoDB and file backup)
 */
app.post("/api/autotrader/state", async (req, res) => {
    try {
        const updateData = req.body || {};
        await saveStoredAutotraderState(updateData);
        res.json({
            success: true,
            state: autotraderState
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    }
});

// -------------------------------------------------------------
// STATIC FILE SERVING
// -------------------------------------------------------------

// Serve GridMaker.html as the root page
app.get("/", (req, res) => {
    res.sendFile(path.join(__dirname, "GridMaker.html"));
});

// Serve static assets from project directory
app.use(express.static(__dirname));

app.listen(PORT, () => {
    console.log(`====================================================`);
    console.log(`🚀 Bybit GridMaker Server running on port ${PORT}`);
    console.log(`📡 Bybit API configured: ${BYBIT_API_KEY ? "YES (from ENV)" : "NO (Mock / Read-Only Mode)"}`);
    console.log(`🌐 Network: ${IS_TESTNET ? "Testnet" : "Mainnet"}`);
    console.log(`====================================================`);
});
