require("dotenv").config();
const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");
const { PassThrough } = require("stream");
let PImage = null;
try { PImage = require("pureimage"); } catch {}

const {
  Client,
  GatewayIntentBits,
  EmbedBuilder,
  AttachmentBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  PermissionsBitField,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  SlashCommandBuilder,
} = require("discord.js");

// ====== STAGE 3: PEOPLE RATING BOT ======
// Этот файл — финальный этап. Он включает:
// stage 2 — поток карточек, личные оценки, матрицу голосов и расширенный «Мой статус»
// stage 3 — финальную агрегацию, двухэтажную чёрную строку «не знают» и полную полировку логики

const DISCORD_TOKEN = process.env.DISCORD_TOKEN;
const GUILD_ID = process.env.GUILD_ID;
const MOD_ROLE_ID = process.env.MOD_ROLE_ID || "";
const LOG_CHANNEL_ID = process.env.LOG_CHANNEL_ID || "";
const GRAPHIC_TIERLIST_CHANNEL_ID = process.env.GRAPHIC_TIERLIST_CHANNEL_ID || "";
const GRAPHIC_TIERLIST_TITLE = process.env.GRAPHIC_TIERLIST_TITLE || "People Tier List";
const ROOT_COMMAND_NAME = String(process.env.ROOT_COMMAND_NAME || "rater")
  .trim()
  .toLowerCase()
  .replace(/[^a-z0-9_-]/g, "")
  .slice(0, 32) || "rater";

const DB_PATH = process.env.DB_PATH || path.join(__dirname, "db.json");
const GRAPHIC_AVATAR_DISK_DIR = process.env.GRAPHIC_AVATAR_CACHE_DIR || path.join(__dirname, "graphic_avatar_cache");

const DEFAULT_GRAPHIC_MESSAGE_TEXT = [
  "Коллективный тир-лист людей сервера.",
  "Финальный этап активен: кнопка «Оценивать» запускает личные карточки с 5 тирами и «не знаю».",
  "Кнопка «Мой статус» показывает твой прогресс, личный тир-лист, последние действия и текущую статистику по голосам.",
].join(" ");

const DEFAULT_GRAPHIC_GUIDE_TEXT = [
  "Как пользоваться тир-листом.",
  "1. Нажми «Оценивать», чтобы открыть карточки людей.",
  "2. Ставь тир от 5 до 1 или жми «Не знаю», если человека не знаешь.",
  "3. «Мой статус» показывает твой прогресс, личный тир-лист и статистику по тебе.",
  "4. «Оценить заново» запускает полную переоценку твоего личного тир-листа.",
  "5. Итоговый общий PNG тир-лист обновляется после изменений.",
].join("\n");

const BOARD_ROW_ORDER = ["5", "4", "3", "2", "1", "unknown", "new"];
const NUMERIC_ROW_IDS = new Set(["1", "2", "3", "4", "5"]);

const DEFAULT_ROW_LABELS = {
  "5": "5",
  "4": "4",
  "3": "3",
  "2": "2",
  "1": "1",
  unknown: "не знают",
  new: "новые",
};

const DEFAULT_ROW_COLORS = {
  "5": "#ff6b6b",
  "4": "#ff9f43",
  "3": "#feca57",
  "2": "#1dd1a1",
  "1": "#54a0ff",
  unknown: "#101010",
  new: "#8f9bb3",
};

const DEFAULT_ROW_ICON_SCALES = {
  "5": 1,
  "4": 1,
  "3": 1,
  "2": 1,
  "1": 1,
  unknown: 0.56,
  new: 1,
};

const RATING_BUTTON_ORDER = ["5", "4", "3", "2", "1", "unknown"];
const PERSONAL_TIER_ORDER = ["5", "4", "3", "2", "1", "unknown"];
const SESSION_HISTORY_LIMIT = Math.max(10, Number(process.env.SESSION_HISTORY_LIMIT) || 24);
const NEW_ROW_EXIT_THRESHOLD = 3;
const UNKNOWN_ROW_PERCENT_THRESHOLD = 30;


const STAGE_PLAN = {
  stage1: [
    "удалить доменную логику ELO, pending, review и submit-channel",
    "перевести базу с ratings/submissions на people/votes/sessions",
    "сохранить и обобщить PNG-движок, кэш аватарок и панель PNG",
    "подготовить 7-полосную модель доски и кнопки «Начать оценку» / «Мой статус»",
    "добавить команды админа для ручного заноса людей в пул и быстрой массовой заготовки",
  ],
  stage2: [
    "сделать личные сессии оценки по карточкам",
    "добавить 6 кнопок под карточкой: 5 тиров и «не знаю»",
    "начать хранить матрицу голосов evaluator -> target",
    "собрать заготовку личного мини-тирлиста и прогресса оценщика",
    "добавить нормальную кнопку «Мой статус» с текущим прогрессом и последним личным раскладом",
  ],
  stage3: [
    "включить финальную агрегацию общего тир-листа по средним оценкам и распределению голосов",
    "полностью включить чёрную полосу «не знают» с двухэтажной раскладкой и нижнюю «не оценивали»",
    "сделать авто-подбор следующей карточки и защиту от самоголоса",
    "дописать статистику по каждому человеку и по каждому оценщику",
    "финально перепроверить миграцию, кнопки, PNG-панель, слэш-команды и интеграцию",
  ],
};

// ====== DB ======
function loadDB() {
  if (!fs.existsSync(DB_PATH)) {
    return { config: {}, people: {}, votes: {}, comments: {}, sessions: {}, meta: {}, legacy: {} };
  }
  try {
    const data = JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
    data.config ||= {};
    data.people ||= {};
    data.votes ||= {};
    data.comments ||= {};
    data.sessions ||= {};
    data.meta ||= {};
    data.legacy ||= {};
    return data;
  } catch {
    return { config: {}, people: {}, votes: {}, comments: {}, sessions: {}, meta: {}, legacy: {} };
  }
}

function ensureDirForFile(filePath) {
  const dir = path.dirname(filePath);
  if (dir) fs.mkdirSync(dir, { recursive: true });
}

function saveDB(db) {
  ensureDirForFile(DB_PATH);
  const tmpPath = `${DB_PATH}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(db, null, 2), "utf8");
  fs.renameSync(tmpPath, DB_PATH);
}

const db = loadDB();

function applyDbDefaults() {
  db.meta ||= {};
  db.meta.schemaVersion = 2;
  db.meta.stage = 3;
  db.meta.updatedAt = new Date().toISOString();

  db.config ||= {};
  db.config.rootCommandName = ROOT_COMMAND_NAME;
  db.config.autoJoinOnStartRating = true;
  db.config.stagePlan ||= STAGE_PLAN;
  db.config.sessionHistoryLimit ||= SESSION_HISTORY_LIMIT;
  db.config.ratingFlow ||= { unknownLabel: "Не знаю", enablePreviewAggregation: true };
  db.config.rowLabels ||= { ...DEFAULT_ROW_LABELS };
  db.config.rowColors ||= { ...DEFAULT_ROW_COLORS };
  db.config.rowIconScales ||= { ...DEFAULT_ROW_ICON_SCALES };
  db.config.coefficients ||= {
    globalRowWeights: { "5": 1, "4": 1, "3": 1, "2": 1, "1": 1, unknown: 1 },
    evaluatorWeights: {},
    targetBiases: {},
    rowInfluenceWeights: { "5": 1, "4": 1, "3": 1, "2": 1, "1": 1, unknown: 1, new: 1 },
  };
  db.comments ||= {};

  for (const rowId of BOARD_ROW_ORDER) {
    if (!db.config.rowLabels[rowId]) db.config.rowLabels[rowId] = DEFAULT_ROW_LABELS[rowId];
    if (!db.config.rowColors[rowId]) db.config.rowColors[rowId] = DEFAULT_ROW_COLORS[rowId];
    if (!db.config.rowIconScales[rowId]) db.config.rowIconScales[rowId] = DEFAULT_ROW_ICON_SCALES[rowId];
  }

  for (const key of ["5", "4", "3", "2", "1", "unknown"]) {
    const raw = Number(db.config.coefficients.globalRowWeights?.[key]);
    db.config.coefficients.globalRowWeights[key] = Number.isFinite(raw) && raw > 0 ? raw : 1;
  }
  for (const key of ["5", "4", "3", "2", "1", "unknown", "new"]) {
    db.config.coefficients.rowInfluenceWeights[key] = 1;
  }
  db.config.coefficients.evaluatorWeights ||= {};
  db.config.coefficients.targetBiases ||= {};
  db.config.coefficients.rowInfluenceWeights ||= { "5": 1, "4": 1, "3": 1, "2": 1, "1": 1, unknown: 1, new: 1 };

  db.config.graphicTierlist ||= {
    title: GRAPHIC_TIERLIST_TITLE,
    dashboardChannelId: GRAPHIC_TIERLIST_CHANNEL_ID || "",
    dashboardMessageId: "",
    lastUpdated: 0,
    image: { width: null, height: null, icon: null },
    panel: { selectedRowId: "5" },
    layout: { unknownBandRows: 2 },
    messageText: DEFAULT_GRAPHIC_MESSAGE_TEXT,
    guideText: DEFAULT_GRAPHIC_GUIDE_TEXT,
  };

  db.config.graphicTierlist.image ||= { width: null, height: null, icon: null };
  db.config.graphicTierlist.panel ||= { selectedRowId: "5" };
  db.config.graphicTierlist.layout ||= { unknownBandRows: 2 };
  if (!Number.isFinite(Number(db.config.graphicTierlist.layout.unknownBandRows)) || Number(db.config.graphicTierlist.layout.unknownBandRows) < 1) {
    db.config.graphicTierlist.layout.unknownBandRows = 2;
  } else {
    db.config.graphicTierlist.layout.unknownBandRows = Math.max(1, Math.min(4, Math.round(Number(db.config.graphicTierlist.layout.unknownBandRows))));
  }
  if (!db.config.graphicTierlist.panel.selectedRowId) db.config.graphicTierlist.panel.selectedRowId = "5";
  if (!db.config.graphicTierlist.title) db.config.graphicTierlist.title = GRAPHIC_TIERLIST_TITLE;
  if (!db.config.graphicTierlist.messageText) db.config.graphicTierlist.messageText = DEFAULT_GRAPHIC_MESSAGE_TEXT;
  if (!db.config.graphicTierlist.guideText) db.config.graphicTierlist.guideText = DEFAULT_GRAPHIC_GUIDE_TEXT;
  if (!db.config.graphicTierlist.dashboardChannelId && GRAPHIC_TIERLIST_CHANNEL_ID) {
    db.config.graphicTierlist.dashboardChannelId = GRAPHIC_TIERLIST_CHANNEL_ID;
  }
}

function migrateLegacyRatingsIfNeeded() {
  // Мягкая миграция: если был старый ELO-бот с db.ratings, переносим людей в новый people-пул.
  const raw = (() => {
    try { return JSON.parse(fs.readFileSync(DB_PATH, "utf8")); } catch { return null; }
  })();
  const legacyRatings = raw?.ratings && typeof raw.ratings === "object" ? raw.ratings : null;
  if (!legacyRatings) return { imported: 0, skipped: 0 };

  let imported = 0;
  let skipped = 0;
  for (const [userId, rating] of Object.entries(legacyRatings)) {
    if (!userId || !rating) {
      skipped++;
      continue;
    }

    const existing = db.people[userId] || {};
    const now = new Date().toISOString();
    db.people[userId] = {
      userId,
      name: existing.name || rating.name || userId,
      username: existing.username || String(rating.username || "").trim() || rating.name || userId,
      avatarUrl: existing.avatarUrl || normalizeDiscordAvatarUrl(rating.avatarUrl || ""),
      createdAt: existing.createdAt || now,
      updatedAt: now,
      source: existing.source || "legacy-rating-import",
      stage1PinnedRowId: existing.stage1PinnedRowId || (rating.tier ? String(rating.tier) : ""),
      legacy: {
        ...(existing.legacy || {}),
        elo: Number(rating.elo) || 0,
        tier: rating.tier ? String(rating.tier) : "",
        importedAt: now,
      },
    };
    imported++;
  }

  db.legacy ||= {};
  db.legacy.migratedFromRatings = imported > 0;
  db.legacy.importedRatings = imported;
  db.legacy.migratedAt = imported > 0 ? new Date().toISOString() : (db.legacy.migratedAt || null);
  return { imported, skipped };
}

applyDbDefaults();
const migrationInfo = migrateLegacyRatingsIfNeeded();
refreshAllPeopleDerivedState();
saveDB(db);

// ====== HELPERS ======
let _guildCache = null;
let graphicFontsReady = false;
let GRAPHIC_FONT_REG = "GraphicFontRegular";
let GRAPHIC_FONT_BOLD = "GraphicFontBold";
let GRAPHIC_FONT_INFO = { regularFile: null, boldFile: null, usedFallback: false, source: "none", loadError: null };
const graphicAvatarCache = new Map();

const voteAuditPanels = new Map();
const VOTE_AUDIT_PAGE_SIZES = [5, 10, 15];
const VOTE_AUDIT_TTL_MS = 1000 * 60 * 30;

function nowIso() {
  return new Date().toISOString();
}

function makeId() {
  return (Date.now().toString(36) + Math.random().toString(36).slice(2, 10)).toUpperCase();
}

function isModerator(member) {
  if (!member) return false;
  if (member.permissions?.has(PermissionsBitField.Flags.Administrator)) return true;
  if (MOD_ROLE_ID && member.roles?.cache?.has(MOD_ROLE_ID)) return true;
  return false;
}

async function getGuild(client) {
  if (_guildCache) return _guildCache;
  if (!GUILD_ID) return null;
  _guildCache = await client.guilds.fetch(GUILD_ID).catch(() => null);
  return _guildCache;
}

async function logLine(client, text) {
  if (!LOG_CHANNEL_ID) return;
  const ch = await client.channels.fetch(LOG_CHANNEL_ID).catch(() => null);
  if (ch?.isTextBased()) await ch.send(text).catch(() => {});
}

function normalizeHexColor(input) {
  const raw = String(input || "").trim();
  const m = raw.match(/^#?([0-9a-fA-F]{6})$/);
  if (!m) return null;
  return `#${m[1].toLowerCase()}`;
}

function hexToRgb(hex) {
  const h = String(hex || "#cccccc").replace("#", "");
  const n = parseInt(h, 16);
  return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
}

function fillColor(ctx, hex) {
  const { r, g, b } = hexToRgb(hex);
  ctx.fillStyle = `rgb(${r},${g},${b})`;
}

function sanitizeFileName(name, fallbackExt = "png") {
  const base = String(name || "")
    .replace(/[^a-zA-Z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80);

  if (!base) return `file.${fallbackExt}`;
  if (!/\.[a-z0-9]{2,5}$/i.test(base)) return `${base}.${fallbackExt}`;
  return base;
}

function isDiscordCdnUrl(url) {
  if (!url) return false;
  try {
    const host = new URL(url).hostname.toLowerCase();
    return host === "cdn.discordapp.com" || host === "media.discordapp.net";
  } catch {
    return false;
  }
}

function normalizeDiscordAvatarUrl(url) {
  if (!url) return "";
  try {
    const u = new URL(url);
    if (!isDiscordCdnUrl(u.toString())) return u.toString();
    u.pathname = (u.pathname || "").replace(/\.(webp|gif|jpg|jpeg)$/i, ".png");
    u.searchParams.set("size", "256");
    u.searchParams.delete("width");
    u.searchParams.delete("height");
    return u.toString();
  } catch {
    return String(url || "");
  }
}

async function downloadToBuffer(url, timeoutMs = 15000) {
  const headers = {
    "User-Agent": "Mozilla/5.0 ChatGPTBot/1.0",
    "Accept": "image/avif,image/webp,image/apng,image/png,image/jpeg,*/*;q=0.8",
  };

  if (typeof fetch === "function") {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal, headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const ab = await res.arrayBuffer();
      return Buffer.from(ab);
    } finally {
      clearTimeout(t);
    }
  }

  return await new Promise((resolve, reject) => {
    const lib = url.startsWith("https:") ? https : http;
    const req = lib.get(url, { headers }, (res) => {
      if (res.statusCode && res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        downloadToBuffer(res.headers.location, timeoutMs).then(resolve).catch(reject);
        return;
      }
      if (res.statusCode !== 200) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks = [];
      res.on("data", (d) => chunks.push(d));
      res.on("end", () => resolve(Buffer.concat(chunks)));
    });
    req.on("error", reject);
    req.setTimeout(timeoutMs, () => req.destroy(new Error("timeout")));
  });
}

function ensureGraphicAvatarDiskDir() {
  try { fs.mkdirSync(GRAPHIC_AVATAR_DISK_DIR, { recursive: true }); } catch {}
}

function getGraphicAvatarDiskPath(userId) {
  ensureGraphicAvatarDiskDir();
  return path.join(GRAPHIC_AVATAR_DISK_DIR, `${String(userId || "unknown")}.png`);
}

async function decodeImageFromBuffer(buf) {
  if (!PImage || !buf) return null;
  try { return await PImage.decodePNGFromStream(bufferToPassThrough(buf)); } catch {}
  try { return await PImage.decodeJPEGFromStream(bufferToPassThrough(buf)); } catch {}
  return null;
}

function bufferToPassThrough(buf) {
  const s = new PassThrough();
  s.end(buf);
  return s;
}

async function loadGraphicAvatarFromDisk(userId) {
  if (!userId) return null;
  const fp = getGraphicAvatarDiskPath(userId);
  if (!fs.existsSync(fp)) return null;
  try {
    const buf = fs.readFileSync(fp);
    const img = await decodeImageFromBuffer(buf);
    if (!img) return null;
    graphicAvatarCache.set(`disk:${userId}`, img);
    return img;
  } catch {
    return null;
  }
}

function saveGraphicAvatarBufferToDisk(userId, buf) {
  if (!userId || !buf?.length) return false;
  try {
    fs.writeFileSync(getGraphicAvatarDiskPath(userId), buf);
    return true;
  } catch {
    return false;
  }
}

function clearGraphicAvatarCache() {
  graphicAvatarCache.clear();
  try {
    if (fs.existsSync(GRAPHIC_AVATAR_DISK_DIR)) {
      for (const f of fs.readdirSync(GRAPHIC_AVATAR_DISK_DIR)) {
        try { fs.unlinkSync(path.join(GRAPHIC_AVATAR_DISK_DIR, f)); } catch {}
      }
    }
  } catch {}
}

function getStagePlanText() {
  const out = [];
  out.push("stage 1");
  for (const line of STAGE_PLAN.stage1) out.push(`- ${line}`);
  out.push("");
  out.push("stage 2");
  for (const line of STAGE_PLAN.stage2) out.push(`- ${line}`);
  out.push("");
  out.push("stage 3");
  for (const line of STAGE_PLAN.stage3) out.push(`- ${line}`);
  return out.join("\n");
}

// ====== PEOPLE / VOTES / SESSIONS BASE ======
function canonicalRowId(input) {
  const raw = String(input || "").trim().toLowerCase();
  if (!raw) return "";
  if (BOARD_ROW_ORDER.includes(raw)) return raw;
  return "";
}

function normalizeVoteValue(input) {
  const raw = String(input || "").trim().toLowerCase();
  if (raw === "unknown") return "unknown";
  if (NUMERIC_ROW_IDS.has(raw)) return raw;
  return "";
}

function voteValueToNumber(value) {
  const v = normalizeVoteValue(value);
  return NUMERIC_ROW_IDS.has(v) ? Number(v) : null;
}

function getRowLabel(rowId) {
  const id = canonicalRowId(rowId);
  return db.config.rowLabels?.[id] || DEFAULT_ROW_LABELS[id] || id;
}

function getRowColor(rowId) {
  const id = canonicalRowId(rowId);
  return db.config.rowColors?.[id] || DEFAULT_ROW_COLORS[id] || "#cccccc";
}

function getRowIconScale(rowId) {
  const id = canonicalRowId(rowId);
  const n = Number(db.config.rowIconScales?.[id]);
  if (Number.isFinite(n) && n > 0.2 && n < 2) return n;
  return DEFAULT_ROW_ICON_SCALES[id] || 1;
}

function setRowColor(rowId, color) {
  const id = canonicalRowId(rowId);
  const hex = normalizeHexColor(color);
  if (!id || !hex) return false;
  db.config.rowColors[id] = hex;
  return true;
}

function resetRowColor(rowId) {
  const id = canonicalRowId(rowId);
  if (!id) return;
  db.config.rowColors[id] = DEFAULT_ROW_COLORS[id] || "#cccccc";
}

function resetAllRowColors() {
  db.config.rowColors = { ...DEFAULT_ROW_COLORS };
}

function clampNumber(value, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, n));
}

function getCoefficientConfig() {
  applyDbDefaults();
  return db.config.coefficients;
}

function getGlobalRowWeight(value) {
  const key = normalizeVoteValue(value) || "unknown";
  const raw = Number(getCoefficientConfig().globalRowWeights?.[key]);
  return Number.isFinite(raw) && raw > 0 ? raw : 1;
}

function setGlobalRowWeight(value, weight) {
  const key = normalizeVoteValue(value);
  const numeric = Number(weight);
  if (!key || !Number.isFinite(numeric) || numeric <= 0) return false;
  getCoefficientConfig().globalRowWeights[key] = numeric;
  return true;
}

function getEvaluatorWeight(userId) {
  const raw = Number(getCoefficientConfig().evaluatorWeights?.[userId]);
  return Number.isFinite(raw) && raw > 0 ? raw : 1;
}

function setEvaluatorWeight(userId, weight) {
  const numeric = Number(weight);
  if (!userId || !Number.isFinite(numeric) || numeric <= 0) return false;
  getCoefficientConfig().evaluatorWeights[userId] = numeric;
  return true;
}

function getTargetBias(userId) {
  const raw = Number(getCoefficientConfig().targetBiases?.[userId]);
  return Number.isFinite(raw) ? raw : 0;
}

function setTargetBias(userId, bias) {
  const numeric = Number(bias);
  if (!userId || !Number.isFinite(numeric)) return false;
  getCoefficientConfig().targetBiases[userId] = numeric;
  return true;
}

function clearPersonCoefficients(userId) {
  if (!userId) return;
  delete getCoefficientConfig().evaluatorWeights?.[userId];
  delete getCoefficientConfig().targetBiases?.[userId];
}

function getRowInfluenceWeight(rowId) {
  const key = canonicalRowId(rowId) || "new";
  const raw = Number(getCoefficientConfig().rowInfluenceWeights?.[key]);
  const fallback = ({ "5": 1, "4": 1, "3": 1, "2": 1, "1": 1, unknown: 1, new: 1 })[key] || 1;
  return Number.isFinite(raw) && raw > 0 ? raw : fallback;
}

function setRowInfluenceWeight(rowId, weight) {
  const key = canonicalRowId(rowId);
  const numeric = Number(weight);
  if (!key || !Number.isFinite(numeric) || numeric <= 0) return false;
  getCoefficientConfig().rowInfluenceWeights[key] = numeric;
  return true;
}

function getEvaluatorBoardRowId(userId) {
  const person = db.people?.[userId];
  return person ? getBoardRowForPerson(person) : "new";
}

function getEvaluatorRowInfluence(userId) {
  return getRowInfluenceWeight(getEvaluatorBoardRowId(userId));
}

function getSessionDraftVoteMap(userId) {
  const map = db.sessions?.[userId]?.draftVotes;
  return map && typeof map === "object" ? map : {};
}

function getSessionDraftCommentMap(userId) {
  const map = db.sessions?.[userId]?.draftComments;
  return map && typeof map === "object" ? map : {};
}

function buildPersonalVoteMap(userId, options = {}) {
  const includeDraft = options.includeDraft !== false;
  const session = db.sessions?.[userId] || null;
  const replacementActive = !!session?.replaceCommitted;
  const out = replacementActive ? {} : { ...(db.votes?.[userId] || {}) };
  if (includeDraft) {
    for (const [targetId, vote] of Object.entries(getSessionDraftVoteMap(userId))) out[targetId] = vote;
  }
  return out;
}

function getStoredComment(evaluatorId, targetId, options = {}) {
  const includeDraft = options.includeDraft !== false;
  if (includeDraft) {
    const draft = getSessionDraftCommentMap(evaluatorId)?.[targetId];
    if (draft && typeof draft.text === "string") return draft;
  }
  return db.comments?.[evaluatorId]?.[targetId] || null;
}

function isReplacementSessionActive(userId) {
  return !!db.sessions?.[userId]?.replaceCommitted;
}

function startReplacementSession(userId) {
  const now = nowIso();
  const preservedVotes = Object.keys(db.votes?.[userId] || {}).length;
  const preservedComments = Object.keys(db.comments?.[userId] || {}).length;
  db.sessions ||= {};
  db.sessions[userId] = {
    userId,
    sessionId: makeId(),
    startedAt: now,
    updatedAt: now,
    activeTargetId: "",
    lastCompletedTargetId: "",
    lastVoteValue: "",
    votesCastThisSession: 0,
    history: [],
    draftVotes: {},
    draftComments: {},
    replaceCommitted: true,
    preservedVotes,
    preservedComments,
    stage: 3,
  };
  db.sessions[userId].activeTargetId = pickNextTargetForEvaluator(userId);
  saveDB(db);
  return { session: db.sessions[userId], preservedVotes, preservedComments };
}

function stopRatingSession(userId) {
  const session = db.sessions?.[userId];
  if (!session) return null;
  session.stoppedAt = nowIso();
  session.updatedAt = session.stoppedAt;
  saveDB(db);
  return session;
}

function resumeRatingSession(userId) {
  const session = ensureStage2Session(userId);
  if (session?.stoppedAt) delete session.stoppedAt;
  session.updatedAt = nowIso();
  saveDB(db);
  return session;
}

function buildStoppedRatingPayload(userId, options = {}) {
  const session = db.sessions?.[userId] || null;
  const given = countVotesGivenBy(userId, { includeDraft: true });
  const eligible = Math.max(0, getEligibleTargetIdsForEvaluator(userId).length);
  const target = session?.activeTargetId ? db.people?.[session.activeTargetId] : null;
  const desc = [
    options.headerText || "Оценивание остановлено. Прогресс сохранён.",
    `Прогресс: **${given.total}/${eligible}**.`,
    target ? `Если продолжишь, следующая карточка будет: **${target.username || target.name || target.userId}**.` : "Новых карточек сейчас нет.",
    session?.replaceCommitted
      ? `Старый общий тир-лист пока сохранён. Он заменится только после того, как ты закончишь новый личный тир-лист.`
      : `Текущий личный черновик сохранён.`,
  ].filter(Boolean).join("\n");

  return {
    embeds: [new EmbedBuilder().setTitle("Оценивание остановлено").setDescription(desc)],
    components: [new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("rate_start").setLabel("Продолжить оценку").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("rate_my_status").setLabel("Мой статус").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId("rate_reset_all").setLabel("Оценить заново").setStyle(ButtonStyle.Danger),
    )],
    ephemeral: true,
  };
}

function countVotesGivenBy(userId, options = {}) {
  const map = buildPersonalVoteMap(userId, { includeDraft: options.includeDraft !== false });
  let total = 0;
  let known = 0;
  let unknown = 0;
  for (const vote of Object.values(map)) {
    const value = normalizeVoteValue(vote?.value);
    if (!value) continue;
    total++;
    if (value === "unknown") unknown++;
    else known++;
  }
  return { total, known, unknown };
}

function countVotesReceivedBy(targetId) {
  let total = 0;
  let known = 0;
  let unknown = 0;
  let sumKnown = 0;
  let weightedKnownSum = 0;
  let weightedKnownWeight = 0;
  let weightedUnknown = 0;
  let lastVoteAt = "";
  const byValue = { "5": 0, "4": 0, "3": 0, "2": 0, "1": 0, unknown: 0 };

  for (const [evaluatorId, voterMap] of Object.entries(db.votes || {})) {
    const vote = voterMap?.[targetId];
    const value = normalizeVoteValue(vote?.value);
    if (!value) continue;

    const evaluatorWeight = getEvaluatorWeight(evaluatorId);
    const evaluatorRowInfluence = getEvaluatorRowInfluence(evaluatorId);
    const rowWeight = getGlobalRowWeight(value);
    const effectiveWeight = evaluatorWeight * evaluatorRowInfluence * rowWeight;

    total++;
    if (String(vote?.updatedAt || "") > lastVoteAt) lastVoteAt = String(vote.updatedAt || "");
    if (value === "unknown") {
      unknown++;
      byValue.unknown++;
      weightedUnknown += effectiveWeight;
    } else {
      known++;
      const numeric = Number(value) || 0;
      sumKnown += numeric;
      weightedKnownSum += numeric * effectiveWeight;
      weightedKnownWeight += effectiveWeight;
      byValue[value] = (byValue[value] || 0) + 1;
    }
  }

  const baseAverage = weightedKnownWeight ? weightedKnownSum / weightedKnownWeight : null;
  const bias = getTargetBias(targetId);
  const average = baseAverage == null ? null : clampNumber(baseAverage + bias, 1, 5);
  const unknownWeightTotal = weightedUnknown + weightedKnownWeight;
  return {
    total,
    known,
    unknown,
    sumKnown,
    average,
    baseAverage,
    roundedAverage: average == null ? null : Math.max(1, Math.min(5, Math.round(average))),
    unknownShare: unknownWeightTotal ? weightedUnknown / unknownWeightTotal : 0,
    lastVoteAt,
    byValue,
    weightedKnownWeight,
    weightedUnknown,
    targetBias: bias,
  };
}

function choosePreviewRowIdFromAggregate(agg) {
  if (!agg || !agg.total) return "new";
  if (!agg.known) return "new";
  return String(Math.max(1, Math.min(5, Math.round(agg.average || 3))));
}

function getTierThresholds(rowId) {
  const id = canonicalRowId(rowId);
  if (!NUMERIC_ROW_IDS.has(id)) return null;
  const tier = Number(id);
  return {
    rowId: id,
    promotionThreshold: tier < 5 ? tier + 0.5 : null,
    demotionThreshold: tier > 1 ? tier - 0.5 : null,
  };
}

function buildPromotionDemotionMeta(rowId, average) {
  const thresholds = getTierThresholds(rowId);
  const avg = Number(average);
  if (!thresholds || !Number.isFinite(avg)) {
    return {
      promotionThreshold: thresholds?.promotionThreshold ?? null,
      demotionThreshold: thresholds?.demotionThreshold ?? null,
      distanceToPromotion: null,
      distanceToDemotion: null,
      promotionReadyScore: null,
      demotionRiskScore: null,
      withinTierProgress: null,
      edgeState: "none",
    };
  }

  const promotionThreshold = thresholds.promotionThreshold;
  const demotionThreshold = thresholds.demotionThreshold;
  const distanceToPromotion = promotionThreshold == null ? null : Math.max(0, promotionThreshold - avg);
  const distanceToDemotion = demotionThreshold == null ? null : Math.max(0, avg - demotionThreshold);

  let withinTierProgress = null;
  if (promotionThreshold != null && demotionThreshold != null) {
    withinTierProgress = clampNumber((avg - demotionThreshold) / (promotionThreshold - demotionThreshold), 0, 1);
  } else if (promotionThreshold != null) {
    withinTierProgress = clampNumber(1 - (promotionThreshold - avg), 0, 1);
  } else if (demotionThreshold != null) {
    withinTierProgress = clampNumber(avg - demotionThreshold, 0, 1);
  }

  let edgeState = "stable";
  if (promotionThreshold == null && demotionThreshold != null) {
    edgeState = distanceToDemotion <= 0.12 ? "danger-demotion" : "safe-top";
  } else if (promotionThreshold != null && demotionThreshold == null) {
    edgeState = distanceToPromotion <= 0.12 ? "ready-promotion" : "climbing";
  } else if (distanceToPromotion != null && distanceToDemotion != null) {
    if (distanceToPromotion < distanceToDemotion) edgeState = "closer-promotion";
    else if (distanceToDemotion < distanceToPromotion) edgeState = "closer-demotion";
  }

  return {
    promotionThreshold,
    demotionThreshold,
    distanceToPromotion,
    distanceToDemotion,
    promotionReadyScore: distanceToPromotion == null ? null : 1 / (distanceToPromotion + 0.001),
    demotionRiskScore: distanceToDemotion == null ? null : 1 / (distanceToDemotion + 0.001),
    withinTierProgress,
    edgeState,
  };
}

function formatPromotionDemotionLine(aggregate) {
  const rowId = canonicalRowId(aggregate?.rowId);
  if (!NUMERIC_ROW_IDS.has(rowId) || !Number.isFinite(Number(aggregate?.average))) return "Дистанция до апа/дауна: —";

  const parts = [];
  if (aggregate.distanceToPromotion != null && aggregate.promotionThreshold != null) {
    parts.push(`до повышения ${aggregate.promotionThreshold.toFixed(1)}: ${aggregate.distanceToPromotion.toFixed(2)}`);
  }
  if (aggregate.distanceToDemotion != null && aggregate.demotionThreshold != null) {
    parts.push(`до понижения ${aggregate.demotionThreshold.toFixed(1)}: ${aggregate.distanceToDemotion.toFixed(2)}`);
  }
  return parts.length ? parts.join(" | ") : "Дистанция до апа/дауна: —";
}

function comparePromotionDemotionPriority(aAgg, bAgg, rowId) {
  const id = canonicalRowId(rowId);
  const aAverage = Number(aAgg?.average) || 0;
  const bAverage = Number(bAgg?.average) || 0;
  const aKnown = Number(aAgg?.knownCount) || 0;
  const bKnown = Number(bAgg?.knownCount) || 0;
  const aPromo = Number.isFinite(Number(aAgg?.distanceToPromotion)) ? Number(aAgg.distanceToPromotion) : Number.POSITIVE_INFINITY;
  const bPromo = Number.isFinite(Number(bAgg?.distanceToPromotion)) ? Number(bAgg.distanceToPromotion) : Number.POSITIVE_INFINITY;
  const aDemo = Number.isFinite(Number(aAgg?.distanceToDemotion)) ? Number(aAgg.distanceToDemotion) : Number.NEGATIVE_INFINITY;
  const bDemo = Number.isFinite(Number(bAgg?.distanceToDemotion)) ? Number(bAgg.distanceToDemotion) : Number.NEGATIVE_INFINITY;

  if (id === "5") {
    if (bDemo !== aDemo) return bDemo - aDemo;
    if (bAverage !== aAverage) return bAverage - aAverage;
    if (bKnown !== aKnown) return bKnown - aKnown;
    return 0;
  }

  if (id === "1") {
    if (aPromo !== bPromo) return aPromo - bPromo;
    if (bAverage !== aAverage) return bAverage - aAverage;
    if (bKnown !== aKnown) return bKnown - aKnown;
    return 0;
  }

  if (aPromo !== bPromo) return aPromo - bPromo;
  if (bDemo !== aDemo) return bDemo - aDemo;
  if (bAverage !== aAverage) return bAverage - aAverage;
  if (bKnown !== aKnown) return bKnown - aKnown;
  return 0;
}

function buildAggregateForTarget(targetId) {
  const received = countVotesReceivedBy(targetId);
  const rowId = choosePreviewRowIdFromAggregate(received);
  const distanceMeta = buildPromotionDemotionMeta(rowId, received.average);
  return {
    total: received.total,
    knownCount: received.known,
    unknownCount: received.unknown,
    sumKnown: received.sumKnown,
    average: received.average,
    baseAverage: received.baseAverage,
    roundedAverage: received.roundedAverage,
    unknownShare: received.unknownShare,
    distribution: received.byValue,
    rowId,
    lastVoteAt: received.lastVoteAt,
    weightedKnownWeight: received.weightedKnownWeight,
    weightedUnknown: received.weightedUnknown,
    targetBias: received.targetBias,
    promotionThreshold: distanceMeta.promotionThreshold,
    demotionThreshold: distanceMeta.demotionThreshold,
    distanceToPromotion: distanceMeta.distanceToPromotion,
    distanceToDemotion: distanceMeta.distanceToDemotion,
    promotionReadyScore: distanceMeta.promotionReadyScore,
    demotionRiskScore: distanceMeta.demotionRiskScore,
    withinTierProgress: distanceMeta.withinTierProgress,
    edgeState: distanceMeta.edgeState,
  };
}

function refreshAllPeopleDerivedState(save = false) {
  let changed = 0;

  for (const [userId, person] of Object.entries(db.people || {})) {
    if (!person || !userId) continue;

    const agg = buildAggregateForTarget(userId);
    const fallbackRow = canonicalRowId(person.stage1PinnedRowId || person.previewRowId || person.legacy?.tier || "new") || "new";
    const nextPreviewRow = agg.total > 0 ? (agg.knownCount > 0 ? agg.rowId : fallbackRow) : fallbackRow;
    const prevAgg = JSON.stringify(person.stage2Aggregate || {});
    const nextAgg = JSON.stringify(agg);

    if (person.previewRowId !== nextPreviewRow) {
      person.previewRowId = nextPreviewRow;
      changed++;
    }

    if (prevAgg !== nextAgg) {
      person.stage2Aggregate = agg;
      changed++;
    }
  }

  if (save && changed) saveDB(db);
  return changed;
}

function getBoardRowForPerson(person) {
  if (!person) return "new";
  const preview = canonicalRowId(person.previewRowId);
  if (preview) return preview;
  const pinned = canonicalRowId(person.stage1PinnedRowId);
  if (pinned) return pinned;
  const legacy = canonicalRowId(person.legacy?.tier);
  if (legacy) return legacy;
  return "new";
}

function formatBadgeForPerson(person) {
  return "";
}

function sortPeopleForRow(list, rowId) {
  const id = canonicalRowId(rowId);
  list.sort((a, b) => {
    const av = a.aggregate || buildAggregateForTarget(a.userId);
    const bv = b.aggregate || buildAggregateForTarget(b.userId);

    if (id === "unknown") {
      if ((bv.unknownShare || 0) !== (av.unknownShare || 0)) return (bv.unknownShare || 0) - (av.unknownShare || 0);
      if ((bv.unknownCount || 0) !== (av.unknownCount || 0)) return (bv.unknownCount || 0) - (av.unknownCount || 0);
    } else if (NUMERIC_ROW_IDS.has(id)) {
      const promotionDemotionCmp = comparePromotionDemotionPriority(av, bv, id);
      if (promotionDemotionCmp) return promotionDemotionCmp;
    } else {
      if ((bv.total || 0) !== (av.total || 0)) return (bv.total || 0) - (av.total || 0);
      if ((bv.knownCount || 0) !== (av.knownCount || 0)) return (bv.knownCount || 0) - (av.knownCount || 0);
      const aCreated = String(a.createdAt || "");
      const bCreated = String(b.createdAt || "");
      if (aCreated !== bCreated) return bCreated.localeCompare(aCreated);
    }

    return String(a.username || a.name || a.userId || "").localeCompare(String(b.username || b.name || b.userId || ""), "ru");
  });
}

function countPeopleWhoRatedOnce() {
  let count = 0;
  for (const voterMap of Object.values(db.votes || {})) {
    let hasAny = false;
    for (const vote of Object.values(voterMap || {})) {
      if (normalizeVoteValue(vote?.value)) {
        hasAny = true;
        break;
      }
    }
    if (hasAny) count++;
  }
  return count;
}

function getRoundedUnknownPercent(aggregate) {
  return Math.round((Number(aggregate?.unknownShare) || 0) * 100);
}

function shouldStayInNewRow(aggregate) {
  return Number(aggregate?.total || 0) < NEW_ROW_EXIT_THRESHOLD;
}

function shouldAppearInUnknownRow(aggregate) {
  return Number(aggregate?.unknownCount || 0) > 0 && getRoundedUnknownPercent(aggregate) > UNKNOWN_ROW_PERCENT_THRESHOLD;
}

function getNewRowBadgeText(aggregate) {
  const total = Number(aggregate?.total || 0);
  return total > 0 ? String(total) : "";
}

function buildGraphicBucketsFromPeople() {
  const buckets = Object.fromEntries(BOARD_ROW_ORDER.map((id) => [id, []]));
  for (const person of Object.values(db.people || {})) {
    if (!person?.userId) continue;
    const aggregate = person.stage2Aggregate || buildAggregateForTarget(person.userId);
    const primaryRowId = getBoardRowForPerson(person);
    const baseCard = {
      userId: person.userId,
      name: person.name || person.userId,
      username: String(person.username || "").trim() || person.name || person.userId,
      avatarUrl: normalizeDiscordAvatarUrl(person.avatarUrl || ""),
      badgeText: "",
      aggregate,
      received: {
        total: aggregate.total,
        known: aggregate.knownCount,
        unknown: aggregate.unknownCount,
      },
      createdAt: person.createdAt || "",
      source: person.source || "manual",
    };

    if ((aggregate.total || 0) > 0 && buckets[primaryRowId]) {
      buckets[primaryRowId].push({ ...baseCard, rowId: primaryRowId });
    }

    if (shouldStayInNewRow(aggregate)) {
      buckets.new.push({ ...baseCard, rowId: "new", badgeText: getNewRowBadgeText(aggregate), duplicateOf: primaryRowId });
    }

    if (shouldAppearInUnknownRow(aggregate)) {
      buckets.unknown.push({ ...baseCard, rowId: "unknown", duplicateOf: primaryRowId });
    }
  }

  for (const rowId of BOARD_ROW_ORDER) sortPeopleForRow(buckets[rowId], rowId);
  return buckets;
}

async function getFreshDiscordIdentity(client, userId) {
  const out = { name: "", username: "", avatarUrl: "" };
  if (!client || !userId) return out;

  try {
    const guild = await getGuild(client);
    const member = guild ? await guild.members.fetch(userId).catch(() => null) : null;
    if (member) {
      out.name = String(member.displayName || "").trim();
      out.avatarUrl = normalizeDiscordAvatarUrl(member.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
      if (member.user?.username) out.username = String(member.user.username).trim();
    }
  } catch {}

  try {
    const user = await client.users.fetch(userId).catch(() => null);
    if (user) {
      if (!out.username) out.username = String(user.username || "").trim();
      if (!out.avatarUrl) {
        out.avatarUrl = normalizeDiscordAvatarUrl(
          user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }) || user.defaultAvatarURL || ""
        );
      }
      if (!out.name) out.name = out.username || user.id;
    }
  } catch {}

  return out;
}

async function upsertPersonFromUser(client, user, options = {}) {
  const userId = user?.id || user;
  if (!userId) throw new Error("userId is required");

  const existing = db.people[userId] || {};
  const identity = await getFreshDiscordIdentity(client, userId);
  const rowId = canonicalRowId(options.stage1PinnedRowId || existing.stage1PinnedRowId || existing.previewRowId || "");
  const now = nowIso();

  db.people[userId] = {
    userId,
    name: identity.name || existing.name || (typeof user === "object" ? user.username : userId),
    username: identity.username || existing.username || (typeof user === "object" ? user.username : userId),
    avatarUrl: identity.avatarUrl || existing.avatarUrl || "",
    createdAt: existing.createdAt || now,
    updatedAt: now,
    source: options.source || existing.source || "manual",
    stage1PinnedRowId: rowId || existing.stage1PinnedRowId || "",
    previewRowId: existing.previewRowId || rowId || "",
    notes: existing.notes || "",
    legacy: existing.legacy || {},
    stage2Aggregate: existing.stage2Aggregate || buildAggregateForTarget(userId),
  };

  refreshAllPeopleDerivedState();
  saveDB(db);
  return { person: db.people[userId], created: !existing.userId };
}

function removePersonAndVotes(userId) {
  if (!userId || !db.people[userId]) return false;
  delete db.people[userId];

  for (const voterId of Object.keys(db.votes || {})) {
    if (db.votes[voterId] && db.votes[voterId][userId]) delete db.votes[voterId][userId];
    if (voterId === userId) delete db.votes[voterId];
  }
  for (const voterId of Object.keys(db.comments || {})) {
    if (db.comments[voterId] && db.comments[voterId][userId]) delete db.comments[voterId][userId];
    if (voterId === userId) delete db.comments[voterId];
  }

  if (db.sessions?.[userId]) delete db.sessions[userId];
  for (const session of Object.values(db.sessions || {})) {
    if (!session) continue;
    if (session.activeTargetId === userId) session.activeTargetId = "";
    if (session.draftVotes?.[userId]) delete session.draftVotes[userId];
    if (session.draftComments?.[userId]) delete session.draftComments[userId];
    session.updatedAt = nowIso();
  }

  clearPersonCoefficients(userId);
  refreshAllPeopleDerivedState();
  saveDB(db);
  return true;
}

function resetEvaluatorProgress(userId) {
  if (!userId) return { clearedVotes: 0, clearedComments: 0 };
  const clearedVotes = Object.keys(db.votes?.[userId] || {}).length;
  const clearedComments = Object.keys(db.comments?.[userId] || {}).length;
  if (db.votes?.[userId]) delete db.votes[userId];
  if (db.comments?.[userId]) delete db.comments[userId];
  if (db.sessions?.[userId]) delete db.sessions[userId];
  refreshAllPeopleDerivedState();
  saveDB(db);
  return { clearedVotes, clearedComments };
}

function clearTierlistData(mode = "full") {
  const normalizedMode = String(mode || "full").toLowerCase();
  const peopleCount = Object.keys(db.people || {}).length;
  const voteMapCount = Object.keys(db.votes || {}).length;
  const sessionCount = Object.keys(db.sessions || {}).length;
  const commentMapCount = Object.keys(db.comments || {}).length;

  if (normalizedMode === "votes-only") {
    db.votes = {};
    db.comments = {};
    db.sessions = {};
    refreshAllPeopleDerivedState();
    saveDB(db);
    return { mode: normalizedMode, peopleCount, voteMapCount, sessionCount, commentMapCount };
  }

  db.people = {};
  db.votes = {};
  db.comments = {};
  db.sessions = {};
  if (db.config?.coefficients) {
    db.config.coefficients.evaluatorWeights = {};
    db.config.coefficients.targetBiases = {};
  }
  clearGraphicAvatarCache();
  refreshAllPeopleDerivedState();
  saveDB(db);
  return { mode: "full", peopleCount, voteMapCount, sessionCount, commentMapCount };
}

function listCommittedVotes(filters = {}) {
  const rows = [];
  const limit = Math.max(1, Math.min(100, Number(filters.limit) || 25));
  for (const [evaluatorId, voterMap] of Object.entries(db.votes || {})) {
    for (const [targetId, vote] of Object.entries(voterMap || {})) {
      const value = normalizeVoteValue(vote?.value);
      if (!value) continue;
      if (filters.evaluatorId && evaluatorId !== filters.evaluatorId) continue;
      if (filters.targetId && targetId !== filters.targetId) continue;
      const evaluator = db.people?.[evaluatorId];
      const target = db.people?.[targetId];
      const comment = db.comments?.[evaluatorId]?.[targetId]?.text || "";
      rows.push({
        evaluatorId,
        evaluatorLabel: evaluator?.username || evaluator?.name || evaluatorId,
        targetId,
        targetLabel: target?.username || target?.name || targetId,
        value,
        updatedAt: String(vote?.updatedAt || vote?.createdAt || ""),
        comment,
        evaluatorWeight: getEvaluatorWeight(evaluatorId),
        rowInfluence: getEvaluatorRowInfluence(evaluatorId),
      });
    }
  }
  rows.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)) || String(a.evaluatorLabel).localeCompare(String(b.evaluatorLabel), "ru"));
  return rows.slice(0, limit);
}

function formatVoteAuditLines(filters = {}) {
  const rows = listCommittedVotes(filters);
  if (!rows.length) return ["Совпадений не найдено."];
  return rows.map((row, i) => {
    const commentPart = row.comment ? ` | комм: ${row.comment.slice(0, 80)}` : "";
    return `${i + 1}. ${row.evaluatorLabel} -> ${row.targetLabel} = ${row.value === "unknown" ? "Не знаю" : row.value} | eval x${row.evaluatorWeight.toFixed(2)} | row x${row.rowInfluence.toFixed(2)}${commentPart}`;
  });
}

function buildDetailedGivenLines(userId, max = 12) {
  const map = buildPersonalVoteMap(userId, { includeDraft: true });
  const rows = [];
  for (const [targetId, vote] of Object.entries(map)) {
    const value = normalizeVoteValue(vote?.value);
    if (!value) continue;
    const person = db.people?.[targetId];
    const comment = getStoredComment(userId, targetId, { includeDraft: true })?.text || "";
    rows.push({
      label: person?.username || person?.name || targetId,
      value,
      updatedAt: String(vote?.updatedAt || vote?.createdAt || ""),
      comment,
    });
  }
  rows.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)) || String(a.label).localeCompare(String(b.label), "ru"));
  return rows.slice(0, max).map((row) => `• ${row.label} → ${row.value === "unknown" ? "Не знаю" : row.value}${row.comment ? ` | комм: ${row.comment.slice(0, 70)}` : ""}`);
}


function getCommittedVoteRows(filters = {}) {
  const rows = [];
  const valueFilter = normalizeVoteValue(filters.value || "");
  const commentsOnly = !!filters.commentsOnly;

  for (const [evaluatorId, voterMap] of Object.entries(db.votes || {})) {
    for (const [targetId, vote] of Object.entries(voterMap || {})) {
      const value = normalizeVoteValue(vote?.value);
      if (!value) continue;
      if (filters.evaluatorId && evaluatorId !== filters.evaluatorId) continue;
      if (filters.targetId && targetId !== filters.targetId) continue;
      if (valueFilter && value !== valueFilter) continue;

      const evaluator = db.people?.[evaluatorId];
      const target = db.people?.[targetId];
      const comment = String(db.comments?.[evaluatorId]?.[targetId]?.text || "").trim();
      if (commentsOnly && !comment) continue;

      rows.push({
        evaluatorId,
        evaluatorLabel: evaluator?.username || evaluator?.name || evaluatorId,
        targetId,
        targetLabel: target?.username || target?.name || targetId,
        value,
        updatedAt: String(vote?.updatedAt || vote?.createdAt || ""),
        comment,
        evaluatorWeight: getEvaluatorWeight(evaluatorId),
        rowInfluence: getEvaluatorRowInfluence(evaluatorId),
      });
    }
  }

  rows.sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)) || String(a.evaluatorLabel).localeCompare(String(b.evaluatorLabel), "ru"));
  return rows;
}

function normalizeVoteAuditPageSize(input) {
  const raw = Number(input);
  if (!Number.isFinite(raw)) return VOTE_AUDIT_PAGE_SIZES[1];
  let best = VOTE_AUDIT_PAGE_SIZES[0];
  let bestDelta = Math.abs(best - raw);
  for (const value of VOTE_AUDIT_PAGE_SIZES) {
    const delta = Math.abs(value - raw);
    if (delta < bestDelta) {
      best = value;
      bestDelta = delta;
    }
  }
  return best;
}

function cycleVoteAuditPageSize(input) {
  const current = normalizeVoteAuditPageSize(input);
  const idx = VOTE_AUDIT_PAGE_SIZES.indexOf(current);
  return VOTE_AUDIT_PAGE_SIZES[(idx + 1) % VOTE_AUDIT_PAGE_SIZES.length] || VOTE_AUDIT_PAGE_SIZES[0];
}

function cleanupVoteAuditPanels() {
  const now = Date.now();
  for (const [panelId, state] of voteAuditPanels.entries()) {
    const updatedAt = new Date(state?.updatedAt || state?.createdAt || 0).getTime();
    if (!updatedAt || now - updatedAt > VOTE_AUDIT_TTL_MS) voteAuditPanels.delete(panelId);
  }
}

function createVoteAuditPanel(ownerId, initial = {}) {
  cleanupVoteAuditPanels();
  const panelId = makeId().slice(0, 14);
  const state = {
    panelId,
    ownerId,
    evaluatorId: String(initial.evaluatorId || "").trim(),
    targetId: String(initial.targetId || "").trim(),
    value: normalizeVoteValue(initial.value || ""),
    commentsOnly: !!initial.commentsOnly,
    pageSize: normalizeVoteAuditPageSize(initial.pageSize),
    page: Math.max(0, Number(initial.page) || 0),
    createdAt: nowIso(),
    updatedAt: nowIso(),
  };
  voteAuditPanels.set(panelId, state);
  return state;
}

function getVoteAuditPanel(panelId) {
  cleanupVoteAuditPanels();
  const state = voteAuditPanels.get(String(panelId || "")) || null;
  if (!state) return null;
  state.updatedAt = nowIso();
  return state;
}

function getPersonDisplayLabel(userId) {
  const person = db.people?.[userId];
  return String(person?.username || person?.name || userId || "—").trim() || "—";
}

function formatVoteAuditValueLabel(value) {
  const normalized = normalizeVoteValue(value || "");
  if (!normalized) return "Все";
  return normalized === "unknown" ? "Не знаю" : normalized;
}

function formatVoteAuditDate(iso) {
  const text = String(iso || "").trim();
  if (!text) return "—";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text.slice(0, 16).replace("T", " ");
  const pad = (n) => String(n).padStart(2, "0");
  return `${pad(date.getDate())}.${pad(date.getMonth() + 1)} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function summarizeVoteAuditRows(rows) {
  const distribution = { "5": 0, "4": 0, "3": 0, "2": 0, "1": 0, unknown: 0 };
  let commentCount = 0;
  for (const row of rows) {
    const value = normalizeVoteValue(row?.value);
    if (value) distribution[value] = (distribution[value] || 0) + 1;
    if (row?.comment) commentCount++;
  }
  return { distribution, commentCount };
}

function formatVoteAuditRowLine(row, index) {
  const left = `${index}. ${row.evaluatorLabel} → ${row.targetLabel} = ${row.value === "unknown" ? "Не знаю" : row.value}`;
  const middle = `${formatVoteAuditDate(row.updatedAt)} | eval x${row.evaluatorWeight.toFixed(2)} | row x${row.rowInfluence.toFixed(2)}`;
  const comment = row.comment ? ` | комм: ${row.comment.replace(/\s+/g, " ").slice(0, 70)}` : "";
  return `${left} | ${middle}${comment}`;
}

function resolveVoteAuditPersonQuery(raw) {
  const query = String(raw || "").trim();
  if (!query) return { ok: true, userId: "", cleared: true };

  const idMatch = query.match(/^(?:<@!?(\d{5,25})>|(\d{5,25}))$/);
  const directId = idMatch ? (idMatch[1] || idMatch[2] || "") : "";
  if (directId) {
    const hasPerson = !!db.people?.[directId];
    const hasVotes = !!db.votes?.[directId] || Object.values(db.votes || {}).some((map) => !!map?.[directId]);
    if (hasPerson || hasVotes) return { ok: true, userId: directId };
    return { ok: false, error: "По этому ID нет человека в пуле и нет голосов." };
  }

  const normalized = query.toLowerCase();
  const candidates = [];
  for (const [userId, person] of Object.entries(db.people || {})) {
    const username = String(person?.username || "").trim();
    const name = String(person?.name || "").trim();
    const hay = `${username}\n${name}\n${userId}`.toLowerCase();
    if (!hay.includes(normalized)) continue;
    const exact = username.toLowerCase() === normalized || name.toLowerCase() === normalized;
    const starts = username.toLowerCase().startsWith(normalized) || name.toLowerCase().startsWith(normalized);
    candidates.push({ userId, label: username || name || userId, exact, starts });
  }

  candidates.sort((a, b) => Number(b.exact) - Number(a.exact) || Number(b.starts) - Number(a.starts) || a.label.localeCompare(b.label, "ru"));

  if (!candidates.length) return { ok: false, error: "Никого не нашёл. Введи ID, @упоминание или точнее ник." };
  if (candidates.length > 1 && !candidates[0].exact && !candidates[0].starts) {
    return { ok: false, error: `Нашлось несколько людей: ${candidates.slice(0, 5).map((x) => x.label).join(", ")}. Введи точнее.` };
  }
  return { ok: true, userId: candidates[0].userId };
}

function buildVoteAuditPanelComponents(state, totalRows, totalPages) {
  const page = Math.max(0, Math.min(totalPages - 1, Number(state?.page) || 0));
  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`vote_audit_set_eval:${state.panelId}`).setLabel("Оценщик").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId(`vote_audit_set_target:${state.panelId}`).setLabel("Цель").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId(`vote_audit_reset:${state.panelId}`).setLabel("Сброс").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`vote_audit_refresh:${state.panelId}`).setLabel("Обновить").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`vote_audit_close:${state.panelId}`).setLabel("Закрыть").setStyle(ButtonStyle.Danger),
  );

  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId(`vote_audit_prev:${state.panelId}`).setLabel("←").setStyle(ButtonStyle.Secondary).setDisabled(page <= 0 || totalRows === 0),
    new ButtonBuilder().setCustomId(`vote_audit_page:${state.panelId}`).setLabel(`Стр ${Math.max(1, page + 1)}/${Math.max(1, totalPages)}`).setStyle(ButtonStyle.Secondary).setDisabled(true),
    new ButtonBuilder().setCustomId(`vote_audit_next:${state.panelId}`).setLabel("→").setStyle(ButtonStyle.Secondary).setDisabled(page >= totalPages - 1 || totalRows === 0),
    new ButtonBuilder().setCustomId(`vote_audit_limit:${state.panelId}`).setLabel(`Лимит ${state.pageSize}`).setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId(`vote_audit_comments:${state.panelId}`).setLabel(state.commentsOnly ? "Комменты: да" : "Комменты: нет").setStyle(state.commentsOnly ? ButtonStyle.Success : ButtonStyle.Secondary),
  );

  const select = new StringSelectMenuBuilder()
    .setCustomId(`vote_audit_value:${state.panelId}`)
    .setPlaceholder(`Оценка: ${formatVoteAuditValueLabel(state.value)}`)
    .addOptions(
      { label: "Все оценки", value: "all", default: !state.value },
      { label: "Только 5", value: "5", default: state.value === "5" },
      { label: "Только 4", value: "4", default: state.value === "4" },
      { label: "Только 3", value: "3", default: state.value === "3" },
      { label: "Только 2", value: "2", default: state.value === "2" },
      { label: "Только 1", value: "1", default: state.value === "1" },
      { label: "Только Не знаю", value: "unknown", default: state.value === "unknown" },
    );

  return [row1, row2, new ActionRowBuilder().addComponents(select)];
}

function buildVoteAuditPanelPayload(panelId, options = {}) {
  const state = getVoteAuditPanel(panelId);
  if (!state) {
    return {
      embeds: [new EmbedBuilder().setTitle("Панель голосов устарела").setDescription("Открой её заново через команду." )],
      components: [],
      ephemeral: true,
    };
  }

  const filteredRows = getCommittedVoteRows({
    evaluatorId: state.evaluatorId,
    targetId: state.targetId,
    value: state.value,
    commentsOnly: state.commentsOnly,
  });
  const summary = summarizeVoteAuditRows(filteredRows);
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / state.pageSize));
  state.page = Math.max(0, Math.min(totalPages - 1, Number(state.page) || 0));
  const start = state.page * state.pageSize;
  const pageRows = filteredRows.slice(start, start + state.pageSize);
  const lines = pageRows.length
    ? pageRows.map((row, idx) => formatVoteAuditRowLine(row, start + idx + 1))
    : ["Совпадений нет."];

  const embed = new EmbedBuilder()
    .setTitle("Панель просмотра голосов")
    .setDescription([
      options.headerText || "Смотри матрицу голосов по людям.",
      `Оценщик: **${state.evaluatorId ? getPersonDisplayLabel(state.evaluatorId) : "все"}**`,
      `Цель: **${state.targetId ? getPersonDisplayLabel(state.targetId) : "все"}**`,
      `Оценка: **${formatVoteAuditValueLabel(state.value)}** | Только с комментами: **${state.commentsOnly ? "да" : "нет"}**`,
      `Найдено: **${filteredRows.length}** | На странице: **${pageRows.length}** | Лимит: **${state.pageSize}**`,
      `Распределение: **${formatDistributionLine(summary.distribution)}** | Комментов: **${summary.commentCount}**`,
      "",
      lines.join("\n"),
    ].join("\n").slice(0, 4090));

  if (state.targetId) {
    const agg = buildAggregateForTarget(state.targetId);
    embed.addFields({
      name: `Статистика по цели: ${getPersonDisplayLabel(state.targetId)}`,
      value: [
        `Среднее: **${formatAverage(agg.average)}**`,
        `Голосов: **${agg.total}** | Известных: **${agg.knownCount}** | Не знаю: **${agg.unknownCount}**`,
        `Ряд: **${getRowLabel(agg.rowId)}** | ${formatPromotionDemotionLine(agg)}`,
      ].join("\n").slice(0, 1024),
      inline: false,
    });
  }

  if (state.evaluatorId) {
    const given = countVotesGivenBy(state.evaluatorId, { includeDraft: false });
    embed.addFields({
      name: `Статистика по оценщику: ${getPersonDisplayLabel(state.evaluatorId)}`,
      value: [
        `Дал голосов: **${given.total}**`,
        `Обычных: **${given.known}** | Не знаю: **${given.unknown}**`,
        `Личный вес: **x${getEvaluatorWeight(state.evaluatorId).toFixed(2)}** | Вес строки: **x${getEvaluatorRowInfluence(state.evaluatorId).toFixed(2)}**`,
      ].join("\n").slice(0, 1024),
      inline: false,
    });
  }

  return {
    embeds: [embed],
    components: buildVoteAuditPanelComponents(state, filteredRows.length, totalPages),
    ephemeral: true,
  };
}

async function updateVoteAuditModalInteraction(interaction, payload) {
  try {
    if (typeof interaction.update === "function") {
      await interaction.update(payload);
      return true;
    }
  } catch {}

  try {
    if (!interaction.deferred && !interaction.replied) {
      await interaction.reply(payload);
      return true;
    }
  } catch {}

  return false;
}

async function importRoleMembers(client, role) {
  if (!role?.id) return { total: 0, created: 0, updated: 0 };
  const guild = await getGuild(client);
  if (!guild) return { total: 0, created: 0, updated: 0 };

  await guild.members.fetch().catch(() => null);
  const members = guild.members.cache.filter((m) => !m.user.bot && m.roles.cache.has(role.id));

  let created = 0;
  let updated = 0;
  for (const member of members.values()) {
    const res = await upsertPersonFromUser(client, member.user, { source: `role:${role.id}` });
    if (res.created) created++;
    else updated++;
  }

  refreshAllPeopleDerivedState();
  saveDB(db);
  return { total: members.size, created, updated };
}

function getEligibleTargetIdsForEvaluator(userId) {
  return Object.keys(db.people || {}).filter((targetId) => targetId && targetId !== userId);
}

function getPendingTargetIdsForEvaluator(userId) {
  const givenMap = buildPersonalVoteMap(userId, { includeDraft: true });
  return getEligibleTargetIdsForEvaluator(userId).filter((targetId) => !normalizeVoteValue(givenMap?.[targetId]?.value));
}

function getCurrentVote(evaluatorId, targetId, options = {}) {
  const includeDraft = options.includeDraft !== false;
  if (includeDraft) {
    const draft = getSessionDraftVoteMap(evaluatorId)?.[targetId];
    const draftValue = normalizeVoteValue(draft?.value);
    if (draftValue) return draft;
  }
  if (isReplacementSessionActive(evaluatorId)) return null;
  const value = normalizeVoteValue(db.votes?.[evaluatorId]?.[targetId]?.value);
  return value ? db.votes[evaluatorId][targetId] : null;
}

function pickNextTargetForEvaluator(userId) {
  const pending = getPendingTargetIdsForEvaluator(userId).map((targetId) => {
    const person = db.people?.[targetId];
    const agg = person?.stage2Aggregate || buildAggregateForTarget(targetId);
    return { targetId, person, agg };
  });

  pending.sort((a, b) => {
    if ((a.agg.total || 0) !== (b.agg.total || 0)) return (a.agg.total || 0) - (b.agg.total || 0);
    if ((a.agg.knownCount || 0) !== (b.agg.knownCount || 0)) return (a.agg.knownCount || 0) - (b.agg.knownCount || 0);
    if ((a.agg.unknownCount || 0) !== (b.agg.unknownCount || 0)) return (a.agg.unknownCount || 0) - (b.agg.unknownCount || 0);
    return String(a.person?.username || a.person?.name || a.targetId || "").localeCompare(
      String(b.person?.username || b.person?.name || b.targetId || ""),
      "ru"
    );
  });

  return pending[0]?.targetId || "";
}

function ensureStage2Session(userId, options = {}) {
  db.sessions ||= {};
  const now = nowIso();
  let session = db.sessions[userId];

  if (!session || options.forceNew) {
    session = {
      userId,
      sessionId: makeId(),
      startedAt: now,
      updatedAt: now,
      activeTargetId: "",
      lastCompletedTargetId: "",
      lastVoteValue: "",
      votesCastThisSession: 0,
      history: [],
      draftVotes: {},
      draftComments: {},
      stage: 3,
    };
    db.sessions[userId] = session;
  }

  session.stage = 3;
  session.updatedAt = now;
  session.history = Array.isArray(session.history) ? session.history : [];
  session.draftVotes = session.draftVotes && typeof session.draftVotes === "object" ? session.draftVotes : {};
  session.draftComments = session.draftComments && typeof session.draftComments === "object" ? session.draftComments : {};

  const currentTargetId = String(session.activeTargetId || "");
  const currentStillValid = currentTargetId && currentTargetId !== userId && !getCurrentVote(userId, currentTargetId, { includeDraft: true }) && db.people?.[currentTargetId];
  if (!currentStillValid) session.activeTargetId = pickNextTargetForEvaluator(userId);

  saveDB(db);
  return session;
}

function setCommittedVoteForTarget(evaluatorId, targetId, value, meta = {}) {
  const normalized = normalizeVoteValue(value);
  if (!normalized) throw new Error("bad vote value");
  if (!evaluatorId || !targetId) throw new Error("evaluatorId/targetId required");
  if (evaluatorId === targetId) throw new Error("self vote is blocked");

  db.votes ||= {};
  db.votes[evaluatorId] ||= {};

  const previous = db.votes[evaluatorId][targetId] || null;
  const now = nowIso();
  db.votes[evaluatorId][targetId] = {
    evaluatorId,
    targetId,
    value: normalized,
    numericValue: voteValueToNumber(normalized),
    createdAt: previous?.createdAt || now,
    updatedAt: now,
    via: meta.via || previous?.via || "stage3-personal-commit",
    sessionId: meta.sessionId || previous?.sessionId || "",
  };

  if (db.people[targetId]) db.people[targetId].updatedAt = now;
  if (db.people[evaluatorId]) db.people[evaluatorId].updatedAt = now;

  return { previous, vote: db.votes[evaluatorId][targetId] };
}

function commitSessionDrafts(userId, session) {
  session = session || db.sessions?.[userId];
  if (!session) return { committedVotes: 0, committedComments: 0, replacedOldVotes: 0, replacedOldComments: 0 };

  const replacingOld = !!session.replaceCommitted;
  const replacedOldVotes = replacingOld ? Object.keys(db.votes?.[userId] || {}).length : 0;
  const replacedOldComments = replacingOld ? Object.keys(db.comments?.[userId] || {}).length : 0;

  if (replacingOld) {
    db.votes ||= {};
    db.votes[userId] = {};
    db.comments ||= {};
    db.comments[userId] = {};
  }

  let committedVotes = 0;
  let committedComments = 0;
  for (const [targetId, vote] of Object.entries(session.draftVotes || {})) {
    const normalized = normalizeVoteValue(vote?.value);
    if (!normalized || !db.people?.[targetId] || targetId === userId) continue;
    setCommittedVoteForTarget(userId, targetId, normalized, { via: vote?.via || "stage3-personal-commit", sessionId: session.sessionId });
    committedVotes++;
  }

  db.comments ||= {};
  db.comments[userId] ||= {};
  for (const [targetId, comment] of Object.entries(session.draftComments || {})) {
    const text = String(comment?.text || "").trim().slice(0, 1000);
    if (!targetId || !db.people?.[targetId] || targetId === userId) continue;
    if (!text) continue;
    const previous = db.comments[userId][targetId] || null;
    db.comments[userId][targetId] = {
      evaluatorId: userId,
      targetId,
      text,
      createdAt: previous?.createdAt || nowIso(),
      updatedAt: nowIso(),
      sessionId: session.sessionId,
    };
    committedComments++;
  }

  session.draftVotes = {};
  session.draftComments = {};
  session.lastCommittedAt = nowIso();
  delete session.stoppedAt;
  session.replaceCommitted = false;
  refreshAllPeopleDerivedState();
  saveDB(db);
  return { committedVotes, committedComments, replacedOldVotes, replacedOldComments };
}

function applyVoteFromSession(userId, sessionId, value) {
  const session = ensureStage2Session(userId);
  if (!session || session.sessionId !== sessionId) {
    return { ok: false, reason: "stale-session", session: ensureStage2Session(userId, { forceNew: true }) };
  }

  const targetId = String(session.activeTargetId || "");
  if (!targetId || !db.people?.[targetId]) {
    session.activeTargetId = pickNextTargetForEvaluator(userId);
    session.updatedAt = nowIso();
    saveDB(db);
    return { ok: false, reason: "no-target", session };
  }

  if (userId === targetId) {
    session.activeTargetId = pickNextTargetForEvaluator(userId);
    session.updatedAt = nowIso();
    saveDB(db);
    return { ok: false, reason: "self-target", session };
  }

  const normalized = normalizeVoteValue(value);
  const now = nowIso();
  session.draftVotes ||= {};
  const previous = session.draftVotes[targetId] || null;
  session.draftVotes[targetId] = {
    evaluatorId: userId,
    targetId,
    value: normalized,
    numericValue: voteValueToNumber(normalized),
    createdAt: previous?.createdAt || now,
    updatedAt: now,
    via: "stage3-personal-draft",
    sessionId,
  };

  session.lastCompletedTargetId = targetId;
  session.lastVoteValue = normalized;
  session.votesCastThisSession = Number(session.votesCastThisSession || 0) + 1;
  session.updatedAt = now;
  session.history = Array.isArray(session.history) ? session.history : [];
  session.history.unshift({
    targetId,
    value: normalized,
    at: session.updatedAt,
  });
  session.history = session.history.slice(0, db.config.sessionHistoryLimit || SESSION_HISTORY_LIMIT);
  session.activeTargetId = pickNextTargetForEvaluator(userId);

  let committed = null;
  if (!session.activeTargetId) {
    session.completedAt = session.updatedAt;
    committed = commitSessionDrafts(userId, session);
  } else {
    delete session.completedAt;
    saveDB(db);
  }

  return { ok: true, targetId, session, written: { previous, vote: session.draftVotes?.[targetId] || { value: normalized } }, committed };
}

function applyCommentFromSession(userId, sessionId, targetId, text) {
  const session = ensureStage2Session(userId);
  if (!session || session.sessionId !== sessionId) {
    return { ok: false, reason: "stale-session", session: ensureStage2Session(userId, { forceNew: true }) };
  }

  const cleanTargetId = String(targetId || session.activeTargetId || "");
  if (!cleanTargetId || !db.people?.[cleanTargetId] || cleanTargetId === userId) {
    return { ok: false, reason: "bad-target", session };
  }

  const cleanText = String(text || "").trim().slice(0, 1000);
  session.draftComments ||= {};
  if (!cleanText) delete session.draftComments[cleanTargetId];
  else {
    const previous = session.draftComments[cleanTargetId] || db.comments?.[userId]?.[cleanTargetId] || null;
    session.draftComments[cleanTargetId] = {
      evaluatorId: userId,
      targetId: cleanTargetId,
      text: cleanText,
      createdAt: previous?.createdAt || nowIso(),
      updatedAt: nowIso(),
      sessionId,
    };
  }
  session.updatedAt = nowIso();
  saveDB(db);
  return { ok: true, session, targetId: cleanTargetId, comment: session.draftComments[cleanTargetId] || null };
}

function formatAverage(average) {
  return Number.isFinite(average) ? average.toFixed(2) : "—";
}

function listNamesForField(items, max = 8) {
  if (!items.length) return "—";
  const sliced = items.slice(0, max).map((item) => item.label);
  const rest = items.length - sliced.length;
  return rest > 0 ? `${sliced.join(", ")} +${rest}` : sliced.join(", ");
}

function buildPersonalTierFields(userId, options = {}) {
  const map = buildPersonalVoteMap(userId, { includeDraft: options.includeDraft !== false });
  const grouped = Object.fromEntries(PERSONAL_TIER_ORDER.map((id) => [id, []]));

  for (const [targetId, rawVote] of Object.entries(map)) {
    const value = normalizeVoteValue(rawVote?.value);
    if (!value) continue;
    const person = db.people?.[targetId];
    const label = String(person?.username || person?.name || targetId).trim() || targetId;
    grouped[value].push({ targetId, label, at: String(rawVote?.updatedAt || "") });
  }

  for (const rowId of PERSONAL_TIER_ORDER) {
    grouped[rowId].sort((a, b) => String(b.at).localeCompare(String(a.at)) || a.label.localeCompare(b.label, "ru"));
  }

  return PERSONAL_TIER_ORDER.map((rowId) => ({
    name: rowId === "unknown" ? "Не знаю" : `Тир ${rowId}`,
    value: listNamesForField(grouped[rowId]),
    inline: false,
  }));
}

function formatDistributionLine(distribution) {
  const d = distribution || {};
  return `5:${d["5"] || 0} 4:${d["4"] || 0} 3:${d["3"] || 0} 2:${d["2"] || 0} 1:${d["1"] || 0} ?:${d.unknown || 0}`;
}
function rollbackLastSessionVote(userId) {
  const session = ensureStage2Session(userId);
  const history = Array.isArray(session?.history) ? session.history : [];
  const last = history[0] || null;
  if (!session || !last?.targetId) {
    return { ok: false, reason: "no-history", session };
  }

  const targetId = String(last.targetId || "");
  if (!targetId || targetId === userId || !db.people?.[targetId]) {
    return { ok: false, reason: "bad-target", session };
  }

  if (session.draftVotes?.[targetId]) delete session.draftVotes[targetId];
  session.history.shift();
  session.votesCastThisSession = Math.max(0, Number(session.votesCastThisSession || 0) - 1);
  session.activeTargetId = targetId;
  session.lastCompletedTargetId = session.history[0]?.targetId || "";
  session.lastVoteValue = session.history[0]?.value || "";
  delete session.completedAt;
  session.updatedAt = nowIso();
  saveDB(db);
  return { ok: true, targetId, session, removedValue: normalizeVoteValue(last.value) };
}


function buildSessionHistoryLines(userId, max = 6) {
  const history = Array.isArray(db.sessions?.[userId]?.history) ? db.sessions[userId].history : [];
  return history.slice(0, max).map((item) => {
    const person = db.people?.[item.targetId];
    const label = person?.username || person?.name || item.targetId || "—";
    const value = item.value === "unknown" ? "Не знаю" : item.value || "—";
    return `• ${label} → ${value}`;
  });
}

function getAnonymousCommentsForTarget(targetId, max = 8) {
  const items = [];
  for (const evaluatorMap of Object.values(db.comments || {})) {
    const comment = evaluatorMap?.[targetId];
    const text = String(comment?.text || "").trim();
    if (!text) continue;
    items.push({ text, at: String(comment?.updatedAt || comment?.createdAt || "") });
  }
  items.sort((a, b) => String(b.at).localeCompare(String(a.at)));
  return items.slice(0, max);
}

function buildPersonalBucketsForUser(userId, options = {}) {
  const map = buildPersonalVoteMap(userId, { includeDraft: options.includeDraft !== false });
  const buckets = Object.fromEntries(PERSONAL_TIER_ORDER.map((id) => [id, []]));
  for (const [targetId, rawVote] of Object.entries(map)) {
    const value = normalizeVoteValue(rawVote?.value);
    if (!value || !buckets[value]) continue;
    const person = db.people?.[targetId];
    buckets[value].push({
      userId: targetId,
      name: person?.name || targetId,
      username: String(person?.username || person?.name || targetId).trim(),
      avatarUrl: normalizeDiscordAvatarUrl(person?.avatarUrl || ""),
      badgeText: value === "unknown" ? "?" : value,
      updatedAt: String(rawVote?.updatedAt || ""),
      createdAt: person?.createdAt || "",
      aggregate: person?.stage2Aggregate || buildAggregateForTarget(targetId),
    });
  }
  for (const rowId of PERSONAL_TIER_ORDER) {
    buckets[rowId].sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)) || String(a.username).localeCompare(String(b.username), "ru"));
  }
  return buckets;
}

async function renderPersonalTierlistPng(client, userId, options = {}) {
  const buckets = buildPersonalBucketsForUser(userId, { includeDraft: options.includeDraft !== false });
  const total = Object.values(buckets).reduce((sum, list) => sum + list.length, 0);
  if (!total) return null;
  if (!PImage) throw new Error("Не найден модуль pureimage. Установи: npm i pureimage");
  if (!ensureGraphicFonts()) throw new Error(`Не удалось загрузить системный шрифт для PNG. source=${GRAPHIC_FONT_INFO.source || "none"}. ${GRAPHIC_FONT_INFO.loadError || ""}`.trim());

  const { W, H: H_CFG, ICON } = getGraphicImageConfig();
  const rows = PERSONAL_TIER_ORDER;
  const topY = 120;
  const leftW = Math.floor(W * 0.24);
  const rightPadding = 36;
  const footerH = 44;

  const rowLayout = rows.map((rowId) => {
    const list = buckets[rowId] || [];
    const rowIcon = Math.max(42, Math.floor(ICON * getRowIconScale(rowId)));
    const gap = Math.max(10, Math.floor(rowIcon * 0.16));
    const rightW = W - leftW - rightPadding - 24;
    const cols = Math.max(1, Math.floor((rightW + gap) / (rowIcon + gap)));
    if (rowId === "unknown") {
      const bandRows = getUnknownBandRows();
      const bandCapacity = Math.max(1, cols * bandRows);
      const groupsNeeded = Math.max(1, Math.ceil(list.length / bandCapacity));
      const iconsH = Math.max(1, groupsNeeded) * (bandRows * rowIcon + (bandRows - 1) * gap) + Math.max(0, groupsNeeded - 1) * gap;
      const needed = 18 + iconsH + 22 + 12;
      const rowH = Math.max(needed, 18 + (bandRows * rowIcon + (bandRows - 1) * gap) + 22 + 12);
      return { rowId, list, rowIcon, gap, cols, rowH, bandRows, groupsNeeded };
    }
    const rowsNeeded = Math.max(1, Math.ceil(list.length / cols));
    const iconsH = rowsNeeded * (rowIcon + gap) - gap;
    return { rowId, list, rowIcon, gap, cols, rowH: Math.max(18 + iconsH + 22 + 12, 160), bandRows: 1, groupsNeeded: rowsNeeded };
  });

  const neededH = topY + rowLayout.reduce((sum, r) => sum + r.rowH, 0) + footerH;
  const H = Math.max(H_CFG, neededH);
  const title = options.title || `${db.config.graphicTierlist?.title || GRAPHIC_TIERLIST_TITLE} · личный`;

  const img = PImage.make(W, H);
  const ctx = img.getContext("2d");
  fillColor(ctx, "#242424");
  ctx.fillRect(0, 0, W, H);
  fillColor(ctx, "#ffffff");
  setGraphicFont(ctx, 64, "bold");
  ctx.fillText(title, 40, 82);
  fillColor(ctx, "#cfcfcf");
  setGraphicFont(ctx, 22, "regular");
  ctx.fillText(`личных голосов: ${total}. черновик: ${options.includeDraft !== false ? "вкл" : "выкл"}.`, 40, H - 18);

  let yCursor = topY;
  for (const row of rowLayout) {
    const { rowId, list, rowIcon, gap, cols, rowH } = row;
    const y = yCursor;
    yCursor += rowH;

    fillColor(ctx, "#2f2f2f");
    ctx.fillRect(leftW, y, W - leftW - rightPadding, rowH - 12);
    fillColor(ctx, getRowColor(rowId));
    ctx.fillRect(40, y, leftW - 40, rowH - 12);

    const blockH = rowH - 12;
    const labelX = 40 + 56;
    const labelW = (leftW - 40) - 56 - 18;
    const bottomLabelY = y + blockH - 18;
    const titleBoxY = y + 16;
    const titleBoxH = Math.max(44, bottomLabelY - titleBoxY - 18);
    const rowLabel = getRowLabel(rowId);
    drawGraphicTierTitle(ctx, rowLabel, labelX, titleBoxY, labelW, titleBoxH);
    fillColor(ctx, "#111111");
    setGraphicFont(ctx, 24, "regular");
    ctx.fillText(rowId === "unknown" ? "UNKNOWN" : `TIER ${rowId}`, labelX, bottomLabelY);

    const rightX = leftW + 24;
    const rightY = y + 18;
    for (let idx = 0; idx < list.length; idx++) {
      const player = list[idx];
      let x = rightX;
      let yy = rightY;
      if (rowId === "unknown") {
        const bandRows = Math.max(1, row.bandRows || getUnknownBandRows());
        const bandCapacity = Math.max(1, cols * bandRows);
        const groupIndex = Math.floor(idx / bandCapacity);
        const withinGroup = idx % bandCapacity;
        const rowInGroup = Math.floor(withinGroup / cols);
        const col = withinGroup % cols;
        x = rightX + col * (rowIcon + gap);
        yy = rightY + groupIndex * (bandRows * rowIcon + bandRows * gap) + rowInGroup * (rowIcon + gap);
      } else {
        const col = idx % cols;
        const rowIndex = Math.floor(idx / cols);
        x = rightX + col * (rowIcon + gap);
        yy = rightY + rowIndex * (rowIcon + gap);
      }
      const avatar = await loadGraphicAvatarForPlayer(client, player);
      fillColor(ctx, "#171717");
      ctx.fillRect(x - 3, yy - 3, rowIcon + 6, rowIcon + 6);
      if (avatar) ctx.drawImage(avatar, x, yy, rowIcon, rowIcon);
      else {
        fillColor(ctx, "#555555");
        ctx.fillRect(x, yy, rowIcon, rowIcon);
      }
      drawGraphicAvatarNameplates(ctx, player, x, yy, rowIcon);
      drawGraphicAvatarBadge(ctx, player, x, yy, rowIcon);
    }
  }

  const chunks = [];
  const stream = new PassThrough();
  stream.on("data", (c) => chunks.push(c));
  await PImage.encodePNGToStream(img, stream);
  stream.end();
  return Buffer.concat(chunks);
}

async function buildMyStatusPayload(client, userId) {
  const person = db.people?.[userId] || null;
  const given = countVotesGivenBy(userId, { includeDraft: true });
  const received = person?.stage2Aggregate || buildAggregateForTarget(userId);
  const rowId = person ? getBoardRowForPerson(person) : "";
  const eligible = Math.max(0, getEligibleTargetIdsForEvaluator(userId).length);
  const remaining = Math.max(0, eligible - given.total);
  const progressPct = eligible ? Math.min(100, Math.round((given.total / eligible) * 100)) : 0;
  const session = db.sessions?.[userId] || null;
  const anonymousComments = getAnonymousCommentsForTarget(userId, 12);
  const roundedUnknownPercent = getRoundedUnknownPercent(received);

  const summary = new EmbedBuilder()
    .setTitle("Мой статус")
    .setDescription([
      `Осталось оценить: **${remaining}** из **${eligible}**.`,
      `Ты уже оценил: **${given.total}** человек. Готовность: **${progressPct}%**.`,
      `Из твоих оценок обычных: **${given.known}**. «Не знаю»: **${given.unknown}**.`,
      person
        ? `Твоя текущая строка в общем тир-листе: **${getRowLabel(rowId)}**.`
        : "Тебя ещё нет в пуле оцениваемых. Кнопка «Оценивать» автоматически добавит тебя.",
      `Тебя оценили: **${received.total || 0}** раз. Обычных оценок: **${received.knownCount || 0}**. «Не знаю»: **${received.unknownCount || 0}**.`,
      `Твоя средняя оценка сейчас: **${formatAverage(received.average)}**.`,
      `По кнопке «Не знаю» у тебя сейчас: **${roundedUnknownPercent}%**.${roundedUnknownPercent > UNKNOWN_ROW_PERCENT_THRESHOLD ? " Поэтому ты есть и в строке «Не знают»." : ""}`,
      `Распределение голосов по тебе: **${formatDistributionLine(received.distribution)}**.`,
      session?.replaceCommitted ? "Сейчас у тебя идёт полная переоценка. Старый вклад в общий тир-лист сохранён, пока ты не закончишь новый личный тир-лист." : null,
      session?.lastCommittedAt ? "Твой последний личный тир-лист уже был слит в общий." : null,
    ].filter(Boolean).join("\n"));

  const commentsEmbed = new EmbedBuilder()
    .setTitle("Анонимные комментарии обо мне")
    .setDescription(
      anonymousComments.length
        ? anonymousComments.map((item, i) => `**${i + 1}.** ${item.text}`).join("\n\n")
        : "Пока никто не оставлял тебе анонимных комментариев."
    );

  const payload = {
    embeds: [summary, commentsEmbed],
    components: [new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("rate_start").setLabel("Продолжить оценку").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("rate_reset_all").setLabel("Оценить заново").setStyle(ButtonStyle.Danger),
    )],
    ephemeral: true,
  };
  const png = await renderPersonalTierlistPng(client, userId, { includeDraft: true, title: `${db.config.graphicTierlist?.title || GRAPHIC_TIERLIST_TITLE} · ${person?.username || person?.name || userId}` }).catch(() => null);
  if (png) {
    const fileName = `personal-tierlist-${sanitizeFileName(userId, "png")}`;
    const attachment = new AttachmentBuilder(png, { name: fileName });
    payload.embeds.push(new EmbedBuilder().setTitle("Личный графический тир-лист").setImage(`attachment://${fileName}`));
    payload.files = [attachment];
  }
  return payload;
}

function buildStartRatingText(created, session = null) {
  if (session?.replaceCommitted) {
    return "Запущена полная переоценка. Старые голоса пока остаются в общем тир-листе. Они заменятся только после того, как ты закончишь новый личный тир-лист.";
  }
  return created
    ? "Ты добавлен в пул оцениваемых людей. Ниже сразу первая карточка. Голоса пока идут в личный черновой тир-лист."
    : "Продолжаем. Ниже твоя текущая карточка на оценку. Общий тир-лист обновится, когда личный черновик закончится и сольётся.";
}

function buildRateButtons(sessionId, targetId) {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:5`).setLabel("5").setStyle(ButtonStyle.Danger),
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:4`).setLabel("4").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:3`).setLabel("3").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:2`).setLabel("2").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:1`).setLabel("1").setStyle(ButtonStyle.Primary),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId(`rate_vote:${sessionId}:unknown`).setLabel("Не знаю").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId(`rate_comment:${sessionId}:${targetId}`).setLabel("Оставить комментарий").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("rate_back").setLabel("Вернуться").setStyle(ButtonStyle.Secondary),
    ),
  ];
}

function buildSessionProgressLine(userId) {
  const given = countVotesGivenBy(userId, { includeDraft: true });
  const eligible = Math.max(0, getEligibleTargetIdsForEvaluator(userId).length);
  const remaining = Math.max(0, eligible - given.total);
  return `Прогресс: ${given.total}/${eligible}. Осталось: ${remaining}.`;
}

function buildNoTargetsPayload(userId, options = {}) {
  const given = countVotesGivenBy(userId, { includeDraft: true });
  const desc = [
    options.reason === "no-people"
      ? "Пока в пуле нет других людей для оценки."
      : "Оценка пока завершена. Новых людей без твоей оценки сейчас нет.",
    `Ты уже раздал голосов: **${given.total}**.`,
    options.merged ? `Новый личный тир-лист слит в общий. Комментариев слито: **${options.merged.committedComments || 0}**. Голосов слито: **${options.merged.committedVotes || 0}**.${options.merged.replacedOldVotes ? ` Старый вклад заменён: **${options.merged.replacedOldVotes}** голосов.` : ""}` : null,
    "Это сообщение исчезнет само через 10 секунд.",
  ].filter(Boolean).join("\n");

  return {
    embeds: [new EmbedBuilder().setTitle("Оценка завершена").setDescription(desc)],
    components: [],
    ephemeral: true,
    autoDeleteMs: 10000,
  };
}

function scheduleEphemeralDelete(interaction, ms = 10000) {
  const delay = Math.max(1000, Number(ms) || 10000);
  setTimeout(() => {
    interaction.deleteReply().catch(() => {});
  }, delay);
}

async function buildRatingCardPayload(client, userId, session, options = {}) {
  const targetId = String(session?.activeTargetId || "");
  if (!targetId || !db.people?.[targetId]) {
    return buildNoTargetsPayload(userId, { reason: Object.keys(db.people || {}).length <= 1 ? "no-people" : "finished", merged: options.merged || null });
  }

  const person = db.people[targetId];
  const agg = person.stage2Aggregate || buildAggregateForTarget(targetId);
  const distributionLine = formatDistributionLine(agg.distribution);
  const existingComment = getStoredComment(userId, targetId, { includeDraft: true });
  const embed = new EmbedBuilder()
    .setTitle("Оценка человека")
    .setDescription([
      options.headerText || "Выбери тир, нажми «Не знаю» или оставь анонимный комментарий.",
      `**${person.username || person.name || person.userId}**`,
      `<@${person.userId}>`,
      buildSessionProgressLine(userId),
      session?.replaceCommitted ? "Сейчас идёт полная переоценка. Старые голоса пока остаются в общем тир-листе до завершения нового." : null,
      `У него уже есть голосов: **${agg.total || 0}**. Обычных: **${agg.knownCount || 0}**. «Не знаю»: **${agg.unknownCount || 0}**.`,
      `Средняя: **${formatAverage(agg.average)}**. Текущая строка: **${getRowLabel(getBoardRowForPerson(person))}**.`,
      `Распределение голосов по нему: **${distributionLine}**.`,
      existingComment?.text ? `Твой анонимный комментарий к нему уже есть. Длина: **${existingComment.text.length}**.` : "Анонимный комментарий пока не оставлен.",
      options.lastActionText || null,
    ].filter(Boolean).join("\n"));

  if (person.avatarUrl) embed.setThumbnail(person.avatarUrl);

  const payload = {
    embeds: [embed],
    components: buildRateButtons(session.sessionId, targetId),
    ephemeral: true,
  };
  const png = await renderPersonalTierlistPng(client, userId, { includeDraft: true, title: `Личный тир-лист ${db.people?.[userId]?.username || db.people?.[userId]?.name || userId}` }).catch(() => null);
  if (png) {
    const fileName = `personal-progress-${sanitizeFileName(userId, "png")}`;
    payload.files = [new AttachmentBuilder(png, { name: fileName })];
    payload.embeds.push(new EmbedBuilder().setTitle("Твой личный графический тир-лист").setImage(`attachment://${fileName}`));
  }
  return payload;
}
// ====== GRAPHIC TIERLIST / PNG ======
function getGraphicTierlistState() {
  applyDbDefaults();
  return db.config.graphicTierlist;
}

function getGraphicMessageText() {
  const state = getGraphicTierlistState();
  const raw = String(state.messageText ?? DEFAULT_GRAPHIC_MESSAGE_TEXT).trim();
  return raw || DEFAULT_GRAPHIC_MESSAGE_TEXT;
}

function getGraphicMessageTextModalValue() {
  const text = getGraphicMessageText();
  return text.length <= 4000 ? text : text.slice(0, 4000);
}

function getGraphicGuideText() {
  const state = getGraphicTierlistState();
  const raw = String(state.guideText ?? DEFAULT_GRAPHIC_GUIDE_TEXT).trim();
  return raw || DEFAULT_GRAPHIC_GUIDE_TEXT;
}

function getGraphicGuideTextModalValue() {
  const text = getGraphicGuideText();
  return text.length <= 4000 ? text : text.slice(0, 4000);
}

function previewGraphicMessageText(max = 220) {
  const text = getGraphicMessageText().replace(/\s+/g, " ").trim();
  return text.length <= max ? text : `${text.slice(0, Math.max(1, max - 1)).trimEnd()}…`;
}

function previewGraphicGuideText(max = 220) {
  const text = getGraphicGuideText().replace(/\s+/g, " ").trim();
  return text.length <= max ? text : `${text.slice(0, Math.max(1, max - 1)).trimEnd()}…`;
}

function getGraphicDashboardEmbedDescription() {
  return getGraphicMessageText();
}

function buildGuidePayload() {
  return {
    embeds: [new EmbedBuilder().setTitle("Гайд").setDescription(getGraphicGuideText().slice(0, 4096))],
    ephemeral: true,
  };
}

function getGraphicImageConfig() {
  const cfg = getGraphicTierlistState().image || {};
  const w = Number(cfg.width) || 2000;
  const h = Number(cfg.height) || 1400;
  const icon = Number(cfg.icon) || 112;
  return {
    W: Math.max(1200, w),
    H: Math.max(700, h),
    ICON: Math.max(64, icon),
  };
}

function getUnknownBandRows() {
  const state = getGraphicTierlistState();
  return Math.max(1, Math.min(4, Math.round(Number(state.layout?.unknownBandRows) || 2)));
}

function applyGraphicImageDelta(kind, delta) {
  const state = getGraphicTierlistState();
  const cfg = getGraphicImageConfig();
  if (kind === "icon") state.image.icon = Math.max(64, Math.min(256, cfg.ICON + delta));
  if (kind === "width") state.image.width = Math.max(1200, Math.min(4096, cfg.W + delta));
  if (kind === "height") state.image.height = Math.max(700, Math.min(2160, cfg.H + delta));
}

function resetGraphicImageOverrides() {
  const state = getGraphicTierlistState();
  state.image.width = null;
  state.image.height = null;
  state.image.icon = null;
}

function listGraphicFontFiles() {
  const candidates = [
    path.join(__dirname, "assets", "fonts"),
    "/usr/share/fonts/truetype/dejavu",
    "/usr/share/fonts/truetype/liberation2",
    "/usr/share/fonts/truetype/freefont",
  ];
  const out = [];
  for (const dir of candidates) {
    try {
      if (!fs.existsSync(dir)) continue;
      for (const f of fs.readdirSync(dir)) {
        if (f.toLowerCase().endsWith(".ttf")) out.push(path.join(dir, f));
      }
    } catch {}
  }
  return out;
}

function pickGraphicFontFiles() {
  const preferredPairs = [
    ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "system-dejavu"],
    ["/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf", "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf", "system-liberation"],
    [path.join(__dirname, "assets", "fonts", "NotoSans-Regular.ttf"), path.join(__dirname, "assets", "fonts", "NotoSans-Bold.ttf"), "repo-assets"],
  ];

  for (const [regularFile, boldFile, source] of preferredPairs) {
    if (fs.existsSync(regularFile) && fs.existsSync(boldFile)) {
      return { regularFile, boldFile, usedFallback: false, source, loadError: null };
    }
  }

  const any = listGraphicFontFiles();
  if (any.length) return { regularFile: any[0], boldFile: any[0], usedFallback: true, source: "any-ttf", loadError: null };
  return { regularFile: null, boldFile: null, usedFallback: true, source: "none", loadError: "No TTF fonts found" };
}

function ensureGraphicFonts() {
  if (!PImage) return false;
  if (graphicFontsReady) return true;
  const picked = pickGraphicFontFiles();
  GRAPHIC_FONT_INFO = picked;
  if (!picked.regularFile || !picked.boldFile) {
    graphicFontsReady = false;
    return false;
  }
  try {
    PImage.registerFont(picked.regularFile, GRAPHIC_FONT_REG).loadSync();
    PImage.registerFont(picked.boldFile, GRAPHIC_FONT_BOLD).loadSync();
    GRAPHIC_FONT_INFO.loadError = null;
    graphicFontsReady = true;
    return true;
  }
  catch (err) {
    GRAPHIC_FONT_INFO.loadError = String(err?.message || err || "font load failed");
    graphicFontsReady = false;
    return false;
  }
}

function setGraphicFont(ctx, px, kind = "regular") {
  const family = kind === "bold" ? GRAPHIC_FONT_BOLD : GRAPHIC_FONT_REG;
  ctx.font = `${Math.max(1, Math.floor(px))}px ${family}`;
}

function measureGraphicTextWidth(ctx, text) {
  try { return Number(ctx.measureText(String(text || "")).width) || 0; }
  catch { return String(text || "").length * 12; }
}

function centerGraphicTextX(ctx, text, left, width) {
  const tw = measureGraphicTextWidth(ctx, text);
  return Math.floor(left + Math.max(0, (width - tw) / 2));
}

function wrapGraphicTextLines(ctx, text, maxWidth, maxLines = 3) {
  const source = String(text || "").trim();
  if (!source) return [""];
  const words = source.split(/\s+/).filter(Boolean);
  const pieces = [];

  for (const word of words) {
    if (measureGraphicTextWidth(ctx, word) <= maxWidth) {
      pieces.push(word);
      continue;
    }
    let chunk = "";
    for (const ch of word) {
      const candidate = chunk + ch;
      if (!chunk || measureGraphicTextWidth(ctx, candidate) <= maxWidth) chunk = candidate;
      else {
        pieces.push(chunk);
        chunk = ch;
      }
    }
    if (chunk) pieces.push(chunk);
  }

  const out = [];
  let line = "";
  for (const part of pieces) {
    const candidate = line ? `${line} ${part}` : part;
    if (!line || measureGraphicTextWidth(ctx, candidate) <= maxWidth) line = candidate;
    else {
      out.push(line);
      line = part;
    }
  }
  if (line) out.push(line);

  if (out.length <= maxLines) return out;
  const trimmed = out.slice(0, maxLines);
  let last = trimmed[maxLines - 1];
  while (last.length > 1 && measureGraphicTextWidth(ctx, `${last}…`) > maxWidth) last = last.slice(0, -1).trimEnd();
  trimmed[maxLines - 1] = `${last}…`;
  return trimmed;
}

function fitGraphicWrappedText(ctx, text, kind, maxWidth, maxHeight, startPx, minPx = 22, maxLines = 3) {
  for (let px = startPx; px >= minPx; px -= 2) {
    setGraphicFont(ctx, px, kind);
    const lines = wrapGraphicTextLines(ctx, text, maxWidth, maxLines);
    const lineH = Math.max(px + 4, Math.floor(px * 1.15));
    const totalH = lines.length * lineH;
    const widest = Math.max(...lines.map((line) => measureGraphicTextWidth(ctx, line)), 0);
    if (widest <= maxWidth && totalH <= maxHeight) return { px, lines, lineH, totalH };
  }
  setGraphicFont(ctx, minPx, kind);
  const lines = wrapGraphicTextLines(ctx, text, maxWidth, maxLines);
  const lineH = Math.max(minPx + 4, Math.floor(minPx * 1.15));
  return { px: minPx, lines, lineH, totalH: lines.length * lineH };
}

function trimGraphicTextToWidth(ctx, text, maxWidth) {
  let out = String(text || "").trim();
  if (!out) return "";
  if (measureGraphicTextWidth(ctx, out) <= maxWidth) return out;
  while (out.length > 1 && measureGraphicTextWidth(ctx, `${out}…`) > maxWidth) out = out.slice(0, -1).trimEnd();
  return out.length ? `${out}…` : "";
}

function fitGraphicSingleLineText(ctx, text, kind, maxWidth, startPx, minPx = 10) {
  const source = String(text || "").trim();
  if (!source) return { px: minPx, text: "" };
  for (let px = startPx; px >= minPx; px -= 1) {
    setGraphicFont(ctx, px, kind);
    if (measureGraphicTextWidth(ctx, source) <= maxWidth) return { px, text: source };
  }
  setGraphicFont(ctx, minPx, kind);
  return { px: minPx, text: trimGraphicTextToWidth(ctx, source, maxWidth) };
}

function drawGraphicOutlinedText(ctx, text, x, y, fill = "#ffffff", outline = "#000000") {
  const offsets = [
    [-2, 0], [2, 0], [0, -2], [0, 2],
    [-1, -1], [1, -1], [-1, 1], [1, 1],
  ];
  ctx.fillStyle = outline;
  for (const [dx, dy] of offsets) ctx.fillText(text, x + dx, y + dy);
  ctx.fillStyle = fill;
  ctx.fillText(text, x, y);
}

function drawGraphicAvatarNameplates(ctx, player, x, yy, rowIcon) {
  const displayName = String(player?.name || player?.username || player?.userId || "").trim();
  const displayUsername = String(player?.username || player?.name || player?.userId || "").trim();

  const topBarH = Math.max(14, Math.floor(rowIcon * 0.18));
  const bottomBarH = Math.max(18, Math.floor(rowIcon * 0.24));

  if (displayName) {
    ctx.fillStyle = "rgba(0,0,0,0.72)";
    ctx.fillRect(x, yy, rowIcon, topBarH);
    const nameFit = fitGraphicSingleLineText(
      ctx,
      displayName,
      "bold",
      Math.max(10, rowIcon - 10),
      Math.max(9, Math.floor(rowIcon * 0.145)),
      8,
    );
    setGraphicFont(ctx, nameFit.px, "bold");
    ctx.fillStyle = "rgba(255,255,255,0.98)";
    const nameY = yy + Math.max(7, Math.floor((topBarH + nameFit.px) / 2) - 2);
    ctx.fillText(nameFit.text, centerGraphicTextX(ctx, nameFit.text, x, rowIcon), nameY);
  }

  if (displayUsername) {
    ctx.fillStyle = "rgba(0,0,0,0.78)";
    ctx.fillRect(x, yy + rowIcon - bottomBarH, rowIcon, bottomBarH);
    const usernameFit = fitGraphicSingleLineText(
      ctx,
      displayUsername,
      "bold",
      Math.max(10, rowIcon - 10),
      Math.max(10, Math.floor(rowIcon * 0.18)),
      9,
    );
    setGraphicFont(ctx, usernameFit.px, "bold");
    ctx.fillStyle = "rgba(255,255,255,0.98)";
    const usernameY = yy + rowIcon - Math.max(5, Math.floor((bottomBarH - usernameFit.px) / 2)) - 1;
    ctx.fillText(usernameFit.text, centerGraphicTextX(ctx, usernameFit.text, x, rowIcon), usernameY);
  }
}

function drawGraphicAvatarBadge(ctx, player, x, yy, rowIcon) {
  const badgeText = String(player?.badgeText || "").trim();
  if (!badgeText) return;
  const badgeW = Math.max(18, Math.floor(rowIcon * 0.24));
  const badgeH = Math.max(16, Math.floor(rowIcon * 0.2));
  const bx = x + rowIcon - badgeW - 4;
  const by = yy + Math.max(6, Math.floor(rowIcon * 0.28));
  ctx.fillStyle = "rgba(0,0,0,0.82)";
  ctx.fillRect(bx, by, badgeW, badgeH);
  const fit = fitGraphicSingleLineText(ctx, badgeText, "bold", badgeW - 4, Math.max(9, Math.floor(badgeH * 0.8)), 8);
  setGraphicFont(ctx, fit.px, "bold");
  ctx.fillStyle = "rgba(255,255,255,0.98)";
  const textY = by + Math.max(10, Math.floor((badgeH + fit.px) / 2) - 2);
  ctx.fillText(fit.text, centerGraphicTextX(ctx, fit.text, bx, badgeW), textY);
}

function drawGraphicTierTitle(ctx, text, boxX, boxY, boxW, boxH) {
  const fit = fitGraphicWrappedText(ctx, text, "bold", boxW, boxH, 56, 22, 3);
  fillColor(ctx, "#111111");
  setGraphicFont(ctx, fit.px, "bold");
  let y = Math.floor(boxY + Math.max(0, (boxH - fit.totalH) / 2)) + fit.px;
  for (const line of fit.lines) {
    ctx.fillText(line, boxX, y);
    y += fit.lineH;
  }
}

async function fetchGraphicAvatarFromUrl(url) {
  const normalized = normalizeDiscordAvatarUrl(url || "");
  if (!normalized) return { img: null, buf: null, url: "" };
  const cacheHit = graphicAvatarCache.get(normalized);
  if (cacheHit) return { img: cacheHit, buf: null, url: normalized };

  try {
    const buf = await downloadToBuffer(normalized, 15000);
    const img = await decodeImageFromBuffer(buf);
    if (img) {
      graphicAvatarCache.set(normalized, img);
      return { img, buf, url: normalized };
    }
  } catch {}

  return { img: null, buf: null, url: normalized };
}

async function getFreshDiscordAvatarUrls(client, userId) {
  const urls = [];
  if (!client || !userId) return urls;

  try {
    const guild = await getGuild(client);
    const member = guild ? await guild.members.fetch(userId).catch(() => null) : null;
    if (member) {
      const memberUrl = normalizeDiscordAvatarUrl(member.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
      if (memberUrl) urls.push(memberUrl);
      const user = member.user || null;
      if (user) {
        const userUrl = normalizeDiscordAvatarUrl(user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
        const defaultUrl = normalizeDiscordAvatarUrl(user.defaultAvatarURL || "");
        if (userUrl) urls.push(userUrl);
        if (defaultUrl) urls.push(defaultUrl);
      }
    }
  } catch {}

  try {
    const user = await client.users.fetch(userId).catch(() => null);
    if (user) {
      const userUrl = normalizeDiscordAvatarUrl(user.displayAvatarURL({ extension: "png", forceStatic: true, size: 256 }));
      const defaultUrl = normalizeDiscordAvatarUrl(user.defaultAvatarURL || "");
      if (userUrl) urls.push(userUrl);
      if (defaultUrl) urls.push(defaultUrl);
    }
  } catch {}

  return [...new Set(urls.filter(Boolean))];
}

async function loadGraphicAvatarForPlayer(client, person) {
  const userId = person?.userId || "";
  if (userId && graphicAvatarCache.has(`disk:${userId}`)) return graphicAvatarCache.get(`disk:${userId}`);

  const diskImg = await loadGraphicAvatarFromDisk(userId);
  if (diskImg) return diskImg;

  const candidates = [];
  const push = (url) => {
    const normalized = normalizeDiscordAvatarUrl(url || "");
    if (normalized) candidates.push(normalized);
  };

  push(person?.avatarUrl);
  for (const freshUrl of await getFreshDiscordAvatarUrls(client, userId)) push(freshUrl);

  for (const url of [...new Set(candidates)]) {
    const res = await fetchGraphicAvatarFromUrl(url);
    if (!res.img) continue;
    if (userId && res.buf) {
      saveGraphicAvatarBufferToDisk(userId, res.buf);
      graphicAvatarCache.set(`disk:${userId}`, res.img);
    }
    if (person && person.avatarUrl !== res.url) {
      person.avatarUrl = res.url;
      const stored = db.people?.[userId];
      if (stored) {
        stored.avatarUrl = res.url;
        stored.updatedAt = nowIso();
        saveDB(db);
      }
    }
    return res.img;
  }
  return null;
}

async function hydrateGraphicAvatarUrls(client) {
  let changed = 0;
  for (const [userId, person] of Object.entries(db.people || {})) {
    const current = normalizeDiscordAvatarUrl(person?.avatarUrl || "");
    const freshList = await getFreshDiscordAvatarUrls(client, userId);
    const best = freshList[0] || current || "";
    if (!best) continue;
    if (best !== person.avatarUrl) {
      person.avatarUrl = best;
      person.updatedAt = nowIso();
      changed++;
    }
  }
  if (changed) saveDB(db);
  return changed;
}

async function hydrateGraphicUsernames(client) {
  let changed = 0;
  for (const [userId, person] of Object.entries(db.people || {})) {
    const identity = await getFreshDiscordIdentity(client, userId);
    const nextUsername = String(identity.username || person?.username || "").trim();
    const nextName = String(identity.name || person?.name || "").trim();
    if (nextUsername && nextUsername !== person.username) {
      person.username = nextUsername;
      changed++;
    }
    if (nextName && nextName !== person.name) {
      person.name = nextName;
      changed++;
    }
  }
  if (changed) saveDB(db);
  return changed;
}

async function renderGraphicTierlistPng(client = null) {
  if (!PImage) throw new Error("Не найден модуль pureimage. Установи: npm i pureimage");
  if (!ensureGraphicFonts()) throw new Error(`Не удалось загрузить системный шрифт для PNG. source=${GRAPHIC_FONT_INFO.source || "none"}. ${GRAPHIC_FONT_INFO.loadError || ""}`.trim());

  const state = getGraphicTierlistState();
  const buckets = buildGraphicBucketsFromPeople();
  const entries = Object.values(db.people || {});
  const { W, H: H_CFG, ICON } = getGraphicImageConfig();

  const topY = 120;
  const leftW = Math.floor(W * 0.24);
  const rightPadding = 36;
  const footerH = 44;

  const rowLayout = BOARD_ROW_ORDER.map((rowId) => {
    const list = buckets[rowId] || [];
    const rowIcon = Math.max(42, Math.floor(ICON * getRowIconScale(rowId)));
    const gap = Math.max(10, Math.floor(rowIcon * 0.16));
    const rightW = W - leftW - rightPadding - 24;
    const cols = Math.max(1, Math.floor((rightW + gap) / (rowIcon + gap)));

    if (rowId === "unknown") {
      const bandRows = getUnknownBandRows();
      const bandCapacity = Math.max(1, cols * bandRows);
      const groupsNeeded = Math.max(1, Math.ceil(list.length / bandCapacity));
      const iconsH = Math.max(1, groupsNeeded) * (bandRows * rowIcon + (bandRows - 1) * gap) + Math.max(0, groupsNeeded - 1) * gap;
      const needed = 18 + iconsH + 22 + 12;
      const rowH = Math.max(needed, 18 + (bandRows * rowIcon + (bandRows - 1) * gap) + 22 + 12);
      return { rowId, list, rowIcon, gap, cols, rowH, bandRows, groupsNeeded };
    }

    const rowsNeeded = Math.max(1, Math.ceil(list.length / cols));
    const iconsH = rowsNeeded * (rowIcon + gap) - gap;
    const needed = 18 + iconsH + 22 + 12;
    const rowH = Math.max(needed, 160);
    return { rowId, list, rowIcon, gap, cols, rowH, bandRows: 1, groupsNeeded: rowsNeeded };
  });

  const neededH = topY + rowLayout.reduce((sum, r) => sum + r.rowH, 0) + footerH;
  const H = Math.max(H_CFG, neededH);

  const img = PImage.make(W, H);
  const ctx = img.getContext("2d");

  fillColor(ctx, "#242424");
  ctx.fillRect(0, 0, W, H);

  fillColor(ctx, "#ffffff");
  setGraphicFont(ctx, 64, "bold");
  ctx.fillText(state.title || GRAPHIC_TIERLIST_TITLE, 40, 82);

  fillColor(ctx, "#cfcfcf");
  setGraphicFont(ctx, 22, "regular");
  ctx.fillText(`people: ${entries.length}. raters: ${countPeopleWhoRatedOnce()}. updated: ${new Date().toLocaleString("ru-RU")}`, 40, H - 18);

  let yCursor = topY;
  for (const row of rowLayout) {
    const { rowId, list, rowIcon, gap, cols, rowH } = row;
    const y = yCursor;
    yCursor += rowH;

    fillColor(ctx, "#2f2f2f");
    ctx.fillRect(leftW, y, W - leftW - rightPadding, rowH - 12);

    fillColor(ctx, getRowColor(rowId));
    ctx.fillRect(40, y, leftW - 40, rowH - 12);

    const blockH = rowH - 12;
    const labelX = 40 + 56;
    const labelW = (leftW - 40) - 56 - 18;
    const bottomLabelY = y + blockH - 18;
    const titleBoxY = y + 16;
    const titleBoxH = Math.max(44, bottomLabelY - titleBoxY - 18);

    drawGraphicTierTitle(ctx, getRowLabel(rowId), labelX, titleBoxY, labelW, titleBoxH);
    fillColor(ctx, "#111111");
    setGraphicFont(ctx, 24, "regular");
    ctx.fillText(rowId === "unknown" ? "UNKNOWN" : rowId === "new" ? "NEW" : `TIER ${rowId}`, labelX, bottomLabelY);

    const rightX = leftW + 24;
    const rightY = y + 18;

    for (let idx = 0; idx < list.length; idx++) {
      const player = list[idx];
      let x = rightX;
      let yy = rightY;

      if (rowId === "unknown") {
        const bandRows = Math.max(1, row.bandRows || getUnknownBandRows());
        const bandCapacity = Math.max(1, cols * bandRows);
        const groupIndex = Math.floor(idx / bandCapacity);
        const withinGroup = idx % bandCapacity;
        const rowInGroup = Math.floor(withinGroup / cols);
        const col = withinGroup % cols;
        x = rightX + col * (rowIcon + gap);
        yy = rightY + groupIndex * (bandRows * rowIcon + bandRows * gap) + rowInGroup * (rowIcon + gap);
      } else {
        const col = idx % cols;
        const rowIndex = Math.floor(idx / cols);
        x = rightX + col * (rowIcon + gap);
        yy = rightY + rowIndex * (rowIcon + gap);
      }
      const avatar = await loadGraphicAvatarForPlayer(client, player);

      fillColor(ctx, "#171717");
      ctx.fillRect(x - 3, yy - 3, rowIcon + 6, rowIcon + 6);

      if (avatar) {
        ctx.drawImage(avatar, x, yy, rowIcon, rowIcon);
      } else {
        fillColor(ctx, "#555555");
        ctx.fillRect(x, yy, rowIcon, rowIcon);
        fillColor(ctx, "#f3f3f3");
        setGraphicFont(ctx, Math.max(16, Math.floor(rowIcon * 0.28)), "bold");
        const initials = String(player.name || "?")
          .trim()
          .split(/\s+/)
          .slice(0, 2)
          .map((s) => s[0] || "")
          .join("")
          .toUpperCase() || "?";
        const ix = x + Math.max(8, Math.floor((rowIcon - (initials.length * Math.max(14, Math.floor(rowIcon * 0.16)))) / 2));
        const iy = yy + Math.floor(rowIcon / 2) + Math.max(8, Math.floor(rowIcon * 0.08));
        ctx.fillText(initials, ix, iy);
      }

      drawGraphicAvatarNameplates(ctx, player, x, yy, rowIcon);
      drawGraphicAvatarBadge(ctx, player, x, yy, rowIcon);

    }
  }

  const chunks = [];
  const stream = new PassThrough();
  stream.on("data", (c) => chunks.push(c));
  await PImage.encodePNGToStream(img, stream);
  stream.end();
  return Buffer.concat(chunks);
}

function formatCoefficientListLines() {
  const cfg = getCoefficientConfig();
  return [
    `global vote weights: 5=${getGlobalRowWeight("5").toFixed(2)} 4=${getGlobalRowWeight("4").toFixed(2)} 3=${getGlobalRowWeight("3").toFixed(2)} 2=${getGlobalRowWeight("2").toFixed(2)} 1=${getGlobalRowWeight("1").toFixed(2)} unknown=${getGlobalRowWeight("unknown").toFixed(2)}`,
    `row influence: 5=${getRowInfluenceWeight("5").toFixed(3)} 4=${getRowInfluenceWeight("4").toFixed(3)} 3=${getRowInfluenceWeight("3").toFixed(3)} 2=${getRowInfluenceWeight("2").toFixed(3)} 1=${getRowInfluenceWeight("1").toFixed(3)} unknown=${getRowInfluenceWeight("unknown").toFixed(3)} new=${getRowInfluenceWeight("new").toFixed(3)}`,
    `person evaluator overrides: ${Object.keys(cfg.evaluatorWeights || {}).length}`,
    `person target bias overrides: ${Object.keys(cfg.targetBiases || {}).length}`,
  ];
}

let graphicRefreshPromise = Promise.resolve(false);
let graphicAutoBumpTimer = null;
const GRAPHIC_AUTO_BUMP_INTERVAL_MS = 1000 * 60 * 30;

function scheduleGraphicTierlistRefresh(client) {
  graphicRefreshPromise = graphicRefreshPromise
    .catch(() => false)
    .then(async () => {
      try {
        return await refreshGraphicTierlist(client);
      } catch (err) {
        console.error("Graphic refresh failed:", err?.message || err);
        return false;
      }
    });
  return graphicRefreshPromise;
}

async function ensureGraphicMessageNotPinned(msg) {
  if (!msg?.pinned) return false;
  try {
    await msg.unpin("Auto-unpin graphic tierlist dashboard");
    return true;
  } catch {
    return false;
  }
}

async function getGraphicTierlistChannel(client) {
  const state = getGraphicTierlistState();
  const channelId = state.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID;
  if (!channelId) return null;
  const channel = await client.channels.fetch(channelId).catch(() => null);
  return channel?.isTextBased() ? channel : null;
}

async function hasMessagesAfterGraphicDashboard(client) {
  const state = getGraphicTierlistState();
  const channel = await getGraphicTierlistChannel(client);
  if (!channel || !state.dashboardMessageId) return false;

  const msg = await channel.messages.fetch(state.dashboardMessageId).catch(() => null);
  if (!msg) return true;

  await ensureGraphicMessageNotPinned(msg).catch(() => false);

  const latestBatch = await channel.messages.fetch({ limit: 1 }).catch(() => null);
  const latestMsg = latestBatch?.first?.() || null;
  if (!latestMsg) return false;
  return latestMsg.id !== msg.id && latestMsg.createdTimestamp >= msg.createdTimestamp;
}

async function maybeAutoBumpGraphicTierlist(client) {
  const state = getGraphicTierlistState();
  const channel = await getGraphicTierlistChannel(client);
  if (!channel) return false;

  if (!state.dashboardMessageId) {
    await ensureGraphicTierlistMessage(client, channel.id);
    return true;
  }

  const hasLaterMessages = await hasMessagesAfterGraphicDashboard(client);
  if (!hasLaterMessages) return false;

  return await bumpGraphicTierlist(client);
}

function startGraphicTierlistAutoBump(client) {
  if (graphicAutoBumpTimer) clearInterval(graphicAutoBumpTimer);
  graphicAutoBumpTimer = setInterval(() => {
    void maybeAutoBumpGraphicTierlist(client).catch((err) => {
      console.error("Graphic auto-bump failed:", err?.message || err);
    });
  }, GRAPHIC_AUTO_BUMP_INTERVAL_MS);
  if (typeof graphicAutoBumpTimer.unref === "function") graphicAutoBumpTimer.unref();
  return graphicAutoBumpTimer;
}

function buildGraphicDashboardComponents() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("rate_start").setLabel("Оценивать").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("rate_my_status").setLabel("Мой статус").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId("rate_guide").setLabel("Гайд").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId("rate_reset_all").setLabel("Оценить заново").setStyle(ButtonStyle.Danger),
    ),
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("graphic_refresh").setLabel("Обновить PNG").setStyle(ButtonStyle.Secondary),
      new ButtonBuilder().setCustomId("graphic_panel").setLabel("PNG панель").setStyle(ButtonStyle.Secondary),
    ),
  ];
}

async function ensureGraphicTierlistMessage(client, forcedChannelId = null) {
  const state = getGraphicTierlistState();
  const channelId = forcedChannelId || state.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID;
  if (!channelId) return null;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) throw new Error("GRAPHIC_TIERLIST_CHANNEL_ID: не текстовый канал");

  let msg = null;
  if (state.dashboardMessageId) {
    try { msg = await channel.messages.fetch(state.dashboardMessageId); } catch {}
  }

  await hydrateGraphicAvatarUrls(client).catch(() => 0);
  await hydrateGraphicUsernames(client).catch(() => 0);

  const png = await renderGraphicTierlistPng(client);
  const attachment = new AttachmentBuilder(png, { name: "people-tierlist.png" });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage("attachment://people-tierlist.png");

  if (!msg) {
    msg = await channel.send({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents() });
    state.dashboardMessageId = msg.id;
  } else {
    await msg.edit({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents(), attachments: [] });
  }

  await ensureGraphicMessageNotPinned(msg).catch(() => false);

  state.dashboardChannelId = channelId;
  state.lastUpdated = Date.now();
  saveDB(db);
  return msg;
}

async function refreshGraphicTierlist(client) {
  const state = getGraphicTierlistState();
  if (!state.dashboardChannelId || !state.dashboardMessageId) {
    if (GRAPHIC_TIERLIST_CHANNEL_ID) {
      await ensureGraphicTierlistMessage(client, GRAPHIC_TIERLIST_CHANNEL_ID);
      return true;
    }
    return false;
  }

  const channel = await client.channels.fetch(state.dashboardChannelId).catch(() => null);
  if (!channel?.isTextBased()) return false;

  let msg = null;
  try { msg = await channel.messages.fetch(state.dashboardMessageId); } catch {}
  if (!msg) {
    await ensureGraphicTierlistMessage(client, state.dashboardChannelId);
    return true;
  }

  await hydrateGraphicAvatarUrls(client).catch(() => 0);
  await hydrateGraphicUsernames(client).catch(() => 0);
  const png = await renderGraphicTierlistPng(client);
  const attachment = new AttachmentBuilder(png, { name: "people-tierlist.png" });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage("attachment://people-tierlist.png");

  await msg.edit({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents(), attachments: [] });
  await ensureGraphicMessageNotPinned(msg).catch(() => false);
  state.lastUpdated = Date.now();
  saveDB(db);
  return true;
}

async function bumpGraphicTierlist(client) {
  const state = getGraphicTierlistState();
  const channelId = state.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID;
  if (!channelId) return false;

  const channel = await client.channels.fetch(channelId).catch(() => null);
  if (!channel?.isTextBased()) return false;

  await hydrateGraphicUsernames(client).catch(() => 0);
  const png = await renderGraphicTierlistPng(client);
  const attachment = new AttachmentBuilder(png, { name: "people-tierlist.png" });
  const embed = new EmbedBuilder()
    .setTitle(state.title || GRAPHIC_TIERLIST_TITLE)
    .setDescription(getGraphicDashboardEmbedDescription())
    .setImage("attachment://people-tierlist.png");

  const oldMessageId = state.dashboardMessageId || "";
  const msg = await channel.send({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents() });
  await ensureGraphicMessageNotPinned(msg).catch(() => false);

  state.dashboardChannelId = channel.id;
  state.dashboardMessageId = msg.id;
  state.lastUpdated = Date.now();
  saveDB(db);

  if (oldMessageId && oldMessageId !== msg.id) {
    const oldMsg = await channel.messages.fetch(oldMessageId).catch(() => null);
    if (oldMsg) await oldMsg.delete().catch(() => {});
  }

  return true;
}

function buildGraphicPanelRowSelect() {
  const graphic = getGraphicTierlistState();
  const selected = canonicalRowId(graphic.panel?.selectedRowId || "5") || "5";
  const menu = new StringSelectMenuBuilder()
    .setCustomId("graphic_panel_select_row")
    .setPlaceholder("Выбери строку для настройки")
    .setMinValues(1)
    .setMaxValues(1)
    .addOptions(
      BOARD_ROW_ORDER.map((rowId) => ({
        label: `${getRowLabel(rowId)}`,
        value: rowId,
        default: selected === rowId,
      }))
    );
  return new ActionRowBuilder().addComponents(menu);
}

function buildGraphicPanelPayload() {
  const graphic = getGraphicTierlistState();
  const cfg = getGraphicImageConfig();
  const selectedRowId = canonicalRowId(graphic.panel?.selectedRowId || "5") || "5";
  const rowLabel = getRowLabel(selectedRowId);
  const rowColor = getRowColor(selectedRowId);
  const rowScale = getRowIconScale(selectedRowId);

  const embed = new EmbedBuilder()
    .setTitle("PNG Panel")
    .setDescription([
      `**Title:** ${graphic.title || GRAPHIC_TIERLIST_TITLE}`,
      `**Канал:** ${graphic.dashboardChannelId ? `<#${graphic.dashboardChannelId}>` : "не задан"}`,
      `**Message ID:** ${graphic.dashboardMessageId || "—"}`,
      `**Картинка:** ${cfg.W}×${cfg.H}`,
      `**Базовый размер иконок:** ${cfg.ICON}px`,
      `**Выбранная строка:** ${selectedRowId} → **${rowLabel}**`,
      `**Цвет строки:** ${rowColor}`,
      `**Масштаб иконок строки:** ${rowScale}`,
      `**Текст сообщения:** ${previewGraphicMessageText(170)}`,
      `**Текст гайда:** ${previewGraphicGuideText(170)}`,
      `**Чёрная строка:** ${getUnknownBandRows()} ряда в одном горизонтальном блоке`,
      "",
      "Stage 3 завершён. Карточки, матрица голосов, финальная агрегация и PNG-панель уже работают.",
    ].join("\n"));

  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("graphic_panel_refresh").setLabel("Пересобрать").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_title").setLabel("Название PNG").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId("graphic_panel_message_text").setLabel("Текст сообщения").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId("graphic_panel_guide_text").setLabel("Текст гайда").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId("graphic_panel_rename").setLabel("Переименовать строку").setStyle(ButtonStyle.Primary),
  );

  const row2 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("graphic_panel_icon_minus").setLabel("Иконки -").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_icon_plus").setLabel("Иконки +").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_w_minus").setLabel("Ширина -").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_w_plus").setLabel("Ширина +").setStyle(ButtonStyle.Secondary),
  );

  const row3 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("graphic_panel_h_minus").setLabel("Высота -").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_h_plus").setLabel("Высота +").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_set_color").setLabel("Цвет строки").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId("graphic_panel_reset_color").setLabel("Сброс цвета строки").setStyle(ButtonStyle.Secondary),
  );

  const row4 = buildGraphicPanelRowSelect();

  const row5 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("graphic_panel_reset_img").setLabel("Сбросить размеры").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_reset_colors").setLabel("Сбросить все цвета").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_clear_cache").setLabel("Сбросить кэш ав").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_fonts").setLabel("Шрифты").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_close").setLabel("Закрыть").setStyle(ButtonStyle.Danger),
  );

  return { embeds: [embed], components: [row1, row2, row3, row4, row5] };
}

// ====== COMMANDS ======
function buildCommands() {
  return [
    new SlashCommandBuilder()
      .setName(ROOT_COMMAND_NAME)
      .setDescription("People tierlist commands")
      .addSubcommand((s) => s.setName("my-status").setDescription("Показать мой статус и заготовленную статистику"))
      .addSubcommand((s) => s.setName("stageplan").setDescription("План stage 1 → stage 3"))
      .addSubcommand((s) => s.setName("setup").setDescription("Создать/пересоздать PNG тир-лист в канале (модеры)")
        .addChannelOption((o) => o.setName("channel").setDescription("Канал для PNG тир-листа").setRequired(true)))
      .addSubcommand((s) => s.setName("rebuild").setDescription("Пересобрать PNG тир-лист (модеры)"))
      .addSubcommand((s) => s.setName("bump").setDescription("Отправить PNG тир-лист заново вниз канала (модеры)"))
      .addSubcommand((s) => s.setName("dashboard-status").setDescription("Статус PNG тир-листа и базы (модеры)"))
      .addSubcommand((s) => s.setName("panel").setDescription("Панель PNG тир-листа (модеры)"))
      .addSubcommand((s) => s.setName("add-person").setDescription("Добавить человека в пул оцениваемых (модеры)")
        .addUserOption((o) => o.setName("target").setDescription("Пользователь").setRequired(true))
        .addStringOption((o) => o.setName("row").setDescription("Stage 1 preview-строка").setRequired(false)
          .addChoices(
            { name: "5", value: "5" },
            { name: "4", value: "4" },
            { name: "3", value: "3" },
            { name: "2", value: "2" },
            { name: "1", value: "1" },
            { name: "new", value: "new" },
          )))
      .addSubcommand((s) => s.setName("remove-person").setDescription("Удалить человека из пула оцениваемых (модеры)")
        .addUserOption((o) => o.setName("target").setDescription("Пользователь").setRequired(true)))
      .addSubcommand((s) => s.setName("clear-tierlist").setDescription("Очистить тир-лист (модеры)")
        .addStringOption((o) => o.setName("mode").setDescription("Что чистить").setRequired(true)
          .addChoices(
            { name: "Полный вайп", value: "full" },
            { name: "Только оценки", value: "votes-only" },
          )))
      .addSubcommand((s) => s.setName("import-role").setDescription("Добавить в пул всех людей с ролью (модеры)")
        .addRoleOption((o) => o.setName("role").setDescription("Роль").setRequired(true)))
      .addSubcommand((s) => s.setName("set-row-labels").setDescription("Переименовать сразу все 7 строк (модеры)")
        .addStringOption((o) => o.setName("r5").setDescription("Название строки 5").setRequired(true))
        .addStringOption((o) => o.setName("r4").setDescription("Название строки 4").setRequired(true))
        .addStringOption((o) => o.setName("r3").setDescription("Название строки 3").setRequired(true))
        .addStringOption((o) => o.setName("r2").setDescription("Название строки 2").setRequired(true))
        .addStringOption((o) => o.setName("r1").setDescription("Название строки 1").setRequired(true))
        .addStringOption((o) => o.setName("unknown").setDescription("Название чёрной строки").setRequired(true))
        .addStringOption((o) => o.setName("new").setDescription("Название нижней строки").setRequired(true)))
      .addSubcommand((s) => s.setName("set-global-coefficients").setDescription("Настроить глобальные коэффициенты общего тир-листа (модеры)")
        .addNumberOption((o) => o.setName("r5").setDescription("Вес голоса 5").setRequired(true))
        .addNumberOption((o) => o.setName("r4").setDescription("Вес голоса 4").setRequired(true))
        .addNumberOption((o) => o.setName("r3").setDescription("Вес голоса 3").setRequired(true))
        .addNumberOption((o) => o.setName("r2").setDescription("Вес голоса 2").setRequired(true))
        .addNumberOption((o) => o.setName("r1").setDescription("Вес голоса 1").setRequired(true))
        .addNumberOption((o) => o.setName("unknown").setDescription("Вес нажатия Не знаю").setRequired(true)))
      .addSubcommand((s) => s.setName("set-person-coefficient").setDescription("Настроить индивидуальный коэффициент человека (модеры)")
        .addUserOption((o) => o.setName("target").setDescription("Пользователь").setRequired(true))
        .addStringOption((o) => o.setName("kind").setDescription("Тип индивидуального коэффициента").setRequired(true)
          .addChoices(
            { name: "evaluator_weight", value: "evaluator_weight" },
            { name: "target_bias", value: "target_bias" },
            { name: "reset", value: "reset" },
          ))
        .addNumberOption((o) => o.setName("value").setDescription("Значение. Для bias можно отрицательное.").setRequired(false)))
      .addSubcommand((s) => s.setName("coeff-status").setDescription("Показать текущие коэффициенты (модеры)")
        .addUserOption((o) => o.setName("target").setDescription("Пользователь").setRequired(false)))
      .addSubcommand((s) => s.setName("set-row-influence")
        .setDescription("Настроить влияние строк на силу голосов (модеры)")
        .addNumberOption((o) => o.setName("r5").setDescription("Влияние строки 5").setRequired(true))
        .addNumberOption((o) => o.setName("r4").setDescription("Влияние строки 4").setRequired(true))
        .addNumberOption((o) => o.setName("r3").setDescription("Влияние строки 3").setRequired(true))
        .addNumberOption((o) => o.setName("r2").setDescription("Влияние строки 2").setRequired(true))
        .addNumberOption((o) => o.setName("r1").setDescription("Влияние строки 1").setRequired(true))
        .addNumberOption((o) => o.setName("unknown").setDescription("Влияние строки не знают").setRequired(false))
        .addNumberOption((o) => o.setName("new").setDescription("Влияние строки новые").setRequired(false)))
      .addSubcommand((s) => s.setName("analyze-votes").setDescription("Открыть панель просмотра кто кому что поставил (модеры)")
        .addUserOption((o) => o.setName("evaluator").setDescription("Сразу открыть по оценщику").setRequired(false))
        .addUserOption((o) => o.setName("target").setDescription("Сразу открыть по цели").setRequired(false))
        .addIntegerOption((o) => o.setName("limit").setDescription("Строк на страницу: 5, 10 или 15").setRequired(false)))
  ].map((c) => c.toJSON());
}

async function registerGuildCommands(client) {
  if (!GUILD_ID) throw new Error("Нет GUILD_ID в .env");
  const guild = await client.guilds.fetch(GUILD_ID);
  await guild.commands.set(buildCommands());
}

// ====== CLIENT ======
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers,
  ],
});

client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);
  console.log(`Stage 3 boot. root command: /${ROOT_COMMAND_NAME}`);

  await registerGuildCommands(client);

  try {
    const graphic = getGraphicTierlistState();
    if (graphic.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID) {
      await ensureGraphicTierlistMessage(client, graphic.dashboardChannelId || GRAPHIC_TIERLIST_CHANNEL_ID);
      await maybeAutoBumpGraphicTierlist(client).catch(() => false);
      startGraphicTierlistAutoBump(client);
    }
  } catch (e) {
    console.error("Graphic tierlist setup failed:", e?.message || e);
  }

  if (migrationInfo.imported) {
    console.log(`Legacy migration imported ${migrationInfo.imported} people from old ratings.`);
  }

  console.log("Ready");
});

function getInteractionPayloadErrorMessage(err) {
  const message = String(err?.message || err || "Ошибка");
  if (/ENOENT|EACCES|EPERM|read-only|rename/i.test(message)) return "Проблема с памятью или записью файлов. Проверь volume и DB_PATH.";
  if (/Unknown interaction|10062/i.test(message)) return "Интеракция уже протухла. Нажми кнопку заново.";
  if (/Cannot send messages|Missing Access|Missing Permissions|50013/i.test(message)) return "У бота не хватает прав в этом канале.";
  if (/pureimage|font|PNG/i.test(message)) return "Проблема при сборке PNG. Проверь pureimage и шрифты.";
  return message.slice(0, 1800);
}

async function replyInteractionError(interaction, err) {
  const content = `Ошибка. ${getInteractionPayloadErrorMessage(err)}`;
  try {
    if (interaction.deferred) {
      await interaction.editReply({ content, embeds: [], components: [] });
      return;
    }
    if (interaction.replied) {
      await interaction.followUp({ content, ephemeral: true });
      return;
    }
    await interaction.reply({ content, ephemeral: true });
  } catch {}
}

client.on("interactionCreate", async (interaction) => {
  try {
  // ----- SLASH -----
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName !== ROOT_COMMAND_NAME) return;
    const sub = interaction.options.getSubcommand();

    if (sub === "my-status") {
      await interaction.reply(await buildMyStatusPayload(client, interaction.user.id));
      return;
    }

    if (sub === "stageplan") {
      await interaction.reply({ content: getStagePlanText(), ephemeral: true });
      return;
    }

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    if (sub === "setup") {
      await interaction.deferReply({ ephemeral: true });
      const channel = interaction.options.getChannel("channel", true);
      const graphic = getGraphicTierlistState();
      graphic.dashboardChannelId = channel.id;
      saveDB(db);
      await ensureGraphicTierlistMessage(client, channel.id);
      await interaction.editReply({ content: `PNG тир-лист создан/обновлён в <#${channel.id}>.` });
      await logLine(client, `SETUP_PNG by ${interaction.user.tag} in #${channel.id}`);
      return;
    }

    if (sub === "rebuild") {
      await interaction.deferReply({ ephemeral: true });
      const ok = await refreshGraphicTierlist(client);
      await interaction.editReply({ content: ok ? "PNG тир-лист обновлён." : "PNG тир-лист ещё не настроен. Сначала /setup." });
      return;
    }

    if (sub === "bump") {
      await interaction.deferReply({ ephemeral: true });
      const ok = await bumpGraphicTierlist(client);
      await interaction.editReply({ content: ok ? "PNG тир-лист отправлен заново вниз канала." : "PNG тир-лист ещё не настроен. Сначала /setup." });
      return;
    }

    if (sub === "dashboard-status") {
      const graphic = getGraphicTierlistState();
      const cfg = getGraphicImageConfig();
      const lines = [
        `root: /${ROOT_COMMAND_NAME}`,
        `channelId: ${graphic.dashboardChannelId || "—"}`,
        `messageId: ${graphic.dashboardMessageId || "—"}`,
        `title: ${graphic.title || GRAPHIC_TIERLIST_TITLE}`,
        `messageText: ${previewGraphicMessageText(140)}`,
        `guideText: ${previewGraphicGuideText(140)}`,
        `img: ${cfg.W}x${cfg.H}, icon=${cfg.ICON}, unknownBandRows=${getUnknownBandRows()}`,
        `selectedRow: ${graphic.panel?.selectedRowId || "5"} -> ${getRowLabel(graphic.panel?.selectedRowId || "5")}`,
        `people: ${Object.keys(db.people || {}).length}`,
        `voteMaps: ${Object.keys(db.votes || {}).length}`,
        `sessions: ${Object.keys(db.sessions || {}).length}`,
        `stageMeta: schema=${db.meta?.schemaVersion || 0}, stage=${db.meta?.stage || 0}`,
        ...formatCoefficientListLines(),
        `legacy migrated: ${db.legacy?.migratedFromRatings ? `yes (${db.legacy?.importedRatings || 0})` : "no"}`,
        `font regular: ${GRAPHIC_FONT_INFO.regularFile ? path.basename(GRAPHIC_FONT_INFO.regularFile) : "(none)"}`,
        `font bold: ${GRAPHIC_FONT_INFO.boldFile ? path.basename(GRAPHIC_FONT_INFO.boldFile) : "(none)"}`,
        `font source: ${GRAPHIC_FONT_INFO.source || "(none)"}`,
        `font error: ${GRAPHIC_FONT_INFO.loadError || "(none)"}`,
      ];
      await interaction.reply({ content: lines.join("\n"), ephemeral: true });
      return;
    }

    if (sub === "panel") {
      await interaction.reply({ ...buildGraphicPanelPayload(), ephemeral: true });
      return;
    }

    if (sub === "add-person") {
      await interaction.deferReply({ ephemeral: true });
      const target = interaction.options.getUser("target", true);
      const rowId = canonicalRowId(interaction.options.getString("row", false) || "");
      const res = await upsertPersonFromUser(client, target, { source: `manual:${interaction.user.id}`, stage1PinnedRowId: rowId || "" });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({
        content: `${res.created ? "Добавлен" : "Обновлён"}: <@${target.id}>${rowId ? ` | строка stage 1: **${getRowLabel(rowId)}**` : ""}`,
      });
      await logLine(client, `ADD_PERSON ${target.id} row=${rowId || "(auto)"} by ${interaction.user.tag}`);
      return;
    }

    if (sub === "remove-person") {
      await interaction.deferReply({ ephemeral: true });
      const target = interaction.options.getUser("target", true);
      const ok = removePersonAndVotes(target.id);
      if (!ok) {
        await interaction.editReply({ content: "Этого человека нет в пуле." });
        return;
      }
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({ content: `Удалил <@${target.id}> из пула оцениваемых людей.` });
      await logLine(client, `REMOVE_PERSON ${target.id} by ${interaction.user.tag}`);
      return;
    }

    if (sub === "clear-tierlist") {
      await interaction.deferReply({ ephemeral: true });
      const mode = interaction.options.getString("mode", true);
      const result = clearTierlistData(mode);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({
        content: result.mode === "votes-only"
          ? `Ок. Очищены только оценки. Было карт голосов: ${result.voteMapCount}, карт комментариев: ${result.commentMapCount}, сессий: ${result.sessionCount}. Люди оставлены.`
          : `Ок. Полный вайп готов. Было людей: ${result.peopleCount}, карт голосов: ${result.voteMapCount}, карт комментариев: ${result.commentMapCount}, сессий: ${result.sessionCount}. Настройки панели сохранены.`,
      });
      await logLine(client, `CLEAR_TIERLIST mode=${result.mode} by ${interaction.user.tag} people=${result.peopleCount} voteMaps=${result.voteMapCount} commentMaps=${result.commentMapCount} sessions=${result.sessionCount}`);
      return;
    }

    if (sub === "import-role") {
      await interaction.deferReply({ ephemeral: true });
      const role = interaction.options.getRole("role", true);
      const res = await importRoleMembers(client, role);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({
        content: `Импорт роли <@&${role.id}> готов. Найдено: ${res.total}. Создано: ${res.created}. Обновлено: ${res.updated}.`,
      });
      await logLine(client, `IMPORT_ROLE ${role.id} total=${res.total} by ${interaction.user.tag}`);
      return;
    }

    if (sub === "set-row-labels") {
      await interaction.deferReply({ ephemeral: true });
      db.config.rowLabels = {
        "5": interaction.options.getString("r5", true).slice(0, 32),
        "4": interaction.options.getString("r4", true).slice(0, 32),
        "3": interaction.options.getString("r3", true).slice(0, 32),
        "2": interaction.options.getString("r2", true).slice(0, 32),
        "1": interaction.options.getString("r1", true).slice(0, 32),
        unknown: interaction.options.getString("unknown", true).slice(0, 32),
        new: interaction.options.getString("new", true).slice(0, 32),
      };
      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({ content: "Названия всех 7 строк обновлены." });
      return;
    }

    if (sub === "set-global-coefficients") {
      await interaction.deferReply({ ephemeral: true });
      setGlobalRowWeight("5", interaction.options.getNumber("r5", true));
      setGlobalRowWeight("4", interaction.options.getNumber("r4", true));
      setGlobalRowWeight("3", interaction.options.getNumber("r3", true));
      setGlobalRowWeight("2", interaction.options.getNumber("r2", true));
      setGlobalRowWeight("1", interaction.options.getNumber("r1", true));
      setGlobalRowWeight("unknown", interaction.options.getNumber("unknown", true));
      refreshAllPeopleDerivedState();
      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({ content: `Глобальные коэффициенты обновлены. ${formatCoefficientListLines().join(" | ")}` });
      return;
    }

    if (sub === "set-person-coefficient") {
      await interaction.deferReply({ ephemeral: true });
      const target = interaction.options.getUser("target", true);
      const kind = interaction.options.getString("kind", true);
      const value = interaction.options.getNumber("value", false);
      if (kind === "reset") {
        clearPersonCoefficients(target.id);
      } else if (kind === "evaluator_weight") {
        if (!Number.isFinite(value) || value <= 0) {
          await interaction.editReply({ content: "Для evaluator_weight нужен value больше 0." });
          return;
        }
        setEvaluatorWeight(target.id, value);
      } else if (kind === "target_bias") {
        if (!Number.isFinite(value)) {
          await interaction.editReply({ content: "Для target_bias нужен value. Можно отрицательный." });
          return;
        }
        setTargetBias(target.id, value);
      }
      refreshAllPeopleDerivedState();
      saveDB(db);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply({ content: `Индивидуальный коэффициент обновлён для <@${target.id}>. evaluator=x${getEvaluatorWeight(target.id).toFixed(2)} bias=${getTargetBias(target.id).toFixed(2)}` });
      return;
    }

    if (sub === "coeff-status") {
      const target = interaction.options.getUser("target", false);
      if (target) {
        await interaction.reply({
          content: [`<@${target.id}>`, `evaluator weight: x${getEvaluatorWeight(target.id).toFixed(2)}`, `target bias: ${getTargetBias(target.id).toFixed(2)}`].join("\n"),
          ephemeral: true,
        });
        return;
      }
      await interaction.reply({ content: formatCoefficientListLines().join("\n"), ephemeral: true });
      return;
    }

    if (sub === "analyze-votes") {
      const evaluator = interaction.options.getUser("evaluator", false);
      const target = interaction.options.getUser("target", false);
      const limit = interaction.options.getInteger("limit", false);
      const panel = createVoteAuditPanel(interaction.user.id, {
        evaluatorId: evaluator?.id || "",
        targetId: target?.id || "",
        pageSize: limit || VOTE_AUDIT_PAGE_SIZES[1],
      });
      await interaction.reply(buildVoteAuditPanelPayload(panel.panelId, {
        headerText: `Открыта админ-панель голосов. Команда: /${ROOT_COMMAND_NAME} analyze-votes`,
      }));
      return;
    }

    return;
  }

  // ----- BUTTONS -----
  if (interaction.isButton()) {
    if (interaction.customId === "rate_start") {
      await interaction.deferReply({ ephemeral: true });
      const res = await upsertPersonFromUser(client, interaction.user, { source: "self-start" });
      const session = resumeRatingSession(interaction.user.id);
      const payload = await buildRatingCardPayload(client, interaction.user.id, session, {
        headerText: buildStartRatingText(res.created, session),
      });
      await interaction.editReply(payload);
      if (payload.autoDeleteMs) scheduleEphemeralDelete(interaction, payload.autoDeleteMs);
      void scheduleGraphicTierlistRefresh(client);
      return;
    }

    if (interaction.customId === "rate_my_status") {
      await interaction.reply(await buildMyStatusPayload(client, interaction.user.id));
      return;
    }

    if (interaction.customId === "rate_guide") {
      await interaction.reply(buildGuidePayload());
      return;
    }

    if (interaction.customId === "rate_back") {
      await interaction.deferUpdate();
      const result = rollbackLastSessionVote(interaction.user.id);
      if (!result.ok) {
        await interaction.followUp({ content: "Назад идти некуда. Это первая карточка в текущей сессии.", ephemeral: true });
        return;
      }
      await interaction.editReply(await buildRatingCardPayload(client, interaction.user.id, result.session, {
        headerText: `Возвращаю на предыдущего человека. Снял последний выбор: **${result.removedValue === "unknown" ? "Не знаю" : result.removedValue || "—"}**.`,
      }));
      return;
    }

    if (interaction.customId === "rate_reset_all") {
      await interaction.deferReply({ ephemeral: true });
      const rebuild = startReplacementSession(interaction.user.id);
      await interaction.editReply(await buildRatingCardPayload(client, interaction.user.id, rebuild.session, {
        headerText: `Начинаем полную переоценку заново. Старые голоса: ${rebuild.preservedVotes}. Старые комментарии: ${rebuild.preservedComments}. Они пока остаются в общем тир-листе и заменятся только после завершения нового личного тир-листа.`,
      }));
      return;
    }

    if (interaction.customId.startsWith("rate_comment:")) {
      const [, sessionId = "", targetId = ""] = String(interaction.customId).split(":");
      const person = db.people?.[targetId] || null;
      const modal = new ModalBuilder().setCustomId(`rate_comment_modal:${sessionId}:${targetId}`).setTitle("Анонимный комментарий");
      const existing = getStoredComment(interaction.user.id, targetId, { includeDraft: true });
      const input = new TextInputBuilder()
        .setCustomId("comment_text")
        .setLabel(person ? `Комментарий для ${person.username || person.name || person.userId}` : "Комментарий")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(false)
        .setMaxLength(1000)
        .setValue(String(existing?.text || "").slice(0, 1000));
      modal.addComponents(new ActionRowBuilder().addComponents(input));
      await interaction.showModal(modal);
      return;
    }

    if (interaction.customId.startsWith("rate_vote:")) {
      await interaction.deferUpdate();
      const [, sessionId = "", value = ""] = String(interaction.customId).split(":");
      const result = applyVoteFromSession(interaction.user.id, sessionId, value);

      if (!result.ok) {
        if (result.reason === "stale-session") {
          const fresh = result.session || ensureStage2Session(interaction.user.id, { forceNew: true });
          await interaction.editReply(await buildRatingCardPayload(client, interaction.user.id, fresh, {
            headerText: "Старая карточка устарела. Ниже уже свежая.",
          }));
          return;
        }

        if (result.reason === "no-target" || result.reason === "self-target") {
          const session = result.session || ensureStage2Session(interaction.user.id);
          await interaction.editReply(await buildRatingCardPayload(client, interaction.user.id, session, {
            headerText: "Текущая цель пропала или стала невалидной. Беру следующую.",
          }));
          return;
        }

        await interaction.followUp({ content: "Не удалось сохранить голос.", ephemeral: true });
        return;
      }

      const payload = await buildRatingCardPayload(client, interaction.user.id, result.session, {
        headerText: result.committed
          ? (result.committed.replacedOldVotes || result.committed.replacedOldComments
              ? "Голос сохранён. Новый личный тир-лист закончен и заменил твой старый вклад в общем тир-листе."
              : "Голос сохранён. Личный тир-лист закончен и слит в общий.")
          : "Голос сохранён в личный тир-лист.",
        lastActionText: `Последний выбор: **${result.written.vote.value === "unknown" ? "Не знаю" : result.written.vote.value}** для **${db.people?.[result.targetId]?.username || db.people?.[result.targetId]?.name || result.targetId}**.${result.committed ? ` Слитых голосов: ${result.committed.committedVotes || 0}.` : ""}${result.committed?.replacedOldVotes ? ` Заменено старых голосов: ${result.committed.replacedOldVotes}.` : ""}`,
        merged: result.committed || null,
      });
      await interaction.editReply(payload);
      if (payload.autoDeleteMs) scheduleEphemeralDelete(interaction, payload.autoDeleteMs);
      if (result.committed) void scheduleGraphicTierlistRefresh(client);
      return;
    }

    if (interaction.customId.startsWith("vote_audit_")) {
      const [, action = "", panelId = ""] = String(interaction.customId).match(/^vote_audit_([^:]+):?(.*)$/) || [];
      const panel = getVoteAuditPanel(panelId);
      if (!panel) {
        await interaction.reply({ content: "Эта панель уже устарела. Открой её заново командой.", ephemeral: true });
        return;
      }
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      if (interaction.user.id !== panel.ownerId) {
        await interaction.reply({ content: "Это не твоя панель.", ephemeral: true });
        return;
      }

      if (action === "close") {
        await interaction.update({ content: "Панель голосов закрыта.", embeds: [], components: [] });
        return;
      }

      if (action === "set_eval") {
        const modal = new ModalBuilder().setCustomId(`vote_audit_eval_modal:${panel.panelId}`).setTitle("Фильтр по оценщику");
        const input = new TextInputBuilder()
          .setCustomId("audit_person_query")
          .setLabel("ID, @упоминание или ник. Пусто = сброс")
          .setStyle(TextInputStyle.Short)
          .setRequired(false)
          .setMaxLength(120)
          .setValue(panel.evaluatorId ? getPersonDisplayLabel(panel.evaluatorId).slice(0, 120) : "");
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (action === "set_target") {
        const modal = new ModalBuilder().setCustomId(`vote_audit_target_modal:${panel.panelId}`).setTitle("Фильтр по цели");
        const input = new TextInputBuilder()
          .setCustomId("audit_person_query")
          .setLabel("ID, @упоминание или ник. Пусто = сброс")
          .setStyle(TextInputStyle.Short)
          .setRequired(false)
          .setMaxLength(120)
          .setValue(panel.targetId ? getPersonDisplayLabel(panel.targetId).slice(0, 120) : "");
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (action === "reset") {
        panel.evaluatorId = "";
        panel.targetId = "";
        panel.value = "";
        panel.commentsOnly = false;
        panel.page = 0;
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId, { headerText: "Фильтры сброшены." }));
        return;
      }

      if (action === "refresh") {
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId, { headerText: "Панель обновлена." }));
        return;
      }

      if (action === "prev") {
        panel.page = Math.max(0, Number(panel.page || 0) - 1);
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId));
        return;
      }

      if (action === "next") {
        panel.page = Math.max(0, Number(panel.page || 0) + 1);
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId));
        return;
      }

      if (action === "limit") {
        panel.pageSize = cycleVoteAuditPageSize(panel.pageSize);
        panel.page = 0;
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId, { headerText: `Лимит на страницу теперь ${panel.pageSize}.` }));
        return;
      }

      if (action === "comments") {
        panel.commentsOnly = !panel.commentsOnly;
        panel.page = 0;
        panel.updatedAt = nowIso();
        await interaction.update(buildVoteAuditPanelPayload(panel.panelId, { headerText: panel.commentsOnly ? "Показываю только голоса с комментариями." : "Снова показываю все голоса." }));
        return;
      }
    }

    if (interaction.customId === "graphic_refresh") {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      await interaction.deferReply({ ephemeral: true });
      const ok = await refreshGraphicTierlist(client);
      await interaction.editReply(ok ? "PNG тир-лист обновлён." : "PNG тир-лист ещё не настроен. Сначала /setup.");
      return;
    }

    if (interaction.customId === "graphic_panel") {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      await interaction.reply({ ...buildGraphicPanelPayload(), ephemeral: true });
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_")) {
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }

      const graphic = getGraphicTierlistState();
      const selectedRowId = canonicalRowId(graphic.panel?.selectedRowId || "5") || "5";

      if (interaction.customId === "graphic_panel_close") {
        await interaction.update({ content: "Ок.", embeds: [], components: [] });
        return;
      }

      if (interaction.customId === "graphic_panel_fonts") {
        if (!ensureGraphicFonts()) throw new Error(`Не удалось загрузить системный шрифт для PNG. source=${GRAPHIC_FONT_INFO.source || "none"}. ${GRAPHIC_FONT_INFO.loadError || ""}`.trim());
        const files = listGraphicFontFiles();
        const lines = [
          `ttf: ${files.length ? files.map((f) => path.basename(f)).join(", ") : "(none)"}`,
          `picked regular: ${GRAPHIC_FONT_INFO.regularFile ? path.basename(GRAPHIC_FONT_INFO.regularFile) : "(null)"}`,
          `picked bold: ${GRAPHIC_FONT_INFO.boldFile ? path.basename(GRAPHIC_FONT_INFO.boldFile) : "(null)"}`,
          `fallback: ${GRAPHIC_FONT_INFO.usedFallback}`,
          `source: ${GRAPHIC_FONT_INFO.source || "(none)"}`,
          `error: ${GRAPHIC_FONT_INFO.loadError || "(none)"}`,
        ];
        await interaction.reply({ content: lines.join("\n"), ephemeral: true });
        return;
      }

      if (interaction.customId === "graphic_panel_title") {
        const modal = new ModalBuilder().setCustomId("graphic_panel_title_modal").setTitle("Название PNG тир-листа");
        const input = new TextInputBuilder()
          .setCustomId("graphic_title")
          .setLabel("Название наверху картинки")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(80)
          .setValue(String(graphic.title || GRAPHIC_TIERLIST_TITLE).slice(0, 80));
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_message_text") {
        const modal = new ModalBuilder().setCustomId("graphic_panel_message_text_modal").setTitle("Текст сообщения PNG тир-листа");
        const input = new TextInputBuilder()
          .setCustomId("graphic_message_text")
          .setLabel("Текст под заголовком сообщения")
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(true)
          .setMaxLength(4000)
          .setValue(getGraphicMessageTextModalValue());
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_guide_text") {
        const modal = new ModalBuilder().setCustomId("graphic_panel_guide_text_modal").setTitle("Текст гайда PNG тир-листа");
        const input = new TextInputBuilder()
          .setCustomId("graphic_guide_text")
          .setLabel("Текст для кнопки «Гайд»")
          .setStyle(TextInputStyle.Paragraph)
          .setRequired(true)
          .setMaxLength(4000)
          .setValue(getGraphicGuideTextModalValue());
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_rename") {
        const modal = new ModalBuilder().setCustomId(`graphic_panel_rename_modal:${selectedRowId}`).setTitle(`Переименовать строку ${selectedRowId}`);
        const input = new TextInputBuilder()
          .setCustomId("row_name")
          .setLabel("Новое название")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(32)
          .setValue(String(getRowLabel(selectedRowId)).slice(0, 32));
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_set_color") {
        const modal = new ModalBuilder().setCustomId(`graphic_panel_color_modal:${selectedRowId}`).setTitle(`Цвет строки ${selectedRowId}`);
        const input = new TextInputBuilder()
          .setCustomId("row_color")
          .setLabel("HEX цвет. пример #ff6b6b")
          .setStyle(TextInputStyle.Short)
          .setRequired(true)
          .setMaxLength(7)
          .setValue(String(getRowColor(selectedRowId)).slice(0, 7));
        modal.addComponents(new ActionRowBuilder().addComponents(input));
        await interaction.showModal(modal);
        return;
      }

      if (interaction.customId === "graphic_panel_refresh") {
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_icon_minus" || interaction.customId === "graphic_panel_icon_plus") {
        applyGraphicImageDelta("icon", interaction.customId.endsWith("plus") ? 12 : -12);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_w_minus" || interaction.customId === "graphic_panel_w_plus") {
        applyGraphicImageDelta("width", interaction.customId.endsWith("plus") ? 200 : -200);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_h_minus" || interaction.customId === "graphic_panel_h_plus") {
        applyGraphicImageDelta("height", interaction.customId.endsWith("plus") ? 120 : -120);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_img") {
        resetGraphicImageOverrides();
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_color") {
        resetRowColor(selectedRowId);
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_reset_colors") {
        resetAllRowColors();
        saveDB(db);
        await interaction.deferUpdate();
        await refreshGraphicTierlist(client).catch(() => false);
        await interaction.editReply(buildGraphicPanelPayload());
        return;
      }

      if (interaction.customId === "graphic_panel_clear_cache") {
        clearGraphicAvatarCache();
        await interaction.reply({ content: "Кэш аватарок очищен. Следующая пересборка заново подтянет картинки.", ephemeral: true });
        return;
      }
    }
  }

  // ----- SELECT MENU -----
  if (interaction.isStringSelectMenu()) {
    if (interaction.customId.startsWith("vote_audit_value:")) {
      const panelId = String(interaction.customId).split(":")[1] || "";
      const panel = getVoteAuditPanel(panelId);
      if (!panel) {
        await interaction.reply({ content: "Эта панель уже устарела. Открой её заново командой.", ephemeral: true });
        return;
      }
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      if (interaction.user.id !== panel.ownerId) {
        await interaction.reply({ content: "Это не твоя панель.", ephemeral: true });
        return;
      }
      const picked = String(interaction.values?.[0] || "all");
      panel.value = picked === "all" ? "" : normalizeVoteValue(picked);
      panel.page = 0;
      panel.updatedAt = nowIso();
      await interaction.update(buildVoteAuditPanelPayload(panel.panelId, {
        headerText: `Фильтр по оценке: ${formatVoteAuditValueLabel(panel.value)}.`,
      }));
      return;
    }

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    if (interaction.customId === "graphic_panel_select_row") {
      const graphic = getGraphicTierlistState();
      graphic.panel.selectedRowId = canonicalRowId(interaction.values?.[0] || "5") || "5";
      saveDB(db);
      await interaction.update(buildGraphicPanelPayload());
      return;
    }
  }

  // ----- MODALS -----
  if (interaction.isModalSubmit()) {
    if (interaction.customId.startsWith("rate_comment_modal:")) {
      const [, sessionId = "", targetId = ""] = String(interaction.customId).split(":");
      const text = interaction.fields.getTextInputValue("comment_text") || "";
      const result = applyCommentFromSession(interaction.user.id, sessionId, targetId, text);
      if (!result.ok) {
        await interaction.reply({ content: result.reason === "stale-session" ? "Сессия уже устарела. Нажми Оценивать заново." : "Не удалось сохранить комментарий.", ephemeral: true });
        return;
      }
      await interaction.reply({ content: result.comment?.text ? "Анонимный комментарий сохранён. Он станет доступен человеку в Мой статус, когда личный тир-лист сольётся в общий." : "Комментарий очищен.", ephemeral: true });
      return;
    }

    if (interaction.customId.startsWith("vote_audit_eval_modal:") || interaction.customId.startsWith("vote_audit_target_modal:")) {
      const isEval = interaction.customId.startsWith("vote_audit_eval_modal:");
      const panelId = String(interaction.customId).split(":")[1] || "";
      const panel = getVoteAuditPanel(panelId);
      if (!panel) {
        await interaction.reply({ content: "Эта панель уже устарела. Открой её заново командой.", ephemeral: true });
        return;
      }
      if (!isModerator(interaction.member)) {
        await interaction.reply({ content: "Нет прав.", ephemeral: true });
        return;
      }
      if (interaction.user.id !== panel.ownerId) {
        await interaction.reply({ content: "Это не твоя панель.", ephemeral: true });
        return;
      }

      const raw = interaction.fields.getTextInputValue("audit_person_query") || "";
      const resolved = resolveVoteAuditPersonQuery(raw);
      if (!resolved.ok) {
        await interaction.reply({ content: resolved.error, ephemeral: true });
        return;
      }

      if (isEval) panel.evaluatorId = resolved.userId || "";
      else panel.targetId = resolved.userId || "";
      panel.page = 0;
      panel.updatedAt = nowIso();

      const label = isEval ? "оценщика" : "цели";
      const headerText = resolved.cleared
        ? `Фильтр ${label} очищен.`
        : `Фильтр ${label}: ${getPersonDisplayLabel(resolved.userId)}.`;

      const updated = await updateVoteAuditModalInteraction(interaction, buildVoteAuditPanelPayload(panel.panelId, { headerText }));
      if (!updated) {
        await interaction.reply({ content: headerText, ephemeral: true });
      }
      return;
    }

    if (!isModerator(interaction.member)) {
      await interaction.reply({ content: "Нет прав.", ephemeral: true });
      return;
    }

    if (interaction.customId === "graphic_panel_title_modal") {
      const graphic = getGraphicTierlistState();
      const title = (interaction.fields.getTextInputValue("graphic_title") || "").trim().slice(0, 80);
      if (!title) {
        await interaction.reply({ content: "Пустое название.", ephemeral: true });
        return;
      }
      graphic.title = title;
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Теперь PNG называется: **${title}**.`);
      return;
    }

    if (interaction.customId === "graphic_panel_message_text_modal") {
      const graphic = getGraphicTierlistState();
      const text = (interaction.fields.getTextInputValue("graphic_message_text") || "").trim();
      if (!text) {
        await interaction.reply({ content: "Пустой текст.", ephemeral: true });
        return;
      }
      graphic.messageText = text.slice(0, 4000);
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply("Ок. Текст сообщения PNG обновлён.");
      return;
    }

    if (interaction.customId === "graphic_panel_guide_text_modal") {
      const graphic = getGraphicTierlistState();
      const text = (interaction.fields.getTextInputValue("graphic_guide_text") || "").trim();
      if (!text) {
        await interaction.reply({ content: "Пустой текст.", ephemeral: true });
        return;
      }
      graphic.guideText = text.slice(0, 4000);
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await interaction.editReply("Ок. Текст гайда обновлён.");
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_rename_modal:")) {
      const rowId = canonicalRowId(interaction.customId.split(":")[1] || "5") || "5";
      const name = (interaction.fields.getTextInputValue("row_name") || "").trim().slice(0, 32);
      if (!name) {
        await interaction.reply({ content: "Пустое имя.", ephemeral: true });
        return;
      }
      db.config.rowLabels[rowId] = name;
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Теперь строка **${rowId}** называется: **${name}**.`);
      return;
    }

    if (interaction.customId.startsWith("graphic_panel_color_modal:")) {
      const rowId = canonicalRowId(interaction.customId.split(":")[1] || "5") || "5";
      const raw = interaction.fields.getTextInputValue("row_color");
      const hex = normalizeHexColor(raw);
      if (!hex) {
        await interaction.reply({ content: "Нужен HEX цвет вида #ff6b6b", ephemeral: true });
        return;
      }
      setRowColor(rowId, hex);
      saveDB(db);
      await interaction.deferReply({ ephemeral: true });
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.editReply(`Ок. Цвет строки **${rowId}** теперь **${hex}**.`);
      return;
    }
  }
  
  } catch (err) {
    console.error("interactionCreate failed:", err?.stack || err);
    await replyInteractionError(interaction, err);
  }
});

process.on("unhandledRejection", (err) => {
  console.error("unhandledRejection:", err?.stack || err);
});

process.on("uncaughtException", (err) => {
  console.error("uncaughtException:", err?.stack || err);
});

// ====== BOOT ======
if (!DISCORD_TOKEN) {
  console.error("Нет DISCORD_TOKEN в .env");
  process.exit(1);
}

client.login(DISCORD_TOKEN);
