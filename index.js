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
  "Финальный этап активен: кнопка «Начать оценку» запускает личные карточки с 5 тирами и «не знаю».",
  "Кнопка «Мой статус» показывает твой прогресс, личный тир-лист, последние действия и текущую статистику по голосам.",
].join(" ");

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
    return { config: {}, people: {}, votes: {}, sessions: {}, meta: {}, legacy: {} };
  }
  try {
    const data = JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
    data.config ||= {};
    data.people ||= {};
    data.votes ||= {};
    data.sessions ||= {};
    data.meta ||= {};
    data.legacy ||= {};
    return data;
  } catch {
    return { config: {}, people: {}, votes: {}, sessions: {}, meta: {}, legacy: {} };
  }
}

function saveDB(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2), "utf8");
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

  for (const rowId of BOARD_ROW_ORDER) {
    if (!db.config.rowLabels[rowId]) db.config.rowLabels[rowId] = DEFAULT_ROW_LABELS[rowId];
    if (!db.config.rowColors[rowId]) db.config.rowColors[rowId] = DEFAULT_ROW_COLORS[rowId];
    if (!db.config.rowIconScales[rowId]) db.config.rowIconScales[rowId] = DEFAULT_ROW_ICON_SCALES[rowId];
  }

  db.config.graphicTierlist ||= {
    title: GRAPHIC_TIERLIST_TITLE,
    dashboardChannelId: GRAPHIC_TIERLIST_CHANNEL_ID || "",
    dashboardMessageId: "",
    lastUpdated: 0,
    image: { width: null, height: null, icon: null },
    panel: { selectedRowId: "5" },
    layout: { unknownBandRows: 2 },
    messageText: DEFAULT_GRAPHIC_MESSAGE_TEXT,
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

function countVotesGivenBy(userId) {
  const map = db.votes?.[userId] || {};
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
  let lastVoteAt = "";
  const byValue = { "5": 0, "4": 0, "3": 0, "2": 0, "1": 0, unknown: 0 };

  for (const voterMap of Object.values(db.votes || {})) {
    const vote = voterMap?.[targetId];
    const value = normalizeVoteValue(vote?.value);
    if (!value) continue;

    total++;
    if (String(vote?.updatedAt || "") > lastVoteAt) lastVoteAt = String(vote.updatedAt || "");
    if (value === "unknown") {
      unknown++;
      byValue.unknown++;
    } else {
      known++;
      sumKnown += Number(value) || 0;
      byValue[value] = (byValue[value] || 0) + 1;
    }
  }

  const average = known ? sumKnown / known : null;
  return {
    total,
    known,
    unknown,
    sumKnown,
    average,
    roundedAverage: average == null ? null : Math.max(1, Math.min(5, Math.round(average))),
    unknownShare: total ? unknown / total : 0,
    lastVoteAt,
    byValue,
  };
}

function choosePreviewRowIdFromAggregate(agg) {
  if (!agg || !agg.total) return "new";
  if (!agg.known && agg.unknown) return "unknown";
  if (!agg.known) return "unknown";

  const unknownIsDominant = agg.unknown >= Math.max(3, agg.known + 1);
  const unknownShareIsHigh = agg.total >= 4 && agg.unknownShare >= 0.6 && agg.unknown >= agg.known;
  if (unknownIsDominant || unknownShareIsHigh) return "unknown";

  return String(Math.max(1, Math.min(5, Math.round(agg.average || 3))));
}

function buildAggregateForTarget(targetId) {
  const received = countVotesReceivedBy(targetId);
  return {
    total: received.total,
    knownCount: received.known,
    unknownCount: received.unknown,
    sumKnown: received.sumKnown,
    average: received.average,
    roundedAverage: received.roundedAverage,
    unknownShare: received.unknownShare,
    distribution: received.byValue,
    rowId: choosePreviewRowIdFromAggregate(received),
    lastVoteAt: received.lastVoteAt,
  };
}

function refreshAllPeopleDerivedState(save = false) {
  let changed = 0;

  for (const [userId, person] of Object.entries(db.people || {})) {
    if (!person || !userId) continue;

    const agg = buildAggregateForTarget(userId);
    const fallbackRow = canonicalRowId(person.previewRowId || person.stage1PinnedRowId || person.legacy?.tier || "new") || "new";
    const nextPreviewRow = agg.total > 0 ? agg.rowId : fallbackRow;
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
  const rowId = getBoardRowForPerson(person);
  if (NUMERIC_ROW_IDS.has(rowId)) return rowId;
  if (rowId === "unknown") return "?";
  return "NEW";
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
      if ((bv.average || 0) !== (av.average || 0)) return (bv.average || 0) - (av.average || 0);
      if ((bv.knownCount || 0) !== (av.knownCount || 0)) return (bv.knownCount || 0) - (av.knownCount || 0);
    } else {
      const aCreated = String(a.createdAt || "");
      const bCreated = String(b.createdAt || "");
      if (aCreated !== bCreated) return bCreated.localeCompare(aCreated);
    }

    return String(a.username || a.name || a.userId || "").localeCompare(String(b.username || b.name || b.userId || ""), "ru");
  });
}

function buildGraphicBucketsFromPeople() {
  const buckets = Object.fromEntries(BOARD_ROW_ORDER.map((id) => [id, []]));
  for (const person of Object.values(db.people || {})) {
    if (!person?.userId) continue;
    const aggregate = person.stage2Aggregate || buildAggregateForTarget(person.userId);
    const rowId = getBoardRowForPerson(person);
    if (!buckets[rowId]) continue;
    buckets[rowId].push({
      userId: person.userId,
      name: person.name || person.userId,
      username: String(person.username || "").trim() || person.name || person.userId,
      avatarUrl: normalizeDiscordAvatarUrl(person.avatarUrl || ""),
      badgeText: formatBadgeForPerson(person),
      rowId,
      aggregate,
      received: {
        total: aggregate.total,
        known: aggregate.knownCount,
        unknown: aggregate.unknownCount,
      },
      createdAt: person.createdAt || "",
      source: person.source || "manual",
    });
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

  if (db.sessions?.[userId]) delete db.sessions[userId];
  for (const session of Object.values(db.sessions || {})) {
    if (!session || session.activeTargetId !== userId) continue;
    session.activeTargetId = "";
    session.updatedAt = nowIso();
  }

  refreshAllPeopleDerivedState();
  saveDB(db);
  return true;
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
  const givenMap = db.votes?.[userId] || {};
  return getEligibleTargetIdsForEvaluator(userId).filter((targetId) => !normalizeVoteValue(givenMap?.[targetId]?.value));
}

function getCurrentVote(evaluatorId, targetId) {
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
      stage: 3,
    };
    db.sessions[userId] = session;
  }

  session.stage = 3;
  session.updatedAt = now;

  const currentTargetId = String(session.activeTargetId || "");
  const currentStillValid = currentTargetId && currentTargetId !== userId && !getCurrentVote(userId, currentTargetId) && db.people?.[currentTargetId];
  if (!currentStillValid) session.activeTargetId = pickNextTargetForEvaluator(userId);

  saveDB(db);
  return session;
}

function setVoteForTarget(evaluatorId, targetId, value, meta = {}) {
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
    via: meta.via || previous?.via || "stage2",
    sessionId: meta.sessionId || previous?.sessionId || "",
  };

  if (db.people[targetId]) db.people[targetId].updatedAt = now;
  if (db.people[evaluatorId]) db.people[evaluatorId].updatedAt = now;

  refreshAllPeopleDerivedState();
  saveDB(db);
  return { previous, vote: db.votes[evaluatorId][targetId] };
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

  const written = setVoteForTarget(userId, targetId, value, { via: "stage2-card", sessionId });
  session.lastCompletedTargetId = targetId;
  session.lastVoteValue = normalizeVoteValue(value);
  session.votesCastThisSession = Number(session.votesCastThisSession || 0) + 1;
  session.updatedAt = nowIso();
  session.history = Array.isArray(session.history) ? session.history : [];
  session.history.unshift({
    targetId,
    value: normalizeVoteValue(value),
    at: session.updatedAt,
  });
  session.history = session.history.slice(0, db.config.sessionHistoryLimit || SESSION_HISTORY_LIMIT);
  session.activeTargetId = pickNextTargetForEvaluator(userId);
  if (!session.activeTargetId) session.completedAt = session.updatedAt;
  else delete session.completedAt;
  saveDB(db);

  return { ok: true, targetId, session, written };
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

function buildPersonalTierFields(userId) {
  const map = db.votes?.[userId] || {};
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

function buildSessionHistoryLines(userId, max = 6) {
  const history = Array.isArray(db.sessions?.[userId]?.history) ? db.sessions[userId].history : [];
  return history.slice(0, max).map((item) => {
    const person = db.people?.[item.targetId];
    const label = person?.username || person?.name || item.targetId || "—";
    const value = item.value === "unknown" ? "Не знаю" : item.value || "—";
    return `• ${label} → ${value}`;
  });
}

function buildMyStatusPayload(userId) {
  const person = db.people?.[userId] || null;
  const given = countVotesGivenBy(userId);
  const received = person?.stage2Aggregate || buildAggregateForTarget(userId);
  const rowId = person ? getBoardRowForPerson(person) : "";
  const eligible = Math.max(0, getEligibleTargetIdsForEvaluator(userId).length);
  const remaining = Math.max(0, eligible - given.total);
  const progressPct = eligible ? Math.min(100, Math.round((given.total / eligible) * 100)) : 0;
  const session = db.sessions?.[userId] || null;
  const activeTarget = session?.activeTargetId ? db.people?.[session.activeTargetId] : null;
  const lastTarget = session?.lastCompletedTargetId ? db.people?.[session.lastCompletedTargetId] : null;
  const historyLines = buildSessionHistoryLines(userId);
  const summary = new EmbedBuilder()
    .setTitle("Мой статус")
    .setDescription([
      person
        ? `Ты уже в пуле оцениваемых людей. Текущая строка: **${getRowLabel(rowId)}**.`
        : "Тебя ещё нет в пуле оцениваемых людей. Кнопка «Начать оценку» автоматически добавит тебя.",
      `Прогресс оценки: **${given.total}/${eligible}**. Осталось без твоей оценки: **${remaining}**. Готовность: **${progressPct}%**.`,
      `Ты поставил обычных оценок: **${given.known}**. Нажатий «не знаю»: **${given.unknown}**.`,
      `Тебе поставили оценок: **${received.total || 0}**. Обычных: **${received.knownCount || 0}**. «Не знаю»: **${received.unknownCount || 0}**.`,
      `Текущая общая средняя по тебе: **${formatAverage(received.average)}**.`,
      `Распределение полученных голосов: **${formatDistributionLine(received.distribution)}**.`,
      activeTarget ? `Сейчас в активной сессии следующий человек: **${activeTarget.username || activeTarget.name || activeTarget.userId}**.` : "Активной карточки сейчас нет.",
      lastTarget ? `Последний человек, которого ты оценил: **${lastTarget.username || lastTarget.name || lastTarget.userId}**. Последний выбор: **${session?.lastVoteValue === "unknown" ? "Не знаю" : session?.lastVoteValue || "—"}**.` : null,
      person?.legacy?.tier ? `Legacy import найден. Старый tier: **${person.legacy.tier}**.` : null,
      "Stage 3 завершён. Общая доска считает агрегат, чёрная строка раскладывается в два уровня, PNG-панель сохранена.",
    ].filter(Boolean).join("\n"));

  const personal = new EmbedBuilder()
    .setTitle("Мой личный тир-лист")
    .setDescription("Это последний личный расклад людей, который ты уже составил.")
    .addFields(buildPersonalTierFields(userId));

  const history = new EmbedBuilder()
    .setTitle("Последние действия")
    .setDescription(historyLines.length ? historyLines.join("\n") : "Пока история пуста.");

  return { embeds: [summary, personal, history], ephemeral: true };
}

function buildStartRatingText(created) {
  return created
    ? "Ты добавлен в пул оцениваемых людей. Ниже сразу первая карточка."
    : "Продолжаем. Ниже твоя текущая карточка на оценку.";
}

function buildRateButtons(sessionId) {
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
    ),
  ];
}

function buildSessionProgressLine(userId) {
  const given = countVotesGivenBy(userId);
  const eligible = Math.max(0, getEligibleTargetIdsForEvaluator(userId).length);
  const remaining = Math.max(0, eligible - given.total);
  return `Прогресс: ${given.total}/${eligible}. Осталось: ${remaining}.`;
}

function buildNoTargetsPayload(userId, options = {}) {
  const given = countVotesGivenBy(userId);
  const desc = [
    options.reason === "no-people"
      ? "Пока в пуле нет других людей для оценки."
      : "У тебя пока нет новых людей без оценки.",
    `Ты уже раздал голосов: **${given.total}**.`,
    "Когда модеры добавят новых людей или появятся новые аккаунты в пуле, кнопка «Начать оценку» снова даст карточку.",
  ].join("\n");

  return {
    embeds: [new EmbedBuilder().setTitle("Оценка завершена").setDescription(desc)],
    components: [],
    ephemeral: true,
  };
}

function buildRatingCardPayload(userId, session, options = {}) {
  const targetId = String(session?.activeTargetId || "");
  if (!targetId || !db.people?.[targetId]) {
    return buildNoTargetsPayload(userId, { reason: Object.keys(db.people || {}).length <= 1 ? "no-people" : "finished" });
  }

  const person = db.people[targetId];
  const agg = person.stage2Aggregate || buildAggregateForTarget(targetId);
  const distributionLine = formatDistributionLine(agg.distribution);
  const embed = new EmbedBuilder()
    .setTitle("Оценка человека")
    .setDescription([
      options.headerText || "Выбери тир или нажми «Не знаю». Голос сохраняется сразу.",
      `**${person.username || person.name || person.userId}**`,
      `<@${person.userId}>`,
      buildSessionProgressLine(userId),
      `У него уже есть голосов: **${agg.total || 0}**. Обычных: **${agg.knownCount || 0}**. «Не знаю»: **${agg.unknownCount || 0}**.`,
      `Средняя: **${formatAverage(agg.average)}**. Общая строка: **${getRowLabel(getBoardRowForPerson(person))}**.`,
      `Распределение: **${distributionLine}**.`,
      options.lastActionText || null,
    ].filter(Boolean).join("\n"));

  if (person.avatarUrl) embed.setThumbnail(person.avatarUrl);

  return {
    embeds: [embed],
    components: buildRateButtons(session.sessionId),
    ephemeral: true,
  };
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

function previewGraphicMessageText(max = 220) {
  const text = getGraphicMessageText().replace(/\s+/g, " ").trim();
  return text.length <= max ? text : `${text.slice(0, Math.max(1, max - 1)).trimEnd()}…`;
}

function getGraphicDashboardEmbedDescription() {
  return getGraphicMessageText();
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
  } catch (err) {
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
  ctx.fillText(`people: ${entries.length}. stage: 3. updated: ${new Date().toLocaleString("ru-RU")}`, 40, H - 18);

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
        const col = Math.floor(withinGroup / bandRows);
        const rowInGroup = withinGroup % bandRows;
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

      const usernameBarH = Math.max(18, Math.floor(rowIcon * 0.24));
      ctx.fillStyle = "rgba(0,0,0,0.78)";
      ctx.fillRect(x, yy + rowIcon - usernameBarH, rowIcon, usernameBarH);

      const usernameFit = fitGraphicSingleLineText(
        ctx,
        String(player.username || player.name || player.userId || "").trim(),
        "bold",
        Math.max(10, rowIcon - 10),
        Math.max(10, Math.floor(rowIcon * 0.18)),
        9,
      );
      setGraphicFont(ctx, usernameFit.px, "bold");
      ctx.fillStyle = "rgba(255,255,255,0.98)";
      const usernameY = yy + rowIcon - Math.max(5, Math.floor((usernameBarH - usernameFit.px) / 2)) - 1;
      ctx.fillText(usernameFit.text, centerGraphicTextX(ctx, usernameFit.text, x, rowIcon), usernameY);

      const badgeText = String(player.badgeText || "");
      const badgePx = Math.max(14, Math.floor(rowIcon * 0.22));
      setGraphicFont(ctx, badgePx, "bold");
      const badgeW = measureGraphicTextWidth(ctx, badgeText);
      const badgeX = x + rowIcon - badgeW - 8;
      const badgeY = yy + badgePx + 8;
      drawGraphicOutlinedText(ctx, badgeText, badgeX, badgeY, "#ffffff", "#000000");
    }
  }

  const chunks = [];
  const stream = new PassThrough();
  stream.on("data", (c) => chunks.push(c));
  await PImage.encodePNGToStream(img, stream);
  stream.end();
  return Buffer.concat(chunks);
}

function buildGraphicDashboardComponents() {
  return [
    new ActionRowBuilder().addComponents(
      new ButtonBuilder().setCustomId("rate_start").setLabel("Начать оценку").setStyle(ButtonStyle.Primary),
      new ButtonBuilder().setCustomId("rate_my_status").setLabel("Мой статус").setStyle(ButtonStyle.Secondary),
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
    try { await msg.pin(); } catch {}
    state.dashboardMessageId = msg.id;
  } else {
    await msg.edit({ embeds: [embed], files: [attachment], components: buildGraphicDashboardComponents(), attachments: [] });
  }

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
      `**Чёрная строка:** ${getUnknownBandRows()} ряда в одном горизонтальном блоке`,
      "",
      "Stage 3 завершён. Карточки, матрица голосов, финальная агрегация и PNG-панель уже работают.",
    ].join("\n"));

  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder().setCustomId("graphic_panel_refresh").setLabel("Пересобрать").setStyle(ButtonStyle.Secondary),
    new ButtonBuilder().setCustomId("graphic_panel_title").setLabel("Название PNG").setStyle(ButtonStyle.Primary),
    new ButtonBuilder().setCustomId("graphic_panel_message_text").setLabel("Текст сообщения").setStyle(ButtonStyle.Primary),
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
      .addSubcommand((s) => s.setName("dashboard-status").setDescription("Статус PNG тир-листа и stage 1 базы (модеры)"))
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
    }
  } catch (e) {
    console.error("Graphic tierlist setup failed:", e?.message || e);
  }

  if (migrationInfo.imported) {
    console.log(`Legacy migration imported ${migrationInfo.imported} people from old ratings.`);
  }

  console.log("Ready");
});

client.on("interactionCreate", async (interaction) => {
  // ----- SLASH -----
  if (interaction.isChatInputCommand()) {
    if (interaction.commandName !== ROOT_COMMAND_NAME) return;
    const sub = interaction.options.getSubcommand();

    if (sub === "my-status") {
      await interaction.reply(buildMyStatusPayload(interaction.user.id));
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
        `img: ${cfg.W}x${cfg.H}, icon=${cfg.ICON}, unknownBandRows=${getUnknownBandRows()}`,
        `selectedRow: ${graphic.panel?.selectedRowId || "5"} -> ${getRowLabel(graphic.panel?.selectedRowId || "5")}`,
        `people: ${Object.keys(db.people || {}).length}`,
        `voteMaps: ${Object.keys(db.votes || {}).length}`,
        `sessions: ${Object.keys(db.sessions || {}).length}`,
        `stageMeta: schema=${db.meta?.schemaVersion || 0}, stage=${db.meta?.stage || 0}`,
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

    return;
  }

  // ----- BUTTONS -----
  if (interaction.isButton()) {
    if (interaction.customId === "rate_start") {
      const res = await upsertPersonFromUser(client, interaction.user, { source: "self-start" });
      const session = ensureStage2Session(interaction.user.id);
      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.reply(buildRatingCardPayload(interaction.user.id, session, {
        headerText: buildStartRatingText(res.created),
      }));
      return;
    }

    if (interaction.customId === "rate_my_status") {
      await interaction.reply(buildMyStatusPayload(interaction.user.id));
      return;
    }

    if (interaction.customId.startsWith("rate_vote:")) {
      const [, sessionId = "", value = ""] = String(interaction.customId).split(":");
      const result = applyVoteFromSession(interaction.user.id, sessionId, value);

      if (!result.ok) {
        if (result.reason === "stale-session") {
          const fresh = result.session || ensureStage2Session(interaction.user.id, { forceNew: true });
          await interaction.update(buildRatingCardPayload(interaction.user.id, fresh, {
            headerText: "Старая карточка устарела. Ниже уже свежая.",
          }));
          return;
        }

        if (result.reason === "no-target" || result.reason === "self-target") {
          const session = result.session || ensureStage2Session(interaction.user.id);
          await interaction.update(buildRatingCardPayload(interaction.user.id, session, {
            headerText: "Текущая цель пропала или стала невалидной. Беру следующую.",
          }));
          return;
        }

        await interaction.reply({ content: "Не удалось сохранить голос.", ephemeral: true });
        return;
      }

      await refreshGraphicTierlist(client).catch(() => false);
      await interaction.update(buildRatingCardPayload(interaction.user.id, result.session, {
        headerText: "Голос сохранён.",
        lastActionText: `Последний выбор: **${result.written.vote.value === "unknown" ? "Не знаю" : result.written.vote.value}** для **${db.people?.[result.targetId]?.username || db.people?.[result.targetId]?.name || result.targetId}**.`,
      }));
      return;
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
});

// ====== BOOT ======
if (!DISCORD_TOKEN) {
  console.error("Нет DISCORD_TOKEN в .env");
  process.exit(1);
}

client.login(DISCORD_TOKEN);
