// ============================================================
//  БУНКЕР — сервер гри (Express + Socket.io)
//  Один файл, весь стан гри тримається в пам'яті процесу.
// ============================================================

const path = require("path");
const fs = require("fs");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, "public")));

// ------------------------------------------------------------
// Завантаження карт з cards.json (можна редагувати без зміни коду)
// ------------------------------------------------------------
const CARD_DATA = JSON.parse(fs.readFileSync(path.join(__dirname, "cards.json"), "utf-8"));
const CATEGORY_KEYS = Object.keys(CARD_DATA.categories); // порядок категорій

// ------------------------------------------------------------
// Стан кімнат: { [code]: Room }
// ------------------------------------------------------------
const rooms = {};

function randomCode() {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // без 0/O/1/I
  let code;
  do {
    code = Array.from({ length: 5 }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
  } while (rooms[code]);
  return code;
}

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// Роздати кожному гравцю по одній випадковій картці з кожної категорії
function dealCards() {
  const dealt = {};
  for (const key of CATEGORY_KEYS) {
    dealt[key] = pickRandom(CARD_DATA.categories[key].items);
  }
  return dealt;
}

function createRoom(hostPid, hostSocketId, hostNickname, settings) {
  const code = randomCode();
  const room = {
    code,
    hostId: hostPid,
    settings: {
      seats: Math.max(1, Math.min(20, settings.seats || 4)), // скільки виживають
      revealTimeSec: settings.revealTimeSec || 0, // 0 = без таймера
    },
    phase: "lobby", // lobby | playing | voting | round_result | finished
    players: [], // { id, nickname, alive, cards:{}, revealed:{}, isHost }
    order: [], // порядок ходів (масив player id), фіксується при старті
    turnIndex: 0,
    round: 1,
    disaster: null,
    bunker: null,
    votes: {}, // voterId -> targetId
    lastElimination: null,
    createdAt: Date.now(),
  };
  rooms[code] = room;
  addPlayer(room, hostPid, hostSocketId, hostNickname, true);
  return room;
}

function addPlayer(room, pid, socketId, nickname, isHost = false) {
  const player = {
    id: pid, // постійний ID гравця (не змінюється при перепідключенні)
    socketId, // поточне з'єднання (змінюється при перепідключенні)
    connected: true,
    disconnectTimer: null,
    nickname: nickname.trim().slice(0, 24) || "Гравець",
    alive: true,
    isHost,
    cards: null,
    revealed: {},
    votesReceived: 0,
  };
  room.players.push(player);
  return player;
}

function getPlayer(room, id) {
  return room.players.find((p) => p.id === id);
}

function alivePlayers(room) {
  return room.players.filter((p) => p.alive);
}

// Публічний "вигляд" кімнати, який безпечно надсилати всім
// (приховані картки інших гравців не включаються)
function publicRoomState(room, forId) {
  return {
    code: room.code,
    phase: room.phase,
    hostId: room.hostId,
    round: room.round,
    settings: room.settings,
    disaster: room.disaster,
    bunker: room.bunker,
    turnPlayerId: room.phase === "playing" ? room.order[room.turnIndex] : null,
    lastElimination: room.lastElimination,
    categoryLabels: Object.fromEntries(CATEGORY_KEYS.map((k) => [k, CARD_DATA.categories[k].label])),
    categoryOrder: CATEGORY_KEYS,
    votesCastCount: Object.keys(room.votes).length,
    players: room.players.map((p) => ({
      id: p.id,
      nickname: p.nickname,
      alive: p.alive,
      isHost: p.isHost,
      connected: p.connected,
      isYou: p.id === forId,
      revealed: p.revealed, // { category: value } тільки розкриті картки
      hiddenCount: p.cards ? CATEGORY_KEYS.filter((k) => !p.revealed[k]).length : 0,
      hasVoted: !!room.votes[p.id],
    })),
  };
}

function broadcastRoom(room) {
  for (const player of room.players) {
    if (player.socketId) io.to(player.socketId).emit("room_update", publicRoomState(room, player.id));
  }
}

function sendPrivateCards(room, player) {
  if (!player.socketId || !player.cards) return;
  io.to(player.socketId).emit("your_cards", {
    cards: player.cards,
    revealed: player.revealed,
    categoryLabels: Object.fromEntries(CATEGORY_KEYS.map((k) => [k, CARD_DATA.categories[k].label])),
    categoryOrder: CATEGORY_KEYS,
  });
}

function advanceTurn(room) {
  const currentId = room.order[room.turnIndex];
  const aliveIds = alivePlayers(room).map((p) => p.id);
  // Прибираємо з порядку ходів вибулих/вийшовших гравців
  room.order = room.order.filter((id) => aliveIds.includes(id));
  const len = room.order.length;
  if (len === 0) return;

  const pending = room.order.filter((id) => !getPlayer(room, id).revealedThisRound);
  if (pending.length === 0) {
    startVoting(room); // всі живі вже розкрили картку цього раунду
    return;
  }

  const curIdx = room.order.indexOf(currentId);
  if (curIdx >= 0 && !getPlayer(room, currentId).revealedThisRound) {
    room.turnIndex = curIdx; // хід все ще за тим самим гравцем
    return;
  }

  // Поточний гравець походив або пішов — шукаємо наступного, хто ще не ходив
  const start = curIdx >= 0 ? curIdx : room.turnIndex - 1;
  for (let i = 1; i <= len; i++) {
    const idx = (((start + i) % len) + len) % len;
    if (!getPlayer(room, room.order[idx]).revealedThisRound) {
      room.turnIndex = idx;
      return;
    }
  }
}

function startRound(room) {
  room.phase = "playing";
  room.votes = {};
  room.lastElimination = null;
  for (const p of alivePlayers(room)) {
    p.revealedThisRound = false;
  }
  room.order = alivePlayers(room).map((p) => p.id);
  room.turnIndex = 0;

  // Якщо гравець вже розкрив усі 7 карток, авто-пропускаємо
  while (
    room.order.length &&
    CATEGORY_KEYS.every((k) => getPlayer(room, room.order[room.turnIndex]).revealed[k])
  ) {
    getPlayer(room, room.order[room.turnIndex]).revealedThisRound = true;
    room.turnIndex = (room.turnIndex + 1) % room.order.length;
  }
}

function startVoting(room) {
  room.phase = "voting";
  room.votes = {};
  for (const p of room.players) p.votesReceived = 0;
}

function tallyVotesAndEliminate(room) {
  for (const p of room.players) p.votesReceived = 0;
  for (const targetId of Object.values(room.votes)) {
    const target = getPlayer(room, targetId);
    if (target) target.votesReceived++;
  }

  const alive = alivePlayers(room);
  const maxVotes = Math.max(0, ...alive.map((p) => p.votesReceived));
  const topCandidates = alive.filter((p) => p.votesReceived === maxVotes && maxVotes > 0);

  let eliminated = null;
  if (topCandidates.length === 1) {
    eliminated = topCandidates[0];
  } else if (topCandidates.length > 1) {
    // Нічия -> випадковий вибір серед тих, хто набрав найбільше голосів
    eliminated = pickRandom(topCandidates);
  }

  if (eliminated) {
    eliminated.alive = false;
    // Повністю розкрити всі його картки
    for (const key of CATEGORY_KEYS) {
      eliminated.revealed[key] = eliminated.cards[key];
    }
  }

  room.lastElimination = eliminated
    ? { id: eliminated.id, nickname: eliminated.nickname, votes: maxVotes, tie: topCandidates.length > 1 }
    : { id: null, nickname: null, votes: 0, tie: false };

  room.phase = "round_result";

  const remaining = alivePlayers(room).length;
  if (remaining <= room.settings.seats) {
    room.phase = "finished";
  }
}

// ------------------------------------------------------------
// Видалення гравця з кімнати (вихід або тривале відключення)
// ------------------------------------------------------------
const GRACE_MS = 120000; // скільки чекаємо на повернення гравця, що втратив зв'язок

function removePlayer(room, pid) {
  const idx = room.players.findIndex((p) => p.id === pid);
  if (idx === -1) return;

  const removed = room.players[idx];
  clearTimeout(removed.disconnectTimer);
  room.players.splice(idx, 1);
  delete room.votes[pid];

  if (room.players.length === 0) {
    delete rooms[room.code];
    return;
  }

  if (removed.isHost) {
    const next = room.players.find((p) => p.connected) || room.players[0];
    room.players.forEach((p) => (p.isHost = false));
    next.isHost = true;
    room.hostId = next.id;
  }

  if (room.phase === "playing") advanceTurn(room);
  if (room.phase === "voting") {
    const alive = alivePlayers(room);
    if (alive.length && alive.every((p) => room.votes[p.id])) tallyVotesAndEliminate(room);
  }
  if (room.phase === "round_result" || room.phase === "voting" || room.phase === "playing") {
    // Якщо через вихід гравців лишилось не більше за місця — завершуємо гру
    if (alivePlayers(room).length <= room.settings.seats && room.phase !== "lobby") {
      room.phase = "finished";
    }
  }
  broadcastRoom(room);
}

function cleanPid(pid) {
  if (typeof pid === "string" && /^[A-Za-z0-9_-]{8,64}$/.test(pid)) return pid;
  return "p_" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

// ------------------------------------------------------------
// Socket.io обробники подій
// ------------------------------------------------------------
io.on("connection", (socket) => {
  // socket.data.pid / socket.data.code — хто це і в якій кімнаті
  const getCtx = () => {
    const room = rooms[socket.data.code];
    const pid = socket.data.pid;
    if (!room || !pid || !getPlayer(room, pid)) return null;
    return { room, pid, player: getPlayer(room, pid) };
  };
  const isHost = (ctx) => ctx.room.hostId === ctx.pid;

  socket.on("create_room", ({ nickname, seats, revealTimeSec, pid } = {}, cb) => {
    try {
      const myPid = cleanPid(pid);
      const room = createRoom(myPid, socket.id, String(nickname || ""), { seats: parseInt(seats, 10), revealTimeSec });
      socket.data.pid = myPid;
      socket.data.code = room.code;
      socket.join(room.code);
      cb && cb({ ok: true, code: room.code });
      broadcastRoom(room);
    } catch (e) {
      cb && cb({ ok: false, error: "Не вдалося створити кімнату." });
    }
  });

  socket.on("join_room", ({ code, nickname, pid } = {}, cb) => {
    const room = rooms[String(code || "").trim().toUpperCase()];
    if (!room) return cb && cb({ ok: false, error: "Кімнату не знайдено. Перевірте код." });
    const myPid = cleanPid(pid);
    const nick = String(nickname || "").trim();

    // Гравець вже є в кімнаті з цим pid — це повернення, а не новий вхід
    const existing = getPlayer(room, myPid);
    if (existing) {
      clearTimeout(existing.disconnectTimer);
      existing.socketId = socket.id;
      existing.connected = true;
      socket.data.pid = myPid;
      socket.data.code = room.code;
      socket.join(room.code);
      cb && cb({ ok: true, code: room.code });
      broadcastRoom(room);
      sendPrivateCards(room, existing);
      return;
    }

    if (room.phase !== "lobby") return cb && cb({ ok: false, error: "Гра вже почалась, приєднатись не можна." });
    if (room.players.length >= 20) return cb && cb({ ok: false, error: "Кімната переповнена." });
    if (!nick) return cb && cb({ ok: false, error: "Введіть нікнейм." });
    if (room.players.some((p) => p.nickname.toLowerCase() === nick.toLowerCase())) {
      return cb && cb({ ok: false, error: "Гравець з таким ніком вже є в кімнаті." });
    }

    addPlayer(room, myPid, socket.id, nick);
    socket.data.pid = myPid;
    socket.data.code = room.code;
    socket.join(room.code);
    cb && cb({ ok: true, code: room.code });
    broadcastRoom(room);
  });

  // Автоматичне повернення в кімнату після втрати зв'язку / перезавантаження сторінки
  socket.on("rejoin", ({ code, pid } = {}, cb) => {
    const room = rooms[String(code || "").trim().toUpperCase()];
    const player = room && typeof pid === "string" ? getPlayer(room, pid) : null;
    if (!player) return cb && cb({ ok: false });

    clearTimeout(player.disconnectTimer);
    player.socketId = socket.id;
    player.connected = true;
    socket.data.pid = pid;
    socket.data.code = room.code;
    socket.join(room.code);
    cb && cb({ ok: true, code: room.code });
    broadcastRoom(room);
    sendPrivateCards(room, player);
  });

  socket.on("start_game", () => {
    const ctx = getCtx();
    if (!ctx || !isHost(ctx)) return;
    const { room } = ctx;
    if (room.phase !== "lobby") return;
    if (room.players.length < 2) {
      socket.emit("error_message", "Потрібно щонайменше 2 гравці, щоб почати гру.");
      return;
    }
    if (room.settings.seats >= room.players.length) {
      socket.emit(
        "error_message",
        `Кількість місць у бункері (${room.settings.seats}) має бути меншою за кількість гравців (${room.players.length}).`
      );
      return;
    }

    room.disaster = pickRandom(CARD_DATA.disasters);
    room.bunker = pickRandom(CARD_DATA.bunkers);
    room.round = 1;

    for (const p of room.players) {
      p.cards = dealCards();
      p.revealed = {};
      p.alive = true;
    }

    startRound(room);
    broadcastRoom(room);
    for (const p of room.players) sendPrivateCards(room, p);
  });

  socket.on("reveal_card", ({ category } = {}) => {
    const ctx = getCtx();
    if (!ctx) return;
    const { room, pid, player } = ctx;
    if (room.phase !== "playing" || !player.alive) return;
    if (room.order[room.turnIndex] !== pid) {
      socket.emit("error_message", "Зараз не ваша черга.");
      return;
    }
    if (!CATEGORY_KEYS.includes(category)) return;
    if (player.revealed[category]) {
      socket.emit("error_message", "Цю картку вже розкрито.");
      return;
    }

    player.revealed[category] = player.cards[category];
    player.revealedThisRound = true;

    advanceTurn(room);
    broadcastRoom(room);
    sendPrivateCards(room, player);
  });

  socket.on("start_voting", () => {
    const ctx = getCtx();
    if (!ctx || !isHost(ctx) || ctx.room.phase !== "playing") return;
    startVoting(ctx.room);
    broadcastRoom(ctx.room);
  });

  socket.on("cast_vote", ({ targetId } = {}) => {
    const ctx = getCtx();
    if (!ctx) return;
    const { room, pid, player } = ctx;
    if (room.phase !== "voting") return;
    const target = getPlayer(room, targetId);
    if (!player.alive || !target || !target.alive) return;
    if (targetId === pid) {
      socket.emit("error_message", "Не можна голосувати за самого себе.");
      return;
    }
    if (room.votes[pid]) return; // вже проголосував

    room.votes[pid] = targetId;

    const alive = alivePlayers(room);
    if (alive.every((p) => room.votes[p.id])) tallyVotesAndEliminate(room);
    broadcastRoom(room);
  });

  socket.on("host_finish_voting", () => {
    const ctx = getCtx();
    if (!ctx || !isHost(ctx) || ctx.room.phase !== "voting") return;
    tallyVotesAndEliminate(ctx.room);
    broadcastRoom(ctx.room);
  });

  socket.on("next_round", () => {
    const ctx = getCtx();
    if (!ctx || !isHost(ctx) || ctx.room.phase !== "round_result") return;
    ctx.room.round++;
    startRound(ctx.room);
    broadcastRoom(ctx.room);
  });

  socket.on("leave_room", () => {
    const ctx = getCtx();
    if (!ctx) return;
    socket.leave(ctx.room.code);
    socket.data.code = null;
    removePlayer(ctx.room, ctx.pid);
  });

  socket.on("disconnect", () => {
    const ctx = getCtx();
    if (!ctx) return;
    const { room, pid, player } = ctx;
    if (player.socketId !== socket.id) return; // старе з'єднання, гравець вже перепідключився

    player.connected = false;
    broadcastRoom(room);
    // Не видаляємо одразу: телефон міг просто заснути. Даємо час повернутись.
    clearTimeout(player.disconnectTimer);
    player.disconnectTimer = setTimeout(() => {
      const r = rooms[room.code];
      if (!r) return;
      const p = getPlayer(r, pid);
      if (p && !p.connected) removePlayer(r, pid);
    }, GRACE_MS);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Бункер запущено: http://localhost:${PORT}`);
});
