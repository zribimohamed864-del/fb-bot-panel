	const fs = require("fs"), path = require("path");
const login = require("nextgen-fca");
const DIR = path.join(__dirname, "accounts");
const DATA = path.join(__dirname, "data");
fs.mkdirSync(DATA, { recursive: true });
const ADMINS = fs.readFileSync(path.join(__dirname, "admin.txt"), "utf8").split(/[\s,]+/).filter(Boolean);
const OWNER = ADMINS[0];

const DBLACK_MSG = "👁️🪐 𝗗 𝗕𝗟𝗔𝗖𝗞 🪐👁️\n🧿✨ 𝕯 ⚡ 𝕭𝕷𝕬𝕮𝕶 ✨🧿";
const HELP = [
  "🪐━━━━━━━━━━━━━━━━━━━🪐",
  "   👁️ ⟦ 𝐂𝐎𝐌𝐌𝐀𝐍𝐃𝐒 • الأوامر ⟧ 👁️",
  "🪐━━━━━━━━━━━━━━━━━━━🪐",
  "🧿 ديبلاك + الوقت + الرسالة 🪐",
  "⏱️ مثال: ديبلاك 50ث مرحبا ⟵ كل 50 ثانية",
  "⏱️ مثال: ديبلاك 5د مرحبا ⟵ كل 5 دقائق",
  "🧿 ديبلاك ايقاف ⟵ إيقافها ⛔",
  "👁️ اسم + الاسم ⟵ قفل اسم الجروب 🔒",
  "👁️ فتح اسم ⟵ فك القفل 🔓",
  "🪐 كنية + الكنية ⟵ قفل كل الكنيات 🔒",
  "🪐 فتح كنية ⟵ فك قفل الكنيات 🔓",
  "🧹 مسح كنية ⟵ يمسح كنيات الجميع",
  "👑 ملك ⟵ رد على رسالة شخص ليصير أدمن في البوت",
  "🚪 سحب ملك ⟵ رد على رسالته لسحب صلاحياته",
  "📊 حالة ⟵ يعرض الحسابات الشغالة",
  "🗑️ مسح ⟵ رد على رسالة البوت لحذفها",
  "🪐━━━━━━━━━━━━━━━━━━━🪐",
].join("\n");

const sleep = ms => new Promise(r => setTimeout(r, ms));
const norm = s => (s || "").replace(/[\u200e\u200f\u202a-\u202e\u064B-\u0652\u0640]/g, "").replace(/[أإآ]/g, "ا").replace(/ى/g, "ي").replace(/ديبلاك/g, "دي بلاك").replace(/\s+/g, " ").trim();
const log = (...a) => console.log(new Date().toLocaleTimeString(), ...a);
process. on("uncaughtException", e => log("خطأ:", e.message));
process.on("unhandledRejection", e => log("خطأ:", e && e.message));

const MQ = {};
try {
  const mqtt = require(require.resolve("mqtt", { paths: [path.dirname(require.resolve("nextgen-fca"))] }));
  const orig = mqtt.connect;
  mqtt.connect = function (...a) {
    const c = orig.apply(this, a);
    try {
      const o = a.find(x => x && typeof x === "object") || {};
      const u = String((c.options && c.options.username) || o.username || "");
      const m = u.match(/"u":\s*"?(\d+)/);
      if (m) MQ[m[1]] = c; else MQ.last = c;
    } catch {}
    return c;
  };
} catch (e) { console.log("mqtt hook failed:", e.message); }

const accs = {};
const stFile = f => path.join(DATA, f + ".state.json");
const loadSt = f => { try { return JSON.parse(fs.readFileSync(stFile(f))); } catch { return { dblack: {}, name: {}, nick: {} }; } };
const save = acc => fs.writeFileSync(stFile(acc.file), JSON.stringify(acc.st));
const clear = (acc, k) => { clearInterval(acc.timers[k]); delete acc.timers[k]; };
const GRP = {};
const isGrp = t => (GRP[t] !== undefined ? GRP[t] : String(t).length >= 16);
let reqN = 0;
const typing = (acc, t, on) => {
  const api = acc.api;
  const c = MQ[String(api.getCurrentUserID())] || MQ.last;
  if (c && typeof c.publish === "function") {
    const inner = JSON.stringify({ thread_key: String(t), is_group_thread: +isGrp(t), is_typing: +on, attribution: 0 });
    const body = JSON.stringify({ app_id: 2220391788200892, payload: JSON.stringify({ label: 3, payload: inner, version: 5849951561777440 }), request_id: ++reqN, type: 4 });
    c.publish("/ls_req", body, { qos: 1, retain: false }, e => { if (e) log("typing err:", e.message || e); });
    return "mqtt";
  }
  if (typeof api.sendTypingIndicatorV2 === "function") { const r = api.sendTypingIndicatorV2(on, t, () => {}); if (r && r.catch) r.catch(e => log("typing err:", e.message || e)); return "v2"; }
  if (/is_typing|sendTyping\b/.test(String(api.sendTypingIndicator))) { api.sendTypingIndicator(on, t, () => {}); return "v2b"; }
  if (on) acc.endTyping = api.sendTypingIndicator(t, () => {}, isGrp(t));
  else if (typeof acc.endTyping === "function") acc.endTyping();
  return "classic";
};
const setNameRaw = (acc, t, name, cb) => {
  const c = (acc.api.__ctx && acc.api.__ctx.mqttClient) || MQ[String(acc.api.getCurrentUserID())] || MQ.last;
  if (c && typeof c.publish === "function" && c.connected !== false) {
    const task = { failure_count: null, label: "32", payload: JSON.stringify({ thread_key: String(t), thread_name: name, sync_group: 1 }), queue_name: String(t), task_id: Math.floor(Math.random() * 1000) + (++reqN) };
    const epoch = parseInt(String((BigInt(Date.now()) << 22n) + BigInt(Math.floor(Math.random() * 4194303))));
    const body = JSON.stringify({ app_id: "2220391788200892", payload: JSON.stringify({ epoch_id: epoch, tasks: [task], version_id: "7158486590867448" }), request_id: ++reqN, type: 3 });
    c.publish("/ls_req", body, { qos: 1, retain: false }, e => cb(e || null));
    return;
  }
  acc.api.setTitle(name, t, cb);
};
const NAMEWAIT = {};
const em = e => String((e && (e.message || e.error)) || (typeof e === "string" ? e : JSON.stringify(e))).slice(0, 120);
const nameMqtt = (acc, t, name, cb) => {
  const c = (acc.api.__ctx && acc.api.__ctx.mqttClient) || MQ[String(acc.api.getCurrentUserID())] || MQ.last;
  if (!c || typeof c.publish !== "function") return cb("لا يوجد اتصال MQTT");
  const task = { failure_count: null, label: "32", payload: JSON.stringify({ thread_key: String(t), thread_name: name, sync_group: 1 }), queue_name: String(t), task_id: Math.floor(Math.random() * 1000) + (++reqN) };
  const epoch = parseInt(String((BigInt(Date.now()) << 22n) + BigInt(Math.floor(Math.random() * 4194303))));
  const body = JSON.stringify({ app_id: "2220391788200892", payload: JSON.stringify({ epoch_id: epoch, tasks: [task], version_id: "7158486590867448" }), request_id: ++reqN, type: 3 });
  c.publish("/ls_req", body, { qos: 1, retain: false }, e => cb(e || null));
};
const setNameV = async (acc, t, name) => {
  const errs = [], key = acc.file + t;
  const waitEvt = ms => new Promise(res => {
    const w = { res, name };
    NAMEWAIT[key] = w;
    setTimeout(() => { if (NAMEWAIT[key] === w) { delete NAMEWAIT[key]; res(false); } }, ms);
  });
  const verify = async w => { if (await w) return true; const i = await getInfo(acc, t); return i ? i.threadName === name : null; };
  let w = waitEvt(9000);
  let err = await new Promise(r => { try { nameMqtt(acc, t, name, r); } catch (e) { r(e); } });
  if (!err) { const v = await verify(w); if (v !== false) return { ok: true }; }
  errs.push(err ? "mqtt: " + em(err) : "mqtt: لم يتغير");
  w = waitEvt(9000);
  err = await new Promise(r => {
    const to = setTimeout(() => r("انتهت المهلة"), 12000);
    try { acc.api.setTitle(name, t, e => { clearTimeout(to); r(e || null); }); } catch (e) { clearTimeout(to); r(e); }
  });
  if (!err) { const v = await verify(w); if (v !== false) return { ok: true }; }
  errs.push(err ? "api: " + em(err) : "api: لم يتغير");
  return { ok: false, err: errs.join(" | ") };
};
const NICKWAIT = {};
const nickRaw = (acc, t, uid, nick) => new Promise(async res => {
  acc.nw = acc.nw || {}; acc.nc = acc.nc || {};
  if (acc.blockUntil && Date.now() < acc.blockUntil) return res(false);
  const k = t + uid;
  acc.nc[k] = (acc.nc[k] || []).filter(x => Date.now() - x < 600000);
  if (acc.nc[k].length >= 3) return res(false);
  acc.nc[k].push(Date.now());
  const errs = [];
  const fail = () => {
    const m = errs.join(" | ");
    log("nick err:", t, uid, m);
    if (/1390008|too fast|blocked/i.test(m)) acc.blockUntil = Date.now() + 3 * 3600 * 1000;
    if (!acc.nw[t]) { acc.nw[t] = 1; say(acc, t, "ما قدرت أغير الكنية ⚠️\n" + m); }
    res(false);
  };
  const c = (acc.api.__ctx && acc.api.__ctx.mqttClient) || MQ[String(acc.api.getCurrentUserID())] || MQ.last;
  if (c && typeof c.publish === "function") {
    const key = acc.file + k;
    const waiter = new Promise(r2 => {
      NICKWAIT[key] = { r: r2 };
      setTimeout(() => { if (NICKWAIT[key] && NICKWAIT[key].r === r2) delete NICKWAIT[key]; r2(false); }, 9000);
    });
    const task = { failure_count: null, label: "44", payload: JSON.stringify({ thread_key: String(t), contact_id: String(uid), nickname: nick, sync_group: 1 }), queue_name: "thread_participant_nickname", task_id: Math.floor(Math.random() * 1000) + (++reqN) };
    const epoch = parseInt(String((BigInt(Date.now()) << 22n) + BigInt(Math.floor(Math.random() * 4194303))));
    const body = JSON.stringify({ app_id: "2220391788200892", payload: JSON.stringify({ epoch_id: epoch, tasks: [task], version_id: "7158486590867448" }), request_id: ++reqN, type: 3 });
    const perr = await new Promise(r3 => c.publish("/ls_req", body, { qos: 1, retain: false }, e => r3(e || null)));
    if (!perr && (await waiter)) return res(true);
    errs.push(perr ? "mqtt: " + em(perr) : "mqtt: لم يصل تأكيد");
  } else errs.push("mqtt: لا يوجد اتصال");
  const e2 = await new Promise(r4 => {
    const to = setTimeout(() => r4("انتهت المهلة"), 12000);
    try { acc.api.changeNickname(nick, t, uid, e => { clearTimeout(to); r4(e || null); }); } catch (e) { clearTimeout(to); r4(e); }
  });
  if (!e2) return res(true);
  errs.push("api: " + em(e2));
  fail();
});
const applyNicks = async (acc, t) => {
  for (const [uid, nick] of Object.entries(acc.st.nick[t] || {})) {
    if (!acc.st.nick[t]) break;
    await nickRaw(acc, t, uid, nick);
    await sleep(10000 + Math.random() * 3000);
  }
};
const markSeen = (acc, t) => {
  const api = acc.api, cb = e => { if (e) log("seen err:", e.message || e); };
  try { if (typeof api.markAsRead === "function") { const r = api.markAsRead(t, true, cb); if (r && r.catch) r.catch(cb); } } catch (e) { cb(e); }
  try { if (typeof api.markAsSeen === "function") { const r = api.markAsSeen(Date.now(), cb); if (r && r.catch) r.catch(cb); } } catch (e) { cb(e); }
};
const say = (acc, t, text, ms = 3000) => new Promise(res => {
  if (!acc.api) return res();
  try { const m = typing(acc, t, true); if (!acc.tm) { acc.tm = 1; log("typing mode:", m); } } catch (e) { log("typing err:", e.message); }
  setTimeout(() => {
    try { typing(acc, t, false); } catch {}
    try { acc.api.sendMessage(text, t, () => res()); } catch { res(); }
  }, ms);
});
const getInfo = (acc, t) => new Promise(r => {
  acc.lastIE = acc.lastIE || {}; acc.ie = acc.ie || {};
  const errTxt = e => String((e && (e.message || e.error)) || (typeof e === "string" ? e : JSON.stringify(e))).slice(0, 160);
  let done = false, to = null;
  const fin = d => { if (!done) { done = true; clearTimeout(to); r(d || null); } };
  to = setTimeout(() => { acc.lastIE[t] = "انتهت المهلة"; fin(null); }, 15000);
  const norm2 = d => { if (d && d.threadName === undefined && d.name !== undefined) d.threadName = d.name; return d; };
  const tryDep = prev => {
    if (!acc.api || typeof acc.api.getThreadInfoDeprecated !== "function") { acc.lastIE[t] = prev; return fin(null); }
    try {
      acc.api.getThreadInfoDeprecated(t, (e2, d2) => {
        if (e2 || !d2) {
          acc.lastIE[t] = prev + " | " + errTxt(e2);
          if (!acc.ie[t]) { acc.ie[t] = 1; log("info err:", t, acc.lastIE[t]); }
          return fin(null);
        }
        fin(norm2(d2));
      });
    } catch (e) { acc.lastIE[t] = prev + " | " + errTxt(e); fin(null); }
  };
  try { acc.api.getThreadInfo(t, (e, d) => { if (e || !d) return tryDep(errTxt(e) || "لا بيانات"); fin(norm2(d)); }); }
  catch (e) { tryDep(errTxt(e)); }
});

function startDblack(acc, t, now) {
  clear(acc, "d" + t);
  const d = acc.st.dblack[t];
  if (!d || typeof d !== "object") return;
  if (now) say(acc, t, d.msg);
  acc.timers["d" + t] = setInterval(() => say(acc, t, d.msg, 1500), d.sec * 1000);
}
async function fixName(acc, t) {
  acc.fx = acc.fx || {};
  if (acc.fx[t]) { acc.fx[t] = 2; return; }
  try {
    do {
      acc.fx[t] = 1;
      const want = acc.st.name[t];
      if (!want || !acc.api) break;
      const now = Date.now();
      acc.hist = (acc.hist || []).filter(x => now - x < 60000);
      acc.h2 = acc.h2 || {}; acc.h2[t] = (acc.h2[t] || []).filter(x => now - x < 60000);
      if (acc.h2[t].length >= 12) { log("حماية: توقف دقيقتين", t); await sleep(120000); acc.h2[t] = []; }
      acc.h2[t].push(Date.now());
      if (acc.hist.length >= 40) { log("حماية الحساب", t); await sleep(60000); }
      acc.hist.push(Date.now());
      const r = await setNameV(acc, t, want);
      if (!r.ok) { log("title err:", t, r.err); await sleep(5000); }
      await sleep(1500 + Math.random() * 1000);
    } while (acc.fx[t] === 2);
  } finally { acc.fx[t] = 0; }
}
function startName(acc, t) {
  clear(acc, "n" + t);
  acc.timers["n" + t] = setInterval(async () => {
    if (!acc.api || !acc.st.name[t] || (acc.fx && acc.fx[t])) return;
    const i = await getInfo(acc, t);
    if (i && i.threadName !== acc.st.name[t]) fixName(acc, t);
  }, 2000);
}
function startNick(acc, t) {
  clear(acc, "k" + t);
  let busy = false;
  acc.timers["k" + t] = setInterval(async () => {
    if (busy || !acc.api) return;
    busy = true;
    try {
      const i = await getInfo(acc, t);
      if (i) for (const [uid, nick] of Object.entries(acc.st.nick[t] || {})) {
        if (!acc.st.nick[t]) break;
        if (((i.nicknames || {})[uid] || "") !== nick) {
          await nickRaw(acc, t, uid, nick);
          await sleep(10000);
        }
      }
    } catch {}
    busy = false;
  }, 5000);
}
function restore(acc) {
  for (const t in acc.st.dblack) startDblack(acc, t, false);
  for (const t in acc.st.name) startName(acc, t);
  for (const t in acc.st.nick) startNick(acc, t);
}

async function handle(acc, ev) {
  if (ev.type === "event" && ev.logMessageType === "log:thread-name") {
    const t0 = ev.threadID, want = acc.st.name[t0];
    const cur = ev.logMessageData && ev.logMessageData.name;
    { const k = acc.file + t0, w = NAMEWAIT[k]; if (w && (cur === undefined || cur === w.name)) { delete NAMEWAIT[k]; w.res(true); } }
    if (want && cur !== want && String(ev.author) !== String(acc.api.getCurrentUserID())) {
      acc.evt = acc.evt || {};
      if (!acc.evt[t0]) acc.evt[t0] = setTimeout(() => { acc.evt[t0] = null; fixName(acc, t0); }, 3000 + Math.random() * 2000);
    }
    return;
  }
  if (ev.type === "event" && ev.logMessageType === "log:user-nickname") {
    const t0 = ev.threadID, d = ev.logMessageData || {};
    const uid = String(d.participant_id), cur = d.nickname || "";
    const mine = String(ev.author) === String(acc.api.getCurrentUserID());
    const w = NICKWAIT[acc.file + t0 + uid];
    if (w && mine) { delete NICKWAIT[acc.file + t0 + uid]; w.r(true); }
    const m = acc.st.nick[t0];
    if (m && !mine && m[uid] !== undefined && m[uid] !== cur) {
      acc.nq = (acc.nq || Promise.resolve()).then(async () => {
        await sleep(8000 + Math.random() * 4000);
        if (acc.st.nick[t0]) await nickRaw(acc, t0, uid, m[uid]);
      }).catch(() => {});
    }
    return;
  }
  if (ev.type !== "message" && ev.type !== "message_reply") return;
  const raw = (ev.body || "").trim();
  if (!raw || !ADMINS.includes(String(ev.senderID))) return;
  const t = ev.threadID, full = norm(raw);
  if (typeof ev.isGroup === "boolean") GRP[t] = ev.isGroup;
  markSeen(acc, t);
  const [cmd, ...rest] = raw.split(/\s+/);
  const c = norm(cmd), arg = rest.join(" ");

  if (full === "مسح") {
    const r = ev.messageReply;
    if (r && String(r.senderID) === String(acc.api.getCurrentUserID())) acc.api.unsendMessage(r.messageID, () => {});
    return;
  }
  if (full === "ملك" || full === "سحب ملك") {
    if (String(ev.senderID) !== OWNER) return;
    const r = ev.messageReply;
    if (!r) return say(acc, t, "رد على رسالة الشخص أولاً ↩️");
    const target = String(r.senderID), on = full === "ملك";
    if (target === OWNER || target === String(acc.api.getCurrentUserID())) return;
    const has = ADMINS.includes(target);
    if (on && has) return say(acc, t, "هو ملك من قبل 👑");
    if (!on && !has) return say(acc, t, "ما هو ملك أصلا ⚠️");
    if (on) ADMINS.push(target); else ADMINS.splice(ADMINS.indexOf(target), 1);
    fs.writeFileSync(path.join(__dirname, "admin.txt"), ADMINS.join("\n") + "\n");
    return say(acc, t, on ? "مبروك اصبحت ملكا 👑" : "برا يا طيز 🚪");
  }
  if (full === "حالة") return say(acc, t, "🪐 حالة الحسابات:\n" + Object.values(accs).map(a => (a.api ? "✅ " : "❌ ") + a.file + (a.api ? "" : " — " + (a.why || "يتصل..."))).join("\n"));
  if (full === "مسح كنية") {
    clear(acc, "k" + t); delete acc.st.nick[t]; save(acc);
    const i = await getInfo(acc, t);
    let ids = (i && i.participantIDs) || (ev.participantIDs || []).map(String);
    const cur = (i && i.nicknames) || {};
    if (i) ids = ids.filter(u => cur[u]);
    if (!ids.length) return say(acc, t, i ? "ما فيه كنيات لمسحها ✅" : "ما قدرت أعرف أعضاء الجروب ⚠️\n" + ((acc.lastIE && acc.lastIE[t]) || ""));
    acc.nw = acc.nw || {}; acc.nw[t] = 0; acc.nc = {};
    say(acc, t, "جاري مسح الكنيات 🧹 (" + ids.length + " عضو)، ياخذ وقت لحماية الحساب");
    (async () => {
      let ok = 0;
      for (const u of ids) { if (await nickRaw(acc, t, String(u), "")) ok++; await sleep(10000 + Math.random() * 3000); }
      say(acc, t, "تم مسح الكنيات ✅ (" + ok + "/" + ids.length + ")");
    })();
    return;
  }
  if (full === "اوامر") return say(acc, t, HELP);
  const mm = raw.replace(/[\u200e\u200f\u202a-\u202e]/g, "").match(/^د[يى]\s*بلاك(?:\s+([\s\S]*))?$/);
  if (mm) {
    const rest = (mm[1] || "").trim();
    const running = acc.st.dblack[t] && typeof acc.st.dblack[t] === "object";
    if (norm(rest) === "ايقاف") {
      if (!running) return say(acc, t, "ديبلاك مش شغال أصلا ⚠️");
      clear(acc, "d" + t); delete acc.st.dblack[t]; save(acc);
      return say(acc, t, "تم إيقاف ديبلاك ⛔");
    }
    if (running) return say(acc, t, "ديبلاك راه يشتغل ✅");
    const tm = rest.match(/(?:^|\s)(\d+)\s*(ثانية|ثواني|ثا|ث|دقيقة|دقائق|دقيقه|دق|د|s|sec|m|min)(?=\s|$)/i);
    let sec = 120, msg = rest;
    if (tm) {
      const n = parseInt(tm[1], 10);
      sec = /^(د|دق|دقيقة|دقائق|دقيقه|m|min)$/i.test(tm[2]) ? n * 60 : n;
      msg = rest.replace(tm[0], " ").trim();
    }
    if (sec < 5) sec = 5;
    if (!msg) return say(acc, t, "اكتب الرسالة بعد الأمر، مثال: ديبلاك 50ث رسالتي");
    acc.st.dblack[t] = { msg, sec }; save(acc);
    await say(acc, t, "تم تشغيل ديبلاك 🪐");
    return startDblack(acc, t, true);
  }
  if (full === "فتح اسم") {
    clear(acc, "n" + t); delete acc.st.name[t]; save(acc);
    return say(acc, t, "تم فتح الاسم 🔓");
  }
  if (full === "فتح كنية") {
    clear(acc, "k" + t); delete acc.st.nick[t]; save(acc);
    return say(acc, t, "تم فتح الكنيات 🔓");
  }
  if (c === "اسم" && arg) {
    const r = await setNameV(acc, t, arg);
    if (!r.ok) return say(acc, t, "ما قدرت أغير الاسم ⚠️\n" + r.err);
    acc.st.name[t] = arg; save(acc); startName(acc, t);
    return say(acc, t, "تم قفل الاسم 🔒");
  }
  if (c === "كنية") {
    const i = await getInfo(acc, t);
    let ids = i && i.participantIDs, cur = (i && i.nicknames) || {};
    if (!ids || !ids.length) ids = (ev.participantIDs || []).map(String);
    if (!ids.length) return say(acc, t, "ما قدرت أعرف أعضاء الجروب ⚠️\n" + ((acc.lastIE && acc.lastIE[t]) || ""));
    if (!i && !arg) return say(acc, t, "اكتب الكنية بعد الأمر، مثال: كنية سلام");
    const map = {};
    for (const u of ids) map[String(u)] = arg || cur[u] || "";
    acc.st.nick[t] = map; save(acc); startNick(acc, t);
    acc.nw = acc.nw || {}; acc.nw[t] = 0; acc.nc = {};
    if (!i) applyNicks(acc, t);
    return say(acc, t, "تم قفل الكنيات 🔒 (" + ids.length + " عضو)");
  }
}

const normCookies = require("./cookies.js");
const WAITS = [60, 300, 900, 3600, 10800];
async function run(file) {
  const acc = accs[file] = accs[file] || { file, api: null, st: loadSt(file), timers: {}, fails: 0 };
  const p = path.join(DIR, file);
  while (true) {
    if (!fs.existsSync(p)) { delete accs[file]; delete started[file]; return; }
    let mt = 0;
    try {
      mt = fs.statSync(p).mtimeMs;
      const appState = normCookies(fs.readFileSync(p, "utf8"));
      const names = appState.map(c => c.key);
      if (!names.includes("c_user") || !names.includes("xs")) {
        acc.why = "الكوكيز ناقصة (لازم c_user و xs)";
        log(file, "❌", acc.why);
        while (fs.existsSync(p) && fs.statSync(p).mtimeMs === mt) await sleep(30000);
        continue;
      }
      acc.api = await new Promise((res, rej) => login({ appState }, (e, a) => e ? rej(e) : res(a)));
      acc.api.setOptions({ listenEvents: true, selfListen: false, logLevel: "silent", AutoRefreshFbDtsg: false });
      try { fs.writeFileSync(p, JSON.stringify(acc.api.getAppState())); } catch {}
      acc.fails = 0; acc.why = "";
      log(file, "متصل ✅");
      restore(acc);
      await new Promise(resolve => {
        acc.stop = acc.api.listenMqtt((err, ev) => err ? resolve() : handle(acc, ev));
      });
      log(file, "انقطع الاتصال، إعادة المحاولة...");
      acc.api = null;
      try { acc.stop && acc.stop(); } catch {}
      await sleep(120000);
      continue;
    } catch (e) {
      acc.api = null; acc.fails = (acc.fails || 0) + 1;
      const m = String((e && (e.error || e.message)) || e).slice(0, 160);
      acc.why = /userID|blocked|unknown location|checkpoint/i.test(m) ? "فيسبوك رفض الجلسة (كوكيز منتهية أو تأكيد مطلوب)" : m;
      log(file, "فشل:", acc.why, "(محاولة " + acc.fails + ")");
      if (acc.fails >= 3 && /الجلسة/.test(acc.why)) {
        log(file, "⏸ متوقف: ضع كوكيز جديدة في الملف وسيعيد المحاولة وحده");
        const t0 = Date.now();
        while (fs.existsSync(p) && fs.statSync(p).mtimeMs === mt && Date.now() - t0 < 6 * 3600 * 1000) await sleep(30000);
        acc.fails = 0;
        continue;
      }
    }
    await sleep(WAITS[Math.min(acc.fails, WAITS.length) - 1] * 1000);
  }
}

const started = {};
const startAll = () => {
  try { fs.readdirSync(DIR).filter(f => f.endsWith(".json")).forEach((f, i) => { if (!started[f]) { started[f] = 1; setTimeout(() => run(f), i * 20000); } }); } catch {}
};
startAll();
setInterval(startAll, 60000);
