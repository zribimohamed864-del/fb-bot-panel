module.exports = function normCookies(input) {
  let list = input;
  if (typeof input === "string") {
    try { list = JSON.parse(input); } catch (e) { list = []; }
  }
  if (!Array.isArray(list)) return [];
  return list.map(x => ({
    key: x.key || x.name,
    value: x.value,
    domain: (x.domain || ".facebook.com").replace(/^\./, ""),
    path: x.path || "/",
    hostOnly: !!x.hostOnly,
    creation: x.creation || new Date().toISOString(),
    lastAccessed: new Date().toISOString()
  }));
};
