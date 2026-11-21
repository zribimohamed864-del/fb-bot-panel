const fs = require("fs");
const f = "node_modules/nextgen-fca/src/sendTypingIndicatorV2.js";
let s = fs.readFileSync(f, "utf8");
s = s.replace("=> mqttClient.publish", "=> ctx.mqttClient.publish");
if (!s.includes("api.__ctx")) s = s.replace("module.exports = function (defaultFuncs, api, ctx) {", "module.exports = function (defaultFuncs, api, ctx) {\n\tapi.__ctx = ctx;");
fs.writeFileSync(f, s);
console.log("patch ok");
