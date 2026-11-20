const { execSync } = require("child_process");
const fs = require("fs");
try { if (!fs.existsSync("node_modules/nextgen-fca")) execSync("npm install", { stdio: "inherit" }); } catch (e) { console.log("npm install failed:", e.message); }
try { require("./patch.js"); } catch (e) { console.log("patch failed:", e.message); }
require("./bot.js");
