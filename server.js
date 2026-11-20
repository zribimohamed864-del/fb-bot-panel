const http=require("http"),fs=require("fs"),path=require("path"),{spawn}=require("child_process");
const HOME=process.env.HOME,PASS=process.env.PANEL_PASS||"1234";
const BOTS_DIR=process.env.BOTS_DIR||path.join(__dirname,"bots");
const bots={},events=[];
const ev=m=>{events.unshift(new Date().toLocaleString()+" — "+m);if(events.length>200)events.pop()};
fs.readdirSync(BOTS_DIR).filter(d=>/^acc\d+$/.test(d)&&fs.existsSync(path.join(BOTS_DIR,d,"index.js"))).sort().forEach(d=>{
 bots[d]={id:d,n:+d.slice(3),dir:path.join(BOTS_DIR,d),p:null,since:null,stopped:null,logs:[],timer:null,until:null}});
function start(b,why){
 if(b.p)return;
 const p=spawn("node",["index.js"],{cwd:b.dir});b.p=p;b.since=Date.now();
 const add=d=>{b.logs.push(...String(d).split("\n").filter(Boolean));if(b.logs.length>200)b.logs.splice(0,b.logs.length-200)};
 p.stdout.on("data",add);p.stderr.on("data",add);
 p.on("close",()=>{if(b.p===p){b.p=null;b.since=null}ev("البوت "+b.n+" توقف")});
 ev("البوت "+b.n+" "+why)}
function stop(b,panel){clearTimeout(b.timer);b.until=null;if(panel)b.stopped=Date.now();if(b.p)b.p.kill()}
function restart(b){const p=b.p;if(!p)return start(b,"شُغّل");clearTimeout(b.timer);b.until=null;p.once("close",()=>start(b,"أُعيد تشغيله"));p.kill()}
http.createServer((q,r)=>{
 if(q.url==="/"){r.setHeader("Content-Type","text/html; charset=utf-8");return r.end(fs.readFileSync(path.join(__dirname,"index.html")))}
 if(q.headers.authorization!==PASS){r.statusCode=401;return r.end()}
 const u=q.url.split("/");let body="";q.on("data",c=>body+=c);
 q.on("end",()=>{
  const j=o=>{r.setHeader("Content-Type","application/json");r.end(JSON.stringify(o))};
  if(u[2]==="bots"&&!u[3])return j(Object.values(bots).map(b=>({id:b.id,n:b.n,dir:b.dir,on:!!b.p,since:b.since,stopped:b.stopped,until:b.until,logs:b.logs.slice(-40)})));
  if(u[2]==="events")return j(events);
  const b=bots[u[3]];if(!b){r.statusCode=404;return r.end()}
  if(u[4]==="start"){start(b,"شُغّل من اللوحة");return j({ok:1})}
  if(u[4]==="stop"){stop(b,true);ev("البوت "+b.n+" أُوقف من اللوحة");return j({ok:1})}
  if(u[4]==="restart"){restart(b);return j({ok:1})}
  if(u[4]==="timed"){const m=+body;if(!(m>0))return j({ok:0});start(b,"شُغّل لمدة "+m+" د");clearTimeout(b.timer);b.until=Date.now()+m*6e4;b.timer=setTimeout(()=>{stop(b,true);ev("انتهت مدة البوت "+b.n)},m*6e4);return j({ok:1})}
  r.statusCode=404;r.end()});
}).listen(process.env.PORT||3000,"0.0.0.0",()=>console.log("Panel: http://localhost:3000"));
process.on("SIGINT",()=>process.exit());
process.on("exit",()=>Object.values(bots).forEach(b=>b.p&&b.p.kill()));
const AUTO_H=+process.env.AUTO_H||3;
setInterval(()=>{Object.values(bots).forEach(b=>{if(b.p&&b.since&&!b.until&&Date.now()-b.since>AUTO_H*36e5){ev("إعادة تشغيل تلقائية للبوت "+b.n);restart(b)}})},6e4);
let netUp=true;
function checkNet(){
 require("https").get("https://www.facebook.com",{timeout:8000},r=>{r.resume();netBack()}).on("error",()=>{netUp=false}).on("timeout",function(){this.destroy()});
}
function netBack(){
 if(!netUp){netUp=true;ev("رجع النت، إعادة تشغيل البوتات");Object.values(bots).forEach(b=>{if(b.p&&!b.until)restart(b)})}
}
setInterval(checkNet,2e4);
