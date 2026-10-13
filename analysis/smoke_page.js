/* headless smoke test: run the spliced page scripts against a minimal DOM stub */
'use strict';
const fs=require('fs'), vm=require('vm');
const html=fs.readFileSync('/home/vagvaz/Projects/ai/agent_db_sim/analysis/agent_db_sim_analyzer.html','utf8');
const scripts=[...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map(m=>m[1]);

function el(id){
  const defaults={N:'8',tauThink:'2',tauLLM:'1.5',cLLM:'4',csLLM:'1',tauDB:'0.3',cDB:'8',csDB:'1',
    gridN:'2048',horizon:'12',sweepMax:'120',mcN:'20000',mcWarm:'0.1',mcSeed:'42',
    N_s:'8',cLLM_s:'4',cDB_s:'8'};
  return {
    id, value:defaults[id]||'8', textContent:'', innerHTML:'', disabled:false, style:{},
    classList:{ toggle(){}, add(){}, remove(){}, contains(){return false;} },
    addEventListener(){}, setAttribute(){},
  };
}
const ids={};
const ctx={
  console,
  document:{
    readyState:'complete',
    getElementById(id){ return ids[id]||(ids[id]=el(id)); },
    querySelectorAll(){ return []; },
    addEventListener(){},
  },
  Date, Math, JSON, Infinity, isFinite, parseInt, parseFloat,
};
ctx.window=ctx;
ctx.setTimeout=setTimeout; ctx.clearTimeout=clearTimeout;
vm.createContext(ctx);
vm.runInContext(scripts[0],ctx);   // core
vm.runInContext(scripts[1],ctx);   // app (init() runs computeAnalytic)

const p50a=ids['t_p50'].textContent, xa=ids['t_x'].textContent;
console.log('analytic tiles: p50=',p50a,' X=',xa,' rho=',ids['t_rho'].textContent);
console.log('banner:', ids['banner'].textContent);
if(!p50a||p50a==='–'||!isFinite(parseFloat(p50a))) { console.log('FAIL analytic tiles'); process.exit(1); }
if(ids['t_x'].textContent==='–'){ console.log('FAIL tiles'); process.exit(1); }

// MC path
vm.runInContext('setTab("M")',ctx);
const wait=setTimeout(()=>{},50);
setTimeout(()=>{
  const mcP50=ids['t_p50'].textContent, alt=ids['a_p50'].textContent;
  console.log('MC tiles: p50=',mcP50,' alt=',alt);
  console.log('table header brief:', ids['tblh'].innerHTML.slice(0,80).replace(/\s+/g,' '));
  const okMc= isFinite(parseFloat(mcP50)) && /analytic/.test(alt);
  console.log(okMc? 'SMOKE PASS':'SMOKE FAIL');
  process.exit(okMc?0:1);
}, 700);
