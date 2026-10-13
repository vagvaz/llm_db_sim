/* verification suite for the analyzer math core (node analysis/verify_core.js) */
'use strict';
const C=require('./core.js');
let pass=0, fail=0;
function ok(name, cond, detail){
  if(cond){ pass++; console.log('  ok  '+name+(detail?'  ('+detail+')':'')); }
  else{ fail++; console.log('  FAIL '+name+(detail?'  ('+detail+')':'')); }
}

/* 1. Erlang C ground truths (Kleinrock Vol I, ch.3) */
ok('ErlangC(1,0.5)=0.5', Math.abs(C.erlangC(1,0.5)-0.5)<1e-12, C.erlangC(1,0.5)+'');
ok('ErlangC(2,1)=1/3', Math.abs(C.erlangC(2,1)-1/3)<1e-12, C.erlangC(2,1)+'');
ok('ErlangC(5,3)≈0.23625', Math.abs(C.erlangC(5,3)-0.2362)<2e-4, C.erlangC(5,3).toFixed(6));

/* 2. textbook closed M/M/1//2 chain: 2 agents, no think, two M/M/1 stations τ=1 */
{
  const s=C.solveClosedChain({N:2,tauThink:0,tauLLM:1,cLLM:1,csLLM:1,tauDB:1,cDB:1,csDB:1});
  ok('closed M/M/1//2: X=2/3', Math.abs(s.X-2/3)<1e-9, s.X.toFixed(6));
  ok('closed M/M/1//2: L=1 each', Math.abs(s.LL-1)<1e-9 && Math.abs(s.LD-1)<1e-9,
     'LL='+s.LL.toFixed(4)+' LD='+s.LD.toFixed(4));
  ok('closed M/M/1//2: R=1.5', Math.abs(s.LL/s.X-1.5)<1e-9, (s.LL/s.X).toFixed(6));
}

/* 3. N=1 chain: no queueing possible */
{
  const P={N:1,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P);
  ok('N=1 exact throughput', Math.abs(s.X-1/(2+1.5+0.3))<1e-9, s.X.toFixed(6));
  ok('N=1 zero wait', s.WLq<1e-9 && s.WDq<1e-9, 'WL='+s.WLq.toExponential(2));
}

/* 4. defaults consistency */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P);
  const d=C.latencyDistribution(P,s,2048,12);
  ok('defaults: rho<0.9995', !s.saturated, 'rhoL='+s.rhoL.toFixed(3)+' rhoD='+s.rhoD.toFixed(3));
  ok('defaults: grid mean ≈ chain mean (1%)', Math.abs(d.mean-s.T)/s.T<0.01,
     'grid='+d.mean.toFixed(4)+' chain='+s.T.toFixed(4));
  ok('defaults: mass capture ≥0.995', d.M>0.995, d.M.toFixed(5));
  ok('defaults: percentile ordering', d.p50<d.p90 && d.p90<d.p95 && d.p95<d.p99 && d.p90<Infinity,
     [d.p50,d.p90,d.p95,d.p99].map(v=>v.toFixed(2)).join('/'));
  ok('defaults: X<capacity', s.X<Math.min(4/1.5,8/0.3), 'X='+s.X.toFixed(4));
}

/* 5. SCV behavior */
{
  const P1={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const P0=Object.assign({},P1,{csLLM:0,csDB:0});
  const s1=C.solve(P1), s0=C.solve(P0);
  ok('SCV=0 → throughput not lower', s0.X>=s1.X-1e-9,
     'X: '+s0.X.toFixed(3)+' vs '+s1.X.toFixed(3));
  const d1=C.latencyDistribution(P1,s1,2048,12);
  const d0=C.latencyDistribution(P0,s0,2048,12);
  ok('SCV=0 p99 < SCV=1 p99', d0.p99<d1.p99, d0.p99.toFixed(2)+' vs '+d1.p99.toFixed(2));
}

/* 6. sampler moments (Gamma, Marsaglia–Tsang) */
for(const scv of [0.5,1,2.5]){
  const n=50000, rng=C.mulberry32(7);
  const g=C.makeSampler(1.5,scv,rng);
  let m=0,m2=0;
  for(let i=0;i<n;i++){ const v=g(); m+=v; m2+=v*v; }
  m/=n; m2/=n;
  ok('sampler SCV='+scv+' mean', Math.abs(m-1.5)/1.5<0.02, 'err='+(Math.abs(m-1.5)/1.5*100).toFixed(2)+'%');
  ok('sampler SCV='+scv+' var', Math.abs((m2-m*m)/(1.5*1.5*scv)-1)<0.10,
     'ratio='+((m2-m*m)/(1.5*1.5*scv)).toFixed(3));
}

/* 7. MC vs textbook closed M/M/1//2 */
{
  const P={N:2,tauThink:0,tauLLM:1,cLLM:1,csLLM:1,tauDB:1,cDB:1,csDB:1};
  const m=C.mcSim(P,20000,0.1,42);
  ok('MC M/M/1//2 X≈2/3', Math.abs(m.X-2/3)<0.05, 'X='+m.X.toFixed(4));
  const st=C.mcStats(m.lat);
  ok('MC M/M/1//2 mean lat≈3.0', Math.abs(st.mean-3.0)<0.12, st.mean.toFixed(4));
}

/* 8. MC vs analytic (same stochastic model) */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P);
  const m=C.mcSim(P,20000,0.1,42);
  const st=C.mcStats(m.lat);
  const dX=Math.abs(m.X-s.X)/s.X, dM=Math.abs(st.mean-s.T)/s.T;
  ok('MC vs analytic X (3%)', dX<0.03, 'dX='+(dX*100).toFixed(2)+'%');
  ok('MC vs analytic mean lat (3%)', dM<0.03, 'dM='+(dM*100).toFixed(2)+'%');
  ok('MC p50 finite positive', isFinite(st.p50)&&st.p50>0, 'p50mc='+st.p50.toFixed(3));
}

/* 9. P(wait>0) — approximate calibration check (open issue #1 in README).
   The analytic π̂ is a Poisson-arrival imputation; in closed chains the empirical
   value can be substantially higher. Bounded loose, not exact. */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P);
  const m=C.mcSim(P,20000,0.1,42);
  let fL=0,fD=0; for(const w of m.wl) if(w>0) fL++; for(const w of m.wd) if(w>0) fD++;
  const empL=fL/m.kept, empD=fD/m.kept;
  ok('P(wait) same order (loose, documented approximation)',
     Math.abs(empL-s.wL.pi)<0.25 && Math.abs(empD-s.wD.pi)<0.25,
     'π̂L='+s.wL.pi.toFixed(3)+' emp='+empL.toFixed(3)+' π̂D='+s.wD.pi.toFixed(3)+' emp='+empD.toFixed(3));
}

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
