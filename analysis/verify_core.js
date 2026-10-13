/* verification suite for the analyzer math core (node analysis/verify_core.js)
   Ground truth anchors: brute-force CTMC stationary solves (independent method),
   textbook closed-chain values, and statistical properties of the samplers. */
'use strict';
const C=require('./core.js');
let pass=0, fail=0;
function ok(name, cond, detail){
  if(cond){ pass++; console.log('  ok  '+name+(detail?'  ('+detail+')':'')); }
  else{ fail++; console.log('  FAIL '+name+(detail?'  ('+detail+')':'')); }
}

/* ---------- independent exact CTMC (for small closed chains) ----------
   compositions of N over the station dims; column-source generator Q[r,i]=rate(i->r);
   stationary via replacing one row with normalization + full Gaussian solve. */
function ctmcExact(N, tauThink, stations){
  const off = tauThink>0 ? 1 : 0;
  const dims = off + stations.length;
  const states=[];
  (function rec(k, left, acc){
    if(k===dims-1){ states.push(acc.concat([left])); return; }
    for(let x=0;x<=left;x++) rec(k+1, left-x, acc.concat([x]));
  })(0, N, []);
  const idx={}; states.forEach((s,i)=>idx[s.join(',')]=i);
  const nS=states.length;
  // build A = Q with last row replaced by ones; b = e_last, then pi = solve
  const A=Array.from({length:nS},()=>new Float64Array(nS));
  const b=new Float64Array(nS); b[nS-1]=1;
  states.forEach((s,i)=>{
    let rate=0;
    if(off && s[0]>0){ // think -> station0
      const t=s.slice(); t[0]--; t[1]+=1;
      const r=s[0]/tauThink; A[idx[t.join(',')]][i]+=r; rate+=r;
    }
    for(let k=0;k<stations.length;k++){
      const bk=s[off+k];
      if(bk>0){
        const t = s.slice();
        t[off+k]--;
        if(k<stations.length-1) t[off+k+1]++;
        else t[0]++;   /* last station → think dim if present, else station 0 */
        const r=Math.min(bk,stations[k][1])/stations[k][0];
        A[idx[t.join(',')]][i]+=r; rate+=r;
      }
    }
    A[i][i]-=rate;
  });
  A[nS-1].fill(1);
  // Gaussian elimination with partial pivoting
  const M=A.map(r=>Array.from(r));
  for(let col=0; col<nS; col++){
    let piv=col;
    for(let r=col+1;r<nS;r++) if(Math.abs(M[r][col])>Math.abs(M[piv][col])) piv=r;
    if(Math.abs(M[piv][col])<1e-14) return null;
    [M[col],M[piv]]=[M[piv],M[col]]; const tb=b[col]; b[col]=b[piv]; b[piv]=tb;
    for(let r=col+1;r<nS;r++){
      const f=M[r][col]/M[col][col];
      if(f===0) continue;
      for(let cc=col;cc<nS;cc++) M[r][cc]-=f*M[col][cc];
      b[r]-=f*b[col];
    }
  }
  const pi=new Float64Array(nS);
  for(let r=nS-1;r>=0;r--){
    let s=b[r];
    for(let cc=r+1;cc<nS;cc++) s-=M[r][cc]*pi[cc];
    pi[r]=s/M[r][r];
  }
  // residual check on the pure generator
  let resid=0;
  {
    const Q=Array.from({length:nS},()=>new Float64Array(nS));
    states.forEach((s,i)=>{
      let rate=0;
      if(off && s[0]>0){ const t=s.slice(); t[0]--; t[1]+=1; const r=s[0]/tauThink; Q[idx[t.join(',')]][i]+=r; rate+=r; }
      for(let k=0;k<stations.length;k++){
        const bk=s[off+k];
        if(bk>0){ const t=s.slice(); t[off+k]--;
          if(k<stations.length-1) t[off+k+1]++;
          else if(off) t[0]++; else t[1]++;
          const r=Math.min(bk,stations[k][1])/stations[k][0];
          Q[idx[t.join(',')]][i]+=r; rate+=r; }
      }
      Q[i][i]-=rate;
    });
    for(let i=0;i<nS;i++){
      let acc=0;
      for(let j=0;j<nS;j++) acc+=Q[j][i]*pi[j];
      resid=Math.max(resid,Math.abs(acc));
    }
  }
  // metrics: X = mean departure rate over service stations
  const per=[];
  for(let k=0;k<stations.length;k++){
    let L=0, Xs=0;
    states.forEach((s,i)=>{
      const b=s[off+k]; L+=pi[i]*b; Xs+=pi[i]*Math.min(b,stations[k][1])/stations[k][0];
    });
    per.push({L,Xs});
  }
  return {X:per.reduce((a,p)=>a+p.Xs,0)/stations.length,
          L:per.map(p=>p.L), resid};
}

/* 1. Erlang C ground truths (Kleinrock Vol I, ch.3) */
ok('ErlangC(1,0.5)=0.5', Math.abs(C.erlangC(1,0.5)-0.5)<1e-12, C.erlangC(1,0.5)+'');
ok('ErlangC(2,1)=1/3', Math.abs(C.erlangC(2,1)-1/3)<1e-12, C.erlangC(2,1)+'');
ok('ErlangC(5,3)≈0.23625', Math.abs(C.erlangC(5,3)-0.2362)<2e-4, C.erlangC(5,3).toFixed(6));

/* 2. textbook closed M/M/1//2 chain: 2 agents, no think, two M/M/1 stations τ=1 */
{
  const s=C.solve({N:2,tauThink:0,tauLLM:1,cLLM:1,csLLM:1,tauDB:1,cDB:1,csDB:1});
  ok('closed M/M/1//2: X=2/3', Math.abs(s.X-2/3)<1e-9, s.X.toFixed(6));
  ok('closed M/M/1//2: L=1 each', Math.abs(s.LL-1)<1e-9 && Math.abs(s.LD-1)<1e-9,
     'LL='+s.LL.toFixed(4)+' LD='+s.LD.toFixed(4));
  ok('closed M/M/1//2: R=1.5', Math.abs(s.LL/s.X-1.5)<1e-9, (s.LL/s.X).toFixed(6));
}
/* 2b. same case vs the in-suite exact CTMC */
{
  const ct=ctmcExact(2,0,[[1,1],[1,1]]);
  ok('CTMC M/M/1//2 X=2/3 (independent solve)', ct && Math.abs(ct.X-2/3)<1e-6, ct?('X='+ct.X.toFixed(6)+' resid='+ct.resid.toExponential(1)):'null');
  const s=C.solve({N:2,tauThink:0,tauLLM:1,cLLM:1,csLLM:1,tauDB:1,cDB:1,csDB:1});
  ok('Buzen == CTMC (2-station)', Math.abs(s.X-ct.X)<1e-6, 'dX='+Math.abs(s.X-ct.X).toExponential(1));
}

/* 3. legacy two-endpoint defaults vs python exact CTMC anchor */
{
  const s=C.solve({N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1});
  ok('legacy N=8: X == 2.01274291 (0.5e-5)', Math.abs(s.X-2.01274291)<5e-6, s.X.toFixed(8));
  ok('legacy N=8: L anchor', Math.abs(s.LL-3.37069)<5e-5 && Math.abs(s.LD-0.60382)<5e-5,
     'LL='+s.LL.toFixed(5)+' LD='+s.LD.toFixed(5));
}
/* 4. N=1 chain: no queueing possible */
{
  const s=C.solve({N:1,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1});
  ok('N=1 exact throughput', Math.abs(s.X-1/(2+1.5+0.3))<1e-9, s.X.toFixed(6));
  ok('N=1 zero wait', s.WLq<1e-9 && s.WDq<1e-9, 'WL='+s.WLq.toExponential(2));
}
/* 5. defaults consistency */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P), d=C.latencyDistribution(P,s,2048,12);
  ok('defaults: rho<0.9995', !s.saturated, 'rhoL='+s.rhoL.toFixed(3)+' rhoD='+s.rhoD.toFixed(3));
  ok('defaults: grid mean ≈ chain mean (1%)', Math.abs(d.mean-s.T)/s.T<0.01,
     'grid='+d.mean.toFixed(4)+' chain='+s.T.toFixed(4));
  ok('defaults: mass capture ≥0.995', d.M>0.995, d.M.toFixed(5));
  ok('defaults: percentile ordering', d.p50<d.p90 && d.p90<d.p95 && d.p95<d.p99 && d.p90<Infinity,
     [d.p50,d.p90,d.p95,d.p99].map(v=>v.toFixed(2)).join('/'));
  ok('defaults: X<capacity', s.X<Math.min(4/1.5,8/0.3), 'X='+s.X.toFixed(4));
}
/* 6. SCV behavior */
{
  const P1={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const P0=Object.assign({},P1,{csLLM:0,csDB:0});
  const s1=C.solve(P1), s0=C.solve(P0);
  ok('SCV=0 → throughput not lower', s0.X>=s1.X-1e-9, 'X: '+s0.X.toFixed(3)+' vs '+s1.X.toFixed(3));
  const d1=C.latencyDistribution(P1,s1,2048,12), d0=C.latencyDistribution(P0,s0,2048,12);
  ok('SCV=0 p99 < SCV=1 p99', d0.p99<d1.p99, d0.p99.toFixed(2)+' vs '+d1.p99.toFixed(2));
}
/* 7. sampler moments (Gamma, Marsaglia–Tsang) */
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
/* 8. MC vs textbook closed M/M/1//2 */
{
  const P={N:2,tauThink:0,tauLLM:1,cLLM:1,csLLM:1,tauDB:1,cDB:1,csDB:1};
  const m=C.mcSim(P,20000,0.1,42);
  ok('MC M/M/1//2 X≈2/3', Math.abs(m.X-2/3)<0.05, 'X='+m.X.toFixed(4));
  const st=C.mcStats(m.lat);
  ok('MC M/M/1//2 mean lat≈3.0', Math.abs(st.mean-3.0)<0.12, st.mean.toFixed(4));
}
/* 9. MC vs analytic (same stochastic model) */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P), m=C.mcSim(P,20000,0.1,42), st=C.mcStats(m.lat);
  ok('MC vs analytic X (3%)', Math.abs(m.X-s.X)/s.X<0.03, 'dX='+(Math.abs(m.X-s.X)/s.X*100).toFixed(2)+'%');
  ok('MC vs analytic mean lat (3%)', Math.abs(st.mean-s.T)/s.T<0.03,
     'dM='+(Math.abs(st.mean-s.T)/s.T*100).toFixed(2)+'%');
  ok('MC p50 finite positive', isFinite(st.p50)&&st.p50>0, 'p50mc='+st.p50.toFixed(3));
}

/* ============ multi-station chains (K>2) ============ */
/* 10. symmetric K=3 chain vs in-suite exact CTMC */
{
  const P={N:3,tauThink:1,stations:[{name:'a',tau:1,c:2,scv:1},{name:'b',tau:1,c:2,scv:1},{name:'c',tau:1,c:2,scv:1}]};
  const s=C.solve(P);
  const ct=ctmcExact(3,1,[[1,2],[1,2],[1,2]]);
  ok('K=3 symmetric: Buzen==CTMC', Math.abs(s.X-ct.X)<1e-6,
     'X='+s.X.toFixed(6)+' ct='+ct.X.toFixed(6)+' resid='+ct.resid.toExponential(1));
  ok('K=3 symmetric: L pieces match', s.st.every((z,i)=>Math.abs(z.L-ct.L[i])<1e-6),
     s.st.map(z=>z.L.toFixed(4)).join('/')+' vs '+ct.L.map(v=>v.toFixed(4)).join('/'));
  ok('K=3 CTMC anchor X=0.73282443', Math.abs(ct.X-0.73282443)<1e-7, ct.X.toFixed(8));
}
/* 11. asymmetric K=3 chain vs exact CTMC */
{
  const P={N:4,tauThink:0.8,stations:[{name:'a',tau:1.2,c:1,scv:1},{name:'b',tau:0.5,c:3,scv:1},{name:'c',tau:0.9,c:2,scv:1}]};
  const s=C.solve(P);
  const ct=ctmcExact(4,0.8,[[1.2,1],[0.5,3],[0.9,2]]);
  ok('K=3 asymmetric: Buzen==CTMC', Math.abs(s.X-ct.X)<1e-6,
     'X='+s.X.toFixed(6)+' ct='+ct.X.toFixed(6));
}
/* 12. decomposed-DB chain at defaults scale vs exact CTMC */
{
  const P={N:8,tauThink:2,stations:[{name:'LLM',tau:1.5,c:4,scv:1},{name:'DBcmp',tau:0.18,c:4,scv:1},{name:'DBsto',tau:0.12,c:4,scv:1}]};
  const s=C.solve(P);
  const ct=ctmcExact(8,2,[[1.5,4],[0.18,4],[0.12,4]]);
  ok('K=3 decDB N=8: Buzen==CTMC', Math.abs(s.X-ct.X)<1e-6,
     'X='+s.X.toFixed(6)+' ct='+ct.X.toFixed(6)+' resid='+ct.resid.toExponential(1));
  ok('K=3 decDB: CTMC anchor 2.012739', Math.abs(ct.X-2.012739)<2e-6, ct.X.toFixed(8));
}
/* 13. zero-think K=3 percentile floor: p50 ≥ T0 */
{
  const P={N:6,tauThink:0,stations:[{name:'a',tau:1,c:2,scv:1},{name:'b',tau:1,c:2,scv:1},{name:'c',tau:1,c:2,scv:1}]};
  const s=C.solve(P), d=C.latencyDistribution(P,s,2048,12);
  ok('K=3 zero-think: p50≥T0', d.p50>=d.T0-1e-9,
     'p50='+d.p50.toFixed(3)+' T0='+d.T0.toFixed(3));
  ok('K=3 zero-think: grid≈T (1%)', Math.abs(d.mean-s.T)/s.T<0.01,
     'grid='+d.mean.toFixed(4)+' T='+s.T.toFixed(4));
}
/* 14. MC vs analytic for the decDB 3-station chain */
{
  const P={N:8,tauThink:2,stations:[{name:'LLM',tau:1.5,c:4,scv:1},{name:'DBcmp',tau:0.18,c:4,scv:1},{name:'DBsto',tau:0.12,c:4,scv:1}]};
  const s=C.solve(P), m=C.mcSim(P,20000,0.1,7), st=C.mcStats(m.lat);
  ok('MC decDB 3-station: X (3%)', Math.abs(m.X-s.X)/s.X<0.03,
     'mc='+m.X.toFixed(4)+' an='+s.X.toFixed(4));
  ok('MC decDB 3-station: mean (3%)', Math.abs(st.mean-s.T)/s.T<0.03,
     'mc='+st.mean.toFixed(3)+' an='+s.T.toFixed(3));
}

/* 15. P(wait>0) — approximate calibration check (documented approximation).
   The analytic π̂ matches the exact mean wait; the empirical no-wait probability
   in a closed chain deviates. Bounded loose, not exact. */
{
  const P={N:8,tauThink:2,tauLLM:1.5,cLLM:4,csLLM:1,tauDB:0.3,cDB:8,csDB:1};
  const s=C.solve(P), m=C.mcSim(P,20000,0.1,42);
  let fL=0,fD=0; for(const w of m.wl) if(w>0) fL++; for(const w of m.wd) if(w>0) fD++;
  ok('P(wait) same order (loose, documented approximation)',
     Math.abs(fL/m.kept-s.wL.pi)<0.25 && Math.abs(fD/m.kept-s.wD.pi)<0.25,
     'π̂L='+s.wL.pi.toFixed(3)+' emp='+(fL/m.kept).toFixed(3));
}

console.log('\n'+pass+' passed, '+fail+' failed');
process.exit(fail?1:0);
