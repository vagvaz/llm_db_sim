/* agent_db_sim analyzer — math core v2 (browser + node dual-use).
   Generalized: chain of K M/G/c service stations (+ optional IS think node),
   closed loop of N agents visiting every station once per cycle. */
/*MATHCORE-START*/
'use strict';
/*MATHCORE-START*/

/* ---------- RNG & samplers ---------- */
function mulberry32(seed){
  let a=seed>>>0;
  return function(){
    a|=0; a=(a+0x6D2B79F5)|0;
    let t=Math.imul(a^(a>>>15), 1|a);
    t=(t+Math.imul(t^(t>>>7), 61|t))^t;
    return ((t^(t>>>14))>>>0)/4294967296;
  };
}
/* Gamma(k, scale 1): Marsaglia–Tsang k>=1, Stug trick k<1 */
function gamma1(rng, k){
  if(k<1) return gamma1(rng, k+1)*Math.pow(Math.max(rng(),1e-16), 1/k);
  const d=k-1/3, c=1/Math.sqrt(9*d);
  for(;;){
    let x, v;
    for(;;){
      const u=Math.max(rng(),1e-16);
      x=Math.sqrt(-2*Math.log(u))*Math.cos(2*Math.PI*rng());
      v=1+c*x;
      if(v>0) break;
    }
    v=v*v*v;
    const u=Math.max(rng(),1e-16);
    if(u<1-0.0331*x*x*x*x) return d*v;
    if(Math.log(u) < 0.5*x*x + d*(1 - v + Math.log(v))) return d*v;
  }
}
/* service-time sampler with mean tau, SCV scv */
function makeSampler(tau, scv, rng){
  if(scv<0.02) return ()=>tau;
  const k=1/scv, theta=tau*scv;
  if(Math.abs(k-1)<1e-9) return ()=> -Math.log(Math.max(rng(),1e-16))*tau;
  return ()=> gamma1(rng,k)*theta;
}

/* ---------- Erlang C ---------- */
function erlangB(c, a){ let b=1; for(let k=1;k<=c;k++) b = a*b/(k + a*b); return b; }
function erlangC(c, a){ const b=erlangB(c,a); return b/(1-(a/c)*(1-b)); }
function logfact(k){ let s=0; for(let i=2;i<=k;i++) s+=Math.log(i); return s; }

/* ---------- product-form weights (log space) ---------- */
function mmcfLog(c, tau, nMax){
  const lf=new Array(nMax+1); lf[0]=0;
  const lt=Math.log(tau);
  let cur=0;
  for(let n=1;n<=nMax;n++){
    if(n<=c) cur = n*lt - logfact(n);
    else     cur = lf[n-1] + (lt - Math.log(c));
    lf[n]=cur;
  }
  return lf;
}
function isfLog(tau, nMax){
  const lf=new Array(nMax+1); lf[0]=0;
  const lt=Math.log(tau);
  for(let n=1;n<=nMax;n++) lf[n]=lf[n-1]+lt-Math.log(n);
  return lf;
}
function normExp(lf){
  let m=-Infinity; for(const v of lf) if(v>m) m=v;
  const f=new Array(lf.length);
  for(let i=0;i<lf.length;i++) f[i]=Math.exp(lf[i]-m);
  return f;
}
function convArr(a,b,nMax){
  const g=new Array(nMax+1).fill(0);
  const na=Math.min(a.length-1,nMax), nb=Math.min(b.length-1,nMax);
  for(let i=0;i<=na;i++){
    const ai=a[i]; if(!ai) continue;
    for(let j=0;j<=Math.min(nb,nMax-i);j++) g[i+j]+=ai*b[j];
  }
  return g;
}

/* ---------- station config ----------
   P.stations = [{name,tau,c,scv}, ...]  (K ≥ 1), or legacy fields
   {tauLLM,cLLM,csLLM,tauDB,cDB,csDB} mapped to [LLM, DB]. */
function deriveStations(P){
  if(Array.isArray(P.stations) && P.stations.length>0){
    return P.stations.map((s,i)=>({
      name:String(s.name||('st'+(i+1))),
      tau:  Math.max(1e-6, +s.tau||1e-6),
      c:    Math.max(1, Math.round(+s.c||1)),
      scv:  Math.max(0, +s.scv||0)
    }));
  }
  return [
    {name:'LLM', tau:Math.max(1e-3,+P.tauLLM||1e-3), c:Math.max(1,Math.round(+P.cLLM||1)), scv:Math.max(0,+P.csLLM||0)},
    {name:'DB',  tau:Math.max(1e-3,+P.tauDB ||1e-3), c:Math.max(1,Math.round(+P.cDB ||1)), scv:Math.max(0,+P.csDB ||0)}
  ];
}

/* Exact closed chain (Gordon–Newell/Buzen) over arbitrary station chains.
   Produces per-station marginals via prefix/suffix convolutions (O(K·N²)). */
function solveChain(N, tauThink, stations){
  const fs=[];
  const hasThink = tauThink>1e-9;
  if(hasThink) fs.push(normExp(isfLog(tauThink,N)));
  const svcIdx = stations.map((st,k)=>{ fs.push(normExp(mmcfLog(st.c, st.tau, N))); return (hasThink? k+1 : k); });
  const Kt=fs.length;
  const unit=new Array(N+1).fill(0); unit[0]=1;
  const pref=[unit];
  for(let k=1;k<=Kt;k++) pref.push(convArr(pref[k-1], fs[k-1], N));
  const suf=new Array(Kt+1); suf[Kt]=unit;
  for(let k=Kt-1;k>=0;k--) suf[k]=convArr(fs[k], suf[k+1], N);
  const GN=pref[Kt][N];
  if(!(GN>0)) return {ok:false};

  const st=stations.map((sd,k)=>{
    const idx=svcIdx[k];
    const rest=convArr(pref[idx], suf[idx+1], N);
    let L=0, Xs=0;
    for(let n=1;n<=N;n++){
      const p=fs[idx][n]*rest[N-n]/GN;
      L+=n*p;
      Xs+=Math.min(n,sd.c)/sd.tau*p;
    }
    return {name:sd.name, tau:sd.tau, c:sd.c, scv:sd.scv, L, Xs};
  });
  const X=st.reduce((a,s)=>a+s.Xs,0)/st.length;   // flow balance: all Xs equal (exact)
  const Lsum=st.reduce((a,s)=>a+s.L,0);
  st.forEach(s=>{ s.rho=X*s.tau/s.c; s.R=s.L/X; });
  return {ok:true, X, st, LT:Math.max(0,N-Lsum), N};
}

/* Full solve: exact chain (SCV=1) + Allen–Cunneen SCV inflation + calibrated
   per-station wait mixtures {pi:P(wait>0), mu:Exp tail rate, mean matched}. */
function solve(P){
  const stations=deriveStations(P);
  const core=solveChain(Math.round(P.N), Math.max(0,P.tauThink||0), stations);
  if(!core.ok) return core;
  const X=core.X;
  core.st.forEach(s=>{
    s.corr=(1+s.scv)/2;
    s.Wq=Math.max(0,s.R-s.tau)*s.corr;
    s.Lq=X*s.Wq;
    /* closed-chain calibration: waiting agent sees all c servers busy → tail Exp(c/τ);
       π̂ = μ̂·W (clamped) rescales to keep the mixture mean = exact chain mean wait */
    let mu=s.tau>0? s.c/s.tau : 1, pi;
    if(mu<=1e-12){ pi=1; mu=s.Wq>0?1/s.Wq:1e6; }
    else{
      pi=mu*s.Wq;
      if(pi>=1){ pi=1; mu=1/Math.max(s.Wq,1e-12); }
    }
    s.mix={mu,pi};
  });
  const T=(P.tauThink||0)+core.st.reduce((a,s)=>a+s.R,0);
  const saturated=core.st.every(s=>s.rho>=0.9995);
  /* legacy aliases for the two-endpoint topology [LLM, DB] */
  const out=Object.assign(core,{stations, T, saturated, satMax:Math.max.apply(null,core.st.map(s=>s.rho))});
  if(core.st.length===2){
    const a=core.st[0], b=core.st[1];
    out.LL=a.L; out.LD=b.L;
    out.RL=a.R; out.RD=b.R;
    out.rhoL=a.rho; out.rhoD=b.rho;
    out.WLq=a.Wq; out.WDq=b.Wq;
    out.corrL=a.corr; out.corrD=b.corr;
    out.wL=a.mix; out.wD=b.mix;
  }
  return out;
}

/* Latency distribution (per-bin masses): latency = T0(Σ τ) + ExpThink ⊛ mix_0 ⊛ … ⊛ mix_{K−1} */
function latencyDistribution(P, sol, gridN, horizonMult){
  const meanLat=(P.tauThink||0)+sol.st.reduce((a,s)=>a+s.R,0);
  const horizon=horizonMult*Math.max(meanLat,1e-6);
  const dx=horizon/(gridN-1);
  const T0=sol.st.reduce((a,s)=>a+s.tau,0);
  const dEx=m=>1-Math.exp(-m*dx);
  let f;
  if(P.tauThink>1e-9){
    const rT=1/P.tauThink;
    f=new Float64Array(gridN);
    for(let i=0;i<gridN;i++) f[i]=Math.exp(-rT*i*dx)*dEx(rT);
  } else { f=new Float64Array(gridN); f[0]=1; }
  const mkWait=w=>{
    const g=new Float64Array(gridN);
    g[0]=(1-w.pi)+w.pi*dEx(w.mu);
    for(let i=1;i<gridN;i++) g[i]=w.pi*Math.exp(-w.mu*i*dx)*dEx(w.mu);
    return g;
  };
  const convF=(a,b)=>{
    const n=a.length, out=new Float64Array(n);
    for(let i=0;i<n;i++){
      const ai=a[i]; if(ai===0) continue;
      for(let j=0;j<n-i;j++) out[i+j]+=ai*b[j];
    }
    return out;
  };
  for(const s of sol.st) f=convF(f,mkWait(s.mix));
  const cdf=new Float64Array(gridN);
  let acc=0;
  for(let i=0;i<gridN;i++){ acc+=f[i]; cdf[i]=acc; }
  const M=acc||1;
  const pct=p=>{
    const tg=p*M;
    if(cdf[gridN-1]<tg) return Infinity;
    let lo=0,hi=gridN-1;
    while(lo<hi){ const mid=(lo+hi)>>1; if(cdf[mid]<tg) lo=mid+1; else hi=mid; }
    if(lo===0) return (tg/cdf[0])*dx + T0;
    const i=Math.max(1,lo), c0=cdf[i-1], c1=cdf[i];
    return ((i-1)+((c1>c0)?(tg-c0)/(c1-c0):0))*dx + T0;
  };
  let m=0,m2=0;
  for(let i=0;i<gridN;i++){ m+=i*dx*f[i]; m2+=i*dx*i*dx*f[i]; }
  m=m/M+T0; m2=m2/M;
  return {p50:pct(.5),p90:pct(.9),p95:pct(.95),p99:pct(.99),
          mean:m, sd:Math.sqrt(Math.max(0,m2-m*m)),
          f:Array.from(f), cdf, M, dx, horizon, gridN, T0};
}

/* ---------- Monte Carlo: exact FCFS event simulation over the station chain ---------- */
function mcSim(P, nKeep, warmFrac, seed){
  const N=Math.round(P.N);
  const stations=deriveStations(P), K=stations.length;
  const nTotal=Math.max(Math.round(nKeep/(1-Math.min(warmFrac,0.9))),nKeep);
  const skip=nTotal-nKeep;
  const rng=mulberry32(seed>>>0);
  const sT=makeSampler(Math.max(P.tauThink||0,0),1,rng);
  const samplers=stations.map(s=>makeSampler(s.tau,s.scv,rng));

  /* phase: −1 thinking · p=0..K−1 at station p (queued if wake=∞, in service otherwise) */
  const phase=new Int8Array(N).fill(-1);
  const wake=new Float64Array(N);
  const dT=new Float64Array(N), sojSum=new Float64Array(N);
  const arr=new Float64Array(N*K), wq=new Float64Array(N*K);
  for(let i=0;i<N;i++){ phase[i]=-1; wake[i]=sT(); dT[i]=wake[i]; }  // initial think; dT stored (approx from 0)

  const qs=[]; for(let k=0;k<K;k++) qs.push([]);
  let busy=[], busyClk=[]; for(let k=0;k<K;k++){ busy.push(0); busyClk.push(0); }
  let now=0, completed=0, kept=0, firstComp=Infinity, lastComp=0;
  const latA=new Float64Array(nKeep);
  const wqA=[]; for(let k=0;k<K;k++) wqA.push(new Float64Array(nKeep));
  let guard=0, maxIter=nTotal*(K+1)*4+20000;
  void firstComp;

  const dispatch=k=>{
    const c=stations[k].c;
    while(busy[k]<c && qs[k].length){
      const a=qs[k].shift();
      wq[a*K+k]=now-arr[a*K+k];
      const s=samplers[k]();
      busy[k]++; busyClk[k]+=s;
      wake[a]=now+s;
    }
  };

  while(kept<nKeep){
    if(guard++>maxIter) return {ok:false};
    let a=-1;
    for(let i=0;i<N;i++){
      if(phase[i]===-1 || (phase[i]>=0 && isFinite(wake[i]))){
        if(a<0 || wake[i]<wake[a]) a=i;
      }
    }
    if(a<0) break;
    now=wake[a];
    const ph=phase[a];
    if(ph===-1){               /* think done → station 0 */
      arr[a*K]=now; phase[a]=0; wake[a]=Infinity;
      qs[0].push(a); dispatch(0);
    } else {                   /* service done at station ph */
      busy[ph]--; dispatch(ph);
      sojSum[a]+=now-arr[a*K+ph];
      if(ph<K-1){
        arr[a*K+ph+1]=now; phase[a]=ph+1; wake[a]=Infinity;
        qs[ph+1].push(a); dispatch(ph+1);
      } else {
        /* completed cycle */
        completed++;
        if(completed>skip){
          for(let k=0;k<K;k++) wqA[k][kept]=wq[a*K+k];
          latA[kept]=dT[a]+sojSum[a];
          if(kept===0) firstComp=now;
          lastComp=now;
          kept++;
        }
        const s=sT(); dT[a]=s; sojSum[a]=0;
        phase[a]=-1; wake[a]=now+s;
      }
    }
  }
  if(kept<nKeep) return {ok:false};
  const K0=Math.max(1e-9,lastComp-firstComp);
  const rho=busyClk.map((b,k)=>b/(stations[k].c*lastComp));
  const out={ok:true, lat:latA, wq:wqA, kept, X:(kept-1)/K0, rho, K, stations};
  /* means per station from kept samples */
  out.wqMean=wqA.map(w=>{ let s=0; for(const v of w) s+=v; return s/kept; });
  if(K===2){ out.wl=out.wq[0]; out.wd=out.wq[1]; }
  return out;
}
function mcStats(arr){
  const n=arr.length, s=Float64Array.from(arr).sort();
  const q=p=>{
    const idx=p*(n-1), lo=Math.floor(idx), hi=Math.min(n-1,lo+1);
    return s[lo]+(s[hi]-s[lo])*(idx-lo);
  };
  let m=0; for(const v of arr) m+=v; m/=n;
  let v2=0; for(const w of arr) v2+=(w-m)*(w-m);
  return {p50:q(.5),p90:q(.9),p95:q(.95),p99:q(.99),mean:m,sd:Math.sqrt(v2/n),sorted:s};
}

/*MATHCORE-END*/
/* node export guard (browser ignores this) */
if(typeof module!=='undefined' && module.exports){
  module.exports={mulberry32,gamma1,makeSampler,erlangB,erlangC,logfact,mmcfLog,isfLog,
    normExp,convArr,deriveStations,solveChain,solveChain:solveChain,solve,latencyDistribution,mcSim,mcStats,erlangC};
}
/*MATHCORE-END*/
