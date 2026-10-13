/* agent_db_sim analyzer — math core (browser + node dual-use) */
/*MATHCORE-START*/
'use strict';

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
/* Gamma(k, scale 1): Marsaglia–Tsang for k>=1, Stug trick (G(k)=G(k+1)*U^(1/k)) for k<1 */
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
/* service-time sampler with mean tau, SCV scv (Var/mean²) */
function makeSampler(tau, scv, rng){
  if(scv<0.02) return ()=>tau;
  const k=1/scv, theta=tau*scv;
  if(Math.abs(k-1)<1e-9) return ()=> -Math.log(Math.max(rng(),1e-16))*tau;
  return ()=> gamma1(rng,k)*theta;
}

/* ---------- Erlang C (M/M/c) ---------- */
function erlangB(c, a){ let b=1; for(let k=1;k<=c;k++) b = a*b/(k + a*b); return b; }
function erlangC(c, a){ const b=erlangB(c,a); return b/(1-(a/c)*(1-b)); }
function logfact(k){ let s=0; for(let i=2;i<=k;i++) s+=Math.log(i); return s; }

/* ---------- product-form weights (log-space to avoid overflow) ---------- */
/* M/M(c) station weight: f(n) = τ^n/n! (n≤c), τ^c/c!·(τ/c)^(n−c) (n>c) */
function mmcfLog(c, tau, nMax){
  const lf=new Array(nMax+1); lf[0]=0;
  const lt=Math.log(tau);
  let cur=0;
  for(let n=1;n<=nMax;n++){
    if(n<=c)     cur = n*lt - logfact(n);
  else         cur = lf[n-1] + (lt - Math.log(c));   /* keeps (τ/c) factor at n=c+1 too */
    lf[n]=cur;
  }
  return lf;
}
/* infinite-server (think) node weight: f(n)= τ^n / n! */
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

/* Exact closed chain (Gordon–Newell/Buzen): [think IS if τt>0] ⊛ LLM M/M/cL ⊛ DB M/M/cD */
function solveClosedChain(P){
  const N=Math.round(P.N), cL=Math.round(P.cLLM), cD=Math.round(P.cDB);
  const tauL=P.tauLLM, tauD=P.tauDB, tauT=Math.max(0,P.tauThink);
  const f1=normExp(mmcfLog(cL,tauL,N));
  const f2=normExp(mmcfLog(cD,tauD,N));
  const muL=n=>Math.min(n,cL)/tauL, muD=n=>Math.min(n,cD)/tauD;

  let GN, piL, piD;
  if(tauT>0){
    const f0=normExp(isfLog(tauT,N));
    const restL=convArr(f0,f2,N);
    GN=convArr(restL,f1,N)[N];
    if(!(GN>0)) return {ok:false};
    piL=n=>f1[n]*restL[N-n]/GN;
    const restD=convArr(f0,f1,N);
    piD=n=>f2[n]*restD[N-n]/GN;
  } else {
    const G=convArr(f1,f2,N);
    GN=G[N];
    if(!(GN>0)) return {ok:false};
    piL=n=>f1[n]*f2[N-n]/GN;
    piD=n=>f2[n]*f1[N-n]/GN;
  }
  let LL=0,XL=0,LD=0,XD=0;
  for(let n=1;n<=N;n++){
    const pL=piL(n); LL+=n*pL; XL+=muL(n)*pL;
    const pD=piD(n); LD+=n*pD; XD+=muD(n)*pD;
  }
  const X=(XL+XD)/2;
  return {ok:true, X, LL, LD, LT:Math.max(0,N-LL-LD), rhoL:X*tauL/cL, rhoD:X*tauD/cD, N};
}

/* Full analytic solve: exact chain (SCV=1) + Allen–Cunneen SCV inflation + calibrated
   per-station wait mixtures {pi:P(wait>0), mu:exp tail rate}. */
function solve(P){
  const s=solveClosedChain(P);
  if(!s.ok) return s;
  const cL=Math.round(P.cLLM), cD=Math.round(P.cDB);
  const corrL=(1+P.csLLM)/2, corrD=(1+P.csDB)/2;
  const RL=s.LL/s.X, RD=s.LD/s.X;
  const WLq=Math.max(0,RL-P.tauLLM)*corrL;
  const WDq=Math.max(0,RD-P.tauDB)*corrD;
  const cal=(tau,c,X,Wq)=>{
    /* closed-chain calibration: a waiting agent sees all c servers busy, so the
       wait tail is Exp(c/τ) (validated empirically vs MC bake-off); π̂ rescaled
       so the mixture mean still equals the exact chain mean wait. */
    let mu=c/tau, pi;
    if(mu<=1e-12){ pi=1; mu=Wq>0?1/Wq:1e6; }
    else{
      pi=mu*Wq;
      if(pi>=1){ pi=1; mu=1/Math.max(Wq,1e-12); }
    }
    return {mu,pi};
  };
  const wL=cal(P.tauLLM,cL,s.X,WLq);
  const wD=cal(P.tauDB,cD,s.X,WDq);
  return Object.assign(s,{RL,RD,WLq,WDq,corrL,corrD,wL,wD,T:P.tauThink+RL+RD,
                          saturated:(s.rhoL>=0.9995||s.rhoD>=0.9995)});
}

/* Latency distribution (per-bin masses on a uniform grid):
   latency = T0(τL+τD) + ExpThink ⊛ WaitMixLLM ⊛ WaitMixDB */
function latencyDistribution(P, sol, gridN, horizonMult){
  const meanLat=P.tauThink+sol.RL+sol.RD;
  const horizon=horizonMult*Math.max(meanLat,1e-6);
  const dx=horizon/(gridN-1);
  const T0=P.tauLLM+P.tauDB;
  const dEx=m=>1-Math.exp(-m*dx);
  const fT=new Float64Array(gridN);
  if(P.tauThink>1e-9){
    const rT=1/P.tauThink;
    for(let i=0;i<gridN;i++) fT[i]=Math.exp(-rT*i*dx)*dEx(rT);
  } else fT[0]=1;
  const mkWait=w=>{
    const f=new Float64Array(gridN);
    f[0]=(1-w.pi)+w.pi*dEx(w.mu);                      // no-wait delta + [0,dx) tail mass
    for(let i=1;i<gridN;i++) f[i]=w.pi*Math.exp(-w.mu*i*dx)*dEx(w.mu);
    return f;
  };
  const convF=(a,b)=>{
    const n=a.length, out=new Float64Array(n);
    for(let i=0;i<n;i++){
      const ai=a[i]; if(ai===0) continue;
      for(let j=0;j<n-i;j++) out[i+j]+=ai*b[j];
    }
    return out;
  };
  let f=convF(mkWait(sol.wL),mkWait(sol.wD));
  f=convF(fT,f);
  const cdf=new Float64Array(gridN);
  let acc=0;
  for(let i=0;i<gridN;i++){ acc+=f[i]; cdf[i]=acc; }
  const M=acc||1;
  const pct=p=>{
    const tg=p*M;
    if(cdf[gridN-1]<tg) return Infinity;
    let lo=0,hi=gridN-1;
    while(lo<hi){ const mid=(lo+hi)>>1; if(cdf[mid]<tg) lo=mid+1; else hi=mid; }
    if(lo===0) return (tg/cdf[0])*dx + T0;            // percentile inside first bin
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

/* ---------- Monte Carlo: FCFS discrete-event simulation of the closed loop ---------- */
/* Correct M/G/c semantics with FIFO queues; agents as a state machine. */
function mcSim(P, nKeep, warmFrac, seed){
  const N=Math.round(P.N), cL=Math.round(P.cLLM), cD=Math.round(P.cDB);
  const nTotal=Math.max(Math.round(nKeep/(1-Math.min(warmFrac,0.9))),nKeep);
  const skip=nTotal-nKeep;
  const rng=mulberry32(seed>>>0);
  const sT=makeSampler(Math.max(P.tauThink,0),1,rng);
  const sL=makeSampler(P.tauLLM,P.csLLM,rng);
  const sD=makeSampler(P.tauDB,P.csDB,rng);

  /* agent phases: 0 think,1 in LLQ,2 LLM busy,3 in DBQ,4 DB busy */
  const phase=new Int8Array(N);
  const wake=new Float64Array(N);
  const dT=new Float64Array(N);
  const arrL=new Float64Array(N), arrD=new Float64Array(N);
  const wLat=new Float64Array(N), wDat=new Float64Array(N), sojL=new Float64Array(N);
  for(let i=0;i<N;i++){ phase[i]=0; wake[i]=sT(); }   // initial think from t=0

  const qL=[], qD=[];
  let busyL=0, busyD=0, busyClkL=0, busyClkD=0;
  let now=0, completed=0, kept=0, lastComp=0;
  let firstCycleStart=Infinity, firstComp=Infinity;
  const latA=new Float64Array(nKeep), wLA=new Float64Array(nKeep), wDA=new Float64Array(nKeep);
  let guard=0, maxIter=nTotal*6+20000;

  const dispatchL=()=>{
    while(busyL<cL && qL.length){
      const a=qL.shift();
      wLat[a]=now-arrL[a];
      const s=sL();
      busyL++; busyClkL+=s;
      phase[a]=2; wake[a]=now+s;
    }
  };
  const dispatchD=()=>{
    while(busyD<cD && qD.length){
      const a=qD.shift();
      wDat[a]=now-arrD[a];
      const s=sD();
      busyD++; busyClkD+=s;
      phase[a]=4; wake[a]=now+s;
    }
  };

  while(kept<nKeep){
    if(guard++>maxIter) return {ok:false};
    let a=-1;
    for(let i=0;i<N;i++)
      if((phase[i]===0||phase[i]===2||phase[i]===4)&&(a<0||wake[i]<wake[a])) a=i;
    if(a<0) break;
    now=wake[a];
    const ph=phase[a];
    if(ph===0){                       // think done → enter LLM queue
      arrL[a]=now; phase[a]=1; wake[a]=Infinity;
      qL.push(a); dispatchL();
    } else if(ph===2){                // LLM done → dispatch LLM → enter DB queue
      busyL--; sojL[a]=now-arrL[a]; dispatchL();
      arrD[a]=now; phase[a]=3; wake[a]=Infinity;
      qD.push(a); dispatchD();
    } else {                          // DB done → record cycle → think again
      busyD--; dispatchD();
      completed++;
      if(completed>skip){
        wLA[kept]=wLat[a]; wDA[kept]=wDat[a];
        latA[kept]=dT[a]+sojL[a]+(now-arrD[a]);
        if(kept===0){ firstComp=now; firstCycleStart=arrL[a]-dT[a]; }
        lastComp=now;
        kept++;
      }
      const s=sT(); dT[a]=s; phase[a]=0; wake[a]=now+s;
    }
  }
  if(kept<nKeep) return {ok:false};
  return {ok:true, lat:latA, wl:wLA, wd:wDA, kept:kept,
          X:(kept-1)/Math.max(1e-9,(lastComp-firstComp)),
          rhoL:busyClkL/(cL*lastComp), rhoD:busyClkD/(cD*lastComp),
          window:(lastComp-firstCycleStart)};
}

/* percentile helper on a Float64Array (sorted copy) */
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
    normExp,convArr,solveClosedChain,solve,latencyDistribution,mcSim,mcStats};
}
