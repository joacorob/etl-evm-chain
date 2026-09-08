#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
revert_reveng v2 — reverse-engineering de fee_apr REAL sostenido por par blue-chip.

Hallazgo de probing (clave): el endpoint lista de revert acepta
  ?token0=<addr_lower>&token1=<addr_lower>   (addresses en LOWERCASE)
  ?pool=<pool_addr>                          (pool exacto)
y filtros server-side que andan: has-withdrawn=false, in-range, sort, page.

En vez de ordenar por APR (que trae spam/scam con APR roto), consultamos
PARES CONOCIDOS y medimos el fee_apr SOSTENIDO (revert ya da fees/deposits
annualizado) de posiciones con plata real, viejas y sin retiros. Eso resuelve
empíricamente la pregunta del modelo xYield: ¿el fee in-range real es 30% o 59%?
"""
import sys, json, math, urllib.request, urllib.parse
from collections import defaultdict
import statistics as st

MIN_AGE_D, MIN_VALUE, MAX_VALUE, PAGES = 30.0, 10_000.0, 100_000_000.0, 4

T = {  # tokens por chain (LOWERCASE)
 "mainnet": {"WETH":"0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2","USDC":"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
             "USDT":"0xdac17f958d2ee523a2206206994597c13d831ec7","WBTC":"0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
             "DAI":"0x6b175474e89094c44da98b954eedeac495271d0f"},
 "base": {"WETH":"0x4200000000000000000000000000000000000006","USDC":"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
          "cbBTC":"0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"},
 "arbitrum": {"WETH":"0x82af49447d8a07e3bd95bd0d56f35241523fbab1","USDC":"0xaf88d065e77c8cc2239327c5edb3a432268e5831",
              "USDT":"0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9","WBTC":"0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"},
}
PAIRS = [  # (chain, A, B)
 ("mainnet","WETH","USDC"),("mainnet","WETH","USDT"),("mainnet","WBTC","USDC"),
 ("mainnet","USDC","USDT"),("mainnet","DAI","USDC"),
 ("base","WETH","USDC"),("base","cbBTC","USDC"),("base","cbBTC","WETH"),
 ("arbitrum","WETH","USDC"),("arbitrum","WBTC","USDC"),
]
STABLES={"USDC","USDT","DAI","cbBTC"}  # cbBTC no es stable pero marca pares BTC
def klass(a,b):
    s={"USDC","USDT","DAI"}; aS,bS=a in s,b in s
    return "stable/stable" if aS and bS else ("stable/vol" if aS or bS else "vol/vol")

def fetch(chain,a,b,page):
    t0,t1=sorted([a,b])  # uniswap ordena por address
    q=urllib.parse.urlencode({"token0":t0,"token1":t1,"has-withdrawn":"false","limit":100,"page":page,
                              "sort":"underlying_value","desc":"true"})  # ballenas: plata real gestionada
    url=f"https://api.revert.finance/v1/positions/{chain}/uniswapv3?{q}"
    try:
        with urllib.request.urlopen(url,timeout=30) as r: return json.loads(r.read()).get("data",[])
    except Exception as e:
        print(f"  err {chain} {e}",file=sys.stderr); return []

def fee_apr_of(p):
    pf=(p.get("performance") or {}).get("hodl") or {}
    try:
        v=float(pf.get("fee_apr")); return v if 0<=v<1000 else None
    except: return None

def hw_pct(tl,tu):
    dt=tu-tl
    if dt<=0 or dt>200000: return None  # full-range / inválido
    return (math.sqrt(1.0001**dt)-1)*100

def q(v,p):
    v=sorted(v); return v[min(len(v)-1,int(len(v)*p))] if v else float("nan")

def main():
    allrows=defaultdict(list)  # (chain,pairlabel,fee_tier) -> [row]
    for chain,A,B in PAIRS:
        ta,tb=T[chain].get(A),T[chain].get(B)
        if not ta or not tb: continue
        got=0
        for page in range(1,PAGES+1):
            data=fetch(chain,ta,tb,page)
            if not data: break
            for p in data:
                if p.get("has_withdrawn") or p.get("exited"): continue
                age=float(p.get("age") or 0); val=float(p.get("underlying_value") or 0)
                fa=fee_apr_of(p)
                if fa is None or age<MIN_AGE_D or not (MIN_VALUE<=val<=MAX_VALUE): continue
                hw=hw_pct(p.get("tick_lower",0),p.get("tick_upper",0))
                allrows[(chain,f"{A}/{B}",p.get("fee_tier"))].append(
                    {"fa":fa,"age":age,"val":val,"hw":hw,"inR":p.get("in_range")})
                got+=1
        print(f"[{chain} {A}/{B}] {got} sostenibles",file=sys.stderr)

    print("\n"+"="*92)
    print("FEE_APR REAL SOSTENIDO por par/fee-tier (age>=30d, sin retiros, val $10k-100M, revert API)")
    print("="*92)
    print(f"{'chain':8}{'pair':12}{'fee':>6}{'n':>4}{'medAPR':>8}{'p25':>6}{'p75':>7}{'p90':>7}  {'medHW±%':>8} {'%inR':>5} {'clase':>11}")
    agg=[]
    for (chain,pair,ft),rows in allrows.items():
        if len(rows)<5: continue
        fa=[r["fa"] for r in rows]; hw=[r["hw"] for r in rows if r["hw"] is not None]
        inr=sum(1 for r in rows if r["inR"])/len(rows)*100
        a,b=pair.split("/")
        agg.append((st.median(fa),chain,pair,ft,len(rows),q(fa,.25),q(fa,.75),q(fa,.90),
                    st.median(hw) if hw else None,inr,klass(a,b)))
    agg.sort(key=lambda x:(x[1],x[2],-x[0]))
    for med,chain,pair,ft,n,p25,p75,p90,hw,inr,kl in agg:
        hws=f"{hw:.1f}" if hw else "full"
        print(f"{chain:8}{pair:12}{ft:>6}{n:>4}{med:>7.1f}%{p25:>5.0f}%{p75:>6.0f}%{p90:>6.0f}%  {hws:>7} {inr:>4.0f}% {kl:>11}")

    print("\n--- CONCENTRADAS (±hw<40%): el fee real del LP activo, por par ---")
    print(f"{'chain':8}{'pair':12}{'fee':>6}{'n':>4}{'medAPR':>8}{'p75':>7}  {'medHW±%':>8}")
    conc=defaultdict(list)
    for (chain,pair,ft),rows in allrows.items():
        for r in rows:
            if r["hw"] is not None and r["hw"]<40: conc[(chain,pair,ft)].append(r)
    cagg=[]
    for k,rows in conc.items():
        if len(rows)<5: continue
        fa=[r["fa"] for r in rows]; hw=[r["hw"] for r in rows]
        cagg.append((st.median(fa),*k,len(rows),q(fa,.75),st.median(hw)))
    cagg.sort(reverse=True)
    for med,chain,pair,ft,n,p75,hw in cagg:
        print(f"{chain:8}{pair:12}{ft:>6}{n:>4}{med:>7.1f}%{p75:>6.0f}%  {hw:>7.1f}")

    print("\n--- Veredicto para el modelo xYield (WETH/USDC, fee real in-range) ---")
    for (chain,pair,ft),rows in sorted(allrows.items()):
        if pair in ("WETH/USDC","WETH/USDT"):
            cr=[r for r in rows if r["hw"] is not None and r["hw"]<40]
            if len(cr)>=5:
                fa=[r["fa"] for r in cr]
                print(f"  {chain:8} {pair} fee={ft}: concentradas n={len(cr)} medAPR={st.median(fa):.1f}% "
                      f"p75={q(fa,.75):.0f}% (vs 30% conservador / 59% optimista del MC)")

if __name__=="__main__": main()
