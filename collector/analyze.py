import json, os, glob
from collections import defaultdict, Counter
from datetime import datetime, timezone, timedelta, date

TR_TZ=timezone(timedelta(hours=3))
DATA_DIR="data"
ERKEN_DK=3; PENCERE_DK=12; BIRLES_DK=18; SON_DRK_KALAN=3; TRAFIK_MIN=2

def load_schedule(hat_no):
    path=os.path.join(DATA_DIR,"schedules",f"{hat_no}.json")
    if not os.path.exists(path): return None
    try:
        with open(path) as f: return json.load(f)
    except: return None

def load_day_logs(hat_no,day_str):
    pattern=os.path.join(DATA_DIR,"lines",str(hat_no),"logs",day_str,"*.json")
    logs=[]
    for fp in sorted(glob.glob(pattern)):
        try:
            with open(fp) as f: logs.append(json.load(f))
        except: pass
    return logs

def is_weekday(day_str): return date.fromisoformat(day_str).weekday()<5
def sched_dt(day_str,hhmm):
    h,m=map(int,hhmm.split(":"))
    d=date.fromisoformat(day_str)
    return datetime(d.year,d.month,d.day,h,m,tzinfo=TR_TZ)
def parse_ts(ts): return datetime.fromisoformat(ts)

def detect_sefer_iptali(logs,seferler,day_str,hat_no,yon):
    viols=[]
    for saat in seferler:
        t0=sched_dt(day_str,saat)-timedelta(minutes=PENCERE_DK)
        t1=sched_dt(day_str,saat)+timedelta(minutes=PENCERE_DK)
        pencere=[lg for lg in logs if t0<=parse_ts(lg["timestamp"])<=t1]
        if not pencere: continue
        araclar=set()
        for lg in pencere:
            for b in lg.get("buses_on_route",[]): araclar.add(b.get("OtobusId"))
        if not araclar:
            viols.append({"tip":"SEFERİPTAL","hat_no":hat_no,"tarih":day_str,"saat":saat,"yon":yon,
                "aciklama":f"Hat {hat_no}: {saat} tarifeli sefer {PENCERE_DK} dk boyunca hic arac yok.","araclar":[],"severity":"critical"})
    return viols

def detect_erken_kalkis(logs,seferler,day_str,hat_no,yon):
    viols=[]; app_key="baslangic_gidis" if yon=="gidis" else "baslangic_donus"
    for saat in seferler:
        t_sefer=sched_dt(day_str,saat)
        t0=t_sefer-timedelta(minutes=15); t1=t_sefer-timedelta(minutes=ERKEN_DK)
        erken={}
        for lg in logs:
            ts=parse_ts(lg["timestamp"])
            if not (t0<=ts<=t1): continue
            for a in lg.get("approaching",{}).get(app_key,[]):
                if a.get("KalanDurakSayisi",99)<=SON_DRK_KALAN:
                    oid=a.get("OtobusId")
                    if oid and oid not in erken: erken[oid]=ts
        for oid,ts in erken.items():
            fark=(t_sefer-ts).total_seconds()/60
            if fark>=ERKEN_DK:
                viols.append({"tip":"ERKANKALKIS","hat_no":hat_no,"tarih":day_str,"saat":saat,"yon":yon,
                    "arac_id":oid,"aciklama":f"Hat {hat_no}: Arac #{oid}, {saat} seferinden ~{int(fark)} dk once kalkti.",
                    "araclar":[oid],"severity":"warning"})
    return viols

def detect_birlesim(logs,seferler,day_str,hat_no,yon):
    viols=[]; app_key="baslangic_donus" if yon=="gidis" else "baslangic_gidis"
    arac_kayit=defaultdict(list)
    for lg in logs:
        ts=parse_ts(lg["timestamp"])
        lg_trafik={b["OtobusId"]:b.get("trafik_kume",0) for b in lg.get("buses_on_route",[])}
        for a in lg.get("approaching",{}).get(app_key,[]):
            if a.get("KalanDurakSayisi",99)<=SON_DRK_KALAN:
                oid=a.get("OtobusId")
                if oid: arac_kayit[oid].append((ts,lg_trafik.get(oid,0)))
    for oid,kayitlar in arac_kayit.items():
        if len(kayitlar)<2: continue
        kayitlar.sort(key=lambda x:x[0])
        ilk,son=kayitlar[0][0],kayitlar[-1][0]
        bekleme=(son-ilk).total_seconds()/60
        if bekleme<BIRLES_DK: continue
        ort_trafik=sum(k[1] for k in kayitlar)/len(kayitlar)
        ilgili=[s for s in seferler if ilk<=sched_dt(day_str,s)<=son+timedelta(minutes=10)]
        viols.append({"tip":"SEFERBİRLEŞ","hat_no":hat_no,"tarih":day_str,"saat":son.strftime("%H:%M"),"yon":yon,
            "arac_id":oid,"aciklama":f"Hat {hat_no}: Arac #{oid}, {ilk.strftime('%H:%M')}-{son.strftime('%H:%M')} ({int(bekleme)} dk) son durakta bekledi.{' Atlanan:'+','.join(ilgili) if ilgili else ''}",
            "araclar":[oid],"birlesilen_seferler":ilgili,"bekleme_dakika":int(bekleme),"trafik_skoru":round(ort_trafik,1),
            "severity":"medium" if ort_trafik>=TRAFIK_MIN else "high"})
    return viols

def analyze_line(hat_no,day_str):
    sched=load_schedule(hat_no); logs=load_day_logs(hat_no,day_str)
    if not logs: return []
    if not sched or not sched.get("tarife_var"):
        print(f"  [HAT {hat_no}] Tarife yok, sadece canli."); return []
    key="weekday" if is_weekday(day_str) else "weekend"
    viols=[]
    for yon,yon_key in [("gidis","gidis"),("donus","donus")]:
        seferler=sched.get(yon_key,{}).get(key,[])
        if not seferler: continue
        viols+=detect_sefer_iptali(logs,seferler,day_str,hat_no,yon)
        viols+=detect_erken_kalkis(logs,seferler,day_str,hat_no,yon)
        viols+=detect_birlesim(logs,seferler,day_str,hat_no,yon)
    print(f"  [HAT {hat_no}] {len(viols)} ihlal")
    return viols

def save_violations(hat_no,viols):
    path=os.path.join(DATA_DIR,"lines",str(hat_no),"violations.json")
    existing=[]
    if os.path.exists(path):
        try:
            with open(path) as f: existing=json.load(f)
        except: pass
    def vkey(v): return f"{v['tip']}|{v['tarih']}|{v['saat']}|{v['yon']}|{v.get('arac_id','_')}"
    ex_keys={vkey(v) for v in existing}
    yeni=[v for v in viols if vkey(v) not in ex_keys]
    cutoff=(datetime.now(TR_TZ)-timedelta(days=30)).strftime("%Y-%m-%d")
    combined=sorted([v for v in existing+yeni if v["tarih"]>=cutoff],key=lambda v:(v["tarih"],v["saat"]),reverse=True)
    os.makedirs(os.path.dirname(path),exist_ok=True)
    with open(path,"w") as f: json.dump(combined,f,ensure_ascii=False,indent=2)

def merge_all():
    all_viols=[]
    for fp in glob.glob(os.path.join(DATA_DIR,"lines","*","violations.json")):
        try:
            with open(fp) as f: all_viols+=json.load(f)
        except: pass
    all_viols.sort(key=lambda v:(v["tarih"],v["saat"]),reverse=True)
    out=os.path.join(DATA_DIR,"violations","violations.json")
    os.makedirs(os.path.dirname(out),exist_ok=True)
    with open(out,"w") as f: json.dump(all_viols,f,ensure_ascii=False,indent=2)
    tips=Counter(v["tip"] for v in all_viols)
    hats=Counter(v["hat_no"] for v in all_viols)
    saatler=Counter(v["saat"] for v in all_viols)
    araclar=defaultdict(lambda:defaultdict(int))
    for v in all_viols:
        for oid in v.get("araclar",[]):
            araclar[oid][v["tip"]]+=1; araclar[oid]["toplam"]+=1
    top_araclar=sorted([{"oid":oid,**dict(c)} for oid,c in araclar.items()],key=lambda x:-x.get("toplam",0))[:10]
    patterns={"guncelleme":datetime.now(TR_TZ).isoformat(),"toplam_ihlal":len(all_viols),
              "tip_dagilimi":dict(tips),"hat_dagilimi":{str(k):v for k,v in hats.items()},
              "riskli_saatler":[{"saat":s,"ihlal":n} for s,n in saatler.most_common(10)],
              "surekli_ihlalci_araclar":top_araclar}
    with open(os.path.join(DATA_DIR,"patterns.json"),"w") as f: json.dump(patterns,f,ensure_ascii=False,indent=2)
    print(f"[MERGE] {len(all_viols)} toplam ihlal")

def main():
    now=datetime.now(TR_TZ); today=now.strftime("%Y-%m-%d"); yesterday=(now-timedelta(days=1)).strftime("%Y-%m-%d")
    with open(os.path.join(DATA_DIR,"watched_lines.json")) as f: wl=json.load(f)
    active=[l for l in wl.get("lines",[]) if l.get("active")]
    for line in active:
        hat_no=line["hat_no"]; print(f"\n[ANALYZE] Hat {hat_no}")
        all_v=[]
        for day in [yesterday,today]: all_v+=analyze_line(hat_no,day)
        save_violations(hat_no,all_v)
    merge_all()

if __name__=="__main__": main()
