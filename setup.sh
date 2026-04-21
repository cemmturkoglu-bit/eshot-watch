#!/bin/bash
cd ~/Desktop/eshot-watch

# Temizle
rm -f collector/*.py collector/requirements.txt index.html
mkdir -p collector data/schedules data/lines data/violations .github/workflows

# collect.py
cat > collector/collect.py << 'PYEOF'
import requests, json, os, math
from datetime import datetime, timezone, timedelta

TR_TZ=timezone(timedelta(hours=3))
API_BASE="https://openapi.izmir.bel.tr/api/iztek"
DATA_DIR="data"
GPS_LAG_METRE=30
TRAFIK_R_KM=0.8
TRAFIK_MIN=2
HIZ_YAVAS=10

def haversine(lat1,lon1,lat2,lon2):
    R=6371000
    p1,p2=math.radians(lat1),math.radians(lat2)
    dp,dl=math.radians(lat2-lat1),math.radians(lon2-lon1)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

def api_get(path):
    try:
        r=requests.get(f"{API_BASE}/{path}",timeout=15)
        if r.status_code==204: return []
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  HATA {path}: {e}"); return []

def load_prev(hat_no):
    path=os.path.join(DATA_DIR,"lines",str(hat_no),"latest.json")
    try:
        with open(path) as f: return json.load(f)
    except: return None

def enrich(buses,prev,interval=300):
    prev_map={b["OtobusId"]:b for b in (prev or {}).get("buses_on_route",[])}
    yavas=[]; enriched=[]
    for b in buses:
        oid=b.get("OtobusId"); lat,lon=b.get("KoorX",0),b.get("KoorY",0)
        e=dict(b); e.update({"hareket_metre":None,"hiz_kmh":None,"gps_lag":False,"trafik_kume":0})
        if oid in prev_map:
            pb=prev_map[oid]
            dist=haversine(lat,lon,pb.get("KoorX",0),pb.get("KoorY",0))
            spd=(dist/interval)*3.6
            e["hareket_metre"]=round(dist,1); e["hiz_kmh"]=round(spd,1); e["gps_lag"]=dist<GPS_LAG_METRE
            if spd<HIZ_YAVAS: yavas.append((lat,lon,oid))
        enriched.append(e)
    for e in enriched:
        lat,lon=e.get("KoorX",0),e.get("KoorY",0)
        e["trafik_kume"]=sum(1 for(vl,vn,vi) in yavas if vi!=e["OtobusId"] and haversine(lat,lon,vl,vn)<TRAFIK_R_KM*1000)
    return enriched

def collect_line(line):
    hat_no=line["hat_no"]
    print(f"\n[HAT {hat_no}] toplanıyor...")
    prev=load_prev(hat_no)
    raw=api_get(f"hatotobuskonumlari/{hat_no}")
    buses=enrich(raw.get("HatOtobusKonumlari",raw) if isinstance(raw,dict) else raw,prev)
    approaching={}
    for key,durak_id in [("baslangic_gidis",line.get("durak_baslangic_gidis")),("baslangic_donus",line.get("durak_baslangic_donus"))]:
        if durak_id:
            data=api_get(f"hattinyaklasanotobusleri/{hat_no}/{durak_id}")
            approaching[key]=data if isinstance(data,list) else []
        else: approaching[key]=[]
    now=datetime.now(TR_TZ); d_str=now.strftime("%Y-%m-%d"); t_str=now.strftime("%H-%M")
    snap={"hat_no":hat_no,"timestamp":now.isoformat(),"timestamp_unix":int(now.timestamp()),
          "buses_on_route":buses,"approaching":approaching,
          "gps_lag_araclar":[b["OtobusId"] for b in buses if b.get("gps_lag")],
          "trafik_alanlari":[{"lat":b["KoorX"],"lon":b["KoorY"],"arac_sayisi":b["trafik_kume"]+1} for b in buses if b.get("trafik_kume",0)>=TRAFIK_MIN]}
    line_dir=os.path.join(DATA_DIR,"lines",str(hat_no))
    log_dir=os.path.join(line_dir,"logs",d_str)
    os.makedirs(log_dir,exist_ok=True)
    with open(os.path.join(log_dir,f"{t_str}.json"),"w") as f: json.dump(snap,f,ensure_ascii=False)
    with open(os.path.join(line_dir,"latest.json"),"w") as f: json.dump(snap,f,ensure_ascii=False,indent=2)
    print(f"  {len(buses)} arac | lag:{len(snap['gps_lag_araclar'])}")

def main():
    with open(os.path.join(DATA_DIR,"watched_lines.json")) as f: wl=json.load(f)
    active=[l for l in wl.get("lines",[]) if l.get("active")]
    print(f"[COLLECT] {len(active)} hat")
    for line in active:
        try: collect_line(line)
        except Exception as e: print(f"  HATA Hat {line['hat_no']}: {e}")
    now=datetime.now(TR_TZ)
    summary={"timestamp":now.isoformat(),"lines":[]}
    for line in active:
        p=os.path.join(DATA_DIR,"lines",str(line["hat_no"]),"latest.json")
        if os.path.exists(p):
            try:
                with open(p) as f: d=json.load(f)
                summary["lines"].append({"hat_no":line["hat_no"],"ad":line.get("ad",""),"arac_sayisi":len(d.get("buses_on_route",[])),"timestamp":d.get("timestamp"),"gps_lag":len(d.get("gps_lag_araclar",[]))})
            except: pass
    with open(os.path.join(DATA_DIR,"latest.json"),"w") as f: json.dump(summary,f,ensure_ascii=False,indent=2)
    print("[COLLECT] Tamam.")

if __name__=="__main__": main()
PYEOF

# analyze.py
cat > collector/analyze.py << 'PYEOF'
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
PYEOF

# fetch_schedule.py
cat > collector/fetch_schedule.py << 'PYEOF'
import requests, json, os, re, sys
from datetime import datetime, timezone, timedelta
try:
    from bs4 import BeautifulSoup
    BS4=True
except: BS4=False

TR_TZ=timezone(timedelta(hours=3))
DATA_DIR="data"
SCHED_DIR=os.path.join(DATA_DIR,"schedules")
HEADERS={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","Accept-Language":"tr-TR,tr;q=0.9"}

def scrape_tarife(hat_no,yon_id):
    if not BS4: return []
    url=f"https://www.eshot.gov.tr/tr/UlasimSaatleri/{hat_no}/{yon_id}"
    try:
        r=requests.get(url,timeout=20,headers=HEADERS)
        if r.status_code!=200: return []
        soup=BeautifulSoup(r.text,"html.parser")
        saatler=set()
        for text in soup.stripped_strings:
            for m in re.findall(r'\b([0-2]?\d:[0-5]\d)\b',text):
                h,mn=map(int,m.split(":"))
                if 0<=h<=23 and 0<=mn<=59: saatler.add(f"{h:02d}:{mn:02d}")
        return sorted(saatler)
    except: return []

def fetch_and_save(hat_no):
    os.makedirs(SCHED_DIR,exist_ok=True)
    out_path=os.path.join(SCHED_DIR,f"{hat_no}.json")
    if os.path.exists(out_path):
        age=(datetime.now().timestamp()-os.path.getmtime(out_path))/3600
        if age<24: print(f"[HAT {hat_no}] Tarife guncel."); return
    print(f"[HAT {hat_no}] Tarife cekiliyor...")
    gidis=[]; donus=[]
    if BS4:
        for offset in range(300):
            yon_id=offset+1
            s=scrape_tarife(hat_no,yon_id)
            if s and len(s)>=5:
                if not gidis: gidis=s; print(f"  Gidis bulundu: yon_id={yon_id}")
                elif not donus: donus=s; print(f"  Donus bulundu: yon_id={yon_id}"); break
            if offset>500 and not gidis: break
    schedule={"hat_no":hat_no,"guncelleme":datetime.now(TR_TZ).isoformat(),
              "tarife_var":bool(gidis or donus),
              "gidis":{"weekday":gidis,"weekend":[]},"donus":{"weekday":donus,"weekend":[]}}
    with open(out_path,"w") as f: json.dump(schedule,f,ensure_ascii=False,indent=2)
    print(f"[HAT {hat_no}] {'Tarife kaydedildi' if gidis else 'Tarife bulunamadi - sadece canli takip'}.")

def main():
    hat_nos=[int(x) for x in sys.argv[1:]] if len(sys.argv)>1 else []
    if not hat_nos:
        with open(os.path.join(DATA_DIR,"watched_lines.json")) as f: wl=json.load(f)
        hat_nos=[l["hat_no"] for l in wl.get("lines",[]) if l.get("active")]
    for hat_no in hat_nos: fetch_and_save(hat_no)

if __name__=="__main__": main()
PYEOF

# add_line.py
cat > collector/add_line.py << 'PYEOF'
import json, os, sys, requests
DATA_DIR="data"
API_BASE="https://openapi.izmir.bel.tr/api/iztek"

def add_line(hat_no):
    wl_path=os.path.join(DATA_DIR,"watched_lines.json")
    with open(wl_path) as f: wl=json.load(f)
    existing=[l for l in wl["lines"] if l["hat_no"]==hat_no]
    if existing:
        existing[0]["active"]=True; print(f"Hat {hat_no} aktif edildi.")
    else:
        ad=f"Hat {hat_no}"
        try:
            r=requests.get(f"{API_BASE}/hatotobuskonumlari/{hat_no}",timeout=15)
            if r.status_code==200:
                buses=r.json().get("HatOtobusKonumlari",[])
                if buses: ad=buses[0].get("HatAdi",ad)
        except: pass
        wl["lines"].append({"hat_no":hat_no,"ad":ad,"active":True,"durak_baslangic_gidis":None,"durak_baslangic_donus":None})
        print(f"Hat {hat_no} eklendi: {ad}")
    with open(wl_path,"w") as f: json.dump(wl,f,ensure_ascii=False,indent=2)
    line_dir=os.path.join(DATA_DIR,"lines",str(hat_no))
    os.makedirs(os.path.join(line_dir,"logs"),exist_ok=True)
    vp=os.path.join(line_dir,"violations.json")
    if not os.path.exists(vp):
        with open(vp,"w") as f: json.dump([],f)

if __name__=="__main__":
    if len(sys.argv)<2: print("Kullanim: python add_line.py <hat_no>"); sys.exit(1)
    add_line(int(sys.argv[1]))
PYEOF

echo "requests==2.31.0
beautifulsoup4==4.12.3" > collector/requirements.txt

# data dosyaları
echo '{"_aciklama":"Izlenen hatlar","lines":[{"hat_no":684,"ad":"Urla - F.Altay Ekspres","active":true,"durak_baslangic_gidis":50605,"durak_baslangic_donus":51916}]}' > data/watched_lines.json
echo '[]' > data/violations/violations.json
echo '{"timestamp":"2025-01-01T00:00:00+03:00","lines":[]}' > data/latest.json
echo '{"toplam_ihlal":0,"tip_dagilimi":{},"hat_dagilimi":{},"riskli_saatler":[],"surekli_ihlalci_araclar":[]}' > data/patterns.json

echo "TUM DOSYALAR OLUSTURULDU"
ls -la collector/
