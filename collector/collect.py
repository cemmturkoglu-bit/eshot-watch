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
