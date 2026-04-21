import requests, json, os, math
from datetime import datetime, timezone, timedelta

HAT_ID=684
API_BASE="https://openapi.izmir.bel.tr/api/iztek"
TR_TZ=timezone(timedelta(hours=3))
DURAK_URLA=50605
DURAK_FALTAY=51916

def haversine(lat1,lon1,lat2,lon2):
    R=6371000
    p1,p2=math.radians(lat1),math.radians(lat2)
    dp,dl=math.radians(lat2-lat1),math.radians(lon2-lon1)
    a=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

def load_prev():
    try:
        with open("data/latest.json") as f: return json.load(f)
    except: return None

def api_get(path):
    try:
        r=requests.get(f"{API_BASE}/{path}",timeout=15)
        if r.status_code==204: return []
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"HATA {path}: {e}"); return []

def main():
    now=datetime.now(TR_TZ)
    prev=load_prev()
    prev_map={b["OtobusId"]:b for b in (prev or {}).get("buses_on_route",[])}
    raw=api_get(f"hatotobuskonumlari/{HAT_ID}")
    buses=raw.get("HatOtobusKonumlari",raw) if isinstance(raw,dict) else raw
    enriched=[]
    for b in buses:
        e=dict(b)
        oid=b.get("OtobusId")
        lat,lon=b.get("KoorX",0),b.get("KoorY",0)
        if oid in prev_map:
            pb=prev_map[oid]
            dist=haversine(lat,lon,pb.get("KoorX",0),pb.get("KoorY",0))
            e["hareket_metre"]=round(dist,1)
            e["hiz_kmh"]=round((dist/300)*3.6,1)
            e["gps_lag"]=dist<30
        else:
            e["hareket_metre"]=None; e["hiz_kmh"]=None; e["gps_lag"]=False
        enriched.append(e)
    urla=api_get(f"hattinyaklasanotobusleri/{HAT_ID}/{DURAK_URLA}")
    faltay=api_get(f"hattinyaklasanotobusleri/{HAT_ID}/{DURAK_FALTAY}")
    snap={"timestamp":now.isoformat(),"timestamp_unix":int(now.timestamp()),
          "buses_on_route":enriched,"urla_approaching":urla if isinstance(urla,list) else [],
          "faltay_approaching":faltay if isinstance(faltay,list) else [],
          "gps_lag_araclar":[b["OtobusId"] for b in enriched if b.get("gps_lag")]}
    d=now.strftime("%Y-%m-%d"); t=now.strftime("%H-%M")
    os.makedirs(f"data/logs/{d}",exist_ok=True)
    with open(f"data/logs/{d}/{t}.json","w") as f: json.dump(snap,f,ensure_ascii=False)
    with open("data/latest.json","w") as f: json.dump(snap,f,ensure_ascii=False)
    print(f"OK {now.strftime('%H:%M')} | {len(enriched)} arac")

if __name__=="__main__": main()
