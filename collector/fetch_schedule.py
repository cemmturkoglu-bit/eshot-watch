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
