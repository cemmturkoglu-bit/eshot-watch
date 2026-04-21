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
