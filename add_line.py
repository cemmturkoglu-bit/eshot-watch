"""
Yeni hat izlemeye ekler.
Kullanım: python collector/add_line.py 684
"""

import json
import os
import sys
import requests

DATA_DIR = "data"
API_BASE = "https://openapi.izmir.bel.tr/api/iztek"


def get_hat_info(hat_no: int) -> dict:
    """Hat bilgisini API'den çeker."""
    try:
        r = requests.get(f"{API_BASE}/hatotobuskonumlari/{hat_no}", timeout=15)
        if r.status_code == 200:
            data = r.json()
            buses = data.get("HatOtobusKonumlari", [])
            ad = buses[0].get("HatAdi", f"Hat {hat_no}") if buses else f"Hat {hat_no}"
            return {"hat_no": hat_no, "ad": ad, "arac_sayisi": len(buses)}
    except Exception as e:
        print(f"API hatası: {e}")
    return {"hat_no": hat_no, "ad": f"Hat {hat_no}", "arac_sayisi": 0}


def add_line(hat_no: int):
    wl_path = os.path.join(DATA_DIR, "watched_lines.json")

    with open(wl_path, encoding="utf-8") as f:
        wl = json.load(f)

    # Zaten var mı?
    existing = [l for l in wl["lines"] if l["hat_no"] == hat_no]
    if existing:
        # Aktif et
        existing[0]["active"] = True
        print(f"Hat {hat_no} zaten listede — aktif edildi.")
    else:
        # Hat bilgisini çek
        info = get_hat_info(hat_no)

        # Kalkış durak ID'lerini bulmaya çalış
        # (Kullanıcı sonradan manuel ekleyebilir)
        new_line = {
            "hat_no": hat_no,
            "ad": info["ad"],
            "active": True,
            "durak_baslangic_gidis": None,
            "durak_baslangic_donus": None,
        }
        wl["lines"].append(new_line)
        print(f"Hat {hat_no} eklendi: {info['ad']} ({info['arac_sayisi']} araç)")

    with open(wl_path, "w", encoding="utf-8") as f:
        json.dump(wl, f, ensure_ascii=False, indent=2)

    # Klasör yapısını oluştur
    line_dir = os.path.join(DATA_DIR, "lines", str(hat_no))
    os.makedirs(os.path.join(line_dir, "logs"), exist_ok=True)

    violations_path = os.path.join(line_dir, "violations.json")
    if not os.path.exists(violations_path):
        with open(violations_path, "w") as f:
            json.dump([], f)

    print(f"Hat {hat_no} izlemeye alındı.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Kullanım: python add_line.py <hat_no>")
        sys.exit(1)
    add_line(int(sys.argv[1]))
