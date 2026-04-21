"""
ESHOT Hat Tarife Çekici
Verilen hat numarası için ESHOT sitesinden tarife saatlerini çeker ve
data/schedules/{hat_no}.json dosyasına kaydeder.

Strateji:
  1. ESHOT açık veri portalından hat ID'lerini bul
  2. Her iki yön için ESHOT tarifesini scrape et
  3. Durak listesini de çek
  4. Başarısız olursa boş tarife ile devam et (sadece canlı takip)
"""

import requests
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup

TR_TZ    = timezone(timedelta(hours=3))
DATA_DIR = "data"
SCHED_DIR = os.path.join(DATA_DIR, "schedules")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

# ESHOT açık veri hat listesi API
HAT_LIST_URL = "https://acikveri.bizizmir.com/api/3/action/datastore_search?resource_id=6d05e381-c066-4771-9a8d-74af3de19a01&limit=500&q={hat_no}"

# ESHOT tarife sayfası
TARIFE_URL   = "https://www.eshot.gov.tr/tr/UlasimSaatleri/{hat_no}/{yon_id}"
TARIFE_URL2  = "https://www.eshot.gov.tr/tr/OtobusHatSaatleri/252?HatId={hat_id}"

# Durak listesi
DURAK_URL    = "https://openapi.izmir.bel.tr/api/iztek/hattinyaklasanotobusleri/{hat_no}/{durak_id}"


def fetch_hat_info(hat_no: int) -> dict:
    """Hat adı ve yön ID'lerini bulmaya çalışır."""
    # Önce canlı API'den kontrol et — hat var mı?
    try:
        r = requests.get(
            f"https://openapi.izmir.bel.tr/api/iztek/hatotobuskonumlari/{hat_no}",
            timeout=15, headers=HEADERS
        )
        if r.status_code == 200:
            data = r.json()
            buses = data.get("HatOtobusKonumlari", [])
            print(f"[HAT {hat_no}] API'de bulundu. Anlık {len(buses)} araç.")
        else:
            print(f"[HAT {hat_no}] API 204/hata: hat boş veya yok.")
    except Exception as e:
        print(f"[HAT {hat_no}] API hatası: {e}")

    return {"hat_no": hat_no}


def scrape_tarife(hat_no: int, yon_id: int) -> list[str]:
    """
    ESHOT UlasimSaatleri sayfasından kalkış saatlerini çeker.
    Döndürür: ['06:00', '06:25', ...] veya []
    """
    url = TARIFE_URL.format(hat_no=hat_no, yon_id=yon_id)
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")

        # Saat formatını ara: HH:MM
        saatler = set()
        for text in soup.stripped_strings:
            matches = re.findall(r'\b([0-2]?\d:[0-5]\d)\b', text)
            for m in matches:
                h, mn = map(int, m.split(":"))
                if 0 <= h <= 23 and 0 <= mn <= 59:
                    saatler.add(f"{h:02d}:{mn:02d}")

        saatler = sorted(saatler)
        print(f"[HAT {hat_no}] yon_id={yon_id}: {len(saatler)} saat bulundu")
        return saatler

    except Exception as e:
        print(f"[HAT {hat_no}] Tarife scrape hatası (yon={yon_id}): {e}")
        return []


def scrape_tarife_v2(hat_no: int) -> dict:
    """
    ESHOT OtobusHatSaatleri sayfasından tarife çeker.
    Hafta içi / hafta sonu + her iki yön için.
    """
    # Birden fazla yon_id dene (genellikle sequential)
    # 684 için: gidiş=288, dönüş=289
    # Genel kural: 2 × hat_no civarı veya ardışık
    # En güvenilir yol: her iki yön sayfasını dene

    result = {
        "gidis": {"weekday": [], "weekend": []},
        "donus": {"weekday": [], "weekend": []},
    }

    # ESHOT'un mobil API'si bazen JSON döner
    try:
        api_url = f"https://www.eshot.gov.tr/api/HatSaatleri?hatNo={hat_no}"
        r = requests.get(api_url, timeout=15, headers=HEADERS)
        if r.status_code == 200 and r.headers.get("content-type","").startswith("application/json"):
            data = r.json()
            print(f"[HAT {hat_no}] JSON API çalıştı!")
            return data
    except Exception:
        pass

    # HTML scrape — yon_id aralığı tara
    # ESHOT ID pattern: eshot sayfası URL'inden çek
    search_url = f"https://www.eshot.gov.tr/tr/OtobusHatListesi?HatNo={hat_no}"
    try:
        r = requests.get(search_url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            # Tarife linklerini bul
            links = soup.find_all("a", href=re.compile(r"/tr/UlasimSaatleri/"))
            for link in links:
                href = link.get("href", "")
                parts = href.strip("/").split("/")
                if len(parts) >= 3:
                    try:
                        yon_id = int(parts[-1])
                        saatler = scrape_tarife(hat_no, yon_id)
                        if saatler:
                            # İlk bulunan = gidiş, ikinci = dönüş
                            if not result["gidis"]["weekday"]:
                                result["gidis"]["weekday"] = saatler
                            elif not result["donus"]["weekday"]:
                                result["donus"]["weekday"] = saatler
                    except ValueError:
                        pass
    except Exception as e:
        print(f"[HAT {hat_no}] Hat listesi scrape hatası: {e}")

    # Fallback: ardışık ID'leri dene (hat_no * 2 civarı)
    if not result["gidis"]["weekday"]:
        base = hat_no * 2 - 100  # yaklaşık tahmin
        for offset in range(200):
            yon_id = base + offset
            if yon_id < 1:
                continue
            saatler = scrape_tarife(hat_no, yon_id)
            if saatler and len(saatler) >= 5:
                if not result["gidis"]["weekday"]:
                    result["gidis"]["weekday"] = saatler
                    # Dönüş bir sonraki ID
                    donus = scrape_tarife(hat_no, yon_id + 1)
                    if donus and len(donus) >= 5:
                        result["donus"]["weekday"] = donus
                    break
            if offset > 50 and not saatler:
                # Çok uzaklaştık, dur
                break

    return result


def fetch_and_save(hat_no: int) -> bool:
    """
    Hat için tarife çeker ve dosyaya kaydeder.
    True: başarılı, False: tarife alınamadı (canlı takip devam eder)
    """
    os.makedirs(SCHED_DIR, exist_ok=True)
    out_path = os.path.join(SCHED_DIR, f"{hat_no}.json")

    # Mevcut dosya varsa ve 24 saatten yeni ise atla
    if os.path.exists(out_path):
        mtime = os.path.getmtime(out_path)
        age_h = (datetime.now().timestamp() - mtime) / 3600
        if age_h < 24:
            print(f"[HAT {hat_no}] Tarife güncel (son güncelleme {age_h:.1f} saat önce), atlanıyor.")
            return True

    print(f"[HAT {hat_no}] Tarife çekiliyor...")
    hat_info = fetch_hat_info(hat_no)
    tarife   = scrape_tarife_v2(hat_no)

    schedule = {
        "hat_no":      hat_no,
        "guncelleme":  datetime.now(TR_TZ).isoformat(),
        "tarife_var":  bool(tarife["gidis"]["weekday"] or tarife["donus"]["weekday"]),
        "gidis":       tarife["gidis"],
        "donus":       tarife["donus"],
        "_kaynak":     "eshot.gov.tr scrape",
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schedule, f, ensure_ascii=False, indent=2)

    if schedule["tarife_var"]:
        print(f"[HAT {hat_no}] Tarife kaydedildi: {out_path}")
        return True
    else:
        print(f"[HAT {hat_no}] Tarife bulunamadı — sadece canlı takip çalışacak.")
        return False


def main():
    # Komut satırından hat numarası alınabilir
    if len(sys.argv) > 1:
        hat_nos = [int(x) for x in sys.argv[1:]]
    else:
        # watched_lines.json'dan al
        wl_path = os.path.join(DATA_DIR, "watched_lines.json")
        if not os.path.exists(wl_path):
            print("watched_lines.json bulunamadı.")
            return
        with open(wl_path, encoding="utf-8") as f:
            wl = json.load(f)
        hat_nos = [l["hat_no"] for l in wl.get("lines", []) if l.get("active")]

    for hat_no in hat_nos:
        fetch_and_save(hat_no)


if __name__ == "__main__":
    main()
