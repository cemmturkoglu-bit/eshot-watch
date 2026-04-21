"""
ESHOT 684 Hat İzleme - İhlal Analiz Motoru

Tespit edilen ihlaller:
  SEFERİPTAL   — Tarife saatinde hiç araç yok
  ERKENKALKIŞ  — Araç tarifeden 3+ dk önce kalkmış
  SEFERBİRLEŞ  — Araç son durağa gelmiş, 18+ dk beklemiş (trafik dışı)

Ek modüller:
  - Trafik vs. birleştirme ayrımı (trafik_kume verisi kullanılır)
  - Haftalık örüntü analizi (en riskli saatler, en çok ihlal yapan araçlar)
  - Şikayet yanıtı kategorilendirme (arıza / sürücü / trafik / red)
"""

import json
import os
import glob
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date

TR_TZ      = timezone(timedelta(hours=3))
DATA_DIR   = "data"
LOG_DIR    = os.path.join(DATA_DIR, "logs")
VIOL_FILE  = os.path.join(DATA_DIR, "violations", "violations.json")
PAT_FILE   = os.path.join(DATA_DIR, "patterns.json")
COMP_FILE  = os.path.join(DATA_DIR, "complaints.json")
SCHED_FILE = os.path.join(DATA_DIR, "schedule.json")

# ─── Eşikler ────────────────────────────────────────────────────────────────
ERKEN_DK       = 3    # Tarifeden bu kadar erken kalkış → ihlal
PENCERE_DK     = 12   # Tarife ±N dk içinde araç görülmeli
BIRLES_DK      = 18   # Son durağa yakın bu kadar bekleme → şüpheli
SON_DURAK_KALAN = 3   # KalanDurakSayisi ≤ bu → son durağa yakın
TRAFIK_KUME_MIN = 2   # Bu kadar trafik_kume varsa → gerçek trafik kabul et

# Şikayet yanıtı anahtar kelimeleri
BAHANE_KEYS = {
    "arıza":   ["arıza", "arızalı", "teknik", "motor", "bakım", "servis", "onarım"],
    "sürücü":  ["sürücü", "şoför", "hasta", "sağlık", "devir", "izin", "istirahat"],
    "trafik":  ["trafik", "yoğunluk", "kaza", "yol", "kapandı", "olumsuz"],
    "red":     ["haklı", "uygun", "mevzuat", "geçerli", "makul"],
    "kabul":   ["haksız", "uyarı", "tutanak", "soruşturma", "disiplin"],
}


# ─── Yardımcılar ─────────────────────────────────────────────────────────────
def load_schedule() -> dict:
    with open(SCHED_FILE, encoding="utf-8") as f:
        return json.load(f)

def load_day_logs(day_str: str) -> list:
    files = sorted(glob.glob(os.path.join(LOG_DIR, day_str, "*.json")))
    logs  = []
    for fp in files:
        try:
            with open(fp, encoding="utf-8") as f:
                logs.append(json.load(f))
        except Exception:
            pass
    return logs

def parse_ts(ts: str) -> datetime:
    return datetime.fromisoformat(ts)

def sched_dt(day_str: str, hhmm: str) -> datetime:
    h, m = map(int, hhmm.split(":"))
    d    = date.fromisoformat(day_str)
    return datetime(d.year, d.month, d.day, h, m, tzinfo=TR_TZ)

def is_weekday(day_str: str) -> bool:
    return date.fromisoformat(day_str).weekday() < 5

def sked_key(day_str: str) -> str:
    return "weekday" if is_weekday(day_str) else "weekend"

def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default

def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── İhlal Tespiti ───────────────────────────────────────────────────────────

def detect_sefer_iptali(logs, schedule, day_str, yon) -> list:
    """Tarife ±PENCERE_DK dk içinde hatta hiç araç yoksa → SEFERİPTAL."""
    seferler   = schedule.get(yon, {}).get(sked_key(day_str), [])
    violations = []

    for saat in seferler:
        t0 = sched_dt(day_str, saat) - timedelta(minutes=PENCERE_DK)
        t1 = sched_dt(day_str, saat) + timedelta(minutes=PENCERE_DK)

        pencere = [lg for lg in logs if t0 <= parse_ts(lg["timestamp"]) <= t1]
        if not pencere:
            continue  # O pencerede log yok, sistem kapalıydı

        araclar = set()
        for lg in pencere:
            for b in lg.get("buses_on_route", []):
                araclar.add(b.get("OtobusId"))

        if not araclar:
            violations.append({
                "tip":      "SEFERİPTAL",
                "tarih":    day_str,
                "saat":     saat,
                "yon":      yon,
                "aciklama": (
                    f"{saat} tarifeli sefer için {PENCERE_DK} dk boyunca "
                    f"hatta hiç araç görülmedi."
                ),
                "araclar":  [],
                "severity": "critical",
            })

    return violations


def detect_erken_kalkis(logs, schedule, day_str, yon) -> list:
    """
    Kalkış durağına yakın araç (KalanDurakSayisi ≤ eşik), tarife saatinden
    ERKEN_DK dk önce görülüyorsa → ERKENKALKIŞ.
    """
    seferler   = schedule.get(yon, {}).get(sked_key(day_str), [])
    yaklasan_k = "urla_approaching" if yon == "urla_faltay" else "faltay_approaching"
    violations = []

    for saat in seferler:
        t_sefer  = sched_dt(day_str, saat)
        t_izle0  = t_sefer - timedelta(minutes=15)
        t_izle1  = t_sefer - timedelta(minutes=ERKEN_DK)

        # Bu pencerede kalkış noktasına yakın olan araçlar
        erken = {}
        for lg in logs:
            ts = parse_ts(lg["timestamp"])
            if not (t_izle0 <= ts <= t_izle1):
                continue
            for a in lg.get(yaklasan_k, []):
                if a.get("KalanDurakSayisi", 99) <= SON_DURAK_KALAN:
                    oid = a.get("OtobusId")
                    if oid and oid not in erken:
                        erken[oid] = ts

        for oid, ts in erken.items():
            fark = (t_sefer - ts).total_seconds() / 60
            if fark >= ERKEN_DK:
                violations.append({
                    "tip":      "ERKENKALKIŞ",
                    "tarih":    day_str,
                    "saat":     saat,
                    "yon":      yon,
                    "arac_id":  oid,
                    "aciklama": (
                        f"Araç #{oid}, {saat} tarifeli seferden "
                        f"~{int(fark)} dk önce kalkış noktasını terk etti."
                    ),
                    "araclar":  [oid],
                    "severity": "warning",
                })

    return violations


def detect_sefer_birlestirme(logs, schedule, day_str, yon) -> list:
    """
    Araç son durağa yakın BIRLES_DK dk+ bekliyorsa → SEFERBİRLEŞ.

    Trafik ayrımı:
      - Aynı log kaydında trafik_kume >= TRAFIK_KUME_MIN ise trafik olarak işaretle.
      - Bekleyen araç eşiği aşıyor ama trafik_kume yüksekse severity düşürülür.
    """
    yaklasan_k = "faltay_approaching" if yon == "urla_faltay" else "urla_approaching"
    seferler   = schedule.get(yon, {}).get(sked_key(day_str), [])
    violations = []

    # arac_kayitlar[oid] = [(ts, trafik_kume), ...]  — son durağa yakın gözlemler
    arac_kayitlar: dict[int, list] = defaultdict(list)

    for lg in logs:
        ts = parse_ts(lg["timestamp"])
        # Trafik verisi: son durağa yakın bölgedeki trafik kümesi
        lg_trafik = {b["OtobusId"]: b.get("trafik_kume", 0)
                     for b in lg.get("buses_on_route", [])}

        for a in lg.get(yaklasan_k, []):
            if a.get("KalanDurakSayisi", 99) <= SON_DURAK_KALAN:
                oid = a.get("OtobusId")
                if oid:
                    tkume = lg_trafik.get(oid, 0)
                    arac_kayitlar[oid].append((ts, tkume))

    for oid, kayitlar in arac_kayitlar.items():
        if len(kayitlar) < 2:
            continue
        kayitlar.sort(key=lambda x: x[0])
        ilk, son     = kayitlar[0][0], kayitlar[-1][0]
        bekleme_dk   = (son - ilk).total_seconds() / 60
        ort_trafik   = sum(k[1] for k in kayitlar) / len(kayitlar)

        if bekleme_dk < BIRLES_DK:
            continue

        # Hangi tarife seferleri bu pencereye denk geliyor?
        ilgili = [
            s for s in seferler
            if ilk <= sched_dt(day_str, s) <= son + timedelta(minutes=10)
        ]

        # Trafik yoksa → yüksek şüphe; trafik varsa → orta şüphe
        trafik_var = ort_trafik >= TRAFIK_KUME_MIN
        severity   = "high" if not trafik_var else "medium"
        trafik_notu = (
            "" if not trafik_var
            else f" (Not: Bu sürede ortalama {ort_trafik:.1f} araçlık trafik kümesi tespit edildi — "
                 "gerçek trafik olabilir, yine de kayıt altına alındı.)"
        )

        violations.append({
            "tip":               "SEFERBİRLEŞ",
            "tarih":             day_str,
            "saat":              son.strftime("%H:%M"),
            "yon":               yon,
            "arac_id":           oid,
            "aciklama": (
                f"Araç #{oid}, {ilk.strftime('%H:%M')}–{son.strftime('%H:%M')} "
                f"({int(bekleme_dk)} dk) son durağa yakın bekledi."
                f"{' Atlanan seferler: ' + ', '.join(ilgili) if ilgili else ''}"
                f"{trafik_notu}"
            ),
            "araclar":           [oid],
            "birlesilen_seferler": ilgili,
            "bekleme_dakika":    int(bekleme_dk),
            "trafik_skoru":      round(ort_trafik, 1),
            "severity":          severity,
        })

    return violations


# ─── Haftalık Örüntü Analizi ─────────────────────────────────────────────────

def update_patterns(violations: list):
    """
    Son 30 güne ait ihlalleri analiz ederek örüntü dosyasını günceller:
      - En riskli saatler (hangi saat diliminde en çok ihlal)
      - En çok ihlal yapan araçlar (OtobusId bazlı)
      - İhlal türü dağılımı
      - Gün-of-week dağılımı
    """
    saatler: dict[str, int]         = defaultdict(int)
    araclar: dict[int, dict]        = defaultdict(lambda: defaultdict(int))
    tipler:  dict[str, int]         = defaultdict(int)
    gun_of_week: dict[str, int]     = defaultdict(int)
    yon_dagilim: dict[str, int]     = defaultdict(int)

    for v in violations:
        saatler[v["saat"]] += 1
        tipler[v["tip"]]   += 1
        try:
            d = date.fromisoformat(v["tarih"])
            gun_of_week[d.strftime("%A")] += 1
        except Exception:
            pass
        yon_dagilim[v.get("yon", "?")] += 1
        for oid in v.get("araclar", []):
            araclar[oid][v["tip"]] += 1
            araclar[oid]["toplam"] += 1

    # En riskli 10 saat
    top_saatler = sorted(saatler.items(), key=lambda x: -x[1])[:10]

    # En çok ihlal yapan 10 araç
    top_araclar = sorted(
        [{"oid": oid, **dict(counts)} for oid, counts in araclar.items()],
        key=lambda x: -x.get("toplam", 0)
    )[:10]

    patterns = {
        "guncelleme":    datetime.now(TR_TZ).isoformat(),
        "toplam_ihlal":  len(violations),
        "tip_dagilimi":  dict(tipler),
        "yon_dagilimi":  dict(yon_dagilim),
        "gun_dagilimi":  dict(gun_of_week),
        "riskli_saatler": [{"saat": s, "ihlal": n} for s, n in top_saatler],
        "surekli_ihlalci_araclar": top_araclar,
    }

    save_json(PAT_FILE, patterns)
    print(f"[ÖRÜNTÜ] Güncellendi: {len(top_saatler)} riskli saat, "
          f"{len(top_araclar)} tekrarlayan araç.")


# ─── Şikayet Yanıtı Kategorilendirme ─────────────────────────────────────────

def kategorize_yanit(yanit_metni: str) -> str:
    """Gelen ESHOT yanıtını anahtar kelimelerle kategorize eder."""
    t = yanit_metni.lower()
    for kategori, kelimeler in BAHANE_KEYS.items():
        if any(k in t for k in kelimeler):
            return kategori
    return "belirsiz"


def process_complaints(violations: list):
    """
    complaints.json içindeki şikayet yanıtlarını okur, kategorize eder,
    ilgili ihlal kaydına bağlar ve özet üretir.
    """
    complaints = load_json(COMP_FILE, [])
    if not complaints:
        return

    # İhlal ID haritası: tip|tarih|saat|yon → violation
    viol_map = {}
    for v in violations:
        key = f"{v['tip']}|{v['tarih']}|{v['saat']}|{v['yon']}"
        viol_map[key] = v

    bahane_sayac: dict[str, int] = defaultdict(int)

    for c in complaints:
        if "kategori" not in c:
            c["kategori"] = kategorize_yanit(c.get("yanit_metni", ""))
        bahane_sayac[c["kategori"]] += 1

        # İlgili ihlalin üzerine şikayet sonucunu yaz
        key = f"{c.get('ihlal_tip')}|{c.get('ihlal_tarih')}|{c.get('ihlal_saat')}|{c.get('ihlal_yon')}"
        if key in viol_map:
            viol_map[key]["sikayet_sonucu"]  = c["kategori"]
            viol_map[key]["sikayet_referans"] = c.get("referans_no", "")

    save_json(COMP_FILE, complaints)
    print(f"[ŞİKAYET] Bahane dağılımı: {dict(bahane_sayac)}")


# ─── Ana Akış ────────────────────────────────────────────────────────────────

def analyze_day(day_str: str, schedule: dict) -> list:
    logs = load_day_logs(day_str)
    if not logs:
        return []

    violations = []
    for yon in ["urla_faltay", "faltay_urla"]:
        violations += detect_sefer_iptali(logs, schedule, day_str, yon)
        violations += detect_erken_kalkis(logs, schedule, day_str, yon)
        violations += detect_sefer_birlestirme(logs, schedule, day_str, yon)

    print(f"[ANALİZ] {day_str}: {len(violations)} ihlal tespit edildi.")
    return violations


def main():
    now       = datetime.now(TR_TZ)
    today     = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    schedule  = load_schedule()

    new_viols = []
    for day in [yesterday, today]:
        new_viols += analyze_day(day, schedule)

    # Mevcut ihlalleri yükle
    existing = load_json(VIOL_FILE, [])

    def vkey(v):
        return f"{v['tip']}|{v['tarih']}|{v['saat']}|{v['yon']}|{v.get('arac_id','_')}"

    existing_keys = {vkey(v) for v in existing}
    yeni = [v for v in new_viols if vkey(v) not in existing_keys]

    # 30 günden eskiyi temizle
    cutoff   = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    combined = [v for v in existing + yeni if v["tarih"] >= cutoff]
    combined.sort(key=lambda v: (v["tarih"], v["saat"]), reverse=True)

    # Şikayet yanıtlarını işle
    process_complaints(combined)

    save_json(VIOL_FILE, combined)
    print(f"[TAMAM] violations.json: {len(combined)} kayıt ({len(yeni)} yeni).")

    # Haftalık örüntü güncellemesi
    update_patterns(combined)


if __name__ == "__main__":
    main()
