"""
ESHOT 684 Hat İzleme - Veri Toplayıcı
GitHub Actions tarafından her 5 dakikada bir çalıştırılır.

Özellikler:
  - GPS lag tespiti (araç hareket etmiş ama konum donmuş)
  - Araç hız hesabı (koordinat delta / zaman)
  - Trafik kümeleme (aynı bölgede yavaşlayan araç sayısı)
"""

import requests
import json
import os
import math
from datetime import datetime, timezone, timedelta

HAT_ID   = 684
API_BASE = "https://openapi.izmir.bel.tr/api/iztek"
TR_TZ    = timezone(timedelta(hours=3))

DURAK_URLA   = 50605
DURAK_FALTAY = 51916

GPS_LAG_METRE    = 30    # Bu kadar az hareket → konum donmuş
TRAFIK_RADIUS_KM = 0.8   # Bu yarıçap içinde yavaş araç kümesi
TRAFIK_MIN_ARAC  = 2     # Trafik sayılması için minimum araç
HIZ_YAVAS_KMPH   = 10    # Bu altı = yavaş/durmuş


def haversine_metre(lat1, lon1, lat2, lon2) -> float:
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a  = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def hiz_kmh(metre, saniye) -> float:
    return (metre / saniye) * 3.6 if saniye > 0 else 0.0


def load_prev() -> dict | None:
    try:
        with open("data/latest.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def enrich_buses(current: list, prev: dict | None, interval_sn=300) -> list:
    """
    Her araç için önceki snapshot ile karşılaştırır:
      hareket_metre  → kaç metre yer değiştirdi
      hiz_kmh        → tahmini anlık hız
      gps_lag        → konum donmuş mu (hatta görünüyor ama hiç hareket yok)
      trafik_kume    → yakınında kaç araç daha yavaş (gerçek trafik sinyali)

    Trafik kümesi analizi: Aynı anda birden fazla araç aynı bölgede
    yavaşladıysa → muhtemelen gerçek trafik.
    Tek araç yavaşladıysa → birleştirme / durma şüphesi.
    """
    prev_map = {b["OtobusId"]: b for b in (prev or {}).get("buses_on_route", [])}
    yavas = []  # (lat, lon, oid) — hız eşiğin altında

    enriched = []
    for b in current:
        oid = b.get("OtobusId")
        lat = b.get("KoorX", 0)
        lon = b.get("KoorY", 0)
        e   = dict(b)
        e.update({
            "hareket_metre": None,
            "hiz_kmh":       None,
            "gps_lag":       False,
            "trafik_kume":   0,
        })

        if oid in prev_map:
            pb   = prev_map[oid]
            dist = haversine_metre(lat, lon, pb.get("KoorX", 0), pb.get("KoorY", 0))
            spd  = hiz_kmh(dist, interval_sn)
            e["hareket_metre"] = round(dist, 1)
            e["hiz_kmh"]       = round(spd, 1)
            # GPS lag: hatta görünüyor ama 5 dk'da 30 m'den az hareket
            e["gps_lag"]       = dist < GPS_LAG_METRE
            if spd < HIZ_YAVAS_KMPH:
                yavas.append((lat, lon, oid))

        enriched.append(e)

    # Trafik kümesi: yakınında başka kaç yavaş araç var?
    for e in enriched:
        lat, lon = e.get("KoorX", 0), e.get("KoorY", 0)
        e["trafik_kume"] = sum(
            1 for (vl, vn, vi) in yavas
            if vi != e["OtobusId"]
            and haversine_metre(lat, lon, vl, vn) < TRAFIK_RADIUS_KM * 1000
        )

    return enriched


def api_get(path) -> list | dict:
    try:
        r = requests.get(f"{API_BASE}/{path}", timeout=15)
        if r.status_code == 204:
            return []
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[HATA] {path}: {e}")
        return []


def save(buses, urla, faltay):
    now   = datetime.now(TR_TZ)
    d_str = now.strftime("%Y-%m-%d")
    t_str = now.strftime("%H-%M")

    trafik_alanlari = [
        {
            "lat":        b["KoorX"],
            "lon":        b["KoorY"],
            "arac_sayisi": b["trafik_kume"] + 1,
        }
        for b in buses if b.get("trafik_kume", 0) >= TRAFIK_MIN_ARAC
    ]

    snap = {
        "timestamp":          now.isoformat(),
        "timestamp_unix":     int(now.timestamp()),
        "buses_on_route":     buses,
        "urla_approaching":   urla,
        "faltay_approaching": faltay,
        "trafik_alanlari":    trafik_alanlari,
        "gps_lag_araclar":    [b["OtobusId"] for b in buses if b.get("gps_lag")],
    }

    os.makedirs(f"data/logs/{d_str}", exist_ok=True)
    with open(f"data/logs/{d_str}/{t_str}.json", "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)
    with open("data/latest.json", "w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

    print(
        f"[OK] {now.strftime('%H:%M')} | {len(buses)} araç | "
        f"GPS lag: {len(snap['gps_lag_araclar'])} | "
        f"Trafik: {len(trafik_alanlari)} nokta"
    )


def main():
    print(f"[ESHOT-WATCH] {datetime.now(TR_TZ).isoformat()}")

    prev   = load_prev()
    raw    = api_get(f"hatotobuskonumlari/{HAT_ID}")
    buses  = enrich_buses(
        raw.get("HatOtobusKonumlari", raw) if isinstance(raw, dict) else raw,
        prev
    )
    urla   = api_get(f"hattinyaklasanotobusleri/{HAT_ID}/{DURAK_URLA}")
    faltay = api_get(f"hattinyaklasanotobusleri/{HAT_ID}/{DURAK_FALTAY}")

    save(
        buses,
        urla if isinstance(urla, list) else [],
        faltay if isinstance(faltay, list) else [],
    )


if __name__ == "__main__":
    main()
