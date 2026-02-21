import json
import sqlite3
import requests
import schedule
import time
from datetime import datetime
from pathlib import Path

BASE_URL = "https://www.vision-environnement.com/live/image/webcam/{key}.JPG"
YT_THUMB = "https://img.youtube.com/vi/{yt_id}/maxresdefault.jpg"

# "url" overrides BASE_URL when the camera uses a different endpoint.
# Saint-Barth cameras stream via YouTube Live → thumbnail updated periodically.
# Baie_Saint-Jean is currently offline (server HTTP 500).
CAMERAS = [
    {"name": "Saint-Jean_Plaine",   "key": "st-barth-saint-jean",       "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="028GpBNaiJs")},
    {"name": "Aeroport",            "key": "st-barth-avions-decollage",  "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="3PQNnVqrJEw")},
    {"name": "Port_Gustavia",       "key": "st-barth-port-gustavia",     "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="zaXzE6ZJAE8")},
    {"name": "Col_Tourmente",       "key": "st-barth-col-tourmente",     "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="15pVqwQb7A0")},
    {"name": "Rade_Gustavia",       "key": "st-barth-rade-gustavia",     "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="jBEtwmuJPLA")},
    {"name": "Flamand_Beach",       "key": "st-barth-flamand-beach",     "island": "Saint-Barth",  "url": YT_THUMB.format(yt_id="DATTZ-F82Zk")},
    # Baie_Saint-Jean : offline (serveur HTTP 500) — retirée temporairement
    {"name": "Anses_Arlet",         "key": "ansesdarlet",                "island": "Martinique",   "url": "https://s1.vision-environnement.com/live/modules/timelapse/captureptz/ansesdarlet.jpg"},
    {"name": "Maho_Beach",          "key": "mahobeach",                  "island": "Saint-Martin"},
]

OUTPUT_DIR = Path("./captures")
DB_PATH    = Path("./sargassum_data.db")


def _get_db_conn():
    """Ouvre la connexion SQLite et crée la table webcam_captures si absente."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS webcam_captures (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT    NOT NULL,
            island      TEXT    NOT NULL,
            camera_name TEXT    NOT NULL,
            camera_key  TEXT,
            file_path   TEXT,
            success     INTEGER NOT NULL,
            http_status INTEGER,
            file_size   INTEGER,
            raw_metadata TEXT
        )
    """)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.commit()
    return conn

def get_url(camera):
    return camera.get("url") or BASE_URL.format(key=camera["key"])

def capture_image(camera, db_conn=None):
    url = get_url(camera)
    now = datetime.now()
    ts  = now.strftime("%Y-%m-%dT%H:%M:%S")
    save_dir = OUTPUT_DIR / camera["island"] / camera["name"] / now.strftime("%Y-%m-%d")
    save_dir.mkdir(parents=True, exist_ok=True)
    filename = save_dir / f"{now.strftime('%Y-%m-%d_%H-%M')}.jpg"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        if r.status_code == 200 and len(r.content) > 5000:
            filename.write_bytes(r.content)
            print(f"  ✅ {camera['island']} | {camera['name']}")
            if db_conn:
                db_conn.execute(
                    """INSERT INTO webcam_captures
                       (captured_at, island, camera_name, camera_key,
                        file_path, success, http_status, file_size, raw_metadata)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (ts, camera["island"], camera["name"], camera.get("key"),
                     str(filename.resolve()), 1, r.status_code, len(r.content),
                     json.dumps({"url": url})),
                )
                db_conn.commit()
            return True
        else:
            print(f"  ⚠️  {camera['name']} → HTTP {r.status_code}")
            if db_conn:
                db_conn.execute(
                    """INSERT INTO webcam_captures
                       (captured_at, island, camera_name, camera_key,
                        file_path, success, http_status, file_size, raw_metadata)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (ts, camera["island"], camera["name"], camera.get("key"),
                     None, 0, r.status_code, len(r.content),
                     json.dumps({"url": url, "reason": "status_or_size"})),
                )
                db_conn.commit()
            return False
    except Exception as e:
        print(f"  ❌ {camera['name']} → {e}")
        if db_conn:
            db_conn.execute(
                """INSERT INTO webcam_captures
                   (captured_at, island, camera_name, camera_key,
                    file_path, success, http_status, file_size, raw_metadata)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (ts, camera["island"], camera["name"], camera.get("key"),
                 None, 0, None, None,
                 json.dumps({"url": url, "error": str(e)})),
            )
            db_conn.commit()
        return False

def capture_all(db_conn=None):
    print(f"\n{'='*40}\n📸 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*40}")
    ok = sum(capture_image(c, db_conn=db_conn) for c in CAMERAS)
    print(f"\n✅ {ok}/{len(CAMERAS)} caméras OK")

def test_cameras():
    print("\n🔍 Test des URLs...\n")
    for cam in CAMERAS:
        url = get_url(cam)
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            ok = r.status_code == 200 and len(r.content) > 5000
            print(f"  {'✅' if ok else '❌'} | {cam['island']} | {cam['name']} ({len(r.content)} bytes)")
        except Exception as e:
            print(f"  ❌ | {cam['name']} → {e}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test_cameras()
    else:
        once = "--once" in sys.argv
        print("🌊 Sargassum Webcam Capture System" + (" (--once)" if once else ""))
        db_conn = _get_db_conn()
        try:
            capture_all(db_conn=db_conn)
            if not once:
                schedule.every().hour.do(capture_all, db_conn=db_conn)
                while True:
                    schedule.run_pending()
                    time.sleep(60)
        finally:
            db_conn.close()
