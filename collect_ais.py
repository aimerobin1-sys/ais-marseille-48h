import asyncio
import csv
import json
import os
import time
from datetime import datetime, timezone
import requests
import websockets

# --- CONFIGURATION ---
API_KEY = os.getenv("AISSTREAM_API_KEY", "").strip()
RUN_NUMBER = os.getenv("RUN_NUMBER", "1")
DURATION_SECONDS = 20400  # 5h40 (marge de sécurité sous la limite de 6h)
OUTPUT_FILE = f"data_marseille_bloc_{RUN_NUMBER}.csv"

# Zone englobant Fos-sur-Mer (Bassins Ouest) et Marseille (Bassins Est)
BBOX_AISSTREAM = [[[43.20, 4.80], [43.45, 5.45]]]
BBOX_OPENWATERS_WS = [[43.20, 4.80], [43.45, 5.45]]
BBOX_OPENWATERS_REST = "43.20,4.80,43.45,5.45"

CSV_HEADERS = [
    "timestamp_utc", "source", "msg_type", "mmsi", "ship_name",
    "ship_type", "lat", "lon", "sog", "cog", "heading",
    "nav_status", "destination", "imo", "draught"
]

# Cache mémoire pour associer nom/type/destination aux positions
vessel_meta = {}


def is_target_ship(ship_type):
    """Conserve les navires de passagers/ferries (60-69) et cargo/ro-ro/conteneurs (70-79), ou type inconnu (None/0)."""
    if ship_type is None or ship_type == 0:
        return True
    try:
        st = int(ship_type)
        return 60 <= st <= 79
    except (ValueError, TypeError):
        return True


def init_csv():
    file_exists = os.path.isfile(OUTPUT_FILE)
    f = open(OUTPUT_FILE, mode="a", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
    if not file_exists:
        writer.writeheader()
    return f, writer


def fetch_openwaters_snapshot(writer, csv_file):
    """Récupère l'état initial des navires déjà présents dans les bassins via REST."""
    url = f"https://ais.openwaters.io/v1/vessels?bbox={BBOX_OPENWATERS_REST}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        features = resp.json().get("features", [])
        now_iso = datetime.now(timezone.utc).isoformat()
        count = 0
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [None, None])
            mmsi = props.get("mmsi")
            ship_type = props.get("ship_type")
            if not mmsi or not is_target_ship(ship_type):
                continue

            vessel_meta[mmsi] = {
                "ship_name": props.get("name", "").strip(),
                "ship_type": ship_type,
                "destination": props.get("destination", "").strip(),
                "imo": props.get("imo", ""),
                "draught": props.get("draught", "")
            }
            writer.writerow({
                "timestamp_utc": props.get("last_seen", now_iso),
                "source": "openwaters_snapshot",
                "msg_type": "Snapshot",
                "mmsi": mmsi,
                "ship_name": vessel_meta[mmsi]["ship_name"],
                "ship_type": ship_type,
                "lat": coords[1],
                "lon": coords[0],
                "sog": props.get("sog"),
                "cog": props.get("cog"),
                "heading": props.get("heading"),
                "nav_status": props.get("nav_status"),
                "destination": vessel_meta[mmsi]["destination"],
                "imo": vessel_meta[mmsi]["imo"],
                "draught": vessel_meta[mmsi]["draught"],
            })
            count += 1
        csv_file.flush()
        print(f"[Snapshot OpenWaters] {count} navires enregistrés.")
    except Exception as e:
        print(f"[Erreur Snapshot OpenWaters] {e}")


async def listen_aisstream(end_time, writer, csv_file):
    """Écoute temps réel aisstream.io avec reconnexion automatique."""
    if not API_KEY:
        print("[AISStream] Clé API manquante, flux ignoré.")
        return

    uri = "wss://stream.aisstream.io/v0/stream"
    sub_msg = {
        "APIKey": API_KEY,
        "BoundingBoxes": BBOX_AISSTREAM,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData", "StandardClassBPositionReport"]
    }

    while time.time() < end_time:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub_msg))
                print("[AISStream] Connecté.")
                while time.time() < end_time:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        break
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(60, remaining))
                    data = json.loads(raw)
                    msg_type = data.get("MessageType")
                    meta = data.get("MetaData", {})
                    mmsi = meta.get("MMSI")
                    if not mmsi:
                        continue

                    now_iso = datetime.now(timezone.utc).isoformat()
                    info = vessel_meta.setdefault(mmsi, {})
                    if meta.get("ShipName"):
                        info["ship_name"] = meta["ShipName"].strip()

                    if msg_type == "ShipStaticData":
                        static = data.get("Message", {}).get("ShipStaticData", {})
                        st = static.get("Type")
                        info["ship_type"] = st
                        info["destination"] = static.get("Destination", "").strip()
                        info["imo"] = static.get("ImoNumber", "")
                        info["draught"] = static.get("MaximumStaticDraught", "")
                        if not is_target_ship(st):
                            continue
                        writer.writerow({
                            "timestamp_utc": now_iso,
                            "source": "aisstream",
                            "msg_type": "ShipStaticData",
                            "mmsi": mmsi,
                            "ship_name": info.get("ship_name", ""),
                            "ship_type": st,
                            "lat": meta.get("latitude"),
                            "lon": meta.get("longitude"),
                            "destination": info["destination"],
                            "imo": info["imo"],
                            "draught": info["draught"],
                        })

                    elif msg_type in ("PositionReport", "StandardClassBPositionReport"):
                        st = info.get("ship_type")
                        if not is_target_ship(st):
                            continue
                        pos = data.get("Message", {}).get(msg_type, {})
                        writer.writerow({
                            "timestamp_utc": now_iso,
                            "source": "aisstream",
                            "msg_type": msg_type,
                            "mmsi": mmsi,
                            "ship_name": info.get("ship_name", ""),
                            "ship_type": st,
                            "lat": pos.get("Latitude", meta.get("latitude")),
                            "lon": pos.get("Longitude", meta.get("longitude")),
                            "sog": pos.get("Sog"),
                            "cog": pos.get("Cog"),
                            "heading": pos.get("TrueHeading"),
                            "nav_status": pos.get("NavigationalStatus"),
                            "destination": info.get("destination", ""),
                            "imo": info.get("imo", ""),
                            "draught": info.get("draught", ""),
                        })
                    csv_file.flush()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[AISStream] Reconnexion après erreur : {e}")
            await asyncio.sleep(5)


async def listen_openwaters_ws(end_time, writer, csv_file):
    """Écoute temps réel secondaire Open Waters WebSocket."""
    uri = "wss://ais.openwaters.io/v1/stream"
    sub_msg = {"type": "subscribe", "bbox": BBOX_OPENWATERS_WS, "snapshot": False}

    while time.time() < end_time:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub_msg))
                print("[OpenWaters WS] Connecté.")
                while time.time() < end_time:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        break
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(60, remaining))
                    data = json.loads(raw)
                    props = data.get("properties", data)
                    geom = data.get("geometry", {})
                    coords = geom.get("coordinates", [None, None])
                    mmsi = props.get("mmsi")
                    if not mmsi:
                        continue

                    info = vessel_meta.setdefault(mmsi, {})
                    st = props.get("ship_type") or info.get("ship_type")
                    if not is_target_ship(st):
                        continue

                    if props.get("name"):
                        info["ship_name"] = props["name"].strip()
                    if props.get("ship_type"):
                        info["ship_type"] = props["ship_type"]
                    if props.get("destination"):
                        info["destination"] = props["destination"].strip()

                    writer.writerow({
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "source": "openwaters_ws",
                        "msg_type": data.get("type", "Update"),
                        "mmsi": mmsi,
                        "ship_name": info.get("ship_name", ""),
                        "ship_type": st,
                        "lat": coords[1] if len(coords) > 1 else props.get("lat"),
                        "lon": coords[0] if len(coords) > 0 else props.get("lon"),
                        "sog": props.get("sog"),
                        "cog": props.get("cog"),
                        "heading": props.get("heading"),
                        "nav_status": props.get("nav_status"),
                        "destination": info.get("destination", ""),
                        "imo": info.get("imo", ""),
                        "draught": info.get("draught", ""),
                    })
                    csv_file.flush()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"[OpenWaters WS] Reconnexion après erreur : {e}")
            await asyncio.sleep(5)


async def main():
    print(f"Démarrage acquisition Bloc {RUN_NUMBER} pour {DURATION_SECONDS}s...")
    csv_file, writer = init_csv()
    fetch_openwaters_snapshot(writer, csv_file)

    end_time = time.time() + DURATION_SECONDS
    await asyncio.gather(
        listen_aisstream(end_time, writer, csv_file),
        listen_openwaters_ws(end_time, writer, csv_file)
    )
    csv_file.close()
    print(f"Acquisition Bloc {RUN_NUMBER} terminée. Fichier : {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
