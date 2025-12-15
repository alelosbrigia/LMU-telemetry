#!/usr/bin/env python3
import sys, re
import numpy as np
import pandas as pd
import duckdb

EXCLUDE = {"channelsList", "eventsList", "metadata"}

# Gruppi logici (usati dalla GUI)
GROUPS = {
    "Driver": ["throttle", "brake", "clutch", "steer", "ffb"],
    "Powertrain": ["engine", "rpm", "gear", "boost", "turbo", "regen", "energy", "fuel", "soc"],
    "Dynamics": ["speed", "yaw", "g_", "accel", "acceleration"],
    "AeroSusp": ["rideheight", "susp", "deflection", "wing", "flap", "downforce", "drag"],
    "Tyres": ["tyre", "tire", "pressure", "rubber", "carcass", "rim", "wear", "compound", "temp"],
    "Environment": ["ambient", "track_temperature", "wind", "wetness", "cloud", "track temperature", "ambient temperature"],
    "States": ["abs", "tc", "tccut", "map", "bias", "flag", "state", "status", "pits", "limiter", "headlights"]
}

# Ruote
WHEEL_MAP = {"value1": "FL", "value2": "FR", "value3": "RL", "value4": "RR"}

def safe(name: str) -> str:
    return re.sub(r"[^\w]+", "_", name).strip("_")

def normalize_name(raw: str) -> str:
    s = raw

    # Centre/Center + Left/Right
    s = re.sub(r"(?:_|\\b)(Centre|Center)(?:\\b|_)", "_C_", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:_|\\b)Left(?:\\b|_)", "_L_", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:_|\\b)Right(?:\\b|_)", "_R_", s, flags=re.IGNORECASE)

    # Inner/Middle/Outer
    s = re.sub(r"(?:_|\\b)Inner(?:\\b|_)", "_I_", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:_|\\b)Middle(?:\\b|_)", "_M_", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:_|\\b)Outer(?:\\b|_)", "_O_", s, flags=re.IGNORECASE)

    s = re.sub(r"_+", "_", s).strip("_")
    return s

def is_step(name: str) -> bool:
    n = name.lower()
    # canali discreti/stati (hold-last-value)
    return any(k in n for k in [
        "gear", "lap", "flag", "state", "status", "active", "activated",
        "abs", "tc", "tccut", "map", "pits", "limiter", "headlights", "finish"
    ])

def guess_units_decimals(ch: str):
    n = ch.lower()

    # Temperature
    if "temp" in n or "temperature" in n:
        return ("degC", 1)

    # Pressure (tyres/boost)
    if "pressure" in n or "boost" in n or "turbo" in n:
        return ("bar", 3)

    # Distances / Heights / Suspension
    if "rideheight" in n or "ride_height" in n or ("height" in n and "headlights" not in n) or "susp" in n or "deflection" in n:
        return ("mm", 1)

    # Speed
    if "speed" in n:
        return ("km/h", 1)

    # RPM
    if "rpm" in n:
        return ("rpm", 0)

    # Angles / steering
    if "angle" in n or "steer" in n:
        return ("deg", 1)

    # Accelerations / G
    if "g_force" in n or "accel" in n or "acceleration" in n:
        return ("g", 3)

    # Default
    return ("", 2)

def main():
    if len(sys.argv) < 4:
        print("Uso: python duckdb_to_motec_unified.py file.duckdb output.csv Driver=100 Tyres=20 ...")
        sys.exit(1)

    db = sys.argv[1]
    out_csv = sys.argv[2]

    # Argomenti: Group=Hz
    group_hz = {}
    for g in sys.argv[3:]:
        k, v = g.split("=")
        group_hz[k] = int(v)

    # Master Hz = max
    master_hz = max(group_hz.values())
    dt = 1.0 / master_hz

    con = duckdb.connect(db, read_only=True)

    tables = [
        r[0] for r in con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='main' AND table_type='BASE TABLE'
        """).fetchall()
        if r[0] not in EXCLUDE
    ]

    # Durata sessione (preferisci GPS Time)
    session_end = 0.0
    if "GPS Time" in tables:
        g = con.execute('SELECT value FROM "GPS Time"').fetchdf()
        if len(g):
            session_end = float(g["value"].iloc[-1] - g["value"].iloc[0])

    if session_end <= 0:
        # fallback: usa max( (n-1)/master_hz ) — grezzo ma evita 0
        for t in tables:
            n = int(con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0])
            if n > 1:
                session_end = max(session_end, (n - 1) / master_hz)

    master_time = np.arange(0.0, session_end + dt, dt, dtype=float)
    data = {"Time": master_time}

    # Per evitare duplicati se due gruppi matchano lo stesso canale/tabella
    added_cols = set()

    for group, hz in group_hz.items():
        patterns = GROUPS.get(group, [])

        for t in tables:
            tl = t.lower()
            if not any(p in tl for p in patterns):
                continue

            df = con.execute(f'SELECT * FROM "{t}"').fetchdf()
            if df.empty:
                continue

            # timeline "gruppo"
            t_ch = np.arange(0.0, len(df) / hz, 1.0 / hz, dtype=float)
            if len(t_ch) > len(df):
                t_ch = t_ch[:len(df)]

            for c in df.columns:
                y = pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float)
                if np.isfinite(y).sum() < 5:
                    continue

                # value1..4 -> FL/FR/RL/RR
                suffix = WHEEL_MAP.get(str(c).lower(), str(c))
                raw_name = (t if len(df.columns) == 1 else f"{t}_{suffix}")
                name = safe(normalize_name(raw_name))

                if name in added_cols:
                    continue

                if is_step(name):
                    idx = np.searchsorted(t_ch, master_time, side="right") - 1
                    idx[idx < 0] = 0
                    idx[idx >= len(y)] = len(y) - 1
                    data[name] = y[idx]
                    added_cols.add(name)
                else:
                    m = np.isfinite(y)
                    if m.sum() < 2:
                        continue
                    data[name] = np.interp(master_time, t_ch[m], y[m], left=np.nan, right=np.nan)
                    added_cols.add(name)

    con.close()

    out = pd.DataFrame(data).ffill().fillna(0)

    # Beacon + LapTime
    if "Lap" in out.columns:
        lap = out["Lap"]
        out["Beacon"] = (lap.diff().fillna(0) != 0).astype(int)
        out["LapTime"] = out["Time"] - out.groupby(lap)["Time"].transform("min")
    else:
        out["Beacon"] = 0
        out["LapTime"] = out["Time"]

    # Ordine colonne
    cols = ["Time", "Beacon", "LapTime"] + [c for c in out.columns if c not in ("Time", "Beacon", "LapTime")]
    out[cols].to_csv(out_csv, index=False, float_format="%.6f")
    print("OK ->", out_csv)

    # META: units + decimals
    meta_rows = []
    for c in cols:
        if c in ("Time", "Beacon", "LapTime"):
            continue
        u, d = guess_units_decimals(c)
        meta_rows.append((c, u, d))

    meta_path = out_csv.replace(".csv", ".meta.csv")
    pd.DataFrame(meta_rows, columns=["channel", "units", "decimals"]).to_csv(meta_path, index=False)
    print("META ->", meta_path)

if __name__ == "__main__":
    main()
