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
TOKEN_OVERRIDES = {
    "gps": "GPS",
    "rpm": "RPM",
    "tc": "TC",
    "abs": "ABS",
    "ers": "ERS",
    "mgu": "MGU",
    "g": "G",
}

MOTEC_BASE_NAMES = {
    "speed": "Speed",
    "vehicle speed": "Speed",
    "engine rpm": "Engine RPM",
    "rpm": "Engine RPM",
    "throttle": "Throttle Position",
    "throttle position": "Throttle Position",
    "brake": "Brake Position",
    "brake pedal": "Brake Position",
    "brake pressure": "Brake Pressure",
    "brake bias": "Brake Bias",
    "clutch": "Clutch Position",
    "gear": "Gear",
    "steer": "Steering Angle",
    "steering": "Steering Angle",
    "steering angle": "Steering Angle",
    "steering torque": "Steering Torque",
    "ffb": "Steering Torque",
    "yaw": "Yaw Rate",
    "yaw rate": "Yaw Rate",
    "g long": "Long Accel",
    "g lat": "Lat Accel",
    "g vert": "Vert Accel",
    "long accel": "Long Accel",
    "lat accel": "Lat Accel",
    "vert accel": "Vert Accel",
    "rideheight": "Ride Height",
    "ride height": "Ride Height",
    "susp": "Susp Pos",
    "suspension": "Susp Pos",
    "suspension travel": "Susp Pos",
    "damper": "Damper Pos",
    "deflection": "Spring Deflection",
    "wing": "Wing Angle",
    "flap": "Flap Angle",
    "downforce": "Downforce",
    "drag": "Drag",
    "tyre temp": "Tyre Temp",
    "tire temp": "Tyre Temp",
    "tyre carcass": "Tyre Carcass Temp",
    "tyre rubber": "Tyre Rubber Temp",
    "tyre pressure": "Tyre Pressure",
    "tire pressure": "Tyre Pressure",
    "tyre wear": "Tyre Wear",
    "tire wear": "Tyre Wear",
    "tyre rim": "Rim Temp",
    "track temperature": "Track Temp",
    "track temp": "Track Temp",
    "ambient temperature": "Ambient Temp",
    "ambient temp": "Ambient Temp",
    "wind speed": "Wind Speed",
    "wind direction": "Wind Direction",
    "lap": "Lap",
    "lap time": "Lap Time",
    "laptime": "Lap Time",
    "boost": "Boost Pressure",
    "turbo": "Turbo Speed",
    "regen": "Regen Level",
    "energy": "Energy",
    "fuel": "Fuel Level",
    "soc": "State of Charge",
    "state of charge": "State of Charge",
    "flag": "Flag",
    "pits": "Pit Status",
    "limiter": "Pit Limiter",
    "headlights": "Headlights",
    "map": "Engine Map"
}

MOTEC_CONTAINS = [
    ("g force long", "Long Accel"),
    ("g force lat", "Lat Accel"),
    ("g force vert", "Vert Accel"),
    ("accel x", "Long Accel"),
    ("accel y", "Lat Accel"),
    ("accel z", "Vert Accel"),
    ("wheel speed", "Wheel Speed"),
    ("ride height", "Ride Height"),
    ("tyre temp", "Tyre Temp"),
    ("tire temp", "Tyre Temp"),
    ("tyre pressure", "Tyre Pressure"),
    ("tire pressure", "Tyre Pressure"),
]

def split_words(text: str) -> str:
    text = re.sub(r"_+", " ", text)
    text = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", text)
    return re.sub(r"\s+", " ", text).strip()

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

def motec_standard_name(name: str) -> str:
    base = name.strip("_")
    wheel = None
    layer = None
    side = None

    for pattern, attr in ((r"_(fl|fr|rl|rr)$", "wheel"), (r"_(i|m|o)$", "layer"), (r"_(l|r|c)$", "side")):
        m = re.search(pattern, base, flags=re.IGNORECASE)
        if m:
            value = m.group(1).upper()
            base = base[: -len(m.group(0))]
            if attr == "wheel":
                wheel = value
            elif attr == "layer":
                layer = value
            else:
                side = value

    base_words = split_words(base)
    base_key = base_words.lower()

    base_name = MOTEC_BASE_NAMES.get(base_key)
    if not base_name:
        for pattern, mapped in MOTEC_CONTAINS:
            if pattern in base_key:
                base_name = mapped
                break

    if not base_name:
        tokens = []
        for token in base_words.split():
            tok_lower = token.lower()
            tokens.append(TOKEN_OVERRIDES.get(tok_lower, token.capitalize()))
        base_name = " ".join(tokens)

    suffix_parts = [p for p in (layer, side, wheel) if p]
    full_name = " ".join([part for part in [base_name, *suffix_parts] if part]).strip()
    return full_name or name.replace("_", " ")

def is_step(name: str) -> bool:
    n = name.lower()
    # canali discreti/stati (hold-last-value)
    return any(k in n for k in [
        "gear", "lap", "flag", "state", "status", "active", "activated",
        "abs", "tc", "tccut", "map", "pit", "limiter", "headlights", "finish"
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

def detect_laps(df: pd.DataFrame):
    lap_col = None
    candidates = [c for c in df.columns if "lap" in c.lower()]
    for c in candidates:
        if c.lower() == "lap":
            lap_col = c
            break
    if lap_col is None and candidates:
        lap_col = candidates[0]

    if lap_col:
        lap_series = pd.to_numeric(df[lap_col], errors="coerce")
        if lap_series.notna().any():
            return lap_series.fillna(method="ffill").fillna(1).astype(int)

    for keyword in ("lap time", "laptime"):
        lap_time_cols = [c for c in df.columns if keyword in c.lower()]
        if lap_time_cols:
            lt = pd.to_numeric(df[lap_time_cols[0]], errors="coerce").fillna(method="ffill").fillna(0.0)
            resets = lt.diff().fillna(0) < 0
            laps = resets.cumsum() + 1
            return laps.astype(int)

    lap_dist_cols = [c for c in df.columns if "lap distance" in c.lower()]
    if lap_dist_cols:
        ld = pd.to_numeric(df[lap_dist_cols[0]], errors="coerce").fillna(method="ffill").fillna(0.0)
        resets = ld.diff().fillna(0) < 0
        laps = resets.cumsum() + 1
        return laps.astype(int)

    return None

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
                name = motec_standard_name(normalize_name(raw_name))

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

    # Beacon + LapTime + Lap
    lap_series = detect_laps(out)
    if lap_series is None:
        out["Lap"] = 1
        out["Beacon"] = 0
        out["LapTime"] = out["Time"]
    else:
        out["Lap"] = lap_series
        beacon = (lap_series.diff().fillna(1) != 0).astype(int)
        beacon.iloc[0] = 1
        out["Beacon"] = beacon
        out["LapTime"] = out["Time"] - out.groupby(lap_series)["Time"].transform("min")

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
