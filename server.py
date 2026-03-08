#!/usr/bin/env python3
"""
F1 X-Mode Angle Counter — Backend Server v4 (2026 rules)
Tracks rear wing X-Mode (Straight Mode) activations.
Uses lap-based counting since OpenF1 car_data.drs is null for 2026.
Each Straight Mode zone per lap = 2 deployments (open + close).
Ferrari: 180° per deployment, all others: 30° per deployment.
"""

import asyncio
import json
import os
import traceback
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse

# ── Constants ──────────────────────────────────────────────────────────
OPENF1_BASE = "https://api.openf1.org/v1"
YEAR = 2026
POLL_INTERVAL_LIVE = 1.0
POLL_INTERVAL_IDLE = 60.0
SESSION_CHECK_INTERVAL = 120
CACHE_DIR = Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# Cache version — increment this whenever the counting algorithm changes
# to force reprocessing of all sessions on next deploy.
CACHE_VERSION = 2  # v2: lap-based counting (replaces DRS-based v1)

# 2026 rules: most teams use ~30° wing movement.
# Ferrari rear wings open 180°.
# We count BOTH open and close as separate deployments.
X_MODE_ANGLE = 30
FERRARI_ANGLE = 180

# ── Straight Mode Zones Per Circuit (2026) ─────────────────────────────
# Each zone = 1 open + 1 close = 2 deployments per lap
# Sources: FIA event notes, motorsport.com, the-race.com
CIRCUIT_ZONES = {
    # Melbourne: Originally 5, reduced to 4 after FP2 (T8-T9 removed for safety)
    "Melbourne": 4,
    # Shanghai: TBD - estimate based on circuit characteristics
    "Shanghai": 5,
    # Suzuka: TBD
    "Suzuka": 4,
    # Sakhir (Bahrain): TBD
    "Sakhir": 5,
    # Jeddah: TBD
    "Jeddah": 5,
    # Miami: TBD
    "Miami": 5,
    # Montreal: TBD
    "Montreal": 5,
    # Monte Carlo (Monaco): Fewer zones due to tight circuit
    "Monte Carlo": 3,
    # Catalunya (Barcelona): TBD
    "Catalunya": 5,
    # Spielberg (Austria): TBD
    "Spielberg": 4,
    # Silverstone: TBD
    "Silverstone": 5,
    # Spa-Francorchamps: TBD
    "Spa-Francorchamps": 5,
    # Hungaroring: Fewer straights
    "Hungaroring": 4,
    # Zandvoort: TBD
    "Zandvoort": 4,
    # Monza: Many straights
    "Monza": 6,
    # Madring: TBD
    "Madring": 4,
    # Baku: Long straights
    "Baku": 5,
    # Singapore: TBD
    "Singapore": 5,
    # Austin (COTA): TBD
    "Austin": 5,
    # Mexico City: TBD
    "Mexico City": 4,
    # Interlagos (São Paulo): TBD
    "Interlagos": 4,
    # Las Vegas: Long straights
    "Las Vegas": 5,
    # Lusail (Qatar): TBD
    "Lusail": 5,
    # Yas Marina (Abu Dhabi): TBD
    "Yas Marina Circuit": 5,
}
DEFAULT_ZONES = 4  # Fallback if circuit not mapped


# ── State ──────────────────────────────────────────────────────────────
driver_season_stats = {}   # {num: {total_activations, total_angle, race_activations: {sk: count}}}
driver_live_state = {}     # {num: {lap_count, laps_seen: set}}
driver_metadata = {}       # {num: {name_acronym, team_name, ...}}
current_session = None
current_meeting = None
all_sessions = []
all_meetings = []
completed_sessions = set()
is_live = False
last_update_time = None
sse_clients = []
historical_loading = False
historical_progress = ""

client: httpx.AsyncClient = None


# ── Cache ──────────────────────────────────────────────────────────────
def save_cache():
    data = {
        "cache_version": CACHE_VERSION,
        "driver_season_stats": {str(k): v for k, v in driver_season_stats.items()},
        "driver_metadata": {str(k): v for k, v in driver_metadata.items()},
        "completed_sessions": list(completed_sessions),
    }
    (CACHE_DIR / "season_data.json").write_text(json.dumps(data))


def load_cache():
    global driver_season_stats, driver_metadata, completed_sessions
    cache_file = CACHE_DIR / "season_data.json"
    if cache_file.exists():
        data = json.loads(cache_file.read_text())
        cached_version = data.get("cache_version", 1)
        if cached_version < CACHE_VERSION:
            print(f"[CACHE] Stale cache (v{cached_version} < v{CACHE_VERSION}). Clearing and reprocessing.")
            cache_file.unlink()
            return
        driver_season_stats = {int(k): v for k, v in data.get("driver_season_stats", {}).items()}
        for num in driver_season_stats:
            ra = driver_season_stats[num].get("race_activations", {})
            driver_season_stats[num]["race_activations"] = {int(k): v for k, v in ra.items()}
        driver_metadata = {int(k): v for k, v in data.get("driver_metadata", {}).items()}
        completed_sessions = set(data.get("completed_sessions", []))
        print(f"[CACHE] Loaded v{cached_version} — {len(completed_sessions)} cached sessions, {len(driver_metadata)} drivers")


# ── HTTP ───────────────────────────────────────────────────────────────
async def fetch_json(endpoint, params=None, timeout=30, retries=3):
    for attempt in range(retries):
        try:
            url = f"{OPENF1_BASE}/{endpoint}"
            resp = await client.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "detail" in data:
                    return None
                return data
            if resp.status_code == 429:
                await asyncio.sleep(2 * (attempt + 1))
                continue
            return None
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
            else:
                print(f"[WARN] fetch {endpoint}: {e}")
                return None


# ── Data Loading ───────────────────────────────────────────────────────
async def load_season_data():
    global all_meetings, all_sessions
    m = await fetch_json("meetings", {"year": YEAR})
    if isinstance(m, list):
        all_meetings = m
    s = await fetch_json("sessions", {"year": YEAR})
    if isinstance(s, list):
        all_sessions = s


async def load_drivers(session_key):
    global driver_metadata
    drivers = await fetch_json("drivers", {"session_key": session_key})
    if isinstance(drivers, list) and drivers:
        for d in drivers:
            num = d["driver_number"]
            driver_metadata[num] = {
                "driver_number": num,
                "broadcast_name": d.get("broadcast_name", ""),
                "full_name": d.get("full_name", ""),
                "name_acronym": d.get("name_acronym", ""),
                "team_name": d.get("team_name", ""),
                "team_colour": d.get("team_colour", "FFFFFF"),
                "headshot_url": d.get("headshot_url", ""),
                "first_name": d.get("first_name", ""),
                "last_name": d.get("last_name", ""),
            }
        return True
    return False


def get_zones_for_circuit(circuit_name):
    """Get the number of Straight Mode zones for a circuit."""
    return CIRCUIT_ZONES.get(circuit_name, DEFAULT_ZONES)


def get_angle(num):
    team = driver_metadata.get(num, {}).get("team_name", "").lower()
    return FERRARI_ANGLE if "ferrari" in team else X_MODE_ANGLE


def ensure_stats(num):
    if num not in driver_season_stats:
        driver_season_stats[num] = {"total_activations": 0, "total_angle": 0, "race_activations": {}}
    if num not in driver_live_state:
        driver_live_state[num] = {"lap_count": 0, "laps_seen": set()}


# ── Lap-Based X-Mode Counting ─────────────────────────────────────────
async def count_xmode_laps(session_key, driver_number, zones_per_lap):
    """Count X-Mode activations based on completed laps.
    Each lap has `zones_per_lap` straight mode zones.
    Each zone = 2 deployments (open + close)."""
    laps = await fetch_json("laps", {
        "session_key": session_key,
        "driver_number": driver_number,
    })
    if not isinstance(laps, list):
        return 0

    # Count laps with valid duration (completed laps only)
    completed_laps = sum(1 for lap in laps if lap.get("lap_duration") is not None)

    # Each lap: zones * 2 deployments (open + close)
    activations = completed_laps * zones_per_lap * 2
    return activations


# ── Historical Processing ─────────────────────────────────────────────
async def process_session(session_info):
    """Process a single historical session using lap-based counting."""
    global historical_progress
    session_key = session_info["session_key"]
    circuit = session_info.get("circuit_short_name", "")
    zones = get_zones_for_circuit(circuit)

    if session_key in completed_sessions:
        return

    # Load drivers for this session
    drivers = await fetch_json("drivers", {"session_key": session_key})
    if not isinstance(drivers, list) or not drivers:
        print(f"[WARN] No drivers for session {session_key}")
        return

    # Update metadata from this race session (more current than pre-season)
    for d in drivers:
        num = d["driver_number"]
        driver_metadata[num] = {
            "driver_number": num,
            "broadcast_name": d.get("broadcast_name", ""),
            "full_name": d.get("full_name", ""),
            "name_acronym": d.get("name_acronym", ""),
            "team_name": d.get("team_name", ""),
            "team_colour": d.get("team_colour", "FFFFFF"),
            "headshot_url": d.get("headshot_url", ""),
            "first_name": d.get("first_name", ""),
            "last_name": d.get("last_name", ""),
        }

    driver_nums = [d["driver_number"] for d in drivers]
    print(f"[HIST] Processing {circuit} ({session_info.get('session_name', '')}) - {len(driver_nums)} drivers, {zones} zones/lap")

    # Process in small batches to avoid rate limits
    for i in range(0, len(driver_nums), 3):
        batch = driver_nums[i:i + 3]
        tasks = [count_xmode_laps(session_key, num, zones) for num in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for num, result in zip(batch, results):
            ensure_stats(num)
            count = result if isinstance(result, int) else 0
            angle = get_angle(num)
            driver_season_stats[num]["race_activations"][session_key] = count
            driver_season_stats[num]["total_activations"] = sum(
                driver_season_stats[num]["race_activations"].values()
            )
            driver_season_stats[num]["total_angle"] = driver_season_stats[num]["total_activations"] * angle

        await asyncio.sleep(0.3)

    completed_sessions.add(session_key)
    save_cache()
    await broadcast_state()
    print(f"[HIST] Completed {circuit} - {session_info.get('session_name', '')}")


async def load_historical():
    """Process all completed race/sprint sessions."""
    global historical_loading, historical_progress
    historical_loading = True
    now = datetime.now(timezone.utc)

    past_sessions = [
        s for s in all_sessions
        if s.get("session_type") in ("Race", "Sprint")
        and s.get("date_end")
        and datetime.fromisoformat(s["date_end"]) < now
        and s["session_key"] not in completed_sessions
    ]

    total = len(past_sessions)
    for idx, s in enumerate(past_sessions):
        historical_progress = f"Processing {idx + 1}/{total}: {s.get('circuit_short_name', '')} {s.get('session_name', '')}"
        print(f"[HIST] {historical_progress}")
        try:
            await process_session(s)
        except Exception as e:
            print(f"[ERROR] session {s['session_key']}: {e}")
            traceback.print_exc()

    historical_loading = False
    historical_progress = ""
    print(f"[HIST] Done. {len(completed_sessions)} sessions processed.")


# ── Session Detection ──────────────────────────────────────────────────
def find_active_session():
    now = datetime.now(timezone.utc)
    for s in all_sessions:
        if not s.get("date_start") or not s.get("date_end"):
            continue
        start = datetime.fromisoformat(s["date_start"])
        end = datetime.fromisoformat(s["date_end"])
        if (start - timedelta(minutes=5)) <= now <= (end + timedelta(minutes=30)):
            return s
    return None


def find_meeting(session):
    mk = session.get("meeting_key")
    for m in all_meetings:
        if m.get("meeting_key") == mk:
            return m
    return None


# ── Live Polling (Lap-based) ──────────────────────────────────────────
async def poll_live():
    """Poll for new lap completions during a live session."""
    global last_update_time
    if not current_session:
        return

    sk = current_session["session_key"]
    circuit = current_session.get("circuit_short_name", "")
    zones = get_zones_for_circuit(circuit)

    # Get all known driver numbers
    known_drivers = list(driver_metadata.keys())
    if not known_drivers:
        return

    # Fetch laps for all drivers
    laps_data = await fetch_json("laps", {"session_key": sk})
    if not isinstance(laps_data, list):
        return

    # Group by driver
    by_driver = {}
    for lap in laps_data:
        dn = lap.get("driver_number")
        if dn in driver_metadata:  # Only count known drivers
            by_driver.setdefault(dn, []).append(lap)

    changed = False
    for num, laps in by_driver.items():
        ensure_stats(num)
        live = driver_live_state[num]

        # Count completed laps (with valid duration)
        completed = sum(1 for l in laps if l.get("lap_duration") is not None)

        if completed != live["lap_count"]:
            live["lap_count"] = completed
            activations = completed * zones * 2

            angle = get_angle(num)
            driver_season_stats[num]["race_activations"][sk] = activations
            driver_season_stats[num]["total_activations"] = sum(
                driver_season_stats[num]["race_activations"].values()
            )
            driver_season_stats[num]["total_angle"] = driver_season_stats[num]["total_activations"] * angle
            changed = True

    if changed:
        last_update_time = datetime.now(timezone.utc).isoformat()


# ── Payload ────────────────────────────────────────────────────────────
def build_payload():
    drivers_list = []
    # Only include drivers from metadata (no phantom driver numbers)
    for num in driver_metadata:
        stats = driver_season_stats.get(num, {"total_activations": 0, "total_angle": 0, "race_activations": {}})
        meta = driver_metadata[num]
        live = driver_live_state.get(num, {})
        current_sk = current_session["session_key"] if current_session else None
        current_count = stats["race_activations"].get(current_sk, 0) if current_sk else 0

        last_race_count = 0
        if stats["race_activations"]:
            completed_race_sks = [
                s for s in all_sessions
                if s["session_key"] in stats["race_activations"]
                and s.get("session_type") in ("Race", "Sprint")
                and s["session_key"] in completed_sessions
            ]
            if completed_race_sks:
                last_sk = max(completed_race_sks, key=lambda s: s["date_start"])["session_key"]
                last_race_count = stats["race_activations"].get(last_sk, 0)

        drivers_list.append({
            "driver_number": num,
            "name_acronym": meta.get("name_acronym", f"D{num}"),
            "full_name": meta.get("full_name", f"Driver {num}"),
            "team_name": meta.get("team_name", "Unknown"),
            "team_colour": meta.get("team_colour", "FFFFFF"),
            "headshot_url": meta.get("headshot_url", ""),
            "xmode_active": False,  # No per-driver X-Mode status in 2026
            "current_race_activations": current_count,
            "last_race_activations": last_race_count,
            "total_activations": stats["total_activations"],
            "total_angle": stats["total_angle"],
            "angle_per_activation": get_angle(num),
        })

    # Sort: by total_angle desc, then by team name for ties
    drivers_list.sort(key=lambda d: (d["total_angle"], d["team_name"]), reverse=True)

    session_info = None
    if current_session:
        session_info = {
            "session_key": current_session["session_key"],
            "session_type": current_session.get("session_type", ""),
            "session_name": current_session.get("session_name", ""),
            "circuit": current_session.get("circuit_short_name", ""),
            "country": current_session.get("country_name", ""),
        }

    meeting_info = None
    if current_meeting:
        meeting_info = {
            "meeting_name": current_meeting.get("meeting_name", ""),
            "circuit": current_meeting.get("circuit_short_name", ""),
            "country": current_meeting.get("country_name", ""),
        }

    next_race = None
    if not is_live:
        now = datetime.now(timezone.utc)
        future = [
            s for s in all_sessions
            if s.get("session_type") in ("Race", "Sprint")
            and s.get("date_start")
            and datetime.fromisoformat(s["date_start"]) > now
        ]
        if future:
            nr = min(future, key=lambda s: s["date_start"])
            m = find_meeting(nr)
            next_race = {
                "session_name": nr.get("session_name", ""),
                "circuit": nr.get("circuit_short_name", ""),
                "country": nr.get("country_name", ""),
                "date_start": nr.get("date_start", ""),
                "meeting_name": m.get("meeting_name", "") if m else "",
            }

    return {
        "is_live": is_live,
        "session": session_info,
        "meeting": meeting_info,
        "next_race": next_race,
        "drivers": drivers_list,
        "last_update": last_update_time,
        "completed_races": len(completed_sessions),
        "year": YEAR,
        "loading": historical_loading,
        "loading_progress": historical_progress,
    }


# ── SSE ────────────────────────────────────────────────────────────────
async def broadcast_state():
    global last_update_time
    last_update_time = datetime.now(timezone.utc).isoformat()
    payload = build_payload()
    msg = f"data: {json.dumps(payload)}\n\n"
    dead = []
    for q in sse_clients:
        try:
            q.put_nowait(msg)
        except Exception:
            dead.append(q)
    for q in dead:
        if q in sse_clients:
            sse_clients.remove(q)


# ── Main Loop ──────────────────────────────────────────────────────────
async def main_loop():
    global current_session, current_meeting, is_live, driver_live_state

    load_cache()
    await load_season_data()
    print(f"[INFO] {len(all_meetings)} meetings, {len(all_sessions)} sessions")

    # Load driver metadata — try sessions in ascending order (oldest first).
    # Pre-season test sessions reliably have full grids; future sessions may not.
    # Then also try the most recent completed sessions for updated metadata.
    if len(driver_metadata) < 10:
        all_typed = sorted(
            [s for s in all_sessions if s.get("session_type") in ("Race", "Sprint", "Qualifying", "Practice")],
            key=lambda s: s["date_start"],
        )
        for s in all_typed:
            loaded = await load_drivers(s["session_key"])
            if loaded and len(driver_metadata) >= 10:
                print(f"[INFO] Loaded {len(driver_metadata)} drivers from session {s['session_key']} ({s.get('session_name', '')})")
                break
            await asyncio.sleep(0.3)

    # Also refresh from most recent completed race/qualifying for latest metadata
    now = datetime.now(timezone.utc)
    recent_completed = sorted(
        [s for s in all_sessions
         if s.get("date_end") and datetime.fromisoformat(s["date_end"]) < now
         and s.get("session_type") in ("Race", "Qualifying")],
        key=lambda s: s["date_start"],
        reverse=True,
    )
    for s in recent_completed[:3]:
        loaded = await load_drivers(s["session_key"])
        if loaded:
            print(f"[INFO] Refreshed metadata from {s.get('session_name', '')} at {s.get('circuit_short_name', '')}")
            break
        await asyncio.sleep(0.3)

    # Process any completed races we haven't counted yet
    await load_historical()

    await broadcast_state()

    tick = 0
    while True:
        try:
            tick += 1

            if (tick * (POLL_INTERVAL_LIVE if is_live else POLL_INTERVAL_IDLE)) >= SESSION_CHECK_INTERVAL:
                tick = 0
                await load_season_data()

            active = find_active_session()

            if active and (not current_session or current_session["session_key"] != active["session_key"]):
                current_session = active
                current_meeting = find_meeting(active)
                is_live = True
                driver_live_state = {}
                await load_drivers(active["session_key"])
                print(f"[LIVE] {active['session_name']} at {active['circuit_short_name']}")
            elif active:
                is_live = True
            elif not active and current_session:
                print(f"[END] {current_session['session_name']}")
                if current_session["session_type"] in ("Race", "Sprint"):
                    completed_sessions.add(current_session["session_key"])
                    save_cache()
                current_session = None
                current_meeting = None
                is_live = False
            else:
                is_live = False

            if is_live:
                await poll_live()
                await broadcast_state()
                await asyncio.sleep(POLL_INTERVAL_LIVE)
            else:
                await broadcast_state()
                await asyncio.sleep(POLL_INTERVAL_IDLE)

        except Exception as e:
            print(f"[ERROR] loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(5)


# ── FastAPI ────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app):
    global client
    client = httpx.AsyncClient()
    task = asyncio.create_task(main_loop())
    yield
    task.cancel()
    await client.aclose()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/api/state")
async def get_state():
    return JSONResponse(content=build_payload())


@app.get("/api/stream")
async def stream(request: Request):
    queue = asyncio.Queue(maxsize=50)
    sse_clients.append(queue)
    payload = build_payload()
    initial = f"data: {json.dumps(payload)}\n\n"

    async def gen():
        try:
            yield initial
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            if queue in sse_clients:
                sse_clients.remove(queue)

    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})


@app.get("/api/health")
async def health():
    return {"status": "ok", "is_live": is_live, "loading": historical_loading,
            "sessions": len(completed_sessions), "drivers": len(driver_metadata)}


# ── Serve static frontend ─────────────────────────────────────────────
from fastapi.responses import FileResponse

@app.get("/")
async def serve_index():
    return FileResponse(Path(__file__).parent / "index.html")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
