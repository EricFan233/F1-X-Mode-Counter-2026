#!/usr/bin/env python3
"""
F1 X-Mode Angle Counter — Backend Server v3 (2026 rules)
Tracks rear wing X-Mode activations. Each open/close transition = +30°.
Pre-processes historical data, caches to disk, and serves live via SSE.
"""

import asyncio
import json
import os
import time
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

# X-Mode (formerly DRS): 10, 12, 14 = open states
DRS_OPEN_VALUES = {10, 12, 14}

# 2026 rules: most teams use ~30° wing movement.
# Ferrari rear wings open 180°.
# We count BOTH open and close as separate activations.
X_MODE_ANGLE = 30
FERRARI_ANGLE = 180

# ── State ──────────────────────────────────────────────────────────────
driver_season_stats = {}   # {num: {total_activations, total_angle, race_activations: {sk: count}}}
driver_live_state = {}     # {num: {activations, xmode_active, last_drs}}
driver_metadata = {}       # {num: {name_acronym, team_name, ...}}
current_session = None
current_meeting = None
all_sessions = []
all_meetings = []
completed_sessions = set()
last_poll_timestamp = None
is_live = False
last_update_time = None
sse_clients = []
historical_loading = False
historical_progress = ""

client: httpx.AsyncClient = None


# ── Cache ──────────────────────────────────────────────────────────────
def save_cache():
    data = {
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
        driver_season_stats = {int(k): v for k, v in data.get("driver_season_stats", {}).items()}
        # Convert race_activations keys back to int
        for num in driver_season_stats:
            ra = driver_season_stats[num].get("race_activations", {})
            driver_season_stats[num]["race_activations"] = {int(k): v for k, v in ra.items()}
        driver_metadata = {int(k): v for k, v in data.get("driver_metadata", {}).items()}
        completed_sessions = set(data.get("completed_sessions", []))
        print(f"[CACHE] Loaded {len(completed_sessions)} cached sessions, {len(driver_metadata)} drivers")


# ── HTTP ───────────────────────────────────────────────────────────────
async def fetch_json(endpoint, params=None, timeout=30, retries=3):
    for attempt in range(retries):
        try:
            url = f"{OPENF1_BASE}/{endpoint}"
            resp = await client.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                data = resp.json()
                # OpenF1 returns {"detail": "No results found."} for empty queries
                if isinstance(data, dict) and "detail" in data:
                    return None
                return data
            if resp.status_code == 429:  # rate limited
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
    if m:
        all_meetings = m
    s = await fetch_json("sessions", {"year": YEAR})
    if s:
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


def get_angle(num):
    team = driver_metadata.get(num, {}).get("team_name", "").lower()
    return FERRARI_ANGLE if "ferrari" in team else X_MODE_ANGLE


def ensure_stats(num):
    if num not in driver_season_stats:
        driver_season_stats[num] = {"total_activations": 0, "total_angle": 0, "race_activations": {}}
    if num not in driver_live_state:
        driver_live_state[num] = {"activations": 0, "xmode_active": False, "last_drs": 0}


# ── Historical Processing ─────────────────────────────────────────────
async def count_xmode_for_driver(session_key, driver_number, session_start, session_end):
    """Count X-Mode activations: both open and close transitions.
    Each transition (open or close) counts as +1 activation (+30°)."""
    activations = 0
    prev_drs = None
    start = datetime.fromisoformat(session_start)
    end = datetime.fromisoformat(session_end)
    window = timedelta(minutes=5)
    current = start

    while current < end:
        next_time = min(current + window, end)
        # Build URL manually to preserve >= and < operators
        url = (
            f"{OPENF1_BASE}/car_data"
            f"?session_key={session_key}"
            f"&driver_number={driver_number}"
            f"&date>={current.strftime('%Y-%m-%dT%H:%M:%S')}"
            f"&date<{next_time.strftime('%Y-%m-%dT%H:%M:%S')}"
        )
        try:
            resp = await client.get(url, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                for row in data:
                    drs = row.get("drs", 0)
                    if prev_drs is not None:
                        was_open = prev_drs in DRS_OPEN_VALUES
                        is_open = drs in DRS_OPEN_VALUES
                        if was_open != is_open:
                            # Transition detected (open→close or close→open)
                            activations += 1
                    prev_drs = drs
        except Exception:
            pass
        current = next_time
        await asyncio.sleep(0.3)

    return activations


async def process_session(session_info):
    """Process a single historical session."""
    global historical_progress
    session_key = session_info["session_key"]
    session_start = session_info["date_start"]
    session_end = session_info["date_end"]

    if session_key in completed_sessions:
        return

    # Load drivers for this session
    drivers = await fetch_json("drivers", {"session_key": session_key})
    if not drivers:
        print(f"[WARN] No drivers for session {session_key}")
        return

    # Update metadata
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

    # Count DRS for each driver (process concurrently in small batches)
    driver_nums = [d["driver_number"] for d in drivers]

    for i in range(0, len(driver_nums), 2):  # 2 concurrent to avoid rate limits
        batch = driver_nums[i:i+2]
        tasks = [count_xmode_for_driver(session_key, num, session_start, session_end) for num in batch]
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

    completed_sessions.add(session_key)
    save_cache()
    await broadcast_state()


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
        historical_progress = f"Processing {idx+1}/{total}: {s.get('circuit_short_name', '')} {s.get('session_name', '')}"
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


# ── Live Polling ───────────────────────────────────────────────────────
async def poll_live():
    global last_poll_timestamp, last_update_time
    if not current_session:
        return

    sk = current_session["session_key"]
    # Build URL manually to avoid param encoding issues
    url = f"{OPENF1_BASE}/car_data?session_key={sk}"
    if last_poll_timestamp:
        url += f"&date>{last_poll_timestamp}"

    try:
        resp = await client.get(url, timeout=10)
        if resp.status_code != 200:
            return
        data = resp.json()
    except Exception:
        return

    if not data:
        return

    by_driver = {}
    for row in data:
        by_driver.setdefault(row["driver_number"], []).append(row)

    for num, rows in by_driver.items():
        ensure_stats(num)
        live = driver_live_state[num]

        for row in rows:
            drs = row.get("drs", 0)
            was_open = live["last_drs"] in DRS_OPEN_VALUES
            is_open = drs in DRS_OPEN_VALUES
            if was_open != is_open:
                # Transition detected (open→close or close→open)
                live["activations"] += 1
            live["xmode_active"] = is_open
            live["last_drs"] = drs

        angle = get_angle(num)
        driver_season_stats[num]["race_activations"][sk] = live["activations"]
        driver_season_stats[num]["total_activations"] = sum(
            driver_season_stats[num]["race_activations"].values()
        )
        driver_season_stats[num]["total_angle"] = driver_season_stats[num]["total_activations"] * angle

    last_poll_timestamp = data[-1]["date"]
    last_update_time = datetime.now(timezone.utc).isoformat()


# ── Payload ────────────────────────────────────────────────────────────
def build_payload():
    drivers_list = []
    # Include ALL known drivers, even those with no stats yet
    all_driver_nums = set(driver_metadata.keys()) | set(driver_season_stats.keys())
    for num in all_driver_nums:
        stats = driver_season_stats.get(num, {"total_activations": 0, "total_angle": 0, "race_activations": {}})
        meta = driver_metadata.get(num, {})
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
            "xmode_active": live.get("xmode_active", False),
            "current_race_activations": current_count,
            "last_race_activations": last_race_count,
            "total_activations": stats["total_activations"],
            "total_angle": stats["total_angle"],
            "angle_per_activation": get_angle(num),
        })

    # Sort: by total_angle desc, then by team name for ties at zero
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
        future = [s for s in all_sessions if s.get("session_type") == "Race" and datetime.fromisoformat(s["date_start"]) > now]
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
    global current_session, current_meeting, is_live, last_poll_timestamp, driver_live_state

    load_cache()
    await load_season_data()
    print(f"[INFO] {len(all_meetings)} meetings, {len(all_sessions)} sessions")

    # Load driver metadata — try OLDEST sessions first.
    # Pre-season test sessions (earliest in the calendar) reliably have full grids.
    # Future race sessions return no data until race weekend.
    if len(driver_metadata) < 10:
        all_typed_sessions = sorted(
            [s for s in all_sessions if s.get("session_type") in ("Race", "Sprint", "Qualifying", "Practice")],
            key=lambda s: s["date_start"],  # ascending — oldest first
        )
        for s in all_typed_sessions:
            await load_drivers(s["session_key"])
            if len(driver_metadata) >= 10:  # Need at least 10 drivers for a valid grid
                print(f"[INFO] Loaded {len(driver_metadata)} drivers from session {s['session_key']} ({s.get('session_name', '')})")
                break
            await asyncio.sleep(0.3)

    await broadcast_state()

    # Periodically reload cache from disk (precompute.py populates it)
    cache_reload_counter = 0

    tick = 0
    while True:
        try:
            tick += 1
            cache_reload_counter += 1

            # Reload cache from disk every 30s (precompute.py writes it)
            if cache_reload_counter >= 30:
                cache_reload_counter = 0
                load_cache()

            if (tick * (POLL_INTERVAL_LIVE if is_live else POLL_INTERVAL_IDLE)) >= SESSION_CHECK_INTERVAL:
                tick = 0
                await load_season_data()

            active = find_active_session()

            if active and (not current_session or current_session["session_key"] != active["session_key"]):
                current_session = active
                current_meeting = find_meeting(active)
                is_live = True
                last_poll_timestamp = None
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
                last_poll_timestamp = None
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
