# F1 X-Mode Angle Counter — 2026

Live tracker for F1 rear wing X-Mode activations during the 2026 season.

Counts every wing state transition (open and close) at **30° per transition** for all teams, except **Ferrari at 180°** per transition. Data sourced live from [OpenF1](https://openf1.org).

## Features
- Real-time SSE updates during live sessions (1s refresh)
- Cumulative season-long angle tracking
- Driver and team rankings
- Automatic session detection (FP1 through Race)
- Works on mobile and desktop

## Deploy on Railway

1. Push this repo to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub repo
3. Railway auto-detects the `Procfile` and `requirements.txt`
4. Your app is live at `https://your-project.up.railway.app`

### Custom domain
In Railway: Settings → Domains → Add Custom Domain → follow the CNAME instructions from your registrar.

## Local development

```bash
pip install -r requirements.txt
python server.py
# Open http://localhost:8000
```

## Tech stack
- **Backend:** FastAPI + SSE + httpx
- **Frontend:** Vanilla HTML/CSS/JS (single file)
- **Data:** OpenF1 API
