# scraper/espncricinfo_scraper.py
# ─────────────────────────────────────────────────────────
# COMPLETE REWRITE — v4.0
#
# BUGS FIXED vs v3.0:
#   1. ESPN event-ID ≠ ESPNcricinfo match-ID
#      The old code did `str(eid) == str(match_id)` which is ALWAYS
#      False because ESPN uses its own numbering. ESPN data was
#      therefore NEVER used. Fixed: removed ID-equality gate.
#
#   2. KNOWN_MATCHES was checked LAST
#      When HTML returns {} the hardcoded fallback ran but many IDs
#      were simply missing from it. Fixed: KNOWN_MATCHES is now the
#      FIRST thing consulted for static data (teams / venue).
#
#   3. scrape_match() returned {} for any ID not in hardcoded dict
#      even when HTML also failed. Fixed: layered strategy — static
#      base from schedule, live toss from ESPN/HTML on top.
#
# ARCHITECTURE (scrape_match):
#   Layer 1 → KNOWN_MATCHES   : static teams / venue (always fast)
#   Layer 2 → ESPN API         : live toss / match status overlay
#   Layer 3 → HTML scraper     : toss fallback if ESPN blocked
#   Layer 4 → pure HTML parse  : full fallback for unknown match IDs
#
# ARCHITECTURE (get_todays_match_id):
#   1. Date match in KNOWN_MATCHES  (instant)
#   2. ESPN scoreboard API          (team-name fuzzy match → known ID)
#   3. ESPNcricinfo schedule HTML   (last resort)
# ─────────────────────────────────────────────────────────

import os
import re
import time
import random
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd


# ── Confirmed series ID ───────────────────────────────────
IPL_SERIES_ID = os.getenv("IPL_SERIES_ID", "1510719")

SESSION = requests.Session()

_UA_LIST = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4.1 Safari/605.1.15"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
]

# ─────────────────────────────────────────────────────────
# KNOWN IPL 2026 MATCH SCHEDULE
# ─────────────────────────────────────────────────────────
# How to find a match ID:
#   Go to ESPNcricinfo → open the match page → look at the URL:
#   espncricinfo.com/series/ipl-2026-1510719/{slug}-{MATCH_ID}/live-cricket-score
#   The 7-digit number at the end before /live-cricket-score is the match ID.
#
# Add each match before it starts. The scraper uses this dict as the
# primary source for teams and venue — APIs only add live toss/status.
# ─────────────────────────────────────────────────────────
KNOWN_MATCHES = {
    # match_id (int) : static match info
    # "date" must be "YYYY-MM-DD" (IST match date)
    1529286: {
        "team1": "Rajasthan Royals",
        "team2": "Delhi Capitals",
        "venue": "Sawai Mansingh Stadium, Jaipur",
        "date": "2026-05-01",
    },
    # ── Add upcoming matches below ──
    # 1529290: {
    #     "team1": "Mumbai Indians",
    #     "team2": "Chennai Super Kings",
    #     "venue": "Wankhede Stadium, Mumbai",
    #     "date": "2026-05-03",
    # },
    # 1529295: {
    #     "team1": "Kolkata Knight Riders",
    #     "team2": "Sunrisers Hyderabad",
    #     "venue": "Eden Gardens, Kolkata",
    #     "date": "2026-05-04",
    # },
}

# ─────────────────────────────────────────────────────────
# TEAM NAME NORMALISATION
# Converts any short form / city name → canonical full name
# ─────────────────────────────────────────────────────────
_TEAM_ALIASES = {
    "rajasthan":  "Rajasthan Royals",    "rr":   "Rajasthan Royals",
    "delhi":      "Delhi Capitals",      "dc":   "Delhi Capitals",
    "mumbai":     "Mumbai Indians",      "mi":   "Mumbai Indians",
    "kolkata":    "Kolkata Knight Riders","kkr":  "Kolkata Knight Riders",
    "chennai":    "Chennai Super Kings", "csk":  "Chennai Super Kings",
    "hyderabad":  "Sunrisers Hyderabad", "srh":  "Sunrisers Hyderabad",
    "bangalore":  "Royal Challengers Bengaluru",
    "bengaluru":  "Royal Challengers Bengaluru",
    "rcb":        "Royal Challengers Bengaluru",
    "punjab":     "Punjab Kings",        "pbks": "Punjab Kings",
    "lucknow":    "Lucknow Super Giants","lsg":  "Lucknow Super Giants",
    "gujarat":    "Gujarat Titans",      "gt":   "Gujarat Titans",
}

_TEAM_KW = (
    r"Indians|Royals|Kings|Capitals|Knights|Titans|Giants|Sunrisers|"
    r"Challengers|Lucknow|Punjab|Gujarat|Mumbai|Delhi|Rajasthan|"
    r"Kolkata|Chennai|Hyderabad|Bangalore|Bengaluru|Supergiants"
)


def _normalise_team(raw: str) -> str:
    """Map a raw team string (from API / HTML) to canonical full name."""
    if not raw:
        return raw
    raw_l = raw.lower().strip()
    for alias, canonical in _TEAM_ALIASES.items():
        if alias in raw_l:
            return canonical
    return raw  # return as-is if no match


# ─────────────────────────────────────────────────────────
# LOW-LEVEL FETCH HELPERS
# ─────────────────────────────────────────────────────────

def _json_headers():
    return {
        "User-Agent": random.choice(_UA_LIST),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.espncricinfo.com",
        "Referer": "https://www.espncricinfo.com/",
    }


def _html_headers():
    return {
        "User-Agent": random.choice(_UA_LIST),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def _get_json(url, retries=2):
    for attempt in range(retries):
        try:
            res = SESSION.get(url, headers=_json_headers(), timeout=12)
            if res.status_code == 200:
                return res.json()
            if res.status_code in (401, 403, 404):
                print(f"[scraper] Hard block {res.status_code}: {url}")
                return None
            print(f"[scraper] HTTP {res.status_code}, attempt {attempt + 1}: {url}")
            time.sleep(1.5 * (attempt + 1))
        except requests.exceptions.Timeout:
            print(f"[scraper] Timeout attempt {attempt + 1}: {url}")
            time.sleep(2)
        except Exception as exc:
            print(f"[scraper] Request error: {exc}")
            time.sleep(2)
    return None


def _get_html(url, retries=2):
    for attempt in range(retries):
        try:
            res = SESSION.get(url, headers=_html_headers(), timeout=15)
            if res.status_code == 200:
                return res.text
            if res.status_code in (401, 403):
                print(f"[scraper] HTML hard block {res.status_code}: {url}")
                return None
            print(f"[scraper] HTML HTTP {res.status_code}, attempt {attempt + 1}")
            time.sleep(1.5 * (attempt + 1))
        except Exception as exc:
            print(f"[scraper] HTML error: {exc}")
            time.sleep(2)
    return None


def _get_html_selenium(url):
    try:
        import importlib
        webdriver = importlib.import_module("selenium.webdriver").webdriver
        Options  = importlib.import_module("selenium.webdriver.chrome.options").Options
        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument(f"--user-agent={random.choice(_UA_LIST)}")
        driver = webdriver.Chrome(options=opts)
        driver.get(url)
        time.sleep(3)
        html = driver.page_source
        driver.quit()
        return html
    except ModuleNotFoundError:
        print("[scraper] Selenium not installed")
        return None
    except Exception as exc:
        print(f"[scraper] Selenium error: {exc}")
        return None


# ─────────────────────────────────────────────────────────
# SOURCE 1 — ESPN Public Scoreboard API
# ─────────────────────────────────────────────────────────
# NOTE: ESPN uses its OWN event-ID numbering, completely different
# from ESPNcricinfo match IDs. We NEVER compare them. We use ESPN
# only to get toss / status info and optionally to fuzzy-match teams.

_ESPN_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/cricket/ipl/scoreboard"
)


def _espn_best_event():
    """
    Fetch ESPN scoreboard → return (event_id_str, event_dict) for the
    current / most recent IPL match.  Returns (None, None) on failure.

    *** Does NOT try to compare event_id with ESPNcricinfo match_id ***
    """
    data = _get_json(_ESPN_SCOREBOARD)
    if not data:
        return None, None

    events = data.get("events", [])
    if not events:
        print("[scraper] ESPN API: no events in response")
        return None, None

    priority = {"in": 0, "pre": 1, "post": 2}
    events_sorted = sorted(
        events,
        key=lambda e: priority.get(
            e.get("status", {}).get("type", {}).get("state", "post"), 3
        ),
    )
    best = events_sorted[0]
    eid  = best.get("id", "")
    state = best.get("status", {}).get("type", {}).get("state", "?")
    print(f"[scraper] ESPN best event: id={eid}, state={state}")
    return str(eid), best


def _espn_extract_toss(event, team1="", team2=""):
    """
    Extract toss / status data from an ESPN event dict.
    Returns a partial dict (only the keys that were found).
    Teams are passed in so we can normalise ESPN's names to ours.
    """
    if not event:
        return {}

    result = {}

    # Match status
    state = event.get("status", {}).get("type", {}).get("state", "")
    if state == "in":
        result["match_status"] = "live"
    elif state == "post":
        result["match_status"] = "completed"
    else:
        result["match_status"] = "upcoming"

    competitions = event.get("competitions", [event])
    comp = competitions[0] if competitions else {}
    competitors = comp.get("competitors", [])

    # ESPN team names (may differ from our canonical names)
    espn_t1 = competitors[0].get("team", {}).get("displayName", "") if len(competitors) > 0 else ""
    espn_t2 = competitors[1].get("team", {}).get("displayName", "") if len(competitors) > 1 else ""

    for note in comp.get("notes", []):
        text = note.get("text", "")
        m = re.search(
            r"(.+?)\s+won the toss and elected to\s+(bat|bowl|field)",
            text,
            re.IGNORECASE,
        )
        if m:
            raw_winner = m.group(1).strip()
            decision   = "bat" if m.group(2).lower() == "bat" else "field"

            # Map raw_winner → canonical name using team1/team2 we already know
            toss_winner = ""
            for canonical in [team1, team2]:
                if canonical and any(
                    kw.lower() in raw_winner.lower()
                    for kw in canonical.split()
                    if len(kw) > 3
                ):
                    toss_winner = canonical
                    break
            if not toss_winner:
                toss_winner = _normalise_team(raw_winner) or raw_winner

            if decision == "bat":
                chasing = team2 if toss_winner == team1 else team1
            else:
                chasing = toss_winner

            result.update(
                {
                    "toss_winner":  toss_winner,
                    "toss_decision": decision,
                    "toss_done":    True,
                    "chasing_team": chasing,
                }
            )
            print(f"[scraper] ESPN toss → {toss_winner} elected to {decision}")
            break

    return result


# ─────────────────────────────────────────────────────────
# SOURCE 2 — ESPNcricinfo Live Score Page HTML
# ─────────────────────────────────────────────────────────

def _scrape_toss_html(match_id, team1, team2):
    """
    Hit the ESPNcricinfo live-score page and extract toss info.
    Returns a partial dict (toss fields only) or {}.
    """
    url = (
        f"https://www.espncricinfo.com/series/ipl-2026-{IPL_SERIES_ID}"
        f"/{match_id}/live-cricket-score"
    )
    html = _get_html(url)
    if not html:
        print(f"[scraper] Live page blocked, trying Selenium for {match_id}…")
        html = _get_html_selenium(url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    toss_patterns = [
        rf"({_TEAM_KW}(?:\s+\w+)?)\s+won the toss\s+(?:and\s+)?(?:elected|chose)\s+to\s+(bat|bowl|field)",
        rf"Toss[:\s]+({_TEAM_KW}(?:\s+\w+)?)[^,]*,\s*(bat|bowl|field)",
        rf"({_TEAM_KW}(?:\s+\w+)?)[,\s]+(?:bat|bowl|field) first",
    ]

    for pattern in toss_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if not m:
            continue

        raw_winner  = m.group(1).strip()
        decision_raw = m.group(2).lower() if m.lastindex >= 2 else ""

        # Map to canonical team names
        toss_winner = ""
        for canonical in [team1, team2]:
            if canonical and any(
                kw.lower() in raw_winner.lower()
                for kw in canonical.split()
                if len(kw) > 3
            ):
                toss_winner = canonical
                break
        if not toss_winner:
            toss_winner = _normalise_team(raw_winner) or raw_winner

        decision = "bat" if "bat" in decision_raw else "field"
        chasing  = (team2 if toss_winner == team1 else team1) if decision == "bat" else toss_winner

        status = "upcoming"
        if any(kw in page_text.lower() for kw in ("live", "batting", "in progress", "wickets")):
            status = "live"

        result = {
            "toss_winner":  toss_winner,
            "toss_decision": decision,
            "toss_done":    True,
            "chasing_team": chasing,
            "match_status": status,
        }
        print(f"[scraper] HTML toss → {toss_winner} elected to {decision}")
        return result

    print(f"[scraper] HTML: no toss info found on page for match {match_id}")
    return {}


# ─────────────────────────────────────────────────────────
# SOURCE 3 — ESPNcricinfo Schedule Page  (for match-ID discovery)
# ─────────────────────────────────────────────────────────

def _schedule_url():
    return (
        f"https://www.espncricinfo.com/series/ipl-2026-{IPL_SERIES_ID}"
        "/match-schedule-fixtures-and-results"
    )


def _html_find_match_id():
    """
    Scrape ESPNcricinfo schedule page → nearest upcoming match ID.
    Returns int or None.
    """
    url  = _schedule_url()
    html = _get_html(url)
    if not html:
        print("[scraper] Schedule HTML blocked, trying Selenium…")
        html = _get_html_selenium(url)
    if not html:
        return None

    _month_map = {
        "jan": 1, "january": 1, "feb": 2, "february": 2,
        "mar": 3, "march": 3,  "apr": 4, "april": 4,
        "may": 5,
        "jun": 6, "june": 6,  "jul": 7, "july": 7,
        "aug": 8, "august": 8,"sep": 9, "sept": 9, "september": 9,
        "oct": 10,"october": 10,"nov": 11,"november": 11,
        "dec": 12,"december": 12,
    }

    def _parse_date(text):
        for pat in [
            r"\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(?P<day>\d{1,2})(?:,?\s*(?P<year>\d{4}))?",
            r"\b(?P<day>\d{1,2})\s+(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)(?:,?\s*(?P<year>\d{4}))?",
        ]:
            m = re.search(pat, text, re.IGNORECASE)
            if not m:
                continue
            month = _month_map.get(m.group("month").lower()[:3])
            if not month:
                continue
            day  = int(m.group("day"))
            year = int(m.group("year")) if m.group("year") else date.today().year
            try:
                return date(year, month, day)
            except ValueError:
                continue
        return None

    soup = BeautifulSoup(html, "html.parser")
    now  = datetime.now()
    seen, candidates, found_ids = set(), [], []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[/-](\d{7,})(?:/|$)", href)
        if not m:
            continue
        mid = int(m.group(1))
        if mid in seen:
            continue
        seen.add(mid)
        found_ids.append(mid)

        match_dt = None
        container = a
        for _ in range(5):
            txt = container.get_text(" ", strip=True)
            d = _parse_date(txt)
            if d:
                match_dt = datetime(d.year, d.month, d.day, 12, 0)
                break
            container = container.parent
            if container is None:
                break

        if match_dt:
            candidates.append((match_dt, mid))

    if candidates:
        future  = [c for c in candidates if c[0] >= now]
        chosen  = min(future or candidates, key=lambda c: c[0])
        print(f"[scraper] Schedule found {len(candidates)} dated IDs, using {chosen[1]} @ {chosen[0].date()}")
        return chosen[1]

    if found_ids:
        print(f"[scraper] Schedule found {len(found_ids)} IDs (no dates), using {found_ids[0]}")
        return found_ids[0]

    return None


# ─────────────────────────────────────────────────────────
# SOURCE 4 — Full HTML parse fallback (unknown match IDs)
# ─────────────────────────────────────────────────────────

def _html_full_match_info(match_id):
    """
    Last-resort: parse teams + toss entirely from ESPNcricinfo HTML.
    Used only when match_id is NOT in KNOWN_MATCHES.
    """
    url = (
        f"https://www.espncricinfo.com/series/ipl-2026-{IPL_SERIES_ID}"
        f"/{match_id}/live-cricket-score"
    )
    html = _get_html(url)
    if not html:
        html = _get_html_selenium(url)
    if not html:
        return {}

    soup      = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    result = {
        "team1": "", "team2": "", "venue": "",
        "match_status": "upcoming",
        "toss_winner": "", "toss_decision": "",
        "toss_done": False, "chasing_team": "",
        "match_id": str(match_id),
    }

    # ── Teams ──────────────────────────────────────────────
    m = re.search(
        rf"({_TEAM_KW}(?:\s+\w+)?)\s+(?:vs|v)\s+({_TEAM_KW}(?:\s+\w+)?)",
        page_text, re.IGNORECASE,
    )
    if m:
        result["team1"] = _normalise_team(m.group(1).strip())
        result["team2"] = _normalise_team(m.group(2).strip())
    else:
        # keyword scan
        found = []
        for kw, canonical in _TEAM_ALIASES.items():
            if kw in page_text.lower() and canonical not in found:
                found.append(canonical)
            if len(found) == 2:
                break
        if len(found) >= 2:
            result["team1"], result["team2"] = found[0], found[1]

    # ── Venue ──────────────────────────────────────────────
    for kw in [
        "Wankhede", "Eden Gardens", "Chinnaswamy", "M Chinnaswamy",
        "Kotla", "Brabourne", "Chepauk", "Ekana", "HPCA",
        "Narendra Modi", "Sawai Mansingh", "Arun Jaitley",
    ]:
        if kw.lower() in page_text.lower():
            result["venue"] = kw
            break

    # ── Toss ───────────────────────────────────────────────
    for pat in [
        rf"({_TEAM_KW}(?:\s+\w+)?)\s+won the toss\s+(?:and\s+)?(?:elected|chose)\s+to\s+(bat|bowl|field)",
        rf"Toss[:\s]+({_TEAM_KW}(?:\s+\w+)?)[^,]*,\s*(bat|bowl|field)",
    ]:
        m2 = re.search(pat, page_text, re.IGNORECASE)
        if m2:
            tw  = _normalise_team(m2.group(1).strip())
            dec = "bat" if "bat" in m2.group(2).lower() else "field"
            t1, t2 = result["team1"], result["team2"]
            chasing = (t2 if tw == t1 else t1) if dec == "bat" else tw
            result.update({"toss_winner": tw, "toss_decision": dec,
                           "toss_done": True, "chasing_team": chasing})
            break

    if any(k in page_text.lower() for k in ("live", "batting", "in progress")):
        result["match_status"] = "live"

    return result


# ─────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────

def get_todays_match_id():
    """
    Returns int match_id for today's / nearest IPL match, or None.

    Priority:
    1. Date lookup in KNOWN_MATCHES  (instant, no network)
    2. ESPN API event → fuzzy-match teams → resolve to known ID
    3. ESPNcricinfo schedule HTML
    """
    today = date.today().isoformat()

    # ── 1. KNOWN_MATCHES date lookup ──
    for mid, info in sorted(KNOWN_MATCHES.items()):
        if info.get("date") == today:
            print(f"[scraper] Today's match from schedule: {mid} "
                  f"({info['team1']} vs {info['team2']})")
            return mid

    # ── 2. ESPN API ───────────────────
    _, event = _espn_best_event()
    if event:
        state = event.get("status", {}).get("type", {}).get("state", "")
        if state in ("in", "pre"):
            comps = event.get("competitions", [event])
            comp  = comps[0] if comps else {}
            competitors = comp.get("competitors", [])
            espn_names = {
                c.get("team", {}).get("displayName", "").lower()
                for c in competitors
            }
            # Try to resolve to a known ESPNcricinfo match ID by team names
            for mid, info in KNOWN_MATCHES.items():
                for canonical in [info["team1"], info["team2"]]:
                    if any(
                        kw.lower() in n
                        for n in espn_names
                        for kw in canonical.split()
                        if len(kw) > 3
                    ):
                        print(f"[scraper] ESPN event matched to known match {mid}")
                        return mid

    # ── 3. Schedule HTML ─────────────
    mid = _html_find_match_id()
    if mid:
        return mid

    print("[scraper] Could not auto-detect today's match.")
    return None


def scrape_match(match_id):
    """
    Returns a full match_info dict for match_id.

    Layered strategy:
      Layer 1 — KNOWN_MATCHES       : teams + venue  (always tried first)
      Layer 2 — ESPN API toss        : overlay live toss/status
      Layer 3 — HTML toss scrape     : fallback if ESPN missing/blocked
      Layer 4 — full HTML parse      : used only if match not in KNOWN_MATCHES
    """
    # ── Layer 1: Static base from schedule ────────────────
    if match_id in KNOWN_MATCHES:
        base = dict(KNOWN_MATCHES[match_id])          # deep copy
        base.update({
            "match_id":     str(match_id),
            "toss_winner":  "",
            "toss_decision": "",
            "toss_done":    False,
            "chasing_team": "",
            "match_status": "upcoming",
        })
        print(f"[scraper] Layer 1 (KNOWN_MATCHES): "
              f"{base['team1']} vs {base['team2']} @ {base['venue']}")
    else:
        print(f"[scraper] match_id {match_id} not in KNOWN_MATCHES — using HTML fallback")
        base = _html_full_match_info(match_id)
        if base.get("team1"):
            return base
        print("[scraper] All sources exhausted — no match data")
        return {}

    # ── Layer 2: ESPN API for live toss/status ────────────
    _, event = _espn_best_event()
    if event:
        toss_data = _espn_extract_toss(event, base["team1"], base["team2"])
        if toss_data:
            base.update(toss_data)
            print(f"[scraper] Layer 2 (ESPN): toss_done={base.get('toss_done')}, "
                  f"winner={base.get('toss_winner')}, decision={base.get('toss_decision')}")

    # ── Layer 3: HTML toss scrape (if still no toss) ──────
    if not base.get("toss_done"):
        print("[scraper] Layer 3 (HTML toss scrape)…")
        html_toss = _scrape_toss_html(match_id, base["team1"], base["team2"])
        if html_toss:
            base.update(html_toss)

    return base


# ─────────────────────────────────────────────────────────
# FEATURE VECTOR  (unchanged from v3.0)
# ─────────────────────────────────────────────────────────

def _safe_label_transform(encoder, value, default=0):
    try:
        return int(encoder.transform([value])[0])
    except Exception:
        return default


def build_feature_vector(
    match_info, player_lookup, matches,
    team_encoder, venue_encoder, venue_score_history,
    team_pp_eco_lookup, team_oppener_lookup,
    get_team_recent_avg_score, get_season_avg_score,
    get_season_year, get_venue_recent_avg_score,
    get_team_recent_high_score_rate, feature_cols,
):
    team1       = match_info.get("team1", "")
    team2       = match_info.get("team2", "")
    venue       = match_info.get("venue", "")
    toss_winner = match_info.get("toss_winner", "")
    toss_dec    = match_info.get("toss_decision", "").lower()
    today       = pd.Timestamp(date.today())

    def _tm(t):
        return matches[(matches["team1"] == t) | (matches["team2"] == t)].sort_values("match_date")

    def _wr(t):
        tm = _tm(t)
        return float((tm["winner"] == t).sum()) / len(tm) if len(tm) else 0.5

    def _l5(t):
        tm = _tm(t).tail(5)
        return float((tm["winner"] == t).sum()) if len(tm) else 3.0

    def _chase(t):
        tm = _tm(t)
        if not len(tm):
            return 0.5
        cw = tm[
            (tm["winner"] == t)
            & (tm.get("win_by_wickets", pd.Series(0, index=tm.index)) > 0)
        ]
        return float(len(cw)) / len(tm)

    def _h2h(a, b):
        h = matches[
            ((matches["team1"] == a) & (matches["team2"] == b))
            | ((matches["team1"] == b) & (matches["team2"] == a))
        ]
        return int((h["winner"] == a).sum())

    row = {
        "team1":                   _safe_label_transform(team_encoder, team1),
        "team2":                   _safe_label_transform(team_encoder, team2),
        "venue":                   _safe_label_transform(venue_encoder, venue),
        "venue_avg_first_innings": get_venue_recent_avg_score(venue, today),
        "venue_recent_avg":        get_venue_recent_avg_score(venue, today),
        "is_home_team1":           0,
        "toss_winner_is_team1":    1 if toss_winner == team1 else 0,
        "toss_decision_bat":       1 if "bat" in toss_dec else 0,
        "h2h_team1_wins":          float(_h2h(team1, team2)),
        "h2h_team2_wins":          float(_h2h(team2, team1)),
        "chase_win_pct_team1":     _chase(team1),
        "chase_win_pct_team2":     _chase(team2),
        "high_score_chase_t1":     _chase(team1),
        "high_score_chase_t2":     _chase(team2),
        "winrate_team1":           _wr(team1),
        "winrate_team2":           _wr(team2),
        "last5_win_team1":         _l5(team1),
        "last5_win_team2":         _l5(team2),
        "t1_recent_avg_score":     get_team_recent_avg_score(team1, today),
        "t2_recent_avg_score":     get_team_recent_avg_score(team2, today),
        "t1_high_score_rate":      get_team_recent_high_score_rate(team1, today),
        "t2_high_score_rate":      get_team_recent_high_score_rate(team2, today),
        "t1_pp_bowling_economy":   float(team_pp_eco_lookup.get(team1, 7.5)),
        "t2_pp_bowling_economy":   float(team_pp_eco_lookup.get(team2, 7.5)),
        "season_avg_score":        get_season_avg_score(today),
        "season_year":             get_season_year(today),
        "t1_avg_batting_avg":      0.0, "t1_avg_strike_rate":    0.0,
        "t1_top3_batting_avg":     0.0, "t1_avg_economy":        0.0,
        "t1_avg_bowling_avg":      0.0, "t1_recent_strike_rate": 0.0,
        "t1_recent_economy":       0.0, "t2_avg_batting_avg":    0.0,
        "t2_avg_strike_rate":      0.0, "t2_top3_batting_avg":   0.0,
        "t2_avg_economy":          0.0, "t2_avg_bowling_avg":    0.0,
        "t2_recent_strike_rate":   0.0, "t2_recent_economy":     0.0,
        "t1_opener_batting_avg":   float(team_oppener_lookup.get(team1, {}).get("opener_avg_batting_avg", 0.0)),
        "t1_opener_strike_rate":   float(team_oppener_lookup.get(team1, {}).get("opener_avg_strike_rate", 0.0)),
        "t2_opener_batting_avg":   float(team_oppener_lookup.get(team2, {}).get("opener_avg_batting_avg", 0.0)),
        "t2_opener_strike_rate":   float(team_oppener_lookup.get(team2, {}).get("opener_avg_strike_rate", 0.0)),
        "t1_bat_vs_bowl":          0.0, "t2_bat_vs_bowl":        0.0,
        "t1_rolling_season_avg":   get_season_avg_score(today),
        "t2_rolling_season_avg":   get_season_avg_score(today),
    }

    df = pd.DataFrame([row])
    return df.reindex(columns=feature_cols, fill_value=0.0).fillna(0.0)


# ─────────────────────────────────────────────────────────
# CLI — python -m scraper.espncricinfo_scraper
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json, sys

    if len(sys.argv) > 1:
        mid = int(sys.argv[1])
        print(f"[CLI] Using provided match_id: {mid}")
    else:
        mid = get_todays_match_id()

    if mid:
        result = scrape_match(mid)
        print(json.dumps(result, indent=4, default=str))
    else:
        print("No match found. Add today's match to KNOWN_MATCHES or pass match_id as arg.")
        print("Usage: python -m scraper.espncricinfo_scraper 1529286")