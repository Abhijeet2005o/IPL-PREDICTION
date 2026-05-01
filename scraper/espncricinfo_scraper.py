# scraper/espncricinfo_scraper.py
# ─────────────────────────────────────────────────────────
# COMPLETE REWRITE — v3.0
#
# ROOT CAUSE OF PREVIOUS ERRORS:
#   hs-consumer-api.espncricinfo.com returns HTTP 403 for ALL
#   non-browser clients. The old code retried it 3× per series ID
#   across ~6 IDs = 18+ blocked requests just on startup.
#
# WHAT CHANGED:
#   1. hs-consumer-api REMOVED ENTIRELY — it is hard-blocked
#   2. Primary source → ESPN public scoreboard API (no auth, works)
#   3. Secondary source → ESPNcricinfo HTML with real browser headers
#   4. Zero HTTP calls at import time (lazy, on-demand only)
#   5. Fast-fail on 403 — don't waste retries on a hard block
#   6. Series ID hardcoded to 1510719 (confirmed IPL 2026)
# ─────────────────────────────────────────────────────────

import os
import re
import time
import random
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ── IPL 2026 confirmed series ID ──────────────────────────
# Verified from: espncricinfo.com/series/ipl-2026-1510719
IPL_SERIES_ID = os.getenv("IPL_SERIES_ID", "1510719")

# ── Persistent session ────────────────────────────────────
SESSION = requests.Session()

# ── Real browser user-agents ──────────────────────────────
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


# ─────────────────────────────────────────────────────────
# LOW-LEVEL FETCH HELPERS
# ─────────────────────────────────────────────────────────

def _get_json(url, retries=2):
    """
    Fetch JSON with minimal retries.
    FAST-FAIL on 403/404 — these are hard errors, not transient.
    """
    for attempt in range(retries):
        try:
            res = SESSION.get(url, headers=_json_headers(), timeout=12)
            if res.status_code == 200:
                return res.json()
            if res.status_code in (401, 403, 404):
                print(f"[scraper] Hard block {res.status_code} — skipping: {url}")
                return None                          # ← no retry
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
    """
    Fetch HTML page. Fast-fail on 403.
    """
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
    """
    Last-resort headless Chrome for JS-rendered pages.
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options

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
    except Exception as exc:
        print(f"[scraper] Selenium error: {exc}")
        return None


# ─────────────────────────────────────────────────────────
# SOURCE 1 — ESPN Public Scoreboard API  ✅ (unblocked)
# https://site.api.espn.com — public, no auth required
# ─────────────────────────────────────────────────────────

_ESPN_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/cricket/ipl/scoreboard"
)


def _espn_best_event():
    """
    Fetch ESPN scoreboard and return the best current event dict.
    Priority: live ("in") > upcoming ("pre") > most recent ("post").
    Returns (event_id_str, event_dict) or (None, None).
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
        )
    )
    best = events_sorted[0]
    eid = best.get("id", "")
    state = best.get("status", {}).get("type", {}).get("state", "?")
    print(f"[scraper] ESPN API best event: id={eid}, state={state}")
    return str(eid), best


def _espn_parse_event(event):
    """
    Parse an ESPN event dict into a match_info dict.
    """
    if not event:
        return {}

    competitions = event.get("competitions", [event])
    comp = competitions[0] if competitions else {}
    competitors = comp.get("competitors", [])

    team1, team2 = "", ""
    if len(competitors) >= 2:
        team1 = competitors[0].get("team", {}).get("displayName", "")
        team2 = competitors[1].get("team", {}).get("displayName", "")

    venue_obj = comp.get("venue", {})
    venue_str = (
        venue_obj.get("fullName", "")
        or venue_obj.get("address", {}).get("city", "")
    )

    toss_winner, toss_decision, chasing_team = "", "", ""
    toss_done = False

    for note in comp.get("notes", []):
        text = note.get("text", "")
        m = re.search(
            r'(.+?)\s+won the toss and elected to\s+(bat|bowl|field)',
            text, re.IGNORECASE,
        )
        if m:
            toss_winner = m.group(1).strip()
            raw = m.group(2).lower()
            toss_decision = "bat" if raw == "bat" else "field"
            toss_done = True
            if toss_decision == "bat":
                chasing_team = team2 if toss_winner == team1 else team1
            else:
                chasing_team = toss_winner
            break

    return {
        "team1":        team1,
        "team2":        team2,
        "venue":        venue_str,
        "match_status": event.get("status", {}).get("type", {}).get("description", "Scheduled"),
        "toss_winner":  toss_winner,
        "toss_decision": toss_decision,
        "toss_done":    toss_done,
        "chasing_team": chasing_team,
        "match_id":     str(event.get("id", "")),
    }


# ─────────────────────────────────────────────────────────
# SOURCE 2 — ESPNcricinfo Schedule Page HTML  🌐
# ─────────────────────────────────────────────────────────

def _schedule_url():
    return (
        f"https://www.espncricinfo.com/series/ipl-2026-{IPL_SERIES_ID}"
        "/match-schedule-fixtures-and-results"
    )


def _html_find_match_id():
    """
    Scrape the public schedule page for today's or nearest upcoming match ID.
    """
    url = _schedule_url()
    html = _get_html(url)
    if not html:
        print("[scraper] Schedule HTML blocked, trying Selenium…")
        html = _get_html_selenium(url)
    if not html:
        return None

    def _parse_date_from_text(text):
        if not text:
            return None

        month_names = {
            'jan': 1, 'january': 1,
            'feb': 2, 'february': 2,
            'mar': 3, 'march': 3,
            'apr': 4, 'april': 4,
            'may': 5,
            'jun': 6, 'june': 6,
            'jul': 7, 'july': 7,
            'aug': 8, 'august': 8,
            'sep': 9, 'sept': 9, 'september': 9,
            'oct': 10, 'october': 10,
            'nov': 11, 'november': 11,
            'dec': 12, 'december': 12,
        }

        patterns = [
            r'\b(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(?P<day>\d{1,2})(?:,?\s*(?P<year>\d{4}))?',
            r'\b(?P<day>\d{1,2})\s+(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)(?:,?\s*(?P<year>\d{4}))?',
        ]

        for pattern in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                month = month_names.get(m.group('month').lower())
                day = int(m.group('day'))
                year = int(m.group('year')) if m.group('year') else date.today().year
                if month is None:
                    continue
                if year == date.today().year and month < date.today().month - 6:
                    year += 1
                try:
                    return date(year, month, day)
                except ValueError:
                    continue
        return None

    def _parse_time_from_text(text):
        if not text:
            return None

        m = re.search(r"\b(?P<hour>\d{1,2}):(?P<minute>\d{2})\s*(?P<ampm>[AaPp][Mm])?\b", text)
        if not m:
            return None
        hour = int(m.group('hour'))
        minute = int(m.group('minute'))
        ampm = m.group('ampm')
        if ampm:
            if ampm.lower() == 'pm' and hour != 12:
                hour += 12
            elif ampm.lower() == 'am' and hour == 12:
                hour = 0
        return hour, minute

    def _parse_match_datetime(text):
        match_date = _parse_date_from_text(text)
        if not match_date:
            return None
        time_parts = _parse_time_from_text(text)
        if time_parts:
            hour, minute = time_parts
        else:
            hour, minute = 12, 0
        return datetime(match_date.year, match_date.month, match_date.day, hour, minute)

    soup = BeautifulSoup(html, "html.parser")

    seen, candidates, matches_found = set(), [], []
    now = datetime.now()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r'/series/ipl-2026-\d+/.+-(\d{7,})/', href)
        if not m:
            m = re.search(r'[/-](\d{7,})(?:/|$)', href)
        if not m:
            continue

        mid = int(m.group(1))
        if mid in seen:
            continue
        seen.add(mid)
        matches_found.append(mid)

        match_dt = None
        container = a
        for _ in range(4):
            text = container.get_text(" ", strip=True)
            match_dt = _parse_match_datetime(text)
            if match_dt:
                break
            container = container.parent
            if container is None:
                break

        if match_dt:
            candidates.append((match_dt, mid))

    if candidates:
        future = [item for item in candidates if item[0] >= now]
        chosen = min(future or candidates, key=lambda item: item[0])
        print(
            f"[scraper] Schedule HTML found {len(candidates)} dated match IDs, "
            f"using: {chosen[1]} @ {chosen[0].isoformat()}"
        )
        return chosen[1]

    if matches_found:
        print(f"[scraper] Schedule HTML found {len(matches_found)} match IDs, using: {matches_found[0]}")
        return matches_found[0]

    return None


# ─────────────────────────────────────────────────────────
# SOURCE 3 — ESPNcricinfo Match Page HTML  🌐
# ─────────────────────────────────────────────────────────

_TEAM_KW = (
    r"Indians|Royals|Kings|Capitals|Knights|Titans|Giants|Sunrisers|"
    r"Challengers|Lucknow|Punjab|Gujarat|Mumbai|Delhi|Rajasthan|"
    r"Kolkata|Chennai|Hyderabad|Bangalore|Supergiants"
)

def _html_match_info(match_id):
    url = (
        f"https://www.espncricinfo.com/series/ipl-2026-{IPL_SERIES_ID}"
        f"/{match_id}/live-cricket-score"
    )
    html = _get_html(url)
    if not html:
        print(f"[scraper] Match page blocked, trying Selenium for match {match_id}…")
        html = _get_html_selenium(url)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    return _parse_match_page(soup, match_id)


def _parse_match_page(soup, match_id):
    result = {
        "team1": "", "team2": "", "venue": "",
        "match_status": "upcoming",
        "toss_winner": "", "toss_decision": "",
        "toss_done": False, "chasing_team": "",
        "match_id": str(match_id),
    }

    # Teams — try several class patterns
    for cls in ("ds-text-tight-l ds-font-bold", "ds-text-tight-l", "ci-team-score"):
        elems = soup.find_all(class_=cls)
        names = [e.get_text(strip=True) for e in elems if e.get_text(strip=True)]
        if len(names) >= 2:
            result["team1"], result["team2"] = names[0], names[1]
            break

    # Venue
    page_text = soup.get_text(" ", strip=True)
    venue_kw = ["stadium", "ground", "oval", "wankhede", "eden", "chinnaswamy",
                "kotla", "brabourne", "chepauk", "ekana", "hpca", "narendra"]
    for elem in soup.find_all(["span", "div", "p"]):
        text = elem.get_text(strip=True)
        if any(kw in text.lower() for kw in venue_kw) and len(text) < 120:
            result["venue"] = text
            break

    # Toss
    m = re.search(
        rf'([A-Za-z\s]+(?:{_TEAM_KW}))'
        r'\s+won the toss and (?:elected|chose) to\s+(bat|bowl|field)',
        page_text, re.IGNORECASE,
    )
    if m:
        result["toss_winner"] = m.group(1).strip()
        raw = m.group(2).lower()
        result["toss_decision"] = "bat" if raw == "bat" else "field"
        result["toss_done"] = True
        t1, t2 = result["team1"], result["team2"]
        tw = result["toss_winner"]
        result["chasing_team"] = (
            t2 if (result["toss_decision"] == "bat" and tw == t1) else
            t1 if (result["toss_decision"] == "bat" and tw == t2) else
            tw
        )

    if any(kw in page_text.lower() for kw in ("live", "in progress", "batting")):
        result["match_status"] = "live"

    return result


# ─────────────────────────────────────────────────────────
# PUBLIC API  (imported by app.py)
# ─────────────────────────────────────────────────────────

def get_todays_match_id():
    """
    Returns int match_id for today's / next IPL match, or None.
    No calls to hs-consumer-api (hard-blocked).
    """
    # Primary: ESPN public API
    eid, _ = _espn_best_event()
    if eid:
        try:
            return int(eid)
        except (ValueError, TypeError):
            pass

    # Fallback: schedule HTML
    return _html_find_match_id()


def scrape_match(match_id):
    """
    Returns match_info dict for match_id.
    Tries ESPN API event data first, then ESPNcricinfo HTML.
    """
    # Primary: ESPN API (already fetched above, re-fetch to keep stateless)
    eid, event = _espn_best_event()
    if event and str(eid) == str(match_id):
        info = _espn_parse_event(event)
        if info.get("team1") and info.get("team2"):
            print(f"[scraper] Info via ESPN API: {info['team1']} vs {info['team2']}")
            return info
    elif event:
        print(f"[scraper] ESPN API returned event id={eid}, expected {match_id}; falling back to HTML")

    # Fallback: ESPNcricinfo match page HTML
    info = _html_match_info(match_id)
    if info.get("team1"):
        print(f"[scraper] Info via HTML: {info['team1']} vs {info['team2']}")
        return info

    print("[scraper] All sources exhausted.")
    return {}


# ─────────────────────────────────────────────────────────
# FEATURE VECTOR  (logic unchanged, cleaned up)
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
    team1        = match_info.get("team1", "")
    team2        = match_info.get("team2", "")
    venue        = match_info.get("venue", "")
    toss_winner  = match_info.get("toss_winner", "")
    toss_dec     = match_info.get("toss_decision", "").lower()
    today        = pd.Timestamp(date.today())

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
        if not len(tm): return 0.5
        cw = tm[(tm["winner"] == t) &
                (tm.get("win_by_wickets", pd.Series(0, index=tm.index)) > 0)]
        return float(len(cw)) / len(tm)

    def _h2h(a, b):
        h = matches[
            ((matches["team1"] == a) & (matches["team2"] == b)) |
            ((matches["team1"] == b) & (matches["team2"] == a))
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
# CLI
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json
    mid = get_todays_match_id()
    if mid:
        print(json.dumps(scrape_match(mid), indent=4))
    else:
        print("No match found.")