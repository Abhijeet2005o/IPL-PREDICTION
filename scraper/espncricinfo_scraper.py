import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Try cricdata ──────────────────────────────────────────
try:
    from cricdata import CricinfoClient
    CRICINFO_CLIENT = CricinfoClient()
    CRICDATA_AVAILABLE = True
except Exception:
    CRICINFO_CLIENT = None
    CRICDATA_AVAILABLE = False

# ════════════════════════════════════════════════════════════
# ✅ IPL-LOCKED CONSTANTS
# All URLs point directly to the IPL 2026 series.
# No global cricket feeds are used anywhere in this file.
# ════════════════════════════════════════════════════════════

IPL_SERIES_ID = "1510719"

# ✅ Cricbuzz live scores filtered to this IPL series ONLY
IPL_LIVE_SCORES_URL = (
    f"https://www.cricbuzz.com/cricket-match/live-scores/series/{IPL_SERIES_ID}"
)

# ✅ Cricbuzz IPL series schedule — used as fallback when no live match
IPL_SCHEDULE_URL = (
    f"https://www.cricbuzz.com/cricket-series/{IPL_SERIES_ID}"
    f"/indian-premier-league-2026/matches"
)

# ✅ Per-match URLs — only ever called with IPL match IDs
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
MATCH_SQUADS_URL   = "https://www.cricbuzz.com/live-cricket-scorecard/{match_id}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ════════════════════════════════════════════════════════════
# ✅ IPL TEAM DEFINITIONS
# ════════════════════════════════════════════════════════════

TEAM_ALIASES = {
    "CSK":  "Chennai Super Kings",
    "DC":   "Delhi Capitals",
    "DD":   "Delhi Capitals",
    "GL":   "Gujarat Lions",
    "GT":   "Gujarat Titans",
    "KKR":  "Kolkata Knight Riders",
    "LSG":  "Lucknow Super Giants",
    "MI":   "Mumbai Indians",
    "PBKS": "Punjab Kings",
    "KXIP": "Punjab Kings",
    "RR":   "Rajasthan Royals",
    "RCB":  "Royal Challengers Bangalore",
    "SRH":  "Sunrisers Hyderabad",
    "RPS":  "Rising Pune Supergiant",
    "PWI":  "Pune Warriors",
}

TEAM_ABBREVIATIONS = {
    "CSK":  "Chennai Super Kings",
    "DC":   "Delhi Capitals",
    "DD":   "Delhi Capitals",
    "GL":   "Gujarat Lions",
    "GT":   "Gujarat Titans",
    "KKR":  "Kolkata Knight Riders",
    "LSG":  "Lucknow Super Giants",
    "MI":   "Mumbai Indians",
    "PBKS": "Punjab Kings",
    "KXIP": "Punjab Kings",
    "RR":   "Rajasthan Royals",
    "RCB":  "Royal Challengers Bangalore",
    "SRH":  "Sunrisers Hyderabad",
    "RPS":  "Rising Pune Supergiant",
    "PWI":  "Pune Warriors",
}

# ✅ Whitelist — only these 10 active franchises are valid for 2026
VALID_IPL_TEAMS = {
    "Chennai Super Kings",
    "Delhi Capitals",
    "Gujarat Titans",
    "Kolkata Knight Riders",
    "Lucknow Super Giants",
    "Mumbai Indians",
    "Punjab Kings",
    "Rajasthan Royals",
    "Royal Challengers Bangalore",
    "Sunrisers Hyderabad",
}

TEAM_NAME_CORRECTIONS = {
    "Royal Challengers Bengaluru":   "Royal Challengers Bangalore",
    "royal challengers bengaluru":   "Royal Challengers Bangalore",
    "Royal Challengers Bengaluru ":  "Royal Challengers Bangalore",
    "RCB Bangalore":                 "Royal Challengers Bangalore",
}

TEAM_TO_ABBR = {v: k for k, v in TEAM_ABBREVIATIONS.items()}

# All IPL team keywords (full names + abbreviations) in lowercase
# Used for fast page-level IPL verification
IPL_TEAM_KEYWORDS = {name.lower() for name in VALID_IPL_TEAMS}
IPL_TEAM_KEYWORDS.update({abbr.lower() for abbr in TEAM_ABBREVIATIONS})

KNOWN_XI: Dict[int, Dict[str, List[str]]] = {}


# ════════════════════════════════════════════════════════════
# ✅ IPL VALIDATION GATE
# These two functions are the primary defence against
# non-IPL data (BAN, PAK, etc.) ever reaching the ML model.
# ════════════════════════════════════════════════════════════

def validate_ipl_teams(team1: str, team2: str) -> bool:
    """
    Returns True only if BOTH teams are active IPL franchises.

    Call immediately after scraping team names.
    If this returns False → show a UI error and st.stop().
    """
    t1_ok = team1 in VALID_IPL_TEAMS
    t2_ok = team2 in VALID_IPL_TEAMS

    if not t1_ok:
        print(f"[VALIDATION] ❌ '{team1}' is NOT a valid IPL team.")
    if not t2_ok:
        print(f"[VALIDATION] ❌ '{team2}' is NOT a valid IPL team.")

    if t1_ok and t2_ok:
        print(f"[VALIDATION] ✅ {team1} vs {team2} — confirmed IPL match.")
        return True

    return False


def verify_page_is_ipl(soup: BeautifulSoup) -> bool:
    """
    Returns True if the Cricbuzz page belongs to an IPL match.
    Checks for series name text OR ≥2 IPL team keyword hits.

    Called at the top of every scraping function so a wrong
    match_id can never pollute any data.
    """
    page_text = soup.get_text().lower()

    if "indian premier league" in page_text or "ipl 2026" in page_text or "ipl 2025" in page_text:
        print("[IPL-VERIFY] ✅ IPL confirmed via series name.")
        return True

    hits = sum(1 for kw in IPL_TEAM_KEYWORDS if kw in page_text)
    if hits >= 2:
        print(f"[IPL-VERIFY] ✅ IPL confirmed via {hits} team keyword matches.")
        return True

    print("[IPL-VERIFY] ❌ Page is NOT an IPL match.")
    return False


# ════════════════════════════════════════════════════════════
# UTILITY HELPERS
# ════════════════════════════════════════════════════════════

def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _request_soup(url: str, timeout: int = 20) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _correct_team_name(name: str) -> str:
    name = _clean_text(name)
    if not name:
        return name
    if name in TEAM_NAME_CORRECTIONS:
        return TEAM_NAME_CORRECTIONS[name]
    for wrong, right in TEAM_NAME_CORRECTIONS.items():
        if wrong.lower() == name.lower():
            return right
    return name


def _normalize_team_name(name: str, team_encoder=None) -> str:
    name = _correct_team_name(_clean_text(name))
    if not name:
        return name
    upper = name.upper()
    if upper in TEAM_ALIASES:
        return TEAM_ALIASES[upper]
    if name in TEAM_ALIASES:
        return TEAM_ALIASES[name]
    if team_encoder is not None:
        classes = list(team_encoder.classes_)
        if name in classes:
            return name
        for cls in classes:
            if cls.lower() == name.lower():
                return cls
    return name


def _safe_encode(encoder, value: str) -> int:
    classes = list(encoder.classes_)
    if value in classes:
        return int(encoder.transform([value])[0])
    for cls in classes:
        if cls.lower() == value.lower():
            return int(encoder.transform([cls])[0])
    return int(encoder.transform([classes[0]])[0])


def _safe_div(num: float, den: float, fallback: float) -> float:
    return float(num / den) if den else float(fallback)


def _extract_player_name(text: str) -> str:
    """Remove (c), (wk), †, * etc. from a player name string."""
    name = re.sub(r'\s*[\(\[].*?[\)\]]', '', text)
    name = re.sub(r'[†*]', '', name)
    return _clean_text(name)


# ════════════════════════════════════════════════════════════
# ✅ LIVE MATCH DETECTION — series-locked to IPL
# ════════════════════════════════════════════════════════════

def get_todays_match_id() -> Optional[int]:
    """
    Public entry point called by app.py.
    Returns the live IPL match ID for today, or None.

    Uses IPL_LIVE_SCORES_URL (series-locked) so non-IPL matches
    are physically impossible to return.
    """
    match_id = _get_live_ipl_match_id()
    if match_id:
        return match_id
    print("[MATCH-ID] No live match. Falling back to latest scheduled.")
    return _get_latest_ipl_match_id_from_schedule()


def _get_live_ipl_match_id() -> Optional[int]:
    """
    Fetches Cricbuzz IPL-series live scores page and returns
    the first live match ID found.
    URL: /cricket-match/live-scores/series/{IPL_SERIES_ID}
    """
    try:
        print(f"[LIVE-DETECT] Fetching: {IPL_LIVE_SCORES_URL}")
        soup = _request_soup(IPL_LIVE_SCORES_URL)

        for link in soup.select("a[href*='/live-cricket-scores/']"):
            href = link.get("href", "")
            m = re.search(r'/live-cricket-scores/(\d+)', href)
            if m:
                match_id = int(m.group(1))
                print(f"[LIVE-DETECT] ✅ Live IPL match ID: {match_id}")
                return match_id

        print("[LIVE-DETECT] ⚠️ No live IPL match found.")
    except Exception as e:
        print(f"[LIVE-DETECT] Error: {e}")
    return None


def _get_latest_ipl_match_id_from_schedule() -> Optional[int]:
    """
    Fallback: scrapes the IPL series schedule page and returns
    the most recent match ID. Still series-locked — no risk of
    picking up non-IPL matches.
    """
    try:
        print(f"[SCHEDULE] Fetching: {IPL_SCHEDULE_URL}")
        soup = _request_soup(IPL_SCHEDULE_URL)

        match_ids = []
        for link in soup.select(
            "a[href*='/live-cricket-scores/'], a[href*='/cricket-scores/']"
        ):
            href = link.get("href", "")
            m = re.search(r'/(?:live-cricket-scores|cricket-scores)/(\d+)', href)
            if m:
                match_ids.append(int(m.group(1)))

        if match_ids:
            latest = match_ids[-1]
            print(f"[SCHEDULE] ✅ Latest IPL match ID: {latest}")
            return latest

        print("[SCHEDULE] ⚠️ No match IDs on schedule page.")
    except Exception as e:
        print(f"[SCHEDULE] Error: {e}")
    return None


# ════════════════════════════════════════════════════════════
# TEAM NAME EXTRACTION FROM MATCH PAGE
# ════════════════════════════════════════════════════════════

def get_teams_from_cricbuzz(match_id: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Scrapes team names from a Cricbuzz match page.
    ✅ Verifies the page is IPL.
    ✅ Validates both teams are known IPL franchises.
    Returns (team1, team2) or (None, None) on failure.
    """
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-TEAMS] Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        if not verify_page_is_ipl(soup):
            print(f"[CB-TEAMS] ❌ match_id={match_id} is not IPL. Aborting.")
            return None, None

        team_names: List[str] = []

        for sel in [
            ".cb-nav-main .cb-col-50",
            ".cb-minfo-tm-nm",
            ".cb-teams-wrp .cb-col",
            "a[href*='/cricket-team/']",
        ]:
            for el in soup.select(sel):
                name = _normalize_team_name(_clean_text(el.get_text()))
                if name in VALID_IPL_TEAMS and name not in team_names:
                    team_names.append(name)
            if len(team_names) == 2:
                break

        # Fallback: scan page title
        if len(team_names) < 2:
            title = _clean_text(soup.title.get_text() if soup.title else "")
            for ipl_team in VALID_IPL_TEAMS:
                if ipl_team.lower() in title.lower() and ipl_team not in team_names:
                    team_names.append(ipl_team)

        if len(team_names) == 2 and validate_ipl_teams(team_names[0], team_names[1]):
            return team_names[0], team_names[1]

        print(f"[CB-TEAMS] ⚠️ Could not extract 2 IPL teams. Found: {team_names}")
        return None, None

    except Exception as e:
        print(f"[CB-TEAMS] Error: {e}")
        return None, None


# ════════════════════════════════════════════════════════════
# ESPN CRICINFO (series-filtered)
# ════════════════════════════════════════════════════════════

def _get_espn_live_match(match_id: Optional[int] = None) -> Optional[dict]:
    """Fetches ESPN Cricinfo live data, filtered to IPL series only."""
    if not CRICDATA_AVAILABLE or CRICINFO_CLIENT is None:
        return None
    try:
        live = CRICINFO_CLIENT.live_matches()
        candidates = []
        for match in live:
            series      = match.get("series", {})
            series_id   = str(series.get("objectId", "")).strip()
            series_name = _clean_text(series.get("longName", "")).lower()
            if (series_id == str(IPL_SERIES_ID).strip()
                    or "indian premier league" in series_name):
                candidates.append(match)

        if not candidates:
            print("[ESPN] ⚠️ No live IPL matches via ESPN.")
            return None

        if match_id is None:
            return candidates[0]

        for m in candidates:
            if str(m.get("objectId", "")).strip() == str(match_id).strip():
                return m

    except Exception as e:
        print(f"[ESPN] Error: {e}")
    return None


def _extract_xi_from_scorecard(scorecard: dict) -> Dict[str, List[str]]:
    """Extract Playing XI from ESPN scorecard. Skips non-IPL teams."""
    team_xi: Dict[str, List[str]] = {}
    try:
        team_players = (
            scorecard.get("content", {})
            .get("matchPlayers", {})
            .get("teamPlayers", [])
        )
        for entry in team_players:
            team_name = _correct_team_name(
                _clean_text(entry.get("team", {}).get("longName", ""))
            )
            if team_name not in VALID_IPL_TEAMS:
                print(f"[ESPN-XI] ⚠️ Skipping non-IPL team: {team_name}")
                continue
            players = entry.get("players", []) or []
            names   = [_clean_text(p.get("player", {}).get("longName", "")) for p in players]
            names   = [n for n in names if n]
            if team_name and names:
                team_xi[team_name] = names[:11]
                print(f"[ESPN-XI] ✅ {len(names)} players for {team_name}")
    except Exception as e:
        print(f"[ESPN-XI] Error: {e}")
    return team_xi


def _extract_toss_from_espn_info(
    info: dict, team1: str, team2: str
) -> Tuple[str, Optional[str]]:
    if not isinstance(info, dict):
        return "", None

    def deep_find_toss(obj, depth=0, max_depth=8):
        if depth > max_depth:
            return None
        if isinstance(obj, dict):
            for candidate in ["toss", "tossResults", "tossResult", "tossInfo"]:
                if candidate in obj:
                    val = obj[candidate]
                    if isinstance(val, dict) and val:
                        return val
            has_winner   = any(k.lower() in {"tosswinner", "tosswinnerid", "winnerid", "winner"} for k in obj)
            has_decision = any(k.lower() in {"decision", "tossdecision", "elected", "choice"} for k in obj)
            if has_winner and has_decision:
                return obj
            for v in obj.values():
                result = deep_find_toss(v, depth + 1, max_depth)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = deep_find_toss(item, depth + 1, max_depth)
                if result:
                    return result
        return None

    toss = deep_find_toss(info)
    if not toss:
        return "", None

    tw_raw = _clean_text(
        toss.get("winner_team", "") or toss.get("tossWinner", "") or toss.get("winner", "") or ""
    )
    td_raw = _clean_text(
        toss.get("decision", "") or toss.get("tossDecision", "") or ""
    ).lower()

    td = None
    if "bat" in td_raw:
        td = "bat"
    elif "field" in td_raw or "bowl" in td_raw:
        td = "field"

    tw = ""
    if tw_raw:
        for candidate in [team1, team2]:
            if candidate and candidate.lower() in tw_raw.lower():
                tw = candidate
                break

    return tw, td


# ════════════════════════════════════════════════════════════
# PLAYING XI EXTRACTION — Cricbuzz (IPL-validated)
# ════════════════════════════════════════════════════════════

def get_playing_xi_from_cricbuzz(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz match page.
    ✅ verify_page_is_ipl() runs before any parsing.
    Tries 5 methods in order, stops early if both XIs found.
    """
    team_xi: Dict[str, List[str]] = {}

    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-XI] Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup      = BeautifulSoup(resp.text, "html.parser")
        page_text = resp.text

        # ✅ GUARD
        if not verify_page_is_ipl(soup):
            print(f"[CB-XI] ❌ match_id={match_id} is not IPL. Aborting.")
            return {}

        # ── Method 1: match info items ───────────────────────
        for item in soup.select(".cb-mtch-info-itm"):
            label_div = item.select_one(".cb-col.cb-col-27")
            value_div = item.select_one(".cb-col.cb-col-73")
            if not (label_div and value_div):
                continue
            label_text = _clean_text(label_div.get_text()).lower()
            if not any(x in label_text for x in ["squad", "playing", "xi", "team"]):
                continue
            current_team = None
            for t in [team1, team2]:
                if t and t.lower() in label_text:
                    current_team = t
                    break
            if current_team:
                names = [
                    _extract_player_name(lnk.get_text())
                    for lnk in value_div.select("a")
                ]
                names = [n for n in names if n and len(n) > 2][:11]
                if names:
                    team_xi[current_team] = names
                    print(f"[CB-XI] M1: {len(names)} players for {current_team}")

        if len(team_xi) >= 2:
            return team_xi

        # ── Method 2: Playing XI section headers ─────────────
        for header in soup.select(".cb-col.cb-col-100.cb-font-14, .cb-minfo-tm-nm"):
            header_text  = _clean_text(header.get_text())
            current_team = None
            for t in [team1, team2]:
                if not t:
                    continue
                abbr = TEAM_TO_ABBR.get(t, "").lower()
                if (t.lower() in header_text.lower() or
                        (abbr and abbr in header_text.lower())):
                    if "playing" in header_text.lower() or "xi" in header_text.lower():
                        current_team = t
                        break
            if current_team and current_team not in team_xi:
                parent = header.find_parent()
                if parent:
                    links = parent.select("a[href*='/profiles/']") or \
                            parent.select("a[href*='/cricket-player/']")
                    names = [_extract_player_name(lnk.get_text()) for lnk in links]
                    names = [n for n in names if n and len(n) > 2][:11]
                    if names:
                        team_xi[current_team] = names
                        print(f"[CB-XI] M2: {len(names)} players for {current_team}")

        if len(team_xi) >= 2:
            return team_xi

        # ── Method 3: regex on raw page text ─────────────────
        xi_patterns = [
            r'([A-Za-z\s]+?)\s*\(?\s*Playing\s*XI\s*\)?\s*:?\s*([A-Za-z\s,\.]+?)(?=\n|$|[A-Z][a-z]+\s*\()',
            r'([A-Za-z\s]+?)\s+XI\s*:?\s*([A-Za-z\s,\.]+?)(?=\n|$)',
        ]
        for pattern in xi_patterns:
            for match in re.findall(pattern, page_text, re.IGNORECASE | re.MULTILINE):
                team_name_raw = _clean_text(match[0])
                players_str   = match[1]
                current_team  = None
                for t in [team1, team2]:
                    if t and (t.lower() in team_name_raw.lower() or
                              team_name_raw.lower() in t.lower()):
                        current_team = t
                        break
                if current_team and current_team not in team_xi:
                    players = [
                        _extract_player_name(p.strip())
                        for p in players_str.split(",")
                    ]
                    players = [p for p in players if p and len(p) > 2][:11]
                    if len(players) >= 5:
                        team_xi[current_team] = players
                        print(f"[CB-XI] M3: {len(players)} players for {current_team}")

        if len(team_xi) >= 2:
            return team_xi

        # ── Method 4: squad divs ─────────────────────────────
        for section in soup.select(".cb-play11-lft-col, .cb-minfo-tm-plyr"):
            parent = section.find_parent(class_=re.compile(r'cb-col'))
            if not parent:
                continue
            section_text = _clean_text(parent.get_text())
            current_team = None
            for t in [team1, team2]:
                if t and t.lower() in section_text.lower():
                    current_team = t
                    break
            if current_team and current_team not in team_xi:
                names = [
                    _extract_player_name(lnk.get_text())
                    for lnk in section.select("a")
                ]
                names = [n for n in names if n and len(n) > 2][:11]
                if len(names) >= 5:
                    team_xi[current_team] = names
                    print(f"[CB-XI] M4: {len(names)} players for {current_team}")

        if len(team_xi) >= 2:
            return team_xi

        # ── Method 5: "opt to" abbreviation pattern ───────────
        opt_pattern = (
            r'([A-Z]{2,4})\s+opt\s+to\s+(?:bat|bowl|field)[.\s]+'
            r'\1\s*:?\s*([A-Za-z\s,]+?)(?=[A-Z]{2,4}\s*:|$)'
        )
        for match in re.findall(opt_pattern, page_text, re.IGNORECASE):
            abbr        = match[0].upper()
            players_str = match[1]
            if abbr not in TEAM_ABBREVIATIONS:
                continue
            team_full    = TEAM_ABBREVIATIONS[abbr]
            current_team = None
            for t in [team1, team2]:
                if t and t.lower() == team_full.lower():
                    current_team = t
                    break
            if current_team and current_team not in team_xi:
                players = [
                    _extract_player_name(p.strip())
                    for p in players_str.split(",")
                ]
                players = [p for p in players if p and len(p) > 2][:11]
                if len(players) >= 5:
                    team_xi[current_team] = players
                    print(f"[CB-XI] M5: {len(players)} players for {current_team}")

    except Exception as e:
        print(f"[CB-XI] Error: {e}")

    return team_xi


def get_playing_xi_from_scorecard(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz scorecard page.
    ✅ verify_page_is_ipl() runs before any parsing.
    """
    team_xi: Dict[str, List[str]] = {}

    try:
        url = MATCH_SQUADS_URL.format(match_id=match_id)
        print(f"[CB-SCORE] Fetching: {url}")
        soup = _request_soup(url)

        # ✅ GUARD
        if not verify_page_is_ipl(soup):
            print(f"[CB-SCORE] ❌ match_id={match_id} is not IPL. Aborting.")
            return {}

        current_team = None
        for block in soup.select(".cb-col.cb-col-100.cb-ltst-wgt-hdr"):
            block_text = _clean_text(block.get_text())
            for t in [team1, team2]:
                if not t:
                    continue
                abbr = TEAM_TO_ABBR.get(t, "").lower()
                if (t.lower() in block_text.lower() or
                        (abbr and abbr in block_text.lower())):
                    if "innings" in block_text.lower():
                        current_team = t
                        break

            if current_team and current_team not in team_xi:
                parent = block.find_parent()
                if parent:
                    names = []
                    for row in parent.select(".cb-col.cb-col-100.cb-scrd-itms"):
                        lnk = row.select_one("a.cb-text-link")
                        if lnk:
                            name = _extract_player_name(lnk.get_text())
                            if name and len(name) > 2 and name not in names:
                                names.append(name)
                    if names:
                        team_xi[current_team] = names[:11]
                        print(f"[CB-SCORE] {len(names)} batsmen for {current_team}")

        # Supplement bowling side
        for row in soup.select(".cb-col.cb-col-100.cb-scrd-itms"):
            bowler_div = row.select_one(".cb-col.cb-col-40")
            if not bowler_div:
                continue
            bowler_lnk = bowler_div.select_one("a")
            if not bowler_lnk:
                continue
            name = _extract_player_name(bowler_lnk.get_text())
            if not (name and len(name) > 2):
                continue
            for t in [team1, team2]:
                if t in team_xi:
                    other = team2 if t == team1 else team1
                    if other not in team_xi:
                        team_xi[other] = []
                    if name not in team_xi[other] and len(team_xi[other]) < 11:
                        team_xi[other].append(name)

    except Exception as e:
        print(f"[CB-SCORE] Error: {e}")

    return team_xi


# ════════════════════════════════════════════════════════════
# TOSS DETECTION (IPL-validated)
# ════════════════════════════════════════════════════════════

def get_toss_from_cricbuzz(
    match_id: int, team1: str, team2: str
) -> Tuple[str, Optional[str]]:
    """
    Extract toss info from Cricbuzz match page.
    ✅ verify_page_is_ipl() runs before any parsing.
    Returns (toss_winner_team_name, 'bat'|'field') or ("", None).
    """
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-TOSS] Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup      = BeautifulSoup(resp.text, "html.parser")
        page_text = resp.text

        # ✅ GUARD
        if not verify_page_is_ipl(soup):
            print(f"[CB-TOSS] ❌ match_id={match_id} is not IPL. Aborting.")
            return "", None

        opt_patterns = [
            r'([A-Z]{2,4})\s+opt\s+to\s+(bat|bowl|field)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+opt\s+to\s+(bat|bowl|field)',
        ]

        for pattern in opt_patterns:
            for match in re.findall(pattern, page_text, re.I):
                team_part = match[0].strip()
                decision  = match[1].lower().strip()

                toss_decision = "bat" if decision == "bat" else "field"
                toss_winner   = None

                team_abbr = team_part.upper()
                if team_abbr in TEAM_ABBREVIATIONS:
                    team_full = TEAM_ABBREVIATIONS[team_abbr]
                    if team1 and team1.lower() == team_full.lower():
                        toss_winner = team1
                    elif team2 and team2.lower() == team_full.lower():
                        toss_winner = team2

                if not toss_winner:
                    for team in [team1, team2]:
                        if team and (team.lower() in team_part.lower() or
                                     team_part.lower() in team.lower()):
                            toss_winner = team
                            break

                if toss_winner:
                    print(f"[CB-TOSS] ✅ {toss_winner} opted to {toss_decision}")
                    return toss_winner, toss_decision

        print("[CB-TOSS] ⚠️ Toss not detected on page.")

    except Exception as e:
        print(f"[CB-TOSS] Error: {e}")

    return "", None
