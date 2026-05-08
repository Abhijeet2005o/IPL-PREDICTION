import re
import json
from datetime import datetime

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

IPL_SERIES_ID = "1510719"
LIVE_SCORES_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

TEAM_ALIASES = {
    "CSK": "Chennai Super Kings",
    "DC": "Delhi Capitals",
    "DD": "Delhi Capitals",
    "GL": "Gujarat Lions",
    "GT": "Gujarat Titans",
    "KKR": "Kolkata Knight Riders",
    "LSG": "Lucknow Super Giants",
    "MI": "Mumbai Indians",
    "PBKS": "Punjab Kings",
    "KXIP": "Punjab Kings",
    "RR": "Rajasthan Royals",
    "RCB": "Royal Challengers Bangalore",
    "SRH": "Sunrisers Hyderabad",
    "RPS": "Rising Pune Supergiant",
    "PWI": "Pune Warriors",
}

# Normalise scraped names → canonical CSV names
TEAM_NAME_CORRECTIONS = {
    "Royal Challengers Bengaluru": "Royal Challengers Bangalore",
    "royal challengers bengaluru": "Royal Challengers Bangalore",
    "Royal Challengers Bengaluru ": "Royal Challengers Bangalore",
}

# ─────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────
def _clean_text(value):
    """Clean and normalize text, removing extra whitespace."""
    return re.sub(r"\s+", " ", str(value or "")).strip()

def _request_soup(url, timeout=20):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def _request_json(url, timeout=20):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def _correct_team_name(name):
    """Map variant spellings → canonical CSV name."""
    name = _clean_text(name)
    if not name:
        return name
    corrected = TEAM_NAME_CORRECTIONS.get(name)
    if corrected:
        return corrected
    corrected = TEAM_NAME_CORRECTIONS.get(name.lower())
    if corrected:
        return corrected
    # Case-insensitive scan
    for wrong, right in TEAM_NAME_CORRECTIONS.items():
        if wrong.lower() == name.lower():
            return right
    return name

def _deep_find_toss(obj, depth=0, max_depth=8):
    """
    Recursively walk any dict/list structure looking for toss-related keys.
    Returns the first dict that looks like a toss object.
    """
    if depth > max_depth:
        return None
    if isinstance(obj, dict):
        keys_lower = {k.lower(): k for k in obj}
        # Direct toss key
        for candidate in ["toss", "tossresults", "tossresult", "tossinfo"]:
            if candidate in keys_lower:
                val = obj[keys_lower[candidate]]
                if isinstance(val, dict) and val:
                    print(f"[TOSS-DEEP] Found toss key '{candidate}' at depth {depth}")
                    return val
        # Check if THIS dict looks like a toss object
        has_winner = any(k.lower() in {
            "tosswinner", "tosswinnerId", "tosswinner_id", "winnerid",
            "winner_team", "winner", "toss_winner"
        } for k in obj)
        has_decision = any(k.lower() in {
            "decision", "tossdecision", "toss_decision", "elected", "choice"
        } for k in obj)
        if has_winner and has_decision:
            print(f"[TOSS-DEEP] Found toss-like dict at depth {depth}: {list(obj.keys())}")
            return obj
        # Recurse into values
        for v in obj.values():
            result = _deep_find_toss(v, depth + 1, max_depth)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find_toss(item, depth + 1, max_depth)
            if result:
                return result
    return None

# ─────────────────────────────────────────────────────────
# ESPN CRICINFO
# ─────────────────────────────────────────────────────────
def _get_espn_live_match(match_id=None):
    if not CRICDATA_AVAILABLE or CRICINFO_CLIENT is None:
        return None
    try:
        live = CRICINFO_CLIENT.live_matches()
        candidates = []
        for match in live:
            series = match.get("series", {})
            series_id = str(series.get("objectId", "")).strip()
            series_name = _clean_text(series.get("longName", "")).lower()
            if (series_id == str(IPL_SERIES_ID).strip()
                    or "indian premier league" in series_name):
                candidates.append(match)

        if match_id is None:
            return candidates[0] if candidates else None

        match_id = str(match_id).strip()
        for m in candidates:
            if str(m.get("objectId", "")).strip() == match_id:
                return m
    except Exception as e:
        print(f"[ESPN] _get_espn_live_match error: {e}")
    return None

def _extract_xi_from_scorecard(scorecard):
    team_xi = {}
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
            players = entry.get("players", []) or []
            names = [
                _clean_text(p.get("player", {}).get("longName", ""))
                for p in players
            ]
            names = [n for n in names if n]
            if team_name and names:
                team_xi[team_name] = names[:11]
    except Exception as e:
        print(f"[ESPN] _extract_xi_from_scorecard error: {e}")
    return team_xi

def _extract_toss_from_espn_info(info, team1, team2):
    """Extract toss from ESPN match_info."""
    if not isinstance(info, dict):
        print(f"[ESPN-TOSS] match_info is not a dict: {type(info)}")
        return "", None

    print(f"[ESPN-TOSS] match_info top-level keys: {list(info.keys())}")

    common_paths = [
        ("toss",),
        ("matchInfo", "toss"),
        ("match", "toss"),
        ("content", "toss"),
        ("details", "toss"),
    ]

    toss = None
    for path in common_paths:
        current = info
        for key in path:
            current = current.get(key, {})
            if not isinstance(current, dict):
                break
        if isinstance(current, dict) and current:
            print(f"[ESPN-TOSS] Found toss at path {' > '.join(path)}: {current}")
            toss = current
            break

    if toss is None:
        toss = _deep_find_toss(info)
        print(f"[ESPN-TOSS] Deep search result: {toss}")

    if not toss:
        print("[ESPN-TOSS] No toss object found after deep search")
        return "", None

    tw_raw = _clean_text(
        toss.get("winner_team", "")
        or toss.get("tossWinner", "")
        or toss.get("toss_winner", "")
        or toss.get("winnerTeam", "")
        or toss.get("winner", "")
        or toss.get("tossWinnerId", "")
        or toss.get("tosswinner_id", "")
        or toss.get("winnerId", "")
        or toss.get("winner_id", "")
        or ""
    )

    td_raw = _clean_text(
        toss.get("decision", "")
        or toss.get("tossDecision", "")
        or toss.get("toss_decision", "")
        or toss.get("elected", "")
        or toss.get("choice", "")
        or ""
    ).lower()

    print(f"[ESPN-TOSS] Raw extraction: winner='{tw_raw}' decision='{td_raw}'")

    if "bat" in td_raw:
        td = "bat"
    elif "field" in td_raw or "bowl" in td_raw:
        td = "field"
    elif td_raw in {"1"}:
        td = "bat"
    elif td_raw in {"2"}:
        td = "field"
    else:
        td = None

    tw = ""
    if tw_raw:
        for candidate in [team1, team2]:
            if candidate and candidate.lower() == tw_raw.lower():
                tw = candidate
                break
        if not tw:
            for candidate in [team1, team2]:
                if candidate and candidate.lower() in tw_raw.lower():
                    tw = candidate
                    break
        if not tw:
            for candidate in [team1, team2]:
                if candidate and tw_raw.lower() in candidate.lower():
                    tw = candidate
                    break

    print(f"[ESPN-TOSS] Final result: winner='{tw}' decision='{td}'")
    return tw, td

# ─────────────────────────────────────────────────────────
# CRICBUZZ JSON API
# ─────────────────────────────────────────────────────────
CB_MATCH_INFO_URL = "https://www.cricbuzz.com/api/cricket-match/{match_id}/info"
CB_SCORECARD_URL = "https://www.cricbuzz.com/api/cricket-match/{match_id}/scorecard"
CB_COMMENTARY_URL = "https://www.cricbuzz.com/api/cricket-match/{match_id}/commentary"

def _find_cricbuzz_match_id(team1, team2):
    """Find Cricbuzz match ID by searching for teams."""
    try:
        print(f"[CB-FIND] Searching for match: {team1} vs {team2}")
        soup = _request_soup(LIVE_SCORES_URL)
        
        # Look for links containing team names
        team1_lower = team1.lower()
        team2_lower = team2.lower()
        
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            href = link.get("href", "")
            text = _clean_text(link.get_text()).lower()
            
            # Check if both team names are in the link text
            if team1_lower in text and team2_lower in text:
                match = re.search(r"/live-cricket-scores/(\d+)", href)
                if match:
                    match_id = int(match.group(1))
                    print(f"[CB-FIND] Found match ID: {match_id}")
                    return match_id
        
        # Alternative: search by URL pattern
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            href = link.get("href", "")
            # Check for team abbreviations or names in URL
            if any(t.lower() in href.lower() for t in [team1, team2]):
                match = re.search(r"/live-cricket-scores/(\d+)", href)
                if match:
                    match_id = int(match.group(1))
                    print(f"[CB-FIND] Found match ID by URL: {match_id}")
                    return match_id
        
        print(f"[CB-FIND] Could not find match ID for {team1} vs {team2}")
    except Exception as e:
        print(f"[CB-FIND] Error finding match ID: {e}")
    return None

def _cricbuzz_json_match_info(match_id):
    """Cricbuzz JSON API — match info + toss."""
    try:
        url = CB_MATCH_INFO_URL.format(match_id=match_id)
        print(f"[CB-JSON] Fetching: {url}")
        data = _request_json(url)
        match_info = data.get("matchInfo", {}) or {}

        print(f"[CB-JSON] matchInfo keys: {list(match_info.keys())}")

        team1_obj = match_info.get("team1", {}) or {}
        team2_obj = match_info.get("team2", {}) or {}
        team1 = _correct_team_name(_clean_text(team1_obj.get("name", "")))
        team2 = _correct_team_name(_clean_text(team2_obj.get("name", "")))

        venue_obj = match_info.get("venueInfo", {}) or {}
        venue = _clean_text(
            venue_obj.get("ground", "") + ", " + venue_obj.get("city", "")
        ).strip(", ")

        toss = _deep_find_toss(data)
        print(f"[CB-JSON] Deep-found toss: {toss}")

        toss_winner_id = ""
        toss_decision = ""

        if toss:
            toss_winner_id = str(
                toss.get("tossWinnerId", "")
                or toss.get("tosswinner_id", "")
                or toss.get("winnerId", "")
                or toss.get("winner_id", "")
                or toss.get("tossWinner", "")
                or toss.get("winner", "")
            ).strip()

            toss_decision = _clean_text(
                toss.get("decision", "")
                or toss.get("tossDecision", "")
                or toss.get("toss_decision", "")
                or toss.get("elected", "")
                or toss.get("choice", "")
            ).lower()

        print(f"[CB-JSON] Extracted: toss_winner_id='{toss_winner_id}' "
              f"toss_decision='{toss_decision}'")

        toss_winner = ""
        t1_id = str(team1_obj.get("id", "")).strip()
        t2_id = str(team2_obj.get("id", "")).strip()

        if toss_winner_id:
            if toss_winner_id == t1_id:
                toss_winner = team1
            elif toss_winner_id == t2_id:
                toss_winner = team2
            else:
                for candidate in [team1, team2]:
                    if candidate and candidate.lower() in toss_winner_id.lower():
                        toss_winner = candidate
                        break

        if "bat" in toss_decision:
            toss_decision = "bat"
        elif "field" in toss_decision or "bowl" in toss_decision:
            toss_decision = "field"
        else:
            toss_decision = None

        toss_done = bool(toss_winner and toss_decision)

        chasing_team = None
        if toss_done:
            chasing_team = (
                (team2 if toss_winner == team1 else team1)
                if toss_decision == "bat" else toss_winner
            )

        print(f"[CB-JSON] Final: toss_done={toss_done} "
              f"winner='{toss_winner}' decision='{toss_decision}'")

        return {
            "team1": team1,
            "team2": team2,
            "venue": venue or "Unknown Venue",
            "toss_done": toss_done,
            "toss_winner": toss_winner or None,
            "toss_decision": toss_decision,
            "chasing_team": chasing_team,
            "raw": data,
        }
    except Exception as e:
        print(f"[CB-JSON] Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def _cricbuzz_json_xi(match_id):
    """Playing XI from Cricbuzz scorecard JSON."""
    team1_xi, team2_xi = [], []
    try:
        data = _request_json(CB_SCORECARD_URL.format(match_id=match_id))
        scorecard = data.get("scoreCard", []) or []
        teams_seen = {}

        for inning in scorecard:
            bat_team = _correct_team_name(_clean_text(
                inning.get("batTeamDetails", {}).get("batTeamName", "")
            ))
            batsmen = inning.get("batTeamDetails", {}).get("batsmenData", {}) or {}
            names = [
                _clean_text(v.get("batName", ""))
                for v in batsmen.values()
                if v.get("batName")
            ]
            if bat_team and names and bat_team not in teams_seen:
                teams_seen[bat_team] = names[:11]

        keys = list(teams_seen.keys())
        if len(keys) >= 1:
            team1_xi = teams_seen[keys[0]]
        if len(keys) >= 2:
            team2_xi = teams_seen[keys[1]]
    except Exception as e:
        print(f"[CB-JSON] _cricbuzz_json_xi error: {e}")
    return team1_xi, team2_xi

# ─────────────────────────────────────────────────────────
# CRICBUZZ HTML — toss extraction
# ─────────────────────────────────────────────────────────
# Fixed regex patterns (removed markdown formatting artifacts)
_TOSS_PATTERNS = [
    re.compile(
        r"([A-Za-z\s]+?)\s+won\s+the\s+toss\s+and\s+(?:elected|chose)\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    re.compile(
        r"Toss\s*[:\-]\s*([A-Za-z\s]+?)\s*[\(,]\s*(bat|bowl|field)",
        re.I,
    ),
    re.compile(
        r"Toss\s*[:\-]\s*([A-Za-z\s]+?)\s*,\s*opt(?:ed)?\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    re.compile(
        r'"tossWinner"\s*:\s*"([^"]+)".*?"tossDecision"\s*:\s*"([^"]+)"',
        re.I | re.DOTALL,
    ),
    re.compile(
        r"([A-Za-z\s]+?)\s+(?:won|wins)\s+(?:the\s+)?toss",
        re.I,
    ),
    re.compile(
        r"toss[^.]{0,120}?(bat(?:ting)?|bowl(?:ing)?|field(?:ing)?)",
        re.I,
    ),
]

def _parse_toss_from_text(text, team1, team2):
    """Try all patterns. Returns (winner, decision) or ('', None)."""
    for i, pat in enumerate(_TOSS_PATTERNS):
        m = pat.search(text)
        if not m:
            continue

        raw_winner = ""
        raw_decision = ""

        if i < 4:
            raw_winner = _clean_text(m.group(1))
            raw_decision = (m.group(2) if m.lastindex >= 2 else "").lower()
        elif i == 4:
            raw_winner = _clean_text(m.group(1))
            post_match = text[m.end():m.end()+100]
            if "bat" in post_match.lower():
                raw_decision = "bat"
            elif "field" in post_match.lower() or "bowl" in post_match.lower():
                raw_decision = "field"
        else:
            raw_winner = ""
            raw_decision = m.group(1).lower()

        if "bat" in raw_decision:
            decision = "bat"
        elif "bowl" in raw_decision or "field" in raw_decision:
            decision = "field"
        else:
            continue

        winner = ""
        for candidate in [team1, team2]:
            if candidate and candidate.lower() == raw_winner.lower():
                winner = candidate
                break
        if not winner:
            for candidate in [team1, team2]:
                if candidate and candidate.lower() in raw_winner.lower():
                    winner = candidate
                    break
        if not winner and raw_winner:
            t1_score = len(
                set(raw_winner.lower().split()) & set(team1.lower().split())
            ) if team1 else 0
            t2_score = len(
                set(raw_winner.lower().split()) & set(team2.lower().split())
            ) if team2 else 0
            if t1_score > t2_score:
                winner = team1
            elif t2_score > t1_score:
                winner = team2

        if winner and decision:
            print(f"[TOSS-PATTERN] Pattern {i+1} matched: '{winner}' elected to '{decision}'")
            return winner, decision

    return "", None

def _get_toss_from_cricbuzz_html(match_id, team1, team2):
    """Scrape Cricbuzz match page for toss."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[HTML-TOSS] Fetching: {url}")
        soup = _request_soup(url)

        # First check page title for toss info (common pattern)
        title = soup.title.string if soup.title else ""
        if title and "toss" in title.lower():
            print(f"[HTML-TOSS] Checking title: {title}")
            w, d = _parse_toss_from_text(title, team1, team2)
            if w and d:
                return w, d

        selectors = [
            "div.cb-toss-sts",
            "span.cb-toss-sts",
            "div.cb-mtch-info-itm",
            "div[class*='toss']",
            "p[class*='toss']",
            "span[class*='toss']",
            "div.cb-text-complete",
            "div.cb-col-100",
            "div.cb-min-inf",
        ]

        for selector in selectors:
            for el in soup.select(selector):
                text = _clean_text(el.get_text(" ", strip=True))
                if "toss" in text.lower():
                    print(f"[HTML-TOSS] Found toss in [{selector}]: {text[:200]}")
                    w, d = _parse_toss_from_text(text, team1, team2)
                    if w and d:
                        return w, d

        full = _clean_text(soup.get_text(" ", strip=True))
        for sentence in re.split(r'[.!?*\n*]', full):
            if "toss" in sentence.lower():
                print(f"[HTML-TOSS] Toss sentence: {sentence[:200]}")
                w, d = _parse_toss_from_text(sentence, team1, team2)
                if w and d:
                    return w, d

        w, d = _parse_toss_from_text(full, team1, team2)
        if w and d:
            return w, d

        print("[HTML-TOSS] No toss found in HTML")
    except Exception as e:
        print(f"[HTML-TOSS] Error: {e}")
        import traceback
        traceback.print_exc()

    return "", None

def _get_toss_from_cricbuzz_commentary(match_id, team1, team2):
    """Try Cricbuzz commentary API for toss sentence."""
    try:
        url = CB_COMMENTARY_URL.format(match_id=match_id)
        print(f"[COMM-TOSS] Fetching: {url}")
        data = _request_json(url)
        comm_list = (
            data.get("commentaryList", [])
            or data.get("commentary", [])
            or []
        )
        print(f"[COMM-TOSS] Found {len(comm_list)} commentary entries")

        for entry in comm_list:
            text = _clean_text(
                entry.get("commText", "")
                or entry.get("text", "")
                or entry.get("commentary", "")
                or ""
            )
            if "toss" in text.lower():
                print(f"[COMM-TOSS] Toss found: {text[:200]}")
                w, d = _parse_toss_from_text(text, team1, team2)
                if w and d:
                    return w, d
    except Exception as e:
        print(f"[COMM-TOSS] Error: {e}")
        import traceback
        traceback.print_exc()
    return "", None

# ─────────────────────────────────────────────────────────
# PLAYING XI — HTML
# ─────────────────────────────────────────────────────────
def _split_player_list(raw_text):
    text = _clean_text(raw_text)
    if not text:
        return []
    text = re.sub(r"\s*\([^)]*\)", "", text)
    parts = re.split(r"\s*,\s*|\s+[•|]\s+|\s{2,}", text)
    seen, uniq = set(), []
    for name in parts:
        cn = _clean_text(name)
        key = cn.lower()
        if cn and key not in {"playing xi", "impact subs"} and key not in seen:
            seen.add(key)
            uniq.append(cn)
    return uniq

def _extract_playing_xi(page_text, team1, team2):
    t1_xi, t2_xi = [], []
    if not page_text:
        return t1_xi, t2_xi

    if team1 and team2:
        for t_name, other in [(team1, team2), (team2, team1)]:
            pat = re.compile(
                rf"{re.escape(t_name)}\s*(?:Playing\s*)?XI\s*[:\-]\s*(.*?)"
                rf"(?={re.escape(other)}\s*(?:Playing\s*)?XI|Impact\s*Subs|$)",
                re.I | re.DOTALL,
            )
            m = pat.search(page_text)
            if m:
                xi = _split_player_list(m.group(1))
                if t_name == team1:
                    t1_xi = xi
                else:
                    t2_xi = xi

    if not t1_xi or not t2_xi:
        generic = re.findall(
            r"(?:Playing\s*)?XI\s*[:**\-**]\s*(.*?)"
            r"(?=(?:Playing\s*)?XI|Impact\s*Subs|$)",
            page_text, re.I | re.DOTALL,
        )
        if len(generic) >= 2:
            if not t1_xi:
                t1_xi = _split_player_list(generic[0])
            if not t2_xi:
                t2_xi = _split_player_list(generic[1])

    return t1_xi[:11], t2_xi[:11]

# ─────────────────────────────────────────────────────────
# TEAM NAME UTILITIES
# ─────────────────────────────────────────────────────────
def _normalize_team_name(name, team_encoder=None):
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

def _safe_encode(encoder, value):
    classes = list(encoder.classes_)
    if value in classes:
        return int(encoder.transform([value])[0])
    for cls in classes:
        if cls.lower() == value.lower():
            return int(encoder.transform([cls])[0])
    return int(encoder.transform([classes[0]])[0])

def _safe_div(num, den, fallback):
    return float(num / den) if den else float(fallback)

# ─────────────────────────────────────────────────────────
# STAT HELPERS
# ─────────────────────────────────────────────────────────
def _team_winrate(matches, team):
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.5
    return (
        float((tm["winner"] == team).mean()),
        float((tm.tail(5)["winner"] == team).mean()),
    )

def _h2h(matches, team1, team2):
    h = matches[
        ((matches["team1"] == team1) & (matches["team2"] == team2)) |
        ((matches["team1"] == team2) & (matches["team2"] == team1))
    ]
    if h.empty:
        return 0, 0
    return int((h["winner"] == team1).sum()), int((h["winner"] == team2).sum())

def _chase_metrics(matches, team):
    if "win_by_wickets" not in matches.columns:
        return 0.5, 0.4
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.4
    cw = tm[(tm["winner"] == team) & (tm["win_by_wickets"] > 0)]
    return _safe_div(len(cw), len(tm), 0.5), 0.4 if cw.empty else 1.0

def _global_player_defaults(player_lookup):
    cols = {
        "batting_avg": 25.0, "strike_rate": 125.0,
        "economy": 8.5, "bowling_avg": 30.0,
        "recent_strike_rate": 125.0, "recent_economy": 8.5,
    }
    d = {
        c: float(player_lookup[c].mean()) if c in player_lookup.columns else fb
        for c, fb in cols.items()
    }
    d["top3_batting_avg"] = d["batting_avg"]
    return d

def _player_stats_for_xi(player_lookup, xi, defaults):
    if not xi:
        return defaults.copy()
    lk = player_lookup.copy()
    lk["player_norm"] = lk["player"].astype(str).str.lower().str.strip()
    xi_norm = [str(x).lower().strip() for x in xi if str(x).strip()]
    selected = lk[lk["player_norm"].isin(xi_norm)].reset_index(drop=True)
    if selected.empty:
        return defaults.copy()
    out = {
        "batting_avg": float(selected["batting_avg"].mean()),
        "strike_rate": float(selected["strike_rate"].mean()),
        "top3_batting_avg": float(selected.nlargest(3, "batting_avg")["batting_avg"].mean()),
        "economy": float(selected["economy"].mean()),
        "bowling_avg": float(selected["bowling_avg"].mean()),
        "recent_strike_rate": float(selected["recent_strike_rate"].mean()),
        "recent_economy": float(selected["recent_economy"].mean()),
    }
    for k, v in out.items():
        if pd.isna(v):
            out[k] = defaults[k]
    return out

# ─────────────────────────────────────────────────────────
# TOSS RESOLUTION
# ─────────────────────────────────────────────────────────
def _resolve_toss(match_id, team1, team2, espn_info=None, cricbuzz_id=None):
    """
    Try every available source for toss data.
    Returns (toss_winner, toss_decision) or ('', None).
    """
    print(f"\n[TOSS-RESOLVE] Starting toss resolution for teams: '{team1}' vs '{team2}'")
    
    # Use provided Cricbuzz ID or try to find it
    cb_match_id = cricbuzz_id or _find_cricbuzz_match_id(team1, team2)
    if not cb_match_id:
        print("[TOSS-RESOLVE] No Cricbuzz match ID available")
        cb_match_id = match_id  # Fallback to original ID

    # 1. ESPN
    if espn_info is not None:
        print("[TOSS-RESOLVE] Trying ESPN match_info...")
        tw, td = _extract_toss_from_espn_info(espn_info, team1, team2)
        if tw and td:
            print(f"[TOSS-RESOLVE] ✓ SUCCESS via ESPN: {tw} / {td}")
            return tw, td
        print("[TOSS-RESOLVE] ✗ ESPN failed - trying Cricbuzz JSON")

    # 2. Cricbuzz JSON
    try:
        print("[TOSS-RESOLVE] Trying Cricbuzz JSON API...")
        cb = _cricbuzz_json_match_info(cb_match_id)
        if cb and cb.get("toss_done"):
            print(f"[TOSS-RESOLVE] ✓ SUCCESS via CricbuzzJSON: "
                  f"{cb['toss_winner']} / {cb['toss_decision']}")
            return cb["toss_winner"], cb["toss_decision"]
        print("[TOSS-RESOLVE] ✗ Cricbuzz JSON failed - trying HTML")
    except Exception as e:
        print(f"[TOSS-RESOLVE] ✗ CricbuzzJSON exception: {e}")

    # 3. HTML
    print("[TOSS-RESOLVE] Trying Cricbuzz HTML scraping...")
    tw, td = _get_toss_from_cricbuzz_html(cb_match_id, team1, team2)
    if tw and td:
        print(f"[TOSS-RESOLVE] ✓ SUCCESS via HTML: {tw} / {td}")
        return tw, td
    print("[TOSS-RESOLVE] ✗ HTML failed - trying commentary")

    # 4. Commentary
    print("[TOSS-RESOLVE] Trying Cricbuzz commentary...")
    tw, td = _get_toss_from_cricbuzz_commentary(cb_match_id, team1, team2)
    if tw and td:
        print(f"[TOSS-RESOLVE] ✓ SUCCESS via commentary: {tw} / {td}")
        return tw, td

    print("[TOSS-RESOLVE] ✗ All sources exhausted - toss not found")
    print("[TOSS-RESOLVE] RECOMMENDATION: Use manual override or wait for toss")
    return "", None

# ─────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────
def get_todays_match_id():
    """Return today's IPL match ID (Cricbuzz ID)."""
    try:
        soup = _request_soup(LIVE_SCORES_URL)
        links = soup.select("a[href*='/live-cricket-scores/']")
        for link in links:
            href = link.get("href", "")
            if "indian-premier-league" not in href.lower():
                continue
            match = re.search(r"/live-cricket-scores/(\d+)", href)
            if match:
                return int(match.group(1))
    except Exception:
        pass
    
    # Fallback to ESPN if available
    try:
        m = _get_espn_live_match()
        if m:
            return int(m.get("objectId"))
    except Exception:
        pass

    return None

def scrape_match(match_id):
    """
    Scrape all match details.
    Returns dict expected by app.py.
    """
    errors = []
    print(f"\n{'='*60}")
    print(f"[SCRAPE] Starting scrape for match ID: {match_id}")
    print(f"{'='*60}\n")

    # ── 1. ESPN ───────────────────────────────────────────
    try:
        print("[SCRAPE] Attempting ESPN source...")
        espn_match = _get_espn_live_match(match_id=match_id)
        if espn_match:
            series = espn_match.get("series", {})
            s_slug = f"{series.get('slug')}-{series.get('objectId')}"
            m_slug = f"{espn_match.get('slug')}-{espn_match.get('objectId')}"

            info = CRICINFO_CLIENT.match_info(s_slug, m_slug)
            scorecard = CRICINFO_CLIENT.match_scorecard(s_slug, m_slug)

            teams = espn_match.get("teams", []) or []
            t_names = [t.get("team", {}).get("longName", "") for t in teams]
            team1 = _correct_team_name(_clean_text(t_names[0] if t_names else ""))
            team2 = _correct_team_name(_clean_text(t_names[1] if len(t_names) > 1 else ""))

            if not team1 or not team2:
                raise ValueError("Empty team names from ESPN")

            venue = (
                _clean_text(
                    (info or {}).get("venue", {}).get("longName", "")
                ) if isinstance(info, dict) else ""
            ) or _clean_text(
                espn_match.get("ground", {}).get("longName", "")
            ) or "Unknown Venue"

            print(f"[SCRAPE-ESPN] Teams: '{team1}' vs '{team2}'")
            print(f"[SCRAPE-ESPN] Venue: '{venue}'")

            # Find Cricbuzz match ID for the same teams
            cricbuzz_id = _find_cricbuzz_match_id(team1, team2)
            print(f"[SCRAPE-ESPN] Cricbuzz match ID: {cricbuzz_id}")

            tw, td = _resolve_toss(match_id, team1, team2, espn_info=info, cricbuzz_id=cricbuzz_id)

            toss_done = bool(tw and td)
            chasing_team = None
            if toss_done:
                chasing_team = (
                    (team2 if tw == team1 else team1) if td == "bat" else tw
                )

            xi_map = _extract_xi_from_scorecard(scorecard)
            team1_xi = xi_map.get(team1, [])
            team2_xi = xi_map.get(team2, [])
            if not team1_xi:
                for k, v in xi_map.items():
                    if _correct_team_name(k) == team1:
                        team1_xi = v
                        break
            if not team2_xi:
                for k, v in xi_map.items():
                    if _correct_team_name(k) == team2:
                        team2_xi = v
                        break

            print(f"[SCRAPE-ESPN] XI found: Team1={len(team1_xi)} players, Team2={len(team2_xi)} players")
            print(f"[SCRAPE-ESPN] Final toss: done={toss_done} winner='{tw}' decision='{td}'")

            return {
                "match_id": int(match_id),
                "team1": team1,
                "team2": team2,
                "venue": venue,
                "toss_done": toss_done,
                "toss_winner": tw or None,
                "toss_decision": td or None,
                "chasing_team": chasing_team,
                "team1_xi": team1_xi,
                "team2_xi": team2_xi,
                "source": "espn",
                "cricbuzz_id": cricbuzz_id,
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
    except Exception as e:
        errors.append(f"ESPN: {e}")
        print(f"[SCRAPE-ESPN] ✗ Failed: {e}")
        import traceback
        traceback.print_exc()

    # ── 2. Cricbuzz JSON ──────────────────────────────────
    try:
        print("\n[SCRAPE] Attempting Cricbuzz JSON source...")
        cb_info = _cricbuzz_json_match_info(match_id)
        if cb_info and cb_info.get("team1"):
            team1 = cb_info["team1"]
            team2 = cb_info["team2"]

            tw = cb_info["toss_winner"] or ""
            td = cb_info["toss_decision"] or ""

            if not tw or not td:
                tw, td = _resolve_toss(match_id, team1, team2, espn_info=None)

            toss_done = bool(tw and td)
            chasing_team = None
            if toss_done:
                chasing_team = (
                    (team2 if tw == team1 else team1) if td == "bat" else tw
                )

            xi1, xi2 = _cricbuzz_json_xi(match_id)

            print(f"[SCRAPE-CB-JSON] Success: toss_done={toss_done}")

            return {
                "match_id": int(match_id),
                "team1": team1,
                "team2": team2,
                "venue": cb_info.get("venue", "Unknown Venue"),
                "toss_done": toss_done,
                "toss_winner": tw or None,
                "toss_decision": td or None,
                "chasing_team": chasing_team,
                "team1_xi": xi1,
                "team2_xi": xi2,
                "source": "cricbuzz_json",
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
    except Exception as e:
        errors.append(f"CricbuzzJSON: {e}")
        print(f"[SCRAPE-CB-JSON] ✗ Failed: {e}")

    # ── 3. Cricbuzz HTML ──────────────────────────────────
    try:
        print("\n[SCRAPE] Attempting Cricbuzz HTML source...")
        soup = _request_soup(MATCH_URL_TEMPLATE.format(match_id=match_id))
        page_text = _clean_text(soup.get_text(" ", strip=True))
        title_txt = _clean_text(soup.title.get_text() if soup.title else "")

        team1, team2 = "Unknown", "Unknown"
        tm = re.search(
            r"(?:commentary|live-cricket-scores)\s*\|?\s*(.*?)\s+vs\.?\s+(.*?)(?:,|\|)",
            title_txt, re.I,
        )
        if tm:
            team1 = _correct_team_name(_clean_text(tm.group(1)))
            team2 = _correct_team_name(_clean_text(tm.group(2)))

        if team1 == "Unknown":
            meta = soup.find("meta", {"name": "description"})
            if meta:
                mc = re.search(
                    r"([\w\s]+?)\s+vs\.?\s+([\w\s]+?)\s+(?:live|cricket|ipl)",
                    meta.get("content", ""), re.I,
                )
                if mc:
                    team1 = _correct_team_name(_clean_text(mc.group(1)))
                    team2 = _correct_team_name(_clean_text(mc.group(2)))

        venue = "Unknown Venue"
        vm = re.search(r"Venue\s*[:**\-**]\s*(.*?)(?:\s*[•·]\s*|\s{2,}|$)", page_text, re.I)
        if vm:
            venue = _clean_text(vm.group(1))

        tw, td = _resolve_toss(match_id, team1, team2, espn_info=None)
        toss_done = bool(tw and td)
        chasing_team = None
        if toss_done:
            chasing_team = (
                (team2 if tw == team1 else team1) if td == "bat" else tw
            )

        team1_xi, team2_xi = _extract_playing_xi(page_text, team1, team2)

        print(f"[SCRAPE-HTML] Success: toss_done={toss_done}")

        return {
            "match_id": int(match_id),
            "team1": team1,
            "team2": team2,
            "venue": venue,
            "toss_done": toss_done,
            "toss_winner": tw or None,
            "toss_decision": td or None,
            "chasing_team": chasing_team,
            "team1_xi": team1_xi,
            "team2_xi": team2_xi,
            "source": "cricbuzz_html",
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        errors.append(f"CricbuzzHTML: {e}")
        print(f"[SCRAPE-HTML] ✗ Failed: {e}")

    print(f"\n[SCRAPE] All sources failed")
    return {"error": " | ".join(errors)}

# ─────────────────────────────────────────────────────────
# FEATURE VECTOR
# ─────────────────────────────────────────────────────────
def build_feature_vector(
    match_info, player_lookup, matches,
    team_encoder, venue_encoder, venue_score_history,
    team_pp_eco_lookup, team_opener_lookup,
    get_team_recent_avg_score, get_season_avg_score,
    get_season_year, get_venue_recent_avg_score,
    get_team_recent_high_score_rate, feature_cols,
):
    team1 = _normalize_team_name(match_info.get("team1"), team_encoder)
    team2 = _normalize_team_name(match_info.get("team2"), team_encoder)
    venue = _clean_text(match_info.get("venue", ""))
    now = pd.Timestamp(datetime.today().date())

    print(f"[FEAT] Building features for: '{team1}' vs '{team2}'")

    t1_id = _safe_encode(team_encoder, team1)
    t2_id = _safe_encode(team_encoder, team2)
    venue_id = _safe_encode(
        venue_encoder,
        venue if venue in set(venue_encoder.classes_.tolist())
        else venue_encoder.classes_[0],
    )

    t1_h2h, t2_h2h = _h2h(matches, team1, team2)
    t1_wr, t1_l5 = _team_winrate(matches, team1)
    t2_wr, t2_l5 = _team_winrate(matches, team2)
    t1_cp, t1_hc = _chase_metrics(matches, team1)
    t2_cp, t2_hc = _chase_metrics(matches, team2)

    print(f"[FEAT] H2H: t1={t1_h2h} t2={t2_h2h}  WR: t1={t1_wr:.2f} t2={t2_wr:.2f}")

    season_avg = float(get_season_avg_score(now))
    season_year = int(get_season_year(now))
    t1_ravg = float(get_team_recent_avg_score(team1, now))
    t2_ravg = float(get_team_recent_avg_score(team2, now))
    t1_hsr = float(get_team_recent_high_score_rate(team1, now))
    t2_hsr = float(get_team_recent_high_score_rate(team2, now))

    if (
        "venue" in venue_score_history.columns
        and "first_innings_score" in venue_score_history.columns
    ):
        vmask = venue_score_history["venue"] == venue
        venue_avg = (
            float(venue_score_history.loc[vmask, "first_innings_score"].mean())
            if vmask.any() else 167.0
        )
    else:
        venue_avg = 167.0
    venue_recent = float(get_venue_recent_avg_score(venue, now))

    toss_done = bool(match_info.get("toss_done", False))
    toss_winner = _normalize_team_name(
        match_info.get("toss_winner") or "", team_encoder
    )
    toss_decision = _clean_text(match_info.get("toss_decision") or "").lower()

    pp_def = (
        float(sum(team_pp_eco_lookup.values()) / len(team_pp_eco_lookup))
        if team_pp_eco_lookup else 8.5
    )
    t1_pp = float(team_pp_eco_lookup.get(team1, pp_def))
    t2_pp = float(team_pp_eco_lookup.get(team2, pp_def))

    op_def = {"opener_avg_batting_avg": 30.0, "opener_avg_strike_rate": 130.0}
    t1_open = team_opener_lookup.get(team1, op_def)
    t2_open = team_opener_lookup.get(team2, op_def)

    defaults = _global_player_defaults(player_lookup)
    t1_stats = _player_stats_for_xi(
        player_lookup, match_info.get("team1_xi", []), defaults
    )
    t2_stats = _player_stats_for_xi(
        player_lookup, match_info.get("team2_xi", []), defaults
    )

    # ── Historical averages for new PP features (pre-match defaults) ──
    # These features (team1_pp_runs etc.) are actual match-day values
    # in training data, but at prediction time we use historical averages.
    # Typical IPL powerplay (6 overs): ~50 runs, SR~130, ~1-2 wkts, RR~8.3
    pp_runs_default = 50.0
    pp_sr_default = 130.0
    pp_wkts_default = 1.5
    pp_rr_default = 8.3

    feat = {c: 0.0 for c in feature_cols}
    feat.update({
        # ── Core identifiers ──────────────────────────────
        "team1": t1_id,
        "team2": t2_id,
        "venue": venue_id,

        # ── Venue stats ───────────────────────────────────
        "venue_avg_first_innings": venue_avg,
        "venue_recent_avg": venue_recent,

        # ── Home / Toss ───────────────────────────────────
        "is_home_team1": 0,
        "toss_winner_is_team1": int(toss_done and toss_winner == team1),
        "toss_decision_bat": int(toss_done and toss_decision == "bat"),

        # ── H2H ──────────────────────────────────────────
        "h2h_team1_wins": t1_h2h,
        "h2h_team2_wins": t2_h2h,

        # ── Chase metrics ─────────────────────────────────
        "chase_win_pct_team1": t1_cp,
        "chase_win_pct_team2": t2_cp,
        "high_score_chase_t1": t1_hc,
        "high_score_chase_t2": t2_hc,

        # ── Win rates ─────────────────────────────────────
        "winrate_team1": t1_wr,
        "winrate_team2": t2_wr,
        "last5_win_team1": t1_l5,
        "last5_win_team2": t2_l5,

        # ── Recent scoring ────────────────────────────────
        "t1_recent_avg_score": t1_ravg,
        "t2_recent_avg_score": t2_ravg,
        "t1_high_score_rate": t1_hsr,
        "t2_high_score_rate": t2_hsr,

        # ── PP bowling economy (by bowling team) ──────────
        "t1_pp_bowling_economy": t1_pp,
        "t2_pp_bowling_economy": t2_pp,

        # ── Season context ────────────────────────────────
        "season_avg_score": season_avg,
        "season_year": season_year,

        # ── Team 1 player stats ───────────────────────────
        "t1_avg_batting_avg": t1_stats["batting_avg"],
        "t1_avg_strike_rate": t1_stats["strike_rate"],
        "t1_top3_batting_avg": t1_stats["top3_batting_avg"],
        "t1_avg_economy": t1_stats["economy"],
        "t1_avg_bowling_avg": t1_stats["bowling_avg"],
        "t1_recent_strike_rate": t1_stats["recent_strike_rate"],
        "t1_recent_economy": t1_stats["recent_economy"],

        # ── Team 2 player stats ───────────────────────────
        "t2_avg_batting_avg": t2_stats["batting_avg"],
        "t2_avg_strike_rate": t2_stats["strike_rate"],
        "t2_top3_batting_avg": t2_stats["top3_batting_avg"],
        "t2_avg_economy": t2_stats["economy"],
        "t2_avg_bowling_avg": t2_stats["bowling_avg"],
        "t2_recent_strike_rate": t2_stats["recent_strike_rate"],
        "t2_recent_economy": t2_stats["recent_economy"],

        # ── Opener lookup stats (still used as model features) ──
        "t1_opener_batting_avg": float(t1_open.get("opener_avg_batting_avg", 30.0)),
        "t1_opener_strike_rate": float(t1_open.get("opener_avg_strike_rate", 130.0)),
        "t2_opener_batting_avg": float(t2_open.get("opener_avg_batting_avg", 30.0)),
        "t2_opener_strike_rate": float(t2_open.get("opener_avg_strike_rate", 130.0)),

        # ── Composite features ────────────────────────────
        "t1_bat_vs_bowl": _safe_div(t1_stats["batting_avg"], t2_stats["bowling_avg"], 1.0),
        "t2_bat_vs_bowl": _safe_div(t2_stats["batting_avg"], t1_stats["bowling_avg"], 1.0),
        "t1_rolling_season_avg": t1_ravg,
        "t2_rolling_season_avg": t2_ravg,

        # ── NEW: Powerplay match stats (historical avg defaults) ──
        # At prediction time the match hasn't started, so we use typical
        # IPL powerplay averages. The model was trained on actual values;
        # neutral defaults here ensure no spurious signal.
        "team1_pp_runs": pp_runs_default,
        "team1_pp_strike_rate": pp_sr_default,
        "team1_pp_wickets": pp_wkts_default,
        "team1_pp_run_rate": pp_rr_default,
        "team2_pp_runs": pp_runs_default,
        "team2_pp_strike_rate": pp_sr_default,
        "team2_pp_wickets": pp_wkts_default,
        "team2_pp_run_rate": pp_rr_default,
        "pp_strength_diff": 0.0,  # neutral — no bias to either team
        "pp_run_rate_diff": 0.0,  # neutral — no bias to either team
    })

    print(f"[FEAT] Feature vector built. PP defaults injected (pre-match). "
          f"Total cols in feat: {len(feat)}")

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)
