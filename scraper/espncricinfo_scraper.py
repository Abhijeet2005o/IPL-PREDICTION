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

IPL_SERIES_ID = "1510719"
LIVE_SCORES_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
MATCH_SQUADS_URL = "https://www.cricbuzz.com/live-cricket-scorecard/{match_id}"

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

TEAM_ABBREVIATIONS = {
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

TEAM_NAME_CORRECTIONS = {
    "Royal Challengers Bengaluru": "Royal Challengers Bangalore",
    "royal challengers bengaluru": "Royal Challengers Bangalore",
    "Royal Challengers Bengaluru ": "Royal Challengers Bangalore",
}

TEAM_TO_ABBR = {v: k for k, v in TEAM_ABBREVIATIONS.items()}

KNOWN_XI: Dict[int, Dict[str, List[str]]] = {}


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
    corrected = TEAM_NAME_CORRECTIONS.get(name)
    if corrected:
        return corrected
    corrected = TEAM_NAME_CORRECTIONS.get(name.lower())
    if corrected:
        return corrected
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
    """Extract clean player name - removes (c), (wk), †, * etc."""
    name = re.sub(r'\s*[\(\[].*?[\)\]]', '', text)
    name = re.sub(r'[†*]', '', name)
    name = _clean_text(name)
    return name


# ─────────────────────────────────────────────────────────
# ESPN CRICINFO FUNCTIONS
# ─────────────────────────────────────────────────────────
def _get_espn_live_match(match_id: Optional[int] = None) -> Optional[dict]:
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

        match_id_str = str(match_id).strip()
        for m in candidates:
            if str(m.get("objectId", "")).strip() == match_id_str:
                return m
    except Exception as e:
        print(f"[ESPN] _get_espn_live_match error: {e}")
    return None


def _extract_xi_from_scorecard(scorecard: dict) -> Dict[str, List[str]]:
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
            players = entry.get("players", []) or []
            names = [
                _clean_text(p.get("player", {}).get("longName", ""))
                for p in players
            ]
            names = [n for n in names if n]
            if team_name and names:
                team_xi[team_name] = names[:11]
                print(f"[ESPN-XI] Found {len(names)} players for {team_name}")
    except Exception as e:
        print(f"[ESPN] _extract_xi_from_scorecard error: {e}")
    return team_xi


def _extract_toss_from_espn_info(info: dict, team1: str, team2: str) -> Tuple[str, Optional[str]]:
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
            has_winner = any(k.lower() in {"tosswinner", "tosswinnerid", "winnerid", "winner"} for k in obj)
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


# ═══════════════════════════════════════════════════════════
# NEW: CRICBUZZ PLAYING XI EXTRACTION
# ═══════════════════════════════════════════════════════════
def get_playing_xi_from_cricbuzz(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz match page.
    PRIMARY source for live Playing XI data.
    """
    team_xi: Dict[str, List[str]] = {}
    
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-XI] Fetching: {url}")
        
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = resp.text
        
        # METHOD 1: Match info section
        info_items = soup.select(".cb-mtch-info-itm")
        for item in info_items:
            label_div = item.select_one(".cb-col.cb-col-27")
            value_div = item.select_one(".cb-col.cb-col-73")
            
            if label_div and value_div:
                label_text = _clean_text(label_div.get_text()).lower()
                
                if any(x in label_text for x in ["squad", "playing", "xi", "team"]):
                    current_team = None
                    for t in [team1, team2]:
                        if t and t.lower() in label_text:
                            current_team = t
                            break
                    
                    if current_team:
                        player_links = value_div.select("a")
                        names = [_extract_player_name(link.get_text()) for link in player_links]
                        names = [n for n in names if n and len(n) > 2][:11]
                        if names:
                            team_xi[current_team] = names
                            print(f"[CB-XI] Method 1: Found {len(names)} players for {current_team}")
        
        # METHOD 2: Playing XI section headers
        if len(team_xi) < 2:
            headers = soup.select(".cb-col.cb-col-100.cb-font-14, .cb-minfo-tm-nm")
            
            for header in headers:
                header_text = _clean_text(header.get_text())
                
                current_team = None
                for t in [team1, team2]:
                    if t:
                        t_lower = t.lower()
                        abbr = TEAM_TO_ABBR.get(t, "").lower()
                        if (t_lower in header_text.lower() or 
                            (abbr and abbr in header_text.lower())):
                            if "playing" in header_text.lower() or "xi" in header_text.lower():
                                current_team = t
                                break
                
                if current_team and current_team not in team_xi:
                    parent = header.find_parent()
                    if parent:
                        player_links = parent.select("a[href*='/profiles/']")
                        if not player_links:
                            player_links = parent.select("a[href*='/cricket-player/']")
                        
                        names = [_extract_player_name(link.get_text()) for link in player_links]
                        names = [n for n in names if n and len(n) > 2][:11]
                        if names:
                            team_xi[current_team] = names
                            print(f"[CB-XI] Method 2: Found {len(names)} players for {current_team}")
        
        # METHOD 3: Regex pattern from page text
        if len(team_xi) < 2:
            xi_patterns = [
                r'([A-Za-z\s]+?)\s*\(?\s*Playing\s*XI\s*\)?\s*:?\s*([A-Za-z\s,\.]+?)(?=\n|$|[A-Z][a-z]+\s*\()',
                r'([A-Za-z\s]+?)\s+XI\s*:?\s*([A-Za-z\s,\.]+?)(?=\n|$)',
            ]
            
            for pattern in xi_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE | re.MULTILINE)
                for match in matches:
                    team_name_raw = _clean_text(match[0])
                    players_str = match[1]
                    
                    current_team = None
                    for t in [team1, team2]:
                        if t and (t.lower() in team_name_raw.lower() or team_name_raw.lower() in t.lower()):
                            current_team = t
                            break
                    
                    if current_team and current_team not in team_xi:
                        players = [_extract_player_name(p.strip()) for p in players_str.split(",")]
                        players = [p for p in players if p and len(p) > 2][:11]
                        if len(players) >= 5:
                            team_xi[current_team] = players
                            print(f"[CB-XI] Method 3: Found {len(players)} players for {current_team}")
        
        # METHOD 4: Squad divs
        if len(team_xi) < 2:
            squad_sections = soup.select(".cb-play11-lft-col, .cb-minfo-tm-plyr")
            
            for section in squad_sections:
                parent = section.find_parent(class_=re.compile(r'cb-col'))
                if parent:
                    section_text = _clean_text(parent.get_text())
                    
                    current_team = None
                    for t in [team1, team2]:
                        if t and t.lower() in section_text.lower():
                            current_team = t
                            break
                    
                    if current_team and current_team not in team_xi:
                        player_links = section.select("a")
                        names = [_extract_player_name(link.get_text()) for link in player_links]
                        names = [n for n in names if n and len(n) > 2][:11]
                        if len(names) >= 5:
                            team_xi[current_team] = names
                            print(f"[CB-XI] Method 4: Found {len(names)} players for {current_team}")
        
        # METHOD 5: "opt to" pattern
        if len(team_xi) < 2:
            opt_pattern = r'([A-Z]{2,4})\s+opt\s+to\s+(?:bat|bowl|field)[.\s]+\1\s*:?\s*([A-Za-z\s,]+?)(?=[A-Z]{2,4}\s*:|$)'
            matches = re.findall(opt_pattern, page_text, re.IGNORECASE)
            
            for match in matches:
                abbr = match[0].upper()
                players_str = match[1]
                
                if abbr in TEAM_ABBREVIATIONS:
                    team_full = TEAM_ABBREVIATIONS[abbr]
                    current_team = None
                    
                    for t in [team1, team2]:
                        if t and t.lower() == team_full.lower():
                            current_team = t
                            break
                    
                    if current_team and current_team not in team_xi:
                        players = [_extract_player_name(p.strip()) for p in players_str.split(",")]
                        players = [p for p in players if p and len(p) > 2][:11]
                        if len(players) >= 5:
                            team_xi[current_team] = players
                            print(f"[CB-XI] Method 5: Found {len(players)} players for {current_team}")
        
    except Exception as e:
        print(f"[CB-XI] Error: {e}")
    
    return team_xi


def get_playing_xi_from_scorecard(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """Extract Playing XI from Cricbuzz scorecard page."""
    team_xi: Dict[str, List[str]] = {}
    
    try:
        url = MATCH_SQUADS_URL.format(match_id=match_id)
        print(f"[CB-SCORE] Fetching: {url}")
        
        soup = _request_soup(url)
        
        innings_blocks = soup.select(".cb-col.cb-col-100.cb-ltst-wgt-hdr")
        
        current_team = None
        for block in innings_blocks:
            block_text = _clean_text(block.get_text())
            
            for t in [team1, team2]:
                if t:
                    t_lower = t.lower()
                    abbr = TEAM_TO_ABBR.get(t, "").lower()
                    if t_lower in block_text.lower() or (abbr and abbr in block_text.lower()):
                        if "innings" in block_text.lower():
                            current_team = t
                            break
            
            if current_team and current_team not in team_xi:
                parent = block.find_parent()
                if parent:
                    batting_rows = parent.select(".cb-col.cb-col-100.cb-scrd-itms")
                    names = []
                    
                    for row in batting_rows:
                        player_link = row.select_one("a.cb-text-link")
                        if player_link:
                            name = _extract_player_name(player_link.get_text())
                            if name and len(name) > 2 and name not in names:
                                names.append(name)
                    
                    if names:
                        team_xi[current_team] = names[:11]
                        print(f"[CB-SCORE] Found {len(names)} batsmen for {current_team}")
        
        # Get bowlers for opponent team
        bowling_rows = soup.select(".cb-col.cb-col-100.cb-scrd-itms")
        for row in bowling_rows:
            bowler_div = row.select_one(".cb-col.cb-col-40")
            if bowler_div:
                bowler_link = bowler_div.select_one("a")
                if bowler_link:
                    name = _extract_player_name(bowler_link.get_text())
                    if name and len(name) > 2:
                        for t in [team1, team2]:
                            if t in team_xi:
                                other_team = team2 if t == team1 else team1
                                if other_team not in team_xi:
                                    team_xi[other_team] = []
                                if name not in team_xi[other_team] and len(team_xi[other_team]) < 11:
                                    team_xi[other_team].append(name)
        
    except Exception as e:
        print(f"[CB-SCORE] Error: {e}")
    
    return team_xi


def get_toss_from_cricbuzz(match_id: int, team1: str, team2: str) -> Tuple[str, Optional[str]]:
    """Extract toss info from Cricbuzz."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-TOSS] Fetching: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        page_text = resp.text

        opt_patterns = [
            r'([A-Z]{2,4})\s+opt\s+to\s+(bat|bowl|field)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+opt\s+to\s+(bat|bowl|field)',
        ]

        for pattern in opt_patterns:
            matches = re.findall(pattern, page_text, re.I)
            for match in matches:
                team_part = match[0].strip()
                decision = match[1].lower().strip()

                if decision == "bat":
                    toss_decision = "bat"
                elif decision in ["bowl", "field"]:
                    toss_decision = "field"
                else:
                    continue

                toss_winner = None
                team_abbr = team_part.upper()
                if team_abbr in TEAM_ABBREVIATIONS:
                    team_full = TEAM_ABBREVIATIONS[team_abbr]
                    if team1 and team1.lower() == team_full.lower():
                        toss_winner = team1
                    elif team2 and team2.lower() == team_full.lower():
                        toss_winner = team2

                if not toss_winner:
                    for team in [team1, team2]:
                        if team and (team.lower() in team_part.lower() or team_part.lower() in team.lower()):
                            toss_winner = team
                            break

                if toss_winner:
                    print(f"[CB-TOSS] SUCCESS: {toss_winner} opt to {toss_decision}")
                    return toss_winner, toss_decision

        toss_pattern = r'Toss\s*:\s*([A-Za-z\s]+?)\s*\(([^)]+)\)'
        matches = re.findall(toss_pattern, page_text, re.I)
        for match in matches:
            team_name = _clean_text(match[0])
            decision = _clean_text(match[1]).lower()

            if "bat" in decision:
                toss_decision = "bat"
            elif "bowl" in decision or "field" in decision:
                toss_decision = "field"
            else:
                continue

            toss_winner = None
            for team in [team1, team2]:
                if team and (team.lower() in team_name.lower() or team_name.lower() in team.lower()):
                    toss_winner = team
                    break

            if toss_winner:
                return toss_winner, toss_decision

        return "", None

    except Exception as e:
        print(f"[CB-TOSS] Error: {e}")
        return "", None


def get_teams_from_cricbuzz(match_id: int) -> Tuple[str, str]:
    """Extract team names from Cricbuzz."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select("a[href*='/live-cricket-scores/']"):
            text = _clean_text(link.get_text())
            vs_match = re.search(r"([A-Za-z]+)\s+vs\.?\s+([A-Za-z]+)", text, re.I)
            if vs_match:
                t1_abbr = vs_match.group(1).upper()
                t2_abbr = vs_match.group(2).upper()
                team1 = _correct_team_name(TEAM_ABBREVIATIONS.get(t1_abbr, t1_abbr))
                team2 = _correct_team_name(TEAM_ABBREVIATIONS.get(t2_abbr, t2_abbr))
                print(f"[CB-TEAMS] Found: {team1} vs {team2}")
                return team1, team2

        for link in soup.select("a[href*='/live-cricket-scores/']"):
            title = link.get("title", "")
            if "vs" in title.lower():
                vs_match = re.search(r"([A-Za-z\s]+?)\s+vs\.?\s+([A-Za-z\s]+?)(?:,|\s*\d)", title, re.I)
                if vs_match:
                    team1 = _correct_team_name(_clean_text(vs_match.group(1)))
                    team2 = _correct_team_name(_clean_text(vs_match.group(2)))
                    return team1, team2

        header = soup.select_one(".cb-nav-hdr.cb-font-18")
        if header:
            text = _clean_text(header.get_text())
            vs_match = re.search(r"([A-Za-z\s]+?)\s+vs\.?\s+([A-Za-z\s]+)", text, re.I)
            if vs_match:
                team1 = _correct_team_name(_clean_text(vs_match.group(1)))
                team2 = _correct_team_name(_clean_text(vs_match.group(2)))
                return team1, team2

        return "Unknown", "Unknown"
    except Exception as e:
        print(f"[CB-TEAMS] Error: {e}")
        return "Unknown", "Unknown"


def get_venue_from_cricbuzz(match_id: int) -> str:
    """Extract venue from Cricbuzz."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select("a[href*='/venues/']"):
            text = _clean_text(link.get_text())
            if text and len(text) > 5:
                return text

        info_items = soup.select(".cb-mtch-info-itm")
        for item in info_items:
            label = item.select_one(".cb-col.cb-col-27")
            value = item.select_one(".cb-col.cb-col-73")
            if label and value:
                if "venue" in _clean_text(label.get_text()).lower():
                    venue = _clean_text(value.get_text())
                    if venue:
                        return venue

        return "Unknown Venue"
    except Exception as e:
        print(f"[CB-VENUE] Error: {e}")
        return "Unknown Venue"


def get_hardcoded_xi(match_id: int, team1: str, team2: str) -> Tuple[List[str], List[str]]:
    """Get hardcoded XI (LAST RESORT)."""
    if match_id in KNOWN_XI:
        xi_data = KNOWN_XI[match_id]
        team1_xi = xi_data.get(team1, [])
        team2_xi = xi_data.get(team2, [])
        if team1_xi and team2_xi:
            print(f"[HARDCODED-XI] Found XI for match {match_id}")
            return team1_xi, team2_xi
    return [], []


# ─────────────────────────────────────────────────────────
# STAT HELPERS
# ─────────────────────────────────────────────────────────
def _team_winrate(matches: pd.DataFrame, team: str) -> Tuple[float, float]:
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.5
    return (
        float((tm["winner"] == team).mean()),
        float((tm.tail(5)["winner"] == team).mean()),
    )


def _h2h(matches: pd.DataFrame, team1: str, team2: str) -> Tuple[int, int]:
    h = matches[
        ((matches["team1"] == team1) & (matches["team2"] == team2)) |
        ((matches["team1"] == team2) & (matches["team2"] == team1))
    ]
    if h.empty:
        return 0, 0
    return int((h["winner"] == team1).sum()), int((h["winner"] == team2).sum())


def _chase_metrics(matches: pd.DataFrame, team: str) -> Tuple[float, float]:
    if "win_by_wickets" not in matches.columns:
        return 0.5, 0.4
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.4
    cw = tm[(tm["winner"] == team) & (tm["win_by_wickets"] > 0)]
    return _safe_div(len(cw), len(tm), 0.5), 0.4 if cw.empty else 1.0


def _global_player_defaults(player_lookup: pd.DataFrame) -> Dict[str, float]:
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


def _player_stats_for_xi(
    player_lookup: pd.DataFrame, 
    xi: List[str], 
    defaults: Dict[str, float]
) -> Dict[str, float]:
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
# PUBLIC API
# ─────────────────────────────────────────────────────────
def get_todays_match_id() -> Optional[int]:
    """Return today's IPL match ID."""
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

    try:
        m = _get_espn_live_match()
        if m:
            return int(m.get("objectId"))
    except Exception:
        pass
    return None


def scrape_match(match_id: int) -> Dict[str, Any]:
    """
    Scrape all match details including Playing XI.
    
    Data sources (priority order):
    1. Cricbuzz HTML scraping
    2. Cricbuzz scorecard page
    3. ESPN Cricinfo scorecard
    4. Hardcoded fallback
    """
    print(f"\n{'='*60}")
    print(f"[SCRAPE] Starting scrape for match ID: {match_id}")
    print(f"{'='*60}\n")

    result: Dict[str, Any] = {
        "match_id": int(match_id),
        "team1": "Unknown",
        "team2": "Unknown",
        "venue": "Unknown Venue",
        "toss_done": False,
        "toss_winner": None,
        "toss_decision": None,
        "chasing_team": None,
        "team1_xi": [],
        "team2_xi": [],
        "xi_source": None,
        "source": "cricbuzz",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

    # Step 1: Get teams
    print("[SCRAPE] Step 1: Getting team names...")
    team1, team2 = get_teams_from_cricbuzz(match_id)
    result["team1"] = team1
    result["team2"] = team2
    
    # Step 2: Get venue
    print("[SCRAPE] Step 2: Getting venue...")
    result["venue"] = get_venue_from_cricbuzz(match_id)
    
    # Step 3: Get toss
    print("[SCRAPE] Step 3: Getting toss...")
    tw, td = get_toss_from_cricbuzz(match_id, team1, team2)
    if tw and td:
        result["toss_winner"] = tw
        result["toss_decision"] = td
        result["toss_done"] = True
    
    # Step 4: Get Playing XI
    print("[SCRAPE] Step 4: Getting Playing XI...")
    team_xi: Dict[str, List[str]] = {}
    
    # Source 1: Cricbuzz HTML
    print("[SCRAPE] Trying Cricbuzz HTML...")
    team_xi = get_playing_xi_from_cricbuzz(match_id, team1, team2)
    if team_xi.get(team1) and team_xi.get(team2):
        result["xi_source"] = "cricbuzz_html"
    
    # Source 2: Cricbuzz scorecard
    if not team_xi.get(team1) or not team_xi.get(team2):
        print("[SCRAPE] Trying Cricbuzz scorecard...")
        score_xi = get_playing_xi_from_scorecard(match_id, team1, team2)
        for team, players in score_xi.items():
            if team not in team_xi or not team_xi[team]:
                team_xi[team] = players
        if team_xi.get(team1) and team_xi.get(team2) and not result["xi_source"]:
            result["xi_source"] = "cricbuzz_scorecard"
    
    # Source 3: ESPN
    if (not team_xi.get(team1) or not team_xi.get(team2)) and CRICDATA_AVAILABLE:
        print("[SCRAPE] Trying ESPN...")
        try:
            espn_match = _get_espn_live_match(match_id=match_id)
            if espn_match:
                series = espn_match.get("series", {})
                s_slug = f"{series.get('slug')}-{series.get('objectId')}"
                m_slug = f"{espn_match.get('slug')}-{espn_match.get('objectId')}"
                
                scorecard = CRICINFO_CLIENT.match_scorecard(s_slug, m_slug)
                espn_xi = _extract_xi_from_scorecard(scorecard)
                
                for team, players in espn_xi.items():
                    if team not in team_xi or not team_xi[team]:
                        team_xi[team] = players
                
                if team_xi.get(team1) and team_xi.get(team2) and not result["xi_source"]:
                    result["xi_source"] = "espn"
        except Exception as e:
            print(f"[SCRAPE] ESPN error: {e}")
    
    # Source 4: Hardcoded
    if not team_xi.get(team1) or not team_xi.get(team2):
        print("[SCRAPE] Trying hardcoded...")
        hc_t1, hc_t2 = get_hardcoded_xi(match_id, team1, team2)
        if hc_t1 and team1 not in team_xi:
            team_xi[team1] = hc_t1
        if hc_t2 and team2 not in team_xi:
            team_xi[team2] = hc_t2
        if team_xi.get(team1) and team_xi.get(team2) and not result["xi_source"]:
            result["xi_source"] = "hardcoded"
    
    result["team1_xi"] = team_xi.get(team1, [])
    result["team2_xi"] = team_xi.get(team2, [])
    
    # Step 5: Chasing team
    if result["toss_done"]:
        toss_winner = result["toss_winner"]
        toss_decision = result["toss_decision"]
        result["chasing_team"] = (
            (team2 if toss_winner == team1 else team1)
            if toss_decision == "bat" else toss_winner
        )

    # Print summary
    print(f"\n{'='*60}")
    print(f"RESULT: {result['team1']} vs {result['team2']}")
    print(f"Venue: {result['venue']}")
    print(f"Toss: {result['toss_winner']} - {result['toss_decision']}")
    print(f"XI Source: {result['xi_source']}")
    print(f"{result['team1']} XI: {result['team1_xi']}")
    print(f"{result['team2']} XI: {result['team2_xi']}")
    print(f"{'='*60}\n")

    return result


def build_feature_vector(
    match_info: Dict[str, Any],
    player_lookup: pd.DataFrame,
    matches: pd.DataFrame,
    team_encoder,
    venue_encoder,
    venue_score_history: pd.DataFrame,
    team_pp_eco_lookup: Dict[str, float],
    team_opener_lookup: Dict[str, Dict[str, float]],
    get_team_recent_avg_score,
    get_season_avg_score,
    get_season_year,
    get_venue_recent_avg_score,
    get_team_recent_high_score_rate,
    feature_cols: List[str],
) -> pd.DataFrame:
    """Build feature vector for prediction."""
    
    team1 = _normalize_team_name(match_info.get("team1"), team_encoder)
    team2 = _normalize_team_name(match_info.get("team2"), team_encoder)
    venue = _clean_text(match_info.get("venue", ""))
    now = pd.Timestamp(datetime.today().date())

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

    pp_runs_default = 50.0
    pp_sr_default = 130.0
    pp_wkts_default = 1.5
    pp_rr_default = 8.3

    feat = {c: 0.0 for c in feature_cols}
    feat.update({
        "team1": t1_id,
        "team2": t2_id,
        "venue": venue_id,
        "venue_avg_first_innings": venue_avg,
        "venue_recent_avg": venue_recent,
        "is_home_team1": 0,
        "toss_winner_is_team1": int(toss_done and toss_winner == team1),
        "toss_decision_bat": int(toss_done and toss_decision == "bat"),
        "h2h_team1_wins": t1_h2h,
        "h2h_team2_wins": t2_h2h,
        "chase_win_pct_team1": t1_cp,
        "chase_win_pct_team2": t2_cp,
        "high_score_chase_t1": t1_hc,
        "high_score_chase_t2": t2_hc,
        "winrate_team1": t1_wr,
        "winrate_team2": t2_wr,
        "last5_win_team1": t1_l5,
        "last5_win_team2": t2_l5,
        "t1_recent_avg_score": t1_ravg,
        "t2_recent_avg_score": t2_ravg,
        "t1_high_score_rate": t1_hsr,
        "t2_high_score_rate": t2_hsr,
        "t1_pp_bowling_economy": t1_pp,
        "t2_pp_bowling_economy": t2_pp,
        "season_avg_score": season_avg,
        "season_year": season_year,
        "t1_avg_batting_avg": t1_stats["batting_avg"],
        "t1_avg_strike_rate": t1_stats["strike_rate"],
        "t1_top3_batting_avg": t1_stats["top3_batting_avg"],
        "t1_avg_economy": t1_stats["economy"],
        "t1_avg_bowling_avg": t1_stats["bowling_avg"],
        "t1_recent_strike_rate": t1_stats["recent_strike_rate"],
        "t1_recent_economy": t1_stats["recent_economy"],
        "t2_avg_batting_avg": t2_stats["batting_avg"],
        "t2_avg_strike_rate": t2_stats["strike_rate"],
        "t2_top3_batting_avg": t2_stats["top3_batting_avg"],
        "t2_avg_economy": t2_stats["economy"],
        "t2_avg_bowling_avg": t2_stats["bowling_avg"],
        "t2_recent_strike_rate": t2_stats["recent_strike_rate"],
        "t2_recent_economy": t2_stats["recent_economy"],
        "t1_opener_batting_avg": float(t1_open.get("opener_avg_batting_avg", 30.0)),
        "t1_opener_strike_rate": float(t1_open.get("opener_avg_strike_rate", 130.0)),
        "t2_opener_batting_avg": float(t2_open.get("opener_avg_batting_avg", 30.0)),
        "t2_opener_strike_rate": float(t2_open.get("opener_avg_strike_rate", 130.0)),
        "t1_bat_vs_bowl": _safe_div(t1_stats["batting_avg"], t2_stats["bowling_avg"], 1.0),
        "t2_bat_vs_bowl": _safe_div(t2_stats["batting_avg"], t1_stats["bowling_avg"], 1.0),
        "t1_rolling_season_avg": t1_ravg,
        "t2_rolling_season_avg": t2_ravg,
        "team1_pp_runs": pp_runs_default,
        "team1_pp_strike_rate": pp_sr_default,
        "team1_pp_wickets": pp_wkts_default,
        "team1_pp_run_rate": pp_rr_default,
        "team2_pp_runs": pp_runs_default,
        "team2_pp_strike_rate": pp_sr_default,
        "team2_pp_wickets": pp_wkts_default,
        "team2_pp_run_rate": pp_rr_default,
        "pp_strength_diff": 0.0,
        "pp_run_rate_diff": 0.0,
    })

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)


if __name__ == "__main__":
    print("IPL Match Scraper - Updated Version")
    print("With Real-time Playing XI Support\n")
    
    match_id = get_todays_match_id()
    
    if match_id:
        print(f"Found match: {match_id}")
        result = scrape_match(match_id)
        print(json.dumps(result, indent=2, default=str))
    else:
        print("No live IPL match found")
