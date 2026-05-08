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
MATCH_INFO_URL = "https://www.cricbuzz.com/api/html/cricket-scorecard/{match_id}"

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

# Reverse lookup: full name to abbreviation
TEAM_TO_ABBR = {v: k for k, v in TEAM_ABBREVIATIONS.items()}

# ═══════════════════════════════════════════════════════════
# HARDCODED XI - Add new matches here after toss (BACKUP ONLY)
# ═══════════════════════════════════════════════════════════
KNOWN_XI: Dict[int, Dict[str, List[str]]] = {
    # Example format - add matches as needed
    # 152064: {
    #     "Delhi Capitals": ["Player1", "Player2", ...],
    #     "Kolkata Knight Riders": ["Player1", "Player2", ...],
    # },
}

# ─────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────
def _clean_text(value: Any) -> str:
    """Clean and normalize text, removing extra whitespace."""
    return re.sub(r"\s+", " ", str(value or "")).strip()

def _request_soup(url: str, timeout: int = 20) -> BeautifulSoup:
    """Make HTTP request and return BeautifulSoup object."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def _request_json(url: str, timeout: int = 20) -> dict:
    """Make HTTP request and return JSON response."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

def _request_text(url: str, timeout: int = 20) -> str:
    """Make HTTP request and return raw text."""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.text

def _correct_team_name(name: str) -> str:
    """Map variant spellings to canonical CSV name."""
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
    """Normalize team name using aliases and encoder."""
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
    """Safely encode a value, with fallback to first class."""
    classes = list(encoder.classes_)
    if value in classes:
        return int(encoder.transform([value])[0])
    for cls in classes:
        if cls.lower() == value.lower():
            return int(encoder.transform([cls])[0])
    return int(encoder.transform([classes[0]])[0])

def _safe_div(num: float, den: float, fallback: float) -> float:
    """Safe division with fallback."""
    return float(num / den) if den else float(fallback)

def _extract_player_name(text: str) -> str:
    """Extract clean player name from various formats."""
    # Remove common suffixes like (c), (wk), †, etc.
    name = re.sub(r'\s*[\(\[].*?[\)\]]', '', text)
    name = re.sub(r'[†*]', '', name)
    name = _clean_text(name)
    return name

# ─────────────────────────────────────────────────────────
# ESPN CRICINFO FUNCTIONS
# ─────────────────────────────────────────────────────────
def _get_espn_live_match(match_id: Optional[int] = None) -> Optional[dict]:
    """Get live match data from ESPN Cricinfo."""
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

def _extract_xi_from_scorecard(scorecard: dict) -> Dict[str, List[str]]:
    """Extract Playing XI from ESPN scorecard."""
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

def _extract_xi_from_match_info(match_info: dict, team1: str, team2: str) -> Dict[str, List[str]]:
    """Extract Playing XI from ESPN match_info (works before match starts)."""
    team_xi: Dict[str, List[str]] = {}
    
    try:
        # Try multiple paths to find squad/playing XI data
        content = match_info.get("content", match_info)
        
        # Path 1: Check for playing XI in match info
        playing_xi = content.get("playingXI", {})
        if playing_xi:
            for team_key, players in playing_xi.items():
                team_name = _correct_team_name(team_key)
                if isinstance(players, list):
                    names = [_extract_player_name(p.get("longName", p.get("name", ""))) 
                             for p in players if isinstance(p, dict)]
                    if not names:
                        names = [_extract_player_name(str(p)) for p in players if p]
                    if team_name and names:
                        team_xi[team_name] = names[:11]
                        print(f"[ESPN-INFO] Found {len(names)} players for {team_name} from playingXI")
        
        # Path 2: Check matchPlayers
        match_players = content.get("matchPlayers", {})
        if match_players and not team_xi:
            team_players = match_players.get("teamPlayers", [])
            for entry in team_players:
                team_name = _correct_team_name(
                    _clean_text(entry.get("team", {}).get("longName", ""))
                )
                players = entry.get("players", []) or []
                xi_players = [p for p in players if p.get("playerRoleType") == "PLAYING"]
                if not xi_players:
                    xi_players = players[:11]
                names = [_extract_player_name(p.get("player", {}).get("longName", "")) 
                         for p in xi_players]
                names = [n for n in names if n]
                if team_name and names:
                    team_xi[team_name] = names[:11]
                    print(f"[ESPN-INFO] Found {len(names)} players for {team_name} from matchPlayers")
        
        # Path 3: Check squads
        squads = content.get("squads", [])
        if squads and not team_xi:
            for squad in squads:
                team_name = _correct_team_name(
                    _clean_text(squad.get("team", {}).get("longName", ""))
                )
                players = squad.get("players", [])
                # Try to find playing XI within squad
                xi = [p for p in players if p.get("isPlaying", False)]
                if not xi:
                    xi = players[:11]
                names = [_extract_player_name(p.get("longName", p.get("name", ""))) 
                         for p in xi]
                names = [n for n in names if n]
                if team_name and names:
                    team_xi[team_name] = names[:11]
                    print(f"[ESPN-INFO] Found {len(names)} players for {team_name} from squads")
                    
    except Exception as e:
        print(f"[ESPN-INFO] _extract_xi_from_match_info error: {e}")
    
    return team_xi

def _extract_toss_from_espn_info(info: dict, team1: str, team2: str) -> Tuple[str, Optional[str]]:
    """Extract toss from ESPN match_info."""
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
            has_winner = any(k.lower() in {
                "tosswinner", "tosswinnerid", "winnerid", "winner"
            } for k in obj)
            has_decision = any(k.lower() in {
                "decision", "tossdecision", "elected", "choice"
            } for k in obj)
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
        toss.get("winner_team", "")
        or toss.get("tossWinner", "")
        or toss.get("winner", "")
        or ""
    )

    td_raw = _clean_text(
        toss.get("decision", "")
        or toss.get("tossDecision", "")
        or ""
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

# ─────────────────────────────────────────────────────────
# CRICBUZZ PLAYING XI EXTRACTION (NEW!)
# ─────────────────────────────────────────────────────────
def get_playing_xi_from_cricbuzz(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz match page.
    This is the PRIMARY source for live Playing XI data.
    """
    team_xi: Dict[str, List[str]] = {}
    
    try:
        # Try multiple URLs for Playing XI
        urls_to_try = [
            MATCH_URL_TEMPLATE.format(match_id=match_id),
            MATCH_SQUADS_URL.format(match_id=match_id),
            f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/playing-xi",
        ]
        
        for url in urls_to_try:
            try:
                print(f"[CB-XI] Trying URL: {url}")
                soup = _request_soup(url)
                
                # Method 1: Look for Playing XI section with specific class
                xi_sections = soup.select(".cb-col.cb-col-100.cb-play11-lft-col")
                if not xi_sections:
                    xi_sections = soup.select(".cb-play11-lft-col")
                if not xi_sections:
                    xi_sections = soup.select("[class*='play11']")
                if not xi_sections:
                    xi_sections = soup.select("[class*='playing']")
                
                if xi_sections:
                    print(f"[CB-XI] Found {len(xi_sections)} XI sections")
                    current_team = None
                    
                    for section in xi_sections:
                        # Find team header
                        team_header = section.select_one(".cb-col.cb-col-100.cb-font-14")
                        if team_header:
                            team_text = _clean_text(team_header.get_text())
                            for t in [team1, team2]:
                                if t and (t.lower() in team_text.lower() or 
                                         TEAM_TO_ABBR.get(t, "").lower() in team_text.lower()):
                                    current_team = t
                                    break
                        
                        # Find player names
                        player_links = section.select("a[href*='/profiles/']")
                        if not player_links:
                            player_links = section.select("a[href*='/cricket-player/']")
                        
                        if current_team and player_links:
                            names = []
                            for link in player_links[:11]:
                                name = _extract_player_name(link.get_text())
                                if name and len(name) > 2:
                                    names.append(name)
                            if names:
                                team_xi[current_team] = names[:11]
                                print(f"[CB-XI] Found {len(names)} players for {current_team}")
                
                # Method 2: Look for squad/bench section
                if len(team_xi) < 2:
                    squad_divs = soup.select(".cb-col.cb-col-100.cb-ltst-wgt-hdr")
                    for div in squad_divs:
                        header_text = _clean_text(div.get_text())
                        
                        # Check which team this belongs to
                        current_team = None
                        for t in [team1, team2]:
                            if t and t.lower() in header_text.lower():
                                current_team = t
                                break
                        
                        if current_team and "playing" in header_text.lower():
                            # Find the player list that follows
                            next_div = div.find_next_sibling()
                            if next_div:
                                player_links = next_div.select("a[href*='/profiles/']")
                                names = [_extract_player_name(link.get_text()) 
                                        for link in player_links[:11]]
                                names = [n for n in names if n and len(n) > 2]
                                if names:
                                    team_xi[current_team] = names[:11]
                                    print(f"[CB-XI] Found {len(names)} players for {current_team} (method 2)")
                
                # Method 3: Parse from inline JavaScript/JSON
                if len(team_xi) < 2:
                    page_text = str(soup)
                    
                    # Look for JSON data in script tags
                    script_tags = soup.select("script")
                    for script in script_tags:
                        script_content = script.string or ""
                        if "playingXI" in script_content or "playing11" in script_content.lower():
                            # Try to extract JSON
                            json_match = re.search(r'\{[^{}]*"playingXI"[^{}]*\}', script_content)
                            if json_match:
                                try:
                                    data = json.loads(json_match.group())
                                    # Process the JSON data
                                    print(f"[CB-XI] Found playingXI in script tag")
                                except json.JSONDecodeError:
                                    pass
                
                # Method 4: Look for player list in match info section
                if len(team_xi) < 2:
                    info_items = soup.select(".cb-mtch-info-itm")
                    for item in info_items:
                        label = item.select_one(".cb-col.cb-col-27")
                        value = item.select_one(".cb-col.cb-col-73")
                        if label and value:
                            label_text = _clean_text(label.get_text()).lower()
                            if "playing" in label_text or "squad" in label_text:
                                player_links = value.select("a")
                                names = [_extract_player_name(link.get_text()) 
                                        for link in player_links[:11]]
                                names = [n for n in names if n and len(n) > 2]
                                print(f"[CB-XI] Found players in info section: {names}")
                
                # Method 5: Extract from commentary/match status text
                if len(team_xi) < 2:
                    all_text = soup.get_text()
                    
                    # Pattern: "Team Name (Playing XI): Player1, Player2, ..."
                    xi_pattern = r'([A-Za-z\s]+)\s*\(?Playing\s*XI\)?\s*:?\s*([A-Za-z\s,]+)'
                    matches = re.findall(xi_pattern, all_text, re.IGNORECASE)
                    
                    for team_name, players_str in matches:
                        team_name = _clean_text(team_name)
                        for t in [team1, team2]:
                            if t and t.lower() in team_name.lower():
                                players = [_extract_player_name(p.strip()) 
                                          for p in players_str.split(",")]
                                players = [p for p in players if p and len(p) > 2][:11]
                                if players and len(players) >= 5:
                                    team_xi[t] = players
                                    print(f"[CB-XI] Found {len(players)} players for {t} (method 5)")
                                break
                
                if len(team_xi) == 2:
                    break  # Found both teams, exit URL loop
                    
            except Exception as e:
                print(f"[CB-XI] Error with URL {url}: {e}")
                continue
        
    except Exception as e:
        print(f"[CB-XI] get_playing_xi_from_cricbuzz error: {e}")
    
    return team_xi

def get_playing_xi_from_cricbuzz_api(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Try to get Playing XI from Cricbuzz's internal API endpoints.
    """
    team_xi: Dict[str, List[str]] = {}
    
    api_urls = [
        f"https://www.cricbuzz.com/api/html/cricket-scorecard/{match_id}",
        f"https://www.cricbuzz.com/api/cricket-match/{match_id}/full-commentary",
        f"https://www.cricbuzz.com/match-api/{match_id}/commentary.json",
    ]
    
    for api_url in api_urls:
        try:
            print(f"[CB-API] Trying: {api_url}")
            
            # Try JSON first
            try:
                data = _request_json(api_url)
                
                # Look for playing XI in various locations
                for key in ["playingXI", "playing11", "squads", "teams"]:
                    if key in data:
                        squad_data = data[key]
                        if isinstance(squad_data, dict):
                            for team_key, players in squad_data.items():
                                team_name = _correct_team_name(team_key)
                                if isinstance(players, list):
                                    names = [_extract_player_name(
                                        p.get("name", p.get("fullName", str(p))) if isinstance(p, dict) else str(p)
                                    ) for p in players]
                                    names = [n for n in names if n][:11]
                                    if team_name and names:
                                        team_xi[team_name] = names
                                        print(f"[CB-API] Found {len(names)} players for {team_name}")
                        elif isinstance(squad_data, list):
                            for entry in squad_data:
                                if isinstance(entry, dict):
                                    team_name = _correct_team_name(
                                        entry.get("teamName", entry.get("name", ""))
                                    )
                                    players = entry.get("players", entry.get("squad", []))
                                    if isinstance(players, list):
                                        names = [_extract_player_name(
                                            p.get("name", p.get("fullName", str(p))) if isinstance(p, dict) else str(p)
                                        ) for p in players]
                                        names = [n for n in names if n][:11]
                                        if team_name and names:
                                            team_xi[team_name] = names
                                            print(f"[CB-API] Found {len(names)} players for {team_name}")
                
                if len(team_xi) == 2:
                    return team_xi
                    
            except (json.JSONDecodeError, requests.exceptions.JSONDecodeError):
                # Try HTML parsing
                text = _request_text(api_url)
                soup = BeautifulSoup(text, "html.parser")
                
                player_links = soup.select("a[href*='/profiles/']")
                if player_links:
                    # Group by proximity to team headers
                    print(f"[CB-API] Found {len(player_links)} player links in HTML")
                    
        except Exception as e:
            print(f"[CB-API] Error with {api_url}: {e}")
            continue
    
    return team_xi

def get_playing_xi_from_scorecard_page(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Extract Playing XI from the scorecard page (works once batting has started).
    """
    team_xi: Dict[str, List[str]] = {}
    
    try:
        url = f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}"
        print(f"[CB-SCORE] Fetching scorecard: {url}")
        soup = _request_soup(url)
        
        # Find batting sections
        innings_sections = soup.select(".cb-col.cb-col-100.cb-ltst-wgt-hdr")
        
        current_team = None
        for section in innings_sections:
            header_text = _clean_text(section.get_text())
            
            # Check if this is a team header
            for t in [team1, team2]:
                if t:
                    t_lower = t.lower()
                    abbr = TEAM_TO_ABBR.get(t, "").lower()
                    if t_lower in header_text.lower() or (abbr and abbr in header_text.lower()):
                        current_team = t
                        break
            
            if current_team and current_team not in team_xi:
                # Find all batsmen in this section
                batsman_rows = section.find_parent().select(".cb-col.cb-col-100.cb-scrd-itms") if section.find_parent() else []
                
                names = []
                for row in batsman_rows:
                    player_link = row.select_one("a.cb-text-link")
                    if player_link:
                        name = _extract_player_name(player_link.get_text())
                        if name and len(name) > 2 and name not in names:
                            names.append(name)
                
                if names:
                    team_xi[current_team] = names[:11]
                    print(f"[CB-SCORE] Found {len(names)} batsmen for {current_team}")
        
        # Also check bowling section to get remaining players
        bowling_sections = soup.select(".cb-col.cb-col-100.cb-scrd-itms")
        for section in bowling_sections:
            bowler_link = section.select_one("a.cb-text-link")
            if bowler_link:
                name = _extract_player_name(bowler_link.get_text())
                # Try to add to appropriate team (opponent of batting team)
                for t in [team1, team2]:
                    if t in team_xi and name not in team_xi[t]:
                        other_team = team2 if t == team1 else team1
                        if other_team not in team_xi:
                            team_xi[other_team] = []
                        if name not in team_xi[other_team] and len(team_xi[other_team]) < 11:
                            team_xi[other_team].append(name)
                        
    except Exception as e:
        print(f"[CB-SCORE] Error: {e}")
    
    return team_xi

# ─────────────────────────────────────────────────────────
# CRICBUZZ TOSS & TEAM EXTRACTION
# ─────────────────────────────────────────────────────────
def get_toss_from_cricbuzz(match_id: int, team1: str, team2: str) -> Tuple[str, Optional[str]]:
    """Extract toss info from Cricbuzz HTML."""
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

                print(f"[CB-TOSS] Found pattern: '{team_part}' opt to '{decision}'")

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

            print(f"[CB-TOSS] Found Toss pattern: '{team_name}' ({decision})")

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
                print(f"[CB-TOSS] SUCCESS: {toss_winner} elected to {toss_decision}")
                return toss_winner, toss_decision

        print("[CB-TOSS] No toss information found")
        return "", None

    except Exception as e:
        print(f"[CB-TOSS] Error: {e}")
        return "", None

def get_teams_from_cricbuzz(match_id: int) -> Tuple[str, str]:
    """Extract team names from Cricbuzz HTML."""
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
                    print(f"[CB-TEAMS] Found from title: {team1} vs {team2}")
                    return team1, team2

        return "Unknown", "Unknown"
    except Exception as e:
        print(f"[CB-TEAMS] Error: {e}")
        return "Unknown", "Unknown"

def get_venue_from_cricbuzz(match_id: int) -> str:
    """Extract venue from Cricbuzz HTML."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.select("a[href*='/venues/']"):
            text = _clean_text(link.get_text())
            if text and len(text) > 5:
                print(f"[CB-VENUE] Found: {text}")
                return text

        return "Unknown Venue"
    except Exception as e:
        print(f"[CB-VENUE] Error: {e}")
        return "Unknown Venue"

def get_hardcoded_xi(match_id: int, team1: str, team2: str) -> Tuple[List[str], List[str]]:
    """Get hardcoded XI if available for this match (LAST RESORT)."""
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
    """Calculate team win rate overall and last 5 matches."""
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.5
    return (
        float((tm["winner"] == team).mean()),
        float((tm.tail(5)["winner"] == team).mean()),
    )

def _h2h(matches: pd.DataFrame, team1: str, team2: str) -> Tuple[int, int]:
    """Calculate head-to-head wins."""
    h = matches[
        ((matches["team1"] == team1) & (matches["team2"] == team2)) |
        ((matches["team1"] == team2) & (matches["team2"] == team1))
    ]
    if h.empty:
        return 0, 0
    return int((h["winner"] == team1).sum()), int((h["winner"] == team2).sum())

def _chase_metrics(matches: pd.DataFrame, team: str) -> Tuple[float, float]:
    """Calculate chase win percentage."""
    if "win_by_wickets" not in matches.columns:
        return 0.5, 0.4
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.4
    cw = tm[(tm["winner"] == team) & (tm["win_by_wickets"] > 0)]
    return _safe_div(len(cw), len(tm), 0.5), 0.4 if cw.empty else 1.0

def _global_player_defaults(player_lookup: pd.DataFrame) -> Dict[str, float]:
    """Get global player stat defaults."""
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
    """Calculate player stats for a given XI."""
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
    
    IMPROVED: Now tries multiple sources for Playing XI:
    1. Cricbuzz HTML scraping (primary)
    2. Cricbuzz API endpoints
    3. Cricbuzz scorecard page
    4. ESPN Cricinfo scorecard
    5. ESPN Cricinfo match info
    6. Hardcoded fallback
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
        "xi_source": None,  # NEW: Track where XI came from
        "source": "combined",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

    # ── 1. Get team names first ──────────────────────────
    print("[SCRAPE] Step 1: Getting team names...")
    team1, team2 = get_teams_from_cricbuzz(match_id)
    result["team1"] = team1
    result["team2"] = team2
    
    # ── 2. Get venue ─────────────────────────────────────
    print("[SCRAPE] Step 2: Getting venue...")
    result["venue"] = get_venue_from_cricbuzz(match_id)
    
    # ── 3. Get toss information ──────────────────────────
    print("[SCRAPE] Step 3: Getting toss...")
    tw, td = get_toss_from_cricbuzz(match_id, team1, team2)
    if tw and td:
        result["toss_winner"] = tw
        result["toss_decision"] = td
        result["toss_done"] = True
        print(f"[SCRAPE] Toss: {tw} elected to {td}")
    
    # ── 4. Get Playing XI (IMPROVED - Multiple Sources) ─
    print("[SCRAPE] Step 4: Getting Playing XI...")
    team_xi: Dict[str, List[str]] = {}
    
    # Source 1: Cricbuzz HTML scraping
    if not team_xi or len(team_xi) < 2:
        print("[SCRAPE] Trying Cricbuzz HTML scraping...")
        team_xi = get_playing_xi_from_cricbuzz(match_id, team1, team2)
        if len(team_xi) == 2:
            result["xi_source"] = "cricbuzz_html"
    
    # Source 2: Cricbuzz API endpoints
    if not team_xi or len(team_xi) < 2:
        print("[SCRAPE] Trying Cricbuzz API...")
        api_xi = get_playing_xi_from_cricbuzz_api(match_id, team1, team2)
        for team, players in api_xi.items():
            if team not in team_xi or not team_xi[team]:
                team_xi[team] = players
        if len(team_xi) == 2 and not result["xi_source"]:
            result["xi_source"] = "cricbuzz_api"
    
    # Source 3: Cricbuzz scorecard page
    if not team_xi or len(team_xi) < 2:
        print("[SCRAPE] Trying Cricbuzz scorecard...")
        score_xi = get_playing_xi_from_scorecard_page(match_id, team1, team2)
        for team, players in score_xi.items():
            if team not in team_xi or not team_xi[team]:
                team_xi[team] = players
        if len(team_xi) == 2 and not result["xi_source"]:
            result["xi_source"] = "cricbuzz_scorecard"
    
    # Source 4: ESPN Cricinfo
    if (not team_xi or len(team_xi) < 2) and CRICDATA_AVAILABLE:
        print("[SCRAPE] Trying ESPN Cricinfo...")
        try:
            espn_match = _get_espn_live_match(match_id=match_id)
            if espn_match:
                series = espn_match.get("series", {})
                s_slug = f"{series.get('slug')}-{series.get('objectId')}"
                m_slug = f"{espn_match.get('slug')}-{espn_match.get('objectId')}"
                
                # Try match info first (works before match)
                info = CRICINFO_CLIENT.match_info(s_slug, m_slug)
                espn_xi = _extract_xi_from_match_info(info, team1, team2)
                for team, players in espn_xi.items():
                    if team not in team_xi or not team_xi[team]:
                        team_xi[team] = players
                
                # Then try scorecard
                if len(team_xi) < 2:
                    scorecard = CRICINFO_CLIENT.match_scorecard(s_slug, m_slug)
                    score_xi = _extract_xi_from_scorecard(scorecard)
                    for team, players in score_xi.items():
                        if team not in team_xi or not team_xi[team]:
                            team_xi[team] = players
                
                if len(team_xi) == 2 and not result["xi_source"]:
                    result["xi_source"] = "espn"
                    
        except Exception as e:
            print(f"[SCRAPE] ESPN error: {e}")
    
    # Source 5: Hardcoded fallback
    if not team_xi or len(team_xi) < 2:
        print("[SCRAPE] Trying hardcoded XI...")
        hc_t1, hc_t2 = get_hardcoded_xi(match_id, team1, team2)
        if hc_t1 and team1 not in team_xi:
            team_xi[team1] = hc_t1
        if hc_t2 and team2 not in team_xi:
            team_xi[team2] = hc_t2
        if len(team_xi) == 2 and not result["xi_source"]:
            result["xi_source"] = "hardcoded"
    
    # Assign XI to result
    result["team1_xi"] = team_xi.get(team1, [])
    result["team2_xi"] = team_xi.get(team2, [])
    
    # ── 5. Calculate chasing team ────────────────────────
    if result["toss_done"]:
        toss_winner = result["toss_winner"]
        toss_decision = result["toss_decision"]
        result["chasing_team"] = (
            (team2 if toss_winner == team1 else team1)
            if toss_decision == "bat" else toss_winner
        )

    # ── Summary ──────────────────────────────────────────
    print(f"\n[SCRAPE] ═══════════════════════════════════════")
    print(f"[SCRAPE] FINAL RESULT:")
    print(f"[SCRAPE]   Teams: {result['team1']} vs {result['team2']}")
    print(f"[SCRAPE]   Venue: {result['venue']}")
    print(f"[SCRAPE]   Toss: {result['toss_winner']} / {result['toss_decision']}")
    print(f"[SCRAPE]   Team1 XI ({len(result['team1_xi'])} players): {result['team1_xi'][:3]}...")
    print(f"[SCRAPE]   Team2 XI ({len(result['team2_xi'])} players): {result['team2_xi'][:3]}...")
    print(f"[SCRAPE]   XI Source: {result['xi_source']}")
    print(f"[SCRAPE] ═══════════════════════════════════════\n")

    return result

# ─────────────────────────────────────────────────────────
# FEATURE VECTOR BUILDER
# ─────────────────────────────────────────────────────────
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

    print(f"[FEAT] Feature vector built. Total cols: {len(feat)}")

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)


# ─────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Example usage
    print("IPL Match Scraper - Testing...")
    
    # Get today's match
    match_id = get_todays_match_id()
    if match_id:
        print(f"\nFound today's match: {match_id}")
        result = scrape_match(match_id)
        print(f"\nResult: {json.dumps(result, indent=2, default=str)}")
    else:
        print("No live IPL match found")
        
        # Test with a specific match ID
        test_id = 152064  # Example match ID
        print(f"\nTesting with match ID: {test_id}")
        result = scrape_match(test_id)
        print(f"\nResult: {json.dumps(result, indent=2, default=str)}")
