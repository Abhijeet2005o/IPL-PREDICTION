"""
IPL Match Scraper - COMPLETE FIXED VERSION
==========================================
- Only scrapes IPL matches (rejects international matches like BAN vs PAK)
- Validates team names against valid IPL teams
- Multiple sources for Playing XI
- Proper error handling

Author: Updated 2026
"""

import re
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════
# IPL TEAMS - ONLY THESE ARE VALID
# ══════════════════════════════════════════════════════════════

IPL_TEAMS_FULL = {
    "Chennai Super Kings": "CSK",
    "Mumbai Indians": "MI",
    "Royal Challengers Bangalore": "RCB",
    "Royal Challengers Bengaluru": "RCB",
    "Kolkata Knight Riders": "KKR",
    "Delhi Capitals": "DC",
    "Gujarat Titans": "GT",
    "Lucknow Super Giants": "LSG",
    "Rajasthan Royals": "RR",
    "Punjab Kings": "PBKS",
    "Sunrisers Hyderabad": "SRH",
}

IPL_TEAM_ABBR = {
    "CSK": "Chennai Super Kings",
    "MI": "Mumbai Indians",
    "RCB": "Royal Challengers Bangalore",
    "KKR": "Kolkata Knight Riders",
    "DC": "Delhi Capitals",
    "GT": "Gujarat Titans",
    "LSG": "Lucknow Super Giants",
    "RR": "Rajasthan Royals",
    "PBKS": "Punjab Kings",
    "SRH": "Sunrisers Hyderabad",
    # Old/alternate abbreviations
    "DD": "Delhi Capitals",
    "KXIP": "Punjab Kings",
    "RPS": "Rising Pune Supergiant",
    "PWI": "Pune Warriors India",
    "GL": "Gujarat Lions",
    "KTK": "Kochi Tuskers Kerala",
    "DCH": "Deccan Chargers",
}

# International teams to explicitly reject
INTERNATIONAL_TEAMS = {
    "IND", "PAK", "AUS", "ENG", "SA", "NZ", "WI", "SL", "BAN", "AFG", "ZIM", "IRE", "SCO", "NEP", "UAE", "HK", "PNG", "NAM", "OMA", "USA", "CAN",
    "INDIA", "PAKISTAN", "AUSTRALIA", "ENGLAND", "SOUTH AFRICA", "NEW ZEALAND", "WEST INDIES", "SRI LANKA", "BANGLADESH", "AFGHANISTAN", "ZIMBABWE", "IRELAND"
}

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════

RAPIDAPI_KEY = None  # Optional: Get from https://rapidapi.com/cricbuzz-cricket-api

try:
    from cricdata import CricinfoClient
    CRICINFO_CLIENT = CricinfoClient()
    CRICDATA_AVAILABLE = True
except Exception:
    CRICINFO_CLIENT = None
    CRICDATA_AVAILABLE = False

# URLs
IPL_SERIES_ID = "9241"  # IPL 2026
LIVE_SCORES_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
SQUAD_URL_TEMPLATE = "https://www.cricbuzz.com/cricket-match-squads/{match_id}"
SCORECARD_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scorecard/{match_id}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ══════════════════════════════════════════════════════════════
# HARDCODED PLAYING XI (Backup - add after toss)
# ══════════════════════════════════════════════════════════════

KNOWN_XI: Dict[int, Dict[str, List[str]]] = {
    # Example - add your match here after toss announcement:
    # 115200: {
    #     "Chennai Super Kings": ["Ruturaj Gaikwad", "Devon Conway", ...],
    #     "Mumbai Indians": ["Rohit Sharma", "Ishan Kishan", ...],
    # },
}

# ══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════

def _clean_text(value: Any) -> str:
    """Clean and normalize text."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _extract_player_name(text: str) -> str:
    """Extract clean player name - removes (c), (wk), †, * etc."""
    name = re.sub(r'\s*[\(\[].*?[\)\]]', '', str(text))
    name = re.sub(r'[†*]', '', name)
    return _clean_text(name)


def _request_soup(url: str, timeout: int = 20) -> BeautifulSoup:
    """Make HTTP request and return BeautifulSoup object."""
    print(f"[HTTP] GET {url}")
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _safe_div(num: float, den: float, fallback: float = 0.0) -> float:
    """Safe division with fallback."""
    return float(num / den) if den else float(fallback)


# ══════════════════════════════════════════════════════════════
# IPL TEAM VALIDATION
# ══════════════════════════════════════════════════════════════

def is_international_team(team_name: str) -> bool:
    """Check if team is an international team (NOT IPL)."""
    if not team_name:
        return False
    team_upper = team_name.upper().strip()
    return team_upper in INTERNATIONAL_TEAMS


def is_ipl_team(team_name: str) -> bool:
    """Check if team name is a valid IPL team."""
    if not team_name:
        return False
    
    # First check if it's an international team
    if is_international_team(team_name):
        return False
    
    team_upper = team_name.upper().strip()
    team_lower = team_name.lower().strip()
    
    # Check abbreviation
    if team_upper in IPL_TEAM_ABBR:
        return True
    
    # Check full name
    for ipl_team in IPL_TEAMS_FULL.keys():
        if ipl_team.lower() == team_lower:
            return True
        # Partial match (e.g., "Chennai" matches "Chennai Super Kings")
        if len(team_lower) > 3:
            if team_lower in ipl_team.lower() or ipl_team.lower() in team_lower:
                return True
    
    return False


def normalize_ipl_team(team_name: str) -> Optional[str]:
    """
    Convert team name to standard IPL team name.
    Returns None if not an IPL team.
    """
    if not team_name:
        return None
    
    team_clean = _clean_text(team_name)
    team_upper = team_clean.upper()
    team_lower = team_clean.lower()
    
    # Check if international team first
    if is_international_team(team_clean):
        print(f"[VALIDATE] ❌ '{team_clean}' is an international team, not IPL!")
        return None
    
    # Check abbreviation first
    if team_upper in IPL_TEAM_ABBR:
        return IPL_TEAM_ABBR[team_upper]
    
    # Check full names
    for full_name, abbr in IPL_TEAMS_FULL.items():
        if full_name.lower() == team_lower:
            # Normalize Bengaluru -> Bangalore
            if "bengaluru" in full_name.lower():
                return "Royal Challengers Bangalore"
            return full_name
        # Partial match
        if len(team_lower) > 3 and team_lower in full_name.lower():
            if "bengaluru" in full_name.lower():
                return "Royal Challengers Bangalore"
            return full_name
    
    # Special corrections
    if "bengaluru" in team_lower:
        return "Royal Challengers Bangalore"
    if "chennai" in team_lower:
        return "Chennai Super Kings"
    if "mumbai" in team_lower:
        return "Mumbai Indians"
    if "kolkata" in team_lower:
        return "Kolkata Knight Riders"
    if "delhi" in team_lower:
        return "Delhi Capitals"
    if "gujarat" in team_lower:
        return "Gujarat Titans"
    if "lucknow" in team_lower:
        return "Lucknow Super Giants"
    if "rajasthan" in team_lower:
        return "Rajasthan Royals"
    if "punjab" in team_lower:
        return "Punjab Kings"
    if "hyderabad" in team_lower or "sunrisers" in team_lower:
        return "Sunrisers Hyderabad"
    
    return None


def validate_ipl_match(team1: str, team2: str) -> Tuple[bool, str, str, Optional[str]]:
    """
    Validate that both teams are IPL teams.
    Returns: (is_valid, normalized_team1, normalized_team2, error_message)
    """
    t1_normalized = normalize_ipl_team(team1)
    t2_normalized = normalize_ipl_team(team2)
    
    errors = []
    
    if not t1_normalized:
        errors.append(f"'{team1}' is NOT an IPL team")
    
    if not t2_normalized:
        errors.append(f"'{team2}' is NOT an IPL team")
    
    if errors:
        valid_teams = ", ".join(IPL_TEAM_ABBR.keys())
        error_msg = f"{' | '.join(errors)}. Valid IPL teams: {valid_teams}"
        print(f"[VALIDATE] ❌ {error_msg}")
        return False, team1, team2, error_msg
    
    print(f"[VALIDATE] ✅ Valid IPL match: {t1_normalized} vs {t2_normalized}")
    return True, t1_normalized, t2_normalized, None


# ══════════════════════════════════════════════════════════════
# GET LIVE IPL MATCH ID
# ══════════════════════════════════════════════════════════════

def get_live_ipl_match_id() -> Optional[int]:
    """
    Get current live IPL match ID.
    Only returns ID if it's actually an IPL match.
    """
    print("\n[SEARCH] Looking for live IPL matches...")
    
    try:
        soup = _request_soup(LIVE_SCORES_URL)
        
        # Look for IPL matches specifically
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            href = link.get("href", "").lower()
            text = _clean_text(link.get_text()).lower()
            title = link.get("title", "").lower()
            
            # Must contain "ipl" or "indian premier league"
            is_ipl = (
                "indian-premier-league" in href or
                "-ipl-" in href or
                "indian premier league" in text or
                "indian premier league" in title
            )
            
            if not is_ipl:
                continue
            
            # Extract match ID
            match = re.search(r"/live-cricket-scores/(\d+)", href)
            if match:
                match_id = int(match.group(1))
                
                # Verify teams are IPL teams
                combined_text = f"{text} {title}"
                vs_match = re.search(r"([A-Z]{2,4})\s+vs\.?\s+([A-Z]{2,4})", combined_text, re.I)
                if vs_match:
                    t1 = vs_match.group(1).upper()
                    t2 = vs_match.group(2).upper()
                    
                    if t1 in IPL_TEAM_ABBR and t2 in IPL_TEAM_ABBR:
                        print(f"[SEARCH] ✅ Found IPL match: {t1} vs {t2} (ID: {match_id})")
                        return match_id
                    else:
                        print(f"[SEARCH] ⚠️ Match {match_id} has non-IPL teams: {t1} vs {t2}")
                else:
                    # URL has IPL but can't verify teams - return it anyway
                    print(f"[SEARCH] Found potential IPL match: {match_id}")
                    return match_id
        
        print("[SEARCH] ❌ No live IPL match found")
        return None
        
    except Exception as e:
        print(f"[SEARCH] Error: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# TEAM, VENUE, TOSS EXTRACTION
# ══════════════════════════════════════════════════════════════

def get_teams_from_cricbuzz(match_id: int) -> Tuple[str, str]:
    """Extract team names from match page."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        soup = _request_soup(url)
        
        # Method 1: From page title
        title = soup.find("title")
        if title:
            title_text = _clean_text(title.get_text())
            print(f"[TEAMS] Page title: {title_text[:80]}...")
            
            # Try abbreviations first
            vs_match = re.search(r"([A-Z]{2,4})\s+vs\.?\s+([A-Z]{2,4})", title_text)
            if vs_match:
                t1 = vs_match.group(1).upper()
                t2 = vs_match.group(2).upper()
                print(f"[TEAMS] Found abbreviations: {t1} vs {t2}")
                return t1, t2
            
            # Try full names
            vs_match = re.search(r"([A-Za-z\s]+?)\s+vs\.?\s+([A-Za-z\s]+?)(?:,|\s*-|\s*\||$)", title_text)
            if vs_match:
                t1 = _clean_text(vs_match.group(1))
                t2 = _clean_text(vs_match.group(2))
                print(f"[TEAMS] Found names: {t1} vs {t2}")
                return t1, t2
        
        # Method 2: From header
        header = soup.select_one(".cb-nav-hdr, .cb-mat-mnu-wrp")
        if header:
            header_text = _clean_text(header.get_text())
            vs_match = re.search(r"([A-Z]{2,4})\s+vs\.?\s+([A-Z]{2,4})", header_text)
            if vs_match:
                return vs_match.group(1).upper(), vs_match.group(2).upper()
        
        # Method 3: From any link
        for link in soup.select("a"):
            text = _clean_text(link.get_text())
            if " vs " in text.lower():
                vs_match = re.search(r"([A-Z]{2,4})\s+vs\.?\s+([A-Z]{2,4})", text, re.I)
                if vs_match:
                    return vs_match.group(1).upper(), vs_match.group(2).upper()
        
        return "Unknown", "Unknown"
        
    except Exception as e:
        print(f"[TEAMS] Error: {e}")
        return "Unknown", "Unknown"


def get_venue_from_cricbuzz(match_id: int) -> str:
    """Extract venue from match page."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        soup = _request_soup(url)
        
        # Look for venue link
        for link in soup.select("a[href*='/cricket-grounds/'], a[href*='/venues/']"):
            venue = _clean_text(link.get_text())
            if venue and len(venue) > 5:
                print(f"[VENUE] Found: {venue}")
                return venue
        
        # Look in match info
        for item in soup.select(".cb-mtch-info-itm, .cb-col-60"):
            text = _clean_text(item.get_text())
            if "venue" in text.lower():
                # Extract venue name
                venue_match = re.search(r"venue[:\s]+(.+?)(?:\||$)", text, re.I)
                if venue_match:
                    return _clean_text(venue_match.group(1))
        
        return "Unknown Venue"
        
    except Exception as e:
        print(f"[VENUE] Error: {e}")
        return "Unknown Venue"


def get_toss_from_cricbuzz(match_id: int, team1: str, team2: str) -> Tuple[str, Optional[str]]:
    """Extract toss information."""
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        page_text = resp.text

        # Get abbreviations for matching
        t1_abbr = IPL_TEAMS_FULL.get(team1, team1[:3].upper() if team1 else "")
        t2_abbr = IPL_TEAMS_FULL.get(team2, team2[:3].upper() if team2 else "")

        # Patterns to find toss
        patterns = [
            r'([A-Z]{2,4})\s+opt(?:ed)?\s+to\s+(bat|bowl|field)',
            r'([A-Za-z\s]+?)\s+opt(?:ed)?\s+to\s+(bat|bowl|field)',
            r'([A-Z]{2,4})\s+won\s+(?:the\s+)?toss.*?(bat|bowl|field)',
            r'Toss[:\s]+([A-Za-z\s]+?),?\s+(?:elected|chose|opt).*?(bat|bowl|field)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                team_part = _clean_text(match[0]).upper()
                decision = match[1].lower() if len(match) > 1 else None

                toss_decision = "bat" if decision == "bat" else "field" if decision else None

                # Match team
                toss_winner = None
                
                # Check abbreviation match
                if team_part == t1_abbr or team_part == team1.upper()[:3]:
                    toss_winner = team1
                elif team_part == t2_abbr or team_part == team2.upper()[:3]:
                    toss_winner = team2
                else:
                    # Try normalizing
                    normalized = normalize_ipl_team(team_part)
                    if normalized == team1:
                        toss_winner = team1
                    elif normalized == team2:
                        toss_winner = team2

                if toss_winner and toss_decision:
                    print(f"[TOSS] ✅ {toss_winner} opted to {toss_decision}")
                    return toss_winner, toss_decision

        print("[TOSS] ⚠️ Toss not found or not yet done")
        return "", None

    except Exception as e:
        print(f"[TOSS] Error: {e}")
        return "", None


# ══════════════════════════════════════════════════════════════
# PLAYING XI EXTRACTION
# ══════════════════════════════════════════════════════════════

def get_playing_xi_from_squad_page(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Get Playing XI from Cricbuzz squad page.
    URL: https://www.cricbuzz.com/cricket-match-squads/{match_id}
    """
    team_xi: Dict[str, List[str]] = {}
    
    try:
        url = SQUAD_URL_TEMPLATE.format(match_id=match_id)
        soup = _request_soup(url)
        
        t1_abbr = IPL_TEAMS_FULL.get(team1, "")
        t2_abbr = IPL_TEAMS_FULL.get(team2, "")
        
        # Look for team sections
        current_team = None
        current_players: List[str] = []
        
        for element in soup.select(".cb-col, .cb-font-14, .cb-font-16"):
            text = _clean_text(element.get_text())
            text_lower = text.lower()
            
            # Check if this is a team header
            team_found = None
            for t, abbr in [(team1, t1_abbr), (team2, t2_abbr)]:
                if t and (t.lower() in text_lower or (abbr and abbr.lower() in text_lower)):
                    if "playing" in text_lower or "xi" in text_lower or "squad" in text_lower:
                        team_found = t
                        break
            
            if team_found:
                # Save previous team's players
                if current_team and current_players:
                    team_xi[current_team] = current_players[:11]
                current_team = team_found
                current_players = []
                continue
            
            # If we have a current team, look for player links
            if current_team:
                player_links = element.select("a[href*='/profiles/']")
                for link in player_links:
                    name = _extract_player_name(link.get_text())
                    if name and len(name) > 2 and name not in current_players:
                        current_players.append(name)
        
        # Save last team
        if current_team and current_players:
            team_xi[current_team] = current_players[:11]
        
        # Alternative: Find all player links and group by context
        if len(team_xi) < 2:
            all_links = soup.select("a[href*='/profiles/']")
            temp_players: Dict[str, List[str]] = {team1: [], team2: []}
            
            for link in all_links:
                name = _extract_player_name(link.get_text())
                if not name or len(name) < 3:
                    continue
                
                # Check parent context for team
                parent = link.find_parent(class_=re.compile(r"cb-col"))
                if parent:
                    parent_text = _clean_text(parent.get_text()).lower()
                    
                    if team1 and team1.lower() in parent_text:
                        if name not in temp_players[team1]:
                            temp_players[team1].append(name)
                    elif team2 and team2.lower() in parent_text:
                        if name not in temp_players[team2]:
                            temp_players[team2].append(name)
            
            for t, players in temp_players.items():
                if players and t not in team_xi:
                    team_xi[t] = players[:11]
        
        if team_xi:
            for t, players in team_xi.items():
                print(f"[SQUAD-PAGE] {t}: {len(players)} players")
        else:
            print("[SQUAD-PAGE] No Playing XI found")
        
    except Exception as e:
        print(f"[SQUAD-PAGE] Error: {e}")
    
    return team_xi


def get_playing_xi_from_scorecard(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """
    Get Playing XI from scorecard page (works after match starts).
    """
    team_xi: Dict[str, List[str]] = {}
    
    try:
        url = SCORECARD_URL_TEMPLATE.format(match_id=match_id)
        soup = _request_soup(url)
        
        t1_abbr = IPL_TEAMS_FULL.get(team1, "")
        t2_abbr = IPL_TEAMS_FULL.get(team2, "")
        
        # Find innings headers
        innings_headers = soup.select(".cb-scrd-hdr-rw, .cb-col-100.cb-scrd-sub-hdr")
        
        for header in innings_headers:
            header_text = _clean_text(header.get_text()).lower()
            
            # Identify team
            current_team = None
            for t, abbr in [(team1, t1_abbr), (team2, t2_abbr)]:
                if t and (t.lower() in header_text or (abbr and abbr.lower() in header_text)):
                    if "innings" in header_text:
                        current_team = t
                        break
            
            if current_team and current_team not in team_xi:
                team_xi[current_team] = []
                
                # Get players from batting rows
                parent = header.find_parent()
                if parent:
                    rows = parent.select(".cb-scrd-itms, .cb-col-100.cb-scrd-itms")
                    for row in rows:
                        player_link = row.select_one("a.cb-text-link, a[href*='/profiles/']")
                        if player_link:
                            name = _extract_player_name(player_link.get_text())
                            if name and len(name) > 2 and name not in team_xi[current_team]:
                                team_xi[current_team].append(name)
        
        # Get bowlers (belong to opposite team)
        bowling_sections = soup.select(".cb-col-38, .cb-bowl-col")
        for section in bowling_sections:
            bowler_link = section.select_one("a")
            if bowler_link:
                name = _extract_player_name(bowler_link.get_text())
                if name and len(name) > 2:
                    # Add to opposing team
                    for t in [team1, team2]:
                        if t in team_xi:
                            other = team2 if t == team1 else team1
                            if other not in team_xi:
                                team_xi[other] = []
                            if name not in team_xi[other] and len(team_xi[other]) < 11:
                                team_xi[other].append(name)
        
        if team_xi:
            for t, players in team_xi.items():
                print(f"[SCORECARD] {t}: {len(players)} players")
        
    except Exception as e:
        print(f"[SCORECARD] Error: {e}")
    
    return team_xi


def get_playing_xi_from_hardcoded(match_id: int, team1: str, team2: str) -> Dict[str, List[str]]:
    """Get Playing XI from hardcoded data (backup)."""
    team_xi: Dict[str, List[str]] = {}
    
    if match_id in KNOWN_XI:
        xi_data = KNOWN_XI[match_id]
        
        if team1 in xi_data:
            team_xi[team1] = xi_data[team1]
            print(f"[HARDCODED] {team1}: {len(xi_data[team1])} players")
        
        if team2 in xi_data:
            team_xi[team2] = xi_data[team2]
            print(f"[HARDCODED] {team2}: {len(xi_data[team2])} players")
    
    return team_xi


# ══════════════════════════════════════════════════════════════
# STAT HELPERS (for feature building)
# ══════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════
# MAIN SCRAPING FUNCTION
# ══════════════════════════════════════════════════════════════

def scrape_match(match_id: int) -> Dict[str, Any]:
    """
    Main function to scrape IPL match data.
    
    Returns error if match is not an IPL match.
    Tries multiple sources for Playing XI.
    """
    print(f"\n{'='*70}")
    print(f"  SCRAPING MATCH: {match_id}")
    print(f"{'='*70}\n")

    result: Dict[str, Any] = {
        "match_id": int(match_id),
        "is_ipl": False,
        "error": None,
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
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

    # ═══════════════════════════════════════════════════════════
    # STEP 1: Get team names
    # ═══════════════════════════════════════════════════════════
    print("[STEP 1] Getting team names...")
    team1_raw, team2_raw = get_teams_from_cricbuzz(match_id)
    print(f"  Raw teams: {team1_raw} vs {team2_raw}")

    # ═══════════════════════════════════════════════════════════
    # STEP 2: Validate IPL match
    # ═══════════════════════════════════════════════════════════
    print("\n[STEP 2] Validating IPL match...")
    is_valid, team1, team2, error_msg = validate_ipl_match(team1_raw, team2_raw)
    
    if not is_valid:
        result["error"] = f"NOT AN IPL MATCH! {error_msg}"
        result["team1"] = team1_raw
        result["team2"] = team2_raw
        print(f"\n{'='*70}")
        print(f"  ❌ ERROR: {result['error']}")
        print(f"{'='*70}\n")
        return result
    
    result["is_ipl"] = True
    result["team1"] = team1
    result["team2"] = team2
    print(f"  ✅ Valid IPL match: {team1} vs {team2}")

    # ═══════════════════════════════════════════════════════════
    # STEP 3: Get venue
    # ═══════════════════════════════════════════════════════════
    print("\n[STEP 3] Getting venue...")
    result["venue"] = get_venue_from_cricbuzz(match_id)

    # ═══════════════════════════════════════════════════════════
    # STEP 4: Get toss
    # ═══════════════════════════════════════════════════════════
    print("\n[STEP 4] Getting toss...")
    tw, td = get_toss_from_cricbuzz(match_id, team1, team2)
    if tw and td:
        result["toss_winner"] = tw
        result["toss_decision"] = td
        result["toss_done"] = True
    else:
        print("  ⚠️ Toss not yet done or not detected")

    # ═══════════════════════════════════════════════════════════
    # STEP 5: Get Playing XI (multiple sources)
    # ═══════════════════════════════════════════════════════════
    print("\n[STEP 5] Getting Playing XI...")
    team_xi: Dict[str, List[str]] = {}

    # Source 1: Squad Page
    print("\n  [5.1] Trying squad page...")
    team_xi = get_playing_xi_from_squad_page(match_id, team1, team2)
    if team_xi.get(team1) and team_xi.get(team2):
        result["xi_source"] = "squad_page"

    # Source 2: Scorecard
    if not team_xi.get(team1) or not team_xi.get(team2):
        print("\n  [5.2] Trying scorecard...")
        score_xi = get_playing_xi_from_scorecard(match_id, team1, team2)
        for team, players in score_xi.items():
            if team not in team_xi or not team_xi[team]:
                team_xi[team] = players
        if team_xi.get(team1) and team_xi.get(team2) and not result["xi_source"]:
            result["xi_source"] = "scorecard"

    # Source 3: Hardcoded
    if not team_xi.get(team1) or not team_xi.get(team2):
        print("\n  [5.3] Trying hardcoded data...")
        hard_xi = get_playing_xi_from_hardcoded(match_id, team1, team2)
        for team, players in hard_xi.items():
            if team not in team_xi or not team_xi[team]:
                team_xi[team] = players
        if team_xi.get(team1) and team_xi.get(team2) and not result["xi_source"]:
            result["xi_source"] = "hardcoded"

    # Assign XI to result
    result["team1_xi"] = team_xi.get(team1, [])
    result["team2_xi"] = team_xi.get(team2, [])

    if not result["xi_source"]:
        print("\n  ⚠️ Playing XI not found - may not be announced yet (announced at toss)")

    # ═══════════════════════════════════════════════════════════
    # STEP 6: Calculate chasing team
    # ═══════════════════════════════════════════════════════════
    if result["toss_done"]:
        tw = result["toss_winner"]
        td = result["toss_decision"]
        result["chasing_team"] = (team2 if tw == team1 else team1) if td == "bat" else tw

    # ═══════════════════════════════════════════════════════════
    # Print Summary
    # ═══════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("  ✅ SCRAPING COMPLETE")
    print(f"{'='*70}")
    print(f"  Match ID:    {result['match_id']}")
    print(f"  Is IPL:      {result['is_ipl']}")
    print(f"  Teams:       {result['team1']} vs {result['team2']}")
    print(f"  Venue:       {result['venue']}")
    print(f"  Toss Done:   {result['toss_done']}")
    print(f"  Toss Winner: {result['toss_winner']}")
    print(f"  Toss Dec:    {result['toss_decision']}")
    print(f"  Chasing:     {result['chasing_team']}")
    print(f"  XI Source:   {result['xi_source']}")
    print(f"\n  {result['team1']} XI ({len(result['team1_xi'])} players):")
    for i, p in enumerate(result['team1_xi'], 1):
        print(f"    {i:2}. {p}")
    print(f"\n  {result['team2']} XI ({len(result['team2_xi'])} players):")
    for i, p in enumerate(result['team2_xi'], 1):
        print(f"    {i:2}. {p}")
    print(f"{'='*70}\n")

    return result


# ══════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*70)
    print("  IPL MATCH SCRAPER")
    print("  Only works for IPL matches - rejects international matches")
    print("="*70)
    
    print("\n✅ Valid IPL Teams:")
    for abbr, name in sorted(IPL_TEAM_ABBR.items()):
        if abbr in ["CSK", "MI", "RCB", "KKR", "DC", "GT", "LSG", "RR", "PBKS", "SRH"]:
            print(f"   {abbr:4} = {name}")
    
    print("\n❌ NOT IPL (will be rejected):")
    print("   IND, PAK, AUS, ENG, SA, NZ, WI, SL, BAN, AFG, etc.")
    
    print("\n" + "-"*70)
    
    # Try to find live IPL match
    match_id = get_live_ipl_match_id()
    
    if not match_id:
        print("\nNo live IPL match found. Enter match ID manually:")
        try:
            user_input = input("Match ID (or press Enter to exit): ").strip()
            if user_input:
                match_id = int(user_input)
            else:
                print("Exiting...")
                exit(0)
        except ValueError:
            print("Invalid input. Exiting...")
            exit(1)
    
    # Scrape the match
    result = scrape_match(match_id)
    
    # Output
    if result.get("error"):
        print(f"\n❌ FAILED: {result['error']}")
        print("\nPlease enter a valid IPL match ID.")
        print("IPL matches have teams like CSK, MI, RCB, KKR, DC, GT, LSG, RR, PBKS, SRH")
    else:
        print("\n✅ SUCCESS!")
        print("\nJSON Output:")
        print("-"*70)
        print(json.dumps(result, indent=2, default=str))
