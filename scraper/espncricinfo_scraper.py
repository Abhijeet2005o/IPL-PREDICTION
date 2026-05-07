import re
from datetime import datetime
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

# ─── Config ───
IPL_SERIES_ID = "1510719"
LIVE_SCORES_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# ─── Team Aliases (FIXED for Bengaluru) ───
TEAM_ALIASES = {
    "RCB": "Royal Challengers Bangalore",
    "RCBENGALURU": "Royal Challengers Bangalore",
    "ROYAL CHALLENGERS BENGALURU": "Royal Challengers Bangalore",
    "KXIP": "Punjab Kings",
    "DD": "Delhi Capitals",
    "LSG": "Lucknow Super Giants",
    "GT": "Gujarat Titans",
    "SRH": "Sunrisers Hyderabad",
    "DC": "Delhi Capitals",
    "PBKS": "Punjab Kings",
    "MI": "Mumbai Indians",
    "CSK": "Chennai Super Kings",
    "KKR": "Kolkata Knight Riders",
    "RR": "Rajasthan Royals",
    "RPS": "Rising Pune Supergiant",
    "PWI": "Pune Warriors",
}

def _clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()

def _request_soup(url, timeout=20):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def _request_json(url, timeout=20):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ─── CRITICAL FIX: Normalize Team Names ───
def _normalize_team_name(name):
    """Fixes 'Bengaluru' to 'Bangalore' and handles aliases"""
    name = _clean_text(name)
    if not name: return name
    upper = name.upper()
    if upper in TEAM_ALIASES: return TEAM_ALIASES[upper]
    if "BENGALURU" in upper: return "Royal Challengers Bangalore"
    return name

# ─── ESPN Cricinfo Logic ───
def _get_espn_live_match(match_id=None):
    try:
        from cricdata import CricinfoClient
        client = CricinfoClient()
        live = client.live_matches()
        for match in live:
            series = match.get("series", {})
            if str(series.get("objectId", "")).strip() == str(IPL_SERIES_ID).strip() or "indian premier league" in str(series.get("longName", "")).lower():
                if match_id is None: return match
                if str(match.get("objectId", "")).strip() == str(match_id): return match
    except: pass
    return None

def _extract_xi_from_scorecard(scorecard):
    team_xi = {}
    try:
        players_data = scorecard.get("content", {}).get("matchPlayers", {}).get("teamPlayers", [])
        for entry in players_data:
            t_name = _clean_text(entry.get("team", {}).get("longName", ""))
            players = entry.get("players", []) or []
            names = [_clean_text(p.get("player", {}).get("longName", "")) for p in players]
            if t_name and names: team_xi[t_name] = names[:11]
    except: pass
    return team_xi

# ─── Cricbuzz Logic ───
_CB_INFO_URL = "https://www.cricbuzz.com/api/cricket-match/{match_id}/info"

def _cricbuzz_match_info(match_id):
    try:
        data = _request_json(_CB_INFO_URL.format(match_id=match_id))
        info = data.get("matchInfo", {})
        t1 = _clean_text(info.get("team1", {}).get("name", ""))
        t2 = _clean_text(info.get("team2", {}).get("name", ""))
        venue = _clean_text(info.get("venueInfo", {}).get("ground", "") + ", " + info.get("venueInfo", {}).get("city", ""))
        
        toss = info.get("tossResults") or info.get("toss") or {}
        toss_done = bool(toss.get("tossWinnerId"))
        
        toss_winner = ""
        toss_winner_id = str(toss.get("tossWinnerId", ""))
        t1_id = str(info.get("team1", {}).get("id", ""))
        t2_id = str(info.get("team2", {}).get("id", ""))
        
        if toss_winner_id == t1_id: toss_winner = t1
        elif toss_winner_id == t2_id: toss_winner = t2
        
        toss_decision = toss.get("decision", "").lower()
        if "bat" in toss_decision: toss_decision = "bat"
        elif "field" in toss_decision: toss_decision = "field"
        
        return {"team1": t1, "team2": t2, "venue": venue, "toss_done": toss_done, "toss_winner": toss_winner, "toss_decision": toss_decision}
    except: return None

# ─── Public API: Scrape Match ───
def scrape_match(match_id):
    """Scrape match details with fallback logic"""
    errors = []

    # 1. Try ESPN
    try:
        espn_match = _get_espn_live_match(match_id)
        if espn_match:
            series = espn_match.get("series", {})
            teams = espn_match.get("teams", [])
            t_names = [t.get("team", {}).get("longName", "") for t in teams]
            t1 = _clean_text(t_names[0]) if t_names else "Team 1"
            t2 = _clean_text(t_names[1]) if len(t_names)>1 else "Team 2"
            
            # Toss Fallback: If ESPN has decision but NO winner, try Cricbuzz
            toss_raw = espn_match.get("match", {}).get("toss", {}) # Adjust path based on actual cricdata structure
            tw_raw = _clean_text(toss_raw.get("winner_team", ""))
            td_raw = _clean_text(toss_raw.get("decision", "")).lower()
            
            # Note: Cricdata structure varies, if toss info is missing, we fallback
            if not tw_raw and td_raw:
                 print(f"[DEBUG] ESPN has decision but no winner. Falling back to Cricbuzz for toss...")
                 cb = _cricbuzz_match_info(match_id)
                 if cb and cb['toss_done']:
                     tw_raw = cb['toss_winner']
                     td_raw = cb['toss_decision']

            toss_done = bool(tw_raw and td_raw)
            chasing = (t2 if tw_raw == t1 else t1) if td_raw == 'bat' else tw_raw if toss_done else None
            
            return {
                "match_id": int(match_id), "team1": t1, "team2": t2, 
                "venue": _clean_text(espn_match.get("ground", {}).get("longName", "")) or "Unknown",
                "toss_done": toss_done, "toss_winner": tw_raw, "toss_decision": td_raw,
                "chasing_team": chasing, "team1_xi": [], "team2_xi": [], "source": "espn"
            }
    except Exception as e:
        errors.append(f"ESPN: {e}")

    # 2. Try Cricbuzz
    try:
        cb = _cricbuzz_match_info(match_id)
        if cb and cb['team1'] != 'Unknown':
            return {
                "match_id": int(match_id), "team1": cb['team1'], "team2": cb['team2'], 
                "venue": cb['venue'], "toss_done": cb['toss_done'], "toss_winner": cb['toss_winner'], 
                "toss_decision": cb['toss_decision'], "chasing_team": None, 
                "team1_xi": [], "team2_xi": [], "source": "cricbuzz"
            }
    except Exception as e:
        errors.append(f"Cricbuzz: {e}")

    return {"error": " | ".join(errors)}

# ─── Feature Vector Builder ───
def build_feature_vector(match_info, player_lookup, matches_df, team_encoder, venue_encoder, venue_score_history, team_pp_eco_lookup, team_opener_lookup, get_recent_avg_func, get_season_avg_func, get_season_year_func, get_venue_avg_func, get_recent_high_rate_func, feature_cols):
    # Normalize Team Names
    team1 = _normalize_team_name(match_info.get("team1", ""))
    team2 = _normalize_team_name(match_info.get("team2", ""))
    
    # Encode
    t1_id = int(team_encoder.transform([team1])[0]) if team1 in team_encoder.classes_ else 0
    t2_id = int(team_encoder.transform([team2])[0]) if team2 in team_encoder.classes_ else 0
    
    # Dummy Date
    now = pd.Timestamp(datetime.now().date())
    
    # Initialize Row with 0s
    feat = {c: 0.0 for c in feature_cols}
    
    # Fill basic features
    feat.update({
        "team1": t1_id, "team2": t2_id,
        "venue": int(venue_encoder.transform([match_info.get("venue", "")])[0]) if match_info.get("venue") in venue_encoder.classes_ else 0,
        "toss_winner_is_team1": 1 if match_info.get("toss_winner") == team1 else 0,
        "toss_decision_bat": 1 if match_info.get("toss_decision") == 'bat' else 0,
        "season_year": get_season_year_func(now)
    })
    
    # Fill dynamic stats using helper functions passed from app.py
    try:
        feat['t1_recent_avg_score'] = get_recent_avg_func(team1, now)
        feat['t2_recent_avg_score'] = get_recent_avg_func(team2, now)
        feat['t1_high_score_rate'] = get_recent_high_rate_func(team1, now)
        feat['t2_high_score_rate'] = get_recent_high_rate_func(team2, now)
        feat['venue_avg_first_innings'] = get_venue_avg_func(match_info.get("venue", ""), now)
        feat['venue_recent_avg'] = get_venue_avg_func(match_info.get("venue", ""), now)
    except: pass

    return pd.DataFrame([feat], columns=feature_cols)
