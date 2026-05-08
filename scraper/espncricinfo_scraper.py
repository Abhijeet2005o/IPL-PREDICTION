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
SCORECARD_URL_TEMPLATE = "https://www.cricbuzz.com/cricket-scorecard/{match_id}"
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

# Map abbreviations to full names
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
    """Extract Playing XI from ESPN scorecard."""
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
                print(f"[ESPN-XI] Found {len(names)} players for {team_name}")
    except Exception as e:
        print(f"[ESPN] _extract_xi_from_scorecard error: {e}")
    return team_xi

def _extract_toss_from_espn_info(info, team1, team2):
    """Extract toss from ESPN match_info."""
    if not isinstance(info, dict):
        return "", None
    
    # Deep search for toss
    def deep_find_toss(obj, depth=0, max_depth=8):
        if depth > max_depth:
            return None
        if isinstance(obj, dict):
            # Direct toss key
            for candidate in ["toss", "tossResults", "tossResult", "tossInfo"]:
                if candidate in obj:
                    val = obj[candidate]
                    if isinstance(val, dict) and val:
                        return val
            # Check if THIS dict looks like a toss object
            has_winner = any(k.lower() in {
                "tosswinner", "tosswinnerid", "winnerid", "winner"
            } for k in obj)
            has_decision = any(k.lower() in {
                "decision", "tossdecision", "elected", "choice"
            } for k in obj)
            if has_winner and has_decision:
                return obj
            # Recurse
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
# CRICBUZZ HTML PARSING
# ─────────────────────────────────────────────────────────
def _extract_from_cricbuzz_html(match_id):
    """Extract match info from Cricbuzz HTML."""
    result = {
        "team1": "Unknown",
        "team2": "Unknown",
        "venue": "Unknown Venue",
        "toss_winner": "",
        "toss_decision": None,
        "team1_xi": [],
        "team2_xi": [],
    }
    
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[CB-HTML] Fetching: {url}")
        soup = _request_soup(url)
        
        # ── Extract teams ──
        # Method 1: Link text with "DC vs KKR"
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            text = _clean_text(link.get_text())
            vs_match = re.search(r"([A-Za-z]+)\s+vs\.?\s+([A-Za-z]+)", text, re.I)
            if vs_match:
                t1_abbr = vs_match.group(1).upper()
                t2_abbr = vs_match.group(2).upper()
                result["team1"] = _correct_team_name(TEAM_ABBREVIATIONS.get(t1_abbr, t1_abbr))
                result["team2"] = _correct_team_name(TEAM_ABBREVIATIONS.get(t2_abbr, t2_abbr))
                print(f"[CB-HTML] Teams: {result['team1']} vs {result['team2']}")
                break
        
        # Method 2: Link title
        if result["team1"] == "Unknown":
            for link in soup.select("a[href*='/live-cricket-scores/']"):
                title = link.get("title", "")
                if "vs" in title.lower():
                    vs_match = re.search(r"([A-Za-z\s]+?)\s+vs\.?\s+([A-Za-z\s]+?)(?:,|\s*\d)", title, re.I)
                    if vs_match:
                        result["team1"] = _correct_team_name(_clean_text(vs_match.group(1)))
                        result["team2"] = _correct_team_name(_clean_text(vs_match.group(2)))
                        print(f"[CB-HTML] Teams from title: {result['team1']} vs {result['team2']}")
                        break
        
        # ── Extract toss ──
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            text = _clean_text(link.get_text())
            # Look for "KKR opt to bowl" or "DC opt to bat"
            opt_match = re.search(r"([A-Za-z]+)\s+opt\s+to\s+(bat|bowl|field)", text, re.I)
            if opt_match:
                team_abbr = opt_match.group(1).upper()
                decision = opt_match.group(2).lower()
                team_full = TEAM_ABBREVIATIONS.get(team_abbr, team_abbr)
                
                # Match to team1 or team2
                if result["team1"].lower() in team_full.lower() or team_full.lower() in result["team1"].lower():
                    result["toss_winner"] = result["team1"]
                elif result["team2"].lower() in team_full.lower() or team_full.lower() in result["team2"].lower():
                    result["toss_winner"] = result["team2"]
                
                if result["toss_winner"]:
                    result["toss_decision"] = "bat" if decision == "bat" else "field"
                    print(f"[CB-HTML] Toss: {result['toss_winner']} opt to {result['toss_decision']}")
                    break
        
        # ── Extract venue ──
        for link in soup.select("a[href*='/venues/']"):
            text = _clean_text(link.get_text())
            if text and len(text) > 5:
                result["venue"] = text
                print(f"[CB-HTML] Venue: {result['venue']}")
                break
        
        # ── Extract Playing XI from scorecard page ──
        try:
            scorecard_url = SCORECARD_URL_TEMPLATE.format(match_id=match_id)
            print(f"[CB-HTML] Fetching scorecard: {scorecard_url}")
            scorecard_soup = _request_soup(scorecard_url)
            
            # Look for player links in scorecard
            player_links = scorecard_soup.select("a[href*='/profiles/']")
            if player_links:
                # Group by team based on section headers or patterns
                all_players = []
                for link in player_links:
                    name = _clean_text(link.get_text())
                    if name and len(name) > 2 and name not in all_players:
                        all_players.append(name)
                
                # If we have 22+ unique players, split them
                if len(all_players) >= 22:
                    result["team1_xi"] = all_players[:11]
                    result["team2_xi"] = all_players[11:22]
                    print(f"[CB-HTML] XI: Team1={len(result['team1_xi'])}, Team2={len(result['team2_xi'])}")
                elif len(all_players) >= 11:
                    result["team1_xi"] = all_players[:11]
                    print(f"[CB-HTML] XI: Team1={len(result['team1_xi'])}")
        except Exception as e:
            print(f"[CB-HTML] Scorecard error: {e}")
        
    except Exception as e:
        print(f"[CB-HTML] Error: {e}")
    
    return result

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
    Combines ESPN (for XI) and Cricbuzz HTML (for toss).
    """
    errors = []
    print(f"\n{'='*60}")
    print(f"[SCRAPE] Starting scrape for match ID: {match_id}")
    print(f"{'='*60}\n")

    # Initialize result
    result = {
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
        "source": "combined",
        "scraped_at": datetime.utcnow().isoformat() + "Z",
    }

    # ── 1. Try ESPN for Playing XI ────────────────────────
    espn_success = False
    if CRICDATA_AVAILABLE and CRICINFO_CLIENT:
        try:
            print("[SCRAPE] Attempting ESPN source for Playing XI...")
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

                if team1 and team2:
                    result["team1"] = team1
                    result["team2"] = team2
                    
                    # Get venue
                    if isinstance(info, dict):
                        venue = _clean_text(info.get("venue", {}).get("longName", ""))
                        if venue:
                            result["venue"] = venue
                    
                    if result["venue"] == "Unknown Venue":
                        result["venue"] = _clean_text(
                            espn_match.get("ground", {}).get("longName", "")
                        ) or "Unknown Venue"

                    # Get Playing XI
                    xi_map = _extract_xi_from_scorecard(scorecard)
                    result["team1_xi"] = xi_map.get(team1, [])
                    result["team2_xi"] = xi_map.get(team2, [])
                    
                    # Try ESPN toss
                    tw, td = _extract_toss_from_espn_info(info, team1, team2)
                    if tw and td:
                        result["toss_winner"] = tw
                        result["toss_decision"] = td
                        result["toss_done"] = True
                    
                    espn_success = True
                    result["source"] = "espn"
                    print(f"[SCRAPE-ESPN] ✓ Teams: {team1} vs {team2}")
                    print(f"[SCRAPE-ESPN] ✓ XI: {len(result['team1_xi'])} vs {len(result['team2_xi'])} players")
        except Exception as e:
            errors.append(f"ESPN: {e}")
            print(f"[SCRAPE-ESPN] ✗ Failed: {e}")

    # ── 2. Try Cricbuzz HTML for toss (if needed) ────────
    if not result["toss_done"] or result["team1"] == "Unknown":
        try:
            print("\n[SCRAPE] Trying Cricbuzz HTML for toss/teams...")
            cb_result = _extract_from_cricbuzz_html(match_id)
            
            # Update teams if not found
            if result["team1"] == "Unknown" and cb_result["team1"] != "Unknown":
                result["team1"] = cb_result["team1"]
                result["team2"] = cb_result["team2"]
                print(f"[SCRAPE-CB] ✓ Teams from Cricbuzz: {result['team1']} vs {result['team2']}")
            
            # Update venue if not found
            if result["venue"] == "Unknown Venue" and cb_result["venue"] != "Unknown Venue":
                result["venue"] = cb_result["venue"]
                print(f"[SCRAPE-CB] ✓ Venue from Cricbuzz: {result['venue']}")
            
            # Update toss if not found
            if not result["toss_done"] and cb_result["toss_winner"] and cb_result["toss_decision"]:
                result["toss_winner"] = cb_result["toss_winner"]
                result["toss_decision"] = cb_result["toss_decision"]
                result["toss_done"] = True
                print(f"[SCRAPE-CB] ✓ Toss from Cricbuzz: {result['toss_winner']} opt to {result['toss_decision']}")
            
            # Update XI if not found
            if not result["team1_xi"] and cb_result["team1_xi"]:
                result["team1_xi"] = cb_result["team1_xi"]
                result["team2_xi"] = cb_result["team2_xi"]
                print(f"[SCRAPE-CB] ✓ XI from Cricbuzz: {len(result['team1_xi'])} vs {len(result['team2_xi'])} players")
            
            if not espn_success:
                result["source"] = "cricbuzz_html"
        except Exception as e:
            errors.append(f"CricbuzzHTML: {e}")
            print(f"[SCRAPE-CB] ✗ Failed: {e}")

    # ── 3. Calculate chasing team ────────────────────────
    if result["toss_done"]:
        team1 = result["team1"]
        team2 = result["team2"]
        toss_winner = result["toss_winner"]
        toss_decision = result["toss_decision"]
        
        result["chasing_team"] = (
            (team2 if toss_winner == team1 else team1)
            if toss_decision == "bat" else toss_winner
        )

    print(f"\n[SCRAPE] Final result:")
    print(f"  Teams: {result['team1']} vs {result['team2']}")
    print(f"  Venue: {result['venue']}")
    print(f"  Toss: {result['toss_winner']} / {result['toss_decision']}")
    print(f"  XI: {len(result['team1_xi'])} vs {len(result['team2_xi'])} players")
    print(f"  Source: {result['source']}")

    return result

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
