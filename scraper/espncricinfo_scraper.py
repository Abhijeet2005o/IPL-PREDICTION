# scraper/espncricinfo_scraper.py
# ESPNcricinfo JSON API scraper — fully defensive with debug output
# If anything fails, run debug_api() first to see the raw response

import requests
import pandas as pd
import numpy as np
from datetime import date, datetime
import json
import time

# ════════════════════════════════════════════════════════
# ⚠️  STEP 1: UPDATE THIS BEFORE RUNNING
# ════════════════════════════════════════════════════════
# How to find series ID:
#   1. Go to espncricinfo.com
#   2. Click on IPL 2026 series
#   3. Look at URL → /series/ipl-2026-XXXXXXX/
#   4. That number XXXXXXX is your series ID
IPL_SERIES_ID = "1551234"
# ════════════════════════════════════════════════════════

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espncricinfo.com/",
}


# ════════════════════════════════════════════════════════
# DEBUG FUNCTION — Run this first if anything fails
# ════════════════════════════════════════════════════════

def debug_api():
    """
    Run this function FIRST if you get any errors.
    It shows you exactly what the API returns so you can fix the scraper.

    Usage in notebook:
        import sys
        sys.path.append('../scraper')
        from espncricinfo_scraper import debug_api
        debug_api()
    """
    print("=" * 60)
    print("DEBUG: Checking ESPNcricinfo API")
    print("=" * 60)

    # Step 1: Test network
    print("\n[1] Testing network connection...")
    try:
        r = requests.get("https://www.espncricinfo.com", headers=HEADERS, timeout=10)
        print(f"    ✅ Connected. Status: {r.status_code}")
    except Exception as e:
        print(f"    ❌ No network: {e}")
        print("    → Check your internet connection")
        return

    # Step 2: Test schedule API
    print(f"\n[2] Testing schedule API with series ID: {IPL_SERIES_ID}")
    url = (
        f"https://hs-consumer-api.espncricinfo.com/v1/pages/series/schedule"
        f"?lang=en&seriesId={IPL_SERIES_ID}"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        print(f"    Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            print(f"    ✅ API works. Top-level keys: {list(data.keys())}")

            # Show today's date
            today_str = date.today().strftime("%Y-%m-%d")
            print(f"\n    Today's date: {today_str}")

            # Try to navigate the structure
            content = data.get("content", data)  # some APIs wrap in 'content'
            schedule_map = content.get("matchScheduleMap", [])
            print(f"    matchScheduleMap length: {len(schedule_map)}")

            # Show first few dates in schedule
            print("\n    Dates in schedule:")
            count = 0
            for block in schedule_map:
                wrappers = block.get("scheduleAdWrapper", [block])
                for w in wrappers:
                    d = w.get("date", "no-date")
                    print(f"      {d}")
                    count += 1
                    if count >= 10:
                        break
                if count >= 10:
                    print("      ... (more dates)")
                    break

        elif r.status_code == 404:
            print(f"    ❌ 404 — Wrong series ID: {IPL_SERIES_ID}")
            print("    → Update IPL_SERIES_ID in espncricinfo_scraper.py")
        else:
            print(f"    ❌ Unexpected status: {r.status_code}")
            print(f"    Response: {r.text[:300]}")

    except json.JSONDecodeError:
        print("    ❌ Response is not JSON")
        print(f"    Raw: {r.text[:300]}")
    except Exception as e:
        print(f"    ❌ Error: {e}")

    print("\n" + "=" * 60)
    print("Copy this output and share it to diagnose errors")
    print("=" * 60)


# ════════════════════════════════════════════════════════
# HELPER: Safe JSON request
# ════════════════════════════════════════════════════════

def _get_json(url, retries=2):
    """
    Makes GET request and returns JSON. Returns None on any failure.
    Retries up to `retries` times on failure.
    """
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)

            if response.status_code == 200:
                return response.json()

            if response.status_code == 404:
                print(f"404 Not Found: {url}")
                print("If this is the schedule URL, update IPL_SERIES_ID")
                return None

            print(f"HTTP {response.status_code} for URL: {url}")
            return None

        except requests.exceptions.ConnectionError:
            print("❌ No internet connection. Check your network.")
            return None
        except requests.exceptions.Timeout:
            if attempt < retries:
                print(f"Timeout. Retrying ({attempt + 1}/{retries})...")
                time.sleep(2)
                continue
            print("❌ Request timed out after retries.")
            return None
        except json.JSONDecodeError:
            print("❌ Response is not valid JSON.")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {type(e).__name__}: {e}")
            return None

    return None


# ════════════════════════════════════════════════════════
# FUNCTION 1: Find today's match ID
# ════════════════════════════════════════════════════════

def get_todays_match_id():
    """
    Finds today's IPL match ID from ESPNcricinfo schedule API.

    Returns:
        int  → matchId if match found today
        None → no match today, or API/network error

    If this returns None unexpectedly, run debug_api() to diagnose.
    """
    url = (
        f"https://hs-consumer-api.espncricinfo.com/v1/pages/series/schedule"
        f"?lang=en&seriesId={IPL_SERIES_ID}"
    )

    print(f"Fetching schedule for series ID: {IPL_SERIES_ID}")
    data = _get_json(url)

    if data is None:
        print("❌ Schedule API failed. Run debug_api() to diagnose.")
        return None

    today_str = date.today().strftime("%Y-%m-%d")
    print(f"Looking for match on: {today_str}")

    try:
        # Handle both response formats:
        # Format A: data["content"]["matchScheduleMap"]
        # Format B: data["matchScheduleMap"]
        content = data.get("content", data)
        schedule_map = content.get("matchScheduleMap", [])

        if not schedule_map:
            print("❌ matchScheduleMap is empty.")
            print("   The series ID may be wrong, or schedule not loaded yet.")
            return None

        for block in schedule_map:
            # Each block can have a "scheduleAdWrapper" list
            # Some blocks have a direct date field
            wrappers = block.get("scheduleAdWrapper", [])

            # Also check if the block itself is a wrapper (some API versions)
            if not wrappers and "date" in block:
                wrappers = [block]

            for wrapper in wrappers:
                wrapper_date = wrapper.get("date", "")

                if wrapper_date != today_str:
                    continue

                # Found today's date!
                match_list = wrapper.get("matchScheduleList", [])

                for schedule in match_list:
                    matches = schedule.get("matches", [])

                    for m in matches:
                        info = m.get("matchInfo", {})
                        match_id = info.get("matchId")

                        if match_id:
                            t1 = info.get("team1", {}).get("teamName", "Team 1")
                            t2 = info.get("team2", {}).get("teamName", "Team 2")
                            print(f"✅ Found match: {t1} vs {t2} (ID: {match_id})")
                            return int(match_id)

        print(f"No IPL match scheduled for today ({today_str})")
        return None

    except (KeyError, TypeError, AttributeError) as e:
        print(f"❌ Error parsing schedule: {e}")
        print("   Run debug_api() to see the raw API structure.")
        return None


# ════════════════════════════════════════════════════════
# FUNCTION 2: Scrape match details
# ════════════════════════════════════════════════════════

def scrape_match(match_id):
    """
    Fetches full match details from ESPNcricinfo match API.

    Args:
        match_id (int): from get_todays_match_id()

    Returns:
        dict with keys:
            team1 (str)         — always
            team2 (str)         — always
            venue (str)         — always
            toss_done (bool)    — True after toss
            toss_winner (str)   — if toss_done
            toss_decision (str) — 'bat' or 'field', if toss_done
            chasing_team (str)  — if toss_done
            team1_xi (list)     — if toss_done and XI announced
            team2_xi (list)     — if toss_done and XI announced
    """
    url = (
        f"https://hs-consumer-api.espncricinfo.com/v1/pages/match/info"
        f"?lang=en&seriesId={IPL_SERIES_ID}&matchId={match_id}"
    )

    print(f"Fetching match info for matchId: {match_id}")
    data = _get_json(url)

    if data is None:
        print("❌ Match API failed.")
        return {}

    result = {"toss_done": False}

    try:
        # ── Team names ──
        # Try both response formats
        match_data = data.get("match", data)
        info = match_data.get("matchInfo", match_data)

        team1_data = info.get("team1", {})
        team2_data = info.get("team2", {})

        result["team1"]    = team1_data.get("teamName", "")
        result["team2"]    = team2_data.get("teamName", "")
        result["team1_id"] = str(team1_data.get("teamId", ""))
        result["team2_id"] = str(team2_data.get("teamId", ""))

        if not result["team1"] or not result["team2"]:
            print("❌ Could not extract team names.")
            print(f"   info keys: {list(info.keys())}")
            return result

        print(f"   Teams: {result['team1']} vs {result['team2']}")

        # ── Venue ──
        venue_data = info.get("venue", {})
        ground = venue_data.get("ground", "")
        city   = venue_data.get("city", "")
        if ground and city:
            result["venue"] = f"{ground}, {city}"
        elif ground:
            result["venue"] = ground
        else:
            result["venue"] = ""

        print(f"   Venue: {result['venue']}")

        # ── Toss ──
        toss = info.get("tossResults", {})

        if toss and toss.get("winnerTeamId"):
            result["toss_done"]  = True
            winner_id            = str(toss.get("winnerTeamId", ""))
            decision_raw         = toss.get("decision", "").lower()

            if winner_id == result["team1_id"]:
                result["toss_winner"] = result["team1"]
                result["toss_loser"]  = result["team2"]
            else:
                result["toss_winner"] = result["team2"]
                result["toss_loser"]  = result["team1"]

            if "bat" in decision_raw:
                result["toss_decision"] = "bat"
                result["chasing_team"]  = result["toss_loser"]
            else:
                result["toss_decision"] = "field"
                result["chasing_team"]  = result["toss_winner"]

            print(f"   Toss: {result['toss_winner']} won → elected to {result['toss_decision']}")
            print(f"   Chasing: {result['chasing_team']}")
        else:
            print("   Toss not done yet (pre-toss mode)")

        # ── Playing XI ──
        if result["toss_done"]:
            result["team1_xi"] = []
            result["team2_xi"] = []

            # Try "matchPlayers" key first
            match_players = data.get("matchPlayers", {})
            team_players  = match_players.get("teamPlayers", [])

            for team_entry in team_players:
                team_name = team_entry.get("team", {}).get("teamName", "")
                players   = team_entry.get("players", [])

                xi = []
                for p in players:
                    player_obj = p.get("player", {})
                    is_playing = p.get("isPlaying", False)
                    # Some API versions use "isInPlayingXI" or "playingXi"
                    if not is_playing:
                        is_playing = p.get("isInPlayingXI", False)

                    if is_playing:
                        name = (
                            player_obj.get("fullName", "")
                            or player_obj.get("name", "")
                        )
                        if name:
                            xi.append(name)

                if team_name == result["team1"]:
                    result["team1_xi"] = xi[:11]
                elif team_name == result["team2"]:
                    result["team2_xi"] = xi[:11]

            # Fallback: try "playingXi" directly
            if not result["team1_xi"] and not result["team2_xi"]:
                for team_entry in team_players:
                    team_name  = team_entry.get("team", {}).get("teamName", "")
                    playing_xi = team_entry.get("playingXi", [])
                    xi = [
                        p.get("fullName", "") or p.get("name", "")
                        for p in playing_xi
                        if p.get("fullName") or p.get("name")
                    ]
                    if team_name == result["team1"]:
                        result["team1_xi"] = xi[:11]
                    elif team_name == result["team2"]:
                        result["team2_xi"] = xi[:11]

            t1_count = len(result["team1_xi"])
            t2_count = len(result["team2_xi"])
            print(f"   Playing XI: {result['team1']}({t1_count}) | {result['team2']}({t2_count})")

            if t1_count == 0 or t2_count == 0:
                print("   ℹ️  XI not yet announced (toss done but no XI yet)")

    except Exception as e:
        print(f"❌ Error parsing match data: {type(e).__name__}: {e}")

    return result


# ════════════════════════════════════════════════════════
# FUNCTION 3: Build feature vector
# ════════════════════════════════════════════════════════

def build_feature_vector(match_info,
                          player_lookup,
                          matches_df,
                          team_encoder,
                          venue_encoder,
                          venue_score_history,
                          team_pp_eco_lookup,
                          team_oppener_lookup,
                          get_team_recent_avg_score,
                          get_season_avg_score,
                          get_season_year,
                          get_venue_recent_avg_score,
                          get_team_recent_high_score_rate,
                          feature_cols):
    """
    Converts match info dict into model-ready feature vector.

    Returns:
        list → [[f1, f2, ...]] ready for model.predict()
    """

    team1 = match_info.get("team1", "")
    team2 = match_info.get("team2", "")
    venue = match_info.get("venue", "")
    today = pd.Timestamp(datetime.today().date())

    # ── Encode teams and venue ──
    known_teams  = set(team_encoder.classes_)
    known_venues = set(venue_encoder.classes_)

    def safe_encode_team(name):
        if name in known_teams:
            return int(team_encoder.transform([name])[0])
        # Try partial match (handles name variations like RCB vs Royal Challengers)
        for kt in known_teams:
            if name.lower() in kt.lower() or kt.lower() in name.lower():
                return int(team_encoder.transform([kt])[0])
        print(f"⚠️  Unknown team: '{name}' — using 0")
        return 0

    def safe_encode_venue(v):
        if v in known_venues:
            return int(venue_encoder.transform([v])[0])
        # Try partial match
        for kv in known_venues:
            if kv.lower() in v.lower() or v.split(",")[0].strip().lower() in kv.lower():
                return int(venue_encoder.transform([kv])[0])
        print(f"⚠️  Unknown venue: '{v}' — using 0")
        return 0

    t1_enc = safe_encode_team(team1)
    t2_enc = safe_encode_team(team2)
    v_enc  = safe_encode_venue(venue)

    # ── Venue features ──
    venue_base   = venue.split(",")[0].strip()  # "Wankhede Stadium"
    past_venue   = venue_score_history[
        (venue_score_history["venue"].str.contains(venue_base, case=False, na=False)) &
        (venue_score_history["match_date"] < today)
    ]
    avg_inn = float(past_venue["first_innings_score"].mean()) if not past_venue.empty else 167.0
    rec_avg = get_venue_recent_avg_score(venue, today)

    # ── Home ground ──
    HOME = {
        "Wankhede"          : "Mumbai Indians",
        "Chidambaram"       : "Chennai Super Kings",
        "Eden Gardens"      : "Kolkata Knight Riders",
        "Chinnaswamy"       : "Royal Challengers Bengaluru",
        "Arun Jaitley"      : "Delhi Capitals",
        "Sawai Mansingh"    : "Rajasthan Royals",
        "Rajiv Gandhi"      : "Sunrisers Hyderabad",
        "Punjab"            : "Punjab Kings",
        "Narendra Modi"     : "Gujarat Titans",
        "Ekana"             : "Lucknow Super Giants",
    }
    home_team = ""
    for keyword, team in HOME.items():
        if keyword.lower() in venue.lower():
            home_team = team
            break
    is_home = 1 if home_team == team1 else 0

    # ── Toss ──
    toss_t1  = 1 if match_info.get("toss_winner") == team1 else 0
    toss_bat = 1 if match_info.get("toss_decision") == "bat" else 0

    # ── H2H ──
    h2h_df = matches_df[
        ((matches_df["team1"] == team1) & (matches_df["team2"] == team2)) |
        ((matches_df["team1"] == team2) & (matches_df["team2"] == team1))
    ]
    h1 = len(h2h_df[h2h_df["winner"] == team1])
    h2 = len(h2h_df[h2h_df["winner"] == team2])

    # ── Chase stats ──
    def cpct(t):
        c = matches_df[matches_df["team2"] == t]
        return len(c[c["winner"] == t]) / len(c) if len(c) > 0 else 0.5

    def hcpct(t):
        c = matches_df[matches_df["team2"] == t]
        return len(c[c["winner"] == t]) / len(c) if len(c) > 0 else 0.4

    def wr(t, n=10):
        m = matches_df[(matches_df["team1"] == t) | (matches_df["team2"] == t)].tail(n)
        return len(m[m["winner"] == t]) / len(m) if len(m) > 0 else 0.5

    def l5(t):
        m = matches_df[(matches_df["team1"] == t) | (matches_df["team2"] == t)].tail(5)
        return int(len(m[m["winner"] == t]))

    c1, c2   = cpct(team1),  cpct(team2)
    hc1, hc2 = hcpct(team1), hcpct(team2)
    wr1, wr2 = wr(team1),    wr(team2)
    l1, l2   = l5(team1),    l5(team2)

    # ── Recent scores ──
    rs1    = get_team_recent_avg_score(team1, today, n=5)
    rs2    = get_team_recent_avg_score(team2, today, n=5)
    rs1_15 = get_team_recent_avg_score(team1, today, n=15)
    rs2_15 = get_team_recent_avg_score(team2, today, n=15)
    hsr1   = get_team_recent_high_score_rate(team1, today)
    hsr2   = get_team_recent_high_score_rate(team2, today)
    pp1    = float(team_pp_eco_lookup.get(team1, 8.5))
    pp2    = float(team_pp_eco_lookup.get(team2, 8.5))
    s_avg  = get_season_avg_score(today)
    s_yr   = get_season_year(today)

    # ── Player stats from XI ──
    def neutral():
        return {
            "avg_batting_avg": 25.0,   "avg_strike_rate": 120.0,
            "top3_batting_avg": 30.0,  "avg_economy": 8.5,
            "avg_bowling_avg": 30.0,   "recent_strike_rate": 120.0,
            "recent_economy": 8.5,
        }

    def xi_stats(xi):
        if not xi:
            return neutral()
        s = player_lookup[player_lookup["player"].isin(xi)]
        if s.empty:
            return neutral()
        def safe_mean(col):
            v = s[col].replace(0, np.nan)
            return float(v.mean()) if v.notna().any() else 0.0
        return {
            "avg_batting_avg"   : float(s["batting_avg"].mean()),
            "avg_strike_rate"   : float(s["strike_rate"].mean()),
            "top3_batting_avg"  : float(s.nlargest(3, "batting_avg")["batting_avg"].mean()),
            "avg_economy"       : safe_mean("economy"),
            "avg_bowling_avg"   : safe_mean("bowling_avg"),
            "recent_strike_rate": float(s["recent_strike_rate"].mean()),
            "recent_economy"    : safe_mean("recent_economy"),
        }

    toss_done = match_info.get("toss_done", False)
    t1_xi = match_info.get("team1_xi", []) if toss_done else []
    t2_xi = match_info.get("team2_xi", []) if toss_done else []
    t1s   = xi_stats(t1_xi)
    t2s   = xi_stats(t2_xi)

    EPS    = 1e-6
    t1_bvb = t1s["avg_strike_rate"] / max(t2s["avg_economy"], EPS)
    t2_bvb = t2s["avg_strike_rate"] / max(t1s["avg_economy"], EPS)

    t1_op = team_oppener_lookup.get(team1, {"opener_avg_batting_avg": 25.0, "opener_avg_strike_rate": 130.0})
    t2_op = team_oppener_lookup.get(team2, {"opener_avg_batting_avg": 25.0, "opener_avg_strike_rate": 130.0})

    # ── Build feature dict ──
    feat_dict = {
        "team1"                   : float(t1_enc),
        "team2"                   : float(t2_enc),
        "venue"                   : float(v_enc),
        "venue_avg_first_innings" : avg_inn,
        "venue_recent_avg"        : rec_avg,
        "is_home_team1"           : float(is_home),
        "toss_winner_is_team1"    : float(toss_t1),
        "toss_decision_bat"       : float(toss_bat),
        "h2h_team1_wins"          : float(h1),
        "h2h_team2_wins"          : float(h2),
        "chase_win_pct_team1"     : c1,
        "chase_win_pct_team2"     : c2,
        "high_score_chase_t1"     : hc1,
        "high_score_chase_t2"     : hc2,
        "winrate_team1"           : wr1,
        "winrate_team2"           : wr2,
        "last5_win_team1"         : float(l1),
        "last5_win_team2"         : float(l2),
        "t1_recent_avg_score"     : rs1,
        "t2_recent_avg_score"     : rs2,
        "t1_high_score_rate"      : hsr1,
        "t2_high_score_rate"      : hsr2,
        "t1_pp_bowling_economy"   : pp1,
        "t2_pp_bowling_economy"   : pp2,
        "season_avg_score"        : s_avg,
        "season_year"             : float(s_yr),
        "t1_avg_batting_avg"      : t1s["avg_batting_avg"],
        "t1_avg_strike_rate"      : t1s["avg_strike_rate"],
        "t1_top3_batting_avg"     : t1s["top3_batting_avg"],
        "t1_avg_economy"          : t1s["avg_economy"],
        "t1_avg_bowling_avg"      : t1s["avg_bowling_avg"],
        "t1_recent_strike_rate"   : t1s["recent_strike_rate"],
        "t1_recent_economy"       : t1s["recent_economy"],
        "t2_avg_batting_avg"      : t2s["avg_batting_avg"],
        "t2_avg_strike_rate"      : t2s["avg_strike_rate"],
        "t2_top3_batting_avg"     : t2s["top3_batting_avg"],
        "t2_avg_economy"          : t2s["avg_economy"],
        "t2_avg_bowling_avg"      : t2s["avg_bowling_avg"],
        "t2_recent_strike_rate"   : t2s["recent_strike_rate"],
        "t2_recent_economy"       : t2s["recent_economy"],
        "t1_opener_batting_avg"   : float(t1_op["opener_avg_batting_avg"]),
        "t1_opener_strike_rate"   : float(t1_op["opener_avg_strike_rate"]),
        "t2_opener_batting_avg"   : float(t2_op["opener_avg_batting_avg"]),
        "t2_opener_strike_rate"   : float(t2_op["opener_avg_strike_rate"]),
        "t1_bat_vs_bowl"          : t1_bvb,
        "t2_bat_vs_bowl"          : t2_bvb,
        "t1_rolling_season_avg"   : rs1_15,
        "t2_rolling_season_avg"   : rs2_15,
    }

    # ── Check for mismatched features ──
    missing_in_dict = [c for c in feature_cols if c not in feat_dict]
    if missing_in_dict:
        print(f"⚠️  Features in feature_cols but missing from feat_dict: {missing_in_dict}")
        print("   Add these keys to feat_dict in build_feature_vector()")

    extra_in_dict = [k for k in feat_dict if k not in feature_cols]
    if extra_in_dict:
        print(f"ℹ️  Keys in feat_dict but not in feature_cols (ignored): {extra_in_dict}")

    # Build vector in exact feature_cols order
    features = [[float(feat_dict.get(col, 0.0)) for col in feature_cols]]

    return features


# ════════════════════════════════════════════════════════
# FUNCTION 4: Manual input (backup when API fails)
# ════════════════════════════════════════════════════════

def manual_match_input(team1, team2, venue,
                        toss_winner, toss_decision,
                        team1_xi=None, team2_xi=None):
    """
    Create match_info manually when API is not working.

    Example:
        from espncricinfo_scraper import manual_match_input
        info = manual_match_input(
            team1         = "Mumbai Indians",
            team2         = "Chennai Super Kings",
            venue         = "Wankhede Stadium, Mumbai",
            toss_winner   = "Mumbai Indians",
            toss_decision = "bat",
        )
    """
    toss_loser   = team2 if toss_winner == team1 else team1
    chasing_team = toss_loser if toss_decision == "bat" else toss_winner

    return {
        "team1"         : team1,
        "team2"         : team2,
        "venue"         : venue,
        "toss_done"     : True,
        "toss_winner"   : toss_winner,
        "toss_decision" : toss_decision,
        "toss_loser"    : toss_loser,
        "chasing_team"  : chasing_team,
        "team1_xi"      : list(team1_xi) if team1_xi else [],
        "team2_xi"      : list(team2_xi) if team2_xi else [],
    }