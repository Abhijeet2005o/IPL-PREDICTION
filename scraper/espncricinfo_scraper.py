import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from cricdata import CricinfoClient

IPL_SERIES_ID = "1510719"
LIVE_SCORES_URL = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_CRICINFO_CLIENT = CricinfoClient()

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


def _clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _request_soup(url):
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def _get_espn_live_match(match_id=None):
    """
    Fetch a live/recent IPL match object from ESPN Cricinfo via cricdata.
    If match_id is provided, returns the matching match object.
    """
    live = _CRICINFO_CLIENT.live_matches()
    candidates = []
    for match in live:
        series = match.get("series", {})
        series_id = str(series.get("objectId", "")).strip()
        series_name = _clean_text(series.get("longName", "")).lower()
        if series_id == str(IPL_SERIES_ID).strip() or "indian premier league" in series_name:
            candidates.append(match)

    if match_id is None:
        return candidates[0] if candidates else None

    match_id = str(match_id).strip()
    for match in candidates:
        if str(match.get("objectId", "")).strip() == match_id:
            return match
    return None


def _extract_xi_from_scorecard(scorecard):
    team_xi = {}
    team_players = (
        scorecard.get("content", {})
        .get("matchPlayers", {})
        .get("teamPlayers", [])
    )
    for entry in team_players:
        team_name = _clean_text(entry.get("team", {}).get("longName", ""))
        players = entry.get("players", []) or []
        names = []
        for player in players:
            name = _clean_text(player.get("player", {}).get("longName", ""))
            if name:
                names.append(name)
        if team_name and names:
            team_xi[team_name] = names[:11]
    return team_xi


def _normalize_team_name(name, team_encoder=None):
    name = _clean_text(name)
    if not name:
        return name
    if name in TEAM_ALIASES:
        return TEAM_ALIASES[name]
    if team_encoder is not None:
        classes = set(team_encoder.classes_.tolist())
        if name in classes:
            return name
    for short, full in TEAM_ALIASES.items():
        if name.upper() == short:
            return full
    return name


def _safe_encode(encoder, value):
    classes = set(encoder.classes_.tolist())
    if value in classes:
        return int(encoder.transform([value])[0])
    return int(encoder.transform([encoder.classes_[0]])[0])


def _safe_div(num, den, fallback):
    return float(num / den) if den else float(fallback)


def _split_player_list(raw_text):
    text = _clean_text(raw_text)
    if not text:
        return []
    text = re.sub(r"\s*\(.*?\)", "", text)
    parts = re.split(r"\s*,\s*|\s+[•|]\s+|\s{2,}", text)
    players = []
    for name in parts:
        clean_name = _clean_text(name)
        if clean_name and clean_name.lower() not in {"playing xi", "impact subs"}:
            players.append(clean_name)
    seen = set()
    uniq = []
    for p in players:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            uniq.append(p)
    return uniq


def _extract_playing_xi(page_text, team1, team2):
    if not page_text:
        return [], []

    t1_xi = []
    t2_xi = []

    if team1 and team2:
        t1_pat = re.compile(
            rf"{re.escape(team1)}\s*Playing\s*XI\s*[:\-]\s*(.*?)(?={re.escape(team2)}\s*Playing\s*XI|Impact\s*Subs|$)",
            flags=re.I,
        )
        t2_pat = re.compile(
            rf"{re.escape(team2)}\s*Playing\s*XI\s*[:\-]\s*(.*?)(?={re.escape(team1)}\s*Playing\s*XI|Impact\s*Subs|$)",
            flags=re.I,
        )
        m1 = t1_pat.search(page_text)
        m2 = t2_pat.search(page_text)
        if m1:
            t1_xi = _split_player_list(m1.group(1))
        if m2:
            t2_xi = _split_player_list(m2.group(1))

    if not t1_xi or not t2_xi:
        generic = re.findall(r"Playing\s*XI\s*[:\-]\s*(.*?)(?=Playing\s*XI|Impact\s*Subs|$)", page_text, flags=re.I)
        if len(generic) >= 2:
            if not t1_xi:
                t1_xi = _split_player_list(generic[0])
            if not t2_xi:
                t2_xi = _split_player_list(generic[1])

    return t1_xi[:11], t2_xi[:11]


def _team_winrate(matches, team):
    team_matches = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if team_matches.empty:
        return 0.5, 0.5
    overall = (team_matches["winner"] == team).mean()
    recent = (team_matches.tail(5)["winner"] == team).mean()
    return float(overall), float(recent)


def _h2h(matches, team1, team2):
    h2h = matches[
        ((matches["team1"] == team1) & (matches["team2"] == team2))
        | ((matches["team1"] == team2) & (matches["team2"] == team1))
    ]
    if h2h.empty:
        return 0, 0
    t1 = int((h2h["winner"] == team1).sum())
    t2 = int((h2h["winner"] == team2).sum())
    return t1, t2


def _chase_metrics(matches, team):
    if "win_by_wickets" not in matches.columns:
        return 0.5, 0.4
    team_matches = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if team_matches.empty:
        return 0.5, 0.4
    chase_wins = team_matches[(team_matches["winner"] == team) & (team_matches["win_by_wickets"] > 0)]
    chase_win_pct = _safe_div(len(chase_wins), len(team_matches), 0.5)
    high_score_chase = 0.4 if chase_wins.empty else 1.0
    return float(chase_win_pct), float(high_score_chase)


def _global_player_defaults(player_lookup):
    cols = {
        "batting_avg": 25.0,
        "strike_rate": 125.0,
        "economy": 8.5,
        "bowling_avg": 30.0,
        "recent_strike_rate": 125.0,
        "recent_economy": 8.5,
    }
    defaults = {}
    for col, fallback in cols.items():
        defaults[col] = float(player_lookup[col].mean()) if col in player_lookup.columns else fallback
    defaults["top3_batting_avg"] = defaults["batting_avg"]
    return defaults


def _player_stats_for_xi(player_lookup, xi, defaults):
    if not xi:
        return defaults.copy()
    lookup = player_lookup.copy()
    lookup["player_norm"] = lookup["player"].astype(str).str.lower().str.strip()
    xi_norm = [str(x).lower().strip() for x in xi if str(x).strip()]
    selected = lookup[lookup["player_norm"].isin(xi_norm)]
    if selected.empty:
        return defaults.copy()
    selected = selected.reset_index(drop=True)
    out = {
        "batting_avg": float(selected["batting_avg"].mean()),
        "strike_rate": float(selected["strike_rate"].mean()),
        "top3_batting_avg": float(selected.sort_values("batting_avg", ascending=False).head(3)["batting_avg"].mean()),
        "economy": float(selected["economy"].mean()),
        "bowling_avg": float(selected["bowling_avg"].mean()),
        "recent_strike_rate": float(selected["recent_strike_rate"].mean()),
        "recent_economy": float(selected["recent_economy"].mean()),
    }
    for key, value in out.items():
        if pd.isna(value):
            out[key] = defaults[key]
    return out


def _parse_toss_winner_field(raw):
    """
    ESPNcricinfo API returns toss winner as either:
      - a nested dict: {"longName": "Mumbai Indians", "objectId": "..."}
      - a plain string: "Mumbai Indians"
      - None / empty
    This helper safely extracts the team name string in all cases.
    """
    if not raw:
        return ""
    if isinstance(raw, dict):
        return _clean_text(
            raw.get("longName", "")
            or raw.get("name", "")
            or raw.get("shortName", "")
        )
    return _clean_text(str(raw))


def get_todays_match_id():
    """Return today's IPL match ID from ESPN Cricinfo live matches."""
    try:
        match = _get_espn_live_match()
        if match:
            return int(match.get("objectId"))
    except Exception:
        pass

    # Fallback to Cricbuzz when ESPN fetch fails.
    try:
        soup = _request_soup(LIVE_SCORES_URL)
        links = soup.select("a[href*='/live-cricket-scores/']")
        for link in links:
            href = link.get("href", "")
            if "indian-premier-league" not in href.lower():
                continue
            m = re.search(r"/live-cricket-scores/(\d+)", href)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def scrape_match(match_id):
    """
    Scrape match details for the given match ID.
    Tries ESPN Cricinfo (via cricdata) first, then falls back to Cricbuzz.
    Returns the structure expected by app.py.
    """

    # ── ESPN Cricinfo path ────────────────────────────────────────────────────
    try:
        match = _get_espn_live_match(match_id=match_id)
        if match:
            series = match.get("series", {})
            series_slug = f"{series.get('slug')}-{series.get('objectId')}"
            match_slug = f"{match.get('slug')}-{match.get('objectId')}"
            info = _CRICINFO_CLIENT.match_info(series_slug, match_slug)
            scorecard = _CRICINFO_CLIENT.match_scorecard(series_slug, match_slug)

            teams = match.get("teams", []) or []
            team_names = [t.get("team", {}).get("longName", "") for t in teams]
            team1 = _clean_text(team_names[0] if len(team_names) > 0 else "Unknown")
            team2 = _clean_text(team_names[1] if len(team_names) > 1 else "Unknown")

            venue = _clean_text(info.get("venue", {}).get("longName", "")) or _clean_text(
                match.get("ground", {}).get("longName", "")
            ) or "Unknown Venue"

            # ── BUG FIX 1: toss winner is a nested dict, not a plain string ──
            toss = info.get("toss", {}) or {}

            # Try both key names the API has been observed to use
            toss_winner_raw = toss.get("winner_team") or toss.get("winner")
            toss_winner = _parse_toss_winner_field(toss_winner_raw)

            toss_decision = _clean_text(toss.get("decision", "")).lower()
            if toss_decision in {"1", "batting"}:
                toss_decision = "bat"
            elif toss_decision in {"2", "bowling", "fielding"}:
                toss_decision = "field"
            elif toss_decision not in {"bat", "field"}:
                toss_decision = None
            # ─────────────────────────────────────────────────────────────────

            chasing_team = None
            if toss_winner and toss_decision == "bat":
                if toss_winner in {team1, team2}:
                    chasing_team = team2 if toss_winner == team1 else team1
            elif toss_winner and toss_decision == "field":
                chasing_team = toss_winner

            xi_map = _extract_xi_from_scorecard(scorecard)
            team1_xi = xi_map.get(team1, [])
            team2_xi = xi_map.get(team2, [])

            return {
                "match_id": int(match_id),
                "team1": team1,
                "team2": team2,
                "venue": venue,
                "toss_done": bool(toss_winner),
                "toss_winner": toss_winner or None,
                "toss_decision": toss_decision,
                "chasing_team": chasing_team,
                "team1_xi": team1_xi,
                "team2_xi": team2_xi,
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
    except Exception:
        # Fall through to Cricbuzz parser below.
        pass

    # ── Cricbuzz fallback path ────────────────────────────────────────────────
    try:
        soup = _request_soup(MATCH_URL_TEMPLATE.format(match_id=match_id))
        page_text = _clean_text(soup.get_text(" ", strip=True))
        title_text = _clean_text(soup.title.get_text() if soup.title else "")

        team1, team2 = "Unknown", "Unknown"
        title_match = re.search(r"commentary\s*\|\s*(.*?)\s+vs\s+(.*?),", title_text, flags=re.I)
        if title_match:
            team1 = _clean_text(title_match.group(1))
            team2 = _clean_text(title_match.group(2))

        venue = "Unknown Venue"
        venue_match = re.search(r"Venue:\s*(.*?)\s*•\s*Date\s*&\s*Time:", page_text, flags=re.I)
        if venue_match:
            venue = _clean_text(venue_match.group(1))

        toss_winner = None
        toss_decision = None
        chasing_team = None

        # ── BUG FIX 2: more robust toss regex for Cricbuzz ───────────────────
        # Primary pattern: "X won the toss and elected to bat/bowl"
        primary = re.search(
            r"([\w][\w\s]+?)\s+won the toss and (?:elected to|opt(?:ed)? to|chose to)\s+(bat|bowl|field)",
            page_text,
            flags=re.I,
        )
        if primary:
            toss_winner = _clean_text(primary.group(1))
            decision_word = primary.group(2).lower()
            toss_decision = "bat" if "bat" in decision_word else "field"
        else:
            # Secondary pattern: "Toss: <team> (<decision>)"
            secondary = re.search(
                r"Toss[:\s]+([A-Za-z][A-Za-z\s]+?)\s*[\(\[,]\s*(bat|bowl|field)",
                page_text,
                flags=re.I,
            )
            if secondary:
                toss_winner = _clean_text(secondary.group(1))
                decision_word = secondary.group(2).lower()
                toss_decision = "bat" if "bat" in decision_word else "field"
            else:
                # Tertiary: original anchor-based pattern kept as last resort
                tertiary = re.search(
                    r"Toss:\s*(.*?)\s*(Have Your Say|Recent\s*:|Live Scorecard|Info)",
                    page_text,
                    flags=re.I,
                )
                if tertiary:
                    toss_text = _clean_text(tertiary.group(1))
                    toss_winner = _clean_text(re.sub(r"\(.*?\)", "", toss_text))
                    lower_toss = toss_text.lower()
                    if "bat" in lower_toss:
                        toss_decision = "bat"
                    elif "bowl" in lower_toss or "field" in lower_toss:
                        toss_decision = "field"
        # ─────────────────────────────────────────────────────────────────────

        if toss_winner and toss_decision:
            # Snap toss_winner to whichever team name it is closest to.
            # This handles cases where the scraped name has extra words.
            if team1 and team1 != "Unknown" and team1.lower() in toss_winner.lower():
                toss_winner = team1
            elif team2 and team2 != "Unknown" and team2.lower() in toss_winner.lower():
                toss_winner = team2

            if toss_winner in {team1, team2}:
                if toss_decision == "bat":
                    chasing_team = team2 if toss_winner == team1 else team1
                else:
                    chasing_team = toss_winner

        team1_xi, team2_xi = _extract_playing_xi(page_text, team1, team2)

        return {
            "match_id": int(match_id),
            "team1": team1,
            "team2": team2,
            "venue": venue,
            "toss_done": bool(toss_winner),
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "chasing_team": chasing_team,
            "team1_xi": team1_xi,
            "team2_xi": team2_xi,
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as exc:
        return {"error": str(exc)}


def build_feature_vector(
    match_info,
    player_lookup,
    matches,
    team_encoder,
    venue_encoder,
    venue_score_history,
    team_pp_eco_lookup,
    team_opener_lookup,
    get_team_recent_avg_score,
    get_season_avg_score,
    get_season_year,
    get_venue_recent_avg_score,
    get_team_recent_high_score_rate,
    feature_cols,
):
    """Build model-ready feature dataframe using available live + historical context."""
    team1 = _normalize_team_name(match_info.get("team1"), team_encoder)
    team2 = _normalize_team_name(match_info.get("team2"), team_encoder)
    venue = _clean_text(match_info.get("venue", ""))

    now = pd.Timestamp(datetime.today().date())
    t1_id = _safe_encode(team_encoder, team1)
    t2_id = _safe_encode(team_encoder, team2)
    venue_id = _safe_encode(venue_encoder, venue if venue in set(venue_encoder.classes_.tolist()) else venue_encoder.classes_[0])

    t1_h2h, t2_h2h = _h2h(matches, team1, team2)
    t1_winrate, t1_last5 = _team_winrate(matches, team1)
    t2_winrate, t2_last5 = _team_winrate(matches, team2)
    t1_chase_pct, t1_high_chase = _chase_metrics(matches, team1)
    t2_chase_pct, t2_high_chase = _chase_metrics(matches, team2)

    season_avg = float(get_season_avg_score(now))
    season_year = int(get_season_year(now))

    t1_recent_avg = float(get_team_recent_avg_score(team1, now))
    t2_recent_avg = float(get_team_recent_avg_score(team2, now))
    t1_high_score_rate = float(get_team_recent_high_score_rate(team1, now))
    t2_high_score_rate = float(get_team_recent_high_score_rate(team2, now))

    if "venue" in venue_score_history.columns and "first_innings_score" in venue_score_history.columns:
        vmask = venue_score_history["venue"] == venue
        venue_avg = float(venue_score_history.loc[vmask, "first_innings_score"].mean()) if vmask.any() else 167.0
    else:
        venue_avg = 167.0
    venue_recent = float(get_venue_recent_avg_score(venue, now))

    toss_winner = _normalize_team_name(match_info.get("toss_winner"), team_encoder)
    toss_done = bool(match_info.get("toss_done", toss_winner))
    toss_decision = _clean_text(match_info.get("toss_decision", "")).lower()

    pp_default = float(sum(team_pp_eco_lookup.values()) / len(team_pp_eco_lookup)) if team_pp_eco_lookup else 8.5
    t1_pp_eco = float(team_pp_eco_lookup.get(team1, pp_default))
    t2_pp_eco = float(team_pp_eco_lookup.get(team2, pp_default))

    opener_default = {
        "opener_avg_batting_avg": 30.0,
        "opener_avg_strike_rate": 130.0,
    }
    t1_open = team_opener_lookup.get(team1, opener_default)
    t2_open = team_opener_lookup.get(team2, opener_default)

    defaults = _global_player_defaults(player_lookup)
    t1_stats = _player_stats_for_xi(player_lookup, match_info.get("team1_xi", []), defaults)
    t2_stats = _player_stats_for_xi(player_lookup, match_info.get("team2_xi", []), defaults)

    feat = {c: 0.0 for c in feature_cols}
    feat.update(
        {
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
            "chase_win_pct_team1": t1_chase_pct,
            "chase_win_pct_team2": t2_chase_pct,
            "high_score_chase_t1": t1_high_chase,
            "high_score_chase_t2": t2_high_chase,
            "winrate_team1": t1_winrate,
            "winrate_team2": t2_winrate,
            "last5_win_team1": t1_last5,
            "last5_win_team2": t2_last5,
            "t1_recent_avg_score": t1_recent_avg,
            "t2_recent_avg_score": t2_recent_avg,
            "t1_high_score_rate": t1_high_score_rate,
            "t2_high_score_rate": t2_high_score_rate,
            "t1_pp_bowling_economy": t1_pp_eco,
            "t2_pp_bowling_economy": t2_pp_eco,
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
            "t1_rolling_season_avg": t1_recent_avg,
            "t2_rolling_season_avg": t2_recent_avg,
        }
    )

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)    for match in candidates:
        if str(match.get("objectId", "")).strip() == match_id:
            return match
    return None


def _extract_xi_from_scorecard(scorecard):
    team_xi = {}
    team_players = (
        scorecard.get("content", {})
        .get("matchPlayers", {})
        .get("teamPlayers", [])
    )
    for entry in team_players:
        team_name = _clean_text(entry.get("team", {}).get("longName", ""))
        players = entry.get("players", []) or []
        names = []
        for player in players:
            name = _clean_text(player.get("player", {}).get("longName", ""))
            if name:
                names.append(name)
        if team_name and names:
            team_xi[team_name] = names[:11]
    return team_xi


def _normalize_team_name(name, team_encoder=None):
    name = _clean_text(name)
    if not name:
        return name
    if name in TEAM_ALIASES:
        return TEAM_ALIASES[name]
    if team_encoder is not None:
        classes = set(team_encoder.classes_.tolist())
        if name in classes:
            return name
    for short, full in TEAM_ALIASES.items():
        if name.upper() == short:
            return full
    return name


def _safe_encode(encoder, value):
    classes = set(encoder.classes_.tolist())
    if value in classes:
        return int(encoder.transform([value])[0])
    return int(encoder.transform([encoder.classes_[0]])[0])


def _safe_div(num, den, fallback):
    return float(num / den) if den else float(fallback)


def _split_player_list(raw_text):
    text = _clean_text(raw_text)
    if not text:
        return []
    text = re.sub(r"\s*\(.*?\)", "", text)
    parts = re.split(r"\s*,\s*|\s+[•|]\s+|\s{2,}", text)
    players = []
    for name in parts:
        clean_name = _clean_text(name)
        if clean_name and clean_name.lower() not in {"playing xi", "impact subs"}:
            players.append(clean_name)
    # Keep unique players in order.
    seen = set()
    uniq = []
    for p in players:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            uniq.append(p)
    return uniq


def _extract_playing_xi(page_text, team1, team2):
    if not page_text:
        return [], []

    t1_xi = []
    t2_xi = []

    if team1 and team2:
        t1_pat = re.compile(
            rf"{re.escape(team1)}\s*Playing\s*XI\s*[:\-]\s*(.*?)(?={re.escape(team2)}\s*Playing\s*XI|Impact\s*Subs|$)",
            flags=re.I,
        )
        t2_pat = re.compile(
            rf"{re.escape(team2)}\s*Playing\s*XI\s*[:\-]\s*(.*?)(?={re.escape(team1)}\s*Playing\s*XI|Impact\s*Subs|$)",
            flags=re.I,
        )
        m1 = t1_pat.search(page_text)
        m2 = t2_pat.search(page_text)
        if m1:
            t1_xi = _split_player_list(m1.group(1))
        if m2:
            t2_xi = _split_player_list(m2.group(1))

    if not t1_xi or not t2_xi:
        generic = re.findall(r"Playing\s*XI\s*[:\-]\s*(.*?)(?=Playing\s*XI|Impact\s*Subs|$)", page_text, flags=re.I)
        if len(generic) >= 2:
            if not t1_xi:
                t1_xi = _split_player_list(generic[0])
            if not t2_xi:
                t2_xi = _split_player_list(generic[1])

    return t1_xi[:11], t2_xi[:11]


def _team_winrate(matches, team):
    team_matches = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if team_matches.empty:
        return 0.5, 0.5
    overall = (team_matches["winner"] == team).mean()
    recent = (team_matches.tail(5)["winner"] == team).mean()
    return float(overall), float(recent)


def _h2h(matches, team1, team2):
    h2h = matches[
        ((matches["team1"] == team1) & (matches["team2"] == team2))
        | ((matches["team1"] == team2) & (matches["team2"] == team1))
    ]
    if h2h.empty:
        return 0, 0
    t1 = int((h2h["winner"] == team1).sum())
    t2 = int((h2h["winner"] == team2).sum())
    return t1, t2


def _chase_metrics(matches, team):
    if "win_by_wickets" not in matches.columns:
        return 0.5, 0.4
    team_matches = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if team_matches.empty:
        return 0.5, 0.4
    chase_wins = team_matches[(team_matches["winner"] == team) & (team_matches["win_by_wickets"] > 0)]
    chase_win_pct = _safe_div(len(chase_wins), len(team_matches), 0.5)
    high_score_chase = 0.4 if chase_wins.empty else 1.0
    return float(chase_win_pct), float(high_score_chase)


def _global_player_defaults(player_lookup):
    cols = {
        "batting_avg": 25.0,
        "strike_rate": 125.0,
        "economy": 8.5,
        "bowling_avg": 30.0,
        "recent_strike_rate": 125.0,
        "recent_economy": 8.5,
    }
    defaults = {}
    for col, fallback in cols.items():
        defaults[col] = float(player_lookup[col].mean()) if col in player_lookup.columns else fallback
    defaults["top3_batting_avg"] = defaults["batting_avg"]
    return defaults


def _player_stats_for_xi(player_lookup, xi, defaults):
    if not xi:
        return defaults.copy()
    lookup = player_lookup.copy()
    lookup["player_norm"] = lookup["player"].astype(str).str.lower().str.strip()
    xi_norm = [str(x).lower().strip() for x in xi if str(x).strip()]
    selected = lookup[lookup["player_norm"].isin(xi_norm)]
    if selected.empty:
        return defaults.copy()
    selected = selected.reset_index(drop=True)
    out = {
        "batting_avg": float(selected["batting_avg"].mean()),
        "strike_rate": float(selected["strike_rate"].mean()),
        "top3_batting_avg": float(selected.sort_values("batting_avg", ascending=False).head(3)["batting_avg"].mean()),
        "economy": float(selected["economy"].mean()),
        "bowling_avg": float(selected["bowling_avg"].mean()),
        "recent_strike_rate": float(selected["recent_strike_rate"].mean()),
        "recent_economy": float(selected["recent_economy"].mean()),
    }
    for key, value in out.items():
        if pd.isna(value):
            out[key] = defaults[key]
    return out


def get_todays_match_id():
    """Return today's IPL match ID from ESPN Cricinfo live matches."""
    try:
        match = _get_espn_live_match()
        if match:
            return int(match.get("objectId"))
    except Exception:
        pass

    # Fallback to previous Cricbuzz scraper behavior when ESPN fetch fails.
    try:
        soup = _request_soup(LIVE_SCORES_URL)
        links = soup.select("a[href*='/live-cricket-scores/']")
        for link in links:
            href = link.get("href", "")
            if "indian-premier-league" not in href.lower():
                continue
            m = re.search(r"/live-cricket-scores/(\d+)", href)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def scrape_match(match_id):
    """
    Scrape match details from Cricbuzz for the given match ID.
    Returns the structure expected by app.py.
    """
    try:
        match = _get_espn_live_match(match_id=match_id)
        if match:
            series = match.get("series", {})
            series_slug = f"{series.get('slug')}-{series.get('objectId')}"
            match_slug = f"{match.get('slug')}-{match.get('objectId')}"
            info = _CRICINFO_CLIENT.match_info(series_slug, match_slug)
            scorecard = _CRICINFO_CLIENT.match_scorecard(series_slug, match_slug)

            teams = match.get("teams", []) or []
            team_names = [t.get("team", {}).get("longName", "") for t in teams]
            team1 = _clean_text(team_names[0] if len(team_names) > 0 else "Unknown")
            team2 = _clean_text(team_names[1] if len(team_names) > 1 else "Unknown")

            venue = _clean_text(info.get("venue", {}).get("longName", "")) or _clean_text(
                match.get("ground", {}).get("longName", "")
            ) or "Unknown Venue"

            toss = info.get("toss", {}) or {}
            toss_winner = _clean_text(toss.get("winner_team", ""))
            toss_decision = _clean_text(toss.get("decision", "")).lower()
            if toss_decision in {"1", "batting"}:
                toss_decision = "bat"
            elif toss_decision in {"2", "bowling", "fielding"}:
                toss_decision = "field"
            elif toss_decision not in {"bat", "field"}:
                toss_decision = None

            chasing_team = None
            if toss_winner and toss_decision == "bat":
                if toss_winner in {team1, team2}:
                    chasing_team = team2 if toss_winner == team1 else team1
            elif toss_winner and toss_decision == "field":
                chasing_team = toss_winner

            xi_map = _extract_xi_from_scorecard(scorecard)
            team1_xi = xi_map.get(team1, [])
            team2_xi = xi_map.get(team2, [])

            return {
                "match_id": int(match_id),
                "team1": team1,
                "team2": team2,
                "venue": venue,
                "toss_done": bool(toss_winner),
                "toss_winner": toss_winner or None,
                "toss_decision": toss_decision,
                "chasing_team": chasing_team,
                "team1_xi": team1_xi,
                "team2_xi": team2_xi,
                "scraped_at": datetime.utcnow().isoformat() + "Z",
            }
    except Exception:
        # Fall back to Cricbuzz parser below.
        pass

    try:
        soup = _request_soup(MATCH_URL_TEMPLATE.format(match_id=match_id))
        page_text = _clean_text(soup.get_text(" ", strip=True))
        title_text = _clean_text(soup.title.get_text() if soup.title else "")

        team1, team2 = "Unknown", "Unknown"
        title_match = re.search(r"commentary\s*\|\s*(.*?)\s+vs\s+(.*?),", title_text, flags=re.I)
        if title_match:
            team1 = _clean_text(title_match.group(1))
            team2 = _clean_text(title_match.group(2))

        venue = "Unknown Venue"
        venue_match = re.search(r"Venue:\s*(.*?)\s*•\s*Date\s*&\s*Time:", page_text, flags=re.I)
        if venue_match:
            venue = _clean_text(venue_match.group(1))

        toss_winner = None
        toss_decision = None
        chasing_team = None
        toss_match = re.search(r"Toss:\s*(.*?)\s*(Have Your Say|Recent\s*:|Live Scorecard|Info)", page_text, flags=re.I)
        if toss_match:
            toss_text = _clean_text(toss_match.group(1))
            toss_winner = _clean_text(re.sub(r"\(.*?\)", "", toss_text))
            lower_toss = toss_text.lower()
            if "bat" in lower_toss:
                toss_decision = "bat"
                if toss_winner in {team1, team2}:
                    chasing_team = team2 if toss_winner == team1 else team1
            elif "bowl" in lower_toss or "field" in lower_toss:
                toss_decision = "field"
                chasing_team = toss_winner

        team1_xi, team2_xi = _extract_playing_xi(page_text, team1, team2)

        return {
            "match_id": int(match_id),
            "team1": team1,
            "team2": team2,
            "venue": venue,
            "toss_done": bool(toss_winner),
            "toss_winner": toss_winner,
            "toss_decision": toss_decision,
            "chasing_team": chasing_team,
            "team1_xi": team1_xi,
            "team2_xi": team2_xi,
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }
    except Exception as exc:
        return {"error": str(exc)}


def build_feature_vector(
    match_info,
    player_lookup,
    matches,
    team_encoder,
    venue_encoder,
    venue_score_history,
    team_pp_eco_lookup,
    team_opener_lookup,
    get_team_recent_avg_score,
    get_season_avg_score,
    get_season_year,
    get_venue_recent_avg_score,
    get_team_recent_high_score_rate,
    feature_cols,
):
    """Build model-ready feature dataframe using available live + historical context."""
    team1 = _normalize_team_name(match_info.get("team1"), team_encoder)
    team2 = _normalize_team_name(match_info.get("team2"), team_encoder)
    venue = _clean_text(match_info.get("venue", ""))

    now = pd.Timestamp(datetime.today().date())
    t1_id = _safe_encode(team_encoder, team1)
    t2_id = _safe_encode(team_encoder, team2)
    venue_id = _safe_encode(venue_encoder, venue if venue in set(venue_encoder.classes_.tolist()) else venue_encoder.classes_[0])

    t1_h2h, t2_h2h = _h2h(matches, team1, team2)
    t1_winrate, t1_last5 = _team_winrate(matches, team1)
    t2_winrate, t2_last5 = _team_winrate(matches, team2)
    t1_chase_pct, t1_high_chase = _chase_metrics(matches, team1)
    t2_chase_pct, t2_high_chase = _chase_metrics(matches, team2)

    season_avg = float(get_season_avg_score(now))
    season_year = int(get_season_year(now))

    t1_recent_avg = float(get_team_recent_avg_score(team1, now))
    t2_recent_avg = float(get_team_recent_avg_score(team2, now))
    t1_high_score_rate = float(get_team_recent_high_score_rate(team1, now))
    t2_high_score_rate = float(get_team_recent_high_score_rate(team2, now))

    if "venue" in venue_score_history.columns and "first_innings_score" in venue_score_history.columns:
        vmask = venue_score_history["venue"] == venue
        venue_avg = float(venue_score_history.loc[vmask, "first_innings_score"].mean()) if vmask.any() else 167.0
    else:
        venue_avg = 167.0
    venue_recent = float(get_venue_recent_avg_score(venue, now))

    toss_winner = _normalize_team_name(match_info.get("toss_winner"), team_encoder)
    toss_done = bool(match_info.get("toss_done", toss_winner))
    toss_decision = _clean_text(match_info.get("toss_decision", "")).lower()

    pp_default = float(sum(team_pp_eco_lookup.values()) / len(team_pp_eco_lookup)) if team_pp_eco_lookup else 8.5
    t1_pp_eco = float(team_pp_eco_lookup.get(team1, pp_default))
    t2_pp_eco = float(team_pp_eco_lookup.get(team2, pp_default))

    opener_default = {
        "opener_avg_batting_avg": 30.0,
        "opener_avg_strike_rate": 130.0,
    }
    t1_open = team_opener_lookup.get(team1, opener_default)
    t2_open = team_opener_lookup.get(team2, opener_default)

    defaults = _global_player_defaults(player_lookup)
    t1_stats = _player_stats_for_xi(player_lookup, match_info.get("team1_xi", []), defaults)
    t2_stats = _player_stats_for_xi(player_lookup, match_info.get("team2_xi", []), defaults)

    feat = {c: 0.0 for c in feature_cols}
    feat.update(
        {
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
            "chase_win_pct_team1": t1_chase_pct,
            "chase_win_pct_team2": t2_chase_pct,
            "high_score_chase_t1": t1_high_chase,
            "high_score_chase_t2": t2_high_chase,
            "winrate_team1": t1_winrate,
            "winrate_team2": t2_winrate,
            "last5_win_team1": t1_last5,
            "last5_win_team2": t2_last5,
            "t1_recent_avg_score": t1_recent_avg,
            "t2_recent_avg_score": t2_recent_avg,
            "t1_high_score_rate": t1_high_score_rate,
            "t2_high_score_rate": t2_high_score_rate,
            "t1_pp_bowling_economy": t1_pp_eco,
            "t2_pp_bowling_economy": t2_pp_eco,
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
            "t1_rolling_season_avg": t1_recent_avg,
            "t2_rolling_season_avg": t2_recent_avg,
        }
    )

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)
