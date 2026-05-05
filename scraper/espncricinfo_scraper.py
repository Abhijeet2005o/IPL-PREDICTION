import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Try cricdata — gracefully skip if not installed ───────
try:
    from cricdata import CricinfoClient
    _CRICINFO_CLIENT = CricinfoClient()
    _CRICDATA_AVAILABLE = True
except Exception:
    _CRICINFO_CLIENT = None
    _CRICDATA_AVAILABLE = False

IPL_SERIES_ID    = "1510719"
LIVE_SCORES_URL  = "https://www.cricbuzz.com/cricket-match/live-scores"
MATCH_URL_TEMPLATE = "https://www.cricbuzz.com/live-cricket-scores/{match_id}"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

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


# ─────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────
# ESPN CRICINFO — cricdata client
# ─────────────────────────────────────────────────────────
def _get_espn_live_match(match_id=None):
    if not _CRICDATA_AVAILABLE or _CRICINFO_CLIENT is None:
        return None
    try:
        live = _CRICINFO_CLIENT.live_matches()
        candidates = []
        for match in live:
            series      = match.get("series", {})
            series_id   = str(series.get("objectId", "")).strip()
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
    except Exception:
        pass
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
            team_name = _clean_text(entry.get("team", {}).get("longName", ""))
            players   = entry.get("players", []) or []
            names     = [
                _clean_text(p.get("player", {}).get("longName", ""))
                for p in players
            ]
            names = [n for n in names if n]
            if team_name and names:
                team_xi[team_name] = names[:11]
    except Exception:
        pass
    return team_xi


# ─────────────────────────────────────────────────────────
# CRICBUZZ — JSON API (faster + more reliable than HTML)
# ─────────────────────────────────────────────────────────
_CB_MATCH_INFO_URL = "https://www.cricbuzz.com/api/cricket-match/{match_id}/info"
_CB_SCORECARD_URL  = "https://www.cricbuzz.com/api/cricket-match/{match_id}/scorecard"


def _cricbuzz_json_match_info(match_id):
    """
    Fetch structured match info from Cricbuzz JSON API.
    Returns dict with toss_winner, toss_decision, teams, venue etc.
    """
    try:
        data = _request_json(
            _CB_MATCH_INFO_URL.format(match_id=match_id)
        )
        match_info = data.get("matchInfo", {}) or {}
        match_score = data.get("matchScore", {}) or {}

        # Teams
        team1_obj = match_info.get("team1", {}) or {}
        team2_obj = match_info.get("team2", {}) or {}
        team1 = _clean_text(team1_obj.get("name", ""))
        team2 = _clean_text(team2_obj.get("name", ""))

        # Venue
        venue_obj = match_info.get("venueInfo", {}) or {}
        venue = _clean_text(
            venue_obj.get("ground", "") + ", " + venue_obj.get("city", "")
        ).strip(", ")

        # Toss
        toss = match_info.get("tossResults", {}) or {}
        toss_winner_id  = str(toss.get("tossWinnerId", "")).strip()
        toss_decision   = _clean_text(toss.get("decision", "")).lower()

        # Map toss winner ID → name
        toss_winner = ""
        if toss_winner_id:
            if toss_winner_id == str(team1_obj.get("id", "")):
                toss_winner = team1
            elif toss_winner_id == str(team2_obj.get("id", "")):
                toss_winner = team2

        # Normalise decision
        if "bat" in toss_decision:
            toss_decision = "bat"
        elif "field" in toss_decision or "bowl" in toss_decision:
            toss_decision = "field"
        else:
            toss_decision = None

        toss_done = bool(toss_winner and toss_decision)

        chasing_team = None
        if toss_done:
            if toss_decision == "bat":
                chasing_team = team2 if toss_winner == team1 else team1
            else:
                chasing_team = toss_winner

        return {
            "team1":         team1,
            "team2":         team2,
            "venue":         venue or "Unknown Venue",
            "toss_done":     toss_done,
            "toss_winner":   toss_winner or None,
            "toss_decision": toss_decision,
            "chasing_team":  chasing_team,
            "raw":           data,
        }
    except Exception:
        return None


def _cricbuzz_json_xi(match_id):
    """Fetch playing XI from Cricbuzz scorecard JSON API."""
    team1_xi, team2_xi = [], []
    try:
        data = _request_json(
            _CB_SCORECARD_URL.format(match_id=match_id)
        )
        # scorecard → innings list
        scorecard = data.get("scoreCard", []) or []
        teams_seen = {}
        for inning in scorecard:
            bat_team = _clean_text(
                inning.get("batTeamDetails", {}).get("batTeamName", "")
            )
            batsmen = inning.get("batTeamDetails", {}).get("batsmenData", {}) or {}
            names   = [
                _clean_text(v.get("batName", ""))
                for v in batsmen.values()
                if v.get("batName")
            ]
            if bat_team and names and bat_team not in teams_seen:
                teams_seen[bat_team] = names[:11]

        team_list = list(teams_seen.keys())
        if len(team_list) >= 1:
            team1_xi = teams_seen[team_list[0]]
        if len(team_list) >= 2:
            team2_xi = teams_seen[team_list[1]]
    except Exception:
        pass
    return team1_xi, team2_xi


# ─────────────────────────────────────────────────────────
# CRICBUZZ HTML scraper — toss detection (5 patterns)
# ─────────────────────────────────────────────────────────
_TOSS_PATTERNS = [
    # Pattern 1 — standard: "TeamName won the toss and elected to bat/field"
    re.compile(
        r"([A-Za-z\s]+?)\s+won\s+the\s+toss\s+and\s+(?:elected|chose)\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # Pattern 2 — Cricbuzz label: "Toss: TeamName (bat/field)"
    re.compile(
        r"Toss\s*[:\-]\s*([A-Za-z\s]+?)\s*[\(\,]\s*(bat|bowl|field)",
        re.I,
    ),
    # Pattern 3 — "Toss: TeamName , opt to bat/field"
    re.compile(
        r"Toss\s*[:\-]\s*([A-Za-z\s]+?)\s*,\s*opt(?:ed)?\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # Pattern 4 — JSON-like inline text
    re.compile(
        r'"tossWinner"\s*:\s*"([^"]+)".*?"tossDecision"\s*:\s*"([^"]+)"',
        re.I | re.DOTALL,
    ),
    # Pattern 5 — broad fallback: any mention of toss + decision
    re.compile(
        r"toss[^.]{0,60}?(bat(?:ting)?|bowl(?:ing)?|field(?:ing)?)",
        re.I,
    ),
]


def _parse_toss_from_text(page_text, team1, team2):
    """
    Try all 5 toss patterns. Returns (toss_winner, toss_decision) or ("", None).
    """
    for i, pat in enumerate(_TOSS_PATTERNS):
        m = pat.search(page_text)
        if not m:
            continue

        if i < 4:
            raw_winner   = _clean_text(m.group(1))
            raw_decision = m.group(2).lower() if m.lastindex >= 2 else ""
        else:
            # Pattern 5 — only decision captured
            raw_winner   = ""
            raw_decision = m.group(1).lower()

        # Normalise decision
        if "bat" in raw_decision:
            decision = "bat"
        elif "bowl" in raw_decision or "field" in raw_decision:
            decision = "field"
        else:
            continue

        # Match winner string to known team name
        winner = ""
        for candidate in [team1, team2]:
            if candidate and candidate.lower() in raw_winner.lower():
                winner = candidate
                break
        if not winner and raw_winner:
            # partial match — pick whichever team name overlaps most
            t1_overlap = len(
                set(raw_winner.lower().split()) &
                set(team1.lower().split())
            ) if team1 else 0
            t2_overlap = len(
                set(raw_winner.lower().split()) &
                set(team2.lower().split())
            ) if team2 else 0
            if t1_overlap > t2_overlap:
                winner = team1
            elif t2_overlap > t1_overlap:
                winner = team2

        if winner and decision:
            return winner, decision

    return "", None


# ─────────────────────────────────────────────────────────
# PLAYING XI — HTML extraction
# ─────────────────────────────────────────────────────────
def _split_player_list(raw_text):
    text = _clean_text(raw_text)
    if not text:
        return []
    text  = re.sub(r"\s*\(.*?\)", "", text)
    parts = re.split(r"\s*,\s*|\s+[•|]\s+|\s{2,}", text)
    seen, uniq = set(), []
    for name in parts:
        cn  = _clean_text(name)
        key = cn.lower()
        if cn and key not in {"playing xi", "impact subs"} and key not in seen:
            seen.add(key)
            uniq.append(cn)
    return uniq


def _extract_playing_xi(page_text, team1, team2):
    if not page_text:
        return [], []

    t1_xi, t2_xi = [], []

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
            r"(?:Playing\s*)?XI\s*[:\-]\s*(.*?)(?=(?:Playing\s*)?XI|Impact\s*Subs|$)",
            page_text,
            re.I | re.DOTALL,
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
    name = _clean_text(name)
    if not name:
        return name
    upper = name.upper()
    if upper in TEAM_ALIASES:
        return TEAM_ALIASES[upper]
    if name in TEAM_ALIASES:
        return TEAM_ALIASES[name]
    if team_encoder is not None:
        if name in set(team_encoder.classes_.tolist()):
            return name
    return name


def _safe_encode(encoder, value):
    classes = set(encoder.classes_.tolist())
    if value in classes:
        return int(encoder.transform([value])[0])
    return int(encoder.transform([encoder.classes_[0]])[0])


def _safe_div(num, den, fallback):
    return float(num / den) if den else float(fallback)


# ─────────────────────────────────────────────────────────
# PLAYER / TEAM STAT HELPERS
# ─────────────────────────────────────────────────────────
def _team_winrate(matches, team):
    tm = matches[(matches["team1"] == team) | (matches["team2"] == team)]
    if tm.empty:
        return 0.5, 0.5
    return float((tm["winner"] == team).mean()), float((tm.tail(5)["winner"] == team).mean())


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
        "batting_avg": 25.0, "strike_rate": 125.0, "economy": 8.5,
        "bowling_avg": 30.0, "recent_strike_rate": 125.0, "recent_economy": 8.5,
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
    xi_norm  = [str(x).lower().strip() for x in xi if str(x).strip()]
    selected = lk[lk["player_norm"].isin(xi_norm)].reset_index(drop=True)
    if selected.empty:
        return defaults.copy()
    out = {
        "batting_avg":      float(selected["batting_avg"].mean()),
        "strike_rate":      float(selected["strike_rate"].mean()),
        "top3_batting_avg": float(selected.nlargest(3, "batting_avg")["batting_avg"].mean()),
        "economy":          float(selected["economy"].mean()),
        "bowling_avg":      float(selected["bowling_avg"].mean()),
        "recent_strike_rate": float(selected["recent_strike_rate"].mean()),
        "recent_economy":   float(selected["recent_economy"].mean()),
    }
    for k, v in out.items():
        if pd.isna(v):
            out[k] = defaults[k]
    return out


# ─────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────
def get_todays_match_id():
    """Return today's IPL match ID — ESPN first, Cricbuzz fallback."""
    # ESPN via cricdata
    try:
        m = _get_espn_live_match()
        if m:
            return int(m.get("objectId"))
    except Exception:
        pass

    # Cricbuzz live-scores page
    try:
        soup  = _request_soup(LIVE_SCORES_URL)
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

    return None


def scrape_match(match_id):
    """
    Scrape all match details for match_id.
    Returns dict expected by app.py / build_feature_vector.

    Priority order:
      1. ESPN Cricinfo (cricdata)       — most structured
      2. Cricbuzz JSON API              — fast + reliable
      3. Cricbuzz HTML scrape           — fallback
    """
    errors = []

    # ── 1. ESPN Cricinfo ──────────────────────────────────
    try:
        espn_match = _get_espn_live_match(match_id=match_id)
        if espn_match:
            series     = espn_match.get("series", {})
            s_slug     = f"{series.get('slug')}-{series.get('objectId')}"
            m_slug     = f"{espn_match.get('slug')}-{espn_match.get('objectId')}"
            info       = _CRICINFO_CLIENT.match_info(s_slug, m_slug)
            scorecard  = _CRICINFO_CLIENT.match_scorecard(s_slug, m_slug)

            teams      = espn_match.get("teams", []) or []
            t_names    = [t.get("team", {}).get("longName", "") for t in teams]
            team1      = _clean_text(t_names[0] if t_names else "Unknown")
            team2      = _clean_text(t_names[1] if len(t_names) > 1 else "Unknown")

            venue = (
                _clean_text(info.get("venue", {}).get("longName", ""))
                or _clean_text(espn_match.get("ground", {}).get("longName", ""))
                or "Unknown Venue"
            )

            toss     = info.get("toss", {}) or {}
            tw_raw   = _clean_text(toss.get("winner_team", ""))
            td_raw   = _clean_text(toss.get("decision", "")).lower()
            if td_raw in {"1", "batting"}:
                td_raw = "bat"
            elif td_raw in {"2", "bowling", "fielding"}:
                td_raw = "field"
            elif td_raw not in {"bat", "field"}:
                td_raw = None

            toss_done    = bool(tw_raw and td_raw)
            chasing_team = None
            if toss_done:
                chasing_team = (
                    (team2 if tw_raw == team1 else team1)
                    if td_raw == "bat" else tw_raw
                )

            xi_map   = _extract_xi_from_scorecard(scorecard)
            team1_xi = xi_map.get(team1, [])
            team2_xi = xi_map.get(team2, [])

            return {
                "match_id":      int(match_id),
                "team1":         team1,
                "team2":         team2,
                "venue":         venue,
                "toss_done":     toss_done,
                "toss_winner":   tw_raw or None,
                "toss_decision": td_raw,
                "chasing_team":  chasing_team,
                "team1_xi":      team1_xi,
                "team2_xi":      team2_xi,
                "source":        "espn",
                "scraped_at":    datetime.utcnow().isoformat() + "Z",
            }
    except Exception as e:
        errors.append(f"ESPN: {e}")

    # ── 2. Cricbuzz JSON API ──────────────────────────────
    try:
        cb_info = _cricbuzz_json_match_info(match_id)
        if cb_info and cb_info.get("team1"):
            team1 = cb_info["team1"]
            team2 = cb_info["team2"]

            # Try to get XI from scorecard JSON
            xi1_json, xi2_json = _cricbuzz_json_xi(match_id)

            return {
                "match_id":      int(match_id),
                "team1":         team1,
                "team2":         team2,
                "venue":         cb_info.get("venue", "Unknown Venue"),
                "toss_done":     cb_info["toss_done"],
                "toss_winner":   cb_info["toss_winner"],
                "toss_decision": cb_info["toss_decision"],
                "chasing_team":  cb_info["chasing_team"],
                "team1_xi":      xi1_json,
                "team2_xi":      xi2_json,
                "source":        "cricbuzz_json",
                "scraped_at":    datetime.utcnow().isoformat() + "Z",
            }
    except Exception as e:
        errors.append(f"CricbuzzJSON: {e}")

    # ── 3. Cricbuzz HTML scrape ───────────────────────────
    try:
        soup      = _request_soup(MATCH_URL_TEMPLATE.format(match_id=match_id))
        page_text = _clean_text(soup.get_text(" ", strip=True))
        title_txt = _clean_text(soup.title.get_text() if soup.title else "")

        # Team names from title
        team1, team2 = "Unknown", "Unknown"
        tm = re.search(
            r"(?:commentary|live-cricket-scores)\s*\|?\s*(.*?)\s+vs\.?\s+(.*?)(?:,|\|)",
            title_txt, re.I,
        )
        if tm:
            team1 = _clean_text(tm.group(1))
            team2 = _clean_text(tm.group(2))

        # Also try meta description
        if team1 == "Unknown":
            meta = soup.find("meta", {"name": "description"})
            if meta:
                mc = re.search(
                    r"([\w\s]+?)\s+vs\.?\s+([\w\s]+?)\s+(?:live|cricket|ipl)",
                    meta.get("content", ""), re.I,
                )
                if mc:
                    team1 = _clean_text(mc.group(1))
                    team2 = _clean_text(mc.group(2))

        # Venue
        venue = "Unknown Venue"
        vm = re.search(r"Venue\s*[:\-]\s*(.*?)(?:\s*[•·]\s*|\s{2,}|$)", page_text, re.I)
        if vm:
            venue = _clean_text(vm.group(1))

        # Toss — 5 pattern engine
        toss_winner, toss_decision = _parse_toss_from_text(page_text, team1, team2)
        toss_done    = bool(toss_winner and toss_decision)
        chasing_team = None
        if toss_done:
            if toss_decision == "bat":
                chasing_team = team2 if toss_winner == team1 else team1
            else:
                chasing_team = toss_winner

        # Playing XI
        team1_xi, team2_xi = _extract_playing_xi(page_text, team1, team2)

        return {
            "match_id":      int(match_id),
            "team1":         team1,
            "team2":         team2,
            "venue":         venue,
            "toss_done":     toss_done,
            "toss_winner":   toss_winner or None,
            "toss_decision": toss_decision,
            "chasing_team":  chasing_team,
            "team1_xi":      team1_xi,
            "team2_xi":      team2_xi,
            "source":        "cricbuzz_html",
            "scraped_at":    datetime.utcnow().isoformat() + "Z",
        }
    except Exception as e:
        errors.append(f"CricbuzzHTML: {e}")

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
    now   = pd.Timestamp(datetime.today().date())

    t1_id    = _safe_encode(team_encoder, team1)
    t2_id    = _safe_encode(team_encoder, team2)
    venue_id = _safe_encode(
        venue_encoder,
        venue if venue in set(venue_encoder.classes_.tolist()) else venue_encoder.classes_[0],
    )

    t1_h2h, t2_h2h             = _h2h(matches, team1, team2)
    t1_wr,  t1_l5               = _team_winrate(matches, team1)
    t2_wr,  t2_l5               = _team_winrate(matches, team2)
    t1_cp,  t1_hc               = _chase_metrics(matches, team1)
    t2_cp,  t2_hc               = _chase_metrics(matches, team2)

    season_avg  = float(get_season_avg_score(now))
    season_year = int(get_season_year(now))
    t1_ravg     = float(get_team_recent_avg_score(team1, now))
    t2_ravg     = float(get_team_recent_avg_score(team2, now))
    t1_hsr      = float(get_team_recent_high_score_rate(team1, now))
    t2_hsr      = float(get_team_recent_high_score_rate(team2, now))

    if ("venue" in venue_score_history.columns
            and "first_innings_score" in venue_score_history.columns):
        vmask     = venue_score_history["venue"] == venue
        venue_avg = (
            float(venue_score_history.loc[vmask, "first_innings_score"].mean())
            if vmask.any() else 167.0
        )
    else:
        venue_avg = 167.0
    venue_recent = float(get_venue_recent_avg_score(venue, now))

    # ── Toss — always read directly from match_info ───────
    toss_done     = bool(match_info.get("toss_done", False))
    toss_winner   = _normalize_team_name(
        match_info.get("toss_winner") or "", team_encoder
    )
    toss_decision = _clean_text(match_info.get("toss_decision") or "").lower()

    pp_def    = (
        float(sum(team_pp_eco_lookup.values()) / len(team_pp_eco_lookup))
        if team_pp_eco_lookup else 8.5
    )
    t1_pp     = float(team_pp_eco_lookup.get(team1, pp_def))
    t2_pp     = float(team_pp_eco_lookup.get(team2, pp_def))

    op_def    = {"opener_avg_batting_avg": 30.0, "opener_avg_strike_rate": 130.0}
    t1_open   = team_opener_lookup.get(team1, op_def)
    t2_open   = team_opener_lookup.get(team2, op_def)

    defaults  = _global_player_defaults(player_lookup)
    t1_stats  = _player_stats_for_xi(player_lookup, match_info.get("team1_xi", []), defaults)
    t2_stats  = _player_stats_for_xi(player_lookup, match_info.get("team2_xi", []), defaults)

    feat = {c: 0.0 for c in feature_cols}
    feat.update({
        "team1":                   t1_id,
        "team2":                   t2_id,
        "venue":                   venue_id,
        "venue_avg_first_innings": venue_avg,
        "venue_recent_avg":        venue_recent,
        "is_home_team1":           0,
        "toss_winner_is_team1":    int(toss_done and toss_winner == team1),
        "toss_decision_bat":       int(toss_done and toss_decision == "bat"),
        "h2h_team1_wins":          t1_h2h,
        "h2h_team2_wins":          t2_h2h,
        "chase_win_pct_team1":     t1_cp,
        "chase_win_pct_team2":     t2_cp,
        "high_score_chase_t1":     t1_hc,
        "high_score_chase_t2":     t2_hc,
        "winrate_team1":           t1_wr,
        "winrate_team2":           t2_wr,
        "last5_win_team1":         t1_l5,
        "last5_win_team2":         t2_l5,
        "t1_recent_avg_score":     t1_ravg,
        "t2_recent_avg_score":     t2_ravg,
        "t1_high_score_rate":      t1_hsr,
        "t2_high_score_rate":      t2_hsr,
        "t1_pp_bowling_economy":   t1_pp,
        "t2_pp_bowling_economy":   t2_pp,
        "season_avg_score":        season_avg,
        "season_year":             season_year,
        "t1_avg_batting_avg":      t1_stats["batting_avg"],
        "t1_avg_strike_rate":      t1_stats["strike_rate"],
        "t1_top3_batting_avg":     t1_stats["top3_batting_avg"],
        "t1_avg_economy":          t1_stats["economy"],
        "t1_avg_bowling_avg":      t1_stats["bowling_avg"],
        "t1_recent_strike_rate":   t1_stats["recent_strike_rate"],
        "t1_recent_economy":       t1_stats["recent_economy"],
        "t2_avg_batting_avg":      t2_stats["batting_avg"],
        "t2_avg_strike_rate":      t2_stats["strike_rate"],
        "t2_top3_batting_avg":     t2_stats["top3_batting_avg"],
        "t2_avg_economy":          t2_stats["economy"],
        "t2_avg_bowling_avg":      t2_stats["bowling_avg"],
        "t2_recent_strike_rate":   t2_stats["recent_strike_rate"],
        "t2_recent_economy":       t2_stats["recent_economy"],
        "t1_opener_batting_avg":   float(t1_open.get("opener_avg_batting_avg", 30.0)),
        "t1_opener_strike_rate":   float(t1_open.get("opener_avg_strike_rate", 130.0)),
        "t2_opener_batting_avg":   float(t2_open.get("opener_avg_batting_avg", 30.0)),
        "t2_opener_strike_rate":   float(t2_open.get("opener_avg_strike_rate", 130.0)),
        "t1_bat_vs_bowl":          _safe_div(t1_stats["batting_avg"], t2_stats["bowling_avg"], 1.0),
        "t2_bat_vs_bowl":          _safe_div(t2_stats["batting_avg"], t1_stats["bowling_avg"], 1.0),
        "t1_rolling_season_avg":   t1_ravg,
        "t2_rolling_season_avg":   t2_ravg,
    })

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)
