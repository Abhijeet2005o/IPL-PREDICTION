# scraper/espncricinfo_scraper.py — v4
#
# Major changes vs v3:
#   1. DROPPED the broken Cricbuzz JSON API paths (/api/cricket-match/{id}/info
#      and /commentary). They return 404 — they were never a real public API.
#      All Cricbuzz extraction is now HTML-based.
#   2. Cricbuzz HTML team-name extraction rewritten: reads the page <title>,
#      <h1>, and og:description meta — never the short link text (which gave
#      "BAN" / "PAK").
#   3. Toss extraction now uses Cricbuzz's *current* live containers
#      (div.cb-text-inprogress, div.cb-text-complete, div.cb-min-stts)
#      and supports the modern phrasings: "X opt to bowl", "X chose to bat",
#      "X won the toss and elected to ...".
#   4. IPL-only enforcement: get_todays_match_id() filters by IPL slug, and
#      scrape_match() validates resolved teams against the IPL alias table.
#      Non-IPL matches return {"error": "non_ipl_match", ...} so the app can
#      show a clean message instead of crashing the predictor with garbage.
#   5. build_feature_vector() unchanged in shape — same columns, same defaults.
#
# Public API (unchanged, app.py works as-is):
#   get_todays_match_id() -> int | None
#   scrape_match(match_id) -> dict
#   build_feature_vector(...) -> pd.DataFrame

import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ── Optional cricdata (ESPN Cricinfo) ─────────────────────
try:
    from cricdata import CricinfoClient
    _CRICINFO_CLIENT = CricinfoClient()
    _CRICDATA_AVAILABLE = True
except Exception:
    _CRICINFO_CLIENT = None
    _CRICDATA_AVAILABLE = False


IPL_SERIES_ID      = "1510719"
LIVE_SCORES_URL    = "https://www.cricbuzz.com/cricket-match/live-scores"
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

# Canonical set of IPL team names (used for the strict IPL filter)
_IPL_CANONICAL = set(TEAM_ALIASES.values())

# Normalise scraped variants → canonical CSV names
_TEAM_NAME_CORRECTIONS = {
    "Royal Challengers Bengaluru":  "Royal Challengers Bangalore",
    "royal challengers bengaluru":  "Royal Challengers Bangalore",
    "Royal Challengers Bengaluru ": "Royal Challengers Bangalore",
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


def _correct_team_name(name):
    name = _clean_text(name)
    if not name:
        return name
    if name in _TEAM_NAME_CORRECTIONS:
        return _TEAM_NAME_CORRECTIONS[name]
    low = name.lower()
    for wrong, right in _TEAM_NAME_CORRECTIONS.items():
        if wrong.lower() == low:
            return right
    # Expand abbreviations
    upper = name.upper()
    if upper in TEAM_ALIASES:
        return TEAM_ALIASES[upper]
    return name


def _is_ipl_team(name):
    """Strict IPL canonical-team check (after _correct_team_name)."""
    return _correct_team_name(name) in _IPL_CANONICAL


# ─────────────────────────────────────────────────────────
# ESPN CRICINFO  (still used as primary if cricdata works)
# ─────────────────────────────────────────────────────────
def _get_espn_live_match(match_id=None):
    if not _CRICDATA_AVAILABLE or _CRICINFO_CLIENT is None:
        return None
    try:
        live = _CRICINFO_CLIENT.live_matches()
        candidates = []
        for match in live:
            series      = match.get("series", {}) or {}
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
    except Exception as e:
        print(f"[ESPN] _get_espn_live_match error: {e}")
    return None


def _extract_xi_from_scorecard(scorecard):
    team_xi = {}
    try:
        team_players = (
            (scorecard or {}).get("content", {})
            .get("matchPlayers", {})
            .get("teamPlayers", [])
        )
        for entry in team_players:
            team_name = _correct_team_name(
                _clean_text(entry.get("team", {}).get("longName", ""))
            )
            players = entry.get("players", []) or []
            names   = [
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
    """ESPN match_info → (toss_winner, toss_decision)."""
    if not isinstance(info, dict):
        return "", None

    # ESPN exposes toss under "match" or as top-level "tossWinnerTeam" + "tossWinnerChoice"
    candidates = [info, info.get("match", {}) or {}, info.get("matchInfo", {}) or {}]

    tw_raw, td_raw = "", ""
    for src in candidates:
        if not isinstance(src, dict):
            continue
        tw_raw = tw_raw or _clean_text(
            (src.get("tossWinnerTeam") or {}).get("longName", "")
            if isinstance(src.get("tossWinnerTeam"), dict)
            else src.get("tossWinnerTeam", "")
        )
        td_raw = td_raw or _clean_text(src.get("tossWinnerChoice", "")).lower()

        toss_obj = src.get("toss") if isinstance(src.get("toss"), dict) else None
        if toss_obj:
            tw_raw = tw_raw or _clean_text(
                toss_obj.get("winner") or toss_obj.get("tossWinner") or ""
            )
            td_raw = td_raw or _clean_text(
                toss_obj.get("decision") or toss_obj.get("tossDecision") or ""
            ).lower()

    if "bat" in td_raw:
        td = "bat"
    elif "field" in td_raw or "bowl" in td_raw:
        td = "field"
    else:
        td = None

    tw = ""
    if tw_raw:
        for cand in [team1, team2]:
            if cand and cand.lower() == tw_raw.lower():
                tw = cand
                break
        if not tw:
            for cand in [team1, team2]:
                if cand and (cand.lower() in tw_raw.lower()
                             or tw_raw.lower() in cand.lower()):
                    tw = cand
                    break
    return tw, td


# ─────────────────────────────────────────────────────────
# CRICBUZZ HTML — TEAMS + VENUE + TOSS  (no JSON API!)
# ─────────────────────────────────────────────────────────
def _extract_teams_from_cricbuzz_html(soup):
    """
    Read full team names from Cricbuzz match HTML.
    Tries (in order): <title>, <h1>, og:description, twitter:title.
    Never returns short codes like 'BAN' / 'PAK'.
    """
    sources = []

    if soup.title and soup.title.string:
        sources.append(_clean_text(soup.title.string))

    h1 = soup.find("h1")
    if h1:
        sources.append(_clean_text(h1.get_text(" ", strip=True)))

    for meta_name in [("property", "og:title"),
                      ("property", "og:description"),
                      ("name", "twitter:title"),
                      ("name", "description")]:
        m = soup.find("meta", {meta_name[0]: meta_name[1]})
        if m and m.get("content"):
            sources.append(_clean_text(m["content"]))

    # Pattern: "<TeamA> vs <TeamB>, ..." OR "<TeamA> vs. <TeamB>"
    pat = re.compile(
        r"([A-Z][A-Za-z .'&-]{2,40}?)\s+vs\.?\s+([A-Z][A-Za-z .'&-]{2,40}?)\s*(?:,|\||-|\s+\d|$)",
        re.I,
    )
    for src in sources:
        m = pat.search(src)
        if not m:
            continue
        t1 = _correct_team_name(m.group(1))
        t2 = _correct_team_name(m.group(2))
        # Reject if either is < 4 chars (usually means we got an abbreviation)
        if len(t1) < 4 or len(t2) < 4:
            continue
        return t1, t2

    return "", ""


def _extract_venue_from_cricbuzz_html(soup):
    # Cricbuzz puts venue in: div.cb-nav-subhdr a (last anchor often venue)
    # Or in og:description: "<...> Venue: Sawai Mansingh Stadium, Jaipur"
    text = _clean_text(soup.get_text(" ", strip=True))

    m = re.search(r"Venue\s*[:\-]\s*([A-Z][A-Za-z0-9 .,'&-]+?)(?:\s*[•·|]|\s{2,}|$)", text)
    if m:
        return _clean_text(m.group(1))

    # Look in subheader anchors
    for a in soup.select("div.cb-nav-subhdr a, a[href*='cricket-grounds']"):
        t = _clean_text(a.get_text(" ", strip=True))
        if t and len(t) > 4:
            return t

    return "Unknown Venue"


# Toss text patterns — ordered most-specific → least
_TOSS_PATTERNS = [
    # "Pakistan won the toss and elected to bowl"
    re.compile(
        r"([A-Z][A-Za-z .'&-]{2,60}?)\s+won\s+the\s+toss\s+and\s+(?:elected|opted|chose|decided)\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # "Pakistan opt to bowl" / "Pakistan opted to bat"
    re.compile(
        r"([A-Z][A-Za-z .'&-]{2,60}?)\s+opt(?:ed)?\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # "Pakistan chose to bowl"
    re.compile(
        r"([A-Z][A-Za-z .'&-]{2,60}?)\s+chose\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # "Pakistan elected to bat"
    re.compile(
        r"([A-Z][A-Za-z .'&-]{2,60}?)\s+elected\s+to\s+(bat|bowl|field)",
        re.I,
    ),
    # "Toss: Pakistan, opted to bowl"
    re.compile(
        r"Toss\s*[:\-]\s*([A-Z][A-Za-z .'&-]{2,60}?)\s*[,(]\s*(?:opt(?:ed)?\s+to\s+)?(bat|bowl|field)",
        re.I,
    ),
]


def _resolve_team_from_raw(raw, team1, team2):
    """Map a raw winner string to one of the known team names."""
    if not raw:
        return ""
    raw_l = raw.lower().strip()
    # Direct alias?
    upper = raw.upper().strip()
    if upper in TEAM_ALIASES:
        full = TEAM_ALIASES[upper]
        if full in (team1, team2):
            return full

    for cand in [team1, team2]:
        if cand and cand.lower() == raw_l:
            return cand
    for cand in [team1, team2]:
        if cand and (cand.lower() in raw_l or raw_l in cand.lower()):
            return cand
    # Token overlap
    raw_tokens = set(raw_l.split())
    best, best_score = "", 0
    for cand in [team1, team2]:
        if not cand:
            continue
        score = len(raw_tokens & set(cand.lower().split()))
        if score > best_score:
            best, best_score = cand, score
    return best if best_score > 0 else ""


def _parse_toss_from_text(text, team1, team2):
    if not text:
        return "", None
    for i, pat in enumerate(_TOSS_PATTERNS):
        for m in pat.finditer(text):
            raw_winner = _clean_text(m.group(1))
            raw_dec    = m.group(2).lower()

            decision = "bat" if "bat" in raw_dec else (
                "field" if ("bowl" in raw_dec or "field" in raw_dec) else None
            )
            if not decision:
                continue

            winner = _resolve_team_from_raw(raw_winner, team1, team2)
            if winner:
                print(f"[TOSS-PATTERN] #{i+1} matched: '{winner}' → {decision}")
                return winner, decision
    return "", None


def _get_toss_from_cricbuzz_html(soup, team1, team2):
    """Find toss in the live Cricbuzz match page."""
    selectors = [
        "div.cb-text-inprogress",
        "div.cb-text-complete",
        "div.cb-text-preview",
        "div.cb-min-stts",
        "div.cb-toss-sts",
        "span.cb-toss-sts",
        "div.cb-mtch-info-itm",
        "div[class*='toss']",
        "p[class*='toss']",
        "span[class*='toss']",
    ]
    for sel in selectors:
        for el in soup.select(sel):
            text = _clean_text(el.get_text(" ", strip=True))
            if not text:
                continue
            # Only consider blocks that mention toss / opt / chose / elected
            if not re.search(r"\btoss\b|\bopt(?:ed)?\b|\bchose\b|\belected\b", text, re.I):
                continue
            print(f"[HTML-TOSS] [{sel}] → {text[:160]}")
            w, d = _parse_toss_from_text(text, team1, team2)
            if w and d:
                return w, d

    # Whole-page sentence sweep
    full = _clean_text(soup.get_text(" ", strip=True))
    for sentence in re.split(r"[.!?\n]", full):
        if re.search(r"\btoss\b|\bopt(?:ed)?\s+to\b|\bchose\s+to\b|\belected\s+to\b",
                     sentence, re.I):
            w, d = _parse_toss_from_text(sentence, team1, team2)
            if w and d:
                return w, d

    return "", None


# ─────────────────────────────────────────────────────────
# PLAYING XI from HTML
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
    t1_xi, t2_xi = [], []
    if not page_text or not team1 or not team2:
        return t1_xi, t2_xi

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

    return t1_xi[:11], t2_xi[:11]


# ─────────────────────────────────────────────────────────
# ENCODING / NORMALISATION HELPERS
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
# STAT HELPERS  (unchanged from v3)
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
    xi_norm  = [str(x).lower().strip() for x in xi if str(x).strip()]
    selected = lk[lk["player_norm"].isin(xi_norm)].reset_index(drop=True)
    if selected.empty:
        return defaults.copy()
    out = {
        "batting_avg":        float(selected["batting_avg"].mean()),
        "strike_rate":        float(selected["strike_rate"].mean()),
        "top3_batting_avg":   float(selected.nlargest(3, "batting_avg")["batting_avg"].mean()),
        "economy":            float(selected["economy"].mean()),
        "bowling_avg":        float(selected["bowling_avg"].mean()),
        "recent_strike_rate": float(selected["recent_strike_rate"].mean()),
        "recent_economy":     float(selected["recent_economy"].mean()),
    }
    for k, v in out.items():
        if pd.isna(v):
            out[k] = defaults[k]
    return out


# ─────────────────────────────────────────────────────────
# TOSS RESOLUTION  (ESPN → Cricbuzz HTML)
# ─────────────────────────────────────────────────────────
def _resolve_toss(soup, team1, team2, espn_info=None):
    print(f"\n[TOSS-RESOLVE] for '{team1}' vs '{team2}'")

    # 1. ESPN match_info
    if espn_info is not None:
        tw, td = _extract_toss_from_espn_info(espn_info, team1, team2)
        if tw and td:
            print(f"[TOSS-RESOLVE] ✓ ESPN: {tw} / {td}")
            return tw, td
        print("[TOSS-RESOLVE] ✗ ESPN had no toss")

    # 2. Cricbuzz HTML
    if soup is not None:
        tw, td = _get_toss_from_cricbuzz_html(soup, team1, team2)
        if tw and td:
            print(f"[TOSS-RESOLVE] ✓ Cricbuzz HTML: {tw} / {td}")
            return tw, td
        print("[TOSS-RESOLVE] ✗ Cricbuzz HTML had no toss")

    print("[TOSS-RESOLVE] ✗ All sources exhausted")
    return "", None


# ─────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────
def get_todays_match_id():
    """Return today's IPL match ID — strictly IPL only."""
    # 1. Try cricdata
    m = _get_espn_live_match()
    if m:
        try:
            return int(m.get("objectId"))
        except Exception:
            pass

    # 2. Cricbuzz live-scores → only IPL links
    try:
        soup  = _request_soup(LIVE_SCORES_URL)
        for link in soup.select("a[href*='/live-cricket-scores/']"):
            href = (link.get("href") or "").lower()
            if "indian-premier-league" not in href and "ipl" not in href:
                continue
            mm = re.search(r"/live-cricket-scores/(\d+)", href)
            if mm:
                return int(mm.group(1))
    except Exception as e:
        print(f"[TODAYS-MATCH] Cricbuzz scrape error: {e}")

    return None


def scrape_match(match_id):
    """
    Scrape match details for `match_id`.
    Returns a dict with at minimum {team1, team2, venue, toss_*, team1_xi, team2_xi}
    OR {"error": "..."} on failure / non-IPL match.
    """
    errors = []
    print(f"\n{'='*60}")
    print(f"[SCRAPE] match_id={match_id}")
    print(f"{'='*60}")

    espn_match, espn_info, espn_scorecard = None, None, None

    # ── 1. ESPN (cricdata) ─────────────────────────────────
    if _CRICDATA_AVAILABLE:
        try:
            print("[SCRAPE] Trying ESPN…")
            espn_match = _get_espn_live_match(match_id=match_id)
            if espn_match:
                series = espn_match.get("series", {}) or {}
                s_slug = f"{series.get('slug')}-{series.get('objectId')}"
                m_slug = f"{espn_match.get('slug')}-{espn_match.get('objectId')}"
                espn_info      = _CRICINFO_CLIENT.match_info(s_slug, m_slug)
                espn_scorecard = _CRICINFO_CLIENT.match_scorecard(s_slug, m_slug)
        except Exception as e:
            errors.append(f"ESPN: {e}")
            print(f"[SCRAPE-ESPN] ✗ {e}")

    # ── 2. Cricbuzz HTML (always fetched — used for toss & fallback) ──
    soup = None
    try:
        url = MATCH_URL_TEMPLATE.format(match_id=match_id)
        print(f"[SCRAPE] Fetching Cricbuzz HTML: {url}")
        soup = _request_soup(url)
    except Exception as e:
        errors.append(f"CricbuzzHTML fetch: {e}")
        print(f"[SCRAPE-CB-HTML] fetch failed: {e}")

    # ── Resolve teams & venue ──────────────────────────────
    team1, team2, venue = "", "", "Unknown Venue"

    if espn_match:
        try:
            teams   = espn_match.get("teams", []) or []
            t_names = [t.get("team", {}).get("longName", "") for t in teams]
            team1   = _correct_team_name(_clean_text(t_names[0] if t_names else ""))
            team2   = _correct_team_name(_clean_text(t_names[1] if len(t_names) > 1 else ""))
            if isinstance(espn_info, dict):
                venue = _clean_text(
                    (espn_info.get("venue") or {}).get("longName", "")
                ) or venue
            if venue == "Unknown Venue":
                venue = _clean_text(
                    (espn_match.get("ground") or {}).get("longName", "")
                ) or venue
        except Exception as e:
            print(f"[SCRAPE-ESPN] team/venue parse: {e}")

    if (not team1 or not team2) and soup is not None:
        ht1, ht2 = _extract_teams_from_cricbuzz_html(soup)
        team1 = team1 or ht1
        team2 = team2 or ht2
        if venue == "Unknown Venue":
            venue = _extract_venue_from_cricbuzz_html(soup)

    print(f"[SCRAPE] Resolved: '{team1}' vs '{team2}' @ '{venue}'")

    if not team1 or not team2:
        return {
            "error":      "Could not determine team names",
            "details":    " | ".join(errors) or None,
            "match_id":   int(match_id),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }

    # ── IPL strict filter ──────────────────────────────────
    if not (_is_ipl_team(team1) and _is_ipl_team(team2)):
        msg = (f"non_ipl_match: '{team1}' vs '{team2}' is not an IPL fixture. "
               f"This predictor only supports IPL matches.")
        print(f"[SCRAPE] ✗ {msg}")
        return {
            "error":      "non_ipl_match",
            "message":    msg,
            "team1":      team1,
            "team2":      team2,
            "match_id":   int(match_id),
            "scraped_at": datetime.utcnow().isoformat() + "Z",
        }

    # Canonicalise
    team1 = _correct_team_name(team1)
    team2 = _correct_team_name(team2)

    # ── Toss ───────────────────────────────────────────────
    tw, td = _resolve_toss(soup, team1, team2, espn_info=espn_info)
    toss_done    = bool(tw and td)
    chasing_team = None
    if toss_done:
        chasing_team = (
            (team2 if tw == team1 else team1) if td == "bat" else tw
        )

    # ── Playing XI ─────────────────────────────────────────
    team1_xi, team2_xi = [], []
    if espn_scorecard:
        xi_map   = _extract_xi_from_scorecard(espn_scorecard)
        team1_xi = xi_map.get(team1, [])
        team2_xi = xi_map.get(team2, [])
    if (not team1_xi or not team2_xi) and soup is not None:
        page_text = _clean_text(soup.get_text(" ", strip=True))
        h1, h2 = _extract_playing_xi(page_text, team1, team2)
        team1_xi = team1_xi or h1
        team2_xi = team2_xi or h2

    print(f"[SCRAPE] ✓ toss_done={toss_done}  XI: {len(team1_xi)}/{len(team2_xi)}")

    return {
        "match_id":      int(match_id),
        "team1":         team1,
        "team2":         team2,
        "venue":         venue,
        "toss_done":     toss_done,
        "toss_winner":   tw or None,
        "toss_decision": td,
        "chasing_team":  chasing_team,
        "team1_xi":      team1_xi,
        "team2_xi":      team2_xi,
        "source":        "espn" if espn_match else "cricbuzz_html",
        "scraped_at":    datetime.utcnow().isoformat() + "Z",
    }


# ─────────────────────────────────────────────────────────
# FEATURE VECTOR (unchanged shape — same columns, same defaults)
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

    print(f"[FEAT] '{team1}' vs '{team2}' @ '{venue}'")

    t1_id    = _safe_encode(team_encoder, team1)
    t2_id    = _safe_encode(team_encoder, team2)
    venue_id = _safe_encode(
        venue_encoder,
        venue if venue in set(venue_encoder.classes_.tolist())
        else venue_encoder.classes_[0],
    )

    t1_h2h, t2_h2h = _h2h(matches, team1, team2)
    t1_wr,  t1_l5  = _team_winrate(matches, team1)
    t2_wr,  t2_l5  = _team_winrate(matches, team2)
    t1_cp,  t1_hc  = _chase_metrics(matches, team1)
    t2_cp,  t2_hc  = _chase_metrics(matches, team2)

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

    toss_done     = bool(match_info.get("toss_done", False))
    toss_winner   = _normalize_team_name(
        match_info.get("toss_winner") or "", team_encoder
    )
    toss_decision = _clean_text(match_info.get("toss_decision") or "").lower()

    pp_def  = (float(sum(team_pp_eco_lookup.values()) / len(team_pp_eco_lookup))
               if team_pp_eco_lookup else 8.5)
    t1_pp   = float(team_pp_eco_lookup.get(team1, pp_def))
    t2_pp   = float(team_pp_eco_lookup.get(team2, pp_def))

    op_def  = {"opener_avg_batting_avg": 30.0, "opener_avg_strike_rate": 130.0}
    t1_open = team_opener_lookup.get(team1, op_def)
    t2_open = team_opener_lookup.get(team2, op_def)

    defaults = _global_player_defaults(player_lookup)
    t1_stats = _player_stats_for_xi(player_lookup, match_info.get("team1_xi", []), defaults)
    t2_stats = _player_stats_for_xi(player_lookup, match_info.get("team2_xi", []), defaults)

    # Powerplay defaults (pre-match neutral values)
    _pp_runs, _pp_sr, _pp_wkts, _pp_rr = 50.0, 130.0, 1.5, 8.3

    feat = {c: 0.0 for c in feature_cols}
    feat.update({
        "team1": t1_id, "team2": t2_id, "venue": venue_id,

        "venue_avg_first_innings": venue_avg,
        "venue_recent_avg":        venue_recent,

        "is_home_team1":        0,
        "toss_winner_is_team1": int(toss_done and toss_winner == team1),
        "toss_decision_bat":    int(toss_done and toss_decision == "bat"),

        "h2h_team1_wins": t1_h2h,
        "h2h_team2_wins": t2_h2h,

        "chase_win_pct_team1":  t1_cp,
        "chase_win_pct_team2":  t2_cp,
        "high_score_chase_t1":  t1_hc,
        "high_score_chase_t2":  t2_hc,

        "winrate_team1":   t1_wr, "winrate_team2": t2_wr,
        "last5_win_team1": t1_l5, "last5_win_team2": t2_l5,

        "t1_recent_avg_score": t1_ravg,
        "t2_recent_avg_score": t2_ravg,
        "t1_high_score_rate":  t1_hsr,
        "t2_high_score_rate":  t2_hsr,

        "t1_pp_bowling_economy": t1_pp,
        "t2_pp_bowling_economy": t2_pp,

        "season_avg_score": season_avg,
        "season_year":      season_year,

        "t1_avg_batting_avg":    t1_stats["batting_avg"],
        "t1_avg_strike_rate":    t1_stats["strike_rate"],
        "t1_top3_batting_avg":   t1_stats["top3_batting_avg"],
        "t1_avg_economy":        t1_stats["economy"],
        "t1_avg_bowling_avg":    t1_stats["bowling_avg"],
        "t1_recent_strike_rate": t1_stats["recent_strike_rate"],
        "t1_recent_economy":     t1_stats["recent_economy"],

        "t2_avg_batting_avg":    t2_stats["batting_avg"],
        "t2_avg_strike_rate":    t2_stats["strike_rate"],
        "t2_top3_batting_avg":   t2_stats["top3_batting_avg"],
        "t2_avg_economy":        t2_stats["economy"],
        "t2_avg_bowling_avg":    t2_stats["bowling_avg"],
        "t2_recent_strike_rate": t2_stats["recent_strike_rate"],
        "t2_recent_economy":     t2_stats["recent_economy"],

        "t1_opener_batting_avg": float(t1_open.get("opener_avg_batting_avg", 30.0)),
        "t1_opener_strike_rate": float(t1_open.get("opener_avg_strike_rate", 130.0)),
        "t2_opener_batting_avg": float(t2_open.get("opener_avg_batting_avg", 30.0)),
        "t2_opener_strike_rate": float(t2_open.get("opener_avg_strike_rate", 130.0)),

        "t1_bat_vs_bowl": _safe_div(t1_stats["batting_avg"], t2_stats["bowling_avg"], 1.0),
        "t2_bat_vs_bowl": _safe_div(t2_stats["batting_avg"], t1_stats["bowling_avg"], 1.0),

        "t1_rolling_season_avg": t1_ravg,
        "t2_rolling_season_avg": t2_ravg,

        # Powerplay defaults
        "team1_pp_runs":        _pp_runs,
        "team1_pp_strike_rate": _pp_sr,
        "team1_pp_wickets":     _pp_wkts,
        "team1_pp_run_rate":    _pp_rr,
        "team2_pp_runs":        _pp_runs,
        "team2_pp_strike_rate": _pp_sr,
        "team2_pp_wickets":     _pp_wkts,
        "team2_pp_run_rate":    _pp_rr,
        "pp_strength_diff":     0.0,
        "pp_run_rate_diff":     0.0,
    })

    print(f"[FEAT] built — toss_done={toss_done} toss_winner_is_team1="
          f"{feat['toss_winner_is_team1']} toss_decision_bat={feat['toss_decision_bat']}")

    return pd.DataFrame([feat], columns=feature_cols).fillna(0)
