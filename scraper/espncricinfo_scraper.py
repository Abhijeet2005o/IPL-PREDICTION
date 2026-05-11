"""
espncricinfo_scraper.py
━━━━━━━━━━━━━━━━━━━━━━
Full IPL scraper for the IPL Match Predictor app.

Sources  : Cricbuzz (primary) + ESPN Cricinfo via cricdata (secondary)
Locked to: IPL series ID 1510719 — no other cricket ever returned.

Public API (called by app.py)
──────────────────────────────
  get_todays_match_id()                               → int | None
  get_match_info(match_id)                            → dict
  get_teams_from_cricbuzz(match_id)                   → (str, str) | (None, None)
  get_toss_from_cricbuzz(match_id, t1, t2)            → (str, str|None)
  get_playing_xi_from_cricbuzz(match_id, t1, t2)      → {team: [players]}
  get_playing_xi_from_scorecard(match_id, t1, t2)     → {team: [players]}
  get_combined_xi(match_id, t1, t2)                   → {team: [players]}
  get_live_score(match_id)                            → dict | None
  get_venue(match_id)                                 → str
  validate_ipl_teams(t1, t2)                          → bool
"""

import re
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════
# ESPN / cricdata  (optional secondary source)
# ══════════════════════════════════════════════════════════════
try:
    from cricdata import CricinfoClient
    _ESPN_CLIENT   = CricinfoClient()
    ESPN_AVAILABLE = True
except Exception:
    _ESPN_CLIENT   = None
    ESPN_AVAILABLE = False


# ══════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════

IPL_SERIES_ID = "1510719"

# ── Cricbuzz URLs ─────────────────────────────────────────────
# Every URL is locked to the IPL series or a specific match.
# We never touch Cricbuzz global live feed.
CB_IPL_LIVE     = f"https://www.cricbuzz.com/cricket-match/live-scores/series/{IPL_SERIES_ID}"
CB_IPL_SCHEDULE = f"https://www.cricbuzz.com/cricket-series/{IPL_SERIES_ID}/indian-premier-league-2026/matches"
CB_MATCH        = "https://www.cricbuzz.com/live-cricket-scores/{mid}"
CB_SCORECARD    = "https://www.cricbuzz.com/live-cricket-scorecard/{mid}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ══════════════════════════════════════════════════════════════
# IPL TEAM DEFINITIONS
# ══════════════════════════════════════════════════════════════

# Hard whitelist — only these 10 franchises are ever valid
VALID_IPL_TEAMS = {
    "Chennai Super Kings",
    "Delhi Capitals",
    "Gujarat Titans",
    "Kolkata Knight Riders",
    "Lucknow Super Giants",
    "Mumbai Indians",
    "Punjab Kings",
    "Rajasthan Royals",
    "Royal Challengers Bangalore",
    "Sunrisers Hyderabad",
}

ABBR_TO_TEAM: Dict[str, str] = {
    "CSK":  "Chennai Super Kings",
    "DC":   "Delhi Capitals",
    "DD":   "Delhi Capitals",
    "GT":   "Gujarat Titans",
    "GL":   "Gujarat Lions",
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

TEAM_TO_ABBR: Dict[str, str] = {v: k for k, v in ABBR_TO_TEAM.items()}

NAME_FIXES: Dict[str, str] = {
    "Royal Challengers Bengaluru":  "Royal Challengers Bangalore",
    "royal challengers bengaluru":  "Royal Challengers Bangalore",
    "Royal Challengers Bengaluru ": "Royal Challengers Bangalore",
    "RCB Bangalore":                "Royal Challengers Bangalore",
    "Rising Pune Supergiants":      "Rising Pune Supergiant",
}

# lowercase keywords for fast page-level IPL detection
_IPL_KW = {n.lower() for n in VALID_IPL_TEAMS} | {a.lower() for a in ABBR_TO_TEAM}

# In-session Playing XI cache
KNOWN_XI: Dict[int, Dict[str, List[str]]] = {}


# ══════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════

def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _fix_name(name: str) -> str:
    name = _clean(name)
    return NAME_FIXES.get(name) or NAME_FIXES.get(name.lower()) or name


def _normalize(name: str) -> str:
    """Resolve abbreviation OR fix spelling to canonical IPL team name."""
    name = _fix_name(name)
    up = name.upper()
    if up in ABBR_TO_TEAM:
        return ABBR_TO_TEAM[up]
    for abbr, full in ABBR_TO_TEAM.items():
        if re.search(rf'\b{abbr}\b', name, re.I):
            return full
    return name


def _get(url: str, timeout: int = 20) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r


def _soup(url: str, timeout: int = 20) -> BeautifulSoup:
    return BeautifulSoup(_get(url, timeout).text, "html.parser")


def _extract_player(text: str) -> str:
    name = re.sub(r'\s*[\(\[].*?[\)\]]', '', text)
    name = re.sub(r'[†*‡]', '', name)
    return _clean(name)


def _cache_xi(match_id: int, team_xi: Dict[str, List[str]]) -> None:
    if match_id not in KNOWN_XI:
        KNOWN_XI[match_id] = {}
    KNOWN_XI[match_id].update(team_xi)


# ══════════════════════════════════════════════════════════════
# IPL VALIDATION  ← primary defence against non-IPL data
# ══════════════════════════════════════════════════════════════

def validate_ipl_teams(team1: str, team2: str) -> bool:
    """
    Returns True only if BOTH teams are active IPL franchises.
    Call this right after scraping team names.
    If False → show Streamlit error and st.stop().
    """
    ok1 = team1 in VALID_IPL_TEAMS
    ok2 = team2 in VALID_IPL_TEAMS
    if not ok1:
        print(f"[VALIDATE] ❌ Not an IPL team: '{team1}'")
    if not ok2:
        print(f"[VALIDATE] ❌ Not an IPL team: '{team2}'")
    if ok1 and ok2:
        print(f"[VALIDATE] ✅ {team1} vs {team2} — confirmed IPL.")
        return True
    return False


def _is_ipl_page(soup: BeautifulSoup) -> bool:
    """
    Returns True if a Cricbuzz page belongs to an IPL match.
    Called at the top of every scraping function.
    """
    text = soup.get_text().lower()
    if any(x in text for x in ["indian premier league", "ipl 2026", "ipl 2025"]):
        print("[IPL-CHECK] ✅ IPL confirmed via series name.")
        return True
    hits = sum(1 for kw in _IPL_KW if kw in text)
    if hits >= 2:
        print(f"[IPL-CHECK] ✅ IPL confirmed via {hits} team keyword hits.")
        return True
    print("[IPL-CHECK] ❌ Page is NOT an IPL match.")
    return False


# ══════════════════════════════════════════════════════════════
# ① AUTO MATCH ID DETECTION
# ══════════════════════════════════════════════════════════════

def get_todays_match_id() -> Optional[int]:
    """
    Main entry point called by app.py.

    Detection order:
      1. Cricbuzz IPL series live scores page  (series-locked)
      2. ESPN Cricinfo live matches            (series-filtered)
      3. Cricbuzz IPL schedule page            (most recent match)

    Returns match_id (int) or None if nothing found.
    """
    print("\n[MATCH-ID] ── Auto-detecting today's IPL match ──")
    mid = _cb_live_match_id()
    if mid:
        return mid
    mid = _espn_live_match_id()
    if mid:
        return mid
    print("[MATCH-ID] No live match found. Trying schedule fallback …")
    return _cb_schedule_match_id()


def _cb_live_match_id() -> Optional[int]:
    try:
        print(f"[CB-LIVE] {CB_IPL_LIVE}")
        s = _soup(CB_IPL_LIVE)
        for a in s.select("a[href*='/live-cricket-scores/']"):
            m = re.search(r'/live-cricket-scores/(\d+)', a.get("href", ""))
            if m:
                mid = int(m.group(1))
                print(f"[CB-LIVE] ✅ Live match ID: {mid}")
                return mid
        print("[CB-LIVE] No live IPL match at this moment.")
    except Exception as e:
        print(f"[CB-LIVE] Error: {e}")
    return None


def _espn_live_match_id() -> Optional[int]:
    if not ESPN_AVAILABLE:
        return None
    try:
        for match in _ESPN_CLIENT.live_matches():
            series    = match.get("series", {})
            series_id = str(series.get("objectId", "")).strip()
            s_name    = _clean(series.get("longName", "")).lower()
            if series_id == IPL_SERIES_ID or "indian premier league" in s_name:
                mid = match.get("objectId")
                if mid:
                    print(f"[ESPN-LIVE] ✅ Live match ID via ESPN: {mid}")
                    return int(mid)
        print("[ESPN-LIVE] No live IPL match via ESPN.")
    except Exception as e:
        print(f"[ESPN-LIVE] Error: {e}")
    return None


def _cb_schedule_match_id() -> Optional[int]:
    try:
        print(f"[CB-SCHED] {CB_IPL_SCHEDULE}")
        s = _soup(CB_IPL_SCHEDULE)
        ids = []
        for a in s.select(
            "a[href*='/live-cricket-scores/'], a[href*='/cricket-scores/']"
        ):
            m = re.search(
                r'/(?:live-cricket-scores|cricket-scores)/(\d+)',
                a.get("href", "")
            )
            if m:
                ids.append(int(m.group(1)))
        if ids:
            mid = ids[-1]
            print(f"[CB-SCHED] ✅ Latest IPL match ID: {mid}")
            return mid
        print("[CB-SCHED] No match IDs on schedule page.")
    except Exception as e:
        print(f"[CB-SCHED] Error: {e}")
    return None


# ══════════════════════════════════════════════════════════════
# ② MATCH INFO  (teams + venue + match metadata)
# ══════════════════════════════════════════════════════════════

def get_match_info(match_id: int) -> dict:
    """
    Returns:
      {team1, team2, venue, city, match_type, match_number, date_str}

    Tries Cricbuzz first, fills missing fields from ESPN.
    Returns empty strings for any field that cannot be found.
    """
    print(f"\n[MATCH-INFO] match_id={match_id}")
    info = _cb_match_info(match_id)

    # Fill any missing fields from ESPN
    if not info.get("team1") or not info.get("team2"):
        espn = _espn_match_info(match_id)
        for k, v in espn.items():
            if not info.get(k) and v:
                info[k] = v

    print(f"[MATCH-INFO] → {info}")
    return info


def _cb_match_info(match_id: int) -> dict:
    base = {
        "team1": "", "team2": "", "venue": "",
        "city": "", "match_type": "T20",
        "match_number": "", "date_str": ""
    }
    try:
        url  = CB_MATCH.format(mid=match_id)
        print(f"[CB-INFO] {url}")
        resp = _get(url)
        s    = BeautifulSoup(resp.text, "html.parser")
        page = resp.text

        if not _is_ipl_page(s):
            print(f"[CB-INFO] ❌ Not IPL.")
            return base

        # ── Team names ────────────────────────────────────────
        teams: List[str] = []

        for sel in [
            ".cb-nav-main .cb-col-50",
            ".cb-minfo-tm-nm",
            "a[href*='/cricket-team/']",
        ]:
            for el in s.select(sel):
                name = _normalize(_clean(el.get_text()))
                if name in VALID_IPL_TEAMS and name not in teams:
                    teams.append(name)
            if len(teams) >= 2:
                break

        # Fallback: title tag  "CSK vs MI, 42nd Match …"
        if len(teams) < 2 and s.title:
            title = _clean(s.title.get_text())
            for part in re.split(r'\s+vs\.?\s+', title, flags=re.I):
                name = _normalize(_clean(re.split(r'[,\-\|]', part)[0]))
                if name in VALID_IPL_TEAMS and name not in teams:
                    teams.append(name)

        if len(teams) >= 2:
            base["team1"], base["team2"] = teams[0], teams[1]

        # ── Match info rows ───────────────────────────────────
        for item in s.select(".cb-mtch-info-itm"):
            lbl_el = item.select_one(".cb-col-27")
            val_el = item.select_one(".cb-col-73")
            if not (lbl_el and val_el):
                continue
            lbl = _clean(lbl_el.get_text()).lower()
            val = _clean(val_el.get_text())

            if "venue" in lbl or "ground" in lbl:
                base["venue"] = val
            elif "city" in lbl:
                base["city"] = val
            elif "match" in lbl and "number" in lbl:
                base["match_number"] = val
            elif "date" in lbl:
                base["date_str"] = val

        # Venue regex fallback
        if not base["venue"]:
            vm = re.search(
                r'(?:at|venue)[:\s]+([A-Z][A-Za-z\s,]+(?:Stadium|Ground)[A-Za-z\s,]*)',
                page, re.I
            )
            if vm:
                base["venue"] = _clean(vm.group(1))

        # Derive city from venue if missing
        if base["venue"] and not base["city"]:
            parts = base["venue"].split(",")
            if len(parts) >= 2:
                base["city"] = _clean(parts[-1])

    except Exception as e:
        print(f"[CB-INFO] Error: {e}")
    return base


def _espn_match_info(match_id: int) -> dict:
    out = {"team1": "", "team2": "", "venue": "", "city": ""}
    if not ESPN_AVAILABLE:
        return out
    try:
        info  = _ESPN_CLIENT.match_info(match_id)
        teams = info.get("teams", [])
        names = [_normalize(_clean(t.get("longName", ""))) for t in teams]
        names = [n for n in names if n in VALID_IPL_TEAMS]
        if len(names) >= 2:
            out["team1"], out["team2"] = names[0], names[1]
        g = info.get("ground", {})
        out["venue"] = _clean(g.get("longName", ""))
        out["city"]  = _clean(g.get("country", {}).get("name", ""))
    except Exception as e:
        print(f"[ESPN-INFO] Error: {e}")
    return out


def get_teams_from_cricbuzz(match_id: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Convenience wrapper: returns (team1, team2) validated as IPL.
    Returns (None, None) if validation fails.
    """
    info = get_match_info(match_id)
    t1   = info.get("team1", "")
    t2   = info.get("team2", "")
    if t1 and t2 and validate_ipl_teams(t1, t2):
        return t1, t2
    return None, None


# ══════════════════════════════════════════════════════════════
# ③ TOSS DETECTION
# ══════════════════════════════════════════════════════════════

def get_toss_from_cricbuzz(
    match_id: int, team1: str, team2: str
) -> Tuple[str, Optional[str]]:
    """
    Returns (winner_full_name, 'bat'|'field').
    Returns ("", None) if toss not yet done or undetectable.

    Detection order:
      1. "XYZ opt to bat/field" text on Cricbuzz match page
      2. Toss row in match info section
      3. ESPN Cricinfo toss data
    """
    print(f"\n[TOSS] match_id={match_id}")
    w, d = _cb_toss(match_id, team1, team2)
    if w:
        return w, d
    w, d = _espn_toss(match_id, team1, team2)
    if w:
        return w, d
    print("[TOSS] ⚠️ Toss not yet detected.")
    return "", None


def _cb_toss(
    match_id: int, team1: str, team2: str
) -> Tuple[str, Optional[str]]:
    try:
        url  = CB_MATCH.format(mid=match_id)
        print(f"[CB-TOSS] {url}")
        resp = _get(url)
        s    = BeautifulSoup(resp.text, "html.parser")
        page = resp.text

        if not _is_ipl_page(s):
            return "", None

        # Pattern 1 — "CSK opt to bat" / "MI opted to field"
        for pat in [
            r'([A-Z]{2,5})\s+opt(?:ed)?\s+to\s+(bat|bowl|field)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,4})\s+opt(?:ed)?\s+to\s+(bat|bowl|field)',
        ]:
            for m in re.finditer(pat, page, re.I):
                raw      = m.group(1).strip()
                dec_raw  = m.group(2).lower()
                decision = "bat" if "bat" in dec_raw else "field"
                winner   = _resolve_team(raw, team1, team2)
                if winner:
                    print(f"[CB-TOSS] ✅ Pattern: {winner} → {decision}")
                    return winner, decision

        # Pattern 2 — "Toss:" row in match info
        for item in s.select(".cb-mtch-info-itm"):
            lbl_el = item.select_one(".cb-col-27")
            val_el = item.select_one(".cb-col-73")
            if not (lbl_el and val_el):
                continue
            if "toss" not in _clean(lbl_el.get_text()).lower():
                continue
            val    = _clean(val_el.get_text())
            # e.g. "Mumbai Indians, elected to bat first"
            parts  = val.split(",")
            t_part = _clean(parts[0]) if parts else ""
            v_low  = val.lower()
            winner = _resolve_team(t_part, team1, team2)
            decision = (
                "bat"   if "bat"                  in v_low else
                "field" if ("field" in v_low or "bowl" in v_low) else
                None
            )
            if winner and decision:
                print(f"[CB-TOSS] ✅ Info row: {winner} → {decision}")
                return winner, decision

    except Exception as e:
        print(f"[CB-TOSS] Error: {e}")
    return "", None


def _espn_toss(
    match_id: int, team1: str, team2: str
) -> Tuple[str, Optional[str]]:
    if not ESPN_AVAILABLE:
        return "", None
    try:
        for match in _ESPN_CLIENT.live_matches():
            if str(match.get("objectId", "")) != str(match_id):
                continue
            toss = (
                match.get("toss") or
                match.get("tossResults") or
                match.get("tossResult") or {}
            )
            if not toss:
                break
            tw  = _clean(toss.get("winner_team") or toss.get("tossWinner") or "")
            td  = _clean(toss.get("decision") or toss.get("tossDecision") or "").lower()
            w   = _resolve_team(tw, team1, team2)
            d   = "bat" if "bat" in td else ("field" if ("field" in td or "bowl" in td) else None)
            if w and d:
                print(f"[ESPN-TOSS] ✅ {w} → {d}")
                return w, d
    except Exception as e:
        print(f"[ESPN-TOSS] Error: {e}")
    return "", None


def _resolve_team(raw: str, team1: str, team2: str) -> Optional[str]:
    """Map a raw string (abbr / partial name) to one of the two match teams."""
    if not raw:
        return None
    raw_up = raw.upper().strip()
    # Direct abbreviation
    if raw_up in ABBR_TO_TEAM:
        full = ABBR_TO_TEAM[raw_up]
        for t in [team1, team2]:
            if t and t.lower() == full.lower():
                return t
    # Substring match
    for t in [team1, team2]:
        if not t:
            continue
        if (t.lower() in raw.lower() or
                raw.lower() in t.lower() or
                TEAM_TO_ABBR.get(t, "").lower() == raw.lower()):
            return t
    return None


# ══════════════════════════════════════════════════════════════
# ④ PLAYING XI
# ══════════════════════════════════════════════════════════════

def get_playing_xi_from_cricbuzz(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz match page using 5 methods.
    Stops as soon as both teams' XIs are found.
    ✅ IPL page guard at entry.
    """
    print(f"\n[CB-XI] match_id={match_id}")
    team_xi: Dict[str, List[str]] = {}

    try:
        url  = CB_MATCH.format(mid=match_id)
        resp = _get(url)
        s    = BeautifulSoup(resp.text, "html.parser")
        page = resp.text

        if not _is_ipl_page(s):
            print("[CB-XI] ❌ Not IPL. Aborting.")
            return {}

        # M1: .cb-mtch-info-itm rows labelled "squad"/"playing"/"xi"
        for item in s.select(".cb-mtch-info-itm"):
            lbl = item.select_one(".cb-col-27")
            val = item.select_one(".cb-col-73")
            if not (lbl and val):
                continue
            lbl_text = _clean(lbl.get_text()).lower()
            if not any(x in lbl_text for x in ["squad", "playing", "xi", "team"]):
                continue
            ct = None
            for t in [team1, team2]:
                if t and t.lower() in lbl_text:
                    ct = t
                    break
            if ct and ct not in team_xi:
                names = [_extract_player(a.get_text()) for a in val.select("a")]
                names = [n for n in names if n and len(n) > 2][:11]
                if names:
                    team_xi[ct] = names
                    print(f"[CB-XI] M1 ✅ {ct}: {len(names)}")
        if len(team_xi) >= 2:
            _cache_xi(match_id, team_xi)
            return team_xi

        # M2: Section headers containing "Playing XI"
        for hdr in s.select(".cb-col-100.cb-font-14, .cb-minfo-tm-nm"):
            ht = _clean(hdr.get_text())
            ct = None
            for t in [team1, team2]:
                if not t:
                    continue
                abbr = TEAM_TO_ABBR.get(t, "").lower()
                if (t.lower() in ht.lower() or
                        (abbr and abbr in ht.lower())):
                    if "playing" in ht.lower() or "xi" in ht.lower():
                        ct = t
                        break
            if ct and ct not in team_xi:
                parent = hdr.find_parent()
                if parent:
                    links = (
                        parent.select("a[href*='/profiles/']") or
                        parent.select("a[href*='/cricket-player/']") or
                        parent.select("a")
                    )
                    names = [_extract_player(a.get_text()) for a in links]
                    names = [n for n in names if n and len(n) > 2][:11]
                    if names:
                        team_xi[ct] = names
                        print(f"[CB-XI] M2 ✅ {ct}: {len(names)}")
        if len(team_xi) >= 2:
            _cache_xi(match_id, team_xi)
            return team_xi

        # M3: Regex on raw page text
        for pat in [
            r'([A-Za-z ]+?)\s*\(?Playing\s*XI\)?\s*:?\s*([A-Za-z ,\.]+?)(?=\n|<|[A-Z][a-z]+ XI)',
            r'([A-Za-z ]+?)\s+XI\s*:?\s*([A-Za-z ,\.]+?)(?=\n|<)',
        ]:
            for m in re.finditer(pat, page, re.I | re.M):
                t_raw = _clean(m.group(1))
                p_raw = m.group(2)
                ct    = None
                for t in [team1, team2]:
                    if t and (t.lower() in t_raw.lower() or t_raw.lower() in t.lower()):
                        ct = t
                        break
                if ct and ct not in team_xi:
                    players = [_extract_player(p) for p in p_raw.split(",")]
                    players = [p for p in players if p and len(p) > 2][:11]
                    if len(players) >= 5:
                        team_xi[ct] = players
                        print(f"[CB-XI] M3 ✅ {ct}: {len(players)}")
        if len(team_xi) >= 2:
            _cache_xi(match_id, team_xi)
            return team_xi

        # M4: .cb-play11 / .cb-minfo-tm-plyr divs
        for sec in s.select(".cb-play11-lft-col, .cb-minfo-tm-plyr, .cb-play11-prfl"):
            parent = sec.find_parent(class_=re.compile(r'cb-col'))
            if not parent:
                continue
            ctx = _clean(parent.get_text())
            ct  = None
            for t in [team1, team2]:
                if t and t.lower() in ctx.lower():
                    ct = t
                    break
            if ct and ct not in team_xi:
                names = [_extract_player(a.get_text()) for a in sec.select("a")]
                names = [n for n in names if n and len(n) > 2][:11]
                if len(names) >= 5:
                    team_xi[ct] = names
                    print(f"[CB-XI] M4 ✅ {ct}: {len(names)}")
        if len(team_xi) >= 2:
            _cache_xi(match_id, team_xi)
            return team_xi

        # M5: "opt to" + inline player list
        for m in re.finditer(
            r'([A-Z]{2,5})\s+opt\s+to\s+(?:bat|bowl|field)[^\n]*?'
            r'\1\s*[:\-]?\s*([A-Za-z ,\.]{20,})',
            page, re.I
        ):
            abbr = m.group(1).upper()
            if abbr not in ABBR_TO_TEAM:
                continue
            full = ABBR_TO_TEAM[abbr]
            ct   = None
            for t in [team1, team2]:
                if t and t.lower() == full.lower():
                    ct = t
                    break
            if ct and ct not in team_xi:
                players = [_extract_player(p) for p in m.group(2).split(",")]
                players = [p for p in players if p and len(p) > 2][:11]
                if len(players) >= 5:
                    team_xi[ct] = players
                    print(f"[CB-XI] M5 ✅ {ct}: {len(players)}")

    except Exception as e:
        print(f"[CB-XI] Error: {e}")

    if team_xi:
        _cache_xi(match_id, team_xi)
    return team_xi


def get_playing_xi_from_scorecard(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """
    Extract Playing XI from Cricbuzz scorecard page.
    Best used when the match is in progress or completed.
    ✅ IPL page guard at entry.
    """
    print(f"\n[CB-SCORE] match_id={match_id}")
    team_xi: Dict[str, List[str]] = {}

    try:
        url = CB_SCORECARD.format(mid=match_id)
        s   = _soup(url)

        if not _is_ipl_page(s):
            print("[CB-SCORE] ❌ Not IPL. Aborting.")
            return {}

        current_team: Optional[str] = None

        for block in s.select(".cb-col-100.cb-ltst-wgt-hdr"):
            bt = _clean(block.get_text())
            for t in [team1, team2]:
                if not t:
                    continue
                abbr = TEAM_TO_ABBR.get(t, "").lower()
                if (t.lower() in bt.lower() or
                        (abbr and abbr in bt.lower())):
                    if "innings" in bt.lower():
                        current_team = t
                        break

            if current_team and current_team not in team_xi:
                parent = block.find_parent()
                if parent:
                    names: List[str] = []
                    for row in parent.select(".cb-col-100.cb-scrd-itms"):
                        lnk = row.select_one("a.cb-text-link")
                        if lnk:
                            n = _extract_player(lnk.get_text())
                            if n and len(n) > 2 and n not in names:
                                names.append(n)
                    if names:
                        team_xi[current_team] = names[:11]
                        print(f"[CB-SCORE] ✅ {current_team}: {len(names)}")

        # Supplement bowlers → fielding team
        for row in s.select(".cb-col-100.cb-scrd-itms"):
            bd = row.select_one(".cb-col-40")
            if not bd:
                continue
            lnk = bd.select_one("a")
            if not lnk:
                continue
            name = _extract_player(lnk.get_text())
            if not (name and len(name) > 2):
                continue
            for t in [team1, team2]:
                if t and t in team_xi:
                    other = team2 if t == team1 else team1
                    team_xi.setdefault(other, [])
                    if name not in team_xi[other] and len(team_xi[other]) < 11:
                        team_xi[other].append(name)

    except Exception as e:
        print(f"[CB-SCORE] Error: {e}")

    if team_xi:
        _cache_xi(match_id, team_xi)
    return team_xi


def _get_xi_espn(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """Extract Playing XI via ESPN Cricinfo cricdata library."""
    team_xi: Dict[str, List[str]] = {}
    if not ESPN_AVAILABLE:
        return team_xi
    try:
        sc = _ESPN_CLIENT.scorecard(match_id)
        for entry in (
            sc.get("content", {})
              .get("matchPlayers", {})
              .get("teamPlayers", [])
        ):
            t_name = _fix_name(_clean(entry.get("team", {}).get("longName", "")))
            if t_name not in VALID_IPL_TEAMS:
                print(f"[ESPN-XI] Skipping non-IPL: {t_name}")
                continue
            players = entry.get("players", []) or []
            names   = [_clean(p.get("player", {}).get("longName", "")) for p in players]
            names   = [n for n in names if n]
            if t_name and names:
                team_xi[t_name] = names[:11]
                print(f"[ESPN-XI] ✅ {t_name}: {len(names)}")
    except Exception as e:
        print(f"[ESPN-XI] Error: {e}")
    return team_xi


def get_combined_xi(
    match_id: int, team1: str, team2: str
) -> Dict[str, List[str]]:
    """
    Master XI resolver — tries all sources until both XIs are found.

    Order:
      1. In-session cache (KNOWN_XI)
      2. Cricbuzz match page  (5 internal methods)
      3. Cricbuzz scorecard
      4. ESPN Cricinfo

    Returns whatever is available — may be partial if XI not yet announced.
    """
    cached = KNOWN_XI.get(match_id, {})
    if len(cached) >= 2:
        return cached

    xi = get_playing_xi_from_cricbuzz(match_id, team1, team2)

    if len(xi) < 2:
        for t, p in get_playing_xi_from_scorecard(match_id, team1, team2).items():
            xi.setdefault(t, p)

    if len(xi) < 2:
        for t, p in _get_xi_espn(match_id, team1, team2).items():
            xi.setdefault(t, p)

    if xi:
        _cache_xi(match_id, xi)
    return xi


# ══════════════════════════════════════════════════════════════
# ⑤ LIVE SCORE
# ══════════════════════════════════════════════════════════════

def get_live_score(match_id: int) -> Optional[dict]:
    """
    Returns current live score as:
      {
        batting_team,  bowling_team,
        score,         overs,        run_rate,
        target,        required_rate,
        status,        innings        (1 or 2)
      }
    Returns None if the match hasn't started or data is unavailable.
    """
    print(f"\n[LIVE-SCORE] match_id={match_id}")
    try:
        url  = CB_MATCH.format(mid=match_id)
        resp = _get(url)
        s    = BeautifulSoup(resp.text, "html.parser")

        if not _is_ipl_page(s):
            return None

        out: dict = {
            "batting_team":  "",
            "bowling_team":  "",
            "score":         "",
            "overs":         "",
            "run_rate":      "",
            "target":        None,
            "required_rate": None,
            "status":        "",
            "innings":       1,
        }

        # Score + overs
        for sel in [".cb-lv-scrs-col", ".cb-scrs-wrp", ".cb-min-bat-rw"]:
            el = s.select_one(sel)
            if el:
                t = _clean(el.get_text())
                m = re.search(r'(\d+)/(\d+)\s*\(?([\d\.]+)\s*ov', t, re.I)
                if m:
                    out["score"] = f"{m.group(1)}/{m.group(2)}"
                    out["overs"] = m.group(3)
                    break

        # Run rate
        for el in s.select(".cb-col-8, .cb-font-12"):
            t = _clean(el.get_text()).lower()
            if "crr" in t or "run rate" in t:
                m = re.search(r'[\d\.]+', t)
                if m:
                    out["run_rate"] = m.group()
                    break

        # Target / required rate
        for el in s.select(".cb-lv-scrs-col, .cb-col"):
            t = _clean(el.get_text()).lower()
            if "target" in t:
                m = re.search(r'target[:\s]+(\d+)', t, re.I)
                if m:
                    out["target"]  = m.group(1)
                    out["innings"] = 2
            if "rrr" in t or "req" in t:
                m = re.search(r'[\d\.]+', t)
                if m:
                    out["required_rate"] = m.group()

        # Status line
        for sel in [".cb-lv-status-in-nm", ".cb-text-complete", ".cb-lv-scrs-col"]:
            el = s.select_one(sel)
            if el:
                txt = _clean(el.get_text())
                if len(txt) > 5:
                    out["status"] = txt
                    break

        print(f"[LIVE-SCORE] {out}")
        return out if out["score"] else None

    except Exception as e:
        print(f"[LIVE-SCORE] Error: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# ⑥ VENUE
# ══════════════════════════════════════════════════════════════

def get_venue(match_id: int) -> str:
    """Returns the venue string for a given match."""
    return get_match_info(match_id).get("venue", "")


# ══════════════════════════════════════════════════════════════
# SELF-TEST  →  python scraper/espncricinfo_scraper.py
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("IPL Scraper — self test")
    print("=" * 60)

    mid = get_todays_match_id()
    print(f"\n🏏 Today's match ID : {mid}")

    if mid:
        info = get_match_info(mid)
        t1   = info.get("team1", "")
        t2   = info.get("team2", "")
        print(f"   Teams   : {t1} vs {t2}")
        print(f"   Venue   : {info.get('venue', '')}")
        print(f"   Date    : {info.get('date_str', '')}")

        if t1 and t2:
            tw, td = get_toss_from_cricbuzz(mid, t1, t2)
            print(f"   Toss    : {tw or '(not yet)'} → {td or '-'}")

            xi = get_combined_xi(mid, t1, t2)
            for team, players in xi.items():
                print(f"   XI {team[:3]} : {', '.join(players)}")

            score = get_live_score(mid)
            if score:
                print(
                    f"   Score   : {score['score']} "
                    f"({score['overs']} ov) | {score['status']}"
                )
            else:
                print("   Score   : match not started / data unavailable")

    print("\nDone.")
