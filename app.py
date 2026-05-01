# app.py
# ─────────────────────────────────────────────────────────
# Location: ipl-prediction/app.py
# Run with: cd ipl-prediction && streamlit run app.py
# ─────────────────────────────────────────────────────────

import streamlit as st
import pandas as pd
import numpy as np
import joblib
import pickle
import sys
import os
from datetime import datetime

sys.path.append(os.path.abspath(r'C:\Users\black\ipl-prediction'))

from scraper import espncricinfo_scraper
from scraper.espncricinfo_scraper import (
    get_todays_match_id,
    scrape_match,
    build_feature_vector,
    IPL_SERIES_ID,
)

# ─────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPL Match Predictor",
    page_icon="🏏",
    layout="wide",
)

# ─────────────────────────────────────────────────────────
# CSS
# ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Inter:wght@400;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.main-title {
    text-align: center;
    font-family: 'Bebas Neue', sans-serif;
    font-size: 3.4rem;
    letter-spacing: 0.14em;
    color: #0f1b4c;
    margin-bottom: 0.15rem;
}
.subtitle {
    text-align: center;
    color: #888;
    font-size: 0.95rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin-bottom: 1.8rem;
}
.pred-box {
    background: linear-gradient(135deg, #0f1b4c 0%, #1e3a8a 100%);
    color: white;
    padding: 2.5rem;
    border-radius: 20px;
    text-align: center;
    margin: 1.5rem 0;
    box-shadow: 0 8px 32px rgba(15,27,76,0.3);
}
.winner-name {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 3rem;
    letter-spacing: 0.1em;
    margin: 0.4rem 0;
}
.badge {
    display: inline-block;
    padding: 0.3rem 1.1rem;
    border-radius: 20px;
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 1rem;
}
.badge-live  { background: #dcfce7; color: #166534; }
.badge-pre   { background: #fef9c3; color: #854d0e; }
.badge-error { background: #fee2e2; color: #991b1b; }

.form-card {
    background: #f8fafc;
    border-left: 4px solid #0f1b4c;
    padding: 0.9rem 1.1rem;
    border-radius: 8px;
    margin: 0.3rem 0;
    line-height: 1.6;
}
.player-tag {
    display: inline-block;
    background: #eff6ff;
    color: #1d4ed8;
    border: 1px solid #bfdbfe;
    padding: 0.2rem 0.65rem;
    border-radius: 20px;
    font-size: 0.8rem;
    margin: 0.18rem;
}
.sh {
    font-family: 'Bebas Neue', sans-serif;
    font-size: 1.35rem;
    letter-spacing: 0.09em;
    color: #0f1b4c;
    border-bottom: 2px solid #e5e7eb;
    padding-bottom: 4px;
    margin: 1.3rem 0 0.6rem;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# LOAD MODELS + DATA  (cached — runs once)
# ─────────────────────────────────────────────────────────
@st.cache_resource
def load_everything():
    winner_model = joblib.load('models/winner_model.pkl')
    score_model  = joblib.load('models/score_model.pkl')
    opener_model = joblib.load('models/opener_model.pkl')
    second_model = joblib.load('models/second_innings_model.pkl')

    team_encoder  = joblib.load('models/team_encoder.pkl')
    venue_encoder = joblib.load('models/venue_encoder.pkl')

    player_lookup = pd.read_csv('player_stats/player_lookup.csv')

    with open('data/feature_cols.pkl', 'rb') as f:
        feature_cols = pickle.load(f)

    m_raw = pd.read_csv('data/all_ipl_matches_data.csv')
    t_raw = pd.read_csv('data/all_teams_data.csv')
    t_map = dict(zip(t_raw['team_id'], t_raw['team_name']))
    m_raw['team1']  = m_raw['team1'].map(t_map)
    m_raw['team2']  = m_raw['team2'].map(t_map)
    m_raw['winner'] = m_raw['match_winner'].map(t_map)
    m_raw.rename(columns={'match_id': 'id'}, inplace=True)
    m_clean = m_raw[m_raw['result'] == 'win'].reset_index(drop=True)
    m_clean['match_date'] = pd.to_datetime(m_clean['match_date'])

    vsh = pd.read_csv('data/venue_score_history.csv')
    vsh['match_date'] = pd.to_datetime(vsh['match_date'])

    tsl = pd.read_csv('data/team_scores_long.csv')
    tsl['match_date'] = pd.to_datetime(tsl['match_date'])

    pp_df = pd.read_csv('data/team_pp_eco.csv')
    pp_eco = dict(zip(pp_df['team_name'], pp_df['avg_pp_economy']))

    op_df = pd.read_csv('data/team_opener_lookup.csv')
    op_lkp = {
        row['team_name']: {
            'opener_avg_batting_avg': float(row['opener_avg_batting_avg']),
            'opener_avg_strike_rate': float(row['opener_avg_strike_rate']),
        }
        for _, row in op_df.iterrows()
    }

    return (winner_model, score_model, opener_model, second_model,
            team_encoder, venue_encoder, player_lookup, feature_cols,
            m_clean, vsh, tsl, pp_eco, op_lkp)


(winner_model, score_model, opener_model, second_model,
 team_encoder, venue_encoder, player_lookup, feature_cols,
 matches, venue_score_history, team_scores_long,
 team_pp_eco_lookup, team_oppener_lookup) = load_everything()


# ─────────────────────────────────────────────────────────
# STAT HELPERS
# ─────────────────────────────────────────────────────────

def get_team_recent_avg_score(team, current_date, n=5):
    past = team_scores_long[
        (team_scores_long['team'] == team) &
        (team_scores_long['match_date'] < current_date)
    ].tail(n)
    return float(past['first_innings_score'].mean()) if len(past) else 167.0


def get_venue_recent_avg_score(venue, current_date, n=15):
    past = venue_score_history[
        (venue_score_history['venue'] == venue) &
        (venue_score_history['match_date'] < current_date)
    ].tail(n)
    if len(past):
        return float(past['first_innings_score'].mean())
    base = venue.split(',')[0].strip()
    past2 = venue_score_history[
        venue_score_history['venue'].str.contains(base, case=False, na=False) &
        (venue_score_history['match_date'] < current_date)
    ].tail(n)
    return float(past2['first_innings_score'].mean()) if len(past2) else 167.0


def get_season_avg_score(current_date):
    yr = current_date.year
    s = team_scores_long[
        (team_scores_long['match_date'].dt.year == yr) &
        (team_scores_long['match_date'] < current_date)
    ]
    if not len(s):
        prev = team_scores_long[team_scores_long['match_date'].dt.year == yr - 1]
        return float(prev['first_innings_score'].mean()) if len(prev) else 180.0
    return float(s['first_innings_score'].mean())


def get_season_year(current_date):
    return int(current_date.year)


def get_team_recent_high_score_rate(team, current_date, n=10):
    threshold = float(team_scores_long['first_innings_score'].quantile(0.75))
    past = team_scores_long[
        (team_scores_long['team'] == team) &
        (team_scores_long['match_date'] < current_date)
    ].tail(n)
    return float((past['first_innings_score'] >= threshold).mean()) if len(past) else 0.3


# ─────────────────────────────────────────────────────────
# SIDEBAR — manual controls
# ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏏 IPL Predictor")
    st.markdown(f"**Series ID:** `{IPL_SERIES_ID}`")
    st.markdown("---")

    st.markdown("### 🔧 Manual Override")
    st.caption(
        "Auto-detection failed? Paste the **Match ID** from the "
        "ESPNcricinfo URL:\n\n"
        "`/series/ipl-2026-1510719/`**`rr-vs-dc-43rd-match-`**`1529286`"
        "`/live-cricket-score`"
    )
    manual_match_id = st.text_input(
        "Match ID", value="", placeholder="e.g. 1529286"
    )
    manual_series_id = st.text_input(
        "Series ID override", value="", placeholder="default: 1510719"
    )
    st.markdown("---")
    st.markdown(
        "📅 [Today's schedule]"
        "(https://www.espncricinfo.com/series/ipl-2026-1510719/"
        "match-schedule-fixtures-and-results)"
    )


# ─────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────
st.markdown('<div class="main-title">🏏 IPL MATCH PREDICTOR</div>',
            unsafe_allow_html=True)
st.markdown('<div class="subtitle">AI · Automatic · IPL 2026</div>',
            unsafe_allow_html=True)
st.markdown("---")

col1, col2, col3 = st.columns([2, 1, 2])
with col2:
    go = st.button("🎯 GET PREDICTION", use_container_width=True, type="primary")


# ─────────────────────────────────────────────────────────
# PREDICTION FLOW
# ─────────────────────────────────────────────────────────
if go:
    today_ts = pd.Timestamp(datetime.today().date())

    # Apply manual series ID override if given
    if manual_series_id.strip():
        espncricinfo_scraper.IPL_SERIES_ID = manual_series_id.strip()
        st.info(f"🔧 Series ID overridden to: `{manual_series_id.strip()}`")

    # ── Step 1: Match ID ──────────────────────────────────
    if manual_match_id.strip():
        match_id = int(manual_match_id.strip())
        st.success(f"🔧 Using manually entered match ID: `{match_id}`")
    else:
        with st.spinner("🔍 Finding today's match…"):
            match_id = get_todays_match_id()

    if match_id is None:
        st.markdown('<div class="badge badge-error">❌ No Match Found</div>',
                    unsafe_allow_html=True)
        st.error(
            "**Auto-detection found no match today.**\n\n"
            "**Fix — 3 options (pick any):**\n\n"
            "1. **Sidebar override** → paste the Match ID from the ESPNcricinfo URL\n"
            "2. Set env variable and restart: `set IPL_SERIES_ID=1510719`\n"
            "3. Wait — the match page may not be live yet (try 1-2 hrs before start)"
        )
        st.stop()

    st.caption(f"📌 Match ID: `{match_id}` · Series: `{espncricinfo_scraper.IPL_SERIES_ID}`")

    # ── Step 2: Match Info ────────────────────────────────
    with st.spinner("📡 Fetching match data…"):
        match_info = scrape_match(match_id)

    with st.expander("🐛 Debug — raw scraper output"):
        st.json(match_info or {})

    if not match_info or not match_info.get("team1"):
        st.markdown('<div class="badge badge-error">❌ Data Fetch Failed</div>',
                    unsafe_allow_html=True)
        st.error(
            "**Could not read match data.**\n\n"
            "Try:\n"
            "- Wait a few minutes and click again (page may not be live)\n"
            "- Confirm the Match ID is correct via the sidebar"
        )
        st.stop()

    # ── Step 3: Predict ───────────────────────────────────
    feats = build_feature_vector(
        match_info, player_lookup, matches,
        team_encoder, venue_encoder, venue_score_history,
        team_pp_eco_lookup, team_oppener_lookup,
        get_team_recent_avg_score, get_season_avg_score,
        get_season_year, get_venue_recent_avg_score,
        get_team_recent_high_score_rate, feature_cols,
    )

    w_pred  = winner_model.predict(feats)[0]
    w_prob  = winner_model.predict_proba(feats)[0]
    s_pred  = score_model.predict(feats)[0]
    op_pred = opener_model.predict(feats)[0]

    pred_winner = match_info['team1'] if w_pred == 1 else match_info['team2']
    win_prob    = w_prob[int(w_pred)] * 100
    toss_done   = match_info.get('toss_done', bool(match_info.get('toss_winner')))

    # ── Badge ─────────────────────────────────────────────
    if toss_done:
        st.markdown('<div class="badge badge-live">🟢 Post-Toss — Toss Factored In</div>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<div class="badge badge-pre">🟡 Pre-Toss — Historical Estimate</div>',
                    unsafe_allow_html=True)

    # ── Match overview ────────────────────────────────────
    st.markdown('<div class="sh">📊 Match Overview</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    c1.metric("🏟️ Venue",  match_info.get('venue', 'N/A')[:45])
    c2.metric("🔵 Team 1", match_info.get('team1', 'N/A'))
    c3.metric("🔴 Team 2", match_info.get('team2', 'N/A'))

    # ── Toss ──────────────────────────────────────────────
    if toss_done:
        st.markdown('<div class="sh">🪙 Toss</div>', unsafe_allow_html=True)
        t1, t2, t3 = st.columns(3)
        t1.metric("Winner",       match_info.get('toss_winner', 'N/A'))
        t2.metric("Decision",     match_info.get('toss_decision', 'N/A').capitalize())
        t3.metric("Chasing Team", match_info.get('chasing_team', 'N/A'))
    print("Expected features:", opener_model.feature_names_in_)
    print("Provided features:", feats.columns if hasattr(feats, "columns") else "Array shape: " + str(feats.shape))

    # ── H2H ───────────────────────────────────────────────
    st.markdown('<div class="sh">⚔️ Head to Head</div>', unsafe_allow_html=True)
    h2h = matches[
        ((matches['team1'] == match_info['team1']) & (matches['team2'] == match_info['team2'])) |
        ((matches['team1'] == match_info['team2']) & (matches['team2'] == match_info['team1']))
    ]
    t1h = int((h2h['winner'] == match_info['team1']).sum())
    t2h = int((h2h['winner'] == match_info['team2']).sum())
    hc1, hc2, hc3 = st.columns(3)
    hc1.metric(f"{match_info['team1']} wins", t1h)
    hc2.metric("Total meetings",              t1h + t2h)
    hc3.metric(f"{match_info['team2']} wins", t2h)

    # ── Form ──────────────────────────────────────────────
    st.markdown('<div class="sh">📈 Recent Form (last 5)</div>', unsafe_allow_html=True)
    fc1, fc2 = st.columns(2)
    rs1 = get_team_recent_avg_score(match_info['team1'], today_ts)
    rs2 = get_team_recent_avg_score(match_info['team2'], today_ts)
    with fc1:
        st.markdown(
            f'<div class="form-card">🔵 <b>{match_info["team1"]}</b><br>'
            f'Avg first-innings score: <b>{rs1:.0f} runs</b></div>',
            unsafe_allow_html=True,
        )
    with fc2:
        st.markdown(
            f'<div class="form-card">🔴 <b>{match_info["team2"]}</b><br>'
            f'Avg first-innings score: <b>{rs2:.0f} runs</b></div>',
            unsafe_allow_html=True,
        )

    # ── Playing XI ────────────────────────────────────────
    if toss_done and match_info.get('team1_xi'):
        st.markdown('<div class="sh">👥 Playing XI</div>', unsafe_allow_html=True)
        xi1, xi2 = st.columns(2)
        with xi1:
            st.markdown(f"**{match_info['team1']}**")
            for p in match_info.get('team1_xi', []):
                st.markdown(f'<span class="player-tag">🏏 {p}</span>',
                            unsafe_allow_html=True)
        with xi2:
            st.markdown(f"**{match_info['team2']}**")
            for p in match_info.get('team2_xi', []):
                st.markdown(f'<span class="player-tag">🏏 {p}</span>',
                            unsafe_allow_html=True)
    else:
        st.info("⏳ Playing XI will appear once announced post-toss.")

    # ── Prediction ────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"""
    <div class="pred-box">
        <div style="opacity:0.6; font-size:0.78rem; letter-spacing:0.12em; text-transform:uppercase;">
            {'Post-Toss Prediction' if toss_done else 'Pre-Toss Estimate'}
        </div>
        <div style="font-size:0.9rem; margin-top:0.6rem; opacity:0.75;">🏆 Predicted Winner</div>
        <div class="winner-name">{pred_winner}</div>
        <div style="font-size:1.4rem; margin-top:0.2rem; font-weight:600;">
            {win_prob:.1f}% confidence
        </div>
    </div>
    """, unsafe_allow_html=True)

    pm1, pm2, pm3, pm4 = st.columns(4)
    pm1.metric("🏆 Winner",          pred_winner)
    pm2.metric("🎲 Win probability",  f"{win_prob:.1f}%")
    pm3.metric("📈 1st inn. score",   f"{int(s_pred)} runs")
    pm4.metric("🏏 Opener runs",      f"~{int(op_pred)}")

    st.markdown(
        f"**{match_info['team1']}** {win_prob:.1f}% "
        f"&nbsp;·&nbsp; "
        f"{100 - win_prob:.1f}% **{match_info['team2']}**"
    )
    st.progress(int(win_prob) / 100)

    if not toss_done:
        st.warning(
            "⚠️ Pre-toss estimate — click **GET PREDICTION** again "
            "after the toss for an updated prediction."
        )

    st.success("✅ Done!")