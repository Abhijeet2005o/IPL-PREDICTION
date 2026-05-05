# app.py — Modern IPL Predictor UI
# Run: streamlit run app.py

import streamlit as st
import pandas as pd
import joblib
import pickle
import sys
import os
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from scraper import espncricinfo_scraper
from scraper.espncricinfo_scraper import (
    get_todays_match_id,
    scrape_match,
    build_feature_vector,
    IPL_SERIES_ID,
)

ENV_SERIES_ID = os.getenv("IPL_SERIES_ID", "").strip()
if ENV_SERIES_ID:
    espncricinfo_scraper.IPL_SERIES_ID = ENV_SERIES_ID

# ─────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPL AI Predictor 2026",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────
# FULL CSS DESIGN SYSTEM
# ─────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Inter:wght@300;400;500;600;700&family=Rajdhani:wght@400;500;600;700&display=swap');

/* ── Reset & Base ── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

.stApp {
    background: linear-gradient(135deg, #0a0a0f 0%, #0d1117 40%, #0a0f1e 70%, #0d0a1a 100%);
    min-height: 100vh;
}

/* Hide streamlit defaults */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 1rem 2rem 1rem !important; max-width: 1400px !important; }
[data-testid="stSidebar"] { background: #0d1117 !important; border-right: 1px solid #1e2a3a !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0d1117; }
::-webkit-scrollbar-thumb { background: #ff6b35; border-radius: 3px; }

/* ── Animated Background Particles ── */
.bg-grid {
    position: fixed;
    top: 0; left: 0;
    width: 100%; height: 100%;
    background-image:
        linear-gradient(rgba(255,107,53,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,107,53,0.03) 1px, transparent 1px);
    background-size: 50px 50px;
    pointer-events: none;
    z-index: 0;
    animation: gridMove 20s linear infinite;
}
@keyframes gridMove {
    0% { transform: translateY(0); }
    100% { transform: translateY(50px); }
}

/* ── Hero Header ── */
.hero {
    position: relative;
    text-align: center;
    padding: 3rem 1rem 2rem;
    z-index: 1;
}
.hero-eyebrow {
    font-family: 'Orbitron', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.4em;
    color: #ff6b35;
    text-transform: uppercase;
    margin-bottom: 0.5rem;
    animation: fadeInDown 0.6s ease;
}
.hero-title {
    font-family: 'Orbitron', monospace;
    font-size: clamp(2rem, 6vw, 4.5rem);
    font-weight: 900;
    line-height: 1;
    background: linear-gradient(135deg, #ffffff 0%, #ff6b35 50%, #ffd700 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.4rem;
    animation: fadeInDown 0.7s ease;
}
.hero-sub {
    font-family: 'Rajdhani', sans-serif;
    font-size: 1.1rem;
    color: rgba(255,255,255,0.45);
    letter-spacing: 0.25em;
    text-transform: uppercase;
    animation: fadeInDown 0.8s ease;
}
.hero-line {
    width: 120px;
    height: 2px;
    background: linear-gradient(90deg, transparent, #ff6b35, transparent);
    margin: 1.2rem auto 0;
    animation: expandWidth 1s ease 0.5s both;
}
@keyframes expandWidth {
    from { width: 0; opacity: 0; }
    to { width: 120px; opacity: 1; }
}
@keyframes fadeInDown {
    from { opacity: 0; transform: translateY(-20px); }
    to { opacity: 1; transform: translateY(0); }
}

/* ── Live Badge ── */
.live-pill {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: rgba(255,107,53,0.12);
    border: 1px solid rgba(255,107,53,0.3);
    color: #ff6b35;
    padding: 0.3rem 1rem;
    border-radius: 50px;
    font-family: 'Orbitron', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.15em;
    margin-bottom: 1rem;
    animation: fadeIn 1s ease;
}
.live-dot {
    width: 7px; height: 7px;
    background: #ff6b35;
    border-radius: 50%;
    animation: pulse 1.5s infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.4; transform: scale(0.7); }
}
@keyframes fadeIn {
    from { opacity: 0; }
    to { opacity: 1; }
}

/* ── Glass Cards ── */
.glass-card {
    background: rgba(255,255,255,0.03);
    backdrop-filter: blur(20px);
    -webkit-backdrop-filter: blur(20px);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 20px;
    padding: 1.8rem;
    margin: 0.8rem 0;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
}
.glass-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,107,53,0.4), transparent);
}
.glass-card:hover {
    border-color: rgba(255,107,53,0.2);
    transform: translateY(-2px);
    box-shadow: 0 20px 40px rgba(0,0,0,0.4);
}

/* ── Section Headers ── */
.sec-header {
    font-family: 'Orbitron', monospace;
    font-size: 0.75rem;
    letter-spacing: 0.25em;
    color: #ff6b35;
    text-transform: uppercase;
    margin-bottom: 1.2rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
}
.sec-header::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, rgba(255,107,53,0.3), transparent);
}

/* ── VS Match Card ── */
.match-vs-card {
    background: linear-gradient(135deg,
        rgba(255,107,53,0.08) 0%,
        rgba(13,17,23,0.9) 50%,
        rgba(255,215,0,0.05) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 24px;
    padding: 2.5rem 2rem;
    text-align: center;
    position: relative;
    overflow: hidden;
    margin: 1rem 0;
}
.match-vs-card::before {
    content: '';
    position: absolute;
    top: -50%; left: 50%;
    transform: translateX(-50%);
    width: 200px; height: 200px;
    background: radial-gradient(circle, rgba(255,107,53,0.08) 0%, transparent 70%);
    pointer-events: none;
}
.team-name-big {
    font-family: 'Orbitron', monospace;
    font-size: clamp(1.2rem, 3vw, 2rem);
    font-weight: 700;
    color: #ffffff;
    letter-spacing: 0.05em;
}
.vs-badge {
    font-family: 'Orbitron', monospace;
    font-size: 1.5rem;
    font-weight: 900;
    color: #ff6b35;
    text-shadow: 0 0 20px rgba(255,107,53,0.5);
    padding: 0 1rem;
}
.venue-tag {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    color: rgba(255,255,255,0.6);
    padding: 0.35rem 1rem;
    border-radius: 50px;
    font-family: 'Inter', sans-serif;
    font-size: 0.8rem;
    margin-top: 0.8rem;
}

/* ── Prediction Hero Card ── */
.pred-hero {
    background: linear-gradient(135deg,
        rgba(255,107,53,0.15) 0%,
        rgba(13,17,23,0.95) 40%,
        rgba(255,215,0,0.08) 100%);
    border: 1px solid rgba(255,107,53,0.25);
    border-radius: 28px;
    padding: 3rem 2rem;
    text-align: center;
    position: relative;
    overflow: hidden;
    margin: 1.5rem 0;
    box-shadow:
        0 0 60px rgba(255,107,53,0.1),
        inset 0 0 60px rgba(0,0,0,0.3);
}
.pred-hero::before {
    content: '';
    position: absolute;
    top: -100px; left: 50%;
    transform: translateX(-50%);
    width: 400px; height: 400px;
    background: radial-gradient(circle, rgba(255,107,53,0.06) 0%, transparent 60%);
    pointer-events: none;
}
.pred-label {
    font-family: 'Orbitron', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.3em;
    color: rgba(255,255,255,0.4);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}
.pred-trophy {
    font-size: 3.5rem;
    margin-bottom: 0.5rem;
    animation: bounce 2s infinite;
}
@keyframes bounce {
    0%, 100% { transform: translateY(0); }
    50% { transform: translateY(-8px); }
}
.pred-winner {
    font-family: 'Orbitron', monospace;
    font-size: clamp(1.8rem, 5vw, 3.5rem);
    font-weight: 900;
    background: linear-gradient(135deg, #ffffff 0%, #ff6b35 60%, #ffd700 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.5rem;
    line-height: 1.1;
}
.pred-conf {
    font-family: 'Rajdhani', sans-serif;
    font-size: 1.8rem;
    font-weight: 600;
    color: #ffd700;
    letter-spacing: 0.05em;
}
.pred-type-badge {
    display: inline-block;
    padding: 0.3rem 1.2rem;
    border-radius: 50px;
    font-family: 'Orbitron', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.15em;
    margin-bottom: 1.5rem;
}
.post-toss { background: rgba(34,197,94,0.15); border: 1px solid rgba(34,197,94,0.3); color: #22c55e; }
.pre-toss  { background: rgba(234,179,8,0.15);  border: 1px solid rgba(234,179,8,0.3);  color: #eab308; }

/* ── Win Probability Bar ── */
.prob-container {
    margin: 1.5rem 0;
    padding: 1.2rem 1.5rem;
    background: rgba(255,255,255,0.03);
    border-radius: 16px;
    border: 1px solid rgba(255,255,255,0.06);
}
.prob-labels {
    display: flex;
    justify-content: space-between;
    margin-bottom: 0.6rem;
}
.prob-team {
    font-family: 'Rajdhani', sans-serif;
    font-size: 0.85rem;
    font-weight: 600;
    color: rgba(255,255,255,0.7);
    letter-spacing: 0.05em;
}
.prob-pct {
    font-family: 'Orbitron', monospace;
    font-size: 0.8rem;
    font-weight: 700;
}
.pct-t1 { color: #ff6b35; }
.pct-t2 { color: #60a5fa; }
.prob-bar-track {
    height: 10px;
    background: rgba(255,255,255,0.06);
    border-radius: 10px;
    overflow: hidden;
    display: flex;
}
.prob-bar-t1 {
    height: 100%;
    border-radius: 10px 0 0 10px;
    background: linear-gradient(90deg, #ff6b35, #ff8c5a);
    transition: width 1s ease;
}
.prob-bar-t2 {
    height: 100%;
    border-radius: 0 10px 10px 0;
    background: linear-gradient(90deg, #3b82f6, #60a5fa);
    transition: width 1s ease;
}

/* ── Stat Cards Row ── */
.stat-card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 16px;
    padding: 1.2rem;
    text-align: center;
    transition: all 0.3s ease;
    height: 100%;
}
.stat-card:hover {
    background: rgba(255,107,53,0.06);
    border-color: rgba(255,107,53,0.2);
    transform: translateY(-3px);
}
.stat-icon { font-size: 1.5rem; margin-bottom: 0.4rem; }
.stat-val {
    font-family: 'Orbitron', monospace;
    font-size: 1.4rem;
    font-weight: 700;
    color: #ffffff;
    line-height: 1;
}
.stat-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.7rem;
    color: rgba(255,255,255,0.4);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-top: 0.3rem;
}

/* ── Toss Card ── */
.toss-card {
    background: linear-gradient(135deg,
        rgba(255,215,0,0.06) 0%,
        rgba(13,17,23,0.8) 100%);
    border: 1px solid rgba(255,215,0,0.15);
    border-radius: 20px;
    padding: 1.5rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    margin: 0.8rem 0;
}
.toss-coin {
    font-size: 2.5rem;
    animation: spin 3s ease-in-out infinite;
}
@keyframes spin {
    0%, 80%, 100% { transform: rotateY(0deg); }
    40% { transform: rotateY(180deg); }
}
.toss-info { flex: 1; }
.toss-winner-name {
    font-family: 'Orbitron', monospace;
    font-size: 1.1rem;
    font-weight: 700;
    color: #ffd700;
}
.toss-decision-text {
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    color: rgba(255,255,255,0.55);
    margin-top: 0.2rem;
}
.decision-badge {
    padding: 0.25rem 0.8rem;
    border-radius: 50px;
    font-family: 'Orbitron', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.1em;
    font-weight: 700;
}
.bat-badge  { background: rgba(34,197,94,0.15); border: 1px solid rgba(34,197,94,0.3); color: #22c55e; }
.bowl-badge { background: rgba(96,165,250,0.15); border: 1px solid rgba(96,165,250,0.3); color: #60a5fa; }

/* ── H2H Display ── */
.h2h-bar {
    display: flex;
    height: 8px;
    border-radius: 8px;
    overflow: hidden;
    margin: 0.8rem 0;
    background: rgba(255,255,255,0.05);
}
.h2h-t1 { background: linear-gradient(90deg, #ff6b35, #ff8c5a); }
.h2h-t2 { background: linear-gradient(90deg, #3b82f6, #60a5fa); }
.h2h-label {
    font-family: 'Orbitron', monospace;
    font-size: 1.8rem;
    font-weight: 900;
    color: #ffffff;
}
.h2h-team {
    font-family: 'Inter', sans-serif;
    font-size: 0.75rem;
    color: rgba(255,255,255,0.5);
    margin-top: 0.2rem;
}

/* ── Player Tags ── */
.player-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-top: 0.8rem;
}
.player-chip {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.09);
    color: rgba(255,255,255,0.75);
    padding: 0.3rem 0.75rem;
    border-radius: 50px;
    font-family: 'Inter', sans-serif;
    font-size: 0.75rem;
    font-weight: 500;
    transition: all 0.2s ease;
    cursor: default;
}
.player-chip:hover {
    background: rgba(255,107,53,0.1);
    border-color: rgba(255,107,53,0.3);
    color: #ff6b35;
}
.xi-team-label {
    font-family: 'Orbitron', monospace;
    font-size: 0.7rem;
    letter-spacing: 0.15em;
    color: rgba(255,255,255,0.4);
    text-transform: uppercase;
    margin-bottom: 0.5rem;
}
.xi-name {
    font-family: 'Rajdhani', sans-serif;
    font-size: 1rem;
    font-weight: 600;
    color: #ffffff;
    margin-bottom: 0.4rem;
}

/* ── Form Section ── */
.form-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.7rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}
.form-row:last-child { border-bottom: none; }
.form-team {
    font-family: 'Rajdhani', sans-serif;
    font-size: 0.95rem;
    font-weight: 600;
    color: rgba(255,255,255,0.8);
}
.form-score {
    font-family: 'Orbitron', monospace;
    font-size: 1.1rem;
    font-weight: 700;
    color: #ff6b35;
}
.form-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.7rem;
    color: rgba(255,255,255,0.3);
}

/* ── Button ── */
.stButton > button {
    background: linear-gradient(135deg, #ff6b35 0%, #ff4500 100%) !important;
    color: white !important;
    border: none !important;
    border-radius: 50px !important;
    font-family: 'Orbitron', monospace !important;
    font-size: 0.75rem !important;
    letter-spacing: 0.15em !important;
    font-weight: 700 !important;
    padding: 0.8rem 2.5rem !important;
    transition: all 0.3s ease !important;
    box-shadow: 0 8px 25px rgba(255,107,53,0.35) !important;
    text-transform: uppercase !important;
}
.stButton > button:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 12px 35px rgba(255,107,53,0.5) !important;
}
.stButton > button:active {
    transform: translateY(0) !important;
}

/* ── Sidebar ── */
[data-testid="stSidebar"] .block-container { padding: 1.5rem 1rem !important; }
.sidebar-logo {
    font-family: 'Orbitron', monospace;
    font-size: 1.1rem;
    font-weight: 900;
    color: #ff6b35;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid rgba(255,107,53,0.2);
}
.sidebar-section {
    font-family: 'Orbitron', monospace;
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    color: rgba(255,255,255,0.3);
    text-transform: uppercase;
    margin: 1.2rem 0 0.6rem;
}
[data-testid="stSidebar"] label {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.8rem !important;
    color: rgba(255,255,255,0.55) !important;
}
[data-testid="stSidebar"] input,
[data-testid="stSidebar"] textarea {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    border-radius: 10px !important;
    color: white !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 0.82rem !important;
}
[data-testid="stSidebar"] input:focus,
[data-testid="stSidebar"] textarea:focus {
    border-color: rgba(255,107,53,0.4) !important;
    box-shadow: 0 0 0 2px rgba(255,107,53,0.1) !important;
}

/* ── Info / Warning / Error Banners ── */
.banner {
    padding: 0.8rem 1.2rem;
    border-radius: 12px;
    font-family: 'Inter', sans-serif;
    font-size: 0.85rem;
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin: 0.6rem 0;
}
.banner-warn  { background: rgba(234,179,8,0.1);  border: 1px solid rgba(234,179,8,0.25);  color: #eab308; }
.banner-err   { background: rgba(239,68,68,0.1);  border: 1px solid rgba(239,68,68,0.25);  color: #ef4444; }
.banner-info  { background: rgba(96,165,250,0.1); border: 1px solid rgba(96,165,250,0.25); color: #60a5fa; }
.banner-ok    { background: rgba(34,197,94,0.1);  border: 1px solid rgba(34,197,94,0.25);  color: #22c55e; }

/* ── Expander ── */
[data-testid="stExpander"] {
    background: rgba(255,255,255,0.02) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 12px !important;
}
[data-testid="stExpander"] summary {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.82rem !important;
    color: rgba(255,255,255,0.45) !important;
}

/* ── Divider ── */
.fancy-divider {
    display: flex;
    align-items: center;
    gap: 1rem;
    margin: 2rem 0;
}
.fancy-divider::before,
.fancy-divider::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(255,107,53,0.25), transparent);
}
.fancy-divider span {
    font-family: 'Orbitron', monospace;
    font-size: 0.6rem;
    letter-spacing: 0.3em;
    color: rgba(255,107,53,0.5);
}

/* ── Metric overrides ── */
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 14px !important;
    padding: 1rem !important;
}
[data-testid="stMetricLabel"] {
    font-family: 'Inter', sans-serif !important;
    font-size: 0.72rem !important;
    color: rgba(255,255,255,0.4) !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}
[data-testid="stMetricValue"] {
    font-family: 'Orbitron', monospace !important;
    font-size: 1.15rem !important;
    color: #ffffff !important;
}

/* ── Spinner ── */
[data-testid="stSpinner"] { color: #ff6b35 !important; }

/* ── Responsive tweaks ── */
@media (max-width: 768px) {
    .hero-title { font-size: 2rem; }
    .pred-winner { font-size: 1.8rem; }
    .match-vs-card { padding: 1.5rem 1rem; }
}
</style>

<div class="bg-grid"></div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# LOAD MODELS + DATA
# ─────────────────────────────────────────────────────────
@st.cache_resource
def load_everything():
    winner_model  = joblib.load('models/winner_model.pkl')
    score_model   = joblib.load('models/score_model.pkl')
    opener_model  = joblib.load('models/opener_model.pkl')
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

    pp_df  = pd.read_csv('data/team_pp_eco.csv')
    pp_eco = dict(zip(pp_df['team_name'], pp_df['avg_pp_economy']))

    op_df  = pd.read_csv('data/team_opener_lookup.csv')
    op_lkp = {
        row['team_name']: {
            'opener_avg_batting_avg':  float(row['opener_avg_batting_avg']),
            'opener_avg_strike_rate':  float(row['opener_avg_strike_rate']),
        }
        for _, row in op_df.iterrows()
    }

    return (winner_model, score_model, opener_model,
            team_encoder, venue_encoder, player_lookup, feature_cols,
            m_clean, vsh, tsl, pp_eco, op_lkp)


(winner_model, score_model, opener_model,
 team_encoder, venue_encoder, player_lookup, feature_cols,
 matches, venue_score_history, team_scores_long,
 team_pp_eco_lookup, team_opener_lookup) = load_everything()


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
    s  = team_scores_long[
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


def align_features_for_model(base_feats, model):
    expected = list(getattr(model, "feature_names_in_", []))
    if not expected:
        return base_feats
    aligned = pd.DataFrame(index=base_feats.index)
    for col in expected:
        if col in base_feats.columns:
            aligned[col] = base_feats[col]
        elif col == "opp_pp_economy" and "t2_pp_bowling_economy" in base_feats.columns:
            aligned[col] = base_feats["t2_pp_bowling_economy"]
        else:
            aligned[col] = 0.0
    return aligned


def parse_xi_input(raw_text):
    if not raw_text or not raw_text.strip():
        return []
    return [p.strip() for p in raw_text.split(",") if p.strip()][:11]


def h2h_stats(team1, team2):
    h = matches[
        ((matches['team1'] == team1) & (matches['team2'] == team2)) |
        ((matches['team1'] == team2) & (matches['team2'] == team1))
    ]
    t1w = int((h['winner'] == team1).sum())
    t2w = int((h['winner'] == team2).sum())
    return t1w, t2w, len(h)


# ─────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div class="sidebar-logo">🏏 IPL AI</div>', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-section">Match Override</div>', unsafe_allow_html=True)
    manual_match_id  = st.text_input("Match ID",    value="", placeholder="e.g. 1529286")
    manual_series_id = st.text_input("Series ID",   value="", placeholder="default: 1510719")

    st.markdown('<div class="sidebar-section">Playing XI (optional)</div>', unsafe_allow_html=True)
    st.caption("Comma-separated. Overrides auto-detected XI.")
    manual_team1_xi = st.text_area("Team 1 XI", value="", placeholder="Player1, Player2 ...", height=80)
    manual_team2_xi = st.text_area("Team 2 XI", value="", placeholder="Player1, Player2 ...", height=80)

    st.markdown("---")
    st.markdown(
        '<div style="font-family:Inter;font-size:0.72rem;color:rgba(255,255,255,0.25);text-align:center;">'
        'IPL AI Predictor v2.0 · 2026</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
# HERO HEADER
# ─────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="live-pill"><div class="live-dot"></div> LIVE PREDICTION ENGINE</div>
    <div class="hero-eyebrow">Powered by Machine Learning</div>
    <div class="hero-title">IPL AI PREDICTOR</div>
    <div class="hero-sub">Indian Premier League · 2026 Edition</div>
    <div class="hero-line"></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────
# CTA BUTTON
# ─────────────────────────────────────────────────────────
_, btn_col, _ = st.columns([2, 1.2, 2])
with btn_col:
    go = st.button("⚡  PREDICT NOW", use_container_width=True, type="primary")


# ─────────────────────────────────────────────────────────
# PREDICTION FLOW
# ─────────────────────────────────────────────────────────
if go:
    today_ts = pd.Timestamp(datetime.today().date())

    if manual_series_id.strip():
        espncricinfo_scraper.IPL_SERIES_ID = manual_series_id.strip()
        st.markdown(
            f'<div class="banner banner-info">🔧 Series ID overridden → <b>{manual_series_id.strip()}</b></div>',
            unsafe_allow_html=True,
        )

    # ── Step 1: Match ID ──────────────────────────────────
    if manual_match_id.strip():
        try:
            match_id = int(manual_match_id.strip())
        except ValueError:
            st.markdown(
                '<div class="banner banner-err">❌ Match ID must be numeric — e.g. <b>1529286</b></div>',
                unsafe_allow_html=True,
            )
            st.stop()
        st.markdown(
            f'<div class="banner banner-info">🔧 Manual match ID → <b>{match_id}</b></div>',
            unsafe_allow_html=True,
        )
    else:
        with st.spinner("🔍  Scanning live matches…"):
            match_id = get_todays_match_id()

    if match_id is None:
        st.markdown("""
        <div class="banner banner-err">
            ❌ No IPL match found today — try the Match ID override in the sidebar
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    st.markdown(
        f'<div class="banner banner-ok">📌 Match ID <b>{match_id}</b> · Series <b>{espncricinfo_scraper.IPL_SERIES_ID}</b></div>',
        unsafe_allow_html=True,
    )

    # ── Step 2: Scrape ────────────────────────────────────
    with st.spinner("📡  Fetching match data…"):
        match_info = scrape_match(match_id)

    with st.expander("🐛  Debug — raw scraper output"):
        st.json(match_info or {})

    if not match_info or match_info.get("error") or not match_info.get("team1"):
        err = (match_info or {}).get("error", "Unknown error")
        st.markdown(
            f'<div class="banner banner-err">❌ Data fetch failed — {err}</div>',
            unsafe_allow_html=True,
        )
        st.stop()

    # Apply manual XI overrides
    xi1 = parse_xi_input(manual_team1_xi)
    xi2 = parse_xi_input(manual_team2_xi)
    if xi1:
        match_info["team1_xi"] = xi1
    if xi2:
        match_info["team2_xi"] = xi2

    # ── Step 3: Build features & predict ─────────────────
    with st.spinner("🧠  Running AI models…"):
        feats = build_feature_vector(
            match_info, player_lookup, matches,
            team_encoder, venue_encoder, venue_score_history,
            team_pp_eco_lookup, team_opener_lookup,
            get_team_recent_avg_score, get_season_avg_score,
            get_season_year, get_venue_recent_avg_score,
            get_team_recent_high_score_rate, feature_cols,
        )
        w_feats = align_features_for_model(feats, winner_model)
        s_feats = align_features_for_model(feats, score_model)
        o_feats = align_features_for_model(feats, opener_model)

        try:
            w_pred  = winner_model.predict(w_feats)[0]
            w_prob  = winner_model.predict_proba(w_feats)[0]
            s_pred  = score_model.predict(s_feats)[0]
            op_pred = opener_model.predict(o_feats)[0]
        except Exception as e:
            st.markdown(
                f'<div class="banner banner-err">❌ Model error — {e}</div>',
                unsafe_allow_html=True,
            )
            st.stop()

    team1      = match_info["team1"]
    team2      = match_info["team2"]
    pred_winner = team1 if w_pred == 1 else team2
    win_prob    = w_prob[int(w_pred)] * 100
    lose_prob   = 100 - win_prob
    toss_done   = bool(match_info.get("toss_done", False))

    # ── UI: Match card ────────────────────────────────────
    st.markdown('<div class="fancy-divider"><span>MATCH</span></div>', unsafe_allow_html=True)

    st.markdown(f"""
    <div class="match-vs-card">
        <div style="display:flex;align-items:center;justify-content:center;gap:1.5rem;flex-wrap:wrap;">
            <div>
                <div class="team-name-big">{team1}</div>
            </div>
            <div class="vs-badge">VS</div>
            <div>
                <div class="team-name-big">{team2}</div>
            </div>
        </div>
        <div style="margin-top:1rem;">
            <span class="venue-tag">📍 {match_info.get("venue","N/A")}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── UI: Toss card ─────────────────────────────────────
    if toss_done:
        toss_winner   = match_info.get("toss_winner", "")
        toss_decision = match_info.get("toss_decision", "")
        chasing       = match_info.get("chasing_team", "")
        dec_class     = "bat-badge" if toss_decision == "bat" else "bowl-badge"
        dec_label     = "🏏 Elected to BAT" if toss_decision == "bat" else "🎳 Elected to BOWL"

        st.markdown('<div class="fancy-divider"><span>TOSS</span></div>', unsafe_allow_html=True)
        st.markdown(f"""
        <div class="toss-card">
            <div class="toss-coin">🪙</div>
            <div class="toss-info">
                <div class="toss-winner-name">{toss_winner}</div>
                <div class="toss-decision-text">won the toss and chose to
                    <span class="decision-badge {dec_class}">{dec_label}</span>
                </div>
                {"<div class='toss-decision-text' style='margin-top:0.4rem;'>⚡ Chasing team: <b style=color:#fff>" + chasing + "</b></div>" if chasing else ""}
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ── UI: Head to Head ──────────────────────────────────
    st.markdown('<div class="fancy-divider"><span>HEAD TO HEAD</span></div>', unsafe_allow_html=True)
    t1w, t2w, total = h2h_stats(team1, team2)
    t1_pct = int(t1w / total * 100) if total else 50
    t2_pct = 100 - t1_pct

    st.markdown(f"""
    <div class="glass-card">
        <div class="sec-header">⚔️ All-Time Head to Head</div>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem;">
            <div>
                <div class="h2h-label" style="color:#ff6b35;">{t1w}</div>
                <div class="h2h-team">{team1}</div>
            </div>
            <div style="text-align:center;">
                <div style="font-family:Orbitron,monospace;font-size:0.7rem;
                            color:rgba(255,255,255,0.3);letter-spacing:0.15em;">{total} MATCHES</div>
            </div>
            <div style="text-align:right;">
                <div class="h2h-label" style="color:#60a5fa;">{t2w}</div>
                <div class="h2h-team">{team2}</div>
            </div>
        </div>
        <div class="h2h-bar">
            <div class="h2h-t1" style="width:{t1_pct}%;"></div>
            <div class="h2h-t2" style="width:{t2_pct}%;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;
                    font-family:Inter,sans-serif;font-size:0.7rem;
                    color:rgba(255,255,255,0.3);margin-top:0.4rem;">
            <span>{t1_pct}%</span>
            <span>{t2_pct}%</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── UI: Recent Form ───────────────────────────────────
    st.markdown('<div class="fancy-divider"><span>FORM</span></div>', unsafe_allow_html=True)
    rs1 = get_team_recent_avg_score(team1, today_ts)
    rs2 = get_team_recent_avg_score(team2, today_ts)

    fc1, fc2 = st.columns(2)
    with fc1:
        st.markdown(f"""
        <div class="glass-card">
            <div class="sec-header">🔵 {team1}</div>
            <div class="form-row">
                <div>
                    <div class="form-team">Avg 1st Innings</div>
                    <div class="form-label">Last 5 matches</div>
                </div>
                <div class="form-score">{rs1:.0f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    with fc2:
        st.markdown(f"""
        <div class="glass-card">
            <div class="sec-header">🔴 {team2}</div>
            <div class="form-row">
                <div>
                    <div class="form-team">Avg 1st Innings</div>
                    <div class="form-label">Last 5 matches</div>
                </div>
                <div class="form-score">{rs2:.0f}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ── UI: Playing XI ────────────────────────────────────
    if toss_done and (match_info.get("team1_xi") or match_info.get("team2_xi")):
        st.markdown('<div class="fancy-divider"><span>PLAYING XI</span></div>', unsafe_allow_html=True)
        xc1, xc2 = st.columns(2)
        with xc1:
            xi_chips = "".join(
                f'<span class="player-chip">🏏 {p}</span>'
                for p in match_info.get("team1_xi", [])
            )
            st.markdown(f"""
            <div class="glass-card">
                <div class="xi-team-label">{team1}</div>
                <div class="player-grid">{xi_chips}</div>
            </div>
            """, unsafe_allow_html=True)
        with xc2:
            xi_chips2 = "".join(
                f'<span class="player-chip">🏏 {p}</span>'
                for p in match_info.get("team2_xi", [])
            )
            st.markdown(f"""
            <div class="glass-card">
                <div class="xi-team-label">{team2}</div>
                <div class="player-grid">{xi_chips2}</div>
            </div>
            """, unsafe_allow_html=True)
    elif toss_done:
        st.markdown(
            '<div class="banner banner-warn">⚠️ Playing XI not available yet — paste manually in sidebar for better accuracy</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="banner banner-info">⏳ Playing XI will appear once announced after the toss</div>',
            unsafe_allow_html=True,
        )

    # ── UI: PREDICTION ────────────────────────────────────
    st.markdown('<div class="fancy-divider"><span>PREDICTION</span></div>', unsafe_allow_html=True)

    badge_class = "post-toss" if toss_done else "pre-toss"
    badge_text  = "✅ POST-TOSS · TOSS FACTORED IN" if toss_done else "⏳ PRE-TOSS · HISTORICAL ESTIMATE"

    st.markdown(f"""
    <div class="pred-hero">
        <div class="pred-label">AI MATCH PREDICTION</div>
        <span class="pred-type-badge {badge_class}">{badge_text}</span>
        <div class="pred-trophy">🏆</div>
        <div class="pred-label">PREDICTED WINNER</div>
        <div class="pred-winner">{pred_winner}</div>
        <div class="pred-conf">{win_prob:.1f}% confidence</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Win probability bar ───────────────────────────────
    p1 = win_prob if pred_winner == team1 else lose_prob
    p2 = 100 - p1
    st.markdown(f"""
    <div class="prob-container">
        <div class="prob-labels">
            <div>
                <div class="prob-team">{team1}</div>
                <div class="prob-pct pct-t1">{p1:.1f}%</div>
            </div>
            <div style="text-align:right;">
                <div class="prob-team">{team2}</div>
                <div class="prob-pct pct-t2">{p2:.1f}%</div>
            </div>
        </div>
        <div class="prob-bar-track">
            <div class="prob-bar-t1" style="width:{p1:.1f}%;"></div>
            <div class="prob-bar-t2" style="width:{p2:.1f}%;"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Stat tiles ────────────────────────────────────────
    sc1, sc2, sc3, sc4 = st.columns(4)
    tiles = [
        ("🏆", pred_winner.split()[-1], "Predicted Winner"),
        ("🎲", f"{win_prob:.1f}%",       "Win Probability"),
        ("📈", f"{int(s_pred)}",          "1st Inn. Score"),
        ("🏏", f"~{int(op_pred)}",        "Opener Runs"),
    ]
    for col, (icon, val, label) in zip([sc1, sc2, sc3, sc4], tiles):
        with col:
            st.markdown(f"""
            <div class="stat-card">
                <div class="stat-icon">{icon}</div>
                <div class="stat-val">{val}</div>
                <div class="stat-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)

    # ── Pre-toss warning ──────────────────────────────────
    if not toss_done:
        st.markdown("""
        <div class="banner banner-warn" style="margin-top:1rem;">
            ⚠️ Pre-toss estimate — click <b>PREDICT NOW</b> again after toss for updated prediction
        </div>
        """, unsafe_allow_html=True)

    # ── Success ───────────────────────────────────────────
    st.markdown("""
    <div class="banner banner-ok" style="margin-top:0.8rem;">
        ✅ Prediction complete — scroll up to see full analysis
    </div>
    """, unsafe_allow_html=True)
