import streamlit as st
import pandas as pd
import joblib
import pickle
import sys
import os
import numpy as np
from datetime import datetime

# ─── Path Setup ───
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from scraper import espncricinfo_scraper
from data.ground_types import get_ground_info

# ─── Imports from Scraper ───
scrape_match = espncricinfo_scraper.scrape_match
build_feature_vector = espncricinfo_scraper.build_feature_vector
get_todays_match_id = espncricinfo_scraper.get_todays_match_id

# ─── Page Config ───
st.set_page_config(
    page_title="IPL Neural Predictor 3D",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── 3D UI CSS ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');

.stApp {
    background: radial-gradient(circle at 10% 20%, rgb(10, 25, 45) 0%, rgb(2, 5, 15) 90%);
    font-family: 'Rajdhani', sans-serif;
    color: #e0e6ed;
}

/* 3D Glassmorphism Cards */
.card-3d {
    background: rgba(255, 255, 255, 0.04);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 20px;
    padding: 20px;
    margin-bottom: 15px;
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.5), inset 0 0 20px rgba(0, 255, 255, 0.02);
    transition: transform 0.3s ease, box-shadow 0.3s ease;
}
.card-3d:hover {
    transform: translateY(-5px) scale(1.01);
    box-shadow: 0 15px 45px 0 rgba(0, 255, 255, 0.15), inset 0 0 25px rgba(0, 255, 255, 0.05);
    border-color: rgba(0, 255, 255, 0.3);
}

/* Neon Text */
h1, h2, h3 {
    font-family: 'Orbitron', sans-serif;
    text-shadow: 0 0 15px rgba(0, 255, 255, 0.4);
    color: #ffffff;
}

/* Metric Boxes */
.metric-box {
    background: linear-gradient(145deg, #0d1624, #050a14);
    border: 1px solid #1f364d;
    border-radius: 15px;
    padding: 15px;
    text-align: center;
    box-shadow: 0 4px 15px rgba(0,0,0,0.6);
    height: 100%;
}
.metric-val {
    font-size: 2.2rem;
    font-weight: 700;
    color: #00ffcc;
    text-shadow: 0 0 10px #00ffcc;
}
.metric-label {
    font-size: 0.85rem;
    color: #88a3bd;
    letter-spacing: 2px;
    text-transform: uppercase;
}

/* Buttons */
.stButton>button {
    background: linear-gradient(90deg, #00C9FF 0%, #92FE9D 100%);
    color: #000;
    font-weight: bold;
    font-family: 'Orbitron', sans-serif;
    border: none;
    border-radius: 50px;
    padding: 10px 25px;
    box-shadow: 0 0 15px rgba(0, 255, 255, 0.5);
    transition: all 0.3s;
}
.stButton>button:hover {
    box-shadow: 0 0 30px rgba(0, 255, 255, 0.8);
    transform: scale(1.05);
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: rgba(5, 10, 20, 0.95) !important;
    border-right: 1px solid rgba(255, 255, 255, 0.1) !important;
}

/* Hide Streamlit Elements */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ─── Load Models & Data ───
@st.cache_resource
def load_everything():
    models = {
        'winner': joblib.load('models/winner_model.pkl'),
        'score': joblib.load('models/score_model.pkl'),
        'second_innings': joblib.load('models/second_innings_model.pkl'),
        'pp_score': joblib.load('models/opener_model.pkl') # Using Opener model for PP Score
    }
    
    encoders = {
        'team': joblib.load('models/team_encoder.pkl'),
        'venue': joblib.load('models/venue_encoder.pkl')
    }
    
    with open('data/feature_cols.pkl', 'rb') as f:
        feature_cols = pickle.load(f)
        
    # Load Data for H2H and Stats
    m_raw = pd.read_csv('data/all_ipl_matches_data.csv')
    t_raw = pd.read_csv('data/all_teams_data.csv')
    t_map = dict(zip(t_raw['team_id'], t_raw['team_name'].str.strip()))
    
    m_raw['team1'] = m_raw['team1'].map(t_map).str.strip()
    m_raw['team2'] = m_raw['team2'].map(t_map).str.strip()
    m_raw['winner'] = m_raw['match_winner'].map(t_map).str.strip()
    m_raw.rename(columns={'match_id': 'id'}, inplace=True)
    matches = m_raw[m_raw['result'] == 'win'].reset_index(drop=True)
    matches['match_date'] = pd.to_datetime(matches['match_date'])
    
    vsh = pd.read_csv('data/venue_score_history.csv')
    vsh['match_date'] = pd.to_datetime(vsh['match_date'])
    
    tsl = pd.read_csv('data/team_scores_long.csv')
    tsl['match_date'] = pd.to_datetime(tsl['match_date'])
    if 'team' in tsl.columns: tsl['team'] = tsl['team'].str.strip()
        
    pp_df = pd.read_csv('data/team_pp_eco.csv')
    if 'team_name' in pp_df.columns: pp_df['team_name'] = pp_df['team_name'].str.strip()
    pp_eco = dict(zip(pp_df['team_name'], pp_df['avg_pp_economy']))
    
    op_df = pd.read_csv('data/team_opener_lookup.csv')
    if 'team_name' in op_df.columns: op_df['team_name'] = op_df['team_name'].str.strip()
    op_lkp = {
        row['team_name']: {
            'opener_avg_batting_avg': float(row['opener_avg_batting_avg']),
            'opener_avg_strike_rate': float(row['opener_avg_strike_rate']),
        }
        for _, row in op_df.iterrows()
    }
    
    return models, encoders, feature_cols, matches, vsh, tsl, pp_eco, op_lkp

try:
    models, encoders, feature_cols, matches, vsh, tsl, pp_eco, op_lkp = load_everything()
except Exception as e:
    st.error(f"❌ Failed to load models/data: {e}")
    st.stop()

# ─── Helpers ───
def h2h_stats(team1, team2):
    # Use the fixer from scraper
    t1 = espncricinfo_scraper._normalize_team_name(team1)
    t2 = espncricinfo_scraper._normalize_team_name(team2)
    
    h = matches[
        ((matches['team1'] == t1) & (matches['team2'] == t2)) |
        ((matches['team1'] == t2) & (matches['team2'] == t1))
    ]
    t1w = int((h['winner'] == t1).sum())
    t2w = int((h['winner'] == t2).sum())
    return t1w, t2w, len(h)

def get_recent_avg(team, n=5):
    team = espncricinfo_scraper._normalize_team_name(team)
    past = tsl[(tsl['team'] == team)].tail(n)
    return float(past['first_innings_score'].mean()) if len(past) > 0 else 167.0

def get_recent_high_rate(team, n=10):
    team = espncricinfo_scraper._normalize_team_name(team)
    threshold = tsl['first_innings_score'].quantile(0.75)
    past = tsl[(tsl['team'] == team)].tail(n)
    return float((past['first_innings_score'] >= threshold).mean()) if len(past) > 0 else 0.3

# ─── Sidebar ───
with st.sidebar:
    st.markdown("<h3 style='color:#00C9FF'>⚙️ Settings</h3>", unsafe_allow_html=True)
    manual_match_id = st.text_input("Match ID (e.g. 1529293)", "")
    
    st.markdown("---")
    st.markdown("<h3 style='color:#00C9FF'>🪙 Toss Override</h3>", unsafe_allow_html=True)
    toss_winner_override = st.selectbox("Toss Winner", ["Auto-detect", "Team 1", "Team 2"])
    toss_decision_override = st.selectbox("Decision", ["Auto-detect", "Bat", "Field"])

# ─── Main UI ───
st.markdown("<h1 style='text-align:center; margin-bottom: 5px;'>🔮 IPL NEURAL PREDICTOR</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center; color:#88a3bd; margin-bottom: 30px;'>Powered by XGBoost Ensemble • Live Data Integration • 3D Engine</p>", unsafe_allow_html=True)

col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
with col_btn2:
    predict_btn = st.button("🚀 INITIATE PREDICTION", use_container_width=True, type="primary")

if predict_btn:
    match_id_input = manual_match_id.strip()
    if match_id_input:
        try:
            match_id = int(match_id_input)
        except:
            st.error("❌ Invalid Match ID")
            st.stop()
    else:
        match_id = get_todays_match_id()
        if not match_id:
            st.error("❌ No match found. Please enter a Match ID.")
            st.stop()
    
    with st.spinner("📡 Scraping Live Data..."):
        try:
            match_info = scrape_match(match_id)
        except Exception as e:
            st.error(f"❌ Scraping Error: {e}")
            st.stop()

    if not match_info or 'error' in match_info:
        st.error(f"❌ Data fetch failed: {match_info.get('error', 'Unknown error')}")
        st.stop()

    # Apply Manual Overrides
    if toss_winner_override != "Auto-detect" and toss_decision_override != "Auto-detect":
        t1_raw = match_info.get("team1", "")
        t2_raw = match_info.get("team2", "")
        match_info["toss_winner"] = t1_raw if toss_winner_override == "Team 1" else t2_raw
        match_info["toss_decision"] = 'bat' if toss_decision_override == "Bat" else 'field'
        match_info["toss_done"] = True
        match_info["chasing_team"] = (t2_raw if match_info["toss_winner"] == t1_raw else t1_raw) if match_info["toss_decision"] == 'bat' else match_info["toss_winner"]

    # ─── Prediction Engine ───
    with st.spinner("🧠 Running Neural Models..."):
        try:
            # Helper functions for the feature vector builder
            def get_team_recent_avg_score(team, cd, n=5): return get_recent_avg(team, n)
            def get_team_recent_high_score_rate(team, cd, n=10): return get_recent_high_rate(team, n)
            
            # Build Features
            feats = build_feature_vector(
                match_info, 
                pd.read_csv('player_stats/player_lookup.csv'),
                matches,
                encoders['team'], encoders['venue'],
                vsh, pp_eco, op_lkp,
                get_team_recent_avg_score, 
                lambda d: 170, 
                lambda d: 2026, 
                lambda v, d: 167, 
                get_team_recent_high_score_rate,
                feature_cols
            )
            feats = feats.fillna(0)
            
            # 1. 1st Innings Score
            pred_1st = float(models['score'].predict(feats)[0])
            
            # 2. 2nd Innings Score (Needs target_score injected)
            second_inn_feats = feats.copy()
            second_inn_feats['target_score'] = pred_1st 
            pred_2nd = float(models['second_innings'].predict(second_inn_feats)[0])
            
            # 3. Powerplay Score (Using opener model file)
            pred_pp = float(models['pp_score'].predict(feats)[0])
            
            # 4. Winner
            winner_probs = models['winner'].predict_proba(feats)[0]
            winner_idx = np.argmax(winner_probs)
            t1 = match_info['team1']
            t2 = match_info['team2']
            winner = t1 if winner_idx == 1 else t2
            conf = float(winner_probs[winner_idx] * 100)

        except Exception as e:
            st.error(f"🤖 Model Error: {e}")
            import traceback
            st.code(traceback.format_exc())
            st.stop()

    # ─── 3D Results UI ───
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Match Header
    st.markdown(f"""
    <div class="card-3d" style="text-align:center;">
        <h2 style="margin:0; color:#88a3bd;">LIVE MATCH</h2>
        <h1 style="font-size: 2.5rem; margin: 10px 0;">
            <span style="color:#00C9FF">{t1}</span> vs <span style="color:#92FE9D">{t2}</span>
        </h1>
        <div style="color: #aaa; font-size: 1.1rem;">📍 {match_info['venue']}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Predictions Grid
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{conf:.1f}%</div><div class="metric-label">Win Probability</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_1st)}</div><div class="metric-label">1st Innings</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_2nd)}</div><div class="metric-label">2nd Innings</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_pp)}</div><div class="metric-label">Powerplay (0-6)</div></div>""", unsafe_allow_html=True)
        
    # Winner Badge
    st.markdown(f"""
    <div class="card-3d" style="text-align:center; margin-top: 20px; border-color: rgba(0, 255, 255, 0.4);">
        <h3 style="color:#88a3bd;">PREDICTED WINNER</h3>
        <h1 style="font-size: 3.5rem; color: #00ffcc; text-shadow: 0 0 25px #00ffcc; margin: 10px 0;">{winner}</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Stats Sections
    col_a, col_b = st.columns(2)
    with col_a:
        t1w, t2w, total = h2h_stats(t1, t2)
        st.markdown(f"""<div class="card-3d"><h3 style="margin-bottom:5px;">⚔️ Head to Head</h3><p style="font-size:1.3rem">{t1} <b style="color:#00C9FF">{t1w}</b> - <b style="color:#92FE9D">{t2w}</b> {t2}</p><p style="color:#aaa; font-size:0.9rem">Total Matches: {total}</p></div>""", unsafe_allow_html=True)
    with col_b:
        r1 = get_recent_avg(t1)
        r2 = get_recent_avg(t2)
        st.markdown(f"""<div class="card-3d"><h3 style="margin-bottom:5px;">📊 Recent Form (Avg)</h3><p style="font-size:1.3rem">{t1}: <b style="color:#00C9FF">{r1:.0f}</b> | {t2}: <b style="color:#92FE9D">{r2:.0f}</b></p></div>""", unsafe_allow_html=True)

    # Pitch Report
    ground = get_ground_info(match_info['venue'])
    st.markdown(f"""
    <div class="card-3d">
        <h3>🏟️ Pitch Report</h3>
        <div style="display:flex; justify-content:space-around; flex-wrap:wrap;">
            <div><b>Type:</b> {ground.get('type', 'Balanced')}</div>
            <div><b>Avg Score:</b> {ground.get('avg_score', 165)}</div>
            <div><b>Dew:</b> {ground.get('dew_factor', 'Medium')}</div>
            <div><b>Strategy:</b> {'Chase' if ground.get('chase_friendly') else 'Set Target'}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
