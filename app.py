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

scrape_match = espncricinfo_scraper.scrape_match
build_feature_vector = espncricinfo_scraper.build_feature_vector
get_todays_match_id = espncricinfo_scraper.get_todays_match_id

# ─── Page Config ───
st.set_page_config(
    page_title="IPL Neural Predictor",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── 3D Glassmorphism UI ───
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;500;700&display=swap');

/* 3D Dark Background */
.stApp {
    background: radial-gradient(circle at 10% 20%, rgb(10, 20, 40) 0%, rgb(0, 5, 15) 90%);
    font-family: 'Rajdhani', sans-serif;
}

/* 3D Floating Cards */
.card-3d {
    background: rgba(255, 255, 255, 0.03);
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 20px;
    padding: 20px;
    margin: 10px;
    box-shadow: 
        0 10px 30px 0 rgba(0, 0, 0, 0.5),
        inset 0 0 15px rgba(0, 255, 255, 0.05);
    transition: transform 0.3s ease, box-shadow 0.3s ease;
}
.card-3d:hover {
    transform: translateY(-5px);
    box-shadow: 
        0 15px 40px 0 rgba(0, 255, 255, 0.15),
        inset 0 0 15px rgba(0, 255, 255, 0.1);
    border-color: rgba(0, 255, 255, 0.3);
}

/* Neon Text */
h1, h2, h3 {
    font-family: 'Orbitron', sans-serif;
    text-shadow: 0 0 10px rgba(0, 255, 255, 0.4);
    color: #fff;
}

/* Metrics */
.metric-box {
    background: linear-gradient(145deg, #0d1624, #050a14);
    border: 1px solid #1f364d;
    border-radius: 15px;
    padding: 15px;
    text-align: center;
    box-shadow: 0 4px 15px rgba(0,0,0,0.6);
}
.metric-val {
    font-size: 2.2rem;
    font-weight: 700;
    color: #00ffcc;
    text-shadow: 0 0 8px #00ffcc;
}
.metric-label {
    font-size: 0.85rem;
    color: #88a3bd;
    letter-spacing: 2px;
    text-transform: uppercase;
}

/* Button */
.stButton>button {
    background: linear-gradient(90deg, #00C9FF 0%, #92FE9D 100%);
    color: #000;
    font-weight: bold;
    font-family: 'Orbitron', sans-serif;
    border: none;
    border-radius: 50px;
    box-shadow: 0 0 15px rgba(0, 255, 255, 0.5);
}
.stButton>button:hover {
    box-shadow: 0 0 25px rgba(0, 255, 255, 0.8);
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: rgba(5, 10, 20, 0.95);
    border-right: 1px solid rgba(255, 255, 255, 0.1);
}

/* Hide default headers */
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
        # Using opener_model.pkl for PP score as discussed
        'pp_score': joblib.load('models/opener_model.pkl') 
    }
    
    encoders = {
        'team': joblib.load('models/team_encoder.pkl'),
        'venue': joblib.load('models/venue_encoder.pkl')
    }
    
    with open('data/feature_cols.pkl', 'rb') as f:
        feature_cols = pickle.load(f)
        
    # Load CSVs for stats
    m_raw = pd.read_csv('data/all_ipl_matches_data.csv')
    t_raw = pd.read_csv('data/all_teams_data.csv')
    t_map = dict(zip(t_raw['team_id'], t_raw['team_name']))
    
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
    
    return models, encoders, feature_cols, matches, vsh, tsl

models, encoders, feature_cols, matches, vsh, tsl = load_everything()

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
    past = tsl[(tsl['team'] == team)].tail(n)
    return past['first_innings_score'].mean() if len(past) > 0 else 167.0

def align_features(feats, model):
    expected = getattr(model, "feature_names_in_", [])
    if not expected: return feats.fillna(0)
    aligned = pd.DataFrame(index=feats.index)
    for col in expected:
        if col in feats.columns:
            aligned[col] = feats[col]
        else:
            aligned[col] = 0.0
    return aligned.fillna(0)

# ─── Sidebar ───
with st.sidebar:
    st.markdown("### ⚙️ SETTINGS")
    manual_match_id = st.text_input("Match ID (e.g. 1529293)", "")
    
    st.markdown("### 🪙 TOSS OVERRIDE")
    toss_winner = st.selectbox("Toss Winner", ["Auto-detect", "Team 1", "Team 2"])
    toss_decision = st.selectbox("Decision", ["Auto-detect", "Bat", "Field"])
    
    st.markdown("### 📝 XI OVERRIDE")
    xi_input = st.text_area("Paste Team 1 XI (comma separated)", height=100)

# ─── Main UI ───
st.markdown("<h1 style='text-align:center;'>🔮 IPL NEURAL PREDICTOR</h1>", unsafe_allow_html=True)

col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
with col_btn2:
    predict_btn = st.button("🚀 INITIATE PREDICTION", use_container_width=True, type="primary")

if predict_btn:
    # 1. Get Match
    match_id_input = manual_match_id.strip()
    if match_id_input:
        match_id = int(match_id_input)
    else:
        match_id = get_todays_match_id()
    
    if not match_id:
        st.error("❌ No match ID found. Please enter one in the sidebar.")
        st.stop()
    
    # 2. Scrape
    with st.spinner("📡 Scraping Live Data..."):
        match_info = scrape_match(match_id)
    
    if 'error' in match_info:
        st.error(f"❌ Scraping Failed: {match_info['error']}")
        st.stop()
    
    # Apply Overrides
    if toss_winner != "Auto-detect" and toss_decision != "Auto-detect":
        t1 = match_info['team1']
        t2 = match_info['team2']
        match_info['toss_winner'] = t1 if toss_winner == "Team 1" else t2
        match_info['toss_decision'] = 'bat' if toss_decision == "Bat" else 'field'
        match_info['toss_done'] = True
        match_info['chasing_team'] = (t2 if match_info['toss_winner'] == t1 else t1) if match_info['toss_decision'] == 'bat' else match_info['toss_winner']

    # 3. Build Features
    with st.spinner("🧠 Running Neural Models..."):
        # Note: We pass minimal data here as build_feature_vector does the heavy lifting
        # You might need to adjust this call based on your exact helper signature
        try:
            feats = build_feature_vector(
                match_info,
                pd.read_csv('player_stats/player_lookup.csv'),
                matches,
                encoders['team'], encoders['venue'],
                vsh,
                dict(pd.read_csv('data/team_pp_eco.csv').values),
                dict(pd.read_csv('data/team_opener_lookup.csv').values),
                lambda t, d: 167.0, # Dummy for get_recent_avg
                lambda d: 167.0, # Dummy for season
                lambda d: 2026, # Dummy for year
                lambda v, d: 167.0, # Dummy for venue
                lambda t, d: 0.3, # Dummy for high score rate
                feature_cols
            )
        except Exception as e:
            st.error(f"⚠️ Feature Error: {e}")
            st.stop()

        feats = feats.fillna(0)
        t1 = match_info['team1']
        t2 = match_info['team2']
        
        # --- Predictions ---
        # 1. Winner
        w_feats = align_features(feats, models['winner'])
        winner_probs = models['winner'].predict_proba(w_feats)[0]
        winner_idx = np.argmax(winner_probs)
        winner = t1 if winner_idx == 1 else t2
        conf = float(winner_probs[winner_idx] * 100)
        
        # 2. 1st Innings
        s_feats = align_features(feats, models['score'])
        pred_1st = float(models['score'].predict(s_feats)[0])
        
        # 3. Powerplay (Using the "opener" model file)
        p_feats = align_features(feats, models['pp_score'])
        pred_pp = float(models['pp_score'].predict(p_feats)[0])
        
        # 4. 2nd Innings
        si_feats = align_features(feats, models['second_innings'])
        si_feats['target_score'] = pred_1st # INJECT 1ST INNINGS SCORE
        pred_2nd = float(models['second_innings'].predict(si_feats)[0])

    # 4. Display UI
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Match Header
    st.markdown(f"""
    <div class="card-3d" style="text-align:center;">
        <h2 style="margin:0; color:#88a3bd;">LIVE MATCH</h2>
        <h1 style="font-size: 2.5rem; margin: 10px 0;">
            <span style="color:#00C9FF">{t1}</span> vs <span style="color:#92FE9D">{t2}</span>
        </h1>
        <div style="color: #aaa;">📍 {match_info['venue']}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Predictions Grid
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{conf:.0f}%</div><div class="metric-label">Win Probability</div></div>""", unsafe_allow_html=True)
    with c2:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_1st)}</div><div class="metric-label">1st Innings</div></div>""", unsafe_allow_html=True)
    with c3:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_2nd)}</div><div class="metric-label">2nd Innings</div></div>""", unsafe_allow_html=True)
    with c4:
        st.markdown(f"""<div class="metric-box"><div class="metric-val">{int(pred_pp)}</div><div class="metric-label">Powerplay (0-6)</div></div>""", unsafe_allow_html=True)
        
    # Winner Card
    st.markdown(f"""
    <div class="card-3d" style="text-align:center; margin-top: 20px;">
        <h3 style="color:#88a3bd;">PREDICTED WINNER</h3>
        <h1 style="font-size: 3rem; color: #00ffcc; text-shadow: 0 0 20px #00ffcc;">{winner}</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Stats & H2H
    t1w, t2w, total = h2h_stats(t1, t2)
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(f"""<div class="card-3d"><h3>⚔️ Head to Head</h3><p style="font-size:1.2rem">{t1} <b>{t1w}</b> - {t2w} <b>{t2}</b> (Total: {total})</p></div>""", unsafe_allow_html=True)
    with col_b:
        r1 = get_recent_avg(t1)
        r2 = get_recent_avg(t2)
        st.markdown(f"""<div class="card-3d"><h3>📊 Recent Form (Avg Score)</h3><p style="font-size:1.2rem">{t1}: <b>{r1:.0f}</b> | {t2}: <b>{r2:.0f}</b></p></div>""", unsafe_allow_html=True)

    # Pitch Report
    ground = get_ground_info(match_info['venue'])
    st.markdown(f"""
    <div class="card-3d">
        <h3>🏟️ Pitch Report</h3>
        <p><b>Type:</b> {ground.get('type', 'Balanced')}</p>
        <p><b>Avg Score:</b> {ground.get('avg_score', 165)}</p>
        <p><b>Strategy:</b> {'Chase' if ground.get('chase_friendly') else 'Set Target'}</p>
    </div>
    """, unsafe_allow_html=True)
