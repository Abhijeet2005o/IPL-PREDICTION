# app.py — Modern IPL Predictor UI with Pitch Report
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

# ── Fixed import — no circular reference ──────────────────
from scraper import espncricinfo_scraper
from data.ground_types import get_ground_info

get_todays_match_id  = espncricinfo_scraper.get_todays_match_id
scrape_match         = espncricinfo_scraper.scrape_match
build_feature_vector = espncricinfo_scraper.build_feature_vector
IPL_SERIES_ID        = espncricinfo_scraper.IPL_SERIES_ID

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
[data-testid="stSidebar"] {
    background: #0d1117 !important;
    border-right: 1px solid #1e2a3a !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: #0d1117; }
::-webkit-scrollbar-thumb { background: #ff6b35; border-radius: 3px; }

/* ── Animated Background Grid ── */
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
    0%   { transform: translateY(0); }
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
    to   { width: 120px; opacity: 1; }
}
@keyframes fadeInDown {
    from { opacity: 0; transform: translateY(-20px); }
    to   { opacity: 1; transform: translateY(0); }
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
    50%       { opacity: 0.4; transform: scale(0.7); }
}
@keyframes fadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
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

/* ═════════════════════════════════════════════════════════
   GROUND TYPE / PITCH REPORT CARD
   ═════════════════════════════════════════════════════════ */
.ground-card {
    background: linear-gradient(135deg,
        rgba(255,255,255,0.04) 0%,
        rgba(13,17,23,0.85) 100%);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 20px;
    padding: 1.6rem 1.5rem;
    margin: 0.8rem 0;
    position: relative;
    overflow: hidden;
}
.ground-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0;
    width: 4px; height: 100%;
    background: var(--accent, #ff6b35);
}

.ground-header {
    display: flex;
    align-items: center;
    gap: 0.9rem;
    margin-bottom: 1.2rem;
}
.ground-icon-big {
    font-size: 2.4rem;
    flex-shrink: 0;
}
.ground-info-block { flex: 1; min-width: 0; }
.ground-type-name {
    font-family: 'Orbitron', monospace;
    font-size: 1.15rem;
    font-weight: 700;
    line-height: 1.2;
    word-wrap: break-word;
}
.ground-venue-name {
    font-family: 'Inter', sans-serif;
    font-size: 0.78rem;
    color: rgba(255,255,255,0.5);
    margin-top: 0.2rem;
}

.ground-desc {
    font-family: 'Inter', sans-serif;
    font-size: 0.82rem;
    color: rgba(255,255,255,0.65);
    line-height: 1.5;
    padding: 0.8rem 1rem;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    margin-bottom: 1rem;
    font-style: italic;
}

.ground-attrs-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 0.6rem;
}
.ground-attr {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px;
    padding: 0.7rem 0.8rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.ground-attr-icon { font-size: 1.1rem; flex-shrink: 0; }
.ground-attr-content { flex: 1; min-width: 0; }
.ground-attr-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.65rem;
    color: rgba(255,255,255,0.4);
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.ground-attr-value {
    font-family: 'Rajdhani', sans-serif;
    font-size: 0.88rem;
    font-weight: 700;
    color: #ffffff;
    margin-top: 0.1rem;
}

.attr-yes { color: #22c55e !important; }
.attr-no  { color: #ef4444 !important; }

@media (max-width: 600px) {
    .ground-icon-big { font-size: 2rem; }
    .ground-type-name { font-size: 1rem; }
    .ground-attrs-grid { grid-template-columns: 1fr 1fr; }
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
    50%       { transform: translateY(-8px); }
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
.post-toss { background: rgba(34,197,94,0.15);  border: 1px solid rgba(34,197,94,0.3);  color: #22c55e; }
.pre-toss  { background: rgba(234,179,8,0.15);   border: 1px solid rgba(234,179,8,0.3);  color: #eab308; }

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
.prob-pct { font-family: 'Orbitron', monospace; font-size: 0.8rem; font-weight: 700; }
.pct-t1   { color: #ff6b35; }
.pct-t2   { color: #60a5fa; }
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

/* ── Stat Cards ── */
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
.stat-icon  { font-size: 1.5rem; margin-bottom: 0.4rem; }
.stat-val   {
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

/* ═════════════════════════════════════════════════════════
   TOSS CARD — vertical mobile-friendly layout
   ═════════════════════════════════════════════════════════ */
.toss-card {
    background: linear-gradient(135deg,
        rgba(255,215,0,0.06) 0%,
        rgba(13,17,23,0.85) 100%);
    border: 1px solid rgba(255,215,0,0.18);
    border-radius: 20px;
    padding: 1.8rem 1.5rem;
    margin: 0.8rem 0;
    position: relative;
    overflow: hidden;
}
.toss-card::before {
    content: '';
    position: absolute;
    top: -40px; right: -40px;
    width: 160px; height: 160px;
    background: radial-gradient(circle, rgba(255,215,0,0.08) 0%, transparent 70%);
    pointer-events: none;
}

.toss-header {
    display: flex;
    align-items: center;
    gap: 0.9rem;
    margin-bottom: 1.2rem;
}
.toss-coin {
    font-size: 2.2rem;
    animation: spin 3s ease-in-out infinite;
    flex-shrink: 0;
}
@keyframes spin {
    0%, 80%, 100% { transform: rotateY(0deg); }
    40%            { transform: rotateY(180deg); }
}
.toss-winner-block { flex: 1; min-width: 0; }
.toss-winner-name {
    font-family: 'Orbitron', monospace;
    font-size: 1.15rem;
    font-weight: 700;
    color: #ffd700;
    line-height: 1.2;
    word-wrap: break-word;
}
.toss-won-label {
    font-family: 'Inter', sans-serif;
    font-size: 0.78rem;
    color: rgba(255,255,255,0.5);
    margin-top: 0.2rem;
    letter-spacing: 0.02em;
}

.toss-decision-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.7rem 0.9rem;
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 12px;
    margin-bottom: 0.6rem;
}
.toss-decision-icon { font-size: 1.1rem; }
.toss-decision-text {
    font-family: 'Inter', sans-serif;
    font-size: 0.82rem;
    color: rgba(255,255,255,0.6);
}
.toss-decision-text b {
    font-weight: 700;
    margin-left: 0.2rem;
}
.bat-text  { color: #22c55e !important; }
.bowl-text { color: #60a5fa !important; }

.toss-chasing-row {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.7rem 0.9rem;
    background: rgba(255,107,53,0.06);
    border: 1px solid rgba(255,107,53,0.15);
    border-radius: 12px;
}
.toss-chasing-icon { font-size: 1.1rem; }
.toss-chasing-text {
    font-family: 'Inter', sans-serif;
    font-size: 0.82rem;
    color: rgba(255,255,255,0.6);
}
.toss-chasing-team {
    font-family: 'Rajdhani', sans-serif;
    font-weight: 700;
    color: #ff6b35;
    margin-left: 0.2rem;
}

@media (max-width: 600px) {
    .toss-coin { font-size: 1.8rem; }
    .toss-winner-name { font-size: 1rem; }
    .toss-decision-row, .toss-chasing-row { padding: 0.6rem 0.75rem; }
    .toss-decision-text, .toss-chasing-text { font-size: 0.78rem; }
}

/* ── H2H Display ── */
.h2h-bar    { display: flex; height: 8px; border-radius: 8px; overflow: hidden; margin: 0.8rem 0; background: rgba(255,255,255,0.05); }
.h2h-t1     { background: linear-gradient(90deg, #ff6b35, #ff8c5a); }
.h2h-t2     { background: linear-gradient(90deg, #3b82f6, #60a5fa); }
.h2h-label  {
    font-family: 'Orbitron', monospace;
    font-size: 1.8rem;
    font-weight: 900;
    color: #ffffff;
}
.h2h-team   {
    font-family: 'Inter', sans-serif;
    font-size: 0.75rem;
    color: rgba(255,255,255,0.5);
    margin-top: 0.2rem;
}

/* ── Player Chips ── */
.player-grid { display: flex; flex-wrap: wrap; gap: 0.4rem; margin-top: 0.8rem; }
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

/* ── Form Section ── */
.form-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.7rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.05);
}
.form-row:last-child { border-bottom: none; }
.form-team  { font-family: 'Rajdhani', sans-serif; font-size: 0.95rem; font-weight: 600; color: rgba(255,255,255,0.8); }
.form-score { font-family: 'Orbitron', monospace; font-size: 1.1rem; font-weight: 700; color: #ff6b35; }
.form-label { font-family: 'Inter', sans-serif; font-size: 0.7rem; color: rgba(255,255,255,0.3); }

/* ── CTA Button ── */
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
.stButton > button:active { transform: translateY(0) !important; }

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

/* ── Inline Banners ── */
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
.banner-warn { background: rgba(234,179,8,0.1);  border: 1px solid rgba(234,179,8,0.25);  color: #eab308; }
.banner-err  { background: rgba(239,68,68,0.1);  border: 1px solid rgba(239,68,68,0.25);  color: #ef4444; }
.banner-info { background: rgba(96,165,250,0.1); border: 1px solid rgba(96,165,250,0.25); color: #60a5fa; }
.banner-ok   { background: rgba(34,197,94,0.1);  border: 1px solid rgba(34,197,94,0.25);  color: #22c55e; }

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

/* ── Fancy Divider ── */
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

/* ── Responsive ── */
@media (max-width: 768px) {
    .hero-title  { font-size: 2rem; }
    .pred-winner { font-size: 1.8rem; }
    .match-vs-card { padding: 1.5rem 1rem; }
}
</style>

<div class="bg-grid"></div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# LOAD MODELS + DATA  (cached — runs once)
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
            'opener_avg_batting_avg': float(row['opener_avg_batting_avg']),
            'opener_avg_strike_rate': float(row['opener_avg_strike_rate']),
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
    base  = venue.split(',')[0].strip()
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
    h   = matches[
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
    st.markdown('<div class="sidebar-logo">🏏 IPL AI 2026</div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">Match Override</div>', unsafe_allow_html=True)
    manual_match_id  = st.text_input("Match ID",  value="", placeholder="e.g. 1529286")
    manual_series_id = st.text_input("Series ID", value="", placeholder="default: 1510719")

    st.markdown('<div class="sidebar-section">Playing XI (optional)</div>', unsafe_allow_html=True)
    st.caption("Comma-separated. Overrides auto-detected XI.")
    manual_team1_xi = st.text_area("Team 1 XI", value="", placeholder="Player1, Player2 ...", height=80)
    manual_team2_xi = st.text_area("Team 2 XI", value="", placeholder="Player1, Player2 ...", height=80)

    st.markdown("---")
    st.markdown(
        '<div style="font-family:Inter;font-size:0.72rem;'
        'color:rgba(255,255,255,0.25);text-align:center;">'
        'IPL AI Predictor v2.0 · 2026</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────
# HERO HEADER
# ─────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
    <div class="live-pill">
        <div class="live-dot"></div> LIVE PREDICTION ENGINE
    </div>
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

    # ── Series ID override ────────────────────────────────
    if manual_series_id.strip():
        espncricinfo_scraper.IPL_SERIES_ID = manual_series_id.strip()
        st.markdown(
            f'<div class="banner banner-info">'
            f'🔧 Series ID overridden → <b>{manual_series_id.strip()}</b>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Step 1: Resolve Match ID ──────────────────────────
    if manual_match_id.strip():
        try:
            match_id = int(manual_match_id.strip())
        except ValueError:
            st.markdown(
                '<div class="banner banner-err">'
                '❌ Match ID must be numeric — e.g. <b>1529286</b>'
                '</div>',
                unsafe_allow_html=True,
            )
            st.stop()
        st.markdown(
            f'<div class="banner banner-info">'
            f'🔧 Manual match ID → <b>{match_id}</b>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        with st.spinner("🔍  Scanning live matches…"):
            match_id = get_todays_match_id()

    if match_id is None:
        st.markdown(
            '<div class="banner banner-err">'
            '❌ No IPL match found today — paste a Match ID in the sidebar to continue'
            '</div>',
            unsafe_allow_html=True,
        )
        st.stop()

    st.markdown(
        f'<div class="banner banner-ok">'
        f'📌 Match ID <b>{match_id}</b> · '
        f'Series <b>{espncricinfo_scraper.IPL_SERIES_ID}</b>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ── Step 2: Scrape match data ─────────────────────────
    with st.spinner("📡  Fetching match data…"):
        match_info = scrape_match(match_id)

    with st.expander("🐛  Debug — raw scraper output"):
        st.json(match_info or {})

    if not match_info or match_info.get("error") or not match_info.get("team1"):
        err = (match_info or {}).get("error", "Unknown error")
        st.markdown(
            f'<div class="banner banner-err">'
            f'❌ Data fetch failed — {err}'
            f'</div>',
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

    # ── Step 3: Build features ────────────────────────────
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

    # ── Computed values ───────────────────────────────────
    team1       = match_info["team1"]
    team2       = match_info["team2"]
    pred_winner = team1 if w_pred == 1 else team2
    win_prob    = w_prob[int(w_pred)] * 100
    lose_prob   = 100 - win_prob
    toss_done   = bool(match_info.get("toss_done", False))

    # ═══════════════════════════════════════════════════════
    # UI RENDERING
    # ═══════════════════════════════════════════════════════

    # ── Match Card ────────────────────────────────────────
    st.markdown(
        '<div class="fancy-divider"><span>MATCH</span></div>',
        unsafe_allow_html=True,
    )
    match_html = (
        f'<div class="match-vs-card">'
        f'<div style="display:flex;align-items:center;justify-content:center;gap:1.5rem;flex-wrap:wrap;">'
        f'<div><div class="team-name-big">{team1}</div></div>'
        f'<div class="vs-badge">VS</div>'
        f'<div><div class="team-name-big">{team2}</div></div>'
        f'</div>'
        f'<div style="margin-top:1rem;">'
        f'<span class="venue-tag">📍 {match_info.get("venue", "N/A")}</span>'
        f'</div>'
        f'</div>'
    )
    st.markdown(match_html, unsafe_allow_html=True)

    # ── Pitch Report / Ground Type Card ───────────────────
    venue_name = match_info.get("venue", "")
    ground = get_ground_info(venue_name)

    st.markdown(
        '<div class="fancy-divider"><span>PITCH REPORT</span></div>',
        unsafe_allow_html=True,
    )

    pace_val = (
        '<span class="attr-yes">YES</span>' if ground["pace_friendly"]
        else '<span class="attr-no">NO</span>'
    )
    spin_val = (
        '<span class="attr-yes">YES</span>' if ground["spin_friendly"]
        else '<span class="attr-no">NO</span>'
    )
    chase_val = (
        '<span class="attr-yes">CHASING</span>' if ground["chase_friendly"]
        else '<span class="attr-no">DEFENDING</span>'
    )

    ground_html = (
        f'<div class="ground-card" style="--accent:{ground["color"]};">'
        f'<div class="ground-header">'
        f'<div class="ground-icon-big">{ground["icon"]}</div>'
        f'<div class="ground-info-block">'
        f'<div class="ground-type-name" style="color:{ground["color"]};">{ground["type"]}</div>'
        f'<div class="ground-venue-name">{venue_name or "Unknown Venue"}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-desc">💡 {ground["description"]}</div>'
        f'<div class="ground-attrs-grid">'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">📊</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">Avg Score</div>'
        f'<div class="ground-attr-value">{ground["avg_score"]}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">📏</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">Boundary</div>'
        f'<div class="ground-attr-value">{ground["boundary_size"]}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">⚡</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">Pace</div>'
        f'<div class="ground-attr-value">{pace_val}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">🌀</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">Spin</div>'
        f'<div class="ground-attr-value">{spin_val}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">💧</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">Dew Factor</div>'
        f'<div class="ground-attr-value">{ground["dew_factor"]}</div>'
        f'</div>'
        f'</div>'
        f'<div class="ground-attr">'
        f'<div class="ground-attr-icon">🎯</div>'
        f'<div class="ground-attr-content">'
        f'<div class="ground-attr-label">2nd Innings</div>'
        f'<div class="ground-attr-value">{chase_val}</div>'
        f'</div>'
        f'</div>'
        f'</div>'
        f'</div>'
    )
    st.markdown(ground_html, unsafe_allow_html=True)

    # ── Toss Card (vertical mobile-friendly layout) ───────
    if toss_done:
        toss_winner   = match_info.get("toss_winner", "")
        toss_decision = match_info.get("toss_decision", "")
        chasing       = match_info.get("chasing_team", "")

        if toss_decision == "bat":
            dec_icon  = "🏏"
            dec_text  = "Elected to"
            dec_word  = "BAT FIRST"
            dec_color = "bat-text"
        else:
            dec_icon  = "🎳"
            dec_text  = "Elected to"
            dec_word  = "BOWL FIRST"
            dec_color = "bowl-text"

        chasing_html = (
            f'<div class="toss-chasing-row">'
            f'<div class="toss-chasing-icon">⚡</div>'
            f'<div class="toss-chasing-text">Chasing '
            f'<span class="toss-chasing-team">{chasing}</span>'
            f'</div>'
            f'</div>'
        ) if chasing else ""

        st.markdown(
            '<div class="fancy-divider"><span>TOSS</span></div>',
            unsafe_allow_html=True,
        )

        toss_html = (
            f'<div class="toss-card">'
            f'<div class="toss-header">'
            f'<div class="toss-coin">🪙</div>'
            f'<div class="toss-winner-block">'
            f'<div class="toss-winner-name">{toss_winner}</div>'
            f'<div class="toss-won-label">won the toss</div>'
            f'</div>'
            f'</div>'
            f'<div class="toss-decision-row">'
            f'<div class="toss-decision-icon">{dec_icon}</div>'
            f'<div class="toss-decision-text">{dec_text} '
            f'<b class="{dec_color}">{dec_word}</b>'
            f'</div>'
            f'</div>'
            f'{chasing_html}'
            f'</div>'
        )
        st.markdown(toss_html, unsafe_allow_html=True)

    # ── Head to Head ──────────────────────────────────────
    st.markdown(
        '<div class="fancy-divider"><span>HEAD TO HEAD</span></div>',
        unsafe_allow_html=True,
    )
    t1w, t2w, total = h2h_stats(team1, team2)
    t1_pct = int(t1w / total * 100) if total else 50
    t2_pct = 100 - t1_pct

    h2h_html = (
        f'<div class="glass-card">'
        f'<div class="sec-header">⚔️ All-Time Record</div>'
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.8rem;">'
        f'<div>'
        f'<div class="h2h-label" style="color:#ff6b35;">{t1w}</div>'
        f'<div class="h2h-team">{team1}</div>'
        f'</div>'
        f'<div style="text-align:center;">'
        f'<div style="font-family:Orbitron,monospace;font-size:0.7rem;color:rgba(255,255,255,0.3);letter-spacing:0.15em;">'
        f'{total} MATCHES'
        f'</div>'
        f'</div>'
        f'<div style="text-align:right;">'
        f'<div class="h2h-label" style="color:#60a5fa;">{t2w}</div>'
        f'<div class="h2h-team">{team2}</div>'
        f'</div>'
        f'</div>'
        f'<div class="h2h-bar">'
        f'<div class="h2h-t1" style="width:{t1_pct}%;"></div>'
        f'<div class="h2h-t2" style="width:{t2_pct}%;"></div>'
        f'</div>'
        f'<div style="display:flex;justify-content:space-between;font-family:Inter,sans-serif;font-size:0.7rem;color:rgba(255,255,255,0.3);margin-top:0.4rem;">'
        f'<span>{t1_pct}%</span>'
        f'<span>{t2_pct}%</span>'
        f'</div>'
        f'</div>'
    )
    st.markdown(h2h_html, unsafe_allow_html=True)

    # ── Recent Form ───────────────────────────────────────
    st.markdown(
        '<div class="fancy-divider"><span>RECENT FORM</span></div>',
        unsafe_allow_html=True,
    )
    rs1 = get_team_recent_avg_score(team1, today_ts)
    rs2 = get_team_recent_avg_score(team2, today_ts)

    fc1, fc2 = st.columns(2)
    with fc1:
        form_html_1 = (
            f'<div class="glass-card">'
            f'<div class="sec-header">🔵 {team1}</div>'
            f'<div class="form-row">'
            f'<div>'
            f'<div class="form-team">Avg 1st Innings Score</div>'
            f'<div class="form-label">Last 5 matches</div>'
            f'</div>'
            f'<div class="form-score">{rs1:.0f}</div>'
            f'</div>'
            f'</div>'
        )
        st.markdown(form_html_1, unsafe_allow_html=True)
    with fc2:
        form_html_2 = (
            f'<div class="glass-card">'
            f'<div class="sec-header">🔴 {team2}</div>'
            f'<div class="form-row">'
            f'<div>'
            f'<div class="form-team">Avg 1st Innings Score</div>'
            f'<div class="form-label">Last 5 matches</div>'
            f'</div>'
            f'<div class="form-score">{rs2:.0f}</div>'
            f'</div>'
            f'</div>'
        )
        st.markdown(form_html_2, unsafe_allow_html=True)

    # ── Playing XI ────────────────────────────────────────
    xi_available = bool(
        match_info.get("team1_xi") or match_info.get("team2_xi")
    )
    if toss_done and xi_available:
        st.markdown(
            '<div class="fancy-divider"><span>PLAYING XI</span></div>',
            unsafe_allow_html=True,
        )
        xc1, xc2 = st.columns(2)
        with xc1:
            chips1 = "".join(
                f'<span class="player-chip">🏏 {p}</span>'
                for p in match_info.get("team1_xi", [])
            )
            xi_html_1 = (
                f'<div class="glass-card">'
                f'<div class="xi-team-label">{team1}</div>'
                f'<div class="player-grid">{chips1}</div>'
                f'</div>'
            )
            st.markdown(xi_html_1, unsafe_allow_html=True)
        with xc2:
            chips2 = "".join(
                f'<span class="player-chip">🏏 {p}</span>'
                for p in match_info.get("team2_xi", [])
            )
            xi_html_2 = (
                f'<div class="glass-card">'
                f'<div class="xi-team-label">{team2}</div>'
                f'<div class="player-grid">{chips2}</div>'
                f'</div>'
            )
            st.markdown(xi_html_2, unsafe_allow_html=True)
    elif toss_done:
        st.markdown(
            '<div class="banner banner-warn">'
            '⚠️ Playing XI not detected — paste players in the sidebar for better accuracy'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div class="banner banner-info">'
            '⏳ Playing XI will appear once announced after the toss'
            '</div>',
            unsafe_allow_html=True,
        )

    # ── Prediction Hero ───────────────────────────────────
    st.markdown(
        '<div class="fancy-divider"><span>AI PREDICTION</span></div>',
        unsafe_allow_html=True,
    )

    badge_class = "post-toss" if toss_done else "pre-toss"
    badge_text  = (
        "✅ POST-TOSS · TOSS FACTORED IN"
        if toss_done else
        "⏳ PRE-TOSS · HISTORICAL ESTIMATE"
    )

    pred_html = (
        f'<div class="pred-hero">'
        f'<div class="pred-label">AI MATCH PREDICTION</div>'
        f'<span class="pred-type-badge {badge_class}">{badge_text}</span>'
        f'<div class="pred-trophy">🏆</div>'
        f'<div class="pred-label">PREDICTED WINNER</div>'
        f'<div class="pred-winner">{pred_winner}</div>'
        f'<div class="pred-conf">{win_prob:.1f}% confidence</div>'
        f'</div>'
    )
    st.markdown(pred_html, unsafe_allow_html=True)

    # ── Win Probability Bar ───────────────────────────────
    p1 = win_prob  if pred_winner == team1 else lose_prob
    p2 = lose_prob if pred_winner == team1 else win_prob

    prob_html = (
        f'<div class="prob-container">'
        f'<div class="prob-labels">'
        f'<div>'
        f'<div class="prob-team">{team1}</div>'
        f'<div class="prob-pct pct-t1">{p1:.1f}%</div>'
        f'</div>'
        f'<div style="text-align:right;">'
        f'<div class="prob-team">{team2}</div>'
        f'<div class="prob-pct pct-t2">{p2:.1f}%</div>'
        f'</div>'
        f'</div>'
        f'<div class="prob-bar-track">'
        f'<div class="prob-bar-t1" style="width:{p1:.1f}%;"></div>'
        f'<div class="prob-bar-t2" style="width:{p2:.1f}%;"></div>'
        f'</div>'
        f'</div>'
    )
    st.markdown(prob_html, unsafe_allow_html=True)

    # ── Stat Tiles ────────────────────────────────────────
    sc1, sc2, sc3, sc4 = st.columns(4)
    tiles = [
        ("🏆", pred_winner.split()[-1], "Predicted Winner"),
        ("🎲", f"{win_prob:.1f}%",       "Win Probability"),
        ("📈", f"{int(s_pred)}",          "1st Inn. Score"),
        ("🏏", f"~{int(op_pred)}",        "Opener Runs"),
    ]
    for col, (icon, val, label) in zip([sc1, sc2, sc3, sc4], tiles):
        with col:
            tile_html = (
                f'<div class="stat-card">'
                f'<div class="stat-icon">{icon}</div>'
                f'<div class="stat-val">{val}</div>'
                f'<div class="stat-label">{label}</div>'
                f'</div>'
            )
            st.markdown(tile_html, unsafe_allow_html=True)

    # ── Footer banners ────────────────────────────────────
    if not toss_done:
        st.markdown(
            '<div class="banner banner-warn" style="margin-top:1.2rem;">'
            '⚠️ Pre-toss estimate — click <b>PREDICT NOW</b> again after '
            'the toss for an updated prediction with toss data'
            '</div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div class="banner banner-ok" style="margin-top:0.6rem;">'
        '✅ Prediction complete — results shown above'
        '</div>',
        unsafe_allow_html=True,
    )
