# data/ground_types.py
"""
IPL venue → pitch characteristics mapping.
Updated based on historical IPL data and pitch reports.
"""

GROUND_TYPES = {
    # ── BATTING PARADISES ──────────────────────────────────
    "M Chinnaswamy Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 175,
        "boundary_size": "Small",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "High",
        "chase_friendly": True,
        "description": "Small ground, true bounce, dew helps chasing teams",
    },
    "Wankhede Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 172,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "High",
        "chase_friendly": True,
        "description": "Hard pitch, true bounce, sea breeze + dew aid chasers",
    },
    "Brabourne Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 170,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Flat batting deck, quick outfield",
    },
    "Punjab Cricket Association Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 175,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "High",
        "chase_friendly": True,
        "description": "True bounce, fast outfield, high-scoring venue",
    },
    "Maharashtra Cricket Association Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 170,
        "boundary_size": "Large",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Flat track, large boundaries reward big hitters",
    },

    # ── BALANCED PITCHES ───────────────────────────────────
    "Narendra Modi Stadium": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Large",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "World's largest stadium, balanced for bat & ball",
    },
    "Eden Gardens": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "High",
        "chase_friendly": True,
        "description": "Historic ground, slight assistance to spin in 2nd innings",
    },
    "Rajiv Gandhi International Stadium": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Even contest between bat and ball",
    },
    "Sawai Mansingh Stadium": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Low",
        "chase_friendly": False,
        "description": "Slight grip for spinners, dew minimal",
    },
    "Himachal Pradesh Cricket Association Stadium": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 168,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "Low",
        "chase_friendly": True,
        "description": "Cool conditions, swing for pacers, true bounce",
    },

    # ── BOWLER FRIENDLY ────────────────────────────────────
    "MA Chidambaram Stadium": {
        "type": "Spinner's Den",
        "icon": "🌀",
        "color": "#3b82f6",
        "avg_score": 158,
        "boundary_size": "Medium",
        "pace_friendly": False,
        "spin_friendly": True,
        "dew_factor": "Low",
        "chase_friendly": False,
        "description": "Slow turner, spinners dominate, defending favored",
    },
    "Arun Jaitley Stadium": {
        "type": "Bowler Friendly",
        "icon": "🎯",
        "color": "#8b5cf6",
        "avg_score": 162,
        "boundary_size": "Small",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Variable bounce, both pacers and spinners get help",
    },
    "Dr DY Patil Sports Academy": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Large",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "High",
        "chase_friendly": True,
        "description": "Large boundaries, hard pitch, dew helps chasers",
    },
    "Barsapara Cricket Stadium": {
        "type": "Bowler Friendly",
        "icon": "🎯",
        "color": "#8b5cf6",
        "avg_score": 160,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": False,
        "description": "Two-paced surface, bowlers find purchase",
    },
    "Holkar Cricket Stadium": {
        "type": "Batting Paradise",
        "icon": "🔥",
        "color": "#ff4500",
        "avg_score": 180,
        "boundary_size": "Small",
        "pace_friendly": True,
        "spin_friendly": False,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Highest-scoring IPL venue historically",
    },
    "JSCA International Stadium Complex": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#22c55e",
        "avg_score": 165,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Even contest, slight pace assistance",
    },

    # ── DEFAULT for unknown grounds ─────────────────────────
    "_DEFAULT": {
        "type": "Balanced",
        "icon": "⚖️",
        "color": "#94a3b8",
        "avg_score": 165,
        "boundary_size": "Medium",
        "pace_friendly": True,
        "spin_friendly": True,
        "dew_factor": "Medium",
        "chase_friendly": True,
        "description": "Standard T20 conditions assumed",
    },
}


def get_ground_info(venue_name):
    """
    Match a venue name to ground info using fuzzy matching.
    Returns the dict with type, icon, color, etc.
    """
    if not venue_name:
        return GROUND_TYPES["_DEFAULT"]

    venue_lower = venue_name.lower().strip()

    # Exact match
    for known_venue, info in GROUND_TYPES.items():
        if known_venue == "_DEFAULT":
            continue
        if known_venue.lower() == venue_lower:
            return info

    # Partial match (venue name contains known stadium)
    for known_venue, info in GROUND_TYPES.items():
        if known_venue == "_DEFAULT":
            continue
        # Check if any significant word matches
        known_words = set(known_venue.lower().split())
        venue_words = set(venue_lower.split())
        common = known_words & venue_words
        # Need at least 2 word overlap, or one big distinctive word
        distinctive = {"chinnaswamy", "wankhede", "eden", "chidambaram",
                       "modi", "chepauk", "brabourne", "kotla", "jaitley",
                       "feroz", "shah", "rajiv", "gandhi", "holkar",
                       "jsca", "barsapara", "dharamsala", "mohali",
                       "chandigarh", "patil", "mca", "sawai", "mansingh"}
        if (common & distinctive) or len(common) >= 2:
            return info

    return GROUND_TYPES["_DEFAULT"]
