"""
Shared configuration for synthetic data generation.
"""
from datetime import datetime, timedelta

# =============================================================================
# REPRODUCIBILITY
# =============================================================================
SEED = 42

# =============================================================================
# DATE RANGES
# =============================================================================
# Last 6 months from today for realistic data
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

# Special events (within the date range)
INCIDENT_END = END_DATE - timedelta(days=21)
INCIDENT_START = INCIDENT_END - timedelta(days=10)

# =============================================================================
# DATA SIZES
# =============================================================================
# Enough rows for meaningful aggregation and Genie exploration
MIN_ROWS_PER_TABLE = 100
DEFAULT_ROWS_PER_TABLE = 150

# =============================================================================
# CATALOG/SCHEMA NAMES
# =============================================================================
CATALOGS = {
    "sports_analytics": ["world_cup_2026", "nfl", "nba"],
    "science_research": ["nasa", "drug_discovery", "semiconductors"],
    "ai_tech": ["genai"],
    "health_nutrition": ["nutrition", "pharmaceuticals"],
    "entertainment": ["iron_chef", "japanese_anime", "rock_bands"],
    "insurance_claims": ["claims", "providers"],
    "history": ["world_war_2", "roman_history"],
    "global_policy": ["international_policy"],
    "serverless_dbx_unifiedchat_catalog": ["demo_mixed"],
}
