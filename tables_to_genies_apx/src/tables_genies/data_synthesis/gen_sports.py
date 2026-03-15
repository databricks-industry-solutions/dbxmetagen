"""Generate synthetic sports data: World Cup 2026, NFL, NBA."""
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession

# =============================================================================
# CONFIGURATION
# =============================================================================
SEED = 42
END_DATE = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
START_DATE = END_DATE - timedelta(days=180)

CATALOG = "serverless_dbx_unifiedchat_catalog"
SCHEMAS = ["world_cup_2026", "nfl", "nba"]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def create_infrastructure(spark, catalog, schemas):
    """Create schemas if they don't exist (catalog must already exist)."""
    print(f"Using catalog: {catalog}")
    
    for schema in schemas:
        print(f"Creating schema: {catalog}.{schema}")
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
            print(f"  ✓ Created schema {schema}")
        except Exception as e:
            print(f"  Note: {e}")

def save_table(spark, df_pandas, catalog, schema, table_name):
    """Convert pandas DataFrame to Spark DataFrame and save as table."""
    full_table_name = f"`{catalog}`.`{schema}`.`{table_name}`"
    print(f"  Saving {full_table_name} ({len(df_pandas):,} rows)")
    
    df_spark = spark.createDataFrame(df_pandas)
    df_spark.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
    print(f"  ✓ Saved {full_table_name}")

# =============================================================================
# SETUP
# =============================================================================
np.random.seed(SEED)
Faker.seed(SEED)
fake = Faker()
spark = SparkSession.builder.getOrCreate()

# =============================================================================
# CREATE INFRASTRUCTURE
# =============================================================================
create_infrastructure(spark, CATALOG, SCHEMAS)

# =============================================================================
# WORLD CUP 2026 SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating World Cup 2026 data...")
print(f"{'='*80}")

# Real World Cup 2026 countries
WC_COUNTRIES = [
    "USA", "Mexico", "Canada", "Argentina", "Brazil", "Uruguay", "Colombia", "Chile",
    "England", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands", "Belgium",
    "Japan", "South Korea", "Australia", "Saudi Arabia", "Iran", "Qatar",
    "Ghana", "Senegal", "Morocco", "Tunisia", "Nigeria", "Cameroon",
    "Costa Rica", "Panama", "Jamaica", "Honduras"
]

# 1. Teams
teams_wc = pd.DataFrame({
    "team_id": [f"WC-{i+1:03d}" for i in range(len(WC_COUNTRIES))],
    "country": WC_COUNTRIES,
    "fifa_ranking": np.random.randint(1, 100, len(WC_COUNTRIES)),
    "confederation": [
        "CONCACAF" if c in ["USA", "Mexico", "Canada", "Costa Rica", "Panama", "Jamaica", "Honduras"] else
        "CONMEBOL" if c in ["Argentina", "Brazil", "Uruguay", "Colombia", "Chile"] else
        "UEFA" if c in ["England", "France", "Germany", "Spain", "Italy", "Portugal", "Netherlands", "Belgium"] else
        "AFC" if c in ["Japan", "South Korea", "Australia", "Saudi Arabia", "Iran", "Qatar"] else
        "CAF"
        for c in WC_COUNTRIES
    ],
    "group_assigned": np.random.choice(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], len(WC_COUNTRIES)),
})
save_table(spark, teams_wc, CATALOG, "world_cup_2026", "teams")

# 2. Players (5 per team)
players_wc_data = []
for _, team in teams_wc.iterrows():
    for i in range(5):
        players_wc_data.append({
            "player_id": f"WCP-{len(players_wc_data)+1:04d}",
            "team_id": team['team_id'],
            "player_name": fake.name(),
            "position": np.random.choice(['Forward', 'Midfielder', 'Defender', 'Goalkeeper'], p=[0.3, 0.3, 0.3, 0.1]),
            "age": np.random.randint(19, 36),
            "caps": np.random.randint(1, 120),
        })
players_wc = pd.DataFrame(players_wc_data)
save_table(spark, players_wc, CATALOG, "world_cup_2026", "players")

# 3. Stadiums
stadiums_wc = pd.DataFrame({
    "stadium_id": [f"WCS-{i+1:02d}" for i in range(16)],
    "stadium_name": [fake.company() + " Stadium" for _ in range(16)],
    "city": np.random.choice(["Los Angeles", "New York", "Mexico City", "Toronto", "Vancouver", "Dallas", "Miami", "Atlanta"], 16),
    "capacity": np.random.randint(50000, 90000, 16),
    "host_country": np.random.choice(["USA", "Mexico", "Canada"], 16, p=[0.6, 0.25, 0.15]),
})
save_table(spark, stadiums_wc, CATALOG, "world_cup_2026", "stadiums")

# 4. Matches (48 group stage + 16 knockout)
matches_wc_data = []
team_ids = teams_wc['team_id'].tolist()
stadium_ids = stadiums_wc['stadium_id'].tolist()

for i in range(64):
    team1, team2 = np.random.choice(team_ids, 2, replace=False)
    score1 = np.random.randint(0, 4)
    score2 = np.random.randint(0, 4)
    
    matches_wc_data.append({
        "match_id": f"WCM-{i+1:03d}",
        "team1_id": team1,
        "team2_id": team2,
        "stadium_id": np.random.choice(stadium_ids),
        "match_date": (datetime(2026, 6, 11) + timedelta(days=i)).strftime("%Y-%m-%d"),
        "team1_score": score1,
        "team2_score": score2,
        "stage": "Group" if i < 48 else "Knockout",
        "attendance": np.random.randint(45000, 85000),
    })
matches_wc = pd.DataFrame(matches_wc_data)
save_table(spark, matches_wc, CATALOG, "world_cup_2026", "matches")

# 5. Group Standings
standings_wc_data = []
for group in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
    group_teams = teams_wc[teams_wc['group_assigned'] == group]['team_id'].tolist()[:4]
    for team_id in group_teams:
        standings_wc_data.append({
            "team_id": team_id,
            "group_name": group,
            "matches_played": np.random.randint(1, 3),
            "wins": np.random.randint(0, 3),
            "draws": np.random.randint(0, 2),
            "losses": np.random.randint(0, 2),
            "goals_for": np.random.randint(0, 8),
            "goals_against": np.random.randint(0, 6),
            "points": np.random.randint(0, 9),
        })
standings_wc = pd.DataFrame(standings_wc_data)
save_table(spark, standings_wc, CATALOG, "world_cup_2026", "group_standings")

# =============================================================================
# NFL SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating NFL data...")
print(f"{'='*80}")

# Real NFL teams
NFL_TEAMS = [
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
    "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
    "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
    "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
]

# 1. Teams
teams_nfl = pd.DataFrame({
    "team_id": [f"NFL-{i+1:02d}" for i in range(len(NFL_TEAMS))],
    "team_name": NFL_TEAMS,
    "conference": np.random.choice(['AFC', 'NFC'], len(NFL_TEAMS)),
    "division": np.random.choice(['North', 'South', 'East', 'West'], len(NFL_TEAMS)),
    "wins": np.random.randint(0, 17, len(NFL_TEAMS)),
    "losses": np.random.randint(0, 17, len(NFL_TEAMS)),
})
save_table(spark, teams_nfl, CATALOG, "nfl", "teams")

# 2. Players (10 per team)
players_nfl_data = []
for _, team in teams_nfl.iterrows():
    for i in range(10):
        players_nfl_data.append({
            "player_id": f"NFLP-{len(players_nfl_data)+1:04d}",
            "team_id": team['team_id'],
            "player_name": fake.name(),
            "position": np.random.choice(['QB', 'RB', 'WR', 'TE', 'OL', 'DL', 'LB', 'DB', 'K', 'P'], p=[0.08, 0.1, 0.12, 0.08, 0.15, 0.15, 0.12, 0.15, 0.03, 0.02]),
            "jersey_number": np.random.randint(1, 100),
            "years_pro": np.random.randint(0, 15),
            "college": fake.company() + " University",
        })
players_nfl = pd.DataFrame(players_nfl_data)
save_table(spark, players_nfl, CATALOG, "nfl", "players")

# 3. Games (17 weeks x 16 games)
games_nfl_data = []
team_ids_nfl = teams_nfl['team_id'].tolist()
for week in range(1, 18):
    for game_num in range(16):
        team1, team2 = np.random.choice(team_ids_nfl, 2, replace=False)
        score1 = np.random.randint(10, 45)
        score2 = np.random.randint(10, 45)
        
        games_nfl_data.append({
            "game_id": f"NFLG-{len(games_nfl_data)+1:04d}",
            "week": week,
            "home_team_id": team1,
            "away_team_id": team2,
            "home_score": score1,
            "away_score": score2,
            "game_date": (START_DATE + timedelta(days=week*7)).strftime("%Y-%m-%d"),
            "attendance": np.random.randint(50000, 75000),
        })
games_nfl = pd.DataFrame(games_nfl_data)
save_table(spark, games_nfl, CATALOG, "nfl", "games")

# 4. Stats (per player per game - sample)
stats_nfl_data = []
game_ids_nfl = games_nfl['game_id'].tolist()[:50]  # Sample 50 games
player_ids_nfl = players_nfl['player_id'].tolist()

for game_id in game_ids_nfl:
    # Random 5 players per game have stats
    game_players = np.random.choice(player_ids_nfl, 5, replace=False)
    for player_id in game_players:
        stats_nfl_data.append({
            "stat_id": f"NFLS-{len(stats_nfl_data)+1:05d}",
            "game_id": game_id,
            "player_id": player_id,
            "passing_yards": np.random.randint(0, 400) if np.random.random() > 0.8 else 0,
            "rushing_yards": np.random.randint(0, 200) if np.random.random() > 0.7 else 0,
            "receiving_yards": np.random.randint(0, 150) if np.random.random() > 0.6 else 0,
            "touchdowns": np.random.randint(0, 3),
        })
stats_nfl = pd.DataFrame(stats_nfl_data)
save_table(spark, stats_nfl, CATALOG, "nfl", "stats")

# 5. Standings
standings_nfl = teams_nfl.copy()
standings_nfl['points_for'] = np.random.randint(200, 500, len(standings_nfl))
standings_nfl['points_against'] = np.random.randint(200, 500, len(standings_nfl))
standings_nfl['win_percentage'] = (standings_nfl['wins'] / (standings_nfl['wins'] + standings_nfl['losses'])).round(3)
save_table(spark, standings_nfl[['team_id', 'wins', 'losses', 'win_percentage', 'points_for', 'points_against']], CATALOG, "nfl", "standings")

# =============================================================================
# NBA SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating NBA data...")
print(f"{'='*80}")

# Real NBA teams
NBA_TEAMS = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets", "Chicago Bulls",
    "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets", "Detroit Pistons", "Golden State Warriors",
    "Houston Rockets", "Indiana Pacers", "LA Clippers", "Los Angeles Lakers", "Memphis Grizzlies",
    "Miami Heat", "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
    "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns", "Portland Trail Blazers",
    "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors", "Utah Jazz", "Washington Wizards"
]

# 1. Teams
teams_nba = pd.DataFrame({
    "team_id": [f"NBA-{i+1:02d}" for i in range(len(NBA_TEAMS))],
    "team_name": NBA_TEAMS,
    "conference": np.random.choice(['Eastern', 'Western'], len(NBA_TEAMS)),
    "division": np.random.choice(['Atlantic', 'Central', 'Southeast', 'Northwest', 'Pacific', 'Southwest'], len(NBA_TEAMS)),
    "wins": np.random.randint(15, 65, len(NBA_TEAMS)),
    "losses": np.random.randint(15, 65, len(NBA_TEAMS)),
    "championships": np.random.choice([0, 0, 0, 1, 1, 2, 3, 5, 17], len(NBA_TEAMS)),
})
save_table(spark, teams_nba, CATALOG, "nba", "teams")

# 2. Players (12 per team)
players_nba_data = []
for _, team in teams_nba.iterrows():
    for i in range(12):
        players_nba_data.append({
            "player_id": f"NBAP-{len(players_nba_data)+1:04d}",
            "team_id": team['team_id'],
            "player_name": fake.name(),
            "position": np.random.choice(['PG', 'SG', 'SF', 'PF', 'C'], p=[0.2, 0.2, 0.2, 0.2, 0.2]),
            "jersey_number": np.random.randint(0, 100),
            "height_cm": np.random.randint(180, 220),
            "years_pro": np.random.randint(0, 20),
            "ppg": round(np.random.lognormal(2, 0.6), 1),  # Points per game
        })
players_nba = pd.DataFrame(players_nba_data)
save_table(spark, players_nba, CATALOG, "nba", "players")

# 3. Games (82 games per team / 2 = 1230 total games, sample 300)
games_nba_data = []
team_ids_nba = teams_nba['team_id'].tolist()
for i in range(300):
    team1, team2 = np.random.choice(team_ids_nba, 2, replace=False)
    score1 = np.random.randint(85, 130)
    score2 = np.random.randint(85, 130)
    
    games_nba_data.append({
        "game_id": f"NBAG-{i+1:04d}",
        "home_team_id": team1,
        "away_team_id": team2,
        "home_score": score1,
        "away_score": score2,
        "game_date": (START_DATE + timedelta(days=i)).strftime("%Y-%m-%d"),
        "attendance": np.random.randint(15000, 21000),
        "overtime": np.random.choice([False, False, False, True], p=[0.9, 0, 0, 0.1]),
    })
games_nba = pd.DataFrame(games_nba_data)
save_table(spark, games_nba, CATALOG, "nba", "games")

# 4. Stats (per player per game - sample)
stats_nba_data = []
game_ids_nba = games_nba['game_id'].tolist()[:100]  # Sample 100 games
player_ids_nba = players_nba['player_id'].tolist()

for game_id in game_ids_nba:
    # 10 players per game
    game_players = np.random.choice(player_ids_nba, 10, replace=False)
    for player_id in game_players:
        stats_nba_data.append({
            "stat_id": f"NBAS-{len(stats_nba_data)+1:05d}",
            "game_id": game_id,
            "player_id": player_id,
            "points": np.random.randint(0, 40),
            "rebounds": np.random.randint(0, 15),
            "assists": np.random.randint(0, 12),
            "minutes_played": np.random.randint(10, 45),
        })
stats_nba = pd.DataFrame(stats_nba_data)
save_table(spark, stats_nba, CATALOG, "nba", "stats")

# 5. Draft Picks (last 5 years, 60 picks per year)
draft_picks_nba_data = []
for year in range(2021, 2026):
    for pick_num in range(1, 61):
        draft_picks_nba_data.append({
            "draft_id": f"NBAD-{year}-{pick_num:02d}",
            "year": year,
            "pick_number": pick_num,
            "round": 1 if pick_num <= 30 else 2,
            "team_id": np.random.choice(team_ids_nba),
            "player_name": fake.name(),
            "position": np.random.choice(['PG', 'SG', 'SF', 'PF', 'C']),
        })
draft_picks_nba = pd.DataFrame(draft_picks_nba_data)
save_table(spark, draft_picks_nba, CATALOG, "nba", "draft_picks")

print(f"\n{'='*80}")
print(f"✓ Sports Analytics data generation complete!")
print(f"  - World Cup 2026: 5 tables")
print(f"  - NFL: 5 tables")
print(f"  - NBA: 5 tables")
print(f"{'='*80}")
