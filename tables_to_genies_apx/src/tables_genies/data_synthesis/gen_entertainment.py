"""Generate synthetic entertainment data: Iron Chef, Japanese Anime, Rock Bands."""
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
SCHEMAS = ["iron_chef", "japanese_anime", "rock_bands"]

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
# IRON CHEF SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Iron Chef data...")
print(f"{'='*80}")

# 1. Chefs
chefs_ic = pd.DataFrame({
    "chef_id": [f"CHEF-{i+1:03d}" for i in range(100)],
    "name": [fake.name() for _ in range(100)],
    "specialty": np.random.choice(['French', 'Italian', 'Japanese', 'Chinese', 'Fusion', 'Molecular'], 100),
    "wins": np.random.randint(0, 50, 100),
    "losses": np.random.randint(0, 30, 100),
    "rating": np.random.uniform(3.0, 5.0, 100).round(1),
    "signature_dish": [fake.word().capitalize() + " " + np.random.choice(['Risotto', 'Sushi', 'Steak', 'Tart']) for _ in range(100)],
})
save_table(spark, chefs_ic, CATALOG, "iron_chef", "chefs")

# 2. Ingredients
ingredients_ic = pd.DataFrame({
    "ingredient_id": [f"ING-{i+1:04d}" for i in range(250)],
    "ingredient_name": [fake.word().capitalize() for _ in range(250)],
    "category": np.random.choice(['Protein', 'Vegetable', 'Spice', 'Grain', 'Dairy', 'Seafood'], 250),
    "cost_per_kg": np.random.lognormal(2, 1, 250).round(2),
    "season": np.random.choice(['Spring', 'Summer', 'Fall', 'Winter', 'Year-round'], 250, p=[0.15, 0.15, 0.15, 0.15, 0.4]),
    "rarity": np.random.choice(['Common', 'Uncommon', 'Rare', 'Exotic'], 250, p=[0.5, 0.3, 0.15, 0.05]),
})
save_table(spark, ingredients_ic, CATALOG, "iron_chef", "ingredients")

# 3. Battles
battles_ic_data = []
chef_ids_ic = chefs_ic['chef_id'].tolist()
ingredient_ids_ic = ingredients_ic['ingredient_id'].tolist()

for i in range(180):
    chef1, chef2 = np.random.choice(chef_ids_ic, 2, replace=False)
    
    battles_ic_data.append({
        "battle_id": f"BATTLE-{i+1:04d}",
        "episode_number": i + 1,
        "chef1_id": chef1,
        "chef2_id": chef2,
        "secret_ingredient_id": np.random.choice(ingredient_ids_ic),
        "battle_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
        "winner_id": chef1 if np.random.random() > 0.5 else chef2,
        "chef1_score": round(np.random.uniform(70, 100), 1),
        "chef2_score": round(np.random.uniform(70, 100), 1),
    })
battles_ic = pd.DataFrame(battles_ic_data)
save_table(spark, battles_ic, CATALOG, "iron_chef", "battles")

# 4. Judges
judges_ic = pd.DataFrame({
    "judge_id": [f"JUDGE-{i+1:02d}" for i in range(40)],
    "name": [fake.name() for _ in range(40)],
    "expertise": np.random.choice(['Fine Dining', 'Food Critic', 'Chef', 'Sommelier', 'Restaurateur'], 40),
    "episodes_judged": np.random.randint(5, 150, 40),
    "strictness": np.random.uniform(1, 10, 40).round(1),
})
save_table(spark, judges_ic, CATALOG, "iron_chef", "judges")

# 5. Episodes
episodes_ic = pd.DataFrame({
    "episode_id": [f"EP-{i+1:04d}" for i in range(180)],
    "battle_id": battles_ic['battle_id'].tolist(),
    "season": np.random.randint(1, 15, 180),
    "episode_number_in_season": np.random.randint(1, 26, 180),
    "air_date": battles_ic['battle_date'].tolist(),
    "viewership_millions": np.random.uniform(0.5, 3.5, 180).round(2),
    "theme": np.random.choice(['Italian Week', 'Seafood', 'Vegetarian', 'Holiday Special', 'Dessert'], 180),
})
save_table(spark, episodes_ic, CATALOG, "iron_chef", "episodes")

# =============================================================================
# JAPANESE ANIME SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Japanese Anime data...")
print(f"{'='*80}")

# 1. Series
series_anime = pd.DataFrame({
    "series_id": [f"ANI-{i+1:04d}" for i in range(120)],
    "title": [fake.catch_phrase().title() for _ in range(120)],
    "genre": np.random.choice(['Shonen', 'Seinen', 'Shojo', 'Mecha', 'Isekai', 'Slice of Life'], 120),
    "episodes": np.random.choice([12, 24, 26, 52, 100, 200, 500], 120, p=[0.3, 0.25, 0.15, 0.1, 0.1, 0.05, 0.05]),
    "first_aired": [fake.date_between(start_date='-10y', end_date=END_DATE) for _ in range(120)],
    "status": np.random.choice(['Ongoing', 'Completed', 'Cancelled'], 120, p=[0.3, 0.6, 0.1]),
})
save_table(spark, series_anime, CATALOG, "japanese_anime", "series")

# 2. Studios
studios_anime = pd.DataFrame({
    "studio_id": [f"STU-{i+1:02d}" for i in range(40)],
    "studio_name": [fake.company() + " Animation" for _ in range(40)],
    "founded_year": np.random.randint(1970, 2020, 40),
    "num_series": np.random.randint(5, 100, 40),
    "headquarters": np.random.choice(['Tokyo', 'Kyoto', 'Osaka', 'Saitama'], 40),
    "employees": np.random.randint(50, 1000, 40),
})
save_table(spark, studios_anime, CATALOG, "japanese_anime", "studios")

# 3. Characters
characters_anime_data = []
series_ids_anime = series_anime['series_id'].tolist()

for i in range(400):
    characters_anime_data.append({
        "character_id": f"CHAR-{i+1:05d}",
        "series_id": np.random.choice(series_ids_anime),
        "name": fake.name(),
        "role": np.random.choice(['Protagonist', 'Antagonist', 'Supporting', 'Comic Relief'], p=[0.15, 0.1, 0.65, 0.1]),
        "age": np.random.randint(10, 80) if np.random.random() > 0.1 else None,
        "power_level": np.random.randint(1, 9999) if np.random.random() > 0.4 else None,
    })
characters_anime = pd.DataFrame(characters_anime_data)
save_table(spark, characters_anime, CATALOG, "japanese_anime", "characters")

# 4. Episodes
episodes_anime_data = []
for series_id in series_ids_anime[:50]:  # Sample 50 series
    num_eps = int(series_anime[series_anime['series_id'] == series_id]['episodes'].iloc[0])
    for ep_num in range(min(num_eps, 26)):  # Cap at 26 for data volume
        episodes_anime_data.append({
            "episode_id": f"ANIEP-{len(episodes_anime_data)+1:05d}",
            "series_id": series_id,
            "episode_number": ep_num + 1,
            "title": fake.catch_phrase(),
            "air_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
            "duration_minutes": np.random.choice([24, 25, 48, 50]),
        })
episodes_anime = pd.DataFrame(episodes_anime_data)
save_table(spark, episodes_anime, CATALOG, "japanese_anime", "episodes")

# 5. Ratings
ratings_anime_data = []
for i in range(300):
    ratings_anime_data.append({
        "rating_id": f"RATING-{i+1:05d}",
        "series_id": np.random.choice(series_ids_anime),
        "user_id": f"USER-{np.random.randint(1, 1000):05d}",
        "score": np.random.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], p=[0.01, 0.02, 0.03, 0.05, 0.08, 0.12, 0.18, 0.25, 0.18, 0.08]),
        "review_text": fake.sentence() if np.random.random() > 0.5 else None,
        "rating_date": fake.date_between(start_date=START_DATE, end_date=END_DATE),
    })
ratings_anime = pd.DataFrame(ratings_anime_data)
save_table(spark, ratings_anime, CATALOG, "japanese_anime", "ratings")

# =============================================================================
# ROCK BANDS SCHEMA
# =============================================================================
print(f"\n{'='*80}")
print("Generating Rock Bands data...")
print(f"{'='*80}")

# Famous rock bands
ROCK_BANDS = [
    "The Rolling Stones", "Led Zeppelin", "Pink Floyd", "The Beatles", "Queen",
    "AC/DC", "Nirvana", "Metallica", "Guns N' Roses", "Aerosmith",
    "The Who", "The Eagles", "Van Halen", "Black Sabbath", "Deep Purple",
    "The Doors", "Kiss", "ZZ Top", "Lynyrd Skynyrd", "Tom Petty and the Heartbreakers"
]

# 1. Bands
bands_rock = pd.DataFrame({
    "band_id": [f"BAND-{i+1:02d}" for i in range(len(ROCK_BANDS))],
    "band_name": ROCK_BANDS,
    "formed_year": np.random.randint(1960, 2000, len(ROCK_BANDS)),
    "genre": np.random.choice(['Classic Rock', 'Hard Rock', 'Heavy Metal', 'Psychedelic', 'Progressive'], len(ROCK_BANDS)),
    "active": np.random.choice([True, False], len(ROCK_BANDS), p=[0.6, 0.4]),
    "country": np.random.choice(['USA', 'UK', 'Australia', 'Canada'], len(ROCK_BANDS), p=[0.6, 0.3, 0.07, 0.03]),
    "members_count": np.random.randint(3, 6, len(ROCK_BANDS)),
})
save_table(spark, bands_rock, CATALOG, "rock_bands", "bands")

# 2. Albums
albums_rock_data = []
band_ids_rock = bands_rock['band_id'].tolist()

for band_id in band_ids_rock:
    num_albums = np.random.randint(5, 20)
    for i in range(num_albums):
        albums_rock_data.append({
            "album_id": f"ALB-{len(albums_rock_data)+1:04d}",
            "band_id": band_id,
            "album_title": fake.catch_phrase(),
            "release_year": np.random.randint(1965, 2025),
            "label": fake.company() + " Records",
            "sales_millions": np.random.lognormal(0, 1.5, 1)[0].round(2),
            "format": np.random.choice(['Vinyl', 'CD', 'Digital', 'Streaming'], p=[0.1, 0.2, 0.3, 0.4]),
        })
albums_rock = pd.DataFrame(albums_rock_data)
save_table(spark, albums_rock, CATALOG, "rock_bands", "albums")

# 3. Songs
songs_rock_data = []
album_ids_rock = albums_rock['album_id'].tolist()

for album_id in album_ids_rock[:150]:  # Sample 150 albums
    num_songs = np.random.randint(8, 15)
    for track_num in range(num_songs):
        songs_rock_data.append({
            "song_id": f"SONG-{len(songs_rock_data)+1:05d}",
            "album_id": album_id,
            "band_id": albums_rock[albums_rock['album_id'] == album_id]['band_id'].iloc[0],
            "song_title": fake.catch_phrase(),
            "track_number": track_num + 1,
            "duration_seconds": np.random.randint(120, 480),
            "plays_millions": np.random.lognormal(0, 2, 1)[0].round(2),
        })
songs_rock = pd.DataFrame(songs_rock_data)
save_table(spark, songs_rock, CATALOG, "rock_bands", "songs")

# 4. Tours
tours_rock_data = []
for band_id in band_ids_rock:
    num_tours = np.random.randint(2, 10)
    for i in range(num_tours):
        tours_rock_data.append({
            "tour_id": f"TOUR-{len(tours_rock_data)+1:04d}",
            "band_id": band_id,
            "tour_name": fake.catch_phrase() + " Tour",
            "start_date": fake.date_between(start_date='-5y', end_date=END_DATE),
            "end_date": fake.date_between(start_date=END_DATE, end_date='+1y'),
            "num_shows": np.random.randint(20, 150),
            "revenue_millions": np.random.lognormal(2, 1.5, 1)[0].round(2),
        })
tours_rock = pd.DataFrame(tours_rock_data)
save_table(spark, tours_rock, CATALOG, "rock_bands", "tours")

# 5. Awards
awards_rock_data = []
album_ids_for_awards = albums_rock['album_id'].tolist()

for i in range(200):
    awards_rock_data.append({
        "award_id": f"AWD-{i+1:04d}",
        "band_id": np.random.choice(band_ids_rock),
        "album_id": np.random.choice(album_ids_for_awards) if np.random.random() > 0.3 else None,
        "award_name": np.random.choice(['Grammy', 'MTV Video Music Award', 'American Music Award', 'Rock and Roll Hall of Fame']),
        "category": np.random.choice(['Album of the Year', 'Best Rock Performance', 'Best Rock Album', 'Lifetime Achievement'], p=[0.15, 0.4, 0.35, 0.1]),
        "year": np.random.randint(1970, 2026),
        "won": np.random.choice([True, False], p=[0.6, 0.4]),
    })
awards_rock = pd.DataFrame(awards_rock_data)
save_table(spark, awards_rock, CATALOG, "rock_bands", "awards")

print(f"\n{'='*80}")
print(f"✓ Entertainment data generation complete!")
print(f"  - Iron Chef: 5 tables")
print(f"  - Japanese Anime: 5 tables")
print(f"  - Rock Bands: 5 tables")
print(f"{'='*80}")
