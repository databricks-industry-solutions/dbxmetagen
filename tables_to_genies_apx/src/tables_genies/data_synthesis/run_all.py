"""
Orchestrator: Run all domain data generators.

This script imports and executes all domain generation scripts sequentially.
Run this file via run_python_file_on_databricks MCP tool after installing faker and holidays.
"""

print("="*80)
print("STARTING COMPREHENSIVE DATA SYNTHESIS")
print("="*80)
print()

# Import and run all domain generators
print("1/9: Generating Sports Analytics data...")
import gen_sports

print("\n2/9: Generating Science Research data...")
import gen_science

print("\n3/9: Generating AI Tech data...")
import gen_ai_tech

print("\n4/9: Generating Health & Nutrition data...")
import gen_health

print("\n5/9: Generating Entertainment data...")
import gen_entertainment

print("\n6/9: Generating Insurance Claims data...")
import gen_insurance

print("\n7/9: Generating History data...")
import gen_history

print("\n8/9: Generating Global Policy data...")
import gen_global_policy

print("\n9/9: Generating Mixed Domain data...")
import gen_mixed

print()
print("="*80)
print("✓ ALL DATA SYNTHESIS COMPLETE!")
print("="*80)
print()
print("Summary:")
print("  - sports_analytics (world_cup_2026, nfl, nba): 15 tables")
print("  - science_research (nasa, drug_discovery, semiconductors): 15 tables")
print("  - ai_tech (genai): 5 tables")
print("  - health_nutrition (nutrition, pharmaceuticals): 10 tables")
print("  - entertainment (iron_chef, japanese_anime, rock_bands): 15 tables")
print("  - insurance_claims (claims, providers): 10 tables")
print("  - history (world_war_2, roman_history): 10 tables")
print("  - global_policy (international_policy): 5 tables")
print("  - serverless_dbx_unifiedchat_catalog (demo_mixed): 3 tables")
print()
print("TOTAL: 88 tables across 20 domains in 9 catalogs")
print("="*80)
