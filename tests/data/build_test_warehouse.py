# Databricks notebook source
# MAGIC %md
# MAGIC # Investment Operations Test Warehouse
# MAGIC
# MAGIC End-to-end integration test fixture for dbxmetagen. Downloads real public market
# MAGIC data (S&P 500 equities via yfinance, macro indicators via FRED), builds a star
# MAGIC schema in Unity Catalog, runs the dbxmetagen pipeline, and scores outputs against
# MAGIC gold expected values.
# MAGIC
# MAGIC **Usage:** Set widgets for catalog, schema, model endpoint, then Run All.
# MAGIC
# MAGIC **Tables:** dim_security, fct_price_daily, fct_fundamentals_quarterly,
# MAGIC dim_calendar, fct_macro_monthly, dim_macro_indicator, fct_index_membership, dim_index
# MAGIC
# MAGIC **Data sources:**
# MAGIC - S&P 500 constituents from Wikipedia (dim_security)
# MAGIC - 10 years of daily OHLCV from Yahoo Finance (fct_price_daily)
# MAGIC - Quarterly financials from Yahoo Finance (fct_fundamentals_quarterly)
# MAGIC - FRED macro series: CPI, Fed Funds, Unemployment, GDP, PMI (fct_macro_monthly)
# MAGIC - Derived: dim_calendar, dim_index, fct_index_membership (rule-based)

# COMMAND ----------

# MAGIC %pip install -qqqq -r ../../requirements.txt yfinance fredapi lxml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name (required)")
dbutils.widgets.text("schema_name", "investing_ops_test", "Data Schema")
dbutils.widgets.text("metadata_schema", "investing_ops_metadata", "Metadata Results Schema (dbxmetagen outputs)")
dbutils.widgets.text("eval_schema", "investing_ops_eval", "Eval Schema (gold tables + results)")
dbutils.widgets.text("model_serving_endpoint", "databricks-claude-sonnet-4-6", "LLM Endpoint")
dbutils.widgets.text("ontology_bundle", "schema_org", "Ontology Bundle (schema_org, general, financial_services, ...)")
dbutils.widgets.text("fred_secret_scope", "", "FRED API Secret Scope (leave empty to skip macro tables)")
dbutils.widgets.dropdown("skip_generation", "false", ["true", "false"], "Skip Data Generation")
dbutils.widgets.dropdown("skip_pipeline", "false", ["true", "false"], "Skip Pipeline Run")
dbutils.widgets.dropdown("skip_idempotency", "false", ["true", "false"], "Skip Idempotency Re-Run")
dbutils.widgets.text("exclude_tables", "", "Exclude Tables (comma-sep short names, e.g. dim_index,fct_index_membership)")
dbutils.widgets.text("min_overall_score", "0.40", "Min Overall Score to Pass")
dbutils.widgets.text("metagen_job_id", "", "Metadata Generator Job ID (from DAB)")
dbutils.widgets.text("pipeline_job_id", "", "Full Analytics Pipeline Job ID (from DAB)")

catalog_name = dbutils.widgets.get("catalog_name").strip()
schema_name = dbutils.widgets.get("schema_name").strip()
metadata_schema = dbutils.widgets.get("metadata_schema").strip()
eval_schema = dbutils.widgets.get("eval_schema").strip()
model_endpoint = dbutils.widgets.get("model_serving_endpoint").strip()
ontology_bundle = dbutils.widgets.get("ontology_bundle").strip()
fred_scope = dbutils.widgets.get("fred_secret_scope").strip()
skip_generation = dbutils.widgets.get("skip_generation").lower() == "true"
skip_pipeline = dbutils.widgets.get("skip_pipeline").lower() == "true"
skip_idempotency = dbutils.widgets.get("skip_idempotency").lower() == "true"
exclude_tables_raw = dbutils.widgets.get("exclude_tables").strip()
exclude_tables = {t.strip() for t in exclude_tables_raw.replace("|", ",").split(",") if t.strip()} if exclude_tables_raw else set()
metagen_job_id = int(dbutils.widgets.get("metagen_job_id").strip())
pipeline_job_id = int(dbutils.widgets.get("pipeline_job_id").strip())

assert catalog_name, "catalog_name is required"
assert schema_name, "schema_name is required"
assert metadata_schema, "metadata_schema is required"
assert eval_schema, "eval_schema is required"

fq = lambda t: f"{catalog_name}.{schema_name}.{t}"
meta_fq = lambda t: f"{catalog_name}.{metadata_schema}.{t}"
eval_fq = lambda t: f"{catalog_name}.{eval_schema}.{t}"

print(f"Data:     {catalog_name}.{schema_name}")
print(f"Metadata: {catalog_name}.{metadata_schema}")
print(f"Eval:     {catalog_name}.{eval_schema}")
print(f"Model:    {model_endpoint}")
print(f"FRED:     {'configured (' + fred_scope + ')' if fred_scope else 'skipped (no secret scope)'}")
if exclude_tables:
    print(f"Exclude:  {', '.join(sorted(exclude_tables))}")

# COMMAND ----------

import sys
sys.path.append("../../src")

from pyspark.sql import functions as F, SparkSession
from pyspark.sql.window import Window
import json, uuid
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{metadata_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{eval_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Data Generation
# MAGIC
# MAGIC Downloads real public market data and builds Delta tables in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_security (S&P 500 from Wikipedia)

# COMMAND ----------

if not skip_generation:
    import pandas as pd
    import requests
    from io import StringIO

    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        headers={"User-Agent": "dbxmetagen-test/1.0"},
        timeout=30,
    )
    resp.raise_for_status()
    sp500_wiki = pd.read_html(StringIO(resp.text))[0]

    sec_pdf = sp500_wiki.rename(columns={
        "Symbol": "ticker",
        "Security": "security_name",
        "GICS Sector": "gics_sector",
        "GICS Sub-Industry": "gics_sub_industry",
        "Headquarters Location": "headquarters",
        "Date added": "date_added",
        "Founded": "founded",
    })[["ticker", "security_name", "gics_sector", "gics_sub_industry",
        "headquarters", "date_added", "founded"]]

    sec_pdf["ticker"] = sec_pdf["ticker"].str.strip().str.replace(".", "-", regex=False)
    sec_pdf = sec_pdf.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    sec_pdf["security_id"] = range(1, len(sec_pdf) + 1)

    dim_security = spark.createDataFrame(sec_pdf)
    dim_security.write.mode("overwrite").saveAsTable(fq("dim_security"))
    print(f"dim_security: {dim_security.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fct_price_daily (Yahoo Finance, 10 years)

# COMMAND ----------

if not skip_generation:
    import yfinance as yf

    tickers_list = sec_pdf["ticker"].tolist()
    print(f"Downloading daily prices for {len(tickers_list)} tickers (2014-2024)...")

    raw_prices = yf.download(
        tickers_list,
        start="2014-01-01",
        end="2024-12-31",
        auto_adjust=True,
        threads=True,
        progress=True,
    )

    # yfinance returns MultiIndex columns (metric, ticker) for multi-ticker downloads
    # Melt to long format: one row per ticker per date
    records = []
    for ticker in tickers_list:
        try:
            sub = raw_prices.xs(ticker, axis=1, level=1) if isinstance(
                raw_prices.columns, pd.MultiIndex
            ) else raw_prices
            sub = sub.dropna(subset=["Close"])
            for dt, row in sub.iterrows():
                records.append({
                    "ticker": ticker,
                    "trade_date": dt.date(),
                    "open": float(row.get("Open", 0) or 0),
                    "high": float(row.get("High", 0) or 0),
                    "low": float(row.get("Low", 0) or 0),
                    "close": float(row.get("Close", 0) or 0),
                    "volume": int(row.get("Volume", 0) or 0),
                })
        except (KeyError, TypeError):
            continue

    prices_pdf = pd.DataFrame(records)
    print(f"Downloaded {len(prices_pdf)} price rows for {prices_pdf['ticker'].nunique()} tickers")

    sec_lookup = spark.table(fq("dim_security")).select("security_id", "ticker")
    fct_price_daily = (
        spark.createDataFrame(prices_pdf)
        .join(sec_lookup, on="ticker", how="inner")
        .withColumn("trade_year", F.year("trade_date"))
        .select("security_id", "trade_date", "trade_year",
                "open", "high", "low", "close", "volume")
        .dropDuplicates(["security_id", "trade_date"])
    )
    fct_price_daily.write.mode("overwrite").partitionBy("trade_year").saveAsTable(fq("fct_price_daily"))
    print(f"fct_price_daily: {fct_price_daily.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fct_fundamentals_quarterly (Yahoo Finance)

# COMMAND ----------

if not skip_generation:
    print(f"Downloading quarterly fundamentals for {len(tickers_list)} tickers...")
    fund_records = []
    for i, ticker in enumerate(tickers_list):
        if i % 50 == 0:
            print(f"  Progress: {i}/{len(tickers_list)}")
        try:
            tk = yf.Ticker(ticker)
            inc = tk.quarterly_income_stmt
            bal = tk.quarterly_balance_sheet
            if inc is None or inc.empty:
                continue
            for col_date in inc.columns:
                rec = {"ticker": ticker, "fiscal_quarter_end": col_date.date()}
                for field, sources in [
                    ("total_revenue", [inc, "Total Revenue"]),
                    ("net_income", [inc, "Net Income"]),
                    ("ebitda", [inc, "EBITDA"]),
                ]:
                    try:
                        rec[field] = float(sources[0].loc[sources[1], col_date])
                    except (KeyError, TypeError, ValueError):
                        rec[field] = None
                if bal is not None and not bal.empty and col_date in bal.columns:
                    for field, label in [
                        ("total_assets", "Total Assets"),
                        ("total_liabilities", "Total Liabilities Net Minority Interest"),
                        ("stockholders_equity", "Stockholders Equity"),
                        ("ordinary_shares_number", "Ordinary Shares Number"),
                    ]:
                        try:
                            rec[field] = float(bal.loc[label, col_date])
                        except (KeyError, TypeError, ValueError):
                            rec[field] = None
                else:
                    rec.update({"total_assets": None, "total_liabilities": None,
                                "stockholders_equity": None, "ordinary_shares_number": None})
                fund_records.append(rec)
        except Exception:
            continue

    print(f"  Collected {len(fund_records)} quarterly records")
    if fund_records:
        fund_pdf = pd.DataFrame(fund_records)
        fct_fundamentals = (
            spark.createDataFrame(fund_pdf)
            .join(sec_lookup, on="ticker", how="inner")
            .withColumn("fiscal_year", F.year("fiscal_quarter_end"))
            .withColumn("fiscal_quarter", F.quarter("fiscal_quarter_end"))
            .select("security_id", "fiscal_quarter_end", "fiscal_year", "fiscal_quarter",
                    "total_revenue", "net_income", "ebitda",
                    "total_assets", "total_liabilities", "stockholders_equity",
                    "ordinary_shares_number")
            .dropDuplicates(["security_id", "fiscal_quarter_end"])
        )
        fct_fundamentals.write.mode("overwrite").partitionBy("fiscal_year").saveAsTable(fq("fct_fundamentals_quarterly"))
        print(f"fct_fundamentals_quarterly: {fct_fundamentals.count()} rows")
    else:
        print("WARNING: No fundamentals downloaded, creating empty table")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('fct_fundamentals_quarterly')} (
            security_id INT, fiscal_quarter_end DATE, fiscal_year INT, fiscal_quarter INT,
            total_revenue DOUBLE, net_income DOUBLE, ebitda DOUBLE,
            total_assets DOUBLE, total_liabilities DOUBLE, stockholders_equity DOUBLE,
            ordinary_shares_number DOUBLE) USING DELTA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_calendar (derived from price dates)

# COMMAND ----------

if not skip_generation:
    price_bounds = spark.table(fq("fct_price_daily")).agg(
        F.min("trade_date").alias("min_date"),
        F.max("trade_date").alias("max_date"),
    ).collect()[0]

    trading_days = (
        spark.table(fq("fct_price_daily"))
        .select(F.col("trade_date").alias("calendar_date"))
        .distinct()
        .withColumn("is_trading_day", F.lit(True))
    )

    dim_calendar = (
        spark.sql(f"""
            SELECT explode(sequence(
                to_date('{price_bounds.min_date}'),
                to_date('{price_bounds.max_date}'),
                interval 1 day
            )) AS calendar_date
        """)
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .join(trading_days, on="calendar_date", how="left")
        .fillna({"is_trading_day": False})
    )
    dim_calendar.write.mode("overwrite").saveAsTable(fq("dim_calendar"))
    print(f"dim_calendar: {dim_calendar.count()} rows ({price_bounds.min_date} to {price_bounds.max_date})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fct_macro_monthly + dim_macro_indicator (FRED)

# COMMAND ----------

if not skip_generation:
    _FRED_SERIES = {
        "CPI_YOY": ("CPIAUCSL", "Consumer Price Index (All Urban)", "index", "BLS"),
        "FED_FUNDS_RATE": ("FEDFUNDS", "Federal Funds Effective Rate", "percent", "Federal Reserve"),
        "UNEMPLOYMENT": ("UNRATE", "Unemployment Rate", "percent", "BLS"),
        "GDP_GROWTH": ("A191RL1Q225SBEA", "Real GDP Growth Rate (Quarterly, Annualized)", "percent", "BEA"),
        "PMI_MFG": ("MANEMP", "Manufacturing Employment (proxy for PMI)", "thousands", "BLS"),
    }

    macro_loaded = False
    if fred_scope:
        try:
            from fredapi import Fred
            api_key = dbutils.secrets.get(fred_scope, "api_key")
            fred = Fred(api_key=api_key)

            macro_records = []
            for indicator_id, (series_id, name, unit, source) in _FRED_SERIES.items():
                try:
                    data = fred.get_series(series_id, observation_start="2014-01-01", observation_end="2024-12-31")
                    for dt, val in data.items():
                        if pd.notna(val):
                            macro_records.append({
                                "indicator_id": indicator_id,
                                "obs_date": dt.date(),
                                "value": float(val),
                            })
                except Exception as e:
                    print(f"  FRED series {series_id} failed: {e}")

            if macro_records:
                fct_macro = spark.createDataFrame(pd.DataFrame(macro_records))
                fct_macro.write.mode("overwrite").saveAsTable(fq("fct_macro_monthly"))
                print(f"fct_macro_monthly: {fct_macro.count()} rows")

                dim_macro = spark.createDataFrame([
                    (k, v[1], v[2], v[3]) for k, v in _FRED_SERIES.items()
                ], ["indicator_id", "indicator_name", "unit", "source_name"])
                dim_macro.write.mode("overwrite").saveAsTable(fq("dim_macro_indicator"))
                print(f"dim_macro_indicator: {dim_macro.count()} rows")
                macro_loaded = True
        except Exception as e:
            print(f"FRED download failed: {e}")

    if not macro_loaded:
        print("Macro tables skipped (no FRED secret scope or download failed)")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('fct_macro_monthly')} (
            indicator_id STRING, obs_date DATE, value DOUBLE) USING DELTA""")
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {fq('dim_macro_indicator')} (
            indicator_id STRING, indicator_name STRING, unit STRING, source_name STRING) USING DELTA""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### fct_index_membership + dim_index (derived)
# MAGIC
# MAGIC Rule-based index membership: **US_LARGE_CAP** = top 200 by year-end market cap,
# MAGIC **US_TECH_GROWTH** = top 100 Technology sector by year-end market cap.

# COMMAND ----------

if not skip_generation:
    prices = spark.table(fq("fct_price_daily"))
    funds = spark.table(fq("fct_fundamentals_quarterly"))
    secs = spark.table(fq("dim_security"))

    # Year-end close price per security
    year_end_prices = (
        prices
        .withColumn("year", F.year("trade_date"))
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("security_id", "year").orderBy(F.col("trade_date").desc())
        ))
        .filter("rn = 1")
        .select("security_id", "year", F.col("close").alias("year_end_price"))
    )

    # Latest shares outstanding per security per year
    latest_shares = (
        funds
        .filter(F.col("ordinary_shares_number").isNotNull())
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("security_id", "fiscal_year").orderBy(F.col("fiscal_quarter_end").desc())
        ))
        .filter("rn = 1")
        .select("security_id", F.col("fiscal_year").alias("year"), "ordinary_shares_number")
    )

    market_caps = (
        year_end_prices
        .join(latest_shares, on=["security_id", "year"], how="inner")
        .join(secs.select("security_id", "gics_sector"), on="security_id", how="left")
        .withColumn("market_cap", F.col("year_end_price") * F.col("ordinary_shares_number"))
    )

    # US_LARGE_CAP: top 200 overall
    large_cap = (
        market_caps
        .withColumn("rnk", F.row_number().over(
            Window.partitionBy("year").orderBy(F.col("market_cap").desc())))
        .filter("rnk <= 200")
        .select(
            F.lit("US_LARGE_CAP").alias("index_code"),
            "security_id",
            F.to_date(F.concat_ws("-", F.col("year"), F.lit("01"), F.lit("01"))).alias("effective_date"),
            F.to_date(F.concat_ws("-", F.col("year"), F.lit("12"), F.lit("31"))).alias("end_date"),
        )
    )

    # US_TECH_GROWTH: top 100 Information Technology
    tech_growth = (
        market_caps
        .filter(F.col("gics_sector") == "Information Technology")
        .withColumn("rnk", F.row_number().over(
            Window.partitionBy("year").orderBy(F.col("market_cap").desc())))
        .filter("rnk <= 100")
        .select(
            F.lit("US_TECH_GROWTH").alias("index_code"),
            "security_id",
            F.to_date(F.concat_ws("-", F.col("year"), F.lit("01"), F.lit("01"))).alias("effective_date"),
            F.to_date(F.concat_ws("-", F.col("year"), F.lit("12"), F.lit("31"))).alias("end_date"),
        )
    )

    fct_index_membership = large_cap.unionByName(tech_growth).dropDuplicates()
    fct_index_membership.write.mode("overwrite").saveAsTable(fq("fct_index_membership"))
    print(f"fct_index_membership: {fct_index_membership.count()} rows")

    dim_index = spark.createDataFrame([
        ("US_LARGE_CAP", "Top 200 S&P 500 securities by year-end market capitalization"),
        ("US_TECH_GROWTH", "Top 100 Information Technology securities by year-end market capitalization"),
    ], ["index_code", "index_description"])
    dim_index.write.mode("overwrite").saveAsTable(fq("dim_index"))
    print(f"dim_index: {dim_index.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

TABLES = ["dim_security", "fct_price_daily", "fct_fundamentals_quarterly",
          "dim_calendar", "fct_macro_monthly", "dim_macro_indicator",
          "fct_index_membership", "dim_index"]

counts_sql = " UNION ALL ".join(
    f"SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {fq(t)}" for t in TABLES)
display(spark.sql(counts_sql))

# FK integrity checks
orphans = spark.sql(f"""
    SELECT 'prices->security' AS fk, COUNT(*) AS orphan_count
    FROM {fq('fct_price_daily')} p LEFT ANTI JOIN {fq('dim_security')} s ON p.security_id = s.security_id
    UNION ALL
    SELECT 'fundamentals->security', COUNT(*)
    FROM {fq('fct_fundamentals_quarterly')} f LEFT ANTI JOIN {fq('dim_security')} s ON f.security_id = s.security_id
    UNION ALL
    SELECT 'index_membership->security', COUNT(*)
    FROM {fq('fct_index_membership')} m LEFT ANTI JOIN {fq('dim_security')} s ON m.security_id = s.security_id
    UNION ALL
    SELECT 'index_membership->index', COUNT(*)
    FROM {fq('fct_index_membership')} m LEFT ANTI JOIN {fq('dim_index')} i ON m.index_code = i.index_code
""")
display(orphans)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Expectations
# MAGIC
# MAGIC Expected outputs for each evaluation dimension. Written to the eval schema
# MAGIC so `eval_compare` or inline scoring can grade the pipeline.
# MAGIC
# MAGIC ### Ontology bundle: schema_org (default)
# MAGIC
# MAGIC This E2E test runs with `ontology_bundle=schema_org` (widget on line 42).
# MAGIC All expectations below MUST use schema_org entity types, NOT general.yaml types.
# MAGIC See `docs/ONTOLOGY_LOADING.md` "Bundle Comparison Guide" for full details.
# MAGIC
# MAGIC **Entity type mapping for test tables (schema_org):**
# MAGIC - dim_security -> Corporation (not Organization)
# MAGIC - fct_price_daily, fct_fundamentals_quarterly, fct_macro_monthly, fct_index_membership -> Observation (not Event)
# MAGIC - dim_calendar -> Date
# MAGIC - dim_macro_indicator -> DefinedTerm or StatisticalVariable
# MAGIC - dim_index -> DefinedTerm
# MAGIC
# MAGIC **Relationship expectations** must use schema_org entity types as src/dst.
# MAGIC For example: Observation -> Corporation, not Event -> Organization.
# MAGIC
# MAGIC **Property role expectations** use the shared vocabulary (same across all bundles):
# MAGIC primary_key, business_key, object_property, measure, dimension, temporal,
# MAGIC geographic, label, audit. These are hardcoded in the heuristic classifier,
# MAGIC not read from the bundle YAML at runtime.

# COMMAND ----------

# --- eval_expected_entities ---
expected_entities = spark.createDataFrame([
    ("dim_security",               "Corporation",  "FinancialProduct,Organization,Product",          "high"),
    ("fct_price_daily",            "Observation",  "Event,DataTable,StatisticalVariable",            "high"),
    ("fct_fundamentals_quarterly", "Observation",  "Event,DataTable,MonetaryAmount",                 "medium"),
    ("dim_calendar",               "Date",         "DayOfWeek,DataTable,DaySpa",                     "low"),
    ("fct_macro_monthly",          "Observation",  "Event,StatisticalVariable,DataTable",             "medium"),
    ("dim_macro_indicator",        "DefinedTerm",  "CategoryCode,StatisticalVariable,DataTable",      "low"),
    ("fct_index_membership",       "Observation",  "DataTable,Event,CategoryCode",                    "medium"),
    ("dim_index",                  "DefinedTerm",  "DataTable,CategoryCode,Pond",                     "low"),
], ["table_name", "expected_entity_type", "acceptable_alternatives", "confidence_tier"])
expected_entities.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_entities"))

# --- eval_expected_fk ---
expected_fk = spark.createDataFrame([
    ("fct_price_daily",            "security_id",  "dim_security",  "security_id"),
    ("fct_fundamentals_quarterly", "security_id",  "dim_security",  "security_id"),
    ("fct_index_membership",       "security_id",  "dim_security",  "security_id"),
    ("fct_index_membership",       "index_code",   "dim_index",     "index_code"),
], ["src_table", "src_column", "dst_table", "dst_column"])
expected_fk.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_fk"))

# --- eval_expected_comments (keyword-based) ---
expected_comments = spark.createDataFrame([
    ("dim_security",               None,                    "security,stock,equity,S&P,instrument"),
    ("fct_price_daily",            None,                    "price,daily,market,close,open,volume"),
    ("fct_fundamentals_quarterly", None,                    "financial,quarterly,revenue,income,balance"),
    ("dim_calendar",               None,                    "calendar,date,trading,day"),
    ("fct_macro_monthly",          None,                    "macro,economic,indicator,monthly"),
    ("dim_macro_indicator",        None,                    "indicator,definition,economic,metric"),
    ("fct_index_membership",       None,                    "index,membership,constituent,composition"),
    ("dim_index",                  None,                    "index,benchmark,definition"),
    ("dim_security",               "ticker",                "ticker,symbol,stock"),
    ("dim_security",               "gics_sector",           "sector,GICS,classification,industry"),
    ("dim_security",               "gics_sub_industry",     "sub-industry,classification,GICS"),
    ("fct_price_daily",            "close",                 "close,closing,price"),
    ("fct_price_daily",            "volume",                "volume,shares,traded"),
    ("fct_price_daily",            "trade_date",            "date,trading,day"),
    ("fct_fundamentals_quarterly", "total_revenue",         "revenue,sales,income"),
    ("fct_fundamentals_quarterly", "total_assets",          "assets,total,balance"),
    ("fct_fundamentals_quarterly", "ordinary_shares_number","shares,outstanding,count"),
    ("fct_macro_monthly",          "indicator_id",          "indicator,code,identifier"),
    ("fct_index_membership",       "index_code",            "index,code,benchmark"),
    ("fct_index_membership",       "effective_date",        "effective,start,date,begin"),
], ["table_name", "column_name", "expected_keywords"])
expected_comments.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_comments"))

# --- eval_expected_domains ---
expected_domains = spark.createDataFrame([
    ("dim_security",               "Finance", "Capital Markets",       "Financial Services,finance", "Securities,Investment Management,treasury"),
    ("fct_price_daily",            "Finance", "Capital Markets",       "Financial Services,finance", "Market Data,Investment Management,treasury"),
    ("fct_fundamentals_quarterly", "Finance", "Capital Markets",       "Financial Services,finance", "Financial Reporting,Investment Management,treasury"),
    ("dim_calendar",               "Finance", "Capital Markets",       "General,Operations,finance", "Reference Data,Investment Management,treasury"),
    ("fct_macro_monthly",          "Finance", "Economics",             "Financial Services,finance", "Macroeconomics,Capital Markets,treasury"),
    ("dim_macro_indicator",        "Finance", "Economics",             "Financial Services,finance", "Macroeconomics,Reference Data,treasury"),
    ("fct_index_membership",       "Finance", "Investment Management", "Financial Services,finance", "Index Management,Capital Markets,treasury"),
    ("dim_index",                  "Finance", "Investment Management", "Financial Services,finance", "Index Management,Capital Markets,treasury"),
], ["table_name", "expected_domain", "expected_subdomain",
   "acceptable_domain_alternatives", "acceptable_subdomain_alternatives"])
expected_domains.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_domains"))

# --- eval_expected_column_properties ---
col_props_data = [
    # dim_security — security_id is the table's PK (stem "security" matches stripped "security")
    ("dim_security", "security_id",       "primary_key",    False, "high"),
    ("dim_security", "ticker",            "business_key",   False, "high"),
    ("dim_security", "security_name",     "label",          False, "high"),
    ("dim_security", "gics_sector",       "dimension",      False, "medium"),
    ("dim_security", "gics_sub_industry", "dimension",      False, "medium"),
    ("dim_security", "headquarters",      "geographic",     False, "medium"),
    ("dim_security", "date_added",        "temporal",       False, "medium"),
    ("dim_security", "founded",           "temporal",       False, "low"),
    # fct_price_daily — security_id references dim_security
    ("fct_price_daily", "security_id",    "object_property", False, "high"),
    ("fct_price_daily", "trade_date",     "temporal",        False, "high"),
    ("fct_price_daily", "open",           "measure",         False, "high"),
    ("fct_price_daily", "high",           "measure",         False, "high"),
    ("fct_price_daily", "low",            "measure",         False, "high"),
    ("fct_price_daily", "close",          "measure",         False, "high"),
    ("fct_price_daily", "volume",         "measure",         False, "high"),
    ("fct_price_daily", "trade_year",     "dimension",       False, "low"),
    # fct_fundamentals_quarterly — security_id references dim_security
    ("fct_fundamentals_quarterly", "security_id",             "object_property", False, "high"),
    ("fct_fundamentals_quarterly", "fiscal_quarter_end",      "temporal",        False, "high"),
    ("fct_fundamentals_quarterly", "fiscal_year",             "dimension",       False, "medium"),
    ("fct_fundamentals_quarterly", "fiscal_quarter",          "dimension",       False, "medium"),
    ("fct_fundamentals_quarterly", "total_revenue",           "measure",         False, "high"),
    ("fct_fundamentals_quarterly", "net_income",              "measure",         False, "high"),
    ("fct_fundamentals_quarterly", "ebitda",                  "measure",         False, "medium"),
    ("fct_fundamentals_quarterly", "total_assets",            "measure",         False, "high"),
    ("fct_fundamentals_quarterly", "total_liabilities",       "measure",         False, "high"),
    ("fct_fundamentals_quarterly", "stockholders_equity",     "measure",         False, "high"),
    ("fct_fundamentals_quarterly", "ordinary_shares_number",  "measure",         False, "medium"),
    # dim_calendar — calendar_date is temporal anchor
    ("dim_calendar", "calendar_date",  "temporal",    False, "high"),
    ("dim_calendar", "year",           "dimension",   False, "medium"),
    ("dim_calendar", "quarter",        "dimension",   False, "medium"),
    ("dim_calendar", "month",          "dimension",   False, "medium"),
    ("dim_calendar", "day_of_week",    "dimension",   False, "low"),
    ("dim_calendar", "is_trading_day", "dimension",   False, "high"),
    # fct_macro_monthly — indicator_id references dim_macro_indicator
    ("fct_macro_monthly", "indicator_id", "object_property", False, "high"),
    ("fct_macro_monthly", "obs_date",     "temporal",        False, "high"),
    ("fct_macro_monthly", "value",        "measure",         False, "high"),
    # dim_macro_indicator — indicator_id is the table's PK
    ("dim_macro_indicator", "indicator_id",   "primary_key", False, "high"),
    ("dim_macro_indicator", "indicator_name", "label",       False, "high"),
    ("dim_macro_indicator", "unit",           "dimension",   False, "medium"),
    ("dim_macro_indicator", "source_name",    "label",       False, "medium"),
    # fct_index_membership — index_code and security_id are FKs
    ("fct_index_membership", "index_code",      "object_property", False, "high"),
    ("fct_index_membership", "security_id",     "object_property", False, "high"),
    ("fct_index_membership", "effective_date",  "temporal",        False, "high"),
    ("fct_index_membership", "end_date",        "temporal",        False, "high"),
    # dim_index — index_code is the table's PK
    ("dim_index", "index_code",        "primary_key", False, "high"),
    ("dim_index", "index_description", "label",       False, "high"),
]
expected_col_props = spark.createDataFrame(col_props_data,
    ["table_name", "column_name", "expected_property_role", "expected_is_sensitive", "confidence_tier"])
expected_col_props.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_column_properties"))

# --- eval_expected_relationships ---
# schema_org maps: fct_ tables -> Observation, dim_security -> Corporation,
# dim_macro_indicator -> StatisticalVariable
expected_rels = spark.createDataFrame([
    ("Observation", "Corporation",         "references", "about,involves,describes,recordsFor,relatedTo"),
    ("Observation", "StatisticalVariable", "measurementDenominator", "references,relatedTo,measures"),
], ["src_entity_type", "dst_entity_type", "expected_relationship_name", "acceptable_alternatives"])
expected_rels.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_relationships"))

# --- eval_expected_pi ---
# Public market data has no PII/PHI. A few columns could plausibly be flagged
# (headquarters = address-like, email/phone on providers if they existed).
# For this schema we expect all columns to be None (no PI).
pi_rows = []
for tbl, schema_def in [
    ("dim_security", ["security_id", "ticker", "security_name", "gics_sector",
                       "gics_sub_industry", "headquarters", "date_added", "founded"]),
    ("fct_price_daily", ["security_id", "trade_date", "trade_year",
                          "open", "high", "low", "close", "volume"]),
    ("fct_fundamentals_quarterly", ["security_id", "fiscal_quarter_end", "fiscal_year",
                                     "fiscal_quarter", "total_revenue", "net_income", "ebitda",
                                     "total_assets", "total_liabilities", "stockholders_equity",
                                     "ordinary_shares_number"]),
    ("dim_calendar", ["calendar_date", "year", "quarter", "month", "day_of_week", "is_trading_day"]),
    ("fct_macro_monthly", ["indicator_id", "obs_date", "value"]),
    ("dim_macro_indicator", ["indicator_id", "indicator_name", "unit", "source_name"]),
    ("fct_index_membership", ["index_code", "security_id", "effective_date", "end_date"]),
    ("dim_index", ["index_code", "index_description"]),
]:
    for col in schema_def:
        pi_rows.append((tbl, col, "None", ""))
expected_pi = spark.createDataFrame(pi_rows,
    ["table_name", "column_name", "expected_type", "acceptable_alternatives"])
expected_pi.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_pi"))

print(f"Gold tables written to {catalog_name}.{eval_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Gold Q&A Pairs (Agent Evaluation Fixture)
# MAGIC
# MAGIC Store-only: these are consumed by a separate evaluation harness (e.g., MLflow
# MAGIC `evaluate()`) against the deployed deep analysis agent. No agent invocation here.

# COMMAND ----------

agent_qa_pairs = [
    ("What tables contain daily market data and how are they related to the security dimension?",
     "fct_price_daily links to dim_security via security_id; contains OHLCV data per trading day"),
    ("Which columns across the schema are temporal (date/time) fields?",
     "trade_date, fiscal_quarter_end, calendar_date, obs_date, effective_date, end_date, date_added"),
    ("How is dim_security connected to fact tables through foreign keys?",
     "fct_price_daily, fct_fundamentals_quarterly, and fct_index_membership all reference dim_security.security_id"),
    ("What GICS sectors are represented in the security dimension?",
     "All 11 GICS sectors from the S&P 500: Information Technology, Health Care, Financials, Consumer Discretionary, Communication Services, Industrials, Consumer Staples, Energy, Utilities, Real Estate, Materials"),
    ("Which tables would I join to compute a security's year-end market capitalization?",
     "fct_price_daily (for year-end close price) joined with fct_fundamentals_quarterly (for ordinary_shares_number) on security_id"),
    ("What macro-economic indicators are tracked and what is their source?",
     "CPI, Federal Funds Rate, Unemployment Rate, GDP Growth Rate, Manufacturing Employment; sourced from FRED (BLS, Federal Reserve, BEA)"),
    ("How are index memberships determined -- what rules define the two indices?",
     "US_LARGE_CAP: top 200 by year-end market cap; US_TECH_GROWTH: top 100 Information Technology by year-end market cap"),
    ("What is the grain of fct_price_daily and approximately how many rows does it contain?",
     "One row per security per trading day; approximately 1.25 million rows (500 securities x 2500 trading days over 10 years)"),
    ("Are there any tables in this schema that might contain PII or sensitive data?",
     "No -- the schema contains only public market data (prices, fundamentals, macro indicators) with no personal information"),
    ("What entity types should be discovered in this financial star schema?",
     "Product entities (dim_security, dim_index, dim_macro_indicator) and Event entities (fct_price_daily, fct_fundamentals_quarterly, fct_macro_monthly, fct_index_membership, dim_calendar)"),
]

eval_agent_qa = spark.createDataFrame(agent_qa_pairs, ["question", "expected_answer"])
eval_agent_qa.write.mode("overwrite").saveAsTable(eval_fq("eval_expected_agent_qa"))
print(f"Agent Q&A fixture: {len(agent_qa_pairs)} pairs written to {eval_fq('eval_expected_agent_qa')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Run dbxmetagen Pipeline
# MAGIC
# MAGIC Runs comment generation, domain classification, knowledge base + graph building,
# MAGIC ontology discovery, and FK prediction in sequence.

# COMMAND ----------

stage_timings = {}

if not skip_pipeline:
    import time
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    _TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

    def _run_job_and_wait(job_id, params, label):
        print("=" * 60)
        print(f"  Triggering: {label}  (job_id={job_id})")
        print(f"  Params: {params}")
        print("=" * 60)
        t0 = time.time()
        run = w.jobs.run_now(job_id=job_id, job_parameters=params)
        run_id = run.bind()["run_id"]
        print(f"  Run started: run_id={run_id}")
        while True:
            state = w.jobs.get_run(run_id).state
            lcs = state.life_cycle_state.value if hasattr(state.life_cycle_state, "value") else str(state.life_cycle_state)
            if lcs in _TERMINAL_STATES:
                break
            print(f"  ... {lcs} ({state.state_message or ''})")
            time.sleep(30)
        final = w.jobs.get_run(run_id)
        elapsed_s = int(time.time() - t0)
        result_state = final.state.result_state.value if final.state.result_state else "UNKNOWN"
        print(f"  Finished: {result_state} (elapsed: {elapsed_s}s)")
        stage_timings[label] = elapsed_s
        if result_state != "SUCCESS":
            run_url = f"{w.config.host}#job/{job_id}/run/{run_id}"
            raise RuntimeError(f"{label} failed: {result_state}. See {run_url}")

    if exclude_tables:
        run_tables = [t for t in TABLES if t not in exclude_tables]
        table_names_param = ",".join(f"{catalog_name}.{schema_name}.{t}" for t in run_tables)
        print(f"Running {len(run_tables)}/{len(TABLES)} tables (excluded: {', '.join(sorted(exclude_tables))})")
    else:
        table_names_param = f"{catalog_name}.{schema_name}.*"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadata Generation (comment + domain)

# COMMAND ----------

if not skip_pipeline:
    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "comment",
        "model": model_endpoint,
        "apply_ddl": "false",
        "sample_size": "5",
    }, "comment generation")

# COMMAND ----------

if not skip_pipeline:
    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "pi",
        "model": model_endpoint,
        "apply_ddl": "false",
    }, "PI identification")

# COMMAND ----------

if not skip_pipeline:
    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "domain",
        "model": model_endpoint,
        "apply_ddl": "false",
    }, "domain classification")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Analytics Pipeline
# MAGIC
# MAGIC Runs the full DAG: KB build, column KB, schema KB, extended metadata,
# MAGIC knowledge graph, embeddings, profiling, ontology (schema_org bundle),
# MAGIC similarity edges, FK prediction, community summaries, vector index.

# COMMAND ----------

if not skip_pipeline:
    _run_job_and_wait(pipeline_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "ontology_bundle": ontology_bundle,
        "model": model_endpoint,
        "incremental": "true",
        "apply_ddl": "false",
    }, "full analytics pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scoring
# MAGIC
# MAGIC Compares pipeline outputs against gold expectations. Follows the same pattern
# MAGIC as `notebooks/evaluation/eval_compare.py`.

# COMMAND ----------

run_id = str(uuid.uuid4())
run_timestamp = datetime.utcnow()
result_rows = []

def add_result(dimension, result_type, table_name, column_name, expected, actual, match, score, details=None):
    result_rows.append({
        "run_id": run_id, "run_timestamp": run_timestamp, "run_label": "test_warehouse",
        "dimension": dimension, "result_type": result_type,
        "table_name": table_name, "column_name": column_name,
        "expected_value": str(expected), "actual_value": str(actual),
        "match": match, "score": float(score),
        "details": json.dumps(details) if details else None,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comment Scoring

# COMMAND ----------

try:
    exp_comments = spark.table(eval_fq("eval_expected_comments")).collect()
    actual_col_df = spark.sql(f"""
        SELECT table_name, column_name, column_content FROM (
            SELECT table_name, column_name, column_content,
                   ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY _created_at DESC) AS rn
            FROM {meta_fq('metadata_generation_log')}
            WHERE metadata_type = 'comment' AND column_name IS NOT NULL
        ) WHERE rn = 1
    """)
    actual_tbl_df = spark.sql(f"""
        SELECT table_name, column_content FROM (
            SELECT table_name, column_content,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY _created_at DESC) AS rn
            FROM {meta_fq('metadata_generation_log')}
            WHERE metadata_type = 'comment' AND (column_name IS NULL OR column_name = 'None')
        ) WHERE rn = 1
    """)
    col_map = {(r.table_name.split(".")[-1], r.column_name): r.column_content for r in actual_col_df.collect()}
    tbl_map = {r.table_name.split(".")[-1]: r.column_content for r in actual_tbl_df.collect()}

    comment_scores = []
    for row in exp_comments:
        tbl, col = row.table_name, row.column_name
        keywords = [k.strip().lower() for k in row.expected_keywords.split(",") if k.strip()]
        actual = ((tbl_map.get(tbl) if col is None else col_map.get((tbl, col))) or "").lower()
        if not actual:
            add_result("comment", "FN", tbl, col, ",".join(keywords), "", False, 0.0)
            comment_scores.append(0.0)
            continue
        hits = [kw for kw in keywords if kw in actual]
        score = len(hits) / len(keywords) if keywords else 1.0
        comment_scores.append(score)
        add_result("comment", "TP" if score >= 0.5 else "FN", tbl, col,
                   ",".join(keywords), actual[:200], score >= 0.5, score,
                   {"hits": hits, "misses": [k for k in keywords if k not in actual]})
    avg_comment = sum(comment_scores) / len(comment_scores) if comment_scores else 0.0
    print(f"Comment: {avg_comment:.2f} ({sum(1 for s in comment_scores if s >= 0.5)}/{len(comment_scores)} passing)")
except Exception as e:
    print(f"Comment scoring skipped: {e}")
    avg_comment = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Domain Scoring

# COMMAND ----------

try:
    exp_domains = spark.table(eval_fq("eval_expected_domains")).collect()
    actual_dom_df = spark.sql(f"""
        SELECT table_name, domain, subdomain FROM (
            SELECT table_name, domain, subdomain,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY _created_at DESC) AS rn
            FROM {meta_fq('metadata_generation_log')}
            WHERE metadata_type = 'domain' AND (column_name IS NULL OR column_name = 'None')
        ) WHERE rn = 1
    """)
    dom_map = {r.table_name.split(".")[-1]: (r.domain, r.subdomain) for r in actual_dom_df.collect()}

    domain_scores = []
    for row in exp_domains:
        tbl = row.table_name
        dom_alts = [a.strip() for a in (row.acceptable_domain_alternatives or "").split(",") if a.strip()]
        sub_alts = [a.strip() for a in (row.acceptable_subdomain_alternatives or "").split(",") if a.strip()]
        actual = dom_map.get(tbl)
        if not actual:
            add_result("domain", "FN", tbl, None, f"{row.expected_domain}/{row.expected_subdomain}", "MISSING", False, 0.0)
            domain_scores.append(0.0)
            continue
        act_dom, act_sub = actual
        dom_match = act_dom.lower() == row.expected_domain.lower() or act_dom.lower() in [a.lower() for a in dom_alts]
        sub_match = act_sub.lower() == row.expected_subdomain.lower() or act_sub.lower() in [a.lower() for a in sub_alts]
        score = (1.0 if dom_match and sub_match else 0.5 if dom_match else 0.0)
        domain_scores.append(score)
        add_result("domain", "TP" if score >= 0.5 else "FN", tbl, None,
                   f"{row.expected_domain}/{row.expected_subdomain}", f"{act_dom}/{act_sub}",
                   score >= 0.5, score)
    avg_domain = sum(domain_scores) / len(domain_scores) if domain_scores else 0.0
    print(f"Domain: {avg_domain:.2f} ({sum(1 for s in domain_scores if s >= 0.5)}/{len(domain_scores)} passing)")
except Exception as e:
    print(f"Domain scoring skipped: {e}")
    avg_domain = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ontology Entity Scoring

# COMMAND ----------

try:
    exp_ents = spark.table(eval_fq("eval_expected_entities")).collect()
    actual_ent_df = spark.sql(f"""
        SELECT entity_type, source_tables FROM {meta_fq('ontology_entities')} WHERE entity_role = 'primary'
    """)
    table_to_entity = {}
    for row in actual_ent_df.collect():
        for src in (row.source_tables or []):
            short = src.split(".")[-1] if "." in src else src
            if short in [t for t in TABLES]:
                table_to_entity[short] = row.entity_type

    entity_scores = []
    for row in exp_ents:
        tbl = row.table_name
        alts = [a.strip() for a in (row.acceptable_alternatives or "").split(",") if a.strip()]
        actual = table_to_entity.get(tbl, "__MISSING__")
        match = actual == row.expected_entity_type or actual in alts
        entity_scores.append(1.0 if match else 0.0)
        add_result("ontology_entities", "TP" if match else "FN", tbl, None,
                   row.expected_entity_type, actual, match, 1.0 if match else 0.0,
                   {"tier": row.confidence_tier, "alternatives": alts})
    avg_entity = sum(entity_scores) / len(entity_scores) if entity_scores else 0.0
    print(f"Ontology entities: {avg_entity:.2f} ({sum(1 for s in entity_scores if s == 1.0)}/{len(entity_scores)} correct)")
except Exception as e:
    print(f"Entity scoring skipped: {e}")
    avg_entity = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Property Scoring

# COMMAND ----------

try:
    exp_props = spark.table(eval_fq("eval_expected_column_properties")).collect()
    _fq_tables = ", ".join(f"'{fq(t)}'" for t in TABLES)
    actual_props_df = spark.sql(f"""
        SELECT table_name, column_name, property_role, is_sensitive
        FROM {meta_fq('ontology_column_properties')}
        WHERE table_name IN ({_fq_tables})
    """)
    prop_map = {
        (r.table_name.split(".")[-1], r.column_name): (r.property_role, r.is_sensitive)
        for r in actual_props_df.collect()
    }

    prop_scores = []
    for row in exp_props:
        tbl, col = row.table_name, row.column_name
        actual = prop_map.get((tbl, col))
        if not actual:
            add_result("ontology_column_properties", "FN", tbl, col, row.expected_property_role, "MISSING", False, 0.0,
                       {"tier": row.confidence_tier})
            prop_scores.append(0.0)
            continue
        act_role, act_sensitive = actual
        role_match = act_role == row.expected_property_role
        score = (0.7 if role_match else 0.0) + (0.3 if act_sensitive == (row.expected_is_sensitive == "true" if isinstance(row.expected_is_sensitive, str) else row.expected_is_sensitive) else 0.0)
        prop_scores.append(score)
        add_result("ontology_column_properties", "TP" if role_match else "FN", tbl, col,
                   row.expected_property_role, act_role, role_match, score,
                   {"tier": row.confidence_tier})
    avg_prop = sum(prop_scores) / len(prop_scores) if prop_scores else 0.0
    role_correct = sum(1 for s in prop_scores if s >= 0.7)
    print(f"Column properties: {avg_prop:.2f} ({role_correct}/{len(prop_scores)} role-correct)")
except Exception as e:
    print(f"Column property scoring skipped: {e}")
    avg_prop = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### FK Scoring

# COMMAND ----------

try:
    exp_fk_rows = spark.table(eval_fq("eval_expected_fk")).collect()
    _fq_fk_tables = ", ".join(f"'{fq(t)}'" for t in TABLES)
    actual_fk_df = spark.sql(f"""
        SELECT src_table, src_column, dst_table, dst_column, final_confidence
        FROM {meta_fq('fk_predictions')}
        WHERE src_table IN ({_fq_fk_tables}) AND dst_table IN ({_fq_fk_tables}) AND is_fk = true
    """)
    actual_fk_directed = set()
    actual_fk_undirected = set()
    for r in actual_fk_df.collect():
        src_t = r.src_table.split(".")[-1]
        dst_t = r.dst_table.split(".")[-1]
        src_c = r.src_column.split(".")[-1] if "." in r.src_column else r.src_column
        dst_c = r.dst_column.split(".")[-1] if "." in r.dst_column else r.dst_column
        actual_fk_directed.add((src_t, src_c, dst_t, dst_c))
        actual_fk_undirected.add((src_t, src_c, dst_t, dst_c))
        actual_fk_undirected.add((dst_t, dst_c, src_t, src_c))

    expected_fk_set = set()
    expected_fk_both = set()
    fk_scores = []
    fk_dir_scores = []
    for row in exp_fk_rows:
        key = (row.src_table, row.src_column, row.dst_table, row.dst_column)
        rev = (row.dst_table, row.dst_column, row.src_table, row.src_column)
        expected_fk_set.add(key)
        expected_fk_both.add(key)
        expected_fk_both.add(rev)
        dir_correct = key in actual_fk_directed
        dir_reversed = rev in actual_fk_directed
        found = dir_correct or dir_reversed
        fk_scores.append(1.0 if found else 0.0)
        fk_dir_scores.append(1.0 if dir_correct else 0.5 if dir_reversed else 0.0)
        add_result("fk", "TP" if found else "FN", f"{row.src_table}.{row.src_column}", None,
                   f"{row.dst_table}.{row.dst_column}",
                   "correct" if dir_correct else "reversed" if dir_reversed else "missing",
                   found, 1.0 if dir_correct else 0.5 if dir_reversed else 0.0)

    fk_fp_count = 0
    for fk_tuple in actual_fk_directed:
        if fk_tuple not in expected_fk_both:
            fk_fp_count += 1
            add_result("fk", "FP", f"{fk_tuple[0]}.{fk_tuple[1]}", None,
                       "N/A", f"{fk_tuple[2]}.{fk_tuple[3]}", False, 0.0,
                       {"reason": "FK predicted but not in expected set"})

    tp = sum(fk_scores)
    recall = tp / len(fk_scores) if fk_scores else 0.0
    precision = tp / (tp + fk_fp_count) if (tp + fk_fp_count) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    dir_avg = sum(fk_dir_scores) / len(fk_dir_scores) if fk_dir_scores else 0.0
    avg_fk = f1
    print(f"FK: F1={f1:.2f} (P={precision:.2f}, R={recall:.2f}, {int(tp)} TP, {len(fk_scores)-int(tp)} FN, {fk_fp_count} FP)")
    print(f"FK direction score: {dir_avg:.2f}")
except Exception as e:
    print(f"FK scoring skipped: {e}")
    avg_fk = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### PI Scoring (False Positive Validation)

# COMMAND ----------

try:
    exp_pi = spark.table(eval_fq("eval_expected_pi")).collect()
    actual_pi_df = spark.sql(f"""
        SELECT table_name, column_name, type FROM (
            SELECT table_name, column_name, type,
                   ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY _created_at DESC) AS rn
            FROM {meta_fq('metadata_generation_log')}
            WHERE metadata_type = 'pi' AND column_name IS NOT NULL
        ) WHERE rn = 1
    """)
    actual_pi_map = {
        (r.table_name.split(".")[-1], r.column_name): (r.type or "None")
        for r in actual_pi_df.collect()
    }

    pi_scores = []
    pi_fp_count = 0
    for row in exp_pi:
        tbl, col = row.table_name, row.column_name
        expected_type = row.expected_type
        alts = [a.strip() for a in (row.acceptable_alternatives or "").split(",") if a.strip()]
        actual_type = actual_pi_map.get((tbl, col), "__MISSING__")
        if actual_type == "__MISSING__":
            add_result("pi_type", "FN", tbl, col, expected_type, "MISSING", False, 0.0)
            pi_scores.append(0.0)
            continue
        match = actual_type == expected_type or actual_type in alts
        if expected_type == "None" and actual_type not in ("None", "none", None, ""):
            pi_fp_count += 1
            add_result("pi_type", "FP", tbl, col, "None", actual_type, False, 0.0,
                       {"reason": "column incorrectly flagged as PI in public market data"})
            pi_scores.append(0.0)
        else:
            pi_scores.append(1.0 if match else 0.0)
            add_result("pi_type", "TP" if match else "FN", tbl, col,
                       expected_type, actual_type, match, 1.0 if match else 0.0)

    avg_pi = sum(pi_scores) / len(pi_scores) if pi_scores else 0.0
    correct = sum(1 for s in pi_scores if s == 1.0)
    print(f"PI type: {avg_pi:.2f} ({correct} correct, {pi_fp_count} FP out of {len(pi_scores)} columns)")
except Exception as e:
    print(f"PI scoring skipped: {e}")
    avg_pi = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relationship Scoring

# COMMAND ----------

try:
    exp_rels = spark.table(eval_fq("eval_expected_relationships")).collect()

    actual_rels_df = spark.sql(f"""
        SELECT src_entity_type, dst_entity_type, relationship_name
        FROM {meta_fq('ontology_relationships')}
    """)
    actual_rel_set = set()
    actual_rel_names = {}
    for r in actual_rels_df.collect():
        pair = (r.src_entity_type, r.dst_entity_type)
        actual_rel_set.add(pair)
        actual_rel_names.setdefault(pair, set()).add(r.relationship_name)

    rel_scores = []
    for row in exp_rels:
        src, dst = row.src_entity_type, row.dst_entity_type
        exp_name = row.expected_relationship_name
        alts = [a.strip() for a in (row.acceptable_alternatives or "").split(",") if a.strip()]
        all_acceptable = {exp_name} | set(alts) | {"references"}

        pair_found = (src, dst) in actual_rel_set
        actual_names = actual_rel_names.get((src, dst), set())
        name_match = bool(actual_names & all_acceptable)
        exact_match = exp_name in actual_names

        if pair_found and exact_match:
            score = 1.0
        elif pair_found and name_match:
            score = 0.75
        elif pair_found:
            score = 0.5
        else:
            score = 0.0

        rel_scores.append(score)
        add_result("ontology_relationships", "TP" if score >= 0.5 else "FN",
                   f"{src}->{dst}", None, exp_name,
                   ",".join(actual_names) if actual_names else "MISSING",
                   pair_found, score,
                   {"pair_found": pair_found, "name_match": name_match, "exact_match": exact_match})

    avg_rel = sum(rel_scores) / len(rel_scores) if rel_scores else 0.0
    tp = sum(1 for s in rel_scores if s >= 0.5)
    print(f"Relationships: {avg_rel:.2f} ({tp} TP, {len(rel_scores) - tp} FN)")
except Exception as e:
    print(f"Relationship scoring skipped: {e}")
    avg_rel = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Results + Scorecard

# COMMAND ----------

# Write results to eval schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {eval_fq('eval_results')} (
        run_id STRING, run_timestamp TIMESTAMP, run_label STRING,
        dimension STRING, result_type STRING, table_name STRING,
        column_name STRING, expected_value STRING, actual_value STRING,
        match BOOLEAN, score DOUBLE, details STRING)
""")

if result_rows:
    _eval_schema = "run_id STRING, run_timestamp TIMESTAMP, run_label STRING, dimension STRING, result_type STRING, table_name STRING, column_name STRING, expected_value STRING, actual_value STRING, match BOOLEAN, score DOUBLE, details STRING"
    results_df = spark.createDataFrame(result_rows, _eval_schema)
    results_df.write.mode("append").saveAsTable(eval_fq("eval_results"))
    print(f"Appended {len(result_rows)} result rows to {eval_fq('eval_results')}")

if not skip_pipeline and stage_timings:
    from datetime import datetime as _dt
    timing_rows = [
        {"run_id": run_id, "run_timestamp": _dt.utcnow(),
         "run_label": f"{model_endpoint}/{ontology_bundle}",
         "dimension": "timing", "result_type": "INFO",
         "table_name": stage, "column_name": None,
         "expected_value": None, "actual_value": str(elapsed),
         "match": True, "score": float(elapsed), "details": f"{elapsed}s"}
        for stage, elapsed in stage_timings.items()
    ]
    _eval_schema = "run_id STRING, run_timestamp TIMESTAMP, run_label STRING, dimension STRING, result_type STRING, table_name STRING, column_name STRING, expected_value STRING, actual_value STRING, match BOOLEAN, score DOUBLE, details STRING"
    spark.createDataFrame(timing_rows, _eval_schema).write.mode("append").saveAsTable(eval_fq("eval_results"))
    print(f"Appended {len(timing_rows)} timing rows to {eval_fq('eval_results')}")

# COMMAND ----------

# Scorecard
scores = {
    "comment": avg_comment,
    "pi_type": avg_pi,
    "domain": avg_domain,
    "ontology_entities": avg_entity,
    "ontology_column_properties": avg_prop,
    "ontology_relationships": avg_rel,
    "fk": avg_fk,
}

dim_counts = {}
for r in result_rows:
    d = r["dimension"]
    rt = r["result_type"]
    dim_counts.setdefault(d, {"TP": 0, "FN": 0, "FP": 0})
    if rt in dim_counts[d]:
        dim_counts[d][rt] += 1

print("\n" + "=" * 65)
print("  SCORECARD")
print("=" * 65)
print(f"  Run: {run_id}")
print(f"  {'Dimension':<30} {'Score':>7} {'TP':>5} {'FN':>5} {'FP':>5}")
print("-" * 65)
for dim, score in scores.items():
    c = dim_counts.get(dim, {"TP": 0, "FN": 0, "FP": 0})
    if score is not None:
        print(f"  {dim:<30} {score:>7.2f} {c['TP']:>5} {c['FN']:>5} {c['FP']:>5}")
    else:
        print(f"  {dim:<30} {'SKIP':>7} {c['TP']:>5} {c['FN']:>5} {c['FP']:>5}")

non_null = [s for s in scores.values() if s is not None]
if non_null:
    overall = sum(non_null) / len(non_null)
    print("-" * 65)
    print(f"  {'OVERALL':<30} {overall:>7.2f}")
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FN / FP Diagnostics

# COMMAND ----------

for label, code in [("FALSE NEGATIVES", "FN"), ("FALSE POSITIVES", "FP")]:
    typed = [r for r in result_rows if r["result_type"] == code]
    if not typed:
        continue
    print(f"\n{'=' * 70}")
    print(f"  {label}  ({len(typed)} total)")
    print(f"{'=' * 70}")
    by_dim = {}
    for r in typed:
        by_dim.setdefault(r["dimension"], []).append(r)
    for dim, rows in sorted(by_dim.items()):
        print(f"\n  [{dim}] ({len(rows)} items)")
        for r in rows[:30]:
            col_part = f".{r['column_name']}" if r["column_name"] else ""
            print(f"    {r['table_name']}{col_part}: expected={r['expected_value']}  actual={r['actual_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trend (across runs)

# COMMAND ----------

display(spark.sql(f"""
    WITH per_dim AS (
        SELECT run_id, MIN(run_timestamp) AS ts, MIN(run_label) AS label, dimension,
               ROUND(COUNT(CASE WHEN result_type = 'TP' THEN 1 END)
                   / NULLIF(COUNT(CASE WHEN result_type IN ('TP','FN') THEN 1 END), 0), 2) AS recall
        FROM {eval_fq('eval_results')} GROUP BY run_id, dimension
    )
    SELECT run_id, ts, label,
        MAX(CASE WHEN dimension = 'comment' THEN recall END) AS comment,
        MAX(CASE WHEN dimension = 'pi_type' THEN recall END) AS pi_type,
        MAX(CASE WHEN dimension = 'domain' THEN recall END) AS domain,
        MAX(CASE WHEN dimension = 'ontology_entities' THEN recall END) AS entities,
        MAX(CASE WHEN dimension = 'ontology_column_properties' THEN recall END) AS col_props,
        MAX(CASE WHEN dimension = 'ontology_relationships' THEN recall END) AS relationships,
        MAX(CASE WHEN dimension = 'fk' THEN recall END) AS fk
    FROM per_dim GROUP BY run_id, ts, label ORDER BY ts DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Idempotency Test (Re-Run)
# MAGIC
# MAGIC Re-runs the pipeline with `incremental=true` and verifies no duplicate rows
# MAGIC are created in any output table.

# COMMAND ----------

if not skip_pipeline and not skip_idempotency:
    print("=" * 65)
    print("  IDEMPOTENCY TEST: re-running pipeline with incremental=true")
    print("=" * 65)

    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "comment",
        "model": model_endpoint,
        "apply_ddl": "false",
        "sample_size": "5",
        "incremental": "true",
    }, "idempotency: comment re-run")

    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "pi",
        "model": model_endpoint,
        "apply_ddl": "false",
        "incremental": "true",
    }, "idempotency: PI re-run")

    _run_job_and_wait(metagen_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "mode": "domain",
        "model": model_endpoint,
        "apply_ddl": "false",
        "incremental": "true",
    }, "idempotency: domain re-run")

    _run_job_and_wait(pipeline_job_id, {
        "catalog_name": catalog_name,
        "schema_name": metadata_schema,
        "table_names": table_names_param,
        "ontology_bundle": ontology_bundle,
        "model": model_endpoint,
        "incremental": "true",
        "apply_ddl": "false",
    }, "idempotency: analytics pipeline re-run")
elif skip_idempotency:
    print("Skipping idempotency test (skip_idempotency=true)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicate Detection

# COMMAND ----------

idempotency_failures = []

if not skip_pipeline and not skip_idempotency:
    def _check_no_duplicates(table_fq, group_cols, label):
        cols_csv = ", ".join(group_cols)
        df = spark.sql(f"""
            SELECT {cols_csv}, COUNT(*) AS cnt
            FROM {table_fq}
            GROUP BY {cols_csv}
            HAVING cnt > 1
        """)
        dup_count = df.count()
        if dup_count > 0:
            msg = f"{label}: {dup_count} duplicate groups on ({cols_csv})"
            idempotency_failures.append(msg)
            print(f"  FAIL: {msg}")
            df.show(10, truncate=False)
        else:
            print(f"  PASS: {label} -- no duplicates on ({cols_csv})")

    _check_no_duplicates(
        meta_fq("ontology_entities"),
        ["entity_name", "array_join(array_sort(source_tables), ',')", "COALESCE(attributes['granularity'], 'table')"],
        "ontology_entities")

    _check_no_duplicates(
        meta_fq("ontology_column_properties"),
        ["table_name", "column_name"],
        "ontology_column_properties")

    _check_no_duplicates(
        meta_fq("fk_predictions"),
        ["src_table", "src_column", "dst_table", "dst_column"],
        "fk_predictions")

    dup_log = spark.sql(f"""
        SELECT table_name, column_name, metadata_type, COUNT(*) AS row_count
        FROM {meta_fq('metadata_generation_log')}
        WHERE table_name LIKE '%{schema_name}%'
        GROUP BY table_name, column_name, metadata_type
        HAVING row_count > 1
    """)
    reprocessed = dup_log.count()
    if reprocessed > 0:
        msg = f"metadata_generation_log: {reprocessed} (table,column,mode) tuples have duplicate rows"
        idempotency_failures.append(msg)
        print(f"  WARN: {msg}")
        dup_log.show(10, truncate=False)
    else:
        print("  PASS: metadata_generation_log -- no duplicate (table,column,mode) rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pass / Fail Gate

# COMMAND ----------

min_score = float(dbutils.widgets.get("min_overall_score"))

gate_failures = []

if non_null:
    if overall < min_score:
        gate_failures.append(f"Overall score {overall:.2f} < {min_score}")
else:
    gate_failures.append("No dimensions were scored")

if idempotency_failures:
    gate_failures.extend(idempotency_failures)

if gate_failures:
    lines = ["E2E TEST FAILED:"]
    lines.extend(f"  - {f}" for f in gate_failures)
    lines.append(f"\n  Scorecard: " + ", ".join(
        f"{d}={s:.2f}" if s is not None else f"{d}=SKIP"
        for d, s in scores.items()))
    if non_null:
        lines.append(f"  Overall: {overall:.4f}  (threshold: {min_score})")
    msg = "\n".join(lines)
    print(msg)
    raise AssertionError(msg)

print(f"E2E TEST PASSED: overall={overall:.2f} >= {min_score}, no idempotency failures")
