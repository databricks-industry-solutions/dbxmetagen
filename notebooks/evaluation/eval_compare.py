# Databricks notebook source
# MAGIC %md
# MAGIC # Evaluation Compare: Score Pipeline Outputs Against Expected Values
# MAGIC
# MAGIC Reads dbxmetagen pipeline outputs for `eval_*` tables, compares against
# MAGIC expected results, computes per-dimension scores, and appends results to
# MAGIC the `eval_results` Delta table.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("eval_schema", "", "Eval Schema (eval tables + expected tables)")
dbutils.widgets.text("pipeline_schema", "", "Pipeline Schema (metadata_generation_log, ontology, fk)")
dbutils.widgets.text("run_label", "", "Run Label (optional tag)")

catalog_name = dbutils.widgets.get("catalog_name").strip()
eval_schema = dbutils.widgets.get("eval_schema").strip()
pipeline_schema = dbutils.widgets.get("pipeline_schema").strip()
run_label = dbutils.widgets.get("run_label").strip()

assert catalog_name, "catalog_name is required"
assert eval_schema, "eval_schema is required"
assert pipeline_schema, "pipeline_schema is required"

eval_fq = lambda t: f"{catalog_name}.{eval_schema}.{t}"
pipe_fq = lambda t: f"{catalog_name}.{pipeline_schema}.{t}"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json
import uuid
from datetime import datetime

run_id = str(uuid.uuid4())
run_timestamp = datetime.utcnow()

# Derive the set of eval table names from the expected_entities table.
# These are short names (e.g. "patients", "encounters") living in the eval schema.
_ent_rows = spark.table(eval_fq("eval_expected_entities")).select("table_name").distinct().collect()
EVAL_TABLE_NAMES = {r.table_name for r in _ent_rows}
_eval_in_clause = ", ".join(f"'{t}'" for t in sorted(EVAL_TABLE_NAMES))

print(f"Eval run: {run_id}")
print(f"Eval schema:     {catalog_name}.{eval_schema}")
print(f"Pipeline schema: {catalog_name}.{pipeline_schema}")
print(f"Eval tables:     {sorted(EVAL_TABLE_NAMES)}")
if run_label:
    print(f"Label: {run_label}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Create eval_results table if not exists

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {eval_fq('eval_results')} (
        run_id STRING,
        run_timestamp TIMESTAMP,
        run_label STRING,
        dimension STRING,
        result_type STRING,
        table_name STRING,
        column_name STRING,
        expected_value STRING,
        actual_value STRING,
        match BOOLEAN,
        score DOUBLE,
        details STRING
    )
""")

existing_cols = {f.name for f in spark.table(eval_fq("eval_results")).schema.fields}
if "result_type" not in existing_cols:
    spark.sql(f"ALTER TABLE {eval_fq('eval_results')} ADD COLUMNS (result_type STRING AFTER dimension)")

result_rows = []

def add_result(dimension, result_type, table_name, column_name, expected, actual, match, score, details=None):
    result_rows.append({
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "run_label": run_label,
        "dimension": dimension,
        "result_type": result_type,
        "table_name": table_name,
        "column_name": column_name,
        "expected_value": str(expected),
        "actual_value": str(actual),
        "match": match,
        "score": float(score),
        "details": json.dumps(details) if details else None,
    })

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Comment Evaluation

# COMMAND ----------

try:
    expected_comments = spark.table(eval_fq("eval_expected_comments")).collect()
    actual_comments_df = spark.sql(f"""
        SELECT table_name, column_name, column_content FROM (
            SELECT table_name, column_name, column_content,
                   ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY _created_at DESC) AS rn
            FROM {pipe_fq('metadata_generation_log')}
            WHERE table_name IN ({_eval_in_clause})
              AND metadata_type = 'comment'
              AND column_name IS NOT NULL
        ) WHERE rn = 1
    """)
    actual_table_comments_df = spark.sql(f"""
        SELECT table_name, column_content FROM (
            SELECT table_name, column_content,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY _created_at DESC) AS rn
            FROM {pipe_fq('metadata_generation_log')}
            WHERE table_name IN ({_eval_in_clause})
              AND metadata_type = 'comment'
              AND (column_name IS NULL OR column_name = 'None')
        ) WHERE rn = 1
    """)

    actual_col_map = {
        (r.table_name, r.column_name): r.column_content
        for r in actual_comments_df.collect()
    }
    actual_tbl_map = {r.table_name: r.column_content for r in actual_table_comments_df.collect()}

    expected_comment_keys = set()
    comment_scores = []
    for row in expected_comments:
        tbl = row.table_name
        col = row.column_name
        expected_comment_keys.add((tbl, col))
        keywords = [k.strip().lower() for k in row.expected_keywords.split(",") if k.strip()]

        if col is None:
            actual = (actual_tbl_map.get(tbl) or "").lower()
        else:
            actual = (actual_col_map.get((tbl, col)) or "").lower()

        if not actual:
            add_result("comment", "FN", tbl, col, ",".join(keywords), "", False, 0.0)
            comment_scores.append(0.0)
            continue

        hits = [kw for kw in keywords if kw in actual]
        score = len(hits) / len(keywords) if keywords else 1.0
        rtype = "TP" if score >= 0.5 else "FN"
        comment_scores.append(score)
        add_result(
            "comment", rtype, tbl, col,
            ",".join(keywords), actual[:200],
            score >= 0.5, score,
            {"hits": hits, "misses": [k for k in keywords if k not in actual]},
        )

    avg_comment = sum(comment_scores) / len(comment_scores) if comment_scores else 0.0
    print(f"Comment: {avg_comment:.2f} ({sum(1 for s in comment_scores if s >= 0.5)}/{len(comment_scores)} items passing)")
except Exception as e:
    print(f"Comment evaluation skipped: {e}")
    avg_comment = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. PI Type Evaluation

# COMMAND ----------

try:
    expected_pi = spark.table(eval_fq("eval_expected_pi")).collect()
    actual_pi_df = spark.sql(f"""
        SELECT table_name, column_name, type FROM (
            SELECT table_name, column_name, type,
                   ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY _created_at DESC) AS rn
            FROM {pipe_fq('metadata_generation_log')}
            WHERE table_name IN ({_eval_in_clause})
              AND metadata_type = 'pi'
              AND column_name IS NOT NULL
        ) WHERE rn = 1
    """)
    actual_pi_map = {
        (r.table_name, r.column_name): (r.type or "None")
        for r in actual_pi_df.collect()
    }

    expected_pi_keys = {(r.table_name, r.column_name) for r in expected_pi}
    expected_pi_lookup = {
        (r.table_name, r.column_name): (r.expected_type, r.acceptable_alternatives)
        for r in expected_pi
    }

    pi_scores = []
    for row in expected_pi:
        tbl, col = row.table_name, row.column_name
        expected_type = row.expected_type
        alts = [a.strip() for a in (row.acceptable_alternatives or "").split(",") if a.strip()]
        actual_type = actual_pi_map.get((tbl, col), "__MISSING__")

        if actual_type == "__MISSING__":
            add_result("pi_type", "FN", tbl, col, expected_type, "MISSING", False, 0.0)
            pi_scores.append(0.0)
            continue

        match = actual_type == expected_type or actual_type in alts
        rtype = "TP" if match else "FN"
        pi_scores.append(1.0 if match else 0.0)
        add_result(
            "pi_type", rtype, tbl, col,
            expected_type, actual_type,
            match, 1.0 if match else 0.0,
            {"acceptable_alternatives": alts},
        )

    pi_fp_count = 0
    for (tbl, col), actual_type in actual_pi_map.items():
        if (tbl, col) in expected_pi_keys:
            continue
        if col in (None, "None"):
            continue
        if actual_type not in ("None", "none", None, ""):
            pi_fp_count += 1
            add_result("pi_type", "FP", tbl, col, "None", actual_type, False, 0.0,
                       {"reason": "pipeline flagged PI on column not in expected set"})

    avg_pi = sum(pi_scores) / len(pi_scores) if pi_scores else 0.0
    correct = sum(1 for s in pi_scores if s == 1.0)
    fn_count = len(pi_scores) - correct
    print(f"PI type: {avg_pi:.2f} ({correct} TP, {fn_count} FN, {pi_fp_count} FP out of {len(pi_scores)} expected)")
except Exception as e:
    print(f"PI evaluation skipped: {e}")
    avg_pi = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Domain Evaluation

# COMMAND ----------

try:
    expected_domains = spark.table(eval_fq("eval_expected_domains")).collect()
    actual_domain_df = spark.sql(f"""
        SELECT table_name, domain, subdomain FROM (
            SELECT table_name, domain, subdomain,
                   ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY _created_at DESC) AS rn
            FROM {pipe_fq('metadata_generation_log')}
            WHERE table_name IN ({_eval_in_clause})
              AND metadata_type = 'domain'
              AND (column_name IS NULL OR column_name = 'None')
        ) WHERE rn = 1
    """)
    actual_domain_map = {
        r.table_name: (r.domain, r.subdomain) for r in actual_domain_df.collect()
    }

    expected_domain_tables = {r.table_name for r in expected_domains}
    domain_scores = []
    for row in expected_domains:
        tbl = row.table_name
        exp_dom = row.expected_domain
        exp_sub = row.expected_subdomain
        dom_alts = [a.strip() for a in (row.acceptable_domain_alternatives or "").split(",") if a.strip()]
        sub_alts = [a.strip() for a in (row.acceptable_subdomain_alternatives or "").split(",") if a.strip()]

        actual = actual_domain_map.get(tbl)
        if not actual:
            add_result("domain", "FN", tbl, None, f"{exp_dom}/{exp_sub}", "MISSING", False, 0.0)
            domain_scores.append(0.0)
            continue

        act_dom, act_sub = actual
        dom_match = act_dom == exp_dom or act_dom in dom_alts
        sub_match = act_sub == exp_sub or act_sub in sub_alts

        if dom_match and sub_match:
            score = 1.0
        elif dom_match:
            score = 0.5
        else:
            score = 0.0

        rtype = "TP" if score >= 0.5 else "FN"
        domain_scores.append(score)
        add_result(
            "domain", rtype, tbl, None,
            f"{exp_dom}/{exp_sub}", f"{act_dom}/{act_sub}",
            score >= 0.5, score,
            {"domain_match": dom_match, "subdomain_match": sub_match},
        )

    domain_fp_count = 0
    for tbl, (act_dom, act_sub) in actual_domain_map.items():
        if tbl not in expected_domain_tables and tbl in EVAL_TABLE_NAMES:
            domain_fp_count += 1
            add_result("domain", "FP", tbl, None, "N/A", f"{act_dom}/{act_sub}", False, 0.0,
                       {"reason": "domain assigned to table not in expected set"})

    avg_domain = sum(domain_scores) / len(domain_scores) if domain_scores else 0.0
    tp = sum(1 for s in domain_scores if s >= 0.5)
    print(f"Domain: {avg_domain:.2f} ({tp} TP, {len(domain_scores) - tp} FN, {domain_fp_count} FP)")
except Exception as e:
    print(f"Domain evaluation skipped: {e}")
    avg_domain = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Ontology Entity Evaluation

# COMMAND ----------

try:
    expected_entities = spark.table(eval_fq("eval_expected_entities")).collect()

    actual_entities_df = spark.sql(f"""
        SELECT entity_type, source_tables
        FROM {pipe_fq('ontology_entities')}
        WHERE entity_role = 'primary'
    """)
    table_to_entity = {}
    for row in actual_entities_df.collect():
        for src in (row.source_tables or []):
            short = src.split(".")[-1] if "." in src else src
            if short in EVAL_TABLE_NAMES:
                table_to_entity[short] = row.entity_type

    expected_entity_tables = {r.table_name for r in expected_entities}
    expected_entity_types = {r.expected_entity_type for r in expected_entities}

    entity_scores = []
    for row in expected_entities:
        tbl = row.table_name
        expected = row.expected_entity_type
        alts = [a.strip() for a in (getattr(row, "acceptable_alternatives", "") or "").split(",") if a.strip()]
        actual = table_to_entity.get(tbl, "__MISSING__")
        match = actual == expected or actual in alts
        rtype = "TP" if match else "FN"
        entity_scores.append(1.0 if match else 0.0)
        add_result(
            "ontology_entities", rtype, tbl, None,
            expected, actual,
            match, 1.0 if match else 0.0,
            {"confidence_tier": row.confidence_tier, "acceptable_alternatives": alts},
        )

    entity_fp_count = 0
    for tbl, etype in table_to_entity.items():
        if tbl not in expected_entity_tables and tbl in EVAL_TABLE_NAMES:
            entity_fp_count += 1
            add_result("ontology_entities", "FP", tbl, None, "N/A", etype, False, 0.0,
                       {"reason": "entity discovered from eval table not in expected set"})

    avg_entity = sum(entity_scores) / len(entity_scores) if entity_scores else 0.0
    tp = sum(1 for s in entity_scores if s == 1.0)
    print(f"Ontology entities: {avg_entity:.2f} ({tp} TP, {len(entity_scores) - tp} FN, {entity_fp_count} FP)")
except Exception as e:
    print(f"Entity evaluation skipped: {e}")
    avg_entity = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Ontology Column Properties Evaluation

# COMMAND ----------

try:
    expected_props = spark.table(eval_fq("eval_expected_column_properties")).collect()

    _fq_eval_in = ", ".join(f"'{catalog_name}.{eval_schema}.{t}'" for t in sorted(EVAL_TABLE_NAMES))
    actual_props_df = spark.sql(f"""
        SELECT table_name, column_name, property_role, is_sensitive,
               discovery_method, confidence, owning_entity_type
        FROM {pipe_fq('ontology_column_properties')}
        WHERE table_name IN ({_fq_eval_in})
    """)
    actual_prop_rows = actual_props_df.collect()
    actual_prop_map = {}
    actual_prop_detail = {}
    for r in actual_prop_rows:
        short = r.table_name.split(".")[-1] if "." in r.table_name else r.table_name
        actual_prop_map[(short, r.column_name)] = (r.property_role, r.is_sensitive)
        actual_prop_detail[(short, r.column_name)] = {
            "discovery_method": r.discovery_method,
            "confidence": r.confidence,
            "owning_entity_type": r.owning_entity_type,
        }

    # Diagnostic: discovery_method breakdown
    from collections import Counter
    method_counts = Counter(d["discovery_method"] for d in actual_prop_detail.values())
    print("Column property discovery_method breakdown:")
    for m, c in method_counts.most_common():
        print(f"  {m}: {c}")
    entity_types = {d["owning_entity_type"] for d in actual_prop_detail.values()}
    print(f"Owning entity types: {sorted(entity_types)}")

    expected_prop_keys = {(r.table_name, r.column_name) for r in expected_props}

    prop_scores = []
    for row in expected_props:
        tbl, col = row.table_name, row.column_name
        exp_role = row.expected_property_role
        exp_sensitive = row.expected_is_sensitive
        tier = row.confidence_tier

        actual = actual_prop_map.get((tbl, col))
        if not actual:
            add_result("ontology_column_properties", "FN", tbl, col, exp_role, "MISSING", False, 0.0, {"tier": tier})
            prop_scores.append(0.0)
            continue

        act_role, act_sensitive = actual
        detail = actual_prop_detail.get((tbl, col), {})
        role_match = act_role == exp_role
        sensitive_match = act_sensitive == exp_sensitive
        score = (0.7 if role_match else 0.0) + (0.3 if sensitive_match else 0.0)
        rtype = "TP" if role_match else "FN"
        prop_scores.append(score)
        add_result(
            "ontology_column_properties", rtype, tbl, col,
            f"{exp_role}|sensitive={exp_sensitive}",
            f"{act_role}|sensitive={act_sensitive}",
            role_match, score,
            {"role_match": role_match, "sensitive_match": sensitive_match, "tier": tier,
             "discovery_method": detail.get("discovery_method"),
             "owning_entity_type": detail.get("owning_entity_type")},
        )

    prop_fp_count = 0
    for (tbl, col), (act_role, act_sensitive) in actual_prop_map.items():
        short = tbl.split(".")[-1] if "." in tbl else tbl
        if (tbl, col) not in expected_prop_keys and short in EVAL_TABLE_NAMES:
            prop_fp_count += 1
            add_result("ontology_column_properties", "FP", tbl, col, "N/A",
                       f"{act_role}|sensitive={act_sensitive}", False, 0.0,
                       {"reason": "property classified for column not in expected set"})

    avg_prop = sum(prop_scores) / len(prop_scores) if prop_scores else 0.0
    role_correct = sum(1 for r in prop_scores if r >= 0.7)
    print(f"Column properties: {avg_prop:.2f} ({role_correct} TP, {len(prop_scores) - role_correct} FN, {prop_fp_count} FP)")
except Exception as e:
    print(f"Column properties evaluation skipped: {e}")
    avg_prop = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Ontology Relationships Evaluation

# COMMAND ----------

try:
    expected_rels = spark.table(eval_fq("eval_expected_relationships")).collect()

    # Build the set of entity types actually discovered from eval tables.
    # Use table_to_entity (built in entity eval above) for tight scoping.
    eval_entity_types = set(table_to_entity.values())
    # Also include expected entity types and their alternatives so we don't
    # miss FPs on the entities we're explicitly testing.
    all_eval_entities = spark.table(eval_fq("eval_expected_entities")).collect()
    for e in all_eval_entities:
        eval_entity_types.add(e.expected_entity_type)
        for a in (getattr(e, "acceptable_alternatives", "") or "").split(","):
            if a.strip():
                eval_entity_types.add(a.strip())

    # Build entity-type alias map: alternative -> canonical
    # e.g. "Provider" -> "Practitioner", "MedicationRequest" -> "Medication"
    _entity_alias = {}
    for e in all_eval_entities:
        canonical = e.expected_entity_type
        for a in (getattr(e, "acceptable_alternatives", "") or "").split(","):
            a = a.strip()
            if a:
                _entity_alias[a] = canonical
                _entity_alias[canonical] = canonical

    def _normalize_entity(etype):
        return _entity_alias.get(etype, etype)

    actual_rels_df = spark.sql(f"""
        SELECT src_entity_type, dst_entity_type, relationship_name
        FROM {pipe_fq('ontology_relationships')}
    """)
    actual_rel_set = set()
    actual_rel_names = {}
    for r in actual_rels_df.collect():
        pair = (r.src_entity_type, r.dst_entity_type)
        actual_rel_set.add(pair)
        actual_rel_names.setdefault(pair, set()).add(r.relationship_name)
        # Also index normalized pairs so aliased matches work
        norm_pair = (_normalize_entity(r.src_entity_type), _normalize_entity(r.dst_entity_type))
        if norm_pair != pair:
            actual_rel_set.add(norm_pair)
            actual_rel_names.setdefault(norm_pair, set()).add(r.relationship_name)

    expected_rel_pairs = set()
    for r in expected_rels:
        expected_rel_pairs.add((r.src_entity_type, r.dst_entity_type))
        norm = (_normalize_entity(r.src_entity_type), _normalize_entity(r.dst_entity_type))
        if norm != (r.src_entity_type, r.dst_entity_type):
            expected_rel_pairs.add(norm)

    scored_pairs = set()
    rel_scores = []
    for row in expected_rels:
        src, dst = row.src_entity_type, row.dst_entity_type
        if (src, dst) in scored_pairs:
            continue
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

        rtype = "TP" if score >= 0.5 else "FN"
        scored_pairs.add((src, dst))
        rel_scores.append(score)
        add_result(
            "ontology_relationships", rtype, f"{src}->{dst}", None,
            exp_name, ",".join(actual_names) if actual_names else "MISSING",
            pair_found, score,
            {"pair_found": pair_found, "name_match": name_match, "exact_match": exact_match},
        )

    rel_fp_count = 0
    for pair in actual_rel_set:
        if pair not in expected_rel_pairs and pair[0] in eval_entity_types and pair[1] in eval_entity_types:
            names = actual_rel_names.get(pair, set())
            rel_fp_count += 1
            add_result("ontology_relationships", "FP", f"{pair[0]}->{pair[1]}", None,
                       "N/A", ",".join(names), False, 0.0,
                       {"reason": "relationship not in expected set"})

    avg_rel = sum(rel_scores) / len(rel_scores) if rel_scores else 0.0
    tp = sum(1 for s in rel_scores if s >= 0.5)
    print(f"Relationships: {avg_rel:.2f} ({tp} TP, {len(rel_scores) - tp} FN, {rel_fp_count} FP)")
except Exception as e:
    print(f"Relationship evaluation skipped: {e}")
    avg_rel = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. FK Prediction Evaluation

# COMMAND ----------

try:
    expected_fk = spark.table(eval_fq("eval_expected_fk")).collect()

    _fq_eval_tbls = [f"{catalog_name}.{eval_schema}.{t}" for t in EVAL_TABLE_NAMES]
    _fq_fk_in = ", ".join(f"'{t}'" for t in sorted(_fq_eval_tbls))
    actual_fk_df = spark.sql(f"""
        SELECT src_table, src_column, dst_table, dst_column, final_confidence
        FROM {pipe_fq('fk_predictions')}
        WHERE (src_table IN ({_fq_fk_in}) OR dst_table IN ({_fq_fk_in}))
          AND is_fk = true
    """)
    actual_fk_directed = set()
    actual_fk_undirected = set()
    actual_fk_confidence = {}
    for r in actual_fk_df.collect():
        src_t = r.src_table.split(".")[-1] if "." in r.src_table else r.src_table
        dst_t = r.dst_table.split(".")[-1] if "." in r.dst_table else r.dst_table
        if src_t not in EVAL_TABLE_NAMES or dst_t not in EVAL_TABLE_NAMES:
            continue
        src_c = r.src_column.split(".")[-1] if "." in r.src_column else r.src_column
        dst_c = r.dst_column.split(".")[-1] if "." in r.dst_column else r.dst_column
        fk_key = (src_t, src_c, dst_t, dst_c)
        rev_key = (dst_t, dst_c, src_t, src_c)
        actual_fk_directed.add(fk_key)
        actual_fk_undirected.add(fk_key)
        actual_fk_undirected.add(rev_key)
        conf = float(r.final_confidence or 0.0)
        actual_fk_confidence[fk_key] = conf
        actual_fk_confidence[rev_key] = conf

    # Diagnostic: show all fk_predictions for patient_id and provider_id (including is_fk=false)
    diag_cols = ["patient_id", "provider_id", "encounter_id"]
    _diag_col_in = ", ".join(f"'{c}'" for c in diag_cols)
    diag_df = spark.sql(f"""
        SELECT src_table, src_column, dst_table, dst_column,
               is_fk, final_confidence, ai_confidence, rule_score
        FROM {pipe_fq('fk_predictions')}
        WHERE src_column IN ({_diag_col_in}) OR dst_column IN ({_diag_col_in})
        ORDER BY src_column, final_confidence DESC
    """)
    print("FK diagnostic -- all candidate rows for key columns:")
    diag_df.show(50, truncate=False)

    expected_fk_set = set()
    expected_fk_both_dirs = set()
    fk_scores = []
    fk_dir_scores = []
    for row in expected_fk:
        key = (row.src_table, row.src_column, row.dst_table, row.dst_column)
        rev = (row.dst_table, row.dst_column, row.src_table, row.src_column)
        expected_fk_set.add(key)
        expected_fk_both_dirs.add(key)
        expected_fk_both_dirs.add(rev)
        dir_correct = key in actual_fk_directed
        dir_reversed = rev in actual_fk_directed
        found = dir_correct or dir_reversed
        dir_score = 1.0 if dir_correct else (0.5 if dir_reversed else 0.0)
        rtype = "TP" if found else "FN"
        fk_scores.append(1.0 if found else 0.0)
        fk_dir_scores.append(dir_score)
        detail = {"direction": "correct" if dir_correct else ("reversed" if dir_reversed else "missing")}
        add_result(
            "fk", rtype, f"{row.src_table}.{row.src_column}", None,
            f"{row.dst_table}.{row.dst_column}",
            detail["direction"],
            found, dir_score, detail,
        )

    fk_fp_count = 0
    for fk_tuple in actual_fk_directed:
        if fk_tuple not in expected_fk_both_dirs:
            fk_fp_count += 1
            add_result("fk", "FP", f"{fk_tuple[0]}.{fk_tuple[1]}", None,
                       "N/A", f"{fk_tuple[2]}.{fk_tuple[3]}", False, 0.0,
                       {"reason": "FK predicted but not in expected set"})

    recall = sum(fk_scores) / len(fk_scores) if fk_scores else 0.0
    true_positives = sum(fk_scores)
    precision = true_positives / (true_positives + max(fk_fp_count, 0)) if (true_positives + fk_fp_count) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    dir_score_avg = sum(fk_dir_scores) / len(fk_dir_scores) if fk_dir_scores else 0.0

    # Confidence-weighted F1: TP score = final_confidence instead of 1.0
    conf_tp_sum = 0.0
    for row in expected_fk:
        key = (row.src_table, row.src_column, row.dst_table, row.dst_column)
        rev = (row.dst_table, row.dst_column, row.src_table, row.src_column)
        conf_tp_sum += max(actual_fk_confidence.get(key, 0.0), actual_fk_confidence.get(rev, 0.0))
    conf_recall = conf_tp_sum / len(expected_fk) if expected_fk else 0.0
    conf_precision = conf_tp_sum / (conf_tp_sum + fk_fp_count) if (conf_tp_sum + fk_fp_count) > 0 else 0.0
    conf_f1 = 2 * conf_precision * conf_recall / (conf_precision + conf_recall) if (conf_precision + conf_recall) > 0 else 0.0

    print(f"FK: F1={f1:.2f} (precision={precision:.2f}, recall={recall:.2f}, {int(true_positives)} TP, {len(fk_scores) - int(true_positives)} FN, {fk_fp_count} FP)")
    print(f"FK direction-aware score: {dir_score_avg:.2f} (1.0=correct, 0.5=reversed, 0.0=missing)")
    print(f"FK confidence-weighted F1: {conf_f1:.2f} (precision={conf_precision:.2f}, recall={conf_recall:.2f})")
    avg_fk = f1
except Exception as e:
    print(f"FK evaluation skipped: {e}")
    avg_fk = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write Results

# COMMAND ----------

if result_rows:
    results_df = spark.createDataFrame(result_rows)
    results_df.write.mode("append").saveAsTable(eval_fq("eval_results"))
    print(f"\nAppended {len(result_rows)} rows to {eval_fq('eval_results')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Diagnostics: FN and FP Examples

# COMMAND ----------

MAX_EXAMPLES = 50

for rtype_label, rtype_code in [("FALSE NEGATIVES (expected but missing/wrong)", "FN"),
                                 ("FALSE POSITIVES (unexpected pipeline output)", "FP")]:
    typed = [r for r in result_rows if r["result_type"] == rtype_code]
    if not typed:
        continue
    print(f"\n{'=' * 70}")
    print(f"  {rtype_label}  ({len(typed)} total)")
    print(f"{'=' * 70}")

    by_dim = {}
    for r in typed:
        by_dim.setdefault(r["dimension"], []).append(r)

    for dim, rows in sorted(by_dim.items()):
        print(f"\n  [{dim}] ({len(rows)} items)")
        for r in rows[:MAX_EXAMPLES]:
            col_part = f".{r['column_name']}" if r["column_name"] else ""
            print(f"    {r['table_name']}{col_part}: expected={r['expected_value']}  actual={r['actual_value']}")
            if r["details"]:
                d = json.loads(r["details"]) if isinstance(r["details"], str) else r["details"]
                detail_parts = [f"{k}={v}" for k, v in d.items()]
                if detail_parts:
                    print(f"      detail: {', '.join(detail_parts)}")
        if len(rows) > MAX_EXAMPLES:
            print(f"    ... and {len(rows) - MAX_EXAMPLES} more")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Scorecard

# COMMAND ----------

print("\n" + "=" * 60)
print("  EVALUATION SCORECARD")
print("=" * 60)
print(f"  Run ID:    {run_id}")
print(f"  Timestamp: {run_timestamp}")
if run_label:
    print(f"  Label:     {run_label}")
dim_counts = {}
for r in result_rows:
    dim = r["dimension"]
    rt = r["result_type"]
    dim_counts.setdefault(dim, {"TP": 0, "FN": 0, "FP": 0})
    if rt in dim_counts[dim]:
        dim_counts[dim][rt] += 1

print(f"  {'Dimension':<30} {'Score':>7} {'TP':>5} {'FN':>5} {'FP':>5}")
print("-" * 65)

scores = {
    "comment": avg_comment,
    "pi_type": avg_pi,
    "domain": avg_domain,
    "ontology_entities": avg_entity,
    "ontology_column_properties": avg_prop,
    "ontology_relationships": avg_rel,
    "fk": avg_fk,
}

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
# MAGIC ## Trend Analysis
# MAGIC
# MAGIC Score history across runs. Useful for tracking pipeline improvements over time
# MAGIC or comparing model endpoints.

# COMMAND ----------

trend_df = spark.sql(f"""
    WITH run_scores AS (
        SELECT
            run_id, run_timestamp, run_label, dimension,
            COUNT(CASE WHEN result_type = 'TP' THEN 1 END) AS tp,
            COUNT(CASE WHEN result_type = 'FN' THEN 1 END) AS fn,
            COUNT(CASE WHEN result_type = 'FP' THEN 1 END) AS fp,
            ROUND(COUNT(CASE WHEN result_type = 'TP' THEN 1 END)
                / NULLIF(COUNT(CASE WHEN result_type IN ('TP','FN') THEN 1 END), 0), 2) AS recall
        FROM {eval_fq('eval_results')}
        GROUP BY run_id, run_timestamp, run_label, dimension
    )
    SELECT *
    FROM run_scores
    ORDER BY run_timestamp DESC, dimension
""")

display(trend_df)

# COMMAND ----------

overall_trend_df = spark.sql(f"""
    WITH per_dim AS (
        SELECT
            run_id,
            MIN(run_timestamp) AS run_timestamp,
            MIN(run_label) AS run_label,
            dimension,
            ROUND(COUNT(CASE WHEN result_type = 'TP' THEN 1 END)
                / NULLIF(COUNT(CASE WHEN result_type IN ('TP','FN') THEN 1 END), 0), 2) AS recall
        FROM {eval_fq('eval_results')}
        GROUP BY run_id, dimension
    ),
    overall AS (
        SELECT
            run_id, run_timestamp, run_label,
            ROUND(AVG(recall), 2) AS overall_score,
            COUNT(DISTINCT dimension) AS dimensions_evaluated
        FROM per_dim
        WHERE recall IS NOT NULL
        GROUP BY run_id, run_timestamp, run_label
    )
    SELECT * FROM overall
    ORDER BY run_timestamp DESC
""")

print("Overall score trend:")
display(overall_trend_df)

# COMMAND ----------

pivot_df = spark.sql(f"""
    WITH per_dim AS (
        SELECT
            run_id,
            MIN(run_timestamp) AS ts,
            MIN(run_label) AS label,
            dimension,
            ROUND(COUNT(CASE WHEN result_type = 'TP' THEN 1 END)
                / NULLIF(COUNT(CASE WHEN result_type IN ('TP','FN') THEN 1 END), 0), 2) AS recall
        FROM {eval_fq('eval_results')}
        GROUP BY run_id, dimension
    )
    SELECT
        run_id, ts, label,
        MAX(CASE WHEN dimension = 'comment' THEN recall END) AS comment,
        MAX(CASE WHEN dimension = 'pi_type' THEN recall END) AS pi_type,
        MAX(CASE WHEN dimension = 'domain' THEN recall END) AS domain,
        MAX(CASE WHEN dimension = 'ontology_entities' THEN recall END) AS entities,
        MAX(CASE WHEN dimension = 'ontology_column_properties' THEN recall END) AS col_props,
        MAX(CASE WHEN dimension = 'ontology_relationships' THEN recall END) AS relationships,
        MAX(CASE WHEN dimension = 'fk' THEN recall END) AS fk
    FROM per_dim
    GROUP BY run_id, ts, label
    ORDER BY ts DESC
""")

print("Score pivot by dimension (each row = one eval run):")
display(pivot_df)
