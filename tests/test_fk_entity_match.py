"""add_entity_match must return one row per candidate and score by role + column.

Validated end to end against real ontology tables (65 candidate rows in, 65 out);
these tests pin the SQL shape so the cross-join can't come back.
"""

from unittest.mock import MagicMock

ENT = "cat.sch.ontology_entities"
RELS = "cat.sch.ontology_relationships"


def _predictor(ent_cols, rel_cols):
    from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
    spark = MagicMock()
    cols = {ENT: ent_cols, RELS: rel_cols}

    def _table(name):
        if name.replace("`", "") not in cols or cols[name.replace("`", "")] is None:
            raise Exception("TABLE_OR_VIEW_NOT_FOUND")
        return MagicMock(columns=cols[name.replace("`", "")])

    spark.table.side_effect = _table
    return FKPredictor(spark, FKPredictionConfig(catalog_name="cat", schema_name="sch")), spark


FULL_ENT = ["entity_type", "entity_role", "source_columns", "source_tables", "confidence"]
FULL_REL = ["src_entity_type", "dst_entity_type", "confidence", "source"]


def _sql(spark):
    return spark.sql.call_args[0][0]


def test_one_primary_per_table_no_entity_cross_join():
    p, spark = _predictor(FULL_ENT, FULL_REL)
    p.add_entity_match(MagicMock())
    sql = _sql(spark)
    # Each table contributes exactly ONE entity (ROW_NUMBER ... = 1), never all of them.
    assert "ROW_NUMBER() OVER" in sql and "WHERE _rn = 1" in sql
    assert "LEFT JOIN prim pa ON c.table_a = pa.table_name" in sql
    assert "LEFT JOIN prim pb ON c.table_b = pb.table_name" in sql
    # The old fan-out join on the raw exploded entity list must be gone.
    assert "LEFT JOIN table_entity" not in sql and "JOIN ent " not in sql
    # Primary role wins the per-table pick.
    assert "CASE WHEN entity_role = 'primary' THEN 0 ELSE 1 END" in sql


def test_column_level_reference_is_constrained_to_other_tables_primary():
    p, spark = _predictor(FULL_ENT, FULL_REL)
    p.add_entity_match(MagicMock())
    sql = _sql(spark)
    assert "SELECT DISTINCT table_name, LOWER(col) AS col_short, entity_type" in sql
    assert "AND ra.entity_type = pb.entity_type" in sql
    assert "AND rb.entity_type = pa.entity_type" in sql
    assert "THEN 1.0" in sql and "THEN 0.6" in sql and "THEN 0.4" in sql


def test_fk_inferred_relationships_excluded():
    p, spark = _predictor(FULL_ENT, FULL_REL)
    p.add_entity_match(MagicMock())
    assert "COALESCE(source, '') != 'fk_inferred'" in _sql(spark)


def test_legacy_schema_without_roles_or_relationships():
    p, spark = _predictor(["entity_type", "source_tables", "confidence"], None)
    p.add_entity_match(MagicMock())
    sql = _sql(spark)
    assert "refcols" not in sql and "ent_rels" not in sql and "entity_role" not in sql
    assert "WHERE _rn = 1" in sql  # still one entity per table


def test_no_ontology_table_returns_neutral_columns():
    p, spark = _predictor(None, None)
    cands = MagicMock()
    p.add_entity_match(cands)
    spark.sql.assert_not_called()
    assert cands.withColumn.call_args_list[0].args[0] == "entity_match"


def test_join_validate_collects_distinct_pairs_not_confidences():
    from dbxmetagen.fk_prediction import FKPredictionConfig, FKPredictor
    p = FKPredictor(MagicMock(), FKPredictionConfig(catalog_name="cat", schema_name="sch"))
    p._table_samples = {}
    judged = MagicMock()
    judged.filter.return_value.select.return_value.distinct.return_value.collect.return_value = []
    from unittest.mock import patch
    fake_f = MagicMock()
    fake_f.col.return_value.__gt__.return_value = True  # Column > 0 on stubbed pyspark
    with patch("dbxmetagen.fk_prediction.F", fake_f):
        p.join_validate(judged)
    cols = judged.filter.return_value.select.call_args.args
    assert cols == ("col_a", "col_b", "table_a", "table_b")


def test_dedup_is_orientation_independent():
    """The source-rank dedup and the evidence propagation must key on the UNORDERED
    pair, so (child, parent) from value-overlap and (a, b) from embeddings merge."""
    import inspect
    from dbxmetagen.fk_prediction import FKPredictor
    src = inspect.getsource(FKPredictor.run)
    assert 'F.least(F.col("col_a"), F.col("col_b"))' in src
    assert 'sim_w = Window.partitionBy("_pair_lo", "_pair_hi")' in src
    assert 'dedup_w = Window.partitionBy("_pair_lo", "_pair_hi")' in src
    assert 'Window.partitionBy("col_a", "col_b").orderBy(' not in src
    assert '"_data_overlap", F.max("_data_overlap").over(sim_w)' in src
