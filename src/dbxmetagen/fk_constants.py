"""Shared, dependency-free constants for the FK-vs-join-key model.

`fk_predictions.relationship_kind` discriminates a true referential FK
('foreign_key' or legacy NULL) from a broad join key ('join_key', e.g. an
ERD-confirmed join that is not a referential constraint). Only true FKs may
become ALTER TABLE ADD CONSTRAINT / predicted_fk graph edges; join keys still
flow to metric-view / Genie joins.

This module deliberately imports nothing (no pyspark) so BOTH the Spark library
(`dbxmetagen.fk_prediction`) and the Spark-free FastAPI app (`api_server`) can
import the same literals -- eliminating the copy-drift the architecture doc
(docs/SEMANTIC_LAYER_LIBRARY_VS_APP.md) warns about.
"""

JOIN_KEY = "join_key"
FOREIGN_KEY = "foreign_key"

# SQL predicate: matches a true FK only (relationship_kind NULL/legacy or
# 'foreign_key'); excludes rows explicitly tagged 'join_key'. Null-safe.
NOT_JOIN_KEY_SQL = "(relationship_kind IS NULL OR relationship_kind <> 'join_key')"
