"""Unit tests for the certified-asset apply policy (src/dbxmetagen/certification.py).

Pure logic only -- no Spark. The live UC read (read_certified_targets) and the Spark-backed
gate_ddls read path are exercised via a fake spark in the gate tests below.
"""

import pytest

from dbxmetagen.certification import (
    normalize_policy,
    resolve_skip_certified,
    parse_ddl_target,
    partition_ddls_by_certification,
    gate_ddls,
)


class TestNormalizePolicy:
    @pytest.mark.parametrize("val,expected", [
        ("block", "block"), ("warn", "warn"), ("allow", "allow"),
        ("BLOCK", "block"), ("  Warn  ", "warn"),
        ("", "warn"), (None, "warn"), ("nonsense", "warn"), ("skip", "warn"),
    ])
    def test_normalize(self, val, expected):
        assert normalize_policy(val) == expected


class TestResolveSkipCertified:
    def test_block_always_skips(self):
        assert resolve_skip_certified("block", False) is True
        assert resolve_skip_certified("block", True) is True

    def test_allow_never_skips(self):
        assert resolve_skip_certified("allow", True) is False
        assert resolve_skip_certified("allow", False) is False

    def test_warn_follows_flag(self):
        assert resolve_skip_certified("warn", False) is False   # legacy apply
        assert resolve_skip_certified("warn", True) is True     # opt-in skip
        assert resolve_skip_certified("warn", "true") is True   # string coercion

    def test_bad_policy_defaults_to_warn(self):
        assert resolve_skip_certified("garbage", True) is True
        assert resolve_skip_certified("garbage", False) is False


class TestParseDdlTarget:
    def test_table_comment(self):
        assert parse_ddl_target("COMMENT ON TABLE cat.sch.tbl IS 'x'") == ("cat.sch.tbl", "")

    def test_table_comment_backticked(self):
        assert parse_ddl_target("COMMENT ON TABLE `Cat`.`Sch`.`Tbl` IS 'x'") == ("cat.sch.tbl", "")

    def test_column_comment_on(self):
        assert parse_ddl_target("COMMENT ON COLUMN cat.sch.tbl.`col` IS 'x'") == ("cat.sch.tbl", "col")

    def test_column_alter(self):
        assert parse_ddl_target("ALTER TABLE cat.sch.tbl ALTER COLUMN `col` COMMENT 'x'") == ("cat.sch.tbl", "col")

    def test_alter_table_set_tags_is_table_level(self):
        assert parse_ddl_target("ALTER TABLE cat.sch.tbl SET TAGS ('domain' = 'x')") == ("cat.sch.tbl", "")

    def test_alter_column_set_tags_is_column_level(self):
        assert parse_ddl_target("ALTER TABLE cat.sch.tbl ALTER COLUMN `col` SET TAGS ('pii'='true')") == ("cat.sch.tbl", "col")

    def test_unparseable_returns_empty(self):
        assert parse_ddl_target("CREATE VIEW v AS SELECT 1") == ("", "")
        assert parse_ddl_target("") == ("", "")


class TestPartition:
    def test_table_certified_skips_table_and_its_columns(self):
        ddls = [
            "COMMENT ON TABLE cat.sch.tbl IS 'x'",
            "ALTER TABLE cat.sch.tbl ALTER COLUMN `c1` COMMENT 'x'",
            "COMMENT ON TABLE cat.sch.other IS 'x'",
        ]
        non, cert = partition_ddls_by_certification(ddls, {"cat.sch.tbl"}, set())
        assert cert == ddls[:2]           # certified table protects its column comment too
        assert non == [ddls[2]]

    def test_column_certified_only(self):
        ddls = [
            "ALTER TABLE cat.sch.tbl ALTER COLUMN `c1` COMMENT 'x'",
            "ALTER TABLE cat.sch.tbl ALTER COLUMN `c2` COMMENT 'x'",
        ]
        non, cert = partition_ddls_by_certification(ddls, set(), {("cat.sch.tbl", "c1")})
        assert cert == [ddls[0]]
        assert non == [ddls[1]]

    def test_nothing_certified(self):
        ddls = ["COMMENT ON TABLE cat.sch.tbl IS 'x'"]
        non, cert = partition_ddls_by_certification(ddls, set(), set())
        assert non == ddls and cert == []


class _FakeRow(dict):
    def __getitem__(self, k):
        return super().__getitem__(k)


class _FakeSpark:
    """Returns certified rows for table_tags/column_tags queries; can be set to raise."""
    def __init__(self, certified_tables=(), certified_cols=(), raise_exc=None):
        self._t = {t.split(".")[2] for t in certified_tables}
        self._c = {(t.split(".")[2], c) for (t, c) in certified_cols}
        self._raise = raise_exc

    def sql(self, q):
        if self._raise:
            raise self._raise
        spark = self

        class _Res:
            def collect(self_inner):
                if "column_tags" in q:
                    return [_FakeRow(t=t, col=c) for (t, c) in spark._c]
                return [_FakeRow(t=t) for t in spark._t]
        return _Res()


class TestGateDdls:
    ddls = [
        "COMMENT ON TABLE cat.sch.tbl IS 'x'",
        "COMMENT ON TABLE cat.sch.certified_tbl IS 'x'",
    ]

    def test_allow_no_gate(self):
        to_apply, hits, action = gate_ddls(_FakeSpark(raise_exc=RuntimeError("should not read")),
                                           self.ddls, "allow", False)
        assert to_apply == self.ddls and hits == [] and action == "allow"

    def test_block_skips_certified(self):
        spark = _FakeSpark(certified_tables=["cat.sch.certified_tbl"])
        to_apply, hits, action = gate_ddls(spark, self.ddls, "block", False)
        assert to_apply == [self.ddls[0]]
        assert hits == [self.ddls[1]]
        assert action == "skipped"

    def test_warn_default_applies_all_but_flags(self):
        spark = _FakeSpark(certified_tables=["cat.sch.certified_tbl"])
        to_apply, hits, action = gate_ddls(spark, self.ddls, "warn", False)
        assert to_apply == self.ddls                 # legacy apply
        assert hits == [self.ddls[1]]                # surfaced
        assert action == "warned"

    def test_warn_with_skip_flag_skips(self):
        spark = _FakeSpark(certified_tables=["cat.sch.certified_tbl"])
        to_apply, hits, action = gate_ddls(spark, self.ddls, "warn", True)
        assert to_apply == [self.ddls[0]]
        assert action == "skipped"

    def test_block_fails_closed_on_read_error(self):
        spark = _FakeSpark(raise_exc=PermissionError("no access to table_tags"))
        with pytest.raises(RuntimeError, match="certified_apply_policy=block"):
            gate_ddls(spark, self.ddls, "block", False)

    def test_warn_fails_open_on_read_error(self):
        spark = _FakeSpark(raise_exc=PermissionError("no access"))
        to_apply, hits, action = gate_ddls(spark, self.ddls, "warn", True)
        assert to_apply == self.ddls                 # fail open: apply rather than break the run
        assert action == "warned"
