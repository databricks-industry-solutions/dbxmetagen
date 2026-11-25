"""Score FK predictions against gold expectations (pure Python, no Spark).

Gold rows come from the UAT builder's ``uat_expected_fk`` table:
``(scenario, src_table, src_column, dst_table, dst_column, expected_is_fk)``.
``expected_is_fk=True`` rows are real FKs (recall targets); ``False`` rows are
explicit traps that must NOT be predicted; ``NULL``/``None`` rows are ambiguous
(the label is genuinely unclear) and are excluded from both recall and FPs.

Matching is UNDIRECTED (a reversed prediction still counts as found) and
direction is reported separately, so a direction bug can't masquerade as a
recall bug. Two kinds of false positive are reported:

- ``fp``: a predicted FK (``is_fk=True``) that is not a gold FK, split into
  ``trap_fp`` (an explicit gold trap) and unlabeled.
- ``high_conf_non_fk``: rows stored with ``is_fk=False`` but a confidence at or
  above ``display_threshold``. Readers that rank by confidence (ERD recommender,
  review UI) still surface these, so they are tracked even though ``is_fk`` is off.
"""

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple

Pair = Tuple[str, str]            # (table.column, table.column), short names
Key = frozenset                   # undirected pair


def _short(table: str, column: str) -> str:
    t = str(table or "").split(".")[-1]
    c = str(column or "").split(".")[-1]
    return f"{t}.{c}".lower()


def _key(a: str, b: str) -> Key:
    return frozenset((a, b))


@dataclass
class FkScore:
    scenario: str
    gold: int = 0
    predicted: int = 0
    tp: int = 0
    fn: int = 0
    fp: int = 0
    trap_fp: int = 0
    reversed: int = 0
    high_conf_non_fk: int = 0
    found: List[Pair] = field(default_factory=list)
    missed: List[Pair] = field(default_factory=list)
    false_positives: List[Pair] = field(default_factory=list)
    trap_hits: List[Pair] = field(default_factory=list)
    reversed_pairs: List[Pair] = field(default_factory=list)
    high_conf_pairs: List[Tuple[str, str, float]] = field(default_factory=list)

    @property
    def precision(self) -> Optional[float]:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) else None

    @property
    def recall(self) -> Optional[float]:
        return self.tp / self.gold if self.gold else None

    @property
    def direction_accuracy(self) -> Optional[float]:
        return (self.tp - self.reversed) / self.tp if self.tp else None

    def as_row(self) -> Dict[str, object]:
        return {
            "scenario": self.scenario, "gold": self.gold, "predicted": self.predicted,
            "tp": self.tp, "fn": self.fn, "fp": self.fp, "trap_fp": self.trap_fp,
            "reversed": self.reversed, "high_conf_non_fk": self.high_conf_non_fk,
            "precision": self.precision, "recall": self.recall,
            "direction_accuracy": self.direction_accuracy,
        }


def score_fk(
    scenario: str,
    gold_rows: Iterable[dict],
    prediction_rows: Iterable[dict],
    ambiguous: Iterable[Pair] = (),
    display_threshold: float = 0.7,
) -> FkScore:
    """Score one scenario.

    ``gold_rows``: dicts with src_table, src_column, dst_table, dst_column, expected_is_fk.
    ``prediction_rows``: dicts with src_table, src_column, dst_table, dst_column, is_fk,
    final_confidence (fk_predictions rows already filtered to the scenario).
    ``ambiguous``: extra pairs excluded from both FP and recall, on top of gold rows
    whose ``expected_is_fk`` is None.
    """
    amb = {_key(a.lower(), b.lower()) for a, b in ambiguous}
    gold_dir: Dict[Key, Pair] = {}
    traps = set()
    for g in gold_rows:
        a, b = _short(g["src_table"], g["src_column"]), _short(g["dst_table"], g["dst_column"])
        label = g.get("expected_is_fk")
        if label is None:
            amb.add(_key(a, b))
        elif bool(label):
            gold_dir[_key(a, b)] = (a, b)
        else:
            traps.add(_key(a, b))

    pred_dir: Dict[Key, Pair] = {}
    best_conf: Dict[Key, Tuple[float, Pair]] = {}
    for p in prediction_rows:
        a, b = _short(p["src_table"], p["src_column"]), _short(p["dst_table"], p["dst_column"])
        k = _key(a, b)
        conf = float(p.get("final_confidence") or 0.0)
        if bool(p.get("is_fk")):
            pred_dir[k] = (a, b)
        elif conf > best_conf.get(k, (-1.0, None))[0]:
            best_conf[k] = (conf, (a, b))

    s = FkScore(scenario=scenario, gold=len(gold_dir), predicted=len(pred_dir))
    for k, exp in gold_dir.items():
        if k in pred_dir:
            s.tp += 1
            s.found.append(exp)
            if pred_dir[k] != exp:
                s.reversed += 1
                s.reversed_pairs.append(exp)
        else:
            s.fn += 1
            s.missed.append(exp)
    for k, got in pred_dir.items():
        if k in gold_dir or k in amb:
            continue
        s.fp += 1
        s.false_positives.append(got)
        if k in traps:
            s.trap_fp += 1
            s.trap_hits.append(got)
    for k, (conf, pair) in best_conf.items():
        if k in pred_dir or k in gold_dir or k in amb:
            continue
        if conf >= display_threshold:
            s.high_conf_non_fk += 1
            s.high_conf_pairs.append((pair[0], pair[1], round(conf, 3)))
    return s


def pooled(scores: Iterable[FkScore]) -> FkScore:
    """Sum counts across scenarios (micro-averaged precision/recall)."""
    out = FkScore(scenario="ALL")
    for s in scores:
        for f in ("gold", "predicted", "tp", "fn", "fp", "trap_fp", "reversed", "high_conf_non_fk"):
            setattr(out, f, getattr(out, f) + getattr(s, f))
    return out


def format_report(scores: List[FkScore]) -> str:
    """Human-readable report: one summary line per scenario plus every miss/FP."""
    def pct(v):
        return "  -  " if v is None else f"{v:5.2f}"

    lines = [f"{'scenario':28s} gold  tp  fn  fp trap rev hiconf  prec  recall  dir"]
    for s in list(scores) + [pooled(scores)]:
        lines.append(
            f"{s.scenario:28s} {s.gold:4d} {s.tp:3d} {s.fn:3d} {s.fp:3d} {s.trap_fp:4d} "
            f"{s.reversed:3d} {s.high_conf_non_fk:6d} {pct(s.precision)} {pct(s.recall)}  "
            f"{pct(s.direction_accuracy)}"
        )
    for s in scores:
        for a, b in s.missed:
            lines.append(f"  [{s.scenario}] MISS  {a} -> {b}")
        for a, b in s.false_positives:
            tag = "TRAP" if (a, b) in s.trap_hits else "FP  "
            lines.append(f"  [{s.scenario}] {tag}  {a} -> {b}")
        for a, b in s.reversed_pairs:
            lines.append(f"  [{s.scenario}] REV   {a} -> {b} (predicted reversed)")
        for a, b, c in s.high_conf_pairs:
            lines.append(f"  [{s.scenario}] HICONF {a} -> {b} conf={c} (is_fk=false)")
    return "\n".join(lines)
