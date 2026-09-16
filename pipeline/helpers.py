"""Stage helpers for a condition-corpus build (screening scope text, arm reconciliation,
unit sizing, coverage bases, partition assertion, id-shape rejection). Exported from the
`condition-corpus-buildout` skill's kernel.py; see RUNBOOK.md for where each is used.

BUILDER: PINNED below is the calibrated parameter block for reference. The values a
build actually runs with come from config/<area>.yaml (`pinned:` and `model:`), and
MODEL_ID is deliberately absent here — the literal id must be written in the config.
"""
import re
import json

PINNED = {
    "FETCH_PACING": 0.9,
    "N_INFLIGHT": 3,
    "MAX_ATTEMPTS": 3,
    "WORKER_MAX_S": 3000,
    "PAPERS_PER_CHUNK": 50,
    "N_PARTS_DEFAULT": 1,
    "MIN_YIELD": 0.60,
    "WARN_BLOCKED": 0.05,
    "MAX_BLOCKED": 0.35,
    "MIN_FT_CHARS": 2000,
    "MAX_FT_TOKENS": 32000,
    "MAX_ABS_TOKENS": 8000,
    "FT_INPUT_CHAR_CAP": 120000,
    "SCREEN_MAX_TOKENS": 2000,
    "FRAME_TOKEN_CEILING": 2000000,
}

ID_SHAPED_PATTERN = (
    r"\b(?:HGNC|CHEBI|CHEBI_|HP|GO|MESH|MSH|UMLS|ENSG|UNIPROT|R-HSA|DOID|OMIM|NCIT)"
    r"\s*[:_]\s*\d+|\bENSG\d{6,}|\bR-HSA-\d+")


def id_shaped_rx():
    """Compiled detector for any model-emitted ontology/accession identifier."""
    return re.compile(ID_SHAPED_PATTERN, re.I)


def scope_definition(area, in_scope_families, offtopic_senses, offtopic_drift=None):
    """Build a screening scope text that cannot be read as a closed list.

    in_scope_families: {"family label": ["condition", ...]}
    offtopic_senses:   {"ABBREV": "the foreign sense(s) that are off topic"}
    offtopic_drift:    list of prose descriptions of topical drift.
    """
    L = ["SCOPE - %s" % area, "",
         "IN SCOPE IS BROAD. The lists below are ILLUSTRATIVE, NOT EXHAUSTIVE.",
         "Do NOT answer off_topic merely because a condition is not named below.", ""]
    for fam, items in in_scope_families.items():
        L.append("* ANY %s is IN SCOPE - including %s." % (fam, ", ".join(items)))
    L += ["", "Comorbidity, epidemiology, mechanism, biomarker, genetics, treatment and",
          "patient-experience studies of any of the above ALL COUNT as in scope.",
          "A study spanning many diseases including an in-scope one IS in scope.", "",
          "OFF TOPIC means one of two specific things:",
          "(1) WRONG ABBREVIATION SENSE - the paper uses an abbreviation in a sense",
          "    foreign to this corpus:"]
    for ab, sense in offtopic_senses.items():
        L.append("    %s = %s" % (ab, sense))
    L.append("(2) TOPICAL DRIFT -")
    for d in (offtopic_drift or ["a paper where an in-scope term appears as one incidental "
                                 "item with no research question about it"]):
        L.append("    %s" % d)
    L += ["", 'If genuinely torn, answer "uncertain". Uncertain papers are EXTRACTED and flagged.']
    return "\n".join(L)


def screen_reconcile(rule_verdict, llm_verdict, llm_span=None):
    """Two-arm screening reconciliation.

    Two asymmetries, and conflating them re-creates real defects:

    * `no_anchor` is ABSENCE of evidence. It must never override an affirmative
      LLM `in_scope` verdict carrying a verbatim span - that rule recovered a
      pyridostigmine-in-heart-failure paper and a Nav1.8 channelopathy.
    * `off_topic` from the rule arm is POSITIVE evidence of a wrong abbreviation
      sense. It must not be overridden by an LLM `in_scope` verdict either -
      that is how abbreviation-collision papers get pulled back into a corpus.
      It forces adjudication, not exclusion.

    Returns in_scope | off_topic | uncertain. Uncertain is EXTRACTED and flagged.
    """
    r = (rule_verdict or "").lower()
    m = (llm_verdict or "").lower()
    if r == "off_topic" and m == "off_topic":
        return "off_topic"
    if r == "off_topic":
        return "uncertain"          # positive wrong-sense evidence: adjudicate
    if m == "in_scope" and llm_span:
        return "in_scope"           # absence of an anchor is not counter-evidence
    if m == "in_scope":
        return "uncertain"          # affirmative verdict with no span to check
    if m == "off_topic":
        return "off_topic" if r == "no_anchor" else "uncertain"
    return "uncertain"


def screen_instrument_alarm(rule_offtopic_n, llm_offtopic_n, n_screened, factor=3.0):
    """Disagreement between arms this large means the INSTRUMENT is broken, not
    the papers. Re-read the prompt before re-reading the corpus."""
    lo, hi = sorted([max(rule_offtopic_n, 1), max(llm_offtopic_n, 1)])
    ratio = hi / lo
    return {"n_screened": n_screened, "rule_off_topic": rule_offtopic_n,
            "llm_off_topic": llm_offtopic_n, "ratio": round(ratio, 2),
            "alarm": bool(ratio >= factor),
            "action": ("STOP - audit the screening prompt for a closed in-scope list "
                       "before trusting either arm" if ratio >= factor else "proceed")}


def plan_units(n_records, prompt_tokens, max_tokens, ceiling=None,
               already_used=0, safety=0.75):
    """Size per-record LLM work BEFORE dispatch. The frame token ceiling is
    cumulative over a frame's whole life and applies to the orchestrator too."""
    if ceiling is None:
        ceiling = PINNED["FRAME_TOKEN_CEILING"]
    per = float(prompt_tokens + max_tokens)
    budget = (ceiling - already_used) * safety
    total = per * n_records
    if budget < per:
        return {"projected_tokens": int(total), "headroom_tokens": int(max(budget, 0)),
                "n_units": None, "records_per_unit": None, "tokens_per_unit": None,
                "fits_in_frame": False, "no_headroom": True,
                "action": ("this frame cannot run even ONE record - dispatch from a fresh "
                           "frame or reduce max_tokens; do NOT retry in place")}
    n_units = max(1, int(total / budget) + (1 if total % budget else 0))
    per_unit = -(-n_records // n_units) if n_records else 0
    return {"projected_tokens": int(total), "headroom_tokens": int(budget),
            "n_units": n_units, "records_per_unit": per_unit,
            "tokens_per_unit": int(per * per_unit),
            "fits_in_frame": bool(total <= budget), "no_headroom": False}


def coverage_bases(n_papers_yielding, n_dispatched, n_screened_in, n_universe):
    """Every coverage number needs its denominator named. Return all of them."""
    def pct(a, b):
        return round(100.0 * a / b, 2) if b else None
    return {"of_dispatched": pct(n_papers_yielding, n_dispatched),
            "of_screened_in_scope": pct(n_papers_yielding, n_screened_in),
            "of_pre_screening_universe": pct(n_papers_yielding, n_universe),
            "bases": {"dispatched": n_dispatched, "screened_in": n_screened_in,
                      "universe": n_universe, "yielding": n_papers_yielding}}


def stratified_rate(df, stratum_col, value_col, agg="mean"):
    """Compare a rate ACROSS strata before calling a drop a regression. Most
    apparent quality drops are composition shifts (abstract-only share)."""
    g = df.groupby(stratum_col)[value_col]
    out = getattr(g, agg)().to_frame(agg)
    out["n"] = g.size()
    out["share"] = (out["n"] / len(df)).round(4)
    return out.sort_values("n", ascending=False)


def assert_partition(frames, key="pmid"):
    """Assert zero overlap across every pair of tier/package frames."""
    names = list(frames)
    sets = {k: set(v[key].dropna()) for k, v in frames.items()}
    pairs = {}
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            inter = sets[a] & sets[b]
            pairs["%s|%s" % (a, b)] = len(inter)
    total = sum(len(s) for s in sets.values())
    union = len(set().union(*sets.values())) if sets else 0
    bad = {k: v for k, v in pairs.items() if v}
    assert not bad, "overlapping %s across packages: %s" % (key, bad)
    assert total == union, "counts do not sum: %d vs union %d" % (total, union)
    return {"pairs_checked": len(pairs), "per_package": {k: len(v) for k, v in sets.items()},
            "union": union}


def gene_case_collision_check(names):
    """Flag ALLCAPS/Titlecase pairs before any case-folding canonicalization.
    These are human/mouse ortholog symbols (MECP2 vs Mecp2) - lowercasing merges
    a mouse-knockout finding into the human gene."""
    sym = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,8}(?:-[A-Za-z0-9]{1,4})?$")
    groups = {}
    for n in set(x for x in names if isinstance(x, str)):
        if sym.match(n):
            groups.setdefault(n.lower(), set()).add(n)
    risky = {k: sorted(v) for k, v in groups.items()
             if len(v) > 1 and any(x.isupper() for x in v)
             and any(x[:1].isupper() and not x.isupper() for x in v)}
    return {"n_symbol_like": sum(len(v) for v in groups.values()),
            "n_ortholog_risk_groups": len(risky), "groups": risky,
            "verdict": "DO NOT case-fold" if risky else "no ortholog pairs detected"}


def reject_id_shaped(record, name_fields=None):
    """Hard-fail any model-emitted identifier. Models emit NAMES; the pipeline
    resolves names to ids against the authority."""
    fields = name_fields or ("entity", "phenotype_terms", "pathway_or_mechanism",
                             "symptom_correlate", "gene_marker", "finding_text")
    hits = {}
    for f in fields:
        v = record.get(f)
        if isinstance(v, str):
            m = id_shaped_rx().findall(v)
            if m:
                hits[f] = m
    return hits


def unpack_pinned_code(archive_path, tier="tier4", dest="pinned"):
    """Pull the pinned pipeline/driver/resolver/invariants out of a published
    package tarball so a new area starts from proven code, not a rewrite."""
    import tarfile
    import os
    os.makedirs(dest, exist_ok=True)
    got = []
    with tarfile.open(archive_path, "r:*") as tf:
        for m in tf.getmembers():
            parts = m.name.split("/")
            if m.isfile() and (tier in parts) and "code" in parts:
                m.name = os.path.basename(m.name)
                tf.extract(m, dest)
                got.append(os.path.join(dest, m.name))
    return sorted(got)
