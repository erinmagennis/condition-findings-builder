"""Unit tests for the ceiling-aware dispatch functions carried into Tier 4.

These are VERIFICATION, not new code: is_ceiling(), done_units() and the ceiling
branch of run() are the pinned driver_tier3_parts.py implementations. The tests pin
the exact failure shapes that motivated them (chunks 244 and 246, which each burned
two ~2M-token attempts) so a future edit that reintroduces the defect fails loudly.

Every case returns a computed pass/fail; nothing is asserted by narration.
"""
import json, re, importlib.util, sys, os

HERE = os.path.dirname(os.path.abspath(__file__))
# BUILDER: the driver reads its constants from $CORPUS_CONFIG. The tests run against the
# POTS config unless the caller points them elsewhere; the dispatch logic under test is
# config-independent (targets are passed in explicitly below).
os.environ.setdefault("CORPUS_CONFIG", os.path.join(HERE, "..", "config", "pots_dysautonomia.yaml"))


def load(path=os.path.join(HERE, "driver.py"), name="driver"):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


class FakeHost:
    """Minimal host stub: only artifacts(filename=...) is exercised by done_units."""
    def __init__(self, filenames):
        self.filenames = list(filenames)
        self.calls = []
    def artifacts(self, filename=None, limit=None, **kw):
        self.calls.append(filename)
        hits = [f for f in self.filenames if filename is None or filename in f]
        return {"count": len(hits), "artifacts": [{"filename": f} for f in hits]}


def _case(name, got, want, note=""):
    return {"case": name, "got": got, "want": want,
            "pass": got == want, "note": note}


def test_is_ceiling(D):
    """The shape that made defect 1: version_id is EMPTY, so any gate that required a
    truthy vid never ran. is_ceiling reads stats.error alone."""
    cases = []
    # --- positives: real ceiling error texts, all with version_id='' ---
    pos = [
        ("chunk244_exact", {"error": "worker killed: exceeded the 2.0M per-frame token ceiling"}),
        ("per_frame_token_phrase", {"error": "per-frame token budget exhausted"}),
        ("token_ceiling_phrase", {"error": "hit token ceiling before finishing 50 papers"}),
        ("case_insensitive", {"error": "TOKEN CEILING REACHED"}),
        ("2p0M_bare", {"error": "frame stopped at 2.0M tokens"}),
        ("nested_in_traceback", {"error": "RuntimeError: ... per-frame token limit ...\nTraceback"}),
    ]
    for nm, st in pos:
        cases.append(_case(f"is_ceiling:{nm}", D.is_ceiling(st), True,
                           "version_id='' — must still be detected"))
    # --- negatives: must NOT be read as a ceiling ---
    neg = [
        ("quota_signature_empty", {"n_findings": 0, "parse_fail": 0, "blocked": 0}),
        ("quota_error_text", {"error": "insufficient account credit / quota exhausted"}),
        ("http503", {"error": "HTTP 503 from Europe PMC after 6 attempts"}),
        ("parse_failure", {"error": "JSONDecodeError at position 4102"}),
        ("empty_error", {"error": ""}),
        ("no_error_key", {}),
        ("none_stats", None),
        ("max_tokens_output", {"error": "stop_reason=max_tokens on 3 papers"}),
    ]
    for nm, st in neg:
        cases.append(_case(f"is_ceiling:{nm}", D.is_ceiling(st), False))
    return cases


def test_ceiling_not_a_budget_failure(D):
    """Defect 2: a ceiling kill shares the quota SURFACE signature (no findings, no
    parse_fail, no blocked). Two of those in a row trip BUDGET_HALT_STREAK in the old
    driver and halt the whole run. Here the surface signature is identical and only
    the error text separates them — so we test that the two are distinguished, and
    that run()'s ceiling branch exits before the `failed += 1` accounting."""
    surface = {"n_findings": 0, "parse_fail": 0, "blocked": 0}
    ceil_st = dict(surface, error="worker killed: 2.0M per-frame token ceiling")
    quota_st = dict(surface, error="")
    cases = [
        _case("shared_surface_signature_is_identical",
              {k: ceil_st[k] for k in surface} == {k: quota_st[k] for k in surface}, True,
              "the two are indistinguishable except by error text"),
        _case("ceiling_detected_on_shared_surface", D.is_ceiling(ceil_st), True),
        _case("quota_not_detected_as_ceiling", D.is_ceiling(quota_st), False),
        _case("gate_reason_ceiling", D.gate(244, {"stats": ceil_st})[3],
              "token-ceiling (split required)"),
        _case("gate_reason_quota", D.gate(244, {"stats": quota_st})[3],
              "quota-signature (empty, no parse_fail, no blocked)"),
    ]
    # structural: in run(), the ceiling branch must `continue` BEFORE `failed += 1`
    src = D.run.__doc__ and ""  # keep linters quiet
    import inspect
    body = inspect.getsource(D.run)
    ceil_at = body.index("if is_ceiling(st) and not vid:")
    cont_at = body.index("continue", ceil_at)
    fail_at = body.index("failed += 1")
    cases.append(_case("ceiling_branch_continues_before_failure_accounting",
                       cont_at < fail_at, True,
                       "a ceiling kill can never increment the fail counter"))
    return cases


def test_done_units(D):
    """Defect 3: a chunk completed as a COMPLETE PART SET must count as done.
    Target set is the POTS Tier-4 range; it is test data, not configuration."""
    cases = []
    T = set(range(256, 286))

    h = FakeHost(["chunk_256_findings.parquet"])
    d, p = D.done_units(h, T)
    cases.append(_case("whole_file_is_done", (sorted(d), p), ([256], {})))

    h = FakeHost(["chunk_257_part0of2_findings.parquet", "chunk_257_part1of2_findings.parquet"])
    d, p = D.done_units(h, T)
    cases.append(_case("complete_part_set_is_done", sorted(d), [257],
                       "the defect-3 shape: without this the chunk re-dispatches forever"))

    h = FakeHost(["chunk_258_part0of2_findings.parquet"])
    d, p = D.done_units(h, T)
    cases.append(_case("partial_part_set_not_done", (sorted(d), p), ([], {258: [(0, 2)]}),
                       "have_parts reports the saved part so only part1 is re-dispatched"))
    cases[-1]["got"] = (sorted(d), {k: sorted(v) for k, v in p.items()})
    cases[-1]["pass"] = cases[-1]["got"] == cases[-1]["want"]

    h = FakeHost(["chunk_259_part0of4_findings.parquet", "chunk_259_part1of4_findings.parquet",
                  "chunk_259_part2of4_findings.parquet", "chunk_259_part3of4_findings.parquet"])
    d, _ = D.done_units(h, T)
    cases.append(_case("complete_4_part_set_after_resplit_is_done", sorted(d), [259]))

    h = FakeHost(["chunk_260_part0of2_findings.parquet", "chunk_260_part1of4_findings.parquet"])
    d, _ = D.done_units(h, T)
    cases.append(_case("mixed_denominator_set_not_done", sorted(d), [],
                       "parts from two different splits never satisfy each other"))

    h = FakeHost(["chunk_255_findings.parquet", "chunk_255_part0of2_findings.parquet",
                  "chunk_255_part1of2_findings.parquet"])
    d, _ = D.done_units(h, T)
    cases.append(_case("out_of_target_chunk_ignored", sorted(d), [],
                       "a chunk outside the target list (prior tier) must not be counted"))

    h = FakeHost(["chunk_261_findings.parquet.bak", "chunk_261_fulltext.json.gz",
                  "chunk_261x_findings.parquet"])
    d, _ = D.done_units(h, T)
    cases.append(_case("anchored_regex_rejects_lookalikes", sorted(d), [],
                       "$-anchored patterns: a .bak or an archive is not a findings file"))

    h = FakeHost(["chunk_262_findings.parquet", "chunk_263_part0of2_findings.parquet",
                  "chunk_263_part1of2_findings.parquet", "chunk_264_part1of2_findings.parquet"])
    d, p = D.done_units(h, T)
    cases.append(_case("mixed_corpus_done_set", sorted(d), [262, 263]))
    cases.append(_case("mixed_corpus_have_parts",
                       {k: sorted(v) for k, v in p.items()},
                       {263: [(0, 2), (1, 2)], 264: [(1, 2)]}))
    return cases


def test_area_namespace(D):
    """BUILDER 2026-09-30: a second area in the same project must not see the first area's
    chunk artifacts as done. Measured on the EDS dry run: with the un-prefixed detector,
    chunks 0-2 were 'already done' from the POTS build and nothing dispatched."""
    cases = []
    T = {0, 1, 2}
    prev = D.FILE_PREFIX
    try:
        D.FILE_PREFIX = "eds_"
        h = FakeHost(["chunk_000_findings.parquet", "chunk_001_findings.parquet", "chunk_002_findings.parquet",
                      "chunk_000_fulltext.json.gz"])
        d, p = D.done_units(h, T); aw, ap = D.archive_units(h, T)
        cases.append(_case("other_area_chunks_are_not_done", (sorted(d), p, sorted(aw), ap), ([], {}, [], {}),
                           "POTS chunk_000..002 must not satisfy an eds_ run"))
        h = FakeHost(["eds_chunk_000_findings.parquet", "chunk_001_findings.parquet",
                      "eds_chunk_002_part0of2_findings.parquet", "eds_chunk_002_part1of2_findings.parquet",
                      "xeds_chunk_001_findings.parquet"])
        d, p = D.done_units(h, T)
        cases.append(_case("own_prefix_whole_and_part_set_done", sorted(d), [0, 2],
                           "prefix anchored at the START: xeds_ is not eds_"))
        D.FILE_PREFIX = ""
        h = FakeHost(["chunk_000_findings.parquet", "eds_chunk_001_findings.parquet"])
        d, _ = D.done_units(h, T)
        cases.append(_case("legacy_empty_prefix_ignores_prefixed_files", sorted(d), [0],
                           "the POTS config (prefix '') keeps its byte-identical legacy regex"))
        task = D.build_task(1, None)
        cases.append(_case("legacy_task_filename_unprefixed", "chunk_001_findings.parquet" in task, True))
        D.FILE_PREFIX = "eds_"
        task = D.build_task(1, (0, 2))
        cases.append(_case("prefixed_task_filename", "eds_chunk_001_part0of2_findings.parquet" in task
                           and "eds_chunk_001_part0of2_fulltext.json.gz" in task, True))
    finally:
        D.FILE_PREFIX = prev
    return cases


def test_work_based_sizing(D):
    """BUILDER 2026-09-30: units are sized by expected FULL-TEXT count, and a watchdog reclaim
    re-splits. Measured cause: EDS dry-run chunk 0 (25/25 OA papers, one unit) exceeded
    WORKER_MAX_S with nothing saved and was re-queued in the same shape twice."""
    import pandas as pd
    cases = []
    ro = pd.DataFrame({"chunk_id": [0]*25 + [1]*25 + [2]*25,
                       "pmcid": ["PMC1"]*25 + ["PMC2"]*13 + [None]*12 + [None]*25,
                       "is_oa": [True]*25 + [True]*13 + [False]*12 + [False]*25,
                       "pmid": [str(i) for i in range(75)]})
    cases.append(_case("all_fulltext_chunk_splits_to_3", D.plan_parts(ro, 0, n_parts_default=1, max_ft_per_unit=12)[0], 3,
                       "25 expected full texts / cap 12 -> 3 parts even though n_parts_default=1"))
    cases.append(_case("half_fulltext_chunk_splits_to_2", D.plan_parts(ro, 1, n_parts_default=1, max_ft_per_unit=12)[0], 2))
    cases.append(_case("abstract_only_chunk_stays_whole", D.plan_parts(ro, 2, n_parts_default=1, max_ft_per_unit=12)[0], 1))
    full50 = pd.DataFrame({"chunk_id": [9]*50, "pmcid": [None]*50, "is_oa": [False]*50, "pmid": [str(i) for i in range(50)]})
    prev_ppc = D.PAPERS_PER_CHUNK; D.PAPERS_PER_CHUNK = 50
    try:
        cases.append(_case("default_parts_holds_for_a_full_chunk", D.plan_parts(full50, 9, n_parts_default=2, max_ft_per_unit=12)[0], 2,
                           "a full 50-paper abstract-only chunk keeps the pinned 2 parts (unit <= 25 papers)"))
    finally:
        D.PAPERS_PER_CHUNK = prev_ppc
    ro2 = ro.assign(fulltext_expected=[True]*5 + [False]*70)
    cases.append(_case("explicit_fulltext_expected_column_wins", D.plan_parts(ro2, 0, 1, 12)[0], 1))
    small = pd.DataFrame({"chunk_id": [7]*6, "pmcid": [None]*6, "is_oa": [False]*6, "pmid": [str(i) for i in range(6)]})
    prev_ppc = D.PAPERS_PER_CHUNK; D.PAPERS_PER_CHUNK = 50
    try:
        cases.append(_case("screened_down_chunk_runs_as_one_unit", D.plan_parts(small, 7, n_parts_default=2, max_ft_per_unit=12)[0], 1,
                           "6 papers must not become 2 units; the default is a ceiling on unit size, not a floor on part count"))
        small_ft = small.assign(pmcid="PMC1", is_oa=True)
        cases.append(_case("small_chunk_still_splits_on_fulltext_density", D.plan_parts(small_ft, 7, n_parts_default=2, max_ft_per_unit=3)[0], 2))
    finally:
        D.PAPERS_PER_CHUNK = prev_ppc
    ro3 = ro.drop(columns=["pmcid", "is_oa"])
    cases.append(_case("unknown_flags_assume_all_fulltext", D.plan_parts(ro3, 2, 1, 12)[0], 3,
                       "no flags -> every paper may be full text -> conservative split"))
    body = open(D.__file__).read()
    wd = body.index("WATCHDOG reclaimed"); rs = body.find("watchdog -> re-splitting", wd)
    cases.append(_case("watchdog_reclaim_resplits", rs > wd and "if WATCHDOG_RESPLIT" in body[wd:rs], True,
                       "a time-out is re-dispatched smaller, exactly like a ceiling kill"))
    cases.append(_case("watchdog_resplit_is_config_gated", "watchdog_resplit" in open(os.path.join(HERE, "config.py")).read(), True))
    return cases


def test_archive_never_gates(D):
    """T4-B: retention is reporting-only. A findings file with NO archive is still done,
    and an archive alone is never done."""
    T = set(range(256, 286))
    cases = []
    h = FakeHost(["chunk_265_findings.parquet"])
    d, _ = D.done_units(h, T)
    aw, ap = D.archive_units(h, T)
    cases.append(_case("findings_without_archive_still_done", (sorted(d), sorted(aw)), ([265], [])))

    h = FakeHost(["chunk_266_fulltext.json.gz"])
    d, _ = D.done_units(h, T)
    aw, _ = D.archive_units(h, T)
    cases.append(_case("archive_alone_is_not_done", (sorted(d), sorted(aw)), ([], [266])))

    h = FakeHost(["chunk_267_part0of2_fulltext.json.gz", "chunk_267_part1of2_fulltext.json.gz"])
    _, ap = D.archive_units(h, T)
    cases.append(_case("part_aware_archive_census",
                       {k: sorted(v) for k, v in ap.items()}, {267: [(0, 2), (1, 2)]}))

    # a gate verdict must be unaffected by archive failure
    good = {"yield_frac": 0.9, "n_findings": 120, "parse_fail": 0, "blocked": 1,
            "blocked_frac": 0.02}
    ok_no_arc = D.gate(265, {"stats": dict(good, archive={"archive_error": "OSError: disk full"})})
    ok_arc = D.gate(265, {"stats": dict(good, archive={"file": "chunk_265_fulltext.json.gz"})})
    cases.append(_case("archive_error_does_not_fail_the_gate", (ok_no_arc[0], ok_no_arc[3]), (True, "ok")))
    cases.append(_case("gate_identical_with_and_without_archive", ok_no_arc == ok_arc, True))
    return cases


def test_task_ordering(D):
    """Findings must be saved BEFORE the archive, in both the worker code and the
    instructions — the ordering is the whole safety argument for change 1."""
    t = D.build_task(256, (0, 2))
    cases = [
        _case("task_saves_findings_before_archive",
              t.index("chunk_256_part0of2_findings.parquet") < t.index("chunk_256_part0of2_fulltext.json.gz"),
              True),
        _case("archive_call_is_guarded_in_worker_code",
              "try:" in t and "save_fulltext_archive" in t and "archive_error" in t, True),
        _case("archive_save_after_parquet_save_in_code",
              t.index("gdf.to_parquet(fn, index=False)") < t.index("save_fulltext_archive"), True),
        _case("part_aware_archive_filename",
              D.archive_name_expected(256, (0, 2)) if hasattr(D, "archive_name_expected")
              else ("chunk_256_part0of2_fulltext.json.gz" in t), True),
        _case("whole_chunk_archive_filename",
              "chunk_256_fulltext.json.gz" in D.build_task(256, None), True),
        _case("three_value_unpack_present",
              "rows, stats, retained = process_chunk(host, chunk_df)" in t, True),
    ]
    return cases


def run_all(path=os.path.join(HERE, "driver.py")):
    D = load(path)
    # The legacy fixtures spell filenames without an area prefix (the POTS shape). Run them under
    # FILE_PREFIX="" whatever config is loaded, so the suite is config-independent; the namespace
    # group then exercises the prefixed behaviour explicitly.
    _prev = D.FILE_PREFIX; D.FILE_PREFIX = ""; D.CODE_TMPL = D._fill_static(D.CODE_TMPL_RAW)
    try:
        groups = {
            "is_ceiling": test_is_ceiling(D),
            "ceiling_not_budget_failure": test_ceiling_not_a_budget_failure(D),
            "done_units": test_done_units(D),
            "archive_never_gates": test_archive_never_gates(D),
            "task_ordering": test_task_ordering(D),
        }
    finally:
        D.FILE_PREFIX = _prev; D.CODE_TMPL = D._fill_static(D.CODE_TMPL_RAW)
    groups.update({
        "area_namespace": test_area_namespace(D),
        "work_based_sizing": test_work_based_sizing(D),
    })
    n = sum(len(v) for v in groups.values())
    npass = sum(1 for v in groups.values() for c in v if c["pass"])
    return {"n_cases": n, "n_pass": npass, "n_fail": n - npass,
            "all_pass": npass == n,
            "failures": [c for v in groups.values() for c in v if not c["pass"]],
            "groups": {k: {"n": len(v), "pass": sum(1 for c in v if c["pass"]), "cases": v}
                       for k, v in groups.items()}}


if __name__ == "__main__":
    print(json.dumps(run_all(), indent=1))
