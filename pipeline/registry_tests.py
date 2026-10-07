"""Registry tests — every invariant in registry.check_registry() and registry.check_membership()
has a PLANTED violation here that the named check must catch, plus a clean fixture that every
check must pass (the negative control: a suite that cannot fail is not a suite).

Run:  python pipeline/registry_tests.py        (exit code 1 on any failing case)
"""
import copy
import hashlib
import json
import os
import sys
import tempfile

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import registry as R  # noqa: E402

SIB = {"corpus": "pots-findings", "corpus_version": "v1.1-test"}
ME = {"corpus": "eds-findings", "corpus_version": "v0-test", "tier": "tier5",
      "pipeline_generation": "tier4", "prompt_vid": "vid-eds-prompt", "prompt_sha256": "e" * 64,
      "prompt_file": "prompt_template.txt", "pipeline_file": "pipeline.py"}


def _case(name, got, want, note=""):
    return {"case": name, "got": got, "want": want, "pass": got == want, "note": note}


def _sha(b):
    return hashlib.sha256(b).hexdigest()


def make_package(root, name, tier, prompt_file, pipeline_file, chunk_ids, rows, extra_code=()):
    d = os.path.join(root, name)
    os.makedirs(os.path.join(d, "code"))
    os.makedirs(os.path.join(d, "data"))
    pbytes = f"PROMPT for {name}\n".encode()
    open(os.path.join(d, "code", prompt_file), "wb").write(pbytes)
    open(os.path.join(d, "code", pipeline_file), "w").write("# pipeline\n")
    for f in extra_code:
        open(os.path.join(d, "code", f), "w").write("x\n")
    df = pd.DataFrame(rows, columns=["pmid", "chunk_id", "year", "doi", "finding_text"])
    table = f"{tier}_findings.parquet"
    df.to_parquet(os.path.join(d, "data", table), index=False)
    man = {"package": f"{name}-v1.1", "tier": tier, "primary_table": table,
           "contents": {"findings": len(df), "papers": df.pmid.nunique(), "chunks": len(chunk_ids), "chunk_ids": chunk_ids},
           "source_corpus_version_id": "src-vid-test",
           "files": [{"path": f"code/{prompt_file}", "bytes": len(pbytes), "sha256": _sha(pbytes)}]}
    json.dump(man, open(os.path.join(d, "MANIFEST.json"), "w"))
    return d


def fixture(root):
    # sibling: two packages, generations schema_v5 (tier1) and tier4 (tier4)
    p1 = make_package(root, "core", "tier1", "schema_v5_extraction_prompt.txt", "extraction_pipeline.py", [0, 1],
                      [("100", 0, 2020, "10.1/a", "f"), ("100", 0, 2020, "10.1/a", "g"), ("101", 0, 2021, None, "f"),
                       ("102", 1, 2019, None, "f")])
    p4 = make_package(root, "hyper", "tier4", "tier4_prompt.txt", "tier4_pipeline.py", [2],
                      [("200", 2, 2022, "10.1/b", "f"), ("201", 2, 2023, None, "f")])
    chunk_order = pd.DataFrame([("100", "10.1/a", 1, 0), ("101", None, 1, 0), ("102", None, 1, 1),
                                ("103", None, 1, 1),            # dispatched, no yield
                                ("200", "10.1/b", 4, 2), ("201", None, 4, 2), ("202", None, 4, 2),  # 202 no yield
                                (None, None, 1, 1)],            # a row without pmid (flagged, not dropped silently)
                               columns=["pmid", "doi", "tier", "chunk_id"])
    universe = pd.DataFrame([("100", "10.1/a", 2020, "eds_core"),   # shared (core)
                             ("200", "10.1/b", 2022, "hsd_jhs"),    # shared (hyper)
                             ("103", None, 2021, "eds_core"),       # new, sibling dispatched no yield
                             ("300", None, 2024, "gjh_trait"),      # new
                             ("301", None, 1999, "eds_core")],      # new, old
                            columns=["pmid", "doi", "year", "theme"])
    return [p1, p4], chunk_order, universe


def shipped_sets(pkg_dirs, corpus):
    s = set()
    for d in pkg_dirs:
        p = R.read_package_provenance(d)
        s |= set(pd.read_parquet(os.path.join(d, "data", p["primary_table"]), columns=["pmid"]).pmid.astype(str))
    return {corpus: s}


def names_failing(results):
    return sorted(r["name"] for r in results if not r["ok"])


def _row(reg, **where):
    m = pd.Series(True, index=reg.index)
    for k, v in where.items():
        m &= reg[k] == v
    idx = reg.index[m]
    assert len(idx) == 1, (where, len(idx))
    return idx[0]


def run():
    cases = []
    with tempfile.TemporaryDirectory() as root:
        pkgs, co, uni = fixture(root)
        reg, log = R.build_registry(pkgs, co, uni, this_corpus=ME, sibling_corpus=SIB,
                                    prompt_vids={R.read_package_provenance(pkgs[1])["prompt_sha256"]: "vid-t4-prompt"},
                                    universe_source_version_id="uni-vid")
        shipped = shipped_sets(pkgs, SIB["corpus"])
        expected = {"core": 3, "hyper": 2, "eds-findings": {"referenced": 2, "planned": 3}}

        # --- build: provenance is READ, not typed ------------------------------------------------
        g = {r["package_dir"]: r["pipeline_generation"] for r in log["packages"]}
        cases.append(_case("build_generation_from_code_listing", g, {"core": "schema_v5", "hyper": "tier4"}))
        cases.append(_case("build_prompt_vid_from_mapping_else_sha",
                           sorted(reg[reg.corpus == SIB["corpus"]].prompt_ref.unique().tolist()),
                           sorted(["sha256:" + R.read_package_provenance(pkgs[0])["prompt_sha256"], "vid-t4-prompt"])))
        cases.append(_case("build_statuses", reg.groupby(["corpus", "extraction_status"]).pmid.nunique().to_dict(),
                           {("eds-findings", "planned"): 3, ("eds-findings", "referenced"): 2,
                            ("pots-findings", "dispatched_no_yield"): 2, ("pots-findings", "extracted"): 5}))
        cases.append(_case("build_flags_chunk_order_row_without_pmid", log["chunk_order_rows_without_pmid"], 1))
        cases.append(_case("build_sibling_dispatched_no_yield_flag",
                           sorted(reg[reg.sibling_dispatched_no_yield.astype(bool)].pmid.tolist()), ["103"]))
        cases.append(_case("build_n_findings_grouped", int(reg.loc[_row(reg, pmid="100", corpus="pots-findings"), "n_findings"]), 2))
        cases.append(_case("build_row_count_is_universe_plus_dispatched", len(reg), 5 + 2 + 5))

        # ambiguous code/ listing must RAISE
        try:
            open(os.path.join(pkgs[0], "code", "second_prompt.txt"), "w").write("x")
            R.read_package_provenance(pkgs[0]); got = "no error"
        except ValueError:
            got = "raised"
        finally:
            os.remove(os.path.join(pkgs[0], "code", "second_prompt.txt"))
        cases.append(_case("build_raises_on_two_prompt_files", got, "raised"))
        # manifest/disk hash disagreement must RAISE
        pf = os.path.join(pkgs[1], "code", "tier4_prompt.txt"); orig = open(pf, "rb").read()
        try:
            open(pf, "wb").write(orig + b"edited")
            R.read_package_provenance(pkgs[1]); got = "no error"
        except ValueError:
            got = "raised"
        finally:
            open(pf, "wb").write(orig)
        cases.append(_case("build_raises_on_prompt_hash_mismatch", got, "raised"))

        # --- negative control: the clean fixture passes everything ------------------------------
        clean = R.check_registry(reg, shipped_pmids=shipped, expected=expected)
        cases.append(_case("clean_fixture_all_ok", names_failing(clean), [], note=str([(r["name"], r["examples"]) for r in clean if not r["ok"]])))
        cases.append(_case("check_count", len(clean), 12))

        # --- planted violations: each must be caught by the NAMED check -------------------------
        def planted(name, mutate, must_fail, shipped_=shipped, expected_=expected, exceptions=None):
            r2 = reg.copy(deep=True)
            r2["shared_with"] = r2["shared_with"].apply(list)
            r2 = mutate(r2)
            res = R.check_registry(r2, shipped_pmids=shipped_, expected=expected_, exceptions=exceptions)
            caught = sorted(set(names_failing(res)) & set(must_fail))
            cases.append(_case(f"planted_{name}", caught, sorted(must_fail),
                               note="failing=" + ",".join(names_failing(res))))

        def add_eds_extracted(r2, pmid, gen):
            i = _row(r2, pmid=pmid, corpus="eds-findings")
            r2.loc[i, ["extraction_status", "reference_only", "n_findings", "pipeline_generation"]] = ["extracted", False, 3, gen]
            r2.at[i, "shared_with"] = ["pots-findings"]
            return r2

        # 1. same pmid extracted in two corpora under the SAME generation
        planted("double_extraction_same_generation", lambda r2: add_eds_extracted(r2, "200", "tier4"),
                ["one_extraction_per_pmid_per_generation", "no_reextraction_of_sibling_paper"])
        # 1b. a documented exception clears BOTH checks (the exception path is itself tested)
        r2 = reg.copy(deep=True); r2["shared_with"] = r2["shared_with"].apply(list); r2 = add_eds_extracted(r2, "200", "tier4")
        exc = pd.DataFrame([{"pmid": "200", "pipeline_generation": "tier4", "reason": "test"}])
        res = R.check_registry(r2, shipped_pmids=shipped, expected=None, exceptions=exc)
        cases.append(_case("exception_clears_double_extraction",
                           sorted(set(names_failing(res)) & {"one_extraction_per_pmid_per_generation", "no_reextraction_of_sibling_paper"}), []))
        # 2. re-extraction under a DIFFERENT generation: generation invariant passes, policy fails
        r2 = reg.copy(deep=True); r2["shared_with"] = r2["shared_with"].apply(list); r2 = add_eds_extracted(r2, "100", "tier4")
        res = R.check_registry(r2, shipped_pmids=shipped)
        cases.append(_case("planted_reextraction_other_generation",
                           {"gen_inv": "one_extraction_per_pmid_per_generation" in names_failing(res),
                            "policy": "no_reextraction_of_sibling_paper" in names_failing(res)},
                           {"gen_inv": False, "policy": True}))

        # 3. referenced pmid that no sibling actually holds
        def fake_ref(r2):
            i = _row(r2, pmid="300", corpus="eds-findings")
            r2.loc[i, ["extraction_status", "reference_only", "universe_status"]] = ["referenced", True, "shared"]
            r2.at[i, "shared_with"] = ["pots-findings"]
            return r2
        planted("referenced_not_in_sibling", fake_ref, ["referenced_exists_in_sibling", "shared_with_consistent"])
        # 3b. referenced row whose sibling registry row exists but pmid is missing from SHIPPED data
        planted("referenced_missing_from_shipped", lambda r2: r2,
                ["referenced_exists_in_sibling"], shipped_={"pots-findings": {"100"}})   # 200 removed from shipped set
        # 4. shared_with blanked on a referenced row
        def blank_sw(r2):
            r2.at[_row(r2, pmid="100", corpus="eds-findings"), "shared_with"] = []; return r2
        planted("shared_with_blank", blank_sw, ["shared_with_consistent", "referenced_exists_in_sibling"])
        # 5. reference_only flipped
        def flip_ro(r2):
            r2.loc[_row(r2, pmid="100", corpus="eds-findings"), "reference_only"] = False; return r2
        planted("reference_only_flipped", flip_ro, ["reference_only_consistent"])
        # 6. universe_status wrong
        def bad_us(r2):
            r2.loc[_row(r2, pmid="100", corpus="eds-findings"), "universe_status"] = "new"; return r2
        planted("universe_status_wrong", bad_us, ["universe_status_consistent"])
        # 7. extracted with zero findings
        def zero_n(r2):
            r2.loc[_row(r2, pmid="101", corpus="pots-findings"), "n_findings"] = 0; return r2
        planted("extracted_zero_findings", zero_n, ["n_findings_by_status"])
        # 8. a pmid extracted in two packages of one corpus
        def two_pkgs(r2):
            row = r2.loc[[_row(r2, pmid="101", corpus="pots-findings")]].copy()
            row["package"] = "hyper"; row["chunk_id"] = 2; row["tier"] = "tier4"
            return pd.concat([r2, row], ignore_index=True)
        planted("partition_violation", two_pkgs, ["partition_within_corpus", "counts_sum"])
        # 9. missing generation on an extracted row / two generations in a package
        def no_gen(r2):
            r2.loc[_row(r2, pmid="101", corpus="pots-findings"), "pipeline_generation"] = None; return r2
        planted("generation_missing", no_gen, ["generation_provenance"])
        def two_gens(r2):
            r2.loc[_row(r2, pmid="101", corpus="pots-findings"), "pipeline_generation"] = "schema_v6"; return r2
        planted("two_generations_one_package", two_gens, ["generation_provenance"])
        # 10. counts: MANIFEST papers disagree with registry
        planted("counts_vs_manifest", lambda r2: r2, ["counts_sum"], expected_={"core": 4})
        planted("counts_vs_expected_status", lambda r2: r2, ["counts_sum"], expected_={"eds-findings": {"referenced": 3}})
        # 11. duplicate key
        planted("duplicate_key", lambda r2: pd.concat([r2, r2.iloc[[0]]], ignore_index=True), ["key_unique"])
        # 12. status outside enum
        def bad_status(r2):
            r2.loc[_row(r2, pmid="300", corpus="eds-findings"), "extraction_status"] = "done"; return r2
        planted("status_enum", bad_status, ["status_enum"])
        # every check in check_registry has at least one planted case above
        checked = {r["name"] for r in clean}
        planted_names = set()
        for c in cases:
            if c["case"].startswith("planted_") and isinstance(c["want"], list):
                planted_names |= set(c["want"])
        planted_names |= {"one_extraction_per_pmid_per_generation", "no_reextraction_of_sibling_paper"}
        cases.append(_case("every_check_has_a_planted_violation", sorted(checked - planted_names), []))

        # --- counts: every rate carries n, denominator, basis ---------------------------------------
        cnt = R.registry_counts(reg, "eds-findings", "pots-findings")
        tc = cnt["this_corpus"]
        cases.append(_case("counts_shared_new", (tc["shared"]["n"], tc["new"]["n"], tc["universe"]), (2, 3, 5)))
        cases.append(_case("counts_by_year_window", {k: v["universe"] for k, v in tc["by_year_window"].items()},
                           {"2019-2026": 4, "2010-2026": 4, "all": 5}))
        cases.append(_case("counts_by_theme_denominators", {k: v["universe"] for k, v in tc["by_theme"].items()},
                           {"eds_core": 3, "gjh_trait": 1, "hsd_jhs": 1}))
        def all_rates_have_basis(o):
            if isinstance(o, dict):
                if set(o) >= {"n", "denominator", "basis"}:
                    return o["basis"] != "" and o["denominator"] >= 0
                return all(all_rates_have_basis(v) for v in o.values())
            return True
        cases.append(_case("counts_rates_carry_basis", all_rates_have_basis(cnt), True))

        # --- membership table -----------------------------------------------------------------------
        enum = {"eds-findings": ["hEDS", "HSD", "EDS_other_type", "EDS_unspecified_or_mixed", "hypermobility_unspecified", "multiple", "general"]}
        mem = pd.DataFrame([{"pmid": "100", "corpus": "eds-findings", "population_labels": ["hEDS", "HSD"],
                             "deciding_span": "patients with hypermobile EDS and HSD", "span_verified_verbatim": True,
                             "definitions_version": "eds v1 (test)", "classifier": "human:XX",
                             "assessed_at": "2026-10-07T00:00:00Z", "basis": "test"}])
        cases.append(_case("membership_clean_ok", names_failing(R.check_membership(mem, reg, enum)), []))
        m2 = mem.copy(); m2.at[0, "population_labels"] = ["EDS_hypermobility"]   # the SIBLING's label, not ours
        cases.append(_case("planted_membership_label_outside_enum", names_failing(R.check_membership(m2, reg, enum)), ["membership_labels_in_enum"]))
        m2 = mem.copy(); m2.at[0, "pmid"] = "999"
        cases.append(_case("planted_membership_pmid_not_in_registry", names_failing(R.check_membership(m2, reg, enum)), ["membership_pmid_in_registry"]))
        m2 = mem.copy(); m2.at[0, "deciding_span"] = "  "
        cases.append(_case("planted_membership_empty_span", names_failing(R.check_membership(m2, reg, enum)), ["membership_span_present"]))
        m2 = pd.concat([mem, mem], ignore_index=True)
        cases.append(_case("planted_membership_duplicate", names_failing(R.check_membership(m2, reg, enum)), ["membership_one_row_per_pmid_corpus_version"]))
        m2 = mem.copy(); m2.at[0, "classifier"] = None
        cases.append(_case("planted_membership_no_classifier", names_failing(R.check_membership(m2, reg, enum)), ["membership_provenance_present"]))
        m2 = mem.drop(columns=["basis"])
        cases.append(_case("planted_membership_schema", names_failing(R.check_membership(m2, reg, enum)), ["membership_schema"]))

        # --- config block ---------------------------------------------------------------------------
        cfg = {"registry": {"corpus": "eds-findings", "corpus_version": "v0",
                            "siblings": [{"corpus": "pots-findings", "corpus_version": "v1.1", "registry_vid": "x", "repo_vid": "y"}],
                            "exceptions": []}}
        cases.append(_case("registry_config_ok", R.registry_config(cfg)["siblings"][0]["corpus"], "pots-findings"))
        bad = copy.deepcopy(cfg); del bad["registry"]["siblings"][0]["registry_vid"]
        try:
            R.registry_config(bad); got = "no error"
        except ValueError:
            got = "raised"
        cases.append(_case("registry_config_rejects_sibling_without_registry_vid", got, "raised"))
        bad = copy.deepcopy(cfg); bad["registry"]["exceptions"] = [{"pmid": "1", "pipeline_generation": "tier4"}]
        try:
            R.registry_config(bad); got = "no error"
        except ValueError:
            got = "raised"
        cases.append(_case("registry_config_rejects_exception_without_reason", got, "raised"))
    return cases


if __name__ == "__main__":
    cases = run()
    npass = sum(c["pass"] for c in cases)
    for c in cases:
        if not c["pass"]:
            print("FAIL", c["case"], "got=", c["got"], "want=", c["want"], c["note"])
    print(f"registry_tests: {npass}/{len(cases)} passed")
    sys.exit(0 if npass == len(cases) else 1)
