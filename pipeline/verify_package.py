#!/usr/bin/env python3
"""Re-derive every headline number in this package from the shipped data.

Run this before you trust anything in the README. It reads only files inside the
package, needs no network, and exits 1 if any claimed number disagrees with the data.

    python code/verify_package.py                 # from the package root
    python code/verify_package.py --json          # machine-readable result
    python code/verify_package.py --self-test     # prove the checker still bites

Design rule inherited from the corpus build: a checker that only ever passes is
untested. --self-test plants regressions and asserts each one is caught.
"""
from __future__ import annotations
import argparse, json, sys, os, re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load(root: Path):
    import pandas as pd
    manifest = json.loads((root / "MANIFEST.json").read_text())
    stats = json.loads((root / "qa" / "package_stats.json").read_text())
    data_file = root / "data" / manifest["primary_table"]
    df = pd.read_parquet(data_file)
    return manifest, stats, df


def checks(df, stats, manifest):
    """Each check returns (name, ok, expected, measured). Measured is always recomputed."""
    import pandas as pd
    out = []

    def add(name, expected, measured):
        out.append(dict(name=name, ok=bool(expected == measured),
                        expected=expected, measured=measured))

    # --- shape -------------------------------------------------------------
    add("findings_rows", stats["findings"], int(len(df)))
    add("papers_unique_pmid", stats["papers"], int(df.pmid.nunique()))
    add("n_chunks", stats["n_chunks"], int(df.chunk_id.nunique()))
    add("chunk_ids", stats["chunk_ids"],
        sorted(int(x) for x in df.chunk_id.dropna().unique()))
    add("n_columns", manifest["schema"]["n_columns"], int(df.shape[1]))
    add("column_names", manifest["schema"]["columns"], list(df.columns))

    # --- the partition claim ----------------------------------------------
    # This package is one block of a five-way partition with zero paper overlap.
    add("tier_source_single_value", [stats["tier_source"]],
        sorted(df.tier_source.dropna().unique().tolist()))

    # --- yield, stratified (never pooled: see docs/COMPARABILITY.md) -------
    strata = {}
    for s, g in df.groupby("fulltext_status", dropna=False):
        strata[str(s)] = dict(papers=int(g.pmid.nunique()), findings=int(len(g)),
                              findings_per_paper=round(len(g) / max(g.pmid.nunique(), 1), 2))
    add("fulltext_strata", stats["fulltext_strata"], strata)

    # --- quote provenance --------------------------------------------------
    L = df.provenance_quote.fillna("").str.len()
    add("rows_with_quote", stats["quote"]["rows_with_quote"], int((L > 0).sum()))
    add("quote_median_chars", stats["quote"]["median_chars"], int(L.median()))
    add("quote_max_chars", stats["quote"]["max_chars"], int(L.max()))
    add("quote_over_200", stats["quote"]["over_200"], int((L > 200).sum()))
    add("quote_over_600", stats["quote"]["over_600"], int((L > 600).sum()))

    # --- ontology grounding: the metric that matters is CORRECTNESS --------
    idr = df.entity_ontology_id_resolved.notna()
    add("rows_with_resolved_id", stats["grounding"]["rows_with_resolved_id"], int(idr.sum()))
    add("id_resolved_frac_all_rows", stats["grounding"]["id_resolved_frac_all_rows"],
        round(float(idr.mean()), 4))
    add("rows_id_and_authority_label",
        stats["grounding"]["rows_id_and_authority_label"],
        int((idr & df.entity_ontology_label.notna()).sum()))
    add("ontology_id_verification",
        stats["grounding"]["verification"],
        {str(k): int(v) for k, v in df.ontology_id_verification.value_counts(dropna=False).items()})

    # --- capability flags: what this tier CAN support ----------------------
    caps = {c: dict(true=int((df[c] == True).sum()), false=int((df[c] == False).sum()),
                    null=int(df[c].isna().sum()))
            for c in ["cap_entity_kind", "cap_hpo_verified", "cap_number_in_quote",
                      "cap_quote_source_retained"]}
    add("capability_flags", stats["capability_flags"], caps)

    # --- null-column and constant-column declarations ---------------------
    add("all_null_columns", sorted(stats["all_null_columns"]),
        sorted([c for c in df.columns if df[c].isna().all()]))

    return out


def manifest_condition_enum():
    m = json.loads((ROOT / "MANIFEST.json").read_text())
    enum = m.get("condition_enum")
    if not enum:
        raise SystemExit("MANIFEST.json lacks 'condition_enum'; build_packages.py must write it "
                         "from the config so the enum-drift check runs against the right enum")
    return enum


def invariants(df, expected_chunks, allotment):
    """The PINNED nine-invariant suite, unmodified, re-run on the shipped slice.

    Imported from code/invariants.py + code/invariants_ext.py shipped inside the package
    (build_packages.py copies them from pipeline/). BUILDER: the condition enum comes
    from MANIFEST.json["condition_enum"], which build_packages.py writes from the config —
    a package therefore carries the enum it was checked against.
    """
    sys.path.insert(0, str(ROOT / "code"))
    import invariants as INV
    import invariants_ext as INVX
    enum = manifest_condition_enum()
    r = INV.run_all(df, expected_chunks, allotment, prior_rows=0, prior_papers=0,
                    condition_enum=enum)
    r9 = INVX.check_authority_prefix_match(df)
    r["checks"].append(r9)
    r["all_ok"] = bool(r["all_ok"] and r9["ok"])
    return r


def self_test(df, stats, manifest):
    """Plant regressions; assert each is caught. A suite that cannot fail is not a suite."""
    import pandas as pd
    results = []

    def expect_caught(label, mutate, check_name):
        d = df.copy()
        d = mutate(d)
        cs = checks(d, stats, manifest)
        hit = [c for c in cs if c["name"] == check_name and not c["ok"]]
        results.append(dict(planted=label, target_check=check_name, caught=bool(hit)))

    expect_caught("drop 100 rows", lambda d: d.iloc[:-100], "findings_rows")
    def _collapse_one_paper(d):
        # Relabel every row of the rarest pmid to the most common one, so the paper
        # count genuinely drops by 1. Naively copying one row's pmid onto its neighbour
        # does NOT work: papers carry ~12 findings each, so adjacent rows usually share
        # a pmid already and nunique() never moves.
        vc = d.pmid.value_counts()
        victim, target = vc.index[-1], vc.index[0]
        return d.assign(pmid=d.pmid.replace({victim: target}))

    expect_caught("collapse one paper into another (paper count drops by 1)",
                  _collapse_one_paper, "papers_unique_pmid")
    expect_caught("truncate every quote to 50 chars",
                  lambda d: d.assign(provenance_quote=d.provenance_quote.fillna("").str[:50]),
                  "quote_median_chars")
    expect_caught("null out every resolved ontology id",
                  lambda d: d.assign(entity_ontology_id_resolved=None),
                  "rows_with_resolved_id")
    expect_caught("drop a column",
                  lambda d: d.drop(columns=[d.columns[-1]]), "n_columns")
    expect_caught("relabel tier_source",
                  lambda d: d.assign(tier_source="tX"), "tier_source_single_value")
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--root", default=str(ROOT))
    a = ap.parse_args()
    root = Path(a.root).resolve()

    manifest, stats, df = _load(root)
    cs = checks(df, stats, manifest)
    inv = invariants(df, stats["chunk_ids"], stats["chunk_paper_allotment"])
    st = self_test(df, stats, manifest) if a.self_test else None

    n_fail = sum(1 for c in cs if not c["ok"])
    inv_declared = stats["invariants"]["all_ok"]
    inv_matches_declared = bool(inv["all_ok"]) == bool(inv_declared)
    st_ok = all(x["caught"] for x in st) if st else True

    result = dict(
        package=manifest["package"],
        headline_checks=dict(total=len(cs), failed=n_fail,
                             failures=[c for c in cs if not c["ok"]]),
        invariants=dict(all_ok=inv["all_ok"], declared=inv_declared,
                        matches_declaration=inv_matches_declared,
                        per_check={c["name"]: c["ok"] for c in inv["checks"]}),
        self_test=(dict(planted=len(st), caught=sum(x["caught"] for x in st), detail=st)
                   if st else "not run"),
        verdict=("PASS" if (n_fail == 0 and inv_matches_declared and st_ok) else "FAIL"),
    )

    if a.json:
        print(json.dumps(result, indent=1, default=str))
    else:
        print(f"package        {result['package']}")
        print(f"headline       {len(cs) - n_fail}/{len(cs)} numbers re-derived and matching")
        for c in cs:
            if not c["ok"]:
                print(f"  MISMATCH {c['name']}\n    README/stats: {c['expected']}\n    data:         {c['measured']}")
        print(f"invariants     measured all_ok={inv['all_ok']} | declared={inv_declared} "
              f"| {'agrees' if inv_matches_declared else 'DISAGREES'}")
        for c in inv["checks"]:
            mark = "ok  " if c["ok"] else "FAIL"
            print(f"  {mark} {c['name']}")
        if st:
            print(f"self-test      {sum(x['caught'] for x in st)}/{len(st)} planted regressions caught")
            for x in st:
                if not x["caught"]:
                    print(f"  NOT CAUGHT: {x['planted']} (target {x['target_check']})")
        print(f"\nVERDICT        {result['verdict']}")
        if result["verdict"] == "FAIL":
            print("\nA FAIL means the shipped data disagrees with the shipped documentation.")
            print("Do not cite a number from the README until this resolves.")

    sys.exit(0 if result["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()
