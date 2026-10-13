#!/usr/bin/env python
"""Tier-2 (pre-2019) screen merge / pass-2 planning / finalize — deterministic, NO model calls.

Written 2026-10-13 when the Tier-2 roster/screen sub-agent hit the host.llm per-frame 2.0M token
ceiling after 2.5 of 17 pass-1 units (a leaf frame cannot delegate). Everything that does not need a
model call lives here so the lead only has to fan the units out and run two commands:

  python scripts/eds_tier2_screen_merge.py merge-pass1 --roster eds_tier2_roster_v1.parquet \
         --reports screen_eds_t2_unit*_report.csv --out screen_eds_t2_pass1
      -> <out>_merged.csv (one row per rostered paper, reconciled with screen.reconcile — pure),
         <out>_uncertain.parquet (the pass-2 input: final_verdict == uncertain, with title/abstract),
         <out>_pass2_units.json (chunk-aligned units of <= 200 papers), <out>_summary.json.
         Refuses to proceed if any rostered pmid is missing from the reports or any row still carries
         an llm_error (those must be retried in a fresh frame; see screen_eds_t2_unit3_retry_pmids.csv).

  python scripts/eds_tier2_screen_merge.py finalize --roster eds_tier2_roster_v1.parquet \
         --pass1 screen_eds_t2_pass1_merged.csv --pass2 popcheck_t2_unit*_report.csv
      -> screen_eds_t2_final.csv, eds_tier2_roster_v2.parquet, eds_tier2_excluded_pmids_v2.parquet/.csv,
         eds_tier2_roster_v2_report.json, EDS_TIER2_COST_PROJECTION.md, eds_tier2_cost_projection.json.
         Rule (population_check.combine, fixed): exclude ONLY where pass-1 LLM off_topic AND pass-2 says no
         listed population studied (both verbatim spans), or both pass-1 arms agreed; everything else is
         extracted and flagged. Chunk ids are never re-cut.
"""
import argparse, glob, json, math, os, sys, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "pipeline"))
import pandas as pd  # noqa: E402
import screen as cscreen  # noqa: E402
import population_check as pc  # noqa: E402

COST_DEFAULT = {"per_fulltext_paper": 18024, "per_abstract_paper": 5326,
                "basis": "cost model v2: least squares on the 4 Tier-1 pilot units (2026-10-07), residuals <= 6%"}


def _expand(patterns):
    out = []
    for p in patterns:
        out.extend(sorted(glob.glob(p)) or [p])
    return out


def units_for(df, cap):
    """Chunk-aligned units of <= cap papers (a chunk never straddles two units)."""
    sizes = df.groupby("chunk_id").size().sort_index()
    units, cur, n = [], [], 0
    for c, s in sizes.items():
        if cur and n + s > cap:
            units.append(cur); cur, n = [], 0
        cur.append(int(c)); n += int(s)
    if cur:
        units.append(cur)
    return units, [int(sizes.loc[u].sum()) for u in units]


def project(v2, cost_model, target_chunks):
    ft_c, ab_c = cost_model["per_fulltext_paper"], cost_model["per_abstract_paper"]
    pcnk = v2.groupby("chunk_id").agg(papers=("pmid", "size"), fulltext=("fulltext_expected", "sum"),
                                      year_min=("year", "min"), year_max=("year", "max"))
    if "screen_verdict_uncertain" in v2:
        pcnk["uncertain"] = v2.groupby("chunk_id").screen_verdict_uncertain.sum()
    else:
        pcnk["uncertain"] = 0
    pcnk["abstract_only"] = pcnk.papers - pcnk.fulltext
    pcnk["units"] = [max(1, min(2, math.ceil(p / 25)), math.ceil(f / 12)) for p, f in zip(pcnk.papers, pcnk.fulltext)]
    pcnk["tokens_nominal"] = pcnk.fulltext * ft_c + pcnk.abstract_only * ab_c
    pcnk["tokens_plus30"] = (pcnk.tokens_nominal * 1.3).round().astype(int)
    pcnk["decade"] = (pcnk.year_min // 10) * 10
    dec = pcnk.groupby("decade")[["papers", "fulltext", "abstract_only", "units", "tokens_nominal", "tokens_plus30"]].sum()
    c2010 = pcnk[pcnk.year_min >= 2010].sort_values(["fulltext", "papers"], ascending=False)
    cpre = pcnk[pcnk.year_max < 1990].sort_values(["papers", "uncertain"], ascending=False)
    pilots = []
    for sel, reason in ((c2010, "2010s chunk with the most full-text papers: exercises the full-text path, the max_fulltext_per_unit split and the worker ceiling; refit the per-FT token coefficient here"),
                        (cpre, "largest pre-1990 chunk (abstract-only, oldest abstract style, highest screen-uncertain share): tests yield on short historical abstracts and the per-abstract coefficient")):
        if len(sel):
            c = sel.index[0]; r = sel.iloc[0]
            pilots.append(dict(chunk_id=int(c), papers=int(r.papers), fulltext_expected=int(r.fulltext), units=int(r.units),
                               years=[int(r.year_min), int(r.year_max)], reason=reason))
    return dict(generated=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"), basis=cost_model,
                papers=int(len(v2)), fulltext_expected=int(v2.fulltext_expected.sum()), abstract_only=int((~v2.fulltext_expected.astype(bool)).sum()),
                units_total=int(pcnk.units.sum()), tokens_nominal=int(pcnk.tokens_nominal.sum()), tokens_plus30=int(pcnk.tokens_plus30.sum()),
                unit_rule="per chunk max(1, min(2, ceil(papers/25)), ceil(fulltext/12))",
                by_decade={int(d): {k: int(v) for k, v in r.items()} for d, r in dec.iterrows()},
                per_chunk={int(c): {k: int(v) for k, v in r.items()} for c, r in pcnk.iterrows()},
                recommended_pilot_chunks=pilots, chunks=sorted(int(c) for c in pcnk.index),
                empty_chunks=sorted(set(target_chunks) - set(int(c) for c in pcnk.index)) if target_chunks else None)


def projection_md(proj, title, provisional_note=None):
    cm = proj["basis"]
    md = [f"# {title}", ""]
    if provisional_note:
        md += [f"**PROVISIONAL.** {provisional_note}", ""]
    md += [f"Generated {proj['generated']}. Basis: {cm['per_fulltext_paper']:,} output tokens per full-text paper, {cm['per_abstract_paper']:,} per abstract-only paper ({cm.get('basis', '')}).",
           f"Units per chunk = {proj['unit_rule']}.", "",
           f"**{proj['papers']:,} papers** ({proj['fulltext_expected']:,} full text expected, {proj['abstract_only']:,} abstract-only), **{proj['units_total']} units**, "
           f"**{proj['tokens_nominal']:,} output tokens nominal**, **{proj['tokens_plus30']:,} at +30%**.", "",
           "| decade | papers | full text | abstract-only | units | tokens nominal | tokens +30% |", "|---|---|---|---|---|---|---|"]
    md += [f"| {d}s | {r['papers']:,} | {r['fulltext']:,} | {r['abstract_only']:,} | {r['units']} | {r['tokens_nominal']:,} | {r['tokens_plus30']:,} |" for d, r in proj["by_decade"].items()]
    md += ["", "## Recommended pilot chunks", ""] + [f"- chunk {p['chunk_id']} ({p['years'][0]}-{p['years'][1]}; {p['papers']} papers, {p['fulltext_expected']} FT, {p['units']} units): {p['reason']}" for p in proj["recommended_pilot_chunks"]]
    if proj.get("empty_chunks"):
        md += ["", f"Chunks with no rostered paper (ids frozen; the driver must dispatch nothing for them): {proj['empty_chunks']}"]
    return "\n".join(md) + "\n"


def merge_pass1(a):
    ro = pd.read_parquet(a.roster); ro["pmid"] = ro.pmid.astype(str)
    reps = [pd.read_csv(p, dtype={"pmid": str}) for p in _expand(a.reports)]
    rep = pd.concat(reps, ignore_index=True)
    dup = rep[rep.pmid.duplicated(keep=False)]
    if len(dup):
        # a retry unit legitimately re-screens pmids from a partial unit: keep the row WITH an llm verdict
        rep = rep.sort_values("llm_verdict", na_position="last").drop_duplicates("pmid", keep="first")
    missing = sorted(set(ro.pmid) - set(rep.pmid)); extra = sorted(set(rep.pmid) - set(ro.pmid))
    errs = rep[rep.llm_error.notna()] if "llm_error" in rep else rep.iloc[0:0]
    print(json.dumps({"reports": len(reps), "rows": int(len(rep)), "duplicates_resolved": int(dup.pmid.nunique()),
                      "missing_from_reports": len(missing), "not_in_roster": len(extra), "rows_with_llm_error": int(len(errs))}))
    if (missing or len(errs)) and not a.allow_partial:
        pd.Series(missing, name="pmid").to_csv(f"{a.out}_missing_pmids.csv", index=False)
        errs.to_csv(f"{a.out}_error_rows.csv", index=False)
        raise SystemExit("REFUSED: every rostered paper needs a pass-1 LLM verdict before merging "
                         f"({len(missing)} missing, {len(errs)} errored) — retry them in a fresh frame")
    if extra:
        raise SystemExit(f"REFUSED: {len(extra)} screened pmids are not in the roster")
    for c in ("llm_verdict", "llm_span", "llm_offtopic_class", "rule_contamination", "llm_error"):
        if c in rep:
            rep[c] = rep[c].astype(object).where(rep[c].notna(), None)   # CSV NaN -> None (helpers expect str|None)
    rec = cscreen.reconcile(rep, ro)                      # pure: recomputes spans/final verdicts/classes
    rec = rec.merge(ro[["pmid", "title", "abstract", "year", "fulltext_expected", "abstract_missing", "dec"]], on="pmid", how="left")
    rec.to_csv(f"{a.out}_merged.csv", index=False)
    unc = rec[rec.final_verdict == "uncertain"].copy()
    unc.to_parquet(f"{a.out}_uncertain.parquet", index=False)
    units, sizes = units_for(unc, a.pass2_cap)
    json.dump({"units": units, "sizes": sizes, "n_uncertain": int(len(unc)), "cap": a.pass2_cap}, open(f"{a.out}_pass2_units.json", "w"), indent=1)
    rule_off = int((rec.rule_verdict == "off_topic").sum()); llm_off = int((rec.llm_verdict == "off_topic").sum())
    from helpers import screen_instrument_alarm
    summ = dict(n=int(len(rec)), partial=bool(missing or len(errs)), n_missing=len(missing), n_llm_error=int(len(errs)),
                final=rec.final_verdict.value_counts().to_dict(), rule=rec.rule_verdict.value_counts().to_dict(),
                llm=rec.llm_verdict.value_counts(dropna=False).astype(str).to_dict(),
                llm_span_verbatim=dict(n=int(rec.llm_span_verbatim.notna().sum()), ok=int(rec.llm_span_verbatim.fillna(False).astype(bool).sum())),
                exclusion_classes=rec.exclusion_class.value_counts().to_dict(),
                n_llm_offtopic_flagged_not_excluded=int(((rec.llm_verdict == "off_topic") & (rec.final_verdict != "off_topic")).sum()),
                instrument_alarm=screen_instrument_alarm(rule_off, llm_off, len(rec)),
                per_theme={t: dict(n=int(len(g)), off_topic=int((g.final_verdict == "off_topic").sum()), uncertain=int((g.final_verdict == "uncertain").sum()))
                           for t, g in rec.groupby("theme")},
                per_decade={int(d): dict(n=int(len(g)), off_topic=int((g.final_verdict == "off_topic").sum()), uncertain=int((g.final_verdict == "uncertain").sum()))
                            for d, g in rec.groupby("dec")},
                pass2_units=dict(n_units=len(units), sizes=sizes),
                basis="denominator = every rostered paper in the merged reports; final_verdict per helpers.screen_reconcile recomputed by screen.reconcile from the saved reports")
    json.dump(summ, open(f"{a.out}_summary.json", "w"), indent=1, default=str)
    print(json.dumps({k: summ[k] for k in ("n", "partial", "final", "pass2_units")}))


def finalize(a):
    ro = pd.read_parquet(a.roster); ro["pmid"] = ro.pmid.astype(str)
    s = pd.read_csv(a.pass1, dtype={"pmid": str})
    p2 = pd.concat([pd.read_csv(p, dtype={"pmid": str}) for p in _expand(a.pass2)], ignore_index=True).drop_duplicates("pmid", keep="last")
    need = set(s[s.final_verdict == "uncertain"].pmid); got = set(p2.pmid)
    if need - got:
        pd.Series(sorted(need - got), name="pmid").to_csv("screen_eds_t2_pass2_missing_pmids.csv", index=False)
        raise SystemExit(f"REFUSED: {len(need - got)} pass-1 uncertain papers have no pass-2 row")
    s1 = s.drop(columns=[c for c in ("title", "abstract") if c in s])
    fin = pc.combine(s1, p2)
    fin = fin.merge(ro[["pmid", "title"]], on="pmid", how="left")
    fin["screen_verdict_uncertain"] = fin.screen_final_verdict_v2 == "uncertain"
    fin.to_csv("screen_eds_t2_final.csv", index=False)
    exc = fin[fin.screen_final_verdict_v2 == "off_topic"]
    cols = {"rule_verdict": "screen_rule_verdict", "llm_verdict": "screen_llm_verdict", "llm_span": "screen_llm_span",
            "llm_reason": "screen_llm_reason", "final_verdict": "screen_pass1_verdict", "exclusion_class": "screen_pass1_exclusion_class"}
    keep = list(cols) + ["pass2_studies_population", "pass2_labels", "pass2_span", "pass2_reason", "pass2_error",
                         "screen_final_verdict_v2", "exclusion_class_v2", "exclusion_basis_v2"]
    v2 = ro[~ro.pmid.isin(exc.pmid)].merge(fin[["pmid"] + keep].rename(columns=cols), on="pmid", how="left")
    v2["screen_final_verdict"] = v2.screen_final_verdict_v2
    v2["screen_verdict_uncertain"] = v2.screen_final_verdict_v2 == "uncertain"
    assert len(v2) + len(exc) == len(ro)
    v2.to_parquet("eds_tier2_roster_v2.parquet", index=False)
    ex = ro[ro.pmid.isin(exc.pmid)].merge(exc[["pmid", "exclusion_class_v2", "exclusion_basis_v2", "llm_span", "llm_reason", "pass2_span", "pass2_reason"]], on="pmid")
    ex.to_parquet("eds_tier2_excluded_pmids_v2.parquet", index=False)
    ex[["pmid", "year", "title", "theme", "chunk_id", "exclusion_class_v2", "exclusion_basis_v2", "llm_span", "pass2_span"]].to_csv("eds_tier2_excluded_pmids_v2.csv", index=False)
    cm = json.load(open(a.cost_model)) if a.cost_model and os.path.exists(a.cost_model) else COST_DEFAULT
    cm = {"per_fulltext_paper": cm.get("per_fulltext_paper", COST_DEFAULT["per_fulltext_paper"]),
          "per_abstract_paper": cm.get("per_abstract_paper", COST_DEFAULT["per_abstract_paper"]), "basis": cm.get("basis", COST_DEFAULT["basis"])}
    tc = json.load(open(a.config))["tier"]["target_chunks"] if a.config and os.path.exists(a.config) else None
    proj = project(v2, cm, tc); proj["roster"] = "eds_tier2_roster_v2.parquet"
    json.dump(proj, open("eds_tier2_cost_projection.json", "w"), indent=1, default=str)
    open("EDS_TIER2_COST_PROJECTION.md", "w").write(projection_md(proj, "EDS Tier 2 — cost projection (roster v2, post-screen)"))
    rep = dict(rostered_v1=int(len(ro)), excluded_v2=int(len(exc)), rostered_v2=int(len(v2)), fulltext_expected_v2=int(v2.fulltext_expected.sum()),
               uncertain_v2=int(v2.screen_verdict_uncertain.sum()), final_v2=fin.screen_final_verdict_v2.value_counts().to_dict(),
               exclusion_basis_v2=fin.exclusion_basis_v2.value_counts().to_dict(), exclusion_class_v2=fin.exclusion_class_v2.value_counts().to_dict(),
               pass2=dict(n=int(len(p2)), no_population=int((p2.pass2_studies_population == False).sum()), population=int((p2.pass2_studies_population == True).sum()),
                          errors=int(p2.pass2_error.notna().sum()), span_verbatim_ok=int(p2.pass2_span_verbatim.fillna(False).astype(bool).sum())),
               chunks=proj["chunks"], per_chunk={int(c): dict(papers=int(r["papers"]), fulltext=int(r["fulltext"])) for c, r in proj["per_chunk"].items()},
               basis="roster v2 = roster v1 minus screen_final_verdict_v2 == off_topic; chunk ids unchanged; uncertain papers kept and flagged")
    json.dump(rep, open("eds_tier2_roster_v2_report.json", "w"), indent=1, default=str)
    print(json.dumps({k: rep[k] for k in ("rostered_v1", "excluded_v2", "rostered_v2", "fulltext_expected_v2", "uncertain_v2", "final_v2")}))
    print(json.dumps({k: proj[k] for k in ("units_total", "tokens_nominal", "tokens_plus30", "recommended_pilot_chunks")}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); sp = ap.add_subparsers(dest="cmd", required=True)
    m = sp.add_parser("merge-pass1"); m.add_argument("--roster", required=True); m.add_argument("--reports", nargs="+", required=True)
    m.add_argument("--out", default="screen_eds_t2_pass1"); m.add_argument("--pass2-cap", type=int, default=200)
    m.add_argument("--allow-partial", action="store_true", help="interim merge while units are outstanding (summary.partial=true)"); m.set_defaults(fn=merge_pass1)
    f = sp.add_parser("finalize"); f.add_argument("--roster", required=True); f.add_argument("--pass1", required=True); f.add_argument("--pass2", nargs="+", required=True)
    f.add_argument("--cost-model", default=os.path.join(HERE, "..", "config", "eds_cost_model_v2.json")); f.add_argument("--config", default=os.path.join(HERE, "..", "config", "eds_tier2.json")); f.set_defaults(fn=finalize)
    a = ap.parse_args(); a.fn(a)
