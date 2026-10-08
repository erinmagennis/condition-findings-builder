"""Cross-corpus paper registry — build, check, count.  Specification: ../REGISTRY.md.

One row per (pmid, corpus, tier, pipeline_generation, prompt_ref, chunk_id, extraction_status).
A paper already EXTRACTED in a sibling corpus is REFERENCED by this corpus (`reference_only`),
never re-extracted.  Every provenance value on a sibling row is READ from the sibling's shipped
package (MANIFEST.json, code/ listing, data/*.parquet) — none is typed.

    reg = build_registry(package_dirs, chunk_order, universe, this_corpus=...)
    results = check_registry(reg, shipped_pmids=..., exceptions=None)   # list of {name, ok, ...}
    counts  = registry_counts(reg, universe)                             # every count with its basis

Nothing here writes to a sibling package.  Shipped data is opened read-only.
"""
import hashlib
import json
import os
import re

import pandas as pd

REGISTRY_SCHEMA_VERSION = "registry-v1 (2026-10-08)"

# v1 adds two terminal non-extraction statuses so a screened-out or text-less paper is never left
# as "planned": excluded_screen (two-pass screen, basis in scope/), not_extractable (no abstract in
# Europe PMC or PubMed and no full text expected).
STATUS = ("extracted", "dispatched_no_yield", "referenced", "planned", "excluded_screen", "not_extractable")
UNIVERSE_STATUS = ("shared", "new")
KEY = ["pmid", "corpus", "tier", "pipeline_generation", "prompt_ref", "chunk_id", "extraction_status"]
COLUMNS = KEY + [
    "prompt_vid", "prompt_sha256", "prompt_file", "pipeline_file",
    "corpus_version", "package", "doi", "year", "theme",
    "n_findings", "shared_with", "reference_only",
    "universe_status", "sibling_dispatched_no_yield",
    "source_version_id", "basis",
]

# code/ listing -> generation label.  The label is DERIVED from the prompt file name shipped in
# code/ (schema_v5_extraction_prompt.txt -> schema_v5; tier4_prompt.txt -> tier4).  Never typed.
_PROMPT_RX = re.compile(r"^(.*?)(?:_extraction)?_prompt(?:_template)?\.txt$")


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def generation_from_prompt_file(prompt_file):
    m = _PROMPT_RX.match(os.path.basename(prompt_file))
    if not m or not m.group(1):
        raise ValueError(f"cannot derive a generation label from prompt file name {prompt_file!r}")
    return m.group(1)


def read_package_provenance(pkg_dir):
    """Read tier, generation, prompt + pipeline file names and prompt sha256 from a shipped
    package.  Raises when the code/ listing is ambiguous or the MANIFEST hash disagrees with the
    file on disk (flag, never repair)."""
    man = json.load(open(os.path.join(pkg_dir, "MANIFEST.json")))
    code = sorted(os.listdir(os.path.join(pkg_dir, "code")))
    prompts = [f for f in code if f.endswith(".txt") and "prompt" in f]
    pipelines = [f for f in code if f.endswith(".py") and "pipeline" in f]
    if len(prompts) != 1:
        raise ValueError(f"{pkg_dir}: expected exactly one prompt file in code/, found {prompts}")
    if len(pipelines) != 1:
        raise ValueError(f"{pkg_dir}: expected exactly one pipeline file in code/, found {pipelines}")
    prompt_file, pipeline_file = prompts[0], pipelines[0]
    listed = {f["path"]: f["sha256"] for f in man.get("files", [])}
    manifest_sha = listed.get(f"code/{prompt_file}")
    disk_sha = sha256_file(os.path.join(pkg_dir, "code", prompt_file))
    if manifest_sha is not None and manifest_sha != disk_sha:
        raise ValueError(f"{pkg_dir}: MANIFEST sha256 for code/{prompt_file} != file on disk")
    return {
        "package": man["package"],
        "package_dir": os.path.basename(os.path.normpath(pkg_dir)),
        "tier": man["tier"],
        "primary_table": man["primary_table"],
        "chunk_ids": [int(c) for c in man["contents"]["chunk_ids"]],
        "manifest_findings": int(man["contents"]["findings"]),
        "manifest_papers": int(man["contents"]["papers"]),
        "prompt_file": prompt_file,
        "pipeline_file": pipeline_file,
        "prompt_sha256": disk_sha,
        "prompt_sha256_in_manifest": manifest_sha is not None,
        "pipeline_generation": generation_from_prompt_file(prompt_file),
        "source_version_id": man.get("source_corpus_version_id"),
    }


def _prompt_ref(prompt_vid, prompt_sha256):
    if prompt_vid:
        return str(prompt_vid)
    if prompt_sha256:
        return "sha256:" + prompt_sha256
    return "unpinned"


def _first_nonnull(s):
    s = s.dropna()
    return s.iloc[0] if len(s) else None


def build_registry(package_dirs, chunk_order, universe, this_corpus, sibling_corpus,
                   prompt_vids=None, universe_source_version_id=None):
    """Build the registry.

    package_dirs   : list of sibling package directories (each with MANIFEST.json, code/, data/)
    chunk_order    : DataFrame(pmid, doi, tier, chunk_id) of every paper the sibling DISPATCHED
                     (yielding or not)
    universe       : DataFrame(pmid, doi, year, theme) of this corpus's draw
    this_corpus    : dict(corpus, corpus_version, tier, pipeline_generation, prompt_vid,
                          prompt_sha256)
    sibling_corpus : dict(corpus, corpus_version)
    prompt_vids    : optional {prompt_sha256: artifact version id} for sibling prompts whose
                     artifact id IS recorded somewhere authoritative (e.g. the sibling's config)
    Returns (registry_df, build_log dict).
    """
    prompt_vids = prompt_vids or {}
    log = {"packages": [], "chunk_order_rows_without_pmid": 0, "chunk_order_chunks_not_in_any_package": []}
    rows = []
    sib = sibling_corpus["corpus"]
    extracted_by_pkg = {}
    chunk_to_pkg = {}
    provs = {}
    for d in package_dirs:
        p = read_package_provenance(d)
        provs[p["package_dir"]] = p
        df = pd.read_parquet(os.path.join(d, "data", p["primary_table"]), columns=["pmid", "chunk_id", "year", "doi"])
        df["pmid"] = df["pmid"].astype(str)
        g = df.groupby("pmid").agg(n_findings=("pmid", "size"), chunk_id=("chunk_id", _first_nonnull),
                                   n_chunks=("chunk_id", "nunique"), year=("year", _first_nonnull), doi=("doi", _first_nonnull))
        if (g["n_chunks"] > 1).any():
            raise ValueError(f"{d}: a pmid spans more than one chunk in shipped data")
        extracted_by_pkg[p["package_dir"]] = set(g.index)
        for c in p["chunk_ids"]:
            chunk_to_pkg[c] = p["package_dir"]
        pref = _prompt_ref(prompt_vids.get(p["prompt_sha256"]), p["prompt_sha256"])
        for pmid, r in g.iterrows():
            rows.append(dict(
                pmid=pmid, corpus=sib, tier=p["tier"], pipeline_generation=p["pipeline_generation"],
                prompt_ref=pref, chunk_id=int(r.chunk_id) if pd.notna(r.chunk_id) else None,
                extraction_status="extracted", prompt_vid=prompt_vids.get(p["prompt_sha256"]),
                prompt_sha256=p["prompt_sha256"], prompt_file=p["prompt_file"], pipeline_file=p["pipeline_file"],
                corpus_version=sibling_corpus["corpus_version"], package=p["package_dir"],
                doi=r.doi, year=int(r.year) if pd.notna(r.year) else None, theme=None,
                n_findings=int(r.n_findings), shared_with=[], reference_only=False,
                universe_status=None, sibling_dispatched_no_yield=False,
                source_version_id=p["source_version_id"],
                basis=f"{p['package_dir']}/data/{p['primary_table']} rows grouped by pmid; generation from code/{p['prompt_file']}",
            ))
        log["packages"].append({k: p[k] for k in ("package_dir", "tier", "pipeline_generation", "prompt_file",
                                                  "pipeline_file", "prompt_sha256", "manifest_papers", "manifest_findings")}
                               | {"papers_in_parquet": int(len(g)), "findings_in_parquet": int(g.n_findings.sum())})
    sib_extracted = set().union(*extracted_by_pkg.values()) if extracted_by_pkg else set()

    # dispatched but no shipped row
    co = chunk_order.copy()
    log["chunk_order_rows_without_pmid"] = int(co["pmid"].isna().sum())
    co = co[co["pmid"].notna()].copy()
    co["pmid"] = co["pmid"].astype(str)
    noy = co[~co["pmid"].isin(sib_extracted)]
    for r in noy.itertuples(index=False):
        pkg = chunk_to_pkg.get(int(r.chunk_id))
        if pkg is None:
            log["chunk_order_chunks_not_in_any_package"].append(int(r.chunk_id))
            prov = None
        else:
            prov = provs[pkg]
        rows.append(dict(
            pmid=r.pmid, corpus=sib, tier=prov["tier"] if prov else f"tier{int(r.tier)}",
            pipeline_generation=prov["pipeline_generation"] if prov else None,
            prompt_ref=_prompt_ref(prompt_vids.get(prov["prompt_sha256"]), prov["prompt_sha256"]) if prov else "unpinned",
            chunk_id=int(r.chunk_id), extraction_status="dispatched_no_yield",
            prompt_vid=prompt_vids.get(prov["prompt_sha256"]) if prov else None,
            prompt_sha256=prov["prompt_sha256"] if prov else None,
            prompt_file=prov["prompt_file"] if prov else None, pipeline_file=prov["pipeline_file"] if prov else None,
            corpus_version=sibling_corpus["corpus_version"], package=pkg, doi=r.doi, year=None, theme=None,
            n_findings=0, shared_with=[], reference_only=False, universe_status=None,
            sibling_dispatched_no_yield=False, source_version_id=prov["source_version_id"] if prov else None,
            basis="chunk_order.parquet pmid absent from every shipped findings parquet; package by MANIFEST chunk_ids",
        ))
    log["chunk_order_chunks_not_in_any_package"] = sorted(set(log["chunk_order_chunks_not_in_any_package"]))
    sib_dispatched = set(co["pmid"])

    # this corpus's universe: shared -> referenced, new -> planned
    u = universe.copy()
    u["pmid"] = u["pmid"].astype(str)
    if u["pmid"].duplicated().any():
        raise ValueError("universe has duplicate pmids")
    tc = this_corpus
    pref = _prompt_ref(tc.get("prompt_vid"), tc.get("prompt_sha256"))
    for r in u.itertuples(index=False):
        shared = r.pmid in sib_extracted
        rows.append(dict(
            pmid=r.pmid, corpus=tc["corpus"], tier=tc["tier"], pipeline_generation=tc["pipeline_generation"],
            prompt_ref=pref, chunk_id=int(r.chunk_id) if hasattr(r, "chunk_id") and pd.notna(r.chunk_id) else None,
            extraction_status="referenced" if shared else "planned",
            prompt_vid=tc.get("prompt_vid") or None, prompt_sha256=tc.get("prompt_sha256"),
            prompt_file=tc.get("prompt_file"), pipeline_file=tc.get("pipeline_file"),
            corpus_version=tc["corpus_version"], package=None, doi=getattr(r, "doi", None),
            year=int(r.year) if pd.notna(r.year) else None, theme=getattr(r, "theme", None),
            n_findings=0, shared_with=[sib] if shared else [], reference_only=bool(shared),
            universe_status="shared" if shared else "new",
            sibling_dispatched_no_yield=bool((not shared) and (r.pmid in sib_dispatched)),
            source_version_id=universe_source_version_id,
            basis=f"universe pmid ∩ {sib} shipped pmids (all packages) -> shared/referenced; else new/planned",
        ))
    reg = pd.DataFrame(rows, columns=COLUMNS)
    reg["chunk_id"] = reg["chunk_id"].astype("Int64")
    reg["year"] = reg["year"].astype("Int64")
    reg["n_findings"] = reg["n_findings"].astype("int64")
    reg.attrs["registry_schema_version"] = REGISTRY_SCHEMA_VERSION
    return reg, log


# ----------------------------------------------------------------------------------------------
# invariants — each returns {name, ok, n_violations, examples, basis}
# ----------------------------------------------------------------------------------------------

def _joinkeys(df, cols):
    """'a|b|c' per row; robust to an empty frame (DataFrame.agg(axis=1) returns a frame when empty)."""
    return sorted({"|".join(str(v) for v in row) for row in df[cols].itertuples(index=False)})


def _res(name, viol, basis, **extra):
    viol = list(viol)
    return {"name": name, "ok": len(viol) == 0, "n_violations": len(viol), "examples": viol[:5], "basis": basis, **extra}


def inv_key_unique(reg):
    d = reg[reg.duplicated(KEY, keep=False)]
    return _res("key_unique", _joinkeys(d, KEY),
                f"no two rows share {KEY}")


def inv_status_enum(reg):
    bad = reg[~reg.extraction_status.isin(STATUS)]
    badu = reg[reg.universe_status.notna() & ~reg.universe_status.isin(UNIVERSE_STATUS)]
    return _res("status_enum", bad.extraction_status.astype(str).unique().tolist() + badu.universe_status.astype(str).unique().tolist(),
                f"extraction_status in {STATUS}; universe_status null or in {UNIVERSE_STATUS}")


def inv_one_extraction_per_pmid_per_generation(reg, exceptions=None):
    ex = reg[reg.extraction_status == "extracted"]
    g = ex.groupby(["pmid", "pipeline_generation"], dropna=False)["corpus"].nunique()
    viol = g[g > 1].reset_index()
    if exceptions is not None and len(exceptions):
        exk = set(zip(exceptions.pmid.astype(str), exceptions.pipeline_generation.astype(str)))
        viol = viol[~viol.apply(lambda r: (str(r.pmid), str(r.pipeline_generation)) in exk, axis=1)]
    return _res("one_extraction_per_pmid_per_generation", _joinkeys(viol, list(viol.columns)),
                "rows with extraction_status=extracted, grouped by (pmid, pipeline_generation): distinct corpus <= 1, "
                "minus rows listed in the documented exceptions table")


def inv_no_reextraction_of_sibling_paper(reg, exceptions=None):
    """Policy rule (stronger than the generation invariant): a pmid extracted in ANY sibling corpus
    is never also extracted in this corpus, whatever the generation, unless documented."""
    ex = reg[reg.extraction_status == "extracted"]
    g = ex.groupby("pmid")["corpus"].nunique()
    viol = g[g > 1].index
    if exceptions is not None and len(exceptions):
        viol = [p for p in viol if str(p) not in set(exceptions.pmid.astype(str))]
    return _res("no_reextraction_of_sibling_paper", [str(p) for p in viol],
                "rows with extraction_status=extracted grouped by pmid: distinct corpus <= 1, minus documented exceptions")


def inv_referenced_exists_in_sibling(reg, shipped_pmids=None):
    """Every referenced row names >=1 sibling in shared_with, and each named sibling has an
    `extracted` registry row for the pmid AND (when shipped_pmids is given) the pmid is present in
    that sibling's shipped findings data."""
    ref = reg[reg.extraction_status == "referenced"]
    ext = reg[reg.extraction_status == "extracted"]
    have = set(zip(ext.pmid.astype(str), ext.corpus))
    viol = []
    for r in ref.itertuples(index=False):
        sw = list(r.shared_with) if r.shared_with is not None else []
        if not sw:
            viol.append(f"{r.pmid}|{r.corpus}|empty shared_with"); continue
        for s in sw:
            if (str(r.pmid), s) not in have:
                viol.append(f"{r.pmid}|{r.corpus}|no extracted registry row in {s}")
            if shipped_pmids is not None and str(r.pmid) not in shipped_pmids.get(s, set()):
                viol.append(f"{r.pmid}|{r.corpus}|absent from {s} shipped data")
    return _res("referenced_exists_in_sibling", viol,
                "referenced rows: each shared_with sibling has an extracted row for the pmid"
                + ("; pmid present in sibling shipped findings parquet" if shipped_pmids is not None else ""))


def inv_shared_with_consistent(reg):
    """shared_with on every row equals the set of OTHER corpora holding an extracted row for the pmid."""
    ext = reg[reg.extraction_status == "extracted"]
    holders = ext.groupby(ext.pmid.astype(str))["corpus"].apply(set).to_dict()
    viol = []
    for r in reg.itertuples(index=False):
        want = holders.get(str(r.pmid), set()) - {r.corpus}
        got = set(r.shared_with) if r.shared_with is not None else set()
        if want != got:
            viol.append(f"{r.pmid}|{r.corpus}|want={sorted(want)} got={sorted(got)}")
    return _res("shared_with_consistent", viol,
                "shared_with == {corpora with an extracted row for the pmid} minus own corpus, on every row")


def inv_reference_only_consistent(reg):
    bad = reg[reg.reference_only.astype(bool) != (reg.extraction_status == "referenced")]
    return _res("reference_only_consistent", (bad.pmid.astype(str) + "|" + bad.corpus).tolist(),
                "reference_only is true exactly on rows with extraction_status=referenced")


def inv_universe_status_consistent(reg):
    # v1 (2026-10-08): a shared paper is REFERENCED when the sibling holds its findings, and may be
    # extracted / no-yield / excluded / not-extractable here when the sibling dispatched it without
    # yield (sibling_dispatched_no_yield=True). A new paper is never "referenced".
    u = reg[reg.universe_status.notna()]
    sib_ny = u.sibling_dispatched_no_yield.fillna(False).astype(bool)
    bad = u[(u.universe_status == "shared") & (u.extraction_status != "referenced") & ~sib_ny]
    bad1 = u[(u.universe_status == "shared") & (u.extraction_status == "referenced") & sib_ny]
    bad2 = u[(u.universe_status == "new") & (u.extraction_status == "referenced")]
    return _res("universe_status_consistent", (pd.concat([bad, bad1, bad2]).pmid.astype(str)).tolist(),
                "shared & not sibling_dispatched_no_yield <=> referenced; shared & sibling_dispatched_no_yield => not referenced; new => never referenced")


def inv_n_findings_by_status(reg):
    bad = reg[((reg.extraction_status == "extracted") & (reg.n_findings < 1))
              | ((reg.extraction_status != "extracted") & (reg.n_findings != 0))]
    return _res("n_findings_by_status", (bad.pmid.astype(str) + "|" + bad.corpus + "|" + bad.extraction_status).tolist(),
                "extracted => n_findings >= 1; every other status => n_findings == 0")


def inv_partition_within_corpus(reg):
    """Within one corpus a pmid is extracted in at most one package (the sibling's pairwise-zero
    paper overlap, re-checked here rather than trusted from MANIFEST.partition)."""
    ex = reg[reg.extraction_status == "extracted"]
    g = ex.groupby(["corpus", "pmid"])["package"].nunique()
    return _res("partition_within_corpus", [f"{c}|{p}" for (c, p) in g[g > 1].index],
                "extracted rows grouped by (corpus, pmid): distinct package <= 1")


def inv_generation_provenance(reg):
    """Every extracted/referenced row carries a generation and a prompt_ref that is not 'unpinned';
    one package carries exactly one (generation, prompt_sha256)."""
    need = reg[reg.extraction_status.isin(["extracted", "referenced"])]
    v1 = need[need.pipeline_generation.isna() | (need.prompt_ref == "unpinned")]
    viol = (v1.pmid.astype(str) + "|" + v1.corpus + "|missing generation or prompt").tolist()
    ex = reg[(reg.extraction_status == "extracted") & reg.package.notna()]
    g = ex.groupby("package").agg(ng=("pipeline_generation", "nunique"), ns=("prompt_sha256", "nunique"))
    viol += [f"{p}|{int(r.ng)} generations|{int(r.ns)} prompts" for p, r in g.iterrows() if r.ng != 1 or r.ns != 1]
    return _res("generation_provenance", viol,
                "extracted/referenced rows: pipeline_generation non-null and prompt_ref != unpinned; one (generation, prompt_sha256) per package")


def inv_counts_sum(reg, expected=None):
    """Per corpus: sum over statuses of distinct pmids == distinct pmids in the corpus (a pmid has
    one status per corpus); and, when `expected` gives {corpus: {status: n}} or
    {package: papers}, the registry agrees with it (MANIFEST contents.papers for the sibling)."""
    viol = []
    for c, g in reg.groupby("corpus"):
        per = g.groupby("extraction_status")["pmid"].nunique().to_dict()
        if sum(per.values()) != g.pmid.nunique():
            viol.append(f"{c}|statuses sum {sum(per.values())} != distinct pmids {g.pmid.nunique()}")
    if expected:
        for k, v in expected.items():
            if isinstance(v, dict):
                per = reg[reg.corpus == k].groupby("extraction_status")["pmid"].nunique().to_dict()
                for s, n in v.items():
                    if per.get(s, 0) != n:
                        viol.append(f"{k}|{s}: registry {per.get(s, 0)} != expected {n}")
            else:
                n = reg[(reg.package == k) & (reg.extraction_status == "extracted")].pmid.nunique()
                if n != v:
                    viol.append(f"{k}|extracted papers: registry {n} != expected {v}")
    return _res("counts_sum", viol, "per corpus: Σ_status distinct pmids == distinct pmids; registry vs expected (MANIFEST papers)")


def check_registry(reg, shipped_pmids=None, exceptions=None, expected=None):
    """Run every invariant. Returns a list of result dicts; nothing is repaired."""
    return [
        inv_key_unique(reg),
        inv_status_enum(reg),
        inv_one_extraction_per_pmid_per_generation(reg, exceptions),
        inv_no_reextraction_of_sibling_paper(reg, exceptions),
        inv_referenced_exists_in_sibling(reg, shipped_pmids),
        inv_shared_with_consistent(reg),
        inv_reference_only_consistent(reg),
        inv_universe_status_consistent(reg),
        inv_n_findings_by_status(reg),
        inv_partition_within_corpus(reg),
        inv_generation_provenance(reg),
        inv_counts_sum(reg, expected),
    ]


# ----------------------------------------------------------------------------------------------
# counts — every number with numerator, denominator, basis
# ----------------------------------------------------------------------------------------------

def _rate(num, den, basis):
    return {"n": int(num), "denominator": int(den), "pct": (round(100.0 * num / den, 2) if den else None), "basis": basis}


def registry_counts(reg, this_corpus_name, sibling_name, year_windows=((2019, 2026), (2010, 2026), (None, None))):
    sib = reg[reg.corpus == sibling_name]
    me = reg[reg.corpus == this_corpus_name]
    out = {"registry_rows": int(len(reg)), "registry_schema_version": REGISTRY_SCHEMA_VERSION}
    out["sibling"] = {
        "corpus": sibling_name,
        "papers_extracted_unique": int(sib[sib.extraction_status == "extracted"].pmid.nunique()),
        "papers_dispatched_unique": int(sib.pmid.nunique()),
        "papers_dispatched_no_yield": int(sib[sib.extraction_status == "dispatched_no_yield"].pmid.nunique()),
        "findings_rows": int(sib.n_findings.sum()),
        "by_package": {p: {"papers_extracted": int(g[g.extraction_status == "extracted"].pmid.nunique()),
                           "papers_dispatched_no_yield": int(g[g.extraction_status == "dispatched_no_yield"].pmid.nunique()),
                           "findings_rows": int(g.n_findings.sum()),
                           "pipeline_generation": _first_nonnull(g.pipeline_generation), "prompt_file": _first_nonnull(g.prompt_file)}
                       for p, g in sib[sib.package.notna()].groupby("package")},
        "basis": "sibling shipped findings parquets (extracted) and chunk_order.parquet (dispatched)",
    }
    U = int(me.pmid.nunique())
    shared = me[me.universe_status == "shared"]
    new = me[me.universe_status == "new"]
    out["this_corpus"] = {
        "corpus": this_corpus_name,
        "universe": U,
        "shared": _rate(shared.pmid.nunique(), U, "universe pmids with >=1 extracted row in sibling / universe"),
        "new": _rate(new.pmid.nunique(), U, "universe pmids with no extracted sibling row / universe"),
        "new_but_sibling_dispatched_no_yield": _rate(me[me.sibling_dispatched_no_yield.astype(bool)].pmid.nunique(), new.pmid.nunique() or 1,
                                                     "new pmids that the sibling dispatched but got no finding rows from / new"),
        "extracted_here": _rate(me[me.extraction_status == "extracted"].pmid.nunique(), U, "extracted in this corpus / universe"),
        "referenced_from_sibling": _rate(me[me.extraction_status == "referenced"].pmid.nunique(), U, "referenced / universe"),
        "planned": _rate(me[me.extraction_status == "planned"].pmid.nunique(), U, "planned (not yet dispatched) / universe"),
        "shared_as_fraction_of_sibling": _rate(shared.pmid.nunique(), out["sibling"]["papers_extracted_unique"],
                                               "shared / sibling papers extracted (unique)"),
    }
    by_year = {}
    for lo, hi in year_windows:
        m = me
        if lo is not None:
            m = m[(m.year >= lo) & (m.year <= hi)]
        label = f"{lo}-{hi}" if lo is not None else "all"
        n = m.pmid.nunique()
        by_year[label] = {"universe": int(n), "year_null": int(m.year.isna().sum()),
                          "shared": _rate(m[m.universe_status == "shared"].pmid.nunique(), n, f"shared in window / universe in window ({label})"),
                          "new": _rate(m[m.universe_status == "new"].pmid.nunique(), n, f"new in window / universe in window ({label})")}
    out["this_corpus"]["by_year_window"] = by_year
    by_theme = {}
    for t, g in me.groupby(me.theme.fillna("(null)")):
        n = g.pmid.nunique()
        by_theme[t] = {"universe": int(n),
                       "shared": _rate(g[g.universe_status == "shared"].pmid.nunique(), n, f"shared in theme / universe in theme ({t})"),
                       "new": _rate(g[g.universe_status == "new"].pmid.nunique(), n, f"new in theme / universe in theme ({t})")}
    out["this_corpus"]["by_theme"] = by_theme
    sp = shared.pmid.astype(str)
    sib_ex = sib[(sib.extraction_status == "extracted") & sib.pmid.astype(str).isin(set(sp))]
    out["this_corpus"]["shared_by_sibling_package"] = {
        p: _rate(g.pmid.nunique(), out["sibling"]["by_package"][p]["papers_extracted"], f"shared pmids in {p} / papers extracted in {p}")
        for p, g in sib_ex.groupby("package")}
    out["this_corpus"]["shared_findings_rows_in_sibling"] = _rate(sib_ex.n_findings.sum(), out["sibling"]["findings_rows"],
                                                                  "finding rows in sibling belonging to shared pmids / all sibling finding rows")
    return out


# ----------------------------------------------------------------------------------------------
# paper_condition_membership — the ADDITIVE table through which a sibling's rows are read
# ----------------------------------------------------------------------------------------------

MEMBERSHIP_COLUMNS = ["pmid", "corpus", "population_labels", "deciding_span", "span_verified_verbatim",
                      "definitions_version", "classifier", "assessed_at", "basis"]


def check_membership(mem, reg, enum_by_corpus):
    """Validate a paper_condition_membership table against the registry and each corpus's enum.
    enum_by_corpus: {corpus: [allowed labels]}.  Returns list of result dicts."""
    res = []
    missing = [c for c in MEMBERSHIP_COLUMNS if c not in mem.columns]
    res.append(_res("membership_schema", missing, f"columns {MEMBERSHIP_COLUMNS} present"))
    if missing:
        return res
    regk = set(zip(reg.pmid.astype(str), reg.corpus))
    res.append(_res("membership_pmid_in_registry",
                    [f"{p}|{c}" for p, c in zip(mem.pmid.astype(str), mem.corpus) if (p, c) not in regk],
                    "every (pmid, corpus) in the membership table has a registry row"))
    bad = []
    for r in mem.itertuples(index=False):
        labs = list(r.population_labels) if r.population_labels is not None else []
        allowed = set(enum_by_corpus.get(r.corpus, []))
        if not labs:
            bad.append(f"{r.pmid}|{r.corpus}|empty labels")
        for l in labs:
            if l not in allowed:
                bad.append(f"{r.pmid}|{r.corpus}|{l} not in enum")
    res.append(_res("membership_labels_in_enum", bad, "every label in population_labels is in that corpus's condition enum; >=1 label"))
    e = mem[mem.deciding_span.isna() | (mem.deciding_span.astype(str).str.strip() == "")]
    res.append(_res("membership_span_present", (e.pmid.astype(str) + "|" + e.corpus).tolist(), "deciding_span is a non-empty verbatim span"))
    d = mem[mem.duplicated(["pmid", "corpus", "definitions_version"], keep=False)]
    res.append(_res("membership_one_row_per_pmid_corpus_version", (d.pmid.astype(str) + "|" + d.corpus).unique().tolist(),
                    "one row per (pmid, corpus, definitions_version)"))
    e2 = mem[mem.definitions_version.isna() | mem.classifier.isna() | mem.basis.isna()]
    res.append(_res("membership_provenance_present", (e2.pmid.astype(str) + "|" + e2.corpus).tolist(),
                    "definitions_version, classifier and basis non-null on every row"))
    return res


# ----------------------------------------------------------------------------------------------
# config block
# ----------------------------------------------------------------------------------------------

def registry_config(cfg):
    """Read and minimally validate the `registry:` block of a config dict (see config/TEMPLATE.yaml)."""
    r = cfg.get("registry") or {}
    out = {"corpus": r.get("corpus"), "corpus_version": r.get("corpus_version"),
           "siblings": list(r.get("siblings") or []), "exceptions": list(r.get("exceptions") or [])}
    if not out["corpus"]:
        raise ValueError("registry.corpus is required")
    for s in out["siblings"]:
        for k in ("corpus", "corpus_version", "registry_vid", "repo_vid"):
            if k not in s:
                raise ValueError(f"registry.siblings[*].{k} is required")
    for e in out["exceptions"]:
        for k in ("pmid", "pipeline_generation", "reason"):
            if k not in e:
                raise ValueError(f"registry.exceptions[*].{k} is required")
    return out


def exceptions_frame(cfg_registry):
    ex = cfg_registry.get("exceptions") or []
    return pd.DataFrame(ex, columns=["pmid", "pipeline_generation", "reason"])


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="check a registry parquet")
    ap.add_argument("registry_parquet")
    a = ap.parse_args()
    reg = pd.read_parquet(a.registry_parquet)
    for r in check_registry(reg):
        print(("PASS" if r["ok"] else "FAIL"), r["name"], r["n_violations"], r["examples"][:3])
