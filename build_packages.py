#!/usr/bin/env python
"""
build_packages.py — reproducible builder for the five POTS-corpus deposit packages,
plus (2026-09-30) a GENERIC single-package mode for a new area:

  python build_packages.py --area-config config/<area>.yaml --corpus findings.parquet --roster roster.parquet \
         --qa-dir qa/ --scope-dir scope/ --out build --version v1 [--slug NAME] [--dry-run-note "..."]

The legacy mode (everything below) is the POTS v1 reproduction and is unchanged.

Reconstructed (2026-09-16) from the 2026-09-02 packaging session's cells, consolidated
into one script. In v1 mode (default flags, config as shipped) it reproduces the v1
packages byte-for-byte when --build-utc and the per-tier gzip mtimes are pinned.

Usage
-----
  python build_packages.py --config package_config.json --corpus findings_all_tiers.parquet \
         --inputs-dir build_inputs --out build [--build-utc 2026-09-02T11:10:50Z]
         [--annotations DIR] [--backfill-doi tier1=10.5281/zenodo.X ...] [--dtype-fix]

Inputs are declared in the config with artifact version ids and/or local paths. Resolution
order for every declared input file: --inputs-dir/<name>  >  config "path"  >  the artifact
store (only when a `host` object is present, i.e. when exec'd inside a Claude Science kernel).

Rules honoured: flag, never silently repair; never mutate the tier data — annotations ship
as ADDITIVE tables in annotations/; every rate carries numerator, denominator and basis.
"""
import argparse, datetime, gzip, hashlib, io, itertools, json, os, re, shutil, sys, tarfile, types
import numpy as np, pandas as pd, pyarrow.parquet as pq

# ----------------------------------------------------------------------------- helpers
def fmt(n): return f"{n:,}"

def sha256f(p, blk=1 << 20):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(blk), b""): h.update(b)
    return h.hexdigest()

def write(path, text):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as fh: fh.write(text)

def jdump(obj, path, **kw):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    json.dump(obj, open(path, "w"), indent=1, **kw)

def du_h(nbytes):
    """macOS `du -h`-style: MiB, one decimal below 10, integer above."""
    mib = nbytes / 1048576
    return f"{mib:.1f}" if mib < 10 else f"{round(mib)}"

def norm_str(s):
    s = str(s).lower().strip()
    s = re.sub(r'[^a-z0-9]+', ' ', s)
    return ' '.join(s.split())

# ----------------------------------------------------------------------------- inputs
class Inputs:
    def __init__(self, cfg, inputs_dir):
        self.cfg = cfg; self.dir = inputs_dir
        self.host = globals().get("host") or getattr(sys.modules.get("__main__"), "host", None)

    def resolve(self, name, spec):
        if self.dir and os.path.exists(os.path.join(self.dir, name)):
            return os.path.join(self.dir, name)
        if isinstance(spec, dict):
            if spec.get("path") and os.path.exists(spec["path"]): return spec["path"]
            vid = spec.get("artifact")
        else:
            vid = spec
        if vid and self.host is not None:
            return self.host.artifact_path(vid)
        raise FileNotFoundError(f"input {name!r} not found: no --inputs-dir copy, no local path, "
                                f"and no artifact store (artifact={vid})")

def all_declared_inputs(cfg):
    """name -> spec for every input file the config declares (checks name collisions)."""
    out = {}
    def add(name, spec):
        if name in out and out[name] != spec:
            raise ValueError(f"input name collision with different sources: {name}")
        out[name] = spec
    for name, spec in cfg["shared_inputs"].items(): add(name, spec)
    for name, spec in cfg["code_assets"].items(): add(name, spec)
    for pkg, pi in cfg["pipe"].items():
        for key in ("pipeline", "prompt", "driver"): add(pi[key][0], pi[key][1])
        for name, spec in pi.get("extra", {}).items(): add(name, spec)
    for grp in ("qa_extra", "scope_extra", "figures"):
        for pkg, mp in cfg.get(grp, {}).items():
            for name, spec in mp.items(): add(name, spec)
    return out

def export_inputs(cfg, dest):
    """Copy every declared input (incl. the corpus) out of the artifact store into dest/."""
    inp = Inputs(cfg, None); os.makedirs(dest, exist_ok=True)
    for name, spec in all_declared_inputs(cfg).items():
        shutil.copyfile(inp.resolve(name, spec), os.path.join(dest, name))
    shutil.copyfile(inp.resolve("corpus", cfg["corpus"]), os.path.join(dest, cfg["corpus"]["filename"]))
    return sorted(os.listdir(dest))

def load_module(path, name):
    src = open(path).read()
    mod = types.ModuleType(name); mod.__dict__["__file__"] = name
    exec(compile(src, name, "exec"), mod.__dict__)
    sys.modules[name] = mod
    return mod

# ----------------------------------------------------------------------------- statistics
def partition(full, cfg):
    """Add chunk_id_i / wave helper columns and return {pkg: boolean mask}."""
    full["chunk_id_i"] = full["chunk_id"].astype("Int64")
    waveB = set(cfg["tier2b_chunk_ids"])
    t2 = full.tier_source.eq("t2")
    full["wave"] = np.where(t2 & full.chunk_id_i.isin(waveB), "B", np.where(t2, "A", ""))
    sel = {}
    for pkg, meta in cfg["packages"].items():
        m = full.tier_source.eq(meta["tier_source"])
        if meta.get("wave"): m = m & full.wave.eq(meta["wave"])
        sel[pkg] = m
    return sel

def pkg_stats(d, pkg, meta):
    n = len(d); st = {}
    st["package"] = pkg; st["label"] = meta["label"]
    st["findings"] = n
    st["papers"] = int(d.pmid.nunique())
    st["chunk_ids"] = sorted(int(x) for x in d.chunk_id_i.dropna().unique())
    st["n_chunks"] = len(st["chunk_ids"])
    st["year_range"] = [int(d.year.min()), int(d.year.max())]
    st["year_hist"] = {str(int(k)): int(v) for k, v in d.groupby("year").pmid.nunique().items()}
    st["themes"] = {k: {"findings": int(v), "papers": int(d[d.theme.eq(k)].pmid.nunique())}
                    for k, v in d.theme.value_counts().items()}
    st["conditions"] = {k: {"findings": int(v), "papers": int(d[d.condition.eq(k)].pmid.nunique())}
                        for k, v in d.condition.value_counts().items()}
    st["source_literature"] = {str(k): int(v) for k, v in d.source_literature.value_counts(dropna=False).items()}
    st["evidence_tier"] = {str(k): int(v) for k, v in d.evidence_tier.value_counts(dropna=False).items()}
    st["finding_type"] = {str(k): int(v) for k, v in d.finding_type.value_counts(dropna=False).items()}
    st["certainty_derived"] = {str(k): int(v) for k, v in d.certainty_derived.value_counts(dropna=False).items()}
    st["causal_or_correlational"] = {str(k): int(v) for k, v in d.causal_or_correlational.value_counts(dropna=False).items()}
    strata = {}
    for s_, g in d.groupby("fulltext_status", dropna=False):
        strata[str(s_)] = dict(papers=int(g.pmid.nunique()), findings=len(g),
                               findings_per_paper=round(len(g) / max(g.pmid.nunique(), 1), 2))
    st["fulltext_strata"] = strata
    st["findings_per_paper_overall"] = round(n / st["papers"], 2)
    L = d.provenance_quote.fillna("").str.len()
    st["quote"] = dict(rows_with_quote=int((L > 0).sum()),
                       median_chars=int(L.median()), p90=int(L.quantile(.9)), p99=int(L.quantile(.99)),
                       max_chars=int(L.max()), over_200=int((L > 200).sum()), over_400=int((L > 400).sum()),
                       over_600=int((L > 600).sum()))
    st["provenance_section"] = {str(k): int(v) for k, v in d.provenance_section.value_counts(dropna=False).head(15).items()}
    idr = d.entity_ontology_id_resolved.notna()
    st["grounding"] = dict(
        rows_with_resolved_id=int(idr.sum()),
        id_resolved_frac_all_rows=round(float(idr.mean()), 4),
        entity_rows=int(d.entity.notna().sum()),
        id_resolved_frac_entity_rows=round(float(idr[d.entity.notna()].mean()), 4),
        rows_id_and_authority_label=int((idr & d.entity_ontology_label.notna()).sum()),
        verification={str(k): int(v) for k, v in d.ontology_id_verification.value_counts(dropna=False).items()},
        match_mode={str(k): int(v) for k, v in d.ontology_id_match_mode.value_counts(dropna=False).items()},
        id_prefix={str(k): int(v) for k, v in d.ontology_id_prefix.value_counts(dropna=False).head(12).items()},
        ontology_id_source={str(k): int(v) for k, v in d.ontology_id_source.value_counts(dropna=False).items()},
    )
    st["hpo"] = dict(rows_with_verified_hpo=int(d.hpo_ids_verified.notna().sum()),
                     verification={str(k): int(v) for k, v in d.hpo_verification.value_counts(dropna=False).items()},
                     rows_model_emitted_hpo=int(d.phenotype_hpo.notna().sum()))
    st["gene"] = dict(rows_gene_marker=int(d.gene_marker.notna().sum()),
                      rows_gene_resolved=int((d.gene_resolved == True).sum()),
                      rows_hgnc=int(d.hgnc_id.notna().sum()), rows_ensembl=int(d.ensembl_id.notna().sum()))
    st["canonicalization"] = dict(rows_canonical_entity=int(d.canonical_entity.notna().sum()),
                                  distinct_raw_entity=int(d.entity.nunique()),
                                  distinct_canonical=int(d.canonical_entity.nunique()),
                                  canonical_source={str(k): int(v) for k, v in d.canonical_source.value_counts(dropna=False).items()})
    st["capability_flags"] = {c: dict(true=int((d[c] == True).sum()), false=int((d[c] == False).sum()),
                                      null=int(d[c].isna().sum()))
                              for c in ["cap_entity_kind", "cap_hpo_verified", "cap_number_in_quote", "cap_quote_source_retained"]}
    st["flags"] = dict(query_contamination=int((d.query_contamination == True).sum()),
                       screen_verdict_uncertain=int((d.screen_verdict_uncertain == True).sum()),
                       null_or_failed_result=int(d.null_or_failed_result.notna().sum()),
                       number_in_quote_true=int((d.number_in_quote == True).sum()),
                       number_in_quote_false=int((d.number_in_quote == False).sum()))
    # NB: computed on d INCLUDING the chunk_id_i / wave helper columns (as v1 did), then popped —
    # the non-stable sort's tie order depends on the full column set.
    fr = (d.notna().mean() * 100).round(2).sort_values(ascending=False)
    st["fill_rates_pct"] = {k: float(v) for k, v in fr.items()}
    st["constant_columns"] = {c: str(d[c].dropna().iloc[0]) for c in d.columns
                              if d[c].nunique(dropna=True) == 1 and d[c].notna().any()}
    st["all_null_columns"] = [c for c in d.columns if d[c].isna().all()]
    return st

def compute_all(full, cfg, inputs, schema_cols, build_utc, corpus_vid):
    P = cfg["packages"]; SIB = list(P)
    sel = partition(full, cfg)
    # corpus-level checks
    assert sum(int(sel[p].sum()) for p in SIB) == len(full) == cfg["corpus_totals"]["findings"], "partition does not cover corpus"
    pm = {p: set(full[sel[p]].pmid) for p in SIB}
    for a, b in itertools.combinations(SIB, 2):
        assert not (pm[a] & pm[b]), f"paper overlap {a}|{b}"
    assert len(set().union(*pm.values())) == cfg["corpus_totals"]["papers"]

    co = pd.read_parquet(inputs.resolve("chunk_order.parquet", cfg["shared_inputs"]["chunk_order.parquet"]))
    alot = co.groupby("chunk_id").pmid.nunique().to_dict()
    alot_s = {str(int(k)): int(v) for k, v in alot.items()}
    INV = load_module(inputs.resolve("tier3_invariants.py", cfg["code_assets"]["tier3_invariants.py"]), "tier3_invariants")
    INVX = load_module(inputs.resolve("tier3_invariants_ext.py", cfg["code_assets"]["tier3_invariants_ext.py"]), "tier3_invariants_ext")
    vocab = pd.read_parquet(inputs.resolve("entity_vocabulary_crosstier.parquet", cfg["shared_inputs"]["entity_vocabulary_crosstier.parquet"]),
                            columns=["raw_entity", "canonical_entity"])

    STATS, INVRES, RES = {}, {}, {}
    for pkg in SIB:
        meta = P[pkg]; d = full[sel[pkg]].copy()
        st = pkg_stats(d, pkg, meta)
        # invariants (pinned suite + 9th check)
        exp_chunks = sorted(int(x) for x in d.chunk_id_i.dropna().unique())
        r = INV.run_all(d, exp_chunks, alot_s, prior_rows=0, prior_papers=0)
        r9 = INVX.check_authority_prefix_match(d)
        r["checks"].append(r9); r["all_ok"] = r["all_ok"] and r9["ok"]
        INVRES[pkg] = r
        st["tier_source"] = str(d.tier_source.iloc[0])
        st["chunk_paper_allotment"] = {str(c): int(alot[c]) for c in st["chunk_ids"] if c in alot}
        st["invariants"] = dict(all_ok=r["all_ok"],
                                per_check={c["name"]: dict(ok=c["ok"], **{k: v for k, v in c.items() if k not in ("name", "ok")})
                                           for c in r["checks"]})
        st["build_utc"] = build_utc
        st["source_corpus_version_id"] = corpus_vid
        st["partition"] = dict(siblings=SIB, this=pkg,
                               corpus_totals=dict(findings=cfg["corpus_totals"]["findings"], papers=cfg["corpus_totals"]["papers"], packages=len(SIB)),
                               pairwise_paper_overlap_all_zero=True,
                               verified_by="pmid set intersection across all 10 sibling pairs at build time")
        # verifiability
        ab = int(d.fulltext_status.isin(["abstract-only", "blocked"]).sum()); retained = bool(meta.get("source_text_retained"))
        st["verifiability"] = dict(
            rows_total=len(d), abstract_derived_rows=ab, abstract_derived_share_pct=round(ab / len(d) * 100, 2),
            fulltext_derived_rows=int(d.fulltext_status.eq("retrieved-fulltext").sum()),
            source_text_retained=retained,
            verifiable_rows=(len(d) if retained else ab),
            verifiable_share_pct=(100.0 if retained else round(ab / len(d) * 100, 2)),
            basis=("all rows: body text retained in gzipped chunk archives, so every quote is re-checkable" if retained else
                   "abstract-derived rows only; body text was discarded after extraction, so full-text-derived quotes have no surviving source"))
        # enum drift
        et = {k: int(v) for k, v in d.evidence_tier.value_counts().items() if k not in INV.ET_OK}
        cd = {k: int(v) for k, v in d.condition.value_counts().items() if k not in INV.CO_OK}
        st["enum_drift"] = dict(evidence_tier=et, condition=cd, total_drifted_rows=sum(et.values()) + sum(cd.values()),
                                share_pct=round((sum(et.values()) + sum(cd.values())) / len(d) * 100, 4))
        # helper columns out of the schema-facing summaries
        for k in ("wave", "chunk_id_i"):
            st["constant_columns"].pop(k, None); st["fill_rates_pct"].pop(k, None)
        st["all_null_columns"] = [c for c in st["all_null_columns"] if c in schema_cols]
        # quickstart expectations
        j = d[["entity"]].merge(vocab.rename(columns={"raw_entity": "entity", "canonical_entity": "canon"}), on="entity", how="left")
        has_derived = d.certainty_derived.notna().any(); col = "certainty_derived" if has_derived else "certainty"
        strong = d[d.evidence_tier.isin(["meta-analysis", "RCT"]) & d[col].eq("confirmed")]
        st["quickstart_expected"] = dict(
            strong_evidence_rows=int(len(strong)),
            vocab_join_canonicalized_rows=int(j.canon.notna().sum()),
            vocab_join_coverage_of_entity_rows_pct=round(float(j.canon.notna().sum() / max(d.entity.notna().sum(), 1) * 100), 2),
            certainty_column_used=col)
        # per-wave string-agreement proxy (pre-adjudication)
        m = d[d.entity_ontology_id_resolved.notna() & d.entity_ontology_label.notna()]
        RES[pkg] = (dict(rows_checked=int(len(m)), string_agree_pct=round(float((m.entity.map(norm_str) == m.entity_ontology_label.map(norm_str)).mean()) * 100, 2))
                    if len(m) else None)
        STATS[pkg] = st
    # id_denotes_entity_rate (tier-level adjudicated measurement) + inheritance disclosure
    for pkg in SIB:
        meta = P[pkg]; st = STATS[pkg]
        st["grounding"]["id_denotes_entity_rate"] = dict(
            value=meta["id_denotes_entity_rate"],
            basis=("measured on this package's own tier" if meta["rate_measured_per_package"]
                   else "INHERITED from Tier 2 as a whole — the 270-row adjudicated sample spans both waves; no per-wave rate exists"),
            adjudicated_sample_n=meta["adjudicated_sample_n"],
            measured_per_package=bool(meta["rate_measured_per_package"]),
            caveat="lower bound; the adjudicator over-calls DIFFERENT by ~17%")
        if meta.get("sibling_wave"):
            st["grounding"]["string_agreement_per_wave"] = dict(
                note="pre-adjudication proxy computed identically for both waves; counts synonyms as disagreements so not comparable to id_denotes_entity_rate",
                this_package=RES[pkg], sibling_wave=RES[meta["sibling_wave"]])
    corpus = dict(
        vsum=sum(STATS[p]["verifiability"]["verifiable_rows"] for p in SIB),
        findings=cfg["corpus_totals"]["findings"], papers=cfg["corpus_totals"]["papers"],
        n_chunks=sum(STATS[p]["n_chunks"] for p in SIB))
    corpus["vpct"] = round(corpus["vsum"] / corpus["findings"] * 100, 2)
    Lq = full.provenance_quote.fillna("").str.len()
    corpus["quote_median"] = int(Lq.median()); corpus["quote_max"] = int(Lq.max())
    corpus["quote_pct_under_400"] = round(float((Lq < 400).mean()) * 100, 1)
    return sel, STATS, INVRES, RES, corpus

# ----------------------------------------------------------------------------- document renderers
class Docs:
    def __init__(self, cfg, prose, STATS, RES, corpus, schema_cols, version):
        self.cfg = cfg; self.P = cfg["packages"]; self.SIB = list(cfg["packages"]); self.prose = prose
        self.S = STATS; self.RES = RES; self.C = corpus; self.cols = schema_cols; self.ver = version
        self.creator = cfg["creator"]; self.keywords = cfg["keywords"]
        # v1.1 documentation revision context (set by build() when --docs-revision v1.1):
        #   {"entity_code_rows": {pkg: n}, "entity_code_rows_corpus": n,
        #    "retrofit": <retrofit_summary.json dict or None>, "condition_ann": {pkg: {...counts}} or None}
        self.v11 = None
        # v1.2 context (set by build() when --docs-revision v1.2): {pkg: {kept_rows, replaced_rows, withheld_rows, ...}}
        self.v12 = None

    def v12_block(self, pkg):
        """README/METHODS/LIMITATIONS paragraph for the v1.2 identifier re-grounding; every number is from the data."""
        m = self.v12[pkg]; L = []
        L.append("**v1.2 (2026-10-21): every model-emitted identifier in this package was looked up by id at its minting")
        L.append("authority (HGNC via MyGene, ChEBI via OLS4, MeSH via NLM) and its label and full synonym list compared with the")
        L.append("entity name; names were also resolved forward to obtain the correct id where one exists. No model was involved.**"); L.append("")
        L.append(f"Of the {fmt(m['model_emitted_rows'])} rows that carried a model-emitted id in v1.1: **{fmt(m['kept_rows'])} denoted their entity** and are now tagged")
        L.append(f"`authority_verified_retrospective` with the match mode recorded ({', '.join(f'{k} {fmt(v)}' for k, v in sorted(m['match_modes'].items()))}); **{fmt(m['replaced_rows'])} ids were replaced** by the id the")
        L.append(f"entity name resolves to (exact label or authority synonym; `ontology_id_source` says so); **{fmt(m['withheld_rows'])} were withheld** (`id_withheld_unverified_v1.2`:")
        L.append("the id denoted a different entity and the name did not resolve). The original model id is preserved on every affected row in")
        L.append("`ontology_id_v11_model_emitted`; on replaced rows `entity_ontology_label` / `ontology_label_final` carry the NEW id's label, fetched by id from the authority. Nothing else changed: no row was added or removed and no other column was edited."); L.append("")
        if m["model_ids_denoting_entity_pct"] is not None:
            L.append(f"Measured on this package's full population: **{m['model_ids_denoting_entity_pct']}% of the model-emitted ids denoted their entity**; the")
            L.append("rest existed but pointed at a neighbouring gene, a related compound or a sibling concept — near-miss assignment, not fabrication.")
            L.append("The v1.1 figure quoted below was an adjudicated SAMPLE and is kept for the record. Full method and corpus-wide numbers: `annotations/POTS_MODEL_ID_BENCHMARK.md`."); L.append("")
        return L

    def rate(self, pkg): return self.P[pkg]["id_denotes_entity_rate_short"]
    def is_old(self, pkg): return not self.P[pkg]["authority_derived"]
    def corpus_wide_line(self): return f"Corpus-wide the figure is {self.C['vpct']}% ({fmt(self.C['vsum'])} of {fmt(self.C['findings'])} rows)."

    # ---- README ---------------------------------------------------------------
    def readme(self, pkg, annotations=None):
        st = self.S[pkg]; meta = self.P[pkg]; pr = self.prose
        d_inv = st["invariants"]; n_ok = sum(1 for c in d_inv["per_check"].values() if c["ok"]); n_tot = len(d_inv["per_check"])
        failing = [k for k, v in d_inv["per_check"].items() if not v["ok"]]
        top_themes = sorted(st["themes"].items(), key=lambda kv: -kv[1]["findings"])
        top_cond = sorted(st["conditions"].items(), key=lambda kv: -kv[1]["findings"])[:8]
        ver = st["verifiability"]; g = st["grounding"]; q = st["quote"]
        L = []; A = L.append
        A(f"# {meta['title']}"); A("")
        A(f"**{fmt(st['findings'])} findings · {fmt(st['papers'])} papers · "
          f"{st['n_chunks']} chunks ({meta['chunk_range']}) · {st['year_range'][0]}–{st['year_range'][1]}**")
        A("")
        A("This package stands alone. It contains its slice of the corpus, the documentation needed to")
        A("read it, the QA evidence behind every number below, and the code to re-derive all of it.")
        A("Nothing here requires another download or a network connection.")
        A(""); A("```")
        A("python code/verify_package.py              # re-derive every number in this README")
        A("python code/verify_package.py --self-test  # prove the checker still catches regressions")
        A("```"); A(""); A("---"); A("")
        if self.v12 is not None and self.is_old(pkg):
            A("## What changed in v1.2"); A("")
            for line in self.v12_block(pkg): A(line)
            A("---"); A("")
        A("## Read this before you query"); A("")
        # package-specific warnings (prose), formatted with computed values
        vals = dict(n_chunks=st["n_chunks"], findings=fmt(st["findings"]), papers=fmt(st["papers"]), corpus_wide=self.corpus_wide_line())
        warn = [(w["head"].format(**vals), w["body"].format(**vals)) for w in pr["readme_warnings"].get(pkg, [])]
        if self.v12 is not None and self.is_old(pkg):
            # v1.2: the v1.1 identifier warning no longer describes the data; replace it with the measured re-grounding
            m = self.v12[pkg]
            warn = [w for w in warn if "model-emitted" not in w[0]]
            warn.insert(0, ("Ontology identifiers were re-grounded in v1.2 — filter on `ontology_id_verification`.",
                            f"Of {fmt(m['model_emitted_rows'])} rows that carried a model-emitted id in v1.1, {fmt(m['kept_rows'])} denoted their entity at the authority and are tagged "
                            f"`authority_verified_retrospective`; {fmt(m['replaced_rows'])} ids were replaced by the id the name resolves to; {fmt(m['withheld_rows'])} were withheld "
                            f"(`id_withheld_unverified_v1.2`) because the id denoted another entity and the name did not resolve. The model's id is kept in `ontology_id_v11_model_emitted` "
                            f"for the record and must not be used for joins. Measured here: {m['model_ids_denoting_entity_pct']}% of the model-emitted ids were correct; the rest were near-miss "
                            f"assignments (a neighbouring gene, a related compound), not fabrications. See `annotations/POTS_MODEL_ID_BENCHMARK.md`."))
        warn.append((pr["readme_warning_fidelity"]["head"], pr["readme_warning_fidelity"]["body"]))
        if self.v11:
            ca = (self.v11.get("condition_ann") or {}).get(pkg)
            here = (f" In this package {fmt(ca['papers'])} papers ({fmt(ca['rows'])} rows) carry `condition == 'MCAS'`: "
                    f"{ca['in_bucket']} verified `in_bucket`, {ca['bucket_violation']} `bucket_violation`, {ca['NEEDS_HUMAN']} unassessed."
                    if ca and ca["papers"] else " This package carries no `MCAS`-labelled rows, but the same caveat applies to every bucket.")
            warn.append(("`condition` is a coarse POPULATION bucket, and the `MCAS` bucket is mostly not MCAS.",
                         "`condition` means the patient population the paper studied — not a diagnosis, not a mechanism-relevance tag. "
                         "Its bucket definitions were written down only on 2026-09-16 (`annotations/CONDITION_BUCKET_DEFINITIONS.md`). "
                         "A stratified census of the 1,083 `MCAS`-labelled papers corpus-wide projects ~69% study mast-cell biology "
                         "with no mast-cell-disorder population, ~10% MCAS as defined, ~5% mastocytosis (a distinct clonal neoplasm). "
                         "The shipped column is unchanged; `annotations/condition_annotations.parquet` (pmid-keyed) carries a per-paper "
                         "verdict, all classifier-derived and `human_confirmed == False`." + here))
        cond_warn_idx = len(warn)
        for i, (h, b) in enumerate(warn, 1):
            A(f"{i}. **{h}**"); A(f"   {b}")
        A(""); A("---"); A("")
        A("## What is in this package"); A("")
        A(f"**Topic:** {meta['themes_desc']}"); A("")
        A("### Themes"); A(""); A("| theme | findings | papers |"); A("|---|---:|---:|")
        for k, v in top_themes: A(f"| `{k}` | {fmt(v['findings'])} | {fmt(v['papers'])} |")
        A(""); A("### Conditions the findings are about"); A("")
        if self.v11:
            A("`condition` is a coarse bucket for the **patient population studied**, which is not the same as which")
            A("literature the paper came from (`source_literature`). A POTS paper reporting on a fibromyalgia")
            A("cohort has `source_literature` of the POTS/dysautonomia literature and `condition == 'fibromyalgia'`.")
            A(f"Bucket definitions: `annotations/CONDITION_BUCKET_DEFINITIONS.md`. Read warning {cond_warn_idx} above before")
            A("counting anything by `condition` — only the `MCAS` bucket has been censused, and it is mostly not MCAS.")
        else:
            A("`condition` is what a finding is about, which is not the same as which literature it came from")
            A("(`source_literature`). A POTS paper reporting a fibromyalgia comorbidity has")
            A("`source_literature` of the POTS/dysautonomia literature and `condition == 'fibromyalgia'`.")
        A(""); A("| condition | findings | papers |"); A("|---|---:|---:|")
        for k, v in top_cond: A(f"| `{k}` | {fmt(v['findings'])} | {fmt(v['papers'])} |")
        if len(st["conditions"]) > 8: A(f"| _{len(st['conditions'])-8} more_ | | |")
        A("")
        A("### Evidence quality"); A(""); A("| | |"); A("|---|---|")
        A(f"| findings per paper (overall) | {st['findings_per_paper_overall']} |")
        for s_, v in sorted(st["fulltext_strata"].items()):
            A(f"| findings per paper — `{s_}` | {v['findings_per_paper']} ({fmt(v['papers'])} papers, {fmt(v['findings'])} findings) |")
        A("")
        A("**Never compare findings-per-paper across packages without stratifying by")
        A("`fulltext_status`.** Yield is driven roughly 3× by whether full text was retrieved, and the")
        A("full-text mix differs sharply between packages. An unstratified comparison mostly measures")
        A("retrieval success, not extraction productivity.")
        A("")
        A("| evidence tier | findings |"); A("|---|---:|")
        for k, v in sorted(st["evidence_tier"].items(), key=lambda kv: -kv[1])[:12]: A(f"| `{k}` | {fmt(v)} |")
        A("")
        A("### Quote provenance"); A("")
        A(f"Every one of the {fmt(st['findings'])} rows carries a `provenance_quote` — the span of source")
        A(f"text the finding was drawn from. Median {q['median_chars']} characters, 90th percentile")
        A(f"{q['p90']}, longest {q['max_chars']}."); A("")
        A(f"**{fmt(ver['verifiable_rows'])} of {fmt(ver['rows_total'])} rows ({ver['verifiable_share_pct']}%) are verifiable** — meaning the")
        A("quote's own source text still exists and can be re-read."); A("")
        A(f"{ver['basis'].capitalize()}."); A("")
        if not ver["source_text_retained"]:
            A("The unverifiable remainder is a **retention** failure, not an extraction failure. Where")
            A("source text is available, traceability across the corpus measures 94–99% with **0%")
            A("wrong-paper false positives in every stratum**. The body text was simply discarded after")
            A("extraction. `qa/` carries the census with its negative control.")
            rt = ((self.v11 or {}).get("retrofit") or {}).get("per_tier", {}).get(pkg)
            if rt:
                va = rt["verifiable_after"]; ft = rt["strata"]["fulltext_rows_vs_fulltext"]
                A(""); A(f"**v1.1 retrofit:** the source of every paper in this package was re-fetched from Europe PMC and every quote")
                A(f"re-checked — **{fmt(va['n'])} of {fmt(va['denominator'])} rows ({va['pct']}%) now have a checkable source**; full-text quotes trace at")
                A(f"{ft['traceable_pct']}% ({fmt(ft['n_traceable'])}/{fmt(ft['n_checked'])}), wrong-paper control {ft['negative_control']['n_false_positives']}/{ft['negative_control']['n_control']}.")
                A("This checks Europe PMC's *current* text, not the bytes the model read, and the text is not shipped; the figure above")
                A("is the shipped-source figure. Row-level results: `annotations/quote_retrofit_*.parquet` (see the Annotations section).")
        else:
            A("This is the only package where verifiability is complete. Traceability measured 99.09%")
            A("against a 0% wrong-paper negative control. `qa/tier4_quote_census.json` carries the census.")
        A("")
        A("### Ontology grounding"); A(""); A("| | |"); A("|---|---|")
        A(f"| rows with a resolved ontology id | {fmt(g['rows_with_resolved_id'])} ({g['id_resolved_frac_all_rows']:.1%} of rows) |")
        A(f"| rows naming an entity at all | {fmt(g['entity_rows'])} |")
        A(f"| id coverage among entity-naming rows | {g['id_resolved_frac_entity_rows']:.1%} |")
        A(f"| rows with both an id and an authority label | {fmt(g['rows_id_and_authority_label'])} |")
        A("")
        A("**Verification status** — this is the column to filter on, not `ontology_id_source`:"); A("")
        A("| `ontology_id_verification` | rows |"); A("|---|---:|")
        for k, v in sorted(g["verification"].items(), key=lambda kv: -kv[1]): A(f"| `{k}` | {fmt(v)} |")
        A(""); A("| identifier prefix | rows |"); A("|---|---:|")
        for k, v in list(sorted(g["id_prefix"].items(), key=lambda kv: -kv[1]))[:8]: A(f"| `{k}` | {fmt(v)} |")
        A("")
        A("### Invariants"); A("")
        A(f"The pinned nine-check suite, run unmodified on this exact slice: **{n_ok} of {n_tot} pass.**"); A("")
        A("| check | result |"); A("|---|---|")
        for k, v in d_inv["per_check"].items(): A(f"| `{k}` | {'pass' if v['ok'] else '**fail — see below**'} |")
        A("")
        if failing:
            A("#### Why these fail, and why they are not repaired"); A("")
            for k in failing:
                note = pr["invariant_failure_notes"].get(k, {}).get(pkg)
                A(f"**`{k}`** — {note if note else 'see qa/package_stats.json for the raw check output.'}"); A("")
            for line in pr["invariant_failure_footer"]: A(line)
            A("")
        else:
            A("All nine pass. The suite is verified against planted regressions, not merely against data that")
            A("happened to pass — `python code/verify_package.py --self-test` re-plants six and asserts each")
            A("is caught."); A("")
        # enum drift block
        dr = st["enum_drift"]
        if dr["total_drifted_rows"]:
            A("### Enum drift — flagged, never coerced"); A("")
            A(f"{dr['total_drifted_rows']} rows ({dr['share_pct']}%) carry a value outside the pinned enum for")
            A("their column. The extraction prompt pinned both vocabularies; the model drifted past the")
            A("constraint anyway. **These values are shipped verbatim.** Silently remapping them would")
            A("destroy the evidence that the drift happened, and two of them are arguments that the enum is")
            A("deficient rather than the output wrong."); A("")
            A("| column | value | rows |"); A("|---|---|---:|")
            for col in ("evidence_tier", "condition"):
                for k, v in sorted(dr[col].items(), key=lambda kv: -kv[1]): A(f"| `{col}` | `{k}` | {v} |")
            A("")
            for key, note in pr["enum_drift_notes"]:
                col, val = key.split(":", 1)
                if val in dr[col] or (col == "evidence_tier" and val == "systematic-review/meta-analysis" and "systematic review" in dr[col]):
                    A(f"- {note}")
            A("")
        # contents
        A("---"); A(""); A("## Contents"); A(""); A("| path | what it is |"); A("|---|---|")
        A(f"| `data/{pkg}_findings.parquet` | **THE DATA.** {fmt(st['findings'])} rows × {len(self.cols)} columns. Start here. |")
        A(f"| `data/{pkg}_findings.csv.gz` | Identical content as CSV, for tools without parquet support. |")
        A("| `data/entity_vocabulary_crosstier.parquet` | Canonicalization lookup, 39,128 raw strings → 36,173 canonical. Mutates no rows. |")
        A("| `data/chunk_order.parquet` | Paper → chunk → tier assignment for all 14,263 corpus papers. |")
        A("| `docs/README.md` | This file. |")
        A("| `docs/DATA_DICTIONARY.md` | Every column, with its measured fill rate in this package. |")
        A("| `docs/COMPARABILITY.md` | What may be pooled with the sibling packages and what will mislead you. |")
        A("| `docs/METHODS.md` | How the extraction ran, with the pinned parameters. |")
        A("| `docs/LIMITATIONS.md` | What is not known, not measured, and known wrong. |")
        A("| `docs/WEBSITE_INTEGRATION.md` | What is new for the site from this package, and what breaks on a careless load. |")
        A("| `qa/package_stats.json` | Every number in this README, machine-readable, with its basis. |")
        A("| `qa/invariants.json` | The nine-check suite output on this slice. |")
        A("| `qa/comparability_report.json` | The full cross-package alignment analysis. |")
        A("| `qa/*` | The original QA evidence from this tier's extraction run. |")
        A("| `code/verify_package.py` | Re-derives every number above from the shipped data. |")
        A("| `code/tier3_invariants*.py` | The pinned invariant modules, byte-identical to the build. |")
        A("| `code/quote_trace.py` | The quote matcher — refuses to report a rate without its negative control. |")
        A("| `code/*` | The extraction pipeline, prompt and driver used for this tier. |")
        if self.v11:
            A("| `code/README.md` | The role of every file in `code/` — including why a Tier-1 package carries `tier3_*` modules. |")
            A("| `code/comparator_symmetry_tests.py` | Pins that the quote matcher decodes entities and folds whitespace on BOTH sides. |")
        A("| `MANIFEST.json` | File inventory with SHA-256 for every file. |")
        A("| `CHECKSUMS.sha256` | `shasum -a 256 -c CHECKSUMS.sha256` to verify the download. |")
        A("| `LICENSE` | CC BY 4.0 for the data; the quote column is separately described. |")
        A("| `CITATION.cff` | How to cite this package. |")
        if pkg in self.cfg.get("scope_extra", {}): A("| `scope/*` | Excluded and no-yield PMIDs with reasons. |")
        if pkg in self.cfg.get("figures", {}): A("| `figures/*` | Per-chunk yield and QA figures. |")
        if annotations: A("| `annotations/*` | ADDITIVE annotation tables keyed to this package's rows/papers — see below. |")
        A("")
        # quickstart
        A("## Quickstart"); A(""); A("```python"); A("import pandas as pd")
        A(f"df = pd.read_parquet('data/{pkg}_findings.parquet')"); A("")
        A("# 1. Findings you can independently verify against source text")
        if ver["source_text_retained"]:
            A("verifiable = df                      # all of them, in this package")
        else:
            A("verifiable = df[df.fulltext_status.isin(['abstract-only', 'blocked'])]")
            A(f"#  -> {fmt(ver['verifiable_rows'])} rows ({ver['verifiable_share_pct']}%)")
        A("")
        if self.is_old(pkg) and self.v12 is not None:
            m = self.v12[pkg]
            A("# 2. Ontology identifiers — filter on VERIFICATION, never on coverage (v1.2 re-grounding)")
            A("df.ontology_id_verification.value_counts()")
            A("#  -> no_id, authority_verified_retrospective, id_withheld_unverified_v1.2")
            A("grounded = df[df.ontology_id_verification.isin(['authority_verified', 'authority_verified_retrospective'])]")
            A(f"#  -> {fmt(m['retro_rows'] + m['original_verified_rows'])} rows whose id was confirmed at its authority ({fmt(m['replaced_rows'])} of them replaced by the id the name resolves to)")
            A("withheld = df[df.ontology_id_verification == 'id_withheld_unverified_v1.2']")
            A(f"#  -> {fmt(m['withheld_rows'])} rows: the model-emitted id denoted another entity; it is kept in ontology_id_v11_model_emitted, not in entity_ontology_id_resolved")
        elif self.is_old(pkg):
            A("# 2. Ontology identifiers: there are NO authority-verified ids in this package.")
            A("df.ontology_id_verification.value_counts()")
            A("#  -> no_id, unverified_model_emitted   (authority_verified appears only in Tiers 3-4)")
            A("#")
            A("# The identifiers present are model-emitted and ~29-42% do not denote their entity.")
            A("# To ground entities here, join the shipped vocabulary and resolve names yourself:")
            A("unverified = df[df.ontology_id_verification == 'unverified_model_emitted']")
            A(f"#  -> {fmt(g['verification']['unverified_model_emitted'])} rows carry an id that has NOT been authority-checked")
        else:
            A("# 2. Ontology identifiers you can trust — filter on VERIFICATION, never on coverage")
            A("grounded = df[df.ontology_id_verification == 'authority_verified']")
            A(f"#  -> {fmt(g['verification'].get('authority_verified', 0))} rows in this package")
        A("")
        qe = st["quickstart_expected"]
        A("# 3. Highest-confidence evidence")
        if qe["certainty_column_used"] == "certainty_derived":
            A("#    certainty_derived is hedge-detection over BOTH finding_text and the quote;")
            A("#    it never overwrites the extractor's own `certainty` judgment.")
            A("strong = df[df.evidence_tier.isin(['meta-analysis', 'RCT'])")
            A("            & df.certainty_derived.eq('confirmed')]")
        else:
            A("#    NOTE: `certainty_derived` is EMPTY in this package - it was introduced in Tier 3.")
            A("#    Use `certainty`, the extractor's own judgment, which is populated here.")
            A("strong = df[df.evidence_tier.isin(['meta-analysis', 'RCT'])")
            A("            & df.certainty.eq('confirmed')]")
        A(f"#  -> {fmt(qe['strong_evidence_rows'])} rows"); A("")
        A("# 4. Marker frequency — case-fold, or canonicalize with the shipped vocabulary")
        A("vocab = pd.read_parquet('data/entity_vocabulary_crosstier.parquet')")
        A("df = df.merge(vocab[['raw_entity', 'canonical_entity']]")
        A("              .rename(columns={'raw_entity': 'entity',")
        A("                               'canonical_entity': 'canon'}),")
        A("              on='entity', how='left')")
        A("top = df.canon.value_counts().head(20)")
        A("```"); A("")
        A("**Never lowercase entity names to merge them.** `MECP2` and `Mecp2` are the human and mouse")
        A("genes and must stay apart. The shipped vocabulary holds every mandated ortholog pair apart")
        A("(CNTNAP2/Cntnap2, IL6/Il6, TH/Th, PIEZO2/Piezo2, …) and scored 0 false merges against 25")
        A("planted should-not-merge controls, with 2,459 of 2,460 should-merge pairs correct.")
        A("")
        for line in pr["readme_schema_evolution_caveat"]: A(line)
        if annotations:
            A("---"); A(""); A("## Annotations (additive)"); A("")
            for line in pr["annotations_intro"]: A(line)
            A(""); A("| file | rows | keyed on | what it is |"); A("|---|---:|---|---|")
            for a in annotations:
                A(f"| `annotations/{a['file']}` | {fmt(a['rows']) if a['rows'] is not None else '—'} | {a['key']} | {a['desc']} |")
            A("")
            if self.v11:
                rt = ((self.v11.get("retrofit") or {}).get("per_tier") or {}).get(pkg)
                if rt:
                    vb, va, ta = rt["verifiable_before"], rt["verifiable_after"], rt["traceable_after_all_verifiable"]
                    ft = rt["strata"]["fulltext_rows_vs_fulltext"]; ab = rt["strata"]["abstract_stratum_rows_vs_abstract"]
                    A("### Quote source retrofit (2026-09-16)"); A("")
                    A("The body text this tier was extracted from was discarded after extraction, so the `quotes verifiable`")
                    A(f"figure above ({ver['verifiable_share_pct']}%) is what the shipped data alone supports. On 2026-09-16 the")
                    A("source text of every paper was re-fetched from Europe PMC at the pinned pacing and every quote re-checked")
                    A("with the shipped comparator (`code/quote_trace.py`, `code/retrofit_census.py`). The fetched text is")
                    A("**not** shipped (see LICENSE); the row-level result is `annotations/quote_retrofit_" + pkg + ".parquet`."); A("")
                    A("| | rows | of | rate | basis |"); A("|---|---:|---:|---:|---|")
                    A(f"| verifiable before | {fmt(vb['n'])} | {fmt(vb['denominator'])} | {vb['pct']}% | shipped data: abstract-derived rows only |")
                    A(f"| verifiable after | {fmt(va['n'])} | {fmt(va['denominator'])} | {va['pct']}% | rows whose own re-fetched source exists |")
                    A(f"| traceable, full-text rows vs re-fetched full text | {fmt(ft['n_traceable'])} | {fmt(ft['n_checked'])} | {ft['traceable_pct']}% | control {ft['negative_control']['n_false_positives']}/{ft['negative_control']['n_control']} wrong-paper false positives |")
                    A(f"| traceable, abstract-stratum rows vs re-fetched abstract | {fmt(ab['n_traceable'])} | {fmt(ab['n_checked'])} | {ab['traceable_pct']}% | control {ab['negative_control']['n_false_positives']}/{ab['negative_control']['n_control']} |")
                    A(f"| traceable, all verifiable rows | {fmt(ta['n'])} | {fmt(ta['denominator'])} | {ta['pct']}% | **stratify on `retrofit_source_depth × fulltext_status` before quoting** |")
                    A("")
                    dg = rt.get("fulltext_rows_downgraded_to_abstract_source", 0); pl = rt["permanent_losses"]["n_rows_no_source"]
                    A(f"{fmt(pl)} rows have no source in Europe PMC today (neither abstract nor body) and stay unverifiable."
                      + (f" {fmt(dg)} full-text rows whose body could not be re-fetched were checked against the abstract only and trace at "
                         "≈0% for source-depth reasons, not because the quote is wrong." if dg else "")
                      + " Europe PMC's *current* text was used, so a")
                    A("paper revised since extraction registers as untraceable. Traceability is well-formedness, not correctness:")
                    A("a traceable quote can still support a misread finding."); A("")
                ca = (self.v11.get("condition_ann") or {}).get(pkg)
                if ca and ca["papers"]:
                    A("### Condition bucket annotations"); A("")
                    A(f"`annotations/condition_annotations.parquet` covers the {fmt(ca['papers'])} papers ({fmt(ca['rows'])} rows) in this")
                    A(f"package labelled `condition == 'MCAS'`: **{ca['in_bucket']} `in_bucket`, {ca['bucket_violation']} `bucket_violation`**")
                    A(f"({ca.get('mastocytosis', 0)} mastocytosis, {ca.get('mechanism_only', 0)} mast-cell biology without a disorder population,")
                    A(f"{ca.get('not_applicable', 0)} neither), **{ca['NEEDS_HUMAN']} `NEEDS_HUMAN`**. Verdicts are classifier-derived against")
                    A("`CONDITION_BUCKET_DEFINITIONS.md` v1 under population-studied semantics and none is human-confirmed.")
                    A("Other buckets are not yet censused. See `annotations/CONDITION_ANNOTATIONS_README.md`."); A("")
        A("---"); A(""); A("## The other four packages"); A("")
        A("This is one block of a five-way partition of a single corpus. **Zero papers are shared")
        A("between any two packages** — verified by pmid set intersection across all ten pairs at build")
        A("time. Per-package counts sum exactly to the corpus totals."); A("")
        A("| package | findings | papers | chunks | identifiers | quotes verifiable |"); A("|---|---:|---:|---:|---|---:|")
        for sp in self.SIB:
            s2 = self.S[sp]; here = " ←" if sp == pkg else ""
            idkind = "authority-derived" if self.P[sp]["authority_derived"] else "model-emitted"
            A(f"| {self.P[sp]['title'].split(' — ')[0]}{here} | {fmt(s2['findings'])} | {fmt(s2['papers'])} | "
              f"{s2['n_chunks']} | {idkind} | {s2['verifiability']['verifiable_share_pct']}% |")
        A(f"| **corpus total** | **{fmt(self.C['findings'])}** | **{fmt(self.C['papers'])}** | **{self.C['n_chunks']}** | | **{self.C['vpct']}%** |")
        A("")
        A("Pooling any subset is safe for row and paper counts, paper metadata, quote presence, the")
        A("harmonized verification columns, and the capability flags. Eleven other metrics are **not**")
        A("comparable across all five — `docs/COMPARABILITY.md` lists each with its reason."); A("")
        A("Tier 2 Part A and Part B are the two waves of a single extraction pass with one pipeline and")
        A("one prompt; pool those two freely."); A(""); A("---"); A("")
        A(f"Built {st['build_utc']} from harmonized corpus `{st['source_corpus_version_id']}`.")
        A("Data CC BY 4.0. Machine-extracted research information — **not medical advice.**")
        return "\n".join(L) + "\n"

    # ---- DATA_DICTIONARY --------------------------------------------------------
    def data_dictionary(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; COLDESC = dict(self.prose["column_descriptions"])
        if self.v11:
            n_ec = self.v11["entity_code_rows"].get(pkg, 0); n_ecc = self.v11["entity_code_rows_corpus"]
            COLDESC["condition"] = ("**Coarse bucket for the patient POPULATION studied** — not a diagnosis, not a mechanism-relevance tag. "
                "Definitions: `annotations/CONDITION_BUCKET_DEFINITIONS.md` v1 (`MCAS` excludes mastocytosis; `EDS_hypermobility` includes HSD and "
                "Beighton-assessed hypermobility; `general` = no listed condition is the subject). The `MCAS` bucket is ~69% mechanism-only papers by "
                "stratified census — see the README's `condition` warning and `annotations/condition_annotations.parquet`. Distinct from `source_literature` "
                "(which literature the paper came from).")
            COLDESC["provenance_quote"] = (COLDESC["provenance_quote"] +
                f" **Verbatim with respect to the text the extraction model read, not to the paper as displayed:** {fmt(n_ec)} rows in this package "
                f"({fmt(n_ecc)} corpus-wide, {100*n_ecc/self.C['findings']:.2f}%) contain raw HTML entity codes such as `p&#x202f;<&#x2009;0.001`, because "
                "Europe PMC text was fed to the model without entity decoding. They trace to source under the shipped comparator, which decodes "
                "entities and folds whitespace-class characters on both sides (`code/quote_trace.py`, `code/comparator_symmetry_tests.py`). "
                "They are never repaired in place — a decoded copy would no longer match the retained source.")
        fr = st["fill_rates_pct"]; nullc = set(st["all_null_columns"]); const = st["constant_columns"]
        L = []; A = L.append
        A(f"# Data Dictionary — `data/{pkg}_findings.parquet`"); A("")
        A(f"**{fmt(st['findings'])} rows × {len(self.cols)} columns.** Every fill rate below is **measured on")
        A("this package's own slice**, not inherited from another tier. Recompute them with")
        A("`python code/verify_package.py`."); A("")
        A(f"> **Scope: {meta['title']}**"); A(">")
        A(f"> {fmt(st['papers'])} papers, chunks {meta['chunk_range']}, of an {fmt(self.C['papers'])}-paper five-package corpus.")
        A("> Four sibling packages hold the rest and share zero papers with this one."); A(">")
        A("> **Not medical advice.** Machine-extracted research information with no diagnostic or")
        A("> treatment recommendations."); A(">")
        A("> **Attribution.** Every row links to its source paper. These are automated summaries of")
        A("> other researchers' work; the original paper is the authority."); A("")
        A("## Four rules before you query"); A("")
        A("1. **Count papers with `pmid.nunique()`.** Every paper figure in this package uses that basis.")
        A("2. **Filter identifiers on `ontology_id_verification`, never on `ontology_id_source`.** The")
        A("   latter has inverted semantics between tiers — see `COMPARABILITY.md`, Trap 3.")
        A("3. **Stratify on `fulltext_status` before comparing any per-paper rate.** It drives yield ~3×.")
        A("4. **A null in a `cap_*` or tier-specific column means NOT PRESENT, not unknown.** This")
        A(f"   package has {len(nullc)} entirely-null columns, listed at the bottom — they are retained for")
        A("   schema compatibility with its siblings, so a pooled read has a stable column set."); A("")
        A("## Columns"); A("")
        A("Ordered as in the parquet. `fill` is the percentage of rows with a non-null value."); A("")
        A("| # | column | fill | description |"); A("|---:|---|---:|---|")
        for i, c in enumerate(self.cols):
            f_ = fr.get(c, 0.0); tag = ""
            if c in nullc: tag = " **[EMPTY in this package]**"
            elif c in const: tag = f" **[CONSTANT: `{const[c]}`]**"
            A(f"| {i} | `{c}` | {f_:.1f}% | {COLDESC[c]}{tag} |")
        A(""); A("## Entirely empty in this package"); A("")
        if nullc:
            A(f"{len(nullc)} columns hold no value on any of the {fmt(st['findings'])} rows. **Null here means the")
            A("property was NOT CAPTURED by this extraction pass — not that it was measured and found")
            A("absent.** They are retained so that this package's schema matches its siblings' and a")
            A("pooled read needs no column alignment."); A("")
            for c in sorted(nullc): A(f"- `{c}` — {COLDESC[c]}")
        else:
            A("None — every column carries at least one value.")
        A(""); A("## Single-valued in this package"); A("")
        if const:
            A("These columns carry exactly one distinct value, so they cannot discriminate anything within")
            A("this package. **`source_fidelity` in particular is not a quality filter that passed** — it")
            A("is a forward-compatibility placeholder."); A("")
            A("| column | its only value |"); A("|---|---|")
            for c, v in sorted(const.items()): A(f"| `{c}` | `{v}` |")
        else:
            A("None.")
        A(""); A("## Multi-value columns"); A("")
        A("`hpo_ids_verified` and `hpo_labels_verified` are **semicolon-delimited**, because 43.2% of")
        A("mappable symptom strings resolve to more than one distinct verified HPO term and a")
        A("one-id-per-row schema cannot express that. `hpo_id_count` is invariant-checked against the")
        A("delimiter count on every build."); A("")
        A("```python"); A("df['hpo_list'] = df.hpo_ids_verified.fillna('').str.split(';')")
        A("df.explode('hpo_list')   # one row per HPO term"); A("```"); A("")
        A(f"In this package, {fmt(st['hpo']['rows_with_verified_hpo'])} rows carry at least one verified HPO identifier."); A("")
        return "\n".join(L) + "\n"

    # ---- METHODS ---------------------------------------------------------------
    def methods(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; pr = self.prose["methods_params"][pkg]; pi = self.cfg["pipe"][pkg]
        L = []; A = L.append
        A(f"# Methods — {meta['title']}"); A(""); A("## What this package is"); A("")
        A(f"{fmt(st['findings'])} structured findings extracted from {fmt(st['papers'])} papers by a large language")
        A("model reading each paper's abstract or full text, one paper at a time, into a pinned schema."); A("")
        A("The corpus was assembled by keyword query against PubMed and Europe PMC across the POTS,")
        A(f"dysautonomia and comorbid-condition literatures, deduplicated, and split into {self.C['n_chunks']}")
        A("50-paper chunks. Chunks were assigned to four tiers by topic. This package holds")
        A(f"chunks {meta['chunk_range']}."); A("")
        A("## Pipeline"); A(""); A("| | |"); A("|---|---|")
        A(f"| extraction model | `{pr['model']}` |")
        A(f"| prompt | `code/{pi['prompt'][0]}` (pinned, shipped) |")
        A(f"| pipeline | `code/{pi['pipeline'][0]}` (pinned, shipped) |")
        A(f"| driver | `code/{pi['driver'][0]}` (pinned, shipped) |")
        A("| full-text source | Europe PMC REST |")
        A("| fetch pacing | 0.9 s at 3 concurrent workers (~3.3 req/s) |")
        A("| papers per chunk | 50 |"); A("")
        A("**Do not raise the fetch concurrency.** Europe PMC rate-limits on aggregate concurrency:")
        A("3 workers at 0.9 s produced ~0–3% blocked papers, while 8.5 req/s produced up to 92%")
        A("blocked. That is the whole reason for the pacing constant."); A("")
        A("### Specific to this pass"); A("")
        for n in pr["notes"]: A(f"- {n}")
        A(""); A("## Identifier grounding"); A("")
        if not self.is_old(pkg):
            A("The extraction model was **never asked for an identifier**. It emitted entity names and gene")
            A("symbols; a multi-authority resolver (OLS4 + UniProt, `code/multi_authority_resolver.py`)")
            A("looked every identifier up afterwards from the name, storing the authority's own label")
            A("alongside it so that correctness could be audited offline with zero API calls."); A("")
            A("This is why this package scores")
            A(f"**{self.rate(pkg)}** on `id_denotes_entity_rate` against Tier 1's {self.rate('tier1')}."); A("")
            A("Fuzzy matches were **adjudicated, never accepted** — 59% of fuzzy OLS4 HPO matches were")
            A("wrong, and accepting them would have reproduced the fabrication defect with a false")
            A("'authority verified' provenance.")
        else:
            A("**The extraction model was asked to emit ontology identifiers directly. This was a mistake,")
            A("and it is the central quality limitation of this package.**"); A("")
            A("There is no reasoning path from an entity name to an arbitrary integer — only recall — and a")
            A("model has no introspective signal for 'do I know this integer?'. The prompt said not to")
            A("invent identifiers, which cannot work. Measured outcome: `anaphylaxis` → *Liver abscess*,")
            A("`IgE` → *Iguanas*."); A("")
            A("A later repair pass normalized case and namespaces on 4.8% of rows. **That pass was")
            A("syntactic and re-derived nothing from names** — zero rows in this package carry a resolved")
            A("identifier without a model-emitted one, so no identifier here was ever independently")
            A("derived from an entity name."); A("")
            if meta["rate_measured_per_package"]:
                A(f"Measured `id_denotes_entity_rate`: **{self.rate(pkg)}** (a lower bound — the")
                A("adjudicator over-calls DIFFERENT by ~17%). Tiers 3 and 4, which emit names only and resolve")
            else:
                A(f"Measured `id_denotes_entity_rate`: **{self.rate(pkg)}** (a lower bound — the adjudicator over-calls")
                A("DIFFERENT by ~17%). **Measured on Tier 2 as a whole — a 270-row adjudicated sample across both")
                A("waves — and inherited by this package. No per-wave rate was computed.** Tiers 3 and 4, which emit names only and resolve")
            A(f"afterwards, score {self.rate('tier3')} and {self.rate('tier4')}."); A("")
            if self.v12 is not None:
                A("The sentences above describe v1.0/v1.1 and are kept as history. In v1.2 the model-emitted ids were")
                A("re-grounded against their authorities row by row:"); A("")
                for line in self.v12_block(pkg): A(line)
            else:
                A("Every affected row is tagged `ontology_id_verification == 'unverified_model_emitted'`,")
                A("so the limitation is queryable rather than merely documented.")
        A(""); A("## Quote provenance"); A("")
        A("Every finding carries the verbatim source span it was drawn from. Traceability is measured by")
        A("`code/quote_trace.py` — exact substring match, or ≥60% 8-shingle overlap after normalization."); A("")
        A("**The matcher refuses to report a rate without its negative control.** The control pairs each")
        A("quote with a *different* paper's text; a working matcher scores ~0%. Every stratum of every")
        A("package scored 0.0% wrong-paper false positives."); A("")
        A("This is not a formality. A traceability check once read 100% because the caller did `if tr:`")
        A("on a truthy tuple — the wrong-paper control read 400/400 and exposed it."); A("")
        A("**Superscripts are deliberately not normalized.** Normalizing `I2` against `I-squared` drops")
        A("the census from 99.52% to 99.04%, so the raw form is retained."); A("")
        if st["verifiability"]["source_text_retained"]:
            A("Body text was retained, so this package's census covers **all** rows against their own")
            A("source. Every other package could only census its abstract-derived rows.")
        else:
            A("Body text was discarded after extraction, so only abstract-derived rows")
            A(f"({fmt(st['verifiability']['verifiable_rows'])}, {st['verifiability']['verifiable_share_pct']}%) could be censused against their own")
            rt = ((self.v11 or {}).get("retrofit") or {}).get("per_tier", {}).get(pkg)
            if rt:
                A("source. In v1.1 every paper's source was re-fetched from Europe PMC (3 workers, 0.9 s pacing) and every quote")
                A("re-checked with the same comparator, each stratum with its own wrong-paper control; results ship as")
                A("`annotations/quote_retrofit_*.parquet` and `annotations/RETROFIT_REPORT.md`. The fetched text was deleted after the census.")
            else:
                A("source. A 97-paper Tier-3 pilot showed re-fetching is feasible: 97/97 recovered from Europe")
                A("PMC, 98.65% traceability, 0% control false positives, at ~2.3 h wall for ~6,200 papers on 3")
                A("workers at the pinned pacing.")
        A(""); A("## Validation"); A("")
        A("Four families of check, run after every merge step, shipped in `code/tier3_invariants.py` and")
        A("`code/tier3_invariants_ext.py` and re-runnable via `code/verify_package.py`:"); A("")
        for line in self.prose["methods_validation_checks"]: A(line)
        A("")
        A(f"**On this package: {sum(1 for c in st['invariants']['per_check'].values() if c['ok'])} of 9 pass.**")
        if not st["invariants"]["all_ok"]:
            A("The failures are pre-existing properties of this extraction pass, characterized in the")
            A("README. They are shipped as failures rather than suppressed: a suite tuned until it passes")
            A("is not a suite.")
        A("")
        A("**The suite was verified by planting regressions**, not by running it on data that happened to")
        A("pass. `verify_package.py --self-test` re-plants six and asserts each is still caught."); A("")
        A("## What validation does NOT cover"); A("")
        A("Everything above tests **well-formedness**: quotes are real text from the right paper,")
        A("identifiers carry authority provenance, counts are conserved, enums are flagged. **None of it")
        A("tests whether a finding correctly interprets its source.**"); A("")
        A("That requires a human reader. An LLM adjudicator shares the extractor's blind spots, so it")
        A("cannot substitute. A 200-row fidelity worksheet with pre-registered thresholds is drawn and")
        A("unscored."); A("")
        A("## Reproducing"); A(""); A("```")
        A("python code/verify_package.py --self-test     # validate the shipped slice"); A("```"); A("")
        A("Re-running extraction requires network access to Europe PMC and an API key for the")
        A("extraction model. The pinned pipeline, prompt and driver are in `code/`; `data/chunk_order.parquet`")
        A("carries the paper→chunk→tier assignment for all 14,263 corpus papers, so any chunk range can")
        A("be re-dispatched. Hold the pacing constants."); A("")
        return "\n".join(L) + "\n"

    # ---- LIMITATIONS -----------------------------------------------------------
    def limitations(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; ver = st["verifiability"]; g = st["grounding"]; is_old = self.is_old(pkg)
        retained = ver["source_text_retained"]
        L = []; A = L.append
        A(f"# Limitations — {meta['title']}"); A("")
        A("Ordered by how much they should change what you conclude. Nothing here is hedging: each item")
        A("is a measured property with its measurement."); A("")
        A("## 1. Scientific fidelity is unmeasured"); A("")
        A("**This is the largest limitation and it applies to every package.**"); A("")
        A("Everything in `qa/` verifies well-formedness — a quote is real text from the right paper, an")
        A("identifier carries authority provenance, counts are conserved, enum drift is flagged. **No")
        A("check asks whether a finding correctly interprets its source.**"); A("")
        A("A finding can be perfectly well-formed and still misread the paper: reverse a direction,")
        A("attribute a control-group result to the case group, or state a correlation as causal where")
        A("the source hedged. Nothing here would catch that."); A("")
        A("Measuring it needs a human domain reader. An LLM adjudicator shares the extractor's blind")
        A("spots and cannot substitute. A 200-row worksheet with pre-registered thresholds is drawn and")
        A("unscored."); A("")
        A("**Practical consequence: treat every row as a pointer to a paper you should read, not as a")
        A("fact you can cite.** The `provenance_quote` and `pmid` columns exist to make that cheap."); A("")
        if is_old and self.v12 is not None:
            m = self.v12[pkg]
            A("## 2. Ontology identifiers were model-emitted; in v1.2 they are re-grounded, and a share is withheld"); A("")
            for line in self.v12_block(pkg): A(line)
            A("What remains limited: (a) `compound_partial` matches mean the entity string named several entities and the id denotes one of")
            A("them — read the `entity` string; (b) `exact_relaxed` tolerates case, plural, Greek letters and punctuation, never string similarity;")
            A(f"(c) the {fmt(m['withheld_rows'])} withheld rows have an entity NAME but no identifier — resolve the name yourself if you need one;")
            A("(d) correctness was established by label correspondence at the authority, which is the same standard Tiers 3-4 used, but it was")
            A("applied retrospectively to ids the model chose, so coverage here is still the model's coverage, not the resolver's."); A("")
            A(f"For the record, v1.1 reported `id_denotes_entity_rate` = **{self.rate(pkg)}** from an adjudicated sample"
              + ("" if meta["rate_measured_per_package"] else " pooled across both Tier-2 waves") + "; the full-population")
            A(f"figure in v1.2 is {m['model_ids_denoting_entity_pct']}% once authority synonyms and spelling variants are credited."); A("")
            A("**Practical consequence: join on `entity_ontology_id_resolved` only where `ontology_id_verification` is")
            A("`authority_verified` or `authority_verified_retrospective`.** Never use `ontology_id_v11_model_emitted` for joins; it is history."); A("")
        elif is_old:
            A("## 2. Ontology identifiers are model-emitted and largely wrong"); A("")
            rate = self.rate(pkg); pct = round((1 - rate) * 100)
            if meta["rate_measured_per_package"]:
                A(f"`id_denotes_entity_rate` = **{rate}** — roughly **{pct}% of the identifiers in this package do not")
                A("denote the entity on their own row.** That is a lower bound; the adjudicator over-calls")
                A("DIFFERENT by ~17%.")
            else:
                sib = meta["sibling_wave"]; ra, rb = self.RES["tier2a"], self.RES["tier2b"]
                A(f"`id_denotes_entity_rate` = **{rate}** — roughly **{pct}% of identifiers do not denote the entity on")
                A("their own row.** That is a lower bound; the adjudicator over-calls DIFFERENT by ~17%."); A("")
                A("**This rate is INHERITED from Tier 2 as a whole, not measured on this wave.** It comes from a")
                A(f"270-row adjudicated sample drawn across both waves pooled ({fmt(ra['rows_checked'] + rb['rows_checked'])} rows with an id and an")
                A("authority label). No per-wave rate was ever measured, so the true value for this package")
                A("alone is unknown."); A("")
                A("The pre-adjudication signal that *can* be computed per wave suggests they are close. Exact")
                A("string agreement between `entity` and the authority's own `entity_ontology_label`, under one")
                A(f"consistent normalizer: **Part A {ra['string_agree_pct']}%** ({fmt(ra['rows_checked'])} rows) versus **Part B {rb['string_agree_pct']}%** "
                  f"({fmt(rb['rows_checked'])} rows) — {abs(ra['string_agree_pct'] - rb['string_agree_pct']):.2f} points apart.")
                A("That is a weaker measure than the adjudicated rate (it counts synonyms as disagreements, so")
                A(f"the absolute numbers are not comparable to {rate}), but it is computed identically for both")
                A("waves and gives no reason to expect them to differ materially."); A("")
                A("Coverage, by contrast, IS measured on this package's own slice and does differ between the")
                A(f"waves: Part A {self.S['tier2a']['grounding']['id_resolved_frac_all_rows']:.1%}, Part B {self.S['tier2b']['grounding']['id_resolved_frac_all_rows']:.1%}.")
            A("")
            A(f"{fmt(g['rows_with_resolved_id'])} rows carry an identifier, all tagged")
            A("`unverified_model_emitted`. The comparison that matters:"); A("")
            A("| package | id coverage | id correctness |"); A("|---|---:|---:|")
            for line in self.prose["coverage_vs_correctness_table"]:
                A(line.replace("| Tier 2 |", "| Tier 2 (A+B pooled) |") if not meta["rate_measured_per_package"] else line)
            A("")
            A("Coverage and correctness rank the packages in *opposite* orders. The high-coverage packages")
            A("are the low-correctness ones."); A("")
            A("**Practical consequence: do not use this package's identifiers as an entity mapping.** Use")
            A("the `entity` string with the shipped vocabulary, or resolve names yourself against OLS4 /")
            A("UniProt — `code/multi_authority_resolver.py` is the resolver Tiers 3-4 used."); A("")
            A("This is the largest correctness gap in the corpus and it is fixable: re-grounding Tiers 1-2")
            A("through the hardened resolver is the second-highest-value outstanding action."); A("")
        else:
            A("## 2. Identifier coverage is low, even though correctness is high"); A("")
            A(f"Identifiers here are authority-derived and score {self.rate(pkg)} on")
            A(f"`id_denotes_entity_rate`. But only {fmt(g['rows_with_resolved_id'])} rows")
            A(f"({g['id_resolved_frac_all_rows']:.1%}) carry one at all."); A("")
            A("The resolver leaves a name unmatched rather than guessing, and fuzzy matches were")
            A("adjudicated rather than accepted (59% of fuzzy OLS4 HPO matches were wrong). **Low coverage")
            A("is the price of the high correctness, and it was the right trade** — but it means an")
            A("identifier-keyed analysis covers a minority of rows."); A("")
            A("**Practical consequence: identifier-keyed views cover a fraction of the findings. Never")
            A("present an identifier-keyed count as a total.** Keyword search on `entity` reaches the rest."); A("")
        A(f"## 3. {'Most quotes cannot be re-checked' if not retained else 'Verifiability is complete here but not corpus-wide'}"); A("")
        if not retained:
            A(f"**{fmt(ver['rows_total']-ver['verifiable_rows'])} of {fmt(ver['rows_total'])} rows ({100-ver['verifiable_share_pct']:.2f}%) have a quote whose source text no")
            A("longer exists.** The body text was discarded after extraction, so a finding drawn from a")
            A("paper's Results section has a quote nobody can verify without re-fetching the paper."); A("")
            A("**This is a retention failure, not an extraction failure.** Where source text survives,")
            A("traceability is 94–99% with 0% wrong-paper false positives in every stratum. The extraction")
            A("was sound; the evidence was thrown away."); A("")
            rt = ((self.v11 or {}).get("retrofit") or {}).get("per_tier", {}).get(pkg)
            if rt:
                va = rt["verifiable_after"]; ft = rt["strata"]["fulltext_rows_vs_fulltext"]
                A("**The v1.1 retrofit recovered most of it, with a weaker guarantee.** Every paper's source was re-fetched from")
                A(f"Europe PMC and every quote re-checked: {fmt(va['n'])} of {fmt(va['denominator'])} rows ({va['pct']}%) now have a checkable source,")
                A(f"full-text quotes trace at {ft['traceable_pct']}%, wrong-paper control {ft['negative_control']['n_false_positives']}/{ft['negative_control']['n_control']}. Weaker because it checks")
                A("Europe PMC's text *today*, not the bytes the model read, and because the fetched text is not shipped (LICENSE) —")
                A("so it is not reproducible offline the way Tier 4's retained archive is. `annotations/quote_retrofit_*.parquet`"); A("")
            else:
                A("It is recoverable: a 97-paper stratified pilot re-fetched **97/97** from Europe PMC at")
                A("98.65% traceability. Roughly 2.3 hours for ~6,200 papers at the pinned pacing."); A("")
        else:
            A("Every row in this package is verifiable — body text was retained. **But corpus-wide")
            A(f"verifiability is {self.C['vpct']}%**, because Tiers 1-3 discarded theirs. If you pool this package")
            A("with its siblings, the pooled verifiability is not 100%."); A("")
        A("## 4. Yield depends on full-text availability, not on the literature"); A("")
        A("Findings per paper by stratum in this package:"); A("")
        A("| stratum | papers | findings | per paper |"); A("|---|---:|---:|---:|")
        for s_, v in sorted(st["fulltext_strata"].items()):
            A(f"| `{s_}` | {fmt(v['papers'])} | {fmt(v['findings'])} | {v['findings_per_paper']} |")
        A("")
        A("The full-text stratum yields roughly 3× the abstract-only stratum. **An unstratified")
        A("comparison of yield between packages mostly measures Europe PMC retrieval success.**"); A("")
        A("A related consequence that is easy to miss: **absence of a finding is not evidence of")
        A("absence.** A marker may be missing because no paper reported it, because the paper was")
        A("abstract-only, or because retrieval failed. These are indistinguishable in the data."); A("")
        A("## 5. Entity coverage, and what a null entity means"); A("")
        ent = g["entity_rows"]
        A(f"{fmt(ent)} of {fmt(st['findings'])} rows ({ent/st['findings']:.1%}) name an entity. The remainder name")
        A("**none by design** — symptom-burden, phenotype, quality-of-life and study-characteristic")
        A("findings have no marker to name."); A("")
        A("**Practical consequence: an entity-keyed view covers roughly half these findings, and that is")
        A("correct behavior, not missing data.** Never present an entity-keyed count as a total."); A("")
        A("Entity strings are **not canonicalized in-row** for Tiers 1-2. Case variants")
        A("(`Interleukin-6` / `interleukin-6`) are distinct strings. Join")
        A("`data/entity_vocabulary_crosstier.parquet`, or case-fold at query time — but **never")
        A("lowercase to merge**, which would collapse human and mouse gene symbols (`MECP2`/`Mecp2`)."); A("")
        A("## 6. Enum drift"); A("")
        dr = st["enum_drift"]
        if dr["total_drifted_rows"]:
            A(f"{dr['total_drifted_rows']} rows ({dr['share_pct']}%) carry a value outside the pinned enum. **Shipped")
            for line in self.prose["limitations_enum_drift_tail"]: A(line)
        else:
            A("No out-of-enum values in this package.")
        if self.v11:
            und = (self.v11.get("undefined_condition_values") or {}).get(pkg) or {}
            A("")
            A("The `condition` column carries three values that appear in the shipped data but have no written")
            A("definition in `annotations/CONDITION_BUCKET_DEFINITIONS.md`: `IBD_gut`, `orthostatic_hypotension` and")
            A("`other patient group` — 4 papers corpus-wide" + (f", of which {sum(und.values())} in this package ({', '.join(f'`{k}` {v}' for k, v in und.items())})" if und else ", none in this package") + ".")
            A("They are extraction-time enum drift, not buckets. Read them as `general` for population counts; they are not")
            A("repaired in the data.")
        A(""); A("## 7. Cross-package traps"); A("")
        for line in self.prose["limitations_cross_package_traps"]: A(line)
        A(""); A("## 8. Corpus construction"); A("")
        for line in self.prose["limitations_corpus_construction"]: A(line)
        A(""); A("## 9. All adjudicated error rates are LLM-judged"); A("")
        A("The identifier-denotation rates, the HPO fuzzy-match verdicts, and the quote-residual")
        A("classifications were adjudicated by a model, not a human. Each has a negative control")
        A("establishing that the instrument discriminates, and controls are reported alongside every")
        A("rate — but a control proves the instrument is not random, not that it is right."); A("")
        if self.v11:
            ca = (self.v11.get("condition_ann") or {}).get(pkg) or {}
            n_ec = self.v11["entity_code_rows"].get(pkg, 0)
            A("## 10. The `condition` bucket is a population label, and one bucket is mostly mislabelled"); A("")
            A("`condition` is a coarse enum for the population studied. Its inclusion rules were written down only on")
            A("2026-09-16 (`annotations/CONDITION_BUCKET_DEFINITIONS.md` v1), after extraction. A stratified census of")
            A("the `MCAS` bucket found ~69% of its papers study mast-cell biology with no MCAS population and ~5% study")
            A("mastocytosis, a different disease. The shipped column is not edited; `annotations/condition_annotations.parquet`")
            if ca.get("papers"):
                A(f"records a per-paper verdict against the definitions. In this package: {ca['in_bucket']} papers `in_bucket`,")
                A(f"{ca['bucket_violation']} `bucket_violation`, {ca['NEEDS_HUMAN']} `NEEDS_HUMAN` — all classifier-derived and unconfirmed. The")
            else:
                A("records a per-paper verdict against the definitions (this package has no `MCAS`-labelled rows). The")
            A("other buckets have not been censused and may carry the same defect at an unknown rate. Eight of the")
            A("thirteen bucket definitions are provisional."); A("")
            A("## 11. Quotes are verbatim to model input, not to the rendered paper"); A("")
            A(f"{fmt(n_ec)} quotes in this package carry raw HTML entity codes (`&#x202f;`, `&#xa0;`, …) because the pipeline")
            A("did not decode Europe PMC text before extraction. They match their source under the shipped comparator,")
            A("which normalizes both sides symmetrically (`code/comparator_symmetry_tests.py`, 8 cases); a naive substring")
            A("check against a clean copy of the paper will miss them. This is a presentation defect, not a fabrication,")
            A("and it is not repaired in place."); A("")
            if not retained:
                rt = ((self.v11.get("retrofit") or {}).get("per_tier") or {}).get(pkg)
                if rt:
                    A("## 12. Verifiability was retrofitted, not retained"); A("")
                    A(f"Section 3 above describes the shipped data: {ver['verifiable_share_pct']}% of rows verifiable. The 2026-09-16 retrofit")
                    A(f"re-fetched source text and lifted that to {rt['verifiable_after']['pct']}% (`annotations/quote_retrofit_{pkg}.parquet`,")
                    A("README §Annotations). The retrofit checked quotes against Europe PMC's *current* text, not the text the model")
                    A("read; the retained-archive guarantee Tier 4 has is not reproducible here. Stratify on `retrofit_source_depth`")
                    A("before quoting any traceability rate from it."); A("")
        A("## What would most improve this package"); A("")
        if self.v11:
            k = 1
            if is_old:
                A(f"{k}. **Re-ground the identifiers** through the resolver Tiers 3-4 used. This is the single"); A("   largest correctness gap and the machinery already exists."); k += 1
            A(f"{k}. **Score the fidelity worksheet** with a human reader — the only way to learn whether the"); A("   extracted science is right. The AI-scorer calibration (Q7/Q8 machine-extendable, Q3/Q10 human) cuts it to ~2–4 h."); k += 1
            A(f"{k}. **Human-confirm the `condition` annotations** — start with the 66 papers whose title contains \"mastocytos\","); A("   then census the remaining buckets against the written definitions."); k += 1
            if not retained: A(f"{k}. **Re-fetch and RETAIN the source text** so the retrofit's verifiability holds against a frozen archive, as in Tier 4."); k += 1
            if retained: A(f"{k}. **Extend identifier coverage** — {g['id_resolved_frac_all_rows']:.1%} is the corpus low, though what is there is right."); k += 1
            if retained: A(f"{k}. **Adjudicate the {fmt(st['flags']['screen_verdict_uncertain'])} `screen_verdict_uncertain` rows** with a human reader (v1 docs said 1,960; the data says {fmt(st['flags']['screen_verdict_uncertain'])}).")
            A(""); return "\n".join(L) + "\n"
        if is_old:
            A("1. **Re-ground the identifiers** through the resolver Tiers 3-4 used. This is the single")
            A("   largest correctness gap and the machinery already exists.")
            A("2. **Re-fetch full text** to lift verifiability from")
            A(f"   {ver['verifiable_share_pct']}% toward ~100%. Pilot success was 97/97.")
            A("3. **Score the fidelity worksheet** with a human reader.")
        else:
            A("1. **Score the fidelity worksheet** with a human reader — the only way to learn whether the")
            A("   extracted science is right.")
            if not retained:
                A("2. **Re-fetch full text** to lift verifiability from")
                A(f"   {ver['verifiable_share_pct']}% toward ~100%.")
                A("3. **Extend identifier coverage** by resolving the currently-unmatched entity names.")
            else:
                A(f"2. **Extend identifier coverage** — {g['id_resolved_frac_all_rows']:.1%} is the corpus low, though what is there is right.")
                # v1 prose constant (1,960); the data's own count is st['flags']['screen_verdict_uncertain'] (2,257 in v1).
                # Kept verbatim to reproduce v1 — flagged in the build report, not silently repaired.
                A(f"3. **Adjudicate the {self.prose['tier4_screen_uncertain_prose_count']} `screen_verdict_uncertain` rows** with a human reader.")
        A("")
        return "\n".join(L) + "\n"

    # ---- code/README.md (v1.1) --------------------------------------------------
    def code_readme(self, pkg, files):
        roles = self.prose.get("code_roles", {}); DEFAULT_ROLE = "Pinned code from this tier's extraction run."
        L = []; A = L.append
        A("# code/ — pinned build and verification code for this package"); A("")
        A("Every file here is byte-identical to what produced or verified the shipped data. Files are named for")
        A("the tier whose build **introduced** them, so a Tier-1 package legitimately carries `tier3_invariants.py`:")
        A("the invariant suite written for Tier 3 is the one run against every package. Shared modules are")
        A("identical across all five packages (sha256-verified at build)."); A("")
        A("| file | role |"); A("|---|---|")
        for f in sorted(files):
            if f == "README.md": continue
            A(f"| `{f}` | {roles.get(f, DEFAULT_ROLE)} |")
        A("")
        A("Run order for a reader: `python verify_package.py --self-test` (re-derives every documented number and")
        A("plants six regressions), then `python normalizer_tests.py` and `python comparator_symmetry_tests.py` (the")
        A("quote matcher's unit tests — one case per historical bug). Nothing here needs the network except the")
        A("extraction pipeline and the retrofit fetcher, which are shipped as the record of what ran, not as tools")
        A("you are expected to re-run."); A("")
        return "\n".join(L) + "\n"

    # ---- WEBSITE_INTEGRATION ----------------------------------------------------
    def site_guide(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; SITE = self.cfg["site"]
        after_f = SITE["cur_findings"] + st["findings"]; after_p = SITE["cur_papers"] + st["papers"]
        shards = -(-after_f // SITE["shard_rows"])
        vals = dict(cur_findings=fmt(SITE["cur_findings"]), cur_papers=fmt(SITE["cur_papers"]), cur_entities=fmt(SITE["cur_entities"]),
                    cur_shards=SITE["cur_shards"], cur_version=SITE["cur_version"], t1_rows=fmt(SITE["t1_rows"]),
                    t1_delta=SITE["t1_rows"] - st["findings"], t2a_rows=fmt(SITE["cur_findings"] - SITE["t1_rows"]),
                    after_findings=fmt(after_f), after_papers=fmt(after_p), shard_rows=fmt(SITE["shard_rows"]), after_shards=shards,
                    findings=fmt(st["findings"]), papers=fmt(st["papers"]))
        L = []; A = L.append
        A(f"# Website integration guide — {meta['title']}"); A("")
        A("What is new for the site from this package, and what breaks if you load it carelessly.")
        A("This guide assumes the three-layer contract in `SITE_CONTRACT.md`: **Layer 1 THE RECORD**")
        A("(never edited), **Layer 2 regenerable** `site/data/*.json` + `emb/*.bin`, **Layer 3")
        A("presentation** (edit freely)."); A("")
        A("## Where this package stands relative to the deployed site"); A("")
        A(f"The site currently serves **{vals['cur_findings']} findings / {vals['cur_papers']} papers /")
        A(f"{vals['cur_entities']} canonical entities**, `meta.json` version `{vals['cur_version']}`,")
        A(f"{vals['cur_shards']} shards."); A("")
        for line in self.prose["site_guide_body"][pkg]: A(line.format(**vals))
        A("")
        for line in self.prose["site_guide_footer"]: A(line)
        A("")
        A(f"Package data: {fmt(st['findings'])} findings, {fmt(st['papers'])} papers, "
          f"{st['n_chunks']} chunks, {st['year_range'][0]}–{st['year_range'][1]}.")
        return "\n".join(L) + "\n"

    # ---- sidecars ---------------------------------------------------------------
    def zenodo_json(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]
        rel = [dict(identifier=f"PLACEHOLDER-DOI-{sp}", relation="references", resource_type="dataset", scheme="doi")
               for sp in self.SIB if sp != pkg]
        return {
            "title": f"{meta['title']} — machine-extracted structured findings ({fmt(st['findings'])} findings, {fmt(st['papers'])} papers)",
            "upload_type": "dataset",
            "description": (
                f"<p>Package {meta['order']} of 5. {fmt(st['findings'])} structured findings extracted from "
                f"{fmt(st['papers'])} papers ({st['year_range'][0]}\u2013{st['year_range'][1]}) covering {meta['themes_desc']}.</p>"
                f"<p>One block of a five-way partition of an {fmt(self.C['papers'])}-paper, {fmt(self.C['findings'])}-finding corpus of the POTS, "
                f"dysautonomia and comorbid-condition literature. The five packages share <strong>zero papers</strong>; "
                f"per-package counts sum exactly to the corpus totals.</p>"
                f"<p><strong>Self-contained.</strong> Includes the findings table (parquet and CSV), a data dictionary "
                f"with fill rates measured on this slice, the QA evidence behind every stated number, the pinned "
                f"extraction and validation code, and <code>code/verify_package.py</code>, which re-derives every "
                f"headline number from the shipped data and exits non-zero on disagreement.</p>"
                f"<p><strong>Ontology identifiers:</strong> "
                + (f"authority-derived (the extraction model emitted names only; a resolver looked identifiers up "
                   f"afterwards), scoring {self.rate(pkg)} on identifier-denotes-entity rate."
                   if not self.is_old(pkg) else
                   f"model-emitted and existence-checked only, scoring {self.rate(pkg)} on "
                   "identifier-denotes-entity rate. Do not use them as an entity mapping \u2014 see LIMITATIONS.md.")
                + "</p>"
                f"<p><strong>Quote verifiability:</strong> {st['verifiability']['verifiable_share_pct']}% of rows have a "
                f"provenance quote whose own source text is still available and re-checkable.</p>"
                f"<p><strong>Scientific fidelity is unmeasured.</strong> All QA verifies well-formedness, not whether "
                f"a finding correctly interprets its source. Treat every row as a pointer to a paper to read.</p>"
                f"<p><strong>Not medical advice.</strong></p>"),
            "creators": [{"name": self.creator["name"], "orcid": self.creator["orcid"]}],
            "keywords": self.keywords,
            "license": "cc-by-4.0", "access_right": "open", "language": "eng",
            "version": f"{pkg}-{self.ver}",
            "related_identifiers": rel,
            "notes": ("The provenance_quote column contains short verbatim excerpts from the source publications "
                      "and is NOT covered by CC BY 4.0 \u2014 that text remains the copyright of the respective "
                      "publishers and authors. See LICENSE for the excerpt policy and takedown commitment."),
        }

    def osf_json(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]
        return {
            "title": meta["title"], "category": "data",
            "description": (f"Package {meta['order']} of 5 \u2014 {fmt(st['findings'])} structured findings from "
                            f"{fmt(st['papers'])} papers on {meta['themes_desc']}. Self-contained: data, docs, QA "
                            f"evidence and re-derivation code. Zero papers shared with the sibling packages."),
            "tags": self.keywords,
            "license": {"name": "CC-By Attribution 4.0 International", "copyright_holders": [self.creator["name"]]},
            "contributors": [{"full_name": self.creator["name"], "bibliographic": True, "orcid": self.creator["orcid"]}],
            "node_structure": {"suggested_parent_project": self.cfg["project_title"], "this_component": meta["title"],
                               "sibling_components": [self.P[s]["title"] for s in self.SIB if s != pkg]},
            "wiki_home": "See docs/README.md \u2014 paste its contents as the component wiki home page.",
            "files_of_note": {"data": f"data/{pkg}_findings.parquet", "start_here": "docs/README.md", "verify": "code/verify_package.py"},
            "notes": "provenance_quote holds verbatim source excerpts and is not CC BY \u2014 see LICENSE.",
        }

    def gh_release(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; ver = st["verifiability"]; g = st["grounding"]
        n_ok = sum(1 for c in st["invariants"]["per_check"].values() if c["ok"]); retained = ver["source_text_retained"]
        L = []; A = L.append
        A(f"# {meta['title']} — `{pkg}-{self.ver}`"); A("")
        A(f"**{fmt(st['findings'])} findings · {fmt(st['papers'])} papers · {st['n_chunks']} chunks · {st['year_range'][0]}–{st['year_range'][1]}**"); A("")
        A(f"Package {meta['order']} of 5. Self-contained: data, documentation, QA evidence and the code to")
        A("re-derive every number. No other download required."); A("")
        A("## At a glance"); A(""); A("| | |"); A("|---|---|")
        A(f"| topic | {meta['themes_desc']} |")
        A(f"| ontology identifiers | {'authority-derived' if not self.is_old(pkg) else 'model-emitted, unverified'} (`id_denotes_entity_rate` {self.rate(pkg)}) |")
        A(f"| identifier coverage | {fmt(g['rows_with_resolved_id'])} rows ({g['id_resolved_frac_all_rows']:.1%}) |")
        A(f"| quote verifiability | {ver['verifiable_share_pct']}% |")
        A(f"| invariants | {n_ok}/9 pass |")
        A("| paper overlap with siblings | zero |"); A("")
        A("## Verify the download"); A(""); A("```"); A("shasum -a 256 -c CHECKSUMS.sha256")
        A("python code/verify_package.py --self-test"); A("```"); A("")
        A("The second command re-derives every number in the README from the shipped parquet and exits")
        A("non-zero if any disagrees. It also re-plants six regressions to prove the checker still bites."); A("")
        A("## Read first"); A("")
        A("- `docs/README.md` — start here")
        A("- `docs/COMPARABILITY.md` — what you may pool with the sibling packages, and the three traps")
        A("- `docs/LIMITATIONS.md` — what is not known, not measured, and known wrong"); A("")
        A("## Caveats that change conclusions"); A("")
        A("- **Scientific fidelity is unmeasured.** QA verifies well-formedness, not correct")
        A("  interpretation. Every row is a pointer to a paper you should read.")
        if self.is_old(pkg):
            A("- **Ontology identifiers are model-emitted.** Roughly")
            A(f"  {round((1-self.rate(pkg))*100)}% do not denote the entity on their row. Not usable as a mapping.")
        if not retained:
            A(f"- **{100-ver['verifiable_share_pct']:.2f}% of quotes cannot be re-checked** — body text was discarded after extraction.")
        else:
            A(f"- **Every quote is re-checkable** — the only package where that is true. {self.corpus_wide_line()}")
        A("- **Absence of a finding is not evidence of absence.** Yield depends ~3× on full-text")
        A("  availability."); A("")
        A("Data CC BY 4.0. `provenance_quote` holds verbatim source excerpts and is not CC BY — see")
        A("`LICENSE`. **Not medical advice.**")
        return "\n".join(L) + "\n"

    def citation_cff(self, pkg):
        st = self.S[pkg]; meta = self.P[pkg]; cr = self.creator
        L = []; A = L.append
        A("cff-version: 1.2.0")
        A('message: "If you use this dataset, please cite it — and cite the source papers for any finding that matters to your work."')
        A("type: dataset"); A(f'title: "{meta["title"]}"'); A(f'version: "{pkg}-{self.ver}"')
        A(f'date-released: "{st["build_utc"][:10]}"'); A("license: CC-BY-4.0"); A(f'repository-code: "https://github.com/{self.cfg.get("github_owner", "<owner>")}/{self.cfg.get("repo_name", "pots-corpus")}"'); A("authors:")
        A(f'  - family-names: "{cr["family"]}"'); A(f'    given-names: "{cr["given"]}"'); A(f'    orcid: "https://orcid.org/{cr["orcid"]}"')
        A("abstract: >-")
        A(f"  Package {meta['order']} of 5. {fmt(st['findings'])} structured findings machine-extracted from")
        A(f"  {fmt(st['papers'])} papers ({st['year_range'][0]}-{st['year_range'][1]}) covering {meta['themes_desc']}.")
        A(f"  One block of a five-way partition of an {fmt(self.C['papers'])}-paper, {fmt(self.C['findings'])}-finding corpus of the POTS,")
        A("  dysautonomia and comorbid-condition literature; the five packages share zero papers.")
        A("  Scientific fidelity is unmeasured: all QA verifies well-formedness, not correct")
        A("  interpretation of sources. Not medical advice.")
        A("keywords:")
        for k in self.keywords: A(f'  - "{k}"')
        A("identifiers:"); A("  - type: doi"); A(f'    value: "PLACEHOLDER-DOI-{pkg}"'); A('    description: "Replace with the DOI minted on deposit."')
        A("references:")
        for sp in self.SIB:
            if sp == pkg: continue
            A("  - type: dataset"); A(f'    title: "{self.P[sp]["title"]}"'); A("    authors:")
            A(f'      - family-names: "{cr["family"]}"'); A(f'        given-names: "{cr["given"]}"')
            A(f'    notes: "Sibling package {self.P[sp]["order"]} of 5. Replace notes with its DOI once minted."')
        return "\n".join(L) + "\n"

# ----------------------------------------------------------------------------- manifest / checksums
def build_manifest(pkg, root, cfg, st, schema_cols, version, meta, creator):
    files = []
    for dp, dns, fns in os.walk(root):
        dns[:] = [x for x in dns if x != "__pycache__"]
        for fn in sorted(fns):
            if fn in ("MANIFEST.json", "CHECKSUMS.sha256") or fn.endswith((".pyc", ".pyo")) or fn == ".DS_Store": continue
            p = os.path.join(dp, fn)
            files.append(dict(path=os.path.relpath(p, root), bytes=os.path.getsize(p), sha256=sha256f(p)))
    files.sort(key=lambda x: x["path"])
    man = dict(
        package=f"{pkg_slug(cfg, pkg)}-{version}", tier=pkg, title=meta["title"], version=f"{pkg}-{version}", built_utc=st["build_utc"],
        creator=dict(name=creator["name"], orcid=creator["orcid"]),
        license=dict(data="CC-BY-4.0", provenance_quote="verbatim source excerpts, copyright of respective publishers; see LICENSE"),
        primary_table=f"{pkg}_findings.parquet",
        contents=dict(findings=st["findings"], papers=st["papers"], chunks=st["n_chunks"], chunk_ids=st["chunk_ids"],
                      year_range=st["year_range"], tier_source=st["tier_source"]),
        schema=dict(n_columns=len(schema_cols), columns=schema_cols, all_null_in_this_package=sorted(st["all_null_columns"]),
                    constant_in_this_package=st["constant_columns"]),
        quality=dict(
            invariants_passing=sum(1 for c in st["invariants"]["per_check"].values() if c["ok"]),
            invariants_total=len(st["invariants"]["per_check"]), invariants_all_ok=st["invariants"]["all_ok"],
            failing_checks=[k for k, v in st["invariants"]["per_check"].items() if not v["ok"]],
            id_resolved_frac=st["grounding"]["id_resolved_frac_all_rows"],
            id_denotes_entity_rate=meta["id_denotes_entity_rate"],
            id_provenance=("authority-derived" if meta["authority_derived"] else "model-emitted, existence-checked only"),
            quote_verifiable_rows=st["verifiability"]["verifiable_rows"], quote_verifiable_pct=st["verifiability"]["verifiable_share_pct"],
            enum_drift_rows=st["enum_drift"]["total_drifted_rows"],
            scientific_fidelity="UNMEASURED — all QA verifies well-formedness, not correct interpretation",
            id_denotes_entity_rate_basis=st["grounding"]["id_denotes_entity_rate"]["basis"]),
        partition=st["partition"], source_corpus_version_id=st["source_corpus_version_id"],
        verify="python code/verify_package.py --self-test",
        files=files, n_files=len(files), total_bytes=sum(f["bytes"] for f in files))
    jdump(man, os.path.join(root, "MANIFEST.json"), default=str)
    with open(os.path.join(root, "CHECKSUMS.sha256"), "w") as fh:
        for f in files: fh.write(f"{f['sha256']}  {f['path']}\n")
    return man

def make_tarball(src_dir, arcname, out_path, mtime):
    """Deterministic tar.gz: sorted members, fixed mtime/uid/gid, no junk files."""
    junk = lambda n: os.path.basename(n) in ("__pycache__", ".DS_Store") or n.endswith((".pyc", ".pyo")) or os.path.basename(n).startswith("._")
    with open(out_path, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=mtime) as gz, tarfile.open(fileobj=gz, mode="w") as tar:
        def filt(ti):
            ti.mtime = mtime; ti.uid = ti.gid = 0; ti.uname = ti.gname = ""
            return ti
        for dp, dns, fns in os.walk(src_dir):
            dns[:] = sorted(x for x in dns if not junk(x))
            rel = os.path.relpath(dp, src_dir); arc = arcname if rel == "." else os.path.join(arcname, rel)
            tar.add(dp, arcname=arc, recursive=False, filter=filt)
            for fn in sorted(fns):
                if junk(fn): continue
                tar.add(os.path.join(dp, fn), arcname=os.path.join(arc, fn), recursive=False, filter=filt)

def tree_stats(root):
    n = 0; tot = 0; big = (0, "")
    for dp, _, fs in os.walk(root):
        for f in fs:
            p = os.path.join(dp, f); s = os.path.getsize(p); n += 1; tot += s
            if s > big[0]: big = (s, os.path.relpath(p, root))
    return n, tot, big

def pkg_slug(cfg, pkg):
    """Package name stem: per-package `slug` from config, else the legacy `pots-corpus-<tier>`."""
    return cfg["packages"][pkg].get("slug") or f"pots-corpus-{pkg}"

def rename_pass(text, cfg, version):
    """Apply the configured names to generated prose. Replaces: legacy package names, the legacy
    repo name, path-like `tierN/` references (to the package folder), and <owner>/<you> placeholders.
    Concept words ('Tier 1', 'tier1_findings.parquet', 'tier_source') are deliberately untouched —
    the tier is the extraction generation and stays as a data field."""
    P = cfg["packages"]
    for pkg in P:
        text = text.replace(f"pots-corpus-{pkg}-{version}", f"{pkg_slug(cfg, pkg)}-{version}")
        text = text.replace(f"pots-corpus-{pkg}", pkg_slug(cfg, pkg))
        text = re.sub(rf"(?<![A-Za-z0-9_/.-]){pkg}/", f"{pkg_slug(cfg, pkg)}/", text)
        slug = pkg_slug(cfg, pkg)
        text = re.sub(rf"^\| {pkg} \|", f"| {slug} |", text, flags=re.M)                       # per-package table rows
        text = re.sub(rf"(sparse-checkout (?:set|add) ){pkg}\b", rf"\g<1>{slug}", text)          # sparse-checkout examples
        text = re.sub(rf"(^|[`\s])cd {pkg}\b", rf"\g<1>cd {slug}", text, flags=re.M)             # cd into a package folder
        text = re.sub(rf"`{pkg}-{re.escape(version)}`", f"`{slug}-{version}`", text)              # git tag names in PUBLISHING
    repo = cfg.get("repo_name")
    if repo:
        text = text.replace("pots-corpus-github-repo", f"{repo}-github-repo").replace("pots-corpus", repo)
    owner = cfg.get("github_owner")
    if owner:
        text = text.replace("<owner>", owner).replace("<you>", owner)
        text = text.replace(f"Replace `{owner}` with the account hosting the repository. ", "")
    return text

def fill_template(text, values):
    out = text
    for k, v in values.items(): out = out.replace(f"@@{k}@@", str(v))
    left = sorted(set(re.findall(r"@@([A-Z0-9_]+)@@", out)))
    if left: raise KeyError(f"unfilled template placeholders: {left}")
    return out

# ----------------------------------------------------------------------------- main build
def build(args):
    cfg = json.load(open(args.config)); tdir = args.templates or cfg.get("templates_dir", "templates")
    prose = json.load(open(os.path.join(tdir, "prose.json")))
    version = args.version or cfg["version"]
    build_utc = args.build_utc or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    build_epoch = int(datetime.datetime.strptime(build_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())
    inputs = Inputs(cfg, args.inputs_dir)
    corpus_path = args.corpus or inputs.resolve(cfg["corpus"]["filename"], cfg["corpus"])
    corpus_vid = cfg["corpus"]["artifact"]
    OUT = args.out; os.makedirs(OUT, exist_ok=True)
    P = cfg["packages"]; SIB = list(P); creator = cfg["creator"]

    full = pq.read_table(corpus_path).to_pandas()
    schema_cols = pq.ParquetFile(corpus_path).schema_arrow.names
    sel, STATS, INVRES, RES, corpus = compute_all(full, cfg, inputs, schema_cols, build_utc, corpus_vid)
    docs = Docs(cfg, prose, STATS, RES, corpus, schema_cols, version)
    log = lambda *a: print(*a, file=sys.stderr)

    # ---- v1.1 documentation revision context (docs only; tier data untouched)
    if getattr(args, "docs_revision", None) in ("v1.1", "v1.2"):
        ec_pat = re.compile(r"&#x?[0-9a-fA-F]+;|&(amp|lt|gt|nbsp|quot);")
        ec = {}
        for pkg in SIB:
            q = full.loc[sel[pkg], "provenance_quote"].astype(str)
            ec[pkg] = int(q.str.contains(ec_pat).sum())
        retro = cond = None
        if args.annotations:
            rs = os.path.join(args.annotations, "retrofit_summary.json")
            if os.path.exists(rs): retro = json.load(open(rs))
            cp = os.path.join(args.annotations, "condition_annotations.parquet")
            if os.path.exists(cp):
                ca = pq.read_table(cp).to_pandas(); cond = {}
                for pkg in SIB:
                    s = ca[ca.package == pkg]
                    cond[pkg] = dict(papers=int(len(s)), rows=int(s.n_rows.sum()),
                                     **{k: int((s.condition_bucket_verdict == k).sum()) for k in ("in_bucket", "bucket_violation", "NEEDS_HUMAN")},
                                     mastocytosis=int((s.census_cat == "mastocytosis").sum()),
                                     mechanism_only=int((s.condition_population_kind == "mechanism_only").sum()),
                                     not_applicable=int((s.condition_population_kind == "not_applicable").sum()))
        UNDEF = ("IBD_gut", "orthostatic_hypotension", "other patient group")
        und = {}
        for pkg in SIB:
            sub = full.loc[sel[pkg] & full.condition.isin(UNDEF)]
            und[pkg] = {k: int(v) for k, v in sub.drop_duplicates("pmid").condition.value_counts().items()}
        docs.v11 = dict(entity_code_rows=ec, entity_code_rows_corpus=sum(ec.values()), retrofit=retro, condition_ann=cond, undefined_condition_values=und)
        log("[v1.1] entity-code rows per package:", ec, "| retrofit summary:", bool(retro), "| condition annotations:", bool(cond))
    # ---- v1.2 documentation revision context: identifier re-grounding counts, measured per package from the data
    if getattr(args, "docs_revision", None) == "v1.2":
        need = {"authority_verified_retrospective", "id_withheld_unverified_v1.2"}
        have = set(full.ontology_id_verification.dropna().astype(str).unique())
        if not (need & have):
            raise SystemExit("--docs-revision v1.2 requires a re-grounded corpus (no authority_verified_retrospective / id_withheld_unverified_v1.2 rows found)")
        if "unverified_model_emitted" in have:
            raise SystemExit("--docs-revision v1.2: corpus still carries unverified_model_emitted rows; re-ground before building")
        v12 = {}
        for pkg in SIB:
            s = full.loc[sel[pkg]]
            vc = s.ontology_id_verification.astype(str).value_counts()
            retro = s[s.ontology_id_verification == "authority_verified_retrospective"]
            replaced = int(retro.ontology_id_source.astype(str).str.startswith("v1.2 forward").sum())
            v12[pkg] = dict(
                retro_rows=int(len(retro)), replaced_rows=replaced, kept_rows=int(len(retro)) - replaced,
                withheld_rows=int(vc.get("id_withheld_unverified_v1.2", 0)), original_verified_rows=int(vc.get("authority_verified", 0)),
                no_id_rows=int(vc.get("no_id", 0)),
                match_modes={str(k): int(v) for k, v in retro[~retro.ontology_id_source.astype(str).str.startswith("v1.2 forward")].ontology_id_match_mode.astype(str).value_counts().items()},
                model_emitted_rows=int(s.ontology_id_v11_model_emitted.notna().sum()) if "ontology_id_v11_model_emitted" in s else None)
            m = v12[pkg]
            m["model_ids_denoting_entity_pct"] = round(100.0 * m["kept_rows"] / m["model_emitted_rows"], 1) if m["model_emitted_rows"] else None
        docs.v12 = v12
        log("[v1.2] identifier re-grounding per package:", {p: (v["kept_rows"], v["replaced_rows"], v["withheld_rows"]) for p, v in v12.items()})

    # ---- annotations discovery
    ann = {p: [] for p in SIB}
    if args.annotations:
        for fn in sorted(os.listdir(args.annotations)):
            fp = os.path.join(args.annotations, fn)
            if not os.path.isfile(fp) or fn.startswith("."): continue
            tiers_in_name = [p for p in SIB if re.search(rf"(^|[_\-.]){p}([_\-.]|$)", fn)]
            targets = tiers_in_name or SIB
            for p in targets:
                rows = None
                if fn.endswith(".parquet"):
                    rows = pq.ParquetFile(fp).metadata.num_rows
                elif fn.endswith(".csv"):
                    rows = sum(1 for _ in open(fp, "rb")) - 1
                desc = prose.get("annotation_descriptions", {}).get(re.sub(rf"_?{p}", "", fn), {})
                ann[p].append(dict(file=fn, src=fp, rows=rows, key=desc.get("key", "see file"), desc=desc.get("desc", "additive annotation table")))

    BUILT = {}; MANS = {}
    for pkg in SIB:
        meta = P[pkg]; st = STATS[pkg]
        root = os.path.join(OUT, f"{pkg_slug(cfg, pkg)}-{version}")
        shutil.rmtree(root, ignore_errors=True)
        for sub in ("data", "docs", "qa", "code", "deposit"): os.makedirs(os.path.join(root, sub), exist_ok=True)
        # ---- data (row order = harmonized corpus order filtered on the tier mask; never mutated)
        d = full[sel[pkg]].drop(columns=["wave", "chunk_id_i"])[schema_cols].reset_index(drop=True)
        assert list(d.columns) == schema_cols and len(d) == st["findings"]
        if args.dtype_fix:
            for c in cfg.get("dtype_fix_boolean_columns", []):
                d[c] = d[c].astype("boolean")
        d.to_parquet(os.path.join(root, "data", f"{pkg}_findings.parquet"), index=False, compression="zstd")
        gz_mtime = (cfg.get("gzip_mtime") or {}).get(pkg, build_epoch) if args.pin_gzip_mtime else build_epoch
        with open(os.path.join(root, "data", f"{pkg}_findings.csv.gz"), "wb") as raw:
            gz = gzip.GzipFile(filename=f"{pkg}_findings.csv", mode="wb", fileobj=raw, mtime=gz_mtime)
            tw = io.TextIOWrapper(gz, encoding="utf-8", newline="")
            d.to_csv(tw, index=False); tw.flush(); gz.close()
        for name in ("entity_vocabulary_crosstier.parquet", "chunk_order.parquet"):
            shutil.copyfile(inputs.resolve(name, cfg["shared_inputs"][name]), os.path.join(root, "data", name))
        # ---- docs
        readme = docs.readme(pkg, annotations=ann[pkg] or None)
        write(os.path.join(root, "docs", "README.md"), readme)
        write(os.path.join(root, "docs", "DATA_DICTIONARY.md"), docs.data_dictionary(pkg))
        write(os.path.join(root, "docs", "METHODS.md"), docs.methods(pkg))
        write(os.path.join(root, "docs", "LIMITATIONS.md"), docs.limitations(pkg))
        write(os.path.join(root, "docs", "WEBSITE_INTEGRATION.md"), docs.site_guide(pkg))
        shutil.copyfile(os.path.join(tdir, "COMPARABILITY.md"), os.path.join(root, "docs", "COMPARABILITY.md"))
        # ---- qa
        jdump(st, os.path.join(root, "qa", "package_stats.json"), default=str)
        jdump(INVRES[pkg], os.path.join(root, "qa", "invariants.json"), default=str)
        for name in ("comparability_report.json", "quote_census_all_tiers.json"):
            shutil.copyfile(inputs.resolve(name, cfg["shared_inputs"][name]), os.path.join(root, "qa", name))
        for name, spec in cfg["qa_extra"][pkg].items():
            shutil.copyfile(inputs.resolve(name, spec), os.path.join(root, "qa", name))
        # ---- code
        shutil.copyfile(os.path.join(tdir, "verify_package.py"), os.path.join(root, "code", "verify_package.py"))
        for name, spec in cfg["code_assets"].items():
            shutil.copyfile(inputs.resolve(name, spec), os.path.join(root, "code", name))
        pi = cfg["pipe"][pkg]
        for name, spec in [pi["pipeline"], pi["prompt"], pi["driver"]] + list(pi.get("extra", {}).items()):
            shutil.copyfile(inputs.resolve(name, spec), os.path.join(root, "code", name))
        if docs.v11:
            write(os.path.join(root, "code", "README.md"), docs.code_readme(pkg, os.listdir(os.path.join(root, "code"))))
        # ---- scope / figures
        for grp, sub in (("scope_extra", "scope"), ("figures", "figures")):
            for name, spec in cfg.get(grp, {}).get(pkg, {}).items():
                os.makedirs(os.path.join(root, sub), exist_ok=True)
                shutil.copyfile(inputs.resolve(name, spec), os.path.join(root, sub, name))
        # ---- annotations (ADDITIVE; tier data untouched)
        for a in ann[pkg]:
            os.makedirs(os.path.join(root, "annotations"), exist_ok=True)
            shutil.copyfile(a["src"], os.path.join(root, "annotations", a["file"]))
        # ---- root files
        shutil.copyfile(os.path.join(tdir, "LICENSE"), os.path.join(root, "LICENSE"))
        write(os.path.join(root, "README.md"), readme)
        write(os.path.join(root, "CITATION.cff"), docs.citation_cff(pkg))
        jdump(docs.zenodo_json(pkg), os.path.join(root, ".zenodo.json"), ensure_ascii=False)
        jdump(docs.osf_json(pkg), os.path.join(root, "deposit", "osf_metadata.json"), ensure_ascii=False)
        write(os.path.join(root, "deposit", "github_release_notes.md"), docs.gh_release(pkg))
        BUILT[pkg] = root
        log(f"[{pkg}] tree written -> {root}")

    # ---- naming pass over generated prose (before manifests so checksums are final)
    TEXT_EXT = (".md", ".cff", ".json", ".csv", ".txt")
    def rename_tree(root):
        n = 0
        for dp, dns, fns in os.walk(root):
            if os.path.basename(dp) in ("data", "code", "qa"): dns[:] = []; continue   # never touch data, pinned code, or QA evidence
            for fn in fns:
                if not fn.endswith(TEXT_EXT) and fn != ".zenodo.json": continue
                fp = os.path.join(dp, fn); t = open(fp, encoding="utf-8").read(); t2 = rename_pass(t, cfg, version)
                if t2 != t: open(fp, "w", encoding="utf-8").write(t2); n += 1
        return n
    for pkg, root in BUILT.items():
        # README.md at package root is a copy of docs/README.md; both get the pass
        log(f"[{pkg}] naming pass touched {rename_tree(root)} files")

    # ---- Zenodo form sheet per package (manual uploads do NOT read .zenodo.json; this is what to paste)
    for pkg, root in BUILT.items():
        z = json.load(open(os.path.join(root, ".zenodo.json")))
        L = [f"# Zenodo upload form — {z.get('title','')}", "",
             "Manual uploads do not read `.zenodo.json`; paste these values into the form. Upload the package",
             f"tarball `{pkg_slug(cfg, pkg)}-{version}.tar.gz` as the single file. Do not publish until every value below is in.", "",
             "| form field | value |", "|---|---|",
             f"| Resource type | Dataset |",
             f"| Title | {z.get('title','')} |",
             f"| Publication date | the day you click Publish (must equal `date-released` in CITATION.cff: {build_utc[:10]}) |",
             f"| Creators | {creator['name']} — ORCID {creator['orcid']} |",
             f"| Description | see block below |",
             f"| License | Creative Commons Attribution 4.0 International (CC-BY-4.0) |",
             f"| Version | {pkg}-{version} |",
             f"| Keywords | {', '.join(z.get('keywords', []))} |",
             f"| Related identifiers | {'; '.join(r['identifier'] + ' (' + r['relation'] + ')' for r in z.get('related_identifiers', []))} |",
             f"| Related identifiers (add) | https://github.com/{cfg.get('github_owner','<owner>')}/{cfg.get('repo_name','pots-corpus')} — isSupplementTo, Software |",
             "", "## Description (paste as-is)", "", z.get("description", ""), ""]
        write(os.path.join(root, "deposit", "ZENODO_FORM.md"), "\n".join(L))

    # ---- DOI backfill (before manifests so checksums are final)
    dois = dict(kv.split("=", 1) for kv in (args.backfill_doi or []))
    unknown = set(dois) - set(SIB)
    if unknown: raise SystemExit(f"--backfill-doi: unknown package(s) {sorted(unknown)}")
    def backfill(path):
        if not os.path.exists(path): return 0
        t = open(path).read(); t2 = t
        for p, doi in dois.items(): t2 = t2.replace(f"PLACEHOLDER-DOI-{p}", doi)
        if t2 != t: open(path, "w").write(t2)
        return int(t2 != t)
    if dois:
        n = 0
        for pkg, root in BUILT.items():
            for rel in ("CITATION.cff", ".zenodo.json", "README.md", "docs/README.md", "deposit/ZENODO_FORM.md", "deposit/osf_metadata.json", "deposit/github_release_notes.md"): n += backfill(os.path.join(root, rel))
        log(f"[doi] backfilled {sorted(dois)} in {n} package files")

    for pkg, root in BUILT.items():
        MANS[pkg] = build_manifest(pkg, root, cfg, STATS[pkg], schema_cols, version, P[pkg], creator)
        log(f"[{pkg}] manifest: {MANS[pkg]['n_files']} files, {MANS[pkg]['total_bytes']/1e6:.1f} MB")

    # ---- per-tier tarballs
    TARS = {}
    for pkg, root in BUILT.items():
        tp = os.path.join(OUT, f"{pkg_slug(cfg, pkg)}-{version}.tar.gz")
        make_tarball(root, os.path.basename(root), tp, build_epoch); TARS[pkg] = tp

    # ---- PACKAGE_INDEX.csv
    rows = []
    for pkg in SIB:
        st = STATS[pkg]; m = MANS[pkg]; ver = st["verifiability"]; g = st["grounding"]; dr = g["id_denotes_entity_rate"]
        rows.append(dict(package=f"{pkg_slug(cfg, pkg)}-{version}", tier=pkg, title=P[pkg]["title"], findings=st["findings"], papers=st["papers"],
                         chunks=st["n_chunks"], chunk_range=P[pkg]["chunk_range"], years=f"{st['year_range'][0]}-{st['year_range'][1]}",
                         id_provenance=m["quality"]["id_provenance"], id_coverage_pct=round(g["id_resolved_frac_all_rows"] * 100, 2),
                         id_coverage_basis="measured on this package", id_denotes_entity_rate=dr["value"],
                         id_denotes_entity_rate_basis=("measured on this package's tier" if dr["measured_per_package"]
                                                       else "inherited from Tier 2 pooled; no per-wave rate exists"),
                         quote_verifiable_pct=ver["verifiable_share_pct"], invariants=f"{m['quality']['invariants_passing']}/9",
                         enum_drift_rows=st["enum_drift"]["total_drifted_rows"], files=m["n_files"], mb=round(m["total_bytes"] / 1e6, 1)))
    idx = pd.DataFrame(rows); idx.to_csv(os.path.join(OUT, "PACKAGE_INDEX.csv"), index=False)

    # ---- repo tree
    REPO_NAME = cfg.get("repo_name", "pots-corpus")
    REPO = os.path.join(OUT, REPO_NAME); shutil.rmtree(REPO, ignore_errors=True); os.makedirs(REPO)
    for pkg in SIB: shutil.copytree(BUILT[pkg], os.path.join(REPO, pkg_slug(cfg, pkg)))
    shutil.copyfile(os.path.join(OUT, "PACKAGE_INDEX.csv"), os.path.join(REPO, "PACKAGE_INDEX.csv"))
    shutil.copyfile(os.path.join(tdir, "LICENSE"), os.path.join(REPO, "LICENSE"))
    shutil.copyfile(os.path.join(tdir, "COMPARABILITY.md"), os.path.join(REPO, "COMPARABILITY.md"))
    shutil.copyfile(os.path.join(tdir, "gitignore"), os.path.join(REPO, ".gitignore"))
    if os.path.exists(os.path.join(tdir, "REBUILD_REPORT.md")): shutil.copyfile(os.path.join(tdir, "REBUILD_REPORT.md"), os.path.join(REPO, "REBUILD_REPORT.md"))

    if dois:
        doi_rows = "\n".join(f"| `{pkg_slug(cfg, p)}-{version}` | https://doi.org/{dois[p]} |" for p in SIB if p in dois)
        DOI_STATUS = dict(
            DOI_STATUS_OVERVIEW="Each package's DOI is filled in below and in its `CITATION.cff`, `.zenodo.json` and `deposit/` files; the four sibling references in every package carry the real DOIs.\n\n| package | DOI |\n|---|---|\n" + doi_rows,
            DOI_STATUS_README="Each package has a `CITATION.cff` carrying its Zenodo DOI:\n\n| package | DOI |\n|---|---|\n" + doi_rows,
            DOI_STATUS_PUBLISHING="The five DOIs were reserved on Zenodo before this build and are filled in everywhere (a search for leftover DOI placeholders returns nothing):\n\n| package | DOI |\n|---|---|\n" + doi_rows + "\n\nThey resolve once each Zenodo record is published (step 1).")
    else:
        DOI_STATUS = dict(
            DOI_STATUS_OVERVIEW="**The DOI fields hold `PLACEHOLDER-DOI-<pkg>`** — reserve five Zenodo DOIs, then rebuild with `--backfill-doi` before publishing.",
            DOI_STATUS_README="Each package has a `CITATION.cff` with a `PLACEHOLDER-DOI-<pkg>` field, to be filled in by a rebuild once the Zenodo DOIs are reserved.",
            DOI_STATUS_PUBLISHING="The DOI fields hold `PLACEHOLDER-DOI-<pkg>`. Reserve five DOIs on Zenodo (New upload -> Get a DOI now, one per package), then rebuild with `--backfill-doi tier1=10.5281/zenodo.N ...` — never hand-edit, because checksums cover these files.")
    idk = lambda p: "authority-derived" if P[p]["authority_derived"] else "model-emitted"
    RETRO = (docs.v11 or {}).get("retrofit") or {}
    RPT = RETRO.get("per_tier", {})
    def retro_pct(p):
        if p in RPT: return f"{RPT[p]['verifiable_after']['pct']}%"
        return "100.0% (retained)" if STATS[p]["verifiability"]["verifiable_share_pct"] >= 99.9 else "—"
    if RETRO:
        cw = RETRO["corpus_wide_tiers1_3"]
        after_rows = cw["verifiable_after"] + sum(STATS[p]["verifiability"]["verifiable_rows"] for p in SIB if p not in RPT)
        AFTER = dict(CORPUS_VERIFIABLE_AFTER_ROWS=fmt(after_rows), CORPUS_VERIFIABLE_AFTER_PCT=f"{100*after_rows/corpus['findings']:.1f}",
                     RETRO_NO_SOURCE=fmt(cw["rows_no_source"]), RETRO_FT_TRACE_PCT=f"{cw['fulltext_depth_traceable_pct']}", RETRO_DOWNGRADED=fmt(cw["fulltext_rows_downgraded_to_abstract"]),
                     VERIF_COL_HEADER="quotes verifiable, shipped source | after re-fetch (v1.1)", VERIF_COL_ALIGN="---:|---:")
    else:
        AFTER = dict(CORPUS_VERIFIABLE_AFTER_ROWS="—", CORPUS_VERIFIABLE_AFTER_PCT="—", RETRO_NO_SOURCE="—", RETRO_FT_TRACE_PCT="—", RETRO_DOWNGRADED="—",
                     VERIF_COL_HEADER="quotes verifiable", VERIF_COL_ALIGN="---:")
    ra, rb = RES["tier2a"], RES["tier2b"]
    common = dict(
        VERSION=version, BUILD_UTC=build_utc, BUILD_DATE=build_utc[:10], CORPUS_VID=corpus_vid, **DOI_STATUS, **AFTER, GITHUB_OWNER=cfg.get("github_owner", "<owner>"), REPO_NAME=cfg.get("repo_name", "pots-corpus"),
        TOTAL_FINDINGS=fmt(corpus["findings"]), TOTAL_PAPERS=fmt(corpus["papers"]), TOTAL_CHUNKS=corpus["n_chunks"],
        CORPUS_VERIFIABLE_PCT=corpus["vpct"], CORPUS_UNVERIFIABLE_ROWS=fmt(corpus["findings"] - corpus["vsum"]),
        QUOTE_MEDIAN_CHARS=corpus["quote_median"], QUOTE_MAX_CHARS=fmt(corpus["quote_max"]), QUOTE_PCT_UNDER_400=corpus["quote_pct_under_400"],
        T2A_FINDINGS=fmt(STATS["tier2a"]["findings"]), T2A_PAPERS=fmt(STATS["tier2a"]["papers"]), T2A_CHUNKS=STATS["tier2a"]["n_chunks"],
        T2B_FINDINGS=fmt(STATS["tier2b"]["findings"]), T2B_PAPERS=fmt(STATS["tier2b"]["papers"]), T2B_CHUNKS=STATS["tier2b"]["n_chunks"],
        T2B_CHUNK_MIN=min(STATS["tier2b"]["chunk_ids"]), T2B_CHUNK_MAX=max(STATS["tier2b"]["chunk_ids"]),
        T3_FINDINGS=fmt(STATS["tier3"]["findings"]),
        T2A_ID_COVERAGE_PCT=f"{STATS['tier2a']['grounding']['id_resolved_frac_all_rows']*100:.2f}",
        T2B_ID_COVERAGE_PCT=f"{STATS['tier2b']['grounding']['id_resolved_frac_all_rows']*100:.2f}",
        T2A_STRING_AGREE=ra["string_agree_pct"], T2B_STRING_AGREE=rb["string_agree_pct"],
        T2_STRING_AGREE_DIFF=f"{abs(ra['string_agree_pct']-rb['string_agree_pct']):.2f}",
        SAMPLE_N_T1=P["tier1"]["adjudicated_sample_n"], SAMPLE_N_T2=P["tier2a"]["adjudicated_sample_n"],
        SAMPLE_N_T3=P["tier3"]["adjudicated_sample_n"], SAMPLE_N_T4=P["tier4"]["adjudicated_sample_n"],
        SITE_FINDINGS=fmt(cfg["site"]["cur_findings"]), SITE_T1_ROWS=fmt(cfg["site"]["t1_rows"]),
        SITE_AFTER_T2B=fmt(cfg["site"]["cur_findings"] + STATS["tier2b"]["findings"]),
    )
    # OVERVIEW.md
    ov_rows = "\n".join(
        f"| `{pkg_slug(cfg, p)}-{version}` | {fmt(STATS[p]['findings'])} | {fmt(STATS[p]['papers'])} | {STATS[p]['n_chunks']} | "
        f"{idk(p)} | {MANS[p]['quality']['id_denotes_entity_rate']} | {STATS[p]['verifiability']['verifiable_share_pct']}% | "
        f"{MANS[p]['quality']['invariants_passing']}/9 |" for p in SIB)
    write(os.path.join(REPO, "OVERVIEW.md"), fill_template(open(os.path.join(tdir, "OVERVIEW.md.tmpl")).read(), dict(common, OVERVIEW_TABLE_ROWS=ov_rows)))
    # root README.md — needs the sizes of the tarballs and trees
    pkg_rows = "\n".join(f"| [`{p}/`]({p}/) | {fmt(MANS[p]['contents']['findings'])} | {fmt(MANS[p]['contents']['papers'])} | "
                         f"{prose['repo_readme_topics'][p]} | {idk(p)} | {MANS[p]['quality']['quote_verifiable_pct']}% | {retro_pct(p)} |" for p in SIB)
    size_rows = "\n".join(f"| {p} | {du_h(os.path.getsize(TARS[p]))} MB | {du_h(tree_stats(BUILT[p])[1])} MB | {fmt(STATS[p]['findings'])} |" for p in SIB)
    n_files, tot, big = tree_stats(REPO)
    per_tier = [tree_stats(os.path.join(REPO, pkg_slug(cfg, p)))[0] for p in SIB]
    readme_vals = dict(common, PACKAGE_TABLE_ROWS=pkg_rows, SIZE_TABLE_ROWS=size_rows,
                       TIER4_ARCHIVE_MB=du_h(os.path.getsize(TARS["tier4"])),
                       REPO_MB_ROUNDED=round(tot / 1e6), REPO_MB=f"{tot/1e6:.1f}", REPO_N_FILES=n_files + 3,  # + README, PUBLISHING, CITATION written below
                       PER_TIER_FILE_COUNTS=" + ".join(str(x) for x in per_tier), ROOT_N_FILES=n_files + 3 - sum(per_tier),
                       LARGEST_FILE_MB=round(big[0] / 1048576))
    # the three root docs are counted in REPO_N_FILES/REPO_MB: add their sizes after rendering (two-pass)
    for _pass in range(2):
        write(os.path.join(REPO, "README.md"), fill_template(open(os.path.join(tdir, "README.md.tmpl")).read(), readme_vals))
        write(os.path.join(REPO, "PUBLISHING.md"), fill_template(open(os.path.join(tdir, "PUBLISHING.md.tmpl")).read(), readme_vals))
        write(os.path.join(REPO, "CITATION.cff"), fill_template(open(os.path.join(tdir, "CITATION.cff.tmpl")).read(), readme_vals))
        n_files, tot, big = tree_stats(REPO)
        readme_vals.update(REPO_N_FILES=n_files, REPO_MB=f"{tot/1e6:.1f}", REPO_MB_ROUNDED=round(tot / 1e6), ROOT_N_FILES=n_files - sum(per_tier))
    for rel in ("OVERVIEW.md", "PUBLISHING.md", "README.md", "CITATION.cff", "PACKAGE_INDEX.csv"):
        fp = os.path.join(REPO, rel); t = open(fp, encoding="utf-8").read(); t2 = rename_pass(t, cfg, version)
        if t2 != t: open(fp, "w", encoding="utf-8").write(t2)
    if dois:
        for rel in ("OVERVIEW.md", "PUBLISHING.md", "README.md", "CITATION.cff"): backfill(os.path.join(REPO, rel))
    make_tarball(REPO, REPO_NAME, os.path.join(OUT, f"{REPO_NAME}-github-repo.tar.gz"), build_epoch)
    log(f"[repo] {n_files} files, {tot/1e6:.1f} MB -> {OUT}/{REPO_NAME}-github-repo.tar.gz")
    summary = dict(build_utc=build_utc, version=version, corpus=corpus, packages={p: dict(findings=STATS[p]["findings"], papers=STATS[p]["papers"],
                   n_files=MANS[p]["n_files"], tarball_bytes=os.path.getsize(TARS[p])) for p in SIB}, repo_files=n_files, repo_bytes=tot,
                   dtype_fix=bool(args.dtype_fix), annotations={p: [a["file"] for a in ann[p]] for p in SIB}, dois=dois)
    jdump(summary, os.path.join(OUT, "BUILD_SUMMARY.json"))
    return summary

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # ---- GENERIC single-package mode (2026-09-30): corpus + config/<area>.yaml -> one package.
    # The legacy five-package POTS reproduction below is untouched (byte-for-byte v1 repro).
    ap.add_argument("--area-config", help="config/<area>.yaml|.json: build ONE package for a new area (packaging/generic_builder.py)")
    ap.add_argument("--roster", help="[generic] frozen roster parquet (becomes data/chunk_order.parquet; allotment for the conservation invariant)")
    ap.add_argument("--qa-dir", help="[generic] folder of QA outputs to ship (quote_census.json, screen_summary.json, draw_report.json, gate_log.json, ...)")
    ap.add_argument("--scope-dir", help="[generic] folder of screening report / flagged / excluded lists")
    ap.add_argument("--slug", help="[generic] package name stem (default <area_slug>-findings)")
    ap.add_argument("--dry-run-note", help="[generic] one sentence written at the top of the README, CITATION and .zenodo.json marking a non-release build")
    ap.add_argument("--corpus-vid", help="[generic] artifact version id of the corpus parquet, recorded in MANIFEST/package_stats")
    ap.add_argument("--code-extra", action="append", default=[], help="[generic] extra files to ship under code/ (repeatable)")
    ap.add_argument("--archives-dir", help="[generic] folder of <prefix>chunk_*_fulltext.json.gz retention archives -> data/archives/")
    ap.add_argument("--config", default="package_config.json")
    ap.add_argument("--corpus", help="harmonized corpus parquet (overrides config/inputs-dir)")
    ap.add_argument("--inputs-dir", help="directory holding the declared input files by name")
    ap.add_argument("--templates", help="templates dir (default: config templates_dir)")
    ap.add_argument("--out", default="build")
    ap.add_argument("--version", help="package version string (default: config version, e.g. v1)")
    ap.add_argument("--build-utc", help="pin the build timestamp, e.g. 2026-09-02T11:10:50Z (reproducible builds)")
    ap.add_argument("--pin-gzip-mtime", action="store_true", help="use config gzip_mtime per tier for csv.gz headers (v1 reproduction); default = build epoch")
    ap.add_argument("--annotations", help="directory of ADDITIVE annotation tables; files carrying a tier name go to that package, others to every package")
    ap.add_argument("--backfill-doi", action="append", metavar="PKG=DOI", help="replace PLACEHOLDER-DOI-PKG with DOI (repeatable)")
    ap.add_argument("--docs-revision", default=None, choices=["v1.1", "v1.2"], help="documentation revision: v1.1 adds the condition-bucket, entity-code, retrofit and code/README sections (docs only; data untouched); v1.2 = v1.1 docs + the identifier re-grounding sections (requires a corpus whose model-emitted ids have been benchmarked: ontology_id_verification in {authority_verified_retrospective, id_withheld_unverified_v1.2})")
    ap.add_argument("--dtype-fix", action="store_true", help="cast is_covid_associated, query_contamination, screen_verdict_uncertain to pandas nullable 'boolean' (default OFF so v1 reproduces)")
    ap.add_argument("--export-inputs", metavar="DIR", help="only: copy every declared input out of the artifact store into DIR (needs a `host` object)")
    args = ap.parse_args(argv)
    if args.area_config:
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "packaging"))
        from generic_builder import build_generic
        if not (args.corpus and args.roster):
            raise SystemExit("--area-config needs --corpus and --roster")
        res = build_generic(args.area_config, args.corpus, args.roster, args.out, args.version or "v0", qa_dir=args.qa_dir,
                            scope_dir=args.scope_dir, slug=args.slug, dry_run_note=args.dry_run_note, build_utc=args.build_utc,
                            corpus_vid=args.corpus_vid, code_extra=args.code_extra, archives_dir=args.archives_dir, log=lambda *a: print(*a, file=sys.stderr))
        print(json.dumps(dict(package=res["manifest"]["package"], tarball=res["tarball"], n_files=res["manifest"]["n_files"],
                              findings=res["stats"]["findings"], papers=res["stats"]["papers"], invariants_all_ok=res["invariants"]["all_ok"]), indent=1))
        return
    if args.export_inputs:
        cfg = json.load(open(args.config)); print("\n".join(export_inputs(cfg, args.export_inputs))); return
    s = build(args)
    print(json.dumps({k: v for k, v in s.items() if k != "corpus"}, indent=1))

if __name__ == "__main__":
    main()
