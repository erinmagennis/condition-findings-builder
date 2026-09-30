"""Stage 5 — resolve identifiers from NAMES over a merged findings table (resolver.py + cache).

Added 2026-09-30 (EDS dry run). RUNBOOK stage 5 names `pipeline/resolver.py` and
`resolution_cache.py` but the repository had no entry point that applies them to a corpus; the
worker's in-pipeline grounding (`ground_findings_df`) routes only chemical/metabolite -> ChEBI,
genes -> MyGene and phenotype_terms -> HPO with the BARE `authority_lookup_ols_hp` tag (the
Tier-3 provenance-claim defect that invariant 9 exists to catch). This stage:

  1. resolves every distinct (entity, entity_kind) pair through resolver.resolve_entity
     (OLS4 hp/chebi/cl/uberon/ncbitaxon, UniProt for gene/protein; exact label or
     authority-asserted synonym; minting ontology recorded), checkpointing the cache;
  2. re-resolves phenotype_terms through resolver.resolve_hpo_terms so `hpo_id_source` carries
     the truthful `_via_<mint>` shape wherever HPO returned an imported term;
  3. writes the Tier-4 column set: entity_ontology_id_resolved / entity_ontology_label /
     ontology_id_source / entity_ontology_id_minting_ontology, hpo_ids_verified /
     hpo_labels_verified / hpo_id_source / hpo_id_minting_ontology / hpo_id_count;
  4. keeps the worker's values under `*_worker` columns and reports agreement — same concept,
     narrower authority coverage; nothing is silently overwritten without a recorded diff;
  5. reports resolution rate BY AUTHORITY with numerator, denominator and basis. These are
     COVERAGE numbers (`id_resolved_frac`); `id_denotes_entity_rate` needs an adjudicated
     correspondence sample and is reported as NOT MEASURED until one exists.

    import resolve_stage; df2, report = resolve_stage.run(df, cache_path="resolution_cache.parquet")
"""
import json, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import resolver  # noqa: E402


def _split_terms(s):
    if not isinstance(s, str) or not s.strip():
        return []
    return [t.strip() for t in s.split(";") if t.strip()]


def run(df, cache_path="resolution_cache.parquet", log=print, checkpoint_every=100):
    import pandas as pd
    t0 = time.time(); calls_before = len(resolver._CACHE)
    cache, cstats = resolver.load_cache_v4(cache_path)
    resolver._CACHE.update(cache)
    # ---- 1. entities
    pairs = sorted({(n.strip(), k) for n, k in zip(df.entity, df.entity_kind)
                    if isinstance(n, str) and n.strip() and isinstance(k, str)})
    routed = [(n, k) for n, k in pairs if k == "gene/protein" or k in resolver.ROUTES]
    log(f"[resolve] {len(pairs)} distinct (entity, kind) pairs; {len(routed)} routable; cache loaded {cstats['loaded']}")
    n_http = 0
    for i, (n, k) in enumerate(routed):
        key = (n.lower(), k)
        if key not in resolver._CACHE:
            resolver.resolve_entity(n, k); n_http += 1
        if (i + 1) % checkpoint_every == 0:
            resolver.save_cache_v4(resolver._CACHE, cache_path)
    resolver.save_cache_v4(resolver._CACHE, cache_path)
    out = df.copy()
    for c in ("entity_ontology_id_resolved", "entity_ontology_label", "ontology_id_source"):
        if c in out.columns:
            out[c + "_worker"] = out[c]
    out = resolver.apply_to_df(out, resolver._CACHE, "entity", "entity_kind", id_col="entity_ontology_id_resolved",
                               label_col="entity_ontology_label", src_col="ontology_id_source",
                               mint_col="entity_ontology_id_minting_ontology")
    # ---- 2. phenotype terms -> HPO with truthful provenance
    terms = sorted({t for s in out.phenotype_terms for t in _split_terms(s)}) if "phenotype_terms" in out.columns else []
    log(f"[resolve] {len(terms)} distinct phenotype terms")
    # HPO term lookups are memoised in the SAME persistent cache under a reserved kind, so a
    # re-run (or the next tier) never re-queries a term (first EDS pass re-queried 371 terms).
    hpo_map = {}
    for i, t in enumerate(terms):
        key = (t.lower(), "__hpo_term__")
        if key not in resolver._CACHE:
            i_, l_, path, mint = resolver._ols(t, "hp"); n_http += 1
            resolver._CACHE[key] = ((i_, l_, path, mint) if i_ else (None, None, "unresolved_nulled", None))
            if (i + 1) % checkpoint_every == 0:
                resolver.save_cache_v4(resolver._CACHE, cache_path)
        hit = resolver._CACHE[key]
        hpo_map[t] = (hit[0], hit[1], (hit[2] if hit[0] and hit[2] in ("exact", "synonym") else
                                       ("synonym" if hit[0] and "synonym" in str(hit[2]) else ("exact" if hit[0] else None))), hit[3])
        if (i + 1) % 50 == 0: log(f"[resolve] hpo {i+1}/{len(terms)}")
    resolver.save_cache_v4(resolver._CACHE, cache_path)
    for c in ("hpo_ids_verified", "hpo_labels_verified", "hpo_id_source", "hpo_id_count"):
        if c in out.columns:
            out[c + "_worker"] = out[c]
    ids_c, labs_c, src_c, mint_c, cnt_c = [], [], [], [], []
    for s in out.phenotype_terms:
        ts = _split_terms(s)
        ids, labels, mints, paths = [], [], [], []
        for t in ts:
            i_, l_, path, mint = hpo_map.get(t, (None, None, None, None))
            if not i_: continue
            ids.append(i_); labels.append(l_ or t); mints.append(mint or ""); paths.append(path)
        if not ids:
            ids_c.append(None); labs_c.append(None); src_c.append("unresolved_nulled"); mint_c.append(None); cnt_c.append(0); continue
        path = "synonym" if "synonym" in paths else "exact"
        foreign = sorted({m for m in mints if m and m != "hp"})
        tag = f"authority_lookup_ols_hp_via_{'+'.join(foreign)}_{path}" if foreign else f"authority_lookup_ols_hp_{path}"
        ids_c.append(";".join(ids)); labs_c.append(";".join(labels)); src_c.append(tag); mint_c.append(";".join(mints)); cnt_c.append(len(ids))
    out["hpo_ids_verified"], out["hpo_labels_verified"], out["hpo_id_source"] = ids_c, labs_c, src_c
    out["hpo_id_minting_ontology"], out["hpo_id_count"] = mint_c, cnt_c
    # ---- 5. report, by authority, with bases
    def rate(num, den, basis):
        return {"numerator": int(num), "denominator": int(den), "pct": (round(100.0 * num / den, 2) if den else None), "basis": basis}
    by_auth = {}
    for kind in sorted({k for _, k in pairs}):
        sub = out[out.entity_kind == kind]
        auth = "uniprot" if kind == "gene/protein" else (resolver.ROUTES.get(kind, (None, None))[1] or "none (unrouted by design)")
        by_auth[kind] = {"authority": auth,
                         "rows": rate(sub.entity_ontology_id_resolved.notna().sum(), len(sub), f"rows with entity_kind={kind} whose entity name resolved"),
                         "distinct_names": rate(sum(1 for n, k in pairs if k == kind and resolver._CACHE.get((n.lower(), k), (None,))[0]),
                                                sum(1 for _, k in pairs if k == kind), "distinct entity names of this kind that resolved")}
    agree = None
    if "entity_ontology_id_resolved_worker" in out.columns:
        both = out.entity_ontology_id_resolved_worker.notna() & out.entity_ontology_id_resolved.notna()
        agree = {"rows_both_resolved": int(both.sum()),
                 "rows_identical_id": int((out.entity_ontology_id_resolved_worker[both] == out.entity_ontology_id_resolved[both]).sum()),
                 "worker_only": int((out.entity_ontology_id_resolved_worker.notna() & out.entity_ontology_id_resolved.isna()).sum()),
                 "stage_only": int((out.entity_ontology_id_resolved_worker.isna() & out.entity_ontology_id_resolved.notna()).sum())}
    hpo_rows = out.hpo_ids_verified.notna()
    report = {
        "wall_s": round(time.time() - t0, 1), "distinct_pairs": len(pairs), "routable_pairs": len(routed),
        "authority_http_lookups_new": n_http, "cache_entries_after": len(resolver._CACHE), "cache_loaded": cstats,
        "entity_rows": rate(out.entity.notna().sum(), len(out), "rows carrying a non-null entity"),
        "entity_id_rows": rate(out.entity_ontology_id_resolved.notna().sum(), len(out), "ALL rows; coverage not correctness"),
        "entity_id_rows_of_entity_rows": rate(out.entity_ontology_id_resolved.notna().sum(), out.entity.notna().sum(), "rows with an entity name"),
        "by_entity_kind": by_auth,
        "source_tags": {str(k): int(v) for k, v in out.ontology_id_source.value_counts().items()},
        "minting_ontologies": {str(k): int(v) for k, v in out.entity_ontology_id_minting_ontology.value_counts(dropna=True).items()},
        "hpo": {"rows_with_phenotype_terms": rate(out.phenotype_terms.notna().sum(), len(out), "rows with >=1 phenotype term name"),
                "rows_with_verified_hpo": rate(hpo_rows.sum(), out.phenotype_terms.notna().sum(), "of rows with phenotype terms"),
                "distinct_terms": rate(sum(1 for t in terms if hpo_map[t][0]), len(terms), "distinct phenotype term names that resolved at OLS4 hp (exact label or authority synonym)"),
                "rows_multi_id": int((out.hpo_id_count > 1).sum()),
                "source_tags": {str(k): int(v) for k, v in out.hpo_id_source.value_counts().items()},
                "rows_with_imported_term": int(out.hpo_id_source.astype(str).str.contains("_via_").sum())},
        "gene": {"rows_with_gene_marker": int(out.gene_marker.notna().sum()) if "gene_marker" in out.columns else None,
                 "rows_hgnc": rate(out.hgnc_id.notna().sum(), out.gene_marker.notna().sum(), "of rows carrying a gene_marker; MyGene symbol lookup in the worker") if "hgnc_id" in out.columns else None},
        "worker_vs_stage_agreement": agree,
        "id_denotes_entity_rate": {"value": None, "basis": "NOT MEASURED: needs an adjudicated correspondence sample (RUNBOOK stage 5 rule 1)"},
        "resolver_version": resolver.RESOLVER_VERSION,
    }
    return out, report
