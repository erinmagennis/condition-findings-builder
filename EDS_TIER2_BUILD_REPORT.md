# EDS findings corpus — Tier 2 (pre-2019) build report

Build date 2026-10-21 (UTC). Companion to `EDS_BUILD_REPORT.md` (Tier 1, 2019–2026). Every number below is read from a saved QA file or recomputed from the shipped table; where a figure comes from a sub-agent's report rather than from a file I can re-derive, I say so.

## 0. Deviations from the approved plan (read these first)

1. **Package shape.** The plan called for a single combined `eds-findings-v0.2` package. The builder/verifier contract (`packaging/generic_builder.py`, `pipeline/verify_package.py`) is one `tier_source` per package, and re-opening that contract would have put a code change between the verifier and the shipped data. What ships instead: the unchanged Tier-1 package `eds-findings-v0`, a new **`eds-findings-tier2-v0`** package, a combined convenience table `eds_findings_all_tiers.parquet` (Tier-1 resolved + Tier-2 resolved, `tier` column), and `paper_registry_v2.parquet` spanning both tiers and the POTS corpus. A combined package can be built later by extending the contract.
2. **Pre-2019 window reduced to the extractable universe.** 5,362 pre-2019 papers were found; 1,503 (28%) have neither an abstract nor full text and were recorded as `not_extractable` with a basis rather than extracted. Extractability is strongly decade-dependent (see §2); the corpus therefore under-represents the literature before 1980 and nearly excludes it before 1970.
3. **Second screening pass added.** Pass-1 two-arm screening left 1,188 papers `uncertain`. A pass-2 "population check" (does the paper study a listed population? verbatim spans required) was run on them, and the exclusion rule was fixed as: exclude only where pass-1 LLM `off_topic` AND pass-2 "no listed population", or where both pass-1 arms agree. This is a model judging a model's call; a 100-paper human sample of pass-2 exclusions is still owed (§7).
4. **Efficiency overrides (user instruction "make sure everything runs efficiently").** 5 concurrent workers at 1.5 s fetch pace (aggregate request rate unchanged), whole-chunk units, no retry on structural under-yield (`retry_under_yield: false`), waves of 10 units, one wave per controller cell, dispatch deadline per session. All are pinned in `config/eds_tier2.yaml` and the config validator warns on them at load.
5. **Gate logs incomplete.** 45 of 106 unit gate records survived in the workspace; the rest were lost to workspace sweeps between sessions and to controller interrupts. Per-unit totals live in the worker frames and aggregate totals in `qa/build_state.json`; `qa/gate_log_coverage_note.json` states exactly which are missing.
6. **Identifier resolution was sharded.** The single-process resolver ran at ~0.8 lookups/s (≈3 h projected); it was stopped after ~300 lookups and replaced with 4 shard processes (`scripts/resolve_shard.py`), each writing its own cache file, merged before `resolve_stage.run`. Same authorities, same cache format; 8,373 lookups in ~40 min, 0 errors.

## 1. Headline numbers (package `eds-findings-tier2-v0`)

| quantity | value | source |
|---|---|---|
| findings | **15,860** | `qa/package_stats.json` |
| papers with ≥1 finding | **2,605** of 2,734 rostered (95.3%) | package_stats / roster v2 |
| year range | 1946–2018 | package_stats |
| chunks | 102 (ids 30, 36–37, 39–137; 31–35 empty after screening) | package_stats |
| findings per paper | median 5, mean 6.09 | recomputed from the table |
| condition rows | EDS_other_type 6,691 · hypermobility_unspecified 2,619 · general 2,081 · EDS_unspecified_or_mixed 2,060 · hEDS 1,055 · multiple 881 · HSD 473 | recomputed |
| `eds_type` on EDS_other_type rows (top) | vascular 2,726 · other named type 848 · classical 831 · kyphoscoliotic 620 · dermatosparaxis 243 · classical-like 224 · not specified 210 · arthrochalasia 199 | recomputed |
| evidence_tier | case-report 4,074 · case-series 2,965 · review/expert-opinion 2,510 · cross-sectional 2,159 · mechanistic/in-vitro 1,771 · case-control 1,237 · cohort 1,030 · RCT 74 · meta-analysis 38 · *systematic-review/meta-analysis 2 (outside enum, kept and flagged)* | recomputed |
| rows flagged `screen_verdict_uncertain` | 1,527 (from 295 of the 340 uncertain-flagged papers) | recomputed |
| verifier | **PASS** — 19/19 headline numbers re-derived, 9/9 invariants ok (8 core + authority_prefix_match_v4), 6/6 planted regressions caught | `code/verify_package.py --self-test` on the extracted tarball |
| checksums | 162/162 OK (`shasum -a 256 -c CHECKSUMS.sha256`) | measured on the extracted tarball |
| shipped tarball | 13,710,134 bytes gz; 162 files, 17,231,040 bytes unpacked | measured |
| tarball sha256 | `22bf36b034078343ae56d77cc59e7924103c4056c127c4bb2779db716bbf2115` | measured |

Combined table `eds_findings_all_tiers.parquet`: 22,961 rows (7,101 Tier 1 + 15,860 Tier 2), 3,284 papers, **0 PMIDs shared between tiers**, identical column set across tiers.

## 2. From universe to roster (scope)

| stage | papers | note |
|---|---|---|
| pre-2019 universe (Europe PMC, same clause set as Tier 1, year < 2019) | 5,362 | `pre2019_universe.parquet` |
| title-only → `not_extractable` | −1,503 | no abstract, no full text; listed in `scope/not_extractable_title_only.csv` |
| roster v1 (frozen, screened) | 3,859 | `eds_tier2_roster_v1.parquet` |
| pass-1 screen (two arms, verbatim spans) | in_scope 2,394 · uncertain 1,188 · off_topic 277 | `qa/screen_summary.json` `final`; 2 model refusals routed to uncertain, never judged |
| pass-2 population check on the 1,188 uncertain | studies a listed population 113 · does not 1,073 · error 2 | `qa/screen_summary.json` `pass2` |
| excluded (v2 rule) | −1,125 | 848 "pass-1 LLM off_topic + pass-2 no population", 277 "both pass-1 arms agree"; classes: topical drift 1,012, wrong abbreviation sense 113 (`scope/excluded_pmids_v2.csv`) |
| **roster v2 (extracted)** | **2,734** | 275 full-text expected, 2,459 abstract-only; 340 carry the uncertain flag |

Extractability by decade (universe → share with abstract or full text): 1930s 0% (8) · 1940s 3% (39) · 1950s 0% (125) · 1960s 1% (245) · 1970s 31% (395) · 1980s 65% (607) · 1990s 80% (881) · 2000s 83% (1,179) · 2010–2018 88% (1,883). The pre-1970 literature is effectively absent from the corpus (4 papers), not because it was screened out but because its text is not available through the sources used.

## 3. Extraction

- Model for every paper-reading stage (screen, population check, extraction): `claude-sonnet-5`, pinned in the config.
- 106 unit files for 102 chunks: chunk 136 (pilot) was split into two parts, and chunks 118, 121 and 126 have a second file from a post-stall re-dispatch; where a unit filename recurs the latest file is the one used (`qa/unit_files_used.csv`). Every unit: 0 blocked papers, 0 guard violations.
- Yield (papers with ≥1 finding / rostered), from roster v2 joined to the table: 1940s 1/1 · 1960s 3/3 · 1970s 79/84 (0.94) · 1980s 289/305 (0.948) · 1990s 476/493 (0.966) · 2000s 629/661 (0.952) · 2010s 1,128/1,187 (0.95). Full-text-expected 272/275 (0.989); abstract-only 2,333/2,459 (0.949); uncertain-flagged 295/340 (0.868) vs in-scope 2,310/2,394 (0.965). The 129 rostered papers without findings are recorded in the registry as `dispatched_no_yield` (not re-tried, by the no-retry rule).
- Output-token spend: waves 16,066,768 + pilot 634,575 = **16.70 M** measured, against a projection of ~18.0 M (cost model v2: 21,127 per full-text paper, 4,951 per abstract paper; `scope/EDS_TIER2_COST_PROJECTION.md`). The 2017–2018 chunks ran 10–20% over model because of truncation-guard continuations on long abstracts.
- Stalls. The ledger records stall stops in waves 2, 4, 6 (twice), 7 (three times) and 9. Root cause as far as it can be measured: the stalled workers never produced an assistant turn — they did not start — about 50 minutes into each burst, recovering after long idle gaps. This is consistent with the account usage cap rather than with the pipeline. No duplicate extraction occurred in any event (0 duplicate rows, latest-per-filename rule); the loss was wall time plus unmeasured partial work of ≤5 units per event. Pipeline responses: system-stall breaker, first-turn check (480 s), dispatch deadline, no-retry-on-under-yield.

## 4. QA (well-formedness; see §7 for what this does not measure)

| check | result | file |
|---|---|---|
| store inventory | 0 pre-cutoff files, 0 duplicate unit files, 106 findings + 106 archive files, all part sets complete | `qa/unit_files_used.csv`, `qa/archive_files_used.csv` |
| assembly | 0 PMIDs outside roster v2, 0 wrong chunk ids, 0 rows without a quote, 0 exact duplicates, `tier == 2` everywhere | `qa/assembly_check.json` |
| quote census (every row, not a sample) | **15,809 / 15,859 traceable = 99.68%** (abstract-only 11,487/11,507 = 99.83%; full text 99.31%); 1 row had no retained source text; negative controls 0/400 and 0/400 false positives | `qa/quote_census.json` |
| invariants (resolved table) | 8/8 pass: schema_completeness, conservation (against roster allotment), monotonicity, no_model_emitted_ids, id_provenance, enum_drift, duplicates, hpo_multivalue | `qa/invariants.json` |
| authority prefix match v4 | 0 mismatched rows / 14,441 ids checked across `hpo_ids_verified`, `entity_ontology_id_resolved`; 0 minting-column disagreements | `qa/authority_prefix_check.json` |
| registry v2 | 22,805 rows; EDS: extracted 3,284 · referenced 1,718 · excluded_screen 1,694 · not_extractable 1,631 · dispatched_no_yield 216; POTS: extracted 11,519 · dispatched_no_yield 2,743; 12/12 invariants | `qa/paper_registry_v2.parquet`, `qa/registry_v2_report.json` |

Identifier resolution (`qa/resolve_report.json`; coverage, not correctness): 11,236 rows carry an entity (70.8%); 3,134 of those resolved to an authority id (27.9%; Tier 1: 23.1%). By kind — anatomical structure 49.7% of rows (UBERON), cell type 52.7% (CL), chemical 58.0% (ChEBI), gene/protein 15.6% (UniProt; Tier 1: 13.2%), physiological measure 21.8% (HP), pathogen 9/13, `other` unrouted by design. Phenotype terms: 12,064 rows carry terms; 7,317 (60.7%) have ≥1 verified HPO id (exact 4,987, synonym 2,296, via MPATH/PATO/UBERON 34); 1,381 of 7,092 distinct term strings resolved. Gene/protein resolution is low in both tiers because the names are mostly descriptive ("type III procollagen", "lysyl hydroxylase") rather than symbols; the route is exact-symbol UniProt and does not guess.

## 5. Shared-paper accounting

Tier 2 is pre-2019 by construction and the POTS corpus's shared-paper window begins later, so no Tier-2 paper is referenced from `pots-findings`; the 1,718 `referenced` rows in the registry are Tier-1-window papers already extracted under POTS. Nothing was re-extracted. The registry key is (pmid, corpus); `inv_key_unique` and `inv_counts_sum` pass.

## 6. Artifacts

| file | version id |
|---|---|
| eds-findings-tier2-v0.tar.gz | bd6ed985-8b4d-4743-a532-f072162c0a4c |
| eds_t2_findings_resolved.parquet | 8b90e50f-122c-4610-996f-43ac8cf4b756 |
| eds_t2_findings_merged_raw.parquet | 5fb6131c-7ac7-4b18-98ed-8e8dad14bf9b |
| eds_findings_all_tiers.parquet | 63eddb23-f4cd-498e-a28e-03e31a83ba96 |
| paper_registry_v2.parquet | fb715249-0c32-4996-9f35-6d17a996480a |
| eds_tier2_roster_v2.parquet | b0295118-bfd3-439e-8fa3-accdfab4d368 |
| resolution_cache.parquet (14,385 entries, both tiers) | 7d390f03-b885-4534-ae26-80a975b0cb10 |
| build_state_t2.json (ledger) | 5d248e7a-7ae8-47b6-8bb9-01c28a4953c5 |
| condition-findings-builder.tar.gz (method repo v20; tests 74/74 both configs, registry 42/42, symmetry 12/12) | e13bf5a6-105a-4db6-ad2e-2c9095b0a5e8 |

## 7. Known gaps and what this build does not claim

- **Scientific fidelity is unmeasured.** Every QA figure above is well-formedness: quotes trace to source, ids come from authorities, counts conserve. Whether a finding faithfully states what the paper found has not been human-adjudicated for either tier. A marker shortlist should not be read off this table before adjudication.
- **Pass-2 exclusions are model-on-model.** 848 papers were excluded on a pass-1 LLM call confirmed by a pass-2 LLM call. The planned 100-paper human sample across both tiers has not been drawn.
- **Pre-1980 coverage is thin** for availability reasons (§2); 1,503 title-only papers are listed, not extracted.
- **`eds_type` leak:** 1,038 rows outside `EDS_other_type` carry an `eds_type` value (EDS_unspecified_or_mixed 832, multiple 153, hEDS 53). The prompt scopes `eds_type` to EDS_other_type; the values were kept as emitted and should be read as the worker's subtype note, not as a condition assignment.
- **2 rows** carry `evidence_tier = systematic-review/meta-analysis`, outside the enum; kept and declared (enum_drift invariant reports them).
- **Gate logs** for 61 of 106 units are absent from the package (§0.5).
- **Screen instrument alarm** is `True` in `qa/screen_summary.json`; on anchor-built rosters the alarm is degenerate (the rule arm anchors every paper by construction) and should not be read as a screening failure.
- **All-or-nothing worker save**: a worker that stalled after partial extraction saved nothing; its re-run paid the full unit cost. Measured only as wall time and tokens, not as findings.
- **Population mapping between the POTS and EDS condition enums, and the connections layer (`CONNECTIONS_DESIGN.md`), are not started.**

## 8. Reproduction

Unpack the repo tarball, then (paths as used in this build):

```
python build_packages.py --area-config config/eds_tier2.yaml \
  --corpus eds_t2_findings_resolved.parquet --roster eds_tier2_roster_v2.parquet \
  --qa-dir qa_t2 --scope-dir scope_t2 --archives-dir t2archives \
  --out build_t2 --version v0 --slug eds-findings-tier2 \
  --corpus-vid 8b90e50f-122c-4610-996f-43ac8cf4b756 --build-utc <iso> \
  --code-extra pipeline/registry.py --code-extra REGISTRY.md --dry-run-note "..."
tar -xzf build_t2/eds-findings-tier2-v0.tar.gz -C /tmp && cd /tmp/eds-findings-tier2-v0
python code/verify_package.py --self-test && shasum -a 256 -c CHECKSUMS.sha256
```
