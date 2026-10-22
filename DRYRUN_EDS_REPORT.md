# DRYRUN_EDS_REPORT — condition-findings-builder on a second condition (EDS/HSD)

Date 2026-09-30. Test area: Ehlers-Danlos syndromes (all 2017 types) + hypermobility spectrum disorder,
2019–2026, Europe PMC. Throwaway corpus; not a deliverable. Purpose: find everything still POTS-specific
in **code rather than config**, fix it in the method repository, and measure the run.

## 0. Deviations (read first)

1. **Pinned parameters deviated by design of the test**: `papers_per_chunk: 25` (pinned 50) and
   `n_parts_default: 1` (pinned 2), so the run was 3 chunks × 25 papers as whole units. The validator
   warned on both at every load, as designed. **This deviation is what exposed the unit-sizing defect
   (§2.3)**: chunk 0 (25/25 open-access, full-text-rich by construction) as one unit exceeded
   `WORKER_MAX_S=3000` s. Chunk 0 was then re-run as **3 sized parts** (8–9 papers each) while chunks 1–2
   ran whole — the tier therefore mixes whole and split units, which the RUNBOOK says never to do in
   a release tier; acceptable here because nothing is compared across tiers.
2. **Chunk 0 attempt 1 (~50 min of extraction) produced nothing** — the worker saves all-or-nothing;
   its output tokens were not captured (upper bound: the 3 parts' 532,958 tokens). Attempts 2 and 3 were
   spawned by the pinned driver as identical whole units, **never executed a step** (no assistant turn in
   their frames), and were stopped by hand after the driver cell died (§2.3, §4).
3. **The stage-1 roster was drawn under an exclude-mode contamination filter and then re-sampled**
   offline from the saved universe under flag mode (§2.5). The 75 papers are a deterministic
   stratified sample (seed 11) of the flag-mode universe (2312 PMIDs); the two samples differ.
4. **The two-arm screen excluded nothing**: 13 LLM off-topic verdicts met a rule-arm anchor and
   became `uncertain` under the pinned `screen_reconcile` rule (adjudicate; extract and flag) — they are
   listed in `scope/screen_eds_llm_offtopic_flagged.csv`, extracted, and carry
   `screen_verdict_uncertain=true` (131 rows / 14 yielding papers). The instrument alarm fired and
   was audited (§3.3), not silenced.
5. **Environment**: workers ran in the `python` env (pandas 2.3.3), not `litmap` (pandas 3.0.3) that
   the POTS build used — a config choice (`tier.env_name`), recorded because pandas 3 was not
   exercised. The screen used the pinned extraction model id (`claude-sonnet-5`) for the LLM arm.
6. **Stage-6 gate log for run 1 was reconstructed** from the driver's stdout after run 2 clobbered
   `gate_log_parts.json` (§2.7); values are verbatim from that stdout and marked `_source` in `qa/gate_log.json`.
   The stage-1 draw statistics likewise come from the fetch run's stdout (`qa/draw_report.json: fetch_run`).
7. **Not run**: stage-2 cost fit on own chunks before dispatch (the POTS fitted model was used and
   under-projected by 1.49×, §3.6); stages 7/8 (single tier); stages 10/11 (no derived layer; no human
   fidelity scoring — see the package LIMITATIONS). `id_denotes_entity_rate` is **not measured**
   (needs an adjudicated correspondence sample); every identifier number below is coverage.
8. **Rules kept**: pacing 3 workers × 0.9 s never raised; no stored quote edited; identifiers only
   from authorities; nothing published; the `pots-findings` tarball read only.

## 1. What was built

| item | value |
|---|---|
| config | `config/eds.yaml` (from TEMPLATE; 7 buckets with written definitions under population semantics, subtype `eds_type`, 7 abbreviation collisions incl. the newly found HEDS, composed scope text, `draw:` and `screen:` blocks) |
| roster | 75 papers, chunks 0/1/2 = full-text-rich (25/25 PMCID+OA) / mixed (17 PMCID, 14 OA) / abstract-heavy (0 PMCID); themes eds_general 29, heds_hsd 23, gjh 12, eds_other_types 11; years 2019–2026 |
| universe | 2312 unique PMIDs from 4 theme queries (hits per theme {'heds_hsd': 611, 'eds_other_types': 504, 'eds_general': 1746, 'gjh': 524}); 10 regex-flagged; 6 Europe PMC calls, 314 s |
| findings | **886 rows from 69 of 75 papers** (69/75 = 92.0% yielding; 4 abstract-only papers yielded nothing, 2 blocked at fetch) |
| package | `DRYRUN-eds-findings-test-v0-dryrun` — 51 files, 1.32 MB; verifier **PASS**: 19/19 headline numbers re-derived, invariants 9/9, self-test 6/6 planted regressions caught; checksums verified after extracting the tarball |

## 2. Code edited (POTS-specific or method-blocking; file → what → fix)

Every item below required changing code, not config. Baseline before edits: all repo tests passed
(config 100%, driver 40/40, normalizer 10/10, comparator 8/8, invariants_ext all).

| # | file | what was POTS-specific / broken | fix |
|---|---|---|---|
| 2.1 | `pipeline/driver.py` `done_units()`/`archive_units()` (lines ~81–116), worker template `fn=...`, `build_task()` | Chunk artifacts named `chunk_NNN_findings.parquet` and detected by a project-wide filename search: the POTS build's 245 whole-chunk artifacts made EDS chunks 0–2 "already done" (measured: `done_units` → {0,1,2} before any dispatch). | `artifacts.file_prefix` (default `<slug>_`, POTS config `""`) in every filename and both regexes, anchored at both ends; `pipeline.py` `archive_name()`/`save_chunk()` use the same prefix; 5 new driver tests. |
| 2.2 | `pipeline/config.py` module import | `import yaml` at import time. The driver must run in the orchestration kernel (`host.delegate`), which is stdlib-only → `driver.py` unimportable there. | Lazy import; `load_config` reads a `.json` twin; `export-json` CLI. |
| 2.3 | `pipeline/driver.py` `run()` watchdog branch; `helpers.plan_units` | Units sized by paper count; watchdog re-dispatched the same shape. Chunk 0 (25/25 full text, one unit) exceeded 3000 s with nothing saved; identical re-dispatch ×2. `plan_units()` sizes against the token ceiling only. | `plan_parts()`: parts = max(default, ceil(expected_fulltext / `pinned.max_fulltext_per_unit`=12)) from roster `pmcid`+`is_oa` (or `fulltext_expected`); watchdog → re-split (`pinned.watchdog_resplit`); `run(roster_df=...)` accepts a DataFrame or list of dicts. Measured after fix: 3 parts, walls 700/944/1184 s, 448 findings, 24/25 yield. 8 new tests. |
| 2.4 | `pipeline/pipeline.py` `unescape_source()`; `pipeline/quote_trace.py`; `normalizer_tests.py` | Europe PMC abstracts carry `<i>`/`<sub>` tags; the census normalizer never stripped tags while `normalizer_tests.py` tested a *different* function that did. 4/75 screen spans failed on `<i>PLOD1</i>-related`. | One shared `strip_tags` (inline tags → '', block tags → space) in `quote_trace`, used by the pipeline (before the model reads), the screen and the tests; `normalize()` unchanged; +1 normalizer case, +4 comparator cases (12/12). |
| 2.5 | `pipeline/draw.py` (new) | No draw code in the repo ("any tool"). First version excluded regex hits: 8/10 exclusions were genuine EDS papers (sleep apnoea *in hEDS* ↔ "excessive daytime sleepiness"). | Config-driven draw (`draw.themes`, year window, SRC:MED, HAS_ABSTRACT:y, cursor paging at pinned pacing); contamination **flagged, never excluded** (`--exclude-contaminated` opt-in); `--resample-from` re-samples offline. |
| 2.6 | `pipeline/screen.py` (new); `helpers.screen_instrument_alarm` | No screen code in the repo. The alarm is degenerate on an anchor-drawn roster (rule off-topic = 0 by construction; floor at 1 → any ≥3 LLM off-topic fires). | Two-arm screen with verbatim-span check (symmetric normalization), `reconcile()` as a pure function, parse-completeness assertion, per-theme rates, exclusion classes, explicit list of LLM-off-topic-but-anchored papers. Alarm formula left as is; note added to summary and RUNBOOK. |
| 2.7 | `pipeline/driver.py` log filenames | `gate_log_parts.json` etc. fixed names → run 2 clobbered run 1. | `<prefix><stem>_<run_tag>.json`; `run_tag` returned in the report. |
| 2.8 | `pipeline/resolve_stage.py` (new); `pipeline.ground_findings_df` | RUNBOOK stage 5 named `resolver.py` but nothing applied it to a corpus. Worker grounding routes only ChEBI/MyGene/HPO and emits the **bare** `authority_lookup_ols_hp` tag — invariant 9 fired on the worker columns (1 MPATH id, the Tier-3 defect shape). | Stage over the merged table: `resolve_entity` by `entity_kind` (uberon/cl/chebi/hp/ncbitaxon/uniprot), truthful `_via_<mint>` HPO tags, persistent cache incl. HPO terms, worker columns kept as `*_worker` with an agreement count, rates by authority with bases. |
| 2.9 | `pipeline/quote_census.py` `full_census()` | Abstract-only rows checked against the raw roster abstract (tags) rather than the text the model read; `blocked` folded into the full-text stratum; a 1-paper stratum reported "MATCHER SUSPECT". | Archive-first source for every row (roster fallback counted); three strata; "no control possible (<2 papers)" verdict. |
| 2.10 | `build_packages.py`; `packaging/generic_builder.py` (new); `PACKAGING.md` | The script is the POTS five-package reproduction: fails at `partition()` on `tier_source` for any non-POTS corpus (measured); hard-wires `tier1/2a/2b/3/4`, `site`, the wave split, ~40 harmonization columns; templates are POTS prose; `PACKAGING.md` described a generic interface that did not exist; `pipeline/verify_package.py` expected `MANIFEST.condition_enum` that nothing wrote. | `--area-config` mode → one package: data (+ `data/archives/`), docs generated from config and computed stats, qa, scope, code (generation + config + rendered prompt/scope), sidecars, `MANIFEST.json` with `condition_enum`/`model_id`, checksums by script; appends exactly the six harmonization columns the verifier reads and lists them in DATA_DICTIONARY. Legacy mode untouched. |
| 2.11 | `pipeline/pipeline.py` `process_chunk` | Findings rows carried no draw/screen flags (POTS added them only at cross-tier harmonization). | `ROSTER_PASSTHROUGH` columns ride through when present. |

**Docs that assumed POTS** (fixed in place): README "byte-identical dispatch logic" claim (now: identical
under the POTS config's legacy settings; 53 tests pin both behaviours); README layout table and quickstart;
`PACKAGING.md` builder paragraph; RUNBOOK stages 1, 3, 4, 5, 9 and the handoff section (two kernels);
`SCHEMA.md` roster contract (flag columns, prefix, `is_covid` marked POTS-era); `PITFALLS.md` §9;
`TEMPLATE.yaml` (`file_prefix`, `max_fulltext_per_unit`, `watchdog_resplit`); `pots_dysautonomia.yaml`
pins the legacy values. **Still POTS-specific and left**: the prompt template's `Flagged COVID-related
in corpus: {is_covid}` header and `is_covid_associated` field (all 886 rows `False`; harmless but
meaningless for EDS — removing them changes the pinned prompt, deferred to a template version bump);
`config.prompt_slots()` defaults (COMPASS-31, RECOVER, DecodeME) — overridden by config;
`packaging/templates/*.tmpl` (POTS prose; unused by the generic builder).

## 3. Measured numbers

### 3.1 Fetch and extraction (per work unit, from the workers' own STATS)

| unit | papers | full text | abstract-only | blocked | yield | findings | truncated→continued | output tokens |
|---|---|---|---|---|---|---|---|---|
| chunk1 | 25 | 12 | 12 | 1 | 24/25 = 0.96 | 309 | 8 | 351,213 |
| chunk2 | 25 | 0 | 25 | 0 | 21/25 = 0.84 | 129 | 11 | 133,608 |
| c0p0 | 9 | 9 | 0 | 0 | 9/9 = 1.0 | 180 | 2 | 210,248 |
| c0p1 | 9 | 8 | 0 | 1 | 8/9 = 0.8889 | 160 | 2 | 181,269 |
| c0p2 | 7 | 7 | 0 | 0 | 7/7 = 1.0 | 108 | 1 | 141,441 |
| chunk 0 attempt 1 (whole) | 25 | — | — | — | watchdog at 3000 s, nothing saved | 0 | — | not captured |

Totals: full text retrieved **36/75** papers (38 flagged PMCID+OA; 2 blocked: HTTP 503 on search, HTTP 500 on
full text — both recorded in `fetch_note`, neither retried post-run); full-text share of yielding papers
35/69 = 50.7%; findings/paper **12.84 overall** (886/69) — **19.94 full-text (698/35), 5.64 abstract-only
(186/33), 2.0 blocked (2/1)**; per chunk 18.67 / 12.88 / 6.14, i.e. composition, exactly as
`COMPARABILITY.md` warns. `number_in_quote` false 27/456 numeric rows (5.9%; flagged, not fixed).
`certainty_derived`: confirmed 730 / mixed 91 / hedged 65. Enum drift: 1 row `evidence_tier =
systematic-review/meta-analysis` (flagged). Guard violations 0; parse failures 0.

### 3.2 Condition buckets (rows / papers with ≥1 row)

{"hEDS": {"rows": 206, "papers": 17}, "EDS_other_type": {"rows": 205, "papers": 21}, "multiple": {"rows": 133, "papers": 18}, "general": {"rows": 111, "papers": 22}, "hypermobility_unspecified": {"rows": 110, "papers": 19}, "EDS_unspecified_or_mixed": {"rows": 91, "papers": 22}, "HSD": {"rows": 30, "papers": 4}}

`eds_type` filled on 247 rows (205 of the 205 `EDS_other_type` rows; also filled on rows of other
buckets — a prompt-instruction violation, flagged, not fixed). No bucket census was run (a stage-11
activity); the definitions are v1 and untested against the rows.

### 3.3 Screening

75 screened; rule arm anchor 75/75 (the draw is anchor-based); LLM arm in_scope 55 / off_topic 13 / uncertain 7;
spans verbatim 75/75 after the tag fix (71/75 before, 70/75 before dash/tag handling); final in_scope 55 / uncertain 20 / off_topic 0.
Instrument alarm **fired** (ratio 13.0); audit of the 13 LLM off-topic calls by title: 12 topical drift
(veterinary EDS ×2, single-joint orthopaedics/TMJ ×3, mention-only ×5, unrelated ×2) and 1 wrong
abbreviation sense (**HEDS = HCC Early Detection Strategy**, PMID 41730232 — added to the collision
list; later blocked at fetch, 0 findings). Verdict: the prompt is not broken; the alarm's ratio is
degenerate when the rule arm cannot produce off-topic verdicts. LLM off-topic per theme: eds_general 7/29,
heds_hsd 2/23, gjh 2/12, eds_other_types 2/11 (final off-topic per theme is 0 by the reconcile rule).
Screen cost: 75 calls, 10,612 output tokens, 59 s.

### 3.4 Identifier resolution by authority (coverage, not correctness)

| entity_kind | authority | rows resolved | distinct names resolved |
|---|---|---|---|
| anatomical-structure | uberon | 30/94 = 31.91% | 23/56 |
| cell-type | cl | 6/9 = 66.67% | 4/7 |
| chemical/metabolite | chebi | 20/35 = 57.14% | 11/22 |
| gene/protein | uniprot | 27/163 = 16.56% | 18/84 |
| other | none (unrouted by design) | 0/161 = 0.0% | 0/74 |
| pathogen | ncbitaxon | 2/2 = 100.0% | 2/2 |
| physiological-measure | hp | 17/255 = 6.67% | 13/164 |

Entity rows with an id: 102/886 = 11.51% of all rows; 102/595 = 17.14% of rows with an entity name.
Source tags: {'unresolved_nulled': 784, 'authority_lookup_uniprot_exact': 24, 'authority_lookup_ols_uberon_exact': 17, 'authority_lookup_ols_hp_exact': 15, 'authority_lookup_ols_chebi_exact': 15, 'authority_lookup_ols_uberon_synonym': 13, 'authority_lookup_ols_chebi_synonym': 5, 'authority_lookup_ols_cl_exact': 5, 'authority_lookup_uniprot_synonym': 3, 'authority_lookup_ols_ncbitaxon_exact': 2, 'authority_lookup_ols_hp_synonym': 1, 'authority_lookup_ols_cl_synonym': 1, 'authority_lookup_ols_hp_via_go_exact': 1}. Minting ontologies: {'uberon': 30, 'uniprot': 27, 'chebi': 20, 'hp': 16, 'cl': 6, 'ncbitaxon': 2, 'go': 1}.
HPO: 216/660 = 32.73% distinct phenotype terms; rows with ≥1 verified HPO id 386/677 = 57.02% of rows with terms;
multi-id rows 115; imported-term (`_via_`) rows 1. Genes: HGNC 130/137 = 94.89% of gene-marker rows (MyGene, worker).
Worker vs stage agreement where both resolved: 19/19 identical; stage-only 83, worker-only 0.
`id_denotes_entity_rate`: **not measured**. The low `physiological-measure` → HPO rate reflects that measure
names ("Beighton score", "heart rate variability") are not HPO phenotypes — an authority-routing question
for the schema, not a resolver fault.

### 3.5 Quote traceability census (with wrong-paper negative control)

| stratum | checked | traceable | rate | control false positives | control verdict |
|---|---|---|---|---|---|
| abstract-only | 186 | 186 | 100.0% | 0/186 | matcher discriminates |
| full-text (archive-backed) | 698 | 697 | 99.86% | 0/400 | matcher discriminates |
| blocked (fetch failed; extracted from title+abstract) | 2 | 2 | 100.0% | 0/0 | NO CONTROL POSSIBLE (<2 papers with source text in this stratum) - rate reported WITHOUT a control; do not cite it alone |

Coverage: 886/886 quote-bearing rows checkable against retained text (100.0%);
885 traced (99.89%). The single untraceable full-text quote (PMID 40347015) is an R² sentence —
consistent with the "do not normalize superscripts" rule. Invariants: 9/9 pass on the shipped table;
invariant 9 **fails on the worker's HPO columns** (1 MPATH id under a bare `hp` tag) and passes after the
stage-5 retag — the negative control that the check still bites.

### 3.6 Wall time and API calls

| stage | wall | calls |
|---|---|---|
| draw (stage 1) | 314 s | 6 Europe PMC search calls (pageSize 1000, resultType core) |
| screen (stage 3) | 59 s | 75 model calls, 10,612 output tokens |
| worker smoke test (2 papers) | 67 s | 2 model calls (9,003 output tokens), 2 EPMC, ~4 authority |
| extraction run 1 (chunks 0–2 whole) | chunk 2 516 s; chunk 1 1,291 s; chunk 0 killed at 3,000 s; driver cell alive ~110 min, then died (`host.collect` unavailable) | see below |
| extraction run 2 (chunk 0 as 3 parts) | 700 / 944 / 1,184 s; driver 1,186 s | see below |
| resolve (stage 5) | 589 s (chunks 1–2) + 1,605 s (all; 371 HPO terms re-queried — since fixed by caching) | 331 entity lookups (OLS4/UniProt) + 660 distinct HPO terms (1,031 HPO calls made) |
| census + invariants | ~3 s | 0 (offline against the archive) |
| package build + verifier | ~8 s | 0 |

Model output tokens, successful units: **1,017,779** (POTS fitted model projected 684,953 for 38 flagged
full texts → **1.49× under-projection** on a full-text-dense roster; a no-intercept fit on these 5 units gives
≈ 22,600 tokens per full-text paper and ≈ 5,500 per abstract-only paper). Plus screen 10,612 and smoke 9,003;
chunk 0 attempt 1 not captured (≤ 532,958). Europe PMC calls during extraction (estimate; basis: the pinned
fetch path makes 1 search for an abstract-only paper and 2 searches + 2 full-text fetches for a full-text
paper because `get_ft_text` re-fetches what `fetch_fulltext` already read): ≈ 40 + 36×4 + retries on 2
blocked papers (≤6 each) ≈ 200 for the shipped units, plus ≈ 100 for the killed attempt. Aggregate pacing
stayed at 3 workers × 0.9 s; blocked fraction 2/75 = 2.7% (inside the 0–3% band the pacing was tuned for).

### 3.7 Overlap with the POTS corpus (not deduplicated — documented)

**49/75 = 65.33%** of the dry-run papers are already in `pots-findings` v1.1 (11,519 papers); at universe
level 1428/2312 = 61.76% of the 2019–2026 EDS/HSD draw. By stratum: full-text-rich 20/25, mixed 15/25,
abstract-heavy 14/25.

| POTS package | papers | in `EDS_hypermobility` bucket | overlap with dry run | of which in that bucket |
|---|---|---|---|---|
| comorbid-conditions-findings-a | 3123 | 873 | 27 | 26 |
| comorbid-conditions-findings-b | 2589 | 666 | 20 | 19 |
| dysautonomia-core-findings | 3129 | 146 | 2 | 2 |
| migraine-hypermobility-findings | 1091 | 109 | 0 | 0 |
| postviral-autism-findings | 1587 | 24 | 0 | 0 |

POTS `condition` labels carried by the 49 shared papers: {"EDS_hypermobility": 32, "EDS_hypermobility|general": 6, "EDS_hypermobility|general|multiple": 4, "EDS_hypermobility|IBS_gut|multiple|small_fiber_neuropathy": 1, "MCAS|general|multiple": 1, "EDS_hypermobility|dysautonomia|general|multiple|small_fiber_neuropathy": 1, "EDS_hypermobility|MCAS|multiple": 1, "general": 1, "EDS_hypermobility|MCAS|POTS|multiple": 1, "EDS_hypermobility|IBS_gut|POTS|dysautonomia|general|migraine|multiple": 1}.
What a cross-corpus paper registry has to handle, from this: (a) the same PMID extracted twice under two
bucket enums (POTS `EDS_hypermobility` vs EDS `hEDS`/`HSD`/`EDS_other_type`/…) and two prompt generations —
findings are different rows and must not be merged by pmid; (b) two `condition` semantics decisions
(both population, different granularity); (c) provenance-census sources differ (POTS tiers 1–3 have no
retained text); (d) a registry key of (pmid, corpus, tier, pipeline generation, prompt vid) rather than
pmid alone; (e) 47 of the 49 sit in the two Tier-2 packages whose ids are model-emitted-then-existence-
checked, so identifier columns are not comparable either.

## 4. Verdict on reusability

**Partially reusable before this run; reusable for a single-tier build after it — with gaps.** The
condition-agnostic *core* held: the prompt rendered from config with no edit, the parser guard, the
retention archive, invariants, the census with its control and the resolver all ran unchanged and
produced the expected quality signature (0 guard violations, 0 parse failures, 99.89% traceable,
0/586 control false positives, 9/9 invariants). What was **not** reusable was everything *around*
the core: the orchestration (namespace collision, stdlib kernel, unit sizing, log names), the three
stages the RUNBOOK described but the repository did not contain (draw, screen, resolve-over-corpus),
and the packaging, which was the POTS build rather than the method. Eleven code edits, all config-gated
and tested (53 driver cases, 12 comparator, 11 normalizer; the config suite green under the POTS, EDS
and TEMPLATE configs).

Remaining known gaps, in value order:
1. **No human fidelity scoring** (stage 11) and no `id_denotes_entity_rate` — every number here is well-formedness.
2. **All-or-nothing worker save**: a unit that dies loses everything; per-paper checkpointing would have
   saved chunk 0's first 50 minutes.
3. **Driver cell fragility**: `host.collect` became unavailable ~110 min into a background cell, orphaning
   children without a watchdog (attempts 2–3 also never started — the driver cannot tell "never started"
   from "too slow"); the loop needs to be resumable and re-attachable, and to check a child's first turn.
4. **Prompt template still carries POTS-era `is_covid` fields**; removing them is a prompt version bump.
5. **`screen_instrument_alarm`** needs a formulation for anchor-drawn rosters (e.g. LLM off-topic rate
   against a per-theme prior), not a floor-at-1 ratio.
6. **Cost model**: refit per area on the pilot unit before projecting (1.49× miss here); the roster's
   `pmcid`+`is_oa` predicted full-text availability well (36 retrieved of 38 flagged).
7. **Double fetch** of every full text in the pinned fetch path (pacing budget spent twice).
8. **Templates**: `packaging/templates/*.tmpl` remain POTS prose; the generic builder writes its own docs.
9. **Bucket definitions untested** against rows (no census); `eds_type` leaks onto non-`EDS_other_type` rows.
10. **Vocabulary canonicalization, comparability report, derived layer** not exercised (single tier).

## 5. Artifacts

`config/eds.yaml` (+ `eds.json` twin); patched `condition-findings-builder.tar.gz` (new version; README
changelog); `DRYRUN-eds-findings-test.tar.gz` (the package; first README line marks it a ~75-paper dry
run, not a release); `eds_findings_resolved.parquet` (886 rows); `eds_roster.parquet`; QA JSONs
(`invariants.json`, `quote_census.json`, `resolve_report.json`, `screen_summary.json`, `gate_log.json`,
`driver_run_reports.json`, `worker_stats.json`, `overlap_with_pots_corpus.json`). Nothing was published;
the `pots-findings` artifact was not modified.
