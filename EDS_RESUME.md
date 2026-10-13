# EDS_RESUME — resumption spec for the EDS Tier-1 build (written BEFORE the run, 2026-10-07)

If this build is halted (usage cap, kernel death, closed laptop), a fresh session resumes from this file.
Nothing in memory is needed: the driver recomputes `done` from the artifact store.

## Pinned inputs (artifact version ids — full UUIDs)
- method repo (v7, registry + full-build config merged): condition-corpus-builder.tar.gz = f067ddbf-737a-494a-9b28-1db38290eda9
  (config/eds.yaml in the WORKSPACE has since been re-pinned to roster v2 and 30 chunks; the tarball is re-saved after the screen → check the latest version)
- roster v2 (1,463 papers, 30 chunks; chunks 0-24 = roster v1 frozen order, 25-29 = the 223 POTS no-yield papers, round-robin title-only):
  eds_tier1_roster_v2.parquet = 3df07eb6-2398-4bb8-8912-84ac7f6aa826 ; report 06bbbf0f-7a96-4754-99f0-4a11a4761dbc
  (after the screen a v3 with screen columns and off-topic papers removed is saved and re-pinned; use whatever config/eds.yaml artifacts.roster_vid says)
- pipeline module (byte-identical to the dry run's): afa47b13-ae5b-474d-926a-19918ba8b527 ; rendered prompt a8fd46b7-07e6-4938-b495-37f919565bc7
- model id: claude-sonnet-5 (literal). Pinned: 50 papers/chunk, n_parts 2, max 12 full texts/unit, watchdog re-split, 3 workers x 0.9 s, WORKER_MAX_S 3000.
- registry v0: paper_registry_v0.parquet = 7f765114-3e5a-4225-9320-51e435826c3c (rebuild as v1 after extraction)

## Resume procedure
1. Untar the latest condition-corpus-builder.tar.gz; `python pipeline/config.py export-json config/eds.yaml > config/eds.json`.
2. In the orchestration kernel: CORPUS_CONFIG=config/eds.json; import driver as D; roster plan rows = JSON list of (chunk_id, pmid, pmcid, is_oa) from the pinned roster.
3. `D.done_units(host, set(D.TARGET_CHUNKS))` lists finished chunks (artifact filenames eds_chunk_NNN[_partIofN]_findings.parquet). Dispatch ONLY the next wave:
   `D.run(host, D.build_task, D.gate, <wave chunk ids>, split_all=True, roster_df=plan_rows, run_tag='wave<k>')`.
4. Gate per wave (automatic): every unit ok (yield >= 0.60, blocked <= 0.35, 0 guard violations, archive present); cumulative output tokens <= 1.2 x projection-to-date; else STOP and report.
5. Record per wave: qa/wave_<k>.json (gate log, unit stats, tokens, wall). State file: qa/build_state.json (done chunks, waves run, tokens so far).

## Wave plan
pilot = chunks [15, 0] (dense / abstract-heavy) → refit cost model → waves of 5 chunks in frozen order over the remaining 28: [1-5],[6-10],[11-14,16],[17-21],[22-26],[27-29].
Projection (basis: 22,600 tokens per full-text paper, 5,500 per abstract-only, dry run 2026-09-30): 560 full text + 903 abstract-only (155 title-only counted as abstract-only) ≈ 17.6M output tokens; ~64 units; ~6.5-7.5 h wall at 3 workers.

## After extraction
resolve_stage over merged table (cache 5d8f69b1-db10-4252-8e55-45089669fd10 seeds) → invariants → quote census (3 strata, controls) → build_packages.py --area-config → verifier --self-test → registry v1 → EDS_BUILD_REPORT.md.


## Update 2026-10-07 (after pass-2 population check) — state before the pilot
- Pass-2 over 541 uncertain: 485 no listed population / 53 yes / 3 errors (verbatim spans 519/541). Combined verdict v2: in_scope 738 / off_topic 569 / uncertain 156. Both-passes exclusions: 385.
- **Roster v3** = `eds_tier1_roster_v3.parquet` vid `05a75a13-28c5-4bec-9067-a8f6d2e93a3d` — 766 papers / 30 chunks (sizes 6–47), 308 full text expected, 156 uncertain (extracted + flagged), chunk ids frozen from v2.
  - removed: 569 screen-excluded (`eds_tier1_excluded_pmids_v3.parquet` 40532a56-268a-4811-99ca-9561b6006a14, csv 2a61e5bc-9af6-4222-9cad-1c4f9ae12d87) and 128 title-only not-extractable (`eds_tier1_not_extractable_title_only.parquet` 33c00687-8354-402c-b61f-5c25284b28ee; basis: no abstract in EPMC or PubMed, no full text expected). 15 title-only papers WITH full text expected stay in.
  - final screen table `screen_eds_tier1_final.csv` 3d547c01-c725-4ae7-b158-a8f6899bf403; report `eds_tier1_roster_v3_report.json` 09e32cdf-718a-44eb-a72d-8163f4c1542d.
- Repo tarball **v10** = `f474eeaa-b99c-4933-ae91-f7091cfd5064` (roster v3 pinned in config/eds.yaml + eds.json; driver.plan_parts now caps parts at ceil(n_papers/25) so screened-down chunks run as 1 unit; tests driver 55/55 both configs, config all_pass, registry 42/42).
- Projection on v3: 45 units; ≈9.5M output tokens nominal (22,600/FT paper, 5,500/abstract paper from the dry run), ~12.3M at +30%.
- **Pilot = chunks 8 and 0** (NOT 15 & 0 as planned: chunk 15 is 7 papers post-screen). Chunk 8: 38 papers / 23 FT (ceiling case, 2 units); chunk 0: 37 / 2 FT (abstract end). Gates: yield ≥0.60, blocked ≤0.35, 0 guard violations, archives saved. Refit cost per FT / per abstract paper from the two pilots before waves.
- Resume recipe: untar v10 → `CORPUS_CONFIG=$PWD/config/eds.json` → in repl: `import driver as D; D.run(host, D.build_task, D.gate, <chunk ids>, split_all=True, roster_df=plan_rows, run_tag=...)`; done detection is from the artifact store (`eds_chunk_NNN_part*_findings.parquet`), so re-running the same chunk list only dispatches missing units.

## Update 2026-10-07 10:30Z — pilot passed; waves starting
- **Defect found by the pilot**: done-detection took the DRY RUN's `eds_chunk_000_part*of3_*` (2026-09-30) as the full build's chunk 0 and skipped it. Fix: `artifacts.build_after: "2026-10-07T09:00:00Z"` in config/eds.yaml; driver.done_units/archive_units forward it as `after=` to the artifact store (tests: driver 59/59 both configs, incl. build_after_cutoff). **Any gather of eds_chunk_* files for packaging must apply the same cutoff.**
- Pilot (chunks 8, 0): 4 units, all gates ok; 616 findings; 716,680 output tokens for 75 papers. Chunk 8 part vids 087bd321 / e7147270 (archives b12360c9 / 1d31417b); chunk 0 part vids 4260aeaf / 111ce131 (archives 7c79f71b / 26c4aca9).
- Cost model v2 (least squares on 4 units, residuals ≤6%): 18,024 tokens per full-text paper, 5,326 per abstract paper (config/eds_cost_model_v2.json). Remaining 28 chunks / 691 papers (283 FT) → 7.27M projected; automatic stop at >20% over per wave or cumulative.
- Waves (≤5 chunks): [1-5], [6,7,9,10,11], [12-16], [17-21], [22-26], [27-29]; controller pipeline/waves.py (`run_waves`) writes qa/wave_k.json + qa/build_state.json; gate log now carries tokens_out / retained_fulltext / n_expected per unit.
- Resume after an interruption: reload driver with the v11 tarball config and call run_waves again — done units are skipped from the store (with the cutoff), only missing units are dispatched.

## Update 2026-10-08 — build complete
- All 30 chunks extracted (pilot + 6 waves). Wave 2 attempt 1 lost to a system stall (nothing saved; no findings lost); driver now has a stall breaker, store-query retry and a dispatch deadline (repo v13 = dcb8fa23-ba26-4bfc-96cd-c8f25db1afbc, tests 69/69).
- Shipped: eds-findings-v0.tar.gz c8926a5d-1172-4623-a745-50c0d0c396b9 (verifier PASS; invariants 8/9 with chunk 17 declared); eds_findings_resolved.parquet b001212a-1634-4429-842e-7917f0e875fc (7,101 rows / 679 papers); paper_registry_v1.parquet 5d610af6-9188-4697-869c-e9a12f15618c (12/12); build_state.json 8534819d-71e6-4143-afeb-df591388e452; CONNECTIONS_DESIGN.md 9779e8b0-295c-4929-9ac8-29b85d53c7e9; EDS_BUILD_REPORT.md (this session).
- Unit-file rule: retries saved same-named files as separate artifacts; the build uses the LATEST per filename within the build_after cutoff (qa/unit_files_used.csv has the exact vids).
- Next (user-dependent): human sample of the pass-2 exclusions (100 papers); population mapping between the two enums; then the connections ledger per CONNECTIONS_DESIGN.md.


## Tier 2 roster/screen — 2026-10-13 (roster frozen; screen 2.5/17 units; BLOCKED on the host.llm per-frame ceiling)
- Config: `config/eds_tier2.yaml` (+ `.json`) = eds.yaml with ONLY tier.number 2, label, year_range 1936-2018, file_prefix `eds_t2_`, build_after `2026-10-13T22:10:10Z`, roster_vid `PENDING` (pin roster v2 when it exists), target_chunks [30..137]. Tests under CORPUS_CONFIG=config/eds_tier2.json: config 17/17, driver 69/69, registry 42/42; validator 0 warnings.
- Universe 5,362 (pre2019_universe.parquet bdc9ec9f). Abstracts: Europe PMC EXT_ID batches of 100 (54 calls, 0.9 s, 0 errors) -> 3,840; PubMed efetch backfill (1,522 requested, 8 calls of 200) -> +3; 1,519 missing. Availability by decade (% with abstract): 1930s 0, 1940s 0, 1950s 0, 1960s 1.2, 1970s 30.9, 1980s 64.9, 1990s 79.9, 2000s 83.0, 2010s 87.1.
- Frozen order year asc then pmid asc (numeric); 50/chunk; chunk ids 30-137 cut over the FULL universe. Title-only split (abstract missing AND no full text expected): 1,503 removed -> `eds_tier2_not_extractable_title_only.parquet` 2b49ce97-4da1-4704-adb9-e9594a1ec056 (csv ff9557cc-ed63-48f4-93da-9a92235c2851); 16 title-only papers WITH full text expected stay in. Chunks 31-35 emptied (1940s-1960s); ids NOT re-cut — the driver must dispatch nothing for them.
- **Roster v1** = `eds_tier2_roster_v1.parquet` **9895e8f4-766a-429b-b708-f44f2c387e54** — 3,859 papers, 103 non-empty chunks (1-49 papers; max 23 FT), 392 full text expected, 13 query_contamination flags kept; report 95532d00-824e-4010-89c6-7ce071633185. Text convention: title/abstract stored as fetched (tags retained, as Tier-1 roster v3); abstract_missing judged on strip_tags(abstract).
- Pass-1 screen (claude-sonnet-5, max_tokens 2000, concurrency 8, ~72 s / 240 papers): unit 1 (chunks 30-49, 228) report 8b2aee87-7478-4595-a3e5-4f78c0f34b4a summary 530fdcb6-f421-48fe-b7dc-67e5cb19b798 excluded 037c8cf2-3cc3-421f-9713-78ed58e7084b flagged 18ab8c2c-02db-4777-ae56-f9f089d6b214; unit 2 (50-56, 239) 029c5302-a15a-433e-ab49-07a6df0b4881 / 44aa8dfd-034c-42fe-bc8d-e14f7664bee2 / fcb6b650-b913-491e-8ebe-25e364618533 / 958d8fac-537f-46c9-b80e-6497be21340d; unit 3 PARTIAL (57-62, 136/246 valid) 96c083d6-727b-4e5d-b5fa-94005d46a1c6 / 60bba21e-018a-4e0c-8415-6e189593ce7d / 8d5f1158-5e25-4692-b465-b6caa053f791 / d7499e5a-7097-4fb0-9f4e-67c4997f72e2.
  - **Blocker**: `host.llm()` enforces a cumulative 2.0M-token ceiling per frame (prompt + reserved max_tokens ≈ 3.5k per screen call → ~570 calls). It is NOT the FRAME_TOKEN_CEILING heuristic in helpers.plan_units (that only sizes one call). 109 calls in unit 3 failed with the ceiling error, 1 call in unit 2 and 1 in unit 3 failed JSON parse; those 111 pmids are in `screen_eds_t2_pass1_retry_pmids.csv` 7e2135eb-951f-4ef5-a188-17606110b574 (their final_verdict 'uncertain' is the None-LLM path, NOT a verdict). A leaf sub-agent cannot delegate, so units 3R, 4-17 (3,257 calls) must be fanned out by the lead, one fresh frame per unit (≤250 papers) — briefs in EDS_TIER2_SCREEN_HANDOFF.md.
  - Interim (602 valid): final in_scope 402 / uncertain 174 / off_topic 26 (all both-arm; 26 topical_drift); rule anchor 556 / no_anchor 46; LLM in_scope 407 / off_topic 150 / uncertain 45; span verbatim 586/602. Instrument alarm fired in every unit (ratio 54-63; rule off_topic 0 by construction). 20-span audit of LLM off_topic calls (seed 20261013): 20/20 read as topical drift or incidental mention — single-joint ankle instability surgery (GJH as a covariate), Down's/SLE/fragile-X papers listing hypermobility, Marfan/OPPG/wound-healing reviews naming EDS in a list, general collagen/elastin biochemistry reviews (no anchor → excluded, population `general`), Scheuermann's disease matched by "kyphoscoliotic", veterinary dermatosparaxis calf-bone biochemistry and a feline skin-fragility case (not framed as human-EDS models — borderline; routed to pass 2 as uncertain, not excluded). No wrong-abbreviation-sense hits so far. Interim merge a333a67e-23d8-405c-b563-e302d3c0e959 / 02069750-7407-44da-a38a-b4442f7c14fb.
- Deterministic remainder: `scripts/eds_tier2_screen_merge.py` (merge-pass1 → uncertain set + pass-2 units ≤200; finalize → screen_eds_t2_final.csv, roster v2, excluded v2, report, cost projection with pilots). Tested on the interim units (refusal path and a synthetic pass-2).
- **Cost projection (PROVISIONAL, roster v1 pre-screen)** EDS_TIER2_COST_PROJECTION.md 5069d286-5c61-4ceb-a6cf-be0c1310e6c7 / json f4ccb25f-aa8b-4e9b-ad99-910f413cca14: 3,859 papers (392 FT) → 195 units, 25,530,650 output tokens nominal, 33,189,846 at +30% (18,024/FT, 5,326/abstract). Expect the post-screen figure at ~70-96% of this. Provisional pilots: chunk 136 (2018; 45 papers / 23 FT, 2 units — FT ceiling case) and chunk 52 (1985-86; 37 papers / 0 FT — oldest-abstract yield case). Re-derive from roster v2.
- Repo tarball: v15 9af8369e-ac8d-4882-9129-59f406d18b4c (config only) → v16 (this note's version; adds scripts/eds_tier2_screen_merge.py, EDS_TIER2_SCREEN_HANDOFF.md, EDS_TIER2_COST_PROJECTION.md). Not produced this session: roster v2, excluded list, final screen table, registry v2.
- Lesson: unit-dispatch per-record LLM work to SEPARATE FRAMES, not merely separate calls — the ceiling is per frame and cumulative; and give the screen stage to an agent that can delegate.
