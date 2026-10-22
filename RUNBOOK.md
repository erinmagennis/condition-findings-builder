# RUNBOOK — building a condition corpus, stage by stage

Part of `condition-findings-builder`. Twelve stages (0–11). Each states what it produces, the gate
that must pass before the next one, and the specific thing that went wrong on the POTS build. Do
not skip a gate because a stage "obviously" worked — four of the worst defects on that build
passed every gate that existed at the time.

**Where the code is.** Everything a stage runs lives in this tree: `pipeline/config.py` (the
config), `pipeline/helpers.py` (stage helpers: `scope_definition`, `screen_reconcile`,
`plan_units`, `coverage_bases`, `stratified_rate`, `assert_partition`, `reject_id_shaped`,
`gene_case_collision_check`), `pipeline/pipeline.py` + `pipeline/driver.py` (extraction),
`pipeline/resolver.py` (identifiers), `pipeline/invariants.py`, `pipeline/invariants_ext.py`,
`pipeline/quote_census.py`, `pipeline/quote_trace.py` (QA), `pipeline/verify_package.py` and
`build_packages.py` (packaging), `calibration/` (AI-scorer calibration). Fill `config/<area>.yaml`
from `config/TEMPLATE.yaml` first; every stage reads it.

---

## Stage 0 — Scope the area (do this before any search)

Produce **two written lists**. They drive the draw AND the screen, and getting them wrong is
recoverable only by re-drawing.

**A. Condition families, stated as families and not as a closed list.** For each family, name the
family and give illustrative members. The prior build's screening prompt listed in-scope
conditions and the model excluded *familial dysautonomia from a dysautonomia corpus* — 303 of 577
papers marked off-topic against the rule arm's 20. A closed list reads as exhaustive no matter
what the surrounding prose says. Use `scope_definition()`, which emits the
"ILLUSTRATIVE, NOT EXHAUSTIVE / do not answer off_topic merely because a condition is not named"
framing.

**B. Abbreviation collisions.** For every abbreviation the area uses, write down the *foreign*
senses. Real examples that contaminated a draw: `ASD` = autism spectrum disorder / adult spinal
deformity / atrial septal defect (28 papers, 40 findings); `HSD` = hypermobility spectrum disorder
/ hydroxysteroid dehydrogenase; `EDS` = Ehlers-Danlos / excessive daytime sleepiness; `SS` =
Sjögren / systemic sclerosis / steady-state; `PANS` = the paediatric syndrome / polyarteritis
nodosa; `MS`, `MD`, `CM` used as method or statistic labels. Any area with abbreviation-based
queries inherits this and it cannot be fixed after extraction — only flagged.

Also decide, in one sentence each: what "off topic" means for this area (wrong abbreviation sense
vs topical drift are different defects and want different handling), and whether a
multi-disease study that includes an in-scope condition counts (it should).

**C. A written definition for every `condition` bucket, and the population-vs-relevance
decision** (`condition.enum[*].definition`, `condition.semantics`, `condition.semantics_statement`
in the config; the validator refuses a bucket without one). Decide, before extraction, whether
`condition` means the patient POPULATION studied or the condition a finding INFORMS; the POTS
decision was *population*. Each definition states inclusions and the nearest exclusion with where
it goes instead. Version the definitions (`condition.definitions_version`) and **re-run any label
audit when they change.**

*Evidence.* The POTS build never wrote these down. Its `MCAS` bucket — 1,083 papers, 8,834 rows —
was censused against freshly fetched abstracts on 2026-09-16: ~69% mast-cell biology with no
mast-cell-disorder population, ~10% MCAS as then defined, ~5% mastocytosis (a distinct clonal
diagnosis), ~6% both, ~10% neither (stratified projection; 903 papers unsampled). A "markers of
MCAS" count from that bucket is mostly a count of markers of mast cells in general. Separately, an
AI scorer asked "is the condition label right?" flagged 10 of 24 clean rows with no definitions in
the prompt, 0 of 24 with a definition later found wrong, and 3 of 24 — exactly the three
mastocytosis papers — with the decided definition: a 42-point swing from wording alone. A human
scorer would have hit the same ambiguity and scored it inconsistently across a 13-hour session.

Relabels found later are recorded **additively** (`annotations/README.md`), never by editing the
shipped column.

**Gate:** all three exist in `config/<area>.yaml` and `python pipeline/config.py validate` passes;
the scope text does not contain a bare enumerated list of in-scope conditions without the
not-exhaustive framing.

---

## Stage 1 — Draw the universe and freeze the chunk order

Draw with `pipeline/draw.py` (one Europe PMC query per theme from the config's `draw:` block; the
roster contract the pipeline expects is in `SCHEMA.md` § Roster), deduplicate on PMID, and FLAG —
never exclude — records matching `draw.contamination_patterns` (`query_contamination=true`; the
screen adjudicates). On the EDS dry run 8 of 10 regex "exclusions" were genuine EDS papers: sleep
apnoea IN hEDS matched "excessive daytime sleepiness". Then:

1. Assign every paper a **theme tag** from the query that found it. Theme is the unit of
   off-topic-rate reporting later, and the rates differ enormously by theme — one build measured
   37.2% off-topic in `hypermobility_spectrum` vs 5.9% in an author-augmented theme.
2. Sort into a **frozen order** and cut fixed **50-paper chunks**. Write the chunk map to a
   parquet and pin its version id. Chunk id becomes the unit of work, resume, provenance and
   comparability for the entire project — it must never be recomputed.
3. **Tier the chunks**: contiguous chunk ranges, one tier per condition wave. Tier 1 is the core
   condition; later tiers are comorbid/adjacent conditions.
4. **Boundary chunks.** A chunk straddling two tiers must be assigned to exactly one, **in
   writing, before dispatch**. The prior build's tier-1/tier-2 boundary chunk produced a
   reconciliation puzzle that cost real hours and left two defensible paper counts (6,252 vs
   6,244) that had to be registered separately forever.

`tier.target_chunks` in the config must be an **explicit list**, never `range(N)` — the validator
rejects anything else. The original driver had `N_CHUNKS=286` and would have dispatched every later
tier unasked.

**Gate:** chunk map saved and pinned; tier ranges written down; every boundary chunk assigned.

---

## Stage 2 — Cost the run before you spend it

Size every unit with `helpers.plan_units()` before dispatch. Never quote a projection from a
nominal per-item price — fit it on your own completed chunks. The prior build's fitted model:

```
out_tokens ~= 54,013 + 15,893 * n_fulltext_papers      # per 50-paper chunk
```

and open-access flags **undercount full-text need by ~30%**, so calibrate
`actual ~= 1.30 * flag_count`. Measured cost was ~1.44% of a 20x monthly cap per chunk, dominated
by full-text extraction.

If the budget does not cover the universe, do **not** run chunks in order until it runs out. Use
coverage-target set-cover — a 50%-condition-coverage floor was met by 12 chunks instead of 82.

**Gate:** a projection with its basis stated, and an explicit decision about which subset runs.

---

## Stage 3 — Screen every chunk, with two arms

Screening is per-record LLM work and therefore subject to everything in `PITFALLS.md` §1
(unit dispatch). The scope text is `python pipeline/config.py render-scope config/<area>.yaml`. A 577-paper screen run as one in-frame fan-out breached the 2.0M cumulative
per-frame token ceiling at 1.693M, then a reflexive retry added 0.625M; re-issued as three units
it finished in 11 minutes. Call `plan_units()` first, always.

The code is `pipeline/screen.py` (`run_screen`, and `reconcile()` as a pure function over the saved
per-paper report so a comparator fix never re-spends the screen). Compare model spans against
title/abstract with `quote_trace.strip_tags` + `normalize` on BOTH sides: Europe PMC titles carry
`<i>…</i>`; on the EDS dry run 4 of 75 verbatim spans failed on tags alone.

**Arm 1 — rules.** Regex anchors for in-scope condition terms in title/abstract
(`config.screen.rule_anchors`). Verdicts: `anchor` / `no_anchor`; a contamination pattern with no
anchor → `off_topic`.

**Arm 2 — LLM.** The rendered scope text plus the paper's title and abstract. Require a
**verbatim span** with any `in_scope` verdict. `max_tokens >= 2000`: at 600, 180 responses came
back empty with `stop_reason="max_tokens"` — budget spent, nothing returned.

**Reconcile with `screen_reconcile()`.** The rule that matters: *absence of a keyword is not
evidence of wrong sense.* A `no_anchor` rule verdict must never override an affirmative LLM
`in_scope` verdict carrying a span — that rule recovered a pyridostigmine-in-heart-failure paper
and a Nav1.8 sympathetic-ganglia channelopathy.

**Check the instrument, not the papers.** Run `screen_instrument_alarm()`. Arm disagreement of 3x
or more means the screening prompt is broken. The 303-vs-20 disagreement was the tell; the
corrected prompt gave 73 of 577. **Caveat (EDS dry run):** on an anchor-drawn roster the rule arm's
off-topic count is 0 by construction, the helper floors each arm at 1, and any LLM off-topic count
≥ 3 fires. Audit the LLM off-topic list by title (`*_llm_offtopic_flagged.csv`) before calling the
prompt broken; 13/13 read as legitimate drift or wrong sense there (incl. a new collision, HEDS =
HCC Early Detection Strategy).

**Uncertain papers are EXTRACTED and flagged**, never excluded. The cost of uncertainty is a
flag; the cost of a wrong exclusion is lost data. 238 uncertain papers were extracted with
`final_verdict` recorded in the screening report.

Report off-topic rate **per theme**, and classify each exclusion as wrong-abbreviation-sense or
topical drift — they are different defect classes and a reader needs to know which one they have.

**Gate:** arm ratio < 3x; screening report saved with a per-paper verdict; excluded PMIDs listed
in their own file so the exclusion is auditable and reversible.

---

## Stage 4 — Extract

The pipeline is `pipeline/pipeline.py`, dispatched by `pipeline/driver.py` with
`CORPUS_CONFIG=config/<area>.yaml`; the prompt is `pipeline/prompt_template.txt` rendered from the
config (`python pipeline/config.py render-prompt`). Save the pipeline module and the rendered prompt
as artifacts and pin their ids in `artifacts:` before the first dispatch. This stage's own rules:

**Schema.** Emit names only (`SCHEMA.md`). Add `reject_id_shaped()` as a parser guard — the model will
occasionally emit an id-shaped string even when the prompt forbids it in three places, and the
guard must discard the field rather than pass it through. Ship these columns from the start:

- `provenance_quote` + `provenance_section` — the traceability anchor, verbatim, never paraphrased
- multi-value phenotype terms as semicolon-delimited **names** (43.2% of mappable symptom strings
  resolve to more than one distinct verified id; a one-id-per-row schema silently dropped 5,710
  findings' phenotype grounding)
- `certainty_derived` alongside the extractor's own `certainty` — hedge detection over both
  `finding_text` and quote; never overwrite the extractor's judgment
- `number_in_quote` boolean — is the quantitative value actually in the quote? ~9.9% fail this
  and it is a genuine open question, not a bug to auto-fix
- a provenance tag per identifier recording **which authority and which match mode**
- **do not include a pathway id field.** Only 14 of 147 mechanism strings were real ontology
  terms; keep the mechanism as free text.

**Quote selection rule, in the prompt:** when a finding carries a number, the quote must be the
span where that number literally appears — including a table row or figure caption, with
`provenance_section` set accordingly. Never a nearby sentence that merely discusses it.

**Retain the source text.** `retain_source_text: true` (the config validator refuses anything
else) writes a per-chunk gzipped archive of the EXACT text each extraction saw. ~3.2x compression,
~0.1 MB per half-chunk. This is what makes a quote census possible later; without it, three
quarters of the corpus is permanently uncheckable.

**Decode HTML entities before the model reads the text** (`unescape_source_text: true`; the
pipeline applies `html.unescape` twice, because some Europe PMC records are double-escaped). The
POTS build did not, and 128 shipped quotes (t1 2, t2 82, t3 2, t4 42; 0.09% of rows) carry raw
codes such as `p&#x202f;<&#x2009;0.001`. They are verbatim to what the model read — the Tier-4
archive carries the same codes and 42/42 trace against it — but not to what the paper displays.
Because the archive stores the text *as handed to the model*, unescaping upstream keeps quote,
archive and census on the same bytes. At comparison time normalize **both** sides
(`quote_trace.normalize` does; `comparator_symmetry_tests.py` pins the property) and **never
repair a stored quote**: unescaping the 42 Tier-4 quotes alone took them from 42/42 traceable to
0/42 against their own archive.

**Fetch discipline.** `Accept: */*` on the full-text endpoint — sending
`Accept: application/json` to an XML endpoint returned empty for every request, downgraded 57
papers to abstract-only, mislabelled them `blocked`, and produced 5.51 findings/paper against a
12.33 mean. Assert the body is non-empty and `> MIN_FT_CHARS` before treating a paper as
abstract-only. Record the actual HTTP status or exception in a `fetch_note` column; make `blocked`
retryable with a post-run sweep.

**Size units by WORK, not by paper count.** The driver's `plan_parts()` raises a chunk's part count
until no unit carries more than `pinned.max_fulltext_per_unit` (12) papers expected to yield full
text (roster `pmcid` + `is_oa`, or a `fulltext_expected` column). Measured: 25/25 full-text papers as
one unit exceeded `WORKER_MAX_S=3000` with nothing saved (the worker saves all-or-nothing); the same
papers as 3 parts took 700–1184 s each. A watchdog-reclaimed unit is re-dispatched SPLIT
(`pinned.watchdog_resplit`), exactly like a ceiling kill — re-queuing the same shape re-queues the
same timeout. Pass `roster_df=` to `driver.run()` (a DataFrame or a plain list of dict rows; the
orchestration kernel is stdlib-only, so export the roster's `chunk_id/pmcid/is_oa` as JSON).

**Per-chunk gate.** Yield >= `MIN_YIELD`, blocked <= `MAX_BLOCKED`, warn at `WARN_BLOCKED`.
`blocked` is a **distinct stratum** — neither full-text nor abstract-only — and must be binned
separately in every later audit. A chunk failing the floor for an access reason will not improve
on re-run; do not treat it as an extraction-quality failure.

**Empty output with `parse_fail: 0` and `blocked: 0` is a quota signature, not a real negative.**
Retry it. One such chunk returned 694 findings on re-run and would otherwise have been silently
missing.

**Gate:** `driver.verify_pinned_params("pipeline/pipeline.py")` all `ok`; pilot chunk (4 units)
passes yield and blocked gates with 0 ceiling events, 0 guard violations, 0 missing archives; only
then dispatch waves of 6–7. Record per exclusion *which*
defect class it is — a final screening summary that left 121 of 194 exclusions unclassified could
not support the claim made about them in prose.

---

## Stage 5 — Resolve identifiers from names

`pipeline/resolve_stage.py` applies `pipeline/resolver.py` (multi-authority, exact-label, records the
minting ontology) to the merged findings table with a persistent cache (`resolution_cache.parquet`,
HPO terms memoised under the reserved kind `__hpo_term__`) and writes the resolution rate BY
AUTHORITY with numerator/denominator/basis. It also re-tags `hpo_id_source` truthfully: the
in-pipeline `ground_findings_df` still emits the bare `authority_lookup_ols_hp` tag, and invariant 9
fired on those worker columns on the EDS dry run (1 MPATH id) — the stage's `_via_<mint>` tag is what
passes. `helpers.reject_id_shaped()` is the parser guard. The whole stage in three rules:

1. **Correspondence, not existence.** "Does this id exist?" is nearly worthless — dense id spaces
   mean almost any plausible integer exists. Ask "does this id denote THIS entity?" by comparing
   the authority's own returned label to the name.
2. **Exact label agreement only.** Repairing ids via fuzzy match produced 303 exact and 345 fuzzy
   matches; adjudicating the fuzzy set found **59% wrong** — the same defect with a new mechanism
   and a false "authority verified" provenance trail. Fuzzy matches are candidates for
   adjudication, never accepted.
3. **A wrong value is worse than a missing one.** The tier that nulled more aggressively was the
   more accurate one (0.55% vs 3.8% residual error). Absent is honest; wrong corrupts joins
   silently.

Record the minting ontology rather than prefix-constraining foreign ids. Measurement beat the
handoff's instruction here: all 21 offending foreign-prefix ids re-verified with exact label
agreement, so constraining would have discarded 49 correct ids.

Name the metrics for what they measure: `id_parse_rate`, `id_exists_rate`,
`id_denotes_entity_rate`. **`id_resolved_frac` is a coverage metric and ranks tiers backwards** —
the tier with the most ids had nearly the worst correctness (0.708) and the tier with the fewest
had the best (0.998).

**Gate:** every id carries an authority + match-mode tag; correspondence audit run on the full
population (it needs zero API calls if the resolver stored labels).

---

## Stage 6 — QA after every merge

Modules: `pipeline/invariants.py` (+ `invariants_ext.py`), `pipeline/quote_census.py`,
`pipeline/quote_trace.py`. Tests: `normalizer_tests.py`, `comparator_symmetry_tests.py`,
`invariants_ext.run_unit_tests()`.

1. **Unit-test the normalizer BEFORE measuring anything.** Five alarming numbers on the prior
   build were the measuring instrument, not the corpus: tags stripped to `''` glueing words
   (`diseaseThe`), HTML entities not double-unescaped (`&#x3b1;1` never matching `α1`), a regex
   `r'<[^>]+>'` eating `(P < .05). Eight participants improved >` as one tag, an
   over-strict `len(docs)==1` check, and the `Accept`-header bug. Keep the test file with one case
   per historical bug.
2. **Quote census with a negative control**, per stratum (abstract-only / retrieved-full-text /
   blocked). Where the source text is stored, run a **census, not a sample**. Measured across four
   tiers: 94.24 / 94.57 / 96.79 / 99.09% traceable, wrong-paper control **0.0% in every tier**.
   **Normalize both sides, never one.** Run `comparator_symmetry_tests.py` (8 cases: coded quote vs
   clean source, clean vs coded, double-escaped, literal nbsp/thin-space, superscripts, a negative
   control) before any census or retrofit. The one-sided bug found on 2026-09-16 lived in the
   AI-judge harness, which normalized the source and not the quote — not in the census, whose
   `quote_traceability()` was symmetric by construction: the full Tier-4 census was 15,504/15,646
   row-identical on raw and unescaped archive, control 0/400.
3. **Do not normalize superscripts.** `I²` → `I2` dropped a census from 99.52% to 99.04%.
4. **Invariants**, run after every merge: row conservation, id provenance present, no
   monotonicity break, no duplicate rows, counts summing. Unit-test the invariant module itself
   with adversarial cases — one invariant had its own defect, unioning a per-id tag list into one
   allowed set and rejecting a truthful positional declaration.
5. **Assert parse-completeness on every LLM adjudication**: `assert len(parsed) == len(input)`, and
   the response parser must **raise** on a missing key, never skip it. A 70-item batch that parsed
   50 inverted the verdict distribution (42% → 21%). The calibration harness committed the same
   error while being built for it: the judge renamed one question key and an exact-key parser
   dropped it, so Q9 read as "untested" (`calibration/fidelity_calibration.py::parse_judge` now
   matches on the `Q<N>` prefix and raises). Unparsed is missing data, not a neutral omission.
6. **Derive every number in every document.** Two errors came from typing a total instead of
   computing it. `pipeline/verify_package.py` re-derives every headline number from the shipped
   parquet; a doc that hand-maintains a count will drift the moment the data does.

**If a checker seems to need changing, suspect the data convention first.** One session invented a
new provenance-tag scheme, which then forced a modification to a pinned invariant — the corpus
already shipped a convention that passed the unmodified checker.

**Gate:** invariants run and their results saved as JSON; failures **declared in the README**, not
fixed by loosening the check.

---

## Stage 7 — The next tier

The prior tier is **immutable** (`annotations/README.md` for how later information is recorded): no edits, no re-runs, no
"improvements" to its numbers. A combined figure gets a new name and its own stated basis.

Three things drift across tiers and must be watched:

- **`entity_kind` vocabularies diverge.** Three tiers used three different kind vocabularies and
  one had no such column at all. Reconcile once, at the end, not per tier.
- **Same-name/different-semantics columns.** One column meant *id kept* in one tier and
  *id nulled* in another, so the obvious filter silently dropped 2,242 valid rows. Another name
  carried two unrelated concepts (abbreviation-collision rows vs screening-uncertain rows). Hunt
  these deliberately after every merge; separate the concepts and never let one name carry two.
- **Findings-per-paper differences are usually composition.** 13.67 vs 16.38 looked like a
  regression; per stratum the rates matched (6.44/6.48 abstract-only, 20.31/20.85 full-text) — one
  tier was simply 62% abstract-only vs 29%. Use `stratified_rate()` before calling anything a
  regression.

**Gate:** `helpers.assert_partition()` passes across every pair of tiers; per-tier counts sum exactly to
the corpus total.

---

## Stage 8 — Cross-tier reconciliation and vocabulary

Now, once, across all tiers:

**Comparability report.** List, explicitly: what is safe to pool, what is not, and why. The prior
build ended with 11 metrics not comparable across four tiers, and shipped that list as a
first-class document.

**Vocabulary canonicalization.** Deferred to here on purpose — 97% of each new tier's entity
strings are new. Rules:

- **Never `str.lower()`.** `gene_case_collision_check()` first: ALLCAPS/Titlecase symbol pairs are
  human/mouse orthologs under HGNC/MGI nomenclature (`MECP2`/`Mecp2`, `CNTNAP2`/`Cntnap2`,
  `PIEZO2`/`Piezo2`). Rows labelled `CNTNAP2` quote "Cntnap2–/– mice". A default lowercasing
  canonical function would have merged 11 such pairs.
- **Gene-symbol protection must be evidence-only** — build the protected lexicon from rows whose
  gene actually resolved against an authority, plus authoritatively-kinded entities. Never a model
  judgement.
- **Plant controls and report both directions.** The prior pass: 0/25 false merges, 2,459/2,460
  correct merges. The single miss was a kind-conflict guard conservatively refusing to merge an
  antibody with its gene — a split, never a wrong merge. Prefer that asymmetry.
- Canonicalization is a **lookup table**; it mutates no corpus row.

**Gate:** planted controls reported in both directions; zero false merges.

---

## Stage 9 — Package for deposit

See `PACKAGING.md`. `build_packages.py --area-config config/<area>.yaml` (→ one self-contained
package via `packaging/generic_builder.py`; the legacy `--config` mode is the POTS five-package
reproduction) is the builder; save it as a first-class artifact and regenerate `CHECKSUMS.sha256` by script, never by
hand. `pipeline/verify_package.py --self-test` in every folder is the gate.

---

## Stage 10 — The derived layer (site, exports, dashboards)

The corpus is the source of truth; anything generated from it — a website's JSON, exported CSVs,
cached rollups — is a **derived layer** and must be regenerable. Before writing over any generated
output whose producing code you did not personally write, apply the fidelity gate below.

The move that matters: **regenerate the ALREADY-SHIPPED output with your reconstructed generator
and prove it matches, before you point that generator at new data.** Schema inspection does not
reveal the failures this catches.

Two rules earned here:

- **A "do not touch tier 1" rule protects the scientific record, not every file.** Derived and
  presentation layers are regenerable and may be rebuilt freely. When a freeze rule seems to block
  something useful, ask what the rule is protecting — that reframing is what unblocked a rebuild.
- **Verify the assembled artifact independently**, never from the producing agent's self-report. A
  wrong version (863 rows instead of 2,803) was assembled into a deliverable and caught only by
  checking the output itself.

Ship the guardrails alongside the output: a rebuild script, a verifier, and a written contract
naming what is safe to edit by hand. The person maintaining the site should not have to guess.

## Stage 11 — Fidelity

See `FIDELITY.md`. This is the gate between "well-formed" and "correct", it needs a human domain
reader, and nothing else in this runbook substitutes for it. Before any human time is spent, run
the AI-scorer calibration (`calibration/CALIBRATION_HARNESS.md`): a planted-defect arm plus clean
controls decides which questions a machine may take, and it found an instrument defect (the
undefined `condition` buckets) in the human worksheet itself.

---

## Handoff between sessions

A build spans many sessions. Write a **handoff document** at the end of each one containing:
current state per tier as a table; the exact resume snippet with pinned version ids and the
literal model id (`model.id` in the config — "a current reasoning model" is not a record); what is
half-done and *why* (a transient connection error and a token-ceiling kill demand different
responses); the pinned parameter block; lessons from that session; and next actions in value
order. Supersede old handoffs explicitly — several stale per-tier handoffs coexisting is its own
failure mode.

**Two kernels.** The driver runs where `host.delegate` lives — a stdlib-only kernel (no pyyaml,
no pandas). Prepare `config/<area>.json` (`python pipeline/config.py export-json`) and the roster
plan JSON in the analysis kernel; the driver reads those. Save the driver's logs per run
(`<prefix>gate_log_parts_<run_tag>.json`); a driver cell that dies (EDS dry run: `host.collect`
became unavailable after ~110 min) leaves in-flight children without a watchdog — check
`host.children()` after any driver error.

**Make every dispatch cell self-contained** — re-exec the driver, redefine the helpers. A wave
silently did nothing for 146 minutes because a kernel restart between cells erased the wave
function (`NameError`, discovered only at the cell's completion). And **verify children spawned**,
not merely that the cell started.
