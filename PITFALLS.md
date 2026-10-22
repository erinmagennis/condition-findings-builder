# PITFALLS — the failure catalogue

Part of `condition-findings-builder`. Read this before dispatching anything expensive. Every entry
happened on the POTS build. Ordered by how much work the mistake cost.

## 1 · Orchestration and dispatch

**The token ceiling is per-frame and CUMULATIVE over the frame's whole life — including the
orchestrator's.** A 577-paper screen run as one in-frame fan-out breached the 2.0M ceiling at
1.693M, and the reflexive retry of the failed 181 added another 0.625M. Re-issued as three
concurrent units of ~192 it finished in 11 minutes. **Unit-dispatch ALL per-record LLM work** —
screening, adjudication and annotation, not just extraction. Size it first:
`n_records x (prompt_tokens + max_tokens)` vs remaining headroom (`plan_units()`).

**`max_tokens >= 2000` for any model emitting JSON.** At 600, 180 responses came back empty with
`stop_reason="max_tokens"`: budget spent, nothing returned.

**Make every dispatch cell self-contained.** Re-exec the driver, redefine the helpers. A wave did
nothing for 146 minutes — `NameError: name 'wave' is not defined`, because the kernel restarted
between cells. The cell was "running" the whole time.

**Verify children SPAWNED, not that the cell started.** A running cell is not evidence of
dispatched work.

**Distinguish a ceiling kill from a transient connection error.** One worker died after 13
consecutive connection failures; that is retryable and must never count toward a budget halt.
A ceiling kill means re-unitize.

**Derive `done_units` from artifact existence, and make it part-aware.** Never auto-requeue.
Artifact-existence derivation is what prevents the duplicate-dispatch loop; part-awareness is what
lets a half-done chunk re-run only its missing half.

**Never `range(N)` for target chunks.** An explicit list only (`tier.target_chunks`; the config
validator rejects anything else). A driver with `N_CHUNKS=286` would have dispatched three unasked
tiers.

**Pin the literal model id.** `model.id` in the config is the string recorded in every package's
METHODS (`claude-sonnet-5` for the POTS build) and confirmed by `driver.verify_pinned_params()`. A
run card that says "a current reasoning model" cannot be reproduced or compared.

## 2 · Identifiers

**Never ask a model for an arbitrary identifier.** 27.7% of ontology ids and ~34% of phenotype ids
were fabricated: `anaphylaxis` → *Liver abscess*, `D-dimer` → *Dog Diseases*, `IgE` → *Iguanas*,
`vivid dreams` → *Increased body fat percentage*. The prompt said "do NOT invent — leave null if
unsure", twice. It cannot work: there is no reasoning path from a name to an arbitrary integer,
and a model has no introspective signal for "do I know this integer?". Repairing this cost more
than the original extraction.

**Model-emitted fields split cleanly by mechanism:**

| field type | examples | measured accuracy |
|---|---|---|
| **RECALL** (arbitrary ids) | HGNC, ChEBI, MeSH, GO, HPO ids | 21–34% **wrong** |
| **COPY** (transcribed from visible text) | quotes, entity names, accessions, PMIDs | 96–99% right |

A dataset-accession field validated against *nothing* was 98.6% real — because accessions get
copied out of data-availability statements. The mechanism predicts the error rate better than any
amount of validation does.

**Validating existence is nearly worthless.** The resolver called real authorities and asked "does
this id exist?"; in dense id spaces a fabricated plausible integer usually does. Worse: the
authority's own label was **already stored in the table** and never compared to the entity name.
The evidence to catch a 27.7% defect sat in the data the whole time, and the check turned out to
be a full-population audit with zero API calls.

**Fuzzy matching recreates the defect you just fixed.** 59% of fuzzy authority matches were wrong.
Accepting them swaps model-recalled wrong ids for authority-retrieved wrong ids — same defect, new
mechanism, now with a false "authority verified" provenance trail.

**A metric that cannot fail when the data is wrong is not a quality metric.** "96.8% grounding
resolution" measured parse-and-exist. Name metrics for what they measure.

## 3 · Measuring your own work

**Suspect your own code before the data.** Five alarming numbers were the instrument:

| symptom | actual cause |
|---|---|
| 8% of quotes "missing" from abstracts | tags stripped to `''` glued words: `diseaseThe` |
| still 8% missing | HTML entities not double-unescaped: `&#x3b1;1` never matched `α1` |
| still 5.8% missing | `r'<[^>]+>'` ate `(P < .05). Eight participants improved >` as one tag |
| an ontology resolved only 12% | code required `len(docs)==1`; valid terms return several |
| "0 of 24 papers have full text" | sent `Accept: application/json` to an XML endpoint |

The last one also cost the production pipeline 57 papers, mislabelled `blocked`.

**The stopping rule.** Stop fixing a matcher when the instrument is *verified*, not when the
residual becomes *explainable*. Three successive fixes all moving the number the same direction is
evidence you are still climbing out of a hole — it is easy to read that as convergence.

**Every quality metric gets a negative control.** Pair quotes with the WRONG paper; a sound matcher
scores ~0%. Measured 0.0% vs 98.7% — that separation is what makes the headline believable.
A traceability check once read a clean 100% because the caller did `if tr:` on a truthy tuple, and
only the wrong-paper control (reading 400/400) exposed it.

**Measurement code needs the same tests as analysis code.** "Validate against ground truth" was
applied to the data and "looks reasonable" to the measuring code.

**Normalize both sides of a comparison, never one — and never repair the stored side.** 128
shipped quotes (0.09%) carry raw HTML entity codes because the model was fed un-unescaped Europe
PMC text; the retained archive carries the same codes and they trace 42/42 against it. Freshly
fetched text is clean, so a comparator that normalizes only the source reports all 128 as
fabricated — which is what the AI-judge harness did (the census, whose `quote_traceability()`
normalizes both sides, was unaffected: 15,504/15,646 row-identical on raw vs unescaped archive).
The obvious "repair" — unescape the stored quote — took the 42 Tier-4 rows from 42/42 to 0/42
traceable against their own archive. Fix upstream for new builds (`unescape_source_text: true`),
normalize at comparison time (`quote_trace.normalize`, pinned by `comparator_symmetry_tests.py`),
and state the limitation in the data dictionary for rows already shipped.

**An AI scorer is an instrument and must be calibrated before human time is spent.** Plant one
defect class per question (8 classes on the POTS pilot) plus clean controls, and report the flag
rate on clean rows as `flag_rate_unadjudicated` — a flag on an unmutated row is a judge error, a
real shipped defect, or a fetch artifact, and the harness cannot tell them apart. Questions in a
shared prompt are not independent: rewording Q7 moved Q2's flag rate 0/24 → 3/24, so re-run the
whole control set when any one question changes. Details in `FIDELITY.md` and
`calibration/CALIBRATION_HARNESS.md`.

**An unparsed LLM response is missing data, not a neutral omission.** A 70-item adjudication parsed
50; the partial sample said 42% of flagged conflicts were fine, the complete run said 21%. The 20
unparsed items were not a random subset. `assert len(parsed) == len(input)`, small batches, and an
explicit "exactly one line per item" instruction.

**Distinguish a stalled worker from a failed one.** `total_out_tokens: 0` with `parse_fail: 0` and
`blocked: 0` is a quota signature, not a real negative. Retry: one such chunk yielded 694 findings.

## 4 · Numbers, bases and prose

**State the denominator.** The completed tier's coverage was simultaneously **85.97% of
screened-in papers and 74.57% of the pre-screening universe**; mid-run, with chunks still
undispatched, an of-dispatched basis sat above both and then collapsed into the screened-in one as
the last chunks landed. All true; none is "the" coverage. A figure quoted from a mid-run state
goes stale silently — recompute it at completion.

**Never hardcode a computed number into a document.** "52 of 57" was actually 55; a fact count said
29 after the registry grew to 31; a paper-accounting line omitted a reconciling term so a
subtraction looked like it did not close. Derive every number and `assert` the parts sum to the
whole. This is the same error as a fabricated identifier, applied to your own prose.

**Recompute before quoting, especially your own numbers.** A 64-chunk figure was quoted as a
95-chunk total; a subset findings count was quoted as the corpus count; a findings-per-paper
figure counted papers where the metric counted findings.

**Never reconstruct the reason for a discrepancy from memory.** A `7/10` validation was reframed as
`7/7` with an explanation that was never checked — the authority had actually returned data for
10/10 and the real reason was different. Inventing a plausible reconciliation is the same error as
inventing an identifier.

**Report bases, never bare percentages.** Three yield figures with three different denominators
were quoted interchangeably, making progress look like quality.

**Report a fail-then-fix sequence, not just the end state.** "13/13 verification passed" conflated
two runs with different check counts, one of which had failed 12/13 before a fix.

**Check the repo before assigning the user homework, and inspect before scoping.** Two action items
handed over were already done — one was already in the shipped `.gitignore`, one was a page that
existed and was complete.

**Verify the assembled artifact independently.** A wrong artifact version (863 rows instead of
2,803) was assembled into a deliverable and caught only because the output was verified rather
than trusted from the producing agent's self-report. When resolving a sub-agent's output by
filename, take the **latest** version — an id captured mid-run may be stale.

## 5 · Schema and vocabulary

**The schema can be the defect.** 63% of symptom strings described multiple phenotypes
(`"chronic musculoskeletal pain, trauma, disturbed proprioception"`); a one-id-per-row schema could
not express them, so 5,710 findings carried no phenotype id at all — invisible as a defect because
the field was simply null. Splitting into atomic terms and allowing multiple ids took coverage from
1,495 to 6,928 rows, all authority-verified. **Before repairing values, check whether the field's
shape can hold the truth.**

**Never `str.lower()` entity strings.** 320 case-fragmented groups existed, 11 of them
human/mouse ortholog symbol pairs. Rows labelled `CNTNAP2` quote "Cntnap2–/– mice". A default
lowercasing canonical function merges a mouse-knockout finding into the human gene.
`helpers.gene_case_collision_check()` before any canonicalization.

**Hunt same-name/different-semantics columns after every merge.** One column meant *id kept* in one
tier and *id nulled* in another — the obvious filter silently dropped 2,242 valid rows. One name
carried two unrelated concepts (abbreviation-collision rows vs screening-uncertain rows). An
alignment pass found eight more. Separate the concepts; never let one name carry two.

**A bucket enum without written definitions is a defect waiting to be measured.** The POTS
`MCAS` bucket (1,083 papers) was ~69% mast-cell biology with no MCAS population, ~10% MCAS as
later defined, ~5% mastocytosis — because nobody had written what `MCAS` meant or whether
`condition` meant the population studied or the condition informed. The same undefined enum made
an AI scorer's flag rate on 24 clean rows swing 41.7% → 0% → 12.5% on wording alone. Write the
definitions and the semantics decision into the config before extraction (RUNBOOK stage 0 C);
record later relabels additively (`annotations/README.md`).

**Enum drift happens past a pinned constraint** — 0.055% out-of-enum values with both enums pinned
in the prompt. Use a validator that **flags and never coerces**, with a `NEEDS_HUMAN` class for
values that must not be auto-remapped. Two such values recurred: a category for findings about
other diseases used as mechanistic analogies (the enum is deficient, not the output), and a
probable typo that must not be silently corrected.

**Drop fields the data cannot support.** Only 14 of 147 mechanism strings were real ontology terms;
the rest were prose no single term denotes. A field at 66% accuracy with nothing marking it is
worse than no field plus correct free text.

## 6 · Screening and the draw

**A closed list reads as exhaustive.** See `RUNBOOK.md` Stage 0. 303 vs 20 off-topic.

**Absence of a keyword is not evidence of wrong sense.** A rule `no_anchor` must never override an
affirmative LLM `in_scope` verdict carrying a verbatim span.

**Arm disagreement is an instrument fault.** The 303-vs-20 gap was the tell that the prompt was
broken — not that the papers were off topic.

**Abbreviation contamination is a draw defect, not an extraction defect.** 28 papers entered on an
ambiguous abbreviation; 22 correctly yielded nothing, 6 yielded 40 well-formed findings from
off-topic papers. At 0.15% of rows they change no aggregate, but they must be flagged before
anyone counts markers per condition. **Flag with a column; do not delete** — the exclusion stays
visible, auditable and reversible.

**A no-yield set can be a query problem, not a yield problem.** 162 papers returned zero findings.
An LLM triage (with a control: 15 known-yielding papers scored 93%) said 22% looked extractable —
but 0 of 9 of those were on topic. They were spinal-surgery papers. Re-dispatching them would have
spent a work unit extracting the wrong subject matter. Check *relevance* before concluding
recoverable coverage exists.

**Blocked papers are a third stratum**, neither full-text nor abstract-only. Binning on
"full-text vs everything else" silently pools them into abstract-only.

## 7 · Evidence versus instruction

**Evidence beats a handoff instruction — but measure first.** A handoff said to prefix-constrain
foreign-ontology ids; measurement showed all 21 offenders were correct with exact label agreement,
so constraining would have discarded 49 verified ids. The instruction was followed *after* being
tested, not before.

**If a checker seems to need changing, suspect the data convention first.** A session invented a
provenance-tag scheme, which then forced modifying a pinned invariant. The corpus already shipped
a convention that passed the unmodified checker.

**Do not tune a suite until it passes.** Three of the five packages declare invariant
failures. Those declarations are why the passing numbers are believable.

**Regenerate checksums by script, never by hand, and save the packaging script as an artifact.**
The POTS packages were built by an inline cell that was never saved; the packaging step was
therefore not reproducible and is being reconstructed as `build_packages.py`. A hand-edited
`CHECKSUMS.sha256` drifts the first time any sidecar is backfilled.

**Adversarial review earns its keep — and needs checking too.** An automated reviewer caught an
invariant flag set as a literal instead of derived, a field-subset comparison producing 18
false-positive duplicate groups, and a statistic attributed to the wrong population. It was also
wrong sometimes, flagging work as unsupported when the evidence existed in artifacts it could not
see. Verify by reading the artifact, not by arguing from memory.

**Give a sub-agent something to be inconsistent with.** A delegated repair was told a step was
"free and correct"; it gated the step anyway and found that a gene field did not always denote the
row's entity, nulling 197 rows instead of propagating a mismatch. Another found a matcher bug worth
+6.9pp because being *below* a stated benchmark forced it to dig.

## 8 · Environment

- 8 GB machine, frequently under 0.5 GB free. Load parquet columns selectively, batch, stream large
  arrays to disk, and check available memory before a large allocation.
- The control-plane kernel could not read the project parquets (`PermissionError` on the analysis
  interpreter's files). Read data from an analysis cell in the project env, not from the
  orchestration kernel. This broke a live yield gate once.
- Sub-agents run in their own working directories. Pass files by artifact marker; retrieve outputs
  from the artifact store by filename, resolving the latest version.
- Europe PMC rate-limits on **aggregate** concurrency across workers, not per worker.


## 9 · Second-condition dry run (EDS/HSD, 2026-09-30) — what config could not fix

Each item was found by running the repository on a second area with the POTS build's own rules,
and is fixed in code with a test; details and numbers in `DRYRUN_EDS_REPORT.md`.

1. **Artifact-store namespace.** The done-detector searched the project for `chunk_NNN_findings.parquet`
   and found the POTS build's 245 chunks; an EDS run at chunk ids 0–2 was "already done" before it
   started. Fix: `artifacts.file_prefix` (default `<slug>_`) in every chunk filename and both
   detector regexes, anchored at both ends.
2. **The orchestration kernel is stdlib-only.** `import yaml` at module level made the driver
   unimportable where `host.delegate` lives. Fix: lazy import + `.json` config twin.
3. **Units sized by count, not work.** 25/25 full-text papers as one unit exceeded `WORKER_MAX_S`
   with nothing saved; the watchdog re-queued the identical shape twice. Fix: `plan_parts()`
   (`max_fulltext_per_unit`), watchdog → re-split. The POTS fitted cost model under-projected output
   tokens by 1.49x on this full-text-dense roster (1,017,779 measured vs 684,953 projected).
4. **Tags in Europe PMC abstracts.** `<i>PLOD1</i>-related` broke 4/75 verbatim span checks; the
   census normalizer never stripped tags while the test file tested one that did. Fix: one
   `strip_tags` (inline tags → '', block tags → space) shared by pipeline, screen and tests.
5. **Regex contamination filters over-exclude.** 8/10 "excluded" records were genuine EDS papers.
   Flag, never exclude; the screen adjudicates.
6. **The packaging script was the POTS build, not the method.** `build_packages.py` failed at
   `partition()` on `tier_source`; `PACKAGING.md` claimed a generic interface that did not exist.
   Fix: `--area-config` generic single-package mode.
7. **Two runs, one log file.** `gate_log_parts.json` was clobbered by the second driver run.
   Fix: per-run filenames.
8. **`blocked` folded into the full-text census stratum**, contradicting stage 4's "distinct
   stratum" rule. Fix: three strata; a <2-paper stratum says "no control possible" rather than
   "matcher suspect".
