# FIDELITY — the only instrument that tests whether the science is right

Part of `condition-findings-builder`. Everything else in this repository verifies **well-formedness**: that a quote is real text from the
right paper, that an identifier denotes its entity, that counts are conserved. All of it can pass
while a finding misstates what the paper concluded. This is the audit that answers that question,
and it is the gate on publishing a corpus as anything other than a pointer set.

**It cannot be run by a model alone.** An LLM adjudicator shares the extractor's failure modes, so
agreement between them is uninformative — it would be grading its own homework. Parts of the
instrument *can* be machine-extended, but only after the calibration below, and Q3 and Q10 never.
Any report that uses machine scoring states which questions, with the measured agreement rate
attached to every machine-extended number.

## Draw the sample

200 findings, stratified, seed recorded. Draw only from papers whose source a scorer can actually
reach (open-access, or source text you retained). Strata used on the prior build, with why each
exists:

| stratum | n | why |
|---|---:|---|
| core full-text | 60 | the main stratum; source re-fetchable |
| core abstract-only | 40 | the abstract IS the complete source |
| numbers not in quote | 40 | the largest measured defect class — is it fabrication or narrow quoting? |
| earliest extraction wave | 25 | wave-to-wave comparison |
| mechanistic//in-vitro | 20 | tier-justification check |
| blocked-fetch | 15 | a distinct access stratum, extracted at abstract depth |

Adapt the strata to the defects *your* build measured. A stratum exists to answer a question you
already have, not for symmetry.

## The instrument — 10 questions per finding

Score against the **source paper**, not against the extracted row. (The POTS Tier-4 protocol; the
Tier-2 version had seven.)

1. **quote_verbatim** — does the provenance quote appear in the paper as written? (Y / N / reformatted)
2. **direction_correct** — is the stated direction what the paper reports? (Y / N)
3. **quote_supports_finding** — does *that* quote support *that* finding, or is the finding drawn
   from elsewhere / overreaching what the sentence says? (Y / partial / N)
4. **tier_justified** — does the evidence tier match the actual study design? (Y / N)
5. **causality_not_overstated** — is the causal-vs-correlational field honest about what was shown? (Y / N)
6. **numbers_correct** — are the quantitative values correct per the paper; if absent from the
   quote, are they at least correct? (Y / N / NA)
7. **condition_correct** — is the condition label the right **bucket** for this finding, judged
   against the WRITTEN bucket definitions and the population-vs-relevance statement in the config?
   (Y / N). The worksheet must print those definitions; without them the question is unanswerable
   consistently (see calibration result below).
8. **entity_id_correct** — does the resolved identifier's authority label denote what the entity
   name means in this paper? (Y / N / NA)
9. **hpo_terms_complete_and_correct** — do the verified phenotype labels capture the phenotypes the
   finding describes — none wrong, none missing? (Y / partial / N / NA)
10. **paper_in_scope** — is the paper genuinely in scope for the area? (Y / N / NA; answered for
    the screen-uncertain stratum, NA elsewhere)

## Pre-register the thresholds BEFORE scoring

Written down first, so a result cannot be rationalised afterwards.

| question | blocking threshold | rationale |
|---|---|---|
| Q2 direction | < 95% in any stratum | a flipped direction inverts the science |
| Q3 support | < 90% overall | this is the provenance contract |
| Q6 numbers | < 90% in the numbers-not-in-quote stratum | decides fabrication vs narrow quoting |
| Q8 entity id | < 95% in the id-resolved stratum | the name→authority design must hold |
| Q9 phenotype terms | < 90% in the multi-term stratum | wrong recovered terms are added noise |
| Q10 in scope | < 85% in the screen-uncertain stratum | tests the extract-uncertain policy |
| Q1, Q4, Q5, Q7 | < 85% | quality signal; not release-blocking alone |

A blocking failure means: **do not publish that stratum.** Remediate and re-audit. If the failure
is prompt-level it will affect every later tier too — which is an argument for fixing the prompt
now, not for delaying the next tier.

## Calibrate any AI scorer first — `calibration/`

Before a human reads a paper, run `calibration/fidelity_calibration.py` (how-to in
`calibration/CALIBRATION_HARNESS.md`). Two arms:

- **Planted arm** — inject one known defect class per question into shipped rows (8 classes:
  quote swapped in from another paper → Q1, direction flipped → Q2, tier upgraded → Q4,
  correlational upgraded to causal → Q5, digits perturbed → Q6, condition relabelled → Q7,
  authority label swapped for nonsense → Q8, wrong phenotype appended → Q9) plus unmutated
  **clean controls**. Measures catch rate per class and flag rate on clean rows. Needs no human
  scores. The judge reads source text fetched **fresh**, never the retention archive.
- **Agreement arm** — the judge scores the same rows the human scored; per-question agreement.

Rules the POTS pilot (48 calls; 8/8 classes caught at 3/3; 25 flags on 16/24 clean rows) paid for:

1. **Report `flag_rate_unadjudicated`, never a false-positive rate.** A flag on a clean row is one
   of three things the harness cannot separate: a judge error, a real defect the corpus shipped
   (three plausible direction errors were found this way), or a fetch artifact (the entity-code
   quotes). No question is declared machine-extendable while it has unadjudicated flags, however
   high its catch rate; adjudicating them (~30 min for 25) is the cheapest human work available.
2. **Questions in a shared prompt are not independent.** Rewording Q7 alone moved Q2's flag rate
   0/24 → 3/24. Re-run the *whole* control set whenever any one question is reworded.
3. **The parser raises on a missing question key.** The judge renamed `Q9_…` and an exact-key
   parser dropped it silently, so Q9 read as "untested" — the lesson-4 error committed inside the
   instrument built for it. `parse_judge()` matches on the `Q<N>` prefix and raises.
4. **Q3 (quote supports finding) and Q10 (scope) stay human.** Q3 is the same reading operation
   the extractor performed — maximum correlation — and is the provenance contract; Q10 is a
   judgement about corpus design with no ground truth to plant against.
5. **Q7 and Q8 are machine-extendable only with the bucket definitions in the prompt.** Same
   question, same 24 rows, same model: 10/24 flagged with no definitions, 0/24 with a definition
   later found wrong (MCAS "includes mastocytosis"), 3/24 — exactly the three mastocytosis papers —
   with the decided one. `fidelity_calibration.configure(cfg)` renders Q7 from the config's
   definitions so the human worksheet and the judge see the same text.
6. **Pin the judge model id** in the report, as for the extractor (the POTS pilot: `claude-sonnet-5`).

Real-run size (`required_n()`): a flag rate bounded to 10% ± 5pp and a catch-rate lower bound of
80% need ~138 clean controls and ~62 per defect class (~634 judge calls). The pilot's 3 per class
gives a 43.8–100% interval — directional signal only.

## Effort and logistics

Roughly **2–4 minutes per finding with the paper open** — 200 findings ≈ 7–13 hours all-human.
With Q1/Q4/Q5/Q6/Q7/Q8 machine-extended after adjudication, the human scores a calibration subset
plus Q2/Q3/Q10 and adjudicates ~25–150 flags: 2–4 hours. It splits cleanly by stratum across
several readers, and a per-stratum worksheet is the natural unit of work. Ship the worksheet as a
CSV: one row per finding, `source_url` included, the Q columns blank, the bucket definitions
printed in the header. Scoring, per-stratum rates, confidence intervals and threshold verdicts are computed from
the returned file.

## What a clean result does and does not license

**Does:** publishing the corpus with a measured fidelity rate per stratum.

**Does not:** claiming that any LLM-adjudicated figure elsewhere in the QA — identifier residual
error rates, quote-residual classifications, fuzzy-match verdicts — is human-verified. Those
remain LLM-adjudicated estimates unless this audit covers them explicitly.

Until it is scored, the corpus README must say plainly that scientific fidelity is unmeasured, and
say it **above** the data, not in a footnote.
