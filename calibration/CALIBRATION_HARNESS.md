# CALIBRATION_HARNESS — calibrating an AI fidelity scorer

`fidelity_calibration.py` decides, per question of the `FIDELITY.md` instrument, whether a
language-model judge may score it. It is run **before** any human reads a paper. It came out of
the POTS Tier-4 pilot (2026-09-15/16; 48 judge calls; judge model `claude-sonnet-5`), and its own
controls found two defects in the harness and one in the human worksheet.

## What it needs

- a findings table (`pandas.DataFrame`) with the `ROW_FIELDS` columns plus `pmid` and an
  `audit_id`;
- `srcmap: {pmid: source_text}` — source text **fetched fresh** from Europe PMC (abstract, or
  full text where open), passed through `normalize_source()`. Never the retention archive: the
  judge must not read the same bytes the extractor read;
- the area config (`pipeline/config.py`), so Q7 and Q10 carry the WRITTEN bucket definitions and
  the area families;
- an orchestration environment that provides `host.llm` (list form, `max_concurrency`) and
  `host.reasoning_model()`; or pass `model=` with the literal id you will record.

```python
import sys; sys.path += ["pipeline", "calibration"]
from config import load_config
import fidelity_calibration as F

cfg = load_config("config/my_area.yaml")
q7_text, q10_text = F.configure(cfg)          # record both texts in the report
```

## Arm 1 — planted defects + clean controls (no human scores needed)

```python
rows, truth = F.make_planted_set(df, srcmap, per_defect=3, n_clean=24, seed=20260915)
res = F.run_judge(host, rows, srcmap, model="<literal judge model id>", max_concurrency=6)
assert res["complete"], res["errors"]         # every requested row parsed, or stop here
for rec, v in zip(truth, res["verdicts"]):
    rec["verdicts"] = v
catch, flags = F.analyze_planted(truth)
verdicts = F.machine_extendable(catch, flags, adjudicated_flags=False)
```

Eight defect classes, one per question that has a plantable defect: `quote_swap`→Q1,
`direction_flip`→Q2, `tier_upgrade`→Q4, `causal_upgrade`→Q5, `number_perturb`→Q6,
`condition_relabel`→Q7 (alternatives drawn from the config's enum), `entity_label_swap`→Q8,
`hpo_inject`→Q9. `plant()` returns `applicable=False` when a row cannot carry the defect (no
number to perturb, already causal, …) and the set-builder moves on.

`catch` gives the catch rate per class; `flags` gives, per question, the number of **clean**
control rows the judge flagged.

## `flag_rate_unadjudicated` — what it means and what it does not

The flag rate on clean rows is **not a false-positive rate**. A flag on an unmutated row is one
of three things, and nothing in the harness can tell them apart:

1. a **judge error**;
2. a **real defect the corpus shipped** — on the pilot, three Q2 flags were plausible direction
   errors (a row asserting *elevated* where the abstract says "significantly different"; an
   administered agent labelled as a disease-state elevation);
3. an **artifact of the source fetch** — the entity-code quotes (`p&#x202f;<&#x2009;0.001`),
   verbatim to what the extractor read but not to freshly fetched text; and quotes that run past
   what the abstract contains (abstract-depth extraction from a paper whose sentence continues).

So `machine_extendable()` returns `needs flag adjudication` for any question with a non-zero
flag rate until `adjudicated_flags=True` is passed — which you may only do after a human has
classified every flagged clean row (write them out with the judge's reason and a blank
`human_class` column; ~30 minutes for 25). Adjudicated judge errors become the false-positive
rate; adjudicated real defects go to the corpus's issue list; fetch artifacts are a comparator
symmetry problem (`pipeline/comparator_symmetry_tests.py`), never a stored-quote repair.

## Rules the pilot paid for

- **Questions in a shared prompt are not independent.** Rewording Q7 alone moved Q2's flag rate
  from 0/24 to 3/24. Re-run the *whole* control set whenever any one question is reworded; do not
  assume the other nine are unaffected.
- **The parser raises.** `parse_judge(strict=True)` matches keys on the `Q<N>` prefix (the judge
  renamed `Q9_hpo_terms_complete_and_correct` to `Q9_hpo_labels_verified` on the pilot) and
  raises when any question is missing. An exact-key parser had dropped Q9 silently and reported
  it as UNTESTED. `run_judge()` returns `complete=False` and the error list rather than a partial
  table.
- **Q7 needs the bucket definitions in the prompt.** Same 24 clean rows, same model, same source
  text: 10/24 flagged with the question undefined; 0/24 with a definition later found wrong
  (MCAS "includes mastocytosis", which suppressed three true positives); 3/24 — exactly the three
  mastocytosis-labelled-MCAS papers — with the decided definition. `configure(cfg)` renders the
  config's definitions into Q7; the human worksheet must print the same text.
- **Q3 and Q10 stay human.** Q3 (does the quote support the finding) is the same reading
  operation the extractor performed and is the provenance contract; Q10 (in scope) is a judgement
  about corpus design with no ground truth to plant. Neither has a defect class here by design.
- **Q10's clean denominator excludes screen-uncertain rows** — the corpus never claimed those
  papers were in scope, so a flag there is not a false positive (`analyze_planted` does this).
- **Symmetric normalization in the judge's view.** `build_judge_prompt` passes quote-like row
  fields through `normalize_source()` because the source text has been; the stored row is never
  altered. This was the one-sided bug the pilot found in its own harness.
- **Record the literal judge model id** in the report.

## Arm 2 — agreement with the human worksheet

After the human worksheet (`FIDELITY.md`) is returned:

```python
judge = {rec["audit_id"]: v for rec, v in zip(truth, res["verdicts"]) if v}
F.analyze_agreement(judge, human_df, id_col="audit_id")   # per-question exact-match agreement
```

A question is machine-extendable only if it clears all three: catch rate ≥ 90% on its defect
class, adjudicated false-positive rate ≤ 10%, and agreement with the human ≥ the pre-registered
floor. Every machine-extended number in a report carries its measured agreement rate.

## Size the real run

```python
F.required_n(target_flag_rate=0.10, half_width=0.05, catch_floor=0.90, lower_bound=0.80)
# -> ~138 clean controls, ~62 per defect class, ~634 judge calls
```

The pilot's 3 per class gives a 95% interval of 43.8–100% — directional signal that the classes
are detectable, not a measured catch rate.

## Pilot result (POTS Tier 4, for orientation)

| question | defect class | catch | flags unadjudicated | verdict |
|---|---|---:|---:|---|
| Q1 | quote_swap | 3/3 | 2/24 | needs flag adjudication |
| Q2 | direction_flip | 3/3 | 3/24 | needs flag adjudication (flags likely real defects) |
| Q3 | — | untested | 2/24 | keep human |
| Q4 | tier_upgrade | 3/3 | 4/24 | needs flag adjudication |
| Q5 | causal_upgrade | 3/3 | 3/24 | needs flag adjudication |
| Q6 | number_perturb | 3/3 | 1/24 | needs flag adjudication |
| Q7 | condition_relabel | 3/3 | 3/24 (decided definitions) | machine-extendable with definitions in prompt |
| Q8 | entity_label_swap | 3/3 | 0/24 | machine-extendable |
| Q9 | hpo_inject | 3/3 | 6/24 | needs flag adjudication |
| Q10 | — | untested | 4/22 | keep human |
