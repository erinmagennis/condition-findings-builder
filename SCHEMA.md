# SCHEMA — the extraction contract and prompt template

Part of `condition-findings-builder`. The schema below is the one that survived four tiers and two
repair passes of the POTS build. The condition enum, its subtype field, the area description and
the in-prompt examples come from `config/<area>.yaml`; everything else in
`pipeline/prompt_template.txt` is structural and **kept verbatim** — each clause exists because of a
measured defect. `python pipeline/config.py render-prompt config/<area>.yaml` produces the prompt
the pipeline formats per paper (`pipeline/config_tests.py` asserts the POTS config reproduces the
pinned Tier-4 prompt byte-for-byte).

## Design rules

1. **Names, never identifiers.** No id field in the extraction output at all. One transcription
   exception: a database accession copied verbatim from an explicitly stated data-availability
   statement (measured 98.6% real, because it is a copy and not a recall).
2. **One verbatim quote per finding**, plus the section it came from. This is the traceability
   anchor.
3. **Multi-value where the biology is multi-value.** Phenotype/symptom terms as
   semicolon-delimited atomic **names**.
4. **Derived fields sit alongside extractor fields, never over them.** `certainty_derived`
   next to `certainty`; a `number_in_quote` boolean next to the numeric fields.
5. **Free text where no controlled vocabulary fits.** Mechanism/pathway stays prose.
6. **Enums are flagged, never coerced.** Expect ~0.05% drift past a pinned enum.
7. **Every `condition` bucket has a written definition before extraction, and the field's
   semantics are decided in writing.** `condition` is a coarse bucket enum, not a diagnosis. The
   config carries, per bucket, inclusions and the nearest exclusion with where it goes instead,
   plus one statement of whether the field means the patient POPULATION studied (the POTS
   decision) or the condition the finding INFORMS. Version the definitions; re-run any label audit
   when they change. Without them the POTS `MCAS` bucket (1,083 papers) was ~69% mast-cell biology
   with no MCAS population and ~10% MCAS as later defined, and an AI scorer's flag rate on the same
   24 clean rows moved 41.7% → 0% → 12.5% on wording alone. Later relabels are additive tables
   (`annotations/README.md`), never edits to the column.

## Paper-level fields

```
study_type            enum: RCT | cohort/observational | case-control | case-report/case-series |
                            mechanistic/in-vitro | review/narrative |
                            systematic-review/meta-analysis | cross-sectional | other
n_participants        int | null
comparator            enum: healthy controls | other patient group | pre/post within-subject |
                            no comparator | not applicable
demographics          free text | null
is_duplicate_or_preprint_note   free text | null
contradicts_or_extends / limitations_stated / future_direction    one sentence each | null
```

`other patient group` needs a documented adjudication: findings about *other* diseases used as
mechanistic analogies land here, and the enum is deficient rather than the output wrong.

## Finding-level fields

**Entity and grounding**

```
entity            canonical NAME of the biological entity | null
entity_kind       enum: gene/protein | chemical/metabolite | cell-type | anatomical-structure |
                        physiological-measure | pathogen | other        # routes authority lookup
gene_marker       gene symbol AND any variant notation verbatim (rsID, HGVS) | null
phenotype_terms   atomic clinical term NAMES, semicolon-separated | null   # never an id
pathway_or_mechanism   free text | null
symptom_correlate      free text | null
```

**Direction and effect**

```
direction              enum: elevated | depleted/reduced | dysregulated (unspecified direction) |
                             no significant difference | not applicable
direction_reference    enum: vs-disease-state | vs-intervention | not-applicable
quantitative_detail    free text | null
effect_size            {metric: OR|HR|RR|beta|r|d|mean_diff|percent|p_value|other,
                        value: number, ci: "lo-hi"|null, p: number|null} | null
causal_or_correlational  enum: causal-claim | correlational-only | not-stated
null_or_failed_result    enum: positive-effect | null-no-effect | failed-to-replicate |
                               tried-and-abandoned | not-applicable
contradicts_finding_ref  one sentence | null
```

`direction_reference` is what keeps a treatment *effect* from being read as a
patient-vs-control *difference*. It was added after the fact; include it from the start.

**Typed relation** — makes the table graph-loadable without a second pass.

```
relation_type     enum: UPREGULATED_IN | DOWNREGULATED_IN | DYSREGULATED_IN | CAUSES |
                        ASSOCIATED_WITH | CO_OCCURS_WITH | PART_OF_PATHWAY | BIOMARKER_FOR |
                        TARGETED_BY_TREATMENT | DIAGNOSED_BY | PREDICTS | NO_ASSOCIATION
relation_subject / relation_object    free text
```

**Condition and population** — the enum is `config.condition.enum` (values rendered in order
into `{{condition_enum_json}}`); keep `multiple` and `general`. The subtype field name and
values are `config.condition.subtype`.

```
condition               enum: <config.condition.enum values> | multiple | general
                        each value has a WRITTEN definition in the config (design rule 7)
<subtype_field>         enum per config.condition.subtype.values, for ONE condition | null
population_age_group    enum: pediatric | adult | mixed | not-stated
sex_hormonal            enum: female-specific | male-specific | sex-difference-reported |
                              hormonal-axis-implicated | none-stated
anatomical_system       comma-separated from a fixed list
finding_type            enum: mechanism/biomarker | treatment/intervention | symptom-burden |
                              comorbidity | diagnostic/screening
treatment_class         "<class>: <intervention name>" when finding_type=treatment | null
functional_impact / validated_instrument / adverse_event_note   free text | null
```

**Evidence quality**

```
evidence_tier      enum: meta-analysis | RCT | cohort | case-control | cross-sectional |
                         case-series | case-report | mechanistic/in-vitro | review/expert-opinion
evidence_stratum   enum: peer-reviewed | preprint | clinical-trial-registry | patient-registry |
                         large-survey | case-report
certainty          enum: hedged | confirmed | mixed          # the extractor's own judgment
cohort_registry / cohort_name    free text | null
dataset_accession  verbatim accession, only if explicitly stated | null
study_quality      {sample_size_adequacy, controls, preregistered, effect_with_ci_reported,
                    conflict_or_funding_flag, replication_status, quality_caveat}
```

**Provenance**

```
provenance_quote    VERBATIM sentence(s), 1-2 max, copied exactly
provenance_section  enum: title | abstract | introduction | methods | results | discussion |
                          conclusion | figure/table | unknown
```

## Pipeline-derived columns (added after extraction, not asked of the model)

```
certainty_derived        hedge detection over finding_text AND quote; both hedged -> hedged,
                         neither -> confirmed, disagreement -> mixed
number_in_quote          is the value in quantitative_detail/effect_size literally in the quote?
entity_id / entity_label authority-resolved from `entity` + `entity_kind`
ontology_id_verification enum incl. authority_verified          # the column to stratify on
ontology_id_match_mode   exact | (adjudicated fuzzy) 
ontology_id_source       authority + match path + minting ontology tag
fulltext_status          full-text | abstract-only | blocked     # THREE strata, not two
fetch_note               the actual HTTP status or exception
query_contamination      bool + note, for off-topic papers that entered on an abbreviation
final_verdict            the screening verdict, carried through for uncertain papers
cap_quote_source_retained  bool — is this row's source text archived?
```

## Roster contract (what the pipeline reads)

The roster parquet pinned as `artifacts.roster_vid` needs, per paper: `pmid`, `doi`, `title`,
`abstract`, `year`, `journal`, `source_stream`, `theme` (and/or a list column `themes`),
`is_covid` (bool; rendered into the prompt header — a POTS-era slot the template still carries),
`chunk_id` (authoritative; the pipeline only synthesises one when the column is absent). Optional
flag columns ride through to every finding row when present: `query_contamination`,
`screen_final_verdict`, `screen_verdict_uncertain`, `sample_stratum` (2026-09-30). For work-based
unit sizing the driver also reads `pmcid` + `is_oa` (or `fulltext_expected`). Chunk artifacts are
named `<artifacts.file_prefix>chunk_NNN[_partIofN]_findings.parquet` / `_fulltext.json.gz`. Findings rows carry `pmid`, `doi`, `title`, `year`,
`journal`, `source_stream`, `theme`, `chunk_id`, `tier` (from `config.tier.number`).

## Prompt template slots

`pipeline/prompt_template.txt` has nine `{{slot}}` placeholders filled from the config —
`area_description`, `condition_enum_json`, `subtype_field`, `subtype_condition`, `subtype_values`,
`pathogen_examples`, `phenotype_example`, `instrument_examples`, `cohort_examples` — and three
runtime `.format()` slots the pipeline fills per paper: `{themes}`, `{is_covid}`, `{text_kind}`.
Literal JSON braces in the template are escaped `{{`/`}}` for `.format()`; the config-slot regex
requires `{{identifier}}` so the two never collide.

## Prompt clauses to keep verbatim

These are the sentences that do work. They are in the template already; do not edit them.

**On identifiers** (three clauses, because one is not enough):

> NEVER emit an ontology or database identifier. Do NOT output `HGNC:`, `CHEBI:`, `HP:`, `GO:`,
> `MeSH:`, `UMLS:`, `Ensembl:`, `R-HSA-`, or any numeric ontology code in ANY field. There is no
> way for you to know these integers reliably, and the pipeline derives every id from the NAME you
> provide by querying the real authority. Emitting an id-shaped string is a hard error that
> discards the field.
>
> Give the NAME, precisely and canonically (e.g. "interleukin-6" not "IL6 (HGNC:6018)"). Names are
> what you can be right about; identifiers are not.
>
> `dataset_accession` is the ONE exception: copy it VERBATIM only when the text explicitly states
> it. That is transcription from visible text, not recall. Never reconstruct one from memory.

**On quote selection when a number is involved:**

> The provenance_quote must be the span where that number literally appears. If the number is in a
> table row or figure caption and the running prose only describes it, quote the table row or
> caption verbatim and set provenance_section to "figure/table". Never substitute a contextual
> sentence that omits the number.

**On finding count** — an uncapped instruction, because a cap truncates rich papers:

> Extract EVERY distinct, substantive finding the text supports — do NOT cap the count. A rich
> paper may yield 15-25 findings; a thin one 1-2. Do not split one result into fragments, and do
> not pad with trivia, but never drop a real finding just to shorten the list.

**On reviews:**

> Pure review with no specific finding: empty findings list, study_type "review/narrative".

Also pass the paper's corpus theme tags and whether the provided text is full text or
title+abstract, and note that per-finding tags may be narrower than the paper's themes.

## Parser guards

- `helpers.reject_id_shaped(record)` / `pipeline.guard_finding()` on every name-bearing field —
  discard the field, count the event.
- Truncation guard: never accept a response whose `stop_reason` indicates the token limit as
  final. Continue it, or re-run.
- Salvage partial JSON (a list of findings recoverable from a truncated object) rather than
  discarding a whole paper — but count salvaged papers separately.
- Any parser that reads a fixed set of keys from a model response (adjudication, judge, screen)
  must **raise** on a missing key. An unparsed answer once read as a neutral omission and
  reported a question as "untested".

## Source text the model reads

Decode HTML entities twice (`unescape_source_text: true`) before the text reaches the model, and
retain exactly the text handed to it (`retain_source_text: true`, enforced). Quotes are then
verbatim to both the archive and the displayed paper. Where a corpus was built without the
unescape (the POTS tiers), the quotes carrying entity codes are verbatim to the archive, trace
under symmetric normalization, and are **not** to be repaired in place.
