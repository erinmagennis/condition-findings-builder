# REGISTRY — the cross-corpus paper registry

Part of `condition-corpus-builder`. Code: `pipeline/registry.py`; tests: `pipeline/registry_tests.py`
(every rule below has a planted violation there); config: the `registry:` block of
`config/TEMPLATE.yaml`. Schema version `registry-v0 (2026-10-07)`.

## Purpose

Two corpora built with this method in neighbouring areas draw overlapping literatures. The EDS/HSD
dry run found 49/75 of its papers already extracted in `pots-findings` (`DRYRUN_EDS_REPORT.md`
§3.7); over the full Tier-5 universe the registry built here measures 1,718/8,455. Extracting
them again would produce a second set of finding rows for the same PMID under a different
`condition` enum and a different prompt generation, and the obvious join — by pmid — would pool
them. The registry is the one table that says, for every paper either corpus has touched, **where
its extracted rows live, under which pipeline generation, and which corpus is only pointing at
them**.

The rule it enforces: **a paper already EXTRACTED in a sibling corpus is REFERENCED, never
re-extracted.** The referencing corpus exposes the sibling's rows under its own, finer `condition`
enum through an additive paper-level table (below) and never by editing the sibling's rows.

## The registry row

One row per **(pmid, corpus, tier, pipeline_generation, prompt_ref, chunk_id, extraction_status)**.
Within one corpus a pmid has exactly one row (`counts_sum`); across corpora it has one row per
corpus that dispatched or planned it.

| column | type | meaning |
|---|---|---|
| `pmid` | str | PubMed id, as a string; the join key to every findings table |
| `corpus` | str | `pots-findings`, `eds-findings`, … (`registry.corpus` in the config) |
| `corpus_version` | str | the version of that corpus the row describes (`v1.1`, `v0`) |
| `tier` | str | the tier label the corpus uses (`tier1`, `tier2a`, …, `tier5`) |
| `pipeline_generation` | str | DERIVED from the prompt file shipped in the package's `code/`: `schema_v5_extraction_prompt.txt` → `schema_v5`, `schema_v6_…` → `schema_v6`, `tier4_prompt.txt` → `tier4`. For a config-driven corpus it is the generation whose shipped prompt the method's `prompt_template.txt` reproduces byte-for-byte (the POTS config renders to the shipped Tier-4 prompt, so the method generation is `tier4`); otherwise `template@<sha256[:12]>` |
| `prompt_ref` | str | the prompt's identity in the key: the artifact-store version id when one is recorded authoritatively, else `sha256:<hex>` of the shipped prompt file, else `unpinned` (`generation_provenance` forbids `unpinned` on extracted/referenced rows) |
| `prompt_vid` / `prompt_sha256` / `prompt_file` / `pipeline_file` | str \| null | the parts `prompt_ref` was built from, each as read from `MANIFEST.json` / `code/` |
| `chunk_id` | int \| null | the sibling's `chunk_order` chunk; null for `planned` rows until the roster assigns one |
| `extraction_status` | enum | `extracted` — ≥1 finding row shipped in this corpus; `dispatched_no_yield` — in this corpus's `chunk_order` but no shipped row; `referenced` — this corpus points at a sibling's rows; `planned` — in this corpus's universe/roster, not dispatched |
| `n_findings` | int | shipped finding rows for the pmid in this corpus (0 unless `extracted`) |
| `shared_with` | list[str] | the OTHER corpora holding `extracted` rows for the pmid; `[]` when none |
| `reference_only` | bool | true exactly when `extraction_status == referenced` |
| `universe_status` | enum \| null | for rows of the referencing corpus: `shared` (a sibling holds extracted rows) or `new`; null on sibling rows |
| `sibling_dispatched_no_yield` | bool | a `new` paper the sibling DISPATCHED but got no rows from — nothing to reference, flagged so the roster can decide |
| `package`, `doi`, `year`, `theme` | | package directory (sibling rows), DOI and year from the shipped data / universe, draw theme (referencing corpus) |
| `source_version_id` | str | `MANIFEST.source_corpus_version_id` (sibling rows) / the universe artifact id (referencing rows) |
| `basis` | str | how the row was derived, in words, on every row |

## Invariants (`check_registry()` — flag, never repair)

Each returns `{name, ok, n_violations, examples, basis}`. A failure is declared in the report; the
registry is not edited to make it pass.

| name | rule |
|---|---|
| `key_unique` | no two rows share the key |
| `status_enum` | `extraction_status` ∈ the four values; `universe_status` null or ∈ {shared, new} |
| `one_extraction_per_pmid_per_generation` | **the invariant**: among `extracted` rows, each (pmid, pipeline_generation) has ≤ 1 corpus — unless the pair is listed in `registry.exceptions` with a written reason |
| `no_reextraction_of_sibling_paper` | **the policy** (stronger): among `extracted` rows, each pmid has ≤ 1 corpus regardless of generation, minus documented exceptions. A re-extraction under a *new* generation passes the invariant and fails the policy; both are reported |
| `referenced_exists_in_sibling` | every `referenced` row names ≥ 1 sibling in `shared_with`; each named sibling has an `extracted` registry row for the pmid; and, when the sibling's shipped pmid set is supplied, the pmid is present in the sibling's shipped findings parquet |
| `shared_with_consistent` | on every row, `shared_with` equals the set of other corpora with an `extracted` row for the pmid |
| `reference_only_consistent` | `reference_only` ⇔ `extraction_status == referenced` |
| `universe_status_consistent` | `shared` ⇔ `referenced`; `new` ⇒ planned / extracted / dispatched_no_yield |
| `n_findings_by_status` | extracted ⇒ `n_findings ≥ 1`; otherwise `n_findings == 0` |
| `partition_within_corpus` | within one corpus a pmid is extracted in ≤ 1 package (the siblings' pairwise-zero paper overlap, re-measured, not read from `MANIFEST.partition`) |
| `generation_provenance` | extracted/referenced rows carry a generation and a `prompt_ref ≠ unpinned`; one (generation, prompt_sha256) per package |
| `counts_sum` | per corpus Σ over statuses of distinct pmids = distinct pmids; and per package, extracted papers = `MANIFEST.contents.papers` when supplied |

`build_registry()` additionally **raises** (does not guess) when a package's `code/` holds zero or
two prompt or pipeline files, when the MANIFEST sha256 of the prompt file disagrees with the file on
disk, when a pmid spans two chunks in shipped data, or when the universe has duplicate pmids. A
`chunk_order` row without a pmid is excluded and counted in the build log, never dropped silently.

## How a sibling's rows are exposed under this corpus's enum

Never by mutating the sibling's rows (`annotations/README.md`), and never by copying them into this
corpus's findings table. A referenced paper's findings are read **through** an additive paper-level
table shipped by the referencing corpus:

`annotations/paper_condition_membership.parquet` — one row per (pmid, corpus, definitions_version)

| column | type | meaning |
|---|---|---|
| `pmid` | str | the referenced paper; must have a registry row for `corpus` |
| `corpus` | str | the corpus whose enum the labels belong to (`eds-findings`) |
| `population_labels` | list[str] | MULTI-LABEL: the studied population(s) under THIS corpus's `condition` enum and semantics (`population`) — e.g. `[hEDS, HSD]`; every label ∈ the enum; ≥ 1 label |
| `deciding_span` | str | verbatim span from the paper's (freshly fetched) abstract/title that decided the labels; stored quotes are never edited |
| `span_verified_verbatim` | bool | the span was found in the fetched text under symmetric normalization |
| `definitions_version` | str | `config.condition.definitions_version` the labels were judged against |
| `classifier` | str | `human:<initials>` or `model:<literal model id>` |
| `assessed_at` | str | ISO-8601 UTC |
| `basis` | str | how the paper entered the table (full census of shared papers, sample seed, …) |

`check_membership(mem, reg, enum_by_corpus)` enforces: schema; every (pmid, corpus) is in the
registry; every label is in that corpus's enum (the SIBLING's label — `EDS_hypermobility` — is not a
valid EDS label and fails the check); non-empty span; one row per (pmid, corpus,
definitions_version); provenance columns non-null. A reader who wants "EDS findings about hEDS"
joins `registry[referenced] ⋈ membership[hEDS ∈ labels] ⋈ pots-findings rows by pmid`, and the
sibling's own `condition` column rides along untouched for comparison. Negative control to ship with
the table: papers from an unrelated sibling bucket (e.g. `migraine`) run through the same classifier.

## How counts are reported

Every count carries numerator, denominator and basis (`registry_counts()`), and the three bases are
never collapsed into one number:

- **papers extracted here** — `extracted` in this corpus / universe;
- **papers referenced from a sibling** — `referenced` / universe, and the same numerator over the
  sibling's extracted papers (what share of the sibling this corpus leans on);
- **universe** — `shared` and `new` / universe, repeated per year window (2019–2026, 2010–2026,
  all) and per draw theme, each with its own in-window / in-theme denominator;
- **`new_but_sibling_dispatched_no_yield`** — reported separately: these papers are `new` for the
  purpose of extraction but are the reason "universe ∩ sibling chunk_order" (1,941) is not
  "shared" (1,718).

## What the registry does NOT do

- It does not merge findings across prompt generations or corpora. A shared paper's POTS rows
  (`schema_v5`/`schema_v6`/`tier4` generation, POTS enum) and any EDS rows are different rows; the
  registry records that they coexist and which is canonical for which generation. Pooling them is
  the reader's decision, made against `pots-findings/COMPARABILITY.md`.
- It makes no claim of comparability of `condition`, identifier or provenance columns across
  corpora or generations — `COMPARABILITY.md` does; the registry points there.
- It does not adjudicate membership. The POTS `EDS_hypermobility` label on a shared paper's rows is
  recorded as-is; mapping it onto `hEDS`/`HSD`/… is the membership table's job and needs the span.
- It never writes to a sibling package.

## Running it

```
python pipeline/registry_tests.py                       # 42 cases; exit 1 on any failure
python pipeline/registry.py paper_registry_v0.parquet   # re-run the invariants on a built registry
```

Build (see `build_registry` docstring): the sibling's package directories (read-only), its
`chunk_order.parquet`, this corpus's universe/roster, this corpus's generation and prompt id from
`config/<area>.yaml`, and `prompt_vids` for any sibling prompt whose artifact id is recorded in an
authoritative place (the POTS config's `artifacts.prompt_vid` for the Tier-4 prompt; Tiers 1–3 ship
no prompt artifact id and are keyed by `sha256:`).
