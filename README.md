# condition-findings-builder

The method for building a grounded, verifiable findings corpus from the biomedical literature of one condition
area: retrieve the papers, screen them, extract one row per finding with its verbatim source quote, resolve entity
names to authority identifiers, run quality checks with negative controls, and package the result so that anyone
can re-verify it. Fill one config file for a new condition area and run the stages in `RUNBOOK.md`.

This repository holds no data. The datasets built with it are:

| dataset | repository | scope | size |
|---|---|---|---|
| POTS and dysautonomia findings | [pots-findings](https://github.com/erinmagennis/pots-findings) | POTS, dysautonomia and related condition literatures, 2019-2026, five packages | 148,186 findings from 11,519 papers |
| Ehlers-Danlos and hypermobility findings | [eds-findings](https://github.com/erinmagennis/eds-findings) | EDS and HSD, papers published 1946-2026, four packages | 24,200 findings from 3,558 papers |

Each dataset package ships the pinned code that produced it; this tree holds the current condition-agnostic
generation of that code, the runbook, the failure catalogue (`PITFALLS.md`), the schema, and the packaging and
fidelity protocols. The project index is [condition-findings](https://github.com/erinmagennis/condition-findings).

Credit: Erin Magennis. Code under MIT, documentation under CC BY 4.0 (see Licence below).

## The five non-negotiables

Each was paid for on the POTS build; the measured cost is in `PITFALLS.md`.

1. **The model emits names; the pipeline resolves identifiers.** A build that asked the model for ids got
   34.7% of them pointing at a different entity than the one named (full-population lookup, pots-findings v1.2 benchmark). `pipeline/resolver.py` derives every id from the name against the real
   authority, exact label only, and records which authority and which match path.
2. **Retain the text you extracted from.** Three tiers discarded body text; 108,549 of 148,186 (recomputed from the data at every build; a stale handoff said 108,714)
   rows are permanently uncheckable. `retain_source_text: true` is enforced by the config
   validator and the pipeline. The one tier that retained it reached 100% verifiable quotes.
3. **Every quality metric gets a negative control.** Pair quotes with the wrong paper; a sound
   matcher scores ~0%. A traceability check once read 100% because of `if tr:` on a tuple.
4. **State the denominator.** Coverage was simultaneously 85.97% of screened-in papers and 74.57%
   of the universe. `helpers.coverage_bases()` publishes every basis that is distinct.
5. **Flag; never silently repair.** Enum drift, off-topic papers, bucket violations, invariant
   failures: add a column or an additive table, keep the row, declare it in the README. This
   applies to the shipped data of a prior tier absolutely: `annotations/README.md`.

## Quickstart

```
git clone <this repo> my-area-corpus && cd my-area-corpus
pip install pandas pyarrow pyyaml
cp config/TEMPLATE.yaml config/my_area.yaml        # fill EVERY field; each is commented
python pipeline/config.py validate      config/my_area.yaml
python pipeline/config.py render-prompt config/my_area.yaml > prompt_rendered.txt
python pipeline/config.py render-scope  config/my_area.yaml > scope_rendered.txt
export CORPUS_CONFIG=$PWD/config/my_area.yaml
python pipeline/config_tests.py && python pipeline/driver_tests.py \
  && python pipeline/normalizer_tests.py && python pipeline/comparator_symmetry_tests.py
python pipeline/config.py export-json config/my_area.yaml > config/my_area.json   # for the stdlib-only orchestration kernel
```

Then follow `RUNBOOK.md` stage by stage (0 scope → 1 draw → 2 cost → 3 screen → 4 extract →
5 resolve ids → 6 QA → 7 next tier → 8 cross-tier → 9 package → 10 derived layer → 11 fidelity).
The stages that call a language model run inside an orchestration environment that provides
`host.llm` / `host.delegate` / `host.artifacts`; `pipeline/driver.py` is written against that
surface and documents it. Everything else (config, prompt rendering, QA modules, verifier,
calibration analysis) is plain Python.

The config validator **refuses**: a non-literal model id, `retain_source_text` other than `true`,
a `condition` bucket without a written definition, `target_chunks` that is not an explicit list.

## Layout

| path | what |
|---|---|
| `RUNBOOK.md` | the twelve stages with a gate at each boundary |
| `PITFALLS.md` | the failure catalogue: read before dispatching anything expensive |
| `SCHEMA.md` | the extraction contract, roster contract, template slots, prompt clauses kept verbatim |
| `PACKAGING.md` | self-contained deposit packages, the verifier, licensing the quotes |
| `FIDELITY.md` | the human-scored instrument, and how to calibrate an AI scorer before spending human time |
| `config/TEMPLATE.yaml` | every field commented; `config/pots_dysautonomia.yaml` is the POTS build's actual values; `config/eds.yaml` the EDS/HSD dry-run (second worked example) |
| `pipeline/` | the Tier-4 generation of the code, condition-agnostic: `config.py`, `draw.py` (stage 1), `screen.py` (stage 3), `pipeline.py`, `driver.py`, `resolver.py` + `resolve_stage.py` (stage 5), `invariants.py`, `invariants_ext.py`, `quote_census.py`, `quote_trace.py`, `verify_package.py`, helpers, tests |
| `pipeline/prompt_template.txt` | the extraction prompt with nine `{{slot}}`s filled from the config |
| `calibration/` | `fidelity_calibration.py` and how to run its planted and agreement arms |
| `annotations/README.md` | the additive-table convention: never mutate shipped tier data |
| `packaging/` | builder config for the POTS build + doc templates (`prose.json`, verifier, root-doc templates) |
| `build_packages.py` | `--area-config config/<area>.yaml` → ONE package for a new area (`packaging/generic_builder.py`); legacy mode reproduces the POTS v1 five-package build byte-for-byte (see `packaging/README.md`) |

## What was parameterised out of the Tier-4 code

Every condition-specific literal was moved into `config/<area>.yaml` and read through
`pipeline/config.py`. The dispatch logic (`is_ceiling`, `done_units`, `archive_units`, `gate`,
`run`) was byte-identical to the pinned Tier-4 driver until the 2026-09-30 dry run (Changelog);
with the POTS config's legacy settings it still behaves identically, and `pipeline/driver_tests.py`
(53 cases) pins both behaviours. `pipeline/config_tests.py` asserts that rendering the POTS config through
`prompt_template.txt` reproduces the pinned `tier4_prompt.txt` byte-for-byte. Two behavioural
switches were added, both config-gated and both defaulting to the safe value for a new build:
`unescape_source_text` (decode HTML entities before the model reads the text) and the enforced
`retain_source_text`.

## Relation to the `pots-findings` data repository

The five `pots-findings` packages (`dysautonomia-core-findings`, `comorbid-conditions-findings-a/-b`, `postviral-autism-findings`, `migraine-hypermobility-findings`; extraction tiers 1–4 in `MANIFEST.tier`) are the frozen record: each package's `code/` is the exact generation
that produced its rows (three generations across five packages) and is not superseded by this
tree. each package's `annotations/` holds additive tables produced with the conventions in
`annotations/README.md`. This tree is what you fork for the *next* corpus; the data repository is
what you cite for the POTS findings.

## Changelog

### 2026-09-30: EDS/HSD dry run (second-condition reusability test)

A ~75-paper throwaway corpus (Ehlers-Danlos syndromes + HSD, 2019–2026, Europe PMC; 3 chunks of 25,
stratified full-text-rich / mixed / abstract-heavy) was built end-to-end from `config/eds.yaml` to a
verified package (`DRYRUN-eds-findings-test`, 886 findings / 69 papers, 19/19 headline numbers
re-derived, 9/9 invariants, 6/6 planted regressions caught, census 885/886 traceable with 0/586
wrong-paper false positives). Everything that required editing **code rather than config** is listed
in `DRYRUN_EDS_REPORT.md` (repository root; also in the dry-run package's `code/`) and summarised here:

* **`pipeline/config.py`**: PyYAML import made lazy; `load_config` accepts a `.json` twin and a new
  `export-json` command writes it. The driver must run in the orchestration kernel (where
  `host.delegate` lives), which is stdlib-only; a module-level `import yaml` made `driver.py`
  unimportable exactly where it has to run. New `artifacts.file_prefix` (default `<slug>_`).
* **`pipeline/driver.py`**: chunk artifact filenames and the done/archive detectors carry
  `artifacts.file_prefix`, anchored at both ends. Without it a second area in the same project
  inherited the POTS build's `chunk_000..002_findings.parquet` as "already done" and dispatched
  nothing. Work-based unit sizing (`plan_parts`, `pinned.max_fulltext_per_unit`, default 12) and
  watchdog-reclaim → re-split (`pinned.watchdog_resplit`): a 25-full-text-paper whole unit exceeded
  `WORKER_MAX_S=3000` with nothing saved and the pinned loop re-queued the identical unit twice.
  Per-run log filenames (`<prefix>gate_log_parts_<run_tag>.json`): two runs in one directory
  clobbered the first run's gate log. `run(roster_df=...)` accepts a DataFrame or a plain list of
  dict rows. **The dispatch loop is therefore no longer byte-identical to the pinned Tier-4
  driver**; with `file_prefix: ""` and `watchdog_resplit: false` (the POTS config) it behaves
  identically, and `driver_tests.py` (53 cases, was 40) pins both the legacy and the new behaviour.
* **`pipeline/pipeline.py`**: `unescape_source()` also strips whitelisted inline HTML/JATS tags
  (Europe PMC abstracts carry `<i>`, `<sub>`; parsed full text does not); archive/findings filenames
  carry the prefix; roster flag columns (`query_contamination`, `screen_final_verdict`,
  `screen_verdict_uncertain`, `sample_stratum`) ride through to every finding row.
* **`pipeline/quote_trace.py`**: additive `strip_tags()` / `TAG` / `INLINE_TAG` (one definition,
  shared with `normalizer_tests.py`, which previously tested a tag stripper the census never used);
  `normalize()` itself is unchanged. **`pipeline/quote_census.py`**: abstract-only rows are checked
  against the retained archive (text as handed to the model), roster abstract only as fallback;
  `blocked` is its own stratum; a stratum with <2 papers reports "no control possible" instead of
  "matcher suspect". **`comparator_symmetry_tests.py`**: 4 tag cases added (12/12).
* **New stage code the RUNBOOK described but the repository lacked**: `pipeline/draw.py` (stage 1,
  config-driven Europe PMC draw, flag-not-exclude contamination, stratified dry-run sampling),
  `pipeline/screen.py` (stage 3, two arms + `reconcile()` as a pure function so a comparator fix
  never re-spends the screen), `pipeline/resolve_stage.py` (stage 5 over a merged table, truthful
  `_via_<mint>` HPO tags, resolution rate by authority with bases).
* **`build_packages.py --area-config`** → `packaging/generic_builder.py`: corpus + `config/<area>.yaml`
  → ONE self-contained package with `MANIFEST.json` carrying `condition_enum` and `model_id`,
  checksums, sidecars, retained archives under `data/archives/`, and `pipeline/verify_package.py`
  as `code/verify_package.py`. The legacy five-package POTS reproduction is untouched; it fails on
  any non-POTS corpus at `partition()` (`tier_source`), and `PACKAGING.md` has been corrected to say so.
* **Config**: `config/eds.yaml` (the worked second example), `draw:` and `screen:` blocks,
  `pinned.max_fulltext_per_unit`, `pinned.watchdog_resplit`, `artifacts.file_prefix` in
  `TEMPLATE.yaml`; `pots_dysautonomia.yaml` pins the legacy values (`""`, `false`).

Known gaps left open (see the report): the prompt template still carries two POTS-era fields
(`Flagged COVID-related` header, `is_covid_associated`); `helpers.screen_instrument_alarm` is
degenerate on an anchor-drawn roster (rule off-topic is 0 by construction); the in-pipeline
`ground_findings_df` still emits the bare `authority_lookup_ols_hp` tag (invariant 9 fired on the
worker columns for 1 MPATH id: the stage-5 retag is what passes); the worker saves a unit's
findings all-or-nothing; `fetch_fulltext` + `get_ft_text` fetch every full text twice; the
templates under `packaging/templates/*.tmpl` are POTS prose and are not used by the generic builder.

## Licence

- **Code** (`pipeline/`, `calibration/`, `build_packages.py`): MIT: see `LICENSE`.
- **Documentation** (`*.md`, `config/*.yaml`): CC BY 4.0: see `LICENSE-docs`.

The verbatim provenance excerpts in any corpus built with this method are the publishers' and
authors' copyright and are not covered by either licence; `PACKAGING.md` states the excerpt
policy a corpus must publish.

## Citation

Cite the method repository and, for any finding that matters to your work, the source paper. See
`CITATION.cff`. A DOI will be added when the repository is deposited.
