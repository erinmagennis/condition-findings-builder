# condition-corpus-builder

The **method** behind the POTS/dysautonomia findings corpus (148,186 machine-extracted findings
from 11,519 papers, five self-contained data packages), exported as a standalone, forkable
repository. Someone starting a grounded literature-findings corpus for a **new condition area** —
ME/CFS alone, long COVID, a rare-disease cluster — forks this tree, fills **one config file**, and
runs the stages in `RUNBOOK.md`.

This repository holds no data. The data lives in the companion `pots-corpus` repository, whose
five tier packages each ship the *pinned* code that produced them. This tree holds the canonical,
condition-agnostic generation of that code (the Tier-4 generation, the last and most complete),
the runbook, the failure catalogue, the schema, the packaging and fidelity protocols, and the
lessons from the September 2026 audit of the POTS packages.

## The five non-negotiables

Each was paid for on the POTS build; the measured cost is in `PITFALLS.md`.

1. **The model emits names; the pipeline resolves identifiers.** A build that asked for ids got
   27.7% fabricated. `pipeline/resolver.py` derives every id from the name against the real
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
| `PITFALLS.md` | the failure catalogue — read before dispatching anything expensive |
| `SCHEMA.md` | the extraction contract, roster contract, template slots, prompt clauses kept verbatim |
| `PACKAGING.md` | self-contained deposit packages, the verifier, licensing the quotes |
| `FIDELITY.md` | the human-scored instrument, and how to calibrate an AI scorer before spending human time |
| `config/TEMPLATE.yaml` | every field commented; `config/pots_dysautonomia.yaml` is the POTS build's actual values |
| `pipeline/` | the Tier-4 generation of the code, condition-agnostic: `config.py`, `pipeline.py`, `driver.py`, `resolver.py`, `invariants.py`, `invariants_ext.py`, `quote_census.py`, `quote_trace.py`, `verify_package.py`, helpers, tests |
| `pipeline/prompt_template.txt` | the extraction prompt with nine `{{slot}}`s filled from the config |
| `calibration/` | `fidelity_calibration.py` and how to run its planted and agreement arms |
| `annotations/README.md` | the additive-table convention: never mutate shipped tier data |
| `packaging/` | builder config for the POTS build + doc templates (`prose.json`, verifier, root-doc templates) |
| `build_packages.py` | corpus + config → N packages; reproduces the POTS v1 build byte-for-byte (see `packaging/README.md`) |

## What was parameterised out of the Tier-4 code

Every condition-specific literal was moved into `config/<area>.yaml` and read through
`pipeline/config.py`; the dispatch logic (`is_ceiling`, `done_units`, `archive_units`, `gate`,
`run`) is byte-identical to the pinned Tier-4 driver and `pipeline/driver_tests.py` (40 cases)
still passes. `pipeline/config_tests.py` asserts that rendering the POTS config through
`prompt_template.txt` reproduces the pinned `tier4_prompt.txt` byte-for-byte. Two behavioural
switches were added, both config-gated and both defaulting to the safe value for a new build:
`unescape_source_text` (decode HTML entities before the model reads the text) and the enforced
`retain_source_text`.

## Relation to the `pots-corpus` data repository

`pots-corpus/tier1 … tier4` are the frozen record: each package's `code/` is the exact generation
that produced its rows (three generations across five packages) and is not superseded by this
tree. `pots-corpus/annotations/` holds additive tables produced with the conventions in
`annotations/README.md`. This tree is what you fork for the *next* corpus; the data repository is
what you cite for the POTS findings.

## Licence

- **Code** (`pipeline/`, `calibration/`, `build_packages.py`): MIT — see `LICENSE`.
- **Documentation** (`*.md`, `config/*.yaml`): CC BY 4.0 — see `LICENSE-docs`.

The verbatim provenance excerpts in any corpus built with this method are the publishers' and
authors' copyright and are not covered by either licence; `PACKAGING.md` states the excerpt
policy a corpus must publish.

## Citation

Cite the method repository and, for any finding that matters to your work, the source paper. A
`CITATION.cff` will be added when the repository DOI is minted (`grep -r PLACEHOLDER .` before
deposit).
