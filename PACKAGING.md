# PACKAGING — self-contained deposit packages

Part of `condition-corpus-builder`. The goal: a reader downloads **one folder**, has everything, and can re-derive every number in the
documentation from the shipped data without a network connection. The prior build produced five
such packages (220 files / 102.6 MB, verified by opening the archive) from one corpus. They are
built and verifiable but **not yet deposited** — ten of their eleven citation/metadata files still
carry placeholder DOI strings, which is exactly what the pre-publication `grep` in the checklist
below is for.

## The partition

One package per tier. **Zero papers shared between any two** — assert it, do not assume it:

```python
from pipeline.helpers import assert_partition
assert_partition({"tier1": t1, "tier2a": t2a, "tier2b": t2b, "tier3": t3, "tier4": t4}, key="pmid")
```

It checks every pair and that per-package counts sum exactly to the corpus total. Splitting a large
tier into extraction waves (`2a`/`2b`) is fine — say in the docs that the split is **operational,
not topical**, and whether the waves may be pooled.

A tier-level measurement (e.g. an adjudicated correctness rate sampled per tier) that a wave
inherits rather than owns must say so explicitly. One package pair showed the same rate "by
inheritance — neither has a rate of its own", with a per-wave proxy quoted separately.

## Folder shape

```
<pkg>/
  data/     findings table (parquet + csv.gz for tools without parquet),
            entity vocabulary lookup, chunk map
  docs/     README, DATA_DICTIONARY, METHODS, LIMITATIONS, COMPARABILITY,
            (INTEGRATION doc if a site consumes it)
  qa/       invariant results, quote census, per-package statistics
  scope/    screening report, excluded PMIDs
  code/     pinned pipeline, prompt, driver, driver tests, resolver,
            invariant modules, quote census, verifier
  deposit/  repository metadata sidecars, release notes
  annotations/  (optional, ADDITIVE tables keyed on pmid/row_idx — see annotations/README.md)
  README.md  MANIFEST.json  CHECKSUMS.sha256  LICENSE  CITATION.cff
```

Never mutate a shipped tier's data. New information — relabels, retrofit traceability, bucket
verdicts — ships as additive tables alongside it (`annotations/README.md`).

Shared files (entity vocabulary, chunk map, invariant modules) are **duplicated into every
folder**. That is why the packages sum to slightly more than the corpus, and it is deliberate: it
is what lets any one folder be copied out and still work.

Ship both parquet and gzipped CSV. Keep every file under ~20 MB so no large-file extension is
needed for a Git host.

## The builder is a script and an artifact

`build_packages.py --area-config config/<area>.yaml --corpus ... --roster ... --qa-dir ... --scope-dir ...
--archives-dir ...` (→ `packaging/generic_builder.py`) builds ONE self-contained package for a new area with
`MANIFEST.json` (carrying `condition_enum` and `model_id`), `CHECKSUMS.sha256`, sidecars, the retained
archives under `data/archives/` and `code/verify_package.py`; it is the only way packages are built. The
script's legacy mode (`--config package_config.<area>.json`) is the POTS five-package reproduction and
fails on any other corpus at `partition()` — it expects the cross-tier harmonization columns
(`tier_source`, `cap_*`, ...) that the pipeline does not emit; the generic builder appends exactly the
six the verifier reads and lists them as harmonization columns in `DATA_DICTIONARY.md`. Measured on the
EDS dry run, 2026-09-30.
Save it as a first-class artifact with the packages, and run it again after every DOI backfill.
The POTS packages were first built by an inline cell that was never saved, so the packaging step
could not be reproduced; the script is being reconstructed. **Regenerate checksums by script,
never by hand** — a hand-edited `CHECKSUMS.sha256` drifts the first time a sidecar changes.
`MANIFEST.json` must carry `condition_enum` (from the config) so the verifier's enum-drift
invariant runs against the enum the package was built with, and `METHODS.md` records the literal
`model.id`.

## The verifier is the deliverable

`code/verify_package.py` (`pipeline/verify_package.py`, copied into every package) re-derives
**every headline number in the README** from the shipped parquet and exits non-zero on
disagreement.

**It must also plant regressions and assert each is caught** (`--self-test`). A checker that only
ever passes is untested. The prior build's five packages each reported "19/19 headline numbers
re-derived, 6/6 planted regressions caught".

## Declare failures in the README

Three of five packages did not fully pass the invariant suite and **shipped that way on purpose**,
with a table naming which check fails and why (an earlier extraction pass predating a provenance
convention; a handful of rows with ids written into a free-text field). These are pre-existing
characterized properties, not packaging damage. A suite tuned until it passes is not a suite.

## The "three things that will mislead you" section

Every package README leads with the errors a reader is *most likely* to make, in the order they
are likely to make them. The prior build's three, as a template for the shape:

1. **A metric that looks like quality but measures coverage** — and ranked the packages backwards.
   Give the coverage number and the correctness number side by side, and say which to stratify on.
2. **A large share of rows that cannot be re-checked** — and whether that is a retention failure
   or an extraction failure. Say which, with the measured traceability where source text does
   survive, and whether it is recoverable.
3. **A column whose value means opposite things in different packages** — with the wrong filter
   and the right filter written out as code.

Then a `COMPARABILITY.md` naming the safe aggregates and the metrics that are **not** comparable
(11 of them, on the prior build).

## Above the data, in the README

State plainly that **scientific fidelity is unmeasured**: every check verifies well-formedness,
nothing verifies that a finding correctly interprets its source, and that requires a human reader.
"Treat every row as a pointer to a paper worth reading, not as a fact to cite." Plus a
not-medical-advice line.

## Licensing the quotes

Derived structured data can be CC BY. The verbatim provenance excerpts **cannot** — they are the
publishers' and authors' copyright. State the excerpt policy, the median and maximum excerpt
length, why the excerpts exist (a finding without its source span cannot be checked), and a
takedown commitment with a channel that actually works (issues enabled on the repo).

**Whether this is fair use, quotation, or a text-and-data-mining exception is a legal question.**
Put it to the institution's library or scholarly-communications office before a public deposit.
Do not offer a determination.

## Deposit order

DOIs are needed by the other steps, so:

1. **Archive first** — deposit the tarballs (not folders) and mint the DOIs. Each package
   references its siblings, so either reserve all DOIs on drafts and backfill before publishing
   (clean), or publish with placeholders and version up (v1 ships with placeholder text).
2. **Backfill** every placeholder in the citation and metadata sidecars — then re-run
   `build_packages.py` so the checksums and per-file hashes are **regenerated by script**, because
   files inside the packages changed.
3. **Git host** — push the repo, then attach the per-tier tarballs as **release assets** (outside
   Git history). This is what makes single-tier download work for people who do not use Git, and
   it is not optional garnish. Gitignore the tarballs: they are deposit products, not sources.
4. Optional second repository as a parent project with one component per package.

## Pre-publication checklist

- [ ] `build_packages.py` saved as an artifact alongside the packages it produced
- [ ] verifier `--self-test` passes in every folder
- [ ] `METHODS.md` in every folder records the literal model id and the config file version
- [ ] checksums verify clean in every folder
- [ ] author name and identifier correct in every citation file
- [ ] no placeholder DOI strings left anywhere (`grep -r PLACEHOLDER .`)
- [ ] no OS junk files (macOS resource forks, `.DS_Store`) — they regrow on copy and break
      file-count/manifest agreement
- [ ] no `__pycache__` / `.pyc` — running the verifier creates them
- [ ] the excerpt policy is one you are prepared to stand behind

## Give the reader single-tier download instructions

Three paths, all of which must actually work: a single archive from the releases page; a sparse
checkout of one folder; and a direct raw URL for one file (`pd.read_parquet(url)`). State the
compressed and extracted size of each package.
