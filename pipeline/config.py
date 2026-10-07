"""Configuration for a condition-corpus build: load, validate, render.

Everything that was hard-coded for the POTS/dysautonomia build — area name, condition
enum and bucket definitions, scope text, abbreviation collisions, pinned parameters,
model id, roster/pipeline artifact ids, chunk range — lives in ONE YAML file under
`config/`. `config/TEMPLATE.yaml` documents every field; `config/pots_dysautonomia.yaml`
holds the values that produced the POTS corpus.

Three entry points:

    cfg = load_config("config/<area>.yaml")     # parsed + validated dict
    render_prompt(cfg, template_text)           # fill {{slot}} placeholders -> the
                                                #   extraction prompt the pipeline formats
    render_scope(cfg)                           # the screening scope text (stage 3)

Validation is deliberately strict about the things that were paid for:

  * `model.id` must be a literal string (never "current"/"latest"/empty). Every
    package's METHODS records the literal id that produced it.
  * `retain_source_text` must be true. Discarding body text after extraction made
    108,549 of 148,186 rows (= 148,186 − 39,637 verifiable, recomputed from the data at every package build; a stale handoff document said 108,714 and is superseded) permanently uncheckable on the POTS build.
  * every `condition.enum` entry must carry a written `definition`, and
    `condition.semantics_statement` must say whether `condition` means the patient
    POPULATION studied or the condition a finding INFORMS. The MCAS bucket (1,083
    papers) was ~69% mast-cell biology with no MCAS population because neither existed.
  * `tier.target_chunks` must be an explicit list, never a range expression.

Slot syntax in prompt_template.txt: `{{slot_name}}` — lowercase identifier, no spaces.
This does NOT collide with the prompt's own JSON-brace escapes (`{{` followed by a
newline or a quote) or with the runtime `.format()` slots (`{themes}`, `{is_covid}`,
`{text_kind}`), which the pipeline fills per paper after this module has run.
"""
import json
import os
import re

# PyYAML is imported LAZILY (2026-09-30, EDS dry run). The driver must run in the orchestration
# kernel — the only place `host.delegate` exists — and that kernel is stdlib-only (python -I -S,
# no pyyaml, no pandas). A module-level `import yaml` made `driver.py` unimportable exactly where
# it has to run. `load_config()` therefore also accepts a `.json` twin produced by
# `python pipeline/config.py export-json config/<area>.yaml > config/<area>.json` (validated
# identically), so the analysis kernel writes the JSON once and the orchestration kernel reads it.
yaml = None

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)

SLOT_RX = re.compile(r"\{\{([a-z][a-z0-9_]*)\}\}")

# Parameters carried unchanged across the POTS build's four tiers. A config may
# override any of them, but the validator warns when it does, because the values are
# what the measured yield/blocked gates were calibrated on.
PINNED_DEFAULTS = {
    "papers_per_chunk": 50,
    "fetch_pacing_s": 0.9,
    "fetch_attempts": 6,
    "n_inflight": 3,
    "n_parts_default": 2,
    "max_attempts": 3,
    "worker_max_s": 3000,
    "collect_slice_s": 120,
    "min_yield": 0.60,
    "warn_blocked": 0.05,
    "max_blocked": 0.35,
    "min_ft_chars": 2000,
    "ft_input_char_cap": 120000,
    "llm_concurrency": 8,
    # 2026-09-30 (EDS dry run): work-based unit sizing. A unit may carry at most this many papers
    # expected to yield FULL TEXT; the driver raises the part count until it holds. 12 = the POTS
    # 25-paper part at ~50% full text, on which WORKER_MAX_S=3000 was calibrated.
    "max_fulltext_per_unit": 12,
    # A watchdog-reclaimed unit is re-dispatched SPLIT (like a ceiling kill), never the same shape.
    "watchdog_resplit": True,
}
MODEL_DEFAULTS = {"max_tokens_fulltext": 32000, "max_tokens_abstract": 8000,
                  "max_tokens_screen": 2000}

_NON_LITERAL_MODEL = {"", "current", "latest", "default", "a current reasoning model",
                      "reasoning", "sonnet", "opus", "haiku"}


class ConfigError(ValueError):
    pass


def _req(d, path):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            raise ConfigError(f"missing required config field: {path}")
        cur = cur[k]
    return cur


def validate(cfg, warn=print):
    """Raise ConfigError on any non-negotiable violation; `warn` on soft deviations.
    Returns the list of warnings (so a caller can record them)."""
    warnings = []

    def w(msg):
        warnings.append(msg)
        if warn:
            warn("config warning: " + msg)

    for p in ("area.name", "area.slug", "area.description_for_prompt", "area.families",
              "condition.semantics", "condition.semantics_statement",
              "condition.definitions_version", "condition.enum",
              "scope.text", "model.id", "tier.number", "tier.target_chunks", "tier.env_name",
              "package.author", "package.license_code", "package.license_docs"):
        _req(cfg, p)

    mid = cfg["model"]["id"]
    if not isinstance(mid, str) or mid.strip().lower() in _NON_LITERAL_MODEL:
        raise ConfigError("model.id must be the LITERAL model identifier string that will be "
                          "recorded in METHODS (e.g. 'claude-sonnet-5'), not a role name")

    if cfg.get("retain_source_text") is not True:
        raise ConfigError("retain_source_text must be true (non-negotiable: without the text "
                          "the model read, quotes cannot be re-checked; 108,549/148,186 rows "
                          "were lost this way on the POTS build)")

    sem = cfg["condition"]["semantics"]
    if sem not in ("population", "relevance"):
        raise ConfigError("condition.semantics must be 'population' or 'relevance'")
    if len(str(cfg["condition"]["semantics_statement"]).strip()) < 40:
        raise ConfigError("condition.semantics_statement must be a full written sentence")

    enum = cfg["condition"]["enum"]
    if not isinstance(enum, list) or not enum:
        raise ConfigError("condition.enum must be a non-empty list of {value, definition}")
    seen = set()
    for e in enum:
        if not isinstance(e, dict) or "value" not in e or "definition" not in e:
            raise ConfigError(f"condition.enum entry needs value AND definition: {e!r}")
        if not str(e["definition"]).strip() or len(str(e["definition"]).strip()) < 20:
            raise ConfigError(f"condition.enum '{e['value']}' has no written definition")
        if e["value"] in seen:
            raise ConfigError(f"duplicate condition value {e['value']}")
        seen.add(e["value"])
    for must in ("multiple", "general"):
        if must not in seen:
            w(f"condition.enum lacks '{must}' — SCHEMA.md recommends keeping it")

    tc = cfg["tier"]["target_chunks"]
    if not isinstance(tc, list) or not all(isinstance(x, int) for x in tc):
        raise ConfigError("tier.target_chunks must be an EXPLICIT list of ints (never range(N))")

    sub = cfg["condition"].get("subtype")
    if sub:
        for k in ("field", "condition", "values"):
            if k not in sub:
                raise ConfigError(f"condition.subtype.{k} missing")
        if sub["condition"] not in seen:
            raise ConfigError("condition.subtype.condition is not in condition.enum")

    pinned = cfg.setdefault("pinned", {})
    for k, v in PINNED_DEFAULTS.items():
        if k not in pinned:
            pinned[k] = v
        elif pinned[k] != v:
            w(f"pinned.{k}={pinned[k]} deviates from the calibrated value {v}")
    if pinned["n_inflight"] > 3:
        w("pinned.n_inflight > 3: Europe PMC rate-limits on aggregate concurrency; "
          "~8.5 req/s produced a 92% blocked rate")
    for k, v in MODEL_DEFAULTS.items():
        cfg["model"].setdefault(k, v)
    if cfg["model"]["max_tokens_screen"] < 2000:
        w("model.max_tokens_screen < 2000: at 600, 180 JSON responses came back empty")

    if cfg.get("unescape_source_text") is None:
        cfg["unescape_source_text"] = True
    if cfg["unescape_source_text"] is False:
        w("unescape_source_text=false reproduces the POTS behaviour: 128 shipped quotes "
          "carry raw HTML entity codes. Set true for any new build.")

    cfg.setdefault("abbreviation_collisions", [])
    if not cfg["abbreviation_collisions"]:
        w("abbreviation_collisions is empty — every abbreviation-based query inherits the "
          "wrong-sense contamination described in PITFALLS.md §6")
    cfg.setdefault("artifacts", {})
    # Per-area filename prefix for chunk artifacts (findings parquet + retention archive).
    # The driver's done-detector searches the WHOLE project's artifact store by filename, so a
    # second area in the same project would otherwise find the first area's chunk_000_findings.parquet
    # and dispatch nothing (measured on the EDS dry run: chunks 0-2 were "already done" from the
    # POTS build). Default "<slug>_"; the POTS config sets "" to keep its legacy filenames.
    if "file_prefix" not in cfg["artifacts"] or cfg["artifacts"]["file_prefix"] is None:
        cfg["artifacts"]["file_prefix"] = f"{cfg['area']['slug']}_"
    ba = cfg["artifacts"].get("build_after")
    if ba is not None and not re.match(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}(:\d{2})?Z?)?$", str(ba)):
        raise ConfigError("artifacts.build_after must be an ISO date or datetime (e.g. 2026-10-07T09:00:00Z)")
    fp = str(cfg["artifacts"]["file_prefix"])
    if fp and not re.fullmatch(r"[a-z0-9_]+_", fp):
        raise ConfigError("artifacts.file_prefix must be lowercase [a-z0-9_] and end with '_' (or be empty)")
    cfg.setdefault("prompt", {})
    return warnings


def load_config(path=None, warn=print):
    """Load + validate. `path` defaults to $CORPUS_CONFIG."""
    path = path or os.environ.get("CORPUS_CONFIG")
    if not path:
        raise ConfigError("no config path given and CORPUS_CONFIG is not set")
    with open(path, encoding="utf-8") as fh:
        if path.lower().endswith(".json"):
            cfg = json.load(fh)
        else:
            global yaml
            if yaml is None:
                try:
                    import yaml as _y; yaml = _y
                except ImportError as e:
                    raise ImportError("pyyaml is required to read a .yaml config: pip install pyyaml, "
                                      "or point CORPUS_CONFIG at the .json twin written by "
                                      "`python pipeline/config.py export-json <config.yaml>`") from e
            cfg = yaml.safe_load(fh)
    if not isinstance(cfg, dict):
        raise ConfigError(f"{path} did not parse to a mapping")
    cfg["_path"] = os.path.abspath(path)
    cfg["_warnings"] = validate(cfg, warn=warn)
    return cfg


# ---------------------------------------------------------------- rendering ----

def condition_values(cfg):
    return [e["value"] for e in cfg["condition"]["enum"]]


def prompt_slots(cfg):
    """The {{slot}} values derived from a config. Rendering is exact-string: the POTS
    config reproduces tier4_prompt.txt byte-for-byte (config_tests.py asserts this)."""
    sub = cfg["condition"].get("subtype") or {}
    p = cfg.get("prompt", {})
    return {
        "area_description": cfg["area"]["description_for_prompt"],
        "condition_enum_json": json.dumps(condition_values(cfg), separators=(",", ":")),
        "subtype_field": sub.get("field", "condition_subtype"),
        "subtype_condition": sub.get("condition", condition_values(cfg)[0]),
        "subtype_values": "[" + ",".join("'%s'" % v for v in sub.get("values", ["not specified"])) + "]",
        "pathogen_examples": p.get("pathogen_examples", "EBV/Lyme"),
        "phenotype_example": p.get("phenotype_example",
                                   "orthostatic intolerance; chronic fatigue; joint hypermobility"),
        "instrument_examples": p.get("instrument_examples", "COMPASS-31, SF-36, Beighton"),
        "cohort_examples": p.get("cohort_examples", "RECOVER, UK Biobank, DecodeME, All of Us"),
    }


def render_prompt(cfg, template_text=None):
    """Fill every {{slot}}; raise if a slot has no value or a value is unused."""
    if template_text is None:
        tpath = cfg.get("prompt", {}).get("template") or os.path.join(HERE, "prompt_template.txt")
        if not os.path.isabs(tpath):
            tpath = os.path.join(REPO, tpath)
        template_text = open(tpath, encoding="utf-8").read()
    slots = prompt_slots(cfg)
    used = set()

    def sub(m):
        k = m.group(1)
        if k not in slots:
            raise ConfigError(f"prompt template slot {{{{{k}}}}} has no config value")
        used.add(k)
        return str(slots[k])
    out = SLOT_RX.sub(sub, template_text)
    leftover = SLOT_RX.findall(out)
    if leftover:
        raise ConfigError(f"unrendered slots: {leftover}")
    return out


def render_scope(cfg):
    """Screening scope text. `scope.text` is used verbatim when present (the POTS build
    hand-wrote it); otherwise it is composed from families + off-topic senses with the
    NOT-EXHAUSTIVE framing that stops a closed list reading as exhaustive."""
    sc = cfg["scope"]
    if sc.get("text"):
        return sc["text"].rstrip("\n")
    fam = "\n".join(f"* {f['name']}: {', '.join(f.get('members', []))}" for f in cfg["area"]["families"])
    off = "\n".join(f"    {c['term']} = {' or '.join(c['offtopic_senses'])} (not {c['corpus_sense']});"
                    for c in cfg.get("abbreviation_collisions", []))
    drift = sc.get("topical_drift", "")
    return (f"SCOPE — a biomarker/mechanism corpus for {cfg['area']['name']}.\n\n"
            f"IN SCOPE is BROAD and the lists below are ILLUSTRATIVE, NOT EXHAUSTIVE:\n{fam}\n\n"
            f"OFF TOPIC means one of two specific things:\n"
            f"(1) WRONG ABBREVIATION SENSE — the paper uses an abbreviation in a sense foreign to this corpus:\n{off}\n"
            f"(2) TOPICAL DRIFT — {drift}\n\n"
            f"When a paper studies one of many diseases including an in-scope one, that is IN SCOPE.\n"
            f"If genuinely torn, answer \"uncertain\". Do NOT answer off_topic merely because a "
            f"condition is not named in the illustrative lists above.")


def bucket_definitions_text(cfg):
    """One line per bucket, for the Q7 judge prompt and the human worksheet."""
    lines = [f"`condition` semantics ({cfg['condition']['semantics']}): "
             f"{cfg['condition']['semantics_statement']}",
             f"definitions version: {cfg['condition']['definitions_version']}"]
    for e in cfg["condition"]["enum"]:
        lines.append(f"- {e['value']}: {e['definition']}")
    return "\n".join(lines)


def pipeline_config(cfg):
    """The JSON-serialisable subset the worker embeds (pipeline.configure())."""
    return {
        "model_id": cfg["model"]["id"],
        "max_tokens_fulltext": cfg["model"]["max_tokens_fulltext"],
        "max_tokens_abstract": cfg["model"]["max_tokens_abstract"],
        "pinned": dict(cfg["pinned"]),
        "retain_source_text": cfg["retain_source_text"],
        "unescape_source_text": cfg["unescape_source_text"],
        "subtype_field": (cfg["condition"].get("subtype") or {}).get("field", "condition_subtype"),
        "condition_enum": condition_values(cfg),
        "tier_number": cfg["tier"]["number"],
        "roster_vid": cfg["artifacts"].get("roster_vid"),
        "prompt_vid": cfg["artifacts"].get("prompt_vid"),
        "area_slug": cfg["area"]["slug"],
        "file_prefix": cfg["artifacts"]["file_prefix"],
        # 2026-10-07: ISO timestamp; chunk artifacts created BEFORE it are invisible to done/archive
        # detection. The EDS pilot marked chunk 0 "done" from the DRY RUN's eds_chunk_000_part*of3
        # files (2026-09-30, a 75-paper roster) and never extracted the full-build chunk 0.
        "build_after": cfg["artifacts"].get("build_after"),
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3 or sys.argv[1] not in ("render-prompt", "render-scope", "validate",
                                                 "bucket-definitions", "export-json"):
        print("usage: python config.py render-prompt|render-scope|validate|bucket-definitions|export-json "
              "<config.yaml>", file=sys.stderr)
        sys.exit(2)
    cfg = load_config(sys.argv[2], warn=lambda m: print(m, file=sys.stderr))
    if sys.argv[1] == "render-prompt":
        sys.stdout.write(render_prompt(cfg))
    elif sys.argv[1] == "render-scope":
        print(render_scope(cfg))
    elif sys.argv[1] == "bucket-definitions":
        print(bucket_definitions_text(cfg))
    elif sys.argv[1] == "export-json":
        # the validated, defaults-filled config as JSON for the stdlib-only orchestration kernel
        out = {k: v for k, v in cfg.items() if not k.startswith("_")}
        out["_exported_from"] = os.path.basename(cfg["_path"])
        print(json.dumps(out, indent=1, ensure_ascii=False))
    else:
        print(json.dumps({"ok": True, "warnings": cfg["_warnings"],
                          "condition_enum": condition_values(cfg), "model_id": cfg["model"]["id"]},
                         indent=1))
