"""Config + template tests. Every case is a computed pass/fail.

  1. The POTS config renders pipeline/prompt_template.txt to EXACTLY the pinned
     tier4_prompt.txt (byte-identical), so templating changed nothing the model sees.
  2. The POTS scope text renders to the pinned tier4_scope_definition.txt.
  3. Planted regressions in a config are REJECTED: non-literal model id, retain_source_text
     false, a bucket without a written definition, target_chunks as a string, a subtype
     condition outside the enum.
  4. TEMPLATE.yaml itself is a valid config once its placeholders are filled — i.e. the
     template documents every required field.

Run:  python config_tests.py [path/to/pinned/tier4_prompt.txt path/to/tier4_scope_definition.txt]
"""
import copy, json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import yaml
from config import load_config, render_prompt, render_scope, validate, ConfigError, prompt_slots, SLOT_RX

POTS = os.path.join(HERE, "..", "config", "pots_dysautonomia.yaml")
TEMPLATE = os.path.join(HERE, "..", "config", "TEMPLATE.yaml")


def _case(name, got, want, note=""):
    return {"case": name, "got": got, "want": want, "pass": got == want, "note": note}


def test_pots_reproduces_pinned(prompt_ref=None, scope_ref=None):
    cfg = load_config(POTS, warn=None)
    cases = []
    rendered = render_prompt(cfg)
    # NB: "{{" alone is legitimate — the prompt escapes JSON braces for .format(); only a
    # {{identifier}} slot counts as unrendered.
    cases.append(_case("pots_prompt_has_no_unrendered_slot", SLOT_RX.findall(rendered), []))
    cases.append(_case("pots_prompt_keeps_runtime_format_slots",
                       all(s in rendered for s in ("{themes}", "{is_covid}", "{text_kind}")), True))
    cases.append(_case("pots_prompt_enum_rendered",
                       '"condition": one of ["POTS","ME_CFS","long_COVID","EDS_hypermobility","fibromyalgia",'
                       '"MCAS","dysautonomia","small_fiber_neuropathy","autism","migraine","IBS_gut",'
                       '"multiple","general"],' in rendered, True))
    cases.append(_case("pots_subtype_line_rendered",
                       "\"pots_subtype\": \"if condition=POTS and subtype named: one of "
                       "['hyperadrenergic','neurogenic','hypovolemic','not specified'], else null\"," in rendered, True))
    if prompt_ref and os.path.exists(prompt_ref):
        ref = open(prompt_ref, encoding="utf-8").read()
        cases.append(_case("pots_prompt_byte_identical_to_tier4_prompt", rendered == ref, True,
                           f"{len(rendered)} vs {len(ref)} chars"))
    if scope_ref and os.path.exists(scope_ref):
        ref = open(scope_ref, encoding="utf-8").read().rstrip("\n")
        cases.append(_case("pots_scope_identical_to_tier4_scope", render_scope(cfg) == ref, True))
    cases.append(_case("pots_model_id_literal", cfg["model"]["id"], "claude-sonnet-5"))
    cases.append(_case("pots_target_chunks_explicit_30", len(cfg["tier"]["target_chunks"]), 30))
    cases.append(_case("pots_every_bucket_defined",
                       all(len(e["definition"]) > 20 for e in cfg["condition"]["enum"]), True))
    return cases


def _reject(name, mutate):
    cfg = yaml.safe_load(open(POTS, encoding="utf-8"))
    mutate(cfg)
    try:
        validate(cfg, warn=None)
        return _case(name, "accepted", "rejected")
    except ConfigError as e:
        return _case(name, "rejected", "rejected", str(e)[:80])


def test_planted_rejections():
    def no_def(c): c["condition"]["enum"][5]["definition"] = ""
    def model_role(c): c["model"]["id"] = "a current reasoning model"
    def retain_off(c): c["retain_source_text"] = False
    def chunks_str(c): c["tier"]["target_chunks"] = "range(256, 286)"
    def bad_subtype(c): c["condition"]["subtype"]["condition"] = "NOT_A_BUCKET"
    def no_semantics(c): c["condition"]["semantics"] = "either"
    def dup(c): c["condition"]["enum"].append({"value": "POTS", "definition": "duplicate value planted here"})
    return [_reject("reject_bucket_without_definition", no_def),
            _reject("reject_non_literal_model_id", model_role),
            _reject("reject_retain_source_text_false", retain_off),
            _reject("reject_target_chunks_not_list", chunks_str),
            _reject("reject_subtype_condition_outside_enum", bad_subtype),
            _reject("reject_unknown_semantics", no_semantics),
            _reject("reject_duplicate_enum_value", dup)]


def test_template_is_complete():
    cfg = yaml.safe_load(open(TEMPLATE, encoding="utf-8"))
    cases = []
    try:
        validate(cfg, warn=None)
        cases.append(_case("template_validates_as_shipped", True, True,
                           "placeholder values satisfy every required field"))
    except ConfigError as e:
        cases.append(_case("template_validates_as_shipped", str(e), True))
    # the template's slots must all be rendered by the shipped prompt template
    try:
        r = render_prompt(cfg)
        cases.append(_case("template_renders_prompt", SLOT_RX.findall(r), []))
    except ConfigError as e:
        cases.append(_case("template_renders_prompt", str(e), False))
    cases.append(_case("template_documents_every_slot",
                       sorted(prompt_slots(cfg).keys()),
                       sorted(["area_description", "condition_enum_json", "subtype_field", "subtype_condition",
                               "subtype_values", "pathogen_examples", "phenotype_example",
                               "instrument_examples", "cohort_examples"])))
    return cases


def run_all(prompt_ref=None, scope_ref=None):
    groups = {"pots_reproduces_pinned": test_pots_reproduces_pinned(prompt_ref, scope_ref),
              "planted_rejections": test_planted_rejections(),
              "template_complete": test_template_is_complete()}
    n = sum(len(v) for v in groups.values()); npass = sum(1 for v in groups.values() for c in v if c["pass"])
    return {"n_cases": n, "n_pass": npass, "n_fail": n - npass, "all_pass": npass == n,
            "failures": [c for v in groups.values() for c in v if not c["pass"]], "groups": groups}


if __name__ == "__main__":
    pr = sys.argv[1] if len(sys.argv) > 1 else None
    sr = sys.argv[2] if len(sys.argv) > 2 else None
    r = run_all(pr, sr)
    print(json.dumps({k: v for k, v in r.items() if k != "groups"}, indent=1))
    sys.exit(0 if r["all_pass"] else 1)
