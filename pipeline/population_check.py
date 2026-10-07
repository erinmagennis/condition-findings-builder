"""Stage 3b — second-pass POPULATION check for screen-uncertain papers.

Added 2026-10-07 (EDS Tier-1 build). The two-arm screen's reconcile rule sends "LLM off_topic
against a rule-arm anchor" to `uncertain` (adjudicate; extract and flag). On a roster drawn by
anchor terms that class can be large (EDS: 435 of 1,463), and most are incidental mentions. This
module is the adjudication instrument the rule asks for: an INDEPENDENT, differently-worded
single-question pass — "does this paper STUDY one of the listed populations?" — judged against
the config's own bucket definitions (condition.semantics_statement + enum definitions), with a
verbatim deciding span. It is still a model judging a model and is declared as such; it never
excludes on its own: `combine()` excludes only where the first-pass LLM verdict was off_topic AND
this pass says no population is studied. Everything else is extracted and flagged.

    df2, summary = run_population_check(host, cfg, uncertain_df, out_prefix)
    final = combine(screen_report_df, df2)      # adds pass2_* columns and screen_final_verdict_v2
"""
import json, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import bucket_definitions_text, condition_values  # noqa: E402
from quote_trace import normalize as _norm, strip_tags  # noqa: E402
from helpers import plan_units  # noqa: E402

PROMPT = """You are checking ONE paper for a findings corpus whose `condition` field means the patient POPULATION the paper studied — not a condition the paper mentions, cites as a differential, lists as a risk factor, or uses as one covariate among many.

BUCKET DEFINITIONS (the populations that count):
{definitions}

Answer with ONLY a JSON object:
{{
  "studies_listed_population": true or false,
  "population_labels": a list of bucket values from the definitions above that name a population this paper actually STUDIES (empty list if none; use "general" only if the paper studies a connective-tissue/hypermobility question with no listed population),
  "span": "a VERBATIM phrase copied from the title or abstract that shows the population was studied (or that shows it was only mentioned) — required",
  "reason": "one sentence"
}}

TITLE: {title}

ABSTRACT: {abstract}
"""


def _parse(txt):
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(json)?\s*|\s*```$", "", s).strip()
    i, j = s.find("{"), s.rfind("}")
    if i == -1 or j == -1:
        raise ValueError("no JSON object")
    d = json.loads(s[i:j + 1])
    for k in ("studies_listed_population", "population_labels", "span"):
        if k not in d:
            raise KeyError(f"missing key {k!r}")
    if not isinstance(d["studies_listed_population"], bool):
        raise ValueError("studies_listed_population must be boolean")
    return d


def run_population_check(host, cfg, df, out_prefix="popcheck", max_concurrency=8, log=print):
    import pandas as pd
    defs = bucket_definitions_text(cfg); enum = set(condition_values(cfg))
    model = cfg["model"]["id"]; mt = int(cfg["model"]["max_tokens_screen"])
    rows = df.to_dict("records")
    prompts = [PROMPT.format(definitions=defs, title=r.get("title") or "", abstract=r.get("abstract") or "") for r in rows]
    plan = plan_units(len(rows), int(max(len(p) for p in prompts) / 3.5), mt)
    if not plan.get("fits_in_frame"):
        raise SystemExit(f"population check does not fit in one frame: split into {plan['n_units']} units")
    res = host.llm([{"prompt": p, "max_tokens": mt, "model": model} for p in prompts], max_concurrency=max_concurrency)
    assert len(res) == len(rows), f"parse-completeness: {len(res)} responses for {len(rows)} inputs"
    out = []; n_err = 0; toks = 0
    for r, resp in zip(rows, res):
        rec = dict(pmid=str(r["pmid"]))
        if "error" in resp:
            n_err += 1; rec.update(pass2_studies_population=None, pass2_labels=None, pass2_span=None, pass2_span_verbatim=None,
                                   pass2_reason=None, pass2_error=str(resp["error"])[:200]); out.append(rec); continue
        toks += (resp.get("usage") or {}).get("output_tokens", 0)
        try:
            d = _parse(resp["text"])
            labels = [l for l in (d.get("population_labels") or []) if isinstance(l, str)]
            bad = [l for l in labels if l not in enum]
            span = d.get("span"); ok = bool(span) and _norm(strip_tags(str(span))) in _norm(strip_tags(f"{r.get('title') or ''} {r.get('abstract') or ''}"))
            rec.update(pass2_studies_population=bool(d["studies_listed_population"]), pass2_labels=";".join(labels) or None,
                       pass2_labels_outside_enum=";".join(bad) or None, pass2_span=span, pass2_span_verbatim=ok,
                       pass2_reason=d.get("reason"), pass2_error=None)
        except Exception as e:
            n_err += 1; rec.update(pass2_studies_population=None, pass2_labels=None, pass2_span=None, pass2_span_verbatim=None,
                                   pass2_reason=None, pass2_error=f"parse: {type(e).__name__}: {str(e)[:120]}")
        out.append(rec)
    assert len(out) == len(rows)
    o = pd.DataFrame(out); o.to_csv(f"{out_prefix}_report.csv", index=False)
    summary = dict(n=int(len(o)), n_errors=n_err, output_tokens=toks, model_id=model, max_tokens=mt, plan=plan,
                   studies_population=o.pass2_studies_population.value_counts(dropna=False).astype(str).to_dict(),
                   span_verbatim=dict(n=int(o.pass2_span_verbatim.notna().sum()), ok=int(o.pass2_span_verbatim.fillna(False).sum())),
                   labels_outside_enum=int(o.pass2_labels_outside_enum.notna().sum()),
                   basis="second-pass population check over screen-uncertain papers only; the paper is judged against the config's bucket definitions; a model judging a model, declared")
    json.dump(summary, open(f"{out_prefix}_summary.json", "w"), indent=1, default=str)
    return o, summary


def combine(screen_df, pass2_df):
    """Exclude ONLY where pass-1 LLM said off_topic AND pass-2 says no listed population is studied
    (with a verbatim span). Everything else keeps its pass-1 verdict; uncertain stays uncertain
    (extracted and flagged). A pass-2 error never changes a verdict."""
    import pandas as pd
    s = screen_df.copy(); s["pmid"] = s.pmid.astype(str); p = pass2_df.copy(); p["pmid"] = p.pmid.astype(str)
    m = s.merge(p, on="pmid", how="left")
    both_off = (m.final_verdict == "uncertain") & (m.llm_verdict == "off_topic") & (m.pass2_studies_population == False) & (m.pass2_span_verbatim == True)
    m["screen_final_verdict_v2"] = m.final_verdict.where(~both_off, "off_topic")
    m["exclusion_class_v2"] = m.exclusion_class.where(~both_off, m.llm_offtopic_class.fillna("topical_drift"))
    m["exclusion_basis_v2"] = None
    m.loc[both_off, "exclusion_basis_v2"] = "pass1 LLM off_topic + pass2 no listed population studied (both with verbatim spans); rule-arm anchor was a mention"
    m.loc[m.screen_final_verdict_v2 == "off_topic", "exclusion_basis_v2"] = m.loc[m.screen_final_verdict_v2 == "off_topic", "exclusion_basis_v2"].fillna("pass1 both arms (reconcile)")
    return m
