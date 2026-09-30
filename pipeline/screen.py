"""Stage 3 — two-arm screen (rules + LLM), reconciled with helpers.screen_reconcile.

Added 2026-09-30 (EDS dry run). RUNBOOK stage 3 described the screen but the repository held
no code for it; the POTS build's screen was an unsaved cell. This module is the procedure as
written in the RUNBOOK, reading everything condition-specific from the config:

  * Arm 1 (rules): `screen.rule_anchors` regexes over title+abstract -> anchor | no_anchor;
    a `draw.contamination_patterns` hit WITHOUT an anchor -> off_topic (positive wrong-sense
    evidence, which forces adjudication and never silent exclusion).
  * Arm 2 (LLM): the rendered scope text (config.render_scope) + title + abstract; JSON verdict
    in_scope | off_topic | uncertain with a VERBATIM span for any in_scope verdict; `max_tokens`
    = model.max_tokens_screen (>= 2000); the pinned model id.
  * Reconcile with helpers.screen_reconcile; run helpers.screen_instrument_alarm (>= 3x arm
    disagreement = broken instrument, stop). Parse-completeness is asserted: the parser RAISES
    on a missing key, and len(parsed) must equal len(input).
  * Every exclusion is classified (wrong_abbreviation_sense | topical_drift | unclassified) —
    a final summary that leaves exclusions unclassified cannot support claims about them.

Runs inside the orchestration kernel (needs `host.llm`):
    import screen; rep = screen.run_screen(host, cfg, roster_df, out_prefix="screen_eds")
Writes <prefix>_report.csv (per-paper verdicts), <prefix>_excluded_pmids.csv, <prefix>_summary.json.
"""
import json, os, re, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import render_scope  # noqa: E402
from helpers import screen_reconcile, screen_instrument_alarm, plan_units  # noqa: E402
from quote_trace import normalize as _norm, strip_tags  # noqa: E402  symmetric normalization


def span_is_verbatim(span, title, abstract):
    """Europe PMC titles/abstracts carry HTML tags and entities (<i>PLOD1</i>, &#x2009;). The
    model reads them and copies a clean span; comparing raw vs clean fails. Normalize BOTH
    sides with the census normalizer — never one side (PITFALLS §3)."""
    if not span:
        return False
    return _norm(strip_tags(span)) in _norm(strip_tags(f"{title or ''} {abstract or ''}"))


def reconcile(report_df, roster):
    """Pure function: recompute final verdicts + exclusion classes from a saved per-paper
    report and the roster (no model calls), so a comparator fix never re-spends the screen."""
    import pandas as pd
    t = roster.assign(pmid=roster.pmid.astype(str)).set_index("pmid")
    df = report_df.copy(); df["pmid"] = df.pmid.astype(str)
    ok = [span_is_verbatim(sp, t.title.get(pm), t.abstract.get(pm)) if isinstance(sp, str) else False
          for pm, sp in zip(df.pmid, df.llm_span)]
    df["llm_span_verbatim"] = ok
    df["final_verdict"] = [screen_reconcile(rv, lv, sp if v else None)
                           for rv, lv, sp, v in zip(df.rule_verdict, df.llm_verdict, df.llm_span, ok)]
    df["exclusion_class"] = [((lc if isinstance(lc, str) else
                               ("wrong_abbreviation_sense" if isinstance(rc, str) and rc else "unclassified"))
                              if fv == "off_topic" else None)
                             for fv, lc, rc in zip(df.final_verdict, df.llm_offtopic_class, df.rule_contamination)]
    return df

LLM_PROMPT = """{scope}

You are screening ONE paper for this corpus. Read the title and abstract below and answer with ONLY a JSON object:
{{
  "verdict": one of ["in_scope","off_topic","uncertain"],
  "span": "a VERBATIM phrase copied from the title or abstract that decides the verdict (required for in_scope; give one for off_topic too when possible), else null",
  "offtopic_class": if verdict is off_topic, one of ["wrong_abbreviation_sense","topical_drift"], else null,
  "reason": "one sentence"
}}

TITLE: {title}

ABSTRACT: {abstract}
"""


def rule_arm(title, abstract, anchors, contamination):
    blob = f"{title or ''} {abstract or ''}"
    anchor = any(a.search(blob) for a in anchors)
    contam = [c.pattern for c in contamination if c.search(blob)]
    if anchor:
        return "anchor", contam
    if contam:
        return "off_topic", contam
    return "no_anchor", contam


def _parse(txt):
    """RAISE on a missing key; never skip. Unparsed is missing data, not a neutral omission."""
    s = txt.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(json)?\s*|\s*```$", "", s).strip()
    i, j = s.find("{"), s.rfind("}")
    if i == -1 or j == -1:
        raise ValueError("no JSON object in response")
    d = json.loads(s[i:j + 1])
    for k in ("verdict", "span"):
        if k not in d:
            raise KeyError(f"screen response lacks key {k!r}")
    if d["verdict"] not in ("in_scope", "off_topic", "uncertain"):
        raise ValueError(f"bad verdict {d['verdict']!r}")
    return d


def run_screen(host, cfg, roster, out_prefix="screen", max_concurrency=8, log=print):
    import pandas as pd
    sc = cfg.get("screen") or {}
    anchors = [re.compile(p, re.I) for p in sc.get("rule_anchors", [])]
    if not anchors:
        raise SystemExit("config.screen.rule_anchors is empty: the rule arm needs in-scope anchor regexes")
    contamination = [re.compile(p, re.I) for p in (cfg.get("draw") or {}).get("contamination_patterns", [])]
    scope = render_scope(cfg)
    model = cfg["model"]["id"]; mt = int(cfg["model"]["max_tokens_screen"])
    if mt < 2000:
        raise SystemExit("model.max_tokens_screen < 2000: at 600, 180 JSON responses came back empty")

    rows = roster.to_dict("records")
    prompts = [LLM_PROMPT.format(scope=scope, title=r.get("title") or "", abstract=r.get("abstract") or "")
               for r in rows]
    approx_tokens = int(max(len(p) for p in prompts) / 3.5)
    plan = plan_units(len(rows), approx_tokens, mt)
    log(f"[screen] plan: {json.dumps(plan)}")
    if not plan.get("fits_in_frame"):
        raise SystemExit(f"screen does not fit in one frame: split into {plan['n_units']} units")

    reqs = [{"prompt": p, "max_tokens": mt, "model": model} for p in prompts]
    res = host.llm(reqs, max_concurrency=max_concurrency)
    assert len(res) == len(rows), f"parse-completeness: {len(res)} responses for {len(rows)} inputs"

    out = []; n_err = 0; out_tokens = 0
    for r, resp in zip(rows, res):
        rv, contam = rule_arm(r.get("title"), r.get("abstract"), anchors, contamination)
        rec = dict(pmid=str(r["pmid"]), theme=r.get("theme"), chunk_id=r.get("chunk_id"),
                   rule_verdict=rv, rule_contamination="; ".join(contam) if contam else None,
                   query_contamination=bool(r.get("query_contamination")))
        if "error" in resp:
            n_err += 1
            rec.update(llm_verdict=None, llm_span=None, llm_offtopic_class=None, llm_reason=None,
                       llm_error=str(resp["error"])[:200], llm_stop_reason=None)
        else:
            out_tokens += (resp.get("usage") or {}).get("output_tokens", 0)
            try:
                d = _parse(resp["text"])
                span = d.get("span")
                # a span must actually be verbatim in title+abstract or it is not a span
                span_ok = span_is_verbatim(span, r.get("title"), r.get("abstract"))
                rec.update(llm_verdict=d["verdict"], llm_span=span, llm_span_verbatim=span_ok,
                           llm_offtopic_class=d.get("offtopic_class"), llm_reason=d.get("reason"),
                           llm_error=None, llm_stop_reason=resp.get("stop_reason"))
                if not span_ok:
                    span = None
            except Exception as e:
                n_err += 1; span = None
                rec.update(llm_verdict=None, llm_span=None, llm_span_verbatim=False, llm_offtopic_class=None,
                           llm_reason=None, llm_error=f"parse: {type(e).__name__}: {str(e)[:120]}",
                           llm_stop_reason=resp.get("stop_reason"))
        rec["final_verdict"] = screen_reconcile(rec["rule_verdict"], rec.get("llm_verdict"), span)
        if rec["final_verdict"] == "off_topic":
            rec["exclusion_class"] = (rec.get("llm_offtopic_class") or
                                      ("wrong_abbreviation_sense" if rec["rule_contamination"] else "unclassified"))
        else:
            rec["exclusion_class"] = None
        out.append(rec)
    assert len(out) == len(rows)
    df = pd.DataFrame(out)
    return summarize(df, out_prefix, dict(n_llm_errors=n_err, llm_output_tokens=out_tokens, model_id=model,
                                          max_tokens_screen=mt, plan=plan))


def summarize(df, out_prefix, extra=None):
    import pandas as pd
    df.to_csv(f"{out_prefix}_report.csv", index=False)
    exc = df[df.final_verdict == "off_topic"][["pmid", "theme", "exclusion_class", "llm_span", "llm_reason"]]
    exc.to_csv(f"{out_prefix}_excluded_pmids.csv", index=False)
    # Papers the LLM arm called off_topic but the rule arm anchored: screen_reconcile returns
    # `uncertain` (adjudicate; extracted and flagged). They are LISTED here so the adjudication
    # queue is auditable — "uncertain" must never make a verdict disappear.
    adj = df[(df.llm_verdict == "off_topic") & (df.final_verdict != "off_topic")][
        ["pmid", "theme", "rule_verdict", "final_verdict", "llm_offtopic_class", "llm_span", "llm_reason"]]
    adj.to_csv(f"{out_prefix}_llm_offtopic_flagged.csv", index=False)

    rule_off = int((df.rule_verdict == "off_topic").sum()); llm_off = int((df.llm_verdict == "off_topic").sum())
    alarm = screen_instrument_alarm(rule_off, llm_off, len(df))
    per_theme = {}
    for t, g in df.groupby("theme"):
        per_theme[t] = dict(n=int(len(g)), off_topic=int((g.final_verdict == "off_topic").sum()),
                            uncertain=int((g.final_verdict == "uncertain").sum()),
                            off_topic_pct=round(100 * (g.final_verdict == "off_topic").mean(), 2))
    summary = dict(n_screened=int(len(df)), **(extra or {}),
                   final=df.final_verdict.value_counts().to_dict(),
                   rule=df.rule_verdict.value_counts().to_dict(),
                   llm=df.llm_verdict.value_counts(dropna=False).astype(str).to_dict() if len(df) else {},
                   llm_span_verbatim_rate=dict(n=int(df.llm_span_verbatim.notna().sum()) if "llm_span_verbatim" in df else 0,
                                               ok=int(df.llm_span_verbatim.fillna(False).sum()) if "llm_span_verbatim" in df else 0),
                   exclusion_classes=df.exclusion_class.value_counts(dropna=True).to_dict(),
                   n_unclassified_exclusions=int((df.exclusion_class == "unclassified").sum()),
                   instrument_alarm=alarm,
                   instrument_alarm_note=("screen_instrument_alarm floors each arm at 1, so with an anchor-drawn roster "
                                          "(rule off_topic == 0 by construction) ANY llm off_topic count >= 3 fires. "
                                          "Audit the LLM off_topic list by title before treating the alarm as a prompt fault."),
                   per_theme=per_theme,
                   n_llm_offtopic_flagged_not_excluded=int(len(adj)),
                   basis=("denominator = every rostered paper in the target chunks. final_verdict follows "
                          "helpers.screen_reconcile: off_topic ONLY when both arms agree (or LLM off_topic with no "
                          "rule anchor) -> excluded and listed in *_excluded_pmids.csv; an LLM off_topic against a "
                          "rule-arm anchor -> uncertain -> EXTRACTED and flagged (screen_verdict_uncertain) and listed "
                          "in *_llm_offtopic_flagged.csv for adjudication; LLM uncertain -> uncertain."))
    json.dump(summary, open(f"{out_prefix}_summary.json", "w"), indent=1, default=str)
    return df, summary
