"""Calibration harness for an AI fidelity scorer.

Two arms, and they answer different questions:

  PLANTED arm  - inject known defects, measure catch rate per defect class and
                 false-positive rate per question on clean rows. Needs NO human
                 scores, so it runs before anyone reads a paper. If the judge
                 cannot catch a planted direction flip, calibration is moot.
  AGREEMENT arm - run the judge blind on the same rows a human scored, measure
                 per-question agreement. Needs the returned human worksheet.

A question is machine-extendable only if it clears BOTH: high catch rate on its
defect class AND low false-positive rate on clean rows AND high agreement with
the human scores. Anything else stays human.

Independence note: the judge must read source text FETCHED FRESH, not the
retained extraction archive. Reading the same bytes the extractor read is the
most correlated possible setup and the one the audit exists to avoid.

BUILDER (condition-agnostic): call configure(cfg) with a loaded config before
building prompts or planting. It rewrites Q7 (condition bucket) from the config's
WRITTEN bucket definitions and Q10 (paper in scope) from the area description, and
takes the condition_relabel alternatives from the enum. The module-level defaults
are the POTS wording the pilot ran with (CALIBRATION_REPORT.md, arm C).

Measured on the POTS pilot (24 clean rows, same model, same source text): Q7's
flag rate was 41.7% with no definitions in the prompt, 0% with a definition later
found to be wrong (MCAS "includes mastocytosis"), 12.5% with the decided definition
— and those 3/24 were exactly the three mastocytosis-labelled-MCAS papers. The
question is machine-extendable only with the definitions in the prompt.
"""
import json
import re
import random

QUESTIONS = [
    ("Q1_quote_verbatim", "Does provenance_quote appear in the source text as written?",
     "Y | reformatted | N"),
    ("Q2_direction_correct", "Is the stated direction what the source reports for this entity?",
     "Y | N"),
    ("Q3_quote_supports_finding",
     "Does THIS quote support THIS finding, or is the finding drawn from elsewhere "
     "or overreaching what the sentence says?", "Y | partial | N"),
    ("Q4_tier_justified", "Does evidence_tier match the study design the source describes?",
     "Y | N"),
    ("Q5_causality_not_overstated",
     "Is causal_or_correlational honest about what the source actually showed?", "Y | N"),
    ("Q6_numbers_correct",
     "Are quantitative_detail / effect_size correct per the source?", "Y | N | NA"),
    ("Q7_condition_correct",
     "Is the condition label the right BUCKET for this finding? The field is a coarse enum, "
     "not a precise diagnosis: MCAS means mast cell activation syndrome specifically and "
     "EXCLUDES mastocytosis, which is a distinct clonal diagnosis; EDS_hypermobility covers joint hypermobility and hypermobility spectrum "
     "disorder as well as Ehlers-Danlos; dysautonomia covers any autonomic disorder including "
     "MSA, syncope and orthostatic intolerance; 'general' is the correct answer when no listed "
     "condition is the paper's subject; 'multiple' when several are. Answer N only if the "
     "finding does not belong in the assigned bucket at all.", "Y | N"),
    ("Q8_entity_id_correct",
     "Does entity_ontology_label denote what the entity name means in this source's context?",
     "Y | N | NA"),
    ("Q9_hpo_terms_complete_and_correct",
     "Do hpo_labels_verified capture the phenotypes this finding describes - none wrong, "
     "none missing?", "Y | partial | N | NA"),
    ("Q10_paper_in_scope",
     "Is this paper in scope for a biomarker/mechanism corpus on POTS, dysautonomia and "
     "related conditions (ME/CFS, EDS/hypermobility, fibromyalgia, MCAS, migraine, "
     "small-fibre neuropathy, long COVID, autism, IBS)?", "Y | N | NA"),
]

_Q7_IDX = 6
_Q10_IDX = 9
_CONDITION_ALTS = ["POTS", "ME_CFS", "fibromyalgia", "autism", "IBS_gut"]   # POTS default


def configure(cfg):
    """Rewrite the area-dependent questions from the config (pipeline/config.py dict).

    Q7 carries EVERY bucket definition verbatim plus the population-vs-relevance
    statement; Q10 carries the area name and families. Returns the two rendered
    question texts so a report can quote exactly what the judge was asked."""
    global _CONDITION_ALTS
    cond = cfg["condition"]
    defs = "; ".join(f"{e['value']} = {str(e['definition']).strip()}" for e in cond["enum"])
    q7 = ("Is the condition label the right BUCKET for this finding? The field is a coarse "
          f"enum, not a precise diagnosis, and `condition` means ({cond['semantics']}): "
          f"{str(cond['semantics_statement']).strip()} Bucket definitions "
          f"({cond['definitions_version']}): {defs}. Answer N only if the finding does not "
          "belong in the assigned bucket at all.")
    fams = "; ".join(f"{f['name']}: {', '.join(map(str, f.get('members', [])))}"
                     for f in cfg["area"]["families"])
    q10 = (f"Is this paper in scope for a biomarker/mechanism corpus on {cfg['area']['name']}? "
           f"In-scope families (illustrative, not exhaustive): {fams}.")
    QUESTIONS[_Q7_IDX] = ("Q7_condition_correct", q7, "Y | N")
    QUESTIONS[_Q10_IDX] = ("Q10_paper_in_scope", q10, "Y | N | NA")
    vals = [e["value"] for e in cond["enum"] if e["value"] not in ("multiple", "general")]
    _CONDITION_ALTS = vals or [e["value"] for e in cond["enum"]]
    return q7, q10


# Row fields that are verbatim or near-verbatim source text and therefore must be
# presented to the judge under the same normalization as the source.
QUOTE_LIKE_FIELDS = {"provenance_quote", "quantitative_detail"}

ROW_FIELDS = ["condition", "entity", "entity_kind", "direction", "direction_reference",
              "finding_text", "provenance_quote", "provenance_section",
              "quantitative_detail", "effect_size", "causal_or_correlational",
              "evidence_tier", "certainty", "entity_ontology_id_resolved",
              "entity_ontology_label", "hpo_labels_verified"]

# defect class -> (the question that should catch it, mutation)
DEFECT_TARGET = {
    "direction_flip": "Q2_direction_correct",
    "quote_swap": "Q1_quote_verbatim",
    "causal_upgrade": "Q5_causality_not_overstated",
    "number_perturb": "Q6_numbers_correct",
    "condition_relabel": "Q7_condition_correct",
    "tier_upgrade": "Q4_tier_justified",
    "entity_label_swap": "Q8_entity_id_correct",
    "hpo_inject": "Q9_hpo_terms_complete_and_correct",
}

_FLIP = {"elevated": "depleted/reduced", "depleted/reduced": "elevated"}


def plant(row, defect, donor_pool, rng):
    """Return (mutated_row_dict, applicable) - applicable False when the row
    cannot carry that defect (e.g. no number to perturb)."""
    r = dict(row)
    if defect == "direction_flip":
        if r.get("direction") not in _FLIP:
            return r, False
        r["direction"] = _FLIP[r["direction"]]
    elif defect == "quote_swap":
        donors = [q for q in donor_pool if isinstance(q, str) and len(q) > 80
                  and q != r.get("provenance_quote")]
        if not donors:
            return r, False
        r["provenance_quote"] = rng.choice(donors)
    elif defect == "causal_upgrade":
        if str(r.get("causal_or_correlational")) == "causal-claim":
            return r, False
        r["causal_or_correlational"] = "causal-claim"
    elif defect == "number_perturb":
        v = r.get("quantitative_detail")
        if not isinstance(v, str) or not re.search(r"\d", v):
            return r, False
        def bump(m):
            dig = m.group(0)
            return str((int(dig) + 3) % 10) if dig.isdigit() else dig
        r["quantitative_detail"] = re.sub(r"\d", bump, v, count=2)
    elif defect == "condition_relabel":
        alts = [c for c in _CONDITION_ALTS if c != r.get("condition")]
        if not alts:
            return r, False
        r["condition"] = rng.choice(alts)
    elif defect == "tier_upgrade":
        if str(r.get("evidence_tier")) in ("meta-analysis", "RCT"):
            return r, False
        r["evidence_tier"] = "meta-analysis"
    elif defect == "entity_label_swap":
        if not isinstance(r.get("entity_ontology_label"), str) or not r["entity_ontology_label"]:
            return r, False
        r["entity_ontology_label"] = rng.choice(
            ["Dog Diseases", "Iguanas", "Liver abscess", "Increased body fat percentage"])
    elif defect == "hpo_inject":
        cur = r.get("hpo_labels_verified")
        if not isinstance(cur, str) or not cur:
            return r, False
        r["hpo_labels_verified"] = cur + "; hyperphosphatemia"
    else:
        raise ValueError(defect)
    return r, True


def build_judge_prompt(row, source_text, source_kind="abstract"):
    lines = ["You are auditing one machine-extracted research finding against its source.",
             "",
             f"SOURCE TEXT ({source_kind}) - this is the complete source available for this "
             "paper. Judge only against it.",
             "-----", source_text.strip()[:60000], "-----", "",
             "EXTRACTED ROW:"]
    for f in ROW_FIELDS:
        v = row.get(f)
        if v is None or (isinstance(v, float) and v != v) or v == "":
            v = "null"
        elif f in QUOTE_LIKE_FIELDS and isinstance(v, str):
            # SYMMETRY (2026-09-16): the source above has been through
            # normalize_source(); the row text must be too, or a quote that is
            # verbatim to what the extractor read (`p&#x202f;<&#x2009;0.001`) is
            # judged against clean source and reads as a fabrication. 128 shipped
            # rows carry such codes. Stored data is never altered — this is the
            # judge's view only, and the record keeps the stored form.
            v = normalize_source(v)
        lines.append(f"  {f}: {v}")
    lines += ["", "Answer each question. Judge the ROW against the SOURCE, not for plausibility.",
              "Use NA only when the row has no content for that question (e.g. no identifier, "
              "no phenotype terms, no number). If the source does not contain the information "
              "needed to confirm a claim the row makes, that is N, not NA.", ""]
    for key, q, opts in QUESTIONS:
        lines.append(f"{key}: {q}  [{opts}]")
    lines += ["",
              'Return ONLY a JSON object: {"Q1_quote_verbatim": {"verdict": "...", '
              '"reason": "one short sentence"}, ...} with exactly one entry per question above.']
    return "\n".join(lines)


def parse_judge(text, strict=True):
    """Parse the judge's JSON. Key matching is PREFIX-based (Q1..Q10) because a
    model will rename `Q9_hpo_terms_complete_and_correct` to
    `Q9_hpo_labels_verified` and an exact-key parser drops it silently - which
    reads as 'untested' rather than as missing data. With strict=True a row
    missing any question raises, so a parse gap can never be mistaken for a
    measurement."""
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        if strict:
            raise ValueError("no JSON object in judge response")
        return None
    try:
        obj = json.loads(m.group(0))
    except Exception as e:
        if strict:
            raise ValueError("unparseable judge JSON: %s" % e)
        return None
    by_num = {}
    for k, v in obj.items():
        mm = re.match(r"\s*(Q\d+)", str(k))
        if mm:
            by_num.setdefault(mm.group(1), v)
    out = {}
    missing = []
    for key, _, _ in QUESTIONS:
        num = key.split("_")[0]
        v = by_num.get(num)
        if isinstance(v, dict):
            out[key] = (str(v.get("verdict", "")).strip(), str(v.get("reason", ""))[:300])
        elif v is not None:
            out[key] = (str(v).strip(), "")
        else:
            missing.append(key)
    if missing and strict:
        raise ValueError("judge omitted %s" % ",".join(missing))
    return out or None


def run_judge(host, rows, srcmap, model=None, max_concurrency=6, max_tokens=2500):
    """rows: list of dicts each carrying 'pmid' plus ROW_FIELDS. Returns list
    aligned to rows (None where the call or parse failed)."""
    reqs, idx = [], []
    for i, r in enumerate(rows):
        src = srcmap.get(str(r.get("pmid")))
        if not src:
            continue
        reqs.append({"prompt": build_judge_prompt(r, src),
                     "max_tokens": max_tokens,
                     "model": model or host.reasoning_model()})
        idx.append(i)
    raw = host.llm(reqs, max_concurrency=max_concurrency)
    assert len(raw) == len(reqs), f"judge returned {len(raw)} for {len(reqs)} requests"
    out = [None] * len(rows)
    texts = [None] * len(rows)
    errors = []
    for i, res in zip(idx, raw):
        if isinstance(res, dict) and res.get("text"):
            texts[i] = res["text"]
            try:
                out[i] = parse_judge(res["text"], strict=True)
            except ValueError as e:
                errors.append((i, str(e)))
        else:
            errors.append((i, "no text returned"))
    n_expected = len(reqs)
    n_ok = sum(1 for i in idx if out[i])
    return {"verdicts": out, "texts": texts, "errors": errors,
            "n_requested": n_expected, "n_parsed": n_ok,
            "complete": n_ok == n_expected,
            "n_no_source": len(rows) - n_expected}


def _flagged(verdict, question):
    """Did the judge flag this question as a problem?"""
    if verdict is None:
        return None
    v = (verdict.get(question) or ("", ""))[0].strip().lower()
    if not v:
        return None
    if question == "Q1_quote_verbatim":
        return v in ("n", "no", "reformatted")
    return v.startswith("n") and v != "na" or v == "partial"


def analyze_planted(records):
    """records: list of {defect, target_question, verdicts}. defect 'clean' =
    control row. Returns (catch_table, fp_table)."""
    catch, fp = {}, {}
    for rec in records:
        v = rec["verdicts"]
        if rec["defect"] == "clean":
            for key, _, _ in QUESTIONS:
                # A clean row is a presumed-correct control only where the shipped
                # value IS a claim. Q10 on a screening-`uncertain` row has no ground
                # truth - the corpus never asserted that paper was in scope - so
                # counting a flag there as a false positive would be wrong.
                if key == "Q10_paper_in_scope" and rec.get("stratum") == "screen_uncertain":
                    continue
                f = _flagged(v, key)
                if f is None:
                    continue
                s = fp.setdefault(key, {"n_clean": 0, "n_flagged": 0})
                s["n_clean"] += 1
                s["n_flagged"] += int(f)
        else:
            q = rec["target_question"]
            f = _flagged(v, q)
            s = catch.setdefault(rec["defect"], {"question": q, "n_planted": 0,
                                                 "n_caught": 0, "n_unparsed": 0})
            s["n_planted"] += 1
            if f is None:
                s["n_unparsed"] += 1
            else:
                s["n_caught"] += int(f)
    for s in catch.values():
        d = s["n_planted"] - s["n_unparsed"]
        s["catch_rate_pct"] = round(100.0 * s["n_caught"] / d, 1) if d else None
    for s in fp.values():
        s["flag_rate_pct"] = (round(100.0 * s["n_flagged"] / s["n_clean"], 1)
                              if s["n_clean"] else None)
    return catch, fp


def normalize_source(s):
    """Normalize fetched source text before it reaches the judge.

    Double-unescape is deliberate: EPMC serves entities that survive one pass
    (`&amp;#x3b1;` -> `&#x3b1;` -> the character), and a single unescape leaves
    codes in the text that then fail to match a quote containing the real
    character. That exact bug produced a false 8% quote-miss rate once already.
    Superscripts are NOT normalized - folding them cost 0.5pp of census
    traceability."""
    import html
    if not isinstance(s, str):
        return s
    t = html.unescape(html.unescape(s))
    t = re.sub(r"<[^>]{1,40}>", " ", t)
    return re.sub(r"\s{2,}", " ", t).strip()


def machine_extendable(catch, flags, min_catch=90.0, max_flag=10.0,
                       adjudicated_flags=False):
    """Verdict per question.

    `flags` is the UNADJUDICATED flag rate on clean control rows - it is NOT a
    false-positive rate. A flag on an unmutated row is one of three things and
    the harness cannot tell them apart: a judge error, a REAL defect the corpus
    shipped, or an artifact of the source fetch. Separating them needs a human
    reading the flagged rows, which is why `adjudicated_flags` defaults to
    False: until those flags are adjudicated, no question can be declared
    machine-extendable, however high its catch rate.

    Questions with no planted defect class are UNTESTED, which is not the same
    as passing."""
    by_q = {v["question"]: (k, v) for k, v in catch.items()}
    rows = []
    for key, _, _ in QUESTIONS:
        c = by_q.get(key)
        f = flags.get(key, {})
        cr = c[1]["catch_rate_pct"] if c else None
        fr = f.get("flag_rate_pct", f.get("fp_rate_pct"))
        if cr is None:
            verdict = "UNTESTED - keep human"
        elif cr < min_catch:
            verdict = "keep human - misses its own defect class"
        elif fr is None:
            verdict = "keep human - no clean controls"
        elif not adjudicated_flags and fr > 0:
            verdict = "needs flag adjudication"
        elif fr <= max_flag:
            verdict = "machine-extendable"
        else:
            verdict = "keep human - flag rate too high"
        rows.append({"question": key, "defect_class": c[0] if c else None,
                     "catch_rate_pct": cr, "flag_rate_unadjudicated_pct": fr,
                     "verdict": verdict})
    return rows


def required_n(target_flag_rate=0.10, half_width=0.05, catch_floor=0.90,
               lower_bound=0.80, n_defect_classes=8):
    """How big the real run has to be for the thresholds to mean anything.
    The pilot's 3-per-class and 24 clean rows give 95% intervals ~55pp and
    ~25pp wide - directional signal only."""
    z = 1.96
    n_clean = int(round(z * z * target_flag_rate * (1 - target_flag_rate) / half_width ** 2))
    n_per_defect = 0
    for n in range(5, 200):
        p = catch_floor
        d = 1 + z * z / n
        c = (p + z * z / (2 * n)) / d
        h = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / d
        if c - h >= lower_bound:
            n_per_defect = n
            break
    return {"n_clean_controls": n_clean, "n_per_defect_class": n_per_defect,
            "n_planted_total": n_per_defect * n_defect_classes,
            "n_judge_calls": n_clean + n_per_defect * n_defect_classes}


def analyze_agreement(judge_verdicts, human_df, id_col="audit_id"):
    """AGREEMENT arm. human_df: the returned worksheet with Q columns filled.
    judge_verdicts: {audit_id: parsed_verdicts}. Exact-match agreement per
    question over rows where BOTH scored."""
    out = []
    for key, _, _ in QUESTIONS:
        if key not in human_df.columns:
            continue
        n = agree = 0
        for _, hr in human_df.iterrows():
            hv = str(hr.get(key, "")).strip().lower()
            jv = judge_verdicts.get(hr[id_col])
            if not hv or hv in ("nan",) or jv is None or key not in jv:
                continue
            n += 1
            agree += int(jv[key][0].strip().lower()[:1] == hv[:1])
        out.append({"question": key, "n_both_scored": n,
                    "agreement_pct": round(100.0 * agree / n, 1) if n else None})
    return out


def make_planted_set(df, srcmap, per_defect=3, n_clean=24, seed=20260915):
    """Build a blind judge input set: planted rows + clean controls, shuffled.
    Returns (rows_for_judge, truth) where truth is aligned to rows."""
    rng = random.Random(seed)
    pool = df[df.pmid.astype(str).isin(srcmap)].copy()
    donors = list(pool.provenance_quote.dropna().astype(str))
    recs = []
    avail = pool.to_dict("records")
    rng.shuffle(avail)
    for defect in DEFECT_TARGET:
        placed, tries = 0, 0
        for row in avail:
            if placed >= per_defect or tries > 400:
                break
            tries += 1
            mut, ok = plant(row, defect, donors, rng)
            if ok:
                recs.append({"row": mut, "defect": defect,
                             "target_question": DEFECT_TARGET[defect],
                             "audit_id": row.get("audit_id")})
                placed += 1
    for row in avail[:n_clean]:
        recs.append({"row": dict(row), "defect": "clean", "target_question": None,
                     "audit_id": row.get("audit_id")})
    rng.shuffle(recs)
    return [r["row"] for r in recs], recs
