"""Stage 1 — draw the paper universe from Europe PMC, per theme, and freeze a chunked roster.

Added 2026-09-30 (EDS dry run). The RUNBOOK said "draw with any tool"; the POTS build's draw
was an unsaved notebook cell, so a second area had to re-invent it. This module reads the
`draw:` block of config/<area>.yaml (one Europe PMC query per THEME), appends the year window,
SRC:MED and HAS_ABSTRACT:y, pages with cursorMark at the pinned pacing, deduplicates on PMID
(first theme wins; every theme a paper was found under is kept in `themes`), FLAGS probable
wrong-sense hits with `draw.contamination_patterns` (query_contamination=true; excluded from the
roster but written to <out>_excluded.parquet so the exclusion is auditable and reversible),
sorts into a frozen order and cuts fixed `pinned.papers_per_chunk` chunks.

Roster contract (SCHEMA.md § Roster): pmid, doi, title, abstract, year, journal, source_stream,
theme, themes, is_covid, chunk_id  (+ pmcid, is_oa, in_epmc kept for stratification reports).

    python pipeline/draw.py config/<area>.yaml --out roster.parquet
    python pipeline/draw.py config/<area>.yaml --out roster.parquet \
        --sample-strata "fulltext_rich=25,mixed=25,abstract_heavy=25" --seed 11   # dry-run sampling

Every count printed is computed from the records fetched, never typed.
"""
import argparse, json, os, re, sys, time, urllib.parse, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import load_config  # noqa: E402

EPMC = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"


def _get_json(url, timeout=60, attempts=6):
    delay = 3.0
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r), None
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < attempts - 1:
                time.sleep(delay); delay *= 2.2; continue
            return None, f"HTTP {e.code}"
        except Exception as e:
            if i < attempts - 1:
                time.sleep(delay); delay *= 2.2; continue
            return None, f"{type(e).__name__}: {str(e)[:60]}"


def draw_theme(theme, query, cfg, pacing, page_size=1000, max_pages=50, log=print):
    yr = cfg["year_range"]
    q = f'{query} AND (PUB_YEAR:[{yr["start"]} TO {yr["end"]}]) AND (SRC:MED) AND (HAS_ABSTRACT:y)'
    cursor = "*"; out = []; n_calls = 0
    for _ in range(max_pages):
        url = (f"{EPMC}?query={urllib.parse.quote(q)}&format=json&resultType=core"
               f"&pageSize={page_size}&cursorMark={urllib.parse.quote(cursor)}")
        d, err = _get_json(url); n_calls += 1
        if err:
            raise RuntimeError(f"draw {theme}: {err}")
        res = d.get("resultList", {}).get("result", [])
        for r in res:
            out.append({
                "pmid": str(r.get("pmid") or ""), "doi": r.get("doi"), "title": r.get("title"),
                "abstract": r.get("abstractText"), "year": int(r["pubYear"]) if r.get("pubYear") else None,
                "journal": (r.get("journalInfo", {}) or {}).get("journal", {}).get("title") or r.get("journalTitle"),
                "pmcid": r.get("pmcid"), "is_oa": r.get("isOpenAccess") == "Y",
                "in_epmc": r.get("inEPMC") == "Y", "theme": theme,
            })
        log(f"[draw] {theme}: page -> {len(res)} records (cum {len(out)} / hitCount {d.get('hitCount')})")
        nxt = d.get("nextCursorMark")
        if not res or not nxt or nxt == cursor:
            break
        cursor = nxt
        time.sleep(pacing)
    return out, n_calls


def draw(cfg, log=print, exclude=False):
    dr = cfg.get("draw") or {}
    if not dr.get("themes"):
        raise SystemExit("config has no draw.themes block (one Europe PMC query per theme)")
    pacing = float(cfg["pinned"]["fetch_pacing_s"])
    recs = {}; themes_of = {}; per_theme = {}; calls = 0
    for theme, query in dr["themes"].items():
        rows, n = draw_theme(theme, " ".join(str(query).split()), cfg, pacing, log=log); calls += n
        per_theme[theme] = len(rows)
        for r in rows:
            if not r["pmid"]:
                continue
            themes_of.setdefault(r["pmid"], []).append(theme)
            recs.setdefault(r["pmid"], r)          # first theme wins as primary `theme`
    pats = [re.compile(p, re.I) for p in dr.get("contamination_patterns", [])]
    kept, excluded = [], []
    for pmid, r in recs.items():
        r["themes"] = themes_of[pmid]
        blob = f"{r.get('title') or ''} {r.get('abstract') or ''}"
        hits = [p.pattern for p in pats if p.search(blob)]
        r["query_contamination"] = bool(hits)
        r["contamination_pattern"] = "; ".join(hits) if hits else None
        r["source_stream"] = dr.get("source_stream", "europepmc")
        r["is_covid"] = bool(re.search(r"COVID|SARS-CoV-2", blob, re.I))
                # FLAG, never exclude: on the EDS dry run 8 of 10 regex "exclusions" were genuine EDS
        # papers (sleep apnoea IN hEDS matched "excessive daytime sleepiness"). The two-arm
        # screen (stage 3) adjudicates flagged records; --exclude-contaminated is opt-in.
        (excluded if (hits and exclude) else kept).append(r)
    report = {"per_theme_hits": per_theme, "unique_pmids": len(recs), "kept": len(kept),
              "flagged_contamination": sum(1 for r in kept if r["query_contamination"]),
              "excluded_contamination": len(excluded), "exclude_mode": bool(exclude), "api_calls": calls,
              "year_range": dict(cfg["year_range"]), "pacing_s": pacing}
    return kept, excluded, report


def sample_strata(kept, spec, seed):
    """Dry-run sampling: 'fulltext_rich=N,mixed=N,abstract_heavy=N'. fulltext_rich draws from
    papers with a PMCID and OA flag (full text likely); abstract_heavy from papers with no PMCID;
    mixed from whatever remains. Deterministic under `seed`. Returns (rows, plan)."""
    import random
    rng = random.Random(seed)
    want = dict((k.strip(), int(v)) for k, v in (kv.split("=") for kv in spec.split(",")))
    ft = [r for r in kept if r.get("pmcid") and r.get("is_oa")]
    ab = [r for r in kept if not r.get("pmcid")]
    rng.shuffle(ft); rng.shuffle(ab)
    chosen = {}; plan = {}
    for stratum, n in want.items():
        if stratum == "fulltext_rich":
            pool = [r for r in ft if r["pmid"] not in chosen]
        elif stratum == "abstract_heavy":
            pool = [r for r in ab if r["pmid"] not in chosen]
        else:
            pool = [r for r in kept if r["pmid"] not in chosen]; rng.shuffle(pool)
        take = pool[:n]
        for r in take:
            chosen[r["pmid"]] = dict(r, sample_stratum=stratum)
        plan[stratum] = {"requested": n, "drawn": len(take), "pool": len(pool)}
    return list(chosen.values()), plan


def to_roster(rows, papers_per_chunk, order="sample_then_pmid"):
    """Freeze order and cut fixed chunks. For a stratified dry run, chunks follow the strata in
    the order requested (so chunk 0 is the full-text-rich one); otherwise pmid order."""
    import pandas as pd
    df = pd.DataFrame(rows)
    if "sample_stratum" in df.columns and order == "sample_then_pmid":
        strata = list(dict.fromkeys(df.sample_stratum))
        df["_s"] = df.sample_stratum.map({s: i for i, s in enumerate(strata)})
        df = df.sort_values(["_s", "pmid"], kind="stable").drop(columns="_s")
    else:
        df = df.sort_values("pmid", kind="stable")
    df = df.reset_index(drop=True)
    df["chunk_id"] = df.index // int(papers_per_chunk)
    cols = ["pmid", "doi", "title", "abstract", "year", "journal", "source_stream", "theme", "themes",
            "is_covid", "chunk_id", "pmcid", "is_oa", "in_epmc", "query_contamination"]
    if "sample_stratum" in df.columns: cols.append("sample_stratum")
    return df[cols]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("config"); ap.add_argument("--out", required=True)
    ap.add_argument("--sample-strata", default=None); ap.add_argument("--seed", type=int, default=11)
    ap.add_argument("--exclude-contaminated", action="store_true",
                    help="drop regex-flagged records from the roster (default: keep + flag)")
    ap.add_argument("--resample-from", nargs="*", default=None, metavar="PARQUET",
                    help="skip the fetch; rebuild the universe from saved *_universe/_excluded parquet(s)")
    a = ap.parse_args(argv)
    cfg = load_config(a.config, warn=lambda m: print(m, file=sys.stderr))
    if a.resample_from:
        import pandas as pd
        u = pd.concat([pd.read_parquet(f) for f in a.resample_from], ignore_index=True).drop_duplicates("pmid")
        kept = [dict(r) for r in u.to_dict("records")]
        for r in kept:
            if hasattr(r.get("themes"), "tolist"): r["themes"] = r["themes"].tolist()
        excluded = []
        report = {"resampled_from": a.resample_from, "unique_pmids": len(kept),
                  "flagged_contamination": sum(1 for r in kept if r.get("query_contamination")), "api_calls": 0}
    else:
        kept, excluded, report = draw(cfg, exclude=a.exclude_contaminated)
    if a.sample_strata:
        rows, plan = sample_strata(kept, a.sample_strata, a.seed); report["sample_plan"] = plan
    else:
        rows = kept
    import pandas as pd
    roster = to_roster(rows, cfg["pinned"]["papers_per_chunk"])
    roster.to_parquet(a.out, index=False)
    stem = re.sub(r"\.parquet$", "", a.out)
    pd.DataFrame(kept).to_parquet(stem + "_universe.parquet", index=False)
    pd.DataFrame(excluded).to_parquet(stem + "_excluded.parquet", index=False) if excluded else None
    report["roster_rows"] = int(len(roster)); report["chunks"] = sorted(int(c) for c in roster.chunk_id.unique())
    report["chunk_sizes"] = {str(k): int(v) for k, v in roster.chunk_id.value_counts().sort_index().items()}
    json.dump(report, open(stem + "_draw_report.json", "w"), indent=1)
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
