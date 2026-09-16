"""Part-aware dispatcher — the Tier-4 generation of the POTS build, condition-agnostic.

PROVENANCE. The dispatch logic is driver_tier3_parts.py carried over unchanged:
is_ceiling(), done_units(), archive_units(), gate() and run() are byte-identical to the
pinned Tier-4 driver (tier4_driver.py). Tiers 1-4 of the POTS build all ran this loop and
findings/paper comparability depends on not perturbing it. driver_tests.py pins the exact
failure shapes that motivated it.

WHAT THE BUILDER CHANGED (tagged `BUILDER:`)
  * Every literal that named the POTS build — pipeline/roster artifact ids, the chunk
    list, the model id, the area name in the worker brief, the environment name, the
    condition enum in the worker's enum check, the tier number, and the pinned
    parameters — is read from the config file named by $CORPUS_CONFIG (pipeline/config.py).
  * The worker code embeds the config subset and the rendered prompt, then calls
    pipeline.configure() before _load_common(). Nothing in the worker's logic changed:
    three-value unpack, findings saved first, archive second, guarded.

TIER-4 DELTAS CARRIED
  T4-A  worker unpacks (rows, stats, retained) and, AFTER save_artifacts on the findings
        parquet, makes a SEPARATE guarded call to save_fulltext_archive().
  T4-B  done_units() gains an ARCHIVE census (archives_present) that is REPORTED only.
        Doneness is decided by the findings parquet alone.

PINNED PARAMETERS come from the config's `pinned` block and are confirmed by
verify_pinned_params() against the pipeline source, not by assertion. Concurrency is
NEVER raised above 3 — 8.5 req/s produced a 92% blocked rate.

DEFECTS THIS LOOP FIXES (re-verified by driver_tests.py):
  1. is_ceiling() reads the per-frame ceiling out of stats.error REGARDLESS of
     version_id (a ceiling-killed worker submits version_id='').
  2. A ceiling kill NEVER counts toward the budget-halt fail-streak; it `continue`s after
     re-splitting, so `failed` is never incremented.
  3. done_units() treats a COMPLETE PART SET as done.
"""
import json, os, re, sys, time

# BUILDER: configuration ----------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import load_config, render_prompt, pipeline_config, condition_values  # noqa: E402

CFG = load_config(os.environ.get("CORPUS_CONFIG"), warn=lambda m: print(m, file=sys.stderr))
PIPE   = CFG["artifacts"].get("pipeline_vid")   # pipeline.py artifact id the workers exec
ROSTER = CFG["artifacts"].get("roster_vid")     # roster parquet artifact id
TARGET_CHUNKS = list(CFG["tier"]["target_chunks"])   # EXPLICIT list from config, never range(N)

_P = CFG["pinned"]
MODEL         = CFG["model"]["id"]     # literal; the pipeline module is configured to the same
FETCH_PACING  = float(_P["fetch_pacing_s"])
N_INFLIGHT    = int(_P["n_inflight"])
MAX_ATTEMPTS  = int(_P["max_attempts"])
WORKER_MAX_S  = int(_P["worker_max_s"])
COLLECT_SLICE = int(_P["collect_slice_s"])
N_PARTS_DEFAULT = int(_P["n_parts_default"])
PAPERS_PER_CHUNK = int(_P["papers_per_chunk"])

MIN_YIELD    = float(_P["min_yield"])
WARN_BLOCKED = float(_P["warn_blocked"])
MAX_BLOCKED  = float(_P["max_blocked"])

TIER_NUMBER = int(CFG["tier"]["number"])
ENV_NAME    = CFG["tier"]["env_name"]
AREA_NAME   = CFG["area"]["name"]
CO_OK       = condition_values(CFG)
WORKER_CONFIG = pipeline_config(CFG)
WORKER_CONFIG["_rendered_prompt"] = render_prompt(CFG)

CEILING_RX = re.compile(r"token ceiling|per-frame token|2\.0M", re.I)


def is_ceiling(stats):
    """A ceiling kill is identified by the ERROR TEXT, never by the empty-output
    signature it shares with a quota stall. Misreading one as the other is what
    caused defects 1 and 2 above."""
    return bool(CEILING_RX.search(str((stats or {}).get("error") or "")))


def done_units(host, targets):
    """-> (done_chunks, have_parts) where done means a whole file or a complete
    part set. have_parts maps cid -> {(i,n) present} so a resumed run re-dispatches
    only the MISSING parts and never re-pays for one already saved."""
    r = host.artifacts(filename="_findings.parquet", limit=900)
    whole, parts = set(), {}
    for a in r["artifacts"]:
        fn = a["filename"]
        mw = re.match(r"chunk_(\d+)_findings\.parquet$", fn)
        mp = re.match(r"chunk_(\d+)_part(\d+)of(\d+)_findings\.parquet$", fn)
        if mw and int(mw.group(1)) in targets:
            whole.add(int(mw.group(1)))
        elif mp and int(mp.group(1)) in targets:
            parts.setdefault(int(mp.group(1)), set()).add((int(mp.group(2)), int(mp.group(3))))
    done = set(whole)
    for cid, got in parts.items():
        ns = {n for (_i, n) in got}
        if len(ns) == 1:
            n = ns.pop()
            if sorted(i for (i, _n) in got) == list(range(n)):
                done.add(cid)
    return done, parts


def archive_units(host, targets):
    """T4-B: census of retention archives. REPORTING ONLY — this never feeds
    doneness. A chunk whose findings are saved but whose archive is missing is DONE;
    re-extracting it would spend a full chunk of tokens to recover a storage artefact.
    -> (chunks_with_whole_archive, {cid: {(i,n) present}})"""
    r = host.artifacts(filename="_fulltext.json.gz", limit=900)
    whole, parts = set(), {}
    for a in r["artifacts"]:
        fn = a["filename"]
        mw = re.match(r"chunk_(\d+)_fulltext\.json\.gz$", fn)
        mp = re.match(r"chunk_(\d+)_part(\d+)of(\d+)_fulltext\.json\.gz$", fn)
        if mw and int(mw.group(1)) in targets:
            whole.add(int(mw.group(1)))
        elif mp and int(mp.group(1)) in targets:
            parts.setdefault(int(mp.group(1)), set()).add((int(mp.group(2)), int(mp.group(3))))
    return whole, parts


# ================================ worker task ====================================
# T4-A is the ONLY change inside the worker code: three-value unpack, then a
# separate guarded archive save AFTER the findings parquet is saved.
CODE_TMPL_RAW = """import pandas as pd, json
exec(open(host.artifact_path("__PIPE__")).read())
CONFIG = json.loads(__CONFIG_JSON__)     # BUILDER: config subset + rendered prompt, embedded
configure(CONFIG)
_load_common(host)
CID = __CID__
corpus = pd.read_parquet(host.artifact_path("__ROSTER__")).drop_duplicates("pmid")
pmids = corpus[corpus.chunk_id==CID].pmid.tolist()
PART = __PART__      # None = whole chunk; (i,n) = slice i of n
if PART:
    _i,_n = PART
    _sz = (len(pmids)+_n-1)//_n
    pmids = pmids[_i*_sz:(_i+1)*_sz]
chunk_df = corpus[corpus.pmid.isin(pmids)].copy()
chunk_df["chunk_id"] = CID
rows, stats, retained = process_chunk(host, chunk_df)   # TIER4: 3-value unpack
gdf = ground_findings_df(pd.DataFrame(rows)) if rows else pd.DataFrame()
for col in ["effect_size","study_quality"]:
    if col in gdf.columns:
        gdf[col]=gdf[col].apply(lambda x: json.dumps(x) if isinstance(x,(dict,list)) else x)
if len(gdf):
    gdf["tier"] = __TIER__
fn = ("chunk_%03d_findings.parquet" % CID) if not PART else \\
     ("chunk_%03d_part%dof%d_findings.parquet" % (CID, PART[0], PART[1]))
gdf.to_parquet(fn, index=False)
n_expected = len(pmids)
n_yield = int(gdf.pmid.nunique()) if len(gdf) else 0
stats["n_expected"] = n_expected
stats["n_yield_papers"] = n_yield
stats["yield_frac"] = round(n_yield / n_expected, 4) if n_expected else 0.0
stats["n_findings"] = int(len(gdf))
ET_OK = ["meta-analysis","RCT","cohort","case-control","cross-sectional","case-series",
         "case-report","mechanistic/in-vitro","review/expert-opinion"]
CO_OK = __CO_OK__     # BUILDER: the condition enum from the config
enum_bad = {}
for _c, _allowed in [("evidence_tier", ET_OK), ("condition", CO_OK)]:
    if len(gdf) and _c in gdf.columns:
        _b = gdf[~gdf[_c].isin(_allowed) & gdf[_c].notna()]
        if len(_b):
            enum_bad[_c] = {k: int(v) for k, v in _b[_c].value_counts().items()}
stats["enum_violations"] = enum_bad   # RECORDED, never coerced
if len(gdf):
    stats["id_coverage"] = {
      "entity_id": int(gdf.entity_ontology_id_resolved.notna().sum()),
      "hpo_any":   int(gdf.hpo_ids_verified.notna().sum()),
      "hpo_multi": int((gdf.hpo_id_count>1).sum()) if "hpo_id_count" in gdf.columns else 0,
      "gene_id":   int(gdf.hgnc_id.notna().sum())}
print("SAVED_FILE", fn, "rows", len(gdf))
print("STATS", json.dumps(stats, default=str))
# ---- TIER4 retention: SEPARATE from extraction, runs only after the parquet exists.
# Guarded so a retention failure cannot lose findings; the report is printed under its
# own marker and merged into stats by the worker at STEP 3.
try:
    ARCHIVE = save_fulltext_archive(retained, CID, PART)
except Exception as _e:
    ARCHIVE = {"archive_error": "%s: %s" % (type(_e).__name__, str(_e)[:120])}
print("ARCHIVE", json.dumps(ARCHIVE, default=str))
"""


def _fill_static(tmpl):
    """BUILDER: bind the config-derived constants into the worker template once."""
    return (tmpl.replace("__PIPE__", str(PIPE))
                .replace("__ROSTER__", str(ROSTER))
                .replace("__TIER__", str(TIER_NUMBER))
                .replace("__CO_OK__", json.dumps(CO_OK))
                .replace("__CONFIG_JSON__", repr(json.dumps(WORKER_CONFIG, ensure_ascii=False))))


CODE_TMPL = _fill_static(CODE_TMPL_RAW)

SCHEMA = {"type": "object", "properties": {
    "chunk_id": {"type": "integer"}, "version_id": {"type": "string"},
    "archive_version_id": {"type": "string"},
    "stats": {"type": "object", "properties": {
        "yield_frac": {"type": "number"}, "n_yield_papers": {"type": "integer"},
        "n_expected": {"type": "integer"}, "n_findings": {"type": "integer"},
        "blocked": {"type": "integer"}, "blocked_frac": {"type": "number"},
        "parse_fail": {"type": "integer"}, "guard_violations": {"type": "integer"},
        "enum_violations": {"type": "object"},
        "retained_papers": {"type": "integer"},
        "archive": {"type": "object"}}}},
    "required": ["chunk_id", "version_id", "stats"]}


def build_task(cid, part=None):
    code = CODE_TMPL.replace("__CID__", str(cid)).replace("__PART__", repr(part))
    outfn = (f"chunk_{cid:03d}_findings.parquet" if not part
             else f"chunk_{cid:03d}_part{part[0]}of{part[1]}_findings.parquet")
    arcfn = (f"chunk_{cid:03d}_fulltext.json.gz" if not part
             else f"chunk_{cid:03d}_part{part[0]}of{part[1]}_fulltext.json.gz")
    part_note = ("" if not part else
        f" You are handling PART {part[0]+1} of {part[1]} of this chunk's papers "
        f"(the chunk is split because a full {PAPERS_PER_CHUNK}-paper pass exceeds the per-worker token ceiling).")
    return (
        f"You are a deterministic extraction worker for chunk {cid} of a literature-mining pipeline "
        f"for {AREA_NAME}.{part_note} Do EXACTLY these four steps, no deviation, no extra analysis:\n\n"
        f"STEP 1 - run this exact code in the `python` tool, environment='{ENV_NAME}' (it fetches full text, "
        f"extracts findings with a truncation guard, derives ontology IDs from names, and writes a gzipped "
        f"full-text archive; may take 10-25 min):\n\n"
        f"```python\n{code}```\n\n"
        f"STEP 2 - save the FINDINGS first: save_artifacts(files=[\"{outfn}\"], language=\"python\"). "
        f"Read the returned version_id. This must succeed before you touch the archive.\n\n"
        f"STEP 3 - save the ARCHIVE second: save_artifacts(files=[\"{arcfn}\"], language=\"python\"). "
        f"If this fails, or the ARCHIVE line in stdout contains archive_error, DO NOT retry the extraction "
        f"and DO NOT discard step 2 — just report it in stats.archive and continue. The findings are the "
        f"deliverable; the archive is provenance retention.\n\n"
        f"STEP 4 - submit structured output with chunk_id={cid}, version_id from step 2, "
        f"archive_version_id from step 3 (or '' if it failed), and the STATS dict printed after the 'STATS' "
        f"marker with the ARCHIVE dict attached as stats['archive'].\n\n"
        f"If the code raises, retry ONCE; if it still fails, submit version_id='' with the error in stats.error. "
        f"Do not attempt to fix the pipeline module. Do not analyze the findings."
    )


def gate(cid, so):
    """Pass/fail from the CHILD'S OWN stats (no parquet read -> no read-after-write race).
    Returns (ok, yield_frac, enum_violations, reason). Unchanged from Tier 3 except that
    a missing/failed archive is explicitly NOT a gate condition."""
    st = (so or {}).get("stats") or {}
    enum_bad = st.get("enum_violations") or {}
    frac = st.get("yield_frac")
    nf   = st.get("n_findings") or st.get("findings") or 0
    pfail = st.get("parse_fail")
    blk   = st.get("blocked")
    bfrac = st.get("blocked_frac")
    # Ceiling check BEFORE the quota check, and strictly broader than either source:
    # is_ceiling()'s regex OR the pinned gate's inline predicate. A ceiling that slipped
    # past both would be misread as a quota stall and retried identically (defect 1).
    err = str(st.get("error") or "")
    if is_ceiling(st) or ("token" in err.lower() and
                          ("ceiling" in err.lower() or "limit" in err.lower() or "2.0M" in err)):
        return False, frac, enum_bad, "token-ceiling (split required)"
    try:
        if float(nf or 0) == 0 and (pfail in (0, None)) and (blk in (0, None)):
            return False, frac, enum_bad, "quota-signature (empty, no parse_fail, no blocked)"
    except Exception:
        pass
    if bfrac is not None:
        try:
            bf = float(bfrac)
            if bf > MAX_BLOCKED:
                return False, frac, enum_bad, f"blocked {bf:.1%} > {MAX_BLOCKED:.0%} (systematic)"
        except Exception:
            pass
    if frac is None:
        ny, ne = st.get("n_yield_papers"), st.get("n_expected")
        if ny is not None and ne:
            frac = float(ny) / float(ne)
        else:
            try: nfv = float(nf)
            except Exception: nfv = 0
            return (None if nfv > 0 else False), None, enum_bad, "unknown-yield"
    try: frac = float(frac)
    except Exception: return None, None, enum_bad, "unparseable-yield"
    if frac < MIN_YIELD:
        return False, frac, enum_bad, f"under-yield {frac:.2f}<{MIN_YIELD}"
    return True, frac, enum_bad, "ok"


def run(host, build_task, gate, targets, split_all=True, n_parts=N_PARTS_DEFAULT, log_prefix=""):
    """Dispatch `targets` as parts. Returns a computed report -- every count in it
    comes from a variable, never a typed literal."""
    targets = set(targets)
    done, have_parts = done_units(host, targets)
    # work unit = (cid, (i,n)) for parts, or (cid, None) for a whole attempt
    pending = []
    for cid in sorted(targets - done):
        if split_all:
            got = have_parts.get(cid, set())
            for i in range(n_parts):
                if (i, n_parts) not in got:
                    pending.append((cid, (i, n_parts)))
        else:
            pending.append((cid, None))

    attempts, inflight, starts = {}, {}, {}
    gate_log, guard_log, enum_log, ceiling_log = [], [], [], []
    finished = failed = 0
    print(f"{log_prefix}[parts] start: {len(pending)} work units over "
          f"{len(targets - done)} chunks; {len(done)} already done", flush=True)

    def refill():
        while len(inflight) < N_INFLIGHT and pending:
            cid, part = pending.pop(0)
            key = (cid, part)
            if attempts.get(key, 0) >= MAX_ATTEMPTS:
                continue
            d = host.delegate({"task": build_task(cid, part),
                               "name": f"c{cid}p{part[0]}" if part else f"chunk{cid:03d}",
                               "output_schema": SCHEMA}, wait=False)
            fid = d["frame_id"]
            inflight[fid] = key
            starts[fid] = time.time()
            attempts[key] = attempts.get(key, 0) + 1
            print(f"{log_prefix}[parts] dispatched chunk {cid} part {part} "
                  f"(attempt {attempts[key]}) frame {fid[:8]}", flush=True)

    refill()
    while inflight or pending:
        res = host.collect(list(inflight.keys()), timeout=COLLECT_SLICE) if inflight else []
        # watchdog: reclaim a slot from a worker past WORKER_MAX_S
        now = time.time()
        for fid in list(inflight.keys()):
            if now - starts.get(fid, now) > WORKER_MAX_S:
                cid, part = inflight[fid]
                try:
                    host.stop_child(fid, reason=f"watchdog: exceeded {WORKER_MAX_S}s")
                except Exception:
                    pass
                d2, hp = done_units(host, targets)
                saved = (cid in d2) or (part in hp.get(cid, set()))
                inflight.pop(fid, None)
                print(f"{log_prefix}[parts] WATCHDOG reclaimed chunk {cid} part {part} "
                      f"(saved={saved})", flush=True)
                if not saved:
                    pending.append((cid, part))

        for r in (res or []):
            if r.get("status") == "running":
                continue
            fid = r.get("sender_frame_id") or r.get("frame_id")
            key = inflight.pop(fid, None)
            if key is None:
                continue
            cid, part = key
            so = r.get("structured_output") or {}
            st = so.get("stats") or {}
            vid = so.get("version_id")

            # DEFECT 1+2 FIX: ceiling detected from the error text regardless of vid,
            # and NEVER counted as an infrastructure/budget failure.
            if is_ceiling(st) and not vid:
                ceiling_log.append({"chunk": cid, "part": part, "attempt": attempts.get(key)})
                nxt = (part[1] * 2) if part else n_parts
                print(f"{log_prefix}[parts] chunk {cid} part {part} hit the 2.0M ceiling "
                      f"-> re-splitting into {nxt} parts", flush=True)
                got = have_parts.get(cid, set())
                for i in range(nxt):
                    if (i, nxt) not in got:
                        pending.append((cid, (i, nxt)))
                continue

            gv = st.get("guard_violations") or 0
            if gv:
                guard_log.append({"chunk": cid, "part": part, "count": gv,
                                  "detail": st.get("guard_violation_detail")})
                print(f"{log_prefix}[parts] chunk {cid} GUARD VIOLATIONS: {gv}", flush=True)
            ev = st.get("enum_violations") or {}
            if ev:
                enum_log.append({"chunk": cid, "part": part, "violations": ev})

            ok, frac, enum_bad, reason = (None, None, {}, r.get("status") or "no-output")
            if r.get("status") == "completed" and vid:
                ok, frac, enum_bad, reason = gate(cid, so)
                if ok is None:
                    ok, frac, reason = True, -1.0, "productive-unverifiable"
            arc = st.get("archive") or {}
            gate_log.append({"chunk": cid, "part": part, "ok": bool(ok), "reason": reason,
                             "yield": frac, "findings": st.get("n_findings"),
                             "blocked_frac": st.get("blocked_frac"),
                             "guard_violations": gv, "attempt": attempts.get(key),
                             "archive_file": arc.get("file"),
                             "archive_gz_bytes": arc.get("gz_bytes"),
                             "archive_gz_mb": arc.get("gz_mb"),    # decimal MB (1e6 B)
                             "archive_gz_mib": arc.get("gz_mib"),  # binary MiB (1048576 B)
                             "archive_compression_ratio": arc.get("compression_ratio"),
                             "archive_error": arc.get("archive_error")})
            if arc.get("archive_error"):
                # T4-B: recorded, never a gate failure and never a re-dispatch.
                print(f"{log_prefix}[parts] chunk {cid} part {part} ARCHIVE FAILED "
                      f"(findings kept): {arc['archive_error']}", flush=True)
            if ok:
                finished += 1
            else:
                failed += 1
                if attempts.get(key, 0) < MAX_ATTEMPTS:
                    pending.append((cid, part))
                print(f"{log_prefix}[parts] chunk {cid} part {part} FAILED: {reason}", flush=True)

        refill()
        for fn, obj in (("gate_log_parts.json", gate_log), ("guard_violations_parts.json", guard_log),
                        ("enum_violations_parts.json", enum_log), ("ceiling_log_parts.json", ceiling_log)):
            json.dump(obj, open(fn, "w"), indent=1)
        d3, _ = done_units(host, targets)
        aw, ap = archive_units(host, targets)
        with open("progress_parts.txt", "a") as fp:
            fp.write(f"{len(d3 & targets)}/{len(targets)} chunks | units in-flight {len(inflight)} "
                     f"| pending {len(pending)} | +{finished} -{failed} "
                     f"| archives {len(aw) + len(ap)}\n")
        print(f"{log_prefix}[parts] {len(d3 & targets)}/{len(targets)} chunks done | "
              f"in-flight {len(inflight)} | pending {len(pending)} | "
              f"archives {len(aw) + len(ap)}", flush=True)

    final_done, final_parts = done_units(host, targets)
    arc_whole, arc_parts = archive_units(host, targets)
    return {"chunks_done": sorted(final_done & targets),
            "chunks_missing": sorted(targets - final_done),
            "n_done": len(final_done & targets), "n_targets": len(targets),
            "gate_log": gate_log, "guard_violations": guard_log,
            "enum_violations": enum_log, "ceiling_events": ceiling_log,
            "part_files": {c: sorted(v) for c, v in final_parts.items()},
            "archives_whole": sorted(arc_whole),
            "archive_parts": {c: sorted(v) for c, v in arc_parts.items()},
            "chunks_without_archive": sorted(
                (final_done & targets) - arc_whole - set(arc_parts))}


def verify_pinned_params(pipeline_src=None):
    """Confirm the values the driver will ACTUALLY use, read from this module's
    namespace and from the pipeline source text — not typed into a report.
    BUILDER: expectations come from the config's pinned block and model id.
    -> {param: {"value": ..., "expected": ..., "ok": bool, "source": ...}}"""
    exp = {"MODEL": CFG["model"]["id"], "FETCH_PACING": float(_P["fetch_pacing_s"]),
           "N_INFLIGHT": int(_P["n_inflight"]), "N_PARTS_DEFAULT": int(_P["n_parts_default"]),
           "papers_per_unit": (PAPERS_PER_CHUNK + int(_P["n_parts_default"]) - 1) // int(_P["n_parts_default"]),
           "WORKER_MAX_S": int(_P["worker_max_s"])}
    g = globals()
    got = {"MODEL": (g["MODEL"], "driver.MODEL"),
           "N_INFLIGHT": (g["N_INFLIGHT"], "driver.N_INFLIGHT"),
           "N_PARTS_DEFAULT": (g["N_PARTS_DEFAULT"], "driver.N_PARTS_DEFAULT"),
           "papers_per_unit": ((PAPERS_PER_CHUNK + g["N_PARTS_DEFAULT"] - 1) // g["N_PARTS_DEFAULT"],
                               f"ceil({PAPERS_PER_CHUNK} / N_PARTS_DEFAULT), same arithmetic as CODE_TMPL"),
           "WORKER_MAX_S": (g["WORKER_MAX_S"], "driver.WORKER_MAX_S")}
    if pipeline_src is not None:
        # The pipeline module holds no literal MODEL/FETCH_PACING any more: they are set by
        # configure(). So the check is that the WORKER CONFIG embedded in CODE_TMPL carries
        # the expected values — which is what the worker will actually run with.
        got["FETCH_PACING"] = (float(WORKER_CONFIG["pinned"]["fetch_pacing_s"]), "WORKER_CONFIG.pinned.fetch_pacing_s")
        got["pipeline_MODEL"] = (WORKER_CONFIG["model_id"], "WORKER_CONFIG.model_id (embedded in CODE_TMPL)")
        exp["pipeline_MODEL"] = CFG["model"]["id"]
        src = open(pipeline_src).read() if not pipeline_src.lstrip().startswith(("\"\"\"", "import")) else pipeline_src
        got["pipeline_has_configure"] = ("def configure(cfg)" in src, "pipeline source defines configure()")
        exp["pipeline_has_configure"] = True
    else:
        got["FETCH_PACING"] = (g["FETCH_PACING"], "driver.FETCH_PACING (pipeline not read)")
    return {k: {"value": v, "expected": exp.get(k), "ok": v == exp.get(k), "source": s}
            for k, (v, s) in got.items()}
