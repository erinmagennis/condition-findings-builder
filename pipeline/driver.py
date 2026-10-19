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

BUILDER 2026-09-30 (EDS dry run): chunk artifact filenames carry the config's
`artifacts.file_prefix` ("<slug>_" by default, "" for the legacy POTS names) and the
done/archive detectors match the prefix anchored at BOTH ends. Without this, a second area in
the same project saw the POTS build's chunk_000..002_findings.parquet as already done and
dispatched nothing. With prefix "" the regexes are byte-identical to the Tier-4 driver's.
"""
import json, os, re, sys, time

# BUILDER: configuration ----------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import load_config, render_prompt, pipeline_config, condition_values  # noqa: E402

CFG = load_config(os.environ.get("CORPUS_CONFIG"), warn=lambda m: print(m, file=sys.stderr))
BUILD_AFTER = CFG["artifacts"].get("build_after")
STORE_RETRIES, STORE_RETRY_PAUSE_S = 4, 20
# 2026-10-08 stall breaker. Waves 0-1: 24 units, wall 420-1517 s (median ~700 s). Wave 2: every
# in-flight unit ran >3000 s and saved nothing, i.e. a SYSTEM stall, and the per-unit watchdog
# re-split and re-dispatched into it for 3 h. Rule: if no unit has completed for STALL_S and every
# in-flight unit is older than STALL_S, stop dispatching, stop the in-flight workers, and return
# {"stalled": True}. STALL_S is adaptive: 2.5x the longest completed unit so far, floored at
# STALL_MIN_S and capped at WORKER_MAX_S (the watchdog). Never re-split on a stall.
# (BUILD_AFTER above: ISO cutoff; older same-prefix chunk files are NOT this build's)
PIPE   = CFG["artifacts"].get("pipeline_vid")   # pipeline.py artifact id the workers exec
ROSTER = CFG["artifacts"].get("roster_vid")     # roster parquet artifact id
TARGET_CHUNKS = list(CFG["tier"]["target_chunks"])   # EXPLICIT list from config, never range(N)

_P = CFG["pinned"]
STALL_MIN_S = int(_P.get("stall_min_s", 1500))
RETRY_UNDER_YIELD = bool(_P.get("retry_under_yield", True))
# 2026-10-19: stalled workers had NO assistant turn at all (sub-agent never started: model/usage
# unavailable). Declare a system stall as soon as EVERY in-flight worker is older than FIRST_TURN_S
# and none has produced an assistant message, instead of waiting the full STALL window.
FIRST_TURN_S = int(_P.get("first_turn_s", 480))   # Tier 2 sets false (structural low yields are accepted, flagged)
MODEL         = CFG["model"]["id"]     # literal; the pipeline module is configured to the same
FETCH_PACING  = float(_P["fetch_pacing_s"])
N_INFLIGHT    = int(_P["n_inflight"])
MAX_ATTEMPTS  = int(_P["max_attempts"])
WORKER_MAX_S  = int(_P["worker_max_s"])
COLLECT_SLICE = int(_P["collect_slice_s"])
N_PARTS_DEFAULT = int(_P["n_parts_default"])
PAPERS_PER_CHUNK = int(_P["papers_per_chunk"])

MAX_FT_PER_UNIT = int(_P.get("max_fulltext_per_unit", 12))   # BUILDER 2026-09-30: work-based unit sizing
WATCHDOG_RESPLIT = bool(_P.get("watchdog_resplit", True))       # BUILDER 2026-09-30: reclaim -> split, not same shape
MIN_YIELD    = float(_P["min_yield"])
WARN_BLOCKED = float(_P["warn_blocked"])
MAX_BLOCKED  = float(_P["max_blocked"])

TIER_NUMBER = int(CFG["tier"]["number"])
ENV_NAME    = CFG["tier"]["env_name"]
FILE_PREFIX = str(CFG["artifacts"].get("file_prefix", ""))   # BUILDER 2026-09-30: per-area namespace
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


def _list_chunk_files(host):
    """2026-10-07 wave 2: one transient `host_call 'list_artifacts' failed` inside the slice loop
    killed the whole wave controller (3 h of in-flight work orphaned). Retry the store query a few
    times with a pause; re-raise only if it keeps failing."""
    last = None
    for attempt in range(STORE_RETRIES):
        try:
            return host.artifacts(filename=f"{FILE_PREFIX}chunk_", limit=900,
                                  **({"after": BUILD_AFTER} if BUILD_AFTER else {}))
        except Exception as e:  # noqa: BLE001 - host_call errors surface as RuntimeError
            last = e
            print(f"[parts] store query failed (attempt {attempt + 1}/{STORE_RETRIES}): {str(e)[:120]}", flush=True)
            time.sleep(STORE_RETRY_PAUSE_S)
    raise last


def done_units(host, targets):
    """-> (done_chunks, have_parts) where done means a whole file or a complete
    part set. have_parts maps cid -> {(i,n) present} so a resumed run re-dispatches
    only the MISSING parts and never re-pays for one already saved."""
    r = _list_chunk_files(host)
    whole, parts = set(), {}
    px = re.escape(FILE_PREFIX)
    for a in r["artifacts"]:
        fn = a["filename"]
        # ANCHORED at both ends with the area prefix: "chunk_000_findings.parquet" from another
        # area in the same project must NOT satisfy "eds_chunk_000_findings.parquet".
        mw = re.match(rf"{px}chunk_(\d+)_findings\.parquet$", fn)
        mp = re.match(rf"{px}chunk_(\d+)_part(\d+)of(\d+)_findings\.parquet$", fn)
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
    r = _list_chunk_files(host)
    whole, parts = set(), {}
    px = re.escape(FILE_PREFIX)
    for a in r["artifacts"]:
        fn = a["filename"]
        mw = re.match(rf"{px}chunk_(\d+)_fulltext\.json\.gz$", fn)
        mp = re.match(rf"{px}chunk_(\d+)_part(\d+)of(\d+)_fulltext\.json\.gz$", fn)
        if mw and int(mw.group(1)) in targets:
            whole.add(int(mw.group(1)))
        elif mp and int(mp.group(1)) in targets:
            parts.setdefault(int(mp.group(1)), set()).add((int(mp.group(2)), int(mp.group(3))))
    return whole, parts


def expected_fulltext(roster_df, cid):
    """Papers in chunk `cid` likely to yield FULL TEXT: an explicit `fulltext_expected` column if
    the roster has one, else pmcid present AND is_oa. Europe PMC OA flags undercount by ~30% on
    the POTS build, so this is a floor, not a ceiling."""
    # Accepts a pandas DataFrame OR a plain list of dict rows: the driver runs in the stdlib-only
    # orchestration kernel, where the roster arrives as JSON (python pipeline/config.py is the
    # same story). Column absence -> conservative: every paper may be full text.
    if hasattr(roster_df, "columns"):
        rows = roster_df[roster_df.chunk_id == cid].to_dict("records"); cols = set(roster_df.columns)
    else:
        rows = [r for r in roster_df if int(r.get("chunk_id", -1)) == int(cid)]
        cols = set().union(*(r.keys() for r in rows)) if rows else set()
    n = len(rows)
    def _truthy(v):
        return bool(v) and not (isinstance(v, float) and v != v)
    if "fulltext_expected" in cols:
        return sum(1 for r in rows if _truthy(r.get("fulltext_expected"))), n
    if "pmcid" in cols and "is_oa" in cols:
        return sum(1 for r in rows if r.get("pmcid") not in (None, "") and not (isinstance(r.get("pmcid"), float))
                   and _truthy(r.get("is_oa"))), n
    return n, n


def plan_parts(roster_df, cid, n_parts_default=None, max_ft_per_unit=None):
    """BUILDER 2026-09-30: size a chunk's work units by FULL-TEXT DENSITY, not by paper count.
    Measured cause: the EDS dry run's chunk 0 (25/25 open-access papers) as ONE unit exceeded
    WORKER_MAX_S=3000 s with nothing saved; POTS 25-paper parts were 29-62% abstract-only and
    the pinned watchdog was calibrated on that composition. Returns (n_parts, n_ft, n_papers):
    n_parts = max(n_parts_default, ceil(n_ft / max_ft_per_unit))."""
    n_parts_default = N_PARTS_DEFAULT if n_parts_default is None else int(n_parts_default)
    max_ft_per_unit = MAX_FT_PER_UNIT if max_ft_per_unit is None else int(max_ft_per_unit)
    n_ft, n_papers = expected_fulltext(roster_df, cid)
    need = -(-n_ft // max(1, max_ft_per_unit)) if n_ft else 1
    # 2026-10-07: a chunk that lost most of its papers to screening (EDS Tier 1: 6-49 papers per
    # 50-slot chunk after 569 exclusions) must not be split into MORE units than its papers need.
    # The default part count exists to keep a unit at <= papers_per_chunk/n_parts_default papers;
    # never exceed ceil(n_papers / that unit size), and never below 1.
    unit_cap = max(1, -(-PAPERS_PER_CHUNK // max(1, n_parts_default)))
    by_count = max(1, -(-n_papers // unit_cap)) if n_papers else 1
    return max(1, min(n_parts_default, by_count), need), n_ft, n_papers


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
fn = ("__PREFIX__chunk_%03d_findings.parquet" % CID) if not PART else \\
     ("__PREFIX__chunk_%03d_part%dof%d_findings.parquet" % (CID, PART[0], PART[1]))
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
                .replace("__PREFIX__", FILE_PREFIX)
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
    outfn = (f"{FILE_PREFIX}chunk_{cid:03d}_findings.parquet" if not part
             else f"{FILE_PREFIX}chunk_{cid:03d}_part{part[0]}of{part[1]}_findings.parquet")
    arcfn = (f"{FILE_PREFIX}chunk_{cid:03d}_fulltext.json.gz" if not part
             else f"{FILE_PREFIX}chunk_{cid:03d}_part{part[0]}of{part[1]}_fulltext.json.gz")
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


def run(host, build_task, gate, targets, split_all=True, n_parts=N_PARTS_DEFAULT, log_prefix="",
        roster_df=None, run_tag=None, dispatch_deadline=None):
    """Dispatch `targets` as parts. Returns a computed report -- every count in it
    comes from a variable, never a typed literal.

    BUILDER 2026-09-30: when `roster_df` is given, each chunk's part count is plan_parts()
    (full-text density), never below `n_parts`; a chunk whose plan is 1 part and split_all=False
    runs whole. Without a roster the legacy behaviour is unchanged."""
    targets = set(targets)
    # BUILDER 2026-09-30: per-run log filenames. Two runs in one working directory (EDS dry run:
    # chunks 0-2, then chunk 0 re-sized) clobbered gate_log_parts.json; the first run's gate
    # entries survived only in the cell's stdout.
    run_tag = run_tag or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    _logfn = lambda stem: f"{FILE_PREFIX}{stem}_{run_tag}.json"
    done, have_parts = done_units(host, targets)
    # work unit = (cid, (i,n)) for parts, or (cid, None) for a whole attempt
    pending = []
    unit_plan = {}
    for cid in sorted(targets - done):
        np_ = n_parts
        if roster_df is not None:
            np_, n_ft, n_pap = plan_parts(roster_df, cid, n_parts_default=n_parts)
            unit_plan[cid] = {"n_parts": np_, "expected_fulltext": n_ft, "papers": n_pap,
                              "max_fulltext_per_unit": MAX_FT_PER_UNIT}
            print(f"{log_prefix}[parts] chunk {cid}: {n_ft}/{n_pap} papers expected full text -> "
                  f"{np_} part(s) (cap {MAX_FT_PER_UNIT} full texts per unit)", flush=True)
        if split_all or np_ > 1:
            got = have_parts.get(cid, set())
            for i in range(np_):
                if (i, np_) not in got:
                    pending.append((cid, (i, np_)))
        else:
            pending.append((cid, None))

    attempts, inflight, starts = {}, {}, {}
    gate_log, guard_log, enum_log, ceiling_log = [], [], [], []
    finished = failed = 0
    print(f"{log_prefix}[parts] start: {len(pending)} work units over "
          f"{len(targets - done)} chunks; {len(done)} already done", flush=True)

    # 2026-10-08: `dispatch_deadline` (epoch seconds) = the user's usage window end minus the
    # longest unit we have observed. After it, NO new unit is started: in-flight units finish and
    # save, pending ones are left for the next session (done-detection re-plans them from the
    # store). A unit killed mid-run saves nothing, so the deadline is what bounds loss to zero.
    def refill():
        if dispatch_deadline is not None and time.time() >= dispatch_deadline and pending:
            if not refill.announced:
                print(f"{log_prefix}[parts] dispatch deadline reached: {len(pending)} unit(s) left for the next session", flush=True)
                refill.announced = True
            return
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

    refill.announced = False
    last_completion = time.time(); completed_walls = []
    def _stall_s():
        return min(WORKER_MAX_S, max(STALL_MIN_S, int(2.5 * max(completed_walls)) if completed_walls else STALL_MIN_S))
    def _never_started(fid):
        try:
            fr = host.frames(frame_id=fid, max_results=12)
            return not any(m.get("role") == "assistant" for m in (fr.get("messages") or []))
        except Exception:  # noqa: BLE001 - a store hiccup must not fabricate a stall
            return False
    def _system_stalled(now):
        if not inflight:
            return False
        if now - last_completion >= _stall_s() and all(now - starts.get(fid, now) >= _stall_s() for fid in inflight):
            return True
        # first-turn check: every in-flight worker older than FIRST_TURN_S and none has started
        old_enough = [fid for fid in inflight if now - starts.get(fid, now) >= FIRST_TURN_S]
        if old_enough and len(old_enough) == len(inflight) and all(_never_started(fid) for fid in old_enough):
            print(f"{log_prefix}[parts] first-turn check: {len(inflight)} in-flight workers older than {FIRST_TURN_S} s with no assistant turn", flush=True)
            return True
        return False
    stalled = False
    def _past_deadline():
        return dispatch_deadline is not None and time.time() >= dispatch_deadline
    refill()
    while inflight or (pending and not _past_deadline()):
        res = host.collect(list(inflight.keys()), timeout=COLLECT_SLICE) if inflight else []
        now = time.time()
        for r in res or []:
            if r.get("status") in ("completed", "failed", "cancelled") and r.get("frame_id") in starts:
                last_completion = now; completed_walls.append(now - starts[r["frame_id"]])
        if _system_stalled(now):
            stalled = True
            print(f"{log_prefix}[parts] SYSTEM STALL: no unit completed for {int(now - last_completion)} s and all "
                  f"{len(inflight)} in-flight units are older than {_stall_s()} s -> stopping workers, NOT re-dispatching", flush=True)
            for fid, (cid, part) in list(inflight.items()):
                try:
                    host.stop_child(fid, reason="system stall: no unit completed for %d s" % int(now - last_completion))
                except Exception as e:  # noqa: BLE001
                    print(f"{log_prefix}[parts] stop_child({fid[:8]}) failed: {str(e)[:100]}", flush=True)
                pending.insert(0, (cid, part)); attempts[(cid, part)] = max(0, attempts.get((cid, part), 1) - 1)
            inflight.clear()
            break
        # watchdog: reclaim a slot from a worker past WORKER_MAX_S
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
                    if WATCHDOG_RESPLIT:
                        # BUILDER 2026-09-30: a unit that ran out of TIME is re-dispatched SMALLER,
                        # exactly like a token-ceiling kill. Re-queuing the same shape re-queues
                        # the same timeout (EDS chunk 0: 3 identical attempts, nothing saved).
                        nxt = (part[1] * 2) if part else max(2, n_parts)
                        got = have_parts.get(cid, set())
                        ceiling_log.append({"chunk": cid, "part": part, "attempt": attempts.get((cid, part)),
                                            "kind": "watchdog", "resplit_to": nxt})
                        print(f"{log_prefix}[parts] chunk {cid} part {part} watchdog -> re-splitting "
                              f"into {nxt} parts", flush=True)
                        for i in range(nxt):
                            if (i, nxt) not in got:
                                pending.append((cid, (i, nxt)))
                    else:
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
                             # 2026-10-07: measured spend + realised full-text count per unit, so the
                             # wave controller's budget gate reads the worker's own numbers.
                             "tokens_out": st.get("total_out_tokens"),
                             "retained_fulltext": st.get("retained_fulltext_papers"),
                             "n_expected": st.get("n_expected"),
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
                # 2026-10-09 efficiency rule: an under-yield unit that SAVED its findings, with blocked
                # fraction inside the band and no guard violations, is structural (thin abstracts, wrong-
                # sense papers, sibling no-yield papers) — Tier 1 spent ~0.6M tokens on retries that
                # reproduced the same yield. Accept it with a flag instead of re-dispatching.
                structural = (RETRY_UNDER_YIELD is False and vid and str(reason).startswith("under-yield")
                              and (st.get("blocked_frac") or 0) <= MAX_BLOCKED and not gv)
                if structural:
                    gate_log[-1]["accepted_structural_under_yield"] = True
                    print(f"{log_prefix}[parts] chunk {cid} part {part} under-yield accepted as structural "
                          f"(saved, blocked {st.get('blocked_frac')}, 0 guard violations): {reason}", flush=True)
                elif attempts.get(key, 0) < MAX_ATTEMPTS:
                    pending.append((cid, part))
                print(f"{log_prefix}[parts] chunk {cid} part {part} FAILED: {reason}", flush=True)

        refill()
        for fn, obj in ((_logfn("gate_log_parts"), gate_log), (_logfn("guard_violations_parts"), guard_log),
                        (_logfn("enum_violations_parts"), enum_log), (_logfn("ceiling_log_parts"), ceiling_log)):
            json.dump(obj, open(fn, "w"), indent=1)
        d3, _ = done_units(host, targets)
        aw, ap = archive_units(host, targets)
        with open(f"{FILE_PREFIX}progress_parts_{run_tag}.txt", "a") as fp:
            fp.write(f"{len(d3 & targets)}/{len(targets)} chunks | units in-flight {len(inflight)} "
                     f"| pending {len(pending)} | +{finished} -{failed} "
                     f"| archives {len(aw) + len(ap)}\n")
        print(f"{log_prefix}[parts] {len(d3 & targets)}/{len(targets)} chunks done | "
              f"in-flight {len(inflight)} | pending {len(pending)} | "
              f"archives {len(aw) + len(ap)}", flush=True)

    deferred_units = [{"chunk": c, "part": p} for (c, p) in pending]   # left for the next session / after the stall
    final_done, final_parts = done_units(host, targets)
    arc_whole, arc_parts = archive_units(host, targets)
    return {"run_tag": run_tag, "unit_plan": unit_plan, "chunks_done": sorted(final_done & targets),
            "deferred_units": deferred_units, "dispatch_deadline": dispatch_deadline,
            "stalled": stalled, "stall_threshold_s": _stall_s(), "completed_unit_walls_s": [round(w) for w in completed_walls],
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
