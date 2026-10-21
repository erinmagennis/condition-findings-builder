# Sharded identifier resolver (2026-10-21, Tier-2 EDS): run N processes over the uncached (entity,kind) pairs and HPO terms
# of a merged findings table, each writing shards/cache_shard<i>.parquet; merge with resolver.load_cache_v4 + save_cache_v4
# before resolve_stage.run. 4 shards gave ~2.6-3.2 lookups/s aggregate vs ~0.8/s single-process (8,373 lookups in 40 min, 0 errors).
# Usage: python scripts/resolve_shard.py <i> <N> [budget_s]   (expects eds_t2_findings_merged_raw.parquet and resolution_cache_snapshot.parquet in cwd; edit paths for other areas)
import sys, os, time, json, pandas as pd
sys.path.insert(0, "ccb/condition-corpus-builder/pipeline")
import resolver as RS, resolve_stage as RSS
i, N = int(sys.argv[1]), int(sys.argv[2]); budget_s = float(sys.argv[3]) if len(sys.argv) > 3 else 7200
SEED = "resolution_cache_snapshot.parquet"; OUT = f"shards/cache_shard{i}.parquet"; os.makedirs("shards", exist_ok=True)
F = pd.read_parquet("eds_t2_findings_merged_raw.parquet")
cache, cst = RS.load_cache_v4(SEED); RS._CACHE.update(cache)
pairs = sorted({(n.strip(), k) for n, k in zip(F.entity, F.entity_kind) if isinstance(n, str) and n.strip() and isinstance(k, str)})
routed = [(n, k) for n, k in pairs if k == "gene/protein" or k in RS.ROUTES]
terms = sorted({t for s in F.phenotype_terms for t in RSS._split_terms(s)})
todo = [("e", n, k) for n, k in routed if (n.lower(), k) not in RS._CACHE] + [("t", t, None) for t in terms if (t.lower(), "__hpo_term__") not in RS._CACHE]
mine = todo[i::N]
new = {}
def snapshot():
    tmp = OUT + ".tmp"; RS.save_cache_v4(new, tmp); os.replace(tmp, OUT)
t0 = time.time(); n = 0; errs = 0
print(f"[shard {i}/{N}] todo total {len(todo)} mine {len(mine)}", flush=True)
for typ, nm, k in mine:
    if time.time() - t0 > budget_s: break
    if typ == "e":
        key = (nm.lower(), k)
        try: RS.resolve_entity(nm, k)
        except Exception: errs += 1; RS._CACHE.setdefault(key, (None, None, "unresolved_error", None))
    else:
        key = (nm.lower(), "__hpo_term__")
        try:
            i_, l_, path, mint = RS._ols(nm, "hp"); RS._CACHE[key] = ((i_, l_, path, mint) if i_ else (None, None, "unresolved_nulled", None))
        except Exception: errs += 1; RS._CACHE[key] = (None, None, "unresolved_error", None)
    if key in RS._CACHE: new[key] = RS._CACHE[key]
    n += 1
    if n % 50 == 0: snapshot(); print(f"[shard {i}] {n}/{len(mine)} {int(time.time()-t0)}s", flush=True)
snapshot()
print(json.dumps({"shard": i, "lookups": n, "errors": errs, "wall_s": int(time.time()-t0), "new": len(new), "left": len(mine)-n}), flush=True)
