
# EDS Tier-1 wave controller (stdlib only; runs in the orchestration kernel). ONE wave per call so
# the orchestrator can persist qa/ to the artifact store between waves. State of record = the
# artifact store (per-unit findings + archives); qa/build_state.json is a derived ledger.
import json, os, time

def project_units(D, plan, cids, cost_model):
    per_ft, per_abs = cost_model; tot = 0
    for cid in cids:
        _, n_ft, n_pap = D.plan_parts(plan, cid); tot += n_ft * per_ft + (n_pap - n_ft) * per_abs
    return tot

def run_wave(host, D, plan, k, cids, cost_model, budget_total_tokens, dispatch_deadline=None,
             state_path="qa/build_state.json", over_frac=0.20):
    os.makedirs("qa", exist_ok=True)
    state = json.load(open(state_path)) if os.path.exists(state_path) else {"waves": [], "stopped": None, "spent_measured_tokens": 0, "spent_projected_tokens": 0}
    if state.get("stopped"):
        print("[wave] controller is stopped:", state["stopped"]); return state
    done, _ = D.done_units(host, set(cids)); todo = [c for c in cids if c not in done]
    if not todo:
        print(f"[wave {k}] all chunks already done"); return state
    proj = project_units(D, plan, todo, cost_model)
    print(f"[wave {k}] chunks {todo}: projected {proj/1e6:.2f}M output tokens; dispatch deadline "
          f"{time.strftime('%H:%M:%SZ', time.gmtime(dispatch_deadline)) if dispatch_deadline else 'none'}", flush=True)
    t0 = time.time()
    rep = D.run(host, D.build_task, D.gate, todo, split_all=True, roster_df=plan, run_tag=f"wave{k}", dispatch_deadline=dispatch_deadline)
    rep["wall_s"] = round(time.time() - t0)
    json.dump(rep, open(f"qa/wave_{k}.json", "w"), indent=1, default=str)
    ok_units = [g for g in rep["gate_log"] if g["ok"]]
    measured = sum(int(g["tokens_out"]) for g in ok_units if g.get("tokens_out"))
    guard = sum(int(g.get("guard_violations") or 0) for g in rep["gate_log"])
    entry = {"wave": k, "chunks": todo, "chunks_done": rep["chunks_done"], "chunks_missing": rep["chunks_missing"],
             "deferred_units": rep["deferred_units"], "stalled": rep["stalled"],
             "projected_tokens": proj, "measured_tokens": measured, "units_ok": len(ok_units),
             "n_gate_fail": len(rep["gate_log"]) - len(ok_units), "guard_violations": guard, "wall_s": rep["wall_s"],
             "findings": sum(int(g["findings"] or 0) for g in ok_units),
             "chunks_without_archive": rep["chunks_without_archive"], "ceiling_events": len(rep["ceiling_events"]),
             "unit_walls_s": rep["completed_unit_walls_s"], "finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    state["waves"].append(entry)
    # projected spend counts only the chunks that actually completed
    done_proj = project_units(D, plan, rep["chunks_done"], cost_model)
    state["spent_projected_tokens"] += done_proj; state["spent_measured_tokens"] += measured
    print(f"[wave {k}] done {len(rep['chunks_done'])}/{len(todo)} | units ok {len(ok_units)} | gate fails {entry['n_gate_fail']} | guard {guard} | "
          f"measured {measured:,} vs projected(done) {done_proj:,.0f} | stalled {rep['stalled']} | deferred {len(rep['deferred_units'])} | wall {rep['wall_s']}s", flush=True)
    if rep["stalled"]:
        state["stopped"] = f"wave {k}: system stall (no unit completed for {rep['stall_threshold_s']} s); {len(rep['deferred_units'])} units deferred"
    elif guard:
        state["stopped"] = f"wave {k}: {guard} guard violations"
    elif rep["chunks_missing"] and not rep["deferred_units"]:
        state["stopped"] = f"wave {k}: chunks {rep['chunks_missing']} failed after {D.MAX_ATTEMPTS} attempts"
    elif done_proj and measured > done_proj * (1 + over_frac):
        state["stopped"] = f"wave {k}: measured {measured:,} > projected {done_proj:,.0f} by more than {int(over_frac*100)}%"
    elif state["spent_measured_tokens"] > budget_total_tokens * (1 + over_frac):
        state["stopped"] = f"cumulative measured {state['spent_measured_tokens']:,} exceeds budget {budget_total_tokens:,} by >{int(over_frac*100)}%"
    json.dump(state, open(state_path, "w"), indent=1, default=str)
    print("STATE", json.dumps({k2: v for k2, v in state.items() if k2 != "waves"}), flush=True)
    return state
