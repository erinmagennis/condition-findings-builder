
# Wave controller for the EDS Tier-1 build. Runs in the orchestration kernel (stdlib only).
# Usage (repl): exec(open("handoff/waves.py").read()); run_waves(host, D, plan, waves, cost_model, budget_total_tokens)
import json, os, time

def _tok_from_result(rep):
    """Sum worker output tokens from gate_log stats where present (stats.tokens_out); fallback None."""
    tot, n = 0, 0
    for g in rep.get("gate_log", []):
        t = g.get("tokens_out")
        if t: tot += int(t); n += 1
    return (tot if n else None), n

def project_units(D, plan, cids, cost_model):
    """Projected output tokens for chunk ids under a (per_ft, per_abs) cost model."""
    per_ft, per_abs = cost_model
    tot = 0
    for cid in cids:
        _, n_ft, n_pap = D.plan_parts(plan, cid)
        tot += n_ft * per_ft + (n_pap - n_ft) * per_abs
    return tot

def run_waves(host, D, plan, waves, cost_model, budget_total_tokens, state_path="qa/build_state.json", over_frac=0.20):
    os.makedirs("qa", exist_ok=True)
    state = json.load(open(state_path)) if os.path.exists(state_path) else {"waves": [], "stopped": None, "spent_measured_tokens": 0, "spent_projected_tokens": 0}
    for k, cids in enumerate(waves):
        done, _ = D.done_units(host, set(cids))
        todo = [c for c in cids if c not in done]
        if not todo:
            print(f"[wave {k}] all chunks already done; skipping", flush=True); continue
        proj = project_units(D, plan, todo, cost_model)
        print(f"[wave {k}] chunks {todo}: projected {proj/1e6:.2f}M output tokens", flush=True)
        t0 = time.time()
        rep = D.run(host, D.build_task, D.gate, todo, split_all=True, roster_df=plan, run_tag=f"wave{k}")
        rep["wall_s"] = round(time.time() - t0)
        json.dump(rep, open(f"qa/wave_{k}.json", "w"), indent=1, default=str)
        measured, n_meas = _tok_from_result(rep)
        failed_units = [g for g in rep["gate_log"] if not g["ok"]]
        guard = sum(int(g.get("guard_violations") or 0) for g in rep["gate_log"])
        entry = {"wave": k, "chunks": todo, "chunks_done": rep["chunks_done"], "chunks_missing": rep["chunks_missing"],
                 "projected_tokens": proj, "measured_tokens": measured, "units_with_token_counts": n_meas,
                 "n_gate_fail": len(failed_units), "guard_violations": guard, "wall_s": rep["wall_s"],
                 "chunks_without_archive": rep["chunks_without_archive"], "ceiling_events": len(rep["ceiling_events"])}
        state["waves"].append(entry); state["spent_projected_tokens"] += proj
        if measured: state["spent_measured_tokens"] += measured
        json.dump(state, open(state_path, "w"), indent=1, default=str)
        print(f"[wave {k}] done {len(rep['chunks_done'])}/{len(todo)} | gate fails {len(failed_units)} | guard {guard} | "
              f"measured {measured} vs projected {proj} | wall {rep['wall_s']}s", flush=True)
        # --- automatic gates (user-approved): stop on budget overrun or structural failure ---
        if rep["chunks_missing"]:
            state["stopped"] = f"wave {k}: chunks {rep['chunks_missing']} missing after {D.MAX_ATTEMPTS} attempts"; break
        if guard:
            state["stopped"] = f"wave {k}: {guard} guard violations"; break
        if measured and measured > proj * (1 + over_frac):
            state["stopped"] = f"wave {k}: measured {measured} > projected {proj} by more than {int(over_frac*100)}%"; break
        if state["spent_measured_tokens"] > budget_total_tokens * (1 + over_frac):
            state["stopped"] = f"cumulative measured {state['spent_measured_tokens']} exceeds budget {budget_total_tokens} by >{int(over_frac*100)}%"; break
    json.dump(state, open(state_path, "w"), indent=1, default=str)
    print("STATE", json.dumps({k: v for k, v in state.items() if k != "waves"}), flush=True)
    return state
