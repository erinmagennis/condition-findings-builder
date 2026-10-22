# EDS Tier 2 — roster/screen handoff (2026-10-13)

## State
- Roster v1 frozen: `eds_tier2_roster_v1.parquet` vid `9895e8f4-766a-429b-b708-f44f2c387e54` — 3,859 papers in chunks 30-137 (chunks 31-35 emptied by the title-only split; ids never re-cut). Title-only not-extractable: 1,503 (`eds_tier2_not_extractable_title_only.parquet` 2b49ce97-4da1-4704-adb9-e9594a1ec056).
- Pass-1 screen: units 1 and 2 complete (467 papers; vids in EDS_RESUME.md). Unit 3 PARTIAL: 136/246 LLM verdicts; 110 rows hit the host.llm per-frame 2.0M token ceiling (+1 JSON parse failure in unit 2) -> retry list `screen_eds_t2_pass1_retry_pmids.csv` (111 pmids). Units 4-17 NOT RUN.
- Why it stopped: `host.llm()` enforces a cumulative 2.0M-token ceiling per frame (prompt + reserved max_tokens; ~570 screen calls at max_tokens 2000). The roster/screen sub-agent was a leaf frame (no `host.delegate`), so the remaining units must be fanned out by the lead to one fresh sub-agent per unit — exactly as Tier 1 did (6 concurrent OPERON units, 2026-10-07).
- Interim pass-1 over the 602 valid papers: in_scope 402 / uncertain 174 / off_topic 26 (both arms); LLM arm off_topic 150 — 20-span audit: 20/20 legitimate topical drift or incidental mention (details in EDS_RESUME.md).

## Pass-1 units remaining (chunk-aligned, <=250 papers; each fits run_screen's in-frame plan)

| unit | chunk ids | papers |
|---|---|---|
| 3R | retry list (chunks 53,57,60,61,62) | 111 |
| 4 | 63-68 | 236 |
| 5 | 69-74 | 241 |
| 6 | 75-80 | 218 |
| 7 | 81-85 | 221 |
| 8 | 86-90 | 218 |
| 9 | 91-96 | 250 |
| 10 | 97-101 | 210 |
| 11 | 102-107 | 240 |
| 12 | 108-112 | 220 |
| 13 | 113-117 | 220 |
| 14 | 118-122 | 232 |
| 15 | 123-127 | 227 |
| 16 | 128-132 | 224 |
| 17 | 133-137 | 189 |

Total remaining pass-1 calls: 3,257. Run <= 6 units concurrently (Tier-1 precedent). Unit file names must be `screen_eds_t2_unit{k}_*`; the merge script keeps, for a pmid screened twice, the row that carries an LLM verdict.

## After pass 1
```
python scripts/eds_tier2_screen_merge.py merge-pass1 --roster eds_tier2_roster_v1.parquet --reports screen_eds_t2_unit*_report.csv --out screen_eds_t2_pass1
```
Refuses while any rostered paper lacks an LLM verdict. Writes `screen_eds_t2_pass1_uncertain.parquet` and `screen_eds_t2_pass1_pass2_units.json` (chunk-aligned units <= 200). Save both, then dispatch one sub-agent per pass-2 unit with the brief below.

## After pass 2
```
python scripts/eds_tier2_screen_merge.py finalize --roster eds_tier2_roster_v1.parquet --pass1 screen_eds_t2_pass1_merged.csv --pass2 popcheck_t2_unit*_report.csv
```
Writes screen_eds_t2_final.csv, eds_tier2_roster_v2.parquet, eds_tier2_excluded_pmids_v2.{parquet,csv}, eds_tier2_roster_v2_report.json, EDS_TIER2_COST_PROJECTION.md (+json). Then: pin roster v2's vid as artifacts.roster_vid in config/eds_tier2.yaml, `python pipeline/config.py export-json config/eds_tier2.yaml > config/eds_tier2.json`, re-run config/driver/registry tests under CORPUS_CONFIG=config/eds_tier2.json, re-tar, update EDS_RESUME.md.

Rule (fixed, population_check.combine): exclude ONLY where pass-1 LLM off_topic AND pass-2 'no listed population studied' (both verbatim spans) or both pass-1 arms agree; everything else is extracted and flagged.

## Sub-agent briefs — pass 1

### Unit 3R
```
You are SCREEN UNIT 3R of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); rt = pd.read_csv('{{artifact:7e2135eb-951f-4ef5-a188-17606110b574}}', dtype={'pmid': str}); sub = ro[ro.pmid.astype(str).isin(rt.pmid)].copy()
   assert len(sub) == 111; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit3R', max_concurrency=8)
   ONE model call per paper (111 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit3R_report.csv','screen_eds_t2_unit3R_summary.json','screen_eds_t2_unit3R_excluded_pmids.csv','screen_eds_t2_unit3R_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 4
```
You are SCREEN UNIT 4 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([63, 64, 65, 66, 67, 68])].copy()
   assert len(sub) == 236; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit4', max_concurrency=8)
   ONE model call per paper (236 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit4_report.csv','screen_eds_t2_unit4_summary.json','screen_eds_t2_unit4_excluded_pmids.csv','screen_eds_t2_unit4_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 5
```
You are SCREEN UNIT 5 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([69, 70, 71, 72, 73, 74])].copy()
   assert len(sub) == 241; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit5', max_concurrency=8)
   ONE model call per paper (241 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit5_report.csv','screen_eds_t2_unit5_summary.json','screen_eds_t2_unit5_excluded_pmids.csv','screen_eds_t2_unit5_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 6
```
You are SCREEN UNIT 6 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([75, 76, 77, 78, 79, 80])].copy()
   assert len(sub) == 218; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit6', max_concurrency=8)
   ONE model call per paper (218 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit6_report.csv','screen_eds_t2_unit6_summary.json','screen_eds_t2_unit6_excluded_pmids.csv','screen_eds_t2_unit6_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 7
```
You are SCREEN UNIT 7 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([81, 82, 83, 84, 85])].copy()
   assert len(sub) == 221; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit7', max_concurrency=8)
   ONE model call per paper (221 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit7_report.csv','screen_eds_t2_unit7_summary.json','screen_eds_t2_unit7_excluded_pmids.csv','screen_eds_t2_unit7_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 8
```
You are SCREEN UNIT 8 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([86, 87, 88, 89, 90])].copy()
   assert len(sub) == 218; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit8', max_concurrency=8)
   ONE model call per paper (218 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit8_report.csv','screen_eds_t2_unit8_summary.json','screen_eds_t2_unit8_excluded_pmids.csv','screen_eds_t2_unit8_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 9
```
You are SCREEN UNIT 9 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([91, 92, 93, 94, 95, 96])].copy()
   assert len(sub) == 250; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit9', max_concurrency=8)
   ONE model call per paper (250 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit9_report.csv','screen_eds_t2_unit9_summary.json','screen_eds_t2_unit9_excluded_pmids.csv','screen_eds_t2_unit9_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 10
```
You are SCREEN UNIT 10 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([97, 98, 99, 100, 101])].copy()
   assert len(sub) == 210; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit10', max_concurrency=8)
   ONE model call per paper (210 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit10_report.csv','screen_eds_t2_unit10_summary.json','screen_eds_t2_unit10_excluded_pmids.csv','screen_eds_t2_unit10_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 11
```
You are SCREEN UNIT 11 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([102, 103, 104, 105, 106, 107])].copy()
   assert len(sub) == 240; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit11', max_concurrency=8)
   ONE model call per paper (240 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit11_report.csv','screen_eds_t2_unit11_summary.json','screen_eds_t2_unit11_excluded_pmids.csv','screen_eds_t2_unit11_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 12
```
You are SCREEN UNIT 12 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([108, 109, 110, 111, 112])].copy()
   assert len(sub) == 220; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit12', max_concurrency=8)
   ONE model call per paper (220 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit12_report.csv','screen_eds_t2_unit12_summary.json','screen_eds_t2_unit12_excluded_pmids.csv','screen_eds_t2_unit12_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 13
```
You are SCREEN UNIT 13 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([113, 114, 115, 116, 117])].copy()
   assert len(sub) == 220; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit13', max_concurrency=8)
   ONE model call per paper (220 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit13_report.csv','screen_eds_t2_unit13_summary.json','screen_eds_t2_unit13_excluded_pmids.csv','screen_eds_t2_unit13_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 14
```
You are SCREEN UNIT 14 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([118, 119, 120, 121, 122])].copy()
   assert len(sub) == 232; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit14', max_concurrency=8)
   ONE model call per paper (232 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit14_report.csv','screen_eds_t2_unit14_summary.json','screen_eds_t2_unit14_excluded_pmids.csv','screen_eds_t2_unit14_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 15
```
You are SCREEN UNIT 15 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([123, 124, 125, 126, 127])].copy()
   assert len(sub) == 227; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit15', max_concurrency=8)
   ONE model call per paper (227 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit15_report.csv','screen_eds_t2_unit15_summary.json','screen_eds_t2_unit15_excluded_pmids.csv','screen_eds_t2_unit15_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 16
```
You are SCREEN UNIT 16 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([128, 129, 130, 131, 132])].copy()
   assert len(sub) == 224; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit16', max_concurrency=8)
   ONE model call per paper (224 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit16_report.csv','screen_eds_t2_unit16_summary.json','screen_eds_t2_unit16_excluded_pmids.csv','screen_eds_t2_unit16_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

### Unit 17
```
You are SCREEN UNIT 17 of 17 (stage 3, two-arm screen) for the EDS Tier-2 (pre-2019) corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version; folder condition-findings-builder/). Read pipeline/screen.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0, 'condition-findings-builder/pipeline'); import config as ccfg, screen as cscreen, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   ro = pd.read_parquet('{{artifact:9895e8f4-766a-429b-b708-f44f2c387e54}}'); sub = ro[ro.chunk_id.isin([133, 134, 135, 136, 137])].copy()
   assert len(sub) == 189; df, summary = cscreen.run_screen(host, cfg, sub, out_prefix='screen_eds_t2_unit17', max_concurrency=8)
   ONE model call per paper (189 calls, max_tokens 2000, model from the config = claude-sonnet-5). Do not raise concurrency; do not change the prompt; do not re-run if the first run completed. If any row has llm_error, list those pmids in your output (they get a retry unit; never invent a verdict).
3. save_artifacts(files=['screen_eds_t2_unit17_report.csv','screen_eds_t2_unit17_summary.json','screen_eds_t2_unit17_excluded_pmids.csv','screen_eds_t2_unit17_llm_offtopic_flagged.csv'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary's n_screened, final, llm, rule, llm_span_verbatim_rate, n_llm_offtopic_flagged_not_excluded, instrument_alarm.alarm, n_llm_errors, llm_output_tokens; deviations = ['none'] or what you cut.
Rules: flag, never repair; every count with its denominator; no publishing; if a domain is blocked request access rather than substituting. The instrument alarm WILL fire (anchor-drawn roster: rule off_topic = 0 by construction) — report it, do not stop or edit the prompt.
```

## Sub-agent brief template — pass 2 (fill k/K/chunk_ids/n from screen_eds_t2_pass1_pass2_units.json)
```
You are POPULATION-CHECK UNIT {k} of {K} (stage 3b) for the EDS Tier-2 corpus build. Do exactly this; no extraction, no extra analysis.
1. Untar the method repo (artifact d8a146bc-4cc5-4bee-bb16-8e628e9c708c, latest version). Read pipeline/population_check.py's docstring.
2. In the `python` tool (environment='python'):
   import sys; sys.path.insert(0,'condition-findings-builder/pipeline'); import config as ccfg, population_check as pc, pandas as pd
   cfg = ccfg.load_config('condition-findings-builder/config/eds_tier2.yaml', warn=None)
   u = pd.read_parquet('{{artifact:<screen_eds_t2_pass1_uncertain.parquet vid>}}'); sub = u[u.chunk_id.isin({chunk_ids})].copy(); assert len(sub) == {n}
   df, summary = pc.run_population_check(host, cfg, sub, out_prefix='popcheck_t2_unit{k}', max_concurrency=8)
   One model call per paper, max_tokens 2000, the config's model. Do not change the prompt; do not re-run a completed run.
3. save_artifacts(files=['popcheck_t2_unit{k}_report.csv','popcheck_t2_unit{k}_summary.json'], language='python').
4. Submit structured output: artifacts (filename -> version_id), counts = the summary dict, deviations = ['none'] or what you cut.
Rules: flag never repair; counts with denominators; no publishing.
```
