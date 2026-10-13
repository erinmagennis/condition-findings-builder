# EDS Tier 2 — cost projection (PROVISIONAL, roster v1 pre-screen)

**PROVISIONAL.** Computed on roster v1 (3,859 papers, BEFORE the two-pass screen) because the screen could not be completed in this frame (host.llm per-frame 2.0M ceiling after 2.5 of 17 units; leaf frame cannot delegate). Tier 1 removed 569/1,463 (38.9%) at the screen; the interim Tier-2 pass-1 over 602 papers shows 26 both-arm exclusions (4.3%) and 174 uncertain (28.9%) pending pass 2, so the post-screen figure is expected to land between ~70% and ~96% of these numbers. Re-run `scripts/eds_tier2_screen_merge.py finalize` after pass 2 to replace this file.

Generated 2026-10-13T22:24:28+00:00. Basis: 18,024 output tokens per full-text paper, 5,326 per abstract-only paper (cost model v2 (config/eds_cost_model_v2.json): least squares on the 4 Tier-1 pilot units (2026-10-07)).
Units per chunk = per chunk max(1, min(2, ceil(papers/25)), ceil(fulltext/12)).

**3,859 papers** (392 full text expected, 3,467 abstract-only), **195 units**, **25,530,650 output tokens nominal**, **33,189,846 at +30%**.

| decade | papers | full text | abstract-only | units | tokens nominal | tokens +30% |
|---|---|---|---|---|---|---|
| 1940s | 1 | 1 | 0 | 1 | 18,024 | 23,431 |
| 1960s | 3 | 0 | 3 | 2 | 15,978 | 20,772 |
| 1970s | 139 | 0 | 139 | 11 | 740,314 | 962,407 |
| 1980s | 403 | 1 | 402 | 24 | 2,159,076 | 2,806,801 |
| 1990s | 678 | 3 | 675 | 34 | 3,649,122 | 4,743,858 |
| 2000s | 1,000 | 28 | 972 | 48 | 5,681,544 | 7,386,007 |
| 2010s | 1,635 | 359 | 1,276 | 75 | 13,266,592 | 17,246,570 |

## Recommended pilot chunks

- chunk 136 (2018-2018; 45 papers, 23 FT, 2 units): 2010s chunk with the most full-text papers: exercises the full-text path, the max_fulltext_per_unit split and the worker ceiling; refit the per-FT token coefficient here
- chunk 52 (1985-1986; 37 papers, 0 FT, 2 units): largest pre-1990 chunk (abstract-only, oldest abstract style, highest screen-uncertain share): tests yield on short historical abstracts and the per-abstract coefficient

Chunks with no rostered paper (ids frozen; the driver must dispatch nothing for them): [31, 32, 33, 34, 35]
