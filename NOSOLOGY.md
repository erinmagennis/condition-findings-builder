# NOSOLOGY — how the EDS corpus handles changing classifications (2026-10-21)

`condition` and `eds_type` follow the **2017 International Classification of the Ehlers-Danlos Syndromes** (Malfait et al. 2017: 13 types; hypermobility spectrum disorders per Castori et al. 2017). The literature in Tier 2 (1946–2018) was written under three earlier systems — Berlin 1988 (numbered types I–XI), Villefranche 1997 (six descriptive types), and the pre-1988 eponymous literature. Three consequences:

1. **Type numerals in quotes are preserved verbatim**; the extractor maps them to current names in `eds_type` where the paper allows. Map:

| historical type | historical name | 2017 equivalent |
|---|---|---|
| I | gravis | classical EDS (cEDS) |
| II | mitis | classical EDS (cEDS) |
| III | hypermobile / benign hypermobility | hypermobile EDS (hEDS) or HSD — the 2017 criteria are stricter; a pre-2017 'type III' cohort is not equivalent to a 2017 hEDS cohort |
| IV | arterial/ecchymotic | vascular EDS (vEDS) |
| V | X-linked | not retained (unconfirmed) |
| VI | ocular-scoliotic | kyphoscoliotic EDS (kEDS-PLOD1); VIB → musculocontractural EDS (mcEDS-CHST14) |
| VIIA/VIIB | arthrochalasis multiplex congenita | arthrochalasia EDS (aEDS) |
| VIIC | dermatosparaxis | dermatosparaxis EDS (dEDS) |
| VIII | periodontal | periodontal EDS (pEDS; C1R/C1S) |
| IX | occipital horn syndrome / X-linked cutis laxa | REMOVED (1997): occipital horn syndrome, ATP7A copper transport — scope_class eds_historical_reclassified |
| X | fibronectin-deficient | REMOVED (1997); unconfirmed entity |
| XI | familial joint instability | REMOVED (1997); absorbed into joint hypermobility syndrome / HSD |

2. **hEDS is not stable across eras.** A pre-2017 'EDS type III' / 'EDS hypermobility type' / 'joint hypermobility syndrome' cohort is labelled per the written bucket definition (hEDS if the paper labels it EDS-hypermobility type; hypermobility_unspecified if JHS/BJHS without an EDS label). Readers wanting 2017-criteria cohorts must filter on year ≥ 2017; 693 of 1,055 Tier-2 hEDS rows are pre-2017.

3. **Papers PubMed indexes under EDS that the 2017 classification excludes** are extracted in the *nosology supplement* package with a `scope_class` on every row:

| scope_class | meaning | supplement papers |
|---|---|---:|
| `eds_2017` | studies a 2017-recognised type or HSD (screen false negatives, incl. veterinary EDS-like disorders framed as EDS) | 5 |
| `eds_historical_reclassified` | an entity the older literature called EDS, since removed — chiefly occipital horn syndrome (EDS type IX, ATP7A) | 33 |
| `adjacent_hctd` | a different heritable connective-tissue or related disorder (Loeys-Dietz, Marfan, cutis laxa, osteogenesis imperfecta, equine HERDA / Warmblood fragile foal syndrome) with EDS as differential, comparison or mechanistic neighbour | 15 |
| `mention_only` | EDS appears only incidentally; the paper studies something else — NOT extracted; listed with span in `scope/tier2/mesh_indexed_mention_only_95.csv` | 95 |

Classification: model-assisted (claude-sonnet-5) with a verbatim justification span per paper, over the 148 MeSH-indexed papers the two-pass population screen had excluded (`qa/tier2/eds_recall_vs_mesh.json`). The spans ship in the roster; a human read of the 148 is part of the exclusion audit. Rows in the main packages (eds-findings-v0, eds-findings-tier2-v0) are implicitly `scope_class = eds_2017`; the column is added to those packages at their next version.

Wording for READMEs: *"`condition` follows the 2017 International Classification of the Ehlers-Danlos Syndromes. Papers written under earlier classifications, and papers on adjacent heritable connective-tissue disorders that PubMed indexes under EDS, are included with a `scope_class` label; historical type numerals are mapped to current names in NOSOLOGY.md and preserved verbatim in quotes."*
