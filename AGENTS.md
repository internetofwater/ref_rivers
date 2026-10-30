# Agent context for ref_rivers

This repository builds the reference mainstem registry for geoconnex.us. The
human-facing manual is in `/docs` (served at
<https://internetofwater.github.io/ref_rivers/>); read
`docs/concepts/mainstems.md`, `docs/concepts/persistence.md`,
`docs/reference/attributes.md` and `docs/develop/workflow.md` before changing
pipeline code. This file covers what those pages do not: how the working tree
is laid out, how to run and inspect the pipeline, and the rules that keep the
registry trustworthy.

## The one rule that matters

`registry/ref_rivers.csv` is the registry. Each row is a permanent identifier.
Rows are appended, never edited or deleted. A mainstem that is wrong is marked
`superseded` in the output and given a `new_mainstemid`; its row stays. A
change that would rewrite or drop a registry row, or that would change which
flowlines an existing id refers to, is a supersession and needs human review.
Do not make it quietly. Say so and stop.

`registry/providers.csv` names the identifier systems the registry can
reference (eNHD v1 to v3, NHDPlusHR). Adding a source network means adding a
provider row and is a major version change (see `docs/develop/workflow.md`).

## Layout

| Path | What it is | Tracked |
| --- | --- | --- |
| `_targets.R` | Pipeline definition | yes |
| `R/get_data.R` | `get_*()` fetchers: ScienceBase downloads, SHA-256 pinned | yes |
| `R/v2_v3_compare.R` | `reconcile_mainstems()`: carry v2 ids forward onto v3 candidates | yes |
| `R/make_mainstems.R` | `initialize_mainstems()`, `make_mainstems()`, `validate_mainstems()`, `make_nonref()` and helpers | yes |
| `R/write_output.R` | `write_lookups()`: v2 levelpath join and HR network trace | yes |
| `R/registry.R` | `build_registry()`, `write_registry()` | yes |
| `registry/` | The registry and provider tables | yes |
| `data/review/` | Hand-curated review inputs read by the pipeline (deprecations, outlet fixes, changelog) | yes |
| `docs/`, `mkdocs.yml`, `requirements.txt` | MkDocs manual, deployed by `.github/workflows/docs.yml` | yes |
| `NEWS.md` | Release notes per version | yes |
| `data/` | Downloaded sources. `data/nhdp` is a junction to a local NHDPlusV2 copy | no |
| `_targets/` | targets store. Object cache for every target | no |
| `out/` | Published outputs: `mainstems.gpkg`, `extra_mainstems.gpkg`, lookup CSVs | no |
| `temp/`, `*temp.gpkg`, `cache/` | Scratch | no |
| `*.R` at the root (`diag_*.R`, `test_*.R`) | One-off diagnostics. Run with `Rscript`, read before trusting | varies |

Planning notes live at the root while work is in
progress and describe the next batch of additions. Read them if they exist.

## Pipeline shape

Targets, in dependency order:

1. **Sources**: `mainstems_v1..v3`, `enhd_v1..v3`, `ref_rivers_v21`,
   `ref_rivers_v30`, `ref_net_v1`, `hr_net`, `nhdp_gdb`, `nhdp_geo`. All
   downloads. Several GB on first run.
2. **Reconcile**: `reconciled_mainstems` matches v2 ids to v3 candidates.
3. **Assemble**: `raw_mainstems` is the full candidate set with new additions
   and HR transboundary rivers. `lp_v3_lookup` is written from it.
4. **Publish schema**: `mainstems` (names, lengths, drainage area, downstream
   and encompassing basins, EPSG:4326 LINESTRING), then `mainstem_gpkg`.
5. **Crosswalks**: `lookup` writes `out/nhdpv2_lookup.csv` (uri, comid) and
   `out/nhdphr_lookup.csv` (uri, nhdplushr_permid, nhdplushr_id).
6. **Validate**: `validate` runs the invariants from
   `docs/reference/attributes.md`. It errors, it does not warn.
7. **Non-reference**: `non_ref_mainstems` writes `out/extra_mainstems.gpkg`,
   the candidates that were not promoted to reference mainstems.
8. **Registry**: `registry` appends new rows; `write_reg` writes the CSV.

Review inputs under `data/review/` are read inside these functions by
relative path, not declared as file targets. Editing one does not invalidate
the target that reads it. After changing a review file, invalidate the
consuming target by hand (`tar_invalidate(raw_mainstems)` or similar).

## Running and inspecting

Run from the repo root with a current R. nhdplusTools must be newer than
1.0.1 or `_targets.R` stops.

```r
targets::tar_make()                    # full build; hours on a fresh store
targets::tar_make(names = "validate")  # build through one target
targets::tar_outdated()                # what would rebuild
targets::tar_visnetwork()              # dependency graph
targets::tar_read(mainstems)           # load a cached object for analysis
targets::tar_load(c(raw_mainstems, enhd_v3))
targets::tar_invalidate(lookup)        # force one target to rerun
```

For analysis prefer `tar_read()` over re-reading `out/*.gpkg`. The targets
store is the same data and loads faster. `mainstems` is an sf object of about
850k rows; drop geometry (`sf::st_drop_geometry()`) before joins and
summaries, and use `sf::st_read(query = "SELECT ... FROM ...")` when you only
need a few columns from a geopackage.

Multi-line R goes in a script file, not the terminal. Put scratch scripts
under `temp/` or the agent scratchpad, not the repo root, unless they are
meant to be kept.

Do not run `tar_make()` to test a small change if a cheaper check exists:
`tar_make(names = "...")` on the one target, or `tar_load()` the inputs and
call the function directly. The full build downloads and processes national
data.

## Data vocabulary

- **mainstem**: a head-to-outlet flowpath. The integer id is the identifier;
  geometry and attributes are its current best representation.
- **uri**: `https://geoconnex.us/ref/mainstems/{id}`. Published columns carry
  the URI, the registry carries the bare integer.
- **head / outlet** pairs: one per identifier system (`nhdpv2_COMID`,
  `nhdplushr_id`, `nhd_permid`, `nhdpv2HUC12`). Blank means no representation
  in that system, not missing data.
- **levelpath (lp)**: NHDPlus level path id. `lp_mainstem_v3` is the v3
  candidate id a mainstem was derived from; `out/lpv3_lookup.csv` maps uri to
  it.
- **eNHD**: enhanced NHDPlusV2 attribute tables (the `enhd_*` targets),
  parquet, one per version.
- **hr_net**: full NHDPlusHR flow network used for the HR crosswalk trace.
- **ref_net_v1**: the reference network geopackage mixing NHDPlusV2 and
  NHDPlusHR (`source` column) used to add transboundary mainstems.
- **superseded / new_mainstemid**: retained but replaced, and by what.
- **3DHP / nhd_permid**: NHD permanent identifiers tie mainstems to 3DHP.

Background on these terms, and on HY_Features, NHDPlus, and geoconnex, is in
the manual under `docs/concepts/` and `docs/reference/ecosystem.md`.

## Working conventions

- Use nhdplusTools and hydroloom functions for network navigation and
  attributes before writing your own.
- Minimum change. Touch only what the task needs. Mention unrelated dead code
  rather than removing it.
- Diagnostics written during a task should state what question they answer at
  the top and how to run them, like `diag_refnet_hr_coverage.R`.
- Never put local filesystem paths in tracked files. `data/nhdp` is a junction
  on the maintainer's machine; the pipeline target that creates it is
  `hydrogeofetch::download_nhdplusv2("data/nhdp")`.
- Validation failures mean the data is wrong, not that the check is wrong.
  Investigate the data before loosening a check.
- Docs are checked with `mkdocs build --strict`; a broken link fails CI.
- Commit messages end with `Assisted-by: <agent> <model>`.
- Release process: update `NEWS.md`, build, validate, publish `out/` to a
  ScienceBase data release under a new DOI, commit the registry.

## Where to look when something is off

| Symptom | Start here |
| --- | --- |
| Checksum mismatch on download | `R/get_data.R`; upstream file changed, do not just update the hash |
| Validation error | `validate_mainstems()` in `R/make_mainstems.R`, then the attribute doc |
| Missing HR ids in lookup | `assign_hr_mainstems()` and `lockstep_*()` in `R/write_output.R`; NEWS v3.1, v3.2 |
| v2 id not carried into v3 | `reconcile_mainstems()` and `data/review/deprecated_v3.geojson` |
| Outlet on a diverted path | `fix_hr_outlets()` and `data/review/drop_diverted_outlet.csv` |
| New row in registry unexpected | `build_registry()`; compare `ms$uri` to the registry's `mainstem` column |
