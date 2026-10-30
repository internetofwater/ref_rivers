# OCONUS batch from source

The v3.4 OCONUS mainstems are built today from
`data/review/oconus_beta_2026/oconus_beta_mainstems.gpkg`, which two
untracked scripts (`nhd_snapshot_selection.R`, `export_beta_set.R`) wrote
from NHD HU4 downloads that are not pinned or cited. A fresh clone cannot
build `oconus_mainstems`. This plan moves both scripts into the pipeline so
that every input is fetched, SHA-256 pinned and cited, and nothing beta-only
is tracked.

**Done when:** a `tar_make()` that reads no file under
`data/review/oconus_beta_2026/` rebuilds `oconus_mainstems` identical in
attributes and geometry to the current cached target, `validate` passes, and
`registry` appends 0 rows.

## Inputs

| Input | Source | Pin |
| --- | --- | --- |
| NHD HU4 1901-1902, 1905-1909, 2001-2009, 2101-2102, 2201-2204 | `prd-tnm` staged products, FileGDB zips | SHA-256 per zip |
| NHD HU4 1903 | same, GeoPackage zip | SHA-256 |
| `nhd_3dhp_oconus_mainstems`, `nhd_final_registry`, `nhd_3dhp_oconus_flownetwork`, `nhd_3dhp_nonnetwork_mainstems` | ScienceBase 650cba99, doi:10.5066/P94H0DAG | SHA-256 (already known) |
| `registry/oconus_beta_ids.csv` | tracked | file target |

The 1903 FileGDB as published holds 68,180 of its ~645k flowlines; its
GeoPackage edition is complete. The format per HU4 is fixed in the pin table
instead of being detected at run time. NHD has been frozen since these
products were published (Dec 2023 to Jan 2024); S3 dates match the metadata
`pubdate`. Zips are kept on disk so the hash can be checked; hydrogeofetch's
`download_nhd()` deletes them, so it is not used.

Citation, in `get_nhd_hu4()` and in `docs/reference/products.md` and NEWS:

> U.S. Geological Survey, 2023-2024, USGS National Hydrography Dataset Best
> Resolution (NHD) for Hydrological Unit (HU) 4, FileGDB and GeoPackage
> staged products: <https://www.usgs.gov/national-hydrography>, accessed
> from <https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHD/HU4/>.

## Steps

1. **Fetcher.** `get_nhd_hu4()` in `R/get_data.R`: downloads the 23 zips to
   `data/nhd_hu4/zip/`, checks SHA-256, unzips, returns the 23 source paths.
   `get_nhd_3dhp_release()` gains the flow network and non-network hashes.
   Check: hashes recorded from a fresh download; the function returns 23
   paths that exist. About 3.6 GB.
2. **Functions and targets.** Split the two scripts into functions in
   `R/oconus.R`:
   - `read_nhd_flowlines(sources)`: name, GNIS id, HU4 per permanent id.
   - `select_oconus_mainstems(flowlines, membership, registry, network, nonnetwork)`:
     the selection rule, unchanged, including the non-network exclusion and
     downstream closure. Stops if a selected member is missing.
   - `build_oconus_batch(selection, ..., sources)`: head, outlet, order,
     names, geometry, 50 m simplification per domain (what the current
     output used), held-back check. Returns sf; stops if anything is held
     back, because the id table must cover the batch exactly.
   - `make_oconus_mainstems()` takes the sf instead of a gpkg path.
   The 3DHP-selection comparison is a one-off diagnostic and does not move.
   Targets: `nhd_hu4`, `nhd_flowlines`, `oconus_network`, `oconus_nonnetwork`,
   `oconus_selection`, `oconus_batch`; `oconus_beta_file` goes away.
3. **Check.** Save the current `oconus_mainstems` to `temp/`, rebuild, compare.
   Expect identical; any difference is investigated, not accepted. Then
   `validate`, `registry` (0 rows), `tar_outdated()`.
4. **Docs.** NHD citation in `products.md` and the v3.4 NEWS entry; NEWS
   wording that the batch is built in the pipeline.

## Left as is

- `data/review/make_oconus_beta_ids.R` read the beta gpkg to mint the id
  table once. The table is tracked and never regenerated; the script stays
  untracked.
- `data/review/oconus_beta_2026/` and `temp/oconus_beta/` stay untracked and
  can be deleted after step 3 passes.
