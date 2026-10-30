# OCONUS beta mainstems: plan

Add the first reference mainstems for Alaska, Hawaii, Puerto Rico and the
U.S. Virgin Islands, and Guam and the CNMI. Each new `ref/mainstems` identifier
is defined as exactly the flowline set of one `usgs/mainstems` identifier as
minted in the 2024 data release (ScienceBase 650cba99, doi:10.5066/P94H0DAG),
against the final NHD snapshot that release was built from. Nothing in the
batch comes from 3DHP production content: not the FY26 annual product, not the
3DHP service, not the EDH work units. That keeps the usgs to ref relation an
exact sameAs for every row in the batch, which is what 3dhp_mainstem_ids
records.

"Beta" describes the selection rule, not the identifiers. Once a row is in
`registry/ref_rivers.csv` it is permanent and can only be superseded. The rule
can change for the next batch; these rows cannot be taken back.

## Basis

| Input | Source | Gives |
|---|---|---|
| `nhd_3dhp_oconus_mainstems.csv` | 2024 release | flowline `permanent_identifier` to usgs mainstem: membership |
| `nhd_3dhp_oconus_flownetwork.csv` | 2024 release | `upmain`, `downmain` edges: head, outlet, downstream mainstem |
| `nhd_final_registry.csv` | 2024 release | `permanent_identifier` to `id3dhp`, `lengthkm`, `ftype`, `fdate` (latest 2023-09-06) |
| `NHD_H_{hu4}_HU4_GDB.zip`, HU4s 1901 to 1909, 2001 to 2009, 2101, 2102, 2201 to 2204 | prd-tnm staged products, files dated 2023-12-07 to 2024-01-08 | `gnis_id`, `gnis_name` and geometry of `NHDFlowline` |

The HU4 geodatabases are the final NHD. Step 1 proves they are the same
snapshot the registry describes by requiring every member flowline of every
candidate to be present in them by `permanent_identifier`. Alaska is about
2.9 GB of download; the islands are under 50 MB.

The selection rule stays as settled in
`3dhp_mainstem_ids/dev/oconus_named_mainstems` (floors Alaska 50 km, Hawaii
10, Puerto Rico and USVI 10, Guam and CNMI 4, more than 90% named; Alaska
over 100 km and more than 50% named; anything a selected mainstem flows to).
It is recomputed on NHD names and lengths so that the batch does not depend
on 3DHP names either.

## What changes against the 3DHP-based selection

The 3DHP-based run selected 1,262. Known differences once the basis moves:

- Four of the 1,262 are not in the OCONUS network minting. They are in the
  non-network minting file: three Hawaiian ditches and Canal Moca in Puerto
  Rico. They leave the batch. Reference mainstems are network flowpaths.
- Transboundary Yukon basin rivers are longer in the 2024 release, which
  carries the Canadian NHD reaches the FY26 Alaska product does not: Yukon
  River 2,515 km against 2,114, Porcupine River 907 against 446. Firth,
  Beaver Creek, Kandik, Ladue, Chilkat, the Black River forks and others
  change the same way. Their percent named can move either way.
- The eight rivers touched by EDH work unit 300338 (Koyukuk, Noatak, Alatna,
  John, South Fork Koyukuk, the Henshaw Creek forks, Henry Creek) are ordinary
  NHD mainstems in the 2024 release. The hold-back in the earlier draft of this
  plan is gone.
- Every one of the 164,072 member flowlines of the 1,258 network candidates
  is in `nhd_final_registry`, so lengths are available for all of them.

Step 1 reports the actual difference, mainstem by mainstem.

## What ref_rivers has today, and what this adds

- No mainstem in `out/mainstems.gpkg` lies outside CONUS and its transboundary
  basins. Every active row carries an NHDPlusV2 or NHDPlusHR head and outlet
  pair; `validate_mainstems()` requires one of the two.
- `head_nhd_permid` and `outlet_nhd_permid` already exist in the schema as
  "the link to 3DHP" and are filled for 850,233 CONUS rows. They are the key
  for OCONUS rows, which have no NHDPlusV2 and no NHDPlusHR representation in
  the reference network.
- Integers in `ref/mainstems` run 1 to 2,667,748. The usgs integers of the
  candidates run 1,000,706 to 9,999,545 and 63 of them already exist as ref
  ids, so the usgs integer cannot be reused as the ref integer.
- `assign_new_ms_ids()` is a stub that stops if any id is missing. v3 ids came
  from v3 level paths; OCONUS has no level path.

## Decisions

1. **Id space.** New ids are a contiguous block starting at 3,000,001, assigned
   in a fixed order (domain, then usgs integer) and written once to a tracked
   table `registry/oconus_beta_ids.csv` with columns `mainstem` (ref integer),
   `usgs_mainstem` (usgs URI), `domain`, `reason`. The pipeline reads the table
   rather than regenerating it, so a rebuild cannot renumber. Nothing in the
   integer is meaningful. Alternative not taken: filling gaps below 2,667,748
   as the stub suggests; a visible block makes the batch easy to audit.
2. **Provider.** New row in `registry/providers.csv`: id 5,
   https://doi.org/10.5066/P94H0DAG, the release that defines both the usgs
   mainstems and the permanent identifiers used as `head` and `out`.
3. **Head and outlet.** NHD `permanent_identifier` of the head and outlet
   flowline from the 2024 flow network: outlet is the member flowline whose
   `downmain` edge leaves the mainstem or has none; head is the member
   flowline with no `upmain` edge arriving from inside the mainstem. Exactly
   one of each per mainstem, or the mainstem is held back and listed.
4. **Attributes.** `primary_name` and `primary_name_gnis_id` from the GNIS name
   covering the most member length; `name_at_outlet` from the outlet flowline;
   `lengthkm` as the sum of registry `lengthkm`; `downstream_mainstem_id` from
   the `downmain` relation mapped through the id table (every candidate's
   downstream mainstem is in the batch, by the closure clause);
   `encompassing_mainstem_basins` as the downstream chain.
   `outlet_drainagearea_sqkm` NA and the HUC12, RF1, NHDPlusV1, NHDPlusV2 and
   NHDPlusHR pairs empty, all of which the validator allows. Geometry: member
   flowlines from the HU4 geodatabases ordered head to outlet along `downmain`,
   collapsed with `collapse_lines()`, EPSG:4326.
5. **Integration point.** OCONUS rows are built as their own target in the
   published schema and bound to `mainstems` after `make_mainstems()`, before
   `validate`, `write_lookups`, `make_nonref` and `registry`. The CONUS chain
   (`initialize_mainstems()` and its level path checks) is not touched.
6. **Version.** v3.4, a minor release: additions and a new provider, no change
   to existing rows.

## Steps

Each step is one review. Success is stated before the step runs.

### Step 1. Recompute the selection on the NHD snapshot

In 3dhp_mainstem_ids, `dev/oconus_named_mainstems/nhd_snapshot_selection.R`:
download the 23 HU4 geodatabases with `hydrogeofetch::download_nhd()` into
the shared `data/` cache; read `NHDFlowline` attributes (`permanent_identifier`,
`gnis_id`, `gnis_name`); join membership, lengths and names; apply the rule;
write `mainstems_oconus_nhd.csv` and a comparison table against the 3DHP-based
`named_mainstems_oconus.csv` (in both, only in one, with the reason).

Success: every member flowline of every candidate is found in a geodatabase
by `permanent_identifier`; the comparison shows the four non-network features
gone and otherwise only transboundary and name-coverage movements that can be
explained row by row. Download about 3 GB, run under an hour, no keyboard
needed once started.

### Step 2. Export the batch

`dev/oconus_named_mainstems/export_beta_set.R`: head and outlet permanent
identifiers (decision 3), collapsed geometry, names, length, downstream usgs
id, domain, reason, for the selected set. Writes `oconus_beta_mainstems.gpkg`
(layer `mainstems`, one row per usgs mainstem) and `oconus_beta_flowlines.csv`
(`usgs_mainstem`, `permanent_identifier`, `id3dhp`) for the lookup, plus
`held_back.csv` for any mainstem without exactly one head and one outlet.

Success: one LINESTRING per mainstem with geometry length within 2% of the
registry length sum; held-back list empty or explained. Minutes.

### Step 3. Bring the batch into ref_rivers

- Copy the gpkg and flowline csv to `data/review/oconus_beta_2026/` and
  force-add them, as the other review artifacts are. Expected size under
  10 MB; if larger, simplify to the display tolerance v3 geometry has.
- `registry/providers.csv`: row 5 (decision 2).
- `registry/oconus_beta_ids.csv`: generated once by a script in `data/review/`,
  then tracked (decision 1).
- `R/oconus.R`: `make_oconus_mainstems(gpkg, id_table)` returns rows in the
  published schema with `featuretype`, `superseded = FALSE`, `new_mainstemid = ""`.
- `_targets.R`: targets `oconus_beta_file`, `oconus_ids_file`,
  `oconus_mainstems`; `mainstems` becomes
  `bind_rows(make_mainstems(raw_mainstems), oconus_mainstems)`.
- `validate_mainstems()`: the "at least one head/outlet ID pair" check accepts
  the NHD permid pair as a third option. No other check changes.
- `build_registry()`: provider 5 when both NHDPlusV2 and NHDPlusHR heads are
  empty and the NHD permid head is not; head and out are the permids.
- `write_lookups()`: rows with no NHDPlusV2 and no NHDPlusHR representation are
  filtered out of `hr_in` before the `lp_mainstem_v3` assertion and out of the
  final "every mainstem resolves" check. New `out/nhd_permid_lookup.csv`
  (`uri`, `permanent_identifier`, `id3dhp`) from the step 2 flowline csv.
- `make_nonref()`: confirm it ignores the new rows (it keys on NHDPlusV2 and HR
  ids); no change expected.

Success: `tar_make()` completes; `validate` passes;
`git diff registry/ref_rivers.csv` shows only appended rows, as many as the
batch, all provider 5, no duplicate head or out; `out/mainstems.gpkg` grows by
the same number and every pre-existing `uri` keeps its attributes byte for
byte against the v3.2 release gpkg. A full pipeline run is hours (the HR
lookup dominates); the new target and validation alone are minutes.

### Step 4. Release notes, docs, downstream registries

- `NEWS.md`: v3.4 entry with the basis, the rule, per-domain counts, the
  transboundary note, provider 5, the id block.
- `docs/concepts/persistence.md` version table; `docs/reference/products.md`
  provider and lookup tables.
- 3dhp_mainstem_ids: `mainstem_crosswalk` and `mainstem_registry` gain exact
  usgs to ref sameAs rows for the batch on the next run against v3.4; its SPEC
  note that OCONUS is "registry only" is updated.
- reference.geoconnex.us and the geoconnex redirects pick the rows up from the
  published gpkg; not part of this plan.

## What breaks if this is wrong

- A wrong head or outlet permid mints a permanent identifier for the wrong
  path. Step 2's one-head-one-outlet check and the per-domain maps are the
  review.
- A geodatabase that is not the registry's snapshot would give geometry or
  names for a different flowline set. Step 1's membership check catches a
  missing flowline; it does not catch a flowline whose geometry was edited
  after 2023-09-06 while keeping its identifier. Those are rare in the frozen
  NHD and affect display geometry only, never the identifier.
- An id block that overlaps a future CONUS addition. The block is tracked; the
  v3 pipeline never assigns ids above 2,667,748 on its own, and
  `build_registry()` stops on a duplicate mainstem id.
- The validator relaxation in step 3 is the one change to CONUS code paths. It
  widens an `OR`; it cannot let a CONUS row through that failed before.

## Out of scope

Supersession of any of these later; American Samoa; usgs mainstems minted or
re-shaped by EDH after the 2024 release; `head_id3dhp` columns.
