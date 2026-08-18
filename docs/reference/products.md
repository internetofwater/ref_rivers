# Data products

Each release produces a registry tracked in source control, a spatial dataset, and crosswalk tables. The registry is the persistence anchor; everything else is derived and can be rebuilt.

## The registry

`registry/ref_rivers.csv` holds one row per mainstem — the identifier and the head and outlet identifiers that define it, plus a provider code for the dataset those identifiers are defined in.

| Column | Meaning |
| --- | --- |
| `mainstem` | Integer mainstem identifier; the URI is `https://geoconnex.us/ref/mainstems/{mainstem}` |
| `head` | Headwater catchment identifier in the provider's namespace |
| `out` | Outlet catchment identifier in the provider's namespace |
| `provider` | Foreign key to `registry/providers.csv` |

`registry/providers.csv` resolves the provider code to the data release that defines the identifiers:

| id | Provider |
| --- | --- |
| 1 | [Updated CONUS river network attributes (E2NHDPlusV2, NWMv2.1)](https://doi.org/10.5066/P9W79I7Q) |
| 2 | [Same, version 2.0](https://doi.org/10.5066/P976XCVT) |
| 3 | [Updated CONUS river network attributes and geometry, version 3.0](https://doi.org/10.5066/P13IRYTB) |
| 4 | [NHDPlus High Resolution National Release 2](https://doi.org/10.5066/P13V7GVY) |

The registry only grows. New rows are appended for new mainstems and existing rows are left alone; if the mainstem set comes out shorter than the registry already on disk, the build stops rather than writing.

## Spatial data

`out/mainstems.gpkg` carries one feature per mainstem with the attributes documented in [Mainstem attributes](attributes.md). The published equivalent is `mainstem_summary.gpkg` in the data release:

> David L Blodgett, 2025, Mainstem Rivers of the Conterminous United States (Version 3.0): U.S. Geological Survey data release, <https://doi.org/10.5066/P13LNDDQ>.

!!! warning "Geometry is for visualization"
    The mainstem geometry in this file is highly simplified and is intended for display, not measurement. Any analysis should use source dataset geometry, reached through the crosswalks below or through NLDI navigation.

`out/extra_mainstems.gpkg` holds candidate mainstems present in the source network that are not part of the reference registry — the conservative omissions described in [Persistence and change](../concepts/persistence.md). These have no persistent identifiers and should not be referenced.

## Crosswalks

| File | Columns | Maps |
| --- | --- | --- |
| `out/nhdpv2_lookup.csv` | `uri`, `comid` | NHDPlusV2 COMID to mainstem URI |
| `out/nhdphr_lookup.csv` | `uri`, `nhdplushr_permid`, `nhdplushr_id` | NHDPlusHR permanent identifier and integer identifier to mainstem URI |
| `out/lpv3_lookup.csv` | `uri`, `lp_mainstem_v3` | Mainstem URI to v3 source level path |

Both crosswalks cover active mainstems only and assign each flowline to exactly one mainstem. In the HR crosswalk, assignment runs in stream-level order so that where paths overlap the larger river takes the flowline; a small number of mainstems whose HR footprint falls entirely inside a higher-precedence mainstem get no exclusive HR flowline and resolve through the v2 crosswalk instead.

## Review outputs

`data/review/` carries files for people checking a release rather than consuming it: `deprecated_v3.geojson` and `deprecated_lookup.csv` for superseded mainstems and their replacements, `changelog_v3.csv` for mainstems whose headwater or outlet moved more than 10 km, and `missing_reference_mainstems.csv`.
