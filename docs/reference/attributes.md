# Mainstem attributes

These are the columns of `mainstems.gpkg` and the properties served by `reference.geoconnex.us`. Property names are lowercased in the served GeoJSON.

!!! note "Two collections are currently served"
    `https://geoconnex.us/ref/mainstems/{id}` resolves to the `mainstems` collection, which serves the v2-era subset of this schema — no `primary_name`, `head_nhdplushr_id`, or `head_nhd_permid`. The full v3 schema below is served by the `mainstems_v3` collection and is what the data release and `mainstems.gpkg` carry. The `datasets` array is attached to `mainstems`, because that is what the crawler sees resolved identifiers land on.

## Identity

| Attribute | Description |
| --- | --- |
| `id` | Integer mainstem identifier |
| `uri` | `https://geoconnex.us/ref/mainstems/{id}` — the identifier to store and share |
| `featuretype` | HY_Features types: `HY_FlowPath` and `HY_WaterBody` — see note below |
| `superseded` | Logical. `TRUE` means the mainstem is retained but no longer maintained |
| `new_mainstemid` | For superseded mainstems, the URI or URIs that replace it. Empty otherwise |

`featuretype` is served as a literal list of type URIs:

```
['https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath', 'https://www.opengis.net/def/schema/hy_features/hyf/HY_WaterBody']
```

The dual typing is not a hedge. A mainstem is composed of flowlines, and a flowline satisfies the waterbody-flowpath constraint of the hydrographic network — it is at once a flow path and a linear representation of a flowing body of water. Note that a polygonal waterbody is a separate feature type in the logical model and is not what this attribute refers to.

## Topology

| Attribute | Description |
| --- | --- |
| `downstream_mainstem_id` | URI of the mainstem this one flows into. Empty at terminal outlets |
| `encompassing_mainstem_basins` | List of URIs whose drainage basins contain this mainstem |

## Names

| Attribute | Description |
| --- | --- |
| `name_at_outlet` | GNIS name at the outlet |
| `name_at_outlet_gnis_id` | GNIS identifier for that name, as a geoconnex URI |
| `primary_name` | The name appearing on the greatest share of the mainstem's length |
| `primary_name_gnis_id` | GNIS identifier for the primary name, as a geoconnex URI |

Names are not unique and are not identifiers. Where `name_at_outlet` and `primary_name` disagree, the river changes name along its length.

## Measures

| Attribute | Description |
| --- | --- |
| `lengthkm` | Length of the mainstem path in kilometers |
| `outlet_drainagearea_sqkm` | Total drainage area at the outlet in square kilometers |

## Head and outlet references

Each mainstem carries head and outlet identifiers in every identifier system it has been referenced to. A blank value means the mainstem has no representation in that system.

| Attribute pair | System |
| --- | --- |
| `head_nhdpv2_COMID`, `outlet_nhdpv2_COMID` | NHDPlusV2 COMID, as geoconnex URIs |
| `head_nhdplushr_id`, `outlet_nhdplushr_id` | NHDPlusHR identifiers |
| `head_nhd_permid`, `outlet_nhd_permid` | NHD permanent identifiers, the link to 3DHP |
| `head_nhdpv2HUC12`, `outlet_nhdpv2HUC12` | NHDPlusV2-era HUC12, as geoconnex URIs |
| `head_2020HUC12`, `outlet_2020HUC12` | 2020 WBD HUC12 codes |
| `head_nhdpv1_COMID`, `outlet_nhdpv1_COMID` | NHDPlusV1 COMID |
| `head_rf1ID`, `outlet_rf1ID` | RF1 reach identifiers |

Accumulating these rather than replacing them is what lets data referenced against an older hydrography keep resolving — the crosswalk to the current representation is carried on the mainstem itself.

## Validation guarantees

The build enforces these invariants before writing output, so consumers can rely on them:

- `id` and `uri` are unique and non-null, and every `uri` matches `^https://geoconnex\.us/ref/mainstems/\d+$`.
- Superseded rows retain a non-empty `id` and `uri`, and carry an empty `downstream_mainstem_id`.
- Every active mainstem has a complete head and outlet pair in NHDPlusV2 or in NHDPlusHR, and no two active mainstems share a `head_nhdpv2_COMID`.
- Every non-empty `new_mainstemid`, `downstream_mainstem_id`, and `encompassing_mainstem_basins` entry references an active mainstem.
- Every URI-valued reference attribute matches its namespace pattern or is empty.
- `lengthkm` is greater than 0 and less than 5,000; `outlet_drainagearea_sqkm` is null or within [0, 3,000,000).
- Geometry is a single LINESTRING per mainstem.
- In the crosswalks, each source identifier maps to exactly one mainstem.
