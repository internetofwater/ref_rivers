# Find the mainstem for a place

Which route you take depends on what you are starting from — coordinates, an existing hydrography identifier, a river name, or a monitoring site.

## From coordinates

The NLDI resolves a point to the NHDPlusV2 catchment containing it, and the reference service resolves a bounding box to mainstems. In R, `nhdplusTools` wraps both:

```r
library(nhdplusTools)
library(sf)

pt <- st_sfc(st_point(c(-89.38, 43.07)), crs = 4326)

# nearest flowline and its COMID
fl <- get_flowline_index(get_nhdplus(AOI = st_buffer(pt, 0.05)), pt)

# the mainstem the flowline belongs to
get_nldi_feature(list(featureSource = "comid", featureID = fl$COMID))
```

Directly against the reference service, a small bounding box returns candidate mainstems with their names and drainage areas:

```bash
curl "https://reference.geoconnex.us/collections/mainstems/items?bbox=-89.45,43.02,-89.30,43.12&f=json"
```

## From a COMID or other hydrography identifier

Use the crosswalk tables published with each release. `nhdpv2_lookup.csv` maps every NHDPlusV2 COMID in an active mainstem to that mainstem's URI, and `nhdphr_lookup.csv` does the same for NHDPlusHR identifiers. These are the authoritative crosswalks — a join against them is exact, where a spatial match is not.

```r
lookup <- readr::read_csv("nhdpv2_lookup.csv")
dplyr::filter(lookup, comid == 13293750)
```

A single flowline belongs to exactly one mainstem, including at divergences — the primary downstream path continues the mainstem and the diverted path belongs to another. Where mainstem paths overlap in the NHDPlusHR crosswalk, the larger river claims the flowline, assigned in stream-level order.

## From a name

`name_at_outlet` carries the GNIS name at the mainstem's outlet and `primary_name` the name most common along its length. Names are not unique — there are many Mill Creeks — so filter by name and then disambiguate by location or drainage area:

```bash
curl "https://reference.geoconnex.us/collections/mainstems/items?name_at_outlet=Kickapoo%20River&f=json"
```

## From a monitoring site

Many monitoring networks already publish mainstem references through geoconnex. If your site has a geoconnex URI, request it and read the mainstem link from the response rather than doing the spatial work yourself. See [Work with data linked to mainstems](linked-data.md).
