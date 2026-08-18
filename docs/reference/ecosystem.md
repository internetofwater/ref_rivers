# Where mainstems are used

Mainstem identifiers are worth adopting to the extent other people's data carries them. These are the systems where they appear.

## geoconnex

[geoconnex](https://docs.geoconnex.us/) is the identifier and redirect system that mainstem URIs live in, and `reference.geoconnex.us` is the community catalog that serves reference mainstem descriptions. The geoconnex crawler harvests published linked data and attaches monitoring locations and datasets to the mainstems they reference — see [Work with data linked to mainstems](../using/linked-data.md). Water Quality Portal sites are the largest contributor by count.

Mainstems is one of several reference collections geoconnex publishes; the identifier policy and web architecture common to all of them are documented in [Reference Features](https://docs.geoconnex.us/reference/reference_features). The `dams` and `gages` collections carry `mainstem_uri`, which makes them directly queryable by river.

## 3D Hydrography Program

Mainstem identifiers provide persistence for [3DHP](https://www.usgs.gov/3d-hydrography-program). 3DHP replaces the NHD with hydrography derived primarily from elevation data, and its data model assigns a persistent mainstem identifier to every on-network feature — densification, correction, and general evolution of the representation proceed without disturbing the identifiers or the links made against them. The `head_nhd_permid` and `outlet_nhd_permid` attributes carry the connection from this registry.

## HydroAdd3D

[HydroAdd3D](https://www.usgs.gov/3d-hydrography-program/hydroadd3d) addresses observations — flow and water-quality measurements, intakes, outfalls — to the 3DHP network. Its description of the method used to derive a network address:

> Hydrographic addressing uses a unique identifier (a Mainstem-ID) and either a measure or an elevation value to create a unique location coordinate on the stream network.

That pairing is what this registry's omission of position assumes. The mainstem identifier says which river; the measure or elevation says where along it, and is supplied by the application rather than carried on the reference feature — see [Link your data to a mainstem](../using/link.md).

It takes single requests by URL, batches as JSON, and coordinate tables as CSV, returning the matched network position with a map and a download. Both the [web application](https://apps.usgs.gov/hydroadd3d/) and the [API](https://apps.usgs.gov/hydroadd3d/api/openapi) are public.

## NLDI

The [Network Linked Data Index](https://api.water.usgs.gov/nldi/swagger-ui/index.html) navigates the NHDPlusV2 network and returns features referenced to it. Upstream-mainstem (`UM`) navigation from a mainstem's outlet returns the flowlines composing that mainstem, and the same call can return monitoring sites, basins, and other registered feature sources along the way.

## nhdplusTools, hydroloom, and HyRiver

[nhdplusTools](https://doi-usgs.github.io/nhdplusTools/) (renamed `hydrogeofetch` recently) provides access to NHDPlus data, NLDI navigation, and network indexing in R; [hydroloom](https://doi-usgs.github.io/hydroloom/) provides the underlying network manipulation. Both are the practical route from a mainstem identifier to work on the network it identifies.

[HyRiver](https://docs.hyriver.io/) covers equivalent ground in Python. [PyNHD](https://docs.hyriver.io/readme/pynhd.html) is the package that matters here: its `NLDI` class navigates the same service, `WaterData` and `NHDPlusHR` retrieve mid- and high-resolution flowlines, and `prepare_nhdplus`, `vector_accumulation`, and `topological_sort` do the network work hydroloom does in R.

PyNHD also speaks to the reference collections directly. Its [`GeoConnex`](https://docs.hyriver.io/examples/notebooks/geoconnex.html) class queries `reference.geoconnex.us` three ways — by geometry, by identifier, and by CQL filter — returning a GeoDataFrame:

```python
from pynhd import GeoConnex

gcx = GeoConnex()
gcx.item = "mainstems"
ms = gcx.bygeometry(basin.to_crs(4326).union_all(), predicate="within")
```

## hydro_snap

[hydro_snap](https://code.usgs.gov/wma/nhgf/reference-fabric/hydro_snap) assigns mainstem URIs to site locations and lets an analyst check the result by eye. It takes a table of coordinates — gages, monitoring locations, facilities discharging to streams — snaps each to an NHDPlusV2.1 flowline by point-in-catchment join, and returns `comid`, `gnis_name`, and the mainstem URI for every site. An R Shiny map interface then steps through them so the assignment can be accepted, corrected by clicking a different flowline, or set null where no flowline is correct.

hydro_snap supports the hand review described in [Link your data to a mainstem](../using/link.md)/

Development past the v1.0.0 release indexes each site a second time against 3DHP and puts the two panels side by side, so a site that lands on one mainstem through the NHDPlusV2 crosswalk and a different one through 3DHP is visible. Mainstem lookups were moved to v3.2 of reference mainstems and now run through `hydrogeofetch::add_mainstems()`. Two independent routes to a mainstem, disagreeing on a given site, is the practical form of the accumulation described in [Mainstem attributes](attributes.md).

> Breitmeyer, S., Anderson, S., Conlon, M., and Blodgett, D., 2026, hydro_snap: R Shiny application for quality control and snapping of site locations to USGS National Hydrography Dataset (NHD) v2.1 flowlines: U.S. Geological Survey software release, <https://doi.org/10.5066/P1VQ33G6>.

## Hydrofabric

A hydrologic geospatial fabric combines a network of mainstems composed of flowlines, geospatial representations of those flowlines, catchment areas draining to them, and a library of points of interest linked to the network. Mainstem identifiers are the first of those four components, and they carry through catchment aggregation, so model outputs on an aggregated modeling unit can be referenced to rivers without a separate crosswalk.

Background is in [Hydrofabrics: what are they and how do we identify them?](https://water.usgs.gov/themes/hydrofabric/) and:

> Blodgett, D., Johnson, J.M., and Bock, A., 2023, Generating a reference flow network with improved connectivity to support durable data integration and reproducibility in the coterminous US: Environmental Modelling & Software, v. 165, p. 105726, <https://doi.org/10.1016/j.envsoft.2023.105726>.

## The hydrofabric logical data model

The logical model that mainstems sit within — flowline, flowpath, catchment, mainstem, hydrolocation, hydrologic unit, and waterbody feature types, and the flow network relating them — is documented in:

> Blodgett, D., ed., 2026, Logical data model for hydrographic data based on HY_Features concepts: OGC Engineering Report 25-045, <https://docs.ogc.org/per/25-045.html>.

It covers the model behind the registry rather than the registry itself: how mainstems relate to catchments and hydrolocations, why position along a mainstem is not recorded, and how non-dendritic connectivity coexists with a dendritic network of mainstems.
