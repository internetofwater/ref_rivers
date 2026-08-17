# Where mainstems are used

Mainstem identifiers are only worth adopting if other people's data carries them. This is where they appear.

## geoconnex

[geoconnex](https://docs.geoconnex.us/) is the identifier and redirect system that mainstem URIs live in, and `reference.geoconnex.us` is the community catalog that serves reference mainstem descriptions. The geoconnex crawler harvests published linked data and attaches monitoring locations and datasets to the mainstems they reference — see [Work with data linked to mainstems](../using/linked-data.md). Water Quality Portal sites are the largest contributor by count.

Mainstems is one of several reference collections geoconnex publishes; the identifier policy and web architecture common to all of them are documented in [Reference Features](https://docs.geoconnex.us/reference/reference_features). The `dams` and `gages` collections carry `mainstem_uri`, which makes them directly queryable by river.

## 3D Hydrography Program

Mainstem identifiers provide persistence for [3DHP](https://www.usgs.gov/3d-hydrography-program). 3DHP replaces the NHD with hydrography derived primarily from elevation data, and its data model assigns a persistent mainstem identifier to every on-network feature — densification, correction, and general evolution of the representation proceed without disturbing the identifiers or the links made against them. The `head_nhd_permid` and `outlet_nhd_permid` attributes carry the connection from this registry.

## NLDI

The [Network Linked Data Index](https://api.water.usgs.gov/nldi/swagger-ui/index.html) navigates the NHDPlusV2 network and returns features referenced to it. Upstream-mainstem (`UM`) navigation from a mainstem's outlet returns the flowlines composing that mainstem, and the same call can return monitoring sites, basins, and other registered feature sources along the way.

## nhdplusTools and hydroloom

[nhdplusTools](https://doi-usgs.github.io/nhdplusTools/) provides access to NHDPlus data, NLDI navigation, and network indexing in R; [hydroloom](https://doi-usgs.github.io/hydroloom/) provides the underlying network manipulation. Both are the practical route from a mainstem identifier to work on the network it names.

## Hydrofabric

A hydrologic geospatial fabric combines a network of mainstems composed of flowlines, geospatial representations of those flowlines, catchment areas draining to them, and a library of points of interest linked to the network. Mainstem identifiers are the first of those four components, and they carry through catchment aggregation, so model outputs on an aggregated modeling unit can be referenced to rivers without a separate crosswalk.

A reference flow network is the most resolved and validated network available, the one other datasets are related to. Persistent mainstem identifiers are what let it serve that role over time: smaller rivers and basins can be added, and representations improved, without minting or retiring identifiers for what was already there.

Background is in [Progress Toward a Reference Hydrologic Geospatial Fabric for the United States](https://waterdata.usgs.gov/blog/hydrofabric/) and:

> Blodgett, D., Johnson, J.M., and Bock, A., 2023, Generating a reference flow network with improved connectivity to support durable data integration and reproducibility in the coterminous US: Environmental Modelling & Software, v. 165, p. 105726, <https://doi.org/10.1016/j.envsoft.2023.105726>.

## The hydrofabric logical data model

The logical model that mainstems sit within — flowline, flowpath, catchment, mainstem, hydrolocation, hydrologic unit, and waterbody feature types, and the flow network relating them — is documented in:

> Blodgett, D., ed., 2026, Logical data model for hydrographic data based on HY_Features concepts: OGC Engineering Report 25-045, <https://docs.ogc.org/per/25-045.html>.

Read it when you need the model behind the registry rather than the registry itself: how mainstems relate to catchments and hydrolocations, why position along a mainstem is not recorded, and how non-dendritic connectivity coexists with a dendritic network of mainstems.
