# Reference Rivers

Reference mainstems give every river in the United States a stable identifier that persists across the datasets used to represent it. A monitoring site, model output, a water-related permit, or a photograph of a river can be referenced to a mainstem once, and that reference will remain valid even when the underlying hydrography is replaced. A mainstem identifier is for a river, not a particular set of flowlines.

The mainstem registry currently holds about 850,000 mainstem identifiers covering the conterminous United States and the parts of Canada and Mexico needed to cover transboundary networks. Each identifier is defined by a headwater location, an outlet location, and a network dataset in which the path between them can be traced.

## Start here

- **Find:** If you have a location and want its mainstem, see [Find the mainstem for a place](using/find.md). 
- **Link:** If you maintain a dataset and want to reference it to rivers, see [Link your data to a mainstem](using/link.md). 
- **Discover:** If you already have mainstem identifiers and want geometry, network paths, or other people's data on the same river, see [Work with data linked to mainstems](using/linked-data.md).
- **Understand:** For the underlying model — what a mainstem is and why the identifier is separated from the geometry — start with [What a mainstem is](concepts/mainstems.md).

## What this manual covers

Reference mainstems is one collection among several published as geoconnex reference features. The identifier policy shared by all of them — an identifier is unique, permanent, never reused, never removed — and the resolution architecture behind it are documented at [Reference Features](https://docs.geoconnex.us/reference/reference_features). This manual covers only what is specific to mainstems: how decisions are made to add an identifier to the collection, how representations may br improved over time, how supersession of mainstems found to be in error is handled, what published data tables contain, and how to work with mainstems specifically rather than reference features in general.

This manyal relies on documentation that exists elsewhere and points there rather than restating them: the [NHDPlus](https://www.epa.gov/waterdata/get-nhdplus-national-hydrography-dataset-plus-data) and [3D Hydrography Program](https://www.usgs.gov/3d-hydrography-program) datasets, the [geoconnex](https://docs.geoconnex.us/) system, the [NLDI](https://api.water.usgs.gov/nldi/swagger-ui/index.html) navigation service, and the [hydrogeofetch](https://doi-usgs.github.io/nhdplusTools/) and [hydroloom](https://doi-usgs.github.io/hydroloom/) R packages.

## More Information:

> David L Blodgett, 2025, Mainstem Rivers of the Conterminous United States (Version 3.0): U.S. Geological Survey data release, <https://doi.org/10.5066/P13LNDDQ>.

The logical model is documented in OGC Engineering Report 25-045, [Logical data model for hydrographic data based on HY_Features concepts](https://docs.ogc.org/per/25-045.html). The mainstem and drainage basin feature types are described in:

> Blodgett, D., Johnson, J.M., Sondheim, M., Wieczorek, M., and Frazier, N., 2021, Mainstems: A logical data model implementing mainstem and drainage basin feature types based on WaterML2 Part 3: HY Features concepts: Environmental Modelling & Software, v. 135, p. 104927, <https://doi.org/10.1016/j.envsoft.2020.104927>.

!!! warning "Provisional"
    This information is preliminary or provisional and is subject to revision. It has not received final approval by the U.S. Geological Survey.
