reference mainstems v3
========================

The primary change in reference mainstems v3 is the addition of about 819k mainstem identifiers.
New mainstems derive from "Mainstem Rivers of the Conterminous United 
States (Version 3.0)" data release (https://doi.org/10.5066/P13LNDDQ). Criteria 
used to determine additions required that a headwater and outlet flowline and the path
connecting them could be identified in the NHDPlusV2, NHDPlusHR, and the
snapshot of the NHD that initialized the 3DHP dataset (https://doi.org/10.5066/P94H0DAG). More 
information about these additions can be found below.

33745 of 33900 V2 mainstem ids remain active in V3. The 155 (less than 0.05%) mainstem 
ids being retired remain in the dataset but are marked superseded. All newly superseded 
mainstems are in transboundary basins where NHDPlusHR geometry replaces mainstems 
derived from NHDPlusV2. All mainstems being retired have been manually reviewed, and 
new mainstems that supersede them have been identified and listed in their attributes. 

Superseded mainstems with mainstem ids that replace them can be found in [this file](data/review/deprecated_v3.geojson).

Even though 847k additions could be made, only 819k of the candidates were added to reference mainstems.
The release takes a somewhat conservative approach where downstream 
connectivity is in question. Any mainstem that matched criteria for inclusion but where 
downstream connectivity was in question was omitted. This primarily
affects places where a mainstem flows to a mainstem that did not match the criteria for inclusion and 
would be isolated in the network of reference mainstems if included at this time. Roughly 
28k features fit this criteria. Many will be included in a future release as the overall network 
of mainstems fills out. 

In reference mainstems V3, transboundary basins have been updated significantly. 
NHDPlusHR now serves as the source geospatial representation for reference rivers in HU04s:

- North East: "0101", "0102", "0103", "0104", "0105", "0108",
- Great Lakes: "0401", "0409", "0420", "0427", "0429", "0430", "0431", "0432", "0416", "0433", "0417", "0421", "0422",
- North Central: "0901", "0902", "0903", "0904",
- North West: "1005", "1006",
- South Central: "1303", "1304", "1308", "1309", 
- South West: "1503", "1508", "1505",
- Pacific North West: "1701", "1702", "1703", "1711",
- Pacific South West: "1807", "1810"

**Details of superseded mainstems**

Changes to mainstems with drainage area greater than 500 sqkm include:

Superseded due to flow direction incompatibility:

- https://geoconnex.us/ref/mainstems/1891988
- https://geoconnex.us/ref/mainstems/2290511
- https://geoconnex.us/ref/mainstems/2575658

Superseded due to missing network connectivity:

- https://geoconnex.us/ref/mainstems/403884
- https://geoconnex.us/ref/mainstems/471634
- https://geoconnex.us/ref/mainstems/1873485
- https://geoconnex.us/ref/mainstems/1886819
- https://geoconnex.us/ref/mainstems/1872383
- https://geoconnex.us/ref/mainstems/424107
- https://geoconnex.us/ref/mainstems/1876065
- https://geoconnex.us/ref/mainstems/2571793
- https://geoconnex.us/ref/mainstems/1892314
- https://geoconnex.us/ref/mainstems/1611505

Superseded due to general network incompatibility:

- https://geoconnex.us/ref/mainstems/1875793

**Details of newly introduced mainstems**

In transboundary basins, previous (NHDPlusV2-based) representations of mainstems 
were compared to the new (NHDPlusHR-based) representation. This comparison drew 
primarily from the "Mainstem Rivers of the Conterminous United States 
(Version 3.0)" data release (https://doi.org/10.5066/P13LNDDQ). 

Validation for reference mainstems v3 (this update) compared headwater and outlet 
locations. All changes where a headwater or outlet moved more 
than 10km and all mainstems where both the headwater and outlet moved by more 
than 10% the overall length of the mainstem were reviewed. If the new 
representation was found to be the mainstem of the same overall drainage basin, 
it was kept. Spot checks of mainstem representations with changes less than 
10km or 10% of overall length confirmed that the validation efforts performed 
for https://doi.org/10.5066/P13LNDDQ are appropriate. A list of mainstems with 
headwater or outlet changes greater than 10km are listed in 
"data/review/changelog_v3.csv".

Sixteen mainstems have been added that are in NHDPlusHR but fully in Canada 
and do not connect to the NHDPlusV2 domain. They ensure 
rivers that emanate from the NHDPlusV2 domain connect to a complete network.

Where mainstems were deprecated and marked superseded, new reference mainstems
were introduced. The deprecated mainstems are now marked "superseded: true" and
the "new_mainstemid" attribute has been populated with current mainstems 
(superseded: false) that should be considered as replacements. The majority of these
are cases where the NHDPlusHR does not connect where NHDPlusV2 did connect.

reference mainstems v2
========================

This is a unique update that is being applied in a pragmatic and situationally specific way. Future updates will likely be very different as they will involve inclusion of a new connecting network and addition of additional mainstem identifiers.

The second release of reference mainstems is a minor update in which 

- 33564 will now reference a new reference data release
- 161 mainstems have been superseded because the outlet from a given headwater has moved significantly, 
- 23 mainstem headwaters have been identified to no longer be headwaters and will be superseded,
- 105 have been identified as the same with a slightly different outlet location.

A new version of the v1.0 source data was used for this release. 

> David L Blodgett, 2023, Mainstem Rivers of the Conterminous United States (version 2.0): U.S. Geological Survey data release, https://doi.org/10.5066/P92U7ZUT. 

The reference fabric is defined in:

> David L. Blodgett, 2023, Updated CONUS river network attributes based on the E2NHDPlusV2 and NWMv2.1 networks (version 2.0): U.S. Geological Survey data release, https://doi.org/doi:10.5066/P976XCVT. 

The base geometry for this new reference fabric is still the NHDPlusV2: https://www.epa.gov/waterdata/get-nhdplus-national-hydrography-dataset-plus-data 

Updates to the registry will be applied for all mainstems. Categories of change include:

1. the 33564 will be updated in place to now indicate that the Version 2.0 network at https://doi.org/doi:10.5066/P976XCVT is now the connecting network that should be used for that mainstem.
1. the 161 superseded mainstems will not be altered but new mainstems will be added and the old ones will be linked to their replacement(s) in the mainstem lookup table published via geoconnex.us. 
1. the 23 that are in error had empty geometry and have not been used. They will remain in the registry and no longer appear in the mainstem lookup table.
1. the 105 with new outlets will not be altered but will have a new row in the registry with the new outlet.

reference mainstems v1
========================

The initial release of the mainstems registry was created from v1 of:

> Blodgett, D.L., 2022, Mainstem Rivers of the Conterminous United States: U.S. Geological Survey data release, https://doi.org/10.5066/P9BTKP3T. 

It identified 33853 mainstem identifiers with head and outlet identifiers bound to the NHDPlusV2.1 comid system. The network connecting headwaters to outlets is defined in: 

> Blodgett, D.L., 2022, Updated CONUS river network attributes based on the E2NHDPlusV2 and NWMv2.1 networks: U.S. Geological Survey data release, https://doi.org/10.5066/P9W79I7Q. 

*NOTE for next release:* a bug was found in the nework used for this initial release that will affect about 300 of the 33000 mainstems included in v1. Some v1 mainstems will be superseded as a result. Given that only NHDPlusV2.1 comids are used, the impact of this change will be minimal.
