reference mainstems v3.4 (DRAFT)
========================

v3.4 adds 1,318 reference mainstems outside the conterminous United States: Alaska (1,085), Hawaii (109), Puerto Rico and the U.S. Virgin Islands (98), and Guam and the Northern Mariana Islands (26). Existing registry rows and mainstems are unchanged.

Each new mainstem is the flowline set of one `usgs/mainstems` identifier from the NHD-based initial 3DHP network release (https://doi.org/10.5066/P94H0DAG), registered as provider 5. That release was built on the final NHD snapshot, not 3DHP production content, so each new mainstem is the same feature as its usgs mainstem. Head and outlet are NHD permanent identifiers; NHDPlusV2 and NHDPlusHR heads and outlets and outlet drainage area are blank.

A usgs mainstem is selected, using NHD names and lengths, if it:

1. is longer than 50 km in Alaska, 4 km in Guam and the Northern Marianas, or 10 km elsewhere, and more than 90 percent named;
2. is in Alaska, longer than 100 km and more than 50 percent named; or
3. is downstream of a selected mainstem.

Mainstems that share a flowline with the release's non-network minting are excluded (this removed Canal Moca, Puerto Rico). American Samoa has no named NHD flowlines.

- Identifiers 3000001 to 3001318 are assigned in domain, then usgs integer, order. `registry/oconus_ids.csv` maps each to its usgs mainstem.
- New crosswalk `nhd_permid_lookup.csv` (`uri`, `nhd_permid`, `id3dhp`) maps NHD permanent identifier and 3DHP identifier to mainstem uri for 851,502 mainstems. CONUS rows are republished from `nhd_3dhp_mainstem_lookup` in 3dhp_mainstem_ids v1.3 (https://code.usgs.gov/wma/nhgf/reference-fabric/3dhp/3dhp_mainstem_ids/-/releases/v1.3).
- Transboundary rivers carry their Canadian NHD reaches (the Yukon River is about 2,515 km). `primary_name_gnis_id` comes from U.S. reaches where there are any.
- Geometry is simplified to 50 m for display.

Names and geometry come from the NHD HU4 staged products, with HU4 1903 read from the GeoPackage edition because its FileGDB holds 68,180 of roughly 645,000 flowlines:

> U.S. Geological Survey, 2023-2024, USGS National Hydrography Dataset Best Resolution (NHD) for Hydrological Unit (HU) 4, FileGDB and GeoPackage staged products: https://www.usgs.gov/national-hydrography, accessed from https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHD/HU4/.

reference mainstems v3.3
========================

v3.3 replaces `nhdphr_lookup.csv` and nothing else; the other v3.2 artifacts are current for v3.3. The v3.2 lookup covered only mainstems in the transboundary HU04s where NHDPlusHR is the source geometry (#25). The v3.3 lookup is built by navigating the full NHDPlusHR network from each mainstem's head to its outlet, so it covers every mainstem with an NHDPlusHR head and outlet. Where paths overlap, larger rivers (lower streamlevel) take the flowline. A small number of mainstems whose NHDPlusHR path falls entirely inside a larger one get no flowline of their own and resolve through the NHDPlusV2 lookup.

reference mainstems v3.2
========================

v3.2 improves the nhdplushr_lookup.csv output and fixes several issues:

- Fix topo_sort bug related to HR representations
- Fix missing nhdplushr head/outlet entries (#18)
- Patch missing HR mainstems in lookup (#18)
- Improve handling of duplicates and cross-region lookups
- Fix major cross-domain river outlet ids (#22)
- Update nhdplushr outlet ids (#21)
- Add validation checks

reference mainstems v3.1
========================

v3.1 introduces a lookup table from mainstem uri to nhdplushr id

v3.1 is primarily a post-process validation release. No meaningful changes have been made to mainstems. A small number (15) of mainstems that were missed in v3 validation have been marked as superseded.

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
2. the 161 superseded mainstems will not be altered but new mainstems will be added and the old ones will be linked to their replacement(s) in the mainstem lookup table published via geoconnex.us. 
3. the 23 that are in error had empty geometry and have not been used. They will remain in the registry and no longer appear in the mainstem lookup table.
4. the 105 with new outlets will not be altered but will have a new row in the registry with the new outlet.

reference mainstems v1
========================

The initial release of the mainstems registry was created from v1 of:

> Blodgett, D.L., 2022, Mainstem Rivers of the Conterminous United States: U.S. Geological Survey data release, https://doi.org/10.5066/P9BTKP3T. 

It identified 33853 mainstem identifiers with head and outlet identifiers bound to the NHDPlusV2.1 comid system. The network connecting headwaters to outlets is defined in: 

> Blodgett, D.L., 2022, Updated CONUS river network attributes based on the E2NHDPlusV2 and NWMv2.1 networks: U.S. Geological Survey data release, https://doi.org/10.5066/P9W79I7Q. 

*NOTE for next release:* a bug was found in the nework used for this initial release that will affect about 300 of the 33000 mainstems included in v1. Some v1 mainstems will be superseded as a result. Given that only NHDPlusV2.1 comids are used, the impact of this change will be minimal.
