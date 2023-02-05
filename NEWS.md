reference mainstems v1.1
========================

This is a unique update that is being applied in a pragmatic and situationally specific way. Future updates will likely be very different as they will involve inclusion of a new connecting network and addition of additional mainstem identifiers.

The second release of reference mainstems is a minor update in which 

- 33564 will now reference a new reference data release
- 161 mainstems have been superseded, 
- 23 have been identified as in error and will not appear in the reference mainstems lookup table, and  
- 105 have been identified as the same with a slightly different outlet location.

A new version of the v1.0 source data was used for this release. 

> David L Blodgett, 2023, Mainstem Rivers of the Conterminous United States (version 2.0): U.S. Geological Survey data release, https://doi.org/10.5066/P92U7ZUT. 

The reference fabric is defined in:

> David L. Blodgett, 2023, Updated CONUS river network attributes based on the E2NHDPlusV2 and NWMv2.1 networks (version 2.0): U.S. Geological Survey data release, https://doi.org/doi:10.5066/P976XCVT. 

The base geometry for this new reference fabric is still the NHDPlusV2: https://www.epa.gov/waterdata/get-nhdplus-national-hydrography-dataset-plus-data 

Updates to the registry will be applied for all mainstems. Categories of change include:

1. the 33564 will be updated in place to now indicate that the Version 2.0 network at https://doi.org/doi:10.5066/P976XCVT is now the connecting network that should be used for that mainstem.
1. the 161 superseded mainstems will not be altered but new mainstems will be added and the old ones will be linked to their replacement(s) in the mainstem lookup table published via geoconnex.us. 
1. the 23 that are in error had empty geometry and have not been used. They will remain and no longer appear in the mainstem lookup table.
1. the 105 with new outlets will not be altered but will have a new row in the registry with the new outlet.

reference mainstems v1.0
========================

The initial release of the mainstems registry was created from v1 of:

> Blodgett, D.L., 2022, Mainstem Rivers of the Conterminous United States: U.S. Geological Survey data release, https://doi.org/10.5066/P9BTKP3T. 

It identified 33853 mainstem identifiers with head and outlet identifiers bound to the NHDPlusV2.1 comid system. The network connecting headwaters to outlets is defined in: 

> Blodgett, D.L., 2022, Updated CONUS river network attributes based on the E2NHDPlusV2 and NWMv2.1 networks: U.S. Geological Survey data release, https://doi.org/10.5066/P9W79I7Q. 

*NOTE for next release:* a bug was found in the nework used for this initial release that will affect about 300 of the 33000 mainstems included in v1. Some v1 mainstems will be superseded as a result. Given that only NHDPlusV2.1 comids are used, the impact of this change will be minimal.
