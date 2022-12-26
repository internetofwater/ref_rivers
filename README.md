# Reference Rivers for geoconnex.us

# *NOTE: the contents of this readme are a working draft community contribution is being sought to form the strategy detailed here.*

This repository houses workflow software for compilation of community reference river features. The output of this workflow will generate Persistent Identifiers for the [geoconnex.us system](https://github.com/internetofwater/geoconnex.us), reference landing page content for the [reference.geoconnex.us system](https://reference.geoconnex.us/), and be used as the basis for persistence in the  [Network Linked Data Index](https://labs.waterdata.usgs.gov/about-nldi/index.html).

The base dataset that has informed this work so far can be found in the "mainstem summary gpkg" file associated with this data release:

> Blodgett, D.L., 2022, Mainstem Rivers of the Conterminous United States: U.S. Geological Survey data release, https://doi.org/10.5066/P9BTKP3T. 

v1.0 of the above dataset was used for initial release of mainstems. A v1.1  of the above dataset was incorporated when this workflow repository was instantiated (December, 2022). [NEWS.md](NEWS.md) will be used for all future modifications.

# Reference mainstem river strategy

The intention of the collection of mainstem river identifiers created by the workflow in this repository is to provide a minimal yet sufficient set of identifiers for rivers with initial focus on the United States. 

See ["Progress Toward a Reference Hydrologic Geospatial Fabric for the United States"](https://waterdata.usgs.gov/blog/hydrofabric/) for background on the work that has led to the creation of this work. Complete background on the Mainstems data model can be found here:

> Blodgett, D., Johnson, J.M., Sondheim, M., Wieczorek, M., and Frazier, N., 2021, Mainstems: A logical data model implementing mainstem and drainage basin feature types based on WaterML2 Part 3: HY Features concepts: Environmental Modelling &amp; Software, v. 135, p. 104927, https://doi.org/10.1016/j.envsoft.2020.104927. 

## Summary

In essence, a "mainstem" is defined by three pieces of information.  
1. A headwater location that is at the top of a network.
1. An outlet location that is terminal or flows to a larger river.
1. A network of features connecting the headwater and outlet.

In the registry of mainstems defined here, these three take the form of:
1. A headwater catchment identifier.
1. An outlet catchment identifier.
1. A dataset that the identifiers are defined in.

To be considered for this work, all identifiers are bound to URI namespaces that resolve some description of the features in question.

## Example: 

Mainstem `https://geoconnex.us/ref/mainstems/29559` is defined by headwater catchment: `https://geoconnex.us/nhdplusv2/comid/1233891` and outlet catchment: `https://geoconnex.us/nhdplusv2/comid/21412883`. These catchment identifiers are defined in the NHDPlusV2 dataset's `comid` namespace, `https://geoconnex.us/nhdplusv2/comid/` which includes a network of features that connect the headwater to the outlet. Using this information a person working with a feature referenced to mainstem `https://geoconnex.us/ref/mainstems/29559` can retrieve the collection of features that the mainstem is composed of. 

## How will this system of mainstem rivers evolve?

First and foremost, *no mainstem defined in this registry will ever be removed*. However, over time, two types of change are expected:  
1. The best available reference representation for a given mainstem will improve.
1. The mainstem a given segment of a river is a member of may be superseded.

In the first case, the mainstem identifier will persist and headwater and outlet identifiers from updated source datasets will be added. In this case, the original headwater and outlet identifiers will remain in the registry associated with the mainstem identifier and an additional headwater and outlet representation (sourced from a new, ostensibly more accurate dataset) of the mainstem will be added. 

In the second case, the mainstem identifier will be superseded. A different system of network path(s) will connect headwater and outlet locations that were previously connected by a different mainstem path. In this case, the original mainstem, headwater, and outlet identifiers will remain in the registry but the mainstem identifier will be superseded and not be associated with improvements added over time. 

## Reference Fabric

Given that this registry is new and has not yet been used broadly, the mechanisms for maintenance of the dataset and references to it are largely to be determined but the summary above provides a summary of the strategy for the mainstem identifiers themselves.

As this work progresses, the identifiers and mainstem definitions provided will be included in a best-available "reference hydrologic geospatial fabric". The data and tools of this fabric will facilitate maintenance of data linked to mainstems. At any point in time, all non-superseded mainstems will be included in this reference fabric and identifiers from previous reference fabrics will be mapped to the latest, non-superseded set of mainstems.

# Architecture

This project exists in a linked data architecture that relies on Web uniform resource identifiers (URIs) for both digital and real world entities. 

There are three types of resources in the architecture:
1. Real-world rivers identified by a so-called "non-information URI".
1. Community reference information about the real world river identified by a URL that is the target of a redirect from a non-information URI.
1. A particular representation of the real world river.

In practice, these urls will look like: 
1. `https://geoconnex.us/ref/mainstems/29559` (which will redirect to 2)
1. `https://reference.geoconnex.us/collections/mainstems/items/29559` (which will provide information about the reference river, including a link to 3)
1. `https://labs.waterdata.usgs.gov/api/nldi/linked-data/comid/21412883/navigation/UM/flowlines?distance=9999&f=json` (which is the NLDI upstream mainstem navigation starting from the outlet of the reference river.)

It is important to maintain this separation because no one organization, other than a community organization set up to fulfill this role, can be expected to be both the community reference catalog and a provider of their own information.  

# Project structure

The project uses the [`{targets}` R package](https://books.ropensci.org/targets/) for workflow management.  

- `_targets.R` contains the workflow summary.
- `/R` functions defined for this project.
- `/data` data downloaded for this project.
- `/temp` temporary output that may be of interest for debugging.
- `/out` output to be contributed elsewhere. 
- `/registry` registry of dams tracked in source control.
- `/docs` contains artifacts to be served via github.io

# Contributing

First, thank you for considering a contribution! For this to work, everyone with unique dam locations need to be willing to contribute those locations here. 

This is a new project and, as such, exactly how contributions are made will be flexible and a work in progress. If you have questions or want to contribute, just reach out [in the issues](https://github.com/internetofwater/ref_rivers/issues) and/or submit a pull request. The maintainer(s) are more than happy to coordinate and do whatever legwork is needed to get new reference locations into the registry.

As time goes on and the nature of contributions becomes more clear, this guidance will become more specific, but until then, just get in touch and we'll work together.

## Disclaimer

This information is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The information has not received final approval by the U.S. Geological Survey (USGS) and is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the information.

This software is in the public domain because it contains materials that originally came from the U.S. Geological Survey  (USGS), an agency of the United States Department of Interior. For more information, see the official USGS copyright policy at [https://www.usgs.gov/visual-id/credit_usgs.html#copyright](https://www.usgs.gov/visual-id/credit_usgs.html#copyright)

Although this software program has been used by the USGS, no warranty, expressed or implied, is made by the USGS or the U.S. Government as to the accuracy and functioning of the program and related program material nor shall the fact of distribution constitute any such warranty, and no responsibility is assumed by the USGS in connection therewith.

This software is provided "AS IS."

 [
    ![CC0](https://i.creativecommons.org/p/zero/1.0/88x31.png)
  ](https://creativecommons.org/publicdomain/zero/1.0/)
