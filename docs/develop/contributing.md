# Releases and contributing

## Releases

Release notes are in [NEWS.md](https://github.com/internetofwater/ref_rivers/blob/main/NEWS.md) and version numbering is described in [Persistence and change](../concepts/persistence.md). A release publishes an updated registry in the repository, an updated data release under a new DOI, and updated content on `reference.geoconnex.us`.

Downstream projects should pin to a version and update deliberately. Because superseded mainstems remain resolvable, a project that lags a release still works — it just may not reflect the current best representation.

## Contributing

Contributions are welcome and the process is still taking shape. Open an [issue](https://github.com/internetofwater/ref_rivers/issues) or a pull request, and the maintainers will do the legwork needed to get new reference locations into the registry.

Useful contributions include mainstems that are missing or misrepresented, headwater or outlet locations that are wrong, name attributions that disagree with local usage, and connectivity problems in transboundary basins. Include the mainstem URI, what you observe, and what you expect. For a candidate that should exist but does not, give the headwater and outlet identifiers in a dataset the registry already references.

Not every report leads to a change in identifiers. A wrong geometry is fixed by improving the representation attached to an existing mainstem; a wrong path through the network may require supersession, which is reviewed by hand because it is permanent.

## License and disclaimer

The content is released under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/). The software is in the public domain as a work of the U.S. Geological Survey.

This information is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The information has not received final approval by the U.S. Geological Survey and is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from its authorized or unauthorized use.
