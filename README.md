# Reference Rivers for geoconnex.us

This repository houses workflow software for compilation of community reference river features. The output of this workflow will generate Persistent Identifiers for the [geoconnex.us system](https://github.com/internetofwater/geoconnex.us), reference landing page content for the [reference.geoconnex.us system](https://reference.geoconnex.us/), and be used as the basis for persistence in [3DHP data](https://www.usgs.gov/3d-hydrography-program).

## Documentation

The users manual is at **<https://internetofwater.github.io/ref_rivers/>**, built from `/docs` in this repository. It covers what a mainstem is and why the identifier is separated from the geometry, how to find the mainstem for a place and link your data to it, the published attribute schema and the invariants the build enforces, and how the pipeline works.

Start with [What a mainstem is](https://internetofwater.github.io/ref_rivers/concepts/mainstems/) for the model, [Mainstem identifiers](https://internetofwater.github.io/ref_rivers/concepts/identifiers/) for what the integer at the end of a URI means and what it resolves to, and [Persistence and change](https://internetofwater.github.io/ref_rivers/concepts/persistence/) for how the registry evolves — no mainstem defined here is ever removed, though a mainstem may be superseded and its best available representation will improve over time.

The identifier policy shared by every geoconnex reference collection, and the resolution architecture behind it, are documented once at [Reference Features](https://docs.geoconnex.us/reference/reference_features) rather than repeated here.

## Data releases

> David L Blodgett, 2025, Mainstem Rivers of the Conterminous United States (Version 3.0): U.S. Geological Survey data release, https://doi.org/10.5066/P13LNDDQ.

Previous versions:
- v2.0: https://doi.org/10.5066/P92U7ZUT (2023)
- v1.0: https://doi.org/10.5066/P9BTKP3T (2022)

Version 3.0 (released January 2026) expanded the registry to roughly 850k mainstem identifiers, adding 818,908 new mainstems while retaining 33,745 of the 33,900 from v2. It incorporates NHDPlusHR geometry for transboundary basins and ensures network connectivity across NHDPlusV2, NHDPlusHR, and the NHD snapshot used for the 3DHP dataset (https://doi.org/10.5066/P94H0DAG). See [NEWS.md](NEWS.md) for release detail and the [version history](https://internetofwater.github.io/ref_rivers/concepts/persistence/) for what changed in each.

## Building

The project uses the [`{targets}` R package](https://books.ropensci.org/targets/) for workflow management. `_targets.R` holds the workflow definition and `/R` holds the functions it calls; run it with `targets::tar_make()`. The pipeline downloads several gigabytes of source data on first run. See the [build workflow](https://internetofwater.github.io/ref_rivers/develop/workflow/) for the directory layout, what each stage does, and what adding a new source network involves.

The documentation site is MkDocs with the Material theme. Install `requirements.txt` and run `mkdocs serve` to preview, or `mkdocs build --strict` to check it the way CI does; pushes to `main` that touch `/docs` deploy through `.github/workflows/docs.yml`.

## Contributing

Contributions are welcome and the process is still taking shape. Open an [issue](https://github.com/internetofwater/ref_rivers/issues) or a pull request and the maintainers will do the legwork needed to get new reference locations into the registry. [Releases and contributing](https://internetofwater.github.io/ref_rivers/develop/contributing/) describes what makes a useful report and what happens to it.

## Disclaimer

This information is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The information has not received final approval by the U.S. Geological Survey (USGS) and is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the information.

This software is in the public domain because it contains materials that originally came from the U.S. Geological Survey  (USGS), an agency of the United States Department of Interior. For more information, see the official USGS copyright policy at [https://www.usgs.gov/visual-id/credit_usgs.html#copyright](https://www.usgs.gov/visual-id/credit_usgs.html#copyright)

Although this software program has been used by the USGS, no warranty, expressed or implied, is made by the USGS or the U.S. Government as to the accuracy and functioning of the program and related program material nor shall the fact of distribution constitute any such warranty, and no responsibility is assumed by the USGS in connection therewith.

This software is provided "AS IS."

 [
    ![CC0](https://i.creativecommons.org/p/zero/1.0/88x31.png)
  ](https://creativecommons.org/publicdomain/zero/1.0/)
