# Build workflow

The registry is rebuilt by a [`{targets}`](https://books.ropensci.org/targets/) pipeline in R. `_targets.R` holds the workflow definition and `/R` holds the functions it calls. Most consumers never need to run this — the outputs are published in releases — but the pipeline is the specification of how mainstems are derived.

## Running it

```r
install.packages(c("targets", "dplyr", "sf", "sbtools", "nhdplusTools", "arrow", "readr"))
targets::tar_make()
```

The pipeline requires `nhdplusTools` (or hydrogeofetch) newer than 1.0.1 and will stop otherwise. It downloads several gigabytes of source data into `/data` on first run, including the full NHDPlusV2 national geodatabase. Source data releases are pinned by SHA-256 checksum, so a changed upstream file fails the build rather than silently altering output.

## Directory layout

| Path | Contents |
| --- | --- |
| `_targets.R` | Workflow definition |
| `/R` | Functions: data retrieval, mainstem construction, registry, output writing |
| `/registry` | Registry and provider tables, tracked in source control |
| `/data` | Downloaded source data, not tracked |
| `/data/review` | Review artifacts for a release |
| `/temp` | Debugging output |
| `/out` | Output for publication elsewhere |
| `/docs` | This site |

## What the pipeline does

Source networks come first — mainstem databases and enhanced NHDPlus attribute tables (eNHD) for v1, v2, and v3, the v3 reference rivers release, the NHDPlusHR lookup, and the NHDPlusV2 geodatabase. `reconcile_mainstems()` then matches v2 mainstems against v3 candidates so that existing identifiers carry forward, and `initialize_mainstems()` assembles the full candidate set including new additions and HR-sourced transboundary rivers.

`make_mainstems()` produces the published schema: name resolution from GNIS, length and drainage area at the outlet, downstream and encompassing basin relationships, and CRS enforcement. `validate_mainstems()` applies the invariants listed in [Mainstem attributes](../reference/attributes.md). `write_lookups()` builds the crosswalks — the v2 table by joining mainstems to eNHD level paths, the HR table by navigating the HR network from head to outlet, first along the primary-downstream path and falling back to a full-network trace where that does not reach the outlet.

`build_registry()` adds rows for new mainstems and `write_registry()` writes the updated table. New rows are appended; existing rows are never rewritten.

## Adding a new source network

A new base network for existing mainstems is a major version change. The work is a new `get_*()` target for the source, extension of `initialize_mainstems()` to reconcile against it, new head and outlet attribute pairs in the published schema, a new provider row, and a new crosswalk in `write_lookups()`. Existing head and outlet references stay where they are.
