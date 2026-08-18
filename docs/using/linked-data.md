# Work with data linked to mainstems

A mainstem identifier is associated with three pieces of information: geometry for the river, the network of features that compose it, and everyone else's data referenced to the same river.

## Geometry

The reference service at `reference.geoconnex.us` returns the current reference representation as GeoJSON:

```bash
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=json"
```

[Open in a browser](https://geoconnex.us/ref/mainstems/2259484)

For bulk work, take `mainstems.gpkg` from the data release rather than paging the service — see [Data products](../reference/products.md).

This geometry is a highly simplified single line, published for display rather than measurement. Any analysis — length, sinuosity, intersection, snapping — should use source dataset geometry.

## The network the mainstem is composed of

When you need the actual flowlines — for routing, accumulation, or attribute joins — navigate the source hydrography from the mainstem's outlet. The [Network Linked Data Index](https://api.water.usgs.gov/nldi/swagger-ui/index.html) (NLDI) upstream-mainstem navigation returns exactly the flowlines belonging to the mainstem:

```bash
curl "https://api.water.usgs.gov/nldi/linked-data/comid/21412883/navigation/UM/flowlines?distance=9999&f=json"
```

[Open in a browser](https://api.water.usgs.gov/nldi/linked-data/comid/21412883/navigation/UM/flowlines?distance=9999)

Alternatively, filter the crosswalk table for the mainstem URI and join the resulting COMIDs or NHDPlusHR identifiers to the network you already hold. The crosswalk is cheaper for bulk work; the NLDI is better for one-off navigation and for reaching tributaries, basins, and referenced sites in the same call.

The two routes agree closely without being identical. As an example, for the Colorado River (`mainstems/29559`), the crosswalk carries 1,876 flowlines and a UM trace from the outlet returns 1,874, differing on four of them around a divergence. The crosswalk assigns every flowline to exactly one mainstem, as [What a mainstem is](../concepts/mainstems.md) describes; NLDI navigation follows the NHDPlus main path, which can take a side channel the registry has assigned elsewhere. Where an assignment has to match the registry, use the crosswalk. Future releases of the NLDI will converge reference mainstems with the underlying networks the NLDI represents.

=== "R"

    ```r
    comids <- dplyr::filter(readr::read_csv("nhdpv2_lookup.csv"),
                            uri == "https://geoconnex.us/ref/mainstems/29559")$comid
    net <- nhdplusTools::get_nhdplus(comid = comids)
    ```

=== "Python"

    ```python
    import pandas as pd
    from pynhd import NLDI

    lookup = pd.read_csv("nhdpv2_lookup.csv")
    comids = lookup.loc[lookup.uri == "https://geoconnex.us/ref/mainstems/29559", "comid"]

    # or navigate instead of joining
    net = NLDI().navigate_byid("comid", "21412883", "upstreamMain", "flowlines", distance=9999)
    ```

## Other people's data on the same river

Reference mainstem items carry a `datasets` array assembled by the geoconnex crawler from published linked data. Each entry gives a monitoring location, what is measured, the temporal coverage, and a distribution URL that retrieves the data:

```json
{
  "monitoringLocation": "https://geoconnex.us/iow/wqp/MEDEP_WQX-58662",
  "datasetDescription": "Temperature, water at MEDEP_WQX-58662",
  "variableMeasured": "Temperature, water",
  "variableUnit": "degrees Celsius",
  "temporalCoverage": "1998-01-01T00:00:00Z/2008-12-31T00:00:00Z",
  "distributionName": "Water Quality Portal",
  "distributionURL": "https://www.waterqualitydata.us/data/Result/search?siteid=...",
  "wkt": "POINT (-70.315673 44.442244)"
}
```

One request for a river returns monitoring across agencies and programs, because each of them referenced the same URI. Coverage reflects who has published to geoconnex, so absence in the array means the data was not published there, not that the data does not exist.

## Dams and gages on a river

The `dams` and `gages` collections carry a `mainstem_uri` attribute, so infrastructure on a river is one filtered request rather than a spatial analysis:

```r
animas <- "https://geoconnex.us/ref/mainstems/35394"
q <- sprintf("mainstem_uri = '%s'", animas)
gages <- sf::st_read(paste0("https://reference.geoconnex.us/collections/gages/items?f=json&filter=",
                            URLencode(q)), quiet = TRUE)
```

Each record's `subjectof` points at the source system's record for that dam or gage. Worked examples in both R and Python are in the geoconnex documentation under [finding hydrologically related features](https://docs.geoconnex.us/access/examples/related). In Python, [PyNHD](https://docs.hyriver.io/readme/pynhd.html)'s [`GeoConnex`](https://docs.hyriver.io/examples/notebooks/geoconnex.html) class wraps these collections directly — set `gcx.item` to the collection and query by geometry, by identifier, or by CQL filter.

## Aggregating along a river

Because `downstream_mainstem_id` and `encompassing_mainstem_basins` are carried on every mainstem, you can roll data up a basin without a separate topology dataset. Follow `downstream_mainstem_id` to walk toward the outlet, or filter on `encompassing_mainstem_basins` containing a given URI to select everything draining through it.

Every mainstem flowing directly into a given one is a single filtered request:

```bash
curl -G "https://reference.geoconnex.us/collections/mainstems/items" \
  --data-urlencode "f=json" \
  --data-urlencode "downstream_mainstem_id=https://geoconnex.us/ref/mainstems/35394"
```

[Open in a browser](https://reference.geoconnex.us/collections/mainstems/items?downstream_mainstem_id=https://geoconnex.us/ref/mainstems/35394)

=== "R"

    ```r
    animas <- "https://geoconnex.us/ref/mainstems/35394"
    trib <- sf::st_read(paste0("https://reference.geoconnex.us/collections/mainstems/items",
                               "?f=json&downstream_mainstem_id=", URLencode(animas)),
                        quiet = TRUE)
    ```

=== "Python"

    ```python
    from pynhd import GeoConnex

    gcx = GeoConnex()
    gcx.item = "mainstems"
    trib = gcx.byid("downstream_mainstem_id", "https://geoconnex.us/ref/mainstems/35394")
    ```

That returns direct tributaries only — eight, for the Animas. For everything draining through a mainstem rather than just the rivers adjoining it, filter on `encompassing_mainstem_basins` instead.
