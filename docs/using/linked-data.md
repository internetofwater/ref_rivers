# Work with data linked to mainstems

A mainstem identifier is useful in three directions: it gets you geometry for the river, it gets you the network of features that compose it, and it gets you everyone else's data referenced to the same river.

## Geometry

The reference service returns the current best representation as GeoJSON:

```bash
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=json"
```

For bulk work, take `mainstems.gpkg` from the data release rather than paging the service — see [Data products](../reference/products.md). The same caution applies: that geometry is simplified for display.

## The network the mainstem is composed of

The reference geometry is a highly simplified single line, published for display rather than measurement. Any analysis — length, sinuosity, intersection, snapping — should use source dataset geometry. When you need the actual flowlines — for routing, accumulation, or attribute joins — navigate the source hydrography from the mainstem's outlet. The NLDI upstream-mainstem navigation returns exactly the flowlines belonging to the mainstem:

```bash
curl "https://api.water.usgs.gov/nldi/linked-data/comid/21412883/navigation/UM/flowlines?distance=9999&f=json"
```

Equivalently, filter the crosswalk table for the mainstem URI and join the resulting COMIDs or NHDPlusHR identifiers to the network you already hold. The crosswalk is cheaper for bulk work and exact by construction; the NLDI is better for one-off navigation and for reaching tributaries, basins, and referenced sites in the same call.

```r
comids <- dplyr::filter(readr::read_csv("nhdpv2_lookup.csv"),
                        uri == "https://geoconnex.us/ref/mainstems/29559")$comid
net <- nhdplusTools::get_nhdplus(comid = comids)
```

## Other people's data on the same river

Reference mainstem items carry a `datasets` array assembled by the geoconnex crawler from published linked data. Each entry names a monitoring location, what is measured, the temporal coverage, and a distribution URL that retrieves the data:

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

This is the payoff of the identifier system in practice — one request against a river returns monitoring across agencies and programs that never coordinated with each other, because each of them referenced the same URI. Coverage reflects who has published to geoconnex, so absence in the array means the data was not published there, not that the data does not exist.

The two link types in each entry behave differently and are worth handling separately. `monitoringLocation` is a geoconnex identifier: resolve it and you get more linked data, and your graph grows. `distributionURL` points at a comma-separated download from the Water Quality Portal — retrievable, but opaque to a linked data client, so it terminates the graph. Plan for a client that follows the first automatically and hands the second to whatever code reads tabular data.

## Dams and gages on a river

The `dams` and `gages` collections carry a `mainstem_uri` attribute, so infrastructure on a river is one filtered request rather than a spatial analysis:

```r
animas <- "https://geoconnex.us/ref/mainstems/35394"
q <- sprintf("mainstem_uri = '%s'", animas)
gages <- sf::st_read(paste0("https://reference.geoconnex.us/collections/gages/items?f=json&filter=",
                            URLencode(q)), quiet = TRUE)
```

Each record's `subjectof` points at the source system's record for that dam or gage. Worked examples in both R and Python are in the geoconnex documentation under [finding hydrologically related features](https://docs.geoconnex.us/access/examples/related).

For a graph query rather than a feature request, take the JSON-LD form and load it into a triple store, or use the [SPARQL endpoint](https://docs.geoconnex.us/playground/sparql) against the geoconnex graph:

```bash
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=jsonld"
```

## Aggregating along a river

Because `downstream_mainstem_id` and `encompassing_mainstem_basins` are carried on every mainstem, you can roll data up a basin without a separate topology dataset. Follow `downstream_mainstem_id` to walk toward the outlet, or filter on `encompassing_mainstem_basins` containing a given URI to select everything draining through it.
