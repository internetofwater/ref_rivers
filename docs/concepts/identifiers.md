# Mainstem identifiers

A mainstem identifier is a URI of the form `https://geoconnex.us/ref/mainstems/29559`. Resolving it redirects to a description on `reference.geoconnex.us`, and representations can be negotiated from the identifier itself:

```bash
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=json"    # GeoJSON
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=jsonld"  # JSON-LD (RDF)
curl -L "https://geoconnex.us/ref/mainstems/2259484?f=html"    # HTML
```

An `Accept` header works the same way and survives the redirect, so a linked data client needs no query parameter:

```bash
curl -L -H "Accept: application/ld+json" "https://geoconnex.us/ref/mainstems/2259484"
```

The resolution architecture, the rule against referencing landing-content URLs, and the identifier policy shared by all reference collections are documented in [Reference Features](https://docs.geoconnex.us/reference/reference_features). What follows is specific to mainstems.

## The integer is opaque

The number at the end derives from the level path identifier of the dataset that originated the mainstem. Do not parse it, and do not expect it to work as a level path identifier anywhere else.

The distinction matters more than it looks. An NHDPlus level path is a grouping attribute, not an identifier: it is tied to the hydrologic sequence numbering of the network and changes whenever that numbering changes, and the choice of which upstream branch continues the path is sensitive to flowline names and to how densely the network is mapped. A mainstem identifier is minted once against a headwater and an outlet and then held fixed, which is exactly the property a level path does not have.

## What resolves, and what it links to

A mainstem description carries the attributes documented in [Mainstem attributes](../reference/attributes.md), a simplified geometry for display, and links out in two directions. Head and outlet identifiers in each hydrography system point down to the network the mainstem is composed of. `downstream_mainstem_id` and `encompassing_mainstem_basins` point across to other mainstems, so the basin hierarchy can be walked from any starting point without a separate topology dataset.

## Querying the collection

The mainstems collection supports the standard OGC API - Features access patterns — items by identifier, bounding-box and property filters, paging, and vector tiles:

```bash
# by bounding box
curl "https://reference.geoconnex.us/collections/mainstems/items?bbox=-89.5,42.9,-89.2,43.2&f=json"

# by name
curl "https://reference.geoconnex.us/collections/mainstems/items?name_at_outlet=Yahara%20River&f=json"
```

Every published attribute is queryable; the authoritative list is at `https://reference.geoconnex.us/collections/mainstems/queryables`, and collection metadata including the full schema is at `https://reference.geoconnex.us/collections/mainstems`.

Two mainstem collections are served. `mainstems` is what a mainstem identifier resolves to and what the geoconnex crawler attaches datasets to; it carries the v2-era subset of the schema. `mainstems_v3` carries the full v3 schema, including `primary_name` and the NHDPlusHR and NHD permanent identifier pairs. See [Mainstem attributes](../reference/attributes.md).
