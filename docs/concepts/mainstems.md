# What a mainstem is

A mainstem is the path a river takes from a headwater to an outlet. Identifying that path lets you attach information to a river as a whole rather than to the individual stream segments that compose it in a given dataset — the Colorado River is one river, whether a given hydrography represents it as 400 flowlines or 4,000.

The mainstems logical data model has four feature types: *headwater*, *outlet*, *mainstem*, and *drainage basin*. Its founding assumption is that every drainage basin has one and only one headwater source area, and a single mainstem flowing to a single outlet. A nested set of drainage basins — and the dendritic network of mainstems associated with them — follows from that assumption, which is what makes hydrographic addressing possible with so little information.

Three pieces of information define a mainstem in practice:

1. A headwater location at the top of a network.
2. An outlet location that is either terminal or flows to a larger river.
3. A network of features connecting the headwater to the outlet.

In the underlying model, the headwater and outlet are *hydrolocations* — points along the network — and either may be null where a dataset does not identify them. The mainstems registry is more constrained than the model requires: every reference mainstem has a headwater and an outlet, recorded as catchment or flowline identifiers in the dataset that defines them. All three are bound to URI namespaces that resolve to some description of the feature in question, where available.

## Why headwater and outlet rather than geometry

Geometry changes with every hydrography update — a channel is remapped, a divergence is re-coded, a segment is split — but the physical fact that a particular headwater drains through a particular outlet changes only when the river or its representation changes substantively. Storing the endpoints and the connecting network, rather than the line itself, means an improved representation can be attached to the same identifier instead of requiring a new one.

This is what makes a mainstem a reference feature rather than a data product. The identifier persists; the geometry served alongside it is just a current reference representation and is expected to change.

## Three scales of flowpath

Mainstem is one of three feature types that all realize the HY_Flowpath concept of the HY_Features standard, each at a different resolution. Keeping them distinct avoids most of the confusion around what a mainstem identifier does.

| Feature type | Extent | Catchment relationship |
| --- | --- | --- |
| flowline | A single linear segment | May have no catchment at all — a canal or a buried conduit still connects the network |
| flowpath | Inlet of a catchment to its outlet; an aggregate (1 or more) of flowlines | One-to-one with a catchment |
| mainstem | Headwater to basin outlet; a composite of flowlines | Its basin is derivable but not published |

The registry publishes only the third. Reach-scale and catchment-scale identitifiers are contained in hydrography datasets — COMIDs in NHDPlusV2, permanent identifiers in NHDPlusHR and NHD — and the crosswalks in [Data products](../reference/products.md) connect them.

## Mainstems and drainage basins

A mainstem is the linear representation of a drainage basin, while a drainage basin is the total upstream area draining to an outlet: a (usually aggregate) catchment with no inflows and a single outlet. Basin polygons themselves are not published in reference mainstems.

What the registry does carry is the basin hierarchy as attributes on the mainstem. `downstream_mainstem_id` identifies the mainstem a given river flows into, and `encompassing_mainstem_basins` lists the mainstems whose basins contain this one. Together they give a navigable nesting without a second identifier system.

## Divergences and non-dendritic connectivity

Real river networks are not dendritic — channels split around islands, braid, and divert into distributary fans. The mainstem network is dendritic, and the two are reconciled by carrying the non-dendritic connectivity in the flow network rather than in the mainstems: each connection between two flowlines is flagged as primary or not, in both directions, so that exactly one upstream connection and one downstream connection at any junction is the main path. Following the main path from a headwater to an outlet yields the mainstem; the other paths belong to other mainstems.

The consequence is a strict rule: every flowline is part of one and only one mainstem. That is what makes a mainstem a *composite* of flowlines rather than a loose aggregate of them, and it is how a join using the crosswalk tables can be unambiguous.

This same primary-and-secondary logic is what NHDPlus encodes in its stream level and divergence attributes, and what the EPA River Reach File (RF1) encoded before it. One case is not yet settled in the underlying model: no explicit distinction is drawn between a diverted channel that stays within a river's watercourse and one that leaves the valley to form a separate river. Where that distinction (braided channels vs interbasin transfers) matters to your work, do not expect the mainstem assignment to carry it.

## Why mainstems is intentionally minimal

Conflating and validating identifiers across dataset versions costs more as the number of datasets grows, when the cost to conflate exceeds available resources, identifiers effectively stop being persistent in practice regardless of what was promised. The mainstem feature type is designed with that constraint in mind: the smallest number of identified features that still uniquely identifies every flowline in a fully resolved network.

This is why the registry mints identifiers for rivers and not reaches, and why relative position along a mainstem is deliberately not recorded. A link that says only "this is on the Kickapoo River" survives a remapping of the channel; a link that says "at measure 43.2 percent along reach X" does not.

The model is also not an attempt to represent hydrologic process or to characterize river geomorphology — it identifies features so that data about them can be integrated, and stops there. It supports indexing, navigation, and addressing; anything that depends on the shape of the channel needs the source hydrography.

## Scope beyond the United States

Mainstem identifiers are not inherently national. The same model has been applied globally using MERIT hydrography and Natural Earth names (<https://doi.org/10.5066/P9O15C70>), and this registry's transboundary basins carry NHDPlusHR-derived mainstems that extend into Canada. The initial focus is the United States; the model does not assume it.
