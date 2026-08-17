# Link your data to a mainstem

Linking means storing the mainstem URI alongside your feature — a monitoring site, a reach-based assessment, a permit, a modeled output — so that anyone holding the same URI can find your data and you can find theirs.

## What to store

Store the full URI, `https://geoconnex.us/ref/mainstems/29559`, not the bare integer. The URI is resolvable and self-describing; the integer requires a reader to know the namespace, and it will be mistaken for a level path identifier sooner or later.

Store the identifier, never the `reference.geoconnex.us` URL you land on after the redirect — see [Reference Features](https://docs.geoconnex.us/reference/reference_features).

If your feature is a point on a river, store the mainstem URI plus your own position information — a COMID, a measure, or coordinates. The mainstem reference says which river; it does not say where on the river, and losing that distinction is the most common linking error.

The omission is deliberate on the reference side. Position along a river is the part of a link that breaks when the channel is remapped or the segmentation changes, so the reference model records only that a location is on a given mainstem and leaves measure to the applications that need it. Keep yours where you can recompute it.

## Choosing the right mainstem

Every location gets one mainstem and only one. Point features take the mainstem of the flowline they sit on, resolved through the COMID or NHDPlusHR crosswalk rather than by nearest-line distance. At a confluence, the location belongs to the receiving mainstem — the one flow continues along — not to the tributary arriving there. Where a site's position is uncertain by more than a few tens of meters near a junction, review it by hand.

Features that span a river — a reach assessment, a segment-based model output — take the mainstem of the segments they cover. If they cross a confluence such that more than one mainstem applies, record more than one link rather than picking the larger river.

Polygon features on the water follow the same one-mainstem rule with a tiebreaker: a lake or wide-river polygon takes the most downstream mainstem flowing out of it. Where more than one mainstem exits a single waterbody, either split the polygon or link it to the most prominent one.

## Maintaining links

Re-check your links against each minor release. The work is a filter on `superseded` and a follow of `new_mainstemid`, described in [Persistence and change](../concepts/persistence.md). Between releases nothing needs to happen.

Where a hand-reviewed link disagrees with the crosswalk — a site the crosswalk puts on the tributary that field knowledge puts on the main river — keep your determination and record why. The registry does not attempt to be authoritative about the position of your features.

## Publishing links through geoconnex

To make your links discoverable, publish them through the [geoconnex](https://docs.geoconnex.us/) system so the crawler picks them up and your data appears on the mainstems it references. The mechanics — namespace registration, identifier minting, sitemaps, and how to structure landing content and relations — are documented in the [contributing guide](https://docs.geoconnex.us/contributing/overview) and the [JSON-LD primer](https://docs.geoconnex.us/reference/data-formats/jsonld/primer/).

The mainstem-specific part is which URI to put in the `hyf:referencedPosition` of your feature, which is what the rest of this page is about.
