# Persistence and change

Every geoconnex reference collection commits to identifiers that are unique, permanent, never reused, and never removed. That commitment is the whole of the shared policy; how a collection honors it — what qualifies for an identifier, how representations improve, when an identifier is superseded — is decided by the collection's stewards and belongs in its own documentation. This page is that documentation for mainstems.

No mainstem defined in this registry will ever be removed. Everything below follows from holding that fixed while the hydrography underneath keeps changing.

Two kinds of change are expected over time.

**The best available representation improves.** The identifier persists and headwater and outlet identifiers from the updated source dataset are added alongside the originals. Nothing is dropped — a mainstem accumulates representations, so data referenced against an older source still resolves.

**A mainstem is superseded.** Sometimes an improved network shows that the path connecting a headwater to an outlet is not the path a previous dataset described, and the river is better represented by a different arrangement of mainstems. In that case the original identifier remains in the registry, is marked `superseded: true`, and stops accumulating improvements. The `new_mainstemid` attribute names the current mainstem or mainstems that should be considered its replacement.

Supersession is rare and reviewed by hand. Of the 33,900 mainstems in v2, 155 — under 0.05 percent — were superseded in v3, all in transboundary basins where NHDPlusHR geometry replaced NHDPlusV2-derived paths.

## What this means for your data

If you hold mainstem identifiers, you do not need to re-reference your data when a new version is released. Check the `superseded` flag against the current registry on some schedule that suits you, and follow `new_mainstemid` where it is set. Everything else keeps working, possibly against better geometry than when you first linked.

Consumers that need only active mainstems should filter on `superseded == false`. The crosswalk tables described in [Data products](../reference/products.md) include only active mainstems.

## Version numbering

Versions take the form major.minor.patch.

**Major** — one or more base networks are incorporated, or a breaking change is made. Expected no more than once every five to ten years.

**Minor** — additions to the registry, supersessions, non-breaking data model changes, or new outputs. Released as need arises.

**Patch** — attribute value improvements, documentation, bug fixes, and anything else that requires nothing of downstream projects beyond picking up updated files.

## Version history in brief

| Version | Date | Change |
| --- | --- | --- |
| v1 | 2022 | 33,853 mainstems, head and outlet bound to NHDPlusV2.1 COMIDs. |
| v2 | 2023 | Re-referenced to an updated network; 161 superseded for outlets that moved significantly, 23 superseded for headwaters that were not headwaters, 105 given new outlet rows. |
| v3 | Jan 2026 | 818,908 mainstems added; 33,745 v2 mainstems retained; NHDPlusHR adopted as source geometry in 39 transboundary HU04s. |
| v3.1 | 2026 | Mainstem-to-NHDPlusHR lookup added; 15 mainstems superseded in post-release validation. |
| v3.2 | 2026 | HR lookup corrections, cross-region and cross-domain outlet fixes, added validation checks. |

Full detail is in [NEWS.md](https://github.com/internetofwater/ref_rivers/blob/main/NEWS.md).

## Why v3 is conservative

About 847,000 candidate mainstems met the inclusion criteria for v3, and roughly 819,000 were added. The omitted 28,000 are cases where downstream connectivity was in question — typically a mainstem flowing to a mainstem that did not itself qualify, which would leave the new feature isolated in the reference network. Adding an identifier is permanent, so an identifier that would need to be superseded once the network fills out is worse than a delayed one. Many of these will be added in later releases.
