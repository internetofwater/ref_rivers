#' Build OCONUS Mainstems in the Published Schema
#'
#' Takes the OCONUS batch (one row per `usgs/mainstems` identifier from
#' the 2024 release, doi:10.5066/P94H0DAG, built by [build_oconus_batch()])
#' and the tracked id table that assigns each a `ref/mainstems` integer, and returns
#' rows in the schema [make_clean_mainstems()] produces. Head and outlet are NHD
#' permanent identifiers; there is no NHDPlusV2 or NHDPlusHR representation.
#'
#' @param batch from [build_oconus_batch()].
#' @param id_table path to `registry/oconus_ids.csv`.
#'
#' @return An `sf` data frame in EPSG:4326 with the columns of
#'   [make_clean_mainstems()], in the same order.
make_oconus_mainstems <- function(batch, id_table) {
  x <- batch
  ids <- readr::read_csv(id_table, col_types = "iccc")

  # the id table is the source of truth for the integers and is never
  # regenerated; every batch row must have an id and every id a batch row.
  stopifnot(!any(duplicated(ids$mainstem)), !any(duplicated(ids$usgs_mainstem)))
  stopifnot(setequal(ids$usgs_mainstem, x$usgs_mainstem))

  x$id <- ids$mainstem[match(x$usgs_mainstem, ids$usgs_mainstem)]
  uri_of <- function(usgs) {
    out <- ids$mainstem[match(usgs, ids$usgs_mainstem)]
    ifelse(is.na(out), "", paste0("https://geoconnex.us/ref/mainstems/", out))
  }

  x$uri <- uri_of(x$usgs_mainstem)
  x$downstream_mainstem_id <- uri_of(x$downstream_usgs_mainstem)
  stopifnot(all(x$downstream_usgs_mainstem == "" | x$downstream_mainstem_id != ""))

  # encompassing basins: the downstream chain, nearest first
  dn <- setNames(x$downstream_mainstem_id, x$uri)
  chain <- function(u) {
    out <- character(0)
    d <- dn[[u]]
    while (d != "") {
      stopifnot(!d %in% out, length(out) < nrow(x))
      out <- c(out, d)
      d <- dn[[d]]
    }
    if (length(out) == 0) "" else paste0("['", paste(out, collapse = "', '"), "']")
  }
  x$encompassing_mainstem_basins <- vapply(x$uri, chain, "", USE.NAMES = FALSE)

  # NHD carries gnis_id -1 on names with no GNIS feature (e.g. Canadian
  # names on transboundary reaches); keep the name, leave the id blank.
  gnis_uri <- function(g) {
    g <- suppressWarnings(as.integer(g))
    ifelse(is.na(g) | g <= 0, "", paste0("https://geoconnex.us/usgs/gnis/", g))
  }
  blank <- function(v) ifelse(is.na(v), "", v)

  x <- dplyr::mutate(x,
    id = as.character(id),
    featuretype = "['https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath', 'https://www.opengis.net/def/schema/hy_features/hyf/HY_WaterBody']",
    name_at_outlet = blank(name_at_outlet),
    name_at_outlet_gnis_id = gnis_uri(name_at_outlet_gnis_id),
    primary_name = blank(primary_name),
    primary_name_gnis_id = gnis_uri(primary_name_gnis_id),
    outlet_drainagearea_sqkm = NA_real_,
    head_nhdpv2_COMID = "", outlet_nhdpv2_COMID = "",
    head_nhdplushr_id = "", outlet_nhdplushr_id = "",
    head_nhdpv2HUC12 = "", outlet_nhdpv2HUC12 = "",
    head_rf1ID = NA_integer_, outlet_rf1ID = NA_integer_,
    head_nhdpv1_COMID = NA_integer_, outlet_nhdpv1_COMID = NA_integer_,
    head_2020HUC12 = "", outlet_2020HUC12 = "",
    superseded = FALSE,
    new_mainstemid = "")

  sf::st_geometry(x) <- "geom"
  if (sf::st_crs(x) != sf::st_crs(4326)) x <- sf::st_transform(x, 4326)

  stopifnot(all(x$head_nhd_permid != "" & x$outlet_nhd_permid != ""))

  # column order of make_clean_mainstems()
  dplyr::select(x, id, uri, featuretype, downstream_mainstem_id,
                encompassing_mainstem_basins, name_at_outlet,
                name_at_outlet_gnis_id, primary_name, primary_name_gnis_id,
                lengthkm, outlet_drainagearea_sqkm,
                head_nhdpv2_COMID, outlet_nhdpv2_COMID,
                head_nhdplushr_id, outlet_nhdplushr_id,
                head_nhd_permid, outlet_nhd_permid,
                head_nhdpv2HUC12, outlet_nhdpv2HUC12,
                head_rf1ID, outlet_rf1ID,
                head_nhdpv1_COMID, outlet_nhdpv1_COMID,
                head_2020HUC12, outlet_2020HUC12,
                superseded, new_mainstemid)
}

#' Build the NHD Permanent Identifier Lookup for OCONUS Mainstems
#'
#' Every member flowline of every OCONUS mainstem, with its 3DHP id.
#' Membership is the 2024 release's `nhd_3dhp_oconus_mainstems.csv` (flowline
#' to usgs mainstem); `id3dhp` comes from its `nhd_final_registry.csv`; the
#' usgs to ref mapping is the tracked id table.
#'
#' @param membership path to `nhd_3dhp_oconus_mainstems.csv`.
#' @param registry path to `nhd_final_registry.csv`.
#' @param id_table path to `registry/oconus_ids.csv`.
#'
#' @return data.frame with `uri`, `permanent_identifier`, `id3dhp`.
build_nhd_permid_lookup <- function(membership, registry, id_table) {
  ids <- readr::read_csv(id_table, col_types = "iccc")

  mem <- data.table::fread(membership, colClasses = "character",
                           col.names = c("permanent_identifier", "usgs_mainstem"))
  mem <- mem[mem$usgs_mainstem %in% ids$usgs_mainstem, ]

  reg <- data.table::fread(registry, select = c("permanent_identifier", "id3dhp"),
                           colClasses = "character")
  reg <- reg[reg$permanent_identifier %in% mem$permanent_identifier, ]

  out <- data.frame(
    uri = paste0("https://geoconnex.us/ref/mainstems/",
                 ids$mainstem[match(mem$usgs_mainstem, ids$usgs_mainstem)]),
    permanent_identifier = mem$permanent_identifier,
    id3dhp = reg$id3dhp[match(mem$permanent_identifier, reg$permanent_identifier)],
    stringsAsFactors = FALSE)

  stopifnot(setequal(unique(mem$usgs_mainstem), ids$usgs_mainstem))
  stopifnot(!any(duplicated(out$permanent_identifier)), !anyNA(out$id3dhp))

  out
}

# The 2024 release writes permanent identifiers as long integers or GUIDs,
# about 40% of GUIDs in braces and some in lower case; NHD spells them
# differently again. For matching only; outputs keep the release's spelling.
norm_pid <- function(x) toupper(gsub("[{}]", "", x))

oconus_region <- function(hu4) {
  data.table::fcase(substr(hu4, 1, 2) == "19", "alaska",
                    substr(hu4, 1, 2) == "20", "hawaii",
                    substr(hu4, 1, 2) == "21", "puerto_rico_vi",
                    hu4 %in% c("2201", "2202"), "guam_cnmi",
                    hu4 == "2203", "american_samoa",
                    default = paste0("other_", hu4))
}

#' Read NHD Flowline Names
#'
#' @param sources paths to the NHD HU4 sources from [get_nhd_hu4()].
#'
#' @return data.table with one row per normalised `permanent_identifier`:
#'   `gnis_id`, `gnis_name`, `hu4`, `region`. A flowline at an HU4 boundary
#'   in two sources keeps the row from the first.
read_nhd_flowlines <- function(sources) {
  fl <- data.table::rbindlist(lapply(sources, function(src) {
    q <- "SELECT permanent_identifier, gnis_id, gnis_name FROM NHDFlowline"
    x <- data.table::as.data.table(sf::st_drop_geometry(sf::read_sf(src, query = q)))
    x[, `:=`(gnis_id = as.character(gnis_id),
             hu4 = regmatches(basename(src), regexpr("[0-9]{4}", basename(src))))]
  }))
  fl[, permanent_identifier := norm_pid(permanent_identifier)]
  fl <- fl[!duplicated(permanent_identifier)]
  fl[, region := oconus_region(hu4)]
  fl
}

#' Read OCONUS Mainstem Membership
#'
#' @param membership path to `nhd_3dhp_oconus_mainstems.csv`.
#' @param registry path to `nhd_final_registry.csv`.
#'
#' @return data.table, one row per member flowline: normalised
#'   `permanent_identifier`, `release_pid` (the release's spelling),
#'   `mainstemid` (usgs mainstem uri), `id3dhp`, `lengthkm`.
read_oconus_members <- function(membership, registry) {
  mem <- data.table::fread(membership, colClasses = "character",
                           col.names = c("permanent_identifier", "mainstemid"))
  mem[, release_pid := permanent_identifier]
  mem[, permanent_identifier := norm_pid(permanent_identifier)]
  stopifnot(!anyDuplicated(mem$permanent_identifier))

  reg <- data.table::fread(registry, select = c("permanent_identifier", "id3dhp", "lengthkm"),
                           colClasses = c(permanent_identifier = "character",
                                          id3dhp = "character"))
  reg[, permanent_identifier := norm_pid(permanent_identifier)]
  mem[reg, `:=`(id3dhp = i.id3dhp, lengthkm = i.lengthkm), on = "permanent_identifier"]
  mem
}

#' Select OCONUS Mainstems
#'
#' The minting rule applied to the 2024 release's
#' OCONUS usgs mainstems with names from the final NHD snapshot. A mainstem
#' is selected if it is longer than its domain's threshold and more than 90%
#' named, or (Alaska) longer than 100 km and more than 50% named, and then
#' every mainstem downstream of a selected one. A mainstem that shares a
#' flowline with the 2024 non-network minting is not eligible.
#'
#' @param flowlines from [read_nhd_flowlines()].
#' @param members from [read_oconus_members()].
#' @param network path to `nhd_3dhp_oconus_flownetwork.csv`.
#' @param nonnetwork path to `nhd_3dhp_nonnetwork_mainstems.csv`.
#'
#' @return data.table, one row per selected usgs mainstem.
select_oconus_mainstems <- function(flowlines, members, network, nonnetwork) {
  mem <- data.table::copy(members)
  mem[flowlines, `:=`(in_gdb = TRUE, gnis_id = i.gnis_id, gnis_name = i.gnis_name,
                      region = i.region), on = "permanent_identifier"]
  mem[is.na(in_gdb), in_gdb := FALSE]
  mem[, named := !is.na(gnis_name) & nzchar(trimws(gnis_name))]

  ms <- mem[, {
    nk <- lengthkm[named]
    # primary name: the name covering the most length; its GNIS id is the
    # positive gnis_id covering the most of that name (NHD carries -1 on
    # Canadian reaches of transboundary rivers)
    top <- if (any(named)) {
      s <- tapply(nk, gnis_name[named], sum, na.rm = TRUE)
      nm <- names(s)[which.max(s)]
      ok <- named & gnis_name == nm & suppressWarnings(as.numeric(gnis_id)) > 0
      ok[is.na(ok)] <- FALSE
      gid <- if (any(ok)) {
        t <- tapply(lengthkm[ok], gnis_id[ok], sum, na.rm = TRUE)
        names(t)[which.max(t)]
      } else NA_character_
      c(gid, nm)
    } else c(NA_character_, NA_character_)
    .(region      = names(which.max(table(region, useNA = "ifany")))[1],
      n_flowlines = .N,
      n_in_gdb    = sum(in_gdb),
      n_no_length = sum(is.na(lengthkm)),
      length_km   = sum(lengthkm, na.rm = TRUE),
      named_km    = sum(nk, na.rm = TRUE),
      top_gnis_id = top[1],
      top_name    = top[2])
  }, by = mainstemid]
  ms[, pct_named := data.table::fifelse(length_km > 0, 100 * named_km / length_km, 0)]

  # downstream mainstem, from downmain edges whose two ends sit on different
  # mainstems
  net <- data.table::fread(network, colClasses = "character")[downmain == "TRUE"]
  net[, `:=`(from = norm_pid(from_permanent_identifier), to = norm_pid(to_permanent_identifier))]
  net[mem, from_ms := i.mainstemid, on = c(from = "permanent_identifier")]
  net[mem, to_ms := i.mainstemid, on = c(to = "permanent_identifier")]
  ms_down <- unique(net[!is.na(from_ms) & !is.na(to_ms) & from_ms != to_ms,
                        .(mainstemid = from_ms, dn_mainstemid = to_ms)])

  nonnet_pid <- norm_pid(data.table::fread(nonnetwork, colClasses = "character")$id)
  shared <- mem[permanent_identifier %in% nonnet_pid, unique(mainstemid)]
  ms[, nonnetwork_overlap := mainstemid %in% shared]

  min_km <- c(alaska = 50, hawaii = 10, puerto_rico_vi = 10, guam_cnmi = 4)
  ms[, min_km := min_km[region]]
  ms[, reason := NA_character_]
  ms[!is.na(min_km) & !nonnetwork_overlap & length_km > min_km & pct_named > 90,
     reason := "threshold"]
  ms[is.na(reason) & !nonnetwork_overlap & region == "alaska" & length_km > 100 & pct_named > 50,
     reason := "alaska 100 km, 50% named"]
  sel <- ms[!is.na(reason), mainstemid]
  repeat {
    dn <- ms_down[mainstemid %in% sel & !(dn_mainstemid %in% sel), unique(dn_mainstemid)]
    if (length(dn) == 0) break
    ms[mainstemid %in% dn, reason := "downstream of a minted mainstem"]
    sel <- c(sel, dn)
  }
  ms <- ms[!is.na(reason)]

  # every member of every selected mainstem has a name source and a length
  stopifnot(!any(ms$nonnetwork_overlap), all(ms$n_in_gdb == ms$n_flowlines),
            all(ms$n_no_length == 0))

  ms[, .(mainstemid, region, reason, top_name, top_gnis_id, length_km, pct_named)]
}

#' Build the OCONUS Mainstem Batch
#'
#' Head, outlet, names, downstream usgs mainstem and head-to-outlet geometry
#' for each selected mainstem. Head and outlet come from the 2024 release flow
#' network: the outlet is the member whose downmain edge leaves the mainstem
#' or that has none; the head is the member with no upmain edge arriving from
#' inside the mainstem. Geometry is simplified at 50 m per domain. Stops if a
#' mainstem lacks exactly one head and outlet or if the downmain walk from
#' head to outlet does not visit every member.
#'
#' @param selection from [select_oconus_mainstems()].
#' @param members from [read_oconus_members()].
#' @param flowlines from [read_nhd_flowlines()].
#' @param network path to `nhd_3dhp_oconus_flownetwork.csv`.
#' @param sources paths to the NHD HU4 sources from [get_nhd_hu4()].
#'
#' @return `sf` in EPSG:4326, one row per usgs mainstem, head and outlet
#'   permanent identifiers spelled as in the 2024 release.
build_oconus_batch <- function(selection, members, flowlines, network, sources) {
  ms <- selection
  mem <- members[mainstemid %in% ms$mainstemid]
  stopifnot(!anyNA(mem$id3dhp), !anyNA(mem$lengthkm))
  mem[flowlines, `:=`(gnis_id = i.gnis_id, gnis_name = i.gnis_name), on = "permanent_identifier"]

  net <- data.table::fread(network, colClasses = "character")
  net[, `:=`(from = norm_pid(from_permanent_identifier), to = norm_pid(to_permanent_identifier),
             upmain = upmain == "TRUE", downmain = downmain == "TRUE")]
  net[mem, from_ms := i.mainstemid, on = c(from = "permanent_identifier")]
  net[mem, to_ms := i.mainstemid, on = c(to = "permanent_identifier")]
  net <- net[!is.na(from_ms) | !is.na(to_ms), .(from, to, upmain, downmain, from_ms, to_ms)]

  dm <- net[downmain == TRUE & !is.na(from_ms)]
  # a flowline has at most one downmain edge
  stopifnot(!anyDuplicated(dm$from))
  internal_dm <- dm[!is.na(to_ms) & from_ms == to_ms]
  internal_um_to <- net[upmain == TRUE & !is.na(to_ms) & !is.na(from_ms) & from_ms == to_ms,
                        unique(to)]

  mem[, outlet := !permanent_identifier %in% internal_dm$from]
  mem[, head := !permanent_identifier %in% internal_um_to]
  succ <- setNames(internal_dm$to, internal_dm$from)

  ho <- mem[, .(n = .N, n_head = sum(head), n_outlet = sum(outlet),
                head_permid = permanent_identifier[head][1],
                outlet_permid = permanent_identifier[outlet][1]), by = mainstemid]
  bad <- ho[n_head != 1 | n_outlet != 1]
  if (nrow(bad)) {
    print(bad)
    stop(nrow(bad), " selected mainstems lack exactly one head and one outlet")
  }

  walk <- function(h, o, n) {
    path <- character(n); path[1] <- h; i <- 1L
    while (path[i] != o && i < n) {
      nx <- succ[path[i]]
      if (is.na(nx)) break
      i <- i + 1L; path[i] <- nx
    }
    path[seq_len(i)]
  }
  ord <- data.table::rbindlist(lapply(seq_len(nrow(ho)), function(k) {
    p <- walk(ho$head_permid[k], ho$outlet_permid[k], ho$n[k])
    data.table::data.table(mainstemid = ho$mainstemid[k], permanent_identifier = p,
                           seq = seq_along(p),
                           complete = length(p) == ho$n[k] && p[length(p)] == ho$outlet_permid[k])
  }))
  inc <- unique(ord[complete == FALSE, mainstemid])
  if (length(inc)) {
    print(inc)
    stop(length(inc), " selected mainstems: downmain walk from head misses members")
  }

  # downstream usgs mainstem: the mainstem the outlet's downmain edge reaches
  ho[dm, dn_mainstemid := i.to_ms, on = c(outlet_permid = "from")]
  ho[is.na(dn_mainstemid), dn_mainstemid := ""]

  # geometry
  want <- ord$permanent_identifier
  geo <- do.call(rbind, lapply(sources, function(g) {
    gcol <- attr(sf::read_sf(g, query = "SELECT * FROM NHDFlowline LIMIT 1"), "sf_column")
    x <- sf::read_sf(g, query = sprintf("SELECT permanent_identifier, %s FROM NHDFlowline", gcol))
    x$permanent_identifier <- norm_pid(x$permanent_identifier)
    x <- x[x$permanent_identifier %in% want & !duplicated(x$permanent_identifier), ]
    x <- sf::st_cast(sf::st_transform(sf::st_zm(x), 4326), "MULTILINESTRING")
    sf::st_set_geometry(x, "geom")
  }))
  geo <- geo[!duplicated(geo$permanent_identifier), ]
  stopifnot(all(want %in% geo$permanent_identifier))
  parts <- lengths(lapply(sf::st_geometry(geo), unclass))
  stopifnot(all(parts == 1))
  geo <- suppressWarnings(sf::st_cast(geo, "LINESTRING"))

  geo <- merge(geo, ord[, .(permanent_identifier, mainstemid, seq)], by = "permanent_identifier")
  geo <- geo[order(geo$mainstemid, geo$seq), ]
  lines <- collapse_lines(split(geo, factor(geo$mainstemid, levels = ho$mainstemid)))

  # attributes
  out_attr <- mem[ho[, .(mainstemid, outlet_permid)], on = c(permanent_identifier = "outlet_permid"),
                  .(mainstemid = i.mainstemid, name_at_outlet = gnis_name,
                    name_at_outlet_gnis_id = gnis_id)]
  out <- ms[ho, on = "mainstemid", .(
    usgs_mainstem = mainstemid, domain = region, reason,
    head_nhd_permid = i.head_permid, outlet_nhd_permid = i.outlet_permid,
    primary_name = top_name, primary_name_gnis_id = top_gnis_id,
    lengthkm = length_km, downstream_usgs_mainstem = i.dn_mainstemid)]
  out <- merge(out, out_attr, by.x = "usgs_mainstem", by.y = "mainstemid", sort = FALSE)
  stopifnot(identical(out$usgs_mainstem, ho$mainstemid))
  out[, head_nhd_permid := mem$release_pid[match(head_nhd_permid, mem$permanent_identifier)]]
  out[, outlet_nhd_permid := mem$release_pid[match(outlet_nhd_permid, mem$permanent_identifier)]]
  stopifnot(!anyNA(out$head_nhd_permid), !anyNA(out$outlet_nhd_permid))
  stopifnot(!anyDuplicated(out$head_nhd_permid), !anyDuplicated(out$outlet_nhd_permid))
  dn <- out$downstream_usgs_mainstem[out$downstream_usgs_mainstem != ""]
  stopifnot(all(dn %in% out$usgs_mainstem))
  out <- sf::st_sf(tibble::as_tibble(out), geom = lines)

  stopifnot(all(sf::st_geometry_type(out) == "LINESTRING"))
  rel <- abs(as.numeric(sf::st_length(out)) / 1000 - out$lengthkm) / out$lengthkm
  stopifnot(all(rel < 0.02))

  # simplify in a projected CRS per domain
  crs <- c(alaska = 3338, hawaii = 26904, puerto_rico_vi = 32620, guam_cnmi = 32655)
  simp <- do.call(rbind, lapply(split(out, out$domain), function(d)
    sf::st_transform(sf::st_simplify(sf::st_transform(d, crs[[d$domain[1]]]),
                                     dTolerance = 50), 4326)))
  simp <- simp[match(out$usgs_mainstem, simp$usgs_mainstem), ]
  stopifnot(all(sf::st_geometry_type(simp) == "LINESTRING"), !any(sf::st_is_empty(simp)))
  simp
}
