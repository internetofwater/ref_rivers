# to run interactively, use:
# library(targets)
# library(dplyr)
# library(sf)
# enhd_v3 <- tar_read("enhd_v3")
# ref_rivers <- tar_read("ref_rivers_v30")
# new_net <- tar_read("ref_net_v1")
# hr_net <- tar_read("hr_net")
# changes <- tar_read("reconciled_mainstems")
# out_f <- "out/mainstems.gpkg"
# source("R/get_data.R")

initialize_mainstems <- function(enhd_v3, ref_rivers, new_net, hr_net, changes) {
  ref_rivers <- sf::read_sf(ref_rivers, "mainstems")
  new_net <- sf::read_sf(new_net)
  enhd_v3 <- arrow::read_parquet(enhd_v3)
  hr_net <- readr::read_csv(hr_net)

  lpv3_lookup_file <- "out/lpv3_lookup.csv"
  
  # this is a lookup table from lp_mainstem_v3 used in the new_net and current reference mainstem
  # It's not used here and is only for validation. lp_v3 is on the way out
  lookups <- readr::read_csv(lpv3_lookup_file, col_types = "cc")

  # https://github.com/internetofwater/ref_rivers/issues/15
  drop_ms <- c(
    "https://geoconnex.us/ref/mainstems/2183898", # this mainstem goes missing?
    "https://geoconnex.us/ref/mainstems/2095401", 
    "https://geoconnex.us/ref/mainstems/988957", 
    "https://geoconnex.us/ref/mainstems/1418010", 
    "https://geoconnex.us/ref/mainstems/101089", 
    "https://geoconnex.us/ref/mainstems/2526870", 
    "https://geoconnex.us/ref/mainstems/2009135", 
    "https://geoconnex.us/ref/mainstems/260773",
      # https://github.com/internetofwater/ref_rivers/issues/12
    "https://geoconnex.us/ref/mainstems/1171783",
    "https://geoconnex.us/ref/mainstems/35294",
    "https://geoconnex.us/ref/mainstems/693625",
    "https://geoconnex.us/ref/mainstems/794163",
    "https://geoconnex.us/ref/mainstems/2623369",
    "https://geoconnex.us/ref/mainstems/1951375",
    "https://geoconnex.us/ref/mainstems/2345807"
  )

  stopifnot(all(new_net$id == trimws(new_net$id)))
  stopifnot(all(new_net$toid == trimws(new_net$toid)))

  # TODO: remove this once reference mainstems is refreshed
  if(any(ref_rivers$head_nhdplushr_id %in% hr_net$permid)) {
    hr_ids <- distinct(select(hr_net, nhdplushrid = id, permid))

    ref_rivers$head_nhdplushr_id[ref_rivers$head_nhdplushr_id %in% hr_ids$permid] <- 
      hr_ids$nhdplushrid[match(ref_rivers$head_nhdplushr_id[ref_rivers$head_nhdplushr_id %in% hr_ids$permid], hr_ids$permid)]
  }

  # this was dropped from the reference network 
  ref_rivers$superseded[ref_rivers$uri == "https://geoconnex.us/ref/mainstems/2183898"] <- TRUE

  # check that the list is all stuff that we are planning to remove
  # stopifnot(any(!lookups$uri[!lookups$lp_mainstem_v3 %in% new_net$lp_mainstem_v3] %in% drop_ms))
  
  stopifnot(all(drop_ms %in% ref_rivers$uri))

  # run validations and fix up new_net ids
  new_net <- validate_ms_inputs(ref_rivers, new_net, hr_net, lookups, drop_ms)
  
  # Filter to rows with valid lp_mainstem_v3 and join downstream levelpath info, removing disconnected mainstems.
  new_net_nolp <- get_new_net_nolp(new_net)

  # Extract distinct downstream mainstem relationships, resolving duplicates and validating connectivity.
  new_dm <- get_new_dm(new_net_nolp, enhd_v3)

  # Reconcile superseded, kept, deprecated, added, and replaced mainstems into a unified output dataset.
  ms_out <- get_ms_out(ref_rivers, changes)

  # Verify each outlet id is a member of its assigned levelpath, fixing discrepancies where needed.
  ms_out <- clean_outlet(ms_out, new_net)

  # Combine mainstem data with NHDPlusHR source info and construct HR mainstem geometries in parallel.
  ms_out <- add_hr_mainstems(ms_out, new_net, get_nhdphr_source_extra(new_net_nolp), changes)

  # Match mainstems with existing reference river ids based on headwater locations to assign uris.
  ms_out <- add_ref_uri(ms_out, ref_rivers, drop_ms)

  # Join downstream mainstem relationships and remove orphaned mainstems lacking valid connections.
  ms_out <- add_dn_ms(ms_out, new_dm, ref_rivers, drop_ms)

  # Identify available mainstem ids in the sequence and assign them to new mainstems.
  ms_out <- assign_new_ms_ids(ms_out)

  # Add downstream mainstem IDs by looking up output URIs keyed by levelpath.
  ms_out <- add_dm_ms_id(ms_out)

  # https://github.com/internetofwater/ref_rivers/issues/15
  ms_out$superseded[ms_out$uri %in% drop_ms] <- TRUE

  # quadruple check
  stopifnot(!any(is.na(ms_out$uri)))
  stopifnot(!any(is.na(ms_out$id)))
  stopifnot(all(ref_rivers$uri %in% ms_out$uri))

  # somehow these get flipped to NA Maybe track down where?
  ms_out$head_nhdpv2_COMID[ms_out$uri == "https://geoconnex.us/ref/mainstems/2183898"] <- ""
  ms_out$outlet_nhdpv2_COMID[ms_out$uri == "https://geoconnex.us/ref/mainstems/2183898"] <- ""

  ms_out
}

write_lp_v3_lookup <- function(ms_out, lpv3_lookup_file = "out/lpv3_lookup.csv") {
  # write lpv3_lookup 
  st_drop_geometry(ms_out) |>
    filter(!superseded) |>
    select(uri, lp_mainstem_v3) |>
    distinct() |>
    readr::write_csv(lpv3_lookup_file)

  lpv3_lookup_file
}

make_mainstems <- function(ms_out) {

  ms_out <- make_clean_mainstems(ms_out)

}

add_dm <- function(network) {
  
  lp <- select(st_drop_geometry(network), levelpathi, dnlevelpat) |>
    filter(!is.na(levelpathi) & levelpathi != dnlevelpat)
  
  dnlp <- data.frame(lp = seq(0, (max(lp$levelpathi)))) |>
    left_join(lp, by = c("lp" = "levelpathi"))
  
  dnlp <- dnlp$dnlevelpat[2:nrow(dnlp)]
  
  get_dlp <- function(x, dnlp) {
    out <- dnlp[x]
    
    if(is.na(out) || out == 0) {
      return()
    }
    
    c(out, get_dlp(out, dnlp))
    
  }
  
  all_lp <- unique(lp$levelpathi)
  
  all_lp <- pbapply::pblapply(all_lp, get_dlp, dnlp = dnlp)

  all_lp <- data.frame(levelpathi = unique(lp$levelpathi),
                       dnlp = sapply(all_lp, function(x) if(length(x) > 0) {
                         paste0("['", paste(paste0("https://geoconnex.us/ref/mainstems/", x), 
                                            collapse = "', '"), "']")
                         } else NA))
  
  network |>
    left_join(all_lp, by = "levelpathi") |>
    rename(down_levelpaths = dnlp)
}

split_number_chunks <- function(x, n) {
  split(x, cut(seq_along(x), n, labels = FALSE))
}

collapse_lines <- function(g) {
  
  get_single_line <- function(gg) {
    sf::st_sfc(
      sf::st_linestring(
        sf::st_coordinates(sf::st_geometry(gg))[, 1:2]), 
      crs = sf::st_crs(gg))
  }
  
  do.call(c, lapply(g, get_single_line))
  
}

# library(targets)
# library(dplyr)
# library(sf)
# mainstems <- sf::read_sf("out/mainstems.gpkg", "mainstems")
# new_net <- tar_read("ref_net_v1")
# lookup <- "out/nhdpv2_lookup.csv"
make_nonref <- function(mainstems, new_net, lookup, out_f = "out/extra_mainstems.gpkg") {
  new_net <- sf::read_sf(new_net)
  lookup <- readr::read_csv(lookup)
  
  lookup$id <- paste0("nhdpv2-", lookup$comid)

  extra_geom <- dplyr::bind_rows(new_net[new_net$source == "nhdphr" & is.na(new_net$lp_mainstem_v3),], 
                                 new_net[new_net$source == "nhdpv2" & !new_net$id %in% lookup$id,]) |>
    hydroloom::add_topo_sort() |>
    mutate(levelpath = paste0(vector_proc_unit, levelpath)) |>
    arrange(desc(topo_sort)) |>
    select(-vector_proc_unit, -topo_sort)
  
  out <- sf::st_drop_geometry(extra_geom) |>
    select(id, toid, levelpath, length_km, total_da_sqkm, gnis_name)
  
  common_name <- select(out, levelpath, gnis_name) |>
    filter(gnis_name != "" & !is.na(gnis_name)) |>
    add_count(levelpath, gnis_name) |>
    group_by(levelpath) |>
    mutate(common_name = gnis_name[n == max(n)][1]) |>
    select(-all_of(c("n", "gnis_name"))) |>
    ungroup() |>
    distinct()
  
  out <- left_join(out, common_name, by = "levelpath") |>
    group_by(levelpath) |>
    summarize(levelpath = levelpath[1],
              name_at_outlet = gnis_name[n()],
              common_GNIS_NAME = common_name[1],
              head_id = id[1],
              outlet_id = id[n()],
              lengthkm = sum(length_km),
              outlet_drainagearea_sqkm = total_da_sqkm[1])
    
  extra_geom <- extra_geom |>
    select(levelpath) |>
    group_by(levelpath) |>
    group_split(.keep = TRUE) |>
    split_number_chunks(80)
  
  cl <- parallel::makeCluster(12)
  on.exit(parallel::stopCluster(cl))
  
  ids <- do.call(c, lapply(extra_geom, \(x) unique(do.call(c, lapply(x, \(y) y$levelpath)))))

  extra_geom <- do.call(c, pbapply::pblapply(extra_geom, collapse_lines, cl = cl))
  
  out <- left_join(out, sf::st_sf(levelpath = ids,
                              geom = extra_geom),
                   by = "levelpath")  
  
  sf::write_sf(out, out_f, "extra_mainstems")
}

validate_mainstems <- function(ms_out) {
  
  # must be 4326
  stopifnot(sf::st_crs(ms_out) == sf::st_crs(4326))
  
  # must be LINESTRING
  stopifnot(sf::st_geometry_type(ms_out, by_geometry = FALSE) == "LINESTRING")
  
  # expect ID to be in character format
  stopifnot(is.character(ms_out$id))
  
  # names need to include all of these (geometry has been removed)
  stopifnot(all(c(
    "id", "uri", "featuretype", "downstream_mainstem_id", "encompassing_mainstem_basins", 
"name_at_outlet", "name_at_outlet_gnis_id", "primary_name", "primary_name_gnis_id", 
"lengthkm", "outlet_drainagearea_sqkm", "head_nhdpv2_COMID", 
"outlet_nhdpv2_COMID", "head_nhdplushr_id", "outlet_nhdplushr_id", 
"head_nhd_permid", "outlet_nhd_permid", "head_nhdpv2HUC12", "outlet_nhdpv2HUC12", 
"head_rf1ID", "outlet_rf1ID", "head_nhdpv1_COMID", "outlet_nhdpv1_COMID", 
"head_2020HUC12", "outlet_2020HUC12", "superseded", "new_mainstemid") %in% names(ms_out)))
 
  check_uri <- function(x, rgx, missing = "") {

    if(isFALSE(missing)) {
      stopifnot(all(grepl(rgx, x)))
    } else {
      stopifnot(all(grepl(rgx, x) | x == missing))
    }
  }
  
  # URI checks
  check_uri(ms_out$uri, "^https://geoconnex\\.us/ref/mainstems/\\d+$", FALSE)
  check_uri(ms_out$name_at_outlet_gnis_id, "^https://geoconnex\\.us/usgs/gnis/\\d+$", "")
  check_uri(ms_out$primary_name_gnis_id, "^https://geoconnex\\.us/usgs/gnis/\\d+$", "")
  check_uri(ms_out$head_nhdpv2_COMID, "^https://geoconnex\\.us/nhdplusv2/comid/\\d+$", "")
  check_uri(ms_out$outlet_nhdpv2_COMID, "^https://geoconnex\\.us/nhdplusv2/comid/\\d+$", "") 
  check_uri(ms_out$head_nhdpv2HUC12, "^https://geoconnex\\.us/nhdplusv2/huc12/\\d+$", "")
  check_uri(ms_out$outlet_nhdpv2HUC12, "^https://geoconnex\\.us/nhdplusv2/huc12/\\d+$", "")

  # all new mainstemid must be not superseded
  # checks that none of the new_mainsteid entries are superseded 
  stopifnot(!all(unlist(ms_out$new_mainstemid) %in% ms_out$uri[!ms_out$superseded]))

  # all mainstem topology goes to stuff that exists
  # checks that none of the active topology goes to superseded mainstems
  stopifnot(!all(unlist(ms_out$downstream_mainstem_id) %in% ms_out$uri[!ms_out$superseded]))
  stopifnot(!all(unlist(ms_out$encompassing_mainstem_basins) %in% ms_out$uri[!ms_out$superseded]))

  # length must be positive and less than 5000km
  stopifnot(all(ms_out$lengthkm > 0 & ms_out$lengthkm < 5000))

  # more lenient on drainage area
  stopifnot(all(is.na(ms_out$outlet_drainagearea_sqkm) | (ms_out$outlet_drainagearea_sqkm >= 0 & ms_out$outlet_drainagearea_sqkm < 3e6)))

  TRUE
}

# Also defined in https://code.usgs.gov/wma/nhgf/reference-fabric/reference-network
  idf <- function(ids, prefix = "") {
    if(is.numeric(ids)) ids <- floor(ids)
    ifelse(is.na(ids), NA_character_, 
      trimws(paste0(prefix, format(ids, trim = TRUE, scientific = FALSE))))
  }

validate_ms_inputs <- function(ref_rivers, new_net, hr_net, lookups, drop_ms, add_extra_lookup = "data/review/deprecated_lookup.csv") {

  hr_ids <- distinct(select(hr_net, nhdplushrid = id, permid))
  
  # make sure out join keys are unique
  stopifnot(!any(duplicated(hr_ids$permid)))
  stopifnot(!any(duplicated(new_net$id)))

  # must not have scientific notation in ids
  stopifnot(!any(grepl("e", new_net$lp_mainstem_v3)))
  
  stopifnot(all(new_net$toid == "" | new_net$toid %in% new_net$id)) # verify that all outlets are ""
  
  # expect the lookup to have all current mainstems
  # stopifnot(all(ref_rivers$uri[!ref_rivers$superseded] %in% c(lookups$uri, drop_ms)))
  
  # these are where we needed to add a few extra mainstems to ensure downstream connectivity.
  # they should all be present from lookups.
  add_extra <- readr::read_csv(add_extra_lookup)

  add_extra$id <- idf(add_extra$head_nhdplushr_id, "nhdphr-")

  stopifnot(all(add_extra$id %in% new_net$id))

  # only relevant where there is no lp_mainstem_v3
  add_extra_lps <- filter(add_extra, id %in% new_net$id[is.na(new_net$lp_mainstem_v3)])

  add_extra_lps <- unique(new_net$levelpath[new_net$id %in% add_extra_lps$id])

  add_extra_lps <- data.frame(levelpath = add_extra_lps, lp_mainstem_v3 = format(seq(8e6, 8e6 + length(add_extra_lps) - 1), scientific = FALSE))

  ## need to grab what is being replaced and discount it from lookups ##
  dplyr::rows_update(as.data.frame(new_net), add_extra_lps, by = "levelpath") |> sf::st_sf()
}

get_new_net_nolp <- function(new_net) {
  # we are only considering where lp_mainstem_v3 is populated
  # NOTE that some lp_mainstem_v3 values in this are newly introduced and will not join 
  # to the v2 enhd network.
  new_net_nolp <- filter(sf::st_drop_geometry(new_net), !is.na(lp_mainstem_v3))
  
  # add down levelpath to reference network
  new_net_nolp <- left_join(new_net_nolp, distinct(select(st_drop_geometry(new_net_nolp), 
  id, dnlpv3 = lp_mainstem_v3)), 
  by = c("toid" = "id"))
  
  ################# 
  # work out downstream mainstems using new_net as the source for consistency
  # these are where the dnlpv3 is NA but the network connects. 
  # we don't want to keep mainstems like this.
  #################
  
  disconnected <- new_net_nolp$id[new_net_nolp$toid != "" & is.na(new_net_nolp$dnlpv3) & new_net_nolp$lp_mainstem_v3 < 7e6]
  
  # get all the network above these disconnects
  above_disconnect <- unlist(hydroloom::navigate_network_dfs(new_net_nolp, disconnected, direction = "up"))
  
  avoid <- new_net_nolp$id[as.numeric(new_net_nolp$lp_mainstem_v3) > 7e6]
  
  remove <- filter(new_net_nolp, id %in% above_disconnect & !id %in% avoid)
  
  new_net_nolp <- filter(new_net_nolp, !id %in% remove$id)
  
  # validate this fix 5 on 1/19/26
  stopifnot(nrow(new_net_nolp[new_net_nolp$toid != "" & is.na(new_net_nolp$dnlpv3),]) >= 5)

  new_net_nolp
}

get_new_dm <- function(new_net_nolp, enhd_v3) {
  
  new_dm <- distinct(select(new_net_nolp, 
                            dnlpv3, lp_mainstem_v3)) |>
    mutate(dnlpv3 = tidyr::replace_na(as.numeric(dnlpv3), 0),
           lp_mainstem_v3 = as.numeric(lp_mainstem_v3)) |>
    filter(lp_mainstem_v3 != dnlpv3)
  
  dup_down <- filter(new_dm, lp_mainstem_v3 %in% lp_mainstem_v3[duplicated(lp_mainstem_v3)])
  
  enhd_v3_dnlp <- select(enhd_v3, lp_mainstem_v3 = levelpathi, dnlpv3_enhd = dnlevelpat) |>
    filter(lp_mainstem_v3 != dnlpv3_enhd)
  
  # must be distinct
  stopifnot(length(unique(enhd_v3_dnlp$lp_mainstem_v3)) == nrow(enhd_v3_dnlp))
  
  dup_down <- left_join(dup_down, enhd_v3_dnlp, by = "lp_mainstem_v3")
  
  dup_down_nomatch <- group_by(dup_down, lp_mainstem_v3) |>
    filter(!any(dnlpv3 == dnlpv3_enhd))
  
  dup_down_match <- filter(dup_down, dnlpv3 == dnlpv3_enhd)
  
  override <- data.frame(lp_mainstem_v3 = c(148773, 313256, 2589752, 1919814, 2090617, 2119874, 2599701, 1881754, 8000017, 8000086, 8000092, 8000013, 8000076, 8000081, 8000012, 8000075, 8000080, 8000006, 8000011, 8000074, 8000079),
                         dnlpv3 = c(312191, 183508, 2589168, 1919808, 2090618, 2119871, 2592712, 7000028, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  
  dup_down_match <- bind_rows(dup_down_match, override) |>
    select(-dnlpv3_enhd)
  
  # must be distinct
  stopifnot(nrow(dup_down_match) == length(unique(dup_down_match$lp_mainstem_v3)))
  # must have all lp_mainstems
  stopifnot(all(dup_down$lp_mainstem_v3 %in% dup_down_match$lp_mainstem_v3))
  
  new_dm <- filter(new_dm, !lp_mainstem_v3 %in% dup_down_match$lp_mainstem_v3) |>
    bind_rows(dup_down_match) |>
    distinct()
  
  # must be unique
  stopifnot(!any(duplicated(new_dm$lp_mainstem_v3)))
  # must have all to navigate to
  stopifnot(all(new_dm$dnlpv3 == 0 | new_dm$dnlpv3 %in% new_dm$lp_mainstem_v3))

  new_dm
}

get_ms_out <- function(ref_rivers, changes) {
    ##################
  # reconcile and compile sources of mainstems
  ##################
  
  # category 0: previously superseded reference rivers
  pass_on <- filter(ref_rivers, superseded)
  
  # category 1: changes$keep
  # - these are all the mainstems that are staying the same
  # - don't need to do anything for these -- they just get passed through.
  changes$keep$superseded <- FALSE
  
  # category 2: changes$deprecate
  # - these deprecated mainstems have been verified through manual review.
  # - they are all in the NHDPlusHR domain
  changes$deprecate$superseded <- TRUE
  
  changes$deprecate <- rename(changes$deprecate, any_of(c(comment = "note")))
  # category 3: changes$add
  # - these are all the mainstems that are being added and are in the NHDPlusV2 domain
  # - They will need new mainstem ids
  changes$add$superseded <- FALSE
  
  # category 4: changes$nhdphr_source_replace
  # these are all the mainstems that are being replaced in the NHDPlusHR domain
  # See changes$changelog for changelog info
  stopifnot(all(changes$nhdphr_source_replace$lp_mainstem_v3 %in% changes$keep$lp_mainstem_v3))
  changes$nhdphr_source_replace$superseded <- FALSE
  
  changes$changelog$change <- paste0("January 2026 change: ", changes$changelog$change)
  
  changes$nhdphr_source_replace <- left_join(changes$nhdphr_source_replace, 
                                             dplyr::rename(changes$changelog, comment = change), by = "reference_mainstem")
  
  changes$keep <- filter(changes$keep, !lp_mainstem_v3 %in% changes$nhdphr_source_replace$lp_mainstem_v3)
  
  # category 5: changes$nhdphr_source_new
  # these are all the mainstems that are being added in the NHDPlusHR domain
  stopifnot(all(changes$nhdphr_source_new$lp_mainstem_v3 %in% changes$add$lp_mainstem_v3))
  changes$nhdphr_source_new$superseded <- FALSE
  
  changes$add <- filter(changes$add, !lp_mainstem_v3 %in% changes$nhdphr_source_new$lp_mainstem_v3)

  pass_on$head_nhdpv2_COMID <- paste0("https://geoconnex.us/nhdplusv2/comid/", trimws(format(as.integer(gsub("https://geoconnex.us/nhdplusv2/comid/", "", pass_on$head_nhdpv2_COMID)), scientific = FALSE), "both"))
  pass_on$outlet_nhdpv2_COMID <- paste0("https://geoconnex.us/nhdplusv2/comid/", trimws(format(as.integer(gsub("https://geoconnex.us/nhdplusv2/comid/", "", pass_on$outlet_nhdpv2_COMID)), scientific = FALSE), "both"))

  # bind together changes and get into schema of pass_on
  ms_out <- bind_rows(
    
    select(pass_on, -any_of(c("featuretype", "downstream_mainstem_id", "encompassing_mainstem_basins"))) |>
      hydroloom::st_compatibalize(changes$keep) |>
      mutate(id = as.integer(id),
             name_at_outlet_gnis_id = as.integer(gsub("https://geoconnex.us/usgs/gnis/", "", name_at_outlet_gnis_id))), 
    
    bind_rows(changes$keep,
              # hydroloom::st_compatibalize(changes$deprecate, changes$keep),
              hydroloom::st_compatibalize(changes$add, changes$keep),
              hydroloom::st_compatibalize(changes$nhdphr_source_replace, changes$keep),
              hydroloom::st_compatibalize(changes$nhdphr_source_new, changes$keep)) |>
    mutate(id = as.integer(gsub("https://geoconnex.us/ref/mainstems/", "", reference_mainstem)),
           head_nhdpv2_COMID = ifelse(is.na(head_nhdpv2_COMID), "", 
                                      paste0("https://geoconnex.us/nhdplusv2/comid/", trimws(format(as.integer(head_nhdpv2_COMID), scientific = FALSE), "both"))),
           outlet_nhdpv2_COMID = ifelse(is.na(outlet_nhdpv2_COMID), "", 
                                        paste0("https://geoconnex.us/nhdplusv2/comid/", trimws(format(as.integer(outlet_nhdpv2_COMID), scientific = FALSE), "both"))),
           head_nhdpv2HUC12 = ifelse(is.na(head_nhdpv2HUC12), "", 
                                     paste0("https://geoconnex.us/nhdplusv2/huc12/", head_nhdpv2HUC12)),
           outlet_nhdpv2HUC12 = ifelse(is.na(outlet_nhdpv2HUC12), "", 
                                       paste0("https://geoconnex.us/nhdplusv2/huc12/", outlet_nhdpv2HUC12)),
           lengthkm = round(length, digits = 1),
           totdasqkm = round(totdasqkm, digits = 1),) |> 
      rename(any_of(c(name_at_outlet = "outlet_GNIS_NAME",
                      name_at_outlet_gnis_id = "outlet_GNIS_ID",
                      outlet_drainagearea_sqkm = "totdasqkm",
                      uri = "reference_mainstem",
                      length = "length")))  |>
      select(-any_of(c("length", "totdasqkm", "lp_mainstem_v2", "level"))))
  
  # TODO: validate that these have been removed once we are no longer relying on "changes" from v2 to v3
  # remove two duplicates that should not be in here
  ms_out <- ms_out[!(!is.na(ms_out$id) & ms_out$id == 2244483 & !is.na(ms_out$lp_mainstem_v3) & ms_out$lp_mainstem_v3 == 1895461),]
  ms_out <- ms_out[!(!is.na(ms_out$id) & ms_out$id == 1441404 & !is.na(ms_out$lp_mainstem_v3) & ms_out$lp_mainstem_v3 == 2063152),]

  # at this point we do not have mainstem ids for everything -- that is expected.
  
  # verify that we have lp_mainstem_v3 for everything that is not superseded
  stopifnot(!any(!ms_out$superseded & is.na(ms_out$lp_mainstem_v3)))
  stopifnot(!any(!is.na(ms_out$lp_mainstem_v3) & duplicated(ms_out$lp_mainstem_v3)))
  stopifnot(!any(!is.na(ms_out$id) & duplicated(ms_out$id)))

  ms_out
}

# double check that outlets are members of the path they should be.
clean_outlet <- function(ms_out, new_net) {

  # pecos outlet fix up
  ms_out$outlet_nhdpv2_COMID[ms_out$lp_mainstem_v3 == "2208010" & !is.na(ms_out$lp_mainstem_v3)] <- "https://geoconnex.us/nhdplusv2/comid/334014"

  check <- sf::st_drop_geometry(ms_out) |>
    filter(!superseded) |>
    select(uri, head_nhdpv2_COMID, outlet_nhdpv2_COMID) |>
    # TODO: create a function to flip URI to id and back
    mutate(head_nhdpv2_COMID = gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "nhdpv2-",
                                               head_nhdpv2_COMID),
           outlet_nhdpv2_COMID = gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "nhdpv2-",
                                               outlet_nhdpv2_COMID)) |>
    left_join(select(sf::st_drop_geometry(new_net), id, lp_mainstem_v3), # first join head comid to get levelpath
              by = c("head_nhdpv2_COMID" = "id")) |>
    left_join(filter(select(sf::st_drop_geometry(new_net), id, lp_mainstem_v3), !is.na(lp_mainstem_v3)), # now join by levelpath to get all comids
              by = "lp_mainstem_v3")
  
  check <- group_by(check, lp_mainstem_v3) |>
    # check that one of the ids in the mainstem is the outlet
    mutate(outlet_check = any(id == outlet_nhdpv2_COMID))
  
  # want to make sure that outlet_check is TRUE or NA for all groups
  stopifnot(sort(unique(check$lp_mainstem_v3[!(check$outlet_check | is.na(check$outlet_check))])) == c("131736", "148774")) 
  
  get_check <- function(ms_out) {
    sf::st_drop_geometry(ms_out) |>
      filter(!superseded) |>
      select(uri, head_nhdplushr_id, outlet_nhdplushr_id, lengthkm) |>
      distinct() |>
      mutate(head_nhdplushr_id = paste0("nhdphr-", head_nhdplushr_id), outlet_nhdplushr_id = paste0("nhdphr-", outlet_nhdplushr_id)) |>
      # first join to get levelpath
      left_join(select(sf::st_drop_geometry(new_net), id, lp_mainstem_v3), by = c("head_nhdplushr_id" = "id")) |>
      filter(!is.na(lp_mainstem_v3))
  }

  get_dups <- function (x, col) {
    x[x[[col]] %in% x[[col]][duplicated(x[[col]])], ]
  }

  check <- get_check(ms_out)

  dups <- get_dups(check, "lp_mainstem_v3")

  # see #12 -- these are being removed seperately
  # all this validation will be removed in v3.2 when we drop lpv3 completely
  stopifnot(nrow(dups) < 10)

  dups <- group_by(dups, lp_mainstem_v3) |>
    filter(lengthkm == min(lengthkm))

  check <- filter(check, !lp_mainstem_v3 %in% dups$lp_mainstem_v3) |>
    bind_rows(dups) |>
    select(-lengthkm) |>
    # now join by levelpath to get all ids along each path
    left_join(filter(select(sf::st_drop_geometry(new_net), lp_mainstem_v3, id), !is.na(lp_mainstem_v3)), by = "lp_mainstem_v3")
  
  check <- group_by(check, lp_mainstem_v3) |>
    mutate(outlet_check = any(id == outlet_nhdplushr_id))
  
  # expect that all outlet checks are NA or TRUE
  # if not, we need to find the correct outlet from new_net
  tofix <- unique(check$lp_mainstem_v3[!is.na(check$outlet_check) & !check$outlet_check])

  # If this is more than this we need to look into it
  stopifnot(length(tofix) < 75)

  new_net <- hydroloom::add_topo_sort(new_net)

  outlets <- dplyr::select(sf::st_drop_geometry(new_net), id, topo_sort, lp_mainstem_v3) |>
    filter(lp_mainstem_v3 %in% tofix) |>
    group_by(lp_mainstem_v3) |>
    filter(row_number() == n()) |>
    ungroup() |>
    select(outlet_nhdplushr_id = id, lp_mainstem_v3) |>
    mutate(lp_mainstem_v3 = as.numeric(lp_mainstem_v3),outlet_nhdplushr_id = gsub("nhdphr-", "", outlet_nhdplushr_id))

  ms_out_update <- dplyr::rows_update(as.data.frame(ms_out), outlets, by = "lp_mainstem_v3") |> sf::st_sf()

  check <- get_check(ms_out_update) |>
    filter(!lp_mainstem_v3 %in% dups$lp_mainstem_v3) |>
    left_join(filter(select(sf::st_drop_geometry(new_net), lp_mainstem_v3, id), !is.na(lp_mainstem_v3)), by = "lp_mainstem_v3") |>
    group_by(lp_mainstem_v3) |>
    mutate(outlet_check = any(id == outlet_nhdplushr_id))

  # want to make sure that outlet_check is TRUE or NA for all groups
  stopifnot(all(check$outlet_check | check$lp_mainstem_v3 == "1971028")) # columbia river is the only one that spans into v2 from the hr domain
  
  ms_out_update
}

get_nhdphr_source_extra <- function(new_net_nolp) {
   ### need to get geometry from new_net for the two nhdphr_source changes sets

  nhdphr_source_extra <-  new_net_nolp |>
    mutate(lp_mainstem_v3 = as.numeric(lp_mainstem_v3)) |>
    filter(lp_mainstem_v3 > 7e6)

  common_name <- select(nhdphr_source_extra, lp_mainstem_v3, gnis_name) |>
    filter(gnis_name != "" & !is.na(gnis_name)) |>
    add_count(lp_mainstem_v3, gnis_name) |>
    group_by(lp_mainstem_v3) |>
    mutate(common_name = gnis_name[n == max(n)][1]) |>
    select(-all_of(c("n", "gnis_name"))) |>
    ungroup() |>
    distinct()
  
  left_join(nhdphr_source_extra, common_name, by = "lp_mainstem_v3") |>
    hydroloom::add_topo_sort() |>
    arrange(desc(topo_sort)) |> 
    group_by(lp_mainstem_v3) |>
    summarize(lp_mainstem_v3 = lp_mainstem_v3[1],
              name_at_outlet = gnis_name[n()],
              common_GNIS_NAME = common_name[1],
              head_nhdplushr_id = gsub("nhdphr-", "", id[1]), 
              outlet_nhdplushr_id = gsub("nhdphr-", "", id[n()]),
              lengthkm = sum(length_km),
              outlet_drainagearea_sqkm = total_da_sqkm[1],
              superseded = FALSE)
}

add_hr_mainstems <- function(ms_out, new_net, nhdphr_source_extra, changes) {

  ms_out <- bind_rows(ms_out, nhdphr_source_extra)
  
  new_net$lp_mainstem_v3 <- as.numeric(new_net$lp_mainstem_v3)
  
  nhdphr_source <- filter(new_net, lp_mainstem_v3 %in% c(changes$nhdphr_source_replace$lp_mainstem_v3, 
                                                         changes$nhdphr_source_new$lp_mainstem_v3, 
                                                         nhdphr_source_extra$lp_mainstem_v3)) |>
    mutate(lp_mainstem_v3 = as.numeric(lp_mainstem_v3))
  
  # verify that we have things all lined up
  check <- inner_join(select(st_drop_geometry(ms_out), lp_mainstem_v3, uri),
                      select(st_drop_geometry(nhdphr_source), lp_mainstem_v3, uri_update = reference_mainstem), 
                      by = "lp_mainstem_v3")
  
  stopifnot(all(is.na(check$uri) | check$uri == check$uri_update))
    
  nhdphr_source <- nhdphr_source |>
    hydroloom::add_topo_sort() |>
    select(lp_mainstem_v3, topo_sort) |>
    arrange(desc(topo_sort)) |> 
    group_by(lp_mainstem_v3) |>
    group_split(.keep = TRUE) |>
    split_number_chunks(80)
  
  cl <- parallel::makeCluster(8)
  
  on.exit(parallel::stopCluster(cl))
  
  ids <- do.call(c, lapply(nhdphr_source, \(x) unique(do.call(c, lapply(x, \(y) y$lp_mainstem_v3)))))
  
  nhdphr_source <- do.call(c, pbapply::pblapply(nhdphr_source, collapse_lines, cl = cl))
  
  nhdphr_source <- st_sf(lp_mainstem_v3 = ids,
                         geom = nhdphr_source)  
  
  ms_out$geom[match(nhdphr_source$lp_mainstem_v3, ms_out$lp_mainstem_v3)] <- nhdphr_source$geom

  ms_out
}

add_ref_uri <- function(ms_out, ref_rivers, drop_ms) {
   # there are duplcate headwaters in superseded mainstems
  stopifnot(!any(is.na(ref_rivers$id[ref_rivers$superseded])))

  # remove them
  ms_lookup <- sf::st_drop_geometry(ref_rivers) |>
    filter(!superseded) |>
    select(id, head_nhdpv2_COMID, head_nhdplushr_id) |>
    mutate(id = as.integer(id))

  # nodups
  stopifnot(!any(ms_lookup$head_nhdpv2_COMID != "" & duplicated(ms_lookup$head_nhdpv2_COMID)))

  ms_out$row <- seq_len(nrow(ms_out))

  ms_out$head_nhdpv2_COMID[is.na(ms_out$head_nhdpv2_COMID)] <- ""

  # need to get mainstem ids for every na id in ms_out
  nhdpv2_source <- dplyr::rows_patch(
    as.data.frame(ms_out[ms_out$head_nhdpv2_COMID != "",]), 
    filter(select(ms_lookup, id, head_nhdpv2_COMID), head_nhdpv2_COMID != ""),
    by = "head_nhdpv2_COMID"
  ) |> sf::st_sf()

  nhdphr_source <- dplyr::rows_patch(
    as.data.frame(ms_out[ms_out$head_nhdpv2_COMID == "",]), 
    select(filter(ms_lookup, head_nhdpv2_COMID == ""), id, head_nhdplushr_id),
    by = "head_nhdplushr_id", unmatched = "ignore"
  ) |> sf::st_sf()

  stopifnot(nrow(ms_out) == nrow(nhdphr_source) + nrow(nhdpv2_source))

  ms_out <- bind_rows(nhdpv2_source, nhdphr_source) |>
    arrange(row) |>
    select(-row)

  ms_out$uri <- paste0("https://geoconnex.us/ref/mainstems/", ms_out$id)

  stopifnot(all(ref_rivers$uri %in% ms_out$uri | ref_rivers$uri %in% drop_ms))

  ms_out
}


add_dn_ms <- function(ms_out, new_dm, ref_rivers, drop_ms) {
  ms_out <- left_join(ms_out, new_dm, by = c("lp_mainstem_v3")) |>
    mutate(levelpathi = lp_mainstem_v3, dnlevelpat = ifelse(is.na(dnlpv3), 0, dnlpv3))
  
  ms_out$dnlevelpat[!is.na(ms_out$outlet_nhd_permid) & ms_out$outlet_nhd_permid %in% c("155395996", "a9ee0d00-22f4-44eb-a3d9-e95446a6bac6")] <- 0
  
  missing <- ms_out[!(ms_out$dnlevelpat == 0 | ms_out$dnlevelpat %in% ms_out$levelpathi),]
  
  to_remove <- missing$levelpathi
  
  # we are going to remove the missing ones -- also need anything going to them
  more <- new_dm$lp_mainstem_v3[new_dm$dnlpv3 %in% to_remove]
  
  while(length(more) > 0) {
    to_remove <- c(to_remove, more)
    more <- new_dm$lp_mainstem_v3[new_dm$dnlpv3 %in% more]
  }
  
  remove <- filter(ms_out, lp_mainstem_v3 < 7e6 & lp_mainstem_v3 %in% to_remove & 
    is.na(id)) # Don't remove where a mainstem id has already been set!
  
  # sf::write_sf(remove, "temp.gpkg", "remove_2")
  
  ms_out <- filter(ms_out, !lp_mainstem_v3 %in% remove$lp_mainstem_v3)
  
  stopifnot(all(ref_rivers$uri %in% ms_out$uri | ref_rivers$uri %in% drop_ms))
  
  # See #13
  stopifnot(sum(!(ms_out$dnlevelpat == 0 | ms_out$dnlevelpat %in% ms_out$levelpathi)) < 100)
  
  ms_out$dnlevelpat[!(ms_out$dnlevelpat == 0 | ms_out$dnlevelpat %in% ms_out$levelpathi)] <- 0
  
  new_down <- add_dm(ms_out[!ms_out$superseded,])[["down_levelpaths"]]
  new_down[is.na(new_down)] <- ""

  ms_out$down_levelpaths <- ""
  
  ms_out$down_levelpaths[!ms_out$superseded] <- new_down

  ms_out
}

assign_new_ms_ids <- function(ms_out) {

  # some ids that need to be updated so we can get down mainstem later on.
  available <- seq(min(ms_out$id, na.rm = TRUE), max(ms_out$id, na.rm = TRUE))
  available <- available[!available %in% ms_out$id[!is.na(ms_out$id)]]

  # these need to have mainstem ids assigned  
  update <- is.na(ms_out$id)
  
  # not setup to update right now!
  stopifnot(!any(update))

  # ms_out$id[update] <- sample(available, sum(update))
  
  # ms_out$uri[update] <- paste0("https://geoconnex.us/ref/mainstems/", ms_out$id[update])

  ms_out
}

add_dm_ms_id <- function(ms_out) {
  dm <- select(st_drop_geometry(ms_out), 
    downstream_mainstem_id = uri, lp_mainstem_v3) |>
    filter(!is.na(lp_mainstem_v3)) |>
    distinct()
  
  stopifnot(!any(duplicated(dm$lp_mainstem_v3)))
  
  left_join(ms_out, dm, by = "lp_mainstem_v3")
  
}

make_clean_mainstems <- function(ms_out) {
  ms_out <- ms_out |>
    mutate(type = "['https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath', 'https://www.opengis.net/def/schema/hy_features/hyf/HY_WaterBody']",
           primary_name = common_GNIS_NAME,
           primary_name_gnis_id = common_GNIS_ID,
           head_nhdpv2_COMID = head_nhdpv2_COMID,
           outlet_nhdpv2_COMID = outlet_nhdpv2_COMID,
           head_nhdpv2HUC12 = head_nhdpv2HUC12,
           outlet_nhdpv2HUC12 = outlet_nhdpv2HUC12,
           superseded = ifelse(is.na(superseded), FALSE, superseded)) |>
    mutate(new_mainstemid = ifelse(new_mainstemid == "NA", "", new_mainstemid)) |>
    select(id, uri,
           featuretype = type,
           downstream_mainstem_id,
           encompassing_mainstem_basins = down_levelpaths,
           name_at_outlet, 
           name_at_outlet_gnis_id, 
           primary_name, primary_name_gnis_id,
           lengthkm, outlet_drainagearea_sqkm,
           head_nhdpv2_COMID, outlet_nhdpv2_COMID, 
           head_nhdplushr_id, outlet_nhdplushr_id,
           head_nhd_permid, outlet_nhd_permid,
           head_nhdpv2HUC12, outlet_nhdpv2HUC12, 
           head_rf1ID, outlet_rf1ID, 
           head_nhdpv1_COMID, outlet_nhdpv1_COMID, 
           head_2020HUC12, outlet_2020HUC12,
           superseded, new_mainstemid) |>
    mutate(id = as.character(id))

  length <- as.numeric(units::set_units(sf::st_length(ms_out), "km"))

  ms_out$lengthkm <- ifelse(ms_out$lengthkm < length, length, ms_out$lengthkm)

  ms_out$primary_name_gnis_id <- ifelse(is.na(ms_out$primary_name_gnis_id), "", paste0("https://geoconnex.us/usgs/gnis/", ms_out$primary_name_gnis_id))
  ms_out$name_at_outlet_gnis_id <- ifelse(is.na(ms_out$name_at_outlet_gnis_id), "", paste0("https://geoconnex.us/usgs/gnis/", ms_out$name_at_outlet_gnis_id))
 
  ms_out <- mutate_if(ms_out, is.character, ~tidyr::replace_na(.,""))
  
  if(sf::st_crs(ms_out) != sf::st_crs(4326))
    ms_out <- sf::st_transform(ms_out, 4326)
  
  ms_out
}