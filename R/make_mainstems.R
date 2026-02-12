# to run interactively, use:
# library(targets)
# library(dplyr)
# library(sf)
# old_ms <- tar_read("mainstems_v2")
# new_ms <- tar_read("mainstems_v3")
# enhd_v3 <- tar_read("enhd_v3")
# ref_rivers <- tar_read("ref_rivers_v21")
# new_net <- tar_read("ref_net_v1")
# changes <- tar_read("reconciled_mainstems")
# out_f <- "out/mainstems.gpkg"
# lpv3_lookup_file <- "out/lpv3_lookup.csv"
# source("R/get_data.R")

make_mainstems <- function(old_ms, new_ms, enhd_v3, ref_rivers, new_net, changes, out_f, lpv3_lookup_file = "out/lpv3_lookup.csv") {
  old_ms <- sf::read_sf(old_ms)
  new_ms <- get_mainstem_summary_v3(new_ms)
  ref_rivers <- sf::read_sf(ref_rivers)
  new_net <- sf::read_sf(new_net)
  enhd_v3 <- arrow::read_parquet(enhd_v3)
  
  stopifnot(all(new_net$toid == "" | new_net$toid %in% new_net$id)) # verify that all outlets are ""
  
  add_extra <- readr::read_csv("data/review/deprecated_lookup.csv")

  stopifnot(all(add_extra$head_nhdphr_permid %in% new_net$id))

  # only keep where there is no lp_mainstem_v3
  add_extra <- filter(add_extra, head_nhdphr_permid %in% new_net$id[is.na(new_net$lp_mainstem_v3)])

  add_lps <- unique(new_net$levelpath[new_net$id %in% add_extra$head_nhdphr_permid])

  add_lps <- data.frame(levelpath = add_lps, lp_mainstem_v3 = as.character(seq(8e6, 8e6 + length(add_lps) - 1)))

  new_net <- dplyr::rows_update(as.data.frame(new_net), add_lps, by = "levelpath") |> sf::st_sf()

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
  
  new_dm <- distinct(select(new_net_nolp, 
                            dnlpv3, lp_mainstem_v3)) |>
    mutate(dnlpv3 = tidyr::replace_na(as.numeric(dnlpv3), 0),
           lp_mainstem_v3 = as.numeric(lp_mainstem_v3)) |>
    filter(lp_mainstem_v3 != dnlpv3) |>
    distinct()
  
  dup_down <- filter(new_dm, lp_mainstem_v3 %in% lp_mainstem_v3[duplicated(lp_mainstem_v3)])
  
  enhd_v3_dnlp <- select(enhd_v3, lp_mainstem_v3 = levelpathi, dnlpv3_enhd = dnlevelpat) |>
    filter(lp_mainstem_v3 != dnlpv3_enhd)
  
  # must be distinct
  stopifnot(length(unique(enhd_v3_dnlp$lp_mainstem_v3)) == nrow(enhd_v3_dnlp))
  
  dup_down <- left_join(dup_down, enhd_v3_dnlp, by = "lp_mainstem_v3")
  
  dup_down_nomatch <- group_by(dup_down, lp_mainstem_v3) |>
    filter(!any(dnlpv3 == dnlpv3_enhd))
  
  dup_down_match <- filter(dup_down, dnlpv3 == dnlpv3_enhd)
  
  override <- data.frame(lp_mainstem_v3 = c(148773, 313256, 2589752, 1919814, 2090617, 2119874, 2599701, 1881754, 8000017, 8000086, 8000092, 8000013, 8000076, 8000081, 8000012, 8000075, 8000080, 8000006),
                         dnlpv3 = c(312191, 183508, 2589168, 1919808, 2090618, 2119871, 2592712, 7000028, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
  
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
  
  sf::write_sf(remove, "temp.gpkg", "disconnect_1")
  
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
  
  # bind together changes and get into schema of pass_on
  ms_out <- bind_rows(
    
    select(pass_on, -any_of(c("featuretype", "downstream_mainstem_id", "encompassing_mainstem_basins"))) |>
      hydroloom::st_compatibalize(changes$keep) |>
      mutate(id = as.integer(id)), 
    
    bind_rows(changes$keep,
              hydroloom::st_compatibalize(changes$deprecate, changes$keep),
              hydroloom::st_compatibalize(changes$add, changes$keep),
              hydroloom::st_compatibalize(changes$nhdphr_source_replace, changes$keep),
              hydroloom::st_compatibalize(changes$nhdphr_source_new, changes$keep)) |>
    mutate(id = as.integer(gsub("https://geoconnex.us/ref/mainstems/", "", reference_mainstem)),
           head_nhdpv2_COMID = ifelse(is.na(head_nhdpv2_COMID), "", 
                                      paste0("https://geoconnex.us/nhdplusv2/comid/", head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = ifelse(is.na(outlet_nhdpv2_COMID), "", 
                                        paste0("https://geoconnex.us/nhdplusv2/comid/", outlet_nhdpv2_COMID)),
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
  
  # remove two duplicates that should not be in here
  ms_out <- ms_out[!(!is.na(ms_out$id) & ms_out$id == 2244483 & !is.na(ms_out$lp_mainstem_v3) & ms_out$lp_mainstem_v3 == 1895461),]
  ms_out <- ms_out[!(!is.na(ms_out$id) & ms_out$id == 1441404 & !is.na(ms_out$lp_mainstem_v3) & ms_out$lp_mainstem_v3 == 2063152),]

  stopifnot(all(ref_rivers$uri %in% ms_out$uri))

  # verify that we have lp_mainstem_v3 for everything that is not superseded
  stopifnot(!any(!ms_out$superseded & is.na(ms_out$lp_mainstem_v3)))
  stopifnot(!any(!is.na(ms_out$lp_mainstem_v3) & duplicated(ms_out$lp_mainstem_v3)))
  stopifnot(!any(!is.na(ms_out$id) & duplicated(ms_out$id)))

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
  
  nhdphr_source_extra <- left_join(nhdphr_source_extra, common_name, by = "lp_mainstem_v3") |>
    hydroloom::add_topo_sort() |>
    arrange(desc(topo_sort)) |> 
    group_by(lp_mainstem_v3) |>
    summarize(lp_mainstem_v3 = lp_mainstem_v3[1],
              name_at_outlet = gnis_name[n()],
              common_GNIS_NAME = common_name[1],
              head_nhdplushr_id = id[1],
              outlet_nhdplushr_id = id[n()],
              lengthkm = sum(length_km),
              outlet_drainagearea_sqkm = total_da_sqkm[1],
              superseded = FALSE)
  
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
  
  rm(check)
  
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
  
  rm(changes)
  
  # some ids that need to be updated so we can get down mainstem later on.
  available <- seq(min(ms_out$id, na.rm = TRUE), max(ms_out$id, na.rm = TRUE))
  available <- available[!available %in% ms_out$id[!is.na(ms_out$id)]]

  # these need to have mainstem ids assigned  
  update <- is.na(ms_out$id)
  
  ms_out$id[update] <- sample(available, sum(update))
  
  ms_out$uri[update] <- paste0("https://geoconnex.us/ref/mainstems/", ms_out$id[update])
  
  ms_out <- left_join(ms_out, new_dm, by = c("lp_mainstem_v3")) |>
    mutate(levelpathi = lp_mainstem_v3, dnlevelpat = ifelse(is.na(dnlpv3), 0, dnlpv3))
  
  missing <- ms_out[!(ms_out$dnlevelpat == 0 | ms_out$dnlevelpat %in% ms_out$levelpathi),]
  
  to_remove <- missing$levelpathi
  
  # we are going to remove the missing ones -- also need anything going to them
  more <- new_dm$lp_mainstem_v3[new_dm$dnlpv3 %in% to_remove]
  
  while(length(more) > 0) {
    to_remove <- c(to_remove, more)
    more <- new_dm$lp_mainstem_v3[new_dm$dnlpv3 %in% more]
  }
  
  remove <- filter(ms_out, lp_mainstem_v3 < 7e6 & lp_mainstem_v3 %in% to_remove)

  sf::write_sf(remove, "temp.gpkg", "remove_2")
  
  ms_out <- filter(ms_out, !lp_mainstem_v3 %in% to_remove)
  
  stopifnot(all(ms_out$dnlevelpat == 0 | ms_out$dnlevelpat %in% ms_out$levelpathi))
  
  rm(old_ms)
  rm(enhd_v3)
  rm(enhd_v3_dnlp)
  rm(new_net)
  rm(new_dm)
    
  new_down <- add_dm(ms_out[!ms_out$superseded,])[["down_levelpaths"]]
  new_down[is.na(new_down)] <- ""
  
  ms_out$down_levelpaths <- ""
  
  ms_out$down_levelpaths[!ms_out$superseded] <- new_down

  dm <- select(st_drop_geometry(ms_out), 
               downstream_mainstem_id = uri, lp_mainstem_v3) |>
    filter(!is.na(lp_mainstem_v3)) |>
    distinct()
  
  stopifnot(!any(duplicated(dm$lp_mainstem_v3)))
  
  ms_out <- left_join(ms_out, dm,
                      by = "lp_mainstem_v3")
  
  lpv3_lookup <- st_drop_geometry(ms_out) |>
    filter(!superseded) |>
    select(uri, lp_mainstem_v3) |>
    distinct() |>
    filter(lp_mainstem_v3 < 700000)
  
  readr::write_csv(lpv3_lookup, lpv3_lookup_file)
  
  ms_out <- ms_out |>
    mutate(type = "['https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath', 'https://www.opengis.net/def/schema/hy_features/hyf/HY_WaterBody']",
           primary_name = common_GNIS_NAME,
           primary_name_gnis_id = common_GNIS_ID,
           head_nhdpv2_COMID = ifelse(is.na(head_nhdpv2_COMID), "", 
                                      paste0("https://geoconnex.us/nhdplusv2/comid/", head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = ifelse(is.na(outlet_nhdpv2_COMID), "", 
                                        paste0("https://geoconnex.us/nhdplusv2/comid/", outlet_nhdpv2_COMID)),
           head_nhdpv2HUC12 = ifelse(is.na(head_nhdpv2HUC12), "", 
                                     paste0("https://geoconnex.us/nhdplusv2/huc12/", head_nhdpv2HUC12)),
           outlet_nhdpv2HUC12 = ifelse(is.na(outlet_nhdpv2HUC12), "", 
                                       paste0("https://geoconnex.us/nhdplusv2/huc12/", outlet_nhdpv2HUC12)),
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
           superseded, new_mainstemid) %>%
    mutate(id = as.character(id))
  
    ms_out <- mutate_if(ms_out, is.character, ~tidyr::replace_na(.,""))
  
    if(sf::st_crs(ms_out) != sf::st_crs(4326))
      ms_out <- sf::st_transform(ms_out, 4326)
  
  sf::write_sf(ms_out, out_f, "mainstems")
  
  ms_out
}

add_dm <- function(network) {
  
  lp <- select(st_drop_geometry(network), levelpathi, dnlevelpat) %>%
    filter(!is.na(levelpathi) & levelpathi != dnlevelpat)
  
  dnlp <- data.frame(lp = seq(0, (max(lp$levelpathi)))) %>%
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
  
  network %>%
    left_join(all_lp, by = "levelpathi") %>%
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

merge_ms <- function(x) {
  rows <- nrow(x)
  
  x <- hydroloom::add_topo_sort(x)
  
  stopifnot(nrow(x) == rows)
  
  geom <- select(x, lp_mainstem_v3, topo_sort) |>
    arrange(desc(topo_sort)) |> 
    group_by(lp_mainstem_v3) |>
    group_split(.keep = TRUE) |>
    split_number_chunks(80)
  
  cl <- parallel::makeCluster(8)
  
  on.exit(parallel::stopCluster(cl))
  
  geoms <- do.call(c, pbapply::pblapply(geom, collapse_lines, cl = cl))
  
  ids <- do.call(c, lapply(geom, \(x) unique(do.call(c, lapply(x, \(y) y$lp_mainstem_v3)))))
  
  st_sf(lp_mainstem_v3 = ids,
        geom = geoms)  
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
