# library(dplyr)
# library(sf)
# library(targets)
# tar_load(mainstems)
# tar_load(enhd_v3)
# tar_load(ref_net_v1)
# tar_load(hr_net)

#' writes lookups between mainstem id and various identifier systems
write_lookups <- function(mainstems, enhd_v3, ref_net_v1, hr_net) {
  enhd <- arrow::read_parquet(enhd_v3)
  ref_net <- sf::read_sf(ref_net_v1)
  hr_net <- readr::read_csv(hr_net, col_types = "cccll")

  # Only the hr part of the network
  hr_ref_net <- sf::st_drop_geometry(ref_net) |>
    filter(source == "nhdphr") |>
    select(id, toid, levelpath) |>
    distinct() |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    # get permid in here too
    left_join(distinct(select(hr_net, id, permid)), by = c("nhdplushr_id" = "id")) |>
    distinct()

  # some flowlines span both data sources in reference network
  # they all already have known reference mainstems assignments
  both <- ref_net |>
    sf::st_drop_geometry() |>
    select(levelpath, source, uri = reference_mainstem) |>
    filter(!is.na(uri)) |>
    distinct() |>
    group_by(uri) |>
    filter(n() > 1)
  
  # only interested in current mainstems
  mainstems <- filter(mainstems, !superseded)

  # Seed our output with all the mainstems that are relevant
  v2_out <- sf::st_drop_geometry(mainstems) |>
    select(uri, head_nhdpv2_COMID, outlet_nhdpv2_COMID) |>
    mutate(head_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               outlet_nhdpv2_COMID))) |>
    left_join(distinct(select(enhd, comid, levelpathi)), # first join head comid to get levelpath
              by = c("head_nhdpv2_COMID" = "comid")) |>
    left_join(distinct(select(enhd, comid, levelpathi)), # now join by levelpath to get all comids
              by = "levelpathi")
  
  # group and check that we have the outlet in here
  v2_out <- group_by(v2_out, levelpathi) |>
    mutate(outlet_check = any(comid == outlet_nhdpv2_COMID))
  
  if(sum(is.na(v2_out$outlet_check) > 150)) stop("only a small number of mainstems should not have nhdplusv2 outlets")

  # outlet_check can be NA or TRUE
  if(!all(is.na(v2_out$outlet_check) | v2_out$outlet_check)) stop("all levelpaths should have the outlet in them")
  
  v2_out <- select(ungroup(v2_out), uri, comid) |>
    distinct() |>
    filter(!is.na(comid))

  stopifnot(!any(duplicated(v2_out$comid)))

  #### HR
  # initialize with mainstems
  hr_out <- sf::st_drop_geometry(mainstems) |>
    select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
    distinct() |>
    # first join to get levelpath
    left_join(
      select(hr_ref_net, nhdplushr_id, levelpath), 
      by = c("head_nhdplushr_id" = "nhdplushr_id")
    ) |>
    filter(!is.na(levelpath))

  stopifnot(!any(duplicated(hr_out$nhdplushr_permid)))

  get_dups <- function (x, col) {
    x[x[[col]] %in% x[[col]][duplicated(x[[col]])], ]
  }

  # If a mainstem is only *part* of a levelpath we will have duplication
  # for dups -- we need a split levelpath approach
  dups <- get_dups(hr_out, "levelpath")

  dup_uri <- unique(dups$uri)

  # split on level path -- for each, we need to break into two
  splits <- group_by(dups, levelpath) |>
    group_split()

  # need to assign to individual features
  assign_uri <- function(p, splits, hr_ref_net) {
    # debug
    # message(p)
    ms <- splits[[p]]

    suppressWarnings(
    path <- filter(hr_ref_net, levelpath %in% ms$levelpath) |>
      hydroloom::sort_network() # sorted top to bottom
    )

    path$uri <- NA_character_
    
    for (i in seq_len(nrow(ms))) {
      head_idx <- which(path$nhdplushr_id == ms$head_nhdplushr_id[i])
      outlet_idx <- which(path$nhdplushr_id == ms$outlet_nhdplushr_id[i])

      if(length(outlet_idx) == 0) stop()

      path$uri[head_idx:outlet_idx] <- ms$uri[i]
      path$head_nhdplushr_id <- ms$head_nhdplushr_id[i]
      path$outlet_nhdplushr_id <- ms$outlet_nhdplushr_id[i]
    }
    
    path
  }

  dup_assign <- lapply(seq_along(splits), assign_uri, splits = splits, hr_ref_net = hr_ref_net)

  # dup_assign gets mainstem
  dup_assign <- bind_rows(dup_assign) |>
    rename(nhdplushr_permid = permid)

  stopifnot(all(dup_uri %in% dup_assign$uri))
  stopifnot(!any(duplicated(dup_assign$nhdplushr_id)))
  stopifnot(all(dups$levelpath %in% dup_assign$levelpath))

  # hr_out is what has duplicated levelpath assignments per mainstem id 
  hr_out <- filter(hr_out, !levelpath %in% dups$levelpath) |>
    # now join by levelpath to get all ids along each path
    # this is for the UNDUPLICATED set
    left_join(select(hr_ref_net, id, nhdplushr_id, nhdplushr_permid = permid, levelpath), by = "levelpath") |>
    bind_rows(dup_assign)
  
  stopifnot(!any(duplicated(hr_out$nhdplushr_id)))
  stopifnot(!any(duplicated(hr_out$nhdplushr_permid)))

  hr_out <- group_by(hr_out, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))

  # expect that all outlet checks are NA or TRUE
  # if not, we need to find the correct outlet from ref_net
  tofix <- unique(hr_out$levelpath[!hr_out$outlet_check])

  # If this is more than this we need to look into it
  stopifnot(length(tofix) < 700)

  # update so the outlet is correct according to the network
  ref_net_hr <- filter(ref_net, source == "nhdphr")
  ref_net_hr <- hydroloom::add_topo_sort(ref_net_hr)

  # use the sort to grab the outlet feature
  outlets <- dplyr::select(sf::st_drop_geometry(ref_net_hr), id, topo_sort, levelpath) |>
    filter(levelpath %in% tofix) |>
    group_by(levelpath) |>
    filter(row_number() == n()) |>
    ungroup() |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    select(outlet_nhdplushr_id = nhdplushr_id, levelpath)

  # update outlet_nhdplushr_id for tofix levelpaths
  hr_out <- ungroup(hr_out) |>
    rows_update(outlets, by = "levelpath")

  stopifnot(!any(duplicated(hr_out$nhdplushr_id)))

  hr_out <- group_by(hr_out, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))

  stopifnot(!any(is.na(hr_out$outlet_check)))

  # want to make sure that outlet_check is TRUE or NA for all groups
  stopifnot(all(hr_out$outlet_check))

  hr_out <- select(ungroup(hr_out), uri, nhdplushr_permid, nhdplushr_id)

  # uses "both" from above to find cross-domain mainstems
  # they already have a reference mainstem assignment so can just bind
  hr_portion <- ref_net |>
    sf::st_drop_geometry() |>
    filter(levelpath %in% both$levelpath & source == "nhdphr") |>
    select(uri = reference_mainstem, nhdplushr_id = id) |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", nhdplushr_id)) |>
    # get permid in here too
    left_join(distinct(select(hr_net, id, nhdplushr_permid = permid)), by = c("nhdplushr_id" = "id")) |>
    distinct()

  hr_out <- filter(hr_out, !nhdplushr_id %in% hr_portion$nhdplushr_id)

  stopifnot(!any(hr_portion$nhdplushr_id %in% hr_out$nhdplushr_id))
  stopifnot(!any(duplicated(hr_portion$nhdplushr_id)))

  hr_out <- bind_rows(hr_out, hr_portion)
  
  stopifnot(!any(duplicated(hr_out$nhdplushr_id)))
  
  v2_portion <- ref_net |>
    sf::st_drop_geometry() |>
    filter(levelpath %in% both$levelpath & source == "nhdpv2") |>
    select(uri = reference_mainstem, comid = id) |>
    mutate(comid = as.numeric(gsub("nhdpv2-", "", comid))) |>
    distinct() |>
    filter(!comid %in% v2_out$comid)

  stopifnot(!any(v2_portion$comid %in% v2_out$comid))

  v2_out <- bind_rows(v2_out, v2_portion)

  stopifnot(all(mainstems$uri %in% c(hr_out$uri, v2_out$uri)))

  readr::write_csv(v2_out, "out/nhdpv2_lookup.csv")
  readr::write_csv(hr_out, "out/nhdphr_lookup.csv")

  "out/nhdpv2_lookup.csv"
}