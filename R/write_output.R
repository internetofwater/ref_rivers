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

  hr_ref_net <- sf::st_drop_geometry(ref_net) |>
    filter(source == "nhdphr") |>
    select(id, toid, levelpath) |>
    distinct() |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    left_join(distinct(select(hr_net, id, permid)), by = c("nhdplushr_id" = "id")) |>
    distinct()
  
  mainstems <- filter(mainstems, !superseded)

  out <- sf::st_drop_geometry(mainstems) |>
    filter(!superseded) |>
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
  
  out <- group_by(out, levelpathi) |>
    mutate(outlet_check = any(comid == outlet_nhdpv2_COMID))
  
  if(sum(is.na(out$outlet_check) > 150)) stop("only a small number of mainstems should not have nhdplusv2 outlets")

  # outlet_check can be NA or TRUE
  if(!all(is.na(out$outlet_check) | out$outlet_check)) stop("all levelpaths should have the outlet in them")
  
  out <- select(ungroup(out), uri, comid)  

  out <- distinct(out) |>
    filter(!is.na(comid))

  stopifnot(!any(duplicated(out$comid)))

  readr::write_csv(out, "out/nhdpv2_lookup.csv")

  out <- sf::st_drop_geometry(mainstems) |>
    filter(!superseded) |>
    select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
    distinct() |>
    # first join to get levelpath
    left_join(
      select(hr_ref_net, nhdplushr_id, levelpath), 
      by = c("head_nhdplushr_id" = "nhdplushr_id")
    ) |>
    filter(!is.na(levelpath))

  get_dups <- function (x, col) {
    x[x[[col]] %in% x[[col]][duplicated(x[[col]])], ]
  }

  # If a mainstem is only part of a levelpath we will have duplication
  # for dups -- we need a split levelpath approach
  dups <- get_dups(out, "levelpath")

  splits <- group_by(dups, levelpath) |>
    group_split()

  assign_uri <- function(x, splits, hr_ref_net) {

    message(x)

    x <- splits[[x]]

    suppressWarnings(
    y <- filter(hr_ref_net, levelpath %in% x$levelpath) |>
      hydroloom::sort_network() # sorted top to bottom
    )

    y$uri <- NA_character_
    
    for (i in seq_len(nrow(x))) {
      head_idx <- which(y$nhdplushr_id == x$head_nhdplushr_id[i])
      outlet_idx <- which(y$nhdplushr_id == x$outlet_nhdplushr_id[i])

      if(length(outlet_idx) == 0) stop()

      y$uri[head_idx:outlet_idx] <- x$uri[i]
    }
    
    y
  }

  
  dup_assign <- lapply(seq_along(splits), assign_uri, splits = splits, hr_ref_net = hr_ref_net)

  # dup_assign gets mainstem
  dup_assign <- bind_rows(dup_assign)

  stopifnot(all(dups$levelpath %in% dup_assign$levelpath))

  out <- filter(out, !levelpath %in% dups$levelpath) |>
    # now join by levelpath to get all ids along each path
    left_join(select(hr_ref_net, id, nhdplushr_id, nhdplushr_permid = permid, levelpath), by = "levelpath")

  out <- group_by(out, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))

  # expect that all outlet checks are NA or TRUE
  # if not, we need to find the correct outlet from ref_net
  tofix <- unique(out$levelpath[!out$outlet_check])

  # If this is more than this we need to look into it
  stopifnot(length(tofix) < 700)

  # update so the outlet is correct according to the network
  ref_net_hr <- filter(ref_net, source == "nhdphr")
  ref_net_hr <- hydroloom::add_topo_sort(ref_net_hr)

  outlets <- dplyr::select(sf::st_drop_geometry(ref_net_hr), id, topo_sort, levelpath) |>
    filter(levelpath %in% tofix) |>
    group_by(levelpath) |>
    filter(row_number() == n()) |>
    ungroup() |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    select(outlet_nhdplushr_id = nhdplushr_id, levelpath)

  # update outlet_nhdplushr_id for tofix levelpaths
  out <- ungroup(out) |>
    rows_update(outlets, by = "levelpath")

  out <- group_by(out, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))

  stopifnot(!any(is.na(out$outlet_check)))

  # want to make sure that outlet_check is TRUE or NA for all groups
  stopifnot(all(out$outlet_check))

  out <- select(ungroup(out), uri, nhdplushr_permid, nhdplushr_id)

  readr::write_csv(out, "out/nhdphr_lookup.csv")

  "out/nhdpv2_lookup.csv"
}