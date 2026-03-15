# library(dplyr)
# library(sf)
# library(targets)
# tar_load(mainstems)
# tar_load(enhd_v3)
# tar_load(ref_net_v1)
# tar_load(hr_net)

#' write lookup tables for source networks
#' 
#' @description writes lookups between mainstem id and various identifier systems
#' This could be done a couple ways. this has taken an approach that attempts 
#' to match levelpath to mainstem. This works well for NHDPlus but has a lot 
#' of edge cases for NHDPlusHR. See code comments for specifics.
#' 
#' The other approach would be a more naive, brute force navigation. As the
#' code is, it surfaces a lot of complexity and has led to some important
#' realizations. But this will likely need to be simplified in the future.
#' 
#' This function is mostly about its side affect (writing lookup tables)
#' 
#' Outputs:
#' out/nhdphr_lookups.csv
#' out/nhdpv2_lookups.csv
#' 
#' @param mainstems sf data.frame from workflow
#' @param enhd_v3 data.frame containing enhd v3 network
#' @param ref_net_v1 sf data.frame base network
#' @param hr_net data.frame with nhdplushr flow network
#' 
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
  
  # only interested in current mainstems
  mainstems <- filter(mainstems, !superseded)

  v2_out <- get_v2_lookup(mainstems, enhd)

  #### HR
  # initialize with mainstems
  hr_out_init <- sf::st_drop_geometry(mainstems) |>
    select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
    distinct()

  # expect to find lookups for all where headwater and/or outlet ids in hr_out_init exist
  # in the nhdphr portion of the reference network
  need <- hr_out_init |>
    filter(
      head_nhdplushr_id %in% hr_ref_net$nhdplushr_id | 
        outlet_nhdplushr_id %in% hr_ref_net$nhdplushr_id
    )

  ### CASE 1: duplication where one levelpath is made up of multiple mainstems
  # If a mainstem is only *part* of a levelpath we will have duplication
  # for dups -- we need a split levelpath approach
  hr_out_1 <- dedup_mainstem_levelpath(hr_out_init, hr_ref_net)

  stopifnot(all(!is.na(hr_out_1$id)))

  stopifnot(!any(is.na(hr_out_1$uri)))

  ### CASE 2: levelpath of outlet doesn't include mainstem outlet
  # when this occurs, we need to use a more network-aware approach to build the lookups
  # we expect to find the outlet on a downmain trace from the head.
  hr_out_2 <- assign_mainstems_to_flowlines(hr_out_1, hr_ref_net)

  stopifnot(all(!is.na(hr_out_2$id)))

  stopifnot(!any(is.na(hr_out_2$uri)))

  # some flowlines span both data sources in reference network
  # they all already have known reference mainstems assignments
  both <- sf::st_drop_geometry(ref_net) |> # the whole ref net
    select(source, uri = reference_mainstem) |>
    filter(uri %in% need$uri[!need$uri %in% hr_out_2$uri]) |>
    distinct() |>
    group_by(uri) |>
    filter(n() > 1)

  stopifnot(all(need$uri[!need$uri %in% hr_out_2$uri] %in% both$uri))

  # they already have a reference mainstem assignment so can just bind
  hr_portion <- sf::st_drop_geometry(ref_net) |>
    filter(source == "nhdphr" & reference_mainstem %in% both$uri) |>
    select(uri = reference_mainstem, nhdplushr_id = id) |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", nhdplushr_id)) |>
    # get permid in here too
    left_join(distinct(select(hr_net, id, nhdplushr_permid = permid)), by = c("nhdplushr_id" = "id")) |>
    distinct()

  stopifnot(!any(hr_portion$nhdplushr_id %in% hr_out_2$nhdplushr_id))
  stopifnot(all(!is.na(hr_portion$nhdplushr_id)))
  stopifnot(!any(is.na(hr_portion$uri)))

  # check that we for sure aren't making dups
  stopifnot(!any(hr_portion$uri %in% hr_out_2$uri))
  # and that nothing we are adding is duplicated
  stopifnot(!any(duplicated(hr_portion$nhdplushr_id)))
  stopifnot(!any(is.na(hr_portion$uri)))
  
  # can just bind
  hr_out <- bind_rows(ungroup(hr_out_2), ungroup(hr_portion)) |>
    select(uri, nhdplushr_permid, nhdplushr_id)

  # some major rivers start and end in nhdpv2 but flow through hr
  extras <- sf::st_drop_geometry(ref_net) |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    filter(source == "nhdphr" & !is.na(reference_mainstem) & !nhdplushr_id %in% hr_out$nhdplushr_id) |>
    left_join(distinct(select(hr_net, id, nhdplushr_permid = permid)), by = c("nhdplushr_id" = "id")) |>
    select(uri = reference_mainstem, nhdplushr_id, nhdplushr_permid)

  stopifnot(!any(extras$nhdplushr_id %in% hr_out$nhdplushr_id))
  stopifnot(!any(duplicated(extras$nhdplushr_id)))
  
  hr_out <- bind_rows(hr_out, extras)

  stopifnot(!any(duplicated(hr_out$nhdplushr_id)))
  
  stopifnot(all(mainstems$uri %in% c(hr_out$uri, v2_out$uri)))

  stopifnot(all(!is.na(hr_out$nhdplushr_id)))

  stopifnot(!any(is.na(hr_out$uri)))

  readr::write_csv(v2_out, "out/nhdpv2_lookup.csv")
  readr::write_csv(hr_out, "out/nhdphr_lookup.csv")

  "out/nhdpv2_lookup.csv"
}

#' Get duplicated rows
#'
#' Returns all rows where the value in `col` appears more than once.
#'
#' @param x data.frame
#' @param col character column name to check for duplicates
#' @return data.frame of rows with duplicated values in `col`
#' @keywords internal
get_dups <- function (x, col) {
  x[x[[col]] %in% x[[col]][duplicated(x[[col]])], ]
}


 #' Assign mainstem URIs to flowlines along a split levelpath
 #'
 #' For levelpaths shared by multiple mainstems, assigns each flowline to
 #' the correct mainstem URI based on head/outlet positions.
 #'
 #' @param p integer index into `splits`
 #' @param splits list of data.frames from `group_split` on duplicated levelpaths
 #' @param hr_ref_net data.frame NHDPlusHR reference network
 #' @return data.frame with `uri` assigned to each flowline
 #' @keywords internal
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

#' Build NHDPlusV2 COMID-to-mainstem lookup
#'
#' Joins mainstems to the eNHD network via levelpath to map COMIDs to mainstem URIs.
#'
#' @param mainstems sf data.frame of active mainstems
#' @param enhd data.frame eNHD v3 network with comid and levelpathi columns
#' @return data.frame with `uri` and `comid` columns
#' @keywords internal
get_v2_lookup <- function(mainstems, enhd) {
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

  v2_out
}

#' Deduplicate mainstem-levelpath assignments
#'
#' Resolves cases where one levelpath contains multiple mainstems by splitting
#' and assigning flowlines to the correct mainstem.
#'
#' @param hr_out_init data.frame initial HR output with potential duplicates
#' @param hr_ref_net data.frame NHDPlusHR reference network
#' @return data.frame with deduplicated mainstem assignments
#' @keywords internal
dedup_mainstem_levelpath <- function(hr_out_init, hr_ref_net) {

  hr_out <- hr_out_init |>
    # first join to get levelpath
    left_join(
      select(hr_ref_net, nhdplushr_id, levelpath), 
      by = c("head_nhdplushr_id" = "nhdplushr_id")
    ) |>
    filter(!is.na(levelpath))

  stopifnot(!any(duplicated(hr_out$nhdplushr_permid)))

  dups <- get_dups(hr_out, "levelpath")

  dup_uri <- unique(dups$uri)

  # split on level path -- for each, we need to break into two
  splits <- group_by(dups, levelpath) |>
    group_split()

  # assign uri defined elsewhere
  dup_assign <- lapply(seq_along(splits), assign_uri, splits = splits, hr_ref_net = hr_ref_net)

  # dup_assign gets mainstem
  dup_assign <- bind_rows(dup_assign) |>
    rename(nhdplushr_permid = permid) |>
    filter(!is.na(uri))

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

  hr_out
}


#' Assign mainstems to flowlines via downstream tracing
#'
#' Handles cases where the mainstem outlet is not on the same levelpath as the
#' head by tracing downstream through the network to find the correct path.
#'
#' @param hr_out_1 data.frame current HR output with some unresolved outlets
#' @param hr_ref_net data.frame NHDPlusHR reference network
#' @return data.frame with corrected mainstem-to-flowline assignments
#' @keywords internal
assign_mainstems_to_flowlines <- function(hr_out_1, hr_ref_net) {
  # checks if the outlet is in the levelpath
  ## could check if it is the outlet of the levelpath?
  out <- group_by(hr_out_1, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))

  # if the defined mainstem outlet isn't part of the local mainstem
  # we need to trace downstream to find it.

  # expect that all outlet checks are NA or TRUE
  # if not, we need to find the correct outlet from ref_net
  tofix <- filter(out, !outlet_check) |>
    ungroup() |>
    select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
    distinct()

  # If this is more than this we need to look into it
  stopifnot(nrow(tofix) < 700)

  # update so the outlet is correct according to the network

  # make index ids to do navigations
  ref_ind <- hydroloom::make_index_ids(hr_ref_net, mode = "to")
  
  # make sure we are dendritic
  stopifnot(max(ref_ind$lengths) == 1)

  have_uri <- hr_out_1$id[!is.na(hr_out_1$uri)]
    
  path_fixes <- pbapply::pblapply(
    seq_len(nrow(tofix)), 
    function(row) {
      tryCatch({
        path <- unname(unlist(
          hydroloom:::navigate_network_dfs(ref_ind, paste0("nhdphr-", tofix$head_nhdplushr_id[row]), "down")
        ))
        out_ind <- which(path == paste0("nhdphr-", tofix$outlet_nhdplushr_id[row]))
        # if out isn't in the path just punt
        if(length(out_ind) != 1) {

          # the first is the one we are searching
          # the second is the last one that isn't mapped yet on this path
          out_ind <- which(path %in% have_uri)[2]

        }
        path[1:out_ind]
      }, error = function(e) NULL)
    }
  )

  path_fixes_df <- dplyr::tibble(
    uri = tofix$uri, 
    head_nhdplushr_id = tofix$head_nhdplushr_id, 
    outlet_nhdplushr_id = tofix$outlet_nhdplushr_id,
    id = path_fixes
  ) |>
    tidyr::unnest(id, keep_empty = TRUE) |>
    left_join(select(hr_ref_net, id, nhdplushr_id, nhdplushr_permid = permid, levelpath), by = "id")

  still_missed <- path_fixes_df$uri[is.na(path_fixes_df$id)]

  # TODO: get to the bottom of why these are overlapping
  path_fixes_df <- filter(path_fixes_df, !is.na(id)) |>
    filter(uri != "https://geoconnex.us/ref/mainstems/2636486") # busted

  path_1 <- path_fixes_df$id[path_fixes_df$uri == "https://geoconnex.us/ref/mainstems/878793"]
  path_2 <- path_fixes_df$id[path_fixes_df$uri == "https://geoconnex.us/ref/mainstems/412800"]

  path_fixes_df$uri[path_fixes_df$id %in% path_1[path_1 %in% path_2]] <- "https://geoconnex.us/ref/mainstems/412800"
  path_fixes_df$head_nhdplushr_id[path_fixes_df$uri == "https://geoconnex.us/ref/mainstems/412800"] <- "23002800025263"
  path_fixes_df$outlet_nhdplushr_id[path_fixes_df$uri == "https://geoconnex.us/ref/mainstems/412800"] <- "23002800026353"

  path_fixes_df <- distinct(path_fixes_df)

  stopifnot(any(duplicated(path_fixes_df$nhdplushr_id)))

  # expect some but not all
  stopifnot(any(path_fixes_df$id %in% out$id))
  stopifnot(!all(path_fixes_df$id %in% out$id))

  # expect all URIs to be present because we initialized with heads
  stopifnot(all(path_fixes_df$uri %in% out$uri))

  update <- select(filter(path_fixes_df, id %in% out$id), id, uri)
  
  update$uri[update$id == "nhdphr-23002800086456"] <- "https://geoconnex.us/ref/mainstems/332734"

  update <- distinct(update)

  stopifnot(!any(duplicated(update$id)))

  # need to add rows and then update rows
  out2 <- out |>
    select(-outlet_check, -toid) |>
    bind_rows(filter(path_fixes_df, !nhdplushr_id %in% out$nhdplushr_id)) |>
    rows_update(
      update,
     by = "id"
    ) |>
    distinct()

  stopifnot(!any(duplicated(out2$nhdplushr_id)))
  
  # NA are the ones that we couldn't get above
  stopifnot(all(sort(out$uri[is.na(out$nhdplushr_id)]) == sort(still_missed)))

  out
}

## UNUSED
#' Walk network to find mainstem flowlines for unmatched URIs
#'
#' For mainstems not resolved by levelpath or prior tracing, walks the full
#' reference network from head to outlet to collect all flowline IDs.
#'
#' @param missed data.frame with uri, head_nhdplushr_id, and outlet_nhdplushr_id
#' @param ref_net sf data.frame full reference network
#' @return data.frame with uri, id, nhdplushr_id, levelpath columns
#' @keywords internal
walk_network_find_mainstem <- function(missed, ref_net) {

  ref_ind <- hydroloom::make_index_ids(ref_net, mode = "to")

  stopifnot(max(ref_ind$lengths) == 1)

  paths <- pbapply::pblapply(
    seq_len(nrow(missed)),
    function(row) {
      # needs to work for the whole network
      head <- missed$head_nhdplushr_id[row]
      head <- ifelse(grepl("nhdpv2", head), paste0("nhdphr-", head), head)
      tryCatch({
        path <- unname(unlist(
          hydroloom:::navigate_network_dfs(
            ref_ind,
            head,
            "down"
          )
        ))
        out_ind <- which(path == paste0("nhdphr-", missed$outlet_nhdplushr_id[row]))
        if (length(out_ind) != 1) stop()
        path[1:out_ind]
      }, error = function(e) NULL)
    }
  )

  dplyr::tibble(
    uri = missed$uri,
    id = paths
  ) |>
    tidyr::unnest(id, keep_empty = TRUE) |>
    filter(!is.na(id)) |>
    mutate(nhdplushr_id = gsub("nhdphr-", "", id)) |>
    left_join(select(hr_ref_net, id, levelpath), by = "id")
}