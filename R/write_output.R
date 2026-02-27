write_lookups <- function(mainstems, enhd_v3, ref_net_v1, hr_net) {
  enhd <- arrow::read_parquet(enhd_v3)
  ref_net <- sf::read_sf(ref_net_v1)
  hr_lookup <- readr::read_csv(hr_net)

  hr_ref_net <- sf::st_drop_geometry(ref_net) |>
    filter(source == "nhdphr") |>
    select(nhdplushr_permid = id, levelpath) |>
    distinct() |>
    left_join(select(hr_lookup, nhdplushr_id = id, permid), by = c("nhdplushr_permid" = "permid")) |>
    distinct() |>
    mutate(nhdplushr_id = as.character(nhdplushr_id))
  
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
  
  if(all(is.na(out$outlet_check) | !out$outlet_check)) stop("all levelpaths should have the outlet in them")
  
  out <- select(ungroup(out), uri, comid)  

  readr::write_csv(out, "out/nhdpv2_lookup.csv")

  out <- sf::st_drop_geometry(mainstems) |>
    filter(!superseded) |>
    select(uri, head_nhdplushr_id, outlet_nhdplushr_id, lengthkm, downstream_mainstem_id) |>
    distinct() |>
    # first join to get levelpath
    left_join(select(hr_ref_net, nhdplushr_id, levelpath), by = c("head_nhdplushr_id" = "nhdplushr_id")) |>
    filter(!is.na(levelpath))
    
  get_dups <- function (x, col) {
    x[x[[col]] %in% x[[col]][duplicated(x[[col]])], ]
  }
  dups <- get_dups(out, "levelpath")

  # see #12 -- but as long as they are disconnected, we'll move on by removing the shorter of the two
  stopifnot(all(dups$downstream_mainstem_id == dups$uri))

  dups <- group_by(dups, levelpath) |>
    filter(lengthkm == min(lengthkm))

  out <- filter(out, !levelpath %in% dups$levelpath) |>
    bind_rows(dups) |>
    select(-lengthkm, -downstream_mainstem_id) |>
    # now join by levelpath to get all ids along each path
    left_join(select(hr_ref_net, levelpath, nhdplushr_id, nhdplushr_permid), by = "levelpath")
  
  out <- group_by(out, levelpath) |>
    mutate(outlet_check = any(nhdplushr_id == outlet_nhdplushr_id))
  
  if(all(is.na(out$outlet_check) | !out$outlet_check)) stop("all levelpaths should have the outlet in them")
  
  out <- select(ungroup(out), uri, nhdplushr_permid, nhdplushr_id)  

  readr::write_csv(out, "out/nhdphr_lookup.csv")

  "out/nhdpv2_lookup.csv"
}