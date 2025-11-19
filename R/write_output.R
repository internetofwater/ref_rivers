write_lookups <- function(mainstems, enhd_v2) {
  enhd_v2 <- arrow::read_parquet(enhd_v2)
  
  out <- sf::st_drop_geometry(mainstems) |>
    filter(!superseded) |>
    select(uri, head_nhdpv2_COMID, outlet_nhdpv2_COMID) |>
    mutate(head_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               outlet_nhdpv2_COMID))) |>
    left_join(distinct(select(enhd_v2, comid, levelpathi)),
              by = c("head_nhdpv2_COMID" = "comid")) |>
    left_join(distinct(select(enhd_v2, comid, levelpathi)),
              by = "levelpathi")
  
  out <- group_by(out, levelpathi) |>
    mutate(outlet_check = any(comid == outlet_nhdpv2_COMID))
  
  if(any(!out$outlet_check)) stop("all levelpaths should have the outlet in them")
  
  out <- select(ungroup(out), uri, comid)  

  readr::write_csv(out, "out/nhdpv2_lookup.csv")
}