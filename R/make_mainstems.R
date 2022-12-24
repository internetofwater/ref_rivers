make_mainstems <- function(ms_gpkg, out_f) {
  ms <- sf::read_sf(ms_gpkg)
  
  ms_out <- dplyr::filter(ms, 
                          # This filter is arbitrary but chosen based on the 
                          # size distribution of drainage basin drainage area.
                          # 50sqkm is where it really ramps up.
                          # 200sqkm is where it really tails off.
                          (!is.na(outlet_GNIS_NAME) &  
                             totdasqkm > 50) | 
                            totdasqkm > 200) %>%
    mutate(uri = paste0("https://geoconnex.us/ref/mainstems/", LevelPathI), 
           type = "https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath",
           name_at_outlet = outlet_GNIS_NAME,
           name_at_outlet_gnis_id = outlet_GNIS_ID,
           head_nhdpv2_COMID = ifelse(is.na(head_nhdpv2_COMID), NA, 
                                      paste0("https://geoconnex.us/nhdplusv2/comid/", head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = ifelse(is.na(outlet_nhdpv2_COMID), NA, 
                                        paste0("https://geoconnex.us/nhdplusv2/comid/", outlet_nhdpv2_COMID)),
           head_nhdpv2HUC12 = ifelse(is.na(head_nhdpv2HUC12), NA, 
                                     paste0("https://geoconnex.us/nhdplusv2/huc12/", head_nhdpv2HUC12)),
           outlet_nhdpv2HUC12 = ifelse(is.na(outlet_nhdpv2HUC12), NA, 
                                       paste0("https://geoconnex.us/nhdplusv2/huc12/", outlet_nhdpv2HUC12)),
           lengthkm = round(length, digits = 1),
           outlet_drainagearea_sqkm = round(totdasqkm, digits = 1)) %>%
    select(id = LevelPathI, uri,
           featuretype = type, name_at_outlet, name_at_outlet_gnis_id, head_nhdpv2_COMID, outlet_nhdpv2_COMID, 
           head_nhdpv2HUC12, outlet_nhdpv2HUC12, 
           lengthkm, outlet_drainagearea_sqkm,
           head_rf1ID, outlet_rf1ID, 
           head_nhdpv1_COMID, outlet_nhdpv1_COMID, 
           head_latestHUC12, outlet_latestHUC12) %>%
    mutate(id = as.character(id))
  
  ms_out <- sf::st_transform(ms_out, 4326)
  
  sf::write_sf(ms_out, out_f)
  
  ms_out
}
