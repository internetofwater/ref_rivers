# to run interactively, use:
old_ms <- tar_read("mainstems_v1")
new_ms <- tar_read("mainstems_v2")
old_net <- tar_read("enhd_v1")
new_net <- tar_read("enhd_v2")
changes <- tar_read("reconciled_mainstems")

make_mainstems <- function(old_ms, new_ms, old_net, new_net, changes, out_f) {
  old_ms <- sf::read_sf(old_ms)
  new_ms <- sf::read_sf(new_ms)
  old_net <- arrow::read_parquet(old_net)
  new_net <- arrow::read_parquet(new_net)
  
  new_dm <- distinct(select(new_net, levelpathi, dnlevelpat))
  
  orig_ms <- old_ms |>
    dplyr::filter(# This filter is arbitrary but chosen based on the 
      # size distribution of drainage basin drainage area.
      # 50sqkm is where it really ramps up.
      # 200sqkm is where it really tails off.
      (!is.na(outlet_GNIS_NAME) &  
         totdasqkm > 50) | 
        totdasqkm > 200)
  
  # main set just need levelpathid mapped
  
  match <- right_join(rename(new_ms, new_levelpathi = "LevelPathI"), 
                      select(sf::st_drop_geometry(changes$match), 
                             mainstem_id = old_lp, new_levelpathi), 
                      by = "new_levelpathi")
  
  # update outlet set needs levelpathid mapped -- changes will be in registry
  # could add alternate outlet for these?
  match_new_outlet <- right_join(rename(new_ms, new_levelpathi = "LevelPathI"),
                                 select(sf::st_drop_geometry(changes$update_outlet),
                                        mainstem_id = old_lp, new_levelpathi, old_outlet_comid),
                                 by = "new_levelpathi")
  
  # These are all the ones that we are going to carry forward at all.
  ms_out <- bind_rows(match, match_new_outlet)
  
  # these are new as indicated by the new mainstems dataset but not mapped.
  extra <- rename(new_ms, new_levelpathi = LevelPathI) |>
    filter(!new_levelpathi %in% ms_out$new_levelpathi) |>
    dplyr::filter(# This filter is arbitrary but chosen based on the 
      # size distribution of drainage basin drainage area.
      # 50sqkm is where it really ramps up.
      # 200sqkm is where it really tails off.
      (!is.na(outlet_GNIS_NAME) &  
         totdasqkm > 50) | 
        totdasqkm > 200) |>
    # Use the new levelpath as the mainstem id.
    mutate(mainstem_id = new_levelpathi)

  # some ids that need to be updated so we can get down mainstem later on.
  available <- seq(min(orig_ms$LevelPathI), max(orig_ms$LevelPathI))
  available <- available[!available %in% orig_ms$LevelPathI]
  
  update <- extra$mainstem_id %in% ms_out$mainstem_id
  
  orig_update <- extra$mainstem_id[update]
  new_update <- available[1:sum(update)]
  
  # update in extra
  extra$mainstem_id[update] <- new_update
  extra$new_levelpathi[update] <- new_update
  
  # update in dm tables
  new_dm$levelpathi[match(orig_update, new_dm$levelpathi)] <- new_update
  new_dm$dnlevelpat[match(orig_update, new_dm$dnlevelpat)] <- new_update
    
  
  if(any(extra$mainstem_id %in% ms_out$mainstem_id)) 
    stop("can't add mainstems that exist")
  
  ms_out <- bind_rows(ms_out, extra)
  
  ms_out$superseded = FALSE
  
  # the remove set is where no headwater match exists (a divergence got switched and it's no longer a head)
  # These should not have been added but will be included as superseded and mapped to their replacement.
  # also...
  # the update set need to have a list of superseded mainstems established.
  
  # grab mainstem ids in the remove and update sets. Get the new potential mainstem id for them.
  # If these are in the set that goes through, we can map them, otherwise, 
  # they will just be superseded with no replacement until one has been identified later.
  match_no_match <- select(old_net, comid, mainstem_id = levelpathi) |>
    filter(mainstem_id %in% c(changes$remove$old_lp, changes$update$old_lp)) |>
    left_join(select(new_net, comid, new_mainstemid = levelpathi), by = "comid") |>
    select(-comid) |>
    # this removes levelpath identifiers that will not be minted as mainstems.
    mutate(new_mainstemid = ifelse(new_mainstemid %in% ms_out$new_levelpathi, 
                                   paste0("https://geoconnex.us/ref/mainstems/", new_mainstemid), NA)) |>
    distinct()
  
  # get match_no_match into a list column format
  nm <- sapply(split(match_no_match$new_mainstemid, 
                     match_no_match$mainstem_id), function(x) {
                       o <- x[!is.na(x)]
                       if(length(o) == 0) { NA } else {
                         paste0("['", paste(o, collapse = "', '"), "']") 
                       } })
  
  match_no_match <- data.frame(mainstem_id = as.integer(names(nm)), 
                               new_mainstemid = I(nm))
  
  superseded <- filter(orig_ms, LevelPathI %in% match_no_match$mainstem_id) |>
    rename(mainstem_id = LevelPathI) |>
    left_join(match_no_match, by = "mainstem_id") |>
    mutate(superseded = TRUE) |>
    distinct()
    
  ms_out <- bind_rows(ms_out, superseded)
  
  # do some checks
  # make sure we have all the original mainstem ids
  if(!all(orig_ms$LevelPathI %in% ms_out$mainstem_id)) 
    stop("must include all historic mainstem ids")
  # Make sure all out non superseded have new_levelpathi for network
  if(any(!ms_out$superseded & is.na(ms_out$new_levelpathi))) 
    stop("non-superseded mainstems must exist on new network")

  # add downstream mainstem (for downstream waterbody mapping)
  new_dm <- filter(new_dm, !levelpathi == dnlevelpat)
  id_map <- distinct(select(sf::st_drop_geometry(filter(ms_out, !superseded)), 
                            new_levelpathi, down_mainstem_id = mainstem_id))
  
  ms_out <- left_join(ms_out, new_dm, by = c("new_levelpathi" = "levelpathi")) |>
    left_join(id_map, 
              by = c("dnlevelpat" = "new_levelpathi")) |>
    select(-dnlevelpat, -new_levelpathi) |>
    mutate(levelpathi = mainstem_id, dnlevelpat = ifelse(is.na(down_mainstem_id), 0, down_mainstem_id)) |>
    add_dm()
  
  ms_out <- ms_out |>
    mutate(uri = paste0("https://geoconnex.us/ref/mainstems/", mainstem_id), 
           type = "['https://www.opengis.net/def/schema/hy_features/hyf/HY_FlowPath', 'https://www.opengis.net/def/schema/hy_features/hyf/HY_WaterBody']",
           name_at_outlet = outlet_GNIS_NAME,
           name_at_outlet_gnis_id = outlet_GNIS_ID,
           downstream_mainstem_id = ifelse(!is.na(down_mainstem_id), 
                                           paste0("https://geoconnex.us/ref/mainstems/", down_mainstem_id),
                                           down_mainstem_id),
           head_nhdpv2_COMID = ifelse(is.na(head_nhdpv2_COMID), NA, 
                                      paste0("https://geoconnex.us/nhdplusv2/comid/", head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = ifelse(is.na(outlet_nhdpv2_COMID), NA, 
                                        paste0("https://geoconnex.us/nhdplusv2/comid/", outlet_nhdpv2_COMID)),
           head_nhdpv2HUC12 = ifelse(is.na(head_nhdpv2HUC12), NA, 
                                     paste0("https://geoconnex.us/nhdplusv2/huc12/", head_nhdpv2HUC12)),
           outlet_nhdpv2HUC12 = ifelse(is.na(outlet_nhdpv2HUC12), NA, 
                                       paste0("https://geoconnex.us/nhdplusv2/huc12/", outlet_nhdpv2HUC12)),
           lengthkm = round(length, digits = 1),
           outlet_drainagearea_sqkm = round(totdasqkm, digits = 1),
           new_mainstemid = sapply(unname(new_mainstemid), paste, collapse = ", "),
           superseded = ifelse(is.na(superseded), FALSE, superseded)) |>
    mutate(new_mainstemid = unname(ifelse(new_mainstemid == "", NULL, new_mainstemid))) |>
    select(id = mainstem_id, uri,
           featuretype = type,
           downstream_mainstem_id,
           encompassing_mainstem_basins = down_levelpaths,
           name_at_outlet, 
           name_at_outlet_gnis_id, 
           head_nhdpv2_COMID, outlet_nhdpv2_COMID, 
           head_nhdpv2HUC12, outlet_nhdpv2HUC12, 
           lengthkm, outlet_drainagearea_sqkm,
           head_rf1ID, outlet_rf1ID, 
           head_nhdpv1_COMID, outlet_nhdpv1_COMID, 
           head_2020HUC12, outlet_2020HUC12,
           superseded, new_mainstemid) %>%
    mutate(id = as.character(id))
  
  ms_out <- sf::st_transform(ms_out, 4326)
  
  sf::write_sf(ms_out, out_f)
  
  ms_out
}

add_dm <- function(network) {
  
  lp <- select(network, levelpathi, dnlevelpat) %>%
    filter(levelpathi != dnlevelpat)
  
  dnlp <- data.frame(lp = seq(0, (max(lp$levelpathi)))) %>%
    left_join(lp, by = c("lp" = "levelpathi"))
  
  dnlp <- dnlp$dnlevelpat[2:nrow(dnlp)]
  
  get_dlp <- function(x, dnlp) {
    out <- dnlp[x]
    
    if(out == 0) {
      return()
    }
    
    c(out, get_dlp(out, dnlp))
    
  }
  
  all_lp <- pbapply::pblapply(unique(lp$levelpathi), get_dlp, dnlp = dnlp)
  
  all_lp <- data.frame(levelpathi = unique(lp$levelpathi),
                       dnlp = sapply(all_lp, function(x) if(length(x) > 0) {
                         paste0("['", paste(paste0("https://geoconnex.us/ref/mainstems/", x), 
                                            collapse = "', '"), "']")
                         } else NA))
  
  network %>%
    left_join(all_lp, by = "levelpathi") %>%
    rename(down_levelpaths = dnlp)
}
