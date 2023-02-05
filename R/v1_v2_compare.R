# to run interactively, use:
# old_ms <- tar_read("mainstems_v1")
# new_ms <- tar_read("mainstems_v2")
# old_net <- tar_read("enhd_v1")
# new_net <- tar_read("enhd_v2")

# combines V1 and V2 mainstems returning V1 mainstems to keep, V1 mainstems to 
# supersede, and V2 mainstems to register.
# If headwater and outlet catchments match, it's consired a match. 
# If outlet catchments go to the same downstream path and are near eachother, 
# it's considered a match.
reconcile_mainstems <- function(old_ms, new_ms, old_net, new_net) {
  old_ms <- sf::read_sf(old_ms)
  new_ms <- sf::read_sf(new_ms)
  old_net <- arrow::read_parquet(old_net)
  new_net <- arrow::read_parquet(new_net)
  
  # filter to only those in the geoconnex set
  # this is the filter used in the V1 mainstems registration
  old_ms <- dplyr::filter(old_ms,
                          (!is.na(outlet_GNIS_NAME) &  
                             totdasqkm > 50) | 
                            totdasqkm > 200)
  
  # grab the outlet of the new mainstems
  new_lp_out <- new_net |>
    group_by(levelpathi) |>
    filter(hydroseq == min(hydroseq)) |>
    select(outlet_comid = comid, new_levelpathi = levelpathi, new_dnlevelpat = dnlevelpat) |>
    ungroup()
  
  # grab the outlet of the old mainstems (only used for a check below)
  old_lp_out <- old_net |>
    group_by(levelpathi) |>
    filter(hydroseq == min(hydroseq)) |>
    select(old_outlet_comid_check = comid, old_lp = levelpathi, old_dnlevelpat = dnlevelpat) |>
    ungroup()
  
  # create an old_lp table with a bunch of attributes
  old_lp <- select(sf::st_drop_geometry(old_ms), 
                   old_lp = LevelPathI, 
                   # we will use head comid as the match
                   head_nhdpv2_COMID = head_nhdpv2_COMID, 
                   old_outlet_comid = outlet_nhdpv2_COMID) |>
    # this was just a check to make sure things were what I thought they were.
    left_join(old_lp_out, by = "old_lp")
  
  # This verifies that we loaded the same version of the mainstem summar and net atts
  if(!all(old_lp$old_outlet_comid == old_lp$old_outlet_comid_check)) stop("IDs don't match?")
  
  old_lp <- select(old_lp, -old_outlet_comid_check)
  
  
  ############### FIRST CHECK WHERE HEAD AND OUTLET MATCH ######################
  # join old to new using head comid as the match.
  check_df <- old_lp  |>
    left_join(select(new_ms, new_levelpathi = LevelPathI, head_nhdpv2_COMID, outlet_nhdpv2_COMID),
                          by = "head_nhdpv2_COMID") |>
    # also join new_dnlevelpath by new_levelpathi for outlet checks
    left_join(distinct(select(new_lp_out, new_levelpathi, new_dnlevelpat)),
              by = "new_levelpathi") |>
    sf::st_sf()
  
  rm(old_lp, old_lp_out, new_lp_out)
  
  remove <- filter(check_df, sf::st_is_empty(check_df))
  
  check_df <- filter(check_df, !old_lp %in% remove$old_lp)
  
  # if the outlet comid is the same -- it's a match
  matches <- check_df |>
    filter(old_outlet_comid == outlet_nhdpv2_COMID)
  
  # add the dnlevelpath and check if it at least goes the same place.
  check_df <- check_df |>
    left_join(select(sf::st_drop_geometry(matches), mapped_old_dlp = old_lp, new_levelpathi), 
              by = c("new_dnlevelpat" = "new_levelpathi")) |>
    # filter out stuff we already matched.
    filter(!old_lp %in% matches$old_lp)
  
  # these go to the same downstream path
  matches_dm <- check_df |>
    filter(mapped_old_dlp == old_dnlevelpat)
  
  # get the old mainstem for comparison
  matches_dm_old <- sf::st_sf(old_ms) |>
    filter(LevelPathI %in% matches_dm$old_lp)
  
  matches_dm$diff_dist <- sf::st_distance(hydroloom::get_node(matches_dm_old),
                                          hydroloom::get_node(matches_dm), 
                                          by_element = TRUE)
  rm(matches_dm_old)
  
  matches_dm$lengthm <- sf::st_length(matches_dm)
  
  matches_dm$diff <- as.numeric(matches_dm$diff_dist / matches_dm$lengthm)
  
  # assume anything where diff_dist is 1/10th the length or less is a match
  matches_dm <- filter(matches_dm, diff < 0.1)
  
  check_df <- filter(check_df, !old_lp %in% matches_dm$old_lp)
  
  # these go to something that changed or something different.
  # we can check how far these outlets moved
  matches_diff_dm <- check_df |>
    filter((!is.na(mapped_old_dlp) &
              mapped_old_dlp != old_dnlevelpat))
  
  matches_diff_dm_old <- sf::st_sf(old_ms) |>
    filter(LevelPathI %in% matches_diff_dm$old_lp)
  
  matches_diff_dm$diff_dist <- sf::st_distance(hydroloom::get_node(matches_diff_dm_old),
                                       hydroloom::get_node(matches_diff_dm), 
                                       by_element = TRUE)
  
  rm(matches_diff_dm_old)
  
  matches_diff_dm$lengthm <- sf::st_length(matches_diff_dm)
  matches_diff_dm$diff <- as.numeric(matches_diff_dm$diff_dist / matches_diff_dm$lengthm)

  matches_diff_dm <- filter(matches_diff_dm, diff < 0.1)
  
  check_df <- filter(check_df, !old_lp %in% matches_diff_dm$old_lp)
  
  # these are really different many are where what was a headwater is now 
  # somewhere along a mainstem others just flow to something that changed.
  na_dm <- check_df |>
    filter(!check_df$old_lp %in% matches_diff_dm$old_lp & 
             is.na(mapped_old_dlp))
  
  na_dm_old <- old_ms |>
    filter(LevelPathI %in% na_dm$old_lp)
  
  na_dm$diff_dist <- sf::st_distance(hydroloom::get_node(na_dm_old), 
                                     hydroloom::get_node(na_dm), 
                                     by_element = TRUE)
  
  na_dm$lengthm <- sf::st_length(na_dm)
  
  na_dm$diff <- as.numeric(na_dm$diff_dist / na_dm$lengthm)
  
  matches_na_dm <- filter(na_dm, diff < 0.1)
  
  rm(na_dm, na_dm_old)
  
  # what's left does not match outlets and has an outlet that is more than 
  # 1/10th the mainstem length away from the original. All of these will be
  # superseded in the registry.
  check_df <- filter(check_df, !old_lp %in% matches_na_dm$old_lp)
  
  return(list(match = matches, 
              update_outlet = bind_rows(matches_dm, 
                                        matches_na_dm, 
                                        matches_diff_dm),
              remove = remove,
              update = check_df))
}
