# to run interactively, use:
# library(targets)
# library(dplyr)
# library(sf)
# old_ms <- tar_read("mainstems_v2")
# new_ms <- tar_read("mainstems_v3")
# old_net <- tar_read("enhd_v2")
# new_net <- tar_read("enhd_v3")
# ref_net <- tar_read("ref_net_v1")
# source("R/get_data.R")

# loads comparison tables to check if headwater and outlet pairs are comparable
# returns cases where headwater / outlet pairs do not match
reconcile_mainstems <- function(old_ms, new_ms, old_net, new_net, ref_net) {
  old_ms <- sf::read_sf(old_ms)
  new_ms <- get_mainstem_summary_v3(new_ms)
  old_net <- arrow::read_parquet(old_net)
  new_net <- arrow::read_parquet(new_net)
  ref_net <- sf::read_sf(ref_net)
  
  both_ref_net <- select(st_drop_geometry(ref_net), lp_mainstem_v3, source) |>
    group_by(lp_mainstem_v3) |>
    filter("nhdpv2" %in% source & "nhdphr" %in% source) |>
    ungroup() |>
    distinct() |>
    filter(!lp_mainstem_v3 %in% 37504) # leave in nhdplusv2 domain
  
  switch_to_hr <- ref_net$id[ref_net$lp_mainstem_v3 %in% both_ref_net$lp_mainstem_v3]

  # split ref_net into nhdplusv2 source and nhdplushr source
  # we need to work through them seperately
  nhdplusv2_ref_net <- dplyr::filter(ref_net, source == "nhdpv2" & !id %in% switch_to_hr) |>
    dplyr::mutate(lp_mainstem_v3 = as.integer(lp_mainstem_v3))
  nhdplushr_ref_net <- dplyr::filter(ref_net, source == "nhdphr" | id %in% switch_to_hr) |>
    dplyr::mutate(lp_mainstem_v3 = as.integer(lp_mainstem_v3))
  
  # make double sure
  stopifnot(all(ref_net$id[ref_net$source == "nhdpv2" | ref_net$source == "nhdphr"] %in% c(nhdplusv2_ref_net$id, nhdplushr_ref_net$id)))
  stopifnot(all(switch_to_hr %in% nhdplushr_ref_net$id))
  
  nhdplusv2_check_df <- get_nhdplusv2_domain_check_df(new_ms, old_ms, new_net, old_net, nhdplusv2_ref_net)
  
  if(nrow(nhdplusv2_check_df) > 0) stop("nothing should have changed!")
  
  new_ms_no_v2 <- dplyr::filter(new_ms, !lp_mainstem_v3 %in% nhdplusv2_ref_net$lp_mainstem_v3)

  # these deprecated mainstems have been verified through manual review.
  # they are all in the NHDPlusHR domain
  deprecate <- sf::read_sf("data/review/deprecated_v3.geojson")
  
  # these are changes that were manually reviewed and are ok to keep / not deprecate
  # they are all in the NHDPlusHR domain
  changelog <- readr::read_csv("data/review/changelog_v3.csv")
    
  # Compares new mainstems as defined in ref_net nhdplushr to those defined in old_net
  nhdplushr_check_df <- get_nhdplushr_domain_check_df(nhdplushr_ref_net, old_ms, 
                                                      old_net, new_ms_no_v2, new_net,
                                                      deprecate, changelog)
  
  bad_change <- nhdplushr_check_df[!nhdplushr_check_df$good_head & !nhdplushr_check_df$good_outlet,]
  
  stopifnot(all(bad_change$checked))
  
  # look at lp_mainstem_v3 that are in this set that aren't in the new_ms set.
  new_lp <- nhdplushr_ref_net$lp_mainstem_v3[!is.na(nhdplushr_ref_net$lp_mainstem_v3) & 
                                               !nhdplushr_ref_net$lp_mainstem_v3 %in% new_ms$lp_mainstem_v3]
  
  stopifnot(all(new_lp > 7e6))
  
  # these are all the mainstems that are staying the same
  keep <- dplyr::filter(new_ms, !is.na(reference_mainstem) & !reference_mainstem %in% deprecate$reference_mainstem)
  
  # these are all the mainstems that are being added fand are in the NHDPlusV2 domain
  add <- dplyr::filter(new_ms, is.na(reference_mainstem) & 
                         !is.na(head_nhdplushr_id) & !is.na(outlet_nhdplushr_id) &
                         !is.na(head_nhd_permid) & !is.na(outlet_nhd_permid))
  
  # these are all the mainstems that are being replaced in the NHDPlusHR domain
  nhdphr_source_replace <- dplyr::filter(new_ms, reference_mainstem %in% nhdplushr_check_df$reference_mainstem)
  
  # these are all the mainstems that are being added in the NHDPlusHR domain
  nhdphr_source_new <- dplyr::filter(new_ms, lp_mainstem_v3 %in% add$lp_mainstem_v3 & 
                                       lp_mainstem_v3 %in% nhdplushr_ref_net$lp_mainstem_v3)
  
  return(list(keep = keep,
              add = add, 
              nhdphr_source_replace = nhdphr_source_replace,
              nhdphr_source_new = nhdphr_source_new,
              deprecate = deprecate,
              changelog = changelog,
              new_lp = new_lp))
}

# investigates the lp_mainstem_v3 
get_nhdplushr_domain_check_df <- function(nhdplushr_ref_net, old_ms, old_net, new_ms_no_v2, new_net, deprecate, changelog) {
  
  # Verify what levelpaths are present in the new network
  missing <- filter(new_ms_no_v2, !lp_mainstem_v3 %in% nhdplushr_ref_net$lp_mainstem_v3 & !is.na(reference_mainstem))
  
  # the check list includes all the missing reference mainstems!
  stopifnot(all(missing$reference_mainstem %in% deprecate$reference_mainstem | missing$reference_mainstem == "https://geoconnex.us/ref/mainstems/2244483"))
  
  # remove additional problematic mainstems in deprecation list
  new_ms_no_v2 <- filter(new_ms_no_v2, !reference_mainstem %in% deprecate$reference_mainstem)
  nhdplushr_ref_net <- filter(nhdplushr_ref_net, !reference_mainstem %in% deprecate$reference_mainstem)
  # For those that are present, check headwater / outlet locations
  
  # lp_mainstem_v3
  new_ms_no_v2 <- filter(new_ms_no_v2, lp_mainstem_v3 %in% nhdplushr_ref_net$lp_mainstem_v3 & !is.na(reference_mainstem))
  
  # nhdplushr_ref_net candidates to replace them
  nhdplushr_ref_net <- filter(nhdplushr_ref_net, lp_mainstem_v3 %in% new_ms_no_v2$lp_mainstem_v3 | lp_mainstem_v3 > 7e6)
  
  # we have for sure accounted for all reference mainstems in new_ms_no_v2
  stopifnot(all(nhdplushr_ref_net$reference_mainstem %in% new_ms_no_v2$reference_mainstem | nhdplushr_ref_net$lp_mainstem_v3 > 7e6))
  
  nhdplushr_ref_net <- arrange(nhdplushr_ref_net, reference_mainstem) |>
    group_by(reference_mainstem) |>
    group_split()

  new_ms_no_v2 <- arrange(new_ms_no_v2, reference_mainstem) |>
    hydroloom::st_compatibalize(nhdplushr_ref_net[[1]])

  # we have a unique data.frame keyed on reference_mainstem
  stopifnot(nrow(new_ms_no_v2) == length(unique(new_ms_no_v2$reference_mainstem)))
  
  checks <- pbapply::pblapply(seq_len(nrow(new_ms_no_v2)), \(i) {
    message(i)
    old <- new_ms_no_v2[i,]
    
    new <- nhdplushr_ref_net[[i]]  
  
    # we for sure have the same reference mainstem
    stopifnot(new$reference_mainstem[1] == old$reference_mainstem)
  
    # TODO: remove the total_da_sqkm filter here once 
    # https://code.usgs.gov/wma/nhgf/reference-fabric/reference-network/-/issues/6
    # is done
    new_head <- filter(new, !id %in% toid) |>
      filter(total_da_sqkm == min(total_da_sqkm)) |>
      hydroloom::get_node("start")
    
    new_out <- filter(new, !toid %in% id) |>
      filter(total_da_sqkm == max(total_da_sqkm)) |>
      hydroloom::get_node("end")
    
    old_head <- hydroloom::get_node(old, "start")
    old_out <- hydroloom::get_node(old, "end")
    
    # make sure we have a clean set of old and new heads and outlets
    stopifnot(nrow(old_head) == 1)
    stopifnot(nrow(new_head) == 1)
    stopifnot(nrow(old_out) == 1)
    stopifnot(nrow(new_out) == 1)
  
    data.frame(reference_mainstem = old$reference_mainstem, 
               head_dist = sf::st_distance(old_head, new_head), 
               out_dist = sf::st_distance(old_out, new_out))
        
  })
  
  checks <- bind_rows(checks)
  
  checks$checked <- checks$reference_mainstem %in% changelog$reference_mainstem 
    
  checks <- left_join(checks, select(new_ms_no_v2, reference_mainstem, length))

  checks$outlet_diff <- as.numeric(checks$out_dist) / (checks$length * 1000)
  
  checks$head_diff <- as.numeric(checks$head_dist) / (checks$length* 1000)

  checks$good_outlet <- checks$outlet_diff < 0.1
  checks$good_head <- checks$head_diff < 0.1      
  
  return(checks)
}

get_nhdplusv2_domain_check_df <- function(new_ms, old_ms, new_net, old_net, nhdplusv2_ref_net) {
  # first, look at only NHDPlusV2 sourced mainstems
  new_ms_ref <- filter(new_ms, !is.na(reference_mainstem) & lp_mainstem_v3 %in% nhdplusv2_ref_net$lp_mainstem_v3)
  old_ms_ref <- filter(old_ms, LevelPathI %in% new_ms_ref$lp_mainstem_v2)
  
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
  old_lp <- select(sf::st_drop_geometry(old_ms_ref), 
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
    left_join(select(new_ms_ref, new_levelpathi = lp_mainstem_v3, head_nhdpv2_COMID, outlet_nhdpv2_COMID),
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
  
  if(nrow(matches_dm) > 0) {
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
  }
  
  # these go to something that changed or something different.
  # we can check how far these outlets moved
  matches_diff_dm <- check_df |>
    filter((!is.na(mapped_old_dlp) &
              mapped_old_dlp != old_dnlevelpat))
  
  matches_diff_dm_old <- sf::st_sf(old_ms) |>
    filter(LevelPathI %in% matches_diff_dm$old_lp) |>
    hydroloom::st_compatibalize(matches_diff_dm)
  
  if(nrow(matches_diff_dm_old) > 0) {
    matches_diff_dm$diff_dist <- sf::st_distance(hydroloom::get_node(matches_diff_dm_old),
                                                 hydroloom::get_node(matches_diff_dm), 
                                                 by_element = TRUE)
    
    rm(matches_diff_dm_old)
    
    matches_diff_dm$lengthm <- sf::st_length(matches_diff_dm)
    matches_diff_dm$diff <- as.numeric(matches_diff_dm$diff_dist / matches_diff_dm$lengthm)
    
    matches_diff_dm <- filter(matches_diff_dm, diff < 0.1)
    
  }
  
  check_df <- filter(check_df, !old_lp %in% matches_diff_dm$old_lp)
  
  # these are really different many are where what was a headwater is now 
  # somewhere along a mainstem others just flow to something that changed.
  na_dm <- check_df |>
    filter(!check_df$old_lp %in% matches_diff_dm$old_lp & 
             is.na(mapped_old_dlp))
  
  na_dm_old <- old_ms |>
    filter(LevelPathI %in% na_dm$old_lp) |>
    hydroloom::st_compatibalize(na_dm)
  
  na_dm$diff_dist <- sf::st_distance(hydroloom::get_node(na_dm_old), 
                                     hydroloom::get_node(na_dm), 
                                     by_element = TRUE)
  
  na_dm$lengthm <- sf::st_length(na_dm)
  
  na_dm$diff <- as.numeric(na_dm$diff_dist / na_dm$lengthm)
  
  matches_na_dm <- filter(na_dm, diff < 0.1)
  
  # what's left does not match outlets and has an outlet that is more than 
  # 1/10th the mainstem length away from the original. All of these will be
  # superseded in the registry.
  check_df <- filter(check_df, !old_lp %in% matches_na_dm$old_lp)
}

