get_mainstems_db_1 <- function() {
  
  # Blodgett, D.L., 2022, Mainstem Rivers of the Conterminous United States: 
  # U.S. Geological Survey data release, https://doi.org/10.5066/P9BTKP3T. 
  # https://www.sciencebase.gov/catalog/item/60cb5edfd34e86b938a373f4

  ms_gpkg <- "data/mainstems/mainstems_summary_1.gpkg"
  
  if(!file.exists(ms_gpkg)) {
    dir.create(dirname(ms_gpkg), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("60cb5edfd34e86b938a373f4", 
                                     names = "mainstems_summary.gpkg", 
                                     destinations = ms_gpkg)
  }
  
  ms_gpkg
}


get_mainstems_db_2 <- function() {
  
  # David L Blodgett, 2023, Mainstem Rivers of the Conterminous United States 
  # (version 2.0): U.S. Geological Survey data release, 
  # https://doi.org/10.5066/P92U7ZUT. 
  # https://www.sciencebase.gov/catalog/item/63cb38b2d34e06fef14f40ad
  
  ms_gpkg <- "data/mainstems/mainstems_summary_2.gpkg"
  
  if(!file.exists(ms_gpkg)) {
    dir.create(dirname(ms_gpkg), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("63cb38b2d34e06fef14f40ad", 
                                     names = "mainstems_summary_v2.gpkg", 
                                     destinations = ms_gpkg)
  }
  
  ms_gpkg
}

get_enhd_1 <- function() {
  
  # Blodgett, D.L., 2022, Updated CONUS river network attributes based on the 
  # E2NHDPlusV2 and NWMv2.1 networks: U.S. Geological Survey data release, 
  # https://doi.org/10.5066/P9W79I7Q. 
  # https://www.sciencebase.gov/catalog/item/60c92503d34e86b9389df1c9

  enhd_pqt <- "data/enhd/enhd_nhdplusatts_1.parquet"
  
  if(!file.exists(enhd_pqt)) {
    dir.create(dirname(enhd_pqt), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("60c92503d34e86b9389df1c9", 
                                     names = "enhd_nhdplusatts.parquet", 
                                     destinations = enhd_pqt)
  }
  
  enhd_pqt
  
}

get_enhd_2 <- function() {
  
  # David L. Blodgett, 2023, Updated CONUS river network attributes based on 
  # the E2NHDPlusV2 and NWMv2.1 networks (version 2.0): U.S. Geological Survey 
  # data release, https://doi.org/doi:10.5066/P976XCVT. 
  # https://www.sciencebase.gov/catalog/item/63cb311ed34e06fef14f40a3
  
  enhd_pqt <- "data/enhd/enhd_nhdplusatts_2.parquet"
  
  if(!file.exists(enhd_pqt)) {
    dir.create(dirname(enhd_pqt), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("63cb311ed34e06fef14f40a3", 
                                     names = "enhd_nhdplusatts.parquet", 
                                     destinations = enhd_pqt)
  }
  
  enhd_pqt
  
}

# joins new network attributes for nhdplusv2 and returns a ready to use copy.
join_enhd <- function(enhd_pqt, nhdp_geo) {
  
  enhd <- arrow::read_parquet(enhd_pqt)
  
  select(nhdp_geo, comid = COMID) %>%
    right_join(enhd, by = "comid")
}
