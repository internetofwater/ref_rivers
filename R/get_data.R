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

get_mainstems_db_3 <- function() {
  
  # Blodgett, D.L., 2025 Mainstem Rivers of the Conterminous United States 
  # version 3.0, July 2025): U.S. Geological Survey data release, 
  # https://doi.org/10.5066/P13LNDDQ.
  # https://www.sciencebase.gov/catalog/item/65cbc0b3d34ef4b119cb37e9
  
  ms_gpkg <- "data/mainstems/hr_mainstem_summary_v3.gpkg"
  
  if(!file.exists(ms_gpkg)) {
    dir.create(dirname(ms_gpkg), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("65cbc0b3d34ef4b119cb37e9", 
                                     names = "hr_mainstem_summary_v3.gpkg", 
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

get_enhd_3 <- function() {
  
  # David L. Blodgett, 2023, Updated CONUS river network attributes based on 
  # the E2NHDPlusV2 and NWMv2.1 networks (version 2.0): U.S. Geological Survey 
  # data release, https://doi.org/doi:10.5066/P976XCVT. 
  # https://www.sciencebase.gov/catalog/item/63cb311ed34e06fef14f40a3
  
  enhd_pqt <- "data/enhd/enhd_nhdplusatts_3.parquet"
  
  if(!file.exists(enhd_pqt)) {
    dir.create(dirname(enhd_pqt), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("65cbbb98d34ef4b119cb37c9", 
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


get_ref_network_1 <- function() {
  # https://code.usgs.gov/wma/nhgf/reference-fabric/reference-network/-/packages
  
  ref_net_gpkg <- "data/reference_network/reference_network_1.gpkg"

  if(!file.exists(ref_net_gpkg)) {
    dir.create(dirname(ref_net_gpkg), recursive = TRUE, showWarnings = FALSE)
    
    ref_net_gpkg_zip <- paste0(ref_net_gpkg, ".zip")
    
    url <- "https://code.usgs.gov/wma/nhgf/reference-fabric/reference-network/-/package_files/17267/download"
    sha_256 <- "1747daee3ddd5b0392018f998766ebbf93dbce96270b616b8a9c640d1ab51ebc"
    
    f <- httr::GET(url, httr::write_disk(ref_net_gpkg_zip))
    
    if(!f$status_code == 200) stop("error downloading")
    
    hash <- tools::sha256sum(ref_net_gpkg_zip)
    
    if(hash != sha_256) stop("hash doesn't match")
    
    zip::unzip(ref_net_gpkg_zip, junkpaths = TRUE, exdir = dirname(ref_net_gpkg))

    file.rename("data/reference_network/reference_network.gpkg", ref_net_gpkg)
  }
  
  ref_net_gpkg
  
}

get_ref_rivers <- function(version = "v2.1", sha256sum = "a9161151f3513206b6d5348c827dca6cbc4df147f8de98847ad1fa1e58a6a099") {
  
  url <- paste0("https://github.com/internetofwater/ref_rivers/releases/download/", version, "/mainstems.gpkg")
  
  out_path <- file.path("data/ref_rivers", version, "mainstems.gpkg")
  
  if(!file.exists(out_path)) {
    
    dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
   
    f <- httr::GET(url, httr::write_disk(out_path))
    
    if(!f$status_code == 200) stop("error downloading")
    
    hash <- tools::sha256sum(out_path)
  
    if(sha256sum != hash) stop("hash doesn't match")   
  }
  
  out_path
  
}