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

get_mainstem_summary_v3 <- function(ms_gpkg) {
  
  mainstem_summary_v3 <- sf::read_sf(ms_gpkg, "mainstem_summary")
  
  lookup <- sf::read_sf("data/review/missing_reference_mainstems.csv")
  
  mainstem_summary_v3$reference_mainstem[match(lookup$outlet_nhdpv2_COMID, mainstem_summary_v3$outlet_nhdpv2_COMID)] <- lookup$uri
  
  mainstem_summary_v3
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

get_hr_lookup <- function() {
  url <- "https://code.usgs.gov/wma/nhgf/reference-fabric/reference-network/-/package_files/18469/download"
  sha256sum <- "b2a5bde0a80d2dc7de72b371341e07862e706b8e343cc53886b2daef591fb357"

  out_path <- file.path("data/reference_network/", "hr_net.csv")
  
  if(!file.exists(out_path)) {
    
    dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
   
    out_zip <- paste0(out_path, ".zip")

    f <- httr::GET(url, httr::write_disk(out_zip))
    
    if(!f$status_code == 200) stop("error downloading")
    
    hash <- tools::sha256sum(out_zip)
  
    if(sha256sum != hash) stop("hash doesn't match")   
    
    zip::unzip(out_zip, exdir = dirname(out_path), junkpaths = TRUE)

    unlink(out_zip)
  }
  
  out_path
}

get_nhd_3dhp_release <- function(name) {

  # Blodgett, D.L., Ritmiller, C.L., Gold, B.L., Hayes, L., and Wieczorek, M.,
  # 2024, Network Attributes for the High Resolution National Hydrography
  # Dataset (NHD) based initial 3D Hydrography Program Network: U.S.
  # Geological Survey data release, https://doi.org/10.5066/P94H0DAG.
  # https://www.sciencebase.gov/catalog/item/650cba99d34e823a0273a790

  sha256sums <- c(
    nhd_3dhp_oconus_mainstems = "9e9ec311d643e5056748e2baf54a9ad6c9d22dd95e42efa31792054300517fcf",
    nhd_3dhp_oconus_flownetwork = "1b01d980cadce7a3aa64ea250f419e50ed4fe4d04ea84f1430ea0582d23dad6c",
    nhd_3dhp_nonnetwork_mainstems = "4c92f244a43f7b5b9b5bc985f49a9df56ebdb7b3f9de820e700b227f2a46e183",
    nhd_final_registry = "88dafb4871e6986e106d3187c98376dd970a03257a7cc5eab949198d8405a02b")

  out_path <- file.path("data/nhd_3dhp", paste0(name, ".csv"))

  if(!file.exists(out_path)) {

    dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

    out_zip <- paste0(tools::file_path_sans_ext(out_path), ".zip")

    if(!file.exists(out_zip)) {
      url <- paste0("https://prod-is-usgs-sb-prod-publish.s3.amazonaws.com/",
                    "650cba99d34e823a0273a790/", basename(out_zip))

      f <- httr::GET(url, httr::write_disk(out_zip))

      if(!f$status_code == 200) stop("error downloading")
    }

    hash <- tools::sha256sum(out_zip)

    if(sha256sums[[name]] != hash) stop("hash doesn't match")

    zip::unzip(out_zip, exdir = dirname(out_path), junkpaths = TRUE)
  }

  out_path
}

get_nhd_hu4 <- function() {

  # U.S. Geological Survey, 2023-2024, USGS National Hydrography Dataset Best
  # Resolution (NHD) for Hydrological Unit (HU) 4, FileGDB and GeoPackage
  # staged products: https://www.usgs.gov/national-hydrography
  # https://prd-tnm.s3.amazonaws.com/index.html?prefix=StagedProducts/Hydrography/NHD/HU4/
  #
  # The final NHD snapshot for the OCONUS HU4s. The 1903 FileGDB as published
  # holds 68,180 of its ~645k flowlines; its GeoPackage edition is complete.

  sha256sums <- c(
    NHD_H_1901_HU4_GDB = "119d1d09b04c730a04d48186ca95ccdf8568bd033906eed2e5f7aceb28ca5efb",
    NHD_H_1902_HU4_GDB = "13053ea1c74e7f9af3270d499c1cf363c06ca87a3152af9aad40b2ed22e04a30",
    NHD_H_1903_HU4_GPKG = "11f91f7ee0059b56e929c360774c4703baeee64a6f75cf9b135f34dd24b700a1",
    NHD_H_1905_HU4_GDB = "731997b6d2203efaba07074f55d71e9b45929583043b41a9986993d18b093eff",
    NHD_H_1906_HU4_GDB = "9019791ff4a7d5a8d719c849423ae1a00a97d814021c0f671cce3c9eb38fc732",
    NHD_H_1907_HU4_GDB = "0f570f8a10cf130bbee5b1faf3647eee2f5544f54a4c314707e7c851945eb38e",
    NHD_H_1908_HU4_GDB = "c45d7432f13165fc2ef92770c0cfe43f0b138498922cf639d5d97cbfbcd8c82b",
    NHD_H_1909_HU4_GDB = "3ed405b4f08dbc854a2b95dc79b727e295dfa1de3626955bcf55c25b3bb34e55",
    NHD_H_2001_HU4_GDB = "08f4cc25dcb1b157a04862a548391340c922c80da0c6954d9b4bb9643e66bfbb",
    NHD_H_2002_HU4_GDB = "6a6074ac7ea3c2338fcb637ce945e1b53bff538aceb2504ea784793066bcdfb7",
    NHD_H_2003_HU4_GDB = "b92458bec7d041ce6089727d6f8df71de1c078555396cee8e7a6db24729ae694",
    NHD_H_2004_HU4_GDB = "6331ca6736106aebcc1b30ffce8ba69ab7f5b195764bfc1e5b8b5920260722a4",
    NHD_H_2005_HU4_GDB = "e5219cf71b91b41d837db0e6c8c0f0363b89a74858025328f57c7c85786fb245",
    NHD_H_2006_HU4_GDB = "c36111eaf711fe66eb2aaf8b9e004a17574de48428e308eb0c3105b5794bf3b3",
    NHD_H_2007_HU4_GDB = "3e54fef429a1140f88f2cfde73121a67b7f49ae79a0deafd2e5e811acb18f36e",
    NHD_H_2008_HU4_GDB = "22e01a8711dcee807b79a2931c6ccaab2a59f208443a460098897ca0e90445d0",
    NHD_H_2009_HU4_GDB = "7f56cf37b741693b17582f06d4e1e9e0f41726629c1363086e78e68ab50e9f33",
    NHD_H_2101_HU4_GDB = "79947606e11eecfe8751b10f43fae81b9d15382499ef7a2f3ac39309286237e8",
    NHD_H_2102_HU4_GDB = "6552fe45a50eabcf55e3aa99f8450409285c605c94994ee81ff9526f0e074837",
    NHD_H_2201_HU4_GDB = "c09e3e060953d37621409c53303afab677d28d69e13079c5f8313fd5a2876c97",
    NHD_H_2202_HU4_GDB = "f35c39b22e2b70c23365820f998eef1810b3b8d6f375c80d330ce7f592aa438a",
    NHD_H_2203_HU4_GDB = "4bd238851efda1be9025bb569da8f4b139ea1cc6a6eb7241d5333def055b66d3",
    NHD_H_2204_HU4_GDB = "37d9386deb135a5ab1895f8527de71d4697d1097e915284b53ce05eeac8a6bf8")

  zip_dir <- "data/nhd_hu4/zip"
  src_dir <- "data/nhd_hu4/src"
  dir.create(zip_dir, recursive = TRUE, showWarnings = FALSE)

  vapply(names(sha256sums), function(name) {

    format <- sub(".*_", "", name)
    out_path <- file.path(src_dir, paste0(name, ".", tolower(format)))
    out_zip <- file.path(zip_dir, paste0(name, ".zip"))

    if(!file.exists(out_path)) {

      if(!file.exists(out_zip)) {
        url <- paste0("https://prd-tnm.s3.amazonaws.com/StagedProducts/Hydrography/NHD/HU4/",
                      format, "/", basename(out_zip))

        f <- httr::GET(url, httr::write_disk(out_zip))

        if(!f$status_code == 200) stop("error downloading")
      }

      hash <- tools::sha256sum(out_zip)

      if(sha256sums[[name]] != hash) stop("hash doesn't match")

      zip::unzip(out_zip, exdir = src_dir)
    }

    out_path
  }, "", USE.NAMES = FALSE)
}
