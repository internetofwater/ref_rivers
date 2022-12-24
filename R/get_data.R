get_mainstems_db <- function(v) {
  valid_v <- "v0.1"

  if(!v %in% valid_v) 
    stop("must use v in", paste(valid_v, collapse = ", "))
    
  ms_gpkg <- "data/mainstems/mainstems_summary_v0.1.gpkg"
  
  if(!file.exists(ms_gpkg)) {
    dir.create(dirname(ms_gpkg), recursive = TRUE, showWarnings = FALSE)
    f <- sbtools::item_file_download("60cb5edfd34e86b938a373f4", 
                                     names = "mainstems_summary.gpkg", 
                                     destinations = ms_gpkg)
  }
  
  ms_gpkg
}