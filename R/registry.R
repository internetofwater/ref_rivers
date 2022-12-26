build_registry <- function(ms, registry, providers) {
  
  if(!file.exists(registry)) stop("no registry?")
  
  reg <- readr::read_csv(registry)
  
  ms_id <- gsub("https://geoconnex.us/ref/mainstems/", "", 
                ms$uri)
  
  head_id <- gsub("https://geoconnex.us/nhdplusv2/comid/", "", 
                  ms$head_nhdpv2_COMID)
  
  out_id <- gsub("https://geoconnex.us/nhdplusv2/comid/", "", 
                 ms$outlet_nhdpv2_COMID)
  
  if(nrow(reg) == 1) {
    message("initialize")
    
    dplyr::tibble(mainstem = ms_id,
                  head = head_id,
                  out = out_id, 
                  provider = 1)
    
  } else if(nrow(reg) < nrow(ms)) {
    stop("adding mainstems is not implemented")
  } else if(nrow(reg) == nrow(ms)) {
    reg
  } else {
      stop("mainstems should not be shorter than registry.")
  }
  
}

write_registry <- function(registry, registry_file) {
  readr::write_csv(registry, registry_file)
  
  registry_file
}