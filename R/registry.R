build_registry <- function(ms, registry, providers) {
  
  if(!file.exists(registry)) stop("no registry?")
  
  reg <- readr::read_csv(registry)
  pro <- readr::read_csv(providers)
  
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
    
    if(!all(c("https://doi.org/10.5066/P9W79I7Q", 
              "https://doi.org/10.5066/P976XCVT") %in% 
            pro$provider)) {
      # just being super cautious
      stop("this will only work with the initial two providers.")
    }
    
    ms$id <- as.integer(ms$id)
    
    message(paste("switching", nrow(ms) - sum(ms$superseded), "to provider 2."))
    
    reg <- left_join(reg,
                     select(sf::st_drop_geometry(ms),
                            mainstem = id, superseded),
                     by = "mainstem") |>
      # superseded stay provider 1 others become provider 2
      # generally providers will not be changed in the future
      # but provider 1 was found to be flawed so is being replaced
      # for exact matches.
      mutate(provider = ifelse(superseded, 1, 2)) |>
      select(-superseded)
    
    extra <- !ms$id %in% reg$mainstem
    
    ms_id <- as.integer(ms_id[extra])
    head_id <- as.integer(head_id[extra])
    out_id <- as.integer(out_id[extra])
    
    message(paste("adding", sum(extra), "rows to registry."))
    
    bind_rows(reg, 
              dplyr::tibble(mainstem = ms_id,
                            head = head_id,
                            out = out_id, 
                            provider = 2))
    
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