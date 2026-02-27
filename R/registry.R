# ms <- tar_read("mainstems")
# registry <- tar_read("registry_file")
# providers <- tar_read("provider_file")
build_registry <- function(ms, registry, providers) {
  
  reg <- readr::read_csv(registry)
  pro <- readr::read_csv(providers)
  
  reg$head <- format(reg$head, scientific = FALSE)
  reg$out <- format(reg$head, scientific = FALSE)

  ms_id <- gsub("https://geoconnex.us/ref/mainstems/", "", ms$uri)
  
  head_id <- ifelse(ms$head_nhdpv2_COMID != "",
    gsub("https://geoconnex.us/nhdplusv2/comid/", "", ms$head_nhdpv2_COMID),
    ms$head_nhdplushr_id
  )

  out_id <- ifelse(ms$outlet_nhdpv2_COMID != "", 
    gsub("https://geoconnex.us/nhdplusv2/comid/", "", ms$outlet_nhdpv2_COMID), 
    ms$outlet_nhdplushr_id
  )

  provider <- ifelse(ms$head_nhdpv2_COMID != "", 3, 4)

  # Must all have values
  stopifnot(all(head_id != "" & !is.na(head_id)))
  stopifnot(all(out_id != "") & !is.na(out_id))

  if(nrow(reg) < nrow(ms)) {
    
    stopifnot(pro$provider[pro$id == 3] == "https://doi.org/10.5066/P13IRYTB")
    stopifnot(pro$provider[pro$id == 4] == "https://doi.org/10.5066/P13V7GVY")

    ms$id <- as.integer(ms$id)
    
    extra <- !ms$id %in% reg$mainstem
    
    ms_id <- as.integer(ms_id[extra])
    head_id <- head_id[extra]
    out_id <- out_id[extra]
    provider <- provider[extra]
    
    message(paste("adding", sum(provider == 3), "nhdplusv2 rows to registry."))
    message(paste("adding", sum(provider == 4), "nhdplushr rows to registry."))

    stopifnot(!any(duplicated(ms_id)))
    stopifnot(!any(duplicated(head_id)))
    stopifnot(!any(duplicated(out_id)))

    bind_rows(reg, 
              dplyr::tibble(mainstem = ms_id,
                            head = head_id,
                            out = out_id, 
                            provider = provider))
    
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