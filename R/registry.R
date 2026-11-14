# ms <- tar_read("mainstems")
# registry <- tar_read("registry_file")
# providers <- tar_read("provider_file")
build_registry <- function(ms, registry, providers) {
  
  reg <- read.table(
    file = registry, header = TRUE, sep = ",", colClasses = c("integer", "character", "character", "integer")
  ) |> dplyr::as_tibble()

  reg$head <- trimws(reg$head)
  reg$out <- trimws(reg$out)

  pro <- read.table(
    file = providers, header = TRUE, sep = ",", colClasses = c("integer", "character", "character")
  ) |> dplyr::as_tibble()

  ms_id <- gsub("https://geoconnex.us/ref/mainstems/", "", ms$uri)
  
  # NHDPlusV2 where present, then NHDPlusHR, then the NHD permanent identifier
  # (OCONUS rows, provider 5).
  head_id <- ifelse(ms$head_nhdpv2_COMID != "",
    gsub("https://geoconnex.us/nhdplusv2/comid/", "", ms$head_nhdpv2_COMID),
    ifelse(ms$head_nhdplushr_id != "", ms$head_nhdplushr_id, ms$head_nhd_permid)
  )

  out_id <- ifelse(ms$outlet_nhdpv2_COMID != "", 
    gsub("https://geoconnex.us/nhdplusv2/comid/", "", ms$outlet_nhdpv2_COMID), 
    ifelse(ms$outlet_nhdplushr_id != "", ms$outlet_nhdplushr_id, ms$outlet_nhd_permid)
  )

  provider <- ifelse(ms$head_nhdpv2_COMID != "", 3,
                     ifelse(ms$head_nhdplushr_id != "", 4, 5))

  # Must all have values
  stopifnot(all(head_id != "" & !is.na(head_id)))
  stopifnot(all(out_id != "") & !is.na(out_id))

  if(nrow(reg) < nrow(ms)) {
    
    stopifnot(pro$provider[pro$id == 3] == "https://doi.org/10.5066/P13IRYTB")
    stopifnot(pro$provider[pro$id == 4] == "https://doi.org/10.5066/P13V7GVY")
    if (any(provider == 5))
      stopifnot(pro$provider[pro$id == 5] == "https://doi.org/10.5066/P94H0DAG")

    ms$id <- as.integer(ms$id)
    
    extra <- !ms$id %in% reg$mainstem
    
    ms_id <- as.integer(ms_id[extra])
    head_id <- head_id[extra]
    out_id <- out_id[extra]
    provider <- provider[extra]
    
    message(paste("adding", sum(provider == 3), "nhdplusv2 rows to registry."))
    message(paste("adding", sum(provider == 4), "nhdplushr rows to registry."))
    message(paste("adding", sum(provider == 5), "nhd permanent identifier rows to registry."))

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

  write.table(x = registry, file = registry_file, append = FALSE, sep = ",", quote = FALSE, row.names = FALSE)
  
  registry_file
}