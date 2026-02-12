mainstems <- sf::read_sf("out/mainstems.gpkg", "mainstems")

deprecated <- sf::read_sf("data/review/deprecated_v3.geojson")

new <- readr::read_csv("data/review/deprecated_lookup.csv")

new$head_nhdplushr_id <- as.character(new$head_nhdplushr_id)

all_hr_ids <- readr::read_csv("../../data/final_hr/permid_to_nhdplusid.csv")

to_swtich <- all_hr_ids[all_hr_ids$nhdphr_permid %in% mainstems$head_nhdplushr_id,]

# some head_nhsplushr_ids are actually permids
mainstems$head_nhdplushr_id[match(to_swtich$nhdphr_permid, mainstems$head_nhdplushr_id)] <- to_swtich$id

to_swtich <- all_hr_ids[all_hr_ids$nhdphr_permid %in% mainstems$outlet_nhdplushr_id,]

mainstems$outlet_nhdplushr_id[match(to_swtich$nhdphr_permid, mainstems$outlet_nhdplushr_id)] <- to_swtich$id

# get URI of new mainstem for each of the superseded one
new <- dplyr::left_join(new, 
  dplyr::select(sf::st_drop_geometry(mainstems), uri, head_nhdplushr_id), 
  by = c("head_nhdplushr_id"))

# check that everything is ok with deprecated mainstems
stopifnot(!any(duplicated(deprecated$reference_mainstem)))
stopifnot(!any(is.na(new$uri)))

match_index <- match(deprecated$reference_mainstem, mainstems$uri)

replace <- mainstems[match_index,]

stopifnot(all(replace$uri == deprecated$reference_mainstem))
stopifnot(all(replace$new_mainstemid == ""))

# fix penobscot encompassing
mainstems$encompassing_mainstem_basins[mainstems$uri == "https://geoconnex.us/ref/mainstems/2386091"] <-
  "['https://geoconnex.us/ref/mainstems/2386091']"

# Record the new id for the superseded penobscot fix
new_id <- data.frame(
  reference_mainstem = c("https://geoconnex.us/ref/mainstems/1921673"),
  new_mainstemid = c("https://geoconnex.us/ref/mainstems/2386091")
)

new_id <- dplyr::bind_rows(new_id, 
  dplyr::select(new, reference_mainstem, new_mainstemid = uri))

stopifnot(all(new_id$reference_mainstem %in% deprecated$reference_mainstem))

new_id <- dplyr::group_by(new_id, reference_mainstem) |>
  dplyr::summarise(new_mainstemid = list(new_mainstemid))

new_id$new_mainstemid <- as.character(sapply(new_id$new_mainstemid, \(x) paste(x, collapse = ", ")))

deprecated <- dplyr::left_join(deprecated, new_id, by = "reference_mainstem")

unlink("data/review/deprecated_v3_new.geojson")
sf::write_sf(deprecated, "data/review/deprecated_v3_new.geojson")

mainstems$new_mainstemid[match(new_id$reference_mainstem, mainstems$uri)] <- new_id$new_mainstemid

sf::write_sf(mainstems, "out/mainstems.gpkg")
