# Used to add new_mainstemid attributes to mainstems after workflow
# has completed.

mainstems <- sf::read_sf("out/mainstems.gpkg", "mainstems")

deprecated <- sf::read_sf("data/review/deprecated_v3.geojson")

new <- readr::read_csv("data/review/new_ms_updates.csv")

update <- dplyr::group_by(new, uri) |>
  dplyr::summarise(new_mainstemid = paste0("['", paste(new_mainstem, collapse = "', '"), "']"))

mainstems <- as.data.frame(mainstems) |>
  dplyr::rows_update(update, by = "uri") |>
  sf::st_sf()

update <- dplyr::group_by(new, uri) |>
  dplyr::summarise(new_mainstemid = paste(new_mainstem, collapse = "', '")) |>
  dplyr::rename(reference_mainstem = uri) |>
  dplyr::filter(reference_mainstem %in% deprecated$reference_mainstem)

deprecated <- as.data.frame(deprecated) |>
  dplyr::rows_update(update, by = "reference_mainstem") |>
  sf::st_sf()

unlink("data/review/deprecated_v3_new.geojson")
sf::write_sf(deprecated, "data/review/deprecated_v3_new.geojson")

