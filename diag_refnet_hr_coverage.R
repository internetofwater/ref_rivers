# diag_refnet_hr_coverage.R — Where does the HR crosswalk coverage gap enter?
#
# hr_net.csv has the failing-HU04 features. The crosswalk HR side is built
# from reference_network_1.gpkg (source == 'nhdphr') seeded by the mainstems
# product head/outlet_nhdplushr_id. Check both for the failing domains.
#
# Run: Rscript diag_refnet_hr_coverage.R

suppressPackageStartupMessages({ library(sf); library(dplyr) })

refnet <- "data/reference_network/reference_network_1.gpkg"
ms_gpkg <- "data/mainstems/hr_mainstem_summary_v3.gpkg"

cat("=== reference_network layers ===\n")
print(sf::st_layers(refnet))

lyr <- sf::st_layers(refnet)$name[1]
cat("\nReading id/source from layer:", lyr, "\n")

# attribute-only pull
q <- sprintf("SELECT id, source FROM \"%s\"", lyr)
tbl <- sf::st_read(refnet, query = q, quiet = TRUE)
tbl <- sf::st_drop_geometry(tbl)
cat("total rows:", nrow(tbl), "\n")
cat("source counts:\n"); print(table(tbl$source))

hr <- tbl$id[tbl$source == "nhdphr"]
hr_id <- gsub("nhdphr-", "", hr)

# domain prefixes present in refnet HR
cat("\n=== refnet nhdphr id domain prefixes (leading 7) ===\n")
print(sort(table(substr(hr_id, 1, 7)), decreasing = TRUE))

probe <- c("0101"="5000100053825", "0106"="5000600572708",
           "0110"="10000800000001", "0305"="15001500000100")
cat("\n=== probe ids present in refnet nhdphr ===\n")
for (h in names(probe))
  cat(sprintf("  %s %-16s -> %s\n", h, probe[[h]],
              if (probe[[h]] %in% hr_id) "FOUND" else "ABSENT"))

# mainstems product HR head/outlet coverage
cat("\n=== mainstems head/outlet nhdplushr_id domains ===\n")
ms <- sf::st_drop_geometry(sf::read_sf(ms_gpkg, "mainstem_summary"))
cat("mainstem cols:", paste(names(ms), collapse=", "), "\n")
hd <- as.character(ms$head_nhdplushr_id)
ot <- as.character(ms$outlet_nhdplushr_id)
cat("n mainstems:", nrow(ms),
    " with head_hr:", sum(!is.na(hd) & hd != ""),
    " with outlet_hr:", sum(!is.na(ot) & ot != ""), "\n")
cat("head_nhdplushr_id domain prefixes (leading 7):\n")
print(sort(table(substr(hd[!is.na(hd)], 1, 7)), decreasing = TRUE))

cat("\n=== DONE ===\n")
