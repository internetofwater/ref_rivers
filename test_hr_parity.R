suppressPackageStartupMessages({ library(dplyr); library(readr); library(sf) })
source("R/write_output.R")

t0 <- Sys.time()
msg <- function(...) cat(sprintf("[%5.0fs] ", as.numeric(Sys.time() - t0, units = "secs")), ..., "\n")

# ordering attributes. the pipeline gets these from raw_mainstems$level directly;
# for this standalone test recover them via uri -> lp_mainstem_v3 -> level.
lpv3 <- read_csv("out/lpv3_lookup.csv", col_types = cols(.default = "c"))
lvl <- st_read("data/mainstems/hr_mainstem_summary_v3.gpkg", "mainstem_summary",
               quiet = TRUE) |> st_drop_geometry() |>
  transmute(lp_mainstem_v3 = as.character(lp_mainstem_v3), level) |>
  distinct()
prod <- left_join(lpv3, lvl, by = "lp_mainstem_v3") |>
  transmute(uri, level, lp_mainstem_v3 = as.numeric(lp_mainstem_v3))
msg("loaded ordering:", nrow(prod), "rows,  NA level:", sum(is.na(prod$level)))

ms <- st_read("out/mainstems.gpkg", "mainstems", quiet = TRUE) |> st_drop_geometry() |>
  filter(!superseded) |>
  select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
  distinct() |>
  left_join(prod, by = "uri") |>
  filter(!is.na(head_nhdplushr_id), head_nhdplushr_id != "",
         !is.na(outlet_nhdplushr_id), outlet_nhdplushr_id != "")
msg("mainstems with HR ids:", nrow(ms),
    " NA level:", sum(is.na(ms$level)), " NA lp:", sum(is.na(ms$lp_mainstem_v3)))
stopifnot(!any(is.na(ms$lp_mainstem_v3)), mean(is.na(ms$level)) < 0.01)

msg("building HR network from full hr_net ...")
net <- build_hr_network("data/reference_network/hr_net.csv")
msg("network built: nodes =", length(net$id))

msg("assigning mainstems ...")
new <- assign_hr_mainstems(ms, net)
msg("assignment done: rows =", nrow(new),
    " distinct uri =", n_distinct(new$uri))

stopifnot(!any(duplicated(new$nhdplushr_id)))
missing <- setdiff(ms$uri, new$uri)
msg("mainstems with zero flowlines assigned:", length(missing))
if (length(missing)) {
  md <- filter(ms, uri %in% missing)
  msg("  head == outlet (single-node):", sum(md$head_nhdplushr_id == md$outlet_nhdplushr_id))
  msg("  NA level among missing:", sum(is.na(md$level)))

  # who claimed the outlet node of each missing mainstem?
  own <- setNames(new$uri, new$nhdplushr_id)
  md$claimer <- own[md$outlet_nhdplushr_id]
  msg("  outlet node claimed by another mainstem:", sum(!is.na(md$claimer)),
      " ; unclaimed (truly absent):", sum(is.na(md$claimer)))

  # do the missing mainstems have NHDPlusV2 coverage (so v2 lookup covers them)?
  v2 <- st_read("out/mainstems.gpkg","mainstems",quiet=TRUE) |> st_drop_geometry() |>
    select(uri, head_nhdpv2_COMID) |> filter(uri %in% missing)
  msg("  missing that have a v2 COMID:", sum(v2$head_nhdpv2_COMID != "" & !is.na(v2$head_nhdpv2_COMID)),
      " of ", length(missing))
}

# parity vs current output on the domains it already covers
old <- read_csv("out/nhdphr_lookup.csv", col_types = cols(.default = "c"))
old_domains <- unique(substr(old$nhdplushr_id, 1, 7))
new_sub <- filter(new, substr(nhdplushr_id, 1, 7) %in% old_domains)

cmp <- full_join(
  transmute(old, nhdplushr_id, uri_old = uri),
  transmute(new_sub, nhdplushr_id, uri_new = uri),
  by = "nhdplushr_id"
)
msg("parity on", length(old_domains), "existing domains:")
msg("  ids in old only :", sum(is.na(cmp$uri_new)))
msg("  ids in new only :", sum(is.na(cmp$uri_old)))
both <- filter(cmp, !is.na(uri_old), !is.na(uri_new))
msg("  ids in both     :", nrow(both),
    " matching uri:", sum(both$uri_old == both$uri_new),
    sprintf(" (%.4f)", mean(both$uri_old == both$uri_new)))

msg("total new domains:", n_distinct(substr(new$nhdplushr_id, 1, 7)),
    " vs old:", length(old_domains))
cat("DONE\n")
