# library(dplyr)
# library(sf)
# library(targets)
# tar_load(mainstems)
# tar_load(raw_mainstems)
# tar_load(enhd_v3)
# tar_load(hr_net)

#' write lookup tables for source networks
#'
#' @description writes lookups between mainstem id and various identifier systems.
#'
#' The NHDPlusV2 lookup is built by matching each mainstem to its eNHD levelpath
#' (see [get_v2_lookup()]).
#'
#' The NHDPlusHR lookup is built by navigating the full NHDPlusHR network
#' (`hr_net`) from each mainstem's head to its outlet. Two tiers are used per
#' mainstem: a cheap walk along the primary-downstream ("downmain") spine, and,
#' only when the outlet is not reached that way, a full-network trace. Mainstems
#' are assigned in streamlevel order (larger rivers first, ties broken by
#' levelpath) so that where paths overlap the larger river claims the flowline.
#' See [build_hr_network()] and [assign_hr_mainstems()].
#'
#' This function is mostly about its side effect (writing lookup tables).
#'
#' Outputs:
#' out/nhdphr_lookup.csv
#' out/nhdpv2_lookup.csv
#'
#' @param mainstems sf data.frame from workflow (cleaned mainstems)
#' @param raw_mainstems sf data.frame from workflow carrying `level` (streamlevel)
#'   and `lp_mainstem_v3` used to order HR assignment
#' @param enhd_v3 path to parquet containing enhd v3 network
#' @param hr_net path to csv with the NHDPlusHR flow network
#'
write_lookups <- function(mainstems, raw_mainstems, enhd_v3, hr_net) {
  enhd <- arrow::read_parquet(enhd_v3)

  # streamlevel + levelpath for precedence ordering of the HR crosswalk.
  # these are dropped from the cleaned mainstems schema, so pull from raw.
  ord <- sf::st_drop_geometry(raw_mainstems) |>
    dplyr::select(uri, level, lp_mainstem_v3) |>
    dplyr::distinct()

  # only interested in current mainstems
  mainstems <- dplyr::filter(mainstems, !superseded)

  v2_out <- get_v2_lookup(mainstems, enhd)

  #### HR
  hr_in <- sf::st_drop_geometry(mainstems) |>
    dplyr::select(uri, head_nhdplushr_id, outlet_nhdplushr_id) |>
    dplyr::distinct() |>
    dplyr::left_join(ord, by = "uri")

  # levelpath is always present. a small number of mainstems may lack
  # streamlevel; these sort last (arrange puts NA level last) with levelpath
  # breaking ties, per the levelpath fallback.
  stopifnot(!any(is.na(hr_in$lp_mainstem_v3)))
  stopifnot(mean(is.na(hr_in$level)) < 0.01)

  # mainstems that actually reference the HR network
  ms_with_hr <- dplyr::filter(
    hr_in,
    !is.na(head_nhdplushr_id), head_nhdplushr_id != "",
    !is.na(outlet_nhdplushr_id), outlet_nhdplushr_id != ""
  )

  net <- build_hr_network(hr_net)

  hr_out <- assign_hr_mainstems(ms_with_hr, net)

  # Some mainstems have an HR footprint fully contained within a higher-
  # precedence mainstem's path -- many are degenerate single-flowline mainstems
  # (head == outlet). A flowline belongs to one mainstem, so these get no
  # exclusive HR flowline and are left out of the HR crosswalk; they resolve via
  # the v2 lookup where present.
  hr_subsumed <- setdiff(ms_with_hr$uri, hr_out$uri)
  message("write_lookups: ", length(hr_subsumed),
          " HR mainstems subsumed by higher-precedence mainstems (left out of HR)")
  stopifnot(length(hr_subsumed) < 500L)

  # crosswalk invariants
  stopifnot(!any(duplicated(hr_out$nhdplushr_id)))
  stopifnot(!any(is.na(hr_out$nhdplushr_id)))
  stopifnot(!any(is.na(hr_out$uri)))

  # every mainstem resolves via the hr or v2 lookup, or is subsumed in HR
  stopifnot(all(mainstems$uri %in% c(hr_out$uri, v2_out$uri) |
                mainstems$uri %in% hr_subsumed))

  readr::write_csv(v2_out, "out/nhdpv2_lookup.csv")
  readr::write_csv(hr_out, "out/nhdphr_lookup.csv")

  "out/nhdpv2_lookup.csv"
}

#' Build NHDPlusV2 COMID-to-mainstem lookup
#'
#' Joins mainstems to the eNHD network via levelpath to map COMIDs to mainstem URIs.
#'
#' @param mainstems sf data.frame of active mainstems
#' @param enhd data.frame eNHD v3 network with comid and levelpathi columns
#' @return data.frame with `uri` and `comid` columns
#' @keywords internal
get_v2_lookup <- function(mainstems, enhd) {
  # Seed our output with all the mainstems that are relevant
  v2_out <- sf::st_drop_geometry(mainstems) |>
    select(uri, head_nhdpv2_COMID, outlet_nhdpv2_COMID) |>
    mutate(head_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               head_nhdpv2_COMID)),
           outlet_nhdpv2_COMID = as.numeric(gsub("https://geoconnex.us/nhdplusv2/comid/",
                                               "",
                                               outlet_nhdpv2_COMID))) |>
    left_join(distinct(select(enhd, comid, levelpathi)), # first join head comid to get levelpath
              by = c("head_nhdpv2_COMID" = "comid")) |>
    left_join(distinct(select(enhd, comid, levelpathi)), # now join by levelpath to get all comids
              by = "levelpathi")

  # group and check that we have the outlet in here
  v2_out <- group_by(v2_out, levelpathi) |>
    mutate(outlet_check = any(comid == outlet_nhdpv2_COMID))

  if(sum(is.na(v2_out$outlet_check) > 150)) stop("only a small number of mainstems should not have nhdplusv2 outlets")

  # outlet_check can be NA or TRUE
  if(!all(is.na(v2_out$outlet_check) | v2_out$outlet_check)) stop("all levelpaths should have the outlet in them")

  v2_out <- select(ungroup(v2_out), uri, comid) |>
    distinct() |>
    filter(!is.na(comid))

  stopifnot(!any(duplicated(v2_out$comid)))

  v2_out
}

#' Build the NHDPlusHR navigation network
#'
#' Reads the NHDPlusHR flow network and returns the pieces needed to trace
#' mainstems: a common id universe, the per-id primary-downstream ("downmain")
#' successor and primary-upstream ("upmain") predecessor (both dendritic spines
#' used for the fast traces), the full set of downstream edges (used for the
#' fallback trace), and the permanent id lookup.
#'
#' @param hr_net_path path to the NHDPlusHR network csv
#'   (columns id, permid, toid, upmain, downmain)
#' @return list with `id` (character universe), `permid` (aligned to `id`),
#'   `succ` (integer downmain-successor index, NA at terminals),
#'   `upsucc` (integer upmain-predecessor index, NA at headwaters), and
#'   `full_from`/`full_to` (integer index pairs for all downstream edges)
#' @keywords internal
build_hr_network <- function(hr_net_path) {
  hr_net <- readr::read_csv(hr_net_path, col_types = "cccll")

  # universe of all HR flowline ids
  id <- unique(hr_net$id)

  # permid per id (one row per id)
  pm <- hr_net[!duplicated(hr_net$id), c("id", "permid")]
  permid <- pm$permid[match(id, pm$id)]

  # downmain spine: the single primary-downstream successor per id
  dm <- hr_net[!is.na(hr_net$downmain) & hr_net$downmain, c("id", "toid")]
  stopifnot(!any(duplicated(dm$id)))         # downmain out-degree must be <= 1
  succ <- rep(NA_integer_, length(id))
  succ[match(dm$id, id)] <- match(dm$toid, id)

  # upmain spine: the single primary-upstream predecessor per id
  um <- hr_net[!is.na(hr_net$upmain) & hr_net$upmain & !is.na(hr_net$toid),
               c("id", "toid")]
  stopifnot(!any(duplicated(um$toid)))       # upmain in-degree must be <= 1
  upsucc <- rep(NA_integer_, length(id))
  upsucc[match(um$toid, id)] <- match(um$id, id)

  # full downstream edge list (integer index pairs) for the fallback trace
  fe <- hr_net[!is.na(hr_net$toid), c("id", "toid")]
  full_from <- match(fe$id, id)
  full_to <- match(fe$toid, id)
  keep <- !is.na(full_from) & !is.na(full_to)

  list(id = id,
       permid = permid,
       succ = succ,
       upsucc = upsucc,
       full_from = full_from[keep],
       full_to = full_to[keep])
}

#' Lockstep spine trace: does each mainstem reach its target?
#'
#' Advances all requested mainstems one spine step per round in parallel and
#' reports which reach their target before running off the spine.
#'
#' @param start,target integer per-mainstem start and target node indices
#' @param step integer per-node successor index (a spine: `succ` or `upsucc`)
#' @param which_ms integer indices of the mainstems to trace
#' @return logical vector (length of `start`), TRUE where the target was reached
#' @keywords internal
lockstep_reach <- function(start, target, step, which_ms, max_rounds = 200000L) {
  cur <- start
  act <- which_ms
  reached <- logical(length(start))
  r <- 0L
  while (length(act) && r < max_rounds) {
    r <- r + 1L
    hit <- cur[act] == target[act]
    reached[act[hit]] <- TRUE
    adv <- act[!hit]
    nx <- step[cur[adv]]
    cur[adv] <- nx
    act <- adv[!is.na(nx)]
  }
  reached
}

#' Lockstep spine trace: collect visited (node, rank) pairs
#'
#' Like [lockstep_reach()] but records every visited node with the tracing
#' mainstem's precedence rank. Only pass mainstems known to reach their target
#' (so no off-spine overshoot is recorded).
#'
#' @param start,target integer per-mainstem start and target node indices
#' @param step integer per-node successor index
#' @param ranks integer per-mainstem precedence rank
#' @param which_ms integer indices of the mainstems to trace
#' @return list with integer `node` and `rank` vectors
#' @keywords internal
lockstep_collect <- function(start, target, step, ranks, which_ms,
                             max_rounds = 200000L) {
  cur <- start
  act <- which_ms
  ev_node <- vector("list", 0L)
  ev_rank <- vector("list", 0L)
  r <- 0L
  while (length(act) && r < max_rounds) {
    r <- r + 1L
    nod <- cur[act]
    ev_node[[r]] <- nod
    ev_rank[[r]] <- ranks[act]
    hit <- nod == target[act]
    adv <- act[!hit]
    nx <- step[cur[adv]]
    cur[adv] <- nx
    act <- adv[!is.na(nx)]
  }
  list(node = unlist(ev_node, use.names = FALSE),
       rank = unlist(ev_rank, use.names = FALSE))
}

#' Assign mainstem URIs to NHDPlusHR flowlines by head-to-outlet navigation
#'
#' Traces each mainstem from head to outlet and assigns its URI to every
#' flowline on that path. Mainstems are processed in streamlevel order (smaller
#' `level` first, ties broken by smaller `lp_mainstem_v3`); a flowline is kept
#' by the highest-precedence (smallest rank) mainstem covering it, so where a
#' tributary's path overlaps a larger receiving river the larger river keeps the
#' shared flowlines.
#'
#' Three trace tiers, each vectorized where it matters:
#' \enumerate{
#'   \item downmain-from-head lockstep (resolves ~99% of mainstems);
#'   \item upmain-from-outlet lockstep, run only for tier-1 misses;
#'   \item a bounded downstream breadth-first search over the full network for
#'     the few mainstems whose head and outlet are connected only off both
#'     spines (e.g. across divergences).
#' }
#'
#' @param ms_with_hr data.frame with `uri`, `head_nhdplushr_id`,
#'   `outlet_nhdplushr_id`, `level`, `lp_mainstem_v3`
#' @param net list from [build_hr_network()]
#' @return data.frame with `uri`, `nhdplushr_permid`, `nhdplushr_id`
#' @keywords internal
assign_hr_mainstems <- function(ms_with_hr, net) {
  N <- length(net$id)

  # precedence order: larger rivers (smaller streamlevel) first, then levelpath.
  # after this sort a mainstem's row index *is* its precedence rank (1 = best).
  ms <- dplyr::arrange(ms_with_hr, level, lp_mainstem_v3)
  M <- nrow(ms)
  ranks <- seq_len(M)

  head_i <- match(ms$head_nhdplushr_id, net$id)
  out_i  <- match(ms$outlet_nhdplushr_id, net$id)
  stopifnot(!any(is.na(head_i)))   # coverage of head/outlet ids in hr_net is
  stopifnot(!any(is.na(out_i)))    # expected to be complete

  # --- Tier 1: downmain-from-head ---
  reached_dm <- lockstep_reach(head_i, out_i, net$succ, ranks)
  ev1 <- lockstep_collect(head_i, out_i, net$succ, ranks, which(reached_dm))
  miss1 <- which(!reached_dm)
  message("HR assign: tier-1 (downmain) reached ", sum(reached_dm),
          " / ", M, " ; ", length(miss1), " misses")

  # --- Tier 2: upmain-from-outlet, only for tier-1 misses ---
  ev2 <- list(node = integer(0), rank = integer(0))
  reached_um <- logical(M)
  if (length(miss1)) {
    reached_um <- lockstep_reach(out_i, head_i, net$upsucc, miss1)
    um_ok <- which(reached_um)
    ev2 <- lockstep_collect(out_i, head_i, net$upsucc, ranks, um_ok)
    message("HR assign: tier-2 (upmain) recovered ", length(um_ok),
            " of ", length(miss1), " misses")
  }
  miss2 <- setdiff(miss1, which(reached_um))

  # reduce tier-1 + tier-2 visits to one owner per node, keeping smallest rank
  en <- c(ev1$node, ev2$node)
  ek <- c(ev1$rank, ev2$rank)
  o <- order(en, ek)
  en <- en[o]; ek <- ek[o]
  first <- !duplicated(en)
  owner <- rep(NA_integer_, N)     # stores precedence rank; NA = unclaimed
  owner[en[first]] <- ek[first]

  # --- Tier 3: bounded full-network BFS for the remainder ---
  if (length(miss2)) {
    stopifnot(length(miss2) < 20000L)   # a large remainder means something changed
    message("HR assign: tier-3 (full BFS) for ", length(miss2), " mainstems")

    # CSR successors over the full network (built once, only when needed)
    outdeg <- tabulate(net$full_from, nbins = N)
    ord <- order(net$full_from)
    succ_arr <- net$full_to[ord]
    s_end <- cumsum(outdeg)
    s_start <- s_end - outdeg + 1L

    # reused, generation-stamped scratch (modified in place; kept local so R
    # does not copy the length-N vectors on each update)
    mark <- integer(N)
    par  <- integer(N)
    gen  <- 0L
    BFS_CAP <- 200000L
    unresolved <- integer(0)

    for (k in miss2) {
      gen <- gen + 1L
      s <- head_i[k]; t <- out_i[k]
      mark[s] <- gen; par[s] <- 0L
      frontier <- s
      visited <- 0L
      found <- (s == t)
      while (!found && length(frontier)) {
        f <- frontier[outdeg[frontier] > 0L]
        if (!length(f)) break
        d <- outdeg[f]
        idx <- rep.int(s_start[f], d) + sequence(d) - 1L
        nb <- succ_arr[idx]
        src <- rep.int(f, d)
        newn <- mark[nb] != gen
        nb <- nb[newn]; src <- src[newn]
        if (!length(nb)) break
        dup <- !duplicated(nb)
        nb <- nb[dup]; src <- src[dup]
        mark[nb] <- gen
        par[nb] <- src
        visited <- visited + length(nb)
        if (mark[t] == gen) { found <- TRUE; break }
        if (visited > BFS_CAP) break
        frontier <- nb
      }
      if (!found) { unresolved <- c(unresolved, k); next }

      # reconstruct outlet -> head and claim with precedence
      path <- integer(0); v <- t
      repeat {
        path <- c(path, v)
        if (v == s) break
        v <- par[v]
        if (v == 0L) break
      }
      cur_owner <- owner[path]
      take <- path[is.na(cur_owner) | cur_owner > k]
      owner[take] <- k
    }

    if (length(unresolved))
      stop(length(unresolved), " mainstems have no head-to-outlet path in hr_net")
  }

  keep2 <- which(!is.na(owner))
  data.frame(
    uri = ms$uri[owner[keep2]],
    nhdplushr_permid = net$permid[keep2],
    nhdplushr_id = net$id[keep2],
    stringsAsFactors = FALSE
  )
}
