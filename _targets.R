#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline # nolint

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed. # nolint

# Set target options:
tar_option_set(
  packages = c("dplyr", "sf", "sbtools", "nhdplusTools"), # packages that your targets need to run
  format = "rds" # default storage format
  # Set other options as needed.
)


if(package_version(packageVersion("nhdplusTools")) < "1.0.1") 
  stop("nhdplusTools version must be greater than 1.0.1")

# tar_make_clustermq() configuration (okay to leave alone):
options(clustermq.scheduler = "multiprocess")

# tar_make_future() configuration (okay to leave alone):
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Run the R scripts in the R/ folder with your custom functions:
tar_source()

# source("other_functions.R") # Source other scripts as needed. # nolint

# Replace the target list below with your own:
list(
  tar_target(name = registry_file, command = "registry/ref_rivers.csv", format = "file"),
  tar_target(name = provider_file, command = "registry/providers.csv", format = "file"),
  tar_target(name = mainstems_v1, command = get_mainstems_db_1()),
  tar_target(name = mainstems_v2, command = get_mainstems_db_2()),
  tar_target(name = enhd_v1, command = get_enhd_1()),
  tar_target(name = enhd_v2, command = get_enhd_2()),
  tar_target(name = nhdp_gdb, command = nhdplusTools::download_nhdplusv2("data/nhdp")),
  tar_target(name = nhdp_geo, command = sf::read_sf(nhdp_gdb, "NHDFlowline_Network")),
  tar_target(name = reconciled_mainstems, command = reconcile_mainstems(mainstems_v1, 
                                                                       mainstems_v2,
                                                                       enhd_v1,
                                                                       enhd_v2)),
  tar_target(name = mainstems, command = make_mainstems(mainstems_v1, 
                                                        mainstems_v2,
                                                        enhd_v1,
                                                        enhd_v2,
                                                        reconciled_mainstems,
                                                        "out/mainstems.gpkg")),
  tar_target(name = registry, command = build_registry(mainstems, 
                                                       registry = registry_file,
                                                       providers = provider_file)),
  tar_target(name = write_reg, command = write_registry(registry, registry_file))
)
