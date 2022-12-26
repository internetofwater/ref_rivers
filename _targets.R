#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline # nolint

# Load packages required to define the pipeline:
library(targets)
# library(tarchetypes) # Load other packages as needed. # nolint

# Set target options:
tar_option_set(
  packages = c("dplyr", "sf", "sbtools"), # packages that your targets need to run
  format = "rds" # default storage format
  # Set other options as needed.
)

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
  tar_target(name = mainstems_v0.1, command = get_mainstems_db(v = "v0.1")),
  tar_target(name = mainstems, command = make_mainstems(mainstems_v0.1, 
                                                        "out/mainstems.gpkg")),
  tar_target(name = registry, command = build_registry(mainstems, 
                                                       registry = registry_file,
                                                       providers = provider_file)),
  tar_target(name = write_reg, command = write_registry(registry, registry_file))
)
