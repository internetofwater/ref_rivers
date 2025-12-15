library(targets)

tar_option_set(
  packages = c("dplyr", "sf", "sbtools", "nhdplusTools"),
  format = "rds"
)


if(package_version(packageVersion("nhdplusTools")) < "1.0.1") 
  stop("nhdplusTools version must be greater than 1.0.1")

tar_source()

list(
  tar_target(registry_file, "registry/ref_rivers.csv", format = "file"),
  tar_target(provider_file, "registry/providers.csv", format = "file"),
  tar_target(mainstems_v1, get_mainstems_db_1()),
  tar_target(mainstems_v2, get_mainstems_db_2()),
  tar_target(mainstems_v3, get_mainstems_db_3()),
  tar_target(enhd_v1, get_enhd_1()),
  tar_target(enhd_v2, get_enhd_2()),
  tar_target(enhd_v3, get_enhd_3()),
  tar_target(ref_net_v1, get_ref_network_1()),
  tar_target(nhdp_gdb, nhdplusTools::download_nhdplusv2("data/nhdp")),
  tar_target(nhdp_geo, sf::read_sf(nhdp_gdb, "NHDFlowline_Network")),
  tar_target(reconciled_mainstems, reconcile_mainstems(mainstems_v2, 
                                                       mainstems_v3,
                                                       enhd_v2,
                                                       enhd_v3)),
  tar_target(mainstems, make_mainstems(mainstems_v1, 
                                       mainstems_v2,
                                       enhd_v1,
                                       enhd_v2,
                                       reconciled_mainstems,
                                       "out/mainstems.gpkg")),
  tar_target(lookup, write_lookups(mainstems, enhd_v2)),
  tar_target(registry, build_registry(mainstems, 
                                      registry = registry_file,
                                      providers = provider_file)),
  tar_target(write_reg, write_registry(registry, registry_file))
)
