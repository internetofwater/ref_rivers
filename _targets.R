library(targets)

tar_option_set(
  packages = c("dplyr", "sf", "sbtools", "nhdplusTools"),
  format = "rds"
)


if(package_version(packageVersion("nhdplusTools")) < "1.0.1") 
  stop("nhdplusTools version must be greater than 1.0.1")

tar_source()

list(
  tar_target(name = registry_file, command = "registry/ref_rivers.csv", format = "file"),
  tar_target(name = provider_file, command = "registry/providers.csv", format = "file"),
  tar_target(name = mainstems_v1, command = get_mainstems_db_1()),
  tar_target(name = mainstems_v2, command = get_mainstems_db_2()),
  tar_target(name = mainstems_v3, command = get_mainstems_db_3()),
  tar_target(name = enhd_v1, command = get_enhd_1()),
  tar_target(name = enhd_v2, command = get_enhd_2()),
  tar_target(name = ref_net_v1, command = get_ref_network_1()),
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
  tar_target(name = lookup, command = write_lookups(mainstems, enhd_v2)),
  tar_target(name = registry, command = build_registry(mainstems, 
                                                       registry = registry_file,
                                                       providers = provider_file)),
  tar_target(name = write_reg, command = write_registry(registry, registry_file))
)
