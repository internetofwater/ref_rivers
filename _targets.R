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
  tar_target(ref_rivers_v21, get_ref_rivers(
    version = "v2.1", 
    sha256sum = "a9161151f3513206b6d5348c827dca6cbc4df147f8de98847ad1fa1e58a6a099")),
  tar_target(ref_net_v1, get_ref_network_1()),
  tar_target(hr_net, get_hr_lookup()),
  tar_target(nhdp_gdb, nhdplusTools::download_nhdplusv2("data/nhdp")),
  tar_target(nhdp_geo, sf::read_sf(nhdp_gdb, "NHDFlowline_Network")),
  tar_target(reconciled_mainstems, reconcile_mainstems(mainstems_v2, 
    mainstems_v3,
    enhd_v2,
    enhd_v3, 
    ref_net_v1)),
    tar_target(mainstems, make_mainstems(mainstems_v2, 
      mainstems_v3,
      enhd_v3,
      ref_rivers_v21,
      ref_net_v1,
      reconciled_mainstems,
      "out/mainstems.gpkg")),
  tar_target(lookup, write_lookups(mainstems, enhd_v3, ref_net_v1, hr_net), format = "file"),
  tar_target(validate, validate_mainstems(mainstems)),
  tar_target(non_ref_mainstems, make_nonref(
    mainstems = mainstems, 
    new_net = ref_net_v1, 
    lookup = lookup, 
    out_f = "out/extra_mainstems.gpkg")),
  tar_target(registry, build_registry(mainstems, 
                                      registry = registry_file,
                                      providers = provider_file)),
  tar_target(write_reg, write_registry(registry, registry_file))
)
