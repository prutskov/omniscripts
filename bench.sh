#!/bin/bash
export ENV_NAME=scaleout_bench
export ADDITIONAL_OPTS="-no_ml True -no_ibis True -iterations 1"
export BUILD_MODIN_REVISION=123
export BUILD_OMNISCRIPTS_REVISION=123
export BUILD_REVISION=123
export BUILD_IBIS_REVISION=123
export DATABASE_NAME=agent_test_ibis
export MODIN_USE_CALCITE=True
export PYTHONIOENCODING=UTF-8
export PYTHONUNBUFFERED=1
export DATASETS_PWD=/localdisk/benchmark_datasets

export MODIN_CPUS=112
export SCALEOUT_BACKEND=Ray
export MODIN_EXPERIMENTAL=True

# ./teamcity_build_scripts/19-build_modin_dbe.sh
# no_proxy= http_proxy= https_proxy= ./teamcity_build_scripts/32-ny_taxi_modin_on_ray_20M_records.sh
# no_proxy= http_proxy= https_proxy= ./teamcity_build_scripts/35-plasticc_modin_on_ray.sh
no_proxy= http_proxy= https_proxy= ./teamcity_build_scripts/31-census_modin_on_ray.sh
