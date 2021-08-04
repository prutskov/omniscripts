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
export SCALEOUT_BACKEND=MPI
export MODIN_EXPERIMENTAL=True

source /localdisk/aprutsko/oneapi_packages/setvars.sh

export HOME=/localdisk/aprutsko/scaleout-execution-layer
export PYTHONPATH=$PYTHONPATH:$HOME:$HOME/scaleout/backends/mpi/core

# ./teamcity_build_scripts/19-build_modin_dbe.sh
# no_proxy= http_proxy= https_proxy= ./teamcity_build_scripts/32-ny_taxi_modin_on_ray_20M_records.sh
# no_proxy= http_proxy= https_proxy= ./teamcity_build_scripts/35-plasticc_modin_on_ray.sh

mkdir -p ${PWD}/tmp

# ----------------CENSUS---------------------------------
python run_ibis_tests.py --env_name ${ENV_NAME} --env_check True --save_env True -task benchmark   \
                          --ci_requirements "${PWD}/ci_requirements.yml" -bench_name census                                    \
                          -data_file '${DATASETS_PWD}/census/ipums_education2income_1970-2010.csv.gz'                          \
                          -pandas_mode Modin_on_scaleout -ray_tmpdir ${PWD}/tmp                                                     \
                          -commit_omnisci ${BUILD_REVISION}                                                                    \
                          -commit_omniscripts ${BUILD_OMNISCRIPTS_REVISION}                                                    \
                          -commit_modin ${BUILD_MODIN_REVISION}                                                                \
                          ${ADDITIONAL_OPTS}                                                                                   \
                          ${ADDITIONAL_OPTS_NIGHTLY}                                                                           \
                          ${DB_COMMON_OPTS} ${DB_CENSUS_OPTS}


# PLASTICC
# python3 run_ibis_tests.py --env_name ${ENV_NAME} --env_check True --save_env True -task benchmark                              \
#                           --ci_requirements "${PWD}/ci_requirements.yml" -bench_name plasticc                                  \
#                           -data_file '${DATASETS_PWD}/plasticc/'                                                               \
#                           -pandas_mode Modin_on_scaleout -ray_tmpdir ${PWD}/tmp                                                     \
#                           -commit_omnisci ${BUILD_REVISION}                                                                    \
#                           -commit_omniscripts ${BUILD_OMNISCRIPTS_REVISION}                                                    \
#                           -commit_modin ${BUILD_MODIN_REVISION}                                                                \
#                           ${ADDITIONAL_OPTS}                                                                                   \
#                           ${ADDITIONAL_OPTS_NIGHTLY}                                                                           \
#                           ${DB_COMMON_OPTS} ${DB_PLASTICC_OPTS}
