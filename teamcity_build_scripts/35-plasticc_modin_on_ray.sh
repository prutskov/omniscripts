#!/bin/bash

mkdir -p ${PWD}/tmp
python3 run_ibis_tests.py --env_name ${ENV_NAME} --env_check True --save_env True -task benchmark                              \
                          --ci_requirements "${PWD}/ci_requirements.yml" -bench_name plasticc                                  \
                          -data_file '${DATASETS_PWD}/plasticc/'                                                               \
                          -pandas_mode Modin_on_dask -ray_tmpdir ${PWD}/tmp                                                     \
                          -commit_omnisci ${BUILD_REVISION}                                                                    \
                          -commit_omniscripts ${BUILD_OMNISCRIPTS_REVISION}                                                    \
                          -commit_modin ${BUILD_MODIN_REVISION}                                                                \
                          ${ADDITIONAL_OPTS}                                                                                   \
                          ${ADDITIONAL_OPTS_NIGHTLY}                                                                           \
                          ${DB_COMMON_OPTS} ${DB_PLASTICC_OPTS}
