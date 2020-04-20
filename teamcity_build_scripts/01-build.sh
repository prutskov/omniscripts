#!/bin/bash -e

sed -i 's/ 3279/ 49100/g' initdb.cpp
sed -i 's/ 3279/ 49100/g' Tests/MapDHandlerTestHelpers.h
sed -i 's/ 3279/ 49100/g' QueryRunner/QueryRunner.cpp

if [ -f /usr/local/mapd-deps/mapd-deps.sh ]; then
    . /usr/local/mapd-deps/mapd-deps.sh
else
    cdd=`pwd`
    cd /usr/local/mapd-deps
    for nn in `ls -d -- *` ; do
        echo $nn
    done
    cd $cdd
    . /usr/local/mapd-deps/$nn/mapd-deps-$nn.sh
fi

mkdir -p build
cd build
cmake -DCMAKE_BUILD_TYPE=release -DENABLE_CUDA=off -DENABLE_AWS_S3=off ..
make -j 16

if [ -f /localdisk/benchmark_datasets/omnisci.conf ]; then cp /localdisk/benchmark_datasets/omnisci.conf . ; fi
if [ -f /data/dataf/benchmark_datasets/omnisci.conf ]; then cp /data/dataf/benchmark_datasets/omnisci.conf . ; fi

echo '#!/bin/bash'                                                  >_init_db.sh
echo ''                                                            >>_init_db.sh
echo 'utils_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )' >>_init_db.sh
echo 'rm -rf ${utils_dir}/data'                                    >>_init_db.sh
echo 'mkdir ${utils_dir}/data'                                     >>_init_db.sh
echo '${utils_dir}/bin/initdb --data ${utils_dir}/data'            >>_init_db.sh

chmod 777 ./_init_db.sh
./_init_db.sh

cd ..
