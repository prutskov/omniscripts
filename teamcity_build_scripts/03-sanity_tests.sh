#!/bin/bash -e

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

cd build
make sanity_tests
