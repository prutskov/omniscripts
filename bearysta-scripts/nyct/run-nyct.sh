#!/bin/bash
set -xe
bash -xe prepare_table.sh $slack $OMP_NUM_THREADS
env $env numactl $numa -C $cpus ../../../omniscidb/$build/bin/omnisci_server --config omnisci-bench-taxi-reduced.conf --data=data-$(($slack*$OMP_NUM_THREADS)) --max-num-threads=$OMP_NUM_THREADS --db-query-list=db-query-list-taxi-reduced-$query.sql $* 2>&1
