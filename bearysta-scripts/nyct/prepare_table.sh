#!/bin/bash
all_files=`ls /localdisk/taxi_data_reduced/trips_reduced_*.csv.gz`
set -xe
if [ -z "$1" ]; then
  fragments="default"
else
  slack=$1
  threads=$2
  fragments=$(($slack*$threads))
fi

omnisci_server=../../../omniscidb/${build:-build}/bin/omnisci_server

if [ ! -d "data-$fragments" ]; then
    mkdir -p data-$fragments && ../../../omniscidb/build/bin/initdb --data data-$fragments

    lines=0
    echo "-- copying data">tmp1.sql
    for file in $all_files; do
        echo "COPY trips_reduced FROM '$file' WITH (header='false');" >>tmp1.sql
        if [ -f $file.lc ]; then
            l=`cat $file.lc`
        else
            l=`gunzip -c $file | wc -l`
            echo $l >$file.lc
        fi
        lines=$(($lines+$l))
    done
    echo "}" >>tmp1.sql
    echo Total lines: $lines


    echo "USER admin omnisci {" >tmp.sql
    cat init.sql >>tmp.sql
    if [ -z "$1" ]; then
      sed -ri -e "s/fragment_size=[0-9]*//;s/WITH ?\(\)//" tmp.sql
    else
      frags=$(($lines/$fragments))
      sed -ri -e "s/fragment_size=[0-9]*/fragment_size=$frags/" tmp.sql
    fi
    cat tmp1.sql >> tmp.sql
    $omnisci_server --db-query-list=tmp.sql --exit-after-warmup --data data-$fragments
    rm tmp.sql tmp1.sql
fi
