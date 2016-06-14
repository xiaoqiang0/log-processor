#!/bin/env bash

NF=1
N_POOL=300

#R_SOURCE_DIR=/root/windows/mm1
#R_TARGET_DIR=/root/windows/mm
R_SOURCE_DIR=/mm1
R_TARGET_DIR=/mm


function process_pool_stall ()
{
    while :; do
        local n_proc=`jobs -r | wc -l`
        if [ ${n_proc} -lt ${N_POOL} ]; then
            break
        fi
        echo "sleep"
        sleep 1
    done
}

rm -rf $R_TARGET_DIR/*.gz

#for ((i=0; i< $NF; i++))
#do
#    process_pool_stall
#    rm -f /mm/$i.gz &
#done
#
echo "Start rsync ..."
time rsync -avr $R_SOURCE_DIR/ $R_TARGET_DIR #>/dev/null 2>&1

echo "done"
