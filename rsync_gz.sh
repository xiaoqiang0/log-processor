#!/bin/env bash

NF=4800
N_POOL=300

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

rm -rf /mm/*.gz

#for ((i=0; i< $NF; i++))
#do
#    process_pool_stall
#    rm -f /mm/$i.gz &
#done

echo "Start rsync ..."
time rsync -avr /mm1/ /mm >/dev/null 2>&1

echo "done"
