#!/bin/env bash

N_POOL=200

rm /mm1/* -rf

function create_gz()
{
    head -10000 /access.log.big >/mm1/$1
    gzip /mm1/$1
}

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

for ((i=0; i< 4800; i++))
do
    process_pool_stall
    create_gz $i &
done
