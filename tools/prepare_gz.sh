#!/bin/env bash


lines_per_file=10000
files_nr=4800

N_POOL=200

#R_SOURCE_DIR=/root/windows/mm1
#R_TARGET_DIR=/root/windows/mm
R_SOURCE_DIR=/mm1
R_TARGET_DIR=/mm

rm $R_SOURCE_DIR/* -rf

prj_path=$(cd ..; pwd)
tmpfile=$(mktemp)

function create_gz()
{
    cp $tmpfile $R_SOURCE_DIR/$1
    gzip $R_SOURCE_DIR/$1
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

if [ ! -e "/mm" ];then
    mkdir -p $R_SOURCE_DIR
    mkdir -p $R_TARGET_DIR
fi

echo "clean up first"
rm -rf $R_SOURCE_DIR/*.gz

zcat $prj_path/data/access_vcdn.log.gz |head -$lines_per_file >$tmpfile

echo "creating tempory file $tmpfile"

for ((i=0; i<$files_nr; i++))
do
    process_pool_stall
    create_gz $i &
done
