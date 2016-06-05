#!/bin/env bash


lines_per_file=10000
files_nr=4800

N_POOL=200

rm /mm1/* -rf

prj_path=$(cd `dirname $0`; pwd)
tmpfile=$(mktemp)

function create_gz()
{
    cp $tmpfile /mm1/$1
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

if [ ! -e "/mm" ];then
    mkdir /mm
    mkdir /mm1
fi

echo "clean up first"
rm -rf /mm1/*.gz

zcat $prj_path/data/access.log.gz |head -$lines_per_file >$tmpfile

echo "creating tempory file $tmpfile"

for ((i=0; i<$files_nr; i++))
do
    process_pool_stall
    create_gz $i &
done
