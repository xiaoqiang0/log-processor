#!/bin/env bash

NF=4800
for ((i=0; i< $NF; i++))
do
    rm -fr /mm/$i.gz
done

time rsync -avr /mm1/ /mm >/dev/null 2>&1

echo "done"
