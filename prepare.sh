#!/bin/env bash

for ((i=0; i< 1200; i++))
do
    head -10000 /access.log.big >/mm/${i}
done
