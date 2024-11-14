#!/bin/bash


path=$(pwd)

name=$(awk -F'/' '{print $(NF-1)}' <<< "$path")

bash batchscp.sh ../../$name ~/$name

