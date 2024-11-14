#!/bin/bash

path=$(pwd)
name=$(awk -F'/' '{print $(NF-1)}' <<< "$path")
bash ./batchcmd.sh 'cd ~/'$name'/sbin/ && ./updateLocalIP.sh'

