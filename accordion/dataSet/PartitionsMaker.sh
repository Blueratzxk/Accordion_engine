#!/bin/bash
export LD_LIBRARY_PATH=../libs

path=$(pwd)

userv=$(sed -n '1p' ../userpasswd)
passwdv=$(sed -n '2p' ../userpasswd)
fpath='../DataFileDicts'


name=$(awk -F'/' '{print $(NF-1)}' <<< "$path")

echo $name

echo "./partitionsMaker $fpath $userv $passwdv "~/$name/" $fpath "../sbin/slaves" "tablePartitions.txt""
./partitionsMaker $fpath $userv $passwdv "~/$name/" $fpath "../sbin/slaves" "tablePartitions.txt"
