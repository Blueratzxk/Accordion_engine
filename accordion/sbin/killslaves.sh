#!/bin/bash


#./batchcmd.sh 'pkill OLVP'




time=$(date +%Y_%m_%d_%H_%M_%S)
username=$(whoami)
path=$(pwd)

name=$(awk -F'/' '{print $(NF-1)}' <<< "$path")

for line in `cat slaves`
do
	{
               ssh $line "pkill Accordion" > /dev/null 2>&1 				  
		echo "slave $line closed!"

       } &
	done
wait




