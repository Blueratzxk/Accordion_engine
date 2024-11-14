#!/bin/bash
  
time=$(date +%Y_%m_%d_%H_%M_%S)
username=$(whoami)
path=$(pwd)

name=$(awk -F'/' '{print $(NF-1)}' <<< "$path")

for line in `cat slaves`
do
	{
		    #nohup ssh $line "ls ~/accordion/ ; ~/accordion/run.sh > ~/accordion/logs/"$time".log"  2>&1 &
		    nohup ssh $line "cd ~/$name/ && nohup ./run.sh > log ">/dev/null 2>&1 &
	
		        echo "slave $line open!"

		} &
done
wait

