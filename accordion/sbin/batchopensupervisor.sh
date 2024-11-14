#!/bin/bash

SHELL_FOLDER=$(dirname $(readlink -f $0))
configPath=$SHELL_FOLDER/../../config
source $configPath/config.sh
time=$(date +%Y_%m_%d_%H_%M_%S)
username=$(whoami)
for line in `cat $slaveNamePath`
do
{
    nohup ssh $line "$GWACHOME/sbin/opensupervisor.sh > $GWACHOME/logs/supervisor_"$time".log"  2>&1 &
    echo "Supervisor $line open!"

} &
done
wait

