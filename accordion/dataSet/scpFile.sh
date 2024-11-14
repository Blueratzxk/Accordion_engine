#!/bin/bash

filename=$1
distUser=$2
distIP=$3
distPath=$4
distPasswd=$5

scp $filename $distUser@$distIP:$distPath

#expect -c "
#    spawn scp $filename $distUser@$distIP:$distPath
#    expect {
#	 \"yes/no\" {send \"yes\r\";exp_continue;}
#        \"*password\" {set timeout 500;send \"$distPasswd\r\";}
#    }"

