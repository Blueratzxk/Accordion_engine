#!/bin/bash

ip=$(./getLocalIP.sh)
echo "\'$ip\'"

./httpConfigIpUpdater ../httpconfig.config $ip

