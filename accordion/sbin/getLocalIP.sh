#!/bin/bash
/sbin/ifconfig -a | grep inet | grep -v 127.0.0.1 |grep -v 172* |  grep -v inet6 |awk '{print $2}' | tr -d 'addrs:' | tr -d '>地址:' | tr -d '\n'
#/sbin/ifconfig -a | grep inet | grep -v 127 | grep -v inet6 | grep -v 172 | awk '{print $2}' 

