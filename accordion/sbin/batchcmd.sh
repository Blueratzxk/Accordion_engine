#!/bin/sh

username=$(whoami)
for line in `cat slaves`
do
{
    echo $line
    ssh $line $1
    #echo ' '

}

done
#wait

