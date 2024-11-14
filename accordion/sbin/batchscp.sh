#!/bin/sh
user=$(sed -n '1p' ../userpasswd)

for line in `cat slaves`
do
{
    scp -r $1 $user@$line:$2

} &
done
wait

