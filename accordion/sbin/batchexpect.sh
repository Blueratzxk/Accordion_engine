#!/bin/sh


distUser="wamdm"
distPasswd="rdidke#dt"



username=$(whoami)
for line in `cat slaves`
do
{
	expect -c "
	    spawn ssh $distUser@$line $1
	            expect {
		        \"yes/no\" {send \"yes\r\";exp_continue;}
			\"*password\" {set timeout 500;send \"$distPasswd\r\";}
		}
	"

}

done
#wait

