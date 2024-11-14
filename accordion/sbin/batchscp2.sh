#!/bin/sh

userv=$(sed -n '1p' ../userpasswd)
passwdv=$(sed -n '2p' ../userpasswd)


filename=$1
distUser=$userv
distPath=$2
distPasswd=$passwdv


username=$(whoami)
for line in `cat slaves`
do
{

expect -c "
    spawn scp -r $filename $distUser@$line:$distPath
        expect {
	\"yes/no\" {send \"yes\r\";exp_continue;}
	\"*password\" {set timeout 500;send \"$distPasswd\r\";}
}"


}

done
#wait

