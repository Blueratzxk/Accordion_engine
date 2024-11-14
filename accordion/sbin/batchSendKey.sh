#!/bin/bash

userv=$(sed -n '1p' ../userpasswd)
passwdv=$(sed -n '2p' ../userpasswd)

if [ ! -f ~/.ssh/id_rsa ];then
	 ssh-keygen -t rsa
 else
	  echo "id_rsa has created ..."
fi
 
while read line
	  do
		      user=$userv
		          ip=`echo $line | cut -d " " -f 1`
			      passwd=$passwdv
			          expect <<EOF
      set timeout 10
      spawn ssh-copy-id -i /$user/.ssh/id_rsa.pub $user@$ip
      expect {
        "yes/no" { send "yes\n";exp_continue }
        "password" { send "$passwd\n" }
      }
      expect "password" { send "$passwd\n" }
EOF
  done <  slaves
