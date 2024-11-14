sendFile=$1

echo $sendFile

path=$(pwd)

name=$(awk -F'/' '{print $(NF)}' <<< "$path")

userv=$(sed -n '1p' userpasswd)
passwdv=$(sed -n '2p' userpasswd)


for line in `cat sbin/slaves`
do
{
   echo $line
   scp -r $sendFile $userv@$line:~/$name/
}
 done

