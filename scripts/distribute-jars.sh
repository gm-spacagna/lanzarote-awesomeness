# Use > 0 to consume one or more arguments per pass in the loop (e.g.
# some arguments don't have a corresponding value to go with it such
# as in the --default example).
# note: if this is set to > 0 the /etc/hosts part is not recognized ( may be a bug )
while [[ $# > 1 ]]
do
key="$1"

case $key in
    -p|--password)
    PASSWORD="$2"
    shift # past argument
    ;;
    -u|--username)
    USERNAME="$2"
    shift # past argument
    ;;
    --default)
    DEFAULT=YES
    ;;
    *)
            # unknown option
    ;;
esac
shift # past argument or value
done


nodes="1394 1405 1406 1403 1404"
for node in $nodes
do
echo "copying jar files to gbrdsr00000${node}"

sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no terajdbc4.jar ${USERNAME}@gbrdsr00000${node}.intranet.barcapint.com:/tmp/terajdbc4.jar
sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no tdgssconfig.jar ${USERNAME}@gbrdsr00000${node}.intranet.barcapint.com:/tmp/tdgssconfig.jar

#sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no terajdbc4.jar gbrdsr00000${node}.intranet.barcapint.com:/tmp/terajdbc4.jar
#sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no tdgssconfig.jar gbrdsr00000${node}.intranet.barcapint.com:/tmp/tdgssconfig.jar

done
echo "deploying fat jar to uat"
sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no ../target/scala-2.10/ftb-assembly-0.1.0-SNAPSHOT.jar   ${USERNAME}@10.33.60.180:~/
echo sshpass -p "${PASSWORD}" scp  -o StrictHostKeyChecking=no ../target/scala-2.10/ftb-assembly-0.1.0-SNAPSHOT.jar   ${USERNAME}@10.33.60.180:~/
