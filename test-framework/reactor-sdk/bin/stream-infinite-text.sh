#!/bin/bash
#!/bin/bash
if [ "$#" -ne 1 ] ; then
     echo "Usage: $0 [hostname]" >&2
     exit 1
fi

while : 
do
   i=`curl -q -d "today $msg" http://$1:10000/stream/text  2>&1`
   echo "$i" 
done
