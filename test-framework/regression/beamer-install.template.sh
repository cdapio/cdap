#!/bin/sh 
dir=`dirname $0`;
if [ x$dir = "x." ]
then
    dir=`pwd`
fi
base=`basename $0`;
r=$RANDOM
mkdir -p /tmp/beamer-$r
(pushd /tmp/beamer-$r; uudecode -p $dir/$base | tar xzfv -;popd; pushd /tmp/beamer-$r/beamer; make; popd; rm -rf /tmp/beamer-$r)
exit 0;
