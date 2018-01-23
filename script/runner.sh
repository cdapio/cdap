#!/bin/bash
HIVE_HOME=/usr/hdp/2.3.4.7-4/hive

if [ ! -d $HIVE_HOME ]; then
    echo "File not found!"
    exit
fi

echo -e '1\x01foo' > /tmp/a.txt
echo -e '2\x01bar' >> /tmp/a.txt

CLASSPATH=.


#for i in /opt/cdap/master/lib/*.jar ; do
#    CLASSPATH=$CLASSPATH:$i
#done

CLASSPATH=$CLASSPATH:$(cdap classpath):$HIVE_HOME/conf

for i in ${HIVE_HOME}/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

if [ -f HiveJdbcClient.class ]; then
    mkdir -p co/cask/cdap/explore/executor
    mv HiveJdbcClient*.class co/cask/cdap/explore/executor/
fi

# CLASSPATH=$CLASSPATH:co/cask/cdap/explore/executor/HiveJdbcClient$1.class
# CLASSPATH=$CLASSPATH:co/cask/cdap/explore/executor/HiveJdbcClient$2.class

echo $CLASSPATH

java -cp $CLASSPATH co.cask.cdap.explore.executor.HiveJdbcClient
