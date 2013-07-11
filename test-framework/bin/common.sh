#!/bin/sh

#setting up environment
source env.sh
: ${APP_HOME?"not set."}

FILE=$APP_HOME/examples/WordCount/WordCount.jar

if [ -f $FILE ];
then
   echo "$FILE exists, skipping compile"
else
   ant -f $APP_HOME/examples/build.xml
fi



