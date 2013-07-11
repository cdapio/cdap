#!/bin/sh

#setting up environment
APP_HOME=/Users/alex/distributions/continuuity-developer-suite-1.6.0-SNAPSHOT
echo "Setting up runtime environment..."
echo $APP_HOME

: ${APP_HOME?"not set."}

FILE=$APP_HOME/examples/WordCount/WordCount.jar

if [ -f $FILE ];
then
   echo "$FILE exists, skipping compile"
else
   ant -f $APP_HOME/examples/build.xml
fi



