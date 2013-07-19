#!/bin/sh

#!/bin/sh
if [ $# -lt 2 ]
then
    echo "Usage: procedure.sh <start|stop> <APP_HOME>"
    exit 1
fi

APP_HOME=$1

echo "Starting/stopping all procedures..."
$APP_HOME/bin/reactor-client $1 --application CountCounts --procedure CountCountProcedure
$APP_HOME/bin/reactor-client $1 --application HelloWorld --procedure whoFlow
$APP_HOME/bin/reactor-client $1 --application PurchaseHistory  --procedure PurchaseQuery
$APP_HOME/bin/reactor-client $1 --application WordCount --procedure WordCounter
echo "All applications started"
