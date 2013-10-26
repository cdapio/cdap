#!/bin/bash


# fields:

# timestamp=20131007T103055,symbol=AAPL,exchange=NYSE,broker=JPMC,currency=USD,payload=iVBORw0KGgo

usage()
{
cat <<EOF

usage: $0 options

This script will generate random fake order data


OPTIONS:
   -h    Destination host (default: localhost)
   -p    Destination port (default: 10000)
   -d    optional delay between sending datapoints
   -v    verbose - also print datapoints to stdout

EOF
}

VERBOSE=0

while getopts "vh:p:s:d:" OPTION
do
     case $OPTION in
         h)
             DESTHOST=$OPTARG
             ;;
         p)
             DESTPORT=$OPTARG
             ;;
         s)
             SYMBOL=$OPTARG
             ;;
         d)
             DELAY=$OPTARG
             ;;
         v)
             VERBOSE=1
             ;;
         
         ?)
             echo "Illegal argument $OPTION=$OPTARG" >&2
             usage
             exit
             ;;
     esac
done

#if [ -z "$SYMBOL" ] ; then
#  usage
#  exit 1
#fi

if [ -z "$DESTHOST" ] ; then
  DESTHOST=localhost
fi
if [ -z "$DESTPORT" ] ; then
  DESTPORT=10000
fi
if [ -z "$DELAY" ] ; then
  DELAY=0
fi

#echo $a | awk '{print tolower($0)}'

SYMBOLS=(`find . -name '*.txt' | sed 's/^.*\/\(.*\)\.us.txt$/\1/' | tr '[a-z]' '[A-Z]'`)
EXCHANGES=('NYSE' 'NASDAQ')
BROKERS=('JPMC' 'JPMC' 'BAC')
CURRENCYS=('USD' 'USD' 'USD' 'USD' 'JPY' 'EUR' 'GBP')

index=0
PAYLOAD=""

for i in {a..z}; do arr[index]=$i; index=`expr ${index} + 1`; done
for i in {A..Z}; do arr[index]=$i; index=`expr ${index} + 1`; done
for i in {0..9}; do arr[index]=$i; index=`expr ${index} + 1`; done


while true ; do

  PAYLOAD=""

  rand=$[ $RANDOM % ${#SYMBOLS[@]} ]
  SYMBOL=${SYMBOLS[$rand]}

  rand=$[ $RANDOM % ${#EXCHANGES[@]} ]
  EXCHANGE=${EXCHANGES[$rand]}

  rand=$[ $RANDOM % ${#BROKERS[@]} ]
  BROKER=${BROKERS[$rand]}

  rand=$[ $RANDOM % ${#CURRENCYS[@]} ]
  CURRENCY=${CURRENCYS[$rand]}


  for i in {1..64}; do PAYLOAD="$PAYLOAD${arr[$RANDOM%$index]}"; done

  DATA="timestamp=`date "+%Y%m%dT%H%M%S"`,symbol=$SYMBOL,exchange=$EXCHANGE,broker=$BROKER,currency=$CURRENCY,payload=$PAYLOAD"

  if [[ $VERBOSE -eq 1 ]] ; then
    echo "debug: $DATA"
  fi
  #echo $DATA | nc $DESTHOST $DESTPORT
  curl -X POST -d "$DATA" "http://$DESTHOST:$DESTPORT/v2/streams/orders"

  sleep $DELAY

done

