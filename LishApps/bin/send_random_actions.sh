#!/bin/bash

#
# Sends random social action events to the social-actions stream
#

if [ $# -ne 1 ]; then
	echo "Required argument: <num_actions>"
	exit
fi

num_actions=$1

# Set pointers to URL endpoint

GATEWAY_HOSTNAME=${GATEWAY_HOSTNAME:=localhost}
GATEWAY_REST_BASE_URL=${GATEWAY_REST_BASE_URL:=http://${GATEWAY_HOSTNAME}:10000/rest-stream/}
STREAM_NAME=${STREAM_NAME:=social-actions}
GATEWAY_STREAM_URL=${GATEWAY_STREAM_URL:=$GATEWAY_REST_BASE_URL$STREAM_NAME}

# Initialize variables

action_id=${action_id:=100} # Initial action id (increments from here)
product_id=${product_id:=2000} # Initial product id (randomly increments from here)
product_id_max_increment=${product_id_max_increment:=200}
num_stores=${num_stores:=75} # Number of stores (products auto-bucketed to stores)

#{"@id":"1","product_id":"1","store_id":"201","category":"Sports","actor_id":"301","type":"yay-exp-action","date":"1349125210000","@view":"SIMPLE","score":"1.0"}

# Generate random id

for (( i=0; i<$num_actions; i++ )); do
	product_id=$(($product_id + ($RANDOM % $product_id_max_increment)))
	store_id=$(($product_id % $num_stores))
	date=$((($(date +%s)*1000) + ($RANDOM % 1000)))
	json="{\"@id\":\"$action_id\",\"product_id\":\"$product_id\",\"store_id\":\"$store_id\",\"category\":\"Sports\",\"actor_id\":\"301\",\"type\":\"yay-exp-action\",\"date\":\"$date\",\"@view\":\"SIMPLE\",\"score\":\"1.0\"}"
	echo "Inserting action: $json"
	curl -v "$GATEWAY_STREAM_URL" --data $json
	(( action_id ++ ))
done
