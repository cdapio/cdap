action=$1
gateway=$2

if [ "x$gateway" == "x" ]; then
  gateway="localhost:10000"
fi

app=$script

if [ "x$action" == "x" ]; then
  usage
  exit 1
fi

usage() {
 echo "Usage: $script <deploy|start|stop> [gateway]"
}

deploy() {
  name=`basename $jar`
  response=$(curl --write-out %{http_code} -s --output /dev/null -f -H "X-Archive-Name: $name" -X POST http://$gateway/v2/apps --data-binary @"$jar")
  if [ "x$response" != "x200" ]; then
    echo "Failed to deploy application $jar."
    exit 1
  fi
}
