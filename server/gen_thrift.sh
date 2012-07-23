#!/bin/sh

# Copy in the Thrift files
echo "Updating local thrift definitions"
curl -s "https://raw.github.com/continuuity/flow/master/src/main/thrift/flowservices.thrift?login=continuuity&token=5a5733043bc346ece7890a90f6f30cb5" > flowservices.thrift
curl -s "https://raw.github.com/continuuity/overlord/master/src/main/thrift/flowmonitor.thrift?login=continuuity&token=d352c1a13e36aab9717902b43b91f978" > flowmonitor.thrift

# Generate the new bindings
echo "Generating thrift bindings"

# Clean up any existing bindings
rm -rf ./thrift_bindings/
mkdir thrift_bindings

thrift --gen js:node -out thrift_bindings flowmonitor.thrift
thrift --gen js:node -out thrift_bindings flowservices.thrift