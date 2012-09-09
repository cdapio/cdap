#!/bin/sh

# Copy in the Thrift files
echo "Updating local thrift definitions"
curl -s "https://raw.github.com/continuuity/flow/master/src/main/thrift/flowservices.thrift?login=dmosites&token=39bcb176f48dedba729fb8f1bd3d8f86" > flowservices.thrift
curl -s "https://raw.github.com/continuuity/overlord/master/src/main/thrift/metricsservice.thrift?login=dmosites&token=a4a8b6e77ae879e232485347dc629233" > metricsservice.thrift

# Generate the new bindings
echo "Generating thrift bindings"

# Clean up any existing bindings
rm -rf ./thrift_bindings/
mkdir thrift_bindings

thrift --gen js:node -out thrift_bindings metricsservice.thrift
thrift --gen js:node -out thrift_bindings flowservices.thrift

rm metricsservice.thrift
rm flowservices.thrift
