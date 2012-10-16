#!/bin/sh

# Copy in the Thrift files
echo "Updating local thrift definitions"

curl -s "https://raw.github.com/continuuity/flow/master/src/main/thrift/flowservices.thrift?login=dmosites&token=39bcb176f48dedba729fb8f1bd3d8f86" > flowservices.thrift
curl -s "https://raw.github.com/continuuity/overlord/master/src/thrift/thrift/metadataservice.thrift?login=dmosites&token=f32ca8c4221b48385765b7ef9611b6ae" > metadataservice.thrift
curl -s "https://raw.github.com/continuuity/overlord/master/src/thrift/thrift/metricsservice.thrift?login=dmosites&token=754afd44ca0d9fd7787d6af3c2d3d1b5" > metricsservice.thrift

# Generate the new bindings
echo "Generating thrift bindings"

# Clean up any existing bindings
rm -rf ./thrift_bindings/
mkdir thrift_bindings

thrift --gen js:node -out thrift_bindings metricsservice.thrift
thrift --gen js:node -out thrift_bindings flowservices.thrift
thrift --gen js:node -out thrift_bindings metadataservice.thrift

rm metricsservice.thrift
rm flowservices.thrift
rm metadataservice.thrift
