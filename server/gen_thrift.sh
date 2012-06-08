#! /bin/sh

thrift --gen js:node -out thrift_bindings flowmonitor.thrift
thrift --gen js:node -out thrift_bindings flowservices.thrift