#!/bin/bash 
# Usage deploy_all.sh cluster_name
# deploys all continuuity components except web-cloud-app
# Note: 
#  1) Must set REACTOR_HOME in env
#  2) Must have beamer in path

if [ -z "$REACTOR_HOME" ]; then
    echo "Need to set REACTOR_HOME before running this script. Exiting."
    exit 1
fi    

command -v beamer >/dev/null 2>&1 || { echo "beamer not found in path. Exiting." >&2; exit 1;}

echo "Deploying all components except web-cloud-app"
for component in app-fabric data-fabric overlord kafka gateway watchdog ; do beamer software install $component -c $1; done
