#!/bin/bash
export REACTOR_HOME=/Users/alex/git/reactor
echo $REACTOR_HOME

beamer software install app-fabric -c goatcluster -r
beamer software install data-fabric -c goatcluster -r
beamer software install gateway -c goatcluster -r
beamer software install kafka -c goatcluster -r
beamer software install overlord -c goatcluster -r
beamer software install watchdog -c goatcluster -r
beamer software install web-cloud-app -c goatcluster -r


