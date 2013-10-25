What is Your App ?
======================

Haystack is an application to process and analyze logs, to make it easier to detect anomalies in it.


How to use 
============

  * git clone git@github.com:continuuity/reactor-apps
  * cd reactor-apps
  * cd splunk-lite

System Overview
=================

   * Event Processor
     - Process and analyze logs
     - Aggregate logs in multiple dimensions (hostname, component, loglevels)
     - Detect anomalies based on error and warning thresholds
   * Queries
     - Timebound search on logs
     - Analytics on different dimensions
     - Fetch anomalies
