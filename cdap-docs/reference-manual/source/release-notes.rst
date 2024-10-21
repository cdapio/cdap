.. meta::
    :author: Cask Data, Inc.
    :description: Release notes for the Cask Data Application Platform
    :copyright: Copyright © 2014-2018 Cask Data, Inc.

:hide-nav: true

.. _overview_release-notes:

.. index::
   single: Release Notes

.. _release-notes:

============================================
Cask Data Application Platform Release Notes
============================================

.. Summary
.. New Features
.. Improvements
.. Bug Fixes
.. Known Issues
.. API Changes
.. Deprecated and Removed Features

.. contents::
   :local:
   :class: faq
   :backlinks: none
   :depth: 2

`Release 6.2.0 <http://docs.cask.co/cdap/6.2.0/index.html>`__
=============================================================

Summary
-------

This release introduces a number of new features, improvements, and bug fixes to CDAP. Some of the main highlights of the release are:

1. **Replication**
    - A CDAP application using which you can easily replicate data at low-latency and in real-time from transactional and operational databases into analytical data warehouses.

2. **Google Cloud Dataproc Runtime Improvement**
    - The Google Cloud Dataproc runtime now uses native Dataproc API's for job submission instead of SSH.

3. **Pipeline Studio Improvements**
    - Added the ability to perform bulk operations (copy, delete) in the pipeline Studio. Also added a right-click context menu for the Studio.


New Features
------------

- :cask-issue:`CDAP-16385` - Added JDBC plugin selector widget.

- :cask-issue:`CDAP-16339` - Introduced a new REST endpoint for fetching scheduled time for multiple programs.

- :cask-issue:`CDAP-16243` - Added new capability to start system applications using application specific config during startup.

- :cask-issue:`CDAP-16223` - Added Replication feature.

- :cask-issue:`CDAP-16210` - Added support for connecting to multiple hubs through market.base.urls property in cdap-site.

- :cask-issue:`CDAP-16130` - Added the ability to right-click on the Pipeline Studio canvas to add a Wrangler source. This allows you to add multiple Wrangler sources (source + Wrangler transform) in the same pipeline without losing context.

- :cask-issue:`CDAP-16107` - Added support for Spark 2.4.

- :cask-issue:`CDAP-15941` - Added date picker widget to allow users to specify a single date or date range in a plugin.

- :cask-issue:`CDAP-15633` - Added support to launch a job using Google Cloud Dataproc APIs.

- :cask-issue:`CDAP-9014` -  Added the ability to select multiple plugins and connections from Pipeline Studio copy or delete them in bulk.


Improvements
------------

- :cask-issue:`CDAP-16633` - Added option to generate scoped GoogleCredentials with Google BigQuery and Google Drive scope for all Google BigQuery requests.

- :cask-issue:`CDAP-16572` - Added macro support for Format field in Google Cloud Storage plugin.

- :cask-issue:`CDAP-16525` - Added an option for Database source to replace characters in the field names.

- :cask-issue:`CDAP-16809` - Added support for copying header on compressed file.

- :cask-issue:`CDAP-16656` - Added support for rendering large schemas (>1000 fields) in Pipeline UI by collapsing complex schemas and lazy-load fields in record types.

- :cask-issue:`CDAP-16616` - Make the View Raw Logs and Download Logs buttons to be enabled all the time in the log viewer page.

- :cask-issue:`CDAP-16593` - Added restrictions on the maximum number of network tags for Dataproc VM to be 64.

- :cask-issue:`CDAP-16586` - Changed behavior for selecting multiple nodes in Studio to require the user to hold the key [shift] and click on the plugins (instead of holding [ctrl] and then click).

- :cask-issue:`CDAP-16521` - Improved program startup performance by using a thread pool to start a program instead of starting from a single thread.

- :cask-issue:`CDAP-16517` - Added an option to skip header in the files in delimited, csv, tsv, and text formats.

- :cask-issue:`CDAP-16509` - Reduced memory footprint for StructureRecord which improves overall memory consumption for pipeline execution.

- :cask-issue:`CDAP-16351` - Added an API that returns the names of input stages.

- :cask-issue:`CDAP-16330` - Replaced config.getProperties with config.getRawProperties to make sure validation happens on raw value before macros are evaluated.

- :cask-issue:`CDAP-16324` - Added macro support for Analytics plugins.

- :cask-issue:`CDAP-16308` - Reduced preview startup by 60%. Also added limit to maximum concurrent preview runs (10 by default).

- :cask-issue:`CDAP-16249` - Added ability to show dropped field operation from field level lineage page.

- :cask-issue:`CDAP-16248` - For field level lineage, added ability for user to view all fields in a cause or impact dataset (not just the related fields).

- :cask-issue:`CDAP-16211` - Unified JSON structure used by REST endpoints for fetching pipeline configuration and deploying pipelines.

- :cask-issue:`CDAP-15894` - Added ability for user to navigate to non-target dataset by selecting the header of the dataset in field level lineage.

- :cask-issue:`CDAP-15579` - Added the ability for SparkCompute and SparkSink to record field level lineage.

- :cask-issue:`CDAP-15061` - Added a page level error when the user navigates to an invalid pipeline via the URL.

- :cask-issue:`CDAP-13643` - Added support for recording field level lineage in streaming pipelines.


Bug Fixes
---------

- :cask-issue:`CDAP-16816` - Fixed schedule properties to overwrite preferences set on the application instead of the other way around. This most visibly fixed a bug where the compute profile set on a pipeline schedule or trigger would get overwritten by the profile for the pipeline.

- :cask-issue:`CDAP-16751` - Fixed a bug where UI overwrites scale and precision properties in a schema with decimal logical type if the value is 0.

- :cask-issue:`CDAP-16736` - Fixed record schema comparison to include record name.

- :cask-issue:`CDAP-16725` - Fixed a bug where concurrent preview runs were failing because SparkConf for the new preview runs was getting populated with the configurations from the previously started in-progress preview run.

- :cask-issue:`CDAP-16724` - Fixed a bug in Wrangler that would cause it to go out of memory when sampling a Google Cloud Storage object that has a lot of rows.

- :cask-issue:`CDAP-16664` - Fixed a bug that resulted in failure to update/upsert to Google BigQuery in a different project.

- :cask-issue:`CDAP-16663` - Fixed a bug where UI incorrectly showed "No schema available" when the output of the previous stage is a macro.

- :cask-issue:`CDAP-16655` - Fixed a bug in File source that prevented reading files from Google Cloud Storage.

- :cask-issue:`CDAP-16614` - Fixed the fetch run records API to honor the limit query parameter correctly.

- :cask-issue:`CDAP-16581` - Fixed a bug that prevented a user from using parse-as-json directive in Wrangler.

- :cask-issue:`CDAP-16538` - Fixed a bug in the PluginProperties class where internal map was modifiable.

- :cask-issue:`CDAP-16526` - Fixed Google BigQuery sink to properly allow certain types as clustering fields.

- :cask-issue:`CDAP-16501` - Fixed a bug to correctly update pipeline stage metrics in UI.

- :cask-issue:`CDAP-16471` - Fixed a bug that would leave zombie processes when using the Remote Hadoop Provisioner.

- :cask-issue:`CDAP-16465` - Fixed a bug where Wrangler database connections could show more tables than those in the configured database.

- :cask-issue:`CDAP-16453` - Fixed a bug with LimitingInputFormat that made Database source plugin fail in preview mode.

- :cask-issue:`CDAP-16425` - Fixed macro support for output schema in Google BigQuery source plugin.

- :cask-issue:`CDAP-16309` - Fixed a race condition bug that can cause failure when running Spark program.

- :cask-issue:`CDAP-16240` - Fixed a bug to show master and worker memory in Google Cloud Dataproc compute profiles in GB.

- :cask-issue:`CDAP-16055` - Fixed a bug where the failure message emitted by Spark driver was not being collected.

- :cask-issue:`CDAP-16002` - Fixed a bug that caused errors when Wrangler's parse-as-csv with header was used when reading multiple small files.

- :cask-issue:`CDAP-15775` - Fixed a bug that disallowed writing to an empty Google BigQuery table without any data or schema.

- :cask-issue:`CDAP-15649` - Fixed a bug that would cause the Google BigQuery sink to fail the pipeline run if there was no data to write.

- :cask-issue:`CDAP-14850` - Fixed a bug in the custom date range picker that prevented users from setting a custom date range that is not in the current year.

- :cask-issue:`CDAP-14190` - Fixed a bug where users cannot delete the entire column name in Wrangler.


`Release 6.1.1 <http://docs.cask.co/cdap/6.1.1/index.html>`__
=============================================================

Summary
-------

This release introduces a number of new features, improvements, and bug fixes to CDAP. Some of the main highlights of the release are:

1. **Pipeline improvements**
    - Validation checks for plugins for early error detection and prevention
    - New widgets for better pipeline configurability
    - Wrangler ADLS connection

2. **Field Level Lineage**
    - New, intuitive UI for field level lineage
    - Field level lineage support for more plugins

3. **Platform enhancements**
    - Performance improvements across the platform
    - Migration of more UI components from Angular to React

New Features
------------

- :cask-issue:`CDAP-16102` - Added field level lineage support for Error Transform

- :cask-issue:`CDAP-16037` - Added region support for google cloud plugins

- :cask-issue:`CDAP-15795` - New UI landing page

- :cask-issue:`CDAP-15789` - Allow plugin developers to define filters to show/hide properties based on custom plugin configuration logic.

- :cask-issue:`CDAP-15787` - Introduced new FailureCollector apis for better user experience via contextual error messages

- :cask-issue:`CDAP-15767` - Added support for reading INT96 types in parquet file sources.

- :cask-issue:`CDAP-15728` - New ConfigurationGroup component in UI

- :cask-issue:`CDAP-15723` - Added support for pipeline to run in shared vpc network

- :cask-issue:`CDAP-15619` - Stage level validation for plugin properties.

- :cask-issue:`CDAP-15482` - Added a new REST endpoint that retrieves back all field lineage information about a dataset.

- :cask-issue:`CDAP-15342` - Added support for bytes types in the bigquery sink


Deprecation
-----------

- :cask-issue:`CDAP-15917` - Removed the outdated Validator plugin


Bug Fixes
---------

- :cask-issue:`CDAP-16193` - Fix the preview run state after JVM restarted

- :cask-issue:`CDAP-16146` - content type detection now uses case insensitive file extensions

- :cask-issue:`CDAP-16137` - Fixed bug that prevents users from navigating to pipeline studio (indicating system artifacts being loaded for a long time).

- :cask-issue:`CDAP-15973` - Fixed the dataproc provisioner to log the error message if the dataproc creation operation fails.

- :cask-issue:`CDAP-15899` - Fixed a bug that caused pipeline startup to take longer than needed for cloud runs

- :cask-issue:`CDAP-15879` - Fixed regex usage in GCS and S3 source plugins.

- :cask-issue:`CDAP-15878` - Fixed a bug with the Datastore source that was overly restrictive when validating the user provided schema

- :cask-issue:`CDAP-15809` - Fixing a bug which can cause a thread spinning in an infinite while loop due to multi thread consumers on a queue that allows a single consumer.

- :cask-issue:`CDAP-15770` - Fixed a bug that caused pipeline failures when writing nullable byte fields as json.

- :cask-issue:`CDAP-15757` - Fixed a bug that caused MapReduce and Spark logs to be missing for remote pipeline runs

- :cask-issue:`CDAP-15747` - Fixed a race condition that could cause a program to get stuck in the pending state when stopped in the pending state

- :cask-issue:`CDAP-15742` - Added some safeguards to prevent cloud pipeline runs from getting stuck in certain edge cases

- :cask-issue:`CDAP-15726` - Fixed a bug where secure macros were not evaluated in preview mode

- :cask-issue:`CDAP-15617` - Fixed a bug in the BigQuery source that cause automatic bucket creation to fail if the dataset is in a different project.

- :cask-issue:`CDAP-15583` - Fix bug in new user tour on lower resolution screens

- :cask-issue:`CDAP-15554` - Fixed a bug that wrong resolution is used if a time range is specified for metrics query

- :cask-issue:`CDAP-15535` - Fixed an issue where BigQuery multi sink doesn't work if using an Oracle database as a source.

- :cask-issue:`CDAP-15498` - Fixed the dataproc provisioner to disable YARN pre-emptive container killing and to disable conscrypt.

- :cask-issue:`CDAP-15445` - Fixed a bug in the MLPredictor plugin that caused error when using a classification model

- :cask-issue:`CDAP-15423` - Fixed bug that didn't allow users to paste schema as runtime argument

- :cask-issue:`CDAP-15388` - Spark pipelines no longer try to run sinks in parallel unless runtime argument 'pipeline.spark.parallel.sinks.enabled' is set to 'true'. This prevents pipeline sections from being re-processed in the majority of situations.

- :cask-issue:`CDAP-15373` - Fixed the dataproc provisioner to handle networks that do not use automatic subnet creation

- :cask-issue:`CDAP-15353` - Fixed a Wrangler bug where the wrong jdbc driver would be used in some situations and where required classes could be unavailable.

- :cask-issue:`CDAP-15221` - Fixed a bug about artifact version comparison

- :cask-issue:`CDAP-15206` - Fixed a bug that the rollup of the workflow lineage does not remove the local datasets.

- :cask-issue:`CDAP-15097` - Expanding filename format that UI takes in when uploading artifacts.


Improvements
------------

- :cask-issue:`CDAP-16110` - Fixed batch pipeline preview to read only the preview records instead of the full input.

- :cask-issue:`CDAP-16069` - Greatly improved the time it takes to calculate field level lineage

- :cask-issue:`CDAP-15983` - Set Spark as the default execution engine for batch pipeline

- :cask-issue:`CDAP-15794` - Improved error message for csv, tsv, and delimited formats when the schema has fewer fields than the data

- :cask-issue:`CDAP-15782` - Added support to automatically fill field level lineage for plugins that do not emit any

- :cask-issue:`CDAP-15738` - Upgrades Nodejs version from 8.x to 10.16.2

- :cask-issue:`CDAP-15677` - Added support to restore preview status after restart

- :cask-issue:`CDAP-15659` - Route user directly to the pipeline's detail page from pipeline card in Control Center.

- :cask-issue:`CDAP-15489` - New user experience for log level selection.

- :cask-issue:`CDAP-15265` - Added image version as a configuration setting to the dataproc provisioner

- :cask-issue:`CDAP-16076` - Improved the way pipelines with macros that are provided by intermediate stages run.


`Release 6.0.0 <http://docs.cask.co/cdap/6.0.0/index.html>`__
=============================================================

Summary
-------

This release introduces a number of new features, improvements, and bug fixes to CDAP. Some of the main highlights of the release are:

1. **Storage SPIs**
    - Storage SPIs provide abstraction for all system storage used by CDAP so that CDAP is more portable across runtime enviroments - Hadoop or Hadoop-free environments.

2. **Portable Runtime**
    - Provide a runtime architecture for CDAP to support both Hadoop and Hadoopless environments, such as Kubernetes, in a distributed and secure fashion.

3. **Pipeline Enhancements**
    - Improve experience of building pipelines with the help of features such as copy & paste and minimap of the pipeline.
    - Add support for more data types.


New Features
------------

- :cask-issue:`CDAP-14330` - Added Google Cloud Storage copy and move action plugins.

- :cask-issue:`CDAP-14533` - New pipeline list user interface.

- :cask-issue:`CDAP-14613` - Added minimap to pipeline canvas.

- :cask-issue:`CDAP-14645` - Added support for running CDAP system services in Kubernetes environment.

- :cask-issue:`CDAP-14657` - Added the ability to copy and paste a node in pipeline studio.

- :cask-issue:`CDAP-15058` - Added the ability to limit the number of concurrent pipeline runs.

- :cask-issue:`CDAP-15095` - Added support for toggling Stackdriver integration in Google Cloud Dataproc cluster.

- :cask-issue:`CDAP-15256` - Added support for Numeric and Array types in Google BigQuery plugins.

- :cask-issue:`CDAP-15339` - Added support for showing decimal field types in plugin schemas in pipeline view.


Improvements
------------

- :cask-issue:`CDAP-13632` - Added support for CDH 5.15.

- :cask-issue:`CDAP-14653` - Revamps top navbar for CDAP UI based on material design.

- :cask-issue:`CDAP-14667` - Secure store supports integration with other KMS systems such as Google Cloud KMS using new Secure Store SPIs.

- :cask-issue:`CDAP-7208` - Improved CDAP Master logging of events related to programs that it launches.

- :cask-issue:`CDAP-14343` - Use a shared thread pool for provisioning tasks to increase thread utilization.

- :cask-issue:`CDAP-14569` - Improve performance of LevelDB backed Table implementation.

- :cask-issue:`CDAP-14571` - Wrangler supports secure macros in connection.

- :cask-issue:`CDAP-14617` - Significantly improve performance of Transactional Messaging System.

- :cask-issue:`CDAP-14821` - Added early validation for the properties of the Google BigQuery sink to fail during pipeline deployment instead of at runtime.

- :cask-issue:`CDAP-14823` - Improved the error message when a null value is read for a non-nullable field in avro file sources.

- :cask-issue:`CDAP-15047` - Improved loading of system artifacts to load in parallel instead of sequentially.

- :cask-issue:`CDAP-15059` - Improved Google Cloud Dataproc provisioner to allow configuring default projectID from CDAP configuration.

- :cask-issue:`CDAP-15318` - Added support of using runtime arguments to pass in extra configurations for Google Cloud Dataproc provisioner.

- :cask-issue:`CDAP-14579` - Added support for spaces in file path for Google Cloud Storage plugin.

- :cask-issue:`CDAP-14897` - Google BigQuery source now validates schema when the pipeline is deployed.


Bug Fixes
---------

- :cask-issue:`CDAP-12211` - Fixed a casting bug for the DB source where unsigned integer column were incorrectly being treated as integers instead of longs.

- :cask-issue:`CDAP-13410` - Removed the need for ZooKeeper for service discovery in remote runtime environment.

- :cask-issue:`CDAP-7230` - Fixed an issue with recording lineage for realtime sources.

- :cask-issue:`CDAP-12941` - Fixed dynamic Spark plugin to use appropriate context classloader for loading dynamic Spark code.

- :cask-issue:`CDAP-13554` - Fixed a bug that caused MapReduce pipelines to fail when using too many macros.

- :cask-issue:`CDAP-13982` - Fixed an issue that caused pipelines with too many macros to fail when running in MapReduce.

- :cask-issue:`CDAP-14666` - Fixed an issue with publishing metadata changes for profile assignments.

- :cask-issue:`CDAP-14691` - Fixed a bug that would cause workspace ids to clash when wrangling items of the same name.

- :cask-issue:`CDAP-14702` - Fixed a bug in secure store caused by breaking changes in Java update 171. Users should be able to get secure keys on java 8u171.

- :cask-issue:`CDAP-14708` - Fixed a bug that caused Google Cloud Dataproc clusters to fail provisioning if a firewall rule that denies ingress traffic existed in the project.

- :cask-issue:`CDAP-14709` - Fixed a bug that would cause data preparation to fail when preparing a large file in Google Cloud Storage.

- :cask-issue:`CDAP-14724` - Fixed a bug that caused action-only pipelines to fail when running using a cloud profile.

- :cask-issue:`CDAP-14744` - Fixed an issue with adding business tags to an entity.

- :cask-issue:`CDAP-14778` - Fixed an issue in handling metadata search parameters.

- :cask-issue:`CDAP-14779` - Fixed a bug that would cause pipelines to fail on remote clusters if the very first pipeline run was an action-only pipeline.

- :cask-issue:`CDAP-14857` - Fixed the standard deviation aggregate functions to work, even if there is only one element in a group.

- :cask-issue:`CDAP-14951` - Fixed a bug in the Google BigQuery sink that would cause pipelines to fail when writing to a dataset in a different region.

- :cask-issue:`CDAP-15001` - Fixed a race condition in processing profile assignments.

- :cask-issue:`CDAP-15013` - Fixed an issue that could cause inconsistencies in metadata.

- :cask-issue:`CDAP-15069` - Fixed an issue with displaying workspace metadata in the UI.

- :cask-issue:`CDAP-15127` - Fixed a race condition in the remote runtime scp implementation that could cause process to hang.

- :cask-issue:`CDAP-15196` - Fixed an issue with metadata search result pagination.

- :cask-issue:`CDAP-15223` - Fixed Wrangler DB connection where a bad JDBC driver could stay in cache for 60 minutes, making DB connection not usable.

- :cask-issue:`CDAP-15249` - Fixed a NullPointerException in Google Cloud Dataproc provision for when there was no network configured.

- :cask-issue:`CDAP-15299` - Fixed a bug that caused some aggregator and joiner keys to be dropped if they hashed to the same value as another key.

- :cask-issue:`CDAP-15332` - Fixed a bug in the RuntimeMonitor that doesn't reconnect through SSH correctly, causing failure in monitoring the correct program state.

- :cask-issue:`CDAP-15369` - Fixed Google Cloud Dataproc runtime for Google Cloud Platform projects where OS Login is enabled.


Deprecated and Removed Features
-------------------------------

- :cask-issue:`CDAP-15241` - Deprecated HDFSMove and HDFSDelete plugins from core plugins.

- :cask-issue:`CDAP-14591` - Removed Streams and Stream Views, which were deprecated in CDAP 5.0.

- :cask-issue:`CDAP-14592` - Removed Flow, which was deprecated in CDAP 5.0.

- :cask-issue:`CDAP-14529` - Removed deprecated HDFSSink Plugin.

- :cask-issue:`CDAP-14772` - Removed the plugin endpoints feature to prevent execution of plugin code in the cdap master. Endpoints were only used for schema propagation, which has moved to the pipeline system service.

- :cask-issue:`CDAP-14886` - Removed the support for custom routing for user services.



`Release 5.1.2 <http://docs.cask.co/cdap/5.1.2/index.html>`__
=============================================================

Improvements
------------

- :cask-issue:`CDAP-13430` - Improved performance of Apache Spark pipelines that write to multiple sinks.


Bug Fixes
---------

- :cask-issue:`CDAP-14558` - Fixed a bug where pipeline checkpointing is always on regardless of the value set by the user in realtime pipeline.

- :cask-issue:`CDAP-14578` - Fixed a bug where artifacts could not be uploaded through UI.



`Release 5.1.1 <http://docs.cask.co/cdap/5.1.1/index.html>`__
=============================================================

Improvements
------------

- :cask-issue:`CDAP-14490` - Google Cloud Spanner sink will create database and table if they do not exist.

- :cask-issue:`CDAP-14542` - Added a Dataset Project config property to the Google BigQuery source to allow reading from a dataset in another project.


Bug Fixes
---------

- :cask-issue:`CDAP-12229` - Fixed an issue that caused avro, parquet, and orc classes across file, Google Cloud Storage, and S3 plugins to clash and cause pipeline failures.

- :cask-issue:`CDAP-14511` - Fixed a bug where plugins that register other plugins would not use the correct id when using the PluginSelector API.

- :cask-issue:`CDAP-14515` - Fixed a bug where upgraded CDAP instances were not able to load artifacts.

- :cask-issue:`CDAP-14524` - Fixed an issue where configuration of sink was overwritten by source.

- :cask-issue:`CDAP-14538` - Fixed a packaging bug in kafka-plugins that prevented the plugins from being visible.

- :cask-issue:`CDAP-14549` - Fixed a bug where plugins created by other plugins would not have their macros evaluated.

- :cask-issue:`CDAP-14560` - Removed LZO as a compression option for snapshot and time partitioned fileset sinks since the codec cannot be packaged with the plugin.



`Release 5.1.0 <http://docs.cask.co/cdap/5.1.0/index.html>`__
=============================================================

Summary
-------

This release introduces a number of new features, improvements and bug fixes to CDAP. Some of the main highlights of the release are:

1. **Date and Time Support**
    - Support for Date, Time and Timestamp data types in the CDAP schema. In addition, this support is also now available in pipeline plugins and Data Preparation directives.

2. **Plugin Requirements**
    - A way for plugins to specify certain runtime requirements, and the ability to filter available plugins based on those requirements.

3. **Bootstrapping**
    - A method to automatically bootstrap CDAP with a given state, such as a set of deployed apps, artifacts, namespaces, and preferences.

4. **UI Customization**
    - A way to customize the display of the CDAP UI by enabling or disabling certain features.


New Features
------------

- :cask-issue:`CDAP-14244` - Added support for Date/Time in Preparation. Also, added a new directive parse-timestamp to convert unix timestamp in long or string to Timestamp object.

- :cask-issue:`CDAP-14245` - Added Date, Time, and Timestamp support in plugins (Wrangler, Google BigQuery, Google Cloud Spanner, Database).

- :cask-issue:`CDAP-14021` - Added Date, Time, and Timestamp support in CDAP Schema.

- :cask-issue:`CDAP-14028` - Added Date, Time, and Timestamp support in UI.

- :cask-issue:`CDAP-14053` - Added Google Cloud Spanner source and sink plugins in Pipeline and Google Cloud Spanner connection in Preparation.

- :cask-issue:`CDAP-14185` - Added Google Cloud Pub/Sub realtime source.

- :cask-issue:`CDAP-14088` - Added a new user onboarding tour to CDAP.

- :cask-issue:`CDAP-13990` - Added the ability to customize UI through theme.

- :cask-issue:`CDAP-14022` - Added a framework that can be used to bootstrap a CDAP instance.

- :cask-issue:`CDAP-13746` - Added the ability to configure system wide provisioner properties that can be set by admins but not by users.

- :cask-issue:`CDAP-13924` - Added capability to allow specifying requirements by plugins and filter them on the basis of their requirements.

- :cask-issue:`CDAP-13975` - Added REST endpoints to query the run counts of a program.

- :cask-issue:`CDAP-14260` - Added a REST endpoint to get the latest run record of multiple programs in a single call.

- :cask-issue:`CDAP-13653` - Added support for Apache Spark 2.3.


Improvements
------------

- :cask-issue:`CDAP-13566` - Improved runtime monitoring (which fetches program states, metadata and logs) of remotely launched programs from the CDAP Master by using dynamic port forwarding instead of HTTPS for communication.

- :cask-issue:`CDAP-13977` - Removed duplicate classes to reduce the size of the sandbox by a couple hundred megabytes.

- :cask-issue:`CDAP-14461` - Added cdap-env.sh to allow configuring jvm options while launching the Sandbox.

- :cask-issue:`CDAP-14003` - Added support for bidirectional Field Level Lineage.

- :cask-issue:`CDAP-14013` - Added capability for external dataset to record their schema.

- :cask-issue:`CDAP-14091` - The Google Cloud Dataproc provisioner will try to pick up the project id and credentials from the environment if they are not specified.

- :cask-issue:`CDAP-14104` - The Google Cloud Dataproc provisioner will use internal IP addresses when CDAP is in the same network as the Google Cloud Dataproc cluster.

- :cask-issue:`CDAP-14168` - Added capability to always display current dataset schema in Field Level Lineage.

- :cask-issue:`CDAP-13886` - Improved error handling in Preparation.

- :cask-issue:`CDAP-14023` - Added a FileSink batch sink, FileMove action, and FileDelete action to replace their HDFS counterparts.

- :cask-issue:`CDAP-14097` - Added a configurable jvm option to kill CDAP process immediately on sandbox when an OutOfMemory error occurs.

- :cask-issue:`CDAP-14135` - Added better trace logging for dataset service.

- :cask-issue:`CDAP-14386` - Make Google Cloud Storage, Google BigQuery, and Google Cloud Spanner connection properties optional (project id, service account keyfile path, temporary Google Cloud Storage bucket).

- :cask-issue:`CDAP-14401` - Google Cloud Pub/Sub sink will try to create the topic if it does not exist while preparing for the run.

- :cask-issue:`CDAP-14475` - Added csv, tsv, delimited, json, and blob as formats to the S3 source and sink.

- :cask-issue:`CDAP-14321` - Added csv, tsv, delimited, json, and blob as formats to the File source.

- :cask-issue:`CDAP-9048` - Added a button on external sources and sinks to jump to the dataset detail page.

- :cask-issue:`CDAP-14040` - Added format and suppress query params to the program logs endpoint to match the program run logs endpoint.

- :cask-issue:`CDAP-14132` - Made all CDAP examples to be compatible with Spark 2.

- :cask-issue:`CDAP-14220` - Added worker and master disk size properties to the Google Cloud Dataproc provisioner.

- :cask-issue:`CDAP-14298` - Improved operational behavior of the dataset service.

- :cask-issue:`CDAP-14372` - Fixed wrangler transform to make directives optional. If none are given, the transform is a no-op.

- :cask-issue:`CDAP-14397` - Fixed Preparation to treat files wihtout extension as text files.

- :cask-issue:`CDAP-14398` - Limited the number of files showed in S3 and Google Cloud Storage browser to 1000.

- :cask-issue:`CDAP-14482` - Enhanced Google BigQuery sink to create dataset if the specified dataset does not exist.

- :cask-issue:`CDAP-14489` - Increased log levels for the CDAP Sandbox so that only CDAP classes are at debug level.


Bug Fixes
---------

- :cask-issue:`CDAP-14468` - Fixed the 'distinct' plugin to use a drop down for the list of fields and to have a button to get the output schema.

- :cask-issue:`CDAP-7444` - Ensured that destroy() is always called for MapReduce, even if initialize() fails.

- :cask-issue:`CDAP-13008` - Fixed a bug where Alert Publisher will not work if there is a space in the label.

- :cask-issue:`CDAP-13230` - Fixed a bug that caused Preparation to fail while parsing avro files.

- :cask-issue:`CDAP-13878` - Fixed a misleading error message about hbase classes in cloud runtimes.

- :cask-issue:`CDAP-13887` - Fixed a bug where the metric for failed profile program runs was not getting incremented when the run failed due to provisioning errors.

- :cask-issue:`CDAP-13894` - Fixed a bug where querying metrics by time series will be incorrect after a certain amount of time.

- :cask-issue:`CDAP-13959` - Fixed a bug where profile metrics is incorrect if an app is deleted.

- :cask-issue:`CDAP-13965` - Fixed a deprovisioning bug when cluster creation would fail.

- :cask-issue:`CDAP-13988` - Fixed an error where TMS publishing was retried indefinitely if the first attempt failed.

- :cask-issue:`CDAP-14076` - Fixed a race condition in MapReduce that can cause a deadlock.

- :cask-issue:`CDAP-14098` - Fixed a resource leak in preview feature.

- :cask-issue:`CDAP-14107` - Fixed a bug that would cause RDD versions of the dynamic scala spark plugins to fail.

- :cask-issue:`CDAP-14154` - Fixed a bug where profiles were getting applied to all program types instead of only workflows.

- :cask-issue:`CDAP-14203` - Fixed a race condition by ensuring that a program is started before starting runtime monitoring for it.

- :cask-issue:`CDAP-14211` - Fixed runs count for pipelines in UI to show correct number instead of limiting to 100.

- :cask-issue:`CDAP-14223` - Fixed an issue where Google Cloud Dataproc client was not being closed, resulting in verbose error logs.

- :cask-issue:`CDAP-14261` - Fixed a bug that could cause the provisioning state of stopped program runs to be corrupted.

- :cask-issue:`CDAP-14271` - Fixed a bug that caused Preparation to be unable to list buckets in a Google Cloud Storage connection in certain environments.

- :cask-issue:`CDAP-14303` - Fixed a bug where Google Cloud Dataproc provisioner is not able to provision a singlenode cluster.

- :cask-issue:`CDAP-14390` - Fixed a bug where Preparation could not read json or xml files on Google Cloud Storage.

- :cask-issue:`CDAP-14395` - Fixed Google Cloud Dataproc provisioner to use full API access scopes so that Google Cloud Spanner and Google Cloud Pub/Sub are accessible by default.

- :cask-issue:`CDAP-14435` - Fixed a bug where profile metrics is not deleted when a profile is deleted.


Deprecated and Removed Features
-------------------------------

- :cask-issue:`CDAP-14108` - Removed old and buggy dynamic spark plugins.

- :cask-issue:`CDAP-14456` - Dropped support for MapR 4.1.



`Release 5.0.0 <http://docs.cask.co/cdap/5.0.0/index.html>`__
=============================================================

Summary
-------

1. **Cloud Runtime**
    - Cloud Runtimes allow you to configure batch pipelines to run in a cloud environment.
    - Before the pipeline runs, a cluster is provisioned in the cloud. The pipeline is executed on that cluster, and the cluster is deleted after the run finishes.
    - Cloud Runtimes allow you to only use compute resources when you need them, enabling you to make better use of your resources.

2. **Metadata**
    - *Metadata Driven Processing*
       - Annotate metadata to custom entities such as fields in a dataset, partitions of a dataset, files in a fileset
       - Access metadata from a program or plugin at runtime to facilitate metadata driven processing
    - *Field Level Lineage*
       - APIs to register operations being performed on fields from a program or a pipeline plugin
       - Platform feature to compute field level lineage based on operations

3. **Analytics**
    - A simple, interactive, UI-driven approach to machine learning.
    - Lowers the bar for machine learning, allowing users of any level to understand their data and train models
      while preserving the switches and levers that advanced users might want to tweak.

4. **Operational Dashboard**
    - A real-time interactive interface that visualizes program run statistics
    - Reporting for comprehensive insights into program runs over large periods of time


New Features
------------

Cloud Runtime
.............

- :cask-issue:`CDAP-13089` - Added Cloud Runtimes, which allow users to assign profiles to batch pipelines that control what environment the pipeline will run in. For each program run, a cluster in a cloud environment can be created for just that run, allowing efficient use of resources.

- :cask-issue:`CDAP-13213` - Added a way for users to create compute profiles from UI to run programs in remote (cloud) environments using one of the available provisioners.

- :cask-issue:`CDAP-13206` - Allowed users to specify a compute profile in UI to run the pipelines in cloud environments. Compute profiles can be specified either while running a pipeline manually or via a time schedule or via a pipeline state based trigger.

- :cask-issue:`CDAP-13094` - Added a provisioner that allows users to run pipelines on Google Cloud Dataproc clusters.

- :cask-issue:`CDAP-13774` - Added a provisioner that can run pipelines on remote Apache Hadoop clusters

- :cask-issue:`CDAP-13709` - Added an Amazon Elastic MapReduce provisioner that can run pipelines on AWS EMR.

- :cask-issue:`CDAP-13380` - Added support for viewing logs in CDAP for programs executing using the Cloud Runtime.

- :cask-issue:`CDAP-13432` - Added metadata such has pipelines, schedules and triggers that are associated with profiles. Also added metrics such as the total number of runs of a pipeline using a profile.

- :cask-issue:`CDAP-13494` - Added the ability to disable and enable a profile

- :cask-issue:`CDAP-13276` - Added the capability to export or import compute profiles

- :cask-issue:`CDAP-13359` - Added the ability to set the default profile at namespace and instance levels.

Metadata
........

- :cask-issue:`CDAP-13260` - Added support for annotating metadata to custom entities. For example now a field in a dataset can be annotated with metadata.

- :cask-issue:`CDAP-13264` - Added programmatic APIs for users to register field level operations from programs and plugins.

- :cask-issue:`CDAP-13269` - Added REST APIs to retrieve the fields which were updated for a given dataset in a given time range, a summary of how those fields were computed, and details about operations which were responsible for updated those fields.

- :cask-issue:`CDAP-13511` - Added the ability to view Field Level Lineage for datasets.

Analytics
.........

- :cask-issue:`CDAP-13921` - Added CDAP Analytics as an interactive, UI-driver application that allows users to train machine learning models and use them in their pipelines to make predictions.


Operational Dashboard
.....................

- :cask-issue:`CDAP-12865` - Added a Dashboard for real-time monitoring of programs and pipelines

- :cask-issue:`CDAP-12901` - Added a UI to generate reports on programs and pipelines that ran over a period of time

- :cask-issue:`CDAP-13147` - Added feature to support Reports and Dashboard. Dashboard provides realtime status of program runs and future schedules. Reports is a tool for administrators to take a historical look at their applications program runs, statistics and performance


Other New Features
..................

Data Pipelines
^^^^^^^^^^^^^^

- :cask-issue:`CDAP-12839` - Added 'Error' and 'Alert' ports for plugins that support this functionality. To enable this functionality in your plugin, in addition to emitting alerts and errors from the plugin code, users have to set "emit-errors: true" and "emit-alerts: true" in their plugin json. Users can create connections from 'Error' port to Error Handlers plugins, and from 'Alert' port to Alert plugins

- :cask-issue:`CDAP-13045` - Added support for Apache Phoenix as a source in Data Pipelines.

- :cask-issue:`CDAP-13499` - Added support for Apache Phoenix database as a sink in Data Pipelines.

- :cask-issue:`CDAP-12944` - Added the ability to support macro behavior for all widget types

- :cask-issue:`CDAP-13057` - Added the ability to view all the concurrent runs of a pipeline

- :cask-issue:`CDAP-13006` - Added the ability to view the runtime arguments, logs and other details of a particular run of a pipeline.

- :cask-issue:`CDAP-13242` - Added UI support for Splitter plugins

Data Preparation
^^^^^^^^^^^^^^^^

- :cask-issue:`CDAP-13100` - Added a Google BigQuery connection for Data Preparation

- :cask-issue:`CDAP-12880` - Added a point-and-click interaction to change the data type of a column in the Data Preparation UI

Miscellaneous
^^^^^^^^^^^^^

- :cask-issue:`CDAP-13180` - Added a page to view and manage a namespace. Users can click on the current namespace card in the namespace dropdown to go the namespace's detail page. In this page, they can see entities and profiles created in this namespace, as well as preferences, mapping and security configurations for this namespace.

- :cask-issue:`CDAP-12951` - Added the ability to restart CDAP programs to make it resilient to YARN outages.

- :cask-issue:`CDAP-13242` - Implemented a new Administration page, with two tabs, Configuration and Management. In the Configuration tab, users can view and manage all namespaces, system preferences and system profiles. In the Management tab, users can get an overview of system services in CDAP and scale them.

Improvements
------------

- :cask-issue:`CDAP-13280` - Added Spark 2 support for Kafka realtime source

- :cask-issue:`CDAP-12727`, :cask-issue:`CDAP-13068` - Added support for CDH 5.13 and 5.14.

- :cask-issue:`CDAP-11805` - Added support for EMR 5.4 through 5.7

- :cask-issue:`CDAP-6308` - Upgraded CDAP Router to use Netty 4.1

- :cask-issue:`CDAP-13179` - Added support for automatically restarting long running program types (Service and Flow) upon application master process failure in YARN

- :cask-issue:`CDAP-12549` - Added support for specifying custom consumer configs in Kafka source

- :cask-issue:`CDAP-13143` - Added support for specifying recursive schemas

- :cask-issue:`CDAP-12275` - Added support to pass in YARN application ID in the logging context. This can help in correlating the ID of the program run in CDAP to the ID of the corresponding YARN application, thereby facilitating better debugging.

- :cask-issue:`CDAP-9080` - Added the ability to deploy plugin artifacts without requiring a parent artifact. Such plugins are available for use in any parent artifacts

- :cask-issue:`CDAP-12274` - Added the ability to import pipelines from the add entity modal (plus button)

- :cask-issue:`CDAP-11844` - Added the ability to save the runtime arguments of a pipeline as preferences, so that they do not have to be entered again.

- :cask-issue:`CDAP-12724` - Added the ability to specify dependencies to ScalaSparkCompute Action

- :cask-issue:`CDAP-12426` - Added the ability to update the keytab URI for namespace's impersonation configuration.

- :cask-issue:`CDAP-12279` - Added the ability to upload a User Defined Directive (UDD) using the plus button

- :cask-issue:`CDAP-12963` - Allowed CDAP user programs to talk to Kerberos enabled HiveServer2 in the cluster without using a keytab

- :cask-issue:`CDAP-11096` - Allowed users to configure the transaction isolation level in database plugins

- :cask-issue:`CDAP-13573` - Configured sandbox to have secure store APIs enabled by default

- :cask-issue:`CDAP-13411` - Improved robustness of unit test framework by fixing flaky tests

- :cask-issue:`CDAP-13405` - Increased default twill reserved memory from 300mb to 768mb in order to prevent YARN from killing containers in standard cluster setups.

- :cask-issue:`CDAP-13116` - Macro enabled all fields in the HTTP Callback plugin

- :cask-issue:`CDAP-12974` - Removed concurrent upgrades of HBase coprocessors since it could lead to regions getting stuck in transit.

- :cask-issue:`CDAP-13409` - Updated the CDAP sandbox to use Spark 2.1.0 as the default Spark version.

- :cask-issue:`CDAP-13157` - Improved the documentation for defining Apache Ranger policies for CDAP entities

- :cask-issue:`CDAP-12992` - Improved resiliency of router to zookeeper outages.

- :cask-issue:`CDAP-13756` - Improved the performance of metadata upgrade by adding a dataset cache.

- :cask-issue:`CDAP-7644` - Added CLI command to fetch service logs

- :cask-issue:`CDAP-12989` - Added rate limiting to router logs in the event of zookeeper outages

- :cask-issue:`CDAP-13759` - Renamed system metadata tables to v2.system.metadata_index.d, v2.system.metadata_index.i. and business metadata tables to v2.business.metadata_index.d, v2.business.metadata_index.i

- :cask-issue:`CDAP-6032` - Reduced CDAP Master's local storage usage by deleting temporary directories created for programs as soon as programs are launched on the cluster.

Bug Fixes
---------

- :cask-issue:`CDAP-13033` - Fixed a bug in TMS that prevented from correctly consuming multiple events emitted in the same transaction.

- :cask-issue:`CDAP-12875` - Fixed a bug that caused errors in the File source if it read parquet files that were not generated through Hadoop.

- :cask-issue:`CDAP-12693` - Fixed a bug that caused PySpark to fail to run with Spark 2 in local sandbox.

- :cask-issue:`CDAP-13296` - Fixed a bug that could cause the status of a running program to be falsely returned as stopped if the run happened to change state in the middle of calculating the program state. Also fixed a bug where the state for a suspended workflow was stopped instead of running.

- :cask-issue:`CDAP-7052` - Fixed a bug that prevented MapReduce AM logs from YARN to show the right URI.

- :cask-issue:`CDAP-12973` - Fixed a bug that prevented Spark jobs from running after CDAP upgrade due to caching of jars.

- :cask-issue:`CDAP-13026` - Fixed a bug that prevented a parquet snapshot source and sink to be used in the same pipeline

- :cask-issue:`CDAP-13593` - Fixed a bug that under some race condition, running a pipeline preview may cause the CDAP process to shut down.

- :cask-issue:`CDAP-12752` - Fixed a bug where a Spark program would fail to run when spark authentication is turned on

- :cask-issue:`CDAP-13123` - Fixed a bug where an ad-hoc exploration query on streams would fail in an impersonated namespace.

- :cask-issue:`CDAP-13463` - Fixed a bug where pipelines with conditions on different branches could not be deployed.

- :cask-issue:`CDAP-12743` - Fixed a bug where the Scala Spark compiler had missing classes from classloader, causing compilation failure

- :cask-issue:`CDAP-13372` - Fixed a bug where the upgrade tool did not upgrade the owner meta table

- :cask-issue:`CDAP-12647` - Fixed a bug with artifacts count, as when we we get artifact count from a namespace we also include system artifacts count causing the total artifact count to be much larger than real count.

- :cask-issue:`CDAP-13364` - Fixed a class loading issue and a schema mismatch issue in the whole-file-ingest plugin.

- :cask-issue:`CDAP-12970` - Fixed a dependency bug that could cause HBase region servers to deadlock during a cold start

- :cask-issue:`CDAP-12742` - Fixed an issue that caused pipeline failures if a Spark plugin tried to read or write a DataFrame using csv format.

- :cask-issue:`CDAP-13532` - Fixed an issue that prevented user runtime arguments from being used in CDAP programs

- :cask-issue:`CDAP-13281` - Fixed an issue where Spark 2.2 batch pipelines with HDFS sinks would fail with delegation token issue error

- :cask-issue:`CDAP-12731` - Fixed an issue with that caused hbase sink to fail when used alongside other sinks, using spark execution engine.

- :cask-issue:`CDAP-13002` - Fixed an issue with the retrieval of non-ASCII strings from Table datasets.

- :cask-issue:`CDAP-13040` - Fixed avro fileset plugins so that reserved hive keywords can be used as column names

- :cask-issue:`CDAP-13331` - Fixed macro enabled properties in plugin configuration to only have macro behavior if the entire value is a macro.

- :cask-issue:`CDAP-12988` - Fixed the logs REST API to return a valid json object when filters are specified

- :cask-issue:`CDAP-13110` - Fixes an issue where a dataset's class loader was closed before the dataset itself, preventing the dataset from closing properly.


Deprecated and Removed Features
-------------------------------

- :cask-issue:`CDAP-13721` - Deprecated the aggregation of metadata annotated with all the entities (application, programs, dataset, streams) associated in a run. From this release onwards metadata for program runs behaves like any other entity where a metadata can be directly annotated to it and retrieved from it. For backward compatibility, to achieve the new behavior an additional query parameter 'runAggregation' should be set to false while making the REST call to retrieve metadata of program runs.

- :cask-issue:`CDAP-8141` - Dropped support for CDH 5.1, 5.2, 5.3 and HDP 2.0, 2.1 due to security vulnerabilities identified in them

- :cask-issue:`CDAP-13493` - Removed HDFS, YARN, and HBase operational stats. These stats were not very useful, could generate confusing log warnings, and were confusing when used in conjunction with cloud profiles.

- :cask-issue:`CDAP-13720` - Removed analytics plugins such as decision tree, naive bayes and logistic regression from Hub. The new Analytics flow in the UI should be used as a substitute for this functionality.

- :cask-issue:`CDAP-12584` - Removed deprecated ``cdap sdk`` commands. Use ``cdap sandbox`` commands instead.

- :cask-issue:`CDAP-13680` - Removed deprecated ``cdap.sh`` and ``cdap-cli.sh`` scripts.  Use ``cdap sandbox`` or ``cdap cli`` instead.

- :cask-issue:`CDAP-11870` - Removed deprecated error datasets from pipelines. Error transforms should be used instead of error datasets, as they offer more functionality and flexibility.

- :cask-issue:`CDAP-13353` - Deprecated HDFS Sink. Use the File sink instead.

- :cask-issue:`CDAP-12692` - Removed deprecated stream size based schedules

- :cask-issue:`CDAP-13419` - Deprecated streams and flows. Use Apache Kafka as a replacement technology for streams and spark streaming as a replacement technology for flows. Streams and flows will be removed in 6.0 release.

- :cask-issue:`CDAP-5966` - Removed multiple deprecated programmatic and RESTful API's in CDAP.
    - Deprecated public APIs removed from the ``cdap-api`` module:
    	- Scheduling workflow using ``io.cdap.cdap.api.schedule.Schedule`` in ``AbstractApplication`` has been removed,
    	  use ``io.cdap.cdap.internal.schedule.ScheduleCreationSpec`` for scheduling workflow.
    	- Adding schedule using ``io.cdap.cdap.api.schedule.Schedule`` is removed in ``ApplicationConfigurer``, use
    	  ``io.cdap.cdap.internal.schedule.ScheduleCreationSpec`` for adding schedules.
    	- Deprecated methods ``getStreams``, ``getDatasetModules``
    	  and ``getDatasetSpecs`` have been removed from ``FlowletDefinition``.
    	- ``beforeSubmit`` and ``onFinish`` methods have been removed from ``Mapreduce`` and ``Spark`` interfaces, use
    	  ``ProgramLifecycle#initialize`` and ``ProgramLifecycle#destroy`` instead.
    	- ``RunConstraints``, ``ScheduleSpecification`` and ``Schedule`` classes in package
    	  ``io.cdap.cdap.api.schedule`` have been removed.
    	- ``WorkflowAction``, ``WorkflowActionConfigurer``, ``WorkflowActionSpecification``, ``AbstractWorkflowAction``
    	  have been removed from the package ``io.cdap.cdap.api.workflow``. Use ``CustomAction`` for workflows instead.
    	- ``WorkflowConfigurer#addAction(WorkflowAction action)`` has been removed, use
    	  ``addAction(CustomAction action)`` instead.
    	- ``MapReduceTaskContext#getInputName`` has been removed, use ``getInputContext`` instead.
    - The following deprecations have been removed from the ``cdap-proto`` module:
    	- ``ApplicationDetail#getArtifactVersion`` has been removed, use ``ApplicationDetail#getArtifact`` instead.
    	- ``getId()`` method has been removed in ``ApplicationRecord``, ``DatasetRecord``, ``ProgramLiveInfo`` and
    	  ``ProgramRecord``.
    	- ``Id`` class has been removed.
    	- ``ScheduleUpdateDetail`` has been removed, use ``ScheduleDetail`` instead.
    	- ``ScheduleType`` has been removed, use ``Trigger`` instead.
    	- Methods for getting ``ScheduleSpecification`` - ``toScheduleSpec()`` and
    	  ``toScheduleSpecs(List<ScheduleDetail> details)``, have been removed from ``ScheduleDetail``.
    	- Deprecated ``MetadataRecord`` class has been removed.
    - The following deprecations have been removed from the ``cdap-client`` module:
     	- Removed methods which were using the old ``io.cdap.cdap.proto.Id`` classes in ``ApplicationClient``,
     	  ``ArtifactClient``, ``ClientConfig``, ``DatsetClient``, ``DatasetModuleClient``, ``DatasetTypeClient``,
     	  ``LineageClient``, ``MetricsClient``, ``ProgramClient``, ``ScheduleClient``, ``ServiceClient``,
     	  ``StreamClient``, ``StreamViewClient`` and ``WorkflowClient``.
     	- Removed methods to add and update schedules using ``ScheduleInstanceConfiguration`` in ``ScheduleClient``,
     	  use methods accepting ``ScheduleDetail`` as parameter instead.
    - The REST API to get workflow status using ``current`` endpoint has been removed, use the workflow node state
      endpoint ``/nodes/state`` instead to get workflow status.


Known Issues
------------

- :cask-issue:`CDAP-13853` - Updating the compute profile to use to manually run a pipeline using the UI can remove the
  existing schedules and triggers of the pipeline.

- :cask-issue:`CDAP-13919` - The reports feature does not work with Apache Spark 2.0 currently. As a workaround,
  upgrade to use Spark version 2.1 or later to use reports.

- :cask-issue:`CDAP-13896` - Plugins that are not supported while running a pipeline using a cloud runtime throw
  unclear error messages at runtime.

- :cask-issue:`CDAP-13274` - While some built-in plugins have been updated to emit operations for capturing field level
  lineage, a number of them do not yet emit these operations.

- :cask-issue:`CDAP-13326` - Pipelines cannot propagate dynamic schemas at runtime.

- :cask-issue:`CDAP-13963` - Reading metadata is not supported when pipelines or programs run using a cloud runtime.

- :cask-issue:`CDAP-13971` - Creating a pipeline from Data Preparation when using an Apache Kafka plugin fails. As
  a workaround, after clicking the Create Pipeline button, manually update the schema of the Kafka plugin to set a
  single field named body as a non-nullable string.

- :cask-issue:`CDAP-13910` - Metadata for custom entities is not deleted if it's nearest known ancestor entity
  (parent) is deleted.


`Release 4.3.4 <http://docs.cask.co/cdap/4.3.4/index.html>`__
=============================================================

Improvements
------------

- :cask-issue:`CDAP-13116` - Macro enabled all fields in the HTTP Callback plugin

- :cask-issue:`CDAP-13119` - Optimized the planner to reduce the amount of temporary data required in certain types of mapreduce pipelines.

- :cask-issue:`CDAP-13122` - Minor optimization to reduce the number of mappers used to read intermediate data in mapreduce pipelines

- :cask-issue:`CDAP-13139` - Improves the schema generation for database sources.

- :cask-issue:`CDAP-13179` - Automatic restart of long running program types (Service and Flow) upon application master process failure in YARN

Bug Fixes
---------

- :cask-issue:`CDAP-12875` - Fixed a bug that caused errors in the File source if it read parquet files that were not generated through Hadoop.

- :cask-issue:`CDAP-13110` - Fixed an issue where a dataset's class loader was closed before the dataset itself, preventing the dataset from closing properly.

- :cask-issue:`CDAP-13120` - Fixed a bug that caused directories to be left around if a workflow used a partitioned fileset as a local dataset

- :cask-issue:`CDAP-13123` - Fixed a bug that caused a hive Explore query on Streams to not work.

- :cask-issue:`CDAP-13129` - Fixed a planner bug to ensure that sinks are never placed in two different mapreduce phases in the same pipeline.

- :cask-issue:`CDAP-13158` - Fixed a race condition when running multiple spark programs concurrently at a Workflow fork that can lead to workflow failure

- :cask-issue:`CDAP-13171` - Fixed an issue with creating a namespace if the namespace principal is not a member of the namespace home's group.

- :cask-issue:`CDAP-13191` - Fixed a bug that caused completed run records to be missed when storing run state, resulting in misleading log messages about ignoring killed states.

- :cask-issue:`CDAP-13192` - Fixed a bug in FileBatchSource that prevented ignoreFolders property from working with avro and parquet inputs

- :cask-issue:`CDAP-13205` - Fixed an issue where inconsistencies in the schedulestore caused scheduler service to keep exiting.

- :cask-issue:`CDAP-13217` - Fixed an issue that would cause changes in program state to be ignored if the program no longer existed, resulting in the run record corrector repeatedly failing to correct run records

- :cask-issue:`CDAP-13218` - Fixed the state of Workflow, MapReduce, and Spark program to be reflected correctly as KILLED state when user explicitly terminated the running program

- :cask-issue:`CDAP-13223` - Fixed directive syntaxes in point and click interactions for some date formats

`Release 4.3.3 <http://docs.cask.co/cdap/4.3.3/index.html>`__
=============================================================
Improvements
------------

- :cask-issue:`CDAP-12942` - GroupBy aggregator plugin fields are now macro enabled.

- :cask-issue:`CDAP-12963` - Allow CDAP user programs to talk to Kerberos enabled HiveServer2 in the cluster without using a keytab.

- :cask-issue:`CDAP-12974` - Removed concurrent upgrades of HBase coprocessors since it could lead to regions getting stuck in transit.

Bug Fixes
---------

- :cask-issue:`CDAP-7052` - Fixed a bug that prevented MapReduce AM logs from YARN to show the right URI.

- :cask-issue:`CDAP-7644` - Added CLI command to fetch service logs.

- :cask-issue:`CDAP-12774` - Increased the dataset changeset size and limit to integer max by default.

- :cask-issue:`CDAP-12900` - Fixed a bug where macro for output schema of a node was not saved when the user closed the node properties modal.

- :cask-issue:`CDAP-12930` - Fixed a bug where explore queries would fail against paths in HDFS encryption zones, for certain Hadoop distributions.

- :cask-issue:`CDAP-12945` - Fixed a bug where the old connection is not removed from the pipeline config when you move the connection's pointer to another node.

- :cask-issue:`CDAP-12946` - Fixed a bug in the pipeline planner where pipelines that used an action before multiple sources would either fail to deploy or deploy with an incorrect plan.

- :cask-issue:`CDAP-12970` - Fixed a dependency bug that could cause HBase region servers to deadlock during a cold start.

- :cask-issue:`CDAP-13002` - Fixed an issue with the retrieval of non-ASCII strings from Table datasets.

- :cask-issue:`CDAP-13021` - Messaging table coprocessor now gets upgraded when the underlying HBase version is changed without any change in the CDAP version.

- :cask-issue:`CDAP-13026` - Fixed a bug that prevented a parquet snapshot source and sink to be used in the same pipeline.

- :cask-issue:`CDAP-13033` - Fixed a bug in TMS that prevented correctly consuming multiple events emitted in the same transaction.

- :cask-issue:`CDAP-13037` - Make TransactionContext resilient against getTransactionAwareName() failures.

- :cask-issue:`CDAP-13040` - Fixed avro fileset plugins so that reserved hive keywords can be used as column names.

`Release 4.3.2 <http://docs.cask.co/cdap/4.3.2/index.html>`__
=============================================================
New Features
------------

- :cask-issue:`CDAP-12771` - Added GCS connection to Data Prep.

- :cask-issue:`CDAP-12018` - Added S3 connection to Data Prep.


Improvements
------------

- :cask-issue:`CDAP-11805` - Added support for EMR 5.4 through 5.7.

- :cask-issue:`CDAP-12727` - Added support for CDH 5.13.0.


Bug Fixes
---------

- :cask-issue:`CCDAP-6032` - Minimize master's local storage usage by deleting the temporary directories created on the cdap-master for programs as soon as programs are launched on the cluster.

- :cask-issue:`CDAP-12682` - Fixed an issue where UI was looking for the wrong property for SSL port.

- :cask-issue:`CDAP-12693` - Fixed a bug that causes PySpark to fail to run with Spark 2 in local sandbox.

- :cask-issue:`CDAP-12701` - Fixed a bug that causes deployment of pipelines with condition plugins to fail on Apache Ambari clusters.

- :cask-issue:`CDAP-12731` - Fixed an issue that caused HBase Sink to fail when used alongside other sinks, using the Spark execution engine.

- :cask-issue:`CDAP-12743` - Fixed a bug where the Scala Spark compiler has missing classes from Classloader, causing compilation failure.

- :cask-issue:`CDAP-12752` - Fixed a bug where Spark programs failed to run when Spark authentication is turned on.

- :cask-issue:`CDAP-12769` - Fixed an issue with running the dynamic Scala Spark plugin on Windows. Directory which is used to store the compiled scala classes now contains '.' as a separator instead of ':' which was causing failure on Windows machines.

- :cask-issue:`CDAP-12843` - Fixed an issue that prevented auto-fill of schema for Datasets created by an ORC sink plugin.

`Release 4.3.1 <http://docs.cask.co/cdap/4.3.1/index.html>`__
=============================================================

New Features
------------

- :cask-issue:`CDAP-12592` - Adds new visualization tool to give insights about data prepped up in data preparation tool.

- :cask-issue:`CDAP-12620` - Adds a way to trigger invalid transaction pruning via a REST endpoint.

- :cask-issue:`CDAP-12595` - Adds UI to make HTTP request in CDAP.

Improvements
------------

- :cask-issue:`CDAP-12598` - Added a downgrade command to the pipeline upgrade tool, allowing users to downgrade pipelines to a previous version.

- :cask-issue:`CDAP-12541` - Improved memory usage of data pipeline with joiner in mapreduce execution engine.

- :cask-issue:`CDAP-12176` - Added ability to select/clear all the checkboxes for Provided runtime arguments

- :cask-issue:`CDAP-12646` - Fixed a performance issue with the run record corrector

- :cask-issue:`CDAP-12380` - Added a capability to configure program containers memory settings through runtime arguments and preferences

- :cask-issue:`CDAP-8499` - Applies the extra jvm options configuration to all task containers in MapReduce

- :cask-issue:`CDAP-12546` - Fixed a classloader leakage issue when PySpark is used in local sandbox

- :cask-issue:`CDAP-12593` - Ability to list the datasets based on the set of dataset properties

Bug Fixes
---------

- :cask-issue:`CDAP-12645` - MapReduce Task-related metrics will be emitted from individual tasks instead of MapReduce driver.

- :cask-issue:`CDAP-12628` - Fixed the filter if missing flow in the UI to also apply on null values in addition to empty values.

- :cask-issue:`CDAP-12612` - Fixed the fill-null-or-empty directive to allow spaces in the default value

- :cask-issue:`CDAP-12588` - Fixed a bug that authorization cannot be turned on if kerberos is disabled

- :cask-issue:`CDAP-12578` - Fixed an issue that caused the pipeline upgrade tool to upgrade pipelines in a way that would cause UI failures when the upgraded pipeline is viewed.

- :cask-issue:`CDAP-12577` - Spark compat directories in the system artifact directory will now be automatically checked, regardless of whether they are explicitly set in app.artifacts.dir.

- :cask-issue:`CDAP-12570` - Added option to enable/disable emitting program metrics and option to include or skip task level information in metrics  context. This option can be used with scoping at program and program-type level similar to setting system resources with scoping.

- :cask-issue:`CDAP-12569` - Improved error messaging when there is an error while in publishing metrics in MetricsCollection service.

- :cask-issue:`CDAP-12567` - Fixed a bug that CDAP is not able to clean up and aggregate on streams in an authorization enabled environment

- :cask-issue:`CDAP-12559` - Fixed log message format to include class name and line number when logged in master log

- :cask-issue:`CDAP-12526` - Adds various improvements to the transaction system, including the ability to limit the size of a transaction's change set; better insights into the cause of transaction conflicts; improved concurrency when writing to the transaction log;  better handling of border conditions during invalid transaction pruning; and ease of use for the transaction pruning diagnostic tool.

- :cask-issue:`CDAP-12495` - Fix the units for YARN memory stats on Administration UI page.

- :cask-issue:`CDAP-12482` - Fixed a bug where the app detail contains entity information that the user does not have any privilege on

- :cask-issue:`CDAP-12476` - Fixed preview results for pipelines with condition stages

- :cask-issue:`CDAP-12457` - Fixed a bug that caused failures for Hive queries using MR execution engine in CM 5.12 clusters.

- :cask-issue:`CDAP-12454` - Fixes an issue where transaction coprocessors could sometimes not access their configuration.

- :cask-issue:`CDAP-12451` - UI: Add ability to view payload configuration of pipeline triggers

- :cask-issue:`CDAP-12441` - Fixed a bug that the cache timeout was not changed with the value of ``security.authorization.cache.ttl.secs``

- :cask-issue:`CDAP-12415` - Fixed an issue with not able to use HiveContext in Spark

- :cask-issue:`CDAP-12387` - Added the authorization policy for adding/deleting schedules

- :cask-issue:`CDAP-12377` - Fixes an issue where the transaction service could hang during shutdown.

- :cask-issue:`CDAP-12333` - Fixed issue where loaded data was not consistently rendering when navigating to Data Preparation from other parts of CDAP.

- :cask-issue:`CDAP-12314` - Improves the performance of HBase operations when there are many invalid transactions.

- :cask-issue:`CDAP-12240` - Improves a previously misleading log message.

- :cask-issue:`CDAP-7651` - Fixed an issue that hive query may failed if the configuration has too many variables substitution.

- :cask-issue:`CDAP-7243` - Added mechanism to clean up local dataset if the workflows creating them are killed

- :cask-issue:`CDAP-7049` - Improved the error message in the case that a kerberos principal is deleted or keytab is invalid, during impersonation.


`Release 4.3.0 <http://docs.cask.co/cdap/4.3.0/index.html>`__
=============================================================

Summary
-------

1. **Data Pipelines:**
	- Support for conditional execution of parts of a pipeline
	- Ability for pipelines to trigger other pipelines for cross-team, cross-pipeline inter-connectivity, and to build complex interconnected pipelines.
	- Improved pipeline studio with redesigned nodes, undo/redo capability, metrics
	- Automated upgrade of pipelines to newer CDAP versions
	- Custom icons and labels for pipeline plugins
	- Operational insights into pipelines

2. **Data Preparation:**
	- Support for User Defined Directives (UDD), so users can write their own custom directives for cleansing/preparing data.
	- Restricting Directive Usage and ability to alias Directives for your IT Administrators to control directive access

3. **Governance & Security:**
	- Standardized authorization model
	- Apache Ranger Integration for authorization of CDAP entities

4. **Enhanced support for Apache Spark:**
	- PySpark Support so data scientists can develop their Spark logic in Python, while still taking advantage of enterprise integration capabilities of CDAP
	- Spark Dataframe Support so Spark developers can access CDAP datasets as Spark DataFrames

5. **New Frameworks and Tools:**
	- Microservices for real-time IoT use cases.
	- Distributed Rules Engine - for Business Analysts to effectively manage rules for data transformation and data policy

New Features
------------

Data Pipelines Enhancements
---------------------------

- :cask-issue:`CDAP-12033` - Added a new splitter transform plugin type that can send output to different ports. Also added a union splitter transform that will send records to different ports depending on which type in the union it is and a splitter transform that splits records based on whether the specified field is null.

- :cask-issue:`CDAP-12034` - Added a way for pipeline plugins to emit alerts, and a new AlertPublisher plugin type that publishes those alerts. Added a plugin that publishes alerts to CDAP TMS and an Apache Kafka Alert Publisher plugin to publish alerts to a Kafka topic.

- :cask-issue:`CDAP-12108` - Batch data pipelines now support condition plugin types which can control the flow of execution of the pipeline. Condition plugins in the pipeline have access to the stage statistics such as number of input records, number of output records, number of error records generated from the stages which executed prior to the condition node. Also implemented Apache Commons JEXL based condition plugin which is available by default for the batch data pipelines.

- :cask-issue:`CDAP-12167` - Plugin ``prepareRun`` and ``onFinish`` methods now run in a separate transaction per plugin so that pipelines with many plugins will not timeout.

- :cask-issue:`CDAP-12191` - All pipeline plugins now have access to the pipeline namespace and name through their context object.

- :cask-issue:`CDAP-9107` - Added a feature that allows undoing and redoing of actions in pipeline Studio.

- :cask-issue:`CDAP-12057` - Made pipeline nodes bigger to show the version and metrics on the node.

- :cask-issue:`CDAP-12077` - Revamped pipeline connections, to allow dropping a connection anywhere on the node, and allow selecting and deleting multiple connections using the Delete key.

- :cask-issue:`CDAP-10619` - Added an automated UI flow for users to upgrade pipelines to newer CDAP versions.

- :cask-issue:`CDAP-11889` - Added visualization for pipeline in UI. This helps visualizing runs, logs/warnings and data flowing through each node for each run in the pipeline.

- :cask-issue:`CDAP-12111` - Added support for plugins of plugins. This allows the parent plugin to expose some APIs that its own plugins will implement and extend.

- :cask-issue:`CDAP-12114` - Added ability to support custom label and custom icons for pipeline plugins.

- :cask-issue:`CDAP-10974` - BatchSource, BatchSink, BatchAggregator, BatchJoiner, and Transform plugins now have a way to get SettableArguments when preparing a run, which allows them to set arguments for the rest of the pipeline.

- :cask-issue:`CDAP-10653` - Runtime arguments are now available to the script plugins such as Javascript and Python via the Context object.

- :cask-issue:`CDAP-12472` - Added a method to PluginContext that will return macro evaluated plugin properties.

- :cask-issue:`CDAP-12094` - Enhanced add field transform plugin to add multiple fields

Triggers
--------

- :cask-issue:`CDAP-11912` - Added capabilities to trigger programs and data pipelines based on status of other programs and data pipelines.

- :cask-issue:`CDAP-12382` - Added the capability to use plugin properties and runtime arguments from the triggering data pipeline as runtime arguments in the triggered data pipeline.

- :cask-issue:`CDAP-12232` - Added composite AND and OR trigger.

Data Preparation Enhancements
-----------------------------

- :cask-issue:`CDAP-11618` - Added the ability for users to connect Data Preparation to their existing data in Apache Kafka.

- :cask-issue:`CDAP-12092` - Added point and click interaction for performing various calculations on data in Data Prep.

- :cask-issue:`CDAP-12118` - Added point and click interaction for applying custom transformations in Data Prep.

- :cask-issue:`CDAP-9530` - Added point and click interaction to mask column data.

- :cask-issue:`CDAP-9532` - Added point and click interaction to encode/decode column data

- :cask-issue:`CDAP-11869` - Added point and click interaction to parse Avro and Excel files.

- :cask-issue:`CDAP-11977` - Added point and click interaction for replacing column names in bulk.

- :cask-issue:`CDAP-12091` - Added point and click interaction for defining and incrementing variable.

Spark Enhancements
------------------

- :cask-issue:`CDAP-4871` - Added capabilities to run PySpark programs in CDAP.

Governance and Security Enhancements
------------------------------------

- :cask-issue:`CDAP-12134` - Implemented the new authorization model for CDAP. The old authorization model is no longer supported.

- :cask-issue:`CDAP-12317` - Added a new configuration ``security.authorization.extension.jar.path`` in cdap-site.xml which can be used to add extra classpath and is avalible to cdap security extensions

- :cask-issue:`CDAP-12100` - Removed automatic grant/revoke privileges on CDAP entity creation/deletion.

- :cask-issue:`CDAP-12367` - Added support for authorization on Kerberos principal for impersonation.

- :cask-issue:`CDAP-11839` - Modified the authorization model so that read/write on an entity will not depend on its parent.

- :cask-issue:`CDAP-12135` - Deprecated ``createFilter()`` and added a new ``isVisible`` API in AuthorzationEnforcer. Deprecated grant/revoke APIs for EntityId and added new one for Authorizable which support wildcard privileges

- :cask-issue:`CDAP-12283` - Removed version for artifacts for authorization policy to be consistent with applications. From 4.3 onwards CDAP does not support policies on artifact/application version.

Other New Features
------------------

- :cask-issue:`CDAP-11940` - Added a wizard to allow configuring and deploying microservices in UI.

- :cask-issue:`CDAP-6329` - Enabled GC logging for CDAP services.

- :cask-issue:`CDAP-11448` - Added support for HDInsight 3.6.

- :cask-issue:`CDAP-4874` - CSD now performs a version compatibility check with the active CDAP Parcel

- :cask-issue:`CDAP-12348` - Added live migration of metrics tables from pre 4.3 tables to 4.3 salted metrics tables.

- :cask-issue:`CDAP-12017` - Added capability to salt the row key of the metrics tables so that writes are evenly distributed and there is no region hot spotting

- :cask-issue:`CDAP-12068` - Added a REST API to check the status of metrics processor. We can view the topic level processing stats using this endpoint.

- :cask-issue:`CDAP-12070` - Added option to disable/enable metrics for a program through runtime arguments or preferences. This feature can also be used system wide by enabling/disabling metrics in cdap-site.xml

- :cask-issue:`CDAP-12290` - Added global "CDAP" config to enable/disable metrics emission from user programs.By default metrics is enabled.

- :cask-issue:`CDAP-1952` - DatasetOutputCommiter's methods are now executed in the MapReduce ApplicationMaster, within OutputCommitter's commitJob/abortJob methods. The MapReduceContext.addOutput(Output.of(String, OutputFormatProvider)) API can no longer be used to add OutputFormatProviders that also implement the DatasetOutputCommitter interface.

- :cask-issue:`CDAP-12084` - Allow appending to (or overwriting) a PartitionedFileSet's partitions when using DynamicPartitioner APIs. Introduced a PartitionedFileSet.setMetadata API which now allows modifying partitions' metadata.

- :cask-issue:`CDAP-12085` - Exposed a programmatic API to leverage Hive's functionality to concatenate a partition of a PartitionedFileSet.

- :cask-issue:`CDAP-12378` - Workflow now allows adding configurable conditions with the lifecycle methods.

- :cask-issue:`CDAP-8629` - Allow programs to have concurrent runs in integration test cases.

Bug Fixes
---------

- :cask-issue:`CDAP-12103` - Removed deprecated cdap-etl-realtime artifact.

- :cask-issue:`CDAP-12123` - Removed deprecated deprecated cdap-etl-batch jar from packaging.

- :cask-issue:`CDAP-9150` - Allowed user to override the InputFormat class and OutputFormat class of a FileSet at runtime.

- :cask-issue:`CDAP-12285` - Fixed an issue with the order of HBase compatibility libraries in the class path.

- :cask-issue:`CDAP-9125` - Fixed an issue where CDAP Sentry Integration did not rely on every user having their own individual group.

- :cask-issue:`CDAP-11095` - Added support for a description field in a pipeline config that will be used as the application's description if set.

- :cask-issue:`CDAP-12020` - Reuse network connections for TMS client.

- :cask-issue:`CDAP-12143` - Removes the existing hierarchal authorization model from CDAP

- :cask-issue:`CDAP-12226` - Added an optional delimiter property to the HDFS sink to allow users to configure the delimiter used to separate record fields.

- :cask-issue:`CDAP-12298` - Individual system service status API no longer has to go through CDAP master.

- :cask-issue:`CDAP-9953` - Removed dataset usage in the Hive source and sink, which allows it to work in Spark and fixes a race condition that could cause pipelines to fail with a transaction conflict exception.

- :cask-issue:`CDAP-10228` - Sinks in streaming pipelines no longer have their ``prepareRun`` and ``onFinish`` methods called if the RDD for that batch is empty

- :cask-issue:`CDAP-11704` - Fixed CDAP to work with and publish to YARN Timeline Server in a secure environment.

- :cask-issue:`CDAP-11783` - HBaseDDLExecutor implementation is now localized to the containers without adding it in the container classpath.

- :cask-issue:`CDAP-11800` - Fixed a bug that the stream client gave wrong error message when the authorization check failed for stream read.

- :cask-issue:`CDAP-11880` - Fixed a bug that caused pipelines and other programs to not create datasets at runtime with correct impersonated user.

- :cask-issue:`CDAP-11944` - Removed non-configurable properties from CSD/Ambari

- :cask-issue:`CDAP-11948` - Fixed a bug where committed data could be removed during HBase table flush or compaction.

- :cask-issue:`CDAP-11955` - Fixed a bug where sometimes wrong user was used in explore, which resulted in the failure of deleting namespace.

- :cask-issue:`CDAP-12054` - Fixed PartitionedFileSet to work with CombineFileInputFormat, as input to a batch job.

- :cask-issue:`CDAP-12122` - Fixed a bug in the pipeline planner that caused some pipelines to fail to deploy with a NoSuchElementException

- :cask-issue:`CDAP-12125` - Fixed a bug in MapReduce pipeline timing metrics, where time for a stage could include time spent in other stages.

- :cask-issue:`CDAP-12130` - Fixed an issue that was causing send-to-directive to fail on derived columns in Data Prep.

- :cask-issue:`CDAP-12161` - Fixed a bug in StructuredRecord where a union of null and at least two other types could not be set to a null value.

- :cask-issue:`CDAP-12170` - Fixed a bug where committed files of a PartitionedFileSet could be removed during transaction rollback in the case PartitionOutput#addPartition was called for a partition that already existed. With this fix, PartitionedFileSet#getPartitionOutput should now only be called within a transaction.

- :cask-issue:`CDAP-12193` - Fixed a bug in some MapReduce pipelines that could cause duplicate reads if sources are not properly merged into the same MapReduce.

- :cask-issue:`CDAP-12199` - Fixed a bug that made local datasets inaccessible in a Workflow's initialize and destroy methods.

- :cask-issue:`CDAP-12253` - Fixed a bug where the file batch source was always using a default schema instead of the actual output schema.

- :cask-issue:`CDAP-12269` - Fixed a bug that prevented pipelines from being published when plugin artifact versions were not specified

- :cask-issue:`CDAP-12284` - Fixed a packaging bug that caused debian packages to include the wrong cdap-data-pipeline and cdap-data-streams artifacts for spark2.

- :cask-issue:`CDAP-12351` - Fixes an issue where truncating a file set did not preserve its base directory's ownership and permissions.

- :cask-issue:`CDAP-12360` - Fixed an issue where certain excessive logging could cause a deadlock in CDAP master.

- :cask-issue:`CDAP-12371` - In order to execute Hive queries using MR execution engine in CM 5.12 cluster, the 'yarn.app.mapreduce.am.staging-dir' property needs to be set to '/user' in the YARN Configuration Safety Value in Cloudera Manager.


`Release 4.2.0 <http://docs.cask.co/cdap/4.2.0/index.html>`__
=============================================================

Summary
-------

1. **Spark Enhancements:** Added suppport for Apache Spark 2.x. Users have an option to configure CDAP to use Spark 1.x
or Spark 2.x on their cluster. Also added capability to run interactive Spark code within CDAP.

2. **Enhanced Data Preparation:** Added capabilities in data preparation to connect to the File System (Local and
HDFS) and relational databases, browse and select their existing data, and import into Data Preparation for cleansing,
preparing and transforming.

3. **Event Driven Schedules:** Added capabilities to start CDAP programs based on data availability of partitions of
data in HDFS and pose run contraints to intelligently orchestrate CDAP Workflows.

New Features
------------

Spark Enhancements
------------------

- :cask-issue:`CDAP-7875` - Added support for Spark 2.x. In environments where multiple Spark versions exist, CDAP must be configured to use one or the other

- :cask-issue:`CDAP-11409`- Enable capabilities to run interactive Spark code within CDAP

- :cask-issue:`CDAP-11410` - Added capabilities to run arbitrary Spark code in CDAP Pipelines

- :cask-issue:`CDAP-11411` - Enhancements to speed up launching Spark programs


Enhanced Data Preparation
-------------------------

- :cask-issue:`CDAP-9290` - Adds File System Browser Component to browse Local and HDFS File System from Data Preparation

- :cask-issue:`CDAP-9517` - Adds Data Quality information to Data Preparation table. Currently, it shows the completeness of each column

- :cask-issue:`CDAP-9524` - Added point-and-click interactions for applying directives such as parsing, splitting, find and replace, filling null or empty rows, copying and deleting columns in Data Preparation. They can be invoked by using the dropdown menu for each column

- :cask-issue:`CDAP-11333` - Added point-and-click interaction for cleansing column names

- :cask-issue:`CDAP-11334` - Added a point-and-click interaction to set all column names in Data Preparation

- :cask-issue:`CDAP-11424` - Added the ability to ingest data one-tim from Data Preparation to a CDAP Dataset

- :cask-issue:`CDAP-9556` -  Added macro support for Data Preparation directives


Event Driven Schedules
----------------------
- :cask-issue:`CDAP-7593` - Introduces a new, event-driven scheduling system that can start programs based on data availability in HDFS partitions

- :cask-issue:`CDAP-11338` - Allow users to configure constraints for schedules, such as duration since last run and allowed time range for program execution



Other New Features
------------------
- :cask-issue:`CDAP-11498` - Added capability for CDAP Services to dynamically list available artifacts and dynamically load artifacts

- :cask-issue:`CDAP-7873` - Added support for EMR 5.0 - 5.3

- :cask-issue:`CDAP-11486` - Added the ability for Data Preparation to handle byte arrays of data for processing binary data

- :cask-issue:`CDAP-11422` - Added an API to Spark Streaming sources to provide number of streams being used by a streaming source

- :cask-issue:`CDAP-11681` - Users can now upload, view, and use plugins of type 'sparksink' in Studio.

- :cask-issue:`CDAP-8668` - Modified the log viewer to only show ERROR, WARN, and INFO levels of logs by default, instead of all logs as previously


Bug fixes
---------
- :cask-issue:`CDAP-8289` - Fix a bug where the log level was always set to INFO at the root logger

- :cask-issue:`CDAP-7727` - Fix a bug where extra characters after an artifact version range were being ignored instead of being recognized as invalid

- :cask-issue:`CDAP-7884` - Fixed a bug where users could not read from real Datasets while previewing CDAP Pipelines

- :cask-issue:`CDAP-9422` - Fixed a bug that prevented users from adding extra classpath to Apache Spark drivers and executors

- :cask-issue:`CDAP-9456` - Fixed a bug where impersonated workflow was not creating local datasets with the correct impersonated user

- :cask-issue:`CDAP-11417` - Fixed a bug in Parquet and Avro File sinks that would cause them to fail if they received ByteBuffers instead of byte arrays.

- :cask-issue:`CDAP-11558` - Fixed a bug where writes could only succeed in one MongoDB sink even when multiple MongoDB sinks were present in a pipeline

- :cask-issue:`CDAP-11577` - Fixed a thread leakage bug in Spark (SPARK-20935) after Spark Streaming program completed

- :cask-issue:`CDAP-11588` - Fixed a bug in TMS where fetching from the payload table raised an exception if the fetch had an empty result

- :cask-issue:`CDAP-11643` - Fixed a bug in the Purchase example that could cause purchases to overwrite each other

- :cask-issue:`CDAP-11651` - Fixed a bug that prevented from using logback.xml in Apache Spark Streaming programs.

- :cask-issue:`CDAP-9284` -  Fixed an issue where pipeline metrics were not showing up in pipelines with a large number of nodes

- :cask-issue:`CDAP-11795` - Fixed an issue with retrieving workflow state if it contained an exception without a message

- :cask-issue:`CDAP-11445` - Fixed an issue with the CDAP Ambari service definition where the "cdap" headless user was not unique to the cluster

- :cask-issue:`CDAP-4887` - Fixed the CDAP Upgrade tool to not fail when encountering a non-CDAP table that follows the CDAP naming convention

- :cask-issue:`CDAP-5067` - Fixed an issue where the driver process of a CDAP Workflow was getting restarted when it ran out of memory, causing the Workflow to be executed again from the start node

- :cask-issue:`CDAP-7429` - Fixed an issue with the detection of Apache Spark on HDP 2.5 and above, which caused excess noise on the console

- :cask-issue:`CDAP-8888` - Fixed an issue with the YARN container allocation logic so that the correct container size is used.

- :cask-issue:`CDAP-8911` - Fixed the stream container to terminate cleanly and cleaned up the CDAP Master's Apache Twill JAR files after master shutdown

- :cask-issue:`CDAP-8918` - Fixed an issue where redeployment of an application with a deleted schedule would fail

- :cask-issue:`CDAP-8961` - Fixed warnings about /opt/cdap/master/artifacts not being a directory in unit tests

- :cask-issue:`CDAP-9026` - Fixed an issue due to which CDAP entity roles were not cleanup when the entity was deleted

- :cask-issue:`CDAP-9378` - Fixed an issue where cdap-security.xml was not written under Ambari unless security.enabled in cdap-site.xml was set to true

- :cask-issue:`CDAP-10475` - Fixed the Azure Blob Store source to work with Avro and Parquet formats

- :cask-issue:`CDAP-11384` - Fixed the Azure Blob Store source to work with CDAP FileSets

- :cask-issue:`CDAP-11557` - Fixed the "value is" filter in the Data Preparation UI

- :cask-issue:`CDAP-11815` - Fixed impersonation while upgrading datasets in the Upgrade tool

Deprecations
------------
- :cask-issue:`CDAP-8327` - Add property "metrics.processor.queue.size" with default value 20000 to limit the maximum size of a queue where metrics processor temporarily stores newly fetched metrics in memory before persisting them. Added property "metrics.processor.max.delay.ms" with default value 3000 milliseconds to specify the maximum delay allowed between the latest metrics timestamp and the time when it is processed. The larger this property is, Metrics Processor gets to sleep more often between fetching each batch of metrics but the delay between metrics emission and processing also increases. Deprecated the property "metrics.messaging.fetcher.limit"


`Release 4.1.1 <http://docs.cask.co/cdap/4.1.1/index.html>`__
=============================================================

Summary
-------

1. **Data Preparation:** Point-and-click interactions and integration with the rest of CDAP
   including |---| but not limited to |---| namespaces, security, and pipelines.

2. **Upgrade:** Significant reduction in downtime during CDAP upgrades, by removing some data
   migration and doing required migration in the background after CDAP starts up.

3. **Pipeline Previews**: Added logs, better error messaging, ability to read from existing
   datasets, and a better stop experience.

4. **Logs**: Added a condensed view of logs for CDAP pipelines and programs that does not
   include logs emitted by the CDAP platform and libraries. The condensed view only
   contains lifecycle logs, logs emitted by the program or pipeline, and errors.

5. **Schedules:** Added the ability to update schedules without redeploying the application.

New Features
------------

Data Preparation
................
- :cask-issue:`CDAP-9235` - Users can now interact with and manage multiple workspaces in
  Data Preparation.

- :cask-issue:`WRANGLER-77` - Added point-and-click interactions for applying directives
  such as parsing, splitting, find and replace, filling null or empty rows, copying and
  deleting columns in Data Preparation. They can be invoked by using the dropdown menu for
  each column.

Logs
....
- :cask-issue:`CDAP-9117` - Added option to the log viewer to only show "user" condensed logs.

- :cask-issue:`HYDRATOR-1316` - Logs for previews of CDAP pipelines are now available in
  the CDAP UI via the *Logs* button in Preview mode.

Schedules
.........
- :cask-issue:`CDAP-8902` - Added support for adding, deleting, updating, and retrieving
  workflow schedules.

Other New Features
..................
- :cask-issue:`CDAP-8872` - Upgraded Apache Tephra dependency to the 0.11.0-incubating
  version.

- :cask-issue:`CDAP-9141`, :cask-issue:`HYDRATOR-1453` - Users can now deploy CDAP
  pipelines with a single action plugin. This feature can be used to run external Apache
  Spark programs as CDAP pipelines.

  Added a *sparkprogram* plugin type that can be used to run arbitrary Spark code at the
  beginning or end of a pipeline. An external Spark program can be added by clicking the
  "plus" ("+") button in the CDAP UI, choosing *Library*, and specifying *sparkprogram* as
  the type. It is then available as an Action plugin in the CDAP Studio.

- :cask-issue:`CDAP-9250` - Added support for HDP 2.6.

- :cask-issue:`CDAP-9281` - Added support for CDH 5.11.0.

- :cask-issue:`CDAP-9311` - Added support that allows plugin developers to integrate with
  CDAP services by exposing CDAP service discovery capabilities in the plugin context.

Improvements
------------

Upgrade
.......
- :cask-issue:`CDAP-9278` - Added the running of HBase coprocessor upgrades concurrently
  on CDAP Datasets.

- :cask-issue:`CDAP-9282`, :cask-issue:`CDAP-9283` - Improved the CDAP upgrade process to
  minimize the downtime needed to upgrade, by performing data migration in the background.

Pipeline Previews
.................
- :cask-issue:`CDAP-9017` - Simplified the status, next runtime of pipelines, total number
  of running pipelines, and drafts in the pipeline list view UI.

Schedules
.........
- :cask-issue:`CDAP-8942` - Allow administrators to enable or disable updating schedules
  using the property "app.deploy.update.schedules" in cdap-site.xml. Users can override this
  to enable or disable updating schedules during deployment of an application using the same
  property specified in the configuration of the application.

Other Improvements
..................
- :cask-issue:`CDAP-7731` - Added fetch size and transaction flush interval configurations
  to the Kafka Consumer Flowlet.

- :cask-issue:`CDAP-8430` - Users can now see a contextual message with appropriate
  call(s) to action when no entities are found on the Overview page.

- :cask-issue:`CDAP-8990` - Added new configurations to control the YARN application
  master container memory size, maximum heap memory size, and maximum non-heap memory size:
  ``twill.java.heap.memory.ratio``, ``twill.yarn.am.memory.mb``, and
  ``twill.yarn.am.reserved.memory.mb``.

- :cask-issue:`CDAP-9003` - Increased the default memory allocation for the CDAP Explore service
  container to 2048MB.

- :cask-issue:`CDAP-9027` - Users can now grant and revoke privileges for UNIX groups and
  users when using Apache Sentry as the authorization extension for CDAP.

- :cask-issue:`CDAP-9077` - Added a "cdap apply-pack [pack]" command to the "cdap" script
  that allows for upgrading of individual CDAP components.

Bug Fixes
---------

Upgrade
.......
- :cask-issue:`CDAP-9185` - Fixed an issue with the pipeline upgrade tool that caused it
  to skip CDAP 4.0.x pipelines.

Pipeline Previews
.................
- :cask-issue:`CDAP-7884` - Fixed a bug that preview cannot read from datasets in real
  space.

- :cask-issue:`CDAP-8013` - When previewing a pipeline in the CDAP Studio, disabled all
  writes to sinks. Incoming data to sinks can be viewed in the preview tab of the sink, but
  is not written to the sink.

- :cask-issue:`CDAP-9333` - Fixed an issue where preview of CDAP pipelines did not show
  data for successful stages if a particular stage failed.

Logs
....
- :cask-issue:`CDAP-7138` - Fixed a problem that caused duplicate logs to show up for a
  running pipeline.

- :cask-issue:`CDAP-9248` - Fixed bug where the "Total Messages/Errors/Warnings" at the
  top of logviewer was showing incorrect values.

Schedules
.........
- :cask-issue:`CDAP-8918` - Fixed an issue where redeployment of an application with a
  deleted schedule would fail.

Other Bug Fixes
...............
- :cask-issue:`CDAP-4213` - Removed the requirement of being an admin to run the CDAP
  startup script for Windows.

- :cask-issue:`CDAP-5715` - Made Plugin Endpoint invocation more robust. If a plugin's
  parent can't instantiate the plugin necessary for invoking, CDAP will attempt with other
  parents of the plugin and try to instantiate using them before retuning error.

- :cask-issue:`CDAP-6348` - Fixed an issue with namespace deletion which caused CDAP
  Application test cases to fail in a Windows environment.

- :cask-issue:`CDAP-8862` - Fix an issue with losing a few metrics when a container is
  shutdown.

- :cask-issue:`CDAP-8888` - Fixed an issue with the YARN container allocation logic so
  that the correct container size is used.

- :cask-issue:`CDAP-8913` - Improved the serializability of Tables and IndexedTables when
  used in Spark programs.

- :cask-issue:`CDAP-8945` - Moved the "add plugin" behavior from a plugin's left panel to
  an "Add Entity" button in the CDAP Studio UI.

- :cask-issue:`CDAP-8950` - Fixed an issue in the CDAP UI where navigating from a stream
  card to an overview and then to a detail page made the detail page show a spinner icon
  indefinitely.

- :cask-issue:`CDAP-8980`, :cask-issue:`CDAP-9314` - Fixed an issue with the Spark program
  runtime so that the Kryo serializer can be used.

- :cask-issue:`CDAP-9005` - Fixed an issue where the HBase Queue Debugging Tool failed
  when authorization was enabled.

- :cask-issue:`CDAP-9029`, :cask-issue:`CDAP-9035` - Fixed an issue where users could not
  grant and revoke privileges for UNIX groups and users when using Apache Sentry as the
  authorization extension for CDAP.

- :cask-issue:`CDAP-9046` - Fixed an issue where revoking privileges from a role caused
  the privilege to be revoked from all roles.

- :cask-issue:`CDAP-9086` - Fixed an issue with the Window plugin so that it propagates
  schema properly.

- :cask-issue:`CDAP-9087` - Fixed the Overview panel in home page of the CDAP UI to handle
  unknown entities appropriately.

- :cask-issue:`CDAP-9114` - Added the retrying of local dataset operations when a failure
  happens.

- :cask-issue:`CDAP-9142` - Fixed an issue with the binary format in the Kafka streaming
  source that prevented pipeline deployment.

- :cask-issue:`CDAP-9160` - Fixed an issue that caused YARN containers to be killed due to
  excessive memory usage when impersonation is enabled.

- :cask-issue:`CDAP-9216` - Fixed bug where navigation links were referencing default
  namespace instead of the current namespace.

- :cask-issue:`HYDRATOR-703` - Improved error messages for the 'Get Schema' functionality
  of Database plugins in CDAP Pipelines.

Known Issues
------------
- :cask-issue:`CDAP-9151` - The CDAP CLI commands for getting and setting preferences
  introduced in CDAP 4.1.0 (such as ``set app preferences <app-id> <preferences>``) are not
  working correctly. Use the previous commands (marked as deprecated), such as ``set
  preferences app <runtime-args> <app-id>``, as a workaround.

- :cask-issue:`CDAP-9388` - When creating a stream and uploading data from the wizard in
  the CDAP resource center, the metrics on the cards in the overview do not show appropriate
  numbers. It will just show zero for the number of events and the bytes.

API Changes
-----------

Logs
....
- :cask-issue:`CDAP-9084` - The CDAP Logging APIs now return a 404 status code if the
  entity (the run id) for which logs are requested does not exist.

.. Deprecated and Removed Features


`Release 4.1.0 <http://docs.cask.co/cdap/4.1.0/index.html>`__
=============================================================

New Features
------------

Secure Impersonation
....................

- :cask-issue:`CDAP-8110` - Added support for fine-grained impersonation at the CDAP
  application, dataset, and stream level.

- :cask-issue:`CDAP-8355` - Impersonated namespaces can be configured to disallow the
  impersonation of the namespace owner when running CDAP Explore queries.

Replication and Resiliency
..........................

- :cask-issue:`CDAP-7685` - Provided SPI hooks that users can implement for performing
  HBase DDL operations.

- :cask-issue:`CDAP-8025` - Added a tool to check a cluster's replication status.

- :cask-issue:`CDAP-8032` - CDAP context methods will now be retried according to a
  program's retry policy. These are governed by these properties:

  - ``custom.action.retry.policy.base.delay.ms``
  - ``custom.action.retry.policy.max.delay.ms``
  - ``custom.action.retry.policy.max.retries``
  - ``custom.action.retry.policy.max.time.secs``
  - ``custom.action.retry.policy.type``
  - ``flow.retry.policy.base.delay.ms``
  - ``flow.retry.policy.max.delay.ms``
  - ``flow.retry.policy.max.retries``
  - ``flow.retry.policy.max.time.secs``
  - ``flow.retry.policy.type``
  - ``mapreduce.retry.policy.base.delay.ms``
  - ``mapreduce.retry.policy.max.delay.ms``
  - ``mapreduce.retry.policy.max.retries``
  - ``mapreduce.retry.policy.max.time.secs``
  - ``mapreduce.retry.policy.type``
  - ``service.retry.policy.base.delay.ms``
  - ``service.retry.policy.max.delay.ms``
  - ``service.retry.policy.max.retries``
  - ``service.retry.policy.max.time.secs``
  - ``service.retry.policy.type``
  - ``spark.retry.policy.base.delay.ms``
  - ``spark.retry.policy.max.delay.ms``
  - ``spark.retry.policy.max.retries``
  - ``spark.retry.policy.max.time.secs``
  - ``spark.retry.policy.type``
  - ``system.log.process.retry.policy.base.delay.ms``
  - ``system.log.process.retry.policy.max.retries``
  - ``system.log.process.retry.policy.max.time.secs``
  - ``system.log.process.retry.policy.type``
  - ``system.metrics.retry.policy.base.delay.ms``
  - ``system.metrics.retry.policy.max.retries``
  - ``system.metrics.retry.policy.max.time.secs``
  - ``system.metrics.retry.policy.type``
  - ``worker.retry.policy.base.delay.ms``
  - ``worker.retry.policy.max.delay.ms``
  - ``worker.retry.policy.max.retries``
  - ``worker.retry.policy.max.time.secs``
  - ``worker.retry.policy.type``
  - ``workflow.retry.policy.base.delay.ms``
  - ``workflow.retry.policy.max.delay.ms``
  - ``workflow.retry.policy.max.retries``
  - ``workflow.retry.policy.max.time.secs``
  - ``workflow.retry.policy.type``

- :cask-issue:`CDAP-8037` - Added a ``master.manage.hbase.coprocessors`` setting that can be
  set to false on clusters where the CDAP coprocessors are deployed on every HBase node.

Enhancements to the New CDAP UI
...............................

- :cask-issue:`CDAP-8021` - Added the management of preferences at the application and
  program levels.

- :cask-issue:`CDAP-8198`, :cask-issue:`CDAP-8199`, :cask-issue:`CDAP-8214`,
  :cask-issue:`CDAP-8217` - The CDAP UI added dataset and stream detail and overviews.

- :cask-issue:`CDAP-8203` - The CDAP UI added a "call-to-action" dialog after entity
  creation, so users can easily perform actions on the newly-created entities.

- :cask-issue:`CDAP-8282`, :cask-issue:`CDAP-8376` - Users can now view events and logs of
  programs in the new CDAP UI using the events and log view "fast-action" dialogs.

- :cask-issue:`CDAP-8398` - Users now see on the CDAP UI homepage a "Just Added" section,
  listing and highlighting any entities added in the last five minutes.

- :cask-issue:`HYDRATOR-208` - The CDAP UI added a duration timer to CDAP pipelines.

Logs
....

- :cask-issue:`CDAP-7676`, :cask-issue:`CDAP-9999` - Added a prototype implementation for a rolling HDFS log
  appender.

- :cask-issue:`CDAP-7962` - Program context information, including namespace, program
  name, and program type, are now available in the MDC property of each ILoggingEvent
  emitted from a program container.

- :cask-issue:`CDAP-8108` - Revised the CDAP Log Appender to use `Logback
  <http://logback.qos.ch/>`__\ 's Appender interface.

- :cask-issue:`CDAP-8231` - The log file cleaner thread will remove metadata and, for
  successfully deleted metadata entries, it will delete the corresponding log files. The log
  file cleaner thread will only remove the metadata entries for the old (pre-4.1.0) log
  format.

- :cask-issue:`CDAP-8261` - Logs collected by the CDAP Log Appender will be stored at a
  common ``<cdap>/logs`` path, owned by the cdap user. For security, it is readable only by
  the cdap user.

- :cask-issue:`CDAP-8428` - Added additional metrics about the status of the log
  framework: ``log.process.min.delay`` and ``log.process.max.delay``.

New CDAP Pipeline Plugins
.........................

- :cask-issue:`HYDRATOR-235` - The Kinesis Spark Streaming source plugin is available in
  its own repository at `github.com/hydrator/kinesis-spark-streaming-source
  <https://github.com/hydrator/kinesis-spark-streaming-source>`__.

- :cask-issue:`HYDRATOR-552` - Added a plugin for sampling data from a source, available
  at `github.com/hydrator/sampling-aggregator
  <https://github.com/hydrator/sampling-aggregator>`__.

- :cask-issue:`HYDRATOR-585` - The HTTP Sink plugin (for posting data from a pipeline to
  an external endpoint) has been added at `github.com/hydrator/http-sink
  <https://github.com/hydrator/http-sink>`__.

- :cask-issue:`HYDRATOR-954` - The Kinesis Source plugin now works in realtime pipelines.

- :cask-issue:`HYDRATOR-983` - Added a Feature Generator plugin for a pipeline builder.

- :cask-issue:`HYDRATOR-1049` - Added a DynamoDb Sink as a plugin, available at
  `github.com/hydrator/dynamodb-sink <https://github.com/hydrator/dynamodb-sink>`__.

- :cask-issue:`HYDRATOR-1050` - Added a DynamoDB Batch Source plugin, available at
  `github.com/hydrator/dynamodb-source <https://github.com/hydrator/dynamodb-source>`__.

- :cask-issue:`HYDRATOR-1073` - Added a "Fail This Pipeline" sink plugin in a repo at
  `github.com/hydrator/failpipeline-sink <https://github.com/hydrator/failpipeline-sink>`__;
  this is a sink where, if any records flow to the sink, the pipeline is marked as failed,
  triggering any post-actions that might be scheduled.

- :cask-issue:`HYDRATOR-1074` - Added a plugin for fetching data from an external HTTP
  site and writing the response to HDFS, available at
  `github.com/hydrator/httptohdfs-action <https://github.com/hydrator/httptohdfs-action>`__.

- :cask-issue:`HYDRATOR-1172` - Added a Realtime Stream Source plugin, available at
  `github.com/hydrator/realtime-stream-source
  <https://github.com/hydrator/realtime-stream-source>`__.

- :cask-issue:`HYDRATOR-1249` - The Tokenizer plugin is now available in it own repository
  at `github.com/hydrator/tokenizer-analytics
  <https://github.com/hydrator/tokenizer-analytics>`__.

- :cask-issue:`HYDRATOR-1250` - The NGramTransform plugin is now available in its own
  repository at `github.com/hydrator/ngram-analytics
  <https://github.com/hydrator/ngram-analytics>`__.

- :cask-issue:`HYDRATOR-1251` - The DecisionTree Regression plugins are now available in
  their own repository at `github.com/hydrator/decision-tree-analytics
  <https://github.com/hydrator/decision-tree-analytics>`__.

- :cask-issue:`HYDRATOR-1252` - The SkipGram Feature Generator plugin is now available in
  its own repository at `github.com/hydrator/skipgram-analytics
  <https://github.com/hydrator/skipgram-analytics>`__.

- :cask-issue:`HYDRATOR-1253` - The Naive Bayes Analytics plugin is now available in its
  own repository at `github.com/hydrator/naive-bayes-analytics
  <https://github.com/hydrator/naive-bayes-analytics>`__.

- :cask-issue:`HYDRATOR-1254` - The HashingTF Feature Generator plugin is now available in
  its own repository at `github.com/hydrator/hashing-tf-feature-generator
  <https://github.com/hydrator/hashing-tf-feature-generator>`__.

- :cask-issue:`HYDRATOR-1255` - The LogisticRegression plugins are now available in their
  own repository at `github.com/hydrator/logistic-regression-analytics
  <https://github.com/hydrator/logistic-regression-analytics>`__.

- :cask-issue:`HYDRATOR-1323` - Added a new ErrorTransform plugin-type that can be placed
  after a pipeline stage to consume errors emitted by that stage.

- :cask-issue:`HYDRATOR-1398` - Support added for Table datasets for lookups in plugins
  and pipelines.

Dataset Improvements
....................

- :cask-issue:`CDAP-7596` - Added the ability to reuse an existing file system location
  and Hive table when creating a partitioned file set.

- :cask-issue:`CDAP-7597` - Added configuring the CDAP Explore database and table name for
  a dataset using dataset properties.

- :cask-issue:`CDAP-7683` - Added a tool that pre-builds and loads the HBase coprocessors
  required by CDAP onto HDFS.

- :cask-issue:`CDAP-8070` - Added control of group ownership and permissions through
  dataset properties.

Other New Features
..................

- :cask-issue:`CDAP-4556` - CDAP now uses environment variables in the ``spark-env.sh`` and
  properties in the ``spark-defaults.conf`` when launching Spark programs.

- :cask-issue:`CDAP-5107` - Added an HTTP RESTful endpoint to retrieve a specific property
  for a specific version of an artifact in the ``system`` scope.

- :cask-issue:`CDAP-8122` - Made headers and the request/response bodies available in
  audit logs for certain RESTful endpoints.

- :cask-issue:`CDAP-8292` - Added support for CDH 5.10.0.

Improvements
------------

- :cask-issue:`CDAP-3383` - Enabled in CDAP invalid transaction list pruning, a new
  feature introduced in Apache Tephra. This automates the pruning of the invalid transaction
  list after data for the invalid transaction has been dropped.

- :cask-issue:`CDAP-6046` - Added an easier, additional syntax for the CDAP CLI
  ``set/get/load/delete <type> preferences`` commands, with the preferences at the end of the
  syntax, such as ``set workflow preferences MyApp.My.WF 'a=b c=d'``.

- :cask-issue:`CDAP-7835` - The Metadata Service upgrades the metadata dataset to reduce
  the time required by the upgrade tool during a CDAP upgrade.

- :cask-issue:`CDAP-8019` - Added a configuration to control the timeout of CDAP Explore
  operations: set ``explore.http.timeout`` in the ``cdap-site.xml`` file.

- :cask-issue:`CDAP-8061` - Moved the Cask Market Path to the ``cdap-defaults.xml`` file.
  Users can now configure the path to a private Cask Market using the configuration
  setting ``market.base.url``.

- :cask-issue:`CDAP-8075` - The CDAP UI added one-step deploy wizards for the Cask Market.
  Users can now deploy applications and plugins from the Cask Market with a single click,
  instead of downloading them from the market and then uploading them.

- :cask-issue:`CDAP-8152` - StreamingSource plugins now have access to the CDAP
  SparkExecutionContext to read from datasets and streams.

- :cask-issue:`CDAP-8183` - The CDAP UI now automatically retries loading the homepage
  when the CDAP Server is not up and ready yet.

- :cask-issue:`CDAP-8250` - Reduced non-informative stacktrace information in the log when
  a connection to the CDAP Router is closed prematurely.

- :cask-issue:`CDAP-8565` - Improved the master process stop procedure to support fast
  failover when running with HA. Added a new kill command to force-kill CDAP processes.

- :cask-issue:`HYDRATOR-282` - Updated the CSVParser plugin to change "PDL" to "Pipe
  Delimited" and "TDF" to "Tab Delimited".

- :cask-issue:`HYDRATOR-577` - Changed the Table sink plugin to make using the
  ``schema.row.field`` optional, which allows the ``schema.row.field`` to be used as a
  column in the output.

- :cask-issue:`HYDRATOR-1006` - Updated the Tokenizer plugin to be more forgiving when
  parsing tokens by accepting regex with white spaces; the output schema now contains all
  the fields that were in the input schema and not only the column that is being tokenized.

- :cask-issue:`HYDRATOR-1028` - Changed the Data Generator configuration to be easier to
  use; as the type parameter can only be one of "stream" or "table", changed to using a
  select widget to configure it.

- :cask-issue:`HYDRATOR-1144` - Updated the use of "true/false" select boxes to be
  consistent in their ordering.

- :cask-issue:`HYDRATOR-1149` - Added the ability to read recursive directories to the
  File source plugin.

- :cask-issue:`HYDRATOR-1162` - Added logging to an error-dataset to the LogParser and
  XMLMultiParser plugins.

- :cask-issue:`HYDRATOR-1177` - Plugins can now retrieve the input and output schema of
  their stage in their initialize methods.

- :cask-issue:`WRANGLER-3` - The CDAP UI's Wrangler modal dialog will give a warning when
  you try to close or exit out of it without confirmation.

Bug Fixes
---------

- :cask-issue:`CDAP-2543` - Fixed an issue of a hanging application in the case that a
  user program JAR is missing dependencies.

- :cask-issue:`CDAP-4739` - Fixed an issue to make artifact, datasets, logs, and
  coprocessor JAR locations resilient to an HDFS Namenode HA upgrade.

- :cask-issue:`CDAP-5717` - Fixed an issue with starting the CDAP CLI and the CDAP
  Standalone when the on-disk path has a space in it.

- :cask-issue:`CDAP-6690` - Fixed issues with the formatting of dataset instance
  properties in the output of the CDAP CLI.

- :cask-issue:`CDAP-6704` - Fixed issues with and clarified certain of the CDAP CLI help
  text and its error messages.

- :cask-issue:`CDAP-7155` - Fixed a problem where the Dataset Service failed to start up
  if authorization was enabled and the authorization plugin was slow to respond.

- :cask-issue:`CDAP-7228` - Empty and null metadata tags are now removed in the metadata
  upgrade step of the CDAP Upgrade Tool.

- :cask-issue:`CDAP-7302` - Fixed an issue that caused the CDAP Master to die if HBase was
  down when a follower became the leader.

- :cask-issue:`CDAP-7694` - Fixed an issue where the CDAP service scripts could cause a
  terminal session to not echo characters.

- :cask-issue:`CDAP-7813` - The security policies for accessing entities have been changed
  and the documentation updated to reflect these changes.

- :cask-issue:`CDAP-7911` - The error messages returned for bad requests to the metadata
  search RESTful APIs have been improved.

- :cask-issue:`CDAP-7930` - Performing a metadata search now returns the correct total,
  even if the offset is very large.

- :cask-issue:`CDAP-7935` - Fixed an issue with the CDAP Standalone not starting and
  stopping correctly.

- :cask-issue:`CDAP-7991` - The Cask Market now shows only those entities that are valid
  for the specific version of CDAP viewing them.

- :cask-issue:`CDAP-8001` - Fixed an issue with the retrieving of logs when a namespace
  was deleted and then recreated with same name.

- :cask-issue:`CDAP-8041` - Fixed an issue where the CDAP Master process would hang during
  a shutdown.

- :cask-issue:`CDAP-8086` - Removed an obsolete Update Dataset Specifications step in the
  CDAP Upgrade tool. This step was required only for upgrading from CDAP versions lower than
  3.2 to CDAP version 3.2.

- :cask-issue:`CDAP-8087` - Provided a workaround for Scala bug SI-6240
  (`issues.scala-lang.org/browse/SI-6240
  <https://issues.scala-lang.org/browse/SI-6240>`__) to allow concurrent execution of
  Spark programs in CDAP Workflows.

- :cask-issue:`CDAP-8088` - Fixed the CDAP UI pipeline detail view so that it can be
  rendered in older browsers.

- :cask-issue:`CDAP-8094` - Fixed an issue where the number of records processed during a
  preview run of the realtime data pipeline was being incremented incorrectly.

- :cask-issue:`CDAP-8133` - Fixed an issue with metadata searches with certain offsets
  overflowing and returning an error.

- :cask-issue:`CDAP-8180` - Fixed an issue with the CDAP Standalone not correctly warning
  about the absence of Node.js.

- :cask-issue:`CDAP-8229` - Fix the CDAP UpgradeTool to not rely on the existence of a
  'default' namespace.

- :cask-issue:`CDAP-8313` - Fixed an issue where system artifacts would continuously be
  loaded if there was a partial JAR in the system artifacts directory.

- :cask-issue:`CDAP-8342` - Fixed an issue where CDAP Explore operations from a program
  container running as a user were impersonating the namespace owner. Now they impersonate
  the respective program container users.

- :cask-issue:`CDAP-8367` - Fixed issues with "Hive-on-Spark" on newer versions of CDH
  failing to run Spark jobs due to permission and configuration errors.

- :cask-issue:`CDAP-8442` - Fixed an issue in the CDAP UI where the "Stop Program" modal
  dialog kept loading (showing a spinning wheel) even after the program had been stopped.

- :cask-issue:`CDAP-8446` - Fixed an issue where the Transactional.run method could throw
  the wrong exception if the transaction service was unavailable when it was finishing a
  transaction.

- :cask-issue:`CDAP-8509` - Fixed an issue in the Transactional Messaging System (TMS)
  table upgrade, where the TMS table could be left in a disabled state if the upgrade tool
  is run after an upgraded CDAP Master is started and then stopped.

- :cask-issue:`CDAP-8544` - Lowered the RPC timeout and number of retries for the HBase
  operations performed by CDAP Master services.

- :cask-issue:`CDAP-8628` - Fixed an issue in the log saver and the metrics processor that
  if an exception was thrown during the changing of the number of instances, a container JVM
  process could be left running without performing any work.

- :cask-issue:`CDAP-8634` - Corrected the Javadoc of the PluginConfig's containsMacro()
  method to reflect that it always returns false at runtime.

- :cask-issue:`CDAP-8636` - Fixed an issue with Spark programs not working against CDH
  5.8.4.

- :cask-issue:`CDAP-8672` - Fixed the CDAP Router so that it does not log an error when it
  cannot discover a service. Previously, the message was logged at the debug level.

- :cask-issue:`CDAP-8687` - Fixed an issue where a user who attempts to create an existing
  stream that was created by a different user received all the privileges and the original
  user had their privileges revoked.

- :cask-issue:`CDAP-8694` - Fixed an issue with properly-locating CDAP_HOME in Distributed
  CDAP instances outside the default ``/opt/cdap`` directory.

- :cask-issue:`HYDRATOR-1085` - Fixed an issue where the File Sink plugin was failing when
  writing byte array records.

- :cask-issue:`HYDRATOR-1096` - Fixed an issue with the macro substitution of a Table
  dataset name.

- :cask-issue:`HYDRATOR-1158` - Fixed an issue with the JSON parser failing if no data was
  present for a nullable field.

- :cask-issue:`HYDRATOR-1212` - Fixed an issue where runtime arguments were not being
  passed correctly for the pipeline preview run in the CDAP UI.

- :cask-issue:`HYDRATOR-1219` - Fixed an issue in the Wrangler transform with the handling
  of escaped characters.

- :cask-issue:`HYDRATOR-1226` - Fixed an issue where pipeline previews would not run in a
  non-default namespace.

- :cask-issue:`HYDRATOR-1238` - Fixed an issue where the RunTransform plugin was not
  checking for null fields.

- :cask-issue:`HYDRATOR-1246` - Fixed an issue with the DateTransform plugin and the
  handling of null values.

- :cask-issue:`HYDRATOR-1377` - Fixed an issue with the S3 source and sink plugins in the
  CDAP Standalone.

- :cask-issue:`TRACKER-264` - Fixed an issue with the Data Dictionary's validate API not
  accepting CDAP-schema JSON.

- :cask-issue:`WRANGLER-12` - Added to Wrangler an option to convert column names to be
  schema-compatible.

Known Issues
------------

- :cask-issue:`CDAP-7770` - The current CDAP UI build process does not work on Microsoft Windows.

- :cask-issue:`CDAP-8375` - Invalid Transaction Pruning does not work on a replicated
  cluster. and needs to be disabled by setting the configuration parameter
  ``data.tx.prune.enable`` to ``false`` in the ``cdap-site.xml`` file.

- :cask-issue:`CDAP-8494` - If users navigate to the classic CDAP UI, they cannot come
  back to the new CDAP UI if they click the browser back button.

- :cask-issue:`CDAP-8531`, :cask-issue:`CDAP-8659`, :cask-issue:`CDAP-8791` - If the
  property ``hive.compute.query.using.stats`` is ``true`` in HDP 2.5.x clusters, CDAP
  Explore queries that trigger a MapReduce program can fail.

- :cask-issue:`CDAP-8663` - If a user revokes a privilege on a namespace, the privilege on
  all entities in that namespace are also revoked.

- :cask-issue:`CDAP-8789` - On the CDAP UI, program logs show error logs correctly. When
  switched to "Raw Logs", the error logs are missing. (The same behavior is seen in the
  classic CDAP UI.) CDAP CLI shows all logs correctly.

- :cask-issue:`CDAP-8812` - Long plugin names don't show up in the left sidebar of the
  CDAP Studio when running on Microsoft Windows.

- :cask-issue:`CDAP-8818` - Local datasets appear on the CDAP UI overview page even though
  they are temporary datasets that should be filtered out.

- :cask-issue:`HYDRATOR-1389` - On Windows, users of CDAP Studio must double-click plugin icons
  in order for their node configuration panels to open.

API Changes
-----------

- :cask-issue:`CDAP-6642` - Attempting to delete a system artifact by specifying a user
  namespace (that previously returned a 200, even though the artifact was not deleted) will
  now return a 404, as that combination of system and user will never occur.

- :cask-issue:`CDAP-8445` - The stream endpoint to enqueue messages now returns a 503
  instead of a 500 if it failed because the dataset service was unavailable.

- :cask-issue:`CDAP-8448` - In general, changed the HTTP RESTful endpoints to return a 503
  instead of a 500 when the transaction service was unavailable.

.. _release-notes-cdap-8606:

- :cask-issue:`CDAP-8606` - Among other new properties added to CDAP, new log saver
  properties have been added to CDAP, replacing the previous properties. As a consequence,
  previous properties will no longer work. See the `Appendix: cdap-site.xml
  <http://docs.cask.co/cdap/4.1.0/en/admin-manual/appendices/cdap-site.html>`__ for
  details on these properties.

  **Old Properties**

  - ``log.cleanup.max.num.files``
  - ``log.cleanup.run.interval.mins``
  - ``log.retention.duration.days``

  **New Properties**

  - ``custom.action.retry.policy.base.delay.ms``
  - ``custom.action.retry.policy.max.delay.ms``
  - ``custom.action.retry.policy.max.retries``
  - ``custom.action.retry.policy.max.time.secs``
  - ``custom.action.retry.policy.type``
  - ``data.tx.prune.enable``
  - ``data.tx.prune.plugins``
  - ``data.tx.prune.state.table``
  - ``data.tx.pruning.plugin.class``
  - ``explore.http.timeout``
  - ``flow.retry.policy.base.delay.ms``
  - ``flow.retry.policy.max.delay.ms``
  - ``flow.retry.policy.max.retries``
  - ``flow.retry.policy.max.time.secs``
  - ``flow.retry.policy.type``
  - ``hbase.client.retries.number``
  - ``hbase.rpc.timeout``
  - ``log.pipeline.cdap.dir.permissions``
  - ``log.pipeline.cdap.file.cleanup.batch.size``
  - ``log.pipeline.cdap.file.cleanup.transaction.timeout``
  - ``log.pipeline.cdap.file.max.lifetime.ms``
  - ``log.pipeline.cdap.file.max.size.bytes``
  - ``log.pipeline.cdap.file.permissions``
  - ``log.pipeline.cdap.file.retention.duration.days``
  - ``log.pipeline.cdap.file.sync.interval.bytes``
  - ``log.process.pipeline.auto.buffer.ratio``
  - ``log.process.pipeline.buffer.size``
  - ``log.process.pipeline.checkpoint.interval.ms``
  - ``log.process.pipeline.config.dir``
  - ``log.process.pipeline.event.delay.ms``
  - ``log.process.pipeline.kafka.fetch.size``
  - ``log.process.pipeline.lib.dir``
  - ``log.process.pipeline.logger.cache.expiration.ms``
  - ``log.process.pipeline.logger.cache.size``
  - ``log.publish.partition.key``
  - ``mapreduce.retry.policy.base.delay.ms``
  - ``mapreduce.retry.policy.max.delay.ms``
  - ``mapreduce.retry.policy.max.retries``
  - ``mapreduce.retry.policy.max.time.secs``
  - ``mapreduce.retry.policy.type``
  - ``market.base.url``
  - ``master.manage.hbase.coprocessors``
  - ``metrics.kafka.meta.table``
  - ``metrics.kafka.topic.prefix``
  - ``metrics.messaging.fetcher.limit``
  - ``metrics.messaging.meta.table``
  - ``metrics.messaging.topic.num``
  - ``metrics.topic.prefix``
  - ``router.audit.path.check.enabled``
  - ``security.keytab.path``
  - ``service.retry.policy.base.delay.ms``
  - ``service.retry.policy.max.delay.ms``
  - ``service.retry.policy.max.retries``
  - ``service.retry.policy.max.time.secs``
  - ``service.retry.policy.type``
  - ``spark.retry.policy.base.delay.ms``
  - ``spark.retry.policy.max.delay.ms``
  - ``spark.retry.policy.max.retries``
  - ``spark.retry.policy.max.time.secs``
  - ``spark.retry.policy.type``
  - ``system.log.process.retry.policy.base.delay.ms``
  - ``system.log.process.retry.policy.max.retries``
  - ``system.log.process.retry.policy.max.time.secs``
  - ``system.log.process.retry.policy.type``
  - ``system.metrics.retry.policy.base.delay.ms``
  - ``system.metrics.retry.policy.max.retries``
  - ``system.metrics.retry.policy.max.time.secs``
  - ``system.metrics.retry.policy.type``
  - ``twill.location.cache.dir``
  - ``worker.retry.policy.base.delay.ms``
  - ``worker.retry.policy.max.delay.ms``
  - ``worker.retry.policy.max.retries``
  - ``worker.retry.policy.max.time.secs``
  - ``worker.retry.policy.type``
  - ``workflow.retry.policy.base.delay.ms``
  - ``workflow.retry.policy.max.delay.ms``
  - ``workflow.retry.policy.max.retries``
  - ``workflow.retry.policy.max.time.secs``
  - ``workflow.retry.policy.type``


Deprecated and Removed Features
-------------------------------

- See :ref:`API Changes, CDAP-8606 <release-notes-cdap-8606>` above for removed properties.

- :cask-issue:`CDAP-8753` - Deprecated the ``waitForFinish()`` method in the ProgramManager and
  added the method ``waitForRun()`` to replace it which will wait for the actual run
  records of the given status.


`Release 4.0.1 <http://docs.cask.co/cdap/4.0.1/index.html>`__
=============================================================

Improvement
-----------

- :cask-issue:`CDAP-8047` - Added a step in the CDAP Upgrade Tool to disable TMS
  (Transaction Messaging Service) message and payload tables. The TMS TwillRunnable will
  update the coprocessors of those tables if required and enable the tables.

Bug Fixes
---------

- :cask-issue:`CDAP-7694` - Fixed an issue where the CDAP service scripts could cause a
  terminal session to not echo characters.

- :cask-issue:`CDAP-7992` - The CDAP Security service under Standalone CDAP is no longer
  forced to bind to localhost.

- :cask-issue:`CDAP-8000` - To avoid transaction timeouts, log cleanup is now done in
  configurable batches (controlled by the property log.cleanup.max.num.files) instead of a
  single short transaction.

- :cask-issue:`CDAP-8007` - Fixed a bug in the TMS (Transaction Messaging Service) message
  and payload table coprocessors by changing the accessing of CDAP configuration and TMS
  metadata tables from reading them inline to reading them in a separate thread.

- :cask-issue:`CDAP-8023` - Changed the default CDAP UI port to 11011 to match the CDAP
  4.0.0 release.

- :cask-issue:`CDAP-8086` - Removed an obsolete Update Dataset Specifications step in the
  CDAP Upgrade tool. This step was required only for upgrading from CDAP versions lower than
  3.2 to CDAP Version 3.2.

- :cask-issue:`CDAP-8087` - Provided a workaround for Scala bug SI-6240
  (https://issues.scala-lang.org/browse/SI-6240) to allow concurrent execution of Spark
  programs in CDAP Workflows.

- :cask-issue:`CDAP-8088` - Fixed the CDAP Hydrator detail view so that it can be rendered
  in older browsers.

- :cask-issue:`CDAP-8094` - Fixed an issue where the number of records processed during a
  preview run of the realtime data pipeline was being incremented incorrectly.

- :cask-issue:`CDAP-8126` - Fixed an issue with the flag used by the Node proxy to enable
  SSL between the CDAP UI and CDAP Router.

- :cask-issue:`CDAP-8137` - Fixed an issue with the CDAP CLI where execute commands may be
  interpreted incorrectly.

- :cask-issue:`CDAP-8148` - Fixed an issue in the template path used with the original
  CDAP UI when rendering a dataset detailed view.

- :cask-issue:`CDAP-8158` - Fixed issues with the Ambari UI "Quick Links" and alerts
  definitions for SSL and non-default ports and the writing of the cdap-security.xml file
  when configured under the CDAP Ambari Service.

- :cask-issue:`HYDRATOR-1212` - Fixed an issue where runtime arguments were not being
  passed for the preview run correctly in the CDAP UI.

- :cask-issue:`HYDRATOR-1226` - Fixed an issue where previews would not run in a
  non-default namespace.


`Release 4.0.0 <http://docs.cask.co/cdap/4.0.0/index.html>`__
=============================================================

New Features
------------

- Cask Market

  - :cask-issue:`CDAP-7203` - Adds Cask Market: Cask's *Big Data* app store, providing an
    ecosystem of pre-built Hadoop solutions, re-usable templates, and plugins. Within CDAP,
    users can access the market and create Hadoop solutions or *Big Data* applications with
    easy-to-use guided wizards.

- Cask Wrangler

  - :cask-issue:`WRANGLER-2` - Added Cask Wrangler: a new CDAP extension for interactive
    data preparation.

- CDAP Transactional Messaging System

  - :cask-issue:`CDAP-7211` - Adds a transactional messaging system that is used for
    reliable communication of messages between components. In CDAP 4.0.0, the transactional
    messaging system replaces Kafka for publishing and subscribing audit logs that is used
    within CDAP for computing data lineage.

- Operational Statistics

  - :cask-issue:`CDAP-7670` - Added a pluggable extension to retrieve operational statistics
    in CDAP. Provided extensions for operational stats from YARN, HDFS, HBase, and CDAP.

  - :cask-issue:`CDAP-7703` - Added reporting operational statistics for YARN. They can be
    retrieved using JMX with the domain name ``io.cdap.cdap.operations`` and the property
    ``name`` set to ``yarn``.

  - :cask-issue:`CDAP-7704` - Added reporting operational statistics for HBase. They can be
    retrieved using JMX with the domain name ``io.cdap.cdap.operations`` and the property
    ``name`` set to ``hbase`` as well as through the CDAP UI Administration page.

- Dynamic Log Level

  - :cask-issue:`CDAP-5479` - Allow updating or resetting of log levels for program types
    worker, flow, and service dynamically using REST endpoints.

  - :cask-issue:`CDAP-7214` - Allow setting the log levels for all program types through
    runtime arguments or preferences.

- New Versions of Distributions Supported

  - :cask-issue:`CDAP-6938` - Added support for Amazon EMR 4.6.0+ installation of CDAP via a
    bootstrap action script.

  - :cask-issue:`CDAP-7249` - Added support for HDInsights 3.5.

  - :cask-issue:`CDAP-7291` - Added support for CDH 5.9.

  - :cask-issue:`CDAP-7901` - Added support for HDP 2.5.

- New Hydrator Plugins Added

  - :cask-issue:`HYDRATOR-504` - Added to the Hydrator plugins a Tokenizer Spark compute
    plugin.

  - :cask-issue:`HYDRATOR-512` - Added to the Hydrator plugins a Sink plugin to write to
    Solr search.

  - :cask-issue:`HYDRATOR-517` - Added to the Hydrator plugins a Logistic Regression Spark
    Machine Learning plugin.

  - :cask-issue:`HYDRATOR-668` - Added to the Hydrator plugins a Decision Tree Regression
    Spark Machine Learning plugin.

  - :cask-issue:`HYDRATOR-909` - Added to the Hydrator plugins a SparkCompute Hydrator
    plugin to compute N-Grams of any given String.

  - :cask-issue:`HYDRATOR-935` - Added to the Hydrator plugins a Windows share copy Action
    plugin.

  - :cask-issue:`HYDRATOR-971` - Added to the Hydrator plugins a Hydrator plugin that
    watches a directory and streams file content when new files are added.

  - :cask-issue:`HYDRATOR-973` - Added to the Hydrator plugins an HTTP Poller source plugin
    for streaming pipelines.

  - :cask-issue:`HYDRATOR-977` - Added to the Hydrator plugins an XML parser plugin that can
    parse out multiple records from a single XML document.

  - :cask-issue:`HYDRATOR-981` - Added to the Hydrator plugins an Action plugin to run any
    executable binary.

  - :cask-issue:`HYDRATOR-1029` - Added to the Hydrator plugins an Action plugin to export
    data in an Oracle database.

  - :cask-issue:`HYDRATOR-1091` - Added the ability to run a Hydrator pipeline in a preview
    mode without publishing. It allows users to view the data in each stage of the preview
    run.

  - :cask-issue:`HYDRATOR-1111` - Added to the Hydrator plugins a plugin for transforming
    data according to commands provided by the Cask Wrangler tool.

  - :cask-issue:`HYDRATOR-1146` - Added to the Hydrator plugins a Sink plugin to write to
    Amazon Kinesis from Batch pipelines.

- Cask Tracker

  - :cask-issue:`TRACKER-233` - Added a data dictionary to Cask Tracker for users to define
    columns for datasets, enforce a common naming convention, and apply masking to PII
    (personally identifiable information).

Improvements
------------

- :cask-issue:`CDAP-1280` - Merged various shell scripts into a single script to interface
  with CDAP, called ``cdap``, shipped with both the SDK and Distributed CDAP.

- :cask-issue:`CDAP-1696` - Updated the default CDAP Router port to 11015 to avoid
  conflicting with HiveServer2's default port.

- :cask-issue:`CDAP-3262` - Fixed an issue with the CDAP scripts under Windows not
  handling a JAVA_HOME path with spaces in it correctly. CDAP SDK home directories with
  spaces in the path are not supported (due to issues with the product) and the scripts now
  exit if such a path is detected.

- :cask-issue:`CDAP-4322` - For MapReduce programs using a PartitionedFileSet as input,
  the partition key corresponding to the input split is now exposed to the mapper.

- :cask-issue:`CDAP-4901` - Fixed an issue where an exception from an HttpContentConsumer
  was being silently ignored.

- :cask-issue:`CDAP-5068` - Added pagination for the search RESTful API. Pagination is
  achieved via ``{{offset}}``, ``{{limit}}```, ``{{numCursors}}``, and ``{{cursor}}``
  parameters in the RESTful API.

- :cask-issue:`CDAP-5632` - New menu option in Cloudera Manager when running the CDAP CSD
  enables running utilities such as the HBaseQueueDebugger.

- :cask-issue:`CDAP-6183` - Added the property ``program.container.dist.jars`` to set
  extra jars to be localized to every program container and to be added to classpaths of
  CDAP programs.

- :cask-issue:`CDAP-6425` - Fixed an issue that allowed a FileSet to be created if its
  corresponding directory already existed.

- :cask-issue:`CDAP-6572` - The namespace that integration test cases run against by
  default has been made configurable.

- :cask-issue:`CDAP-6577` - Improved the UpgradeTool to upgrade tables in namespaces with
  impersonation configured.

- :cask-issue:`CDAP-6587` - Added support for impersonation with CDAP Explore (Hive)
  operations, including enabling exploring of a dataset or running queries against it.

- :cask-issue:`CDAP-6635` - Added a feature that implements caching of user credentials in
  CDAP system services.

- :cask-issue:`CDAP-6837` - Fixed an issue in WorkerContext that did not properly
  implement the contract of the Transactional interface. Note that this fix may cause
  incompatibilities with previous releases in certain cases. See :ref:`API Changes,
  CDAP-6837 <release-notes-cdap-6837>` for more details.

- :cask-issue:`CDAP-6862` - Updated more system services to respect the cdap-site
  parameter "master.service.memory.mb".

- :cask-issue:`CDAP-6885` - Added support for concurrent runs of a Spark program.

- :cask-issue:`CDAP-6937` - Added support for running CDAP on Apache HBase 1.2.

- :cask-issue:`CDAP-6938` - Added support for Amazon EMR 4.6.0+ installation of CDAP via a
  bootstrap action script.

- :cask-issue:`CDAP-6984` - Added support for enabling SSL between the CDAP Router and
  CDAP Master.

- :cask-issue:`CDAP-6995` - Adding the capability to clean up log files which do not have
  corresponding metadata.

- :cask-issue:`CDAP-7117` - Added support for checkpointing in Spark Streaming programs to
  persist checkpoints transactionally.

- :cask-issue:`CDAP-7181` - Updated the Windows start scripts to match the new shell
  script functionality.

- :cask-issue:`CDAP-7192` - Added the ability to specify an announce address and port for
  the CDAP AppFabric and Dataset services. Deprecated the properties ``app.bind.address``
  and ``dataset.service.bind.address``, replacing them with ``master.services.bind.address``
  as the bind address for master services. Added the properties
  ``master.services.announce.address``, ``app.announce.port``, and
  ``dataset.service.announce.port`` for use as announce addresses that are different from
  the bind address.

- :cask-issue:`CDAP-7208` - Improved CDAP Master logging of events related to programs
  that it launches.

- :cask-issue:`CDAP-7240` - Fixed a NullPointerException being logged on closing network
  connection.

- :cask-issue:`CDAP-7284` - Upgraded the Apache Tephra version to 0.10-incubating.

- :cask-issue:`CDAP-7287` - Added support for enabling client certificate-based
  authentication to the CDAP Authentication server.

- :cask-issue:`CDAP-7291` - Added support for CDH 5.9.

- :cask-issue:`CDAP-7319` - Provided programs more control over when and how transactions
  are executed.

- :cask-issue:`CDAP-7385` - The Log HTTP Handler and Router have been fixed to allow the
  streaming of larger logs files.

- :cask-issue:`CDAP-7393` - Revised the documentation on the recommended setting for
  ``yarn.nodemanager.delete.debug-delay-sec``.

- :cask-issue:`CDAP-7439` - Removed the requirement in the documentation of running
  ``kinit`` prior to running the CDAP Upgrade Tool when upgrading a package installation of
  CDAP on a secure Hadoop cluster.

- :cask-issue:`CDAP-7476` - Improves how MapReduce configures its inputs, such that
  failures surface immediately.

- :cask-issue:`CDAP-7477` - Fixed an issue in MapReduce that caused skipping the
  ``destroy()`` method if the committing of any of the dataset outputs failed.

- :cask-issue:`CDAP-7557` - ``DynamicPartitioner`` can now limit the number of open
  RecordWriters to one, if the output partition keys are grouped.

- :cask-issue:`CDAP-7659` - Added support for specifying the Hive execution engine at
  runtime (dynamically).

- :cask-issue:`CDAP-7761` - Adds the ``cluster.name`` property that identifies a cluster;
  this property can be set in the cdap-site.xml file.

- :cask-issue:`CDAP-7797` - Added a step in the CDAP Upgrade Tool to upgrade the
  specification of the MetadataDataset.

- :cask-issue:`HYDRATOR-197` - Included an example of an action and post-run plugin in the
  ``cdap-data-pipeline-plugins-archetype``.

- :cask-issue:`HYDRATOR-947` - Improved the MockSource unit test plugin so that it can be
  configured to set an output schema, allowing subsequent plugins in the pipeline to have
  non-null input schemas.

- :cask-issue:`HYDRATOR-966` - Enabled macros for the Hive database, table name, and
  metastore URI properties for the Hive plugins.

- :cask-issue:`HYDRATOR-976` - Added compression options to the HDFS sink plugin.

- :cask-issue:`HYDRATOR-996` - Enhanced the Kafka streaming source to support configurable
  partitions and initial offsets, and to support optionally including the partition and
  offset in the output records.

- :cask-issue:`HYDRATOR-1004` - The File Batch source in Hydrator now ignores empty
  directories.

- :cask-issue:`HYDRATOR-1069` - The CSV parser can now accept a custom delimiter for
  parsing CSV files.

- :cask-issue:`HYDRATOR-1072` - The Script filter plugin has been removed from Hydrator;
  the JavaScript filter can be used instead.

- :cask-issue:`TRACKER-167` - Cask Tracker now includes "unknown" accesses when finding
  top datasets.

Bug Fixes
---------

- :cask-issue:`CDAP-2945` - A MapReduce job using either a FileSet or PartitionedFileSet
  as input no longer fails if there are no input partitions.

- :cask-issue:`CDAP-4535` - The Authentication server announce address is now
  configurable.

- :cask-issue:`CDAP-5012` - Fixed a problem with downloading of large (multiple gigabyte)
  CDAP Explore queries.

- :cask-issue:`CDAP-5061` - Fixed an issue where the metadata of streams was not being
  updated when the stream's schema was altered.

- :cask-issue:`CDAP-5372` - Fixed an issue where a warning was logged instead of an error
  when a MapReduce job failed in the CDAP SDK.

- :cask-issue:`CDAP-5897` - Updated the default CDAP UI port to 11011 to avoid conflicting
  with Accumulo and Cloudera Manager's Activity Monitor.

- :cask-issue:`CDAP-6398` - Authentication handler APIs have been updated to restrict
  which ``cdap-site.xml`` and ``cdap-security.xml`` properties are available to it.

- :cask-issue:`CDAP-6404` - Fixed an issue with searching for an entity in Cask Tracker by
  metadata after a tag with the same prefix has been removed.

- :cask-issue:`CDAP-7031` - Fixed an issue with misleading log messages from the RunRecord
  corrector.

- :cask-issue:`CDAP-7116` - Fixed an issue so as to significantly reduce the chance of a
  schedule misfire in the case where the CPU cannot trigger a schedule within a certain time
  threshold.

- :cask-issue:`CDAP-7138` - Fixed a problem with duplicate logs showing for a running
  program.

- :cask-issue:`CDAP-7154` - On an incorrect ZooKeeper quorum configuration, the CDAP
  Upgrade Tool and other services such as Master, Router, and Kafka will timeout with an
  error instead of hanging indefinitely.

- :cask-issue:`CDAP-7175` - Fixed an issue in the CDAP Upgrade Tool to allow it to run on
  a CDAP instance with authorization enabled.

- :cask-issue:`CDAP-7177` - Fixed an issue where macros were not being substituted for
  postaction plugins.

- :cask-issue:`CDAP-7204` - Lineage information is now returned for deleted datasets.

- :cask-issue:`CDAP-7248` - Fixed an issue with the FileBatchSource not working with Azure
  Blob Storage.

- :cask-issue:`CDAP-7249` - Fixed an issue with CDAP Explore using Tez on Azure HDInsight.

- :cask-issue:`CDAP-7250` - Fixed an issue where dataset usage was not being recorded
  after an application was deleted.

- :cask-issue:`CDAP-7256` - Fixed an issue with the leaking of Hive classes to programs in
  the CDAP SDK.

- :cask-issue:`CDAP-7259` - Added a warning when a PartitionFilter addresses a
  non-existent field.

- :cask-issue:`CDAP-7285` - Fixed an issue that prevented launching of MapReduce jobs on a
  Hadoop-2.7 cluster.

- :cask-issue:`CDAP-7292` - Fixed an issue in the KMeans example that caused it to
  calculate the wrong cluster centroids.

- :cask-issue:`CDAP-7314` - Fixed an issue with the documentation example links to the
  CDAP ETL Guide.

- :cask-issue:`CDAP-7317` - Fixed a misleading error message that occurred when the
  updating of a CDAP Explore table for a dataset failed.

- :cask-issue:`CDAP-7318` - Fixed an issue that would cause MapReduce and Spark programs
  to fail if too many macros were being used.

- :cask-issue:`CDAP-7321` - Fixed an issue with upgrading CDAP using the CDAP Upgrade
  Tool.

- :cask-issue:`CDAP-7324` - Fixed an issue with the CDAP Upgrade Tool while upgrading
  HBase coprocessors.

- :cask-issue:`CDAP-7361` - Fixed an issue with log file corruption if the log saver
  container crashed due to being killed by YARN.

- :cask-issue:`CDAP-7374` - Fixed an issue with Hydrator Studio in the Windows version of
  Chrome that prevented users from opening and editing a node configuration.

- :cask-issue:`CDAP-7394` - Fixed an issue that prevented impersonation in flows from
  working correctly, by not re-using HBaseAdmin across different UGI.

- :cask-issue:`CDAP-7417` - Fixes an issue where the partitions of a PartitionedFileSet
  were not cleaned up properly after a transaction failure.

- :cask-issue:`CDAP-7428` - Fixed an issue preventing having CustomAction and Spark as
  inner classes.

- :cask-issue:`CDAP-7442` - CDAP Ambari Service's required version of Ambari Server was
  increased to 2.2 to support the empty-value-valid configuration attribute.

- :cask-issue:`CDAP-7473` - Fix the logback-container.xml to work on clusters with
  multiple log directories configured for YARN.

- :cask-issue:`CDAP-7482` - Fixed an issue in CDAP logging that caused system logs from
  Kafka to not be saved after an upgrade and for previously-saved logs to become
  inaccessible.

- :cask-issue:`CDAP-7483` - Fixes an issue where a MapReduce using DynamicPartitioner
  would leave behind output files if it failed.

- :cask-issue:`CDAP-7500` - Fixed an issue where a MapReduce classloader gets closed
  prematurely.

- :cask-issue:`CDAP-7514` - Fixed an issue preventing proper class loading isolation for
  explicit transactions executed by programs.

- :cask-issue:`CDAP-7522` - Improved the documentation for read-less increments.

- :cask-issue:`CDAP-7524` - Adds a missing ``@Override`` annotation for the
  ``WorkerContext.execute()`` method.

- :cask-issue:`CDAP-7527` - Fixed an issue that prevented the using of the logback.xml
  from an application JAR.

- :cask-issue:`CDAP-7548` - Fixed an issue in integration tests to allow JDBC connections
  against authorization-enabled and SSL-enabled CDAP instances.

- :cask-issue:`CDAP-7566` - Improved the usability of ServiceManager in integration tests.
  The ``getServiceURL()`` method now waits for the service to be discoverable before
  returning the service's URL.

- :cask-issue:`CDAP-7612` - Fixed an issue where Spark programs could not be started after
  a master failover or restart.

- :cask-issue:`CDAP-7624` - Fixed an issue where readless increments from different
  MapReduce tasks cancelled each other out.

- :cask-issue:`CDAP-7629` - Added additional tests for read-less increments in HBase.

- :cask-issue:`CDAP-7648`, :cask-issue:`CDAP-7663` - Added support for Amazon EMR 4.6.0.

- :cask-issue:`CDAP-7652` - Startup checks now validate the HBase version and error out if
  the HBase version is not supported.

- :cask-issue:`CDAP-7660` - The CDAP Ambari service was updated to use scripts for Auth
  Server/Router alerts in Ambari due to Ambari not supporting CDAP's ``/status`` endpoint
  with WEB check.

- :cask-issue:`CDAP-7664` - CDAP Quick Links in the CDAP Ambari Service now correctly link
  to the CDAP UI.

- :cask-issue:`CDAP-7666` - Fixed the YARN startup check to fail instead of warning if the
  cluster does not have enough capacity to run CDAP services.

- :cask-issue:`CDAP-7680` - Fixed an issue in the CDAP Sentry Extension by which
  privileges were not being deleted when the CDAP entity was deleted.

- :cask-issue:`CDAP-7707` - Files installed by the "cdap" package under ``/etc`` are now
  properly marked as ``config`` files for RPM packages.

- :cask-issue:`CDAP-7724` - Fixed an issue that could cause Spark and MapReduce programs
  to stop improperly, resulting in a failed run record instead of a killed run record.

- :cask-issue:`CDAP-7737` - Fixed the ``cdap-data-pipeline-plugins-archetype`` to export
  everything in the provided ``groupId`` and fixed the archetype to use the provided
  ``groupId`` as the Java package instead of using a hardcoded value.

- :cask-issue:`CDAP-7742` - Fixed the ordering of search results by relevance in the
  search RESTful API.

- :cask-issue:`CDAP-7757` - Now uses the OpenJDK for redistributable images, such as
  Docker and Virtual Machine images.

- :cask-issue:`CDAP-7819` - The Node.js version check in the CDAP SDK was updated to
  properly handle patch-level comparisons.

- :cask-issue:`HYDRATOR-89` - Batch Hydrator pipelines will now log an error instead of a
  warning if they fail in the CDAP SDK.

- :cask-issue:`HYDRATOR-471` - The Database Batch Source now handles $CONDITIONS when
  getting a schema.

- :cask-issue:`HYDRATOR-499` - GetSchema for an aggregator now fails if there are
  duplicate names.

- :cask-issue:`HYDRATOR-791` - Fixed an issue where Hydrator pipelines using a DBSource
  were not working in an HDP cluster.

- :cask-issue:`HYDRATOR-915` - Fixed an issue where pipelines with multiple sinks
  connected to the same action could fail to publish.

- :cask-issue:`HYDRATOR-948` - Fixed an issue with Spark data pipelines not supporting
  argument values in excess of 64K characters.

- :cask-issue:`HYDRATOR-950` - Password field is now masked in the Email post-run plugin.

- :cask-issue:`HYDRATOR-968` - Fixed an issue so that the CDAP UI does not parse macros
  when starting a pipeline in Hydrator.

- :cask-issue:`HYDRATOR-978` - Fixed an issue where macros were not being evaluated in
  streaming source Hydrator plugins.

- :cask-issue:`HYDRATOR-987` - Fixed the UI widget for the S3 source to make its output
  schema non-editable.

- :cask-issue:`HYDRATOR-994` - Stream source duration in the stream source hydrator plugin
  is now macro-enabled.

- :cask-issue:`HYDRATOR-1010` - The Python evaluator can now handle float and double data
  types.

- :cask-issue:`HYDRATOR-1025` - Fixed an issue to format XML correctly in the XML reader
  plugin.

- :cask-issue:`HYDRATOR-1062` - Fixed a serialization issue with StructuredRecords that
  use primitive arrays.

- :cask-issue:`HYDRATOR-1126` - Fixed an issue where the outputSchema plugin function
  expected an input schema to be present.

- :cask-issue:`HYDRATOR-1131` - Added being able to add to an error dataset for malformed
  rows in CSV while parsing using the CSV parser.

- :cask-issue:`HYDRATOR-1132` - A Hydrator application can now set *reducer* task
  resources as a per-worker resource provided for MapReduce pipelines.

- :cask-issue:`HYDRATOR-1168` - Spark pipelines now use 1024mb of memory by default for
  the Spark client that submits the job.

- :cask-issue:`HYDRATOR-1189` - Any Hydrator pipelines that use S3 (either as an S3 source
  or an S3 sink) based on core-plugins version 1.4 (used in CDAP prior to 4.0.0) will not
  execute on a 4.0.x cluster. A workaround is to recreate (clone) the pipeline using a newer
  version of core-plugins (version 1.5 or higher).

- :cask-issue:`TRACKER-217` - Fixed an issue preventing the adding of additional tags
  after an existing tag had been deleted.

- :cask-issue:`TRACKER-225` - Fixed an issue where Cask Tracker was creating too many
  connections to ZooKeeper.

- :cask-issue:`TRACKER-229` - Fixed an issue that was sending program run ids instead of
  program names.

Known Issues
------------

- :cask-issue:`CDAP-6099` - Due to a limitation in the CDAP MapReduce implementation,
  writing to a dataset does not work in a MapReduce Mapper's ``destroy()`` method.

- :cask-issue:`CDAP-7444` - If a MapReduce program fails during startup, the program's
  ``destroy()`` method is never called, preventing any cleanup or action there being taken.

API Changes
-----------

- :cask-issue:`CDAP-1696` - **Updated the default CDAP Router port to 11015** to avoid
  conflicting with HiveServer2's default port. **Note that this change may cause
  incompatibilities with previous releases if hardcoded in scripts or other programs.**

- :cask-issue:`CDAP-5897` - **Updated the default CDAP UI port to 11011** to avoid
  conflicting with Accumulo and Cloudera Manager's Activity Monitor. **Note that this change
  may cause incompatibilities with previous releases if hardcoded in scripts or other
  programs.**

.. _release-notes-cdap-6837:

- :cask-issue:`CDAP-6837` - Fixed an issue in ``WorkerContext`` that did not properly
  implement the contract of the Transactional interface. **Note that this fix may cause
  incompatibilities with previous releases in certain cases.** See below for details on
  how to handle this change in existing code.

  The Transactional API defines::

    void execute(TxRunnable runnable) throws TransactionFailureException;

  and ``WorkerContext`` implements ``Transactional``. However, it declares this method to
  not throw checked exceptions::

    void execute(TxRunnable runnable);

  That means that any ``TransactionFailureException`` thrown from a
  ``WorkerContext.execute()`` is wrapped into a ``RuntimeException``, and callers must
  write code similar to this to handle the exception::

    try {
      getContext().execute(...);
    } catch (Exception e) {
      if (e.getCause() instanceof TransactionFailureException) {
        // Handle it
      } else {
        // What else to expect? It's not clear...
        throw Throwables.propagate(e);
      }
    }

  This is ugly and inconsistent with other implementations of Transactional. We have
  addressed this by altering the ``WorkerContext`` to directly raise the
  ``TransactionFailureException``. **However, code must change to accomodate this.**

  To address this in existing code, such that it will work both in 4.0.0 and earlier
  versions of CDAP, use code similar to this::

      @Override
      public void run() {
        try {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              if (getContext().getRuntimeArguments().containsKey("fail")) {
                throw new RuntimeException("fail");
              }
            }
          });
        } catch (Exception e) {
          if (e instanceof TransactionFailureException) {
            LOG.error("transaction failure");
          } else if (e.getCause() instanceof TransactionFailureException) {
            LOG.error("exception with cause transaction failure");
          } else {
            LOG.error("other failure");
          }
        }
      }

  This code will succeed because it handles both the "new style" of the ``WorkerContext``
  directly throwing a ``TransactionFailureException`` and at the same time handle the
  previous style of the ``TransactionFailureException`` being wrapped in a
  ``RuntimeException``.

  Code that is only used in CDAP 4.0.0 and higher can use a simpler version of this::

      @Override
      public void run() {
        try {
          getContext().execute(new TxRunnable() {
            @Override
            public void run(DatasetContext context) throws Exception {
              if (getContext().getRuntimeArguments().containsKey("fail")) {
                throw new RuntimeException("fail");
              }
            }
          });
        } catch (TransactionFailureException e) {
          ...
        }
      }
    }

- :cask-issue:`CDAP-7544` - The `Metadata HTTP RESTful API
  <http://docs.cask.co/cdap/4.0.0/en/reference-manual/http-restful-api/metadata.html#http-restful-api-metadata-searching>`__
  has been modified to support sorting and
  pagination. To do so, the API now uses additional parameters |---| ``sort``, ``offset``,
  ``limit``, ``numCursors``, and ``cursor`` |---| and the format of the results
  returned when searching has changed. Whereas previous to CDAP 4.0.0 the API returned
  results as a list of results, the API now returns the results as a field in a JSON object.

- :cask-issue:`CDAP-7796` - Two properties are changing in version 4.0.0 of the CSD:

  - ``log.saver.run.memory.megs`` is replaced with ``log.saver.container.memory.mb``

  - ``log.saver.run.num.cores`` is replaced with ``log.saver.container.num.cores``

  Anyone who has modified these properties in previous versions will have to update them
  after upgrading.

Deprecated and Removed Features
-------------------------------

- :cask-issue:`CDAP-5246` - Removed the deprecated Kafka feed for metadata updates. Users
  should instead subscribe to the CDAP Audit feed, which contains metadata update
  notifications in messages with audit type ``METADATA_CHANGE``.

- :cask-issue:`CDAP-6862` - Deprecated "log.saver.run.memory.megs" and
  "log.saver.run.num.cores", in favor of "log.saver.container.memory.mb" and
  "log.saver.container.num.cores", respectively.

- :cask-issue:`CDAP-7475` - Removes deprecated methods ``setInputDataset()``,
  ``setOutputDataset()``, and ``useStreamInput()`` from the MapReduce API, and related
  methods from the MapReduceContext.

- :cask-issue:`CDAP-7718` - Removed the deprecated ``StreamBatchReadable`` class.

- :cask-issue:`CDAP-7127` - The deprecated CDAP Explore service instance property has been
  removed.

- :cask-issue:`CDAP-7205` - Removes the deprecated ``useDatasets()`` method from API and
  documentation.

- :cask-issue:`CDAP-7563` - Removed the usage of deprecated methods from examples.

- :cask-issue:`HYDRATOR-1094` - Removed the deprecated
  ``cdap-etl-batch-source-archetype``, ``cdap-etl-batch-sink-archetype``, and
  ``cdap-etl-transform-archetype`` in favor of the ``cdap-data-pipeline-plugins-archetype``.


`Release 3.6.0 <http://docs.cask.co/cdap/3.6.0/index.html>`__
=============================================================

Improvements
------------

- :cask-issue:`CDAP-5771` - Allow concurrent runs of different versions of a service. A
  RouteConfig can be uploaded to configure the percentage of requests that need to be sent
  to the different versions.

- :cask-issue:`CDAP-7281` - Improved the PartitionedFileSet to validate the schema of a
  partition key. Note that this will break code that uses incorrect partition keys, which
  was previously silently ignored.

- :cask-issue:`CDAP-7343` - All non-versioned endpoints are now directed to applications
  with a default version. Added test cases with a mixed usage of the new versioned endpoints
  and the corresponding non-versioned endpoints.

- :cask-issue:`CDAP-7366` - Added an upgrade step that adds a default version ID to jobs
  and triggers in the Schedule Store.

- :cask-issue:`CDAP-7385` - The Log HTTP Handler and Router have been fixed to allow the
  streaming of larger logs files.

- :cask-issue:`CDAP-7264` - Added an HTTP RESTful API to create applications with a
  version.

- :cask-issue:`CDAP-7265` - Added an HTTP RESTful API to start or stop programs of a
  specific application version.

- :cask-issue:`CDAP-7266` - Added an upgrade step that adds a default application version
  to existing applications.

- :cask-issue:`CDAP-7268` - Added an HTTP RESTful API to store, fetch, and delete
  RouteConfigs for user service endpoint routing control.

- :cask-issue:`CDAP-7272` - User services now include their application version in the
  payload when they announce themselves in Apache Twill.


Bug Fixes
---------

- :cask-issue:`CDAP-3822` - Unit Test framework now has the capability to exclude scala,
  so users can depend on their own version of the library.

- :cask-issue:`CDAP-7250` - Fixed an issue where dataset usage was not being recorded
  after an application was deleted.

- :cask-issue:`CDAP-7314` - Fixed a problem with the documentation example links to the
  CDAP ETL Guide.

- :cask-issue:`CDAP-7321` - Fixed a problem with upgrading CDAP using the CDAP Upgrade
  Tool.

- :cask-issue:`CDAP-7324` - Fixed a problem with the upgrade tool while upgrading HBase
  coprocessors.

- :cask-issue:`CDAP-7334` - Fixed a problem with the listing of applications not returning
  the application version correctly.

- :cask-issue:`CDAP-7353` - Fixed a problem with using "Download All" logs in the
  browser log viewer by having it fetch and stream the response to the client.

- :cask-issue:`CDAP-7359` - Fixed a problem with NodeJS buffering a response before
  sending it to a client.

- :cask-issue:`CDAP-7361` - Fixed a problem with log file corruption if the log saver
  container crashes due to being killed by YARN.

- :cask-issue:`CDAP-7364` - Fixed a problem with the CDAP UI not handling "5xx" error
  codes correctly.

- :cask-issue:`CDAP-7374` - Fixed Hydrator Studio in the Windows version of Chrome to
  allow users to open and edit a node configuration.

- :cask-issue:`CDAP-7386` - Fixed an error in the "CDAP Introduction" tutorial's
  "Transforming Your Data" example of an application configuration.

- :cask-issue:`CDAP-7391` - Fixed an issue that caused unit test failures when using
  ``org.hamcrest`` classes.

- :cask-issue:`CDAP-7392` - Fixed an issue where the Java process corresponding to the
  MapReduce application master kept running even if the application was moved to the FINISHED
  state.

- :cask-issue:`HYDRATOR-791` - Fixed a problem with Hydrator pipelines using a DBSource
  not working in an HDP cluster.

- :cask-issue:`HYDRATOR-948` - Fixed a problem with Spark data pipelines not supporting
  argument values in excess of 64K characters.


`Release 3.5.2 <http://docs.cask.co/cdap/3.5.2/index.html>`__
=============================================================

Known Issues
------------

- :cask-issue:`CDAP-7179` - In CDAP 3.5.0, new ``kafka.server.*`` properties replace older
  properties such as ``kafka.log.dir``, as described in the :ref:`Administration Manual:
  Appendices: cdap-site.xml <appendix-cdap-default-deprecated-properties>`.

  **If you are upgrading from CDAP 3.4.x to 3.5.x** and you have set a value for
  ``kafka.log.dir`` by using Cloudera Manager's :ref:`safety-valve mechanism
  <cloudera-installation-add-service-wizard-configuration>`, you need to change to the new
  property ``kafka.server.log.dirs``, as the deprecated ``kafka.log.dir`` is being ignored
  in favor of the new property. If you don't, your custom value will be replaced with the
  default value.

- :cask-issue:`CDAP-7608` - When running in Standalone CDAP, the Cask Hydrator plugin
  NaiveBayesTrainer has a *permgen* memory leak that leads to an out-of-memory error if
  the plugin is repeatedly used a number of times, as few as six runs. The only workaround
  is to reset the memory by restarting Standalone CDAP.

Improvements
------------

- :cask-issue:`CDAP-3262` - Fixed an issue with the CDAP scripts under Windows not
  handling a JAVA_HOME path with spaces in it correctly. CDAP SDK home directories with
  spaces in the path are not supported (due to issues with the product) and the scripts now
  exit if such a path is detected.

- :cask-issue:`CDAP-4322` - For MapReduce programs using a PartitionedFileSet as input,
  expose the partition key corresponding to the input split to the mapper.

- :cask-issue:`CDAP-6183` - Added the property ``program.container.dist.jars`` to set
  extra jars to be localized to every program container and to be added to classpaths of
  CDAP programs.

- :cask-issue:`CDAP-6572` - The namespace that integration test cases run against by
  default has been made configurable.

- :cask-issue:`CDAP-6577` - Improve UpgradeTool to upgrade tables in namespaces with
  impersonation configured.

- :cask-issue:`CDAP-6885` - Added support for concurrent runs of a Spark program.

- :cask-issue:`CDAP-6587` - Added support for impersonation with CDAP Explore (Hive)
  operations, such as enabling exploring of a dataset or running queries against it.

- :cask-issue:`CDAP-7291` - Added support for CDH 5.9.

- :cask-issue:`CDAP-7385` - The Log HTTP Handler and Router have been fixed to allow the
  streaming of larger logs files.

- :cask-issue:`CDAP-7387` - Added support to LogSaver for impersonation.

- :cask-issue:`CDAP-7404` - Added authorization for schedules in CDAP.

- :cask-issue:`CDAP-7529` - Improved error handling upon failures in namespace creation.

- :cask-issue:`CDAP-7557` - DynamicPartitioner can now limit the number of open
  RecordWriters to one, if the output partition keys are grouped.

- :cask-issue:`CDAP-7682` - Added a property ``kafka.zookeeper.quorum`` to be used across
  all internal clients using Kafka.

- :cask-issue:`CDAP-7761` - Adds ``cluster.name`` as a property that identifies a cluster;
  this property can be set in the ``cdap-site.xml``.

- :cask-issue:`HYDRATOR-979` - Added the Windows Share Copy plugin to the Hydrator plugins.

- :cask-issue:`HYDRATOR-997` - The SSH hostname and the command to be executed are now
  macro-enabled for the SSH action plugin.

Bug Fixes
---------
- :cask-issue:`CDAP-6981` - Fixed an issue that prevented macros from being used with a
  secure KMS store.

- :cask-issue:`CDAP-7116` - Fixed an issue so as to significantly reduce the chance of a
  schedule misfire in the case where the CPU cannot trigger a schedule within a certain time
  threshold.

- :cask-issue:`CDAP-7177` - Fixed an issue where macros were not being substituted for
  postaction plugins.

- :cask-issue:`CDAP-7250` - Fixed an issue where dataset usage was not being recorded
  after an application was deleted.

- :cask-issue:`CDAP-7318` - Fixed an issue that would cause MapReduce and Spark programs
  to fail if too many macros were being used.

- :cask-issue:`CDAP-7391` - Fixed TestFramework classloading to support classes that
  depend on ``org.hamcrest``.

- :cask-issue:`CDAP-7392` - Fixed an issue where the Java process corresponding to the
  MapReduce application master kept running even if the application was moved to the
  FINISHED state.

- :cask-issue:`CDAP-7394` - Fixed an issue with impersonation in flows not working by not
  re-using ``HBaseAdmin`` across different UGI.

- :cask-issue:`CDAP-7396` - Fixed an issue which prevented scheduled jobs from running on
  a namespace with impersonation.

- :cask-issue:`CDAP-7398` - Fixed an issue which prevented an app in a namespace from
  being deleted if a program for the same app is running in a different namespace.

- :cask-issue:`CDAP-7403` - Fixed an issue that prevented the CDAP UI from starting if the
  ``logback.xml`` was configured to log at the INFO or lower level.

- :cask-issue:`CDAP-7404` - Added authorization for schedules in CDAP.

- :cask-issue:`CDAP-7420` - Avoid the caching of ``YarnClient`` in order to fix a problem
  that occurred in namespaces with impersonation configured.

- :cask-issue:`CDAP-7433` - Fixed an issue that prevented ``HBaseQueueDebugger`` from
  running in an impersonated namespace.

- :cask-issue:`CDAP-7435` - Fixed an error which prevented the downloading of large logs
  using the CDAP UI.

- :cask-issue:`CDAP-7438`, :cask-issue:`CDAP-7439` - Removed the requirement of running
  "kinit" prior to running either the Upgrade or Transaction Debugger tools of CDAP on a
  secure Hadoop cluster.

- :cask-issue:`CDAP-7458` - Fixed an issue that prevented the CDAP Upgrade Tool from being
  run for a namespace with authorization turned on.

- :cask-issue:`CDAP-7473` - Fix logback-container.xml to work on clusters with multiple
  log directories configured for YARN.

- :cask-issue:`CDAP-7482` - Fixed a problem in CDAP logging that caused system logs from
  Kafka to not be saved after an upgrade and for previously-saved logs to become
  inaccessible.

- :cask-issue:`CDAP-7500` - Fixed cases where the MapReduce classloader was being closed
  prematurely.

- :cask-issue:`CDAP-7527` - Fixed a problem that prevented the use of a ``logback.xml``
  from an application jar.

- :cask-issue:`CDAP-7548` - Fixed a problem in integration tests to allow JDBC connections
  against authorization-enabled and SSL-enabled CDAP instances.

- :cask-issue:`CDAP-7566` - Improved the usability of ServiceManager in integration tests.
  The ``getServiceURL`` method now waits for the service to be discoverable before
  returning the service's URL.

- :cask-issue:`CDAP-7612` - Fixed cases where Spark programs could not be started after a
  master failover or restart.

- :cask-issue:`CDAP-7660` - The CDAP Ambari service was updated to use scripts for Auth
  Server/Router alerts in Ambari due to Ambari not supporting CDAP's ``/status`` endpoint
  with WEB check.

- :cask-issue:`HYDRATOR-1125` - Fixed a problem that prevented the adding of a schema with
  hyphens in the Hydrator UI.


`Release 3.5.1 <http://docs.cask.co/cdap/3.5.1/index.html>`__
=============================================================

Known Issues
------------

- :cask-issue:`CDAP-7175` - If you are upgrading an authorization-enabled CDAP instance,
  you will need to give the *cdap* user *ADMIN* privileges on all existing CDAP
  namespaces. See the `Administration Manual: Upgrading
  <http://docs.cask.co/cdap/3.5.1/en/admin-manual/upgrading/index.html#upgrading-index>`__
  for your distribution for details.

- :cask-issue:`CDAP-7179` - In CDAP 3.5.0, new ``kafka.server.*`` properties replace older
  properties such as ``kafka.log.dir``, as described in the `Administration Manual:
  Appendices: cdap-site.xml
  <http://docs.cask.co/cdap/3.5.1/en/admin-manual/appendices/cdap-site.html#appendix-cdap-default-deprecated-properties>`__.

  **If you are upgrading from CDAP 3.4.x to 3.5.x** and you have set a value for
  ``kafka.log.dir`` by using Cloudera Manager's `safety-valve mechanism
  <http://docs.cask.co/cdap/3.5.1/en/admin-manual/installation/cloudera.html#cloudera-installation-add-service-wizard-configuration>`__,
  you need to change to the new
  property ``kafka.server.log.dirs``, as the deprecated ``kafka.log.dir`` is being ignored
  in favor of the new property. If you don't, your custom value will be replaced with the
  default value.


Improvements
------------

- :cask-issue:`CDAP-7192` - Added the ability to specify an announce address and port for
  the ``appfabric`` and ``dataset`` services.

  Deprecated the properties ``app.bind.address`` and ``dataset.service.bind.address``,
  replacing them with ``master.services.bind.address`` as the bind address for master
  services.

  Added the properties ``master.services.announce.address``, ``app.announce.port``, and
  ``dataset.service.announce.port`` for use as announce addresses that are different from
  the bind address.

- :cask-issue:`CDAP-7240` - Upgraded the version of ``netty-http`` used in CDAP to version
  0.15, resolving a problem with a NullPointerException being logged on the closing of a
  network connection.

- :cask-issue:`HYDRATOR-578` - Snapshot sinks now allow users to specify a property
  ``cleanPartitionsOlderThan`` that cleans up any snapshots older than "x" days.


Bug Fixes
---------

- :cask-issue:`CDAP-6215` - PartitionConsumer appropriately drops partitions that have
  been deleted from a corresponding PartitionedFileSet.

- :cask-issue:`CDAP-6404` - Fixed an issue with searching for an entity in Cask Tracker by
  metadata after a tag with the same prefix has been removed.

- :cask-issue:`CDAP-7138` - Fixed a problem with duplicate logs showing for a running program.

- :cask-issue:`CDAP-7175` - Fixed a bug in the upgrade tool to allow it to run on a CDAP
  instance with authorization enabled.

- :cask-issue:`CDAP-7178` - Fixed an issue with uploading an application JAR or file to a
  stream through the CDAP UI.

- :cask-issue:`CDAP-7187` - Fixed a problem with the property
  ``dataset.service.bind.address`` having no effect.

- :cask-issue:`CDAP-7199` - Corrected errors in the documentation to correctly show how to
  set the schema on an existing table.

- :cask-issue:`CDAP-7204` - Lineage information is now returned for deleted datasets.

- :cask-issue:`CDAP-7222` - Fixed a problem with being unable to delete a namespace if a
  configured keytab file doesn't exist.

- :cask-issue:`CDAP-7235` - Fixed a problem with a NullPointerException when the CDAP UI fetches a log.

- :cask-issue:`CDAP-7237` - Prevented accidental grant of additional actions to a user as
  part of a grant operation when using Apache Sentry as the authorization provider.

- :cask-issue:`CDAP-7248` - Fixed a problem with the FileBatchSource not working with Azure Blob Storage.

- :cask-issue:`CDAP-7249` - Fixed a problem with CDAP Explore using Tez on Azure HDInsight.

- :cask-issue:`HYDRATOR-912` - Fixed an issue where the Joiner plugin was failing in
  Hydrator pipelines executing in a Spark environment.

- :cask-issue:`HYDRATOR-922` - Fixed a bug that caused the Database Source, Joiner,
  GroupByAggregate, and Deduplicate plugins to fail on certain versions of Spark.

- :cask-issue:`HYDRATOR-932` - Fixed an error in the documentation of the HDFS Source and
  Sink with respect to the alias under high-availability.

- :cask-issue:`TRACKER-217` - Fixed an issue preventing the adding of additional tags
  after an existing tag had been deleted.


`Release 3.5.0 <http://docs.cask.co/cdap/3.5.0/index.html>`__
=============================================================

Known Issues
------------
- :cask-issue:`CDAP-7179` - In CDAP 3.5.0, new ``kafka.server.*`` properties replace older
  properties such as ``kafka.log.dir``, as described in the `Administration Manual:
  Appendices: cdap-site.xml
  <http://docs.cask.co/cdap/3.5.0/en/admin-manual/appendices/cdap-site.html#appendix-cdap-default-deprecated-properties>`__.

  **If you are upgrading from CDAP 3.4.x to 3.5.x,** and you have set a value for
  ``kafka.log.dir`` by using Cloudera Manager's `safety-valve mechanism
  <http://docs.cask.co/cdap/3.5.0/en/admin-manual/installation/cloudera.html#cloudera-installation-add-service-wizard-configuration>`__,
  you need to change to the
  new property ``kafka.server.log.dirs``, as the deprecated ``kafka.log.dir`` is being
  ignored in favor of the new property. If you don't, your custom value will be replaced
  with the default value.

API Changes
-----------

- :cask-issue:`CDAP-4860` - Introduced an "available" (``/available``) endpoint for
  Services to check their availability.

- :cask-issue:`CDAP-5279` - The ``beforeSubmit`` and ``onFinish`` methods of the MapReduce
  and Spark APIs have been deprecated. Changes to the API include:

  1. ``AbstractMapReduce`` and ``AbstractSpark`` now implement ``ProgramLifeCycle``

  2. ``AbstractMapReduce`` and ``AbstractSpark`` now have a ``final
     initialize(context)`` method

  3. ``AbstractMapReduce`` and ``AbstractSpark`` now have a ``protected initialize()``
     method default implementation of which will call ``beforeSubmit()``

  4. User programs will override the no-arg initialize method

  5. Driver will call both versions of the initialize method

- :cask-issue:`CDAP-6150` - The ``isSuccessful()`` method of the WorkflowContext is
  replaced by the ``getState()`` method, which returns the state of the workflow.

- :cask-issue:`CDAP-6930` - **Incompatible Change:** Updated the "cdap-clients" to throw
  ``UnauthorizedException`` when an operation returns ``403 - Forbidden`` from CDAP. Users
  of "cdap-clients" may need to update their code to handle these exceptions.

- :cask-issue:`TRACKER-21` - Renamed the AuditLog service to the TrackerService.

New Features
------------

- :cask-issue:`CDAP-2963` - All HBase Tables created through CDAP will now have a key
  ``cdap.version`` in the ``HTableDescriptor``.

- :cask-issue:`CDAP-3368` - Add location for ``cdap cli`` to PATH in distributed CDAP
  packages.

- :cask-issue:`CDAP-3890` - Improved performance of the Dataset Service.

- :cask-issue:`CDAP-4106` - Created pre-defined alert definitions in the CDAP Ambari
  Service.

- :cask-issue:`CDAP-4107` - Support for HA CDAP installations in the CDAP Ambari Service.

- :cask-issue:`CDAP-4109` - Support for Kerberos-enabled clusters via the CDAP Ambari service.

- :cask-issue:`CDAP-4110` - CDAP Auth Server is now supported in the CDAP Ambari Service
  on Ambari clusters which have Kerberos enabled.

- :cask-issue:`CDAP-4288` - Added an authorization extension backed by Apache Sentry to
  enforce authorization on CDAP entities.

- :cask-issue:`CDAP-4913` - Added a way to cache authorization policies so every
  authorization enforcement request does not have to make a remote call. Caching is
  configurable |---| it can be enabled by setting security.authorization.cache.enabled to
  true. TTL for cache entries (``security.authorization.cache.ttl.secs``) as well as refresh
  interval (``security.authorization.cache.refresh.interval.secs``) is also configurable.

- :cask-issue:`CDAP-5740` - Provided access to ``Partitioner`` and ``Comparator`` classes
  to the ``MapReduceTaskContext`` by implementing ``ProgramLifeCycle``.

- :cask-issue:`CDAP-5770` - Provided setting of YARN container resources requirements for all
  program types via preferences and runtime arguments.

- :cask-issue:`CDAP-6062` - Added protection for a partition of a file set from being
  deleted while a query is reading the partition.

- :cask-issue:`CDAP-6153` - CDAP namespaces can now be mapped to custom namespaces in
  storage providers. While creating a namespace, users can specify the Filesystem directory,
  HBase namespace and Hive database for that namespace. These settings cannot be changed
  once the namespace has been successfully created.

- :cask-issue:`CDAP-6168` - Enable authorization, lineage, and audit log at the data
  operation level for all Datasets.

- :cask-issue:`CDAP-6174` - Addes a new log viewer across CDAP, Cask Hydrator, and Cask
  Tracker, wherever appropriate. Provides easier navigation and debugging functionality for
  logs of different entities.

- :cask-issue:`CDAP-6235` - Added an indicator in the UI of the CDAP mode (distributed or
  standalone, secure or insecure).

- :cask-issue:`CDAP-6393` - Added authorization to the Secure Key HTTP RESTful APIs. To
  create a secure key, a user needs WRITE privilege on the namespace in which the key is
  being created. Users can only view secure keys that they have access to. To delete a key,
  ADMIN privilege is required.

- :cask-issue:`CDAP-6456` - Exposed the secure store APIs to Programs.

- :cask-issue:`CDAP-6516` - Added authorization for listing and viewing CDAP entities.

- :cask-issue:`CDAP-7002` - Fixed an issue where the UI would ignore the configured port
  when connecting to the CDAP Router.

- :cask-issue:`HYDRATOR-156` - Added an alpha feature: Hydrator Data Pipeline preview
  (CDAP SDK only).

- :cask-issue:`HYDRATOR-162` - Added support for executing custom actions in the Cask
  Hydrator pipelines.

- :cask-issue:`HYDRATOR-168` - Re-organized the bottom panel in Cask Hydrator to be
  in-context. Pipeline-level information is moved to a top panel and plugin-level
  information is moved to a modal dialog.

- :cask-issue:`HYDRATOR-379` - Re-organized the left panel in Cask Hydrator studio view to
  have a maximum of four categories of plugin types: Source, Transform, Sink, and Actions.
  All other types are consolidated into one of these types.

- :cask-issue:`HYDRATOR-501` - Implemented the Value Mapper plugin for Cask Hydrator
  plugins. This is a type of transform that maps string values of a field in the input
  record to another value.

- :cask-issue:`HYDRATOR-502` - Added the XML Parser Transform plugin to Cask Hydrator
  plugins. This plugin uses XPath to extract fields from a complex XML Event. It is
  generally used in conjunction with the XML Reader Source Plugin.

- :cask-issue:`HYDRATOR-503` - Added the XML Reader Source Plugin to Cask Hydrator
  plugins. This plugin allows users to read XML files stored on HDFS.

- :cask-issue:`HYDRATOR-506` - Implemented the Cask Hydrator plugin for Row Denormalizer
  aggregator. This plugin converts raw data into de-normalized data based on a key column.
  De-normalized data can be easier and faster to query.

- :cask-issue:`HYDRATOR-507` - Added the Cobol Copybook source plugin to Cask Hydrator
  plugins. This source plugin allows users to read and process mainframe files defined using
  COBOL Copybook.

- :cask-issue:`HYDRATOR-514` - Added the Excel Reader Source plugin to Cask Hydrator
  Plugins. This plugin provides the ability to read data from one or more Excel file(s).

- :cask-issue:`HYDRATOR-629` - Adds macros to pipeline plugin configurations. This allows
  users to set macros for plugin properties which can be provided as runtime arguments while
  scheduling and running the pipeline.

- :cask-issue:`HYDRATOR-634` - Adds a new Run Configuration player for published pipeline
  views. This allows users to set runtime arguments while scheduling or running a pipeline.

- :cask-issue:`HYDRATOR-685` - Added a Twitter source for Spark Streaming pipelines.

- :cask-issue:`TRACKER-96` - Added the ability to edit user properties for a dataset
  directly in Cask Tracker.

- :cask-issue:`TRACKER-98` - Added the Cask Tracker Meter to measure how active a dataset
  is in a cluster on a scale of zero to 100.

- :cask-issue:`TRACKER-100` - Added the ability to add, remove, and manage a common
  dictionary of Preferred Tags in Cask Tracker and apply them to datasets.

- :cask-issue:`TRACKER-104` - Added the ability to preview data directly in the Cask
  Tracker UI.

- :cask-issue:`TRACKER-105` - Added the ability to view usage metrics about datasets in
  Cask Tracker. Users can view how many applications and programs are accessing each dataset
  using service endpoints and the Tracker UI.

Changes
-------

- :cask-issue:`CDAP-5263` - The "CDAP Applications" section in the documentation has been
  split into two separate sections under "CDAP Extensions": "Cask Hydrator" and "Cask
  Tracker".

- :cask-issue:`CDAP-5833` - Eliminated some misleading warnings in the Purchase example.

- :cask-issue:`CDAP-6143` - Added metadata tag for the local datasets.

- :cask-issue:`CDAP-6596` - CDAP Security Extensions are packaged with CDAP Master
  packages and CDAP Parcel.

- :cask-issue:`HYDRATOR-527` - The Script Transform (previously deprecated) has been
  removed, and is replaced with the JavaScript Transform.

- :cask-issue:`HYDRATOR-528` - Secure Store APIs in Hydrator Actions are now exposed.

- :cask-issue:`HYDRATOR-649` - A widget textbox can have a configurable placeholder.

- :cask-issue:`HYDRATOR-653` - Additional Custom Action Hydrator Plugins have been added.

- :cask-issue:`HYDRATOR-682` - The directory containing the Spark Streaming Hydrator
  plugins has been renamed from ``batch.spark`` to ``spark``.

- :cask-issue:`TRACKER-155` - An upgrade process has been added to the UI for Cask
  Tracker.

- :cask-issue:`CDAP-886`, :cask-issue:`CDAP-882` - Access to CDAP Streams via the RESTful
  API, CDAP-CLI, or programmatic API can be authorized through the Security Authorization
  feature.

- :cask-issue:`CDAP-888`, :cask-issue:`CDAP-882` - Enforced authorization in Dataset
  RESTful APIs. Dataset modules, types, and instances are now governed by authorization
  policies.

- :cask-issue:`CDAP-5691`, :cask-issue:`CDAP-5685` - Improved the performance of the
  Dataset Service, with server-side caching of ``getDataset()`` in DatasetService.

- :cask-issue:`CDAP-6154`, :cask-issue:`CDAP-6153` - CDAP Namespaces can now use an
  existing custom HDFS directory. The custom HDFS directory, whose creation/deletion is
  managed by the user, can be specified during the creation of a CDAP Namespace as part of
  its configuration.

- :cask-issue:`CDAP-6155`, :cask-issue:`CDAP-6153` - CDAP Namespaces can now use an
  existing custom HBase namespace. The custom HBase namespace, whose creation/deletion is
  managed by the user, can be specified during the creation of a CDAP Namespace as part of
  its configuration.

- :cask-issue:`CDAP-6156`, :cask-issue:`CDAP-6153` - CDAP Namespaces can now use an
  existing custom Hive database. The custom Hive database, whose creation/deletion is
  managed by the user, can be specified during the creation of a CDAP Namespace as part of
  its configuration.

- :cask-issue:`CDAP-6158`, :cask-issue:`CDAP-6157` - Added support for accessing
  (read/write) dataset across namespaces in CDAP Spark and MapReduce programs.

- :cask-issue:`CDAP-6159`, :cask-issue:`CDAP-6157` - Added support for accessing streams
  (read only) across namespaces in CDAP Spark and MapReduce programs.

- :cask-issue:`HYDRATOR-174`, :cask-issue:`HYDRATOR-157` - Refactored the Spark engine in
  data pipelines to run all non-action pipeline stages in a single Spark program.

- :cask-issue:`HYDRATOR-175`, :cask-issue:`HYDRATOR-157` - Added a streaming pipeline type
  (the Data Streams artifact) to Cask Hydrator for realtime pipelines run using Spark
  Streaming.

- :cask-issue:`HYDRATOR-177`, :cask-issue:`HYDRATOR-157` - Added a Kafka source for
  streaming pipelines.

- :cask-issue:`HYDRATOR-178`, :cask-issue:`HYDRATOR-157` - Added a window plugin to Cask
  Hydrator that enables the creation of sliding windows in a streaming pipeline.

- :cask-issue:`HYDRATOR-181`, :cask-issue:`HYDRATOR-158` - Added an experimental feature
  in the CDAP SDK which allows users to preview the Hydrator pipelines.

- :cask-issue:`HYDRATOR-182` - Hydrator MapReduce or Spark jobs now support multiple
  inputs. This will enable more efficient physical workflow generation due to the reduction
  in the number of MapReduce or Spark programs required for a logical pipeline.

- :cask-issue:`HYDRATOR-165` - Support multiple sources as input to a stage.

- :cask-issue:`HYDRATOR-748` - Re-organizes batch pipeline settings to a top panel to
  schedule a batch pipeline, add post-run actions and set pipeline resources and the engine
  used.

- :cask-issue:`HYDRATOR-712` - Added to Cask Hydrator a Batch Pipeline Configuration
  Schedule.

- :cask-issue:`TRACKER-108`, :cask-issue:`TRACKER-98` - Adds to Cask Tracker a tracker
  meter widget in the UI, including search results page and details page. It displays a
  metric that determines the 'truthfulness' of a dataset/stream being used in CDAP or Cask
  Hydrator.

- :cask-issue:`TRACKER-109`, :cask-issue:`TRACKER-100` - Adds a separate section for tags
  in Cask Tracker. This lists all available tags in CDAP.

- :cask-issue:`TRACKER-149`, :cask-issue:`TRACKER-105` - Adds a histogram for audit log in
  Cask Tracker for easier visualization of usage of a dataset.

Improvements
------------

- :cask-issue:`CDAP-1545` - Created a Docker-specific ENTRYPOINT script to support passing
  arguments.

- :cask-issue:`CDAP-4065` - Improved the way that MapReduce failures are reported.

- :cask-issue:`CDAP-4775` - Warns if either the app-fabric or router bind addresses are
  configured with a loopback address.

- :cask-issue:`CDAP-5000` - The number of containers for the CDAP Explore service is no
  longer configurable and will be ignored upon specification. It will always be set to one
  (1).

- :cask-issue:`CDAP-5336` - Now publishing ``stdout`` and ``stderr`` logs for MapReduce
  containers to CDAP.

- :cask-issue:`CDAP-5601` - Allowing the setting of batch size for flowlet process methods
  via preferences and runtime arguments.

- :cask-issue:`CDAP-5794` - Added support for long-running Spark jobs in a
  Kerberos-enabled cluster.

- :cask-issue:`CDAP-5874` - Added support for starting extensions in distributed mode.

- :cask-issue:`CDAP-5959` - Setting the ``JAVA_LIBRARY_PATH`` now causes CDAP Master to
  load Hadoop native libraries at startup.

- :cask-issue:`CDAP-5969` - CDAP Upgrade tasks are now available in the CDAP Ambari
  Service.

- :cask-issue:`CDAP-6034` - CDAP's Tephra dependency has been changed to depend on the
  `Apache Incubator Tephra project <http://tephra.incubator.apache.org>`__.

- :cask-issue:`CDAP-6206` - Improved the error message given on application deployment
  failure due to a missing Spark library.

- :cask-issue:`CDAP-6216` - Added support in the log API for field suppression in JSON
  format.

- :cask-issue:`CDAP-6246` - Added the ability to specify a CDAP Master's temporary
  directory.

- :cask-issue:`CDAP-6276` - Introduced new experimental dataset APIs for updating a
  dataset's properties.

- :cask-issue:`CDAP-6327` - Allowed specifying individual Java heap sizes for Java
  services in ``cdap-env.sh``.

- :cask-issue:`CDAP-6350` - Declared startup script contents as read-only to prevent them
  from being overridden by a user in ``cdap-env.sh``.

- :cask-issue:`CDAP-6361` - Added "Quick Links" for the CDAP UI, Cask Hydrator, and Cask
  Tracker in the Ambari 2.3+ UI.

- :cask-issue:`CDAP-6362` - Added support for CDAP services over SSL in Ambari.

- :cask-issue:`CDAP-6363` - Provided service dependencies for Ambari (requires Ambari
  2.2+).

- :cask-issue:`CDAP-6384` - Updated the Standalone CDAP VM version of IntelliJ IDE to
  2016.1.3.

- :cask-issue:`CDAP-6573` - Added a tool that allows bringing Hive in-sync with the
  partitions of a (time-)partitioned fileset.

- :cask-issue:`CDAP-6880` - Users can now configure timeouts for internal HTTP connections
  and reads in ``cdap-site.xml``. These are used for all internal HTTP calls.

- :cask-issue:`CDAP-6901` - Added a bootstrap step for authorization in CDAP. As part of
  this step:

  1. The user that CDAP runs as now receives "admin" privileges on the CDAP instance, as
     well as "all" privileges on the system namespace.

  2. The list of users specified in the parameter ``security.authorization.admin.users``
     in cdap-site.xml receives "admin" privileges on the CDAP instance so that they can
     create namespaces.

- :cask-issue:`CDAP-6913` - Changed to use ``YarnClient`` instead of the YARN HTTP API to
  fetch node reports.

- :cask-issue:`CDAP-7021` - Improved program launch performance to avoid large CPU spikes
  when multiple programs are launched at the same time.

- :cask-issue:`CDAP-7046` - At configure time, ``containsMacro(.)`` on plugin properties
  that were provided macro syntax will return true. At runtime, all properties will have
  ``containsMacro(.)`` return false.

- :cask-issue:`HYDRATOR-219` - Added a new editor for complex schema in the Cask Hydrator
  UI.

- :cask-issue:`HYDRATOR-244` - Added support for macros in plugins. This allows Cask
  Hydrator plugin fields to accept macros.

- :cask-issue:`HYDRATOR-289` - Added support to join data from multiple sources in Cask
  Hydrator.

- :cask-issue:`HYDRATOR-392` - Enhanced the Cask Hydrator upgrade tool to upgrade 3.4.x
  pipelines to 3.5.x pipelines.

- :cask-issue:`HYDRATOR-560` - The plugins NaiveBayesTrainer and NaiveBayesClassifier now
  have an optional configurable ``features`` property. If specified as ``none``, ``100`` is
  used as the number of features.

- :cask-issue:`HYDRATOR-578` - Snapshot sinks now allow users to specify a property
  ``cleanPartitionsOlderThan`` that cleans up any snapshots older than ``x`` days.

- :cask-issue:`HYDRATOR-606` - Changed the DBSource plugin to override user-specified
  output schema.

- :cask-issue:`HYDRATOR-607` - Fixed an issue that prevented TPFS sources and sinks
  created by Hydrator pipelines from being used as either input or output for MapReduce or
  Spark.

- :cask-issue:`HYDRATOR-686` - Many existing Hydrator batch and spark plugins now have
  macro-enabled properties, as specified in their reference documentation.

- :cask-issue:`HYDRATOR-713` - Added Encryptor and Decryptor plugins to Cask Hydrator that
  can encrypt or decrypt record fields.

Bug Fixes
---------

- :cask-issue:`CDAP-2501` - The CDAP Router and UI no longer need to be colocated using
  Cloudera Manager.

- :cask-issue:`CDAP-3131` - Running the endpoint of the Program Lifecycle RESTful API now
  returns ``404`` instead of an empty list if a specified application is not found.

- :cask-issue:`CDAP-3732` - Fixed an issue where deploying an application was trying to
  enable CDAP Explore on system tables.

- :cask-issue:`CDAP-3750` - Datasets that use reserved Hive keywords will now have their
  column names properly escaped when executing Hive DDL commands.

- :cask-issue:`CDAP-4007` - Fixed an issue when running multiple unit tests in the same
  JVM.

- :cask-issue:`CDAP-4434` - CDAP startup scripts return success (exit 0) if calling a
  service that is already running.

- :cask-issue:`CDAP-5135` - Fixed an issue where the status of a program that was killed
  through YARN showed in CDAP as having been completed successfully.

- :cask-issue:`CDAP-5291` - Fixed a problem in the fit-to-screen functionality of flow
  diagrams.

- :cask-issue:`CDAP-5536` - Fixed a problem with users putting back a partition to
  PartitionConsumer without processing it.

- :cask-issue:`CDAP-5643` - Fixed certain test cases to not depend on ``US`` as the system
  locale.

- :cask-issue:`CDAP-5676` - Upgraded the Hive version used by the CDAP SDK to Hive-1.2.1
  in order to pick up a fix for parquet tables.

- :cask-issue:`CDAP-5875` - Require Spark on clusters configured for Hive on Spark and
  CDAP Explore service.

- :cask-issue:`CDAP-5882` - Removed conditional restart on distributed CDAP package
  upgrades.

- :cask-issue:`CDAP-6026` - Fixed an issue where an exception thrown in the initialize
  method of the Workflow was causing the Workflow container not to be terminated.

- :cask-issue:`CDAP-6035` - Fixed a problem with correctly setting the context classloader
  for the Workflow ``initialize()`` and ``destroy(``) methods, to provide a consistent
  classloading behavior across all program types.

- :cask-issue:`CDAP-6045` - Fixed an issue where application deployment was failing on
  Windows because of a colon (":") character in the filename.

- :cask-issue:`CDAP-6052` - Fixed a bug that prevented the setting of ``local.data.dir``
  in ``cdap-site.xml`` to an absolute path.

- :cask-issue:`CDAP-6109` - Fixed a NullPointerException issue in Spark when saving RDD to
  a PartitionedFileSet dataset.

- :cask-issue:`CDAP-6115` - Fixed a bug in the Flow system where usage of the primitive
  ``byte``, ``short``, or ``char`` types caused exceptions.

- :cask-issue:`CDAP-6121` - Fixed a bug in Spark where using ``@UseDataset`` caused a
  NullPointerException.

- :cask-issue:`CDAP-6127` - Fixed a bug not allowing the transaction service to bind to a
  configurable port.

- :cask-issue:`CDAP-6147` - Improved the error message in the authorization and lineage
  clients when a ``404`` is returned from the server side.

- :cask-issue:`CDAP-6170` - Fixed an issue that caused an error if an application or
  program attempted to override input/output format properties that were already defined in
  the dataset properties.

- :cask-issue:`CDAP-6280` - Fixed a problem with allowing FileSets and PartitionedFileSets
  to be tagged as explorable in the CDAP UI.

- :cask-issue:`CDAP-6311` - Fixed a bug that the program run record was not correctly
  reflected in CDAP if the corresponding YARN application failed to start.

- :cask-issue:`CDAP-6378` - Fixed the classpath of the MapReduce program launched by CDAP
  to include the CDAP classes before the Apache Twill classes.

- :cask-issue:`CDAP-6386` - Fixed an issue where updating the properties of a dataset
  deleted all of its partitions in Hive.

- :cask-issue:`CDAP-6452` - Add a check for the environment variable
  ``CDAP_UI_COMPRESSION_ENABLED`` to disable UI compression.

- :cask-issue:`CDAP-6455` - Fixed the classpath of a MapReduce program launched by the
  explore service to include the ``cdap-common.jar`` at the beginning.

- :cask-issue:`CDAP-6486` - Fixed an issue that caused a Zookeeper watch to leak memory
  every time a program was started.

- :cask-issue:`CDAP-6510` - Fixed an issue where the ExploreService was attempting (with
  no effect except for a slow down) to run the upgrade procedure for all explorable
  datasets.

- :cask-issue:`CDAP-6515` - Fixed classloading issues related to using Guava's
  ``Optional`` class in Spark, allowing programs to perform left-outer and full-outer joins
  on RDDs.

- :cask-issue:`CDAP-6524` - Plugins now support the ``char`` primitive as a property type.

- :cask-issue:`CDAP-6643` - Fixed an issue that caused massive log messages when there was
  an underlying HDFS issues.

- :cask-issue:`CDAP-6783` - Fixed the classpath ordering in Spark to load the classes from
  ``cdap-common`` first.

- :cask-issue:`CDAP-6829` - Fixed issues that prevented the Log Saver from performing
  cleanup when metadata is present for a non-existing file.

- :cask-issue:`CDAP-6852` - Fixed issues that makes the Log Saver more resilient to errors
  while checkpointing.

- :cask-issue:`CDAP-6860` - Improved performance in cube datasets when querying for more
  than one measure in a query. This will also improve metrics query performance.

- :cask-issue:`CDAP-6929` - Logs from Spark driver and executors are now collected.

- :cask-issue:`CDAP-6935` - Fix a bug where the live-info endpoint was not working for
  Workflows, MapReduce, Worker, and Spark.

- :cask-issue:`CDAP-6939` - Added support in the CDAP UI for Google Chrome releases prior
  to version 44.

- :cask-issue:`CDAP-7026` - Upon namespace creation, all privileges are granted to both
  the user who created the namespace as well as the user that programs will run as in the
  new namespace.

- :cask-issue:`CDAP-7066` - Restart of system services now kills containers if the
  containers are unresponsive so as to not leave stray containers.

- :cask-issue:`CDAP-7082` - Removed bundling the parquet JAR from the ``com.twitter``
  package with CDAP Master.

- :cask-issue:`CDAP-7128` - Fixed a bug on changing the number of Worker instances in CDAP
  Distributed mode.

- :cask-issue:`HYDRATOR-47` - The DBSource plugin now casts ``TINYINT`` and ``SMALLINT``
  to ``INT`` type correctly.

- :cask-issue:`HYDRATOR-54` - The Validator UI configuration is now preserved in a cloned
  pipeline.

- :cask-issue:`HYDRATOR-80` - Fixed an issue where the configuration of the FileSource was
  failing while setting the properties for the FileInputFormat.

- :cask-issue:`HYDRATOR-133` - HDFSSink can now be used alongside other sinks in a
  Hydrator pipeline.

- :cask-issue:`HYDRATOR-149` - Removes the dependency of using labels from plugins in
  pipelines being imported in UI. Any pipeline configuration publishable from the CDAP-CLI
  or the Artifact RESTful HTTP API should now be publishable from UI.

- :cask-issue:`HYDRATOR-398` - Adds the ability to view properties of plugins in pipelines
  created in older versions of Cask Hydrator.

- :cask-issue:`HYDRATOR-438` - Fixed the Hydrator CSVParser plugin so that a nullable
  field is only set to null if the parsed value is an empty string and the field is not
  either a string or nullable string type.

- :cask-issue:`HYDRATOR-451` - The CSVParser plugin now supports accepting a nullable
  string as a field to parse. If the field is null, all other fields are propagated and
  those that would otherwise be parsed by the CSVParser are set to null.

- :cask-issue:`HYDRATOR-459` - Fixed a bug causing the UPPER to lower transform not being
  applied to all columns correctly for DBSink.

- :cask-issue:`HYDRATOR-705` - Fixed an issue with record serialization for non-ASCII
  values in the shuffle phase of Hydrator pipelines.

- :cask-issue:`HYDRATOR-790` - Release CDAP 3.4.0 introduced infinite-scroll for the input
  and output schemas; the version used (1.2.2) of the infinite scroll component had
  performance issues. The version of the infinite scroll component used has been downgraded
  to restore the performance in Hydrator views.

- :cask-issue:`TRACKER-42` - Fixed integrating the navigator app in the Cask Tracker UI.
  The POST body request that was sent while deploying the navigator app was using an
  older, deprecated property (UI was using ``metadataKafkaConfig`` instead of
  ``auditKafkaConfig``). This should enable using the navigator app in the Cask Tracker UI.


`Release 3.4.1 <http://docs.cask.co/cdap/3.4.1/index.html>`__
=============================================================

Bug Fixes
---------
- `CDAP-4388 <https://cdap.atlassian.net/browse/CDAP-4388>`__ - Fixed a race
  condition bug in ResourceCoordinator that prevented performing partition
  assignment in the correct order. It affects the metrics processor and
  stream coordinator.

- `CDAP-5855 <https://cdap.atlassian.net/browse/CDAP-5855>`__ - Avoid the
  cancellation of delegation tokens upon completion of Explore-launched
  MapReduce and Spark jobs, as these delegation tokens are shared by CDAP
  system services.

- `CDAP-5868 <https://cdap.atlassian.net/browse/CDAP-5868>`__ - Removed
  'SNAPSHOT' from the artifact version of apps created by default by the CDAP UI.
  This fixes deploying Cask Tracker and Navigator apps, enabling Cask Tracker
  from the CDAP UI.

- `CDAP-5884 <https://cdap.atlassian.net/browse/CDAP-5884>`__ - Fixed a bug
  that caused SDK builds to fail when using 3.3.x versions of maven.

- `CDAP-5887 <https://cdap.atlassian.net/browse/CDAP-5887>`__ - Fixed the
  Hydrator upgrade tool to correctly write out pipeline configs that
  failed to upgrade.

- `CDAP-5889 <https://cdap.atlassian.net/browse/CDAP-5889>`__ - The CDAP
  Standalone now deploys and starts the Cask Tracker app in the default
  namespace if the Tracker artifact is present.

- `CDAP-5898 <https://cdap.atlassian.net/browse/CDAP-5898>`__ - Shutdown
  external processes started by CDAP (Zookeeper and Kafka) when there is
  an error during either startup or shutdown of CDAP.

- `CDAP-5907 <https://cdap.atlassian.net/browse/CDAP-5907>`__ - Fixed an
  issue where parsing of an AVRO schema was failing when it included
  optional fields such as 'doc' or 'default'.

- `CDAP-5947 <https://cdap.atlassian.net/browse/CDAP-5947>`__ - Fixed a bug
  in the BatchReadableRDD so that it won't skip records when used by
  DataFrame.

Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.4.1/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets <http://docs.cask.co/cdap/3.4.1/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`__).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2920 <https://cdap.atlassian.net/browse/CDAP-2920>`__ - Spark jobs on a
  Kerberos-enabled CDAP cluster cannot run longer than the delegation token expiration.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.

- `CDAP-5900 <https://cdap.atlassian.net/browse/CDAP-5900>`__ - During the
  upgrade to CDAP 3.4.1, publishing to Kafka is halted because the CDAP
  Kafka service is not running. As a consequence, any applications that
  sync to the CDAP metadata will become out-of-sync as changes to the
  metadata made by the upgrade tool will not be published.


`Release 3.4.0 <http://docs.cask.co/cdap/3.4.0/index.html>`__
=============================================================

API Changes
-----------
- `CDAP-5082 <https://cdap.atlassian.net/browse/CDAP-5082>`__ - Added a new Spark Java and Scala API.

New Features
------------
- `CDAP-20 <https://cdap.atlassian.net/browse/CDAP-20>`__ - Removed dependency on the Guava
  library from the ``cdap-api`` module. Applications are now free to use a Guava library
  version of their choice.

- `CDAP-3051 <https://cdap.atlassian.net/browse/CDAP-3051>`__ - Added capability for programs to
  perform administrative dataset operations (create, update, truncate, drop).

- `CDAP-3854 <https://cdap.atlassian.net/browse/CDAP-3854>`__ - Added the capability to
  configure Kafka topic for logs and notifications using the ``cdap-site.xml``.

- `CDAP-3980 <https://cdap.atlassian.net/browse/CDAP-3980>`__ - MapReduce programs submitted via CDAP
  now support multiple configured inputs.

- `CDAP-4807 <https://cdap.atlassian.net/browse/CDAP-4807>`__ - Added an ODBC 3.0 Driver for
  CDAP Datasets for Windows-based applications that support an ODBC interface.

- `CDAP-4970 <https://cdap.atlassian.net/browse/CDAP-4970>`__ - Added capability to fetch the
  schema from a JDBC source specified for a Database plugin from inside Cask Hydrator.

- `CDAP-5011 <https://cdap.atlassian.net/browse/CDAP-5011>`__ - Added a CDAP extension *Cask Tracker*:
  data discovery with metadata, audit, and lineage.

- `CDAP-5146 <https://cdap.atlassian.net/browse/CDAP-5146>`__ - Added a new Cask Hydrator
  ``batchaggregator`` plugin type. An aggregator operates on a collection of records,
  grouping them by a key and performing an aggregation on each group.

- `CDAP-5172 <https://cdap.atlassian.net/browse/CDAP-5172>`__ - Added support for
  authorization extensions in CDAP. Extensions extend an ``Authorizer`` class and provide a
  bundle jar containing all their required dependencies. This jar is then specified using
  the property ``security.authorization.extension.jar.path`` in the ``cdap-site.xml``.

- `CDAP-5191 <https://cdap.atlassian.net/browse/CDAP-5191>`__ - Added an ``FTPBatchSource``
  that can fetch data from an FTP server in a batch pipeline of Cask Hydrator.

- `CDAP-5205 <https://cdap.atlassian.net/browse/CDAP-5205>`__ - Added a global search across
  all CDAP entities in the CDAP UI.

- `CDAP-5274 <https://cdap.atlassian.net/browse/CDAP-5274>`__ - The Cask Hydrator Studio now
  includes the capability to configure a new type of pipeline, a "data pipeline" (beta
  feature).

- `CDAP-5360 <https://cdap.atlassian.net/browse/CDAP-5360>`__ - The CDAP UI now supports
  ``Sparksink`` and ``Sparkcompute`` plugin types, included in a new "data pipeline"
  artifact.

- `CDAP-5361 <https://cdap.atlassian.net/browse/CDAP-5361>`__ - Added a ``SparkTransform``
  plugin type, which allows the running of a Spark job that operates as a transform in an ETL
  batch pipeline.

- `CDAP-5362 <https://cdap.atlassian.net/browse/CDAP-5362>`__ - Added a ``SparkSink`` plugin
  type, which allows the running of a Spark job (such as machine learning) on the output of
  an ETL batch pipeline.

- `CDAP-5392 <https://cdap.atlassian.net/browse/CDAP-5392>`__ - Added support for
  ``FormatSpecification`` in Spark when consuming data from a stream.

- `CDAP-5446 <https://cdap.atlassian.net/browse/CDAP-5446>`__ - Added an example application
  demonstrating the use of Spark Streaming with machine-learning and spam classifying.

- `CDAP-5504 <https://cdap.atlassian.net/browse/CDAP-5504>`__ - Added experimental support for
  using Spark as an execution engine for CDAP Explore.

- `CDAP-5707 <https://cdap.atlassian.net/browse/CDAP-5707>`__ - Added support for using Tez as
  an execution engine for CDAP Explore.

- `CDAP-5846 <https://cdap.atlassian.net/browse/CDAP-5846>`__ - Bundled `Node.js
  <https://nodejs.org/>`__ with the CDAP UI RPM and DEB packages and with the CDAP Parcels.

Improvements
------------
- `CDAP-4071 <https://cdap.atlassian.net/browse/CDAP-4071>`__ - MapReduce programs can now be
  configured to write metadata for each partition created using a ``DynamicPartitioner``.

- `CDAP-4117 <https://cdap.atlassian.net/browse/CDAP-4117>`__ - Fixed an issue of not using
  the correct user account to access HDFS when submitting a YARN application through
  Apache Twill, which caused a cleanup failure (and a confusing error message) upon
  application termination.

- `CDAP-4644 <https://cdap.atlassian.net/browse/CDAP-4644>`__ - Workflow logs now contain logs
  from all of the actions executed by a workflow.

- `CDAP-4842 <https://cdap.atlassian.net/browse/CDAP-4842>`__ - Added a ``hydrator-test``
  module that contains mock plugins for unit testing Hydrator plugins.

- `CDAP-4925 <https://cdap.atlassian.net/browse/CDAP-4925>`__ - Added to the CDAP test
  framework the ability to delete applications and artifacts, retrieve application
  information, update an application, and write and remove properties for artifacts.

- `CDAP-4955 <https://cdap.atlassian.net/browse/CDAP-4955>`__ - Added a 'postaction' Cask
  Hydrator plugin type that runs at the end of a pipeline run, irregardless of whether the
  run succeeded or failed.

- `CDAP-5001 <https://cdap.atlassian.net/browse/CDAP-5001>`__ - Downloading an explore query
  from the CDAP UI will now stream the results directly to the client.

- `CDAP-5037 <https://cdap.atlassian.net/browse/CDAP-5037>`__ - Added a configuration property
  to Cask Hydrator TimePartitionedFileSet (TPFS) sinks that will clean out data that is
  older than a threshold amount of time.

- `CDAP-5039 <https://cdap.atlassian.net/browse/CDAP-5039>`__ - Added runtime macros to
  database and post-action Cask Hydrator plugins.

- `CDAP-5042 <https://cdap.atlassian.net/browse/CDAP-5042>`__ - Added a ``numSplits``
  configuration property to Cask Hydrator database sources to allow users to configure how
  many splits should be used for an import query.

- `CDAP-5046 <https://cdap.atlassian.net/browse/CDAP-5046>`__ - The CDAP UI now allows a
  plugin developer to use a "textarea" in node configurations for displaying a plugin
  property.

- `CDAP-5075 <https://cdap.atlassian.net/browse/CDAP-5075>`__ - Programs now have a
  ``logical.start.time`` runtime argument that is populated by the system to be the start
  time of the program. The argument can be overridden just as other runtime arguments.

- `CDAP-5082 <https://cdap.atlassian.net/browse/CDAP-5082>`__ - Added support for Spark
  streaming (to interact with the transactional datasets in CDAP), and support for
  concurrent Spark execution through Workflow forking.

- `CDAP-5178 <https://cdap.atlassian.net/browse/CDAP-5178>`__ - Changed the format of the Cask
  Hydrator configuration. All pipeline stages are now together in a "stages" array instead
  of being broken up into separate "source", "transforms", and "sinks" arrays.

- `CDAP-5181 <https://cdap.atlassian.net/browse/CDAP-5181>`__ - Added an HTTP RESTful endpoint
  to retrieve the state of all nodes in a workflow.

- `CDAP-5182 <https://cdap.atlassian.net/browse/CDAP-5182>`__ - Added an API to retrieve the
  properties that were used to configure (or reconfigure) a dataset.

- `CDAP-5207 <https://cdap.atlassian.net/browse/CDAP-5207>`__ - Removed dependency on Guava
  from the ``cdap-proto`` module.

- `CDAP-5228 <https://cdap.atlassian.net/browse/CDAP-5228>`__ - Added support for CDH 5.7.

- `CDAP-5330 <https://cdap.atlassian.net/browse/CDAP-5330>`__ - The stream creation endpoint
  now accepts a stream configuration (with TTL, description, format specification, and
  notification threshold).

- `CDAP-5376 <https://cdap.atlassian.net/browse/CDAP-5376>`__ - Added an API for MapReduce to
  retrieve information about the enclosing workflow, including its run ID.

- `CDAP-5378 <https://cdap.atlassian.net/browse/CDAP-5378>`__ - Added access to workflow
  information in a Spark program when it is executed inside a workflow.

- `CDAP-5424 <https://cdap.atlassian.net/browse/CDAP-5424>`__ - Added the ability to track the
  lineage of external sources and sinks in a Cask Hydrator pipeline.

- `CDAP-5512 <https://cdap.atlassian.net/browse/CDAP-5512>`__ - Extended the workflow APIs to
  allow the use of plugins.

- `CDAP-5664 <https://cdap.atlassian.net/browse/CDAP-5664>`__ - Introduced a ``referenceName``
  property (used for lineage and annotation metadata) into all external sources and sinks.
  This needs to be set before using any of these plugins.

- `CDAP-5779 <https://cdap.atlassian.net/browse/CDAP-5779>`__ - Upgraded the Tephra version in
  CDAP to 0.7.1.

Bug Fixes
---------
- `CDAP-3498 <https://cdap.atlassian.net/browse/CDAP-3498>`__ - Upgraded CDAP to use
  Apache Twill ``0.7.0-incubating`` with numerous new features, improvements, and bug
  fixes. See the `Apache Twill release notes
  <http://twill.apache.org/releases/>`__ for details.

- `CDAP-3584 <https://cdap.atlassian.net/browse/CDAP-3584>`__ - Upon transaction rollback, a
  ``PartitionedFileSet`` now rolls back the files for the partitions that were added and/or
  removed in that transaction.

- `CDAP-3749 <https://cdap.atlassian.net/browse/CDAP-3749>`__ - Fixed a bug with the database
  plugins that required a password to be specified if the user was specified, even if the
  password was empty.

- `CDAP-4060 <https://cdap.atlassian.net/browse/CDAP-4060>`__ - Added the status for custom
  actions in workflow diagrams.

- `CDAP-4143 <https://cdap.atlassian.net/browse/CDAP-4143>`__ - Fixed a problem with the
  database source where a semicolon at the end of the query would cause an error.

- `CDAP-4692 <https://cdap.atlassian.net/browse/CDAP-4692>`__ - The CDAP UI now prevents users
  from accidentally losing their DAG by showing a browser-native popup for a confirmation
  before navigating away from the Cask Hydrator Studio view.

- `CDAP-4695 <https://cdap.atlassian.net/browse/CDAP-4695>`__ - Fixed an issue in the Windows
  CDAP SDK where streams could not be deleted.

- `CDAP-4735 <https://cdap.atlassian.net/browse/CDAP-4735>`__ - Fixed an issue that made Java
  extensions unavailable to programs, fixing the JavaScript-based Hydrator transforms under Java 8.

- `CDAP-4908 <https://cdap.atlassian.net/browse/CDAP-4908>`__ - Removed ``tableName`` as a
  required setting from database sources, since the ``importQuery`` is sufficient.

- `CDAP-4921 <https://cdap.atlassian.net/browse/CDAP-4921>`__ - Renamed the Hydrator
  ``Teradata`` batch source to ``Database``. The previous ``Database`` source is no longer
  supported.

- `CDAP-4982 <https://cdap.atlassian.net/browse/CDAP-4982>`__ - Changed the Cask Hydrator
  LogParser transform ``logFormat`` field from a textbox to a dropdown.

- `CDAP-5041 <https://cdap.atlassian.net/browse/CDAP-5041>`__ - Changed several
  ``ExploreConnection`` methods to be no-ops instead of throwing exceptions.

- `CDAP-5062 <https://cdap.atlassian.net/browse/CDAP-5062>`__ - Added a ``fetch.size``
  connection setting to the JDBC driver to control the number of rows fetched per database
  cursor, and increased the default fetch size from 50 to 1000.

- `CDAP-5092 <https://cdap.atlassian.net/browse/CDAP-5092>`__ - Fixed a problem that prevented
  applications written in Scala from being deployed.

- `CDAP-5103 <https://cdap.atlassian.net/browse/CDAP-5103>`__ - Fixed a problem so that when the
  schema for a view was not explicitly specified, the view system metadata will include the
  default schema for the specified format if that is available.

- `CDAP-5131 <https://cdap.atlassian.net/browse/CDAP-5131>`__ - Fixed a problem when filtering
  plugins by their extension plugin type; filtering by the extensions plugin type was
  returning extra results for any plugins that did not have an extension.

- `CDAP-5177 <https://cdap.atlassian.net/browse/CDAP-5177>`__ - Fixed a problem with
  PartitionConsumer not appropriately handling partitions that had been deleted since they
  were added to the working set.

- `CDAP-5241 <https://cdap.atlassian.net/browse/CDAP-5241>`__ - Fixed a problem with metadata
  for a dataset not being deleted when a dataset was deleted.

- `CDAP-5267 <https://cdap.atlassian.net/browse/CDAP-5267>`__ - Fixed a problem with the
  ``PartitionFilter.ALWAYS_MATCH`` not working as an input partition filter.
  ``PartitionFilter`` is now serialized into one key of the runtime arguments, to support
  serialization of ``PartitionFilter.ALWAYS_MATCH``. If there are additional fields in the
  ``PartitionFilter`` that do not exist in the partitioning, the filter will then never match.

- `CDAP-5272 <https://cdap.atlassian.net/browse/CDAP-5272>`__ - Fixed a problem with a null
  pointer exception when null values were written to a database sink in Cask Hydrator.

- `CDAP-5280 <https://cdap.atlassian.net/browse/CDAP-5280>`__ - Corrected the documentation of
  the Query HTTP RESTful API for the retrieving of the status of a query.

- `CDAP-5297 <https://cdap.atlassian.net/browse/CDAP-5297>`__ - Fixed a problem with the CDAP
  UI not supporting pipelines created using previous versions of Cask Hydrator. The UI now
  shows appropriate information to upgrade the pipeline to be able to view it in the UI.

- `CDAP-5417 <https://cdap.atlassian.net/browse/CDAP-5417>`__ - Fixed an issue with running
  the CDAP examples in the CDAP SDK under Windows by setting appropriate memory requirements
  in the ``cdap.bat`` start script.

- `CDAP-5460 <https://cdap.atlassian.net/browse/CDAP-5460>`__ - Fixed a problem with the
  workflow Spark programs status not being updated in the CDAP UI on the program list
  screen when it is run as a part of Workflow.

- `CDAP-5463 <https://cdap.atlassian.net/browse/CDAP-5463>`__ - Fixed an issue when changing
  the number of instances of a worker or service.

- `CDAP-5513 <https://cdap.atlassian.net/browse/CDAP-5513>`__ - Fixed a problem with the
  update of metadata indexes so that search results reflect metadata updates correctly.

- `CDAP-5550 <https://cdap.atlassian.net/browse/CDAP-5550>`__ - Fixed a problem with the
  workflow statistics HTTP RESTful endpoint. The endpoint now has a default limit of 10 and
  a default interval of 10 seconds.

- `CDAP-5557 <https://cdap.atlassian.net/browse/CDAP-5557>`__ - Fixed a problem of not showing
  an appropriate error message in the node configuration when the CDAP backend returns 404
  for a plugin property.

- `CDAP-5583 <https://cdap.atlassian.net/browse/CDAP-5583>`__ - Added the ability to support
  multiple sources in the CDAP UI while constructing a pipeline.

- `CDAP-5619 <https://cdap.atlassian.net/browse/CDAP-5619>`__ - Fixed a problem with the
  import of a pipeline configuration. If the imported pipeline config doesn't have
  artifact information for a plugin, the CDAP UI now defaults to the latest artifact from
  the list of artifacts sent by the backend.

- `CDAP-5629 <https://cdap.atlassian.net/browse/CDAP-5629>`__ - Fixed a problem with losing
  metadata after changing the stream format on a MapR cluster by avoiding the use of Hive
  keywords in the CLF format field names; the 'date' field was renamed to 'request_time'.

- `CDAP-5634 <https://cdap.atlassian.net/browse/CDAP-5634>`__ - Fixed a performance issue when
  rendering/scrolling through large input or output schemas for a plugin in the CDAP UI.

- `CDAP-5652 <https://cdap.atlassian.net/browse/CDAP-5652>`__ - Added command line interface
  command to retrieve the workflow node states.

- `CDAP-5793 <https://cdap.atlassian.net/browse/CDAP-5793>`__ - CDAP Explore jobs properly use
  the latest/updated delegation tokens.

- `CDAP-5844 <https://cdap.atlassian.net/browse/CDAP-5844>`__ - Fixed a problem with the
  updating of the HDFS delegation token for HA mode.

Deprecated and Removed Features
-------------------------------
- See the `CDAP 3.4.0 Javadocs
  <http://docs.cask.co/cdap/3.4.0/en/reference-manual/javadocs/index.html#javadocs>`__
  for a list of deprecated and removed APIs.

- As of *CDAP v3.4.0*, *Metadata Update Notifications* have been deprecated, pending
  removal in a later version. The `CDAP Audit Notifications
  <http://docs.cask.co/cdap/3.4.0/en/developers-manual/building-blocks/audit-logging.html#audit-logging>`__
  contain
  notifications for metadata changes. Please change all uses of *Metadata Update
  Notifications* to consume only those messages from the audit feed that have the ``type``
  field set to ``METADATA_CHANGE``.

.. _known-issues-340:

Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.4.0/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets
  <http://docs.cask.co/cdap/3.4.0/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`__).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2920 <https://cdap.atlassian.net/browse/CDAP-2920>`__ - Spark jobs on a
  Kerberos-enabled CDAP cluster cannot run longer than the delegation token expiration.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.


`Release 3.3.3 <http://docs.cask.co/cdap/3.3.3/index.html>`__
=============================================================

Bug Fix
-------

- `CDAP-5350 <https://cdap.atlassian.net/browse/CDAP-5350>`__ - Fixed an issue that prevented
  MapReduce programs from running on clusters with encryption.


`Release 3.3.2 <http://docs.cask.co/cdap/3.3.2/index.html>`__
=============================================================

Improvements
------------
- `CDAP-5047 <https://cdap.atlassian.net/browse/CDAP-5047>`__ - Added a `Batch Source Plugin
  <http://docs.cask.co/cdap/3.3.2/en/cdap-apps/hydrator/hydrator-plugins/batchsources/azureblobstore.html>`__
  to read from Microsoft Azure Blob Storage.

- `CDAP-5134 <https://cdap.atlassian.net/browse/CDAP-5134>`__ - Added support for CDH 5.6 to CDAP.

Bug Fixes
---------
- `CDAP-4967 <https://cdap.atlassian.net/browse/CDAP-4967>`__ - Fixed a schema-parsing bug
  that prevented the use of schemas where a record is used both as a top-level field and
  also used inside a different record field.

- `CDAP-5019 <https://cdap.atlassian.net/browse/CDAP-5019>`__ - Worked around two issues
  (`SPARK-13441 <https://issues.apache.org/jira/browse/SPARK-13441>`__
  and `YARN-4727 <https://issues.apache.org/jira/browse/YARN-4727>`__) that prevented
  launching Spark jobs on CDH (Cloudera Data Hub) clusters managed with Cloudera Manager
  when using Spark 1.4 or greater.

- `CDAP-5063 <https://cdap.atlassian.net/browse/CDAP-5063>`__ - Fixed a problem with
  the CDAP Master not starting when CDAP and the HiveServer2 services are running on the
  same node in an Ambari cluster.

- `CDAP-5076 <https://cdap.atlassian.net/browse/CDAP-5076>`__ - Fixed a problem with the CDAP
  CLI command "update app" that was parsing the application config incorrectly.

- `CDAP-5094 <https://cdap.atlassian.net/browse/CDAP-5094>`__ - Fixed a problem where the explore
  schema fileset property was being ignored unless an explore format was also present.

- `CDAP-5137 <https://cdap.atlassian.net/browse/CDAP-5137>`__ - Fix a problem with Spark jobs
  not being submitted to the appropriate YARN scheduler queue set for the namespace.


`Release 3.3.1 <http://docs.cask.co/cdap/3.3.1/index.html>`__
=============================================================

Improvements
------------
- `CDAP-4602 <https://cdap.atlassian.net/browse/CDAP-4602>`__ - Updated CDAP to use
  Tephra 0.6.5.

- `CDAP-4708 <https://cdap.atlassian.net/browse/CDAP-4708>`__ - Added system metadata to
  existing entities.

- `CDAP-4723 <https://cdap.atlassian.net/browse/CDAP-4723>`__ - Improved the Hydrator plugin
  archetypes to include build steps to build the deployment JSON for the artifact.

- `CDAP-4773 <https://cdap.atlassian.net/browse/CDAP-4773>`__ - Improved the error logging for
  the Master Stream service when it can't connect to the CDAP AppFabric server.

Bug Fixes
---------
- `CDAP-4117 <https://cdap.atlassian.net/browse/CDAP-4117>`__ - Fixed an issue of not using
  the correct user to access HDFS when submitting a YARN application through Apache Twill,
  which caused cleanup failure on application termination.

- `CDAP-4613 <https://cdap.atlassian.net/browse/CDAP-4613>`__ - Fixed a problem with tooltips
  not appearing in Flow and Workflow diagrams displayed in the Firefox browser.

- `CDAP-4679 <https://cdap.atlassian.net/browse/CDAP-4679>`__ - The Hydrator UI now prevents
  drafts from being created with a name of an already-existing draft. This prevents
  overwriting of existing drafts.

- `CDAP-4688 <https://cdap.atlassian.net/browse/CDAP-4688>`__ - Improved the metadata search
  to return matching entities from both the specified namespace and the system namespace.

- `CDAP-4689 <https://cdap.atlassian.net/browse/CDAP-4689>`__ - Fixed a problem when using an
  Hbase sink as one of multiple sinks in a Hydrator pipeline.

- `CDAP-4720 <https://cdap.atlassian.net/browse/CDAP-4720>`__ - Fixed an issue where system
  metadata updates were not being published to Kafka.

- `CDAP-4721 <https://cdap.atlassian.net/browse/CDAP-4721>`__ - Fixed an issue where metadata
  updates wouldn't be sent when certain entities were deleted.

- `CDAP-4740 <https://cdap.atlassian.net/browse/CDAP-4740>`__ - Added validation to the JSON
  imported in the Hydrator UI.

- `CDAP-4741 <https://cdap.atlassian.net/browse/CDAP-4741>`__ - Fixed a bug with deleting
  artifact metadata when an artifact was deleted.

- `CDAP-4743 <https://cdap.atlassian.net/browse/CDAP-4743>`__ - Fixed the Node.js server proxy
  to handle all backend errors (with and without statusCodes).

- `CDAP-4745 <https://cdap.atlassian.net/browse/CDAP-4745>`__ - Fixed a bug in the Hydrator
  upgrade tool which caused drafts to not get upgraded.

- `CDAP-4753 <https://cdap.atlassian.net/browse/CDAP-4753>`__ - Fixed the Hydrator Stream
  source to not assume an output schema. This is valid when a pipeline is created outside
  Hydrator UI.

- `CDAP-4754 <https://cdap.atlassian.net/browse/CDAP-4754>`__ - Fixed ObjectStore to work when
  parameterized with custom classes.

- `CDAP-4767 <https://cdap.atlassian.net/browse/CDAP-4767>`__ - Fixed an issue where delegation token
  cancellation of CDAP program was affecting CDAP master services.

- `CDAP-4770 <https://cdap.atlassian.net/browse/CDAP-4770>`__ - Fixed the Cask Hydrator UI to
  automatically reconnect with the CDAP backend when the backend restarts.

- `CDAP-4771 <https://cdap.atlassian.net/browse/CDAP-4771>`__ - Fixed an issue in Cloudera
  Manager installations where CDAP container logs would go to the stdout file instead of the
  master log.

- `CDAP-4784 <https://cdap.atlassian.net/browse/CDAP-4784>`__ - Fixed an issue where the
  IndexedTable was dropping indices upon row updates.

- `CDAP-4785 <https://cdap.atlassian.net/browse/CDAP-4785>`__ - Fixed a problem in the upgrade
  tool where deleted datasets would cause it to throw a NullPointerException.

- `CDAP-4790 <https://cdap.atlassian.net/browse/CDAP-4790>`__ - Fixed an issue where the Hbase
  implementation of the Table API returned all rows, when the correct response should have
  been an empty set of columns.

- `CDAP-4800 <https://cdap.atlassian.net/browse/CDAP-4800>`__ - Fixed a problem with the error
  message returned when loading an artifact with an invalid range.

- `CDAP-4806 <https://cdap.atlassian.net/browse/CDAP-4806>`__ - Fixed the PartitionedFileSet's
  DynamicPartitioner to work with Avro OutputFormats.

- `CDAP-4829 <https://cdap.atlassian.net/browse/CDAP-4829>`__ - Fixed a Validator Transform
  function generator in the Hydrator UI.

- `CDAP-4831 <https://cdap.atlassian.net/browse/CDAP-4831>`__ - Allows user-scoped plugins to
  surface the correct widget JSON in the Hydrator UI.

- `CDAP-4832 <https://cdap.atlassian.net/browse/CDAP-4832>`__ - Added the ErrorDataset as an
  option on widget JSON in Hydrator plugins.

- `CDAP-4836 <https://cdap.atlassian.net/browse/CDAP-4836>`__ - Fixed a spacing issue for
  metrics showing in Pipeline diagrams of the Hydrator UI.

- `CDAP-4853 <https://cdap.atlassian.net/browse/CDAP-4853>`__ - Fixed issues with the Hydrator
  UI widgets for the Hydrator Kafka real-time source, JMS real-time source, and CloneRecord
  transform.

- `CDAP-4865 <https://cdap.atlassian.net/browse/CDAP-4865>`__ - Enhanced the CDAP SDK to be
  able to publish metadata updates to an external Kafka, identified by the configuration
  property ``metadata.updates.kafka.broker.list``. Publishing can be enabled by setting
  ``metadata.updates.publish.enabled`` to true. Updates are published to the Kafka topic
  identified by the property ``metadata.updates.kafka.topic``.

- `CDAP-4877 <https://cdap.atlassian.net/browse/CDAP-4877>`__ - Fixed errors in Cask Hydrator
  Plugins. Two plugin documents (``core-plugins/docs/Database-batchsink.md`` and
  ``core-plugins/docs/Database-batchsource.md``) were removed, as the plugins have been moved
  from *core-plugins* to *database-plugins* (to ``database-plugins/docs/Database-batchsink.md``
  and ``database-plugins/docs/Database-batchsource.md``).

- `CDAP-4889 <https://cdap.atlassian.net/browse/CDAP-4889>`__ - Fixed an issue with upgrading
  HBase tables while using the CDAP Upgrade Tool.

- `CDAP-4894 <https://cdap.atlassian.net/browse/CDAP-4894>`__ - Fixed an issue with CDAP
  coprocessors that caused HBase tables to be disabled after upgrading the cluster to a
  highly-available file system.

- `CDAP-4906 <https://cdap.atlassian.net/browse/CDAP-4906>`__ - Fixed the CDAP Upgrade Tool to
  return a non-zero exit status upon error during upgrade.

- `CDAP-4924 <https://cdap.atlassian.net/browse/CDAP-4924>`__ - Fixed a PermGen memory leak
  that occurred while deploying multiple applications with database plugins.

- `CDAP-4927 <https://cdap.atlassian.net/browse/CDAP-4927>`__ - Fixed the CDAP Explore
  Service JDBC driver to do nothing instead of throwing an exception when a commit is
  called.

- `CDAP-4950 <https://cdap.atlassian.net/browse/CDAP-4950>`__ - Added an ``'enableAutoCommit'``
  property to the Cask Hydrator database plugins to enable the use of JDBC drivers that,
  similar to the Hive JDBC driver, do not allow commits.

- `CDAP-4951 <https://cdap.atlassian.net/browse/CDAP-4951>`__ - Changed the upload timeout from the
  CDAP CLI from 15 seconds to unlimited.

- `CDAP-4975 <https://cdap.atlassian.net/browse/CDAP-4975>`__ - Pass ResourceManager delegation tokens
  in the proper format in secure Hadoop HA clusters.

Deprecated and Removed Features
-------------------------------

- See the `CDAP 3.3.1 Javadocs
  <http://docs.cask.co/cdap/3.3.1/en/reference-manual/javadocs/index.html#javadocs>`__
  for a list of deprecated and removed APIs.

- The properties ``router.ssl.webapp.bind.port``, ``router.webapp.bind.port``,
  ``router.webapp.enabled`` have been deprecated and will be removed in a future version.


Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.3.1/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets
  <http://docs.cask.co/cdap/3.3.1/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, mode, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.


`Release 3.3.0 <http://docs.cask.co/cdap/3.3.0/index.html>`__
=============================================================

New Features
------------
- `CDAP-961 <https://cdap.atlassian.net/browse/CDAP-961>`__ -
  Added on demand (dynamic) dataset instantiation through program runtime context.

- `CDAP-2303 <https://cdap.atlassian.net/browse/CDAP-2303>`__ -
  Added lookup capability in context that can be used in existing Script, ScriptFilter and Validator transforms.

- `CDAP-3514 <https://cdap.atlassian.net/browse/CDAP-3514>`__ -
  Added an endpoint to get a count of active queries: ``/v3/namespaces/<namespace-id>/data/explore/queries/count``.

- `CDAP-3857 <https://cdap.atlassian.net/browse/CDAP-3857>`__ -
  Added experimental support for running ETL Batch applications on Spark. Introduced an 'engine' setting in the
  configuration that defaults to ``'mapreduce'``, but can be set to ``'spark'``.

- `CDAP-3944 <https://cdap.atlassian.net/browse/CDAP-3944>`__ -
  Added support to PartitionConsumer for concurrency, plus a limit and filter on read.

- `CDAP-3945 <https://cdap.atlassian.net/browse/CDAP-3945>`__ -
  Added support for limiting the number of concurrent schedule runs.

- `CDAP-4016 <https://cdap.atlassian.net/browse/CDAP-4016>`__ -
  Added Java-8 support for Script transforms.

- `CDAP-4022 <https://cdap.atlassian.net/browse/CDAP-4022>`__ -
  Added RESTful APIs to start or stop multiple programs.

- `CDAP-4023 <https://cdap.atlassian.net/browse/CDAP-4023>`__ -
  Added CLI commands to stop, start, restart, or get status of programs in an application.

- `CDAP-4043 <https://cdap.atlassian.net/browse/CDAP-4043>`__ -
  Added support for ETL transforms written in Python.

- `CDAP-4128 <https://cdap.atlassian.net/browse/CDAP-4128>`__ -
  Added a new JavaScript transform that can emit records using an emitter.

- `CDAP-4135 <https://cdap.atlassian.net/browse/CDAP-4135>`__ -
  Added the capability for MapReduce and Spark programs to localize additional resources during setup.

- `CDAP-4228 <https://cdap.atlassian.net/browse/CDAP-4228>`__ -
  Added the ability to configure which artifact a Hydrator plugin should use.

- `CDAP-4230 <https://cdap.atlassian.net/browse/CDAP-4230>`__ -
  Added DAGs to ETL pipelines, which will allow users to fork and merge. ETLConfig has been
  updated to allow representing a DAG.

- `CDAP-4235 <https://cdap.atlassian.net/browse/CDAP-4235>`__ -
  Added AuthorizationPlugin, for pluggable authorization.

- `CDAP-4263 <https://cdap.atlassian.net/browse/CDAP-4263>`__ -
  Added metadata support for stream views.

- `CDAP-4270 <https://cdap.atlassian.net/browse/CDAP-4270>`__ -
  Added CLI support for metadata and lineage.

- `CDAP-4280 <https://cdap.atlassian.net/browse/CDAP-4280>`__ -
  Added the ability to add metadata to artifacts.

- `CDAP-4289 <https://cdap.atlassian.net/browse/CDAP-4289>`__ -
  Added RESTful APIs to set and get properties for an artifact.

- `CDAP-4264 <https://cdap.atlassian.net/browse/CDAP-4264>`__ -
  Added support for automatically annotating CDAP entities with system metadata when they are created or updated.

- `CDAP-4285 <https://cdap.atlassian.net/browse/CDAP-4285>`__ -
  Added an authorization plugin that uses a system dataset to manage ACLs.

- `CDAP-4403 <https://cdap.atlassian.net/browse/CDAP-4403>`__ -
  Moved Hydrator plugins from the CDAP repository as cdap-etl-lib into its own repository.

- `CDAP-4591 <https://cdap.atlassian.net/browse/CDAP-4591>`__ -
  Improved Metadata Indexing and Search to support searches on words in value and tags.

- `CDAP-4592 <https://cdap.atlassian.net/browse/CDAP-4592>`__ -
  Schema fields are stored as Metadata and are searchable.

- `CDAP-4658 <https://cdap.atlassian.net/browse/CDAP-4658>`__ -
  Added capability in CDAP UI to display system tags.

Improvements
------------
- `CDAP-3079 <https://cdap.atlassian.net/browse/CDAP-3079>`__ -
  Table datasets, and any other dataset that implements ``RecordWritable<StructuredRecord>``,
  can now be written to using Hive.

- `CDAP-3887 <https://cdap.atlassian.net/browse/CDAP-3887>`__ -
  The CDAP Router now has a configurable timeout for idle connections, with a default
  timeout of 15 seconds.

- `CDAP-4045 <https://cdap.atlassian.net/browse/CDAP-4045>`__ -
  A new property master.collect.containers.log has been added to cdap-site.xml, which
  determines if container logs are streamed back to the cdap-master process log. (This has
  always been the default behavior). For MapR installations, this must be turned off (set
  to false).

- `CDAP-4133 <https://cdap.atlassian.net/browse/CDAP-4133>`__ -
  Added ability to retrieve the live-info for the AppFabric system service.

- `CDAP-4209 <https://cdap.atlassian.net/browse/CDAP-4209>`__ -
  Added a method to ``ObjectMappedTable`` and ``ObjectStore`` to retrieve a specific
  number of splits between a start and end keys.

- `CDAP-4233 <https://cdap.atlassian.net/browse/CDAP-4233>`__ -
  Messages logged by Hydrator are now prefixed with the name of the stage that logged them.

- `CDAP-4301 <https://cdap.atlassian.net/browse/CDAP-4301>`__ -
  Added support for CDH5.5

- `CDAP-4392 <https://cdap.atlassian.net/browse/CDAP-4392>`__ -
  Upgraded netty-http dependency in CDAP to 0.14.0.

- `CDAP-4444 <https://cdap.atlassian.net/browse/CDAP-4444>`__ -
  Make ``xmllint`` dependency optional and allow setting variables to skip configuration
  file parsing.

- `CDAP-4453 <https://cdap.atlassian.net/browse/CDAP-4453>`__ -
  Added a schema validation |---| for sources, transforms, and sinks |---| that will
  validate the pipeline stages schema during deployment, and report any issues.

- `CDAP-4518 <https://cdap.atlassian.net/browse/CDAP-4518>`__ -
  CDAP Master service will now log important configuration settings on startup.

- `CDAP-4523 <https://cdap.atlassian.net/browse/CDAP-4523>`__ -
  Added the config setting ``master.startup.checks.enabled`` to control whether CDAP
  Master startup checks are run or not.

- `CDAP-4536 <https://cdap.atlassian.net/browse/CDAP-4536>`__ -
  Improved the installation experience by adding to the CDAP Master service checks of
  pre-requisites such as file system permissions, availability of components such as YARN
  and HBase, resource availability during startup, and to error out if any of the
  pre-requisites fail.

- `CDAP-4548 <https://cdap.atlassian.net/browse/CDAP-4548>`__ -
  Added a config setting 'master.collect.app.containers.log' that can be set to 'false' to
  disable streaming of application logs back to the CDAP Master log.

- `CDAP-4598 <https://cdap.atlassian.net/browse/CDAP-4598>`__ -
  Added an error message when a required field is not provided when configuring Hydrator
  pipeline.

Bug Fixes
---------
- `CDAP-1174 <https://cdap.atlassian.net/browse/CDAP-1174>`__ -
  Prefix start script functions with ``'cdap'`` to prevent namespace collisions.

- `CDAP-2470 <https://cdap.atlassian.net/browse/CDAP-2470>`__ -
  Added a check to cause a DB (source or sink) pipeline to fail during deployment if the
  table (source or sink) was not found, or if an incorrect connection string was provided.

- `CDAP-3345 <https://cdap.atlassian.net/browse/CDAP-3345>`__ -
  Fixed a bug where the TTL for datasets was incorrect; it was reduced by (a factor of
  1000) after an upgrade. After running the upgrade tool, please make sure the TTL values
  of tables are as expected.

- `CDAP-3542 <https://cdap.atlassian.net/browse/CDAP-3542>`__ -
  Fixed an issue where the failure of a program running in a workflow fork node was
  causing other programs in the same fork node to remain in the RUNNING state, even after
  the Workflow was completed.

- `CDAP-3694 <https://cdap.atlassian.net/browse/CDAP-3694>`__ -
  Fixed test failures in the PurchaseHistory, StreamConversion, and WikipediaPipeline
  example apps included in the CDAP SDK.

- `CDAP-3742 <https://cdap.atlassian.net/browse/CDAP-3742>`__ -
  Fixed a bug where certain MapReduce metrics were not being properly emitted when using
  multiple outputs.

- `CDAP-3761 <https://cdap.atlassian.net/browse/CDAP-3761>`__ -
  Fixed a problem with DBSink column names not being used to filter input record fields
  before writing to a DBSink.

- `CDAP-3807 <https://cdap.atlassian.net/browse/CDAP-3807>`__ -
  Added a fix for case sensitivity handling in DBSink.

- `CDAP-3815 <https://cdap.atlassian.net/browse/CDAP-3815>`__ -
  Fixed an issue where the regex filter for S3 Batch Source wasn't getting applied correctly.

- `CDAP-3861 <https://cdap.atlassian.net/browse/CDAP-3861>`__ -
  Fixed an issue about stopping all dependent services when a service is stopped.

- `CDAP-3900 <https://cdap.atlassian.net/browse/CDAP-3900>`__ -
  Fixed a bug when querying for logs of deleted program runs.

- `CDAP-3902 <https://cdap.atlassian.net/browse/CDAP-3902>`__ -
  Fixed a problem with dataset performance degradation because of making multiple remote
  calls for each "get dataset" request.

- `CDAP-3924 <https://cdap.atlassian.net/browse/CDAP-3924>`__ -
  Fixed QueryClient to work against HTTPS.

- `CDAP-4000 <https://cdap.atlassian.net/browse/CDAP-4000>`__ -
  Fixed an issue where a stream that has a view could not be deleted cleanly.

- `CDAP-4067 <https://cdap.atlassian.net/browse/CDAP-4067>`__ -
  Fixed an issue where socket connections to the TransactionManager were not being closed.

- `CDAP-4092 <https://cdap.atlassian.net/browse/CDAP-4092>`__ -
  Fixes an issue that causes worker threads to go into an infinite recursion while
  exceptions are being thrown in channel handlers.

- `CDAP-4112 <https://cdap.atlassian.net/browse/CDAP-4112>`__ -
  Fixed a bug that prevented applications from using HBase directly.

- `CDAP-4119 <https://cdap.atlassian.net/browse/CDAP-4119>`__ -
  Fixed a problem where when CDAP Master switched from active to standby, the programs
  that were running were marked as failed.

- `CDAP-4240 <https://cdap.atlassian.net/browse/CDAP-4240>`__ -
  Fixed a problem in the CLI command used to load an artifact, where the wrong artifact name
  and version was used if the artifact name ends with a number.

- `CDAP-4294 <https://cdap.atlassian.net/browse/CDAP-4294>`__ -
  Fixed a problem where plugins from another namespace were visible when creating an
  application using a system artifact.

- `CDAP-4316 <https://cdap.atlassian.net/browse/CDAP-4316>`__ -
  Fixed a problem with the CLI attempting to connect to CDAP when the hostname and port
  were incorrect.

- `CDAP-4366 <https://cdap.atlassian.net/browse/CDAP-4366>`__ -
  Improved error message when stream views were not found.

- `CDAP-4393 <https://cdap.atlassian.net/browse/CDAP-4393>`__ -
  Fixed an issue where tags search were failing for certain tags.

- `CDAP-4141 <https://cdap.atlassian.net/browse/CDAP-4141>`__ -
  Fixed node.js version checking for the ``cdap sdk`` script in the CDAP SDK.

- `CDAP-4373 <https://cdap.atlassian.net/browse/CDAP-4373>`__ -
  Fixed a problem that prevented MapReduce jobs from being run when the Resource Manager
  switches from active to standby in a Kerberos-enabled HA cluster.

- `CDAP-4384 <https://cdap.atlassian.net/browse/CDAP-4384>`__ -
  Fixed an issue that prevents streams from being read in HA HDFS mode.

- `CDAP-4526 <https://cdap.atlassian.net/browse/CDAP-4526>`__ -
  Fixed init scripts to print service status when stopped.

- `CDAP-4534 <https://cdap.atlassian.net/browse/CDAP-4534>`__ -
  Added configuration 'router.bypass.auth.regex' to exempt certain URLs from authentication.

- `CDAP-4539 <https://cdap.atlassian.net/browse/CDAP-4539>`__ -
  Fixed a problem in the init scripts that forced ``cdap-kafka-server``, ``cdap-router``,
  and ``cdap-auth-server`` to have the Hive client installed.

- `CDAP-4678 <https://cdap.atlassian.net/browse/CDAP-4678>`__ -
  Fixed an issue where the logs and history list on a Hydrator pipeline view was not
  updating on new runs.

Deprecated and Removed Features
-------------------------------

- See the `CDAP 3.3.0 Javadocs <http://docs.cask.co/cdap/3.3.0/en/reference-manual/javadocs/index.html#javadocs>`__
  for a list of deprecated and removed APIs.

- `CDAP-2481 <https://cdap.atlassian.net/browse/CDAP-2481>`__ -
  Removed a deprecated endpoint to retrieve the status of a currently running node in a workflow.

- `CDAP-2943 <https://cdap.atlassian.net/browse/CDAP-2943>`__ -
  Removed the deprecated builder-style Flow API.

- `CDAP-4128 <https://cdap.atlassian.net/browse/CDAP-4128>`__ -
  Deprecated the Script transform.

- `CDAP-4217 <https://cdap.atlassian.net/browse/CDAP-4217>`__ -
  Deprecated createDataSchedule and createTimeSchedule methods in Schedules class and removed
  deprecated Schedule constructor.

- `CDAP-4251 <https://cdap.atlassian.net/browse/CDAP-4251>`__ -
  Removed deprecated fluent style API for Flow configuration. The only supported API is now the configurer style.

Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.3.0/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets
  <http://docs.cask.co/cdap/3.3.0/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.


`Release 3.2.1 <http://docs.cask.co/cdap/3.2.1/index.html>`__
=============================================================

New Features
------------

- `CDAP-3951 <https://cdap.atlassian.net/browse/CDAP-3951>`__ -
  Added the ability for S3 batch sources and sinks to set additional file system properties.

Improvements
------------

- `CDAP-3870 <https://cdap.atlassian.net/browse/CDAP-3870>`__ -
  Added logging and metrics support for *Script*, *ScriptFilter*, and *Validator* transforms.

- `CDAP-3939 <https://cdap.atlassian.net/browse/CDAP-3939>`__ -
  Improved artifact and application deployment failure handling.

Bug Fixes
---------

- `CDAP-3342 <https://cdap.atlassian.net/browse/CDAP-3342>`__ -
  Fixed a problem with the CDAP SDK unable to start on certain Windows machines by updating
  the Hadoop native library in CDAP with a version that does not have a dependency on a
  debug version of the Microsoft ``msvcr100.dll``.

- `CDAP-3815 <https://cdap.atlassian.net/browse/CDAP-3815>`__ -
  Fixed an issue where the regex filter for S3 batch sources wasn't being applied correctly.

- `CDAP-3829 <https://cdap.atlassian.net/browse/CDAP-3829>`__ -
  Fixed snapshot sinks so that the data is explorable as a ``PartitionedFileSet``.

- `CDAP-3833 <https://cdap.atlassian.net/browse/CDAP-3833>`__ -
  Fixed snapshot sinks so that they can be read safely.

- `CDAP-3859 <https://cdap.atlassian.net/browse/CDAP-3859>`__ -
  Fixed a compilation error in the Maven application archetype.

- `CDAP-3860 <https://cdap.atlassian.net/browse/CDAP-3860>`__ -
  Fixed a bug where plugins, packaged in the same artifact as an application class, could not be used by that application class.

- `CDAP-3891 <https://cdap.atlassian.net/browse/CDAP-3891>`__ -
  Updated the documentation to remove references to application templates and adaptors that were removed as of CDAP 3.2.0.

- `CDAP-3949 <https://cdap.atlassian.net/browse/CDAP-3949>`__ -
  Fixed a problem with running certain examples on Linux systems by increasing the maximum
  Java heap size of the Standalone SDK on Linux systems to 2048m.

- `CDAP-3961 <https://cdap.atlassian.net/browse/CDAP-3961>`__ -
  Fixed a missing dependency on ``cdap-hbase-compat-1.1`` package in the CDAP Master package.

Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.2.1/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets
  <http://docs.cask.co/cdap/3.2.1/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.


`Release 3.2.0 <http://docs.cask.co/cdap/3.2.0/index.html>`__
=============================================================

New Features
------------

- `CDAP-2556 <https://cdap.atlassian.net/browse/CDAP-2556>`__ -
  Added support for HBase1.1.

- `CDAP-2666 <https://cdap.atlassian.net/browse/CDAP-2666>`__ -
  Added a new API for creating an application from an artifact.

- `CDAP-2756 <https://cdap.atlassian.net/browse/CDAP-2756>`__ -
  Added the ability to write to multiple outputs from a MapReduce job.

- `CDAP-2757 <https://cdap.atlassian.net/browse/CDAP-2757>`__ -
  Added the ability to dynamically write to multiple partitions of a PartitionedFileSet
  dataset as the output of a MapReduce job.

- `CDAP-3253 <https://cdap.atlassian.net/browse/CDAP-3253>`__ -
  Added a Stream and Dataset Widget to the CDAP UI.

- `CDAP-3390 <https://cdap.atlassian.net/browse/CDAP-3390>`__ -
  Added stream views, enabling reading from a single stream using various formats and
  schemas.

- `CDAP-3476 <https://cdap.atlassian.net/browse/CDAP-3476>`__ -
  Added a Validator Transform that can be used to validate records based on a set of
  available validators and configured to write invalid records to an error
  dataset.

- `CDAP-3516 <https://cdap.atlassian.net/browse/CDAP-3516>`__ -
  Added a service to manage the metadata of CDAP entities.

- `CDAP-3518 <https://cdap.atlassian.net/browse/CDAP-3518>`__ -
  Added the publishing of metadata change notifications to Apache Kafka.

- `CDAP-3519 <https://cdap.atlassian.net/browse/CDAP-3519>`__ -
  Added the ability to compute lineage of a CDAP dataset or stream in a given time window.

- `CDAP-3520 <https://cdap.atlassian.net/browse/CDAP-3520>`__ -
  Added RESTful APIs for adding/retrieving/deleting of metadata for apps/programs/datasets/streams.

- `CDAP-3521 <https://cdap.atlassian.net/browse/CDAP-3521>`__ -
  Added the ability to record a dataset or stream access by a CDAP program.

- `CDAP-3522 <https://cdap.atlassian.net/browse/CDAP-3522>`__ -
  Added the capability to search CDAP entities based on their metadata.

- `CDAP-3523 <https://cdap.atlassian.net/browse/CDAP-3523>`__ -
  Added RESTful APIs for searching CDAP entities based on business metadata.

- `CDAP-3527 <https://cdap.atlassian.net/browse/CDAP-3527>`__ -
  Added a data store to manage business metadata of CDAP entities.

- `CDAP-3549 <https://cdap.atlassian.net/browse/CDAP-3549>`__ -
  Added SSH port forwarding to the CDAP virtual machine.

- `CDAP-3556 <https://cdap.atlassian.net/browse/CDAP-3556>`__ -
  Added a data store for recording data accesses by CDAP programs and computing lineage.

- `CDAP-3590 <https://cdap.atlassian.net/browse/CDAP-3590>`__ -
  Added the ability to write to multiple sinks in ETL real-time and batch applications.

- `CDAP-3591 <https://cdap.atlassian.net/browse/CDAP-3591>`__ -
  Added the ability for real-time ETL pipelines to write to multiple sinks.

- `CDAP-3592 <https://cdap.atlassian.net/browse/CDAP-3592>`__ -
  Added the ability for batch ETL pipelines to write to multiple sinks.

- `CDAP-3626 <https://cdap.atlassian.net/browse/CDAP-3626>`__ -
  For the CSV and TSV stream formats, a "mapping" setting can now be specified, mapping
  stream event columns to schema columns.

- `CDAP-3693 <https://cdap.atlassian.net/browse/CDAP-3693>`__ -
  Added support for CDAP to work with HDP 2.3.


Improvements
------------

- `CDAP-1914 <https://cdap.atlassian.net/browse/CDAP-1914>`__ -
  Added documentation of the RESTful endpoint to retrieve the properties of a stream.

- `CDAP-2514 <https://cdap.atlassian.net/browse/CDAP-2514>`__ -
  Added an interface to load a file into a stream from the CDAP UI.

- `CDAP-2809 <https://cdap.atlassian.net/browse/CDAP-2809>`__ -
  The CDAP UI "Errors" pop-up in the main screen now displays the time and date for each
  error.

- `CDAP-2872 <https://cdap.atlassian.net/browse/CDAP-2872>`__ -
  Updated the Cloudera Manager CSD to use support for logback.

- `CDAP-2950 <https://cdap.atlassian.net/browse/CDAP-2950>`__ -
  Cleaned up the messages shown in the errors dropdown in the CDAP UI.

- `CDAP-3147 <https://cdap.atlassian.net/browse/CDAP-3147>`__ -
  Added a CDAP CLI command to stop a workflow.

- `CDAP-3179 <https://cdap.atlassian.net/browse/CDAP-3179>`__ -
  Added support for upgrading the Hadoop distribution or the HBase version that CDAP is
  running on.

- `CDAP-3257 <https://cdap.atlassian.net/browse/CDAP-3257>`__ -
  Revised the documentation of the file ``cdap-default.xml``, removed properties no longer
  in use, and corrected discrepancies between the documentation and the shipped XML
  file.

- `CDAP-3270 <https://cdap.atlassian.net/browse/CDAP-3270>`__ -
  Improved the help provided in the CDAP CLI for the setting of stream formats.

- `CDAP-3275 <https://cdap.atlassian.net/browse/CDAP-3275>`__ -
  Upgraded netty-http version to 0.12.0.

- `CDAP-3282 <https://cdap.atlassian.net/browse/CDAP-3282>`__ -
  Added a HTTP RESTful API to update the application configuration and artifact version.

- `CDAP-3332 <https://cdap.atlassian.net/browse/CDAP-3332>`__ -
  Added a "clear" button in the CDAP UI for cases where a user decides to not used a
  pre-populated schema.

- `CDAP-3351 <https://cdap.atlassian.net/browse/CDAP-3351>`__ -
  Defined a directory structure to be used for predefined applications.

- `CDAP-3357 <https://cdap.atlassian.net/browse/CDAP-3357>`__ -
  Added documentation in the source code on adding new commands and completers to the CDAP CLI.

- `CDAP-3393 <https://cdap.atlassian.net/browse/CDAP-3393>`__ -
  In the CDAP UI, added visualization for Workflow tokens in Workflows.

- `CDAP-3419 <https://cdap.atlassian.net/browse/CDAP-3419>`__ -
  HBaseQueueDebugger now shows the minimum queue event transaction write pointer both for
  each queue and for all queues.

- `CDAP-3443 <https://cdap.atlassian.net/browse/CDAP-3443>`__ -
  Added an example cdap-env.sh to the shipped packages.

- `CDAP-3464 <https://cdap.atlassian.net/browse/CDAP-3464>`__ -
  Added an example in the documentation explaining how to prune invalid transactions from
  the transaction manager.

- `CDAP-3490 <https://cdap.atlassian.net/browse/CDAP-3490>`__ -
  Modified the CDAP upgrade tool to delete all adapters and the ETLBatch and ETLRealtime
  ApplicationTemplates.

- `CDAP-3495 <https://cdap.atlassian.net/browse/CDAP-3495>`__ -
  Added the ability to persist the runtime arguments with which a program was run.

- `CDAP-3550 <https://cdap.atlassian.net/browse/CDAP-3550>`__ -
  Added support for writing to Amazon S3 in Avro and Parquet formats from batch ETL
  applications.

- `CDAP-3564 <https://cdap.atlassian.net/browse/CDAP-3564>`__ -
  Updated CDAP to use Tephra 0.6.2.

- `CDAP-3610 <https://cdap.atlassian.net/browse/CDAP-3610>`__ -
  Updated the transaction debugger client to print checkpoint information.

Bug Fixes
---------

- `CDAP-1697 <https://cdap.atlassian.net/browse/CDAP-1697>`__ -
  Fixed an issue where failed dataset operations via Explore queries did not invalidate
  the associated transaction.

- `CDAP-1864 <https://cdap.atlassian.net/browse/CDAP-1864>`__ -
  Fixed a problem where users got an incorrect message while creating a dataset in a
  non-existent namespace.

- `CDAP-1892 <https://cdap.atlassian.net/browse/CDAP-1892>`__ -
  Fixed a problem with services returning the same message for all failures.

- `CDAP-1984 <https://cdap.atlassian.net/browse/CDAP-1984>`__ -
  Fixed a problem where a dataset could be created in a non-existent namespace in
  standalone mode.

- `CDAP-2428 <https://cdap.atlassian.net/browse/CDAP-2428>`__ -
  Fixed a problem with the CDAP CLI creating file logs.

- `CDAP-2521 <https://cdap.atlassian.net/browse/CDAP-2521>`__ -
  Fixed a problem with the CDAP CLI not auto-completing when setting a stream format.

- `CDAP-2785 <https://cdap.atlassian.net/browse/CDAP-2785>`__ -
  Fixed a problem with the CDAP UI of buttons staying 'in focus' after clicking.

- `CDAP-2809 <https://cdap.atlassian.net/browse/CDAP-2809>`__ -
  The CDAP UI "Errors" pop-up in the main screen now displays the time and date for each error.

- `CDAP-2892 <https://cdap.atlassian.net/browse/CDAP-2892>`__ -
  Fixed a problem with schedules not being deployed in suspended mode.

- `CDAP-3014 <https://cdap.atlassian.net/browse/CDAP-3014>`__ -
  Fixed a problem where failure of a spark node would cause a workflow to restart indefinitely.

- `CDAP-3073 <https://cdap.atlassian.net/browse/CDAP-3073>`__ -
  Fixed an issue with the Standalone CDAP process periodically crashing with Out-of-Memory
  errors when writing to an Oracle table.

- `CDAP-3101 <https://cdap.atlassian.net/browse/CDAP-3101>`__ -
  Fixed a problem with workflow runs not getting scheduled due to Quartz exceptions.

- `CDAP-3121 <https://cdap.atlassian.net/browse/CDAP-3121>`__ -
  Fixed a problem with discrepancies between the documentation and the defaults actually used by CDAP.

- `CDAP-3200 <https://cdap.atlassian.net/browse/CDAP-3200>`__ -
  Fixed a problem in the CDAP UI with the clone button in an incorrect position when using Firefox.

- `CDAP-3201 <https://cdap.atlassian.net/browse/CDAP-3201>`__ -
  Fixed a problem in the CDAP UI with an incorrect tabbing order when using Firefox.

- `CDAP-3219 <https://cdap.atlassian.net/browse/CDAP-3219>`__ -
  Fixed a problem when specifying the HBase version using the HBASE_VERSION environment variable.

- `CDAP-3233 <https://cdap.atlassian.net/browse/CDAP-3233>`__ -
  Fixed a problem in the CDAP UI error pop-ups not having a default focus on a button.

- `CDAP-3243 <https://cdap.atlassian.net/browse/CDAP-3243>`__ -
  Fixed a problem in the CDAP UI with the default schema shown for streams.

- `CDAP-3260 <https://cdap.atlassian.net/browse/CDAP-3260>`__ -
  Fixed a problem in the CDAP UI with scrolling on the namespaces dropdown on certain pages.

- `CDAP-3261 <https://cdap.atlassian.net/browse/CDAP-3261>`__ -
  Fixed a problem on Distributed CDAP with the serializing of the metadata artifact
  causing a stack overflow.

- `CDAP-3305 <https://cdap.atlassian.net/browse/CDAP-3305>`__ -
  Fixed a problem in the CDAP UI not warning users if they exit or close their browser without saving.

- `CDAP-3313 <https://cdap.atlassian.net/browse/CDAP-3313>`__ -
  Fixed a problem in the CDAP UI with refreshing always returning to the overview page.

- `CDAP-3326 <https://cdap.atlassian.net/browse/CDAP-3326>`__ -
  Fixed a problem with the table batch source requiring a row key to be set.

- `CDAP-3343 <https://cdap.atlassian.net/browse/CDAP-3343>`__ -
  Fixed a problem with the application deployment for apps that contain Spark.

- `CDAP-3349 <https://cdap.atlassian.net/browse/CDAP-3349>`__ -
  Fixed a problem with the display of ETL application metrics in the CDAP UI.

- `CDAP-3355 <https://cdap.atlassian.net/browse/CDAP-3355>`__ -
  Fixed a problem in the CDAP examples with the use of a runtime argument, ``min.pages.threshold``.

- `CDAP-3362 <https://cdap.atlassian.net/browse/CDAP-3362>`__ -
  Fixed a problem with the ``logback-container.xml`` not being copied into master services.

- `CDAP-3374 <https://cdap.atlassian.net/browse/CDAP-3374>`__ -
  Fixed a problem with warning messages in the logs indicating that programs were running
  that actually were not running.

- `CDAP-3376 <https://cdap.atlassian.net/browse/CDAP-3376>`__ -
  Fixed a problem with being unable to deploy the SparkPageRank example application on a cluster.

- `CDAP-3386 <https://cdap.atlassian.net/browse/CDAP-3386>`__ -
  Fixed a problem with the Spark classes not being found when running a Spark program
  through a Workflow in Distributed CDAP on HDP 2.2.

- `CDAP-3394 <https://cdap.atlassian.net/browse/CDAP-3394>`__ -
  Fixed a problem with the deployment of applications through the CDAP UI.

- `CDAP-3399 <https://cdap.atlassian.net/browse/CDAP-3399>`__ -
  Fixed a problem with the SparkPageRankApp example spawning multiple containers in
  distributed mode due to its number of services.

- `CDAP-3400 <https://cdap.atlassian.net/browse/CDAP-3400>`__ -
  Fixed an issue with warning messages about the notification system every time the CDAP
  Standalone is restarted.

- `CDAP-3408 <https://cdap.atlassian.net/browse/CDAP-3408>`__ -
  Fixed a problem with running the CDAP Explore Service on CDH 5.[2,3].

- `CDAP-3432 <https://cdap.atlassian.net/browse/CDAP-3432>`__ -
  Fixed a bug where connecting with a certain namespace from the CLI would not immediately
  display that namespace in the CLI prompt.

- `CDAP-3435 <https://cdap.atlassian.net/browse/CDAP-3435>`__ -
  Fixed an issue where the program status was shown as running even after it is stopped.

- `CDAP-3442 <https://cdap.atlassian.net/browse/CDAP-3442>`__ -
  Fixed a problem that caused application creation to fail if a config setting was given
  to an application that does not use a config.

- `CDAP-3449 <https://cdap.atlassian.net/browse/CDAP-3449>`__ -
  Fixed a problem with the readless increment co-processor not handling multiple readless
  increment columns in the same row.

- `CDAP-3452 <https://cdap.atlassian.net/browse/CDAP-3452>`__ -
  Fixed a problem that prevented explore service working on clusters with secure hive 0.14.

- `CDAP-3458 <https://cdap.atlassian.net/browse/CDAP-3458>`__ -
  Fixed a problem where streams events that had already been processed were re-processed in flows.

- `CDAP-3470 <https://cdap.atlassian.net/browse/CDAP-3470>`__ -
  Fixed an issue with error messages being logged during a master process restart.

- `CDAP-3472 <https://cdap.atlassian.net/browse/CDAP-3472>`__ -
  Fixed the error message returned when trying to stop a program started by a workflow.

- `CDAP-3473 <https://cdap.atlassian.net/browse/CDAP-3473>`__ -
  Fixed a problem with a workflow failure not updating a run record for the inner program.

- `CDAP-3530 <https://cdap.atlassian.net/browse/CDAP-3530>`__ -
  Fixed a problem with the CDAP UI performance when rendering flow diagrams with a large number of nodes.

- `CDAP-3563 <https://cdap.atlassian.net/browse/CDAP-3563>`__ -
  Removed faulty and unused metrics around CDAP file resource usage.

- `CDAP-3574 <https://cdap.atlassian.net/browse/CDAP-3574>`__ -
  Fix an issue with Explore not working on HDP Hive 0.12.

- `CDAP-3603 <https://cdap.atlassian.net/browse/CDAP-3603>`__ -
  Fixed an issue with configuration properties for ETL Transforms being validated at
  runtime instead of when an application is created.

- `CDAP-3618 <https://cdap.atlassian.net/browse/CDAP-3618>`__ -
  Fix a problem where suspended schedules were lost when CDAP master was restarted.

- `CDAP-3660 <https://cdap.atlassian.net/browse/CDAP-3660>`__ -
  Fixed and issue where the Hadoop filesystem object was getting instantiated before the
  Kerberos keytab login was completed, leading to CDAP processes failing after the initial
  ticket expired.

- `CDAP-3700 <https://cdap.atlassian.net/browse/CDAP-3700>`__ -
  Fixed an issue with the log saver having numerous open connections to HBase, causing it
  to go Out-of-Memory.

- `CDAP-3711 <https://cdap.atlassian.net/browse/CDAP-3711>`__ -
  Fixed an issue that prevented the downloading of Explore results on a secure cluster.

- `CDAP-3713 <https://cdap.atlassian.net/browse/CDAP-3713>`__ -
  Fixed an issue where certain RESTful APIs were not returning appropriate error messages
  for internal server errors.

- `CDAP-3716 <https://cdap.atlassian.net/browse/CDAP-3716>`__ -
  Fixed a possible deadlock when CDAP master is restarted with an existing app running on a cluster.

API Changes
-----------

- `CDAP-2763 <https://cdap.atlassian.net/browse/CDAP-2763>`__ -
  Added RESTful APIs for managing artifacts.

- `CDAP-2956 <https://cdap.atlassian.net/browse/CDAP-2956>`__ -
  Deprecated the existing API for configuring a workflow action, replacing it with a
  simpler API.

- `CDAP-3063 <https://cdap.atlassian.net/browse/CDAP-3063>`__ -
  Added CLI commands for managing artifacts.

- `CDAP-3064 <https://cdap.atlassian.net/browse/CDAP-3064>`__ -
  Added an ArtifactClient to interact with Artifact HTTP RESTful APIs.

- `CDAP-3283 <https://cdap.atlassian.net/browse/CDAP-3283>`__ -
  Added artifact information to Application RESTful APIs and the means to filter
  applications by artifact name and version.

- `CDAP-3324 <https://cdap.atlassian.net/browse/CDAP-3324>`__ -
  Added a RESTful API for creating an application from an artifact.

- `CDAP-3367 <https://cdap.atlassian.net/browse/CDAP-3367>`__ -
  Added the ability to delete an artifact.

- `CDAP-3488 <https://cdap.atlassian.net/browse/CDAP-3488>`__ -
  Changed the ETLBatchTemplate from an ApplicationTemplate to an Application.

- `CDAP-3535 <https://cdap.atlassian.net/browse/CDAP-3535>`__ -
  Added an API for programs to retrieve their application specification at runtime.

- `CDAP-3554 <https://cdap.atlassian.net/browse/CDAP-3554>`__ -
  Changed the plugin types from 'source' to either 'batchsource' or 'realtimesource', and
  from 'sink' to either 'batchsink' or 'realtimesink' to reflect that the plugins
  implement different interfaces.

- `CDAP-1554 <https://cdap.atlassian.net/browse/CDAP-1554>`__ -
  Moved constants for default and system namespaces from Common to Id.

- `CDAP-3388 <https://cdap.atlassian.net/browse/CDAP-3388>`__ -
  Added interfaces to ``cdap-spi`` that abstract StreamEventRecordFormat (and dependent
  interfaces) so users can extend the ``cdap-spi`` interfaces.

- `CDAP-3583 <https://cdap.atlassian.net/browse/CDAP-3583>`__ -
  Added a RESTful API for retrieving the metadata associated with a particular run of a
  CDAP program.

- `CDAP-3632 <https://cdap.atlassian.net/browse/CDAP-3632>`__ -
  Added a RESTful API for computing lineage of a CDAP dataset or stream.

Deprecated and Removed Features
-------------------------------

- See the `CDAP 3.2.0 Javadocs
  <http://docs.cask.co/cdap/3.2.0/en/reference-manual/javadocs/index.html#javadocs>`__
  for a list of deprecated and removed APIs.

- `CDAP-2667 <https://cdap.atlassian.net/browse/CDAP-2667>`__ -
  Removed application templates and adapters RESTful APIs, as these templates and adapters
  have been replaced with applications that can be controlled with the
  `Lifecycle HTTP RESTful API
  <http://docs.cask.co/cdap/3.2.0/en/reference-manual/http-restful-api/lifecycle.html#http-restful-api-lifecycle>`__.

- `CDAP-2951 <https://cdap.atlassian.net/browse/CDAP-2951>`__ -
  Removed deprecated methods in cdap-client.

- `CDAP-3596 <https://cdap.atlassian.net/browse/CDAP-3596>`__ -
  Replaced the ETL ApplicationTemplates with the new ETL Applications.

Known Issues
------------
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, debug logs from MapReduce programs are not
  available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.2.0/en/admin-manual/installation/index.html#installation-index>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__ -
  Metrics for `FileSets
  <http://docs.cask.co/cdap/3.2.0/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__
  can show zero values even if there is
  data present, because FileSets do not emit metrics (`CDAP-587
  <https://cdap.atlassian.net/browse/CDAP-587>`).

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.

- `CDAP-3262 <https://cdap.atlassian.net/browse/CDAP-3262>`__ -
  For Microsoft Windows, the Standalone CDAP scripts can fail when used with a JAVA_HOME
  that is defined as a path with spaces in it. A workaround is to use a definition of
  JAVA_HOME that does not include spaces, such as ``C:\PROGRA~1\Java\jdk1.7.0_79\bin`` or
  ``C:\ProgramData\Oracle\Java\javapath``.

- `CDAP-3492 <https://cdap.atlassian.net/browse/CDAP-3492>`__ -
  In the CDAP CLI, executing ``select *`` from a dataset with many fields generates an error.

- `CDAP-3641 <https://cdap.atlassian.net/browse/CDAP-3641>`__ -
  A RESTful API call to retrieve workflow statistics hangs if units (such as "s" for
  seconds) are not provided as part of the query.

- `CDAP-3697 <https://cdap.atlassian.net/browse/CDAP-3697>`__ -
  CDAP Explore is broken on secure CDH 5.1.

- `CDAP-3698 <https://cdap.atlassian.net/browse/CDAP-3698>`__ -
  CDAP Explore is unable to get a delegation token while fetching next results on HDP2.0.

- `CDAP-3749 <https://cdap.atlassian.net/browse/CDAP-3749>`__ -
  The DBSource plugin does not allow a username with an empty password.

- `CDAP-3750 <https://cdap.atlassian.net/browse/CDAP-3750>`__ -
  If a table schema contains a field name that is a reserved word in the Hive DDL, ``'enable explore'`` fails.

- `CDAP-3819 <https://cdap.atlassian.net/browse/CDAP-3819>`__ -
  The Cassandra source does not handles spaces properly in column fields which require a comma-separated list.


`Release 3.1.0 <http://docs.cask.co/cdap/3.1.0/index.html>`__
=============================================================

New Features
------------

**MapR 4.1 Support, HDP 2.2 Support, CDH 5.4 Support**

- `CDAP-1614 <https://cdap.atlassian.net/browse/CDAP-1614>`__ -
  Added HBase 1.0 support.

- `CDAP-2318 <https://cdap.atlassian.net/browse/CDAP-2318>`__ -
  Made CDAP work on the HDP 2.2 distribution.

- `CDAP-2786 <https://cdap.atlassian.net/browse/CDAP-2786>`__ -
  Added support to CDAP 3.1.0 for the MapR 4.1 distro.

- `CDAP-2798 <https://cdap.atlassian.net/browse/CDAP-2798>`__ -
  Added Hive 0.14 support.

- `CDAP-2801 <https://cdap.atlassian.net/browse/CDAP-2801>`__ -
  Added CDH 5.4 Hive 1.1 support.

- `CDAP-2836 <https://cdap.atlassian.net/browse/CDAP-2836>`__ -
  Added support for restart of specific CDAP System Services Instances.

- `CDAP-2853 <https://cdap.atlassian.net/browse/CDAP-2853>`__ -
  Completed certification process for MapR on CDAP.

- `CDAP-2879 <https://cdap.atlassian.net/browse/CDAP-2879>`__ -
  Added Hive 1.0 in Standalone.

- `CDAP-2881 <https://cdap.atlassian.net/browse/CDAP-2881>`__ -
  Added support for HDP 2.2.x.

- `CDAP-2891 <https://cdap.atlassian.net/browse/CDAP-2891>`__ -
  Documented cdap-env.sh and settings OPTS for HDP 2.2.

- `CDAP-2898 <https://cdap.atlassian.net/browse/CDAP-2898>`__ -
  Added Hive 1.1 in Standalone.

- `CDAP-2953 <https://cdap.atlassian.net/browse/CDAP-2953>`__ -
  Added HiveServer2 support in a secure cluster.


**Spark**

- `CDAP-344 <https://cdap.atlassian.net/browse/CDAP-344>`__ -
  Users can now run Spark in distributed mode.

- `CDAP-1993 <https://cdap.atlassian.net/browse/CDAP-1993>`__ -
  Added ability to manipulate the SparkConf.

- `CDAP-2700 <https://cdap.atlassian.net/browse/CDAP-2700>`__ -
  Added the ability to Spark programs of discovering CDAP services in distributed mode.

- `CDAP-2701 <https://cdap.atlassian.net/browse/CDAP-2701>`__ -
  Spark programs are able to collect Metrics in distributed mode.

- `CDAP-2703 <https://cdap.atlassian.net/browse/CDAP-2703>`__ -
  Users are able to collect/view logs from Spark programs in distributed mode.

- `CDAP-2705 <https://cdap.atlassian.net/browse/CDAP-2705>`__ -
  Added examples, guides and documentation for Spark in distributed mode. LogAnalysis
  application demonstrating parallel execution of the Spark and MapReduce programs using
  Workflows.

- `CDAP-2923 <https://cdap.atlassian.net/browse/CDAP-2923>`__ -
  Added support for the WorkflowToken in the Spark programs.

- `CDAP-2936 <https://cdap.atlassian.net/browse/CDAP-2936>`__ -
  Spark program can now specify resources usage for driver and executor process in distributed mode.


**Workflows**

- `CDAP-1983 <https://cdap.atlassian.net/browse/CDAP-1983>`__ -
  Added example application for processing and analyzing Wikipedia data using Workflows.

- `CDAP-2709 <https://cdap.atlassian.net/browse/CDAP-2709>`__ -
  Added ability to add generic keys to the WorkflowToken.

- `CDAP-2712 <https://cdap.atlassian.net/browse/CDAP-2712>`__ -
  Added ability to update the WorkflowToken in MapReduce and Spark programs.

- `CDAP-2713 <https://cdap.atlassian.net/browse/CDAP-2713>`__ -
  Added ability to persist the WorkflowToken per run of the Workflow.

- `CDAP-2714 <https://cdap.atlassian.net/browse/CDAP-2714>`__ -
  Added ability to query the WorkflowToken for the past as well as currently running Workflow runs.

- `CDAP-2752 <https://cdap.atlassian.net/browse/CDAP-2752>`__ -
  Added ability for custom actions to access the CDAP datasets and services.

- `CDAP-2894 <https://cdap.atlassian.net/browse/CDAP-2894>`__ -
  Added an API to retreive the system properties (e.g. MapReduce counters in case of
  MapReduce program) from the WorkflowToken.

- `CDAP-2923 <https://cdap.atlassian.net/browse/CDAP-2923>`__ -
  Added support for the WorkflowToken in the Spark programs.

- `CDAP-2982 <https://cdap.atlassian.net/browse/CDAP-2982>`__ -
  Added verification that the Workflow contains all programs/custom actions with a unique name.


**Datasets**

- `CDAP-347 <https://cdap.atlassian.net/browse/CDAP-347>`__ -
  User can use datasets in beforeSubmit and afterFinish.

- `CDAP-585 <https://cdap.atlassian.net/browse/CDAP-585>`__ -
  Changes to Spark program runner to use File dataset in Spark.
  Spark programs can now use file-based datasets.

- `CDAP-2734 <https://cdap.atlassian.net/browse/CDAP-2734>`__ -
  Added PartitionedFileSet support to setting/getting properties at the Partition level.

- `CDAP-2746 <https://cdap.atlassian.net/browse/CDAP-2746>`__ -
  PartitionedFileSets now record the creation time of each partition in the metadata.

- `CDAP-2747 <https://cdap.atlassian.net/browse/CDAP-2747>`__ -
  PartitionedFileSets now index the creation time of partitions to allow selection of
  partitions that were created after a given time. Introduced BatchPartitionConsumer as a
  way to incrementally consume new data in a PartitionedFileSet.

- `CDAP-2752 <https://cdap.atlassian.net/browse/CDAP-2752>`__ -
  Added ability for custom actions to access the CDAP datasets and services.

- `CDAP-2758 <https://cdap.atlassian.net/browse/CDAP-2758>`__ -
  FileSet now support existing HDFS locations.

  Treat base paths that start with "/" as absolute in the file system. An absolute base
  path for a (Partitioned)FileSet was interpreted as relative to the namespace's data
  directory. Newly created FileSets interpret absolute base paths as absolute in the file
  system.

  Introduced a new property for (Partitioned)FileSets name "data.external". If true, the
  base path of the FileSet is assumed to be managed by some external process. That is, the
  FileSet will not attempt to create the directory, it will not delete any files when the
  FileSet is dropped or truncated, and it will not allow adding or deleting files or
  partitions. In other words, the FileSet is read-only.

- `CDAP-2784 <https://cdap.atlassian.net/browse/CDAP-2784>`__ -
  Added support to write to PartitionedFileSet Partition metadata from MapReduce.

- `CDAP-2822 <https://cdap.atlassian.net/browse/CDAP-2822>`__ -
  IndexedTable now supports scans on the indexed field.


**Metrics**

- `CDAP-2975 <https://cdap.atlassian.net/browse/CDAP-2975>`__ -
  Added pre-split FactTables.

- `CDAP-2326 <https://cdap.atlassian.net/browse/CDAP-2326>`__ -
  Added better unit-test coverage for Cube dataset.

- `CDAP-1853 <https://cdap.atlassian.net/browse/CDAP-1853>`__ -
  Metrics processor scaling no longer needs a master services restart.

- `CDAP-2844 <https://cdap.atlassian.net/browse/CDAP-2844>`__ -
  MapReduce metrics collection no longer use counters, and instead report directly to Kafka.

- `CDAP-2701 <https://cdap.atlassian.net/browse/CDAP-2701>`__ -
  Spark programs are able to collect Metrics in distributed mode.

- `CDAP-2466 <https://cdap.atlassian.net/browse/CDAP-2466>`__ -
  Added CLI for metrics search and query.

- `CDAP-2236 <https://cdap.atlassian.net/browse/CDAP-2236>`__ -
  New CDAP UI switched over to using newer search/query APIs.

- `CDAP-1998 <https://cdap.atlassian.net/browse/CDAP-1998>`__ -
  Removed deprecated Context - Query param in Metrics v3 API.


**Miscellaneous New Features**

- `CDAP-332 <https://cdap.atlassian.net/browse/CDAP-332>`__ -
  Added a Restful end-point for deleting Streams.

- `CDAP-1483 <https://cdap.atlassian.net/browse/CDAP-1483>`__ -
  QueueAdmin now uses Id.Namespace instead of simply String.

- `CDAP-1584 <https://cdap.atlassian.net/browse/CDAP-1584>`__ -
  CDAP CLI now shows the username in the CLI prompt.

- `CDAP-2139 <https://cdap.atlassian.net/browse/CDAP-2139>`__ -
  Removed a duplicate Table of Contents on the Documentation Search page.

- `CDAP-2515 <https://cdap.atlassian.net/browse/CDAP-2515>`__ -
  Added a metrics client for search and query by tags.

- `CDAP-2582 <https://cdap.atlassian.net/browse/CDAP-2582>`__ -
  Documented the licenses of the shipped CDAP UI components.

- `CDAP-2595 <https://cdap.atlassian.net/browse/CDAP-2595>`__ -
  Added data modelling of flows.

- `CDAP-2596 <https://cdap.atlassian.net/browse/CDAP-2596>`__ -
  Added data modelling of MapReduce.

- `CDAP-2617 <https://cdap.atlassian.net/browse/CDAP-2617>`__ -
  Added the capability to get logs for a given time range from CLI.

- `CDAP-2618 <https://cdap.atlassian.net/browse/CDAP-2618>`__ -
  Simplified the Cube sink configurations.

- `CDAP-2670 <https://cdap.atlassian.net/browse/CDAP-2670>`__ -
  Added Parquet sink with time partitioned file dataset.

- `CDAP-2739 <https://cdap.atlassian.net/browse/CDAP-2739>`__ -
  Added S3 batch source for ETLbatch.

- `CDAP-2802 <https://cdap.atlassian.net/browse/CDAP-2802>`__ -
  Stopped using HiveConf.ConfVars.defaultValue, to support Hive >0.13.

- `CDAP-2847 <https://cdap.atlassian.net/browse/CDAP-2847>`__ -
  Added ability to add custom filters to FileBatchSource.

- `CDAP-2893 <https://cdap.atlassian.net/browse/CDAP-2893>`__ -
  Custom Transform now parses log formats for ETL.

- `CDAP-2913 <https://cdap.atlassian.net/browse/CDAP-2913>`__ -
  Provided installation method for EMR.

- `CDAP-2915 <https://cdap.atlassian.net/browse/CDAP-2915>`__ -
  Added an SQS real-time plugin for ETL.

- `CDAP-3022 <https://cdap.atlassian.net/browse/CDAP-3022>`__ -
  Added Cloudfront format option to LogParserTransform.

- `CDAP-3032 <https://cdap.atlassian.net/browse/CDAP-3032>`__ -
  Documented TestConfiguration class usage in unit-test framework.


Improvements
------------

- `CDAP-593 <https://cdap.atlassian.net/browse/CDAP-593>`__ -
  Spark no longer determines the mode through MRConfig.FRAMEWORK_NAME.

- `CDAP-595 <https://cdap.atlassian.net/browse/CDAP-595>`__ -
  Refactored SparkRuntimeService and SparkProgramWrapper.

- `CDAP-665 <https://cdap.atlassian.net/browse/CDAP-665>`__ -
  Documentation received a product-specifc 404 Page.

- `CDAP-683 <https://cdap.atlassian.net/browse/CDAP-683>`__ -
  Changed all README files from markdown to rst format.

- `CDAP-1132 <https://cdap.atlassian.net/browse/CDAP-1132>`__ -
  Improved the CDAP Doc Search Result Sorting.

- `CDAP-1416 <https://cdap.atlassian.net/browse/CDAP-1416>`__ -
  Added links to upper level pages on Docs.

- `CDAP-1572 <https://cdap.atlassian.net/browse/CDAP-1572>`__ -
  Standardized Id classes.

- `CDAP-1583 <https://cdap.atlassian.net/browse/CDAP-1583>`__ -
  Refactored InMemoryWorkerRunner and ServiceProgramRunnner after ServiceWorkers were removed.

- `CDAP-1918 <https://cdap.atlassian.net/browse/CDAP-1918>`__ -
  Switched to using the Spark 1.3.0 release.

- `CDAP-1926 <https://cdap.atlassian.net/browse/CDAP-1926>`__ -
  Streams endpoint accept "now", "now-30s", etc., for time ranges.

- `CDAP-2007 <https://cdap.atlassian.net/browse/CDAP-2007>`__ -
  CLI output for "call service" is rendered in a copy-pastable manner.

- `CDAP-2310 <https://cdap.atlassian.net/browse/CDAP-2310>`__ -
  Kafka Source now able to apply a Schema to the Payload received.

- `CDAP-2388 <https://cdap.atlassian.net/browse/CDAP-2388>`__ -
  Added Java 8 support to CDAP.

- `CDAP-2422 <https://cdap.atlassian.net/browse/CDAP-2422>`__ -
  Removed redundant catch blocks in AdapterHttpHandler.

- `CDAP-2455 <https://cdap.atlassian.net/browse/CDAP-2455>`__ -
  Version in CDAP UI footer is dynamic.

- `CDAP-2482 <https://cdap.atlassian.net/browse/CDAP-2482>`__ -
  Reduced excessive capitalisation in documentation.

- `CDAP-2531 <https://cdap.atlassian.net/browse/CDAP-2531>`__ -
  Adapter details made available through CDAP UI.

- `CDAP-2539 <https://cdap.atlassian.net/browse/CDAP-2539>`__ -
  Added a build identifier (branch, commit) in header of Documentation HTML pages.

- `CDAP-2552 <https://cdap.atlassian.net/browse/CDAP-2552>`__ -
  Documentation Build script now flags errors.

- `CDAP-2554 <https://cdap.atlassian.net/browse/CDAP-2554>`__ -
  Documented that streams can now be deleted.

- `CDAP-2557 <https://cdap.atlassian.net/browse/CDAP-2557>`__ -
  Non-handler logic moved out of DatasetInstanceHandler.

- `CDAP-2570 <https://cdap.atlassian.net/browse/CDAP-2570>`__ -
  CLI prompt changes to 'DISCONNECTED' after CDAP is stopped.

- `CDAP-2578 <https://cdap.atlassian.net/browse/CDAP-2578>`__ -
  Ability to look at configs of created adapters.

- `CDAP-2585 <https://cdap.atlassian.net/browse/CDAP-2585>`__ -
  Use Id in cdap-client rather than Id.Namespace + String.

- `CDAP-2588 <https://cdap.atlassian.net/browse/CDAP-2588>`__ -
  Improvements to the MetricsClient APIs.

- `CDAP-2590 <https://cdap.atlassian.net/browse/CDAP-2590>`__ -
  Switching namespaces when in CDAP UI Operations screens.

- `CDAP-2620 <https://cdap.atlassian.net/browse/CDAP-2620>`__ -
  CDAP clients now use Id classes from cdap proto, instead of plain strings.

- `CDAP-2628 <https://cdap.atlassian.net/browse/CDAP-2628>`__ -
  CDAP UI: Breadcrumbs in Workflow/Mapreduce work as expected.

- `CDAP-2644 <https://cdap.atlassian.net/browse/CDAP-2644>`__ -
  In cdap-clients, no longer need to retrieve runtime arguments before starting a program.

- `CDAP-2651 <https://cdap.atlassian.net/browse/CDAP-2651>`__ -
  CDAP UI: the Namespace is made more prominent.

- `CDAP-2681 <https://cdap.atlassian.net/browse/CDAP-2681>`__ -
  CDAP UI: scrolling no longer enlarges the workflow diagram instead of scrolling through.

- `CDAP-2683 <https://cdap.atlassian.net/browse/CDAP-2683>`__ -
  CDAP UI: added a remove icons for fork and Join.

- `CDAP-2684 <https://cdap.atlassian.net/browse/CDAP-2684>`__ -
  CDAP UI: workflow diagrams are directed graphs.

- `CDAP-2688 <https://cdap.atlassian.net/browse/CDAP-2688>`__ -
  CDAP UI: added search & pagination for lists of apps and datasets.

- `CDAP-2689 <https://cdap.atlassian.net/browse/CDAP-2689>`__ -
  CDAP UI: shows which application is a part of which dataset.

- `CDAP-2691 <https://cdap.atlassian.net/browse/CDAP-2691>`__ -
  CDAP UI: added ability to delete streams.

- `CDAP-2692 <https://cdap.atlassian.net/browse/CDAP-2692>`__ -
  CDAP UI: added pagination for logs.

- `CDAP-2694 <https://cdap.atlassian.net/browse/CDAP-2694>`__ -
  CDAP UI: added a loading icon/UI element when creating an adapter.

- `CDAP-2695 <https://cdap.atlassian.net/browse/CDAP-2695>`__ -
  CDAP UI: long names of adapters are replaced by a short version ending in an ellipsis.

- `CDAP-2697 <https://cdap.atlassian.net/browse/CDAP-2697>`__ -
  CDAP UI: added tab names during adapter creation.

- `CDAP-2716 <https://cdap.atlassian.net/browse/CDAP-2716>`__ -
  CDAP UI: when creating an adapter, the tabbing order shows correctly.

- `CDAP-2733 <https://cdap.atlassian.net/browse/CDAP-2733>`__ -
  Implemented a TimeParitionedFileSet source.

- `CDAP-2811 <https://cdap.atlassian.net/browse/CDAP-2811>`__ -
  Improved Hive version detection.

- `CDAP-2921 <https://cdap.atlassian.net/browse/CDAP-2921>`__ -
  Removed backward-compatibility for pre-2.8 TPFS.

- `CDAP-2938 <https://cdap.atlassian.net/browse/CDAP-2938>`__ -
  Implemented new ETL application template creation.

- `CDAP-2983 <https://cdap.atlassian.net/browse/CDAP-2983>`__ -
  Spark program runner now calls onFailure() of the DatasetOutputCommitter.

- `CDAP-2986 <https://cdap.atlassian.net/browse/CDAP-2986>`__ -
  Spark program now are able to specify runtime arguments when reading or writing a datset.

- `CDAP-2987 <https://cdap.atlassian.net/browse/CDAP-2987>`__ -
  Added an example for Spark using datasets directly.

- `CDAP-2989 <https://cdap.atlassian.net/browse/CDAP-2989>`__ -
  Added an example for Spark using FileSets.

- `CDAP-3018 <https://cdap.atlassian.net/browse/CDAP-3018>`__ -
  Updated workflow guides for workflow token.

- `CDAP-3028 <https://cdap.atlassian.net/browse/CDAP-3028>`__ -
  Improved the system service restart endpoint to handle illegal instance IDs and "service not available".

- `CDAP-3053 <https://cdap.atlassian.net/browse/CDAP-3053>`__ -
  Added schema javadocs that explain how to write the schema to JSON.

- `CDAP-3077 <https://cdap.atlassian.net/browse/CDAP-3077>`__ -
  Add the ability in TableSink to find schema.row.field case-insensitively.

- `CDAP-3144 <https://cdap.atlassian.net/browse/CDAP-3144>`__ -
  Changed CLI command descriptions to use consistent element case.

- `CDAP-3152 <https://cdap.atlassian.net/browse/CDAP-3152>`__ -
  Refactored ETLBatch sources and sinks.

Bug Fixes
---------

- `CDAP-23 <https://cdap.atlassian.net/browse/CDAP-23>`__ -
  Fixed a problem with the DatasetFramework not loading a given dataset with the same classloader across calls.

- `CDAP-68 <https://cdap.atlassian.net/browse/CDAP-68>`__ -
  Made sure all network services in Singlenode only bind to localhost.

- `CDAP-376 <https://cdap.atlassian.net/browse/CDAP-376>`__ -
  Fixed a problem with HBaseOrderedTable never calling HTable.close().

- `CDAP-550 <https://cdap.atlassian.net/browse/CDAP-550>`__ -
  Consolidated Examples, Guides, and Tutorials styles.

- `CDAP-598 <https://cdap.atlassian.net/browse/CDAP-598>`__ -
  Fixed problems with the CDAP ClassLoading model.

- `CDAP-674 <https://cdap.atlassian.net/browse/CDAP-674>`__ -
  Fixed problems with CDAP code examples and versioning.

- `CDAP-814 <https://cdap.atlassian.net/browse/CDAP-814>`__ -
  Resolved issues in the documentation about element versus program.

- `CDAP-1042 <https://cdap.atlassian.net/browse/CDAP-1042>`__ -
  Fixed a problem with specifying dataset selection as input for Spark job.

- `CDAP-1145 <https://cdap.atlassian.net/browse/CDAP-1145>`__ -
  Fixed the PurchaseAppTest.

- `CDAP-1184 <https://cdap.atlassian.net/browse/CDAP-1184>`__ -
  Fixed a problem with the DELETE call not clearing queue metrics.

- `CDAP-1273 <https://cdap.atlassian.net/browse/CDAP-1273>`__ -
  Fixed a problem with the ProgramClassLoader getResource.

- `CDAP-1457 <https://cdap.atlassian.net/browse/CDAP-1457>`__ -
  Fixed a memory leak of user class after running Spark program.

- `CDAP-1552 <https://cdap.atlassian.net/browse/CDAP-1552>`__ -
  Fixed a problem with Mapreduce progress metrics not being interpolated.

- `CDAP-1868 <https://cdap.atlassian.net/browse/CDAP-1868>`__ -
  Fixed a problem with Java Client and CLI not setting set dataset properties on existing datasets.

- `CDAP-1873 <https://cdap.atlassian.net/browse/CDAP-1873>`__ -
  Fixed a problem with warnings and errors when CDAP-Master starts up.

- `CDAP-1967 <https://cdap.atlassian.net/browse/CDAP-1967>`__ -
  Fixed a problem with CDAP-Master failing to start up due to conflicting dependencies.

- `CDAP-1976 <https://cdap.atlassian.net/browse/CDAP-1976>`__ -
  Fixed a problem with examples not following the same pattern.

- `CDAP-1988 <https://cdap.atlassian.net/browse/CDAP-1988>`__ -
  Fixed a problem with creating a Dataset through REST API failing if no properties are provided.

- `CDAP-2081 <https://cdap.atlassian.net/browse/CDAP-2081>`__ -
  Fixed a problem with StreamSizeSchedulerTest failing randomly.

- `CDAP-2140 <https://cdap.atlassian.net/browse/CDAP-2140>`__ -
  Fixed a problem with the CDAP UI not showing system service status when system services are down.

- `CDAP-2177 <https://cdap.atlassian.net/browse/CDAP-2177>`__ -
  Fixed a problem with Enable and Fix LogSaverPluginTest.

- `CDAP-2208 <https://cdap.atlassian.net/browse/CDAP-2208>`__ -
  Fixed a problem with CDAP-Explore service failing on wrapped indexedTable with Avro (specific record) contents.

- `CDAP-2228 <https://cdap.atlassian.net/browse/CDAP-2228>`__ -
  Fixed a problem with Mapreduce not working in Hadoop 2.2.

- `CDAP-2254 <https://cdap.atlassian.net/browse/CDAP-2254>`__ -
  Fixed a problem with an incorrect error message returned by HTTP RESTful Handler.

- `CDAP-2258 <https://cdap.atlassian.net/browse/CDAP-2258>`__ -
  Fixed a problem with an internal error when attempting to start a non-existing program.

- `CDAP-2279 <https://cdap.atlassian.net/browse/CDAP-2279>`__ -
  Fixed a problem with namespace and gear widgets disappearing when the browser window is too narrow.

- `CDAP-2280 <https://cdap.atlassian.net/browse/CDAP-2280>`__ -
  Fixed a problem when starting a flow from the GUI that the GUI does not fully refresh the page.

- `CDAP-2341 <https://cdap.atlassian.net/browse/CDAP-2341>`__ -
  Fixed a problem that when a MapReduce fails to start, it cannot be started or stopped any more.

- `CDAP-2343 <https://cdap.atlassian.net/browse/CDAP-2343>`__ -
  Fixed a problem in the CDAP UI that Mapreduce logs are convoluted with system logs.

- `CDAP-2344 <https://cdap.atlassian.net/browse/CDAP-2344>`__ -
  Fixed a problem with the formatting of logs in the CDAP UI.

- `CDAP-2355 <https://cdap.atlassian.net/browse/CDAP-2355>`__ -
  Fixed a problem with an Adapter CLI help error.

- `CDAP-2356 <https://cdap.atlassian.net/browse/CDAP-2356>`__ -
  Fixed a problem with CLI autocompletion results not sorted in alphabetical order.

- `CDAP-2365 <https://cdap.atlassian.net/browse/CDAP-2365>`__ -
  Fixed a problem that when restarting CDAP-Master, the CDAP UI oscillates between being up and down.

- `CDAP-2376 <https://cdap.atlassian.net/browse/CDAP-2376>`__ -
  Fixed a problem with logs from mapper and reducer not being collected.

- `CDAP-2444 <https://cdap.atlassian.net/browse/CDAP-2444>`__ -
  Fixed a problem with Cloudera Configuring doc needs fixing.

- `CDAP-2446 <https://cdap.atlassian.net/browse/CDAP-2446>`__ -
  Fixed a problem with that examples needing to be updated for new CDAP UI.

- `CDAP-2454 <https://cdap.atlassian.net/browse/CDAP-2454>`__ -
  Fixed a problem with Proto class RunRecord containing the Apache Twill RunId when serialized in REST API response.

- `CDAP-2459 <https://cdap.atlassian.net/browse/CDAP-2459>`__ -
  Fixed a problem with the CDAP UI going into a loop when the Router returns 200 and App Fabric is not up.

- `CDAP-2474 <https://cdap.atlassian.net/browse/CDAP-2474>`__ -
  Fixed a problem with changing the format of the name for the connectionfactory in JMS source plugin.

- `CDAP-2475 <https://cdap.atlassian.net/browse/CDAP-2475>`__ -
  Fixed a problem with JMS source accepting the type and name of the JMS provider plugin.

- `CDAP-2480 <https://cdap.atlassian.net/browse/CDAP-2480>`__ -
  Fixed a problem with the Workflow current run info endpoint missing a /runs/ in the path.

- `CDAP-2489 <https://cdap.atlassian.net/browse/CDAP-2489>`__ -
  Fixed a problem when, in distributed mode and CDAP master restarted, status of the running PROGRAM is always returned as STOPPED.

- `CDAP-2490 <https://cdap.atlassian.net/browse/CDAP-2490>`__ -
  Fixed a problem with checking if invalid Run Records for Spark and MapReduce are part of run from Workflow child programs.

- `CDAP-2491 <https://cdap.atlassian.net/browse/CDAP-2491>`__ -
  Fixed a problem with the MapReduce program in standalone mode not always using LocalJobRunnerWithFix.

- `CDAP-2493 <https://cdap.atlassian.net/browse/CDAP-2493>`__ -
  After the fix for `CDAP-2474 <https://cdap.atlassian.net/browse/CDAP-2474>`__ (ConnectionFactory in JMS source),
  the JSON file requires updating for the change to reflect in CDAP UI.

- `CDAP-2496 <https://cdap.atlassian.net/browse/CDAP-2496>`__ -
  Fixed a problem with CDAP using its own transaction snapshot codec.

- `CDAP-2498 <https://cdap.atlassian.net/browse/CDAP-2498>`__ -
  Fixed a problem with validation while creating adapters only by types and not also by values.

- `CDAP-2517 <https://cdap.atlassian.net/browse/CDAP-2517>`__ -
  Fixed a problem with Explore docs not mentioning partitioned file sets.

- `CDAP-2520 <https://cdap.atlassian.net/browse/CDAP-2520>`__ -
  Fixed a problem with StreamSource not liking values of '0m'.

- `CDAP-2522 <https://cdap.atlassian.net/browse/CDAP-2522>`__ -
  Fixed a problem with TransactionStateCache needing to reference Tephra SnapshotCodecV3.

- `CDAP-2529 <https://cdap.atlassian.net/browse/CDAP-2529>`__ -
  Fixed a problem with CLI not printing an error if it can't connect to CDAP.

- `CDAP-2530 <https://cdap.atlassian.net/browse/CDAP-2530>`__ -
  Fixed a problem with Custom RecordScannable<StructuredRecord> datasets not be explorable.

- `CDAP-2535 <https://cdap.atlassian.net/browse/CDAP-2535>`__ -
  Fixed a problem with IntegrationTestManager deployApplication not being namespaced.

- `CDAP-2538 <https://cdap.atlassian.net/browse/CDAP-2538>`__ -
  Fixed a problem with handling extra whitespace in CLI input.

- `CDAP-2540 <https://cdap.atlassian.net/browse/CDAP-2540>`__ -
  Fixed a problem with the Preferences Namespace CLI help having errors.

- `CDAP-2541 <https://cdap.atlassian.net/browse/CDAP-2541>`__ -
  Added the ability to stop the particular run of a program. Allows concurrent runs of the
  MapReduce and Workflow programs and the ability to stop programs at a per-run level.

- `CDAP-2547 <https://cdap.atlassian.net/browse/CDAP-2547>`__ -
  Fixed a problem with Kakfa Source - not using the persisted offset when the Adapter is restarted.

- `CDAP-2549 <https://cdap.atlassian.net/browse/CDAP-2549>`__ -
  Fixed a problem with a suspended workflow run record not being removed upon app/namespace delete.

- `CDAP-2562 <https://cdap.atlassian.net/browse/CDAP-2562>`__ -
  Fixed a problem with the automated Doc Build failing in develop.

- `CDAP-2564 <https://cdap.atlassian.net/browse/CDAP-2564>`__ -
  Improved the management of dataset resources.

- `CDAP-2565 <https://cdap.atlassian.net/browse/CDAP-2565>`__ -
  Fixed a problem with the transaction latency metric being of incorrect type.

- `CDAP-2569 <https://cdap.atlassian.net/browse/CDAP-2569>`__ -
  Fixed a problem with master process not being resilient to zookeeper exceptions.

- `CDAP-2571 <https://cdap.atlassian.net/browse/CDAP-2571>`__ -
  Fixed a problem with the RunRecord thread not resilient to errors.

- `CDAP-2587 <https://cdap.atlassian.net/browse/CDAP-2587>`__ -
  Fixed a problem with being unable to create default namespaces on starting up SDK.

- `CDAP-2635 <https://cdap.atlassian.net/browse/CDAP-2635>`__ -
  Fixed a problem with Namespace Create ignoring the properties' config field.

- `CDAP-2636 <https://cdap.atlassian.net/browse/CDAP-2636>`__ -
  Fixed a problem with "out of perm gen" space in CDAP Explore service.

- `CDAP-2654 <https://cdap.atlassian.net/browse/CDAP-2654>`__ -
  Fixed a problem with False values showing up as 'false null' in the CDAP Explore UI.

- `CDAP-2685 <https://cdap.atlassian.net/browse/CDAP-2685>`__ -
  Fixed a problem with the CDAP UI: no empty box for transforms.

- `CDAP-2729 <https://cdap.atlassian.net/browse/CDAP-2729>`__ -
  Fixed a problem with CDAP UI not handling downstream system services gracefully.

- `CDAP-2740 <https://cdap.atlassian.net/browse/CDAP-2740>`__ -
  Fixed a problem with CDAP UI not gracefully handling when the nodejs server goes down.

- `CDAP-2748 <https://cdap.atlassian.net/browse/CDAP-2748>`__ -
  Fixed a problem with the currently running and completed status of Spark programs in a
  workflow not highlighted in the UI.

- `CDAP-2765 <https://cdap.atlassian.net/browse/CDAP-2765>`__ -
  Fixed a problem with security warnings when CLI starts up.

- `CDAP-2766 <https://cdap.atlassian.net/browse/CDAP-2766>`__ -
  Fixed a problem with CLI asking for the user/password twice.

- `CDAP-2767 <https://cdap.atlassian.net/browse/CDAP-2767>`__ -
  Fixed a problem with incorrect error messages for namespace deletion.

- `CDAP-2768 <https://cdap.atlassian.net/browse/CDAP-2768>`__ -
  Fixed a problem with CLI and UI listing system.queue as a dataset.

- `CDAP-2769 <https://cdap.atlassian.net/browse/CDAP-2769>`__ -
  Fixed a problem with Use io.cdap.cdap.common.app.RunIds instead of
  org.apache.twill.internal.RunIds for InMemoryServiceProgramRunner.

- `CDAP-2787 <https://cdap.atlassian.net/browse/CDAP-2787>`__ -
  Fixed a problem when the number of MapReduce task metrics going over limit and causing MapReduce to fail.

- `CDAP-2796 <https://cdap.atlassian.net/browse/CDAP-2796>`__ -
  Fixed a problem with emitting duplicate metrics for dataset ops.

- `CDAP-2803 <https://cdap.atlassian.net/browse/CDAP-2803>`__ -
  Fixed a problem with scan operations not reflecting in dataset ops metrics.

- `CDAP-2804 <https://cdap.atlassian.net/browse/CDAP-2804>`__ -
  Fixed a problem with DataSetRecordReader incorrectly reporting dataset ops metrics.

- `CDAP-2810 <https://cdap.atlassian.net/browse/CDAP-2810>`__ -
  Fixed a problem with IncrementAndGet, CompareAndSwap, and Delete ops on Table incorrectly
  reporting two writes each.

- `CDAP-2821 <https://cdap.atlassian.net/browse/CDAP-2821>`__ -
  Fixed a problem with a Spark native library linkage error causing Standalone CDAP to stop.

- `CDAP-2823 <https://cdap.atlassian.net/browse/CDAP-2823>`__ -
  Fixed a problem with the conversion from Avro and to Avro not taking into account nested records.

- `CDAP-2830 <https://cdap.atlassian.net/browse/CDAP-2830>`__ -
  Fixed a problem with CDAP UI dying when CDAP Master is killed.

- `CDAP-2832 <https://cdap.atlassian.net/browse/CDAP-2832>`__ -
  Fixed a problem where suspending a schedule takes a long time and the CDAP UI does not provide any indication.

- `CDAP-2838 <https://cdap.atlassian.net/browse/CDAP-2838>`__ -
  Fixed a problem with poor error message when there is a mistake in security configration.

- `CDAP-2839 <https://cdap.atlassian.net/browse/CDAP-2839>`__ -
  Fixed a problem with the CDAP start script needing updating for the correct Node.js version.

- `CDAP-2848 <https://cdap.atlassian.net/browse/CDAP-2848>`__ -
  Fixed a problem with the Preferences Client test.

- `CDAP-2849 <https://cdap.atlassian.net/browse/CDAP-2849>`__ -
  Fixed a problem with the FileBatchSource reading files in twice if it takes longer that
  one workflow cycle to complete the job.

- `CDAP-2851 <https://cdap.atlassian.net/browse/CDAP-2851>`__ -
  Fixed a problem with RPM and DEB release artifacts being uploaded to incorrect staging directory.

- `CDAP-2854 <https://cdap.atlassian.net/browse/CDAP-2854>`__ -
  Fixed a problem with the instructions for using Docker.

- `CDAP-2855 <https://cdap.atlassian.net/browse/CDAP-2855>`__ -
  Fixed a problem with the example builds in VM failing with a Maven dependency error.

- `CDAP-2860 <https://cdap.atlassian.net/browse/CDAP-2860>`__ -
  Fixed a problem with the documentation for updating dataset properties.

- `CDAP-2861 <https://cdap.atlassian.net/browse/CDAP-2861>`__ -
  Fixed a problem with CDAP UI not mentioning required fields in all entry forms.

- `CDAP-2862 <https://cdap.atlassian.net/browse/CDAP-2862>`__ -
  Fixed a problem with CDAP UI creating multiple namespaces with the same name.

- `CDAP-2866 <https://cdap.atlassian.net/browse/CDAP-2866>`__ -
  Fixed a problem with FileBatchSource not reattempting to read in files if there is a failure.

- `CDAP-2870 <https://cdap.atlassian.net/browse/CDAP-2870>`__ -
  Fixed a problem with Workflow Diagrams.

- `CDAP-2871 <https://cdap.atlassian.net/browse/CDAP-2871>`__ -
  Fixed a problem with the Cloudera Manager Hbase Gateway dependency.

- `CDAP-2895 <https://cdap.atlassian.net/browse/CDAP-2895>`__ -
  Fixed a problem with a put operation on the WorkflowToken not throwing an exception.

- `CDAP-2899 <https://cdap.atlassian.net/browse/CDAP-2899>`__ -
  Fixed a problem with Mapreduce local dirs not getting cleaned up.

- `CDAP-2900 <https://cdap.atlassian.net/browse/CDAP-2900>`__ -
  Fixed a problem with exposing app.template.dir as a config option.

- `CDAP-2904 <https://cdap.atlassian.net/browse/CDAP-2904>`__ -
  Fixed a problem with "Make Request" button overlapping with paths when a path is long.

- `CDAP-2912 <https://cdap.atlassian.net/browse/CDAP-2912>`__ -
  Fixed a problem with HBaseQueueDebugger not sorting queue barriers correctly.

- `CDAP-2922 <https://cdap.atlassian.net/browse/CDAP-2922>`__ -
  Fixed a problem with datasets created through DynamicDatasetContext not having metrics context.
  Datasets in MapReduce and Spark programs, and workers, were not emitting metrics.

- `CDAP-2925 <https://cdap.atlassian.net/browse/CDAP-2925>`__ -
  Fixed a problem with the documentation on how to create datasets with properties.

- `CDAP-2932 <https://cdap.atlassian.net/browse/CDAP-2932>`__ -
  Fixed a problem with the AdapterClient getRuns method constructing a malformed URL.

- `CDAP-2935 <https://cdap.atlassian.net/browse/CDAP-2935>`__ -
  Fixed a problem with the logs endpoint to retrieve the latest entry not working correctly.

- `CDAP-2940 <https://cdap.atlassian.net/browse/CDAP-2940>`__ -
  Fixed a problem with the test case ArtifactStoreTest#testConcurrentSnapshotWrite.

- `CDAP-2941 <https://cdap.atlassian.net/browse/CDAP-2941>`__ -
  Fixed a problem with the ScriptTransform failing to initialize.

- `CDAP-2942 <https://cdap.atlassian.net/browse/CDAP-2942>`__ -
  Fixed a problem with the CDAP UI namespace dropdown failing on standalone restart.

- `CDAP-2948 <https://cdap.atlassian.net/browse/CDAP-2948>`__ -
  Fixed a problem with creating Adapters.

- `CDAP-2952 <https://cdap.atlassian.net/browse/CDAP-2952>`__ -
  Fixed a problem with the plugin avro library not being accessible to MapReduce.

- `CDAP-2955 <https://cdap.atlassian.net/browse/CDAP-2955>`__ -
  Fixed a problem with a NoSuchMethodException when trying to explore Datasets/Stream.

- `CDAP-2971 <https://cdap.atlassian.net/browse/CDAP-2971>`__ -
  Fixed a problem with the dataset registration not registering datasets for applications upon deploy.

- `CDAP-2972 <https://cdap.atlassian.net/browse/CDAP-2972>`__ -
  Fixed a problem with being unable to instantiate dataset in ETLWorker initialization.

- `CDAP-2981 <https://cdap.atlassian.net/browse/CDAP-2981>`__ -
  Fixed a problem with undoing a FileSets upgrade in favor of versioning and backward-compatibility.

- `CDAP-2991 <https://cdap.atlassian.net/browse/CDAP-2991>`__ -
  Fixed a problem with Explore not working when it launches a MapReduce job.

- `CDAP-2992 <https://cdap.atlassian.net/browse/CDAP-2992>`__ -
  Fixed a problem with CLI broken for secure CDAP.

- `CDAP-2996 <https://cdap.atlassian.net/browse/CDAP-2996>`__ -
  Fixed a problem with CDAP UI: Stop Run and Suspend Run buttons needed styling updates.

- `CDAP-2997 <https://cdap.atlassian.net/browse/CDAP-2997>`__ -
  Fixed a problem with SparkProgramRunnerTest failing randomly.

- `CDAP-2999 <https://cdap.atlassian.net/browse/CDAP-2999>`__ -
  Fixed a problem with MapReduce jobs showing the duration for tasks as 17 days before the mapper starts.

- `CDAP-3001 <https://cdap.atlassian.net/browse/CDAP-3001>`__ -
  Fixed a problem with truncating a custom dataset failing with internal server error.

- `CDAP-3002 <https://cdap.atlassian.net/browse/CDAP-3002>`__ -
  Fixed a problem with tick initialDelay not working properly.

- `CDAP-3003 <https://cdap.atlassian.net/browse/CDAP-3003>`__ -
  Fixed a problem with user metrics emitted from flowlets not being queryable using the flow's tags.

- `CDAP-3006 <https://cdap.atlassian.net/browse/CDAP-3006>`__ -
  Fixed a problem with updating cdap-spark-* archetypes.

- `CDAP-3007 <https://cdap.atlassian.net/browse/CDAP-3007>`__ -
  Fixed a problem with testing all Spark apps/guides to work with 3.1 (in dist mode).

- `CDAP-3009 <https://cdap.atlassian.net/browse/CDAP-3009>`__ -
  Fixed a problem with the stream conversion upgrade being in the upgrade tool in 3.1.

- `CDAP-3010 <https://cdap.atlassian.net/browse/CDAP-3010>`__ -
  Fixed a problem with a Bower Dependency Error.

- `CDAP-3011 <https://cdap.atlassian.net/browse/CDAP-3011>`__ -
  Fixed a problem with the IncrementSummingScannerTest failing intermittently.

- `CDAP-3012 <https://cdap.atlassian.net/browse/CDAP-3012>`__ -
  Fixed a problem with the DistributedWorkflowProgramRunner localizing the
  spark-assembly.jar even if the workflow does not contain a Spark program.

- `CDAP-3013 <https://cdap.atlassian.net/browse/CDAP-3013>`__ -
  Fixed a problem with excluding a Spark assembly jar when building a MapReduce job jar.

- `CDAP-3019 <https://cdap.atlassian.net/browse/CDAP-3019>`__ -
  Fixed a problem with the PartitionedFileSet dropPartition not deleting files under the partition.

- `CDAP-3021 <https://cdap.atlassian.net/browse/CDAP-3021>`__ -
  Fixed a problem with allowing Cloudfront data to use BatchFileFilter.

- `CDAP-3023 <https://cdap.atlassian.net/browse/CDAP-3023>`__ -
  Fixed a problem with flowlet instance count defaulting to 1.

- `CDAP-3024 <https://cdap.atlassian.net/browse/CDAP-3024>`__ -
  Fixed a problem with surfacing more logs in CDAP UI for System Services.

- `CDAP-3026 <https://cdap.atlassian.net/browse/CDAP-3026>`__ -
  Fixed a problem with updating SparkPageRank example docs.

- `CDAP-3027 <https://cdap.atlassian.net/browse/CDAP-3027>`__ -
  Fixed a problem with the DFSStreamHeartbeatsTest failing on clusters.

- `CDAP-3030 <https://cdap.atlassian.net/browse/CDAP-3030>`__ -
  Fixed a problem with the loading of custom datasets being broken after upgrading.

- `CDAP-3031 <https://cdap.atlassian.net/browse/CDAP-3031>`__ -
  Fixed a problem with deploying an app with a dataset with an invalid base path returning an "internal error".

- `CDAP-3037 <https://cdap.atlassian.net/browse/CDAP-3037>`__ -
  Fixed a problem with not being able to use a PartitionedFileSet in a custom dataset. If
  a custom dataset embedded a Table and a PartitionedFileSet, loading the dataset at
  runtime would fail.

- `CDAP-3038 <https://cdap.atlassian.net/browse/CDAP-3038>`__ -
  Fixed a problem with logs not showing up in UI when using Spark.

- `CDAP-3039 <https://cdap.atlassian.net/browse/CDAP-3039>`__ -
  Fixed a problem with worker not stopping at the end of a run method in standalone.

- `CDAP-3040 <https://cdap.atlassian.net/browse/CDAP-3040>`__ -
  Fixed a problem with flowlet and stream metrics not being available in distributed mode.

- `CDAP-3042 <https://cdap.atlassian.net/browse/CDAP-3042>`__ -
  Fixed a problem with the BufferingTable not merging buffered writes with multi-get results.

- `CDAP-3043 <https://cdap.atlassian.net/browse/CDAP-3043>`__ -
  Fixed a problem with the Javadocs being broken.

- `CDAP-3044 <https://cdap.atlassian.net/browse/CDAP-3044>`__ -
  Fixed a problem with the user service 'methods' field in service specifications being inaccurate.

- `CDAP-3058 <https://cdap.atlassian.net/browse/CDAP-3058>`__ -
  Fixed a problem with the NamespacedLocationFactory not appending correctly.

- `CDAP-3066 <https://cdap.atlassian.net/browse/CDAP-3066>`__ -
  Fixed a problem with FileBatchSource not failing properly.

- `CDAP-3067 <https://cdap.atlassian.net/browse/CDAP-3067>`__ -
  Fixed a problem with the UpgradeTool throwing a NullPointerException during UsageRegistry.upgrade().

- `CDAP-3070 <https://cdap.atlassian.net/browse/CDAP-3070>`__ -
  Fixed a problem on Ubuntu 14.10 where removing JSON files from templates/plugins/ETLBatch breaks adapters.

- `CDAP-3072 <https://cdap.atlassian.net/browse/CDAP-3072>`__ -
  Fixed a problem with a documentation JavaScript bug.

- `CDAP-3073 <https://cdap.atlassian.net/browse/CDAP-3073>`__ -
  Fixed a problem with out-of-memory perm gen space.

- `CDAP-3085 <https://cdap.atlassian.net/browse/CDAP-3085>`__ -
  Fixed a problem with adding integration tests for datasets.

- `CDAP-3086 <https://cdap.atlassian.net/browse/CDAP-3086>`__ -
  Fixed a problem with the CDAP UI current adapter UI.

- `CDAP-3087 <https://cdap.atlassian.net/browse/CDAP-3087>`__ -
  Fixed a problem with CDAP UI: a session timeout on secure mode.

- `CDAP-3088 <https://cdap.atlassian.net/browse/CDAP-3088>`__ -
  Fixed a problem with CDAP UI: application types need to be updated.

- `CDAP-3092 <https://cdap.atlassian.net/browse/CDAP-3092>`__ -
  Fixed a problem with reading multiple files with one mapper in FileBatchSource.

- `CDAP-3096 <https://cdap.atlassian.net/browse/CDAP-3096>`__ -
  Fixed a problem with running MapReduce on HDP 2.2.

- `CDAP-3098 <https://cdap.atlassian.net/browse/CDAP-3098>`__ -
  Fixed problems with the CDAP UI Adapter UI.

- `CDAP-3099 <https://cdap.atlassian.net/browse/CDAP-3099>`__ -
  Fixed a problem with CDAP UI and that settings icons shift 2px when you click on them.

- `CDAP-3104 <https://cdap.atlassian.net/browse/CDAP-3104>`__ -
  Fixed a problem with CDAP Explore throwing an exception if a Table dataset does not set schema.

- `CDAP-3105 <https://cdap.atlassian.net/browse/CDAP-3105>`__ -
  Fixed a problem with LogParserTransform needing to emit HTTP status code info.

- `CDAP-3106 <https://cdap.atlassian.net/browse/CDAP-3106>`__ -
  Fixed a problem with Hive query - local MapReduce task failure on CDH-5.4.

- `CDAP-3125 <https://cdap.atlassian.net/browse/CDAP-3125>`__ -
  Fixed a problem with the WorkerProgramRunnerTest failing intermittently.

- `CDAP-3127 <https://cdap.atlassian.net/browse/CDAP-3127>`__ -
  Fixed a problem with the Kafka guide not working with CDAP 3.1.0.

- `CDAP-3132 <https://cdap.atlassian.net/browse/CDAP-3132>`__ -
  Fixed a problem with the ProgramLifecycleHttpHandlerTest failing intermittently.

- `CDAP-3145 <https://cdap.atlassian.net/browse/CDAP-3145>`__ -
  Fixed a problem with the Metrics processor not processing metrics.

- `CDAP-3146 <https://cdap.atlassian.net/browse/CDAP-3146>`__ -
  Fixed a problem with the CDAP VM build failing to instal the Eclipse plugin.

- `CDAP-3148 <https://cdap.atlassian.net/browse/CDAP-3148>`__ -
  Fixed a problem with CDAP Explore MapReduce queries failing due to MR-framework being
  localized in the mapper container.

- `CDAP-3149 <https://cdap.atlassian.net/browse/CDAP-3149>`__ -
  Fixed a problem with cycles in an adapter create page causing the browser to freeze.

- `CDAP-3151 <https://cdap.atlassian.net/browse/CDAP-3151>`__ -
  Fixed a problem with CDAP examples shipped with SDK using JDK 1.6.

- `CDAP-3161 <https://cdap.atlassian.net/browse/CDAP-3161>`__ -
  Fixed a problem with MapReduce no longer working with default Cloudera manager settings.

- `CDAP-3173 <https://cdap.atlassian.net/browse/CDAP-3173>`__ -
  Fixed a problem with upgrading to 3.1.0 crashing the HBase co-processor.

- `CDAP-3174 <https://cdap.atlassian.net/browse/CDAP-3174>`__ -
  Fixed a problem with the ETL source/transform/sinks descriptions and documentation.

- `CDAP-3175 <https://cdap.atlassian.net/browse/CDAP-3175>`__ -
  Fixed a problem with the AbstractFlowlet constructors being deprecated when they should not be.


.. API Changes
.. -----------


Deprecated and Removed Features
-------------------------------

- See the `CDAP 3.1.0 Javadocs
  <http://docs.cask.co/cdap/3.1.0/en/reference-manual/javadocs/index.html#javadocs>`__
  for a list of deprecated and removed APIs.


.. _known-issues-310:

Known Issues
------------

- See the above section (*API Changes*) for alterations that can affect existing installations.
- CDAP has been tested on and supports CDH 4.2.x through CDH 5.4.4, HDP 2.0 through 2.6, and
  Apache Bigtop 0.8.0. It has not been tested on more recent versions of CDH.
  See `our Hadoop/HBase Environment configurations
  <http://docs.cask.co/cdap/3.1.0/en/admin-manual/installation/installation.html#install-hadoop-hbase>`__.
- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- Retrieving multiple metrics |---| by issuing an HTTP POST request with a JSON list as
  the request body that enumerates the name and attributes for each metric |---| is currently not
  supported in the `Metrics HTTP RESTful API v3
  <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/metrics.html#query-tips>`__.
  Instead, use the v2 API. It will be supported in a future release.

- `CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__ -
  When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available.

- `CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__ -
  If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, restart the CDAP Master
  |---| which will restart all services |---| as described under "Starting CDAP Services"
  for your particular Hadoop distribution in the `Installation documentation
  <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#starting-services>`__.

- `CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__ -
  CDAP internally creates tables in the "user" space that begin with the word
  ``"system"``. User datasets with names starting with ``"system"`` can conflict if they
  were to match one of those names. To avoid this, do not start any datasets with the word
  ``"system"``.

- `CDAP-1864 <https://cdap.atlassian.net/browse/CDAP-1864>`__ -
  Creating a dataset in a non-existent namespace manifests in the RESTful API with an
  incorrect error message.

- `CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__ -
  The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x as of CDAP 3.0.x.

- `CDAP-2785 <https://cdap.atlassian.net/browse/CDAP-2785>`__ -
  In the CDAP UI, many buttons will remain in focus after being clicked, even if they
  should not retain focus.

- `CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__ -
  A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all.

- `CDAP-2878 <https://cdap.atlassian.net/browse/CDAP-2878>`__ -
  The semantics for TTL are confusing, in that the Table TTL property is
  interpreted as milliseconds in some contexts: ``DatasetDefinition.confgure()`` and
  ``getAdmin()``.

- `CDAP-2945 <https://cdap.atlassian.net/browse/CDAP-2945>`__ -
  If the input partition filter for a PartitionedFileSet does not match any partitions,
  MapReduce jobs can fail.

- `CDAP-3000 <https://cdap.atlassian.net/browse/CDAP-3000>`__ -
  The Workflow token is in an inconsistent state for nodes in a fork while the nodes of
  the fork are still running. It becomes consistent after the join.

- `CDAP-3101 <https://cdap.atlassian.net/browse/CDAP-3101>`__ -
  If there are more than 30 concurrent runs of a workflow, the runs will not be scheduled
  due to a Quartz exception.

- `CDAP-3179 <https://cdap.atlassian.net/browse/CDAP-3179>`__ -
  If you are using CDH 5.3 (CDAP 3.0.0) and are upgrading to CDH 5.4 (CDAP 3.1.0), you
  must first upgrade the underlying HBase before you upgrade CDAP. This means that you perform the
  CDH upgrade before upgrading the CDAP.

- `CDAP-3189 <https://cdap.atlassian.net/browse/CDAP-3189>`__ -
  Large MapReduce jobs can cause excessive logging in the CDAP logs.

- `CDAP-3221 <https://cdap.atlassian.net/browse/CDAP-3221>`__ -
  When running in Standalone CDAP, if a MapReduce job fails repeatedly, then the SDK
  hits an out-of-memory exception due to ``perm gen``. The Standalone needs restarting at
  this point.


`Release 3.0.3 <http://docs.cask.co/cdap/3.0.3/index.html>`__
=============================================================

Bug Fix
-------

- Fixed a Bower dependency error in the CDAP UI
  (`CDAP-3010 <https://cdap.atlassian.net/browse/CDAP-3010>`__).

Known Issues
------------
- See the *Known Issues* of `version 3.0.1 <#known-issues-301>`_\ .

`Release 3.0.2 <http://docs.cask.co/cdap/3.0.2/index.html>`__
=============================================================

Bug Fixes
---------

- Fixed problems with the dataset upgrade tool
  (`CDAP-2962 <https://cdap.atlassian.net/browse/CDAP-2962>`__,
  `CDAP-2897 <https://cdap.atlassian.net/browse/CDAP-2897>`__).

Known Issues
------------
- See the *Known Issues* of `version 3.0.1 <#known-issues-301>`_\ .

`Release 3.0.1 <http://docs.cask.co/cdap/3.0.1/index.html>`__
=============================================================

New Features
------------

- In the CDAP UI, mandatory parameters for Application Template creation are marked with
  asterisks, and if a user tries to create a template without one of those parameters, the
  missing parameter is highlighted
  (`CDAP-2499 <https://cdap.atlassian.net/browse/CDAP-2499>`__).


Improvements
------------

**Tools**

- Added a tool (`HBaseQueueDebugger
  <https://github.com/cdapio/cdap/blob/release/3.0/cdap-master/src/main/java/io/cdap/cdap/data/tools/HBaseQueueDebugger.java>`__)
  that counts consumed and unconsumed entries in a flowlet queue
  (`CDAP-2105 <https://cdap.atlassian.net/browse/CDAP-2105>`__).

**CDAP UI**

- The currently executing node of a workflow is now highlighted in the CDAP UI
  (`CDAP-2615 <https://cdap.atlassian.net/browse/CDAP-2615>`__).

- The list of datasets and the run histories in the CDAP UI are now paginated
  (`CDAP-2626 <https://cdap.atlassian.net/browse/CDAP-2626>`__,
  `CDAP-2627 <https://cdap.atlassian.net/browse/CDAP-2627>`__).

- Added improvements to the CDAP UI when creating Application Templates
  (`CDAP-2601 <https://cdap.atlassian.net/browse/CDAP-2601>`__,
  `CDAP-2602 <https://cdap.atlassian.net/browse/CDAP-2602>`__,
  `CDAP-2603 <https://cdap.atlassian.net/browse/CDAP-2603>`__,
  `CDAP-2605 <https://cdap.atlassian.net/browse/CDAP-2605>`__,
  `CDAP-2606 <https://cdap.atlassian.net/browse/CDAP-2606>`__,
  `CDAP-2607 <https://cdap.atlassian.net/browse/CDAP-2607>`__,
  `CDAP-2610 <https://cdap.atlassian.net/browse/CDAP-2610>`__).

- Improved the error messages returned when there are problems creating Application
  Templates in the CDAP UI
  (`CDAP-2597 <https://cdap.atlassian.net/browse/CDAP-2597>`__).

**CDAP SDK VM**

- Added the Apache Flume agent flume-ng to the CDAP SDK VM
  (`CDAP-2612 <https://cdap.atlassian.net/browse/CDAP-2612>`__).

- Added the ability to copy and paste to the CDAP SDK VM
  (`CDAP-2611 <https://cdap.atlassian.net/browse/CDAP-2611>`__).

- Pre-downloaded the example dependencies into the CDAP SDK VM to speed building of the
  CDAP examples
  (`CDAP-2613 <https://cdap.atlassian.net/browse/CDAP-2613>`__).


Bug Fixes
---------

**General**

- Fixed a problem with the HBase store and flows with multiple queues, where one queue name
  is a prefix of another queue name
  (`CDAP-1996 <https://cdap.atlassian.net/browse/CDAP-1996>`__).

- Fixed a problem with namespaces with underscores in the name crashing the Hadoop HBase
  region servers
  (`CDAP-2110 <https://cdap.atlassian.net/browse/CDAP-2110>`__).

- Removed the requirement to specify the JDBC driver class property twice in the adaptor
  configuration for Database Sources and Sinks
  (`CDAP-2453 <https://cdap.atlassian.net/browse/CDAP-2453>`__).

- Fixed a problem in Distributed CDAP where the status of running program always returns
  as "STOPPED" when the CDAP Master is restarted
  (`CDAP-2489 <https://cdap.atlassian.net/browse/CDAP-2489>`__).

- Fixed a problem with invalid RunRecords for Spark and MapReduce programs that are run as
  part of a Workflow (`CDAP-2490 <https://cdap.atlassian.net/browse/CDAP-2490>`__).

- Fixed a problem with the CDAP Master not being HA (highly available) when a leadership
  change happens
  (`CDAP-2495 <https://cdap.atlassian.net/browse/CDAP-2495>`__).

- Fixed a problem with upgrading of queues with the UpgradeTool
  (`CDAP-2502 <https://cdap.atlassian.net/browse/CDAP-2502>`__).

- Fixed a problem with ObjectMappedTables not deleting missing fields when updating a row
  (`CDAP-2523 <https://cdap.atlassian.net/browse/CDAP-2523>`__,
  `CDAP-2524 <https://cdap.atlassian.net/browse/CDAP-2524>`__).

- Fixed a problem with a stream not being created properly when deploying an application
  after the default namespace was deleted
  (`CDAP-2537 <https://cdap.atlassian.net/browse/CDAP-2537>`__).

- Fixed a problem with the Applicaton Template Kafka Source not using the persisted offset
  when the Adapter is restarted
  (`CDAP-2547 <https://cdap.atlassian.net/browse/CDAP-2547>`__).

- A problem with CDAP using its own transaction snapshot codec, leading to huge snapshot
  files and OutOfMemory exceptions, and transaction snapshots that can't be read using
  Tephra's tools, has been resolved by replacing the codec with Tephra's SnapshotCodecV3
  (`CDAP-2563 <https://cdap.atlassian.net/browse/CDAP-2563>`__,
  `CDAP-2946 <https://cdap.atlassian.net/browse/CDAP-2946>`__,
  `TEPHRA-101 <https://cdap.atlassian.net/browse/TEPHRA-101>`__).

- Fixed a problem with CDAP Master not being resilient in the handling of ZooKeeper
  exceptions
  (`CDAP-2569 <https://cdap.atlassian.net/browse/CDAP-2569>`__).

- Fixed a problem with RunRecords not being cleaned up correctly after certain exceptions
  (`CDAP-2584 <https://cdap.atlassian.net/browse/CDAP-2584>`__).

- Fixed a problem with the CDAP Maven archetype having an incorrect CDAP version in it
  (`CDAP-2634 <https://cdap.atlassian.net/browse/CDAP-2634>`__).

- Fixed a problem with the description of the TwitterSource not describing its output
  (`CDAP-2648 <https://cdap.atlassian.net/browse/CDAP-2648>`__).

- Fixed a problem with the Twitter Source not handling missing fields correctly and as a
  consequence producing tweets (with errors) that were then not stored on disk
  (`CDAP-2653 <https://cdap.atlassian.net/browse/CDAP-2653>`__).

- Fixed a problem with the TwitterSource not calculating the time of tweet correctly
  (`CDAP-2656 <https://cdap.atlassian.net/browse/CDAP-2656>`__).

- Fixed a problem with the JMS Real-time Source failing to load required plugin sources
  (`CDAP-2661 <https://cdap.atlassian.net/browse/CDAP-2661>`__).

- Fixed a problem with executing Hive queries on a distributed CDAP due to a failure to
  load Grok classes
  (`CDAP-2678 <https://cdap.atlassian.net/browse/CDAP-2678>`__).

- Fixed a problem with CDAP Program jars not being cleaned up from the temporary directory
  (`CDAP-2698 <https://cdap.atlassian.net/browse/CDAP-2698>`__).

- Fixed a problem with ProjectionTransforms not handling input data fields with null
  values correctly
  (`CDAP-2719 <https://cdap.atlassian.net/browse/CDAP-2719>`__).

- Fixed a problem with the CDAP SDK running out of memory when MapReduce jobs are run repeatedly
  (`CDAP-2743 <https://cdap.atlassian.net/browse/CDAP-2743>`__).

- Fixed a problem with not using CDAP RunIDs in the in-memory version of the CDAP SDK
  (`CDAP-2769 <https://cdap.atlassian.net/browse/CDAP-2769>`__).

**CDAP CLI**

- Fixed a problem with the CDAP CLI not printing an error if it is unable to connect to a
  CDAP instance
  (`CDAP-2529 <https://cdap.atlassian.net/browse/CDAP-2529>`__).

- Fixed a problem with extra whitespace in commands entered into the CDAP CLI causing errors
  (`CDAP-2538 <https://cdap.atlassian.net/browse/CDAP-2538>`__).

**CDAP SDK Standalone**

- Updated the messages displayed when starting the Standalone CDAP SDK as to components
  and the JVM required (`CDAP-2445 <https://cdap.atlassian.net/browse/CDAP-2445>`__).

- Fixed a problem with the creation of the default namespace upon starting the CDAP SDK
  (`CDAP-2587 <https://cdap.atlassian.net/browse/CDAP-2587>`__).

**CDAP SDK VM**

- Fixed a problem with using the default namespace on the CDAP SDK Virtual Machine Image
  (`CDAP-2500 <https://cdap.atlassian.net/browse/CDAP-2500>`__).

- Fixed a problem with the VirtualBox VM retaining a MAC address obtained from the build host
  (`CDAP-2640 <https://cdap.atlassian.net/browse/CDAP-2640>`__).

**CDAP UI**

- Fixed a problem with incorrect flow metrics showing in the CDAP UI
  (`CDAP-2494 <https://cdap.atlassian.net/browse/CDAP-2494>`__).

- Fixed a problem in the CDAP UI with the properties in the Projection Transform being
  displayed inconsistently
  (`CDAP-2525 <https://cdap.atlassian.net/browse/CDAP-2525>`__).

- Fixed a problem in the CDAP UI not automatically updating the number of flowlet instances
  (`CDAP-2534 <https://cdap.atlassian.net/browse/CDAP-2534>`__).

- Fixed a problem in the CDAP UI with a window resize preventing clicking of the Adapter
  Template drop down menu
  (`CDAP-2573 <https://cdap.atlassian.net/browse/CDAP-2573>`__).

- Fixed a problem with the CDAP UI not performing validation of mandatory parameters
  before the creation of an adapter
  (`CDAP-2575 <https://cdap.atlassian.net/browse/CDAP-2575>`__).

- Fixed a problem with an incorrect version of CDAP being shown in the CDAP UI
  (`CDAP-2586 <https://cdap.atlassian.net/browse/CDAP-2586>`__).

- Reduced the number of clicks required to navigate and perform actions within the CDAP UI
  (`CDAP-2622 <https://cdap.atlassian.net/browse/CDAP-2622>`__,
  `CDAP-2625 <https://cdap.atlassian.net/browse/CDAP-2625>`__).

- Fixed a problem with an additional forward-slash character in the URL causing a "page
  not found error" in the CDAP UI
  (`CDAP-2624 <https://cdap.atlassian.net/browse/CDAP-2624>`__).

- Fixed a problem with the error dropdown of the CDAP UI not scrolling when it has a
  large number of errors
  (`CDAP-2633 <https://cdap.atlassian.net/browse/CDAP-2633>`__).

- Fixed a problem in the CDAP UI with the Twitter Source's consumer key secret not being
  treated as a password field
  (`CDAP-2649 <https://cdap.atlassian.net/browse/CDAP-2649>`__).

- Fixed a problem with the CDAP UI attempting to create an adapter without a name
  (`CDAP-2652 <https://cdap.atlassian.net/browse/CDAP-2652>`__).

- Fixed a problem with the CDAP UI not being able to find the ETL plugin templates on
  distributed CDAP
  (`CDAP-2655 <https://cdap.atlassian.net/browse/CDAP-2655>`__).

- Fixed a problem with the CDAP UI's System Dashboard chart having a y-axis starting at "-200"
  (`CDAP-2699 <https://cdap.atlassian.net/browse/CDAP-2699>`__).

- Fixed a problem with the rendering of stack trace logs in the CDAP UI
  (`CDAP-2745 <https://cdap.atlassian.net/browse/CDAP-2745>`__).

- Fixed a problem with the CDAP UI not working with secure CDAP instances, either clusters
  or standalone
  (`CDAP-2770 <https://cdap.atlassian.net/browse/CDAP-2770>`__).

- Fixed a problem with the coloring of completed runs of Workflow DAGs in the CDAP UI
  (`CDAP-2781 <https://cdap.atlassian.net/browse/CDAP-2781>`__).

**Documentation**

- Fixed errors with the documentation examples of the ETL Plugins
  (`CDAP-2503 <https://cdap.atlassian.net/browse/CDAP-2503>`__).

- Documented the licenses of all shipped CDAP UI components
  (`CDAP-2582 <https://cdap.atlassian.net/browse/CDAP-2582>`__).

- Corrected issues with the building of Javadocs used on the website and removed Javadocs
  previously included in the SDK
  (`CDAP-2730 <https://cdap.atlassian.net/browse/CDAP-2730>`__).

- Added a recommended version (v.12.0) of Node.js to the documentation
  (`CDAP-2762 <https://cdap.atlassian.net/browse/CDAP-2762>`__).


.. _known-issues-301:

Known Issues
------------

- The application in the `cdap-kafka-ingest-guide
  <https://github.com/cdap-guides/cdap-kafka-ingest-guide/tree/release/cdap-3.0-compatible>`__
  does not run on Ubuntu 14.x and CDAP 3.0.x
  (`CDAP-2632 <https://cdap.atlassian.net/browse/CDAP-2632>`__,
  `CDAP-2749 <https://cdap.atlassian.net/browse/CDAP-2749>`__).

- Metrics for `TimePartitionedFileSets
  <http://docs.cask.co/cdap/3.0.1/en/developers-manual/building-blocks/datasets/fileset.html#timepartitionedfileset>`__
  can show zero values even if there is data present
  (`CDAP-2721 <https://cdap.atlassian.net/browse/CDAP-2721>`__).

- In the CDAP UI: many buttons will remain in focus after being clicked, even if they
  should not retain focus
  (`CDAP-2785 <https://cdap.atlassian.net/browse/CDAP-2785>`__).

- When the CDAP-Master dies, the CDAP UI does not repsond appropriately, and instead of
  waiting for routing to the secondary master to begin, it loses its connection
  (`CDAP-2830 <https://cdap.atlassian.net/browse/CDAP-2830>`__).

- A workflow that is scheduled by time will not be run between the failure of the primary
  master and the time that the secondary takes over. This scheduled run will not be
  triggered at all. There is no warnings or messages about the missed run of the
  workflow. (`CDAP-2831 <https://cdap.atlassian.net/browse/CDAP-2831>`__)

- CDAP has been tested on and supports CDH 4.2.x through CDH 5.3.x, HDP 2.0 through 2.1, and
  Apache Bigtop 0.8.0. It has not been tested on more recent versions of CDH.
  See `our Hadoop/HBase Environment configurations
  <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#install-hadoop-hbase>`__.

- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- See the above section (*API Changes*) for alterations that can affect existing installations.

- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__).
- If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, `restart the CDAP Master
  <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#starting-services>`__,
  which will restart all services (`CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__).
- User datasets with names starting with ``"system"`` can potentially cause conflicts
  (`CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__).
- Scaling the number of metrics processor instances doesn't automatically distribute the
  processing load to the newer instances of the metrics processor. The CDAP Master needs to be
  restarted to effectively distribute the processing across all metrics processor instances
  (`CDAP-1853 <https://cdap.atlassian.net/browse/CDAP-1853>`__).
- Creating a dataset in a non-existent namespace manifests in the RESTful API with an
  incorrect error message (`CDAP-1864 <https://cdap.atlassian.net/browse/CDAP-1864>`__).
- Retrieving multiple metrics |---| by issuing an HTTP POST request with a JSON list as
  the request body that enumerates the name and attributes for each metric |---| is currently not
  supported in the
  `Metrics HTTP RESTful API v3 <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/metrics.html#query-tips>`__.
  Instead, use the v2 API. It will be supported in a future release.
- Typically, datasets are bundled as part of applications. When an application is upgraded and redeployed,
  any changes in datasets will not be redeployed. This is because datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the dataset.
  A workaround (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__) is to allow *unchecked dataset upgrades*.
  Upgrades cause the dataset metadata, i.e. its specification including properties, to be updated. The dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that datasets are upgraded when the application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded dataset is to first stop
  all applications that are using the dataset before deploying the new version of the application.
  This lets all containers (flows, services, etc) to pick up the new dataset changes.
  When datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other applications that are using the dataset can work with the new changes.


`Release 3.0.0 <http://docs.cask.co/cdap/3.0.0/index.html>`__
=============================================================

New Features
------------

- Support for application templates has been added (`CDAP-1753 <https://cdap.atlassian.net/browse/CDAP-1753>`__).

- Built-in ETL application templates and plugins have been added (`CDAP-1767 <https://cdap.atlassian.net/browse/CDAP-1767>`__).

- New `CDAP UI <http://docs.cask.co/cdap/3.0.0/en/admin-manual/operations/cdap-ui.html#cdap-ui>`__,
  supports creating ETL applications directly in the web UI.
  See section below (`New 3.0.0 User Interface <#new-user-interface-300>`__) for details.

- Workflow logs can now be retrieved using the `CDP HTTP Logging RESTful API
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/logging.html#http-restful-api-logging>`__
  (`CDAP-1089 <https://cdap.atlassian.net/browse/CDAP-1089>`__).

- Support has been added for suspending and resuming of a workflow (`CDAP-1610
  <https://cdap.atlassian.net/browse/CDAP-1610>`__).

- Condition nodes in a workflow now allow branching based on a boolean predicate
  (`CDAP-1928 <https://cdap.atlassian.net/browse/CDAP-1928>`__).

- Condition nodes in a workflow now allow passing the Hadoop counters from a MapReduce
  program to following Condition nodes in the workflow (`CDAP-1611
  <https://cdap.atlassian.net/browse/CDAP-1611>`__).

- Logs can now be fetched based on the ``run-id`` (`CDAP-1582
  <https://cdap.atlassian.net/browse/CDAP-1582>`__).

- CDAP Tables are `now explorable
  <http://docs.cask.co/cdap/3.0.0/en/developers-manual/data-exploration/tables.html#table-exploration>`__
  (`CDAP-946 <https://cdap.atlassian.net/browse/CDAP-946>`__).

- The `CDAP CLI <http://docs.cask.co/cdap/3.0.0/en/reference-manual/cli-api.html#cli>`__
  supports the new `application template and adapters APIs
  <http://docs.cask.co/cdap/3.0.0/en/application-templates/index.html>`__.
  (`CDAP-1773 <https://cdap.atlassian.net/browse/CDAP-1773>`__).

- The `CDAP CLI <http://docs.cask.co/cdap/3.0.0/en/reference-manual/cli-api.html#cli>`__
  startup options have been changed to accommodate a new option
  of executing a file containing a series of CLI commands, line-by-line.

- Both `grok <http://logstash.net/docs/1.4.2/filters/grok>`__ and
  `syslog <http://en.wikipedia.org/wiki/Syslog>`__ record formats can now be used when
  `setting the format of a stream
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/stream.html#http-restful-api-stream-setting-properties>`__
  (`CDAP-1949 <https://cdap.atlassian.net/browse/CDAP-1949>`__).

- Added HTTP RESTful endpoints for listing datasets and streams as used by adapters,
  programs, and applications, and vice-versa
  (`CDAP-2214 <https://cdap.atlassian.net/browse/CDAP-2214>`__).

- Created a `queue introspection tool <https://github.com/cdapio/cdap/pull/2290>`__,
  for counting processed and unprocessed entries in a
  flowlet queue (`CDAP-2105 <https://cdap.atlassian.net/browse/CDAP-2105>`__).

- Support for CDAP SDK VM build automation has been added (`CDAP-2030 <https://cdap.atlassian.net/browse/CDAP-2030>`__).

- A Cube dataset has been added (`CDAP-1520 <https://cdap.atlassian.net/browse/CDAP-1520>`__).

- A Batch and Real-Time Cube dataset sink has been added (`CDAP-1520 <https://cdap.atlassian.net/browse/CDAP-1966>`__).

- Metrics and status information for MapReduce on a task level is now exposed (`CDAP-1520 <https://cdap.atlassian.net/browse/CDAP-1958>`__).

.. _new-user-interface-300:

New User Interface
------------------
- Introduced a new UI, organization based on namespaces and users.
- Users can switch between namespaces.
- Uses web sockets to retrieve updates from the backend.
- **Development Section**

  - Introduces a UI for programs based on run-ids.
  - Users can view logs and, in certain cases |---| flows |---| flowlets, of a program based on run ids.
  - Shows a list of datasets and streams used by a program, and shows programs using a specific dataset and stream.
  - Shows the history of a program (list of runs).
  - Datasets or streams are explorable on a dataset/stream level or on a global level.
  - Shows program level metrics on under each program.

- **Operations section**

  - Introduces an operations section to explore metrics.
  - Allows users to create custom dashboard with custom metrics.
  - Users can add different types of charts (line, bar, area, pie, donut, scatter, spline,
    area-spline, area-spline-stacked, area-stacked, step, table).
  - Users can add multiple metrics on a single dashboard, or on a single widget on a single dashboard.
  - Users can organize the widgets in either a two, three, or four-column layout.
  - Users can toggle the frequency at which data is polled for a metric.
  - Users can toggle the resolution of data displayed in a graph.

- **Admin Section**

  - Users can manage different objects of CDAP (applications, programs, datasets, and streams).
  - Users can create namespaces.
  - Through the Admin view, users can configure their preferences at the CDAP level, namespace level, or application level.
  - Users can manage the system services, applications, and streams through the Admin view.

- **Adapters**

  - Users can create ETLBatch and ETLRealtime adapters from within the UI.
  - Users can choose from a list of plugins that comes by default with CDAP when creating an adapter.
  - Users can save an adapter as a draft, to be created at a later point-in-time.
  - Users can configure plugin properties with appropriate editors from within the UI when creating an adapter.

- The Old CDAP Console has been deprecated.

Improvement
-----------

- The `metrics system APIs
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/metrics.html#http-restful-api-metrics>`__
  have been revised and improved
  (`CDAP-1596 <https://cdap.atlassian.net/browse/CDAP-1596>`__).
- The metrics system performance has been improved
  (`CDAP-2124 <https://cdap.atlassian.net/browse/CDAP-2124>`__,
  `CDAP-2125 <https://cdap.atlassian.net/browse/CDAP-2125>`__).

Bug Fixes
---------

- The CDAP Authentication server now reports the port correctly when the port is set to 0
  (`CDAP-614 <https://cdap.atlassian.net/browse/CDAP-614>`__).

- History of the programs running under workflow (Spark and MapReduce) is now updated correctly
  (`CDAP-1293 <https://cdap.atlassian.net/browse/CDAP-1293>`__).

- Programs running under a workflow now receive a unique ``run-id``
  (`CDAP-2025 <https://cdap.atlassian.net/browse/CDAP-2025>`__).

- RunRecords are now updated with the RuntimeService to account for node failures
  (`CDAP-2202 <https://cdap.atlassian.net/browse/CDAP-2202>`__).

- MapReduce metrics are now available on a secure cluster
  (`CDAP-64 <https://cdap.atlassian.net/browse/CDAP-64>`__).

API Changes
-----------

- The endpoint (``POST '<base-url>/metrics/search?target=childContext[&context=<context>]'``)
  that searched for the available contexts of metrics has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://cdap.atlassian.net/browse/CDAP-1998>`__). A
  `replacement endpoint
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/metrics.html#http-restful-api-metrics-search-for-contexts>`__
  is available.

- The endpoint (``POST '<base-url>/metrics/search?target=metric&context=<context>'``)
  that searched for metrics in a specified context has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://cdap.atlassian.net/browse/CDAP-1998>`__). A
  `replacement endpoint
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/metrics.html#http-restful-api-metrics-search-for-metrics>`__
  is available.

- The endpoint (``POST '<base-url>/metrics/query?context=<context>[&groupBy=<tags>]&metric=<metric>&<time-range>'``)
  that queried for a metric has been deprecated, pending removal
  in a later version of CDAP (`CDAP-1998 <https://cdap.atlassian.net/browse/CDAP-1998>`__). A
  `replacement endpoint
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/metrics.html#http-restful-api-metrics-querying-a-metric>`__
  is available.

- Metrics: The tag name for service handlers in previous releases was wrongly ``"runnable"``,
  and internally represented as ``"srn"``. These metrics are now tagged as ``"handler"`` (``"hnd"``), and
  metrics queries will only account for this tag name. If you need to query historic metrics
  that were emitted with the old tag ``"runnable"``, use ``"srn"`` to query them (instead of either
  ``"runnable"`` or ``"handler"``).

- The `CDAP CLI <http://docs.cask.co/cdap/3.0.0/en/reference-manual/cli-api.html#cli>`__
  startup options have been changed to accommodate a new option
  of executing a file containing a series of CLI commands, line-by-line.

- The metrics system APIs have been improved (`CDAP-1596 <https://cdap.atlassian.net/browse/CDAP-1596>`__).

- The rules for `resolving resolution
  <http://docs.cask.co/cdap/3.0.0/en/reference-manual/http-restful-api/metrics.html#http-restful-api-metrics-time-range>`__
  when using ``resolution=auto`` in metrics query have been changed
  (`CDAP-1922 <https://cdap.atlassian.net/browse/CDAP-1922>`__).

- Backward incompatible changes in ``InputFormatProvider`` and ``OutputFormatProvider``.
  It won't affect user code that uses ``FileSet`` or ``PartitionedFileSet``.
  It only affects classes who implement the ``InputFormatProvider`` or ``OutputFormatProvider``:

  - ``InputFormatProvider.getInputFormatClass()`` is removed and

    - replaced with ``InputFormatProvider.getInputFormatClassName()``;

  - ``OutputFormatProvider.getOutputFormatClass()`` is removed and

    - replaced with ``OutputFormatProvider.getOutputFormatClassName()``.


Deprecated and Removed Features
-------------------------------

- The `File DropZone <http://docs.cask.co/cdap/2.8.0/en/developers-manual/ingesting-tools/cdap-file-drop-zone.html>`__
  and `File Tailer <http://docs.cask.co/cdap/2.8.0/en/developers-manual/ingesting-tools/cdap-file-tailer.html>`__
  are no longer supported as of Release 3.0.
- Support for *procedures* has been removed. After upgrading, an application that
  contained a procedure must be redeployed.
- Support for *service workers* have been removed. After upgrading, an application that
  contained a service worker must be redeployed.
- The old CDAP Console has been deprecated.
- Support for JDK/JRE 1.6 (Java 6) has ended; JDK/JRE 1.7 (Java 7) is
  `now required for CDAP <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#install-java-runtime>`__ or the
  `CDAP SDK <http://docs.cask.co/cdap/3.0.0/en/developers-manual/getting-started/standalone/index.html#standalone-index>`__.


.. _known-issues-300:

Known Issues
------------

- CDAP has been tested on and supports CDH 4.2.x through CDH 5.3.x, HDP 2.0 through 2.1, and
  Apache Bigtop 0.8.0. It has not been tested on more recent versions of CDH.
  See `our Hadoop/HBase Environment configurations
  <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#install-hadoop-hbase>`__.

- After upgrading CDAP from a pre-3.0 version, any unprocessed metrics data in Kafka will
  be lost and *WARN* log messages will be logged that tell about the inability to process
  old data in the old format.

- See the above section (*API Changes*) for alterations that can affect existing installations.

- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-797 <https://cdap.atlassian.net/browse/CDAP-797>`__).
- If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, `restart the CDAP Master
  <http://docs.cask.co/cdap/3.0.0/en/admin-manual/installation/installation.html#starting-services>`__,
  which will restart all services (`CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__).
- User datasets with names starting with ``"system"`` can potentially cause conflicts
  (`CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__).
- Scaling the number of metrics processor instances doesn't automatically distribute the
  processing load to the newer instances of the metrics processor. The CDAP Master needs to be
  restarted to effectively distribute the processing across all metrics processor instances
  (`CDAP-1853 <https://cdap.atlassian.net/browse/CDAP-1853>`__).
- Creating a dataset in a non-existent namespace manifests in the RESTful API with an
  incorrect error message (`CDAP-1864 <https://cdap.atlassian.net/browse/CDAP-1864>`__).
- Retrieving multiple metrics |---| by issuing an HTTP POST request with a JSON list as
  the request body that enumerates the name and attributes for each metric |---| is currently not
  supported in the
  `Metrics HTTP RESTful API v3 <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/metrics.html#query-tips>`__.
  Instead, use the v2 API. It will be supported in a future release.
- Typically, datasets are bundled as part of applications. When an application is upgraded and redeployed,
  any changes in datasets will not be redeployed. This is because datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the dataset.
  A workaround (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__) is to allow *unchecked dataset upgrades*.
  Upgrades cause the dataset metadata, i.e. its specification including properties, to be updated. The dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that datasets are upgraded when the application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded dataset is to first stop
  all applications that are using the dataset before deploying the new version of the application.
  This lets all containers (flows, services, etc) to pick up the new dataset changes.
  When datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other applications that are using the dataset can work with the new changes.


`Release 2.8.0 <http://docs.cask.co/cdap/2.8.0/index.html>`__
=============================================================

General
-------

- The HTTP RESTful API v2 was deprecated, replaced with the
  `namespaced HTTP RESTful API v3
  <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/index.html#http-restful-api-v3>`__.

- Added log rotation for CDAP programs running in YARN containers
  (`CDAP-1295 <https://cdap.atlassian.net/browse/CDAP-1295>`__).

- Added the ability to submit to non-default YARN queues to provide
  `resource guarantees
  <http://docs.cask.co/cdap/2.8.0/en/admin-manual/operations/resource-guarantees.html#resource-guarantees>`__
  for CDAP Master services, CDAP programs, and Explore Queries
  (`CDAP-1417 <https://cdap.atlassian.net/browse/CDAP-1417>`__).

- Added the ability to `prune invalid transactions
  <http://docs.cask.co/cdap/2.8.0/en/admin-manual/operations/tx-maintenance.html#tx-maintenance>`__
  (`CDAP-1540 <https://cdap.atlassian.net/browse/CDAP-1540>`__).

- Added the ability to specify `custom logback file for CDAP programs
  <http://docs.cask.co/cdap/2.8.0/en/developers-manual/advanced/application-logback.html#application-logback>`__
  (`CDAP-1100 <https://cdap.atlassian.net/browse/CDAP-1100>`__).

- System HTTP services now bind to all interfaces (0.0.0.0), rather than 127.0.0.1.

New Features
------------

- **Command Line Interface (CLI)**

  - CLI can now directly connect to a CDAP instance of your choice at startup by using
    ``cdap cli --uri <uri>``.
  - Support for runtime arguments, which can be listed by running ``"cdap cli --help"``.
  - Table rendering can be configured using ``"cli render as <alt|csv>"``.
    The option ``"alt"`` is the default, with ``"csv"`` available for copy & pasting.
  - Stream statistics can be computed using ``"get stream-stats <stream-id>"``.

- **Datasets**

  - Added an ObjectMappedTable dataset that maps object fields to table columns and that is also explorable.
  - Added a PartitionedFileSet dataset that allows addressing files by meta data and that is also explorable.
  - Table datasets now support a multi-get operation for batched reads.
  - Allow an unchecked dataset upgrade upon application deployment
    (`CDAP-1574 <https://cdap.atlassian.net/browse/CDAP-1574>`__).

- **Metrics**

  - Added new APIs for exploring available metrics, including drilling down into the context of emitted metrics
  - Added the ability to explore (search) all metrics; previously, this was restricted to custom user metrics
  - There are new APIs for querying metrics
  - New capability to break down a metrics time series using the values of one or more tags in its context

- **Namespaces**

  - Applications and programs are now managed within namespaces.
  - Application logs are available within namespaces.
  - Metrics are now collected and queried within namespaces.
  - Datasets can now created and managed within namespaces.
  - Streams are now namespaced for ingestion, fetching, and consuming by programs.
  - Explore operations are now namespaced.

- **Preferences**

  - Users can store preferences (a property map) at the instance, namespace, application,
    or program level.

- **Spark**

  - Spark now uses a configurer-style API for specifying
    (`CDAP-382 <https://cdap.atlassian.net/browse/CDAP-1134>`__).

- **Workflows**

  - Users can schedule a workflow based on increments of data being ingested into a stream.
  - Workflows can be stopped.
  - The execution of a workflow can be forked into parallelized branches.
  - The runtime arguments for workflow can be scoped.

- **Workers**

  - Added `Worker
    <http://docs.cask.co/cdap/2.8.0/en/developers-manual/building-blocks/workers.html#workers>`__,
    a new program type that can be added to CDAP applications, used to run background
    processes and (beta feature) can write to streams through the WorkerContext.

- **Upgrade and Data Migration Tool**

  - Added an `automated upgrade tool
    <http://docs.cask.co/cdap/2.8.0/en/admin-manual/installation/installation.html#upgrading-an-existing-version>`__
    which supports upgrading from
    2.6.x to 2.8.0. (**Note:** Apps need to be both recompiled and re-deployed.)
    Upgrade from 2.7.x to 2.8.0 is not currently supported. If you have a use case for it,
    please reach out to us at `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__.
  - Added a metric migration tool which migrates old metrics to the new 2.8 format.


Improvement
-----------

- Improved flow performance and scalability with a new distributed queue implementation.


API Changes
-----------

- The endpoint (``GET <base-url>/data/explore/datasets/<dataset-name>/schema``) that
  retrieved the schema of a dataset's underlying Hive table has been removed
  (`CDAP-1603 <https://cdap.atlassian.net/browse/CDAP-1603>`__).
- Endpoints have been added to retrieve the CDAP version and the current configurations of
  CDAP and HBase (`Configuration HTTP RESTful API
  <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/configuration.html>`__).


.. _known-issues-280:

Known Issues
------------
- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-64 <https://cdap.atlassian.net/browse/CDAP-64>`__ and `CDAP-797
  <https://cdap.atlassian.net/browse/CDAP-797>`__).
- If the Hive Metastore is restarted while the CDAP Explore Service is running, the
  Explore Service remains alive, but becomes unusable. To correct, `restart the CDAP Master
  <http://docs.cask.co/cdap/2.8.0/en/admin-manual/installation/installation.html#starting-services>`__,
  which will restart all services (`CDAP-1007 <https://cdap.atlassian.net/browse/CDAP-1007>`__).
- User datasets with names starting with ``"system"`` can potentially cause conflicts
  (`CDAP-1587 <https://cdap.atlassian.net/browse/CDAP-1587>`__).
- Scaling the number of metrics processor instances doesn't automatically distribute the
  processing load to the newer instances of the metrics processor. The CDAP Master needs to be
  restarted to effectively distribute the processing across all metrics processor instances
  (`CDAP-1853 <https://cdap.atlassian.net/browse/CDAP-1853>`__).
- Creating a dataset in a non-existent namespace manifests in the RESTful API with an
  incorrect error message (`CDAP-1864 <https://cdap.atlassian.net/browse/CDAP-1864>`__).
- Retrieving multiple metrics |---| by issuing an HTTP POST request with a JSON list as
  the request body that enumerates the name and attributes for each metric |---| is currently not
  supported in the
  `Metrics HTTP RESTful API v3 <http://docs.cask.co/cdap/2.8.0/en/reference-manual/http-restful-api/http-restful-api-v3/metrics.html#query-tips>`__.
  Instead, use the v2 API. It will be supported in a future release.
- Typically, datasets are bundled as part of applications. When an application is upgraded and redeployed,
  any changes in datasets will not be redeployed. This is because datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the dataset.
  A workaround (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__) is to allow *unchecked dataset upgrades*.
  Upgrades cause the dataset metadata, i.e. its specification including properties, to be updated. The dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that datasets are upgraded when the application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded dataset is to first stop
  all applications that are using the dataset before deploying the new version of the application.
  This lets all containers (flows, services, etc) to pick up the new dataset changes.
  When datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other applications that are using the dataset can work with the new changes.
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).


`Release 2.7.1 <http://docs.cask.co/cdap/2.7.1/index.html>`__
=============================================================

API Changes
-----------
-  The property ``security.auth.server.address`` has been deprecated and replaced with
   ``security.auth.server.bind.address`` (`CDAP-639 <https://cdap.atlassian.net/browse/CDAP-639>`__,
   `CDAP-1078 <https://cdap.atlassian.net/browse/CDAP-1078>`__).

New Features
------------

- **Spark**

  - Spark now uses a configurer-style API for specifying (`CDAP-382 <https://cdap.atlassian.net/browse/CDAP-382>`__).
  - Spark can now run as a part of a workflow (`CDAP-465 <https://cdap.atlassian.net/browse/CDAP-465>`__).

- **Security**

  - CDAP Master now obtains and refreshes Kerberos tickets programmatically (`CDAP-1134 <https://cdap.atlassian.net/browse/CDAP-1134>`__).

- **Datasets**

  - A new, experimental dataset type to support time-partitioned File sets has been added.
  - Time-partitioned File sets can be queried with Impala on CDH distributions (`CDAP-926 <https://cdap.atlassian.net/browse/CDAP-926>`__).
  - Streams can be made queryable with Impala by deploying an adapter that periodically
    converts it into partitions of a time-partitioned File set (`CDAP-1129 <https://cdap.atlassian.net/browse/CDAP-1129>`__).
  - Support for different levels of conflict detection: ``ROW``, ``COLUMN``, or ``NONE`` (`CDAP-1016 <https://cdap.atlassian.net/browse/CDAP-1016>`__).
  - Removed support for ``@DisableTransaction`` (`CDAP-1279 <https://cdap.atlassian.net/browse/CDAP-1279>`__).
  - Support for annotating a stream with a schema (`CDAP-606 <https://cdap.atlassian.net/browse/CDAP-606>`__).
  - A new API for uploading entire files to a stream has been added (`CDAP-411 <https://cdap.atlassian.net/browse/CDAP-411>`__).

- **Workflow**

  - Workflow now uses a configurer-style API for specifying (`CDAP-1207 <https://cdap.atlassian.net/browse/CDAP-1207>`__).
  - Multiple instances of a workflow can be run concurrently (`CDAP-513 <https://cdap.atlassian.net/browse/CDAP-513>`__).
  - Programs are no longer part of a workflow; instead, they are added in the application
    and are referenced by a workflow using their names (`CDAP-1116 <https://cdap.atlassian.net/browse/CDAP-1116>`__).
  - Schedules are now at the application level and properties can be specified for
    Schedules; these properties will be passed to the scheduled program as runtime
    arguments (`CDAP-1148 <https://cdap.atlassian.net/browse/CDAP-1148>`__).

.. _known-issues-271:

Known Issues
------------
- When upgrading an existing CDAP installation to 2.7.1, all metrics are reset.
- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-64 <https://cdap.atlassian.net/browse/CDAP-64>`__ and `CDAP-797
  <https://cdap.atlassian.net/browse/CDAP-797>`__).
- When upgrading a cluster from an earlier version of CDAP, warning messages may appear in
  the master log indicating that in-transit (emitted, but not yet processed) metrics
  system messages could not be decoded (*Failed to decode message to MetricsRecord*). This
  is because of a change in the format of emitted metrics, and can result in a small
  amount of metrics data points being lost (`CDAP-745 <https://cdap.atlassian.net/browse/CDAP-745>`__).
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).

- Typically, datasets are bundled as part of applications. When an application is upgraded and redeployed,
  any changes in datasets will not be redeployed. This is because datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the dataset.
  A workaround (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__) is to allow *unchecked dataset upgrades*.
  Upgrades cause the dataset metadata, i.e. its specification including properties, to be updated. The dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that datasets are upgraded when the application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded dataset is to first stop
  all applications that are using the dataset before deploying the new version of the application.
  This lets all containers (flows, services, etc) to pick up the new dataset changes.
  When datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other applications that are using the dataset can work with the new changes.


`Release 2.6.1 <http://docs.cask.co/cdap/2.6.1/index.html>`__
=============================================================

CDAP Bug Fixes
--------------
- Allow an *unchecked dataset upgrade* upon application deployment
  (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__).
- Update the Hive dataset table when a dataset is updated
  (`CDAP-71 <https://cdap.atlassian.net/browse/CDAP-71>`__).
- Use Hadoop configuration files bundled with the Explore Service
  (`CDAP-1250 <https://cdap.atlassian.net/browse/CDAP-1250>`__).

.. _known-issues-261:

Known Issues
------------
- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-64 <https://cdap.atlassian.net/browse/CDAP-64>`__ and `CDAP-797
  <https://cdap.atlassian.net/browse/CDAP-797>`__).
- When upgrading a cluster from an earlier version of CDAP, warning messages may appear in
  the master log indicating that in-transit (emitted, but not yet processed) metrics
  system messages could not be decoded (*Failed to decode message to MetricsRecord*). This
  is because of a change in the format of emitted metrics, and can result in a small
  amount of metrics data points being lost (`CDAP-745 <https://cdap.atlassian.net/browse/CDAP-745>`__).
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).

- Typically, datasets are bundled as part of applications. When an application is upgraded and redeployed,
  any changes in datasets will not be redeployed. This is because datasets can be shared across applications,
  and an incompatible schema change can break other applications that are using the dataset.
  A workaround (`CDAP-1253 <https://cdap.atlassian.net/browse/CDAP-1253>`__) is to allow *unchecked dataset upgrades*.
  Upgrades cause the dataset metadata, i.e. its specification including properties, to be updated. The dataset
  runtime code is also updated. To prevent data loss the existing data and the underlying HBase tables remain as-is.

  You can allow *unchecked dataset upgrades* by setting the configuration property ``dataset.unchecked.upgrade``
  to ``true`` in ``cdap-site.xml``. This will ensure that datasets are upgraded when the application is redeployed.
  When this configuration is set, the recommended process to deploy an upgraded dataset is to first stop
  all applications that are using the dataset before deploying the new version of the application.
  This lets all containers (flows, services, etc) to pick up the new dataset changes.
  When datasets are upgraded using ``dataset.unchecked.upgrade``, no schema compatibility checks are performed by the
  system. Hence it is very important that the developer verify the backward-compatibility, and makes sure that
  other applications that are using the dataset can work with the new changes.


`Release 2.6.0 <http://docs.cask.co/cdap/2.6.0/index.html>`__
=============================================================

API Changes
-----------
-  API for specifying services and MapReduce programs has been changed to use a "configurer"
   style; this will require modification of user classes implementing either MapReduce
   or service as the interfaces have changed (`CDAP-335
   <https://cdap.atlassian.net/browse/CDAP-335>`__).


New Features
------------

- **General**

  - Health checks are now available for CDAP system services
    (`CDAP-663 <https://cdap.atlassian.net/browse/CDAP-663>`__).

- **Applications**

  -  Jar deployment now uses a chunked request and writes to a local temp file
     (`CDAP-91 <https://cdap.atlassian.net/browse/CDAP-91>`__).

- **MapReduce**

  -  MapReduce programs can now read binary stream data
     (`CDAP-331 <https://cdap.atlassian.net/browse/CDAP-331>`__).

- **Datasets**

  - Added `FileSet
    <http://docs.cask.co/cdap/2.6.0/en/developers-manual/building-blocks/datasets/fileset.html#datasets-fileset>`__,
    a new core dataset type for working with sets of files
    (`CDAP-1 <https://cdap.atlassian.net/browse/CDAP-1>`__).

- **Spark**

  - Spark programs now emit system and custom user metrics
    (`CDAP-346 <https://cdap.atlassian.net/browse/CDAP-346>`__).
  - Services can be called from Spark programs and its worker nodes
    (`CDAP-348 <https://cdap.atlassian.net/browse/CDAP-348>`__).
  - Spark programs can now read from streams
    (`CDAP-403 <https://cdap.atlassian.net/browse/CDAP-403>`__).
  - Added Spark support to the CDAP CLI (Command-line Interface)
    (`CDAP-425 <https://cdap.atlassian.net/browse/CDAP-425>`__).
  - Improved speed of Spark unit tests
    (`CDAP-600 <https://cdap.atlassian.net/browse/CDAP-600>`__).
  - Spark programs now display system metrics in the CDAP Console
    (`CDAP-652 <https://cdap.atlassian.net/browse/CDAP-652>`__).

- **Procedures**

  - Procedures have been deprecated in favor of services
    (`CDAP-413 <https://cdap.atlassian.net/browse/CDAP-413>`__).

- **Services**

  - Added an HTTP endpoint that returns the endpoints a particular service exposes
    (`CDAP-412 <https://cdap.atlassian.net/browse/CDAP-412>`__).
  - Added an HTTP endpoint that lists all services
    (`CDAP-469 <https://cdap.atlassian.net/browse/CDAP-469>`__).
  - Default metrics for services have been added to the CDAP Console
    (`CDAP-512 <https://cdap.atlassian.net/browse/CDAP-512>`__).
  - The annotations ``@QueryParam`` and ``@DefaultValue`` are now supported in custom service handlers
    (`CDAP-664 <https://cdap.atlassian.net/browse/CDAP-664>`__).

- **Metrics**

  - System and user metrics now support gauge metrics
    (`CDAP-484 <https://cdap.atlassian.net/browse/CDAP-484>`__).
  - Metrics can be queried using a program’s run-ID
    (`CDAP-620 <https://cdap.atlassian.net/browse/CDAP-620>`__).

- **Documentation**

  - A `Quick Start Guide <http://docs.cask.co/cdap/2.6.0/en/admin-manual/installation/quick-start.html#installation-quick-start>`__ has been added to the
    `CDAP Administration Manual <http://docs.cask.co/cdap/2.6.0/en/admin-manual/index.html#admin-index>`__
    (`CDAP-695 <https://cdap.atlassian.net/browse/CDAP-695>`__).

CDAP Bug Fixes
--------------

- Fixed a problem with readless increments not being used when they were enabled in a dataset
  (`CDAP-383 <https://cdap.atlassian.net/browse/CDAP-383>`__).
- Fixed a problem with applications, whose Spark or Scala user classes were not extended
  from either ``JavaSparkProgram`` or ``ScalaSparkProgram``, failing with a class loading error
  (`CDAP-599 <https://cdap.atlassian.net/browse/CDAP-599>`__).
- Fixed a problem with the `CDAP upgrade tool
  <http://docs.cask.co/cdap/2.6.0/en/admin-manual/installation/installation.html#upgrading-an-existing-version>`__
  not preserving |---| for tables with readless increments enabled |---| the coprocessor
  configuration during an upgrade (`CDAP-1044 <https://cdap.atlassian.net/browse/CDAP-1044>`__).
- Fixed a problem with the readless increment implementation dropping increment cells when
  a region flush or compaction occurred (`CDAP-1062 <https://cdap.atlassian.net/browse/CDAP-1062>`__).

.. _known-issues-260:

Known Issues
------------

- When running secure Hadoop clusters, metrics and debug logs from MapReduce programs are
  not available (`CDAP-64 <https://cdap.atlassian.net/browse/CDAP-64>`__ and `CDAP-797
  <https://cdap.atlassian.net/browse/CDAP-797>`__).
- When upgrading a cluster from an earlier version of CDAP, warning messages may appear in
  the master log indicating that in-transit (emitted, but not yet processed) metrics
  system messages could not be decoded (*Failed to decode message to MetricsRecord*). This
  is because of a change in the format of emitted metrics, and can result in a small
  amount of metrics data points being lost (`CDAP-745
  <https://cdap.atlassian.net/browse/CDAP-745>`__).
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).


`Release 2.5.2 <http://docs.cask.co/cdap/2.5.2/index.html>`__
=============================================================

CDAP Bug Fixes
--------------

- Fixed a problem with a Coopr-provisioned secure cluster failing to start due to a classpath
  issue (`CDAP-478 <https://cdap.atlassian.net/browse/CDAP-478>`__).
- Fixed a problem with the WISE app zip distribution not packaged correctly; a new version
  (0.2.1) has been released (`CDAP-533 <https://cdap.atlassian.net/browse/CDAP-533>`__).
- Fixed a problem with the examples and tests incorrectly using the ByteBuffer.array
  method when reading a stream event (`CDAP-549 <https://cdap.atlassian.net/browse/CDAP-549>`__).
- Fixed a problem with the Authentication Server so that it can now communicate with an LDAP
  instance over SSL (`CDAP-556 <https://cdap.atlassian.net/browse/CDAP-556>`__).
- Fixed a problem with the program class loader to allow applications to use a different
  version of a library than the one that the CDAP platform uses; for example, a different
  Kafka library (`CDAP-559 <https://cdap.atlassian.net/browse/CDAP-559>`__).
- Fixed a problem with CDAP master not obtaining new delegation tokens after running for
  ``hbase.auth.key.update.interval`` milliseconds (`CDAP-562 <https://cdap.atlassian.net/browse/CDAP-562>`__).
- Fixed a problem with the transaction not being rolled back when a user service handler throws an exception
  (`CDAP-607 <https://cdap.atlassian.net/browse/CDAP-607>`__).

Other Changes
-------------

- Improved the CDAP documentation:

  - Re-organized the documentation into three manuals |---| Developers' Manual, Administration
    Manual, Reference Manual |---| and a set of examples, how-to guides and tutorials;
  - Documents are now in smaller chapters, with numerous updates and revisions;
  - Added a link for downloading an archive of the documentation for offline use;
  - Added links to examples relevant to a particular component;
  - Added suggested deployment architectures for Distributed CDAP installations;
  - Added a glossary;
  - Added navigation aids at the bottom of each page; and
  - Tested and updated the Standalone CDAP examples and their documentation.

Known Issues
------------
- Currently, applications that include Spark or Scala classes in user classes not extended
  from either ``JavaSparkProgram`` or ``ScalaSparkProgram`` (depending upon the language)
  fail with a class loading error. Spark or Scala classes should not be used outside of the
  Spark program. (`CDAP-599 <https://cdap.atlassian.net/browse/CDAP-599>`__)
- Metrics for MapReduce programs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
- Writing to datasets through Hive is not supported in CDH4.x
  (`CDAP-988 <https://cdap.atlassian.net/browse/CDAP-988>`__).
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).


`Release 2.5.1 <http://docs.cask.co/cdap/2.5.1/index.html>`__
=============================================================

CDAP Bug Fixes
--------------

- Improved the documentation of the CDAP authentication and stream clients, both Java and Python APIs.
- Fixed problems with the CDAP Command Line Interface (CLI):

  - Did not work in non-interactive mode;
  - Printed excessive debug log messages;
  - Relative paths did not work as expected; and
  - Failed to execute SQL queries.

- Removed dependencies on SNAPSHOT artifacts for *netty-http* and *auth-clients*.
- Corrected an error in the message printed by the startup script ``cdap sdk``.
- Resolved a problem with the reading of the properties file by the CDAP Flume Client of CDAP Ingest library
  without first checking if authentication was enabled.

Other Changes
-------------

- The scripts ``send-query.sh``, ``access-token.sh`` and ``access-token.bat`` has been replaced by the
  `CDAP Command Line Interface <http://docs.cask.co/cdap/2.5.1/en/api.html#command-line-interface>`__, ``cdap cli``.
- The CDAP Command Line Interface now uses and saves access tokens when connecting to a secure CDAP instance.
- The CDAP Java Stream Client now allows empty String events to be sent.
- The CDAP Python Authentication Client's ``configure()`` method now takes a dictionary rather than a filepath.

Known Issues
------------
- Metrics for MapReduce programs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).


`Release 2.5.0 <http://docs.cask.co/cdap/2.5.0/index.html>`__
=============================================================

New Features
------------

Ad-hoc querying
.................
- Capability to write to datasets using SQL
- Added a CDAP JDBC driver allowing connections from Java applications and third-party business intelligence tools
- Ability to perform ad-hoc queries from the CDAP Console:

  - Execute a SQL query from the Console
  - View list of active, completed queries
  - Download query results

Datasets
.................
- Datasets can be tested with TestBase outside of the context of an application
- CDAP now checks datasets for compatibility in a verification stage
- The Transaction engine uses server-side filtering for efficient transactional reads
- Dataset specifications can now be dynamically reconfigured through the use of RESTful endpoints
- The Bundle jar format is now used for dataset libs
- Increments on datasets are now read-less

Services
.................
- Added simplified APIs for using services from other programs such as MapReduce, flows and Procedures
- Added an API for creating services and handlers that can use datasets transactionally
- Added a RESTful API to make requests to a service via the Router

Security
.................
- Added authorization logging
- Added Kerberos authentication to ZooKeeper secret keys
- Added support for SSL

Spark Integration
.................
- Supports running Spark programs as a part of CDAP applications in Standalone mode
- Supports running Spark programs written with Spark versions 1.0.1 or 1.1.0
- Supports Spark's *MLib* and *GraphX* modules
- Includes three examples demonstrating CDAP Spark programs
- Adds display of Spark program logs and history in the CDAP Console

Streams
.................
- Added a collection of applications, tools and APIs specifically for the ETL (Extract, Transform and Loading) of data
- Added support for asynchronously writing to streams

Clients
.................
- Added a Command Line Interface
- Added a Java Client Interface


Major CDAP Bug Fixes
--------------------
- Fixed a problem with a HADOOP_HOME exception stacktrace when unit-testing an application
- Fixed an issue with Hive creating directories in /tmp in the Standalone and unit-test frameworks
- Fixed a problem with type inconsistency of service API calls, where numbers were showing up as strings
- Fixed an issue with the premature expiration of long-term Authentication Tokens
- Fixed an issue with the dataset size metric showing data operations size instead of resource usage

.. _known-issues-250:

Known Issues
------------
- Metrics for MapReduce programs aren't populated on secure Hadoop clusters
- The metric for the number of cores shown in the Resources view of the CDAP Console will be zero
  unless YARN has been configured to enable virtual cores
- A race condition resulting in a deadlock can occur when a TwillRunnable container
  shutdowns while it still has ZooKeeper events to process. This occasionally surfaces when
  running with OpenJDK or JDK7, though not with Oracle JDK6. It is caused by a change in the
  ``ThreadPoolExecutor`` implementation between Oracle JDK6 and OpenJDK/JDK7. Until Apache Twill is
  updated in a future version of CDAP, a work-around is to kill the errant process. The YARN
  command to list all running applications and their ``app-id``\s is::

    yarn application -list -appStates RUNNING

  The command to kill a process is::

    yarn application -kill <app-id>

  All versions of CDAP running Apache Twill version 0.4.0 with this configuration can exhibit this
  problem (`TWILL-110 <https://issues.apache.org/jira/browse/TWILL-110>`__).
