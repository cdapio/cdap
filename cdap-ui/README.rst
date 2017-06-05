==============
CDAP UI Pack 3
==============

The CDAP UI Pack is a UI-only release that can be applied on top of an existing CDAP installation.
The CDAP UI Pack 3 applies on CDAP 4.1.1.

Details
=======
- Release Date: 05/08/2017
- Base CDAP Version: 4.1.1
- Release branch: release/4.1

Installation
============

UNIX/Linux Flavors
------------------
**CDAP Sandbox**
::

  $ cd <CDAP_HOME>
  $ ./bin/cdap sandbox stop
  $ ./bin/cdap apply-pack /path/to/download/cdap-ui-pack-4.1.1_p3.zip
  $ ./bin/cdap sandbox start


**Distributed CDAP**

*Note:* In the instructions below, ``<CDAP_HOME>`` is either:

- ``/opt/cdap`` on RPM (manual or Apache Ambari) installations; or
- the *CDAP* sub-directory of the *Parcel Directory* on Cloudera Manager installations (e.g. ``/opt/cloudera/parcels/CDAP``)

::

  $ cd <CDAP_HOME>
  $ /etc/init.d/cdap-ui stop
  $ cdap apply-pack /path/to/download/cdap-ui-pack-4.1.1_p3.zip
  $ /etc/init.d/cdap-ui start



Windows
-------

::

  > cd <CDAP_HOME>
  > bin\cdap sandbox stop
  > bin\cdap apply-pack \path\to\download\cdap-ui-pack-4.1.1_p3.zip
  > bin\cdap sandbox start


Release Notes
=============

New Features
------------
* `CDAP-9523 <https://issues.cask.co/browse/CDAP-9523>`__ - Added point-and-click interaction for extracting text from a column using Regex Groups
* `CDAP-9515 <https://issues.cask.co/browse/CDAP-9515>`__ - Added point-and-click interaction for exploding data in a row into multiple rows
* `CDAP-9514 <https://issues.cask.co/browse/CDAP-9514>`__ - Added point-and-click interaction for swapping and merging columns
* `CDAP-9510 <https://issues.cask.co/browse/CDAP-9510>`__ - Added point-and-click interaction for changing column name
* `CDAP-9507 <https://issues.cask.co/browse/CDAP-9507>`__ - Added point-and-click interaction for formatting data

Improvements
------------
* `CDAP-9541 <https://issues.cask.co/browse/CDAP-9541>`__ - Improved styling of column directive dropdown icon
* `CDAP-9441 <https://issues.cask.co/browse/CDAP-9441>`__ - Rephrased message when a user is not authorized to access any namespace
* `CDAP-9415 <https://issues.cask.co/browse/CDAP-9415>`__ - Switched from font icons to SVGs on the home page for better loading of images
* `CDAP-9394 <https://issues.cask.co/browse/CDAP-9394>`__ - Added an exact/whole world match option for find and replace
* `CDAP-9255 <https://issues.cask.co/browse/CDAP-9255>`__ - When there are no nodes on the studio, disabled certain actions

Bug Fixes
---------
* `CDAP-10488 <https://issues.cask.co/browse/CDAP-10488>`__ - Made it easier to delete an action plugin easily from Studio
* `CDAP-10312 <https://issues.cask.co/browse/CDAP-10312>`__ - Fixed an issue where users could open multiple popovers in dataprep modal in Pipeline studio
* `CDAP-9596 <https://issues.cask.co/browse/CDAP-9596>`__ - Fixed the parsing of search results in the metadata view
* `CDAP-9445 <https://issues.cask.co/browse/CDAP-9445>`__ - Renaming a column to an existing column should show a warning to users
* `CDAP-9175 <https://issues.cask.co/browse/CDAP-9175>`__ - Fixed the realtime stream source to have a view details button
* `CDAP-9051 <https://issues.cask.co/browse/CDAP-9051>`__ - Fixed the help for parse-as-json
* `CDAP-8963 <https://issues.cask.co/browse/CDAP-8963>`__ - Handled boolean values correctly while previewing explore results





==============
CDAP UI Pack 2
==============
CDAP UI Pack 2 was not released separately. The features in CDAP UI Pack 2 were released as part of CDAP 4.1.1. Please refer to the
release notes of CDAP 4.1.1 for details about these features.



==============
CDAP UI Pack 1
==============

The CDAP UI Pack is a UI-only release that can be applied on top of an existing CDAP installation.

Details
=======
- Release Date: 03/23/2017
- Base CDAP Version: 4.1
- Release branch: release/4.1

Installation
============
Currently, the following manual steps need to be performed to install the CDAP UI Pack.
These steps will be automated in a later CDAP release.

UNIX/Linux Flavors
------------------
**CDAP Sandbox**
::

  $ cd <CDAP_HOME>
  $ ./bin/cdap sandbox stop
  $ zip -m -r ui-backup.zip ui
  $ unzip /path/to/download/cdap-ui-pack.zip
  $ ./bin/cdap sandbox start


**Distributed CDAP**

*Note:* In the instructions below, ``<CDAP_HOME>`` is either:

- ``/opt/cdap`` on RPM (manual or Apache Ambari) installations; or
- the *CDAP* sub-directory of the *Parcel Directory* on Cloudera Manager installations (e.g. ``/opt/cloudera/parcels/CDAP``)

::

  $ cd <CDAP_HOME>
  $ /etc/init.d/cdap-ui stop
  $ zip -m -r ui-backup.zip ui
  $ unzip /path/to/download/cdap-ui-pack-4.1.0_p1.zip
  $ /etc/init.d/cdap-ui start



Windows
-------
1. Using the command prompt, stop the CDAP Sandbox::

    > cd <CDAP_HOME>
    > bin\cdap sandbox stop

2. Open the ``<CDAP_HOME>`` directory in Explorer
3. Compress the ``ui`` to save a backup, by right-clicking on the ``ui`` directory and
   choosing *Send To* -> Compressed (zipped) folder*
4. Delete the ``ui`` directory after the backup is completed
5. Extract the UI pack (cdap-ui-pack-4.1.0_p1.zip) in the ``<CDAP_HOME>`` directory, by right-clicking on the file,
   choosing *Extract All*, and specifying the path to the ``<CDAP_HOME>`` directory
6. A new ``ui`` directory should be created
7. Using the command prompt, start the CDAP Sandbox::

    > cd <CDAP_HOME>
    > bin\cdap sandbox start


Steps to Update Data Preparation Capability
===========================================
1. After installing the CDAP UI Pack and restarting CDAP, from within the CDAP UI go to the Cask Market
2. From the *Solutions* category, follow the steps for the *Data Preparation* solution
3. Go to *Data Preparation* by clicking on the CDAP menu and then choosing *Data Preparation*
4. If a newer version of the *Data Preparation* libraries has been installed, the UI will show an *Update* button
5. Click the *Update* button to update to the newer version of *Data Preparation*


Release Notes
=============

New Features
------------
* `HYDRATOR-163 <https://issues.cask.co/browse/HYDRATOR-163>`__ - Add Placeholders to input boxes in node configuration
* `WRANGLER-77 <https://issues.cask.co/browse/WRANGLER-77>`__ - Added a dropdown on each column to provide click-through experience for directives in Data Preparation
* `WRANGLER-49 <https://issues.cask.co/browse/WRANGLER-49>`__ - Added click-through experience for split column directive in Data Preparation
* `WRANGLER-54 <https://issues.cask.co/browse/WRANGLER-54>`__ - Added click-through experience for filling null or empty cells in Data Preparation

Improvements
------------
* `CDAP-8501 <https://issues.cask.co/browse/CDAP-8501>`__ - Disabled preview button on clusters since preview is not supported in distributed env
* `CDAP-8861 <https://issues.cask.co/browse/CDAP-8861>`__ - Removed CDAP Version Range in market entities display
* `CDAP-8430 <https://issues.cask.co/browse/CDAP-8430>`__ - Improved "No Entities Found" message in the Overview to show Call(s) to Action
* `CDAP-8403 <https://issues.cask.co/browse/CDAP-8403>`__ - Added labels to CDAP Studio actions
* `CDAP-8900 <https://issues.cask.co/browse/CDAP-8900>`__ - Added the ability to update to a newer version of data preparation libraries if available
* `CDAP-7352 <https://issues.cask.co/browse/CDAP-7352>`__ - Made logviewer header row sticky
* `CDAP-4798 <https://issues.cask.co/browse/CDAP-4798>`__ - Improved user experience in explore page
* `CDAP-8964 <https://issues.cask.co/browse/CDAP-8964>`__ - Made Output Schema for sinks macro enabled
* `HYDRATOR-1364 <https://issues.cask.co/browse/HYDRATOR-1364>`__ - Removed most of the "__ui__" field
* `CDAP-8494 <https://issues.cask.co/browse/CDAP-8494>`__ - Fixed browser back button after switching to classic UI
* `CDAP-8828 <https://issues.cask.co/browse/CDAP-8828>`__ - Removed dialog to select pipeline type upon pipeline creation
* `CDAP-8396 <https://issues.cask.co/browse/CDAP-8396>`__ - Added call to action for namespace creation

Bug Fixes
---------
* `CDAP-8554 <https://issues.cask.co/browse/CDAP-8554>`__ - Fixed styling issues while showing Call(s) to actions in Application create wizard
* `CDAP-8412 <https://issues.cask.co/browse/CDAP-8412>`__ - Fixed overflow in namespace creation confirmation modal
* `CDAP-8433 <https://issues.cask.co/browse/CDAP-8433>`__ - Added units for memory for YARN stats on management page
* `CDAP-8950 <https://issues.cask.co/browse/CDAP-8950>`__ - Fixed link from stream overview to stream deatils
* `CDAP-8933 <https://issues.cask.co/browse/CDAP-8933>`__ - Added namespace name to the No entities found message
* `CDAP-8461 <https://issues.cask.co/browse/CDAP-8461>`__ - Clicking back from the Detail page view now opens the overview page with the overview pane opened
* `CDAP-8638 <https://issues.cask.co/browse/CDAP-8638>`__ - Opened each log in a new tab
* `CDAP-8668 <https://issues.cask.co/browse/CDAP-8668>`__ - Fixed UI to show ERROR, WARN and INFO logs by default
* `CDAP-8965 <https://issues.cask.co/browse/CDAP-8965>`__ - Removed Wrangle button from Wrangler Transform. Please use the Data Preparation UI for wrangling.
* `HYDRATOR-1419 <https://issues.cask.co/browse/HYDRATOR-1419>`__ - Fixed browser back button behavior after switching namespace


======================
License and Trademarks
======================

Copyright © 2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
