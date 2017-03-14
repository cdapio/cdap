==============
CDAP UI Pack 1
==============

The CDAP UI Pack is a UI-only release that can be applied on top of a released CDAP.

Details
=======
Release Date: 03/13/2017
Base CDAP Version: 4.1
Release branch: release/4.1
Git Tag: <TBD>

Installation
============
Currently, the following manual steps need to be performed to install a UI pack. These steps will be automated in a
later CDAP release

Unix/Linux Flavors
------------------

**Standalone CDAP**

::

  $ cd ${CDAP_HOME}
  $ ./bin/cdap sdk stop
  $ zip -m -r ui-backup.zip ui
  $ unzip /path/to/download/cdap-ui-pack.zip
  $ ./bin/cdap sdk start


**CDAP Cluster**

::

  $ cd ${CDAP_HOME}                        # (usually /opt/cdap)
  $ /etc/init.d/cdap-ui stop
  $ zip -m -r ui-backup.zip ui
  $ unzip /path/to/download/cdap-ui-pack-4.1.0_p1.zip
  $ /etc/init.d/cdap-ui start



Windows
-------

**Note:** Currently, installation of the UI pack on Windows requires a zip utility - we recommend
`7zip <http://www.7-zip.org/>`__ or equivalent.

1. Using the command prompt, stop the CDAP SDK

::

  $ cd ${CDAP_HOME}
  $ bin\cdap sdk stop

2. Open the ${CDAP_HOME} directory in Explorer
3. Compress the ``ui`` to save a backup, say ui-backup.zip
4. Delete the ``ui`` directory after the backup is completed
5. Extract the UI pack (cdap-ui-pack-4.1.0_p1.zip) in the ${CDAP_HOME} directory
6. A new ``ui`` directory should be created
7. Using the command prompt, start the CDAP SDK

::

  $ cd $(CDAP_HOME}
  $ bin\cdap sdk start


Steps to update Data Preparation capability
===========================================

1. Upon installing the UI Pack, go to Cask Market
2. From the **Solutions** category, follow the steps for the **Data Preparation** solution
3. Go to Data Preparation by clicking on the CDAP menu and then choosing **Data Preparation**
4. If newer version of the Data Preparation libraries have been installed, the UI will show an **Update** button.
5. Click the **Update** button to enable the newer version of Data Preparation.


Release Notes
=============

New Features
------------
* HYDRATOR-163 - Add Placeholders to input boxes in node configuration
* WRANGLER-77 - Added a dropdown on each column to provide click-through experience for directives in Data Preparation
* WRANGLER-49 - Added click-through experience for split column directive in Data Preparation
* WRANGLER-54 - Added click-through experience for filling null or empty cells in Data Preparation

Improvements
------------
* CDAP-8501 - Disabled preview button on clusters since preview is not supported in distributed env
* CDAP-8861 - Removed CDAP Version Range in market entities display
* CDAP-8430 - Improved "No Entities Found" message in the Overview to show Call(s) to Action
* CDAP-8403 - Added labels to CDAP Studio actions
* CDAP-8900 - Added the ability to update to a newer version of data preparation libraries if available
* CDAP-7352 - Made logviewer header row sticky
* CDAP-4798 - Improved user experience in explore page
* CDAP-8964 - Made Output Schema for sinks macro enabled
* HYDRATOR-1364 - Removed most of __ui__ field
* CDAP-8494 - Fixed browser back button after switching to classic UI
* CDAP-8828 - Removed dialog to select pipeline type upon pipeline creation
* CDAP-8396 - Added call to action for namespace creation

Bugs
----
* CDAP-8554 - Fixed styling issues while showing Call(s) to actions in Application create wizard
* CDAP-8412 - Fixed overflow in namespace creation confirmation modal
* CDAP-8433 - Added units for memory for YARN stats on management page
* CDAP-8950 - Fixed link from stream overview to stream deatils
* CDAP-8933 - Added namespace name to the No entities found message
* CDAP-8461 - Clicking back from the Detail page view now opens the overview page with the overview pane opened
* CDAP-8638 - Opened each log in a new tab
* CDAP-8668 - Fixed UI to show ERROR, WARN and INFO logs by default
* CDAP-8965 - Removed Wrangle button from Wrangler Transform. Please use the Data Preparation UI for wrangling.
* HYDRATOR-1419 - Fixed browser back button behavior after switching namespace.


======================
License and Trademarks
======================

Copyright © 2016-2017 Cask Data, Inc.

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
