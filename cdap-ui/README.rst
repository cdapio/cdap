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

  $ cd ${CDAP_HOME}
  $ /etc/init.d/cdap-ui stop
  $ zip -m -r ui-backup.zip ui
  $ unzip /path/to/download/cdap-ui-preview-pack.zip
  $ /etc/init.d/cdap-ui start



Windows
-------

**Note:** Currently, installation of the UI pack on Windows requires a zip utility - we recommend 7zip or equivalent.

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


Release Notes
=============

<Placeholder>


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
