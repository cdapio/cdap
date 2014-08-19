# About the hive-exec patch file

This patch file contains all the changes that had to be made to hive branch `release-0.13.0-rc2`
to make it work with CDAP 2.3.0 in singlenode and in unit-tests.
A hive-exec-0.13.0.jar is already uploaded to Nexus and is used by CDAP in
singlenode and for unit tests.

To modify the hive-exec jar again, follow these steps:

1. Clone Hive repository from Github and checkout to branch `release-0.13.0-rc2`
2. Create another branch: ``git checkout -b my_patched_branch``
3. Apply the existing patch:
  ``git apply --stat explore/hive-exec-0.13.0-continuuity-xxx.patch``
3. Make your changes in the ``ql`` module (it will be packaged as hive-exec.jar)
4. You can modify ``pom.xml`` in module ``ql`` to change the version number of the generated jar, according
to what version of CDAP you are modifying.
5. Deploy your jar to Nexus using this command in the ``ql`` directory:
  ``mvn clean deploy -DskipTests -P hadoop-2 -P sources``
6. Change your dependencies in CDAP to point to the same version number you specified in
   step 4. for hive-exec.
7. Don't forget to create a new patch file and to put it here:
  ``git format-patch release-0.13.0-rc2 --stdout > explore/hive-exec-0.13.0.patch``
  
## License and Trademarks

© Copyright 2014 Cask, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask, Inc. All rights reserved.
