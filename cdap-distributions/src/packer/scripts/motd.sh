#!/bin/bash
#
# Copyright © 2015 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

echo "			Welcome to the CDAP SDK VM

This virtual machine uses a simple graphical interface. The menu can be accessed
by right clicking on the background. If you have a Mac, this is a two-finger tap
on the trackpad. Included are the Eclipse IDE and IntelliJ IDE, Firefox, Git,
Subversion and the CDAP SDK.

The login and password to the machine is "cdap" and the user has sudo privileges
without a password.

The SDK can be stopped/started/restarted using the /etc/init.d/cdap-sdk init
script.

	sudo /etc/init.d/cdap-sdk start
	sudo /etc/init.d/cdap-sdk stop
	sudo /etc/init.d/cdap-sdk restart

Logs for the SDK are found at /opt/cdap/sdk/logs, data is stored at
/opt/cdap/sdk/data, and example applications are at
/opt/cdap/sdk/examples.

" > /etc/motd
