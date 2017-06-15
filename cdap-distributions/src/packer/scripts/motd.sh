#!/bin/bash
#
# Copyright © 2015-2017 Cask Data, Inc.
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

for i in motd welcome.txt issue issue.net ; do

cat > /etc/$i << EOF
			Welcome to the CDAP Sandbox VM

This virtual machine uses a simple graphical interface. The menu can be accessed by
clicking the icon at the bottom left. Included are the Eclipse and IntelliJ IDEs,
Chromium Browser, Git, Java, Maven, Subversion, and the CDAP Sandbox with the Standalone CDAP.

The login and password to the machine is 'cdap' and the user has sudo privileges
without a password.

The Sandbox can be stopped/started/restarted using the /etc/init.d/cdap-sandbox init
script.

	sudo /etc/init.d/cdap-sandbox start
	sudo /etc/init.d/cdap-sandbox stop
	sudo /etc/init.d/cdap-sandbox restart

Logs for the Sandbox are found at /opt/cdap/sandbox/logs, data is stored at
/opt/cdap/sandbox/data, and example applications are at
/opt/cdap/sandbox/examples.

EOF
done

exit 0
