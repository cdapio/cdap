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

#
# Install X.org and remove unused drivers
#

# Install xorg
apt-get install -y xorg

# Remove X11 video drivers
for i in ati cirrus fbdev intel mach64 mga neomagic nouveau openchrome qxl r128 radeon s3 savage siliconmotion sis sisusb tdfx trident ; do 
  apt-get purge -y xserver-xorg-video-$i
done

# Remove X11 input drivers
for i in mouse synaptics wacom ; do
  apt-get purge -y xserver-xorg-input-$i
done

exit 0
