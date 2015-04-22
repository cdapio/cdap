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
# Install Virtualbox Guest Additions
#

# Mount ISO
mount -o ro,loop /root/VBoxGuestAdditions.iso /mnt

# Run installer
/mnt/VBoxLinuxAdditions.run

# Unmount and remove ISO
umount /mnt
rm -f /root/VBoxGuestAdditions.iso
