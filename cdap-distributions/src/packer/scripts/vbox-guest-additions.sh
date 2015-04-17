#!/bin/bash
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
