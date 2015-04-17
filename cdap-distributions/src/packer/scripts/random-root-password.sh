#!/bin/bash
#
# Set a random root password
#

# Install apg
apt-get install -y apg

# Setup cdap for sudo
usermod -G sudo,adm,cdrom,dip,plugdev,lpadmin,sambashare cdap

# Use apg to generate a 14-character password and assign to root
echo "root:`apg -a 1 -m 14 -n 1`" | chpasswd

# Remove apg
apt-get purge -y apg
