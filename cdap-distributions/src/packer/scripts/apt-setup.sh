#!/bin/bash
#
# Update APT cache and upgrade system
#

# Remove bad files from Ubuntu 12 CD
rm -rf /var/lib/apt/lists/*

# Update cache
apt-get update

# Update system
apt-get dist-upgrade -y
