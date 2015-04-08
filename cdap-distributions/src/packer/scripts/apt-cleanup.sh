#!/bin/bash
#
# Removes stuff from APT and clears caches
#

# Remove old kernels/headers
apt-get remove --purge -y $(dpkg -l 'linux-image-*' | sed '/^ii/!d;/'"$(uname -r | sed "s/\(.*\)-\([^0-9]\+\)/\1/")"'/d;s/^[^ ]* [^ ]* \([^ ]*\).*/\1/;/[0-9]/!d')
apt-get remove --purge -y $(dpkg -l 'linux-headers-*' | sed '/^ii/!d;/'"$(uname -r | sed "s/\(.*\)-\([^0-9]\+\)/\1/")"'/d;s/^[^ ]* [^ ]* \([^ ]*\).*/\1/;/[0-9]/!d')

# Remove build packages
apt-get purge -y build-essential autoconf autogen automake autotools-dev

# Remove fuse
apt-get purge -y fuse

# Autoremove and clean
apt-get autoremove -y
apt-get autoclean
