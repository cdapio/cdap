#!/bin/bash
#
# Zero free disk space
#

# Create zero-filled file
dd if=/dev/zero of=/deleteme bs=1M

# Remove file to free space
rm -f /deleteme
