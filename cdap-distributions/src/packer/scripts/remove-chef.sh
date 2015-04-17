#!/bin/bash
#
# Remove Chef and its directories
#

# Remove packages
apt-get purge -y chef

# Remove directory
rm -rf /var/chef
