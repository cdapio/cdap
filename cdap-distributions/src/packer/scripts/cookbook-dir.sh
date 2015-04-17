#!/bin/bash
#
# Sets up Chef cookbook directory for "knife cookbook site install"
#

# Create directory
mkdir -p /var/chef/cookbooks
cd /var/chef/cookbooks

# Initialize Git repository
touch .gitignore
git init
git add .gitignore
git commit -m 'Initial commit'
