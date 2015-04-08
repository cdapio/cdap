#!/bin/bash
#
# Download cookbooks from Chef Supermarket
#

# Grab cookbooks using knife
for cb in idea maven nodejs cdap ; do
  knife cookbook site install $cb
done

# Do not change HOME for cdap user
sed -i '/ home /d' /var/chef/cookbooks/cdap/recipes/sdk.rb
