#!/bin/bash
#
# Install and configure fluxbox
#

# Install
apt-get install -y fluxbox

# Create .fluxbox
mkdir -p ~cdap/.fluxbox
cd ~cdap/.fluxbox

# Symlink idea
ln -sf /opt/idea* /opt/idea

# Populate startup file
echo 'xterm &' > startup
echo '/opt/idea/bin/idea.sh &' >> startup
echo 'firefox http://localhost:9999 http://docs.cask.co/cdap &' >> startup
echo 'exec fluxbox' >> startup

# Fix permissions
chown -R cdap:cdap ~cdap
