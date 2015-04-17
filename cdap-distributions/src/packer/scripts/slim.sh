#!/bin/bash
#
# Install and configure slim
#

# Install
apt-get install -y slim

# Session for cdap user
echo 'exec startfluxbox' >> ~cdap/.xinitrc
chown -R cdap:cdap ~cdap

# Global configuration and auto-login
echo 'sessiondir /usr/share/xsessions/' >> /etc/slim.conf
echo 'default_user cdap' >> /etc/slim.conf
echo 'auto_login yes' >> /etc/slim.conf
