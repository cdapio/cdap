#!/bin/bash
#
# Install X.org and remove unused drivers
#

# Install xorg
apt-get install -y xorg

# Remove X11 video drivers
for i in ati cirrus fbdev intel mach64 mga neomagic nouveau openchrome qxl r128 radeon s3 savage siliconmotion sis sisusb tdfx trident ; do 
  apt-get purge -y xserver-xorg-video-$i
done

# Remove X11 input drivers
for i in mouse synaptics wacom ; do
  apt-get purge -y xserver-xorg-input-$i
done

