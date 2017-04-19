#!/bin/bash
#
# Copyright © 2015-2017 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

#
# Install and configure fluxbox
#

# Install
apt-get install -y --no-install-recommends lxde lxsession-logout chromium-browser openbox-gnome-session policykit-1 || exit 1

# Symlink idea
ln -sf /opt/idea* /opt/idea
# Copy icons
cp -f /opt/idea/bin/idea.png /usr/share/pixmaps
cp -f /usr/local/eclipse/icon.xpm /usr/share/pixmaps/eclipse.xpm
cp -f /opt/cdap/sdk/ui/dist/assets/img/favicon.png /usr/share/pixmaps/cdap.png

# Eclipse Menu entry
cat > /usr/share/applications/eclipse.desktop << EOF
[Desktop Entry]
Encoding=UTF-8
Name=Eclipse IDE
Comment=Start Eclipse IDE
Exec=/usr/local/bin/eclipse
TryExec=/usr/local/bin/eclipse
Type=Application
Icon=eclipse
Categories=GNOME;GTK;Development;
EOF

# CDAP Docs Menu Entry
cat > /usr/share/applications/cdap-docs.desktop << EOF
[Desktop Entry]
Encoding=UTF-8
Name=CDAP Docs
Comment=CDAP Documenation Site
Exec=chromium-browser http://docs.cask.co/cdap 
Type=Application
Icon=cdap
Categories=GNOME;GTK;Development;
EOF

# CDAP UI Menu Entry
cat > /usr/share/applications/cdap-ui.desktop << EOF
[Desktop Entry]
Encoding=UTF-8
Name=CDAP UI
Comment=CDAP UI Web Interface
Exec=chromium-browser http://localhost:9999
Type=Application
Icon=cdap
Categories=GNOME;GTK;Development;
EOF

# Copy welcome.txt and some icons to the desktop
mkdir -p ~cdap/Desktop
cp /etc/welcome.txt ~cdap/Desktop
for i in cdap-ui cdap-docs eclipse idea lxterminal ; do
  cp /usr/share/applications/${i}.desktop ~cdap/Desktop
done

# Customize look and feel
sed -i \
    -e 's/wallpaper_mode=.*/wallpaper_mode=0/' \
    -e 's/desktop_bg=.*/desktop_bg=#7f7f7f/' \
  /usr/share/lxde/pcmanfm/LXDE.conf

# Fix permissions
chown -R cdap:cdap ~cdap
