# to be run on beamer remote host like:
#
# ssh 146.148.40.168 -l adrien 'bash -s' < bin/deploy.sh
#
sudo su -
cd /var/beamer/web-app
git pull origin develop
npm install && bower install --allow-root
./node_modules/gulp/bin/gulp.js build
/etc/init.d/cdap-web-app restart
