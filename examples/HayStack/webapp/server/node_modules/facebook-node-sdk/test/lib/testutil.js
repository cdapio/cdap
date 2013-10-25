var path = require('path');
var fs = require('fs');

exports.basedir = path.join(__dirname, '..', '..');
var covdir = path.join(exports.basedir, 'lib-cov');
var libdir = path.join(exports.basedir, 'lib');

try {
  var stat = fs.statSync(covdir);
  if (stat.isDirectory()) {
    libdir = covdir;
  }
}
catch (e) {
}

exports.libdir = libdir;

exports.fbDefaultConfig = {
  appId: '227710073967374',
  secret: 'a25a2216fb1b772f1c554ebb9d950aec'
}

if ('TEST_FB_APP_ID' in process.env) {
  exports.fbDefaultConfig.appId = process.env.TEST_FB_APP_ID;
}
if ('TEST_FB_SECRET' in process.env) {
  exports.fbDefaultConfig.secret = process.env.TEST_FB_SECRET;
}

