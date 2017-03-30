/*
 * Copyright © 2015-2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

var yauzl = require("yauzl");
var path = require("path");
var mkdirp = require("mkdirp");

var fs = require('fs');
var archiver = require('archiver');
var minimist = require('minimist');
var Listr = require('listr');

var runtimearguments = minimist(process.argv.slice(2));
var newuizippath = runtimearguments['new-ui-zip-path'];
var cdaphome = process.env.CDAP_HOME;

var folderstozip = [
  '/dist/',
  '/cdap_dist/',
  '/node_modules/',
  '/common_dist/',
  '/dll/',
  '/login_dist/',
  '/old_dist/',
  '/server/',
  '/templates/',
  '/wrangler_dist/'
];
var filestozip = [
  '/package.json',
  '/server.js',
  '/VERSION'
];

var skipUpgrade = false;

function checkPreRequisite() {
  if (!newuizippath) {
    throw 'Missing UI Pack archive path. Please provide fully qualified path to the UI Pack archive.';
  }
  if (!fs.existsSync(newuizippath)) {
    throw 'UI Pack archive does not exists at the location \''+ newuizippath + '\'. Please check the path specified.';
  }
  if (newuizippath.slice(newuizippath.length - 4) !== '.zip') {
    throw 'UI Pack archive must be a zip file to be upgraded';
  }
  if (!cdaphome) {
    throw 'Environment variable $CDAP_HOME is not set. Please set or source variable, before running this script.';
  }
  if (!fs.existsSync(cdaphome)) {
    throw 'Path specified by $CDAP_HOME is not a valid CDAP installation directory. Please check the environment before running the script.';
  }
  if (!fs.existsSync(cdaphome + '/ui')) {
    throw cdaphome + '/ui directory does not exists. Make sure ' + cdaphome + ' is CDAP home directory.';
  }
  cdaphome = cdaphome[cdaphome.length - 1] === '/' ? cdaphome : cdaphome + '/';
}


function unzipNewUI() {
  var promise = new Promise(function(resolve, reject) {
    var _resolve = resolve;
    var _reject = reject;
    yauzl.open(newuizippath, {lazyEntries: true, autoClose: true}, function(err, zipfile) {
      if (err) {
        _reject (err);
        return;
      }
      zipfile.readEntry();
      zipfile.on("entry", function(entry) {
        if (/\/$/.test(entry.fileName)) {
          // directory file names end with '/'
          mkdirp(cdaphome + '/' + entry.fileName, function(err) {
            if (err) { _reject(err); return; }
            zipfile.readEntry();
          });
        } else {
          // file entry
          zipfile.openReadStream(entry, function(err, readStream) {
            if (err) {_reject(err); return; }
            // ensure parent directory exists
            mkdirp(path.dirname(cdaphome + '/' + entry.fileName), function(err) {
              if (err) {_reject(err); return; }
              readStream.pipe(fs.createWriteStream(cdaphome + entry.fileName, {flags: 'w+'}));
              readStream.on("end", function() {
                zipfile.readEntry();
              });
            });
          });
        }
      });
      zipfile.once('close', function() {
        _resolve();
      });
    });
  });
  return promise;
}

function zipExistingUI() {
  var promise = new Promise(function(resolve, reject) {

    try {
      var olduizip = fs.createWriteStream(cdaphome + '/ui_backup'+ Date.now() + '.zip');
      var archive = archiver('zip', { store: true });
      olduizip.on('close', function() {
      });

      archive.on('error', function(err) {
        console.log('Error: ', err);
      });

      folderstozip
        .filter(function(folder) {
          return fs.existsSync(cdaphome + '/ui/' + folder);
        })
        .forEach(function(folder) {
          archive.directory(cdaphome + '/ui/' + folder);
        });

      filestozip
        .filter(function(file) {
          return fs.existsSync(cdaphome + '/ui/' + file);
        })
        .forEach(function(file) {
          archive.file(cdaphome + '/ui/' + file);
        });
      archive.pipe(olduizip);
      archive.finalize();
      resolve();
    } catch (e) {
      reject(e);
    }
  });
  return promise;
}

var tasks = new Listr([
  {
    title: 'Verifying environment before upgrading UI Pack',
    task: function() {
      try {
        checkPreRequisite();
      } catch (e) {
        skipUpgrade = true;
        throw new Error(e);
      }
    },
    exitOnError: true
  },
  {
    title: 'Backing existing UI : <' + (cdaphome ? cdaphome : 'unknown-path') + '>',
    skip: function() {
      return skipUpgrade;
    },
    task: zipExistingUI.bind(null),
    exitOnError: true
  },
  {
    title: 'Updating with new UI Pack',
    skip: function() {
      return skipUpgrade;
    },
    task: unzipNewUI.bind(null),
    exitOnError: true
  }
]);

tasks
  .run()
  .catch(function(err) {
    console.error('\n \x1b[31m Failed to upgrade.\n \t', err.message);
    console.log('\n \x1b[33m Usage:');
    console.log('\tnpm run upgrade --new-ui-zip-path=<path-to-zip-file>');
  });
