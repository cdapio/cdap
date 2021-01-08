/*
 * Copyright © 2021 Cask Data, Inc.
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

const fs = require('fs');
const path = require('path');
const yauzl = require("yauzl");
const mkdirp = require("mkdirp");
async function unzipSDK(sdkzippath) {
  new Promise(function(resolve, reject) {
    var _resolve = resolve;
    var _reject = reject;
    try {
      yauzl.open(sdkzippath, {lazyEntries: true, autoClose: true}, function(err, zipfile) {
        if (err) {
          _reject(err);
          return;
        }
        zipfile.readEntry();
        zipfile.on("entry", async function(entry) {
          if (/\/$/.test(entry.fileName)) {
            // directory file names end with '/'
            console.log(path.join('/workspace', entry.fileName));
            try {
              await mkdirp(entry.fileName)
              zipfile.readEntry();
            } catch(err) {
              _reject(err); return; 
            }
          } else {
            // file entry
            zipfile.openReadStream(entry, async function(err, readStream) {
              if (err) {_reject(err); return; }
              // ensure parent directory exists
              try {
                await mkdirp(path.dirname(path.join('.', entry.fileName)));
                readStream.pipe(fs.createWriteStream(path.join('.', entry.fileName), {flags: 'w+', mode: 0o755}));
                readStream.on("end", function() {
                  zipfile.readEntry();
                });
              } catch(err) {
                _reject(err);
                return;
              }
            });
          }
        });
        zipfile.once('close', function() {
          _resolve();
        });
      });
    }catch(e) {
      _reject(e);
    }
  });
}
module.exports = unzipSDK;