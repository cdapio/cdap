/*
 * Copyright © 2019 Cask Data, Inc.
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
var spawn = require('child_process').spawn;
var promise = require('q');
var StringDecoder = require('string_decoder').StringDecoder;
var decoder = new StringDecoder('utf8');
var log4js = require('log4js');
var log = log4js.getLogger('default');
var cache = {};
var buffer = '';

function onHostNameRead(data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    buffer += textChunk;
  }
}

function onHostNameReadFail(data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    log.error(textChunk);
  }
}

function onHostNameReadEnd(deferred, param) {
  if (buffer) {
    buffer = buffer.trim();
  }
  cache[param] = buffer;
  deferred.resolve(cache[param]);
}

function getHostName() {
  var deferred = promise.defer();
  var hostnameTool = spawn('hostname', ['-f']);
  hostnameTool.stderr.on('data', onHostNameReadFail.bind(null, deferred));
  hostnameTool.stdout.on('data', onHostNameRead);
  hostnameTool.stdout.on('end', onHostNameReadEnd.bind(null, deferred));
  return deferred.promise;
}

module.exports = getHostName;
