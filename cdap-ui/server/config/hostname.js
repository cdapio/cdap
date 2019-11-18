/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import { spawn } from 'child_process';
import q from 'q';
import log4js from 'log4js';
import { StringDecoder } from 'string_decoder';

const decoder = new StringDecoder('utf8');
const log = log4js.getLogger('default');
const cache = {};
let buffer = '';

function onHostNameRead(data) {
  const textChunk = decoder.write(data);
  if (textChunk) {
    buffer += textChunk;
  }
}

function onHostNameReadFail(data) {
  const textChunk = decoder.write(data);
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

export function getHostName() {
  const deferred = q.defer();
  const hostnameTool = spawn('hostname', ['-f']);
  hostnameTool.stderr.on('data', onHostNameReadFail.bind(null, deferred));
  hostnameTool.stdout.on('data', onHostNameRead);
  hostnameTool.stdout.on('end', onHostNameReadEnd.bind(null, deferred));
  return deferred.promise;
}
