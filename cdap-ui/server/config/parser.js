// @ts-nocheck
/*
 * Copyright © 2015-2020 Cask Data, Inc.
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

/* global process */

import q from 'q';
import log4js from 'log4js';
import ConfigReader from 'server/config/config-reader';

const cache = {};
let path;
const log = log4js.getLogger('default');

export function extractUISettings() {
  try {
    if (__non_webpack_require__.resolve('./server/config/ui-settings.json')) {
      return __non_webpack_require__('./server/config/ui-settings.json') || {};
    }
  } catch (e) {
    log.info('Unable to find UI settings json file.: ' + e + ' : ' + __dirname);
    return {};
  }
}

/*
 *  Extracts the config
 *  @returns {promise}
 */

export function extractConfig(param) {
  var deferred = q.defer();
  param = param || 'cdap';
  if (cache[param]) {
    deferred.resolve(cache[param]);
    return deferred.promise;
  }

  if (process.env.NODE_ENV === 'production') {
    const configReader = new ConfigReader(param);
    configReader
      .getPromise()
      .then((config) => {
        cache[param] = config;
        deferred.resolve(cache[param]);
      })
      .catch((error) => {
        deferred.reject(error);
      });
  } else {
    try {
      path = getConfigPath(param);
      if (path && path.length) {
        path = path.replace(/\"/g, '');
        cache[param] = __non_webpack_require__(path);
      } else {
        throw 'No configuration JSON provided.(No "cConf" and "sConf" commandline arguments passed)';
      }
    } catch (e) {
      log.warn(e);
      // Indicates the backend is not running in local environment and that we want only the
      // UI to be running. This is here for convenience.
      log.warn('Using development configuration for "' + param + '"');
      cache[param] = __non_webpack_require__('./server/config/development/' + param + '.json');
    }

    deferred.resolve(cache[param]);
  }
  return deferred.promise;
}

function getConfigPath(param) {
  var configName = param === 'security' ? 'sConf' : 'cConf';
  // If cConf and sConf are not provided (Starting node server
  // from console) default to development config.
  if (process.argv.length < 3) {
    return null;
  }
  var args = process.argv.slice(2),
    value = '',
    i;
  for (i = 0; i < args.length; i++) {
    if (args[i].indexOf(configName) !== -1) {
      value = args[i].split('=');
      if (value.length > 1) {
        value = value[1];
      }
      break;
    }
  }
  return value;
}
