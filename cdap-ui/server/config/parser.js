/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

/*global require, module, process */

module.exports = {
  extractConfig: extractConfig,
  extractUISettings: extractUISettings,
  extractUITheme: extractUITheme
};

var promise = require('q'),
    spawn = require('child_process').spawn,
    StringDecoder = require('string_decoder').StringDecoder,
    decoder = new StringDecoder('utf8'),
    log4js = require('log4js'),
    cache = {},
    path,
    buffer = '',
    request = require('request');

var log = log4js.getLogger('default');

function extractUISettings() {
  try {
    if (require.resolve('./ui-settings.json')) {
      return require('./ui-settings.json') || {};
    }
  } catch (e) {
    log.info('Unable to find UI settings json file.');
    return {};
  }
}

/*
 *  Extracts contents of theme object in the specified theme file
 *  @returns {promise}
 */

async function extractUITheme() {
  const uiThemePropertyName = 'ui.theme.file';
  const DEFAULT_CONFIG = {};
  const cdapConfig = await extractConfig('cdap');
  let cdapSiteContents;
  try {
    cdapSiteContents = await getCdapSiteContents(cdapConfig);
  } catch (e) {
    log.info(e.toString());
    return DEFAULT_CONFIG;
  }

  try {
    cdapSiteContents = JSON.parse(cdapSiteContents);
    const uiThemeProperty = cdapSiteContents.find(property => property.name === uiThemePropertyName);
    if (!uiThemeProperty) {
      log.info(`Unable to find ${uiThemePropertyName} property`);
      return DEFAULT_CONFIG;
    }
    const uiThemeConfig = getUIThemeConfig(uiThemeProperty);
    return uiThemeConfig;

  } catch (error) {
    log.info('Unable to parse CDAP config');
    return DEFAULT_CONFIG;
  }


}

/*
 *  Extracts the config
 *  @returns {promise}
 */

function extractConfig(param) {
  var deferred = promise.defer(),
      tool;
  param = param || 'cdap';

  if (cache[param]) {
    deferred.resolve(cache[param]);
    return deferred.promise;
  }

  if (process.env.NODE_ENV === 'production') {
    buffer = '';
    tool = spawn(__dirname + '/../../bin/cdap', ['config-tool', '--'+param]);
    tool.stderr.on('data', configReadFail.bind(this));
    tool.stdout.on('data', configRead.bind(this));
    tool.stdout.on('end', onConfigReadEnd.bind(this, deferred, param));
  } else {
    try {
      path = getConfigPath(param);
      if (path && path.length) {
        path = path.replace(/\"/g, '');
        cache[param] = require(path);
      } else {
        throw 'No configuration JSON provided.(No "cConf" and "sConf" commandline arguments passed)';
      }
    } catch (e) {
      log.warn(e);
      // Indicates the backend is not running in local environment and that we want only the
      // UI to be running. This is here for convenience.
      log.warn('Using development configuration for "' + param + '"');
      cache[param] = require('./development/'+param+'.json');
    }

    deferred.resolve(cache[param]);
  }
  return deferred.promise;
}

function getConfigPath(param) {
  var configName = (param ==='security'? 'sConf': 'cConf');
  // If cConf and sConf are not provided (Starting node server
  // from console) default to development config.
  if (process.argv.length < 3) {
    return null;
  }
  var args = process.argv.slice(2),
      value = '',
      i;
  for (i=0; i<args.length; i++) {
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

function onConfigReadEnd (deferred, param) {
   cache[param] = JSON.parse(buffer);
   deferred.resolve(cache[param]);
}

function configRead (data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    buffer += textChunk;
  }
}

function configReadFail (data) {
  var textChunk = decoder.write(data);
  if (textChunk) {
    log.error(textChunk);
  }
}

function getCdapSiteContents(cdapConfig) {
  let url;
  const deferred = promise.defer();
  const routerServerAddress = cdapConfig['router.server.address'];

  if (cdapConfig['ssl.external.enabled'] === 'true') {
    url = `https://${routerServerAddress}:${cdapConfig['router.ssl.server.port']}`;
  } else {
    url = `http://${routerServerAddress}:${cdapConfig['router.server.port']}`;
  }
  url += '/v3/config/cdap';

  const requestConfig = {
    method: 'GET',
    url,
    rejectUnauthorized: false,
    requestCert: true,
    agent: false
  };

  request(requestConfig, function(err, response, body) {
    if (err || response.statusCode >= 400) {
      deferred.reject(new Error('Unable to get CDAP config'));
      return;
    }
    deferred.resolve(body);
  });

  return deferred.promise;
}

function getUIThemeConfig(uiThemeProperty) {
  let uiThemeConfig = {};
  let uiThemePath = uiThemeProperty.value;
  if (uiThemePath[0] !== '/') {
    // if path doesn't start with /, then it's a relative path
    // have to add the ellipses to navigate back to $CDAP_HOME, before
    // going through the path
    uiThemePath = `../../../${uiThemePath}`;
  }
  try {
    if (require.resolve(uiThemePath)) {
      uiThemeConfig = require(uiThemePath);
      log.info(`UI using theme located at ${uiThemeProperty.value}`);
    }
  } catch (e) {
    // The error can either be file doesn't exist, or file contains invalid json
    log.info(e.toString());
  }
  return uiThemeConfig;
}
