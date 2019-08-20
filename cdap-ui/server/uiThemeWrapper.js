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

const log4js = require('log4js'),
  path = require('path'),
  objectQuery = require('lodash/get'),
  fs = require('fs'),
  DIST_PATH = path.normalize(__dirname + '/../dist'),
  CDAP_DIST_PATH = path.normalize(__dirname + '/../cdap_dist');
const log = log4js.getLogger('default');
const uiThemePropertyName = 'ui.theme.file';

function extractUIThemeWrapper(cdapConfig) {
  const uiThemePath = cdapConfig[uiThemePropertyName];
  return extractUITheme(cdapConfig, uiThemePath);
}

function extractUITheme(cdapConfig, uiThemePath) {
  const DEFAULT_CONFIG = {};

  if (!(uiThemePropertyName in cdapConfig)) {
    log.warn(`Unable to find ${uiThemePropertyName} property`);
    log.warn(`UI using default theme`);
    return DEFAULT_CONFIG;
  }

  let uiThemeConfig = DEFAULT_CONFIG;
  // Absolute path
  if (uiThemePath[0] === '/') {
    try {
      if (require.resolve(uiThemePath)) {
        uiThemeConfig = require(uiThemePath);
        log.info(`UI using theme file: ${uiThemePath}`);
        return uiThemeConfig;
      }
    } catch (e) {
      log.info('UI Theme file not found at: ', uiThemePath);
      throw e;
    }
  }
  // Relative path

  {
    let themePath;

    try {
      // __dirname will always be <entire-cdap-home>/ui/server.
      // So the uiThemePath should be relative to the ui folder
      // Ideally this will be of the form 'server/config/themes/default|light.json
      // For development since we are starting node from the ui folder we need to drop
      // the 'server' from the beginning of the path (config/themes/default|light.json)

      if (uiThemePath.startsWith('server')) {
        uiThemePath = path.join('..', uiThemePath);
      }

      themePath = path.join(__dirname, uiThemePath);

      if (require.resolve(themePath)) {
        uiThemeConfig = require(themePath);
        log.info(`UI using theme file: ${themePath}`);
        return uiThemeConfig;
      }
    } catch (e) {
      // This will show the user what the full path is.
      // This should help them give proper relative path
      log.info('UI Theme file not found at: ', themePath);
      throw e;
    }
  }
  return uiThemeConfig;
}

function getFaviconPath(uiThemeConfig) {
  let faviconPath = DIST_PATH + '/assets/img/favicon.png';
  let themeFaviconPath = objectQuery(uiThemeConfig, ['content', 'favicon-path']);
  if (themeFaviconPath) {
    // If absolute path no need to modify as require'ing absolute path should
    // be fine.
    if (themeFaviconPath[0] !== '/') {
      themeFaviconPath = `${CDAP_DIST_PATH}/${themeFaviconPath}`;
    }
    try {
      if (fs.existsSync(themeFaviconPath)) {
        faviconPath = themeFaviconPath;
      } else {
        log.warn(`Unable to find favicon at path ${themeFaviconPath}`);
      }
    } catch (e) {
      log.warn(`Unable to find favicon at path ${themeFaviconPath}`);
    }
  }
  return faviconPath;
}

module.exports = {
  extractUIThemeWrapper,
  extractUITheme,
  getFaviconPath,
};
