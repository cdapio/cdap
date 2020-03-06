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

import log4js from 'log4js';
import path from 'path';
import get from 'lodash/get';
import merge from 'lodash/merge';
import fs from 'fs';

const CDAP_DIST_PATH = path.normalize(__dirname + '/../public/cdap_dist');
const log = log4js.getLogger('default');
const uiThemePropertyName = 'ui.theme.file';

export function extractUIThemeWrapper(cdapConfig) {
  const uiThemePath = cdapConfig[uiThemePropertyName];
  return extractUITheme(cdapConfig, uiThemePath);
}

function extractUIFeaturesFromConfig(cdapConfig) {
  const FEATURE_PREFIX = 'ui.feature.';
  const acceptableString = ['true', 'false'];

  const uiFeatures = Object.keys(cdapConfig).filter((configKey) => {
    return (
      configKey.startsWith(FEATURE_PREFIX) &&
      acceptableString.indexOf(cdapConfig[configKey].toString()) !== -1
    );
  });

  const featuresMap = {};

  uiFeatures.forEach((configKey) => {
    const featureKey = configKey.slice(FEATURE_PREFIX.length);
    featuresMap[featureKey] = cdapConfig[configKey].toString() === 'true';
  });

  return featuresMap;
}

function mergeUIThemeWithConfig(cdapConfig, themeConfig) {
  const configFeatures = {
    features: extractUIFeaturesFromConfig(cdapConfig),
  };

  return merge(themeConfig, configFeatures);
}

export function extractUITheme(cdapConfig, uiThemePath) {
  const DEFAULT_CONFIG = {};

  if (!(uiThemePropertyName in cdapConfig)) {
    log.warn(`Unable to find ${uiThemePropertyName} property`);
    log.warn(`UI using default theme`);
    return mergeUIThemeWithConfig(cdapConfig, DEFAULT_CONFIG);
  }

  let uiThemeConfig = DEFAULT_CONFIG;
  // Absolute path
  if (uiThemePath[0] === '/') {
    try {
      if (__non_webpack_require__.resolve(uiThemePath)) {
        uiThemeConfig = __non_webpack_require__(uiThemePath);
        log.info(`UI using theme file: ${uiThemePath}`);
        return mergeUIThemeWithConfig(cdapConfig, uiThemeConfig);
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
      /**
       * Two paths
       * theme file path : config/themes/light.json | server/config/themes/light.json
       * __dirname: <cdap-home>/ui | <cdap-home/ui/server
       *
       * The configs exists in <cdap-home>/ui/sever/config/themes/*.json
       *
       */
      themePath = uiThemePath;
      if (uiThemePath.startsWith('server') && __dirname.endsWith('server')) {
        themePath = path.join('..', uiThemePath);
      }

      if (uiThemePath.startsWith('config') && !__dirname.endsWith('server')) {
        themePath = path.join('server', uiThemePath);
      }
      themePath = path.join(__dirname, themePath);

      if (__non_webpack_require__.resolve(themePath)) {
        uiThemeConfig = __non_webpack_require__(themePath);
        log.info(`UI using theme file: ${themePath}`);
        return mergeUIThemeWithConfig(cdapConfig, uiThemeConfig);
      }
    } catch (e) {
      // This will show the user what the full path is.
      // This should help them give proper relative path
      log.info('UI Theme file not found at: ', themePath);

      console.log('e', e);
      throw e;
    }
  }
  return mergeUIThemeWithConfig(cdapConfig, uiThemeConfig);
}

export function getFaviconPath(uiThemeConfig) {
  let faviconPath = CDAP_DIST_PATH + '/cdap_assets/img/favicon.png';
  let themeFaviconPath = get(uiThemeConfig, ['content', 'favicon-path']);
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
