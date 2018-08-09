/*
 * Copyright © 2018 Cask Data, Inc.
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

import cssVars from 'css-vars-ponyfill';
import {objectQuery, isNilOrEmpty} from 'services/helpers';

declare global {
  interface Window {
    CDAP_UI_THEME: any;
  }
}

export function applyTheme() {
  if (
    !objectQuery(window, 'CDAP_UI_THEME', 'styles') ||
    isNilOrEmpty(window.CDAP_UI_THEME.styles)
  ) {
    // need to run this at least once even if there's no theme customization
    // so that css variables are parsed correctly even in older browsers
    cssVars();
    return;
  }

  // this is what's going on under the hood for modern browsers:
  // document.documentElement.style.setProperty(`--${cssVar}`, cssValue);
  cssVars({
    variables: window.CDAP_UI_THEME.styles,
  });
}

interface ThemeObj {
  [key: string]: boolean;
}

function getTheme(): ThemeObj {
  if (isNilOrEmpty(window.CDAP_UI_THEME)) {
    return {};
  }

  let theme: ThemeObj = {};
  const themeJSON = window.CDAP_UI_THEME;
  const specVersion = themeJSON['spec-version'];

  if (specVersion === '1.0') {
    theme = get10Content(theme, themeJSON);
    theme = get10Features(theme, themeJSON);
  }
  return theme;
}

function get10Content(theme, themeJSON): ThemeObj {
  const content = themeJSON.content;
  if ('footer-text' in content) {
    theme.footerText = content['footer-text'];
  }
  if ('footer-link' in content) {
    theme.footerLink = content['footer-link'];
  }
  if ('logo' in content) {
    const logo = window.CDAP_UI_THEME.content.logo;
    if (logo.type) {
      const logoType = logo.type;
      if (logoType === 'inline') {
        theme.logo = objectQuery(logo, 'arguments', 'data');
      } else if (logoType === 'link') {
        theme.logo = objectQuery(logo, 'arguments', 'url');
      }
    }
  }
  return theme;
}

function get10Features(theme, themeJSON): ThemeObj {
  const features = themeJSON.features;
  if ('dashboard' in features) {
    theme.showDashboard = features.dashboard;
  }
  if ('reports' in features) {
    theme.showReports = features.reports;
  }
  if ('data-prep' in features) {
    theme.showDataPrep = features['data-prep'];
  }
  if ('pipelines' in features) {
    theme.showPipelines = features.pipelines;
  }
  if ('analytics' in features) {
    theme.showAnalytics = features.analytics;
  }
  if ('rules-engine' in features) {
    theme.showRulesEngine = features['rules-engine'];
  }
  if ('metadata' in features) {
    theme.showMetadata = features.metadata;
  }
  if ('hub' in features) {
    theme.showHub = features.hub;
  }
  if ('footer' in features) {
    theme.showFooter = features.footer;
  }
  return theme;
}

export const Theme = getTheme();
