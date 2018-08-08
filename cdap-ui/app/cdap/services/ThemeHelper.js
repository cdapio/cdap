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

function isTheme10Spec() {
  if (!objectQuery(window, 'CDAP_UI_THEME', 'spec-version')) {
    return false;
  }
  return parseFloat(window.CDAP_UI_THEME['spec-version']) >= '1.0';
}

export function applyTheme() {
  if (
    !objectQuery(window, 'CDAP_UI_THEME', 'styles') ||
    !isTheme10Spec() ||
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
    variables: window.CDAP_UI_THEME.styles
  });
}

export function hideDashboard() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'dashboard') === 'false'
  );
}

export function hideReports() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'reports') === 'false'
  );
}

export function hideDataPrep() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'data-prep') === 'false'
  );
}

export function hidePipelines() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'pipelines') === 'false'
  );
}

export function hideAnalytics() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'analytics') === 'false'
  );
}

export function hideRulesEngine() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'rules-engine') === 'false'
  );
}

export function hideMetadata() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'metadata') === 'false'
  );
}

export function hideHub() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'hub') === 'false'
  );
}

export function hideFooter() {
  return (
    isTheme10Spec() &&
    objectQuery(window, 'CDAP_UI_THEME', 'features', 'footer') === 'false'
  );
}

export function getFooterText() {
  if (!isTheme10Spec()) {
    return '';
  }
  return objectQuery(window, 'CDAP_UI_THEME', 'content', 'footer-text');
}

export function getFooterLink() {
  if (!isTheme10Spec()) {
    return '';
  }
  return objectQuery(window, 'CDAP_UI_THEME', 'content', 'footer-link');
}

export function getLogo() {
  if (
    !isTheme10Spec() ||
    !objectQuery(window, 'CDAP_UI_THEME', 'content', 'logo', 'type')
  ) {
    return '';
  }

  let logo = window.CDAP_UI_THEME.content.logo;
  let logoType = logo.type;
  if (logoType === 'inline') {
    return objectQuery(logo, 'arguments', 'data');
  } else if (logoType === 'link') {
    return objectQuery(logo, 'arguments', 'url');
  }
}
