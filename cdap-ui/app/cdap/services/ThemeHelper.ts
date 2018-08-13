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
  [key: string]: any;
}

function getTheme(): ThemeObj {
  if (isNilOrEmpty(window.CDAP_UI_THEME)) {
    return {};
  }

  let theme: ThemeObj = {};
  const themeJSON = window.CDAP_UI_THEME;
  const specVersion = themeJSON['spec-version'];

  if (specVersion === '1.0') {
    theme = parse1Point0Spec(themeJSON);
  }
  return theme;
}

function parse1Point0Spec(themeJSON): ThemeObj {
  const theme: ThemeObj = {};

  function getContent(): ThemeObj {
    const contentJson = themeJSON.content;
    const content: ThemeObj = {};
    if (isNilOrEmpty(contentJson)) {
      return content;
    }
    if ('footer-text' in contentJson) {
      content.footerText = contentJson['footer-text'];
    }
    if ('footer-link' in contentJson) {
      content.footerLink = contentJson['footer-link'];
    }
    if ('logo' in contentJson) {
      const logo = window.CDAP_UI_THEME.content.logo;
      if (logo.type) {
        const logoType = logo.type;
        if (logoType === 'inline') {
          content.logo = objectQuery(logo, 'arguments', 'data');
        } else if (logoType === 'link') {
          content.logo = objectQuery(logo, 'arguments', 'url');
        }
      }
    }
    return content;
  }

  function getFeatures(): ThemeObj {
    const featuresJson = themeJSON.features;
    const features: ThemeObj = {};
    if (isNilOrEmpty(featuresJson)) {
      return features;
    }
    if ('dashboard' in featuresJson) {
      features.showDashboard = featuresJson.dashboard;
    }
    if ('reports' in featuresJson) {
      features.showReports = featuresJson.reports;
    }
    if ('data-prep' in featuresJson) {
      features.showDataPrep = featuresJson['data-prep'];
    }
    if ('pipelines' in featuresJson) {
      features.showPipelines = featuresJson.pipelines;
    }
    if ('analytics' in featuresJson) {
      features.showAnalytics = featuresJson.analytics;
    }
    if ('rules-engine' in featuresJson) {
      features.showRulesEngine = featuresJson['rules-engine'];
    }
    if ('metadata' in featuresJson) {
      features.showMetadata = featuresJson.metadata;
    }
    if ('hub' in featuresJson) {
      features.showHub = featuresJson.hub;
    }
    if ('footer' in featuresJson) {
      features.showFooter = featuresJson.footer;
    }
    if ('ingest-data' in featuresJson) {
      features.showIngestData = featuresJson['ingest-data'];
    }
    if ('add-namespace' in featuresJson) {
      features.showAddNamespace = featuresJson['add-namespace'];
    }
    return features;
  }

  return {
    ...theme,
    ...getContent(),
    ...getFeatures(),
  };
}

export const Theme = getTheme();
