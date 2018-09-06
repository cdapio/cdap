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
import isColor from 'is-color';
import isBoolean from 'lodash/isBoolean';

interface IThemeJSON {
  "spec-version": string;
}

interface IOnePoint0SpecJSON extends IThemeJSON {
  "styles"?: {
    "brand-primary-color"?: string;
    "navbar-color"?: string;
    "font-family"?: string;
  };
  "content"?: {
    "product-name"?: string;
    "logo"?: {
      "type"?: string;
      "arguments"?: {
         "url"?: string;
         "data"?: string;
      }
    },
    "footer-text"?: string;
    "footer-link"?: string;
  };
  "features"?: {
    "dashboard"?: boolean;
    "reports"?: boolean;
    "data-prep"?: boolean;
    "pipelines"?: boolean;
    "analytics"?: boolean;
    "rules-engine"?: boolean;
    "metadata"?: boolean;
    "hub"?: boolean;
    "footer"?: boolean;
    "ingest-data"?: boolean;
    "add-namespace"?: boolean;
  };
}

// TODO: Investigate moving this to a separate typings folder, as we shouldn't
// do this in multiple places
declare global {
  /* tslint:disable:interface-name */
  interface Window {
    CDAP_UI_THEME: IOnePoint0SpecJSON;
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

  function apply1Point0Styles() {
    const stylesJSON = window.CDAP_UI_THEME.styles;
    const stylesToApply: IOnePoint0SpecJSON['styles'] = {};
    if ('brand-primary-color' in stylesJSON && isColor(stylesJSON['brand-primary-color'])) {
      stylesToApply['brand-primary-color'] = stylesJSON['brand-primary-color'];
    }
    if ('navbar-color' in stylesJSON && isColor(stylesJSON['navbar-color'])) {
      stylesToApply['navbar-color'] = stylesJSON['navbar-color'];
    }
    // TODO: Validate fonts more rigorously
    if ('font-family' in stylesJSON && typeof stylesJSON['font-family'] === 'string') {
      stylesToApply['font-family'] = stylesJSON['font-family'];
    }

    // this is what's going on under the hood for modern browsers:
    // document.documentElement.style.setProperty(`--${cssVar}`, cssValue);
    cssVars({
      variables: stylesToApply,
    });
  }

  const specVersion = window.CDAP_UI_THEME['spec-version'];
  if (specVersion === '1.0') {
    apply1Point0Styles();
  }
  return;
}

interface IThemeObj {
  productName?: string;
  footerText?: string;
  footerLink?: string;
  logo?: string;
  showDashboard?: boolean;
  showReports?: boolean;
  showDataPrep?: boolean;
  showPipelines?: boolean;
  showAnalytics?: boolean;
  showRulesEngine?: boolean;
  showMetadata?: boolean;
  showHub?: boolean;
  showFooter?: boolean;
  showIngestData?: boolean;
  showAddNamespace?: boolean;
}

function getTheme(): IThemeObj {
  if (isNilOrEmpty(window.CDAP_UI_THEME)) {
    return {};
  }

  let theme: IThemeObj = {};
  const themeJSON = window.CDAP_UI_THEME;
  const specVersion = themeJSON['spec-version'];

  if (specVersion === '1.0') {
    theme = parse1Point0Spec(themeJSON);
  }
  return theme;
}

function parse1Point0Spec(themeJSON: IOnePoint0SpecJSON): IThemeObj {
  const theme: IThemeObj = {};

  function getContent(): IThemeObj {
    const contentJson = themeJSON.content;
    const content: IThemeObj = {
      productName: 'CDAP',
    };
    if (isNilOrEmpty(contentJson)) {
      return content;
    }
    if ('product-name' in contentJson) {
      content.productName = contentJson['product-name'];
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

  function getFeatures(): IThemeObj {
    const featuresJson = themeJSON.features;
    const features: IThemeObj = {
      showDashboard: true,
      showReports: true,
      showDataPrep: true,
      showPipelines: true,
      showAnalytics: true,
      showRulesEngine: true,
      showMetadata: true,
      showHub: true,
      showFooter: true,
      showIngestData: true,
      showAddNamespace: true,
    };
    if (isNilOrEmpty(featuresJson)) {
      return features;
    }
    if ('dashboard' in featuresJson && isBoolean(featuresJson.dashboard)) {
      features.showDashboard = featuresJson.dashboard;
    }
    if ('reports' in featuresJson && isBoolean(featuresJson.reports)) {
      features.showReports = featuresJson.reports;
    }
    if ('data-prep' in featuresJson && isBoolean(featuresJson['data-prep'])) {
      features.showDataPrep = featuresJson['data-prep'];
    }
    if ('pipelines' in featuresJson && isBoolean(featuresJson.pipelines)) {
      features.showPipelines = featuresJson.pipelines;
    }
    if ('analytics' in featuresJson && isBoolean(featuresJson.analytics)) {
      features.showAnalytics = featuresJson.analytics;
    }
    if ('rules-engine' in featuresJson && isBoolean(featuresJson['rules-engine'])) {
      features.showRulesEngine = featuresJson['rules-engine'];
    }
    if ('metadata' in featuresJson && isBoolean(featuresJson.metadata)) {
      features.showMetadata = featuresJson.metadata;
    }
    if ('hub' in featuresJson && isBoolean(featuresJson.hub)) {
      features.showHub = featuresJson.hub;
    }
    if ('footer' in featuresJson && isBoolean(featuresJson.footer)) {
      features.showFooter = featuresJson.footer;
    }
    if ('ingest-data' in featuresJson && isBoolean(featuresJson['ingest-data'])) {
      features.showIngestData = featuresJson['ingest-data'];
    }
    if ('add-namespace' in featuresJson && isBoolean(featuresJson['add-namespace'])) {
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
