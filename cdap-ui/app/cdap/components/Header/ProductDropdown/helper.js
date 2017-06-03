/*
 * Copyright © 2017 Cask Data, Inc.
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

import T from 'i18n-react';

export function getMode() {
  if (window.CDAP_CONFIG.isEnterprise) {
    return T.translate('features.Navbar.ProductDropdown.modes.distributed');
  }
  let sandboxMode = window.CDAP_CONFIG.sandboxMode;
  if (sandboxMode === 'azure' || sandboxMode === 'aws' || sandboxMode === 'gcp') {
    return T.translate('features.Navbar.ProductDropdown.modes.cloudSandbox');
  }
  return T.translate('features.Navbar.ProductDropdown.modes.localSandbox');
}

export function getModeWithCloudProvider() {
  let mode = getMode();
  let cloudProvider = getCloudProvider();
  if (cloudProvider === '') {
    return mode;
  }
  return mode + ' ' + T.translate('features.AboutPage.providers.forLabel') + ' ' + cloudProvider;
}

function getCloudProvider() {
  let provider = '';
  if (window.CDAP_CONFIG.sandboxMode === 'azure') {
    provider = T.translate('features.AboutPage.providers.azure');
  } else if (window.CDAP_CONFIG.sandboxMode === 'aws') {
    provider = T.translate('features.AboutPage.providers.aws');
  } else if (window.CDAP_CONFIG.sandboxMode === 'gcp') {
    provider = T.translate('features.AboutPage.providers.gcp');
  }
  return provider;
}
