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

import skinsJson from '../styles/skins';

export function applySkin() {
  if (window.CDAP_CONFIG.uiTheme) {
    const customizations = skinsJson[window.CDAP_CONFIG.uiTheme];
    Object.keys(customizations).forEach(cssVar => {
      const cssValue = customizations[cssVar];
      document.documentElement.style.setProperty(`--${cssVar}`, cssValue);
    });
  }
}
