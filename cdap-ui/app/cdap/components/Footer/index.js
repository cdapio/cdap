/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

import React from 'react';
import T from 'i18n-react';
import { Theme } from 'services/ThemeHelper';

require('./Footer.scss');

export default function Footer() {
  if (Theme.showFooter === false) {
    return null;
  }

  const footerText = Theme.footerText || T.translate('features.licenseText');
  const footerUrl = Theme.footerLink || 'https://www.apache.org/licenses/LICENSE-2.0';
  return (
    <footer className="app-footer">
      <div className="container">
        <div className="row text-muted">
          <div>
            <p className="text-xs-center">
              <a href={footerUrl} target="_blank" rel="noopener noreferrer">
                {footerText}
              </a>
            </p>
          </div>
        </div>
      </div>
    </footer>
  );
}
