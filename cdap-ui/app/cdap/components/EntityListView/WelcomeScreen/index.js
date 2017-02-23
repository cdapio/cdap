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

import React, {PropTypes} from 'react';
import T from 'i18n-react';
import ResourceCenterButton from 'components/ResourceCenterButton';
require('./WelcomeScreen.scss');

export default function WelcomeScreen({onClose, onAddEntity}) {
  return (
    <div className="splash-screen-container">
      <div className="splash-screen-first-time">
        <h2 className="welcome-message">
          {T.translate('features.EntityListView.SplashScreen.welcomeMessage1')}
        </h2>
        <div className="welcome-message">
          {T.translate('features.EntityListView.SplashScreen.welcomeMessage2')}
        </div>

        <div className="cdap-fist-icon">
          <span className="icon-fist" />
        </div>

        <div
          className="splash-screen-first-time-btn"
          onClick={onAddEntity}
        >
          {T.translate('features.EntityListView.SplashScreen.addentity')}
        </div>
        <div
          className="go-to-cdap"
          onClick={onClose}
        >
          {T.translate('features.EntityListView.SplashScreen.gotoLabel')}
        </div>
        <div className="splash-screen-disclaimer">
          <p>
            {T.translate('features.EntityListView.SplashScreen.disclaimerMessage')}
          </p>
        </div>
      </div>

      <div className="resource-center-hidden">
        <ResourceCenterButton />
      </div>
    </div>
  );
}
WelcomeScreen.propTypes = {
  onClose: PropTypes.func,
  onAddEntity: PropTypes.func
};
