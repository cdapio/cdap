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

import React, {PropTypes, Component} from 'react';
import T from 'i18n-react';
import ResourceCenterButton from 'components/ResourceCenterButton';
import PlusButtonStore from 'services/PlusButtonStore';
import MyUserStoreApi from 'api/userstore';
import isNil from 'lodash/isNil';
import isObject from 'lodash/isObject';
require('./WelcomeScreen.scss');

export default class WelcomeScreen extends Component {
  onAddEntity() {
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: true
      }
    });
  }
  onClose() {
    MyUserStoreApi
      .get()
      .subscribe(res => {
        if (isNil(res)) {
          res = {};
        }
        if (!isObject(res.property)) {
          res.property = {};
        }
        res.property['user-has-visited'] = true;
        MyUserStoreApi.set({}, res.property);
      });
    if (this.props.onClose) {
      this.props.onClose();
    }
  }
  render() {
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
            onClick={this.onAddEntity.bind(this)}
          >
            {T.translate('features.EntityListView.SplashScreen.addentity')}
          </div>
          <div
            className="go-to-cdap"
            onClick={this.onClose.bind(this)}
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
}
WelcomeScreen.propTypes = {
  onClose: PropTypes.func
};
