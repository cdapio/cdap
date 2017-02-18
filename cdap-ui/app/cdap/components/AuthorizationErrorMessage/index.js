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

import React from 'react';
require('./AuthorizationMessage.scss');
import NamespaceStore from 'services/NamespaceStore';
import cookie from 'react-cookie';
import RedirectToLogin from 'services/redirect-to-login';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import T from 'i18n-react';

export default function AuthorizationErrorMessage() {
  let eventEmitter = ee(ee);
  const logout = () => {
    cookie.remove('show-splash-screen-for-session', {path: '/'});
    RedirectToLogin({statusCode: 401});
  };
  const openNamespaceCreateWizard = () => {
    eventEmitter.emit(globalEvents.CREATENAMESPACE);
  };
  let username = NamespaceStore.getState().username;
  return (
    <div className="auth-error-message">
      <h3>
        <span className="fa fa-exclamation-triangle"></span>
        <span>{T.translate('features.AuthorizationMessage.mainMessage')}</span>
      </h3>
      <div className="cta-section">
      <ul>
        <li>
          <span>{T.translate('features.AuthorizationMessage.callToAction1')}</span>
        </li>
        <li>
          <span>{T.translate('features.AuthorizationMessage.callToAction2.message1', {username})}</span>
          <span
            className="link"
            onClick={logout}
          >
            {T.translate('features.AuthorizationMessage.callToAction2.loginLabel')}
          </span>
          <span>{T.translate('features.AuthorizationMessage.callToAction2.message2')}</span>
        </li>
        <li>
          <span
            className="link"
            onClick={openNamespaceCreateWizard}
          >
            {T.translate('features.AuthorizationMessage.callToAction3.message1')}
          </span>
          <span>{T.translate('features.AuthorizationMessage.callToAction3.message2')}</span>
        </li>
      </ul>
        </div>
    </div>
  );
}
