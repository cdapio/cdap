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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import MarketStore from 'components/Market/store/market-store';
import OneStepDeployStore from 'services/WizardStores/OneStepDeploy/OneStepDeployStore';
import OneStepDeployActions from 'services/WizardStores/OneStepDeploy/OneStepDeployActions';
import NamespaceStore from 'services/NamespaceStore';
import 'whatwg-fetch';
import { Observable } from 'rxjs/Observable';
import OneStepDeployWizard from 'components/CaskWizards/OneStepDeploy';
import Cookies from 'universal-cookie';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import isNil from 'lodash/isNil';
import SessionStore from 'services/SessionTokenStore';

const cookie = new Cookies();

export default class OneStepDeployApp extends Component {
  constructor(props) {
    super(props);
    this.eventEmitter = ee(ee);
    this.publishApp = this.publishApp.bind(this);
  }

  componentWillMount() {
    OneStepDeployStore.dispatch({
      type: OneStepDeployActions.setName,
      payload: this.props.input.package.label || this.props.input.package.name,
    });
  }

  buildSuccessInfo(fetchResponse) {
    // fetchResponse has the format "Successfully deployed app {appName}"
    let appName = fetchResponse.slice(fetchResponse.indexOf('app') + 4);
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.ApplicationUpload.success', { appName });
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    let successInfo = {
      message,
      linkLabel,
      linkUrl: `${window.getAbsUIUrl({
        namespaceId: namespace,
      })}/control`,
    };
    return successInfo;
  }

  publishApp() {
    const { name, version } = this.props.input.package;

    let jarName;

    const args = this.props.input.action.arguments;

    args.forEach((arg) => {
      switch (arg.name) {
        case 'jar':
          jarName = arg.value;
          break;
      }
    });

    const marketPath = `/packages/${name}/${version}/${jarName}`;
    const marketHost = MarketStore.getState().selectedMarketHost;
    const marketUrl = encodeURIComponent(`${marketHost}${marketPath}`);

    let namespace = NamespaceStore.getState().selectedNamespace;

    let cdapPath = `/v3/namespaces/${namespace}/apps`;
    cdapPath = encodeURIComponent(cdapPath);

    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': jarName,
      'Session-Token': SessionStore.getState(),
      'X-Requested-With': 'XMLHttpRequest',
    };

    if (window.CDAP_CONFIG.securityEnabled) {
      let token = cookie.get('CDAP_Auth_Token');
      if (!isNil(token)) {
        headers.Authorization = `Bearer ${token}`;
      }
    }

    let fetchUrl = `/forwardMarketToCdap?source=${marketUrl}&target=${cdapPath}`;

    return Observable.create((observer) => {
      fetch(fetchUrl, {
        method: 'GET',
        headers,
        credentials: 'include',
      })
        .then((res) => {
          if (res.status > 299) {
            res.text().then((err) => {
              observer.error(err);
            });
          } else {
            // need to do this, because app name is returned in the response text
            res.text().then((textResponse) => {
              let successInfo = this.buildSuccessInfo(textResponse);

              if (this.props.buildSuccessInfo) {
                successInfo = this.props.buildSuccessInfo();
              }
              this.eventEmitter.emit(globalEvents.APPUPLOAD);
              observer.next(successInfo);
              observer.complete();
            });
          }
        })
        .catch((err) => {
          observer.error(err);
        });
    });
  }

  render() {
    return (
      <OneStepDeployWizard
        isOpen={this.props.isOpen}
        input={this.props.input}
        onClose={this.props.onClose}
        onPublish={this.publishApp}
      />
    );
  }
}

OneStepDeployApp.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  buildSuccessInfo: PropTypes.func,
};
