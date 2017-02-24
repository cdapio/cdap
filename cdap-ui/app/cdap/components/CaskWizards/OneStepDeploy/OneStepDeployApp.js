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

import React, { Component, PropTypes } from 'react';
import OneStepDeployStore from 'services/WizardStores/OneStepDeploy/OneStepDeployStore';
import OneStepDeployActions from 'services/WizardStores/OneStepDeploy/OneStepDeployActions';
import NamespaceStore from 'services/NamespaceStore';
import {constructCdapUrl} from 'services/cdap-url-builder';
import 'whatwg-fetch';
import Rx from 'rx';
import OneStepDeployWizard from 'components/CaskWizards/OneStepDeploy';
import cookie from 'react-cookie';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import isNil from 'lodash/isNil';

export default class OneStepDeployApp extends Component {
  constructor(props) {
    super(props);
    this.eventEmitter = ee(ee);
    this.publishApp = this.publishApp.bind(this);
  }

  componentWillMount() {
    OneStepDeployStore.dispatch({
      type: OneStepDeployActions.setName,
      payload: this.props.input.package.label || this.props.input.package.name
    });
  }

  buildSuccessInfo(fetchResponse) {
    // fetchResponse has the format "Successfully deployed app {appName}"
    let appName = fetchResponse.slice(fetchResponse.indexOf('app') + 4);
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.ApplicationUpload.success', {appName});
    let buttonLabel = T.translate('features.Wizard.ApplicationUpload.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    let successInfo = {
      message,
      buttonLabel,
      buttonUrl: window.getAbsUIUrl({
        namespaceId: namespace,
        appId: appName
      }),
      linkLabel,
      linkUrl: window.getAbsUIUrl({
        namespaceId: namespace
      })
    };
    return successInfo;
  }

  publishApp() {
    const marketBasepath = `${window.CDAP_CONFIG.marketUrl}`;

    const {
      name,
      version
    } = this.props.input.package;

    let jarName;

    const args = this.props.input.action.arguments;

    args.forEach((arg) => {
      switch (arg.name) {
        case 'jar':
          jarName = arg.value;
          break;
      }
    });

    let marketPath = `${marketBasepath}/packages/${name}/${version}/${jarName}`;
    marketPath = encodeURIComponent(marketPath);

    let namespace = NamespaceStore.getState().selectedNamespace;

    let cdapPath = constructCdapUrl({
      _cdapPath: `/namespaces/${namespace}/apps`
    });
    cdapPath = encodeURIComponent(cdapPath);

    let headers = {
      'Content-Type': 'application/octet-stream',
      'X-Archive-Name': jarName,
    };

    if (window.CDAP_CONFIG.securityEnabled) {
      let token = cookie.load('CDAP_Auth_Token');
      if (!isNil(token)) {
        headers.Authorization = `Bearer ${token}`;
      }
    }

    let fetchUrl = `/forwardMarketToCdap?source=${marketPath}&target=${cdapPath}`;

    return Rx.Observable.create((observer) => {
      fetch(fetchUrl, {
        method: 'GET',
        headers,
        credentials: 'include'
      })
        .then((res) => {
          if (res.status > 299) {
            res.text()
              .then((err) => {
                observer.onError(err);
              });
          } else {
            // need to do this, because app name is returned in the response text
            res.text()
              .then((textResponse) => {
                let successInfo = this.buildSuccessInfo(textResponse);
                this.eventEmitter.emit(globalEvents.APPUPLOAD);
                observer.onNext(successInfo);
                observer.onCompleted();
              });
          }
        })
        .catch((err) => {
          observer.onError(err);
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
  onClose: PropTypes.func
};
