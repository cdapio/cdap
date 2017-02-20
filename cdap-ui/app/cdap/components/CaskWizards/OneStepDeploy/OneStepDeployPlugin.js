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
import {MyMarketApi} from 'api/market';
import {MyArtifactApi} from 'api/artifact';

export default class OneStepDeployPlugin extends Component {
  constructor(props) {
    super(props);

    this.publishPlugin = this.publishPlugin.bind(this);
  }

  componentWillMount() {
    OneStepDeployStore.dispatch({
      type: OneStepDeployActions.setName,
      payload: this.props.input.package.label || this.props.input.package.name
    });
  }

  publishPlugin() {
    const marketBasepath = `${window.CDAP_CONFIG.marketUrl}`;

    const {
      name,
      version
    } = this.props.input.package;

    const args = this.props.input.action.arguments;

    let pluginName,
        pluginVersion,
        pluginConfig,
        pluginJar;

    args.forEach((arg) => {
      switch (arg.name) {
        case 'name':
          pluginName = arg.value;
          break;
        case 'version':
          pluginVersion = arg.value;
          break;
        case 'config':
          pluginConfig = arg.value;
          break;
        case 'jar':
          pluginJar = arg.value;
          break;
      }
    });

    let marketPath = `${marketBasepath}/packages/${name}/${version}/${pluginJar}`;
    marketPath = encodeURIComponent(marketPath);

    let namespace = NamespaceStore.getState().selectedNamespace;

    let cdapPath = constructCdapUrl({
      _cdapPath: `/namespaces/${namespace}/artifacts/${pluginName}`
    });
    cdapPath = encodeURIComponent(cdapPath);


    return Rx.Observable.create((observer) => {
      MyMarketApi.getSampleData({
        entityName: name,
        entityVersion: version,
        filename: pluginConfig
      }).subscribe((res) => {
        let pluginJson = res;

        let artifactExtends = pluginJson.parents.reduce( (prev, curr) => `${prev}/${curr}`);
        let artifactPlugins = pluginJson.plugins || [];

        let headers = {
          'Content-Type': 'application/octet-stream',
          'Artifact-Version': pluginVersion,
          'Artifact-Extends': artifactExtends,
          'Artifact-Plugins': artifactPlugins
        };

        if (window.CDAP_CONFIG.securityEnabled) {
          let token = cookie.load('CDAP_Auth_Token');
          headers.Authorization = `Bearer ${token}`;
        }

        let fetchUrl = `/forwardMarketToCdap?source=${marketPath}&target=${cdapPath}`;

        fetch(fetchUrl, {
          method: 'GET',
          headers: headers
        })
          .then((res) => {
            if (res.status > 299) {
              res.text()
                .then((err) => {
                  observer.onError(err);
                });
            } else {
              MyArtifactApi
                .loadPluginConfiguration({
                  namespace,
                  artifactId: pluginName,
                  version: pluginVersion
                }, pluginJson.properties)
                .subscribe(() => {
                  observer.onNext();
                  observer.onCompleted();
                }, (error) => {
                  observer.onError(error);
                });
            }
          })
          .catch((err) => {
            observer.onError(err);
          });

      });

    });

  }

  render() {
    return (
      <OneStepDeployWizard
        isOpen={this.props.isOpen}
        input={this.props.input}
        onClose={this.props.onClose}
        onPublish={this.publishPlugin}
      />
    );
  }
}



OneStepDeployPlugin.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
