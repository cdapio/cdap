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
import {Observable} from 'rxjs/Observable';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadWizardConfig from 'services/WizardConfigs/MicroserviceUploadWizardConfig';
import MicroserviceUploadActionCreator from 'services/WizardStores/MicroserviceUpload/ActionCreator';
import MicroserviceQueueStore from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueStore';
import MicroserviceQueueActions from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueActions';
import NamespaceStore from 'services/NamespaceStore';
import {MyProgramApi} from 'api/program';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import isEmpty from 'lodash/isEmpty';

require('./MicroserviceUpload.scss');

export default class MicroserviceUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: false,
      successInfo: {}
    };
    this.eventEmitter = ee(ee);
  }
  componentWillMount() {
    return MicroserviceUploadActionCreator
      .findMicroserviceArtifact()
      .mergeMap((artifact) => {
        MicroserviceUploadStore.dispatch({
          type: MicroserviceUploadActions.setMicroserviceArtifact,
          payload: { artifact }
        });
        if (isEmpty(artifact)) {
          return Observable.of([]);
        }
        return MicroserviceUploadActionCreator.listMicroservicePlugins(artifact);
      })
      .mergeMap((plugins) => {
        MicroserviceUploadStore.dispatch({
          type: MicroserviceUploadActions.setDefaultMicroservicePlugins,
          payload: { plugins }
        });
        this.setState({
          showWizard: true
        });
        return MicroserviceUploadActionCreator.getMicroservicePluginProperties(plugins[0].name || '');
      })
      .subscribe((propertiesArr) => {
        if (propertiesArr.length > 0 && propertiesArr[0].properties) {
          MicroserviceUploadStore.dispatch({
            type: MicroserviceUploadActions.setMicroservicePluginProperties,
            payload: {pluginProperties: Object.keys(propertiesArr[0].properties)}
          });
        }
      }, () => {
        MicroserviceUploadStore.dispatch({
          type: MicroserviceUploadActions.setMicroservicePluginProperties,
          payload: {pluginProperties: []}
        });
      });
  }
  componentWillUnmount() {
    MicroserviceUploadStore.dispatch({
      type: MicroserviceUploadActions.onReset
    });
    MicroserviceQueueStore.dispatch({
      type: MicroserviceQueueActions.onReset
    });
  }
  onSubmit() {
    return MicroserviceUploadActionCreator
      .uploadArtifact()
      .mergeMap(() => {
        return MicroserviceUploadActionCreator.uploadConfigurationJson();
      })
      .mergeMap(() => {
        return MicroserviceUploadActionCreator.createApplication();
      })
      .mergeMap((res) => {
        this.buildSuccessInfo();
        this.eventEmitter.emit(globalEvents.APPUPLOAD);
        return res;
      });
  }
  toggleWizard(returnResult) {
    if (this.state.showWizard && this.props.onClose) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }
  startMicroservice() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let appId = MicroserviceUploadStore.getState().general.instanceName;

    let params = {
      namespace,
      appId,
      programType: 'workers',
      programId: 'microservice',
      action: 'start'
    };

    return MyProgramApi.action(params)
      .map((res) => {
        window.location.href = window.getAbsUIUrl({
          namespaceId: namespace,
          appId
        });
        return res;
      });
  }
  buildSuccessInfo() {
    let appName = MicroserviceUploadStore.getState().general.instanceName;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.MicroserviceUpload.success', {appName});
    let buttonLabel = T.translate('features.Wizard.MicroserviceUpload.callToAction');
    let links = [
      {
        linkLabel: T.translate('features.Wizard.MicroserviceUpload.secondaryCallToAction'),
        linkUrl: window.getAbsUIUrl({
          namespaceId: namespace,
          appId: appName
        })
      },
      {
        linkLabel: T.translate('features.Wizard.GoToHomePage'),
        linkUrl: window.getAbsUIUrl({
          namespaceId: namespace
        })
      }
    ];
    this.setState({
      successInfo: {
        message,
        buttonLabel,
        buttonUrl: 'javascript:;', // don't go anywhere, all behavior will be handled by buttonOnClick
        buttonOnClick: this.startMicroservice.bind(this),
        links
      }
    });
  }
  render() {
    let input = this.props.input;
    let headerLabel = input.headerLabel;
    let wizardModalTitle = (headerLabel ? headerLabel : T.translate('features.Resource-Center.Application.modalheadertitle'));
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="microservice-upload-wizard artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={MicroserviceUploadWizardConfig}
          wizardType="MicroserviceUpload"
          store={MicroserviceUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          successInfo={this.state.successInfo}
          onClose={this.toggleWizard.bind(this)}
        />
      </WizardModal>
    );
  }
}

MicroserviceUploadWizard.defaultProps = {
  input: {
    action: {
      arguments: {}
    },
    package: {}
  }
};

MicroserviceUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  buildSuccessInfo: PropTypes.func
};
