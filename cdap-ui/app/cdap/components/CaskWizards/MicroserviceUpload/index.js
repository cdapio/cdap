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
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadWizardConfig from 'services/WizardConfigs/MicroserviceUploadWizardConfig';
import MicroserviceUploadActionCreator from 'services/WizardStores/MicroserviceUpload/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

require('components/CaskWizards/ArtifactUpload/ArtifactUpload.scss');

export default class MicroserviceUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: props.isOpen || false,
      successInfo: {}
    };
    this.eventEmitter = ee(ee);
  }
  componentWillUnmount() {
    MicroserviceUploadStore.dispatch({
      type: MicroserviceUploadActions.onReset
    });
  }
  onSubmit() {
    return MicroserviceUploadActionCreator
      .uploadArtifact()
      .flatMap(() => {
        return MicroserviceUploadActionCreator.uploadConfigurationJson();
      })
      .flatMap(() => {
        return MicroserviceUploadActionCreator.createApplication();
      })
      .flatMap((res) => {
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
  buildSuccessInfo() {
    let appName = MicroserviceUploadStore.getState().general.instanceName;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.MicroserviceUpload.success', {appName});
    let buttonLabel = T.translate('features.Wizard.MicroserviceUpload.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    this.setState({
      successInfo: {
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
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={MicroserviceUploadWizardConfig}
          wizardType="MicroserviceUpload"
          store={MicroserviceUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          successInfo={this.state.successInfo}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}

MicroserviceUploadWizard.defaultProps = {
  input: {
    action: {
      arguments: {}
    },
    package: {},
  }
};

MicroserviceUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  buildSuccessInfo: PropTypes.func
};
