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
import PluginArtifactUploadWizard from 'components/CaskWizards/PluginArtifactUpload';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import PluginArtifactUploadWizardConfig from 'services/WizardConfigs/PluginArtifactUploadWizardConfig';
import PluginArtifactUploadStore from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadStore';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class PluginUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.eventEmitter = ee(ee);
    this.onSubmit = this.onSubmit.bind(this);
    this.buildInfo = this.buildInfo.bind(this);
  }

  onSubmit() {
    this.eventEmitter.emit(globalEvents.ARTIFACTUPLOAD);
  }

  buildInfo() {
    let state = PluginArtifactUploadStore.getState();
    let pluginName = state.upload.jar.fileMetadataObj.name;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.PluginArtifact.success', {pluginName});
    let subtitle = T.translate('features.Wizard.PluginArtifact.subtitle');
    let buttonLabel = T.translate('features.Wizard.PluginArtifact.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');

    return {
      message,
      subtitle,
      buttonLabel,
      buttonUrl: window.getHydratorUrl({
        stateName: 'hydrator.create',
        stateParams: {
          namespace
        }
      }),
      linkLabel,
      linkUrl: window.getAbsUIUrl({
        namespaceId: namespace
      })
    };
  }

  render() {
    return (
      <PluginArtifactUploadWizard
        isOpen={this.props.isOpen}
        input={this.props.input}
        onClose={this.props.onClose}
        wizardConfig={PluginArtifactUploadWizardConfig}
        onSubmit={this.onSubmit}
        buildInfo={this.buildInfo}
      />
    );
  }
}

PluginUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
