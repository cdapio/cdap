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
import DirectiveArtifactUploadWizardConfig from 'services/WizardConfigs/DirectiveArtifactUploadWizardConfig';
import PluginArtifactUploadStore from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadStore';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';

export default class DirectiveUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.eventEmitter = ee(ee);
    this.onSubmit = this.onSubmit.bind(this);
    this.buildInfo = this.buildInfo.bind(this);
  }

  onSubmit() {
    this.eventEmitter.emit(globalEvents.DIRECTIVEUPLOAD);
    if (this.props.onSubmit) {
      this.props.onSubmit();
    }
  }

  buildInfo() {
    if (!this.props.displayCTA) { return; }

    let state = PluginArtifactUploadStore.getState();
    let pluginName = state.upload.jar.fileMetadataObj.name;
    let namespace = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.DirectiveUpload.success', {pluginName});
    let subtitle = T.translate('features.Wizard.DirectiveUpload.subtitle');
    let buttonLabel = T.translate('features.Wizard.DirectiveUpload.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');

    return {
      message,
      subtitle,
      buttonLabel,
      buttonUrl: window.getDataPrepUrl({
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
        wizardConfig={DirectiveArtifactUploadWizardConfig}
        onSubmit={this.onSubmit}
        buildInfo={this.buildInfo}
      />
    );
  }
}

DirectiveUploadWizard.defaultProps = {
  displayCTA: true
};

DirectiveUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  displayCTA: PropTypes.bool,
  onSubmit: PropTypes.func
};
