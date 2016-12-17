/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import MarketPluginArtifactUploadWizardConfig from 'services/WizardConfigs/MarketPluginArtifactUploadWizardConfig';
import PluginArtifactUploadStore from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadStore';
import PluginArtifactUploadActions from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadActions';
import ArtifactUploadActionCreator from 'services/WizardStores/PluginArtifactUpload/ActionCreator';
import T from 'i18n-react';
import {MyMarketApi} from 'api/market';
import find from 'lodash/find';

require('./MarketHydratorPluginUpload.less');

export default class MarketHydratorPluginUpload extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
  }
  componentDidMount() {
    const args = this.props.input.action.arguments;
    let config = find(args, {name: 'config'});

    let params = {
      entityName: this.props.input.package.name,
      entityVersion: this.props.input.package.version,
      filename: config.value
    };
    MyMarketApi
      .getSampleData(params)
      .subscribe(res => {
        var jsonBlob = new Blob([res]);
        var jsonFile = new File([jsonBlob], config.value, {type: 'text/json'});
        PluginArtifactUploadStore.dispatch({
          type: PluginArtifactUploadActions.setJson,
          payload: {
            json: JSON.stringify(res),
            jsonFile
          }
        });
      });
  }
  componentWillUnmount() {
    PluginArtifactUploadStore.dispatch({
      type: PluginArtifactUploadActions.onReset
    });
  }

  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }

  onSubmit() {
    return ArtifactUploadActionCreator
      .uploadArtifact()
      .flatMap(() => ArtifactUploadActionCreator.uploadConfigurationJson());
  }


  render() {
    let wizardModalTitle = T.translate('features.Wizard.MarketHydratorPluginUpload.headerlabel');
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={MarketPluginArtifactUploadWizardConfig}
          wizardType="ArtifactUpload"
          store={PluginArtifactUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}

MarketHydratorPluginUpload.defaultProps = {
  input: {
    action: {
      arguments: {}
    },
    package: {}
  }
};
MarketHydratorPluginUpload.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
