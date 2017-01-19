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
import React, { Component, PropTypes } from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import ArtifactUploadWizardConfig from 'services/WizardConfigs/ArtifactUploadWizardConfig';
import MarketArtifactUploadWizardConfig from 'services/WizardConfigs/MarketArtifactUploadWizardConfig';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import ArtifactUploadActionCreator from 'services/WizardStores/ArtifactUpload/ActionCreator';
import T from 'i18n-react';

require('./ArtifactUpload.scss');

export default class ArtifactUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
  }
  getChildContext() {
    return {
      isMarket: this.props.isMarket
    };
  }
  componentWillUnmount() {
    ArtifactUploadStore.dispatch({
      type: ArtifactUploadActions.onReset
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
    return ArtifactUploadActionCreator.uploadArtifact();
  }


  render() {
    let input = this.props.input;
    let pkg = input.package || {};
    let headerLabel = input.headerLabel;

    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + (headerLabel ? headerLabel : T.translate('features.Wizard.ArtifactUpload.headerlabel'));
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={this.props.isMarket ? MarketArtifactUploadWizardConfig : ArtifactUploadWizardConfig}
          wizardType="ArtifactUpload"
          store={ArtifactUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}

ArtifactUploadWizard.defaultProps = {
  input: {
    action: {
      arguments: {}
    },
    package: {},
  },
  isMarket: false
};
ArtifactUploadWizard.childContextTypes = {
  isMarket: PropTypes.bool
};
ArtifactUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  isMarket: PropTypes.bool
};
