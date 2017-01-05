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
import ApplicationUploadWizardConfig from 'services/WizardConfigs/ApplicationUploadWizardConfig';
import ApplicationUploadStore from 'services/WizardStores/ApplicationUpload/ApplicationUploadStore';
import ApplicationUploadActions from 'services/WizardStores/ApplicationUpload/ApplicationUploadActions';
import {UploadApplication} from 'services/WizardStores/ApplicationUpload/ActionCreator';
import T from 'i18n-react';

export default class ApplicationUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: props.isOpen || false
    };
  }
  toggleWizard(returnResult) {
    if (this.state.showWizard && this.props.onClose) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }
  componentWillUnmount() {
    ApplicationUploadStore.dispatch({
      type: ApplicationUploadActions.onReset
    });
  }
  onSubmit() {
    return UploadApplication();
  }
  render() {
    let input = this.props.input;
    let headerLabel = input.headerLabel;
    let wizardModalTitle = (headerLabel ? headerLabel : T.translate('features.Resource-Center.Application.modalheadertitle')) ;
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="artifact-upload-wizard"
      >
        <Wizard
          wizardConfig={ApplicationUploadWizardConfig}
          wizardType="ApplicationUpload"
          store={ApplicationUploadStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}

ApplicationUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  onClose: PropTypes.func,
  input: PropTypes.any
};
