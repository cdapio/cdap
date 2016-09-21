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
import UploadDataWizardConfig from 'services/WizardConfigs/UploadDataWizardConfig';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';

require('./UploadData.less');

export default class UploadDataWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
  }
  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }
  render() {
    return (
      <WizardModal
        title={this.props.context ? this.props.context + " | Upload Data" : "Upload Data"}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="upload-data-wizard"
      >
        <Wizard
          wizardConfig={UploadDataWizardConfig}
          store={UploadDataStore}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}
UploadDataWizard.propTypes = {
  isOpen: PropTypes.bool,
  context: PropTypes.string,
  onClose: PropTypes.func
};
