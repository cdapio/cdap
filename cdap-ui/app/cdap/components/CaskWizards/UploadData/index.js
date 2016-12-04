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
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import UploadDataWizardConfig from 'services/WizardConfigs/UploadDataWizardConfig';
import UploadDataActions from 'services/WizardStores/UploadData/UploadDataActions';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';
import T from 'i18n-react';
import cookie from 'react-cookie';

import head from 'lodash/head';

export default class UploadDataWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
    this.setDefaultConfig();
    this.prepareInputForSteps();
  }
  setDefaultConfig() {
    const args = this.props.input.action.arguments;

    args.forEach((arg) => {
      switch(arg.name) {
        case 'name':
          UploadDataStore.dispatch({
            type: UploadDataActions.setDestinationName,
            payload: {name: arg.value}
          });
          break;
      }
    });
  }
  prepareInputForSteps() {
    let action = this.props.input.action;
    let filename = head(action.arguments.filter(arg => arg.name === 'files'));

    if (filename && filename.value.length) {
      filename = filename.value[0];
    }
    UploadDataStore.dispatch({
      type: UploadDataActions.setFilename,
      payload: { filename }
    });
    UploadDataStore.dispatch({
      type: UploadDataActions.setPackageInfo,
      payload: {
        name: this.props.input.package.name,
        version: this.props.input.package.version
      }
    });
  }
  onSubmit() {
    let state = UploadDataStore.getState();
    let streamId = state.selectdestination.name;
    let filename = state.viewdata.filename;
    let filetype = 'text/' + filename.split('.').pop();
    let fileContents = state.viewdata.data;
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    let authToken = cookie.load('CDAP_Auth_Token');

    return UploadDataActionCreator.uploadData({
      url: `/namespaces/${currentNamespace}/streams/${streamId}/batch`,
      fileContents,
      headers: {
        filetype,
        filename,
        authToken
      }
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
  componentWillUnmount() {
    UploadDataStore.dispatch({
      type: UploadDataActions.onReset
    });
  }
  render() {
    let input = this.props.input;
    let pkg = input.package || {};

    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.UploadData.headerlabel') ;
    return (
      <WizardModal
        title={wizardModalTitle}
        isOpen={this.state.showWizard}
        toggle={this.toggleWizard.bind(this, false)}
        className="upload-data-wizard"
      >
        <Wizard
          wizardConfig={UploadDataWizardConfig}
          wizardType="UploadData"
          store={UploadDataStore}
          onSubmit={this.onSubmit.bind(this)}
          onClose={this.toggleWizard.bind(this)}/>
      </WizardModal>
    );
  }
}
UploadDataWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
