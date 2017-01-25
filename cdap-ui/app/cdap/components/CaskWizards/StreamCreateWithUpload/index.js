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
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
import CreateStreamWithUploadAction from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadActions';
import { CreateStream } from 'services/WizardStores/CreateStreamWithUpload/ActionCreator';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
import NamespaceStore from 'services/NamespaceStore';

import CreateStreamUploadWizardConfig from 'services/WizardConfigs/CreateStreamWithUploadWizardConfig';
import T from 'i18n-react';
require('./StreamCreate.scss');
import cookie from 'react-cookie';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';


export default class StreamCreateWithUploadWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
    this.eventEmitter = ee(ee);
  }
  toggleWizard(returnResult) {
    if (this.state.showWizard) {
      this.props.onClose(returnResult);
    }
    this.setState({
      showWizard: !this.state.showWizard
    });
  }
  componentWillReceiveProps({isOpen}) {
    this.setState({
      showWizard: isOpen
    });
  }
  componentWillUnmount() {
    CreateStreamWithUploadStore.dispatch({
      type: CreateStreamWithUploadAction.onReset
    });
  }
  createStream() {
    let state = CreateStreamWithUploadStore.getState();
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    // FIXME: How to handle empty error messages???
    return CreateStream()
      .flatMap(
        () => {
          if (this.props.withUploadStep) {
            // FIXME: I think we can chain this to the next step. TL;DR - will do.
            let url = `/namespaces/${currentNamespace}/streams/${state.general.name}/batch`;
            let fileContents = state.upload.data;
            let filename = state.upload.filename;
            let filetype = 'text/' + filename.split('.').pop();
            let authToken = cookie.load('CDAP_Auth_Token');
            return UploadDataActionCreator
              .uploadData({
                url,
                fileContents,
                headers: {
                  filename,
                  filetype,
                  authToken
                }
              });
          }
          return Promise.resolve(state.general.name);
        }
      )
      .map((res) => {
        this.eventEmitter.emit(globalEvents.STREAMCREATE);
        return res;
      });
  }
  render() {
    let input = this.props.input || {};
    let pkg = input.package || {};
    let wizardModalTitle = (pkg.label ? pkg.label + " | " : '') + T.translate('features.Wizard.StreamCreate.headerlabel');
    return (
      <div>
        {
          this.state.showWizard ?
            // eww..
            <WizardModal
              title={wizardModalTitle}
              isOpen={this.state.showWizard}
              toggle={this.toggleWizard.bind(this, false)}
              className="create-stream-wizard"
            >
              <Wizard
                wizardConfig={CreateStreamUploadWizardConfig}
                wizardType="StreamCreate"
                onSubmit={this.createStream.bind(this)}
                onClose={this.toggleWizard.bind(this)}
                store={CreateStreamWithUploadStore}/>
            </WizardModal>
          :
            null
        }
      </div>
    );
  }
}
StreamCreateWithUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  withUploadStep: PropTypes.bool
};
