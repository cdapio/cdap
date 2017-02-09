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
import CreateStreamStore from 'services/WizardStores/CreateStream/CreateStreamStore';
import CreateStreamActions from 'services/WizardStores/CreateStream/CreateStreamActions';
import { PublishStream } from 'services/WizardStores/CreateStream/ActionCreator';
import UploadDataActionCreator from 'services/WizardStores/UploadData/ActionCreator';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import NamespaceStore from 'services/NamespaceStore';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';


import CreateStreamWizardConfig, {CreateStreamUploadWizardConfig} from 'services/WizardConfigs/CreateStreamWizardConfig';
import T from 'i18n-react';

export default class StreamCreateWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };

    this.setDefaultConfig();
    this.successInfo = {};
  }
  setDefaultConfig() {
    const args = this.props.input.action.arguments;

    args.forEach((arg) => {
      switch (arg.name) {
        case 'name':
          CreateStreamStore.dispatch({
            type: CreateStreamActions.setName,
            payload: {name: arg.value}
          });
          break;
        case 'description':
          CreateStreamStore.dispatch({
            type: CreateStreamActions.setDescription,
            payload: {description: arg.value}
          });
          break;
      }
    });
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
    CreateStreamStore.dispatch({
      type: CreateStreamActions.onReset
    });
  }
  createStream() {
    let state = CreateStreamStore.getState();
    let name = state.general.name;
    let currentNamespace = NamespaceStore.getState().selectedNamespace;
    // FIXME: How to handle empty error messages???
    return PublishStream()
      .flatMap(
        () => {
          if (this.props.withUploadStep) {
            // FIXME: I think we can chain this to the next step. TL;DR - will do.
            let url = `/namespaces/${currentNamespace}/streams/${state.general.name}/batch`;
            let uplodatastate = UploadDataStore.getState();
            let fileContents = uplodatastate.viewdata.data;
            let filename = uplodatastate.viewdata.filename;
            let filetype = 'text/' + filename.split('.').pop();
            return UploadDataActionCreator
              .uploadData({
                url,
                fileContents,
                headers: {
                  filename,
                  filetype
                }
              });
          }
          return Promise.resolve(name);
        }
      )
      .map((res) => {
        this.buildSuccessInfo(name, currentNamespace);
        this.eventEmitter.emit(globalEvents.STREAMCREATE);
        return res;
      });
  }
  buildSuccessInfo(streamId, namespace) {
    let defaultSuccessMessage = T.translate('features.Wizard.StreamCreate.success');
    let buttonLabel = T.translate('features.Wizard.StreamCreate.callToAction');
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    this.successInfo.message = `${defaultSuccessMessage} "${streamId}".`;
    this.successInfo.buttonLabel = buttonLabel;
    this.successInfo.buttonUrl = `/cdap/ns/${namespace}/streams/${streamId}`;
    this.successInfo.linkLabel = linkLabel;
    this.successInfo.linkUrl = `/cdap/ns/${namespace}`;
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
                wizardConfig={
                  this.props.withUploadStep ?
                    CreateStreamUploadWizardConfig
                    : CreateStreamWizardConfig
                  }
                wizardType="StreamCreate"
                onSubmit={this.createStream.bind(this)}
                successInfo={this.successInfo}
                onClose={this.toggleWizard.bind(this)}
                store={CreateStreamStore}/>
            </WizardModal>
          :
            null
        }
      </div>
    );
  }
}
StreamCreateWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func,
  withUploadStep: PropTypes.bool
};
