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
import CreateStreamWizardConfig from 'services/WizardConfigs/CreateStreamWizardConfig';
import T from 'i18n-react';
require('./StreamCreate.less');

export default class StreamCreateWizard extends Component {
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
    // FIXME: How to handle empty error messages???
    return PublishStream()
      .flatMap(
        res => {
          if (res) {
            return Promise.resolve(res);
          }
          return Promise.resolve('Successfully created Stream - ' + state.general.name);
        }
      );
  }
  render() {
    let wizardModalTitle = (this.props.context ? this.props.context + " | " : '') + T.translate('features.Wizard.StreamCreate.headerlabel') ;
    return (
      <div>
        {
          this.state.showWizard ?
            <WizardModal
              title={wizardModalTitle}
              isOpen={this.state.showWizard}
              toggle={this.toggleWizard.bind(this, false)}
              className="create-stream-wizard"
            >
              <Wizard
                wizardConfig={CreateStreamWizardConfig}
                onSubmit={this.createStream.bind(this)}
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
  context: PropTypes.string,
  onClose: PropTypes.func
};
