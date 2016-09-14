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
import { PublishStream } from 'services/WizardStores/CreateStream/ActionCreator';
import CreateStreamWizardConfig from 'services/WizardConfigs/CreateStreamWizardConfig';

export default class StreamCreateWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
  }
  toggleWizard() {
    this.setState({
      showWizard: !this.state.showWizard
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
    return (
      <div>
        {
          this.state.showWizard ?
            <WizardModal
              title="Create Stream"
              isOpen={this.state.showWizard}
              toggle={this.toggleWizard.bind(this)}
              className="create-stream-wizard"
            >
              <Wizard
                wizardConfig={CreateStreamWizardConfig}
                onSubmit={this.createStream.bind(this)}
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
  isOpen: PropTypes.bool
};
