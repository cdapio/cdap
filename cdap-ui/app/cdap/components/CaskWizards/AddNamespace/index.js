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
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import AddNamespaceWizardConfig from 'services/WizardConfigs/AddNamespaceWizardConfig';
import {MyNamespaceApi} from '../../../api/namespace';

export default class AddNamespaceWizard extends Component {
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

  onSubmit(){
    //PUT /v3/namespaces/<namespace-id>
    let ns = AddNamespaceStore.getState().general.name;
    let description = AddNamespaceStore.getState().general.description;

    MyNamespaceApi.create({
      namespace: ns,
      description: description
    });

    this.toggleWizard();
  }

  render() {
    return (
      <div>
        {
          this.state.showWizard ?
            <WizardModal
              title={this.props.context ? this.props.context + " | Add Namespace" : "Add Namespace"}
              isOpen={this.state.showWizard}
              toggle={this.toggleWizard.bind(this, false)}
              className="add-namespace-wizard"
            >
              <Wizard
                wizardConfig={AddNamespaceWizardConfig}
                onSubmit={this.onSubmit.bind(this)}
                onClose={this.toggleWizard.bind(this)}
                store={AddNamespaceStore}
              />
            </WizardModal>
          :
            null
        }
      </div>
    );
  }
}
AddNamespaceWizard.propTypes = {
  isOpen: PropTypes.bool,
  context: PropTypes.string,
  onClose: PropTypes.func
};
