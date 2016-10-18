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
import AddNamespaceActions from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import AddNamespaceWizardConfig from 'services/WizardConfigs/AddNamespaceWizardConfig';
import { PublishNamespace, PublishPreferences } from 'services/WizardStores/AddNamespace/ActionCreator';

export default class AddNamespaceWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen
    };
  }
  componentWillReceiveProps({isOpen}) {
    this.setState({
      showWizard: isOpen
    });
  }
  createNamespace(){
    return PublishNamespace()
      .flatMap(
        () => {
          return PublishPreferences();
        }
      );
  }
  componentWillUnmount() {
    AddNamespaceStore.dispatch({
      type: AddNamespaceActions.onReset
    });
  }

  render() {
    return (
      <div>
        {
          this.state.showWizard ?
            <WizardModal
              title={this.props.context ? this.props.context + " | Add Namespace" : "Add Namespace"}
              isOpen={this.state.showWizard}
              toggle={this.props.onClose}
              className="add-namespace-wizard"
              backdrop={this.props.backdrop}
            >
              <Wizard
                wizardConfig={AddNamespaceWizardConfig}
                wizardType="Add-Namespace"
                onSubmit={this.createNamespace.bind(this)}
                onClose={this.props.onClose}
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
  onClose: PropTypes.func,
  backdrop: PropTypes.bool
};
