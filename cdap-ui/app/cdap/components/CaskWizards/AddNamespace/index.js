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
import { PublishNamespace } from 'services/WizardStores/AddNamespace/ActionCreator';
import Redirect from 'react-router/Redirect';

export default class AddNamespaceWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      redirectTo: false
    };
    this.redirect = this.redirect.bind(this);
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

  createNamespace(){
    let state = AddNamespaceStore.getState();
    return PublishNamespace()
      .flatMap(
        res => {
          if (res) {
            return Promise.resolve(res);
          }
          return Promise.resolve(state.general.name);
        }
      );
  }
  componentWillUnmount() {
    AddNamespaceStore.dispatch({
      type: AddNamespaceActions.onReset
    });
  }

  redirect(){
    this.setState({
      redirectTo: true
    });
  }

  render() {
    return (
      <div>
      {
        this.state.redirectTo && <Redirect to="/" />
      }
        {
          this.state.showWizard ?
            <WizardModal
              title={this.props.context ? this.props.context + " | Add Namespace" : "Add Namespace"}
              isOpen={this.state.showWizard}
              toggle={this.redirect}
              className="add-namespace-wizard"
            >
              <Wizard
                wizardConfig={AddNamespaceWizardConfig}
                onSubmit={this.createNamespace.bind(this)}
                onClose={this.redirect}
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
