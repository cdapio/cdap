/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import PropTypes from 'prop-types';

import React, { Component } from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import AddNamespaceActions from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import AddNamespaceWizardConfig from 'services/WizardConfigs/AddNamespaceWizardConfig';
import NamespaceStore from 'services/NamespaceStore';
import { PublishNamespace, PublishPreferences } from 'services/WizardStores/AddNamespace/ActionCreator';
import T from 'i18n-react';
import Rx from 'rx';

export default class AddNamespaceWizard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showWizard: this.props.isOpen,
      successInfo: {}
    };
  }
  componentWillReceiveProps({isOpen}) {
    this.setState({
      showWizard: isOpen
    });
  }
  createNamespace() {
    return PublishNamespace()
      .flatMap(
        (res) => {
          if (res.includes('already exists')) {
            return Rx.Observable.throw(res);
          } else {
            this.buildSuccessInfo(res);
            return PublishPreferences();
          }
        }
      );
  }
  componentWillUnmount() {
    AddNamespaceStore.dispatch({
      type: AddNamespaceActions.onReset
    });
  }
  buildSuccessInfo(responseText) {
    // responseText has the format "Namespace '{namespace}' created successfully."
    let newNamespaceId = responseText.split("'")[1];
    let currentNamespaceId = NamespaceStore.getState().selectedNamespace;
    let message = T.translate('features.Wizard.Add-Namespace.Status.creation-success-desc', {namespaceId: newNamespaceId});
    let buttonLabel = T.translate('features.Wizard.Add-Namespace.callToAction', {namespaceId: newNamespaceId});
    let linkLabel = T.translate('features.Wizard.GoToHomePage');
    this.setState({
      successInfo: {
        message,
        buttonLabel,
        buttonUrl: window.getAbsUIUrl({
          namespaceId: newNamespaceId,
        }),
        linkLabel,
        linkUrl: window.getAbsUIUrl({
          namespaceId: currentNamespaceId
        })
      }
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
            >
              <Wizard
                wizardConfig={AddNamespaceWizardConfig}
                wizardType="Add-Namespace"
                onSubmit={this.createNamespace.bind(this)}
                successInfo={this.state.successInfo}
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
  onClose: PropTypes.func
};
