/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import {createNamespace, setNamespacePreferences, editNamespaceProperties} from 'services/WizardStores/AddNamespace/ActionCreator';
import T from 'i18n-react';
import {Observable} from 'rxjs/Observable';
import isEmpty from 'lodash/isEmpty';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';

export default class AddNamespaceWizard extends Component {
  state = {
    showWizard: this.props.isOpen,
    successInfo: {}
  };

  static propTypes = {
    isOpen: PropTypes.bool,
    context: PropTypes.string,
    onClose: PropTypes.func,
    isEdit: PropTypes.bool,
    editableFields: PropTypes.array,
    properties: PropTypes.object,
    activeStepId: PropTypes.string
  };

  static defaultProps = {
    isEdit: false,
    properties: {}
  };

  componentWillMount() {
    if (this.props.properties && !isEmpty(this.props.properties)) {
      AddNamespaceStore.dispatch({
        payload: { ...this.props.properties },
        type: AddNamespaceActions.setProperties
      });
    }
    if (this.props.editableFields && this.props.editableFields.length) {
      AddNamespaceStore.dispatch({
        payload: { editableFields: this.props.editableFields },
        type: AddNamespaceActions.setEditableFields
      });
    }
  }

  eventEmitter = ee(ee);

  componentWillReceiveProps({isOpen}) {
    this.setState({
      showWizard: isOpen
    });
  }

  onSubmit = () => {
    if (this.props.isEdit) {
      return editNamespaceProperties();
    } else {
      return this.createNamespaceAndSetPreferences();
    }
  };

  createNamespaceAndSetPreferences() {
    return createNamespace()
      .mergeMap(
        (res) => {
          if (res.includes('already exists')) {
            return Observable.throw(res);
          } else {
            this.eventEmitter.emit(globalEvents.NAMESPACECREATED);
            this.buildSuccessInfo(res);
            return setNamespacePreferences();
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
                onSubmit={this.onSubmit}
                successInfo={this.state.successInfo}
                onClose={this.props.onClose}
                store={AddNamespaceStore}
                activeStepId={this.props.activeStepId}
              />
            </WizardModal>
          :
            null
        }
      </div>
    );
  }
}
