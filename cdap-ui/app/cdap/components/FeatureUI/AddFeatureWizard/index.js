/*
 * Copyright © 2017 Cask Data, Inc.
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

/* eslint react/prop-types: 0 */

import React from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import AddFeatureWizardConfig from '../../../services/WizardConfigs/AddFeatureWizardConfig';
import AddFeatureStore from '../../../services/WizardStores/AddFeature/AddFeatureStore';

require('./AddFeatureWizard.scss');

class AddFeatureWizard extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      successInfo: {},
      activeStepId: 'schema',
    };
  }

  render() {
    return (
      <WizardModal
        title="New Feature"
        isOpen={this.props.showWizard}
        toggle={this.props.onClose}
        className="add-feature-wizard">
        <Wizard
          wizardConfig={AddFeatureWizardConfig}
          wizardType="Add-Feature"
          onSubmit={this.props.onSubmit}
          successInfo={this.state.successInfo}
          onClose={this.props.onClose}
          activeStepId={this.state.activeStepId}
          store={AddFeatureStore}
        />
      </WizardModal>
    );
  }
}

export default AddFeatureWizard;
