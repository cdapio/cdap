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

import React from 'react';
import WizardModal from 'components/WizardModal';
import Wizard from 'components/Wizard';
import PropTypes from 'prop-types';
import ExploreDatasetWizardConfig from './ExploreDatasetWizardConfig';
import ExploreDatasetStore from '../store/ExploreDatasetStore';


require('./ExploreDatasetWizard.scss');

class ExploreDatasetWizard extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      successInfo: {},
      activeStepId: 'operation',
    };
  }

  render() {
    return (
      <WizardModal
        title="Analyse"
        isOpen={this.props.showWizard}
        toggle={this.props.onClose}
        className="explore-datset-wizard">
        <Wizard
          wizardConfig={ExploreDatasetWizardConfig}
          wizardType="EDA-Operation"
          onSubmit={this.props.onSubmit}
          successInfo={this.state.successInfo}
          onClose={this.props.onClose}
          activeStepId={this.state.activeStepId}
          store={ExploreDatasetStore}
        />
      </WizardModal>
    );
  }
}

export default ExploreDatasetWizard;
ExploreDatasetWizard.propTypes = {
  showWizard: PropTypes.bool,
  onClose: PropTypes.func,
  onSubmit: PropTypes.func,
};
