/*
 * Copyright © 2018 Cask Data, Inc.
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
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {getEngineDisplayLabel} from 'components/PipelineConfigurations/Store';
import DriverResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/DriverResources';
import ExecutorResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/ExecutorResources';
import ClientResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/ClientResources';
require('./ResourcesTabContent.scss');

const mapStateToStepContentHeadingProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    engine: state.engine
  };
};
const StepContentHeading = ({isBatch, engine}) => {
  return (
    <div className="step-content-heading">
      {`Specify the resources for the following processes of the ${getEngineDisplayLabel(engine, isBatch)} program`}
    </div>
  );
};

StepContentHeading.propTypes = {
  isBatch: PropTypes.bool,
  engine: PropTypes.string
};

const ConnectedStepContentHeading = connect(mapStateToStepContentHeadingProps)(StepContentHeading);

export default function ResourcesTabContent({isBatch}) {
  return (
    <div
      id="resources-tab-content"
      className="configuration-step-content"
    >
      <ConnectedStepContentHeading isBatch={isBatch} />
      { !isBatch ? <ClientResources /> : null }
      <DriverResources isBatch={isBatch} />
      <ExecutorResources isBatch={isBatch} />
    </div>
  );
}

ResourcesTabContent.propTypes = {
  isBatch: PropTypes.bool
};
