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
import { connect } from 'react-redux';
import { getEngineDisplayLabel } from 'components/PipelineConfigurations/Store';
import DriverResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/DriverResources';
import ExecutorResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/ExecutorResources';
import ClientResources from 'components/PipelineConfigurations/ConfigurationsContent/ResourcesTabContent/ClientResources';
import T from 'i18n-react';
import classnames from 'classnames';
import { GLOBALS } from 'services/global-constants';
require('./ResourcesTabContent.scss');

const PREFIX = 'features.PipelineConfigurations.Resources';

const mapStateToStepContentHeadingProps = (state, ownProps) => {
  return {
    pipelineType: ownProps.pipelineType,
    engine: state.engine,
  };
};
const StepContentHeading = ({ pipelineType, engine }) => {
  let engineDisplayLabel = getEngineDisplayLabel(engine, pipelineType);
  return (
    <div className="step-content-heading">
      {T.translate(`${PREFIX}.contentHeading`, { engineDisplayLabel })}
    </div>
  );
};

StepContentHeading.propTypes = {
  pipelineType: PropTypes.string,
  engine: PropTypes.string,
};

const ConnectedStepContentHeading = connect(mapStateToStepContentHeadingProps)(StepContentHeading);

function ResourcesTabContent({ pipelineType }) {
  const isBatch = GLOBALS.etlBatchPipelines.includes(pipelineType);
  return (
    <div
      id="resources-tab-content"
      className={classnames('configuration-step-content', {
        'batch-content': isBatch,
        'realtime-content': !isBatch,
      })}
    >
      <ConnectedStepContentHeading pipelineType={pipelineType} />
      <div className="resource-container">
        {!isBatch ? <ClientResources /> : null}
        <DriverResources />
        <ExecutorResources pipelineType={pipelineType} />
      </div>
    </div>
  );
}

ResourcesTabContent.propTypes = {
  pipelineType: PropTypes.string,
};

const mapStateToProps = (state) => {
  return {
    pipelineType: state.pipelineVisualConfiguration.pipelineType,
  };
};
const ConnectedResourceTabContent = connect(mapStateToProps)(ResourcesTabContent);

export default ConnectedResourceTabContent;
