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
require('./ResourcesTabContent.scss');

const PREFIX = 'features.PipelineConfigurations.Resources';

const mapStateToStepContentHeadingProps = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    engine: state.engine,
  };
};
const StepContentHeading = ({ isBatch, engine }) => {
  let engineDisplayLabel = getEngineDisplayLabel(engine, isBatch);
  return (
    <div className="step-content-heading">
      {T.translate(`${PREFIX}.contentHeading`, { engineDisplayLabel })}
    </div>
  );
};

StepContentHeading.propTypes = {
  isBatch: PropTypes.bool,
  engine: PropTypes.string,
};

const ConnectedStepContentHeading = connect(mapStateToStepContentHeadingProps)(StepContentHeading);

function ResourcesTabContent({ isBatch }) {
  return (
    <div
      id="resources-tab-content"
      className={classnames('configuration-step-content', {
        'batch-content': isBatch,
        'realtime-content': !isBatch,
      })}
    >
      <ConnectedStepContentHeading isBatch={isBatch} />
      {!isBatch ? <ClientResources /> : null}
      <div className="row">
        <DriverResources isBatch={isBatch} />
        <ExecutorResources isBatch={isBatch} />
      </div>
    </div>
  );
}

ResourcesTabContent.propTypes = {
  isBatch: PropTypes.bool,
};

const mapStateToProps = (state) => {
  return {
    isBatch: state.pipelineVisualConfiguration.isBatch,
  };
};
const ConnectedResourceTabContent = connect(mapStateToProps)(ResourcesTabContent);

export default ConnectedResourceTabContent;
