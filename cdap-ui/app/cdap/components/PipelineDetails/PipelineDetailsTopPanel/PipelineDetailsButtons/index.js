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
import {Provider, connect} from 'react-redux';
import PipelineConfigurationsStore from 'components/PipelineConfigurations/Store';
import ScheduleButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/ScheduleButton';
import PipelineConfigureButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineConfigureButton';
import PipelineStopButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineStopButton';
import PipelineRunButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineRunButton';
import PipelineSummaryButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineSummaryButton';

const mapStateToConfigureButton = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    pipelineName: ownProps.pipelineName,
    resolvedMacros: state.resolvedMacros,
    runtimeArgs: state.runtimeArgs,
    runs: ownProps.runs,
    currentRun: ownProps.currentRun
  };
};

const mapStateToRunButton = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    pipelineName: ownProps.pipelineName,
    runButtonLoading: ownProps.runButtonLoading,
    runError: ownProps.runError,
    resolvedMacros: state.resolvedMacros,
    runtimeArgs: state.runtimeArgs,
  };
};

const ConnectedConfigureButton = connect(mapStateToConfigureButton)(PipelineConfigureButton);
const ConnectedRunButton = connect(mapStateToRunButton)(PipelineRunButton);

export default function PipelineDetailsButtons({isBatch, pipelineName, schedule, maxConcurrentRuns, scheduleStatus, runs, currentRun, runButtonLoading, runError}) {
  return (
    <Provider store={PipelineConfigurationsStore}>
      <div className="pipeline-details-buttons">
        <ConnectedConfigureButton
          isBatch={isBatch}
          pipelineName={pipelineName}
        />
        <ScheduleButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          schedule={schedule}
          maxConcurrentRuns={maxConcurrentRuns}
          scheduleStatus={scheduleStatus}
        />
        <PipelineStopButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          runs={runs}
          currentRun={currentRun}
        />
        <ConnectedRunButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          runButtonLoading={runButtonLoading}
          runError={runError}
        />
        <PipelineSummaryButton
          isBatch={isBatch}
          pipelineName={pipelineName}
        />
      </div>
    </Provider>
  );
}

PipelineDetailsButtons.propTypes = {
  isBatch: PropTypes.boolean,
  pipelineName: PropTypes.string,
  schedule: PropTypes.string,
  maxConcurrentRuns: PropTypes.number,
  scheduleStatus: PropTypes.string,
  runs: PropTypes.array,
  currentRun: PropTypes.object,
  runButtonLoading: PropTypes.bool,
  runError: PropTypes.string
};
